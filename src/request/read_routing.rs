use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::proto::metapb;
use crate::region::RegionWithLeader;
use crate::Error;
use crate::ReplicaReadType;
use crate::Result;

/// Internal read routing configuration for a single request execution plan.
///
/// This is **not** exposed as a public API because it contains implementation details (like
/// retry-time overrides).
#[derive(Clone, Debug)]
pub(crate) struct ReadRouting {
    replica_read: ReplicaReadType,
    stale_read: bool,
    seed: u32,
    force_leader: Arc<AtomicBool>,
    input_request_source: Option<Arc<str>>,
}

impl Default for ReadRouting {
    fn default() -> Self {
        Self {
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            seed: 0,
            force_leader: Arc::new(AtomicBool::new(false)),
            input_request_source: None,
        }
    }
}

impl ReadRouting {
    pub(crate) fn new(replica_read: ReplicaReadType, stale_read: bool) -> Self {
        Self {
            replica_read,
            stale_read,
            ..Default::default()
        }
    }

    pub(crate) fn set_replica_read(&mut self, replica_read: ReplicaReadType) {
        self.replica_read = replica_read;
    }

    pub(crate) fn set_stale_read(&mut self, stale_read: bool) {
        self.stale_read = stale_read;
    }

    pub(crate) fn set_seed(&mut self, seed: u32) {
        self.seed = seed;
    }

    pub(crate) fn with_request_source(mut self, request_source: Option<Arc<str>>) -> Self {
        self.input_request_source = request_source;
        self
    }

    pub(crate) fn with_seed(mut self, seed: u32) -> Self {
        self.seed = seed;
        self
    }

    pub(crate) fn request_source(&self) -> Option<&str> {
        self.input_request_source.as_deref()
    }

    pub(crate) fn is_stale_read(&self) -> bool {
        self.stale_read && !self.is_forced_leader()
    }

    pub(crate) fn is_forced_leader(&self) -> bool {
        self.force_leader.load(Ordering::Relaxed)
    }

    pub(crate) fn force_leader(&self) {
        self.force_leader.store(true, Ordering::Relaxed);
    }

    pub(crate) fn select_peer(
        &self,
        region: &RegionWithLeader,
        attempt: usize,
    ) -> Result<ReadPeer> {
        self.select_peer_inner(region, attempt, true)
    }

    pub(crate) fn select_peer_for_request_source(
        &self,
        region: &RegionWithLeader,
    ) -> Result<ReadPeer> {
        self.select_peer_inner(region, 0, false)
    }

    fn select_peer_inner(
        &self,
        region: &RegionWithLeader,
        attempt: usize,
        respect_overrides: bool,
    ) -> Result<ReadPeer> {
        let leader = region.leader.as_ref().ok_or(Error::LeaderNotFound {
            region: region.ver_id(),
        })?;

        // `RegionWithLeader::leader` is authoritative; peers list is only used for replica
        // selection and may include witnesses (which cannot serve reads).
        let mut followers = Vec::new();
        let mut learners = Vec::new();
        for peer in &region.region.peers {
            if peer.is_witness {
                continue;
            }
            if peer.store_id == leader.store_id {
                continue;
            }
            match metapb::PeerRole::try_from(peer.role).unwrap_or(metapb::PeerRole::Voter) {
                metapb::PeerRole::Learner => learners.push(peer.clone()),
                _ => followers.push(peer.clone()),
            }
        }

        // Overrides (e.g. stale-read meeting locks, forcing leader re-read).
        if respect_overrides && self.is_forced_leader() {
            return Ok(ReadPeer::leader(leader.clone()));
        }

        if self.stale_read {
            // Stale read uses `Context.stale_read` (and keeps `replica_read=false`), and prefers
            // non-leader replicas. If no replica exists, it falls back to leader.
            let mut replicas = followers;
            replicas.extend(learners);
            let target = if attempt == 0 {
                pick_peer(&replicas, region.id(), self.seed, attempt).unwrap_or_else(|| {
                    // `leader` has already been validated to exist.
                    leader.clone()
                })
            } else {
                // Retry fallback: after a failed stale-read attempt, prefer leader to reduce the
                // chance of repeatedly hitting `RegionError.data_is_not_ready` on lagging replicas.
                //
                // Note: we still keep `Context.stale_read=true` so the server can apply stale-read
                // semantics even when the leader serves the request.
                leader.clone()
            };
            return Ok(ReadPeer {
                target_peer: target,
                replica_read: false,
                stale_read: true,
            });
        }

        let target = match self.replica_read {
            ReplicaReadType::Leader => leader.clone(),
            ReplicaReadType::PreferLeader if attempt == 0 => leader.clone(),
            ReplicaReadType::Follower if attempt == 0 => {
                pick_peer(&followers, region.id(), self.seed, attempt)
                    .unwrap_or_else(|| leader.clone())
            }
            ReplicaReadType::Learner if attempt == 0 => {
                pick_peer(&learners, region.id(), self.seed, attempt)
                    .unwrap_or_else(|| leader.clone())
            }
            ReplicaReadType::Mixed if attempt == 0 => {
                let mut replicas = followers;
                replicas.extend(learners);
                pick_peer(&replicas, region.id(), self.seed, attempt)
                    .unwrap_or_else(|| leader.clone())
            }
            // Retry fallback: prefer leader reads when we have already retried once.
            ReplicaReadType::Follower
            | ReplicaReadType::Learner
            | ReplicaReadType::Mixed
            | ReplicaReadType::PreferLeader => {
                if self.replica_read == ReplicaReadType::PreferLeader {
                    let mut replicas = followers;
                    replicas.extend(learners);
                    pick_peer(&replicas, region.id(), self.seed, attempt)
                        .unwrap_or_else(|| leader.clone())
                } else {
                    leader.clone()
                }
            }
        };

        Ok(ReadPeer {
            replica_read: target.store_id != leader.store_id,
            stale_read: false,
            target_peer: target,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadPeer {
    pub(crate) target_peer: metapb::Peer,
    pub(crate) replica_read: bool,
    pub(crate) stale_read: bool,
}

impl ReadPeer {
    fn leader(peer: metapb::Peer) -> Self {
        Self {
            target_peer: peer,
            replica_read: false,
            stale_read: false,
        }
    }
}

fn pick_peer(
    peers: &[metapb::Peer],
    region_id: u64,
    seed: u32,
    attempt: usize,
) -> Option<metapb::Peer> {
    if peers.is_empty() {
        return None;
    }
    // Deterministic selection (stable across retries for the same region+seed).
    let idx = (region_id as u32)
        .wrapping_add(seed)
        .wrapping_add(attempt as u32)
        % (peers.len() as u32);
    Some(peers[idx as usize].clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer(store_id: u64, role: metapb::PeerRole) -> metapb::Peer {
        metapb::Peer {
            store_id,
            role: role as i32,
            ..Default::default()
        }
    }

    fn make_region(leader_store_id: u64, followers: &[u64], learners: &[u64]) -> RegionWithLeader {
        make_region_with_id(1, leader_store_id, followers, learners, &[])
    }

    fn make_region_with_id(
        region_id: u64,
        leader_store_id: u64,
        followers: &[u64],
        learners: &[u64],
        witnesses: &[u64],
    ) -> RegionWithLeader {
        let leader = make_peer(leader_store_id, metapb::PeerRole::Voter);
        let mut peers = Vec::with_capacity(1 + followers.len() + learners.len());
        peers.push(leader.clone());
        peers.extend(
            followers
                .iter()
                .copied()
                .map(|id| make_peer(id, metapb::PeerRole::Voter)),
        );
        peers.extend(
            learners
                .iter()
                .copied()
                .map(|id| make_peer(id, metapb::PeerRole::Learner)),
        );
        peers.extend(witnesses.iter().copied().map(|id| metapb::Peer {
            store_id: id,
            role: metapb::PeerRole::Voter as i32,
            is_witness: true,
            ..Default::default()
        }));
        RegionWithLeader {
            region: metapb::Region {
                id: region_id,
                peers,
                ..Default::default()
            },
            leader: Some(leader),
        }
    }

    #[test]
    fn default_selects_leader() {
        let routing = ReadRouting::default();
        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);
    }

    #[test]
    fn follower_read_prefers_follower_then_falls_back_to_leader() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Follower);
        let region = make_region(1, &[2], &[]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);
        assert!(!selected.stale_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);
    }

    #[test]
    fn stale_read_prefers_replicas_then_falls_back_to_leader() {
        let mut routing = ReadRouting::default();
        routing.set_stale_read(true);

        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(!selected.replica_read);
        assert!(selected.stale_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(selected.stale_read);

        // No replicas: fall back to leader on the first attempt too.
        let region = make_region(1, &[], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(selected.stale_read);
    }

    #[test]
    fn force_leader_overrides_stale_read() {
        let mut routing = ReadRouting::default();
        routing.set_stale_read(true);
        routing.force_leader();

        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);
    }

    #[test]
    fn prefer_leader_retries_can_shift_to_replica() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::PreferLeader);
        let region = make_region(1, &[2], &[]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);
    }

    #[test]
    fn prefer_leader_retry_attempts_spread_across_replicas_deterministically() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::PreferLeader);
        routing.set_seed(0);

        let region = make_region_with_id(1, 1, &[2, 3], &[], &[]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);

        let selected = routing.select_peer(&region, 2).unwrap();
        assert_eq!(selected.target_peer.store_id, 3);
        assert!(selected.replica_read);
    }

    #[test]
    fn learner_read_prefers_learner_then_falls_back_to_leader() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Learner);
        let region = make_region(1, &[2], &[3]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 3);
        assert!(selected.replica_read);
        assert!(!selected.stale_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);

        // No learners: fall back to leader on the first attempt too.
        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
    }

    #[test]
    fn mixed_read_can_pick_followers_or_learners() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Mixed);
        let region = make_region_with_id(1, 1, &[2], &[3], &[]);

        routing.set_seed(0);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 3);
        assert!(selected.replica_read);

        routing.set_seed(1);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);

        // No replicas: fall back to leader.
        let region = make_region(1, &[], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
    }

    #[test]
    fn mixed_read_retry_falls_back_to_leader() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Mixed);

        let region = make_region_with_id(1, 1, &[2], &[3], &[]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert!(selected.replica_read, "first attempt should prefer replicas");

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);
    }

    #[test]
    fn witness_peers_are_excluded_from_replica_selection() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Follower);

        // Store 2 is a witness and must not be selected for replica reads.
        let region = make_region_with_id(1, 1, &[3], &[], &[2]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 3);
        assert!(selected.replica_read);

        // If all replicas are witnesses, fall back to leader.
        let region = make_region_with_id(1, 1, &[], &[], &[2]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
    }

    #[test]
    fn stale_read_retry_falls_back_to_leader_but_keeps_stale_read_flag() {
        let mut routing = ReadRouting::default();
        routing.set_stale_read(true);
        let region = make_region(1, &[2], &[]);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.stale_read);
        assert!(!selected.replica_read);

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(selected.stale_read);
        assert!(!selected.replica_read);
    }

    #[test]
    fn deterministic_replica_selection_depends_on_seed() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::PreferLeader);

        let region = make_region_with_id(1, 1, &[2, 3], &[], &[]);

        routing.set_seed(0);
        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);

        routing.set_seed(1);
        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 3);
    }

    #[test]
    fn select_peer_for_request_source_ignores_force_leader_override() {
        let mut routing = ReadRouting::default();
        routing.set_stale_read(true);
        routing.force_leader();

        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer_for_request_source(&region).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.stale_read);

        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.stale_read);
    }

    #[test]
    fn error_when_leader_missing() {
        let routing = ReadRouting::default();
        let region = RegionWithLeader {
            region: metapb::Region {
                id: 1,
                ..Default::default()
            },
            leader: None,
        };
        assert!(matches!(
            routing.select_peer(&region, 0),
            Err(Error::LeaderNotFound { .. })
        ));
    }

    #[test]
    fn request_source_is_stored() {
        let routing = ReadRouting::default().with_request_source(Some(Arc::<str>::from("src")));
        assert_eq!(routing.request_source(), Some("src"));
    }
}
