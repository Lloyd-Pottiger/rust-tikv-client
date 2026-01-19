use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::store_health::StoreHealthMap;
use crate::proto::metapb;
use crate::region::{RegionWithLeader, StoreId};
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
    store_match: Arc<[StoreId]>,
    store_health: StoreHealthMap,
}

impl Default for ReadRouting {
    fn default() -> Self {
        Self {
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            seed: 0,
            force_leader: Arc::new(AtomicBool::new(false)),
            input_request_source: None,
            store_match: Arc::from([]),
            store_health: StoreHealthMap::default(),
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

    #[cfg(test)]
    pub(crate) fn with_store_match(mut self, stores: impl Into<Vec<StoreId>>) -> Self {
        self.store_match = Arc::<[StoreId]>::from(stores.into());
        self
    }

    pub(crate) fn mark_store_slow_for(&self, store_id: crate::region::StoreId, duration: Duration) {
        self.store_health.mark_slow_for(store_id, duration);
    }

    #[cfg(test)]
    pub(crate) fn with_store_health(mut self, store_health: StoreHealthMap) -> Self {
        self.store_health = store_health;
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
            ReplicaReadType::PreferLeader if attempt == 0 => {
                if self.store_health.is_slow_now(leader.store_id)
                    && (!followers.is_empty() || !learners.is_empty())
                {
                    // Strongly avoid a slow leader on the first attempt when replicas exist.
                    let mut replicas = followers;
                    replicas.extend(learners);
                    self.select_peer_by_score(
                        region.id(),
                        attempt,
                        leader,
                        &replicas,
                        ScoreStrategy {
                            prefer_leader: true,
                            ..Default::default()
                        },
                    )
                } else {
                    self.select_peer_by_score(
                        region.id(),
                        attempt,
                        leader,
                        &region.region.peers,
                        ScoreStrategy {
                            prefer_leader: true,
                            ..Default::default()
                        },
                    )
                }
            }
            ReplicaReadType::Follower if attempt == 0 => {
                pick_peer(&followers, region.id(), self.seed, attempt)
                    .unwrap_or_else(|| leader.clone())
            }
            ReplicaReadType::Learner if attempt == 0 => {
                pick_peer(&learners, region.id(), self.seed, attempt)
                    .unwrap_or_else(|| leader.clone())
            }
            ReplicaReadType::Mixed if attempt == 0 => self.select_peer_by_score(
                region.id(),
                attempt,
                leader,
                &region.region.peers,
                ScoreStrategy::default(),
            ),
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

    fn select_peer_by_score(
        &self,
        region_id: u64,
        attempt: usize,
        leader: &metapb::Peer,
        peers: &[metapb::Peer],
        strategy: ScoreStrategy,
    ) -> metapb::Peer {
        let mut max_score: StoreSelectionScore = StoreSelectionScore(-1);
        let mut max_peers: Vec<metapb::Peer> = Vec::new();

        for peer in peers {
            if peer.is_witness {
                continue;
            }
            let is_leader = peer.store_id == leader.store_id;
            let score = calculate_score(
                peer,
                is_leader,
                &self.store_health,
                &self.store_match,
                strategy,
            );
            match score.cmp(&max_score) {
                std::cmp::Ordering::Greater => {
                    max_score = score;
                    max_peers.clear();
                    max_peers.push(peer.clone());
                }
                std::cmp::Ordering::Equal => max_peers.push(peer.clone()),
                std::cmp::Ordering::Less => {}
            }
        }

        pick_peer(&max_peers, region_id, self.seed, attempt).unwrap_or_else(|| leader.clone())
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

#[derive(Clone, Copy, Debug, Default)]
struct ScoreStrategy {
    try_leader: bool,
    prefer_leader: bool,
    learner_only: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
struct StoreSelectionScore(i64);

const FLAG_NOT_ATTEMPTED: i64 = 1 << 0;
const FLAG_NORMAL_PEER: i64 = 1 << 1;
const FLAG_PREFER_LEADER: i64 = 1 << 2;
const FLAG_LABEL_MATCHES: i64 = 1 << 3;
const FLAG_NOT_SLOW: i64 = 1 << 4;

fn calculate_score(
    peer: &metapb::Peer,
    is_leader: bool,
    store_health: &StoreHealthMap,
    store_match: &[StoreId],
    strategy: ScoreStrategy,
) -> StoreSelectionScore {
    let mut score = StoreSelectionScore(0);

    let label_matching_enabled = !store_match.is_empty();
    let label_matches = store_match.is_empty() || store_match.contains(&peer.store_id);
    if label_matches {
        score.0 |= FLAG_LABEL_MATCHES;
    }

    let is_slow = store_health.is_slow_now(peer.store_id);
    if !is_slow {
        score.0 |= FLAG_NOT_SLOW;
    }

    // We do not currently track per-peer attempts like client-go; treat the first plan attempt as
    // "not attempted" for score purposes.
    score.0 |= FLAG_NOT_ATTEMPTED;

    let role = metapb::PeerRole::try_from(peer.role).unwrap_or(metapb::PeerRole::Voter);
    if is_leader {
        if strategy.prefer_leader {
            // Compatible with client-go: only "prefer" the leader when it is not slow; otherwise
            // treat it as a normal peer.
            if !is_slow {
                score.0 |= FLAG_PREFER_LEADER;
            } else {
                score.0 |= FLAG_NORMAL_PEER;
            }
        } else if strategy.try_leader {
            // Compatible with client-go: when label matching is enabled, prefer selecting the
            // leader among replicas with the same label-matching result.
            if label_matching_enabled {
                score.0 |= FLAG_PREFER_LEADER;
            } else {
                score.0 |= FLAG_NORMAL_PEER;
            }
        }
    } else if strategy.learner_only {
        if role == metapb::PeerRole::Learner {
            score.0 |= FLAG_NORMAL_PEER;
        }
    } else {
        score.0 |= FLAG_NORMAL_PEER;
    }

    score
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
    use std::time::Duration;

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
    fn prefer_leader_avoids_slow_leader_on_first_attempt() {
        let health = StoreHealthMap::default();
        health.mark_slow_for(1, Duration::from_secs(60));

        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::PreferLeader);
        let routing = routing.with_store_health(health);

        let region = make_region(1, &[2], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);
        assert!(!selected.stale_read);
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
        assert!(
            selected.replica_read,
            "first attempt should prefer replicas"
        );

        let selected = routing.select_peer(&region, 1).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
        assert!(!selected.stale_read);
    }

    #[test]
    fn score_prefers_leader_when_replicas_are_slow() {
        let health = StoreHealthMap::default();
        health.mark_slow_for(2, Duration::from_secs(60));
        health.mark_slow_for(3, Duration::from_secs(60));

        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::Mixed);
        routing.set_seed(0);
        let routing = routing.with_store_health(health);

        let region = make_region_with_id(1, 1, &[2, 3], &[], &[]);
        let selected = routing.select_peer(&region, 0).unwrap();
        assert_eq!(selected.target_peer.store_id, 1);
        assert!(!selected.replica_read);
    }

    #[test]
    fn score_prefers_label_matched_replica_over_prefer_leader() {
        let mut routing = ReadRouting::default();
        routing.set_replica_read(ReplicaReadType::PreferLeader);

        let region = make_region_with_id(1, 1, &[2], &[], &[]);
        let selected = routing
            .with_store_match(vec![2])
            .select_peer(&region, 0)
            .unwrap();
        assert_eq!(selected.target_peer.store_id, 2);
        assert!(selected.replica_read);
    }

    #[test]
    fn calculate_score_matches_client_go_semantics_for_key_cases() {
        let health = StoreHealthMap::default();
        let leader = make_peer(1, metapb::PeerRole::Voter);
        let follower = make_peer(2, metapb::PeerRole::Voter);

        let leader_score = calculate_score(&leader, true, &health, &[], ScoreStrategy::default());
        assert_eq!(
            leader_score.0,
            FLAG_LABEL_MATCHES | FLAG_NOT_SLOW | FLAG_NOT_ATTEMPTED
        );

        let follower_score =
            calculate_score(&follower, false, &health, &[], ScoreStrategy::default());
        assert_eq!(
            follower_score.0,
            FLAG_LABEL_MATCHES | FLAG_NORMAL_PEER | FLAG_NOT_SLOW | FLAG_NOT_ATTEMPTED
        );

        // Slow store loses the NotSlow bit.
        health.mark_slow_for(2, Duration::from_secs(60));
        let follower_score =
            calculate_score(&follower, false, &health, &[], ScoreStrategy::default());
        assert_eq!(
            follower_score.0,
            FLAG_LABEL_MATCHES | FLAG_NORMAL_PEER | FLAG_NOT_ATTEMPTED
        );

        // tryLeader prefers the leader only when label matching is enabled.
        let leader_score = calculate_score(
            &leader,
            true,
            &health,
            &[2],
            ScoreStrategy {
                try_leader: true,
                ..Default::default()
            },
        );
        assert_eq!(
            leader_score.0,
            FLAG_PREFER_LEADER | FLAG_NOT_SLOW | FLAG_NOT_ATTEMPTED
        );
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
