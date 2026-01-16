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
