//! Replica read (follower/learner) configuration.
//!
//! TiKV supports serving **read-only** requests from different replicas of a Raft group.
//! Replica reads can reduce tail latency and improve load distribution, but they come with
//! different trade-offs depending on the mode.
//!
//! This enum mirrors `client-go`'s `kv.ReplicaReadType` at a semantic level, but is a Rust-native
//! API.

use core::fmt;

/// Which replica to read data from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ReplicaReadType {
    /// Read from the region leader.
    Leader,
    /// Read from a follower replica (non-leader voter).
    Follower,
    /// Read from a mixed set of replicas (leader/follower/learner), depending on availability.
    Mixed,
    /// Read from a learner replica.
    Learner,
    /// Prefer reading from leader; fall back to other replicas if leader is unhealthy.
    PreferLeader,
}

impl Default for ReplicaReadType {
    fn default() -> Self {
        Self::Leader
    }
}

impl ReplicaReadType {
    /// Returns `true` if this mode may read from a non-leader replica.
    pub const fn is_follower_read(self) -> bool {
        !matches!(self, ReplicaReadType::Leader)
    }
}

impl fmt::Display for ReplicaReadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ReplicaReadType::Leader => "leader",
            ReplicaReadType::Follower => "follower",
            ReplicaReadType::Mixed => "mixed",
            ReplicaReadType::Learner => "learner",
            ReplicaReadType::PreferLeader => "prefer-leader",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::ReplicaReadType;

    #[test]
    fn default_is_leader() {
        assert_eq!(ReplicaReadType::default(), ReplicaReadType::Leader);
        assert!(!ReplicaReadType::Leader.is_follower_read());
    }

    #[test]
    fn follower_read_modes() {
        for mode in [
            ReplicaReadType::Follower,
            ReplicaReadType::Mixed,
            ReplicaReadType::Learner,
            ReplicaReadType::PreferLeader,
        ] {
            assert!(mode.is_follower_read(), "{mode:?} should be follower-read");
        }
    }

    #[test]
    fn display_is_stable() {
        assert_eq!(ReplicaReadType::Leader.to_string(), "leader");
        assert_eq!(ReplicaReadType::Follower.to_string(), "follower");
        assert_eq!(ReplicaReadType::Mixed.to_string(), "mixed");
        assert_eq!(ReplicaReadType::Learner.to_string(), "learner");
        assert_eq!(ReplicaReadType::PreferLeader.to_string(), "prefer-leader");
    }
}
