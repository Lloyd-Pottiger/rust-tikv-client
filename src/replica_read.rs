// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Replica read / stale read options.
//!
//! This module is intentionally small: it provides only the public types used to
//! configure where reads are served from.

use std::fmt;
use std::sync::Arc;

/// The type of TiKV replica to read from.
///
/// This mirrors the `client-go` v2 `kv.ReplicaReadType` concept.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ReplicaReadType {
    /// Always read from region leader (default).
    #[default]
    Leader,
    /// Prefer reading from followers.
    Follower,
    /// Read from leader/follower/learner, depending on availability.
    Mixed,
    /// Prefer reading from learners.
    Learner,
    /// Prefer reading from leader, but may fall back if leader is abnormal.
    ///
    /// This mode reads from the leader when possible, but may fall back to
    /// other replicas when the leader is unreachable.
    PreferLeader,
}

impl ReplicaReadType {
    /// Returns `true` if this mode may read from non-leader replicas.
    ///
    /// This mirrors client-go `ReplicaReadType.IsFollowerRead()`: all modes except
    /// [`ReplicaReadType::Leader`] are considered follower-read, including
    /// [`ReplicaReadType::PreferLeader`].
    #[inline]
    pub fn is_follower_read(self) -> bool {
        self != ReplicaReadType::Leader
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

/// Adjust replica read selection for point/batch gets based on key count.
///
/// This maps to the client-go `ReplicaReadAdjuster` hook used by `KVSnapshot` for
/// `Get`/`BatchGet`.
pub type ReplicaReadAdjuster = Arc<dyn Fn(usize) -> ReplicaReadType + Send + Sync + 'static>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_read_type_is_follower_read() {
        assert!(!ReplicaReadType::Leader.is_follower_read());
        assert!(ReplicaReadType::Follower.is_follower_read());
        assert!(ReplicaReadType::Mixed.is_follower_read());
        assert!(ReplicaReadType::Learner.is_follower_read());
        assert!(ReplicaReadType::PreferLeader.is_follower_read());
    }
}
