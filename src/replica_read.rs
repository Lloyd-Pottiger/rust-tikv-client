// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Replica read / stale read options.
//!
//! This module is intentionally small: it provides only the public types used to
//! configure where reads are served from.

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
    /// Note: currently treated the same as [`ReplicaReadType::Leader`] by the
    /// Rust client.
    PreferLeader,
}

impl ReplicaReadType {
    #[inline]
    pub(crate) fn is_follower_read(self) -> bool {
        matches!(
            self,
            ReplicaReadType::Follower | ReplicaReadType::Mixed | ReplicaReadType::Learner
        )
    }
}
