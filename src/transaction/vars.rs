use std::sync::atomic::AtomicU32;
use std::sync::Arc;

/// Per-session variables used by the TiKV client.
///
/// This maps to client-go `kv.Variables` and is primarily used to:
/// - configure retry/backoff behavior; and
/// - carry an optional "killed" signal that higher-level layers can use to interrupt long-running
///   transactional operations.
///
/// Note: variables are stored on transactions/snapshots, but not all variables are currently wired
/// into every retry/backoff loop.
#[derive(Clone, Debug)]
pub struct Variables {
    /// Backoff base duration (milliseconds) for lock-fast style retries.
    ///
    /// This maps to client-go `Variables.BackoffLockFast`.
    pub backoff_lock_fast_ms: u64,
    /// Weight used to scale max backoff duration.
    ///
    /// This maps to client-go `Variables.BackOffWeight`.
    pub backoff_weight: u32,
    /// An optional killed signal. A non-zero value indicates the request should be interrupted.
    ///
    /// This maps to client-go `Variables.Killed`.
    pub killed: Option<Arc<AtomicU32>>,
}

impl Variables {
    /// Create variables with the provided killed flag.
    #[must_use]
    pub fn with_killed_flag(killed: Arc<AtomicU32>) -> Variables {
        Variables {
            killed: Some(killed),
            ..Variables::default()
        }
    }
}

impl Default for Variables {
    fn default() -> Self {
        Variables {
            backoff_lock_fast_ms: 10,
            backoff_weight: 2,
            killed: None,
        }
    }
}
