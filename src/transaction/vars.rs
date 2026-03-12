use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::Error;
use crate::Result;

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

    pub(crate) fn lock_fast_base_delay_ms(&self) -> u64 {
        // Match client-go `newBackoffFn` behavior: clamp the base delay to at least 2ms.
        self.backoff_lock_fast_ms.max(2)
    }

    pub(crate) fn backoff_weight_factor(&self) -> u32 {
        self.backoff_weight.max(1)
    }

    pub(crate) fn check_killed(&self) -> Result<()> {
        let Some(killed) = self.killed.as_ref() else {
            return Ok(());
        };
        let killed_signal = killed.load(Ordering::SeqCst);
        if killed_signal == 0 {
            Ok(())
        } else {
            Err(Error::StringError(format!(
                "query interrupted by signal {killed_signal}"
            )))
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    #[test]
    fn lock_fast_base_delay_ms_clamps_to_two_ms() {
        let vars = Variables {
            backoff_lock_fast_ms: 1,
            ..Variables::default()
        };
        assert_eq!(vars.lock_fast_base_delay_ms(), 2);
    }

    #[test]
    fn backoff_weight_factor_clamps_to_one() {
        let vars = Variables {
            backoff_weight: 0,
            ..Variables::default()
        };
        assert_eq!(vars.backoff_weight_factor(), 1);
    }

    #[test]
    fn check_killed_returns_error_when_nonzero() {
        let killed = Arc::new(AtomicU32::new(9));
        let vars = Variables::with_killed_flag(killed);
        let err = vars
            .check_killed()
            .expect_err("expected killed signal to return error");
        match err {
            Error::StringError(message) => {
                assert_eq!(message, "query interrupted by signal 9");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn check_killed_is_noop_when_flag_is_zero() {
        let killed = Arc::new(AtomicU32::new(0));
        let vars = Variables::with_killed_flag(killed);
        vars.check_killed().unwrap();
    }
}
