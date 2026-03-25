use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Error returned when failpoints are disabled.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("failpoints are disabled")]
pub struct FailpointsDisabledError;

static FAILPOINTS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable failpoint evaluation.
///
/// This mirrors client-go `util.EnableFailpoints`.
///
/// Note: failpoints are only effective when the crate is built with `--features fail/failpoints`.
#[doc(alias = "EnableFailpoints")]
pub fn enable_failpoints() {
    FAILPOINTS_ENABLED.store(true, Ordering::Release);
}

/// Evaluate a failpoint and return its optional value.
///
/// This mirrors client-go `util.EvalFailpoint` at a high level.
///
/// - When failpoints are disabled (either not enabled via [`enable_failpoints`] or not compiled
///   with the `fail/failpoints` feature), this returns [`FailpointsDisabledError`].
/// - When enabled, this returns the optional string value configured for the failpoint.
///
/// Note: unlike the Go implementation, this wrapper does not add a name prefix. The Rust client
/// uses plain failpoint names (for example, `"after-prewrite"`).
#[doc(alias = "EvalFailpoint")]
pub fn eval_failpoint(name: &str) -> Result<Option<String>, FailpointsDisabledError> {
    if !FAILPOINTS_ENABLED.load(Ordering::Acquire) || !fail::has_failpoints() {
        return Err(FailpointsDisabledError);
    }

    Ok(fail::eval(name, |value| value).flatten())
}

/// Returns whether a failpoint is active.
///
/// This mirrors the client-go pattern of checking whether `EvalFailpoint(name)` returns `err ==
/// nil` (without inspecting the value).
#[doc(alias = "IsFailpointActive")]
pub fn is_failpoint_active(name: &str) -> Result<bool, FailpointsDisabledError> {
    if !FAILPOINTS_ENABLED.load(Ordering::Acquire) || !fail::has_failpoints() {
        return Err(FailpointsDisabledError);
    }

    Ok(fail::eval(name, |_| Some(())).is_some())
}

pub(crate) async fn sleep_backoff(duration: Duration) -> bool {
    if is_failpoint_active("fastBackoffBySkipSleep").unwrap_or(false) {
        return false;
    }

    tokio::time::sleep(duration).await;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_is_failpoint_active_returns_false_when_unconfigured() {
        let _scenario = fail::FailScenario::setup();
        enable_failpoints();

        assert!(!is_failpoint_active("fastBackoffBySkipSleep").unwrap());
    }

    #[test]
    fn test_is_failpoint_active_returns_true_when_configured() {
        let _scenario = fail::FailScenario::setup();
        enable_failpoints();

        fail::cfg("fastBackoffBySkipSleep", "return(1)").unwrap();
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = fail::cfg("fastBackoffBySkipSleep", "off");
            }
        }
        let _guard = Guard;

        assert!(is_failpoint_active("fastBackoffBySkipSleep").unwrap());
    }

    #[tokio::test]
    async fn test_sleep_backoff_skips_when_failpoint_active() {
        let _scenario = fail::FailScenario::setup();
        enable_failpoints();

        fail::cfg("fastBackoffBySkipSleep", "return(1)").unwrap();
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = fail::cfg("fastBackoffBySkipSleep", "off");
            }
        }
        let _guard = Guard;

        assert!(!sleep_backoff(Duration::from_millis(50)).await);
    }

    #[tokio::test]
    async fn test_sleep_backoff_sleeps_when_failpoint_inactive() {
        let _scenario = fail::FailScenario::setup();
        enable_failpoints();

        assert!(sleep_backoff(Duration::from_millis(1)).await);
    }
}
