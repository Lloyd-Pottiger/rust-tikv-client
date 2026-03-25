use std::sync::atomic::{AtomicBool, Ordering};

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
