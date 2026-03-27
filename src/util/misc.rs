use std::any::Any;
use std::backtrace::Backtrace;
use std::future::Future;

tokio::task_local! {
    static TASK_SESSION_ID: u64;
}

/// Marker key for session id stored in task-local context.
///
/// This mirrors client-go `util.SessionID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SessionID;

/// Runs `future` with a task-local session id.
///
/// This mirrors client-go `util.SetSessionID`, but uses Tokio task-local storage instead of
/// `context.Context`.
pub fn with_session_id<T, F>(session_id: u64, future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_SESSION_ID.scope(session_id, future)
}

/// Returns the task-local session id, if present.
#[must_use]
pub fn session_id() -> Option<u64> {
    TASK_SESSION_ID.try_with(|session_id| *session_id).ok()
}

pub(crate) async fn scope_task_session_id<T, F>(session_id: Option<u64>, future: F) -> T
where
    F: Future<Output = T>,
{
    match session_id {
        Some(session_id) => with_session_id(session_id, future).await,
        None => future.await,
    }
}

/// Runs `exec`, catching any unwinding panic and logging it instead of propagating it.
///
/// This mirrors client-go `util.WithRecovery` for recoverable background work. The optional
/// `recover_fn` is always invoked after `exec` finishes: with `None` on success and with the panic
/// payload on unwinding panic.
///
/// Returns `Some(T)` when `exec` completes successfully and `None` when a panic was recovered.
///
/// Note: this only catches unwinding panics. If the process is configured to abort on panic, the
/// process will still abort.
pub fn with_recovery<T, F, R>(exec: F, recover_fn: Option<R>) -> Option<T>
where
    F: FnOnce() -> T,
    R: FnOnce(Option<&(dyn Any + Send + 'static)>),
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(exec)) {
        Ok(value) => {
            if let Some(recover_fn) = recover_fn {
                recover_fn(None);
            }
            Some(value)
        }
        Err(payload) => {
            if let Some(recover_fn) = recover_fn {
                recover_fn(Some(payload.as_ref()));
            }
            log::error!(
                "panic in recoverable closure: payload={}; stack trace={:?}",
                panic_payload(payload.as_ref()),
                Backtrace::force_capture()
            );
            None
        }
    }
}

fn panic_payload(payload: &(dyn Any + Send + 'static)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{session_id, with_recovery, with_session_id};
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_session_id_task_local_scope_restores_outer_value() {
        assert_eq!(session_id(), None);

        with_session_id(7, async {
            assert_eq!(session_id(), Some(7));

            with_session_id(9, async {
                assert_eq!(session_id(), Some(9));
            })
            .await;

            assert_eq!(session_id(), Some(7));
        })
        .await;

        assert_eq!(session_id(), None);
    }

    #[test]
    fn test_with_recovery_returns_result_and_calls_recover_fn_on_success() {
        let callback_called = Arc::new(AtomicBool::new(false));
        let callback_called_captured = callback_called.clone();

        let result = with_recovery(
            || 42,
            Some(move |panic: Option<&(dyn Any + Send + 'static)>| {
                callback_called_captured.store(true, Ordering::SeqCst);
                assert!(panic.is_none());
            }),
        );

        assert_eq!(result, Some(42));
        assert!(callback_called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_with_recovery_catches_panic_and_passes_payload() {
        let recovered_message = Arc::new(Mutex::new(None::<String>));
        let recovered_message_captured = recovered_message.clone();

        let result = with_recovery(
            || -> usize { panic!("boom") },
            Some(move |panic: Option<&(dyn Any + Send + 'static)>| {
                let panic = panic.expect("panic payload should be present");
                let message = panic
                    .downcast_ref::<&'static str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned());
                *recovered_message_captured.lock().unwrap() = message;
            }),
        );

        assert_eq!(result, None);
        assert_eq!(recovered_message.lock().unwrap().as_deref(), Some("boom"));
    }
}
