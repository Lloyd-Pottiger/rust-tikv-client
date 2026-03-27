//! Async execution helpers mirroring client-go `util/async`.
//!
//! The underlying implementation lives in [`crate::async_util`]. This module keeps the
//! client-go-like package layout under [`crate::util`].

use std::sync::Arc;

#[doc(inline)]
pub use crate::async_util::Callback;
#[doc(inline)]
pub use crate::async_util::CancellationToken;
#[doc(inline)]
pub use crate::async_util::Executor;
#[doc(inline)]
pub use crate::async_util::Pool;
#[doc(inline)]
pub use crate::async_util::RunLoop;
#[doc(inline)]
pub use crate::async_util::RunLoopExecError;
#[doc(inline)]
pub use crate::async_util::State;
#[doc(inline)]
pub use crate::async_util::Task;

/// Create a new callback bound to the provided executor.
///
/// This mirrors client-go `util/async.NewCallback`.
#[doc(alias = "NewCallback")]
pub fn new_callback<T, E>(
    executor: Arc<dyn Executor>,
    f: impl FnOnce(T, Option<E>) + Send + 'static,
) -> Callback<T, E> {
    Callback::new(executor, f)
}

/// Create a new run-loop.
///
/// This mirrors client-go `util/async.NewRunLoop`.
#[doc(alias = "NewRunLoop")]
#[must_use]
pub fn new_run_loop() -> RunLoop {
    RunLoop::new()
}
