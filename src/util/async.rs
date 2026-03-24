//! Async execution helpers mirroring client-go `util/async`.
//!
//! The underlying implementation lives in [`crate::async_util`]. This module keeps the
//! client-go-like package layout under [`crate::util`].

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
