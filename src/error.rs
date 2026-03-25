//! Error types and helpers.
//!
//! This module mirrors client-go's public `error` package layout. All items are also available at
//! the crate root; this module exists mainly to provide a stable, discoverable namespace.

pub use crate::extract_debug_info_str_from_key_error;
pub use crate::is_error_undetermined;
pub use crate::AssertionFailedError;
pub use crate::DeadlockError;
pub use crate::Error;
pub use crate::ProtoAssertionFailed;
pub use crate::ProtoDeadlock;
pub use crate::ProtoKeyError;
pub use crate::ProtoRegionError;
pub use crate::ProtoWriteConflict;
pub use crate::Result;
pub use crate::WriteConflictError;
