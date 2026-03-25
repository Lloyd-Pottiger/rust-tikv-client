//! Client-go style `tikv` namespace.
//!
//! The Rust client exposes most user-facing APIs at the crate root. This module exists mainly to
//! provide a stable namespace that mirrors client-go's public `tikv` package layout.

pub use crate::Backoff as Backoffer;
pub use crate::TransactionClient as KVStore;
