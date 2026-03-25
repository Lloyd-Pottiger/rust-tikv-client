//! Client-go style `tikv` namespace.
//!
//! The Rust client exposes most user-facing APIs at the crate root. This module exists mainly to
//! provide a stable namespace that mirrors client-go's public `tikv` package layout.

pub use crate::Backoff as Backoffer;
pub use crate::TransactionClient as KVStore;

#[doc(inline)]
pub use crate::disable_resource_control;
#[doc(inline)]
pub use crate::enable_resource_control;
#[doc(inline)]
pub use crate::set_resource_control_interceptor;
#[doc(inline)]
pub use crate::unset_resource_control_interceptor;

#[doc(inline)]
#[allow(deprecated)]
pub use crate::set_region_cache_ttl;
#[doc(inline)]
pub use crate::set_region_cache_ttl_with_jitter;
#[doc(inline)]
pub use crate::set_store_liveness_timeout;
