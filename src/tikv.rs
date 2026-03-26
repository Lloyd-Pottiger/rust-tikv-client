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

/// The API V2 prefix byte for transactional keys.
///
/// This mirrors client-go `tikv.CodecV2TxnKeyspacePrefix`.
#[doc(alias = "CodecV2TxnKeyspacePrefix")]
pub const CODEC_V2_TXN_KEYSPACE_PREFIX: u8 = b'x';

/// The API V2 prefix byte for raw keys.
///
/// This mirrors client-go `tikv.CodecV2RawKeyspacePrefix`.
#[doc(alias = "CodecV2RawKeyspacePrefix")]
pub const CODEC_V2_RAW_KEYSPACE_PREFIX: u8 = b'r';

static CODEC_V2_RAW_PREFIX: [u8; 1] = [CODEC_V2_RAW_KEYSPACE_PREFIX];
static CODEC_V2_TXN_PREFIX: [u8; 1] = [CODEC_V2_TXN_KEYSPACE_PREFIX];
static CODEC_V2_PREFIXES: [&[u8]; 2] = [&CODEC_V2_RAW_PREFIX, &CODEC_V2_TXN_PREFIX];

/// A sorted list of prefixes used by API V2.
///
/// This mirrors client-go `apicodec.CodecV2Prefixes`, which is re-exported from `tikv.CodecV2Prefixes`.
#[doc(alias = "CodecV2Prefixes")]
pub fn codec_v2_prefixes() -> &'static [&'static [u8]] {
    &CODEC_V2_PREFIXES
}

/// A sorted list of prefixes excluded from API V1.
///
/// This mirrors client-go `apicodec.CodecV1ExcludePrefixes`, which is re-exported from `tikv.CodecV1ExcludePrefixes`.
#[doc(alias = "CodecV1ExcludePrefixes")]
pub fn codec_v1_exclude_prefixes() -> &'static [&'static [u8]] {
    codec_v2_prefixes()
}
