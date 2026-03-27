//! Client-go style `tikv` namespace.
//!
//! The Rust client exposes most user-facing APIs at the crate root. This module exists mainly to
//! provide a stable namespace that mirrors client-go's public `tikv` package layout.

use std::sync::Arc;

#[doc(inline)]
pub use crate::kv::Getter;
pub use crate::Backoff as Backoffer;
#[doc(inline)]
pub use crate::BinlogWriteResult;
#[doc(inline)]
pub use crate::KvFilter as KVFilter;
#[doc(inline)]
pub use crate::SchemaLeaseChecker;
#[doc(inline)]
pub use crate::SchemaVer;
#[doc(inline)]
pub use crate::Store;
#[doc(inline)]
pub use crate::Transaction as KVTxn;
pub use crate::TransactionClient as KVStore;
#[doc(inline)]
pub use crate::Variables;
#[doc(alias = "MaxTxnTimeUse")]
pub const MAX_TXN_TIME_USE: i64 = crate::MAX_TXN_TIME_USE;

/// Client-go style in-memory mutation buffer type.
///
/// This mirrors client-go `tikv.MemBuffer`.
pub type MemBuffer = crate::transaction::Buffer;

/// Client-go style backoff config type.
///
/// This mirrors client-go `tikv.BackoffConfig` (alias of `retry.Config`).
pub type BackoffConfig = crate::config::retry::Config;

#[doc(inline)]
pub use crate::config::retry::bo_pd_rpc;
#[doc(inline)]
pub use crate::config::retry::bo_region_miss;
#[doc(inline)]
pub use crate::config::retry::bo_tiflash_rpc;
#[doc(inline)]
pub use crate::config::retry::bo_tikv_rpc;
#[doc(inline)]
pub use crate::config::retry::bo_txn_lock;
#[doc(inline)]
pub use crate::config::retry::new_backoffer;
#[doc(inline)]
pub use crate::config::retry::new_backoffer_with_vars;
#[doc(inline)]
pub use crate::config::retry::new_gc_resolve_lock_max_backoffer;
#[doc(inline)]
pub use crate::config::retry::new_noop_backoff;
#[doc(inline)]
pub use crate::config::retry::txn_start_key;

#[doc(inline)]
pub use crate::change_pd_region_meta_circuit_breaker_settings;
#[doc(inline)]
pub use crate::disable_resource_control;
#[doc(inline)]
pub use crate::enable_resource_control;
#[doc(inline)]
pub use crate::load_shutting_down;
#[doc(inline)]
pub use crate::set_resource_control_interceptor;
#[doc(inline)]
pub use crate::store_shutting_down;
#[doc(inline)]
pub use crate::unset_resource_control_interceptor;

#[doc(inline)]
#[allow(deprecated)]
pub use crate::set_region_cache_ttl;
#[doc(inline)]
pub use crate::set_region_cache_ttl_with_jitter;
#[doc(inline)]
pub use crate::set_store_liveness_timeout;

#[doc(inline)]
pub use crate::kv::KeyRange;
#[doc(inline)]
pub use crate::region::RegionVerId;
#[doc(inline)]
pub use crate::region_cache::with_need_buckets;
#[doc(inline)]
pub use crate::region_cache::with_need_region_has_leader_peer;
#[doc(inline)]
pub use crate::region_cache::BatchLocateKeyRangesOpt;
#[doc(inline)]
pub use crate::region_cache::KeyLocation;
#[doc(inline)]
pub use crate::region_cache::RegionCache;

/// Create a region-version identifier.
///
/// This mirrors client-go `tikv.NewRegionVerID`.
#[doc(alias = "NewRegionVerID")]
#[must_use]
pub fn new_region_ver_id(id: u64, conf_ver: u64, ver: u64) -> crate::RegionVerId {
    crate::RegionVerId { id, conf_ver, ver }
}

/// Create a new region cache instance using the global config defaults.
///
/// This mirrors client-go `tikv.NewRegionCache`.
#[doc(alias = "NewRegionCache")]
pub fn new_region_cache<C>(pd_client: Arc<C>) -> crate::RegionCache<C>
where
    C: crate::RetryClientTrait + Send + Sync,
{
    let config = crate::config::get_global_config();
    crate::RegionCache::new_with_ttl(
        pd_client,
        config.region_cache_ttl,
        config.region_cache_ttl_jitter,
    )
}

/// A filter applied to a store's label list.
///
/// This mirrors client-go `tikv.LabelFilter` (and `locate.LabelFilter`).
pub type LabelFilter = fn(labels: &[crate::StoreLabel]) -> bool;

fn store_has_label(labels: &[crate::StoreLabel], key: &str, value: &str) -> bool {
    labels
        .iter()
        .any(|label| label.key == key && label.value == value)
}

/// Determine the store endpoint type from PD store metadata.
///
/// This mirrors client-go `tikv.GetStoreTypeByMeta`.
#[doc(alias = "GetStoreTypeByMeta")]
#[must_use]
pub fn get_store_type_by_meta(store: &crate::proto::metapb::Store) -> crate::tikvrpc::EndpointType {
    crate::tikvrpc::EndpointType::from_store_labels(&store.labels)
}

/// Select stores whose labels contain `<engine, tiflash>` and `<engine_role, write>`.
///
/// This mirrors client-go `tikv.LabelFilterOnlyTiFlashWriteNode`.
#[doc(alias = "LabelFilterOnlyTiFlashWriteNode")]
pub fn label_filter_only_tiflash_write_node(labels: &[crate::StoreLabel]) -> bool {
    store_has_label(
        labels,
        crate::tikvrpc::ENGINE_LABEL_KEY,
        crate::tikvrpc::ENGINE_LABEL_TIFLASH,
    ) && store_has_label(
        labels,
        crate::tikvrpc::ENGINE_ROLE_LABEL_KEY,
        crate::tikvrpc::ENGINE_ROLE_WRITE,
    )
}

/// Select stores whose labels contain `<engine, tiflash>` but do not contain `<engine_role, write>`.
///
/// This mirrors client-go `tikv.LabelFilterNoTiFlashWriteNode`.
#[doc(alias = "LabelFilterNoTiFlashWriteNode")]
pub fn label_filter_no_tiflash_write_node(labels: &[crate::StoreLabel]) -> bool {
    store_has_label(
        labels,
        crate::tikvrpc::ENGINE_LABEL_KEY,
        crate::tikvrpc::ENGINE_LABEL_TIFLASH,
    ) && !store_has_label(
        labels,
        crate::tikvrpc::ENGINE_ROLE_LABEL_KEY,
        crate::tikvrpc::ENGINE_ROLE_WRITE,
    )
}

/// Select stores whose labels contain `<engine, tiflash>`.
///
/// This mirrors client-go `tikv.LabelFilterAllTiFlashNode`.
#[doc(alias = "LabelFilterAllTiFlashNode")]
pub fn label_filter_all_tiflash_node(labels: &[crate::StoreLabel]) -> bool {
    store_has_label(
        labels,
        crate::tikvrpc::ENGINE_LABEL_KEY,
        crate::tikvrpc::ENGINE_LABEL_TIFLASH,
    )
}

/// Select all stores.
///
/// This mirrors client-go `tikv.LabelFilterAllNode`.
#[doc(alias = "LabelFilterAllNode")]
pub fn label_filter_all_node(_labels: &[crate::StoreLabel]) -> bool {
    true
}

/// The key mode for API V2 requests.
///
/// This mirrors client-go `tikv.Mode`.
pub type Mode = crate::request::KeyMode;

/// The keyspace identifier type used by API V2 requests.
///
/// This mirrors client-go `tikv.KeyspaceID`.
pub type KeyspaceID = u32;

/// API V2 raw key mode.
///
/// This mirrors client-go `tikv.ModeRaw`.
#[doc(alias = "ModeRaw")]
pub const MODE_RAW: Mode = Mode::Raw;

/// API V2 transactional key mode.
///
/// This mirrors client-go `tikv.ModeTxn`.
#[doc(alias = "ModeTxn")]
pub const MODE_TXN: Mode = Mode::Txn;

/// The keyspace ID of the default keyspace.
///
/// This mirrors client-go `tikv.DefaultKeyspaceID`.
#[doc(alias = "DefaultKeyspaceID")]
pub const DEFAULT_KEYSPACE_ID: KeyspaceID = 0;

/// The name of the default keyspace.
///
/// This mirrors client-go `tikv.DefaultKeyspaceName`.
#[doc(alias = "DefaultKeyspaceName")]
pub const DEFAULT_KEYSPACE_NAME: &str = "DEFAULT";

/// A sentinel keyspace ID indicating that no keyspace is associated with the request.
///
/// This mirrors client-go `tikv.NullspaceID`.
#[doc(alias = "NullspaceID")]
pub const NULLSPACE_ID: KeyspaceID = u32::MAX;

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
