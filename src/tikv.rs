//! Client-go style `tikv` namespace.
//!
//! The Rust client exposes most user-facing APIs at the crate root. This module exists mainly to
//! provide a stable namespace that mirrors client-go's public `tikv` package layout.

use std::sync::Arc;

use crate::timestamp::TimestampExt;

#[doc(inline)]
pub use crate::kv::Getter;
pub use crate::Backoff as Backoffer;
#[doc(inline)]
pub use crate::BinlogWriteResult;
#[doc(inline)]
pub use crate::GcOptions;
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
/// The maximum time a transaction may use, in milliseconds, from begin to commit.
///
/// This mirrors client-go `tikv.MaxTxnTimeUse`.
pub const MAX_TXN_TIME_USE: i64 = crate::MAX_TXN_TIME_USE;

/// Client-go style in-memory mutation buffer type.
///
/// This mirrors client-go `tikv.MemBuffer`.
pub type MemBuffer = crate::transaction::Buffer;

/// Client-go style backoff config type.
///
/// This mirrors client-go `tikv.BackoffConfig` (alias of `retry.Config`).
pub type BackoffConfig = crate::config::retry::Config;

/// Create a transactional KV store client using the global config.
///
/// This mirrors client-go `tikv.NewKVStore`, but reuses the Rust client's simpler constructor
/// shape that only requires PD endpoints.
#[doc(alias = "NewKVStore")]
pub async fn new_kv_store<S: Into<String>>(pd_endpoints: Vec<S>) -> crate::Result<KVStore> {
    crate::TransactionClient::new(pd_endpoints).await
}

/// Create a transactional KV store client with an explicit configuration.
///
/// This mirrors the configurable part of client-go `tikv.NewKVStore`.
#[doc(alias = "NewKVStore")]
pub async fn new_kv_store_with_config<S: Into<String>>(
    pd_endpoints: Vec<S>,
    config: crate::Config,
) -> crate::Result<KVStore> {
    crate::TransactionClient::new_with_config(pd_endpoints, config).await
}

/// Create a PD client using the global config.
///
/// This mirrors client-go `tikv.NewPDClient`.
#[doc(alias = "NewPDClient")]
pub async fn new_pd_client<S: Into<String>>(
    pd_endpoints: Vec<S>,
) -> crate::Result<crate::PdRpcClient> {
    new_pd_client_with_config(pd_endpoints, crate::config::get_global_config()).await
}

/// Create a PD client with an explicit configuration.
///
/// This mirrors client-go `tikv.NewPDClient` while keeping Rust's explicit config passing style.
#[doc(alias = "NewPDClient")]
pub async fn new_pd_client_with_config<S: Into<String>>(
    pd_endpoints: Vec<S>,
    config: crate::Config,
) -> crate::Result<crate::PdRpcClient> {
    let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
    crate::PdRpcClient::connect(&pd_endpoints, config, true).await
}

/// Client-go style transaction begin option.
///
/// These options are consumed by
/// [`TransactionClient::begin_with_tikv_options`](crate::TransactionClient::begin_with_tikv_options)
/// and
/// [`SyncTransactionClient::begin_with_tikv_options`](crate::SyncTransactionClient::begin_with_tikv_options).
///
/// Repeated options follow client-go semantics: later entries override earlier ones.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum TxnOption {
    /// Override the transaction scope used when starting the transaction.
    ///
    /// This mirrors client-go `tikv.WithTxnScope`.
    TxnScope(String),
    /// Use an explicit start timestamp instead of fetching one from PD.
    ///
    /// This mirrors client-go `tikv.WithStartTS`.
    StartTs(u64),
    /// Enable pipelined DML with the default client-go parameters.
    ///
    /// This mirrors client-go `tikv.WithDefaultPipelinedTxn`.
    DefaultPipelinedTxn,
    /// Enable pipelined DML with custom parameters.
    ///
    /// This mirrors client-go `tikv.WithPipelinedTxn`.
    PipelinedTxn(crate::PipelinedTxnOptions),
}

/// Create a transaction begin option that sets the transaction scope.
///
/// This mirrors client-go `tikv.WithTxnScope`.
#[doc(alias = "WithTxnScope")]
#[must_use]
pub fn with_txn_scope(txn_scope: &str) -> TxnOption {
    TxnOption::TxnScope(txn_scope.to_owned())
}

/// Create a transaction begin option that uses an explicit start timestamp.
///
/// This mirrors client-go `tikv.WithStartTS`.
#[doc(alias = "WithStartTS")]
#[must_use]
pub fn with_start_ts(start_ts: u64) -> TxnOption {
    TxnOption::StartTs(start_ts)
}

/// Create a transaction begin option that enables pipelined DML with default parameters.
///
/// This mirrors client-go `tikv.WithDefaultPipelinedTxn`.
#[doc(alias = "WithDefaultPipelinedTxn")]
#[must_use]
pub fn with_default_pipelined_txn() -> TxnOption {
    TxnOption::DefaultPipelinedTxn
}

/// Create a transaction begin option that enables pipelined DML with custom parameters.
///
/// This mirrors client-go `tikv.WithPipelinedTxn`.
#[doc(alias = "WithPipelinedTxn")]
pub fn with_pipelined_txn(
    flush_concurrency: usize,
    resolve_lock_concurrency: usize,
    write_throttle_ratio: f64,
) -> crate::Result<TxnOption> {
    Ok(TxnOption::PipelinedTxn(crate::PipelinedTxnOptions::new(
        flush_concurrency,
        resolve_lock_concurrency,
        write_throttle_ratio,
    )?))
}

pub(crate) fn build_begin_options(
    options: impl IntoIterator<Item = TxnOption>,
) -> crate::Result<(crate::TransactionOptions, Option<crate::Timestamp>)> {
    let mut txn_options = crate::TransactionOptions::new_optimistic();
    let mut start_ts = None;

    for option in options {
        match option {
            TxnOption::TxnScope(txn_scope) => {
                txn_options = txn_options.txn_scope(txn_scope);
            }
            TxnOption::StartTs(version) => {
                start_ts = Some(crate::Timestamp::from_version(version));
            }
            TxnOption::DefaultPipelinedTxn => {
                txn_options = txn_options.pipelined();
            }
            TxnOption::PipelinedTxn(options) => {
                txn_options = txn_options.pipelined_txn(options);
            }
        }
    }

    Ok((txn_options, start_ts))
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_begin_options_tracks_explicit_start_ts_and_txn_scope() {
        let (options, start_ts) =
            build_begin_options([with_txn_scope("dc1"), with_start_ts(42)]).expect("valid opts");

        assert_eq!(options.txn_scope_as_deref(), Some("dc1"));
        assert_eq!(start_ts, Some(crate::Timestamp::from_version(42)));
    }

    #[test]
    fn build_begin_options_keeps_client_go_begin_optimistic_for_pipelined_txns() {
        let custom = with_pipelined_txn(4, 2, 0.25).expect("custom pipeline");
        let (options, _) =
            build_begin_options([with_default_pipelined_txn(), custom]).expect("valid opts");

        assert!(options.validate().is_ok());
    }

    #[test]
    fn with_pipelined_txn_reuses_pipelined_option_validation() {
        assert!(with_pipelined_txn(0, 1, 0.0).is_err());
        assert!(with_pipelined_txn(1, 0, 0.0).is_err());
        assert!(with_pipelined_txn(1, 1, 1.0).is_err());
    }
}
