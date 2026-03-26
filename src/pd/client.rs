// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use futures::prelude::*;
use futures::stream::BoxStream;
use log::debug;
use log::info;
use log::warn;
use serde_derive::Deserialize;
use tokio::sync::RwLock;

use crate::compat::stream_fn;
use crate::kv::codec;
use crate::pd::retry::RetryClientTrait;
use crate::pd::Cluster;
use crate::pd::HealthFeedbackObserver;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::pdpb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::region_cache::is_tiflash_store;
use crate::region_cache::KeyLocation;
use crate::region_cache::RegionCache;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::TikvConnect;
use crate::store::{KvClient, Store, StoreLimitKvClient};
use crate::BoundRange;
use crate::Config;
use crate::Error;
use crate::Key;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

pub(crate) const HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD: i32 = 80;
pub(crate) const HEALTH_FEEDBACK_SLOW_STORE_TTL: Duration = Duration::from_secs(15);
pub(crate) const BROADCAST_TXN_STATUS_MAX_CONCURRENCY: usize = 10;

/// The PdClient handles all the encoding stuff.
///
/// Raw APIs does not require encoding/decoding at all.
/// All keys in all places (client, PD, TiKV) are in the same encoding (here called "raw format").
///
/// Transactional APIs are a bit complicated.
/// We need encode and decode keys when we communicate with PD, but not with TiKV.
/// We encode keys before sending requests to PD, and decode keys in the response from PD.
/// That's all we need to do with encoding.
///
///  client -encoded-> PD, PD -encoded-> client
///  client -raw-> TiKV, TiKV -raw-> client
///
/// The reason for the behavior is that in transaction mode, TiKV encode keys for MVCC.
/// In raw mode, TiKV doesn't encode them.
/// TiKV tells PD using its internal representation, whatever the encoding is.
/// So if we use transactional APIs, keys in PD are encoded and PD does not know about the encoding stuff.
#[async_trait]
pub trait PdClient: Send + Sync + 'static {
    type KvClient: KvClient + Send + Sync + 'static;

    /// Returns whether the client has been explicitly closed.
    fn is_closed(&self) -> bool {
        false
    }

    /// Closes the client and releases best-effort cached resources.
    async fn close(&self) {}

    /// In transactional API, `region` is decoded (keys in raw format).
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore>;

    /// In transactional API, the key and returned region are both decoded (keys in raw format).
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader>;

    /// Locate the region that contains the last key strictly less than `key`.
    ///
    /// This mirrors client-go `RegionCache.LocateEndKey` behavior and is used by raw reverse scans.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let _ = key;
        Err(Error::Unimplemented)
    }

    /// In transactional API, the returned region is decoded (keys in raw format)
    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader>;

    /// Scan region metadata starting from `start_key`.
    ///
    /// In transactional API, the input keys and returned regions are decoded (keys in raw format).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn scan_regions(
        self: Arc<Self>,
        start_key: Key,
        end_key: Option<Key>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        let _ = (start_key, end_key, limit);
        Err(Error::Unimplemented)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        let _ = dc_location;
        self.get_timestamp().await
    }

    /// Retrieve a minimum [`Timestamp`] from all TSO keyspace groups.
    ///
    /// This maps to client-go `pd.Client.GetMinTS` (and `KVStore.CurrentAllTSOKeyspaceGroupMinTs`).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        Err(Error::Unimplemented)
    }

    /// Retrieve the PD external timestamp.
    ///
    /// This maps to client-go `Oracle.GetExternalTimestamp`.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// Set the PD external timestamp.
    ///
    /// This maps to client-go `Oracle.SetExternalTimestamp`.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        let _ = timestamp;
        Err(Error::Unimplemented)
    }

    /// Scatter regions in PD.
    ///
    /// This maps to client-go `pd.Client.ScatterRegions` (used by `KVStore.SplitRegions`).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<RegionId>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        let _ = (region_ids, group);
        Err(Error::Unimplemented)
    }

    /// Get the current PD operator for a region.
    ///
    /// This maps to client-go `pd.Client.GetOperator` (used by `KVStore.WaitScatterRegionFinish`).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_operator(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<pdpb::GetOperatorResponse> {
        let _ = region_id;
        Err(Error::Unimplemented)
    }

    /// Get the current PD GC safe point.
    ///
    /// This maps to client-go `pd.Client.GetGCSafePoint`.
    ///
    /// On success, returns PD's `safe_point`.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// Get the current PD GC safe point (V2) for a given keyspace.
    ///
    /// This maps to client-go `pd.Client.GetGCSafePointV2`.
    ///
    /// On success, returns PD's `safe_point`.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_gc_safe_point_v2(self: Arc<Self>, keyspace_id: u32) -> Result<u64> {
        let _ = keyspace_id;
        Err(Error::Unimplemented)
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64>;

    /// Update the PD "service GC safe point" for a given service.
    ///
    /// This maps to client-go `pd.Client.UpdateServiceGCSafePoint`.
    ///
    /// On success, returns the PD-reported `min_safe_point` (the effective service GC safe point).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (service_id, ttl, safe_point);
        Err(Error::Unimplemented)
    }

    /// Update the PD "service safe point" for a given keyspace.
    ///
    /// This maps to client-go `pd.Client.UpdateServiceSafePointV2`.
    ///
    /// On success, returns the PD-reported `min_safe_point` (the effective service safe point).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (keyspace_id, service_id, ttl, safe_point);
        Err(Error::Unimplemented)
    }

    /// Update the PD GC safe point for a given keyspace.
    ///
    /// This maps to client-go `pd.Client.UpdateGCSafePointV2`.
    ///
    /// On success, returns the PD-reported `new_safe_point` (the effective GC safe point).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        let _ = (keyspace_id, safe_point);
        Err(Error::Unimplemented)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;

    /// In transactional API, `key` is in raw format
    async fn store_for_key(self: Arc<Self>, key: &Key) -> Result<RegionStore> {
        let region = self.region_for_key(key).await?;
        self.map_region_to_store(region).await
    }

    async fn store_for_id(self: Arc<Self>, id: RegionId) -> Result<RegionStore> {
        let region = self.region_for_id(id).await?;
        self.map_region_to_store(region).await
    }

    async fn all_stores(&self) -> Result<Vec<Store>>;

    /// Returns the list of stores to use for safe-ts maintenance.
    ///
    /// Implementations may override this to include additional store types (for example TiFlash)
    /// that are excluded from [`PdClient::all_stores`] for normal request routing.
    async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
        self.all_stores().await
    }

    /// Returns PD min resolved ts and per-store min resolved ts map, if the PD HTTP API is available.
    ///
    /// This mirrors client-go `pdhttp.Client.GetMinResolvedTSByStoresIDs` (used by the safe-ts updater
    /// fast path).
    ///
    /// When `store_ids` is empty, implementations should return the cluster-level min resolved ts.
    /// The default implementation returns [`Error::Unimplemented`].
    async fn min_resolved_ts_by_stores(
        &self,
        store_ids: &[StoreId],
    ) -> Result<(u64, HashMap<StoreId, u64>)> {
        let _ = store_ids;
        Err(Error::Unimplemented)
    }

    /// Fetch PD store metadata by store id.
    ///
    /// This is used for replica selection when matching store labels.
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn store_meta_by_id(&self, store_id: StoreId) -> Result<metapb::Store> {
        let _ = store_id;
        Err(Error::Unimplemented)
    }

    /// Fetch cached PD store metadata by store id, without issuing new PD requests.
    ///
    /// This is a best-effort helper used in hot paths (for example traffic classification) where
    /// making additional PD calls would be too expensive.
    ///
    /// Implementations should return `None` when the store metadata is not cached locally.
    ///
    /// The default implementation returns `None`.
    fn store_meta_by_id_cached(&self, store_id: StoreId) -> Option<metapb::Store> {
        let _ = store_id;
        None
    }

    /// Request health feedback from a TiKV store.
    ///
    /// Implementations may use the returned feedback to update internal heuristics (for example
    /// marking a store as slow for replica selection).
    ///
    /// The default implementation returns [`Error::Unimplemented`].
    async fn get_health_feedback(&self, store_id: StoreId) -> Result<kvrpcpb::HealthFeedback> {
        let _ = store_id;
        Err(Error::Unimplemented)
    }

    /// Broadcast transaction status to all TiKV stores.
    ///
    /// This is best-effort: per-store errors are logged and ignored.
    async fn broadcast_txn_status_to_all_stores(
        &self,
        txn_statuses: Vec<kvrpcpb::TxnStatus>,
    ) -> Result<()> {
        if txn_statuses.is_empty() {
            return Ok(());
        }

        let stores = self.all_stores().await?;
        let txn_statuses = Arc::new(txn_statuses);
        futures::stream::iter(stores).for_each_concurrent(
            Some(BROADCAST_TXN_STATUS_MAX_CONCURRENCY),
            move |store| {
                let txn_statuses = txn_statuses.clone();
                async move {
                    let mut req = kvrpcpb::BroadcastTxnStatusRequest::default();
                    req.context = Some(kvrpcpb::Context::default());
                    req.txn_status = (*txn_statuses).clone();

                    match store.client.dispatch(&req).await {
                        Ok(resp) => {
                            if resp
                                .downcast::<kvrpcpb::BroadcastTxnStatusResponse>()
                                .is_err()
                            {
                                warn!(
                                    "broadcast_txn_status got unexpected response type for store_id={}",
                                    store.meta.id
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                "broadcast_txn_status failed for store_id={}: {}",
                                store.meta.id, err
                            );
                        }
                    }
                }
            },
        )
        .await;

        Ok(())
    }

    /// Mark a store as "slow" for replica selection purposes.
    ///
    /// This is a best-effort signal used by replica selection scoring. The default implementation
    /// is a no-op.
    fn mark_store_slow(&self, store_id: StoreId, duration: Duration) {
        let _ = (store_id, duration);
    }

    /// Returns true if the store is currently considered "slow".
    ///
    /// The default implementation always returns false.
    fn is_store_slow(&self, store_id: StoreId) -> bool {
        let _ = store_id;
        false
    }

    /// Update the store's estimated wait time from a `ServerIsBusy` response.
    ///
    /// This is a best-effort signal used by load-based replica read. The default implementation
    /// is a no-op.
    fn update_store_load_stats(&self, store_id: StoreId, estimated_wait_ms: u32) {
        let _ = (store_id, estimated_wait_ms);
    }

    /// Returns the current estimated wait time for requests sent to this store.
    ///
    /// This is a best-effort signal used by load-based replica read. The default implementation
    /// returns `Duration::ZERO`.
    fn store_estimated_wait_time(&self, store_id: StoreId) -> Duration {
        let _ = store_id;
        Duration::ZERO
    }

    /// The `txn_size` threshold for ResolveLock "lite" mode.
    ///
    /// When `lock.txn_size < threshold`, lock resolution uses ResolveLock lite (populate
    /// `kvrpcpb::ResolveLockRequest.keys`) to resolve only the conflicting key instead of scanning
    /// the whole region for `start_ts`.
    fn resolve_lock_lite_threshold(&self) -> u64 {
        crate::config::DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD
    }

    /// Transaction size threshold for starting optimistic auto-heartbeat.
    ///
    /// This maps to client-go `TTLRefreshedTxnSize`.
    fn ttl_refreshed_txn_size(&self) -> u64 {
        crate::config::DEFAULT_TTL_REFRESHED_TXN_SIZE
    }

    /// Maximum concurrency for 2PC committer multi-region requests.
    ///
    /// This maps to client-go `CommitterConcurrency`.
    fn committer_concurrency(&self) -> usize {
        crate::config::DEFAULT_COMMITTER_CONCURRENCY
    }

    /// Maximum transaction lifetime for auto-heartbeat.
    ///
    /// This maps to client-go `MaxTxnTTL`.
    fn max_txn_ttl(&self) -> Duration {
        crate::config::DEFAULT_MAX_TXN_TTL
    }

    /// Timeout applied to commit RPC requests.
    ///
    /// When non-zero, `kv_commit` requests use this timeout instead of the global client timeout.
    ///
    /// This maps to client-go `CommitTimeout`.
    fn commit_timeout(&self) -> Duration {
        crate::config::DEFAULT_COMMIT_TIMEOUT
    }

    /// Use async commit only if the number of keys does not exceed this limit.
    ///
    /// This maps to client-go `TiKVClient.AsyncCommit.KeysLimit`.
    fn async_commit_keys_limit(&self) -> usize {
        crate::config::DEFAULT_ASYNC_COMMIT_KEYS_LIMIT
    }

    /// Use async commit only if the total size of keys does not exceed this limit.
    ///
    /// This maps to client-go `TiKVClient.AsyncCommit.TotalKeySizeLimit`.
    fn async_commit_total_key_size_limit(&self) -> u64 {
        crate::config::DEFAULT_ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT
    }

    /// The duration within which it is safe for async commit or 1PC to commit with an old schema.
    ///
    /// This maps to client-go `TiKVClient.AsyncCommit.SafeWindow`.
    fn async_commit_safe_window(&self) -> Duration {
        crate::config::DEFAULT_ASYNC_COMMIT_SAFE_WINDOW
    }

    /// The duration in addition to the safe window to make DDL safe.
    ///
    /// This maps to client-go `TiKVClient.AsyncCommit.AllowedClockDrift`.
    fn async_commit_allowed_clock_drift(&self) -> Duration {
        crate::config::DEFAULT_ASYNC_COMMIT_ALLOWED_CLOCK_DRIFT
    }

    /// Whether TiKV request forwarding is enabled (client-go `EnableForwarding`).
    ///
    /// The default implementation always returns false.
    fn enable_forwarding(&self) -> bool {
        false
    }

    /// Returns the TiKV cluster ID.
    ///
    /// When non-zero, this is attached to KV requests via `kvrpcpb::Context.cluster_id`.
    ///
    /// The default implementation returns `0`.
    fn cluster_id(&self) -> u64 {
        0
    }

    /// Groups consecutive keys by region.
    ///
    /// The input keys must be sorted in increasing key order so that keys from the same region are
    /// contiguous.
    fn group_keys_by_region<K, K2>(
        self: Arc<Self>,
        keys: impl Iterator<Item = K> + Send + Sync + 'static,
    ) -> BoxStream<'static, Result<(Vec<K2>, RegionWithLeader)>>
    where
        K: AsRef<Key> + Into<K2> + Send + Sync + 'static,
        K2: Send + Sync + 'static,
    {
        let keys = keys.peekable();
        stream_fn(keys, move |mut keys| {
            let this = self.clone();
            async move {
                if let Some(key) = keys.next() {
                    let region = this.region_for_key(key.as_ref()).await?;
                    let mut grouped = vec![key.into()];
                    while let Some(key) = keys.peek() {
                        if !region.contains(key.as_ref()) {
                            break;
                        }
                        let Some(next_key) = keys.next() else {
                            break;
                        };
                        grouped.push(next_key.into());
                    }
                    Ok(Some((keys, (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for each region covered by range.
    fn regions_for_range(
        self: Arc<Self>,
        range: BoundRange,
    ) -> BoxStream<'static, Result<RegionWithLeader>> {
        const SCAN_REGION_BATCH_LIMIT: i32 = 1024;

        struct RegionsForRangeState {
            next_start: Option<Key>,
            end_key: Option<Key>,
            scan_supported: bool,
            pending: VecDeque<RegionWithLeader>,
        }

        let (start_key, end_key) = range.into_keys();
        if let Some(end_key) = end_key.as_ref() {
            if !end_key.is_empty() && &start_key >= end_key {
                return futures::stream::empty::<Result<RegionWithLeader>>().boxed();
            }
        }
        stream_fn(
            RegionsForRangeState {
                next_start: Some(start_key),
                end_key,
                scan_supported: true,
                pending: VecDeque::new(),
            },
            move |mut state| {
                let this = self.clone();
                async move {
                    if let Some(region) = state.pending.pop_front() {
                        return Ok(Some((state, region)));
                    }

                    let Some(start_key) = state.next_start.take() else {
                        return Ok(None);
                    };

                    if state.scan_supported {
                        match this
                            .clone()
                            .scan_regions(
                                start_key.clone(),
                                state.end_key.clone(),
                                SCAN_REGION_BATCH_LIMIT,
                            )
                            .await
                        {
                            Ok(regions) if !regions.is_empty() => {
                                let last_end = regions
                                    .last()
                                    .map(|region| region.end_key())
                                    .unwrap_or(Key::EMPTY);
                                let stop = last_end.is_empty()
                                    || state
                                        .end_key
                                        .as_ref()
                                        .map(|end_key| end_key <= &last_end && !end_key.is_empty())
                                        .unwrap_or(false);
                                if !stop && last_end <= start_key {
                                    return Err(Error::StringError(
                                        "PD scan_regions returned a non-advancing range".to_owned(),
                                    ));
                                }
                                state.next_start = if stop { None } else { Some(last_end) };
                                state.pending =
                                    regions.into_iter().collect::<VecDeque<RegionWithLeader>>();
                                if let Some(region) = state.pending.pop_front() {
                                    return Ok(Some((state, region)));
                                }
                                state.scan_supported = false;
                            }
                            Ok(_) | Err(Error::Unimplemented) => {
                                state.scan_supported = false;
                            }
                            Err(err) => return Err(err),
                        }
                    }

                    let region = this.region_for_key(&start_key).await?;
                    let region_end = region.end_key();
                    let stop = state
                        .end_key
                        .as_ref()
                        .map(|end_key| end_key <= &region_end && !end_key.is_empty())
                        .unwrap_or(false)
                        || region_end.is_empty();
                    state.next_start = if stop { None } else { Some(region_end) };
                    Ok(Some((state, region)))
                }
            },
        )
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for ranges in the same region.
    fn group_ranges_by_region(
        self: Arc<Self>,
        mut ranges: Vec<kvrpcpb::KeyRange>,
    ) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
        ranges.reverse();
        stream_fn(Some(ranges), move |ranges| {
            let this = self.clone();
            async move {
                let mut ranges = match ranges {
                    None => return Ok(None),
                    Some(r) => r,
                };

                if let Some(range) = ranges.pop() {
                    let start_key: Key = range.start_key.clone().into();
                    let end_key: Key = range.end_key.clone().into();
                    let region = this.region_for_key(&start_key).await?;
                    let region_start = region.start_key();
                    let region_end = region.end_key();
                    let mut grouped = vec![];
                    if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                        grouped.push(make_key_range(start_key.into(), region_end.clone().into()));
                        ranges.push(make_key_range(region_end.into(), end_key.into()));
                        return Ok(Some((Some(ranges), (grouped, region))));
                    }
                    grouped.push(range);

                    while let Some(range) = ranges.pop() {
                        let start_key: Key = range.start_key.clone().into();
                        let end_key: Key = range.end_key.clone().into();
                        if start_key < region_start || start_key > region_end {
                            ranges.push(range);
                            break;
                        }
                        if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                            grouped
                                .push(make_key_range(start_key.into(), region_end.clone().into()));
                            ranges.push(make_key_range(region_end.into(), end_key.into()));
                            return Ok(Some((Some(ranges), (grouped, region))));
                        }
                        grouped.push(range);
                    }
                    Ok(Some((Some(ranges), (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    fn decode_region(mut region: RegionWithLeader, enable_codec: bool) -> Result<RegionWithLeader> {
        if enable_codec {
            codec::decode_bytes_in_place(&mut region.region.start_key, false)?;
            codec::decode_bytes_in_place(&mut region.region.end_key, false)?;
        }
        Ok(region)
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()>;

    async fn invalidate_region_cache(&self, ver_id: RegionVerId);

    async fn invalidate_store_cache(&self, store_id: StoreId);

    /// Add or refresh a region in the local region cache.
    ///
    /// This is primarily used to apply region metadata returned by TiKV region errors (for
    /// example `EpochNotMatch`) without an extra PD round-trip.
    ///
    /// The default implementation is a no-op.
    #[doc(hidden)]
    async fn add_region_to_cache(&self, region: RegionWithLeader) {
        let _ = region;
    }

    /// Update cached bucket meta for a region when TiKV returns `BucketVersionNotMatch`.
    ///
    /// This allows fast retries without an extra PD round-trip.
    ///
    /// The default implementation is a no-op.
    #[doc(hidden)]
    async fn on_bucket_version_not_match(
        &self,
        ver_id: RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        let _ = (ver_id, version, keys);
    }

    /// Return cached bucket version for a region, if available.
    ///
    /// The default implementation returns `0`.
    #[doc(hidden)]
    async fn buckets_version(&self, ver_id: RegionVerId) -> u64 {
        let _ = ver_id;
        0
    }
}

fn decode_key_location(mut location: KeyLocation, enable_codec: bool) -> Result<KeyLocation> {
    if enable_codec {
        let mut start_key: Vec<u8> = location.start_key.into();
        codec::decode_bytes_in_place(&mut start_key, false)?;
        location.start_key = start_key.into();

        let mut end_key: Vec<u8> = location.end_key.into();
        codec::decode_bytes_in_place(&mut end_key, false)?;
        location.end_key = end_key.into();

        if let Some(buckets) = location.buckets.as_mut() {
            let buckets = Arc::make_mut(buckets);
            for key in &mut buckets.keys {
                codec::decode_bytes_in_place(key, false)?;
            }
        }
    }
    Ok(location)
}

/// This client converts requests for the logical TiKV cluster into requests
/// for a single TiKV store using PD and internal logic.
#[derive(Clone)]
struct KvClientPool<C> {
    inner: Arc<KvClientPoolInner<C>>,
}

struct KvClientPoolInner<C> {
    clients: Vec<C>,
    next: AtomicUsize,
}

impl<C> KvClientPool<C> {
    fn new(clients: Vec<C>) -> Self {
        debug_assert!(!clients.is_empty());
        KvClientPool {
            inner: Arc::new(KvClientPoolInner {
                clients,
                next: AtomicUsize::new(0),
            }),
        }
    }

    fn pick(&self) -> C
    where
        C: Clone,
    {
        let idx = self.inner.next.fetch_add(1, Ordering::Relaxed);
        self.inner.clients[idx % self.inner.clients.len()].clone()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.clients.len()
    }
}

pub struct PdRpcClient<KvC: KvConnect + Send + Sync + 'static = TikvConnect, Cl = Cluster> {
    pd: Arc<RetryClient<Cl>>,
    security_mgr: Arc<SecurityManager>,
    kv_connect: KvC,
    closed: AtomicBool,
    grpc_connection_count: usize,
    kv_client_cache: Arc<RwLock<HashMap<String, KvClientPool<KvC::KvClient>>>>,
    slow_store_until: Mutex<HashMap<StoreId, Instant>>,
    store_estimated_wait_until: Mutex<HashMap<StoreId, Instant>>,
    store_limit: i64,
    store_token_count: Mutex<HashMap<StoreId, Arc<AtomicI64>>>,
    enable_forwarding: bool,
    enable_codec: bool,
    region_cache: RegionCache<RetryClient<Cl>>,
    resolve_lock_lite_threshold: u64,
    ttl_refreshed_txn_size: u64,
    committer_concurrency: usize,
    max_txn_ttl: Duration,
    commit_timeout: Duration,
    async_commit_keys_limit: usize,
    async_commit_total_key_size_limit: u64,
    async_commit_safe_window: Duration,
    async_commit_allowed_clock_drift: Duration,
    pd_http_client: Option<reqwest::Client>,
    pd_http_endpoints: Vec<String>,
    pd_http_use_https: bool,
}

const REGION_CACHE_PRELOAD_LIMIT: i32 = 10_000;

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl>
where
    RetryClient<Cl>: RetryClientTrait + Send + Sync,
{
    /// Locate the region containing `key`.
    ///
    /// Unlike [`RegionCache::locate_key`], this helper automatically applies PD key encoding and
    /// decoding for clients configured with API V2 codecs.
    #[doc(alias = "LocateKey")]
    pub async fn locate_key(&self, key: Key) -> Result<KeyLocation> {
        let enable_codec = self.enable_codec;
        let key = if enable_codec { key.to_encoded() } else { key };

        self.region_cache
            .locate_key(key)
            .await
            .and_then(|location| decode_key_location(location, enable_codec))
    }

    /// Try locating the region containing `key` from the local cache only.
    ///
    /// Returns `Ok(None)` if the region is missing or expired. No PD request is issued.
    #[doc(alias = "TryLocateKey")]
    pub async fn try_locate_key(&self, key: Key) -> Result<Option<KeyLocation>> {
        let enable_codec = self.enable_codec;
        let key = if enable_codec { key.to_encoded() } else { key };

        self.region_cache
            .try_locate_key(key)
            .await
            .map(|location| decode_key_location(location, enable_codec))
            .transpose()
    }

    /// Locate the region containing the last key strictly less than `key`.
    ///
    /// Unlike [`RegionCache::locate_end_key`], this helper automatically applies PD key encoding
    /// and decoding for clients configured with API V2 codecs.
    #[doc(alias = "LocateEndKey")]
    pub async fn locate_end_key(&self, key: Key) -> Result<KeyLocation> {
        let enable_codec = self.enable_codec;
        let key = if enable_codec { key.to_encoded() } else { key };

        self.region_cache
            .locate_end_key(key)
            .await
            .and_then(|location| decode_key_location(location, enable_codec))
    }

    /// Locate a region by region id and return its range metadata.
    ///
    /// Unlike [`RegionCache::locate_region_by_id`], this helper automatically decodes region keys
    /// for clients configured with API V2 codecs.
    #[doc(alias = "LocateRegionByID")]
    pub async fn locate_region_by_id(&self, id: RegionId) -> Result<KeyLocation> {
        self.region_cache
            .locate_region_by_id(id)
            .await
            .and_then(|location| decode_key_location(location, self.enable_codec))
    }

    /// Locate the consecutive regions covering `[start_key, end_key)`.
    ///
    /// Unlike [`RegionCache::locate_key_range`], this helper automatically applies PD key
    /// encoding/decoding for clients configured with API V2 codecs.
    #[doc(alias = "LocateKeyRange")]
    pub async fn locate_key_range(&self, start_key: Key, end_key: Key) -> Result<Vec<KeyLocation>> {
        let enable_codec = self.enable_codec;
        let start_key = if enable_codec {
            start_key.to_encoded()
        } else {
            start_key
        };
        let end_key = if enable_codec {
            end_key.to_encoded()
        } else {
            end_key
        };

        self.region_cache
            .locate_key_range(start_key, end_key)
            .await?
            .into_iter()
            .map(|location| decode_key_location(location, enable_codec))
            .collect()
    }

    /// Locate multiple key ranges and merge adjacent duplicates from the same region.
    ///
    /// Unlike [`RegionCache::batch_locate_key_ranges`], this helper automatically applies PD key
    /// encoding/decoding for clients configured with API V2 codecs.
    #[doc(alias = "BatchLocateKeyRanges")]
    pub async fn batch_locate_key_ranges(
        &self,
        ranges: Vec<crate::kv::KeyRange>,
    ) -> Result<Vec<KeyLocation>> {
        let enable_codec = self.enable_codec;
        let ranges = ranges
            .into_iter()
            .map(|range| {
                crate::kv::KeyRange::new(
                    if enable_codec {
                        range.start_key.to_encoded()
                    } else {
                        range.start_key
                    },
                    if enable_codec {
                        range.end_key.to_encoded()
                    } else {
                        range.end_key
                    },
                )
            })
            .collect();

        self.region_cache
            .batch_locate_key_ranges(ranges)
            .await?
            .into_iter()
            .map(|location| decode_key_location(location, enable_codec))
            .collect()
    }
}

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl> {
    /// Return the shared PD HTTP client, if this client was configured with one.
    pub fn pd_http_client(&self) -> Option<&reqwest::Client> {
        self.pd_http_client.as_ref()
    }

    /// Return the configured PD HTTP endpoints for this client.
    pub fn pd_http_endpoints(&self) -> &[String] {
        &self.pd_http_endpoints
    }

    /// Return whether the configured PD HTTP endpoints should be contacted over HTTPS.
    pub fn pd_http_uses_https(&self) -> bool {
        self.pd_http_use_https
    }

    /// Return the shared region cache used by this PD client.
    pub fn region_cache(&self) -> &RegionCache<RetryClient<Cl>> {
        &self.region_cache
    }

    fn is_explicitly_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.is_explicitly_closed()
    }

    async fn close_inner(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        self.kv_client_cache.write().await.clear();
        self.region_cache.clear().await;

        match self.slow_store_until.lock() {
            Ok(mut guard) => guard.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
        match self.store_estimated_wait_until.lock() {
            Ok(mut guard) => guard.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
        match self.store_token_count.lock() {
            Ok(mut guard) => guard.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }

    #[cfg(test)]
    pub(crate) async fn close(&self) {
        self.close_inner().await;
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_explicitly_closed() {
            return Err(Error::StringError("client is closed".to_owned()));
        }
        Ok(())
    }
}

impl<KvC: KvConnect + Send + Sync + 'static> PdRpcClient<KvC, Cluster> {
    pub(crate) fn spawn_region_cache_preload(self: Arc<Self>, start_key: Key, end_key: Key) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("region cache preload skipped: no tokio runtime");
            return;
        };

        let weak = Arc::downgrade(&self);
        handle.spawn(async move {
            let Some(client) = weak.upgrade() else {
                return;
            };
            if client.is_closed() {
                return;
            }

            let enable_codec = client.enable_codec;
            let start_key = if enable_codec {
                start_key.to_encoded()
            } else {
                start_key
            };
            let end_key = if end_key.is_empty() {
                end_key
            } else if enable_codec {
                end_key.to_encoded()
            } else {
                end_key
            };

            match client
                .region_cache
                .preload_region_index(start_key, end_key, REGION_CACHE_PRELOAD_LIMIT)
                .await
            {
                Ok(loaded) => {
                    debug!("region cache preload finished: loaded {loaded} regions");
                }
                Err(Error::Unimplemented) => {
                    debug!("region cache preload skipped: PD ScanRegions unimplemented");
                }
                Err(err) => {
                    warn!("region cache preload failed: {err}");
                }
            }
        });
    }
}

impl<KvC: KvConnect + Send + Sync + 'static> HealthFeedbackObserver for PdRpcClient<KvC, Cluster> {
    fn observe_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
        crate::stats::set_feedback_slow_score(feedback.store_id, feedback.slow_score);
        if feedback.slow_score >= HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD {
            PdClient::mark_store_slow(self, feedback.store_id, HEALTH_FEEDBACK_SLOW_STORE_TTL);
        }
    }
}

#[async_trait]
impl<KvC: KvConnect + Send + Sync + 'static> PdClient for PdRpcClient<KvC> {
    type KvClient = KvC::KvClient;

    fn is_closed(&self) -> bool {
        self.is_explicitly_closed()
    }

    async fn close(&self) {
        self.close_inner().await;
    }

    fn resolve_lock_lite_threshold(&self) -> u64 {
        self.resolve_lock_lite_threshold
    }

    fn ttl_refreshed_txn_size(&self) -> u64 {
        self.ttl_refreshed_txn_size
    }

    fn committer_concurrency(&self) -> usize {
        self.committer_concurrency
    }

    fn max_txn_ttl(&self) -> Duration {
        self.max_txn_ttl
    }

    fn commit_timeout(&self) -> Duration {
        self.commit_timeout
    }

    fn async_commit_keys_limit(&self) -> usize {
        self.async_commit_keys_limit
    }

    fn async_commit_total_key_size_limit(&self) -> u64 {
        self.async_commit_total_key_size_limit
    }

    fn async_commit_safe_window(&self) -> Duration {
        self.async_commit_safe_window
    }

    fn async_commit_allowed_clock_drift(&self) -> Duration {
        self.async_commit_allowed_clock_drift
    }

    fn enable_forwarding(&self) -> bool {
        self.enable_forwarding
    }

    fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        self.ensure_open()?;
        let store_id = region.get_store_id()?;
        let store = self.region_cache.get_store_by_id(store_id).await?;
        let metapb::Store { address, .. } = store;
        let kv_client = self.kv_client(&address).await?;
        let mut client: Arc<dyn KvClient + Send + Sync> = Arc::new(kv_client);
        let effective_store_limit = if self.store_limit > 0 {
            self.store_limit
        } else {
            crate::store_vars::store_limit()
        };
        if effective_store_limit > 0 {
            let token_count = self.store_token_counter(store_id);
            client = Arc::new(StoreLimitKvClient::new(
                client,
                store_id,
                address.clone(),
                self.store_limit,
                token_count,
            ));
        }
        Ok(RegionStore::new(region, client, address))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        let enable_codec = self.enable_codec;
        let key = if enable_codec {
            key.to_encoded()
        } else {
            key.clone()
        };

        let region = self.region_cache.get_region_by_key(&key).await?;
        Self::decode_region(region, enable_codec)
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        if key.is_empty() {
            return Err(Error::Unimplemented);
        }

        let enable_codec = self.enable_codec;
        let key = if enable_codec {
            key.to_encoded()
        } else {
            key.clone()
        };
        let key_bytes: &[u8] = (&key).into();

        let region = self.region_cache.get_region_by_key(&key).await?;
        if key_bytes > region.region.start_key.as_slice() {
            return Self::decode_region(region, enable_codec);
        }

        let (prev_region, buckets) = self
            .region_cache
            .pd_region_meta_circuit_breaker()
            .execute(|| self.pd.clone().get_prev_region_with_buckets(key.into()))
            .await?;
        self.region_cache
            .add_region_with_buckets(prev_region.clone(), buckets)
            .await;
        Self::decode_region(prev_region, enable_codec)
    }

    fn group_ranges_by_region(
        self: Arc<Self>,
        ranges: Vec<kvrpcpb::KeyRange>,
    ) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
        futures::stream::once(async move {
            self.ensure_open()?;
            let enable_codec = self.enable_codec;
            let encoded_ranges = ranges
                .iter()
                .map(|range| {
                    crate::kv::KeyRange::new(
                        if enable_codec {
                            Key::from(range.start_key.clone()).to_encoded()
                        } else {
                            range.start_key.clone().into()
                        },
                        if enable_codec {
                            Key::from(range.end_key.clone()).to_encoded()
                        } else {
                            range.end_key.clone().into()
                        },
                    )
                })
                .collect();
            let locations = self
                .region_cache
                .batch_locate_key_ranges(encoded_ranges)
                .await?;

            let mut groups = Vec::with_capacity(locations.len());
            let mut pending_ranges = ranges.into_iter();
            let mut current_range = pending_ranges.next();
            for location in locations {
                let region = self
                    .region_cache
                    .get_region_by_id(location.region.id)
                    .await?;
                let region = Self::decode_region(region, enable_codec)?;
                let region_start = region.start_key();
                let region_end = region.end_key();
                let mut grouped = Vec::new();

                loop {
                    let Some(range) = current_range.take() else {
                        break;
                    };

                    let start_key: Key = range.start_key.clone().into();
                    let end_key: Key = range.end_key.clone().into();
                    if start_key < region_start
                        || (!region_end.is_empty() && start_key >= region_end)
                    {
                        current_range = Some(range);
                        break;
                    }
                    if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                        grouped.push(make_key_range(start_key.into(), region_end.clone().into()));
                        current_range = Some(make_key_range(region_end.into(), end_key.into()));
                        break;
                    }

                    grouped.push(range);
                    current_range = pending_ranges.next();
                }

                if !grouped.is_empty() {
                    groups.push((grouped, region));
                }
                if current_range.is_none() {
                    break;
                }
            }

            Ok::<_, Error>(futures::stream::iter(groups.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        let region = self.region_cache.get_region_by_id(id).await?;
        Self::decode_region(region, self.enable_codec)
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Key,
        end_key: Option<Key>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        self.ensure_open()?;
        let enable_codec = self.enable_codec;
        let start_key = if enable_codec {
            start_key.to_encoded()
        } else {
            start_key
        };
        let end_key = end_key.map(|key| if enable_codec { key.to_encoded() } else { key });
        let end_key = end_key.unwrap_or(Key::EMPTY);

        let regions = self
            .region_cache
            .pd_region_meta_circuit_breaker()
            .execute(|| {
                self.pd
                    .clone()
                    .scan_regions(start_key.into(), end_key.into(), limit)
            })
            .await?;
        regions
            .into_iter()
            .map(|region| Self::decode_region(region, enable_codec))
            .collect()
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        self.ensure_open()?;
        let pb_stores = self.region_cache.read_through_all_stores().await?;
        let mut stores = Vec::with_capacity(pb_stores.len());
        for store in pb_stores {
            let client = self.kv_client(&store.address).await?;
            stores.push(Store::new(store, Arc::new(client)));
        }
        Ok(stores)
    }

    async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
        self.ensure_open()?;
        let pb_stores = self
            .region_cache
            .read_through_all_stores_for_safe_ts()
            .await?;
        let mut stores = Vec::with_capacity(pb_stores.len());
        for store in pb_stores {
            let addr = safe_ts_store_address(&store);
            let client = self.kv_client(addr).await?;
            stores.push(Store::new(store, Arc::new(client)));
        }
        Ok(stores)
    }

    async fn min_resolved_ts_by_stores(
        &self,
        store_ids: &[StoreId],
    ) -> Result<(u64, HashMap<StoreId, u64>)> {
        self.ensure_open()?;
        let Some(http_client) = self.pd_http_client.as_ref() else {
            return Err(Error::Unimplemented);
        };
        if self.pd_http_endpoints.is_empty() {
            return Err(Error::Unimplemented);
        }
        pd_http_min_resolved_ts_by_stores(
            http_client,
            &self.pd_http_endpoints,
            self.pd_http_use_https,
            store_ids,
        )
        .await
    }

    async fn store_meta_by_id(&self, store_id: StoreId) -> Result<metapb::Store> {
        self.ensure_open()?;
        self.region_cache.get_store_by_id(store_id).await
    }

    fn store_meta_by_id_cached(&self, store_id: StoreId) -> Option<metapb::Store> {
        self.region_cache.store_meta_by_id_cached(store_id)
    }

    async fn get_health_feedback(&self, store_id: StoreId) -> Result<kvrpcpb::HealthFeedback> {
        self.ensure_open()?;
        let store = self.store_meta_by_id(store_id).await?;
        let address = safe_ts_store_address(&store);
        let client = self.kv_client(address).await?;

        let mut req = kvrpcpb::GetHealthFeedbackRequest::default();
        req.context = Some(kvrpcpb::Context::default());
        let resp = client.dispatch(&req).await?;
        let resp = resp
            .downcast::<kvrpcpb::GetHealthFeedbackResponse>()
            .map_err(|_| {
                Error::StringError("unexpected GetHealthFeedback response type".to_owned())
            })?;
        let resp = *resp;
        if let Some(region_error) = resp.region_error {
            return Err(Error::RegionError(Box::new(region_error)));
        }

        let Some(feedback) = resp.health_feedback else {
            return Err(Error::Unimplemented);
        };
        if feedback.slow_score >= HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD {
            self.mark_store_slow(feedback.store_id, HEALTH_FEEDBACK_SLOW_STORE_TTL);
        }
        Ok(feedback)
    }

    fn mark_store_slow(&self, store_id: StoreId, duration: Duration) {
        if duration.is_zero() {
            return;
        }

        let until = Instant::now() + duration;
        let mut slow_store_until = match self.slow_store_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if matches!(
            slow_store_until.get(&store_id),
            Some(existing) if *existing >= until
        ) {
            return;
        }
        slow_store_until.insert(store_id, until);
    }

    fn is_store_slow(&self, store_id: StoreId) -> bool {
        let now = Instant::now();
        let mut slow_store_until = match self.slow_store_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match slow_store_until.get(&store_id) {
            Some(until) if *until > now => true,
            Some(_) => {
                slow_store_until.remove(&store_id);
                false
            }
            None => false,
        }
    }

    fn update_store_load_stats(&self, store_id: StoreId, estimated_wait_ms: u32) {
        if estimated_wait_ms == 0 {
            return;
        }

        let estimated_wait = Duration::from_millis(u64::from(estimated_wait_ms));
        let until = Instant::now() + estimated_wait;

        let mut store_estimated_wait_until = match self.store_estimated_wait_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        store_estimated_wait_until.insert(store_id, until);
    }

    fn store_estimated_wait_time(&self, store_id: StoreId) -> Duration {
        let now = Instant::now();
        let mut store_estimated_wait_until = match self.store_estimated_wait_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match store_estimated_wait_until.get(&store_id) {
            Some(until) if *until > now => until.duration_since(now),
            Some(_) => {
                store_estimated_wait_until.remove(&store_id);
                Duration::ZERO
            }
            None => Duration::ZERO,
        }
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.ensure_open()?;
        self.pd.clone().get_timestamp().await
    }

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        self.ensure_open()?;
        self.pd
            .clone()
            .get_timestamp_with_dc_location(dc_location)
            .await
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        self.ensure_open()?;
        self.pd.clone().get_min_ts().await
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.ensure_open()?;
        self.pd.clone().get_external_timestamp().await
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.ensure_open()?;
        self.pd.clone().set_external_timestamp(timestamp).await
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<RegionId>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        self.ensure_open()?;
        if region_ids.is_empty() {
            return Ok(pdpb::ScatterRegionResponse::default());
        }
        self.pd.clone().scatter_regions(region_ids, group).await
    }

    async fn get_operator(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<pdpb::GetOperatorResponse> {
        self.ensure_open()?;
        self.pd.clone().get_operator(region_id).await
    }

    async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
        self.ensure_open()?;
        self.pd.clone().get_gc_safe_point().await
    }

    async fn get_gc_safe_point_v2(self: Arc<Self>, keyspace_id: u32) -> Result<u64> {
        self.ensure_open()?;
        self.pd.clone().get_gc_safe_point_v2(keyspace_id).await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        self.ensure_open()?;
        self.pd.clone().update_safepoint(safepoint).await
    }

    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        self.pd
            .clone()
            .update_service_gc_safe_point(service_id, ttl, safe_point)
            .await
    }

    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        self.pd
            .clone()
            .update_service_safe_point_v2(keyspace_id, service_id, ttl, safe_point)
            .await
    }

    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        self.pd
            .clone()
            .update_gc_safe_point_v2(keyspace_id, safe_point)
            .await
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()> {
        self.region_cache.update_leader(ver_id, leader).await
    }

    async fn invalidate_region_cache(&self, ver_id: RegionVerId) {
        self.region_cache.invalidate_region_cache(ver_id).await
    }

    async fn invalidate_store_cache(&self, store_id: StoreId) {
        self.region_cache.invalidate_store_cache(store_id).await
    }

    async fn add_region_to_cache(&self, region: RegionWithLeader) {
        self.region_cache.add_region(region).await;
    }

    async fn on_bucket_version_not_match(
        &self,
        ver_id: RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        self.region_cache
            .on_bucket_version_not_match(ver_id, version, keys)
            .await;
    }

    async fn buckets_version(&self, ver_id: RegionVerId) -> u64 {
        self.region_cache
            .get_buckets_by_ver_id(&ver_id)
            .await
            .map(|buckets| buckets.version)
            .unwrap_or(0)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.ensure_open()?;
        self.pd.load_keyspace(keyspace).await
    }
}

impl PdRpcClient<TikvConnect, Cluster> {
    pub async fn connect(
        pd_endpoints: &[String],
        config: Config,
        enable_codec: bool,
    ) -> Result<PdRpcClient> {
        let mut client = PdRpcClient::new(
            config.clone(),
            |security_mgr| {
                TikvConnect::new(
                    security_mgr,
                    config.timeout,
                    config.copr_req_timeout,
                    config.grpc_max_decoding_message_size,
                    config.grpc_compression_type,
                    config.enable_batch_rpc,
                    config.batch_rpc_max_batch_size,
                    config.batch_rpc_wait_size,
                    config.batch_rpc_max_wait_time,
                )
                .with_batch_rpc_policy(config.batch_rpc_policy.clone())
                .with_batch_rpc_overload_threshold(config.batch_rpc_overload_threshold)
                .with_max_concurrency_request_limit(config.max_concurrency_request_limit)
                .with_copr_cache_config(config.copr_cache.clone())
            },
            |security_mgr| {
                RetryClient::connect(
                    pd_endpoints,
                    security_mgr,
                    config.pd_server_timeout,
                    config.tso_max_pending_count,
                )
            },
            enable_codec,
        )
        .await?;
        client.pd_http_endpoints = pd_endpoints.to_vec();
        Ok(client)
    }

    pub(crate) fn install_health_feedback_observer(self: &Arc<Self>) {
        let observer: Arc<dyn HealthFeedbackObserver> = self.clone();
        self.kv_connect
            .set_health_feedback_observer(Arc::downgrade(&observer));
    }
}

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl> {
    pub(crate) fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    pub(crate) fn security_manager(&self) -> Arc<SecurityManager> {
        self.security_mgr.clone()
    }

    fn store_token_counter(&self, store_id: StoreId) -> Arc<AtomicI64> {
        let mut guard = self
            .store_token_count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .entry(store_id)
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone()
    }

    pub async fn new<PdFut, MakeKvC, MakePd>(
        config: Config,
        kv_connect: MakeKvC,
        pd: MakePd,
        enable_codec: bool,
    ) -> Result<PdRpcClient<KvC, Cl>>
    where
        PdFut: Future<Output = Result<RetryClient<Cl>>>,
        MakeKvC: FnOnce(Arc<SecurityManager>) -> KvC,
        MakePd: FnOnce(Arc<SecurityManager>) -> PdFut,
    {
        config.validate()?;

        let grpc_connection_count = config.grpc_connection_count;
        let store_limit = config.store_limit;
        let enable_forwarding = config.enable_forwarding;
        let resolve_lock_lite_threshold = config.resolve_lock_lite_threshold;
        let ttl_refreshed_txn_size = config.ttl_refreshed_txn_size;
        let committer_concurrency = config.committer_concurrency;
        let max_txn_ttl = config.max_txn_ttl;
        let commit_timeout = config.commit_timeout;
        let async_commit_keys_limit = config.async_commit_keys_limit;
        let async_commit_total_key_size_limit = config.async_commit_total_key_size_limit;
        let async_commit_safe_window = config.async_commit_safe_window;
        let async_commit_allowed_clock_drift = config.async_commit_allowed_clock_drift;
        let (pd_http_client, pd_http_use_https) = build_pd_http_client(&config);
        let security_mgr = Arc::new(config.security_manager()?);

        let pd = Arc::new(pd(security_mgr.clone()).await?);
        let kv_connect = kv_connect(security_mgr.clone());
        let kv_client_cache = Default::default();
        Ok(PdRpcClient {
            pd: pd.clone(),
            security_mgr,
            kv_client_cache,
            kv_connect,
            closed: AtomicBool::new(false),
            grpc_connection_count,
            slow_store_until: Mutex::new(HashMap::new()),
            store_estimated_wait_until: Mutex::new(HashMap::new()),
            store_limit,
            store_token_count: Mutex::new(HashMap::new()),
            enable_forwarding,
            enable_codec,
            region_cache: RegionCache::new_with_ttl(
                pd,
                config.region_cache_ttl,
                config.region_cache_ttl_jitter,
            ),
            resolve_lock_lite_threshold,
            ttl_refreshed_txn_size,
            committer_concurrency,
            max_txn_ttl,
            commit_timeout,
            async_commit_keys_limit,
            async_commit_total_key_size_limit,
            async_commit_safe_window,
            async_commit_allowed_clock_drift,
            pd_http_client,
            pd_http_endpoints: Vec::new(),
            pd_http_use_https,
        })
    }

    async fn kv_client(&self, address: &str) -> Result<KvC::KvClient> {
        self.ensure_open()?;
        if let Some(pool) = self.kv_client_cache.read().await.get(address) {
            return Ok(pool.pick());
        };
        info!("connect to tikv endpoint: {:?}", address);

        let mut clients = Vec::with_capacity(self.grpc_connection_count);
        for _ in 0..self.grpc_connection_count {
            clients.push(self.kv_connect.connect(address).await?);
        }

        let pool = KvClientPool::new(clients);
        let selected = pool.pick();
        self.kv_client_cache
            .write()
            .await
            .insert(address.to_owned(), pool);
        Ok(selected)
    }

    pub(crate) async fn close_addr(&self, address: &str) -> bool {
        self.kv_client_cache.write().await.remove(address).is_some()
    }
}

impl<KvC: KvConnect + Send + Sync + 'static> PdRpcClient<KvC> {
    pub async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: impl Into<String>,
    ) -> Result<Timestamp> {
        self.pd
            .clone()
            .get_timestamp_with_dc_location(dc_location.into())
            .await
    }
}

fn pd_http_base_url(endpoint: &str, use_https: bool) -> String {
    let endpoint = endpoint.trim();
    let endpoint = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let endpoint = endpoint.trim_end_matches('/');

    let scheme = if use_https { "https://" } else { "http://" };
    format!("{scheme}{endpoint}")
}

fn build_pd_http_client(config: &Config) -> (Option<reqwest::Client>, bool) {
    let use_https =
        config.ca_path.is_some() && config.cert_path.is_some() && config.key_path.is_some();
    let mut builder = reqwest::Client::builder().timeout(config.pd_server_timeout);

    if let (Some(ca_path), Some(cert_path), Some(key_path)) =
        (&config.ca_path, &config.cert_path, &config.key_path)
    {
        let ca_pem = match std::fs::read(ca_path) {
            Ok(pem) => pem,
            Err(err) => {
                debug!("pd http client disabled: failed to read CA pem: {err}");
                return (None, use_https);
            }
        };
        let cert_pem = match std::fs::read(cert_path) {
            Ok(pem) => pem,
            Err(err) => {
                debug!("pd http client disabled: failed to read cert pem: {err}");
                return (None, use_https);
            }
        };
        let key_pem = match std::fs::read(key_path) {
            Ok(pem) => pem,
            Err(err) => {
                debug!("pd http client disabled: failed to read key pem: {err}");
                return (None, use_https);
            }
        };

        let ca = match reqwest::Certificate::from_pem(&ca_pem) {
            Ok(ca) => ca,
            Err(err) => {
                debug!("pd http client disabled: invalid CA pem: {err}");
                return (None, use_https);
            }
        };
        builder = builder.add_root_certificate(ca);

        let identity = match reqwest::Identity::from_pkcs8_pem(&cert_pem, &key_pem) {
            Ok(identity) => identity,
            Err(err) => {
                debug!("pd http client disabled: invalid identity pem: {err}");
                return (None, use_https);
            }
        };
        builder = builder.identity(identity);
    }

    match builder.build() {
        Ok(client) => (Some(client), use_https),
        Err(err) => {
            debug!("pd http client disabled: failed to build http client: {err}");
            (None, use_https)
        }
    }
}

async fn pd_http_min_resolved_ts_by_stores(
    http_client: &reqwest::Client,
    endpoints: &[String],
    use_https: bool,
    store_ids: &[StoreId],
) -> Result<(u64, HashMap<StoreId, u64>)> {
    const MIN_RESOLVED_TS_PATH: &str = "/pd/api/v1/min-resolved-ts";

    #[derive(Deserialize)]
    struct MinResolvedTsResponse {
        min_resolved_ts: u64,
        #[serde(default)]
        is_real_time: bool,
        #[serde(default)]
        stores_min_resolved_ts: HashMap<StoreId, u64>,
    }

    let uri = if store_ids.is_empty() {
        MIN_RESOLVED_TS_PATH.to_owned()
    } else {
        let scope = store_ids
            .iter()
            .map(|store_id| store_id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!("{MIN_RESOLVED_TS_PATH}?scope={scope}")
    };

    let mut last_err = None;
    for endpoint in endpoints {
        let base = pd_http_base_url(endpoint, use_https);
        let url = format!("{base}{uri}");
        let resp = match http_client.get(&url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                let message =
                    format!("pd http min-resolved-ts request to {endpoint} failed: {err}");
                last_err = Some(if err.is_timeout() {
                    crate::PdServerTimeoutError::new(message).into()
                } else {
                    Error::StringError(message)
                });
                continue;
            }
        };

        let resp = match resp.error_for_status() {
            Ok(resp) => resp,
            Err(err) => {
                last_err = Some(Error::StringError(format!(
                    "pd http min-resolved-ts request to {endpoint} failed: {err}"
                )));
                continue;
            }
        };

        let decoded = match resp.json::<MinResolvedTsResponse>().await {
            Ok(decoded) => decoded,
            Err(err) => {
                last_err = Some(Error::StringError(format!(
                    "pd http min-resolved-ts response from {endpoint} is invalid: {err}"
                )));
                continue;
            }
        };

        if !decoded.is_real_time {
            last_err = Some(Error::StringError(
                "pd http min-resolved-ts is not enabled".to_owned(),
            ));
            continue;
        }

        return Ok((decoded.min_resolved_ts, decoded.stores_min_resolved_ts));
    }

    Err(last_err.unwrap_or(Error::Unimplemented))
}

fn safe_ts_store_address(store: &metapb::Store) -> &str {
    if is_tiflash_store(store) && !store.peer_address.is_empty() {
        &store.peer_address
    } else {
        &store.address
    }
}

fn make_key_range(start_key: Vec<u8>, end_key: Vec<u8>) -> kvrpcpb::KeyRange {
    let mut key_range = kvrpcpb::KeyRange::default();
    key_range.start_key = start_key;
    key_range.end_key = end_key;
    key_range
}

#[cfg(test)]
pub mod test {
    use std::any::Any;
    use std::io::BufRead;
    use std::io::BufReader;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;

    use futures::executor;
    use futures::executor::block_on;

    use super::*;
    use crate::mock::*;
    use crate::store::Request;

    #[async_trait]
    impl RetryClientTrait for RetryClient<MockCluster> {
        async fn get_region(self: Arc<Self>, _key: Vec<u8>) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            _region_id: RegionId,
        ) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_store(self: Arc<Self>, _id: StoreId) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[tokio::test]
    async fn test_kv_client_caching() {
        let client = block_on(pd_rpc_client());

        let addr1 = "foo";
        let addr2 = "bar";

        let kv1 = client.kv_client(addr1).await.unwrap();
        let kv2 = client.kv_client(addr2).await.unwrap();
        let kv3 = client.kv_client(addr2).await.unwrap();
        assert!(kv1.addr != kv2.addr);
        assert_eq!(kv2.addr, kv3.addr);
    }

    #[tokio::test]
    async fn test_committer_concurrency_is_plumbed_from_config() {
        let config = Config::default().with_committer_concurrency(7);
        let client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    0,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        assert_eq!(client.committer_concurrency, 7);
    }

    #[tokio::test]
    async fn test_async_commit_limits_are_plumbed_from_config() {
        let config = Config::default()
            .with_async_commit_keys_limit(7)
            .with_async_commit_total_key_size_limit(123)
            .with_async_commit_safe_window(Duration::from_secs(10))
            .with_async_commit_allowed_clock_drift(Duration::from_millis(1234));
        let client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    0,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        assert_eq!(client.async_commit_keys_limit, 7);
        assert_eq!(client.async_commit_total_key_size_limit, 123);
        assert_eq!(client.async_commit_safe_window, Duration::from_secs(10));
        assert_eq!(
            client.async_commit_allowed_clock_drift,
            Duration::from_millis(1234)
        );
    }

    #[tokio::test]
    async fn test_max_txn_ttl_is_plumbed_from_config() {
        let config = Config::default().with_max_txn_ttl(Duration::from_secs(123));
        let client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    0,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        assert_eq!(client.max_txn_ttl, Duration::from_secs(123));
    }

    #[derive(Clone)]
    struct CountingKvClient {
        id: usize,
    }

    #[async_trait]
    impl KvClient for CountingKvClient {
        async fn dispatch(&self, _req: &dyn Request) -> Result<Box<dyn Any>> {
            Ok(Box::new(()))
        }
    }

    struct CountingKvConnect {
        connect_calls: AtomicUsize,
    }

    #[async_trait]
    impl KvConnect for CountingKvConnect {
        type KvClient = CountingKvClient;

        async fn connect(&self, _address: &str) -> Result<Self::KvClient> {
            let id = self.connect_calls.fetch_add(1, Ordering::Relaxed);
            Ok(CountingKvClient { id })
        }
    }

    #[tokio::test]
    async fn test_kv_client_pool_round_robin() {
        let config = Config::default().with_grpc_connection_count(3);
        let client = PdRpcClient::new(
            config.clone(),
            |_| CountingKvConnect {
                connect_calls: AtomicUsize::new(0),
            },
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let c0 = client.kv_client("foo").await.unwrap();
        let c1 = client.kv_client("foo").await.unwrap();
        let c2 = client.kv_client("foo").await.unwrap();
        let c3 = client.kv_client("foo").await.unwrap();
        assert_eq!([c0.id, c1.id, c2.id, c3.id], [0, 1, 2, 0]);

        assert_eq!(client.kv_connect.connect_calls.load(Ordering::Relaxed), 3);
        let cache = client.kv_client_cache.read().await;
        let pool = cache.get("foo").expect("kv client pool should be cached");
        assert_eq!(pool.len(), 3);
    }

    #[tokio::test]
    async fn test_kv_client_pool_close_addr_drops_pool() {
        let config = Config::default().with_grpc_connection_count(2);
        let client = PdRpcClient::new(
            config.clone(),
            |_| CountingKvConnect {
                connect_calls: AtomicUsize::new(0),
            },
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let c0 = client.kv_client("foo").await.unwrap();
        assert_eq!(c0.id, 0);
        assert_eq!(client.kv_connect.connect_calls.load(Ordering::Relaxed), 2);

        assert!(client.close_addr("foo").await);
        let c1 = client.kv_client("foo").await.unwrap();
        assert_eq!(c1.id, 2);
        assert_eq!(client.kv_connect.connect_calls.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn test_kv_client_close_clears_pool_and_prevents_reconnect() {
        let config = Config::default().with_grpc_connection_count(2);
        let client = PdRpcClient::new(
            config.clone(),
            |_| CountingKvConnect {
                connect_calls: AtomicUsize::new(0),
            },
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let c0 = client.kv_client("foo").await.unwrap();
        assert_eq!(c0.id, 0);
        assert_eq!(client.kv_connect.connect_calls.load(Ordering::Relaxed), 2);

        client.close().await;
        client.close().await;

        assert!(client.kv_client_cache.read().await.is_empty());

        let err = match client.kv_client("foo").await {
            Ok(client) => panic!("expected closed client error, got client id {}", client.id),
            Err(err) => err,
        };
        assert!(matches!(err, Error::StringError(msg) if msg == "client is closed"));
        assert_eq!(client.kv_connect.connect_calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_pd_rpc_client_cluster_id_propagated() {
        let config = Config::default();
        let client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        assert_eq!(client.cluster_id(), 42);
    }

    #[tokio::test]
    async fn test_pd_rpc_client_applies_grpc_keepalive_config() {
        let config = Config::default()
            .with_grpc_keepalive_time(Duration::from_secs(1))
            .with_grpc_keepalive_timeout(Duration::from_secs(2))
            .with_grpc_initial_window_size(123)
            .with_grpc_initial_conn_window_size(456)
            .with_grpc_connect_timeout(Duration::from_secs(7));

        let seen_configs = Arc::new(Mutex::new(Vec::new()));
        let seen_configs_pd = Arc::clone(&seen_configs);
        let seen_configs_kv = Arc::clone(&seen_configs);

        PdRpcClient::new(
            config.clone(),
            |sm| {
                seen_configs_kv.lock().unwrap().push((
                    sm.grpc_keepalive_config(),
                    sm.grpc_window_sizes(),
                    sm.grpc_connect_timeout(),
                ));
                MockKvConnect
            },
            |sm| {
                seen_configs_pd.lock().unwrap().push((
                    sm.grpc_keepalive_config(),
                    sm.grpc_window_sizes(),
                    sm.grpc_connect_timeout(),
                ));
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let seen_configs = seen_configs.lock().unwrap();
        assert_eq!(seen_configs.len(), 2);
        assert!(seen_configs
            .iter()
            .copied()
            .all(|(keepalive, windows, connect_timeout)| {
                keepalive == (Duration::from_secs(1), Duration::from_secs(2))
                    && windows == (123, 456)
                    && connect_timeout == Duration::from_secs(7)
            }));
    }

    #[tokio::test]
    async fn test_pd_http_min_resolved_ts_by_stores_ids_parses_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut first_line = String::new();
            reader.read_line(&mut first_line).unwrap();
            let path = first_line.split_whitespace().nth(1).unwrap_or("");
            assert!(
                path == "/pd/api/v1/min-resolved-ts?scope=1,2"
                    || path == "/pd/api/v1/min-resolved-ts?scope=1%2C2",
                "unexpected request path: {path:?}"
            );

            let body = r#"{"min_resolved_ts":10,"is_real_time":true,"stores_min_resolved_ts":{"1":100,"2":10}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let mut stream = reader.into_inner();
            stream.write_all(response.as_bytes()).unwrap();
        });

        let config = Config::default();
        let mut client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        client.pd_http_endpoints = vec![addr.to_string()];

        let (min_resolved_ts, store_safe_ts) = pd_http_min_resolved_ts_by_stores(
            client
                .pd_http_client
                .as_ref()
                .expect("pd http client should be constructed"),
            &client.pd_http_endpoints,
            client.pd_http_use_https,
            &[1, 2],
        )
        .await
        .unwrap();
        assert_eq!(min_resolved_ts, 10);
        assert_eq!(store_safe_ts.get(&1).copied(), Some(100));
        assert_eq!(store_safe_ts.get(&2).copied(), Some(10));

        server.join().unwrap();
    }

    #[tokio::test]
    async fn test_pd_http_client_uses_pd_server_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut first_line = String::new();
            reader.read_line(&mut first_line).unwrap();
            let path = first_line.split_whitespace().nth(1).unwrap_or("");
            assert!(
                path == "/pd/api/v1/min-resolved-ts?scope=1,2"
                    || path == "/pd/api/v1/min-resolved-ts?scope=1%2C2",
                "unexpected request path: {path:?}"
            );

            std::thread::sleep(Duration::from_secs(1));
        });

        let config = Config::default()
            .with_timeout(Duration::from_secs(10))
            .with_pd_server_timeout(Duration::from_millis(50));
        let mut client = PdRpcClient::new(
            config.clone(),
            |_| MockKvConnect,
            |sm| {
                futures::future::ok(RetryClient::new_with_cluster(
                    sm,
                    config.timeout,
                    42,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();
        client.pd_http_endpoints = vec![addr.to_string()];

        let fut = pd_http_min_resolved_ts_by_stores(
            client
                .pd_http_client
                .as_ref()
                .expect("pd http client should be constructed"),
            &client.pd_http_endpoints,
            client.pd_http_use_https,
            &[1, 2],
        );

        let err = match tokio::time::timeout(Duration::from_millis(500), fut).await {
            Ok(Ok(_)) => panic!("expected pd http request to fail"),
            Ok(Err(err)) => err,
            Err(_) => panic!("expected pd http request to respect pd_server_timeout"),
        };

        let Error::PdServerTimeout(timeout) = err else {
            panic!("expected PdServerTimeout error, got: {err}");
        };
        assert!(
            timeout.message().contains("pd http min-resolved-ts"),
            "unexpected PdServerTimeout message: {timeout}"
        );
        server.join().unwrap();
    }

    #[test]
    fn test_safe_ts_store_address_prefers_tiflash_peer_address() {
        let mut store = metapb::Store {
            address: "addr".to_owned(),
            peer_address: "peer-addr".to_owned(),
            ..Default::default()
        };
        assert_eq!(safe_ts_store_address(&store), "addr");

        store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });
        assert_eq!(safe_ts_store_address(&store), "peer-addr");

        store.peer_address.clear();
        assert_eq!(safe_ts_store_address(&store), "addr");
    }

    #[test]
    fn test_group_keys_by_region() {
        let client = MockPdClient::default();

        let mut tasks: Vec<Key> = vec![
            vec![1].into(),
            vec![2].into(),
            vec![3].into(),
            vec![5, 2].into(),
            vec![12].into(),
            vec![11, 4].into(),
        ];
        tasks.sort();

        let stream = Arc::new(client).group_keys_by_region(tasks.into_iter());
        let mut stream = executor::block_on_stream(stream);

        let result: Vec<Key> = stream.next().unwrap().unwrap().0;
        assert_eq!(
            result,
            vec![
                vec![1].into(),
                vec![2].into(),
                vec![3].into(),
                vec![5, 2].into()
            ]
        );
        assert_eq!(
            stream.next().unwrap().unwrap().0,
            vec![vec![11, 4].into(), vec![12].into()]
        );
        assert!(stream.next().is_none());
    }

    #[tokio::test]
    async fn test_pd_client_get_timestamp_with_dc_location_in_mock_records_dc_location() {
        let client = Arc::new(MockPdClient::default());

        let _ts = client
            .clone()
            .get_timestamp_with_dc_location("dc1".to_owned())
            .await
            .unwrap();

        assert_eq!(client.get_timestamp_call_count(), 1);
        assert_eq!(client.get_timestamp_dc_locations(), vec!["dc1".to_owned()]);
    }

    #[test]
    fn test_regions_for_range() {
        let client = Arc::new(MockPdClient::default());
        let k1: Key = vec![1].into();
        let k2: Key = vec![5, 2].into();
        let k3: Key = vec![11, 4].into();
        let range1 = (k1, k2.clone()).into();
        let mut stream = executor::block_on_stream(client.clone().regions_for_range(range1));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert!(stream.next().is_none());

        let range2 = (k2, k3).into();
        let mut stream = executor::block_on_stream(client.regions_for_range(range2));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert_eq!(stream.next().unwrap().unwrap().id(), 2);
        assert!(stream.next().is_none());
    }

    #[test]
    fn test_regions_for_range_empty_range_returns_empty_stream() {
        #[derive(Clone)]
        struct EmptyRangePdClient;

        #[async_trait]
        impl PdClient for EmptyRangePdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                _region: RegionWithLeader,
            ) -> Result<RegionStore> {
                Err(Error::Unimplemented)
            }

            async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
                panic!("regions_for_range should not call region_for_key for empty ranges");
            }

            async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
                Err(Error::Unimplemented)
            }

            async fn scan_regions(
                self: Arc<Self>,
                _start_key: Key,
                _end_key: Option<Key>,
                _limit: i32,
            ) -> Result<Vec<RegionWithLeader>> {
                panic!("regions_for_range should not call scan_regions for empty ranges");
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Err(Error::Unimplemented)
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
                Err(Error::Unimplemented)
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                Err(Error::Unimplemented)
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Err(Error::Unimplemented)
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

            async fn invalidate_store_cache(&self, _store_id: StoreId) {}
        }

        let client = Arc::new(EmptyRangePdClient);
        let k1: Key = vec![10].into();
        let k2: Key = vec![10].into();
        let mut stream = executor::block_on_stream(client.regions_for_range((k1, k2).into()));
        assert!(stream.next().is_none());
    }

    #[test]
    fn test_regions_for_range_falls_back_when_scan_regions_returns_empty() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        #[derive(Clone)]
        struct EmptyScanPdClient {
            scan_calls: Arc<AtomicUsize>,
            get_region_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl PdClient for EmptyScanPdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                _region: RegionWithLeader,
            ) -> Result<RegionStore> {
                Err(Error::Unimplemented)
            }

            async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
                self.get_region_calls.fetch_add(1, Ordering::SeqCst);

                let key = <&[u8]>::from(key);
                let first = key.first().copied().unwrap_or(0);
                if first < 10 {
                    Ok(region(1, vec![], vec![10]))
                } else if first < 20 {
                    Ok(region(2, vec![10], vec![20]))
                } else {
                    Ok(region(3, vec![20], vec![]))
                }
            }

            async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
                Err(Error::Unimplemented)
            }

            async fn scan_regions(
                self: Arc<Self>,
                _start_key: Key,
                _end_key: Option<Key>,
                _limit: i32,
            ) -> Result<Vec<RegionWithLeader>> {
                self.scan_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Err(Error::Unimplemented)
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
                Err(Error::Unimplemented)
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                Err(Error::Unimplemented)
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Err(Error::Unimplemented)
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

            async fn invalidate_store_cache(&self, _store_id: StoreId) {}
        }

        fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            RegionWithLeader {
                region: metapb::Region {
                    id,
                    start_key,
                    end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                },
                leader: None,
            }
        }

        let scan_calls = Arc::new(AtomicUsize::new(0));
        let get_region_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(EmptyScanPdClient {
            scan_calls: scan_calls.clone(),
            get_region_calls: get_region_calls.clone(),
        });

        let start: Key = vec![5].into();
        let end: Key = vec![15].into();
        let mut stream = executor::block_on_stream(client.regions_for_range((start, end).into()));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert_eq!(stream.next().unwrap().unwrap().id(), 2);
        assert!(stream.next().is_none());
        assert_eq!(scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(get_region_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_regions_for_range_prefers_scan_regions_when_supported() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        #[derive(Clone)]
        struct ScanPdClient {
            regions: Vec<RegionWithLeader>,
            scan_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl PdClient for ScanPdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                _region: RegionWithLeader,
            ) -> Result<RegionStore> {
                Err(Error::Unimplemented)
            }

            async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
                panic!("regions_for_range should use scan_regions when available");
            }

            async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
                Err(Error::Unimplemented)
            }

            async fn scan_regions(
                self: Arc<Self>,
                start_key: Key,
                end_key: Option<Key>,
                limit: i32,
            ) -> Result<Vec<RegionWithLeader>> {
                self.scan_calls.fetch_add(1, Ordering::SeqCst);

                let end_key = end_key.unwrap_or(Key::EMPTY);
                let bounded_end = !end_key.is_empty();
                let mut out = Vec::new();
                for region in &self.regions {
                    let region_end = region.end_key();
                    if !region_end.is_empty() && region_end <= start_key {
                        continue;
                    }
                    if bounded_end && region.start_key() >= end_key {
                        break;
                    }
                    out.push(region.clone());
                    if limit > 0 && out.len() >= limit as usize {
                        break;
                    }
                    if bounded_end && (region_end.is_empty() || end_key <= region_end) {
                        break;
                    }
                    if region_end.is_empty() {
                        break;
                    }
                }
                Ok(out)
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Err(Error::Unimplemented)
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
                Err(Error::Unimplemented)
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                Err(Error::Unimplemented)
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Err(Error::Unimplemented)
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

            async fn invalidate_store_cache(&self, _store_id: StoreId) {}
        }

        fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            RegionWithLeader {
                region: metapb::Region {
                    id,
                    start_key,
                    end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                },
                leader: None,
            }
        }

        let scan_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(ScanPdClient {
            regions: vec![
                region(1, vec![], vec![10]),
                region(2, vec![10], vec![20]),
                region(3, vec![20], vec![]),
            ],
            scan_calls: scan_calls.clone(),
        });

        let start: Key = vec![5].into();
        let end: Key = vec![15].into();
        let mut stream = executor::block_on_stream(client.regions_for_range((start, end).into()));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert_eq!(stream.next().unwrap().unwrap().id(), 2);
        assert!(stream.next().is_none());
        assert_eq!(scan_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_regions_for_range_scan_regions_continues_after_batch_limit() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        #[derive(Clone)]
        struct ScanPdClient {
            regions: Vec<RegionWithLeader>,
            scan_calls: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl PdClient for ScanPdClient {
            type KvClient = MockKvClient;

            async fn map_region_to_store(
                self: Arc<Self>,
                _region: RegionWithLeader,
            ) -> Result<RegionStore> {
                Err(Error::Unimplemented)
            }

            async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
                panic!("regions_for_range should use scan_regions when available");
            }

            async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
                Err(Error::Unimplemented)
            }

            async fn scan_regions(
                self: Arc<Self>,
                start_key: Key,
                end_key: Option<Key>,
                limit: i32,
            ) -> Result<Vec<RegionWithLeader>> {
                self.scan_calls.fetch_add(1, Ordering::SeqCst);

                let end_key = end_key.unwrap_or(Key::EMPTY);
                let bounded_end = !end_key.is_empty();
                let mut out = Vec::new();
                for region in &self.regions {
                    let region_end = region.end_key();
                    if !region_end.is_empty() && region_end <= start_key {
                        continue;
                    }
                    if bounded_end && region.start_key() >= end_key {
                        break;
                    }
                    out.push(region.clone());
                    if limit > 0 && out.len() >= limit as usize {
                        break;
                    }
                    if bounded_end && (region_end.is_empty() || end_key <= region_end) {
                        break;
                    }
                    if region_end.is_empty() {
                        break;
                    }
                }
                Ok(out)
            }

            async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
                Err(Error::Unimplemented)
            }

            async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
                Err(Error::Unimplemented)
            }

            async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
                Err(Error::Unimplemented)
            }

            async fn all_stores(&self) -> Result<Vec<Store>> {
                Err(Error::Unimplemented)
            }

            async fn update_leader(
                &self,
                _ver_id: RegionVerId,
                _leader: metapb::Peer,
            ) -> Result<()> {
                Ok(())
            }

            async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

            async fn invalidate_store_cache(&self, _store_id: StoreId) {}
        }

        fn key(value: u16) -> Vec<u8> {
            value.to_be_bytes().to_vec()
        }

        fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            RegionWithLeader {
                region: metapb::Region {
                    id,
                    start_key,
                    end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                },
                leader: None,
            }
        }

        let mut regions = Vec::new();
        for idx in 0..1025u16 {
            let start_key = if idx == 0 { vec![] } else { key(idx) };
            let end_key = if idx == 1024 { vec![] } else { key(idx + 1) };
            regions.push(region(u64::from(idx) + 1, start_key, end_key));
        }

        let scan_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(ScanPdClient {
            regions,
            scan_calls: scan_calls.clone(),
        });

        let start: Key = key(0).into();
        let end: Key = key(1025).into();
        let stream = executor::block_on_stream(client.regions_for_range((start, end).into()));
        let mut expected_id = 1u64;
        for region in stream {
            let region = region.unwrap();
            assert_eq!(region.id(), expected_id);
            expected_id += 1;
        }

        assert_eq!(expected_id, 1026);
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_group_ranges_by_region() {
        let client = Arc::new(MockPdClient::default());
        let k1 = vec![1];
        let k2 = vec![5, 2];
        let k3 = vec![11, 4];
        let k4 = vec![16, 4];
        let k5 = vec![250, 251];
        let k6 = vec![255, 251];
        let k_split = vec![10];
        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k1.clone(), k3.clone());
        let range3 = make_key_range(k2.clone(), k4.clone());
        let ranges = vec![range1, range2, range3];

        let mut stream = executor::block_on_stream(client.clone().group_ranges_by_region(ranges));
        let ranges1 = stream.next().unwrap().unwrap();
        let ranges2 = stream.next().unwrap().unwrap();
        let ranges3 = stream.next().unwrap().unwrap();
        let ranges4 = stream.next().unwrap().unwrap();

        assert_eq!(ranges1.1.id(), 1);
        assert_eq!(
            ranges1.0,
            vec![
                make_key_range(k1.clone(), k2.clone()),
                make_key_range(k1.clone(), k_split.clone()),
            ]
        );
        assert_eq!(ranges2.1.id(), 2);
        assert_eq!(ranges2.0, vec![make_key_range(k_split.clone(), k3.clone())]);
        assert_eq!(ranges3.1.id(), 1);
        assert_eq!(ranges3.0, vec![make_key_range(k2.clone(), k_split.clone())]);
        assert_eq!(ranges4.1.id(), 2);
        assert_eq!(ranges4.0, vec![make_key_range(k_split, k4.clone())]);
        assert!(stream.next().is_none());

        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k3.clone(), k4.clone());
        let range3 = make_key_range(k5.clone(), k6.clone());
        let ranges = vec![range1, range2, range3];
        stream = executor::block_on_stream(client.group_ranges_by_region(ranges));
        let ranges1 = stream.next().unwrap().unwrap();
        let ranges2 = stream.next().unwrap().unwrap();
        let ranges3 = stream.next().unwrap().unwrap();
        assert_eq!(ranges1.1.id(), 1);
        assert_eq!(ranges1.0, vec![make_key_range(k1, k2)]);
        assert_eq!(ranges2.1.id(), 2);
        assert_eq!(ranges2.0, vec![make_key_range(k3, k4)]);
        assert_eq!(ranges3.1.id(), 3);
        assert_eq!(ranges3.0, vec![make_key_range(k5, k6)]);
    }

    #[tokio::test]
    async fn test_pd_rpc_client_locate_ranges_decode_codec_keys() {
        fn encoded_key(raw: Vec<u8>) -> Vec<u8> {
            Key::from(raw).to_encoded().into()
        }

        fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            RegionWithLeader {
                region: metapb::Region {
                    id,
                    start_key,
                    end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                },
                leader: None,
            }
        }

        let mut client = pd_rpc_client().await;
        client.enable_codec = true;

        client
            .region_cache
            .add_region_with_buckets(
                region(1, vec![], encoded_key(vec![10])),
                Some(metapb::Buckets {
                    region_id: 1,
                    version: 7,
                    keys: vec![vec![], encoded_key(vec![4]), encoded_key(vec![10])],
                    ..Default::default()
                }),
            )
            .await;
        client
            .region_cache
            .add_region_with_buckets(
                region(2, encoded_key(vec![10]), encoded_key(vec![20])),
                Some(metapb::Buckets {
                    region_id: 2,
                    version: 9,
                    keys: vec![
                        encoded_key(vec![10]),
                        encoded_key(vec![15]),
                        encoded_key(vec![20]),
                    ],
                    ..Default::default()
                }),
            )
            .await;

        let locations = client
            .locate_key_range(Key::from(vec![2]), Key::from(vec![18]))
            .await
            .unwrap();
        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].start_key, Key::from(vec![]));
        assert_eq!(locations[0].end_key, Key::from(vec![10]));
        assert_eq!(locations[0].bucket_version(), 7);
        assert_eq!(
            locations[0].locate_bucket(&Key::from(vec![2])),
            Some(crate::BucketLocation {
                start_key: vec![].into(),
                end_key: vec![4].into(),
            })
        );
        assert_eq!(locations[1].start_key, Key::from(vec![10]));
        assert_eq!(locations[1].end_key, Key::from(vec![20]));
        assert_eq!(locations[1].bucket_version(), 9);
        assert_eq!(
            locations[1].locate_bucket(&Key::from(vec![17])),
            Some(crate::BucketLocation {
                start_key: vec![15].into(),
                end_key: vec![20].into(),
            })
        );

        let locations = client
            .batch_locate_key_ranges(vec![
                crate::kv::KeyRange::new(vec![1], vec![3]),
                crate::kv::KeyRange::new(vec![3], vec![8]),
                crate::kv::KeyRange::new(vec![12], vec![18]),
            ])
            .await
            .unwrap();
        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(locations[0].end_key, Key::from(vec![10]));
        assert_eq!(locations[1].start_key, Key::from(vec![10]));
    }

    #[tokio::test]
    async fn test_pd_rpc_client_single_locate_helpers_decode_codec_keys() {
        fn encoded_key(raw: Vec<u8>) -> Vec<u8> {
            Key::from(raw).to_encoded().into()
        }

        fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
            RegionWithLeader {
                region: metapb::Region {
                    id,
                    start_key,
                    end_key,
                    region_epoch: Some(metapb::RegionEpoch::default()),
                    ..Default::default()
                },
                leader: None,
            }
        }

        let mut client = pd_rpc_client().await;
        client.enable_codec = true;

        client
            .region_cache
            .add_region_with_buckets(
                region(1, vec![], encoded_key(vec![10])),
                Some(metapb::Buckets {
                    region_id: 1,
                    version: 7,
                    keys: vec![vec![], encoded_key(vec![4]), encoded_key(vec![10])],
                    ..Default::default()
                }),
            )
            .await;
        client
            .region_cache
            .add_region_with_buckets(
                region(2, encoded_key(vec![10]), encoded_key(vec![20])),
                Some(metapb::Buckets {
                    region_id: 2,
                    version: 9,
                    keys: vec![
                        encoded_key(vec![10]),
                        encoded_key(vec![15]),
                        encoded_key(vec![20]),
                    ],
                    ..Default::default()
                }),
            )
            .await;

        let location = client.locate_key(Key::from(vec![12])).await.unwrap();
        assert_eq!(location.region.id, 2);
        assert_eq!(location.start_key, Key::from(vec![10]));
        assert_eq!(location.end_key, Key::from(vec![20]));
        assert_eq!(location.bucket_version(), 9);

        let try_location = client
            .try_locate_key(Key::from(vec![3]))
            .await
            .unwrap()
            .expect("expected cached region");
        assert_eq!(try_location.region.id, 1);
        assert_eq!(
            try_location.locate_bucket(&Key::from(vec![3])),
            Some(crate::BucketLocation {
                start_key: vec![].into(),
                end_key: vec![4].into(),
            })
        );

        let end_location = client.locate_end_key(Key::from(vec![10])).await.unwrap();
        assert_eq!(end_location.region.id, 1);
        assert_eq!(end_location.end_key, Key::from(vec![10]));

        let region_location = client.locate_region_by_id(2).await.unwrap();
        assert_eq!(region_location.region.id, 2);
        assert_eq!(region_location.start_key, Key::from(vec![10]));
        assert_eq!(region_location.end_key, Key::from(vec![20]));
    }

    #[tokio::test]
    async fn test_get_health_feedback_returns_feedback_and_marks_store_slow_when_score_exceeds_threshold(
    ) {
        let store_id = 7;
        let feedback_seq_no = 42;
        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            let req = req
                .downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                .expect("GetHealthFeedbackRequest");
            assert!(
                req.context.is_some(),
                "GetHealthFeedbackRequest should populate context"
            );

            let mut resp = kvrpcpb::GetHealthFeedbackResponse::default();
            resp.health_feedback = Some(kvrpcpb::HealthFeedback {
                store_id,
                feedback_seq_no,
                slow_score: HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD,
            });
            Ok(Box::new(resp) as Box<dyn Any>)
        }));

        assert!(!client.is_store_slow(store_id));
        let feedback = client.get_health_feedback(store_id).await.unwrap();
        assert_eq!(feedback.store_id, store_id);
        assert_eq!(feedback.feedback_seq_no, feedback_seq_no);
        assert_eq!(feedback.slow_score, HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD);
        assert!(client.is_store_slow(store_id));
    }

    #[tokio::test]
    async fn test_get_health_feedback_does_not_mark_store_slow_when_score_is_normal() {
        let store_id = 11;
        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            req.downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                .expect("GetHealthFeedbackRequest");

            let mut resp = kvrpcpb::GetHealthFeedbackResponse::default();
            resp.health_feedback = Some(kvrpcpb::HealthFeedback {
                store_id,
                feedback_seq_no: 1,
                slow_score: 1,
            });
            Ok(Box::new(resp) as Box<dyn Any>)
        }));

        client.get_health_feedback(store_id).await.unwrap();
        assert!(!client.is_store_slow(store_id));
    }

    #[tokio::test]
    async fn test_get_health_feedback_rejects_missing_health_feedback() {
        let store_id = 1;
        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(|req: &dyn Any| {
            req.downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
                .expect("GetHealthFeedbackRequest");
            Ok(Box::new(kvrpcpb::GetHealthFeedbackResponse::default()) as Box<dyn Any>)
        }));

        let err = client.get_health_feedback(store_id).await.unwrap_err();
        assert!(matches!(err, Error::Unimplemented), "err={err:?}");
    }

    #[tokio::test]
    async fn test_broadcast_txn_status_to_all_stores_sends_to_each_store() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            let req = req
                .downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                .expect("BroadcastTxnStatusRequest");
            calls_captured.fetch_add(1, Ordering::SeqCst);
            assert!(req.context.is_some());
            assert_eq!(req.txn_status.len(), 1);
            assert_eq!(req.txn_status[0].start_ts, 5);
            Ok(Box::new(kvrpcpb::BroadcastTxnStatusResponse::default()) as Box<dyn Any>)
        }));

        client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        client
            .broadcast_txn_status_to_all_stores(vec![kvrpcpb::TxnStatus {
                start_ts: 5,
                min_commit_ts: 6,
                commit_ts: 0,
                rolled_back: false,
                is_completed: false,
            }])
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_broadcast_txn_status_to_all_stores_is_best_effort() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            req.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                .expect("BroadcastTxnStatusRequest");
            let call_index = calls_captured.fetch_add(1, Ordering::SeqCst);
            if call_index == 0 {
                return Err(Error::StringError("injected error".to_owned()));
            }
            Ok(Box::new(kvrpcpb::BroadcastTxnStatusResponse::default()) as Box<dyn Any>)
        }));

        client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        client
            .broadcast_txn_status_to_all_stores(vec![kvrpcpb::TxnStatus {
                start_ts: 5,
                min_commit_ts: 6,
                commit_ts: 0,
                rolled_back: false,
                is_completed: false,
            }])
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_broadcast_txn_status_to_all_stores_empty_status_is_noop() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let client = MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            let _ = req;
            calls_captured.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(kvrpcpb::BroadcastTxnStatusResponse::default()) as Box<dyn Any>)
        }));

        client
            .broadcast_txn_status_to_all_stores(vec![])
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
