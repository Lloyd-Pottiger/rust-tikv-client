// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
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
use crate::region_cache::RegionCache;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::TikvConnect;
use crate::store::{KvClient, Store};
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

    /// In transactional API, `region` is decoded (keys in raw format).
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore>;

    /// In transactional API, the key and returned region are both decoded (keys in raw format).
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader>;

    /// In transactional API, the returned region is decoded (keys in raw format)
    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader>;

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

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64>;

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
        let (start_key, end_key) = range.into_keys();
        stream_fn(Some(start_key), move |start_key| {
            let end_key = end_key.clone();
            let this = self.clone();
            async move {
                let start_key = match start_key {
                    None => return Ok(None),
                    Some(sk) => sk,
                };

                let region = this.region_for_key(&start_key).await?;
                let region_end = region.end_key();
                if end_key
                    .map(|x| x <= region_end && !x.is_empty())
                    .unwrap_or(false)
                    || region_end.is_empty()
                {
                    return Ok(Some((None, region)));
                }
                Ok(Some((Some(region_end), region)))
            }
        })
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
}

/// This client converts requests for the logical TiKV cluster into requests
/// for a single TiKV store using PD and internal logic.
pub struct PdRpcClient<KvC: KvConnect + Send + Sync + 'static = TikvConnect, Cl = Cluster> {
    pd: Arc<RetryClient<Cl>>,
    kv_connect: KvC,
    kv_client_cache: Arc<RwLock<HashMap<String, KvC::KvClient>>>,
    slow_store_until: Mutex<HashMap<StoreId, Instant>>,
    store_estimated_wait_until: Mutex<HashMap<StoreId, Instant>>,
    enable_codec: bool,
    region_cache: RegionCache<RetryClient<Cl>>,
    resolve_lock_lite_threshold: u64,
    pd_http_client: Option<reqwest::Client>,
    pd_http_endpoints: Vec<String>,
    pd_http_use_https: bool,
}

#[async_trait]
impl<KvC: KvConnect + Send + Sync + 'static> PdClient for PdRpcClient<KvC> {
    type KvClient = KvC::KvClient;

    fn resolve_lock_lite_threshold(&self) -> u64 {
        self.resolve_lock_lite_threshold
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        let store_id = region.get_store_id()?;
        let store = self.region_cache.get_store_by_id(store_id).await?;
        let metapb::Store { address, .. } = store;
        let kv_client = self.kv_client(&address).await?;
        Ok(RegionStore::new(region, Arc::new(kv_client), address))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let enable_codec = self.enable_codec;
        let key = if enable_codec {
            key.to_encoded()
        } else {
            key.clone()
        };

        let region = self.region_cache.get_region_by_key(&key).await?;
        Self::decode_region(region, enable_codec)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        let region = self.region_cache.get_region_by_id(id).await?;
        Self::decode_region(region, self.enable_codec)
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        let pb_stores = self.region_cache.read_through_all_stores().await?;
        let mut stores = Vec::with_capacity(pb_stores.len());
        for store in pb_stores {
            let client = self.kv_client(&store.address).await?;
            stores.push(Store::new(store, Arc::new(client)));
        }
        Ok(stores)
    }

    async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
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
        self.region_cache.get_store_by_id(store_id).await
    }

    async fn get_health_feedback(&self, store_id: StoreId) -> Result<kvrpcpb::HealthFeedback> {
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

        let feedback = resp.health_feedback.ok_or_else(|| {
            Error::StringError("GetHealthFeedback response missing health_feedback".to_owned())
        })?;
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
        self.pd.clone().get_timestamp().await
    }

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        self.pd
            .clone()
            .get_timestamp_with_dc_location(dc_location)
            .await
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        self.pd.clone().get_min_ts().await
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.pd.clone().get_external_timestamp().await
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.pd.clone().set_external_timestamp(timestamp).await
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<RegionId>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        if region_ids.is_empty() {
            return Ok(pdpb::ScatterRegionResponse::default());
        }
        self.pd.clone().scatter_regions(region_ids, group).await
    }

    async fn get_operator(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<pdpb::GetOperatorResponse> {
        self.pd.clone().get_operator(region_id).await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        self.pd.clone().update_safepoint(safepoint).await
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

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
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
                    config.grpc_max_decoding_message_size,
                )
            },
            |security_mgr| {
                RetryClient::connect(
                    pd_endpoints,
                    security_mgr,
                    config.timeout,
                    config.tso_max_pending_count,
                )
            },
            enable_codec,
        )
        .await?;
        client.pd_http_endpoints = pd_endpoints.to_vec();
        Ok(client)
    }
}

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl> {
    pub(crate) fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
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
        let resolve_lock_lite_threshold = config.resolve_lock_lite_threshold;
        let (pd_http_client, pd_http_use_https) = build_pd_http_client(&config);
        let security_mgr = Arc::new(
            if let (Some(ca_path), Some(cert_path), Some(key_path)) =
                (&config.ca_path, &config.cert_path, &config.key_path)
            {
                SecurityManager::load(ca_path, cert_path, key_path)?
            } else {
                SecurityManager::default()
            },
        );

        let pd = Arc::new(pd(security_mgr.clone()).await?);
        let kv_client_cache = Default::default();
        Ok(PdRpcClient {
            pd: pd.clone(),
            kv_client_cache,
            kv_connect: kv_connect(security_mgr),
            slow_store_until: Mutex::new(HashMap::new()),
            store_estimated_wait_until: Mutex::new(HashMap::new()),
            enable_codec,
            region_cache: RegionCache::new(pd),
            resolve_lock_lite_threshold,
            pd_http_client,
            pd_http_endpoints: Vec::new(),
            pd_http_use_https,
        })
    }

    async fn kv_client(&self, address: &str) -> Result<KvC::KvClient> {
        if let Some(client) = self.kv_client_cache.read().await.get(address) {
            return Ok(client.clone());
        };
        info!("connect to tikv endpoint: {:?}", address);
        match self.kv_connect.connect(address).await {
            Ok(client) => {
                self.kv_client_cache
                    .write()
                    .await
                    .insert(address.to_owned(), client.clone());
                Ok(client)
            }
            Err(e) => Err(e),
        }
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
    let mut builder = reqwest::Client::builder().timeout(config.timeout);

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
                last_err = Some(Error::StringError(format!(
                    "pd http min-resolved-ts request to {endpoint} failed: {err}"
                )));
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
        assert!(matches!(err, Error::StringError(_)), "err={err:?}");
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
