// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use core::ops::Range;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use log::debug;

use crate::backoff::{DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
use crate::common::Error;
use crate::config::Config;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::kvrpcpb::{RawScanRequest, RawScanResponse};
use crate::proto::metapb;
use crate::raw::lowering::*;
use crate::request::CollectSingle;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::request::TruncateKeyspace;
use crate::request::{plan, Collect};
use crate::safe_ts::SafeTsCache;
use crate::store::Request as StoreRequest;
use crate::store::{HasRegionError, RegionStore};
use crate::Backoff;
use crate::BoundRange;
use crate::ColumnFamily;
use crate::Error::RegionError;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::TraceControlFlags;
use crate::Value;

const MAX_RAW_KV_SCAN_LIMIT: u32 = 10240;

/// The TiKV raw `Client` is used to interact with TiKV using raw requests.
///
/// Raw requests don't need a wrapping transaction.
/// Each request is immediately processed once executed.
///
/// The returned results of raw request methods are [`Future`](std::future::Future)s that must be
/// awaited to execute.
pub struct Client<PdC: PdClient = PdRpcClient> {
    rpc: Arc<PdC>,
    cf: Option<ColumnFamily>,
    backoff: Backoff,
    /// Whether to use the [`atomic mode`](Client::with_atomic_for_cas).
    atomic: bool,
    trace_id: Option<Vec<u8>>,
    trace_control_flags: TraceControlFlags,
    keyspace: Keyspace,
    safe_ts: SafeTsCache<PdC>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            trace_id: self.trace_id.clone(),
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }
}

impl Client<PdRpcClient> {
    /// Create a raw [`Client`] and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// By default, this uses the global client configuration returned by
    /// [`config::get_global_config`](crate::config::get_global_config). Use
    /// [`RawClient::new_with_config`](crate::RawClient::new_with_config) to provide a per-client
    /// [`Config`](crate::Config).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::RawClient;
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, crate::config::get_global_config()).await
    }

    /// Create a raw [`Client`] with a custom configuration, and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient};
    /// # use futures::prelude::*;
    /// # use std::time::Duration;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new_with_config(
    ///     vec!["192.168.0.100"],
    ///     Config::default().with_timeout(Duration::from_secs(60)),
    /// )
    /// .await
    /// .unwrap();
    /// # });
    /// ```
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        let enable_codec = config.keyspace.is_some();
        let enable_region_cache_preload = config.enable_region_cache_preload;
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let health_feedback_update_interval = config.health_feedback_update_interval;
        let store_liveness_update_interval = config.store_liveness_update_interval;
        let store_liveness_timeout = config.store_liveness_timeout;
        let rpc =
            Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), enable_codec).await?);
        rpc.install_health_feedback_observer();
        crate::pd::spawn_health_feedback_updater(rpc.clone(), health_feedback_update_interval);
        crate::pd::spawn_store_liveness_updater(
            rpc.clone(),
            store_liveness_update_interval,
            store_liveness_timeout,
        );
        let keyspace = match config.keyspace {
            Some(name) => {
                let keyspace = rpc.load_keyspace(&name).await?;
                Keyspace::Enable {
                    keyspace_id: keyspace.id,
                }
            }
            None => Keyspace::Disable,
        };
        if enable_region_cache_preload {
            let (start_key, end_key) = keyspace.prefix_range(KeyMode::Raw);
            rpc.clone().spawn_region_cache_preload(start_key, end_key);
        }
        Ok(Client {
            safe_ts: SafeTsCache::new(rpc.clone(), keyspace),
            rpc,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace,
        })
    }

    /// Returns the TiKV cluster ID.
    pub fn cluster_id(&self) -> u64 {
        self.rpc.cluster_id()
    }

    /// Close cached gRPC connections to a TiKV store address.
    ///
    /// The client will reconnect the next time that address is used.
    pub async fn close_addr(&self, address: &str) -> bool {
        self.rpc.close_addr(address).await
    }

    /// Create a new client which is a clone of `self`, but which uses an explicit column family for
    /// all requests.
    ///
    /// This function returns a new `Client`; requests created with the new client will use the
    /// supplied column family. The original `Client` can still be used (without the new
    /// column family).
    ///
    /// By default, raw clients use the `Default` column family.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient, ColumnFamily};
    /// # use futures::prelude::*;
    /// # use std::convert::TryInto;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"])
    ///     .await
    ///     .unwrap()
    ///     .with_cf(ColumnFamily::Write);
    /// // Fetch a value at "foo" from the Write CF.
    /// let get_request = client.get("foo".to_owned());
    /// # });
    /// ```
    #[must_use]
    pub fn with_cf(&self, cf: ColumnFamily) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: Some(cf),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            trace_id: self.trace_id.clone(),
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }

    /// Set the [`Backoff`] strategy for retrying requests.
    /// The default strategy is [`DEFAULT_REGION_BACKOFF`](crate::backoff::DEFAULT_REGION_BACKOFF).
    /// See [`Backoff`] for more information.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient, ColumnFamily};
    /// # use tikv_client::backoff::DEFAULT_REGION_BACKOFF;
    /// # use futures::prelude::*;
    /// # use std::convert::TryInto;
    /// # futures::executor::block_on(async {
    /// let client = RawClient::new(vec!["192.168.0.100"])
    ///     .await
    ///     .unwrap()
    ///     .with_backoff(DEFAULT_REGION_BACKOFF);
    /// // Fetch a value at "foo" from the Write CF.
    /// let get_request = client.get("foo".to_owned());
    /// # });
    /// ```
    #[must_use]
    pub fn with_backoff(&self, backoff: Backoff) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff,
            atomic: self.atomic,
            trace_id: self.trace_id.clone(),
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Closes this client and releases cached resources.
    ///
    /// This is idempotent. After calling `close`, subsequent requests return an error instead of
    /// silently reconnecting.
    pub async fn close(&self) {
        self.safe_ts.close().await;
        self.rpc.close().await;
    }

    /// Returns whether this client has been explicitly closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.rpc.is_closed()
    }

    fn apply_request_context(&self, ctx: &mut crate::proto::kvrpcpb::Context) {
        if ctx.request_source.is_empty() {
            if let Some(request_source) = crate::request_context::request_source() {
                ctx.request_source = request_source;
            }
        }
        if ctx.resource_control_context.is_none() {
            ctx.resource_control_context =
                crate::request_context::resource_group_name().map(|resource_group_name| {
                    crate::proto::kvrpcpb::ResourceControlContext {
                        resource_group_name,
                        ..Default::default()
                    }
                });
        }
        ctx.trace_id = self
            .trace_id
            .clone()
            .or_else(crate::trace::trace_id)
            .unwrap_or_default();
        ctx.trace_control_flags =
            crate::trace::effective_trace_control_flags(self.trace_control_flags).bits();
    }

    fn apply_request_context_to_request<R: StoreRequest>(&self, request: &mut R) {
        if let Some(ctx) = request.context_mut() {
            self.apply_request_context(ctx);
        }
    }

    /// Create a new client which is a clone of `self`, but which uses the given trace id for all
    /// requests.
    ///
    /// This option writes to `kvrpcpb::Context.trace_id`.
    #[must_use]
    pub fn with_trace_id(&self, trace_id: Vec<u8>) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            trace_id: Some(trace_id),
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }

    /// Create a new client which is a clone of `self`, but with no configured trace id.
    #[must_use]
    pub fn without_trace_id(&self) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            trace_id: None,
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }

    /// Create a new client which is a clone of `self`, but which uses the given trace control
    /// flags for all requests.
    ///
    /// This option writes to `kvrpcpb::Context.trace_control_flags`.
    #[must_use]
    pub fn with_trace_control_flags(&self, flags: TraceControlFlags) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            trace_id: self.trace_id.clone(),
            trace_control_flags: flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }

    /// Returns a handle to the underlying PD client.
    #[must_use]
    pub fn pd_client(&self) -> Arc<PdC> {
        self.rpc.clone()
    }

    /// Get the cluster-wide minimum `safe_ts` across all TiKV stores (and TiFlash stores, if
    /// present).
    ///
    /// This value is a best-effort signal used by stale reads: if it is non-zero, reads at
    /// timestamps less than or equal to the returned `safe_ts` can be served from replicas (subject
    /// to per-region `safe_ts`).
    ///
    /// Returns `0` when the minimum safe-ts cannot be determined (for example, if it has not been
    /// successfully refreshed yet).
    ///
    /// The returned value is cached and best-effort: once a non-zero safe-ts has been observed for
    /// all stores, transient store errors will not cause the returned minimum safe-ts to drop back
    /// to `0`.
    pub async fn min_safe_ts(&self) -> Result<u64> {
        self.safe_ts.min_safe_ts().await
    }

    /// Set to use the atomic mode.
    ///
    /// The only reason of using atomic mode is the
    /// [`compare_and_swap`](Client::compare_and_swap) operation. To guarantee
    /// the atomicity of CAS, write operations like [`put`](Client::put) or
    /// [`delete`](Client::delete) in atomic mode are more expensive. Some
    /// operations are not supported in the mode.
    #[must_use]
    pub fn with_atomic_for_cas(&self) -> Self {
        Client {
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: true,
            trace_id: self.trace_id.clone(),
            trace_control_flags: self.trace_control_flags,
            keyspace: self.keyspace,
            safe_ts: self.safe_ts.clone(),
        }
    }

    /// Get the minimum `safe_ts` for a transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this behaves the same as [`Client::min_safe_ts`].
    /// Otherwise, only stores whose `zone` label matches `txn_scope` are considered, matching
    /// client-go `GetMinSafeTS(txnScope)` behavior.
    ///
    /// Returns `0` when the minimum safe-ts cannot be determined, or when no stores match the
    /// provided scope.
    ///
    /// This method uses the same cached best-effort semantics as [`Client::min_safe_ts`].
    pub async fn min_safe_ts_with_txn_scope(&self, txn_scope: impl AsRef<str>) -> Result<u64> {
        self.safe_ts
            .min_safe_ts_with_txn_scope(txn_scope.as_ref())
            .await
    }

    /// Create a new 'get' request.
    ///
    /// Once resolved this request will result in the fetching of the value associated with the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.get(key);
    /// let result: Option<Value> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking raw get request");
        let start = Instant::now();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut request = new_raw_get_request(key, self.cf.clone());
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        let result = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("get", start.elapsed());
        result
    }

    /// Create a new 'batch get' request.
    ///
    /// Once resolved this request will result in the fetching of the values associated with the
    /// given keys.
    ///
    /// Non-existent entries will not appear in the result. The order of the keys is not retained in the result.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{KvPair, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let req = client.batch_get(keys);
    /// let result: Vec<KvPair> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw batch_get request");
        let start = Instant::now();
        let keys = keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let mut request = new_raw_batch_get_request(keys, self.cf.clone());
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(Collect)
            .plan();
        let result = plan.execute().await.map(|r| {
            r.into_iter()
                .map(|pair| pair.truncate_keyspace(self.keyspace))
                .collect()
        });
        crate::stats::observe_rawkv_cmd_seconds("batch_get", start.elapsed());
        result
    }

    /// Create a new 'batch get values' request.
    ///
    /// This is similar to [`Client::batch_get`], but preserves the input key order and includes a
    /// placeholder for missing keys.
    ///
    /// The returned `Vec` has the same length as the input keys. Missing keys are represented as
    /// `None`.
    pub async fn batch_get_values(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<Option<Value>>> {
        debug!("invoking raw batch_get_values request");

        let keys: Vec<Key> = keys.into_iter().map(Into::into).collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let start = Instant::now();

        let keyspace = self.keyspace;
        let request_keys = keys
            .iter()
            .cloned()
            .map(|key| key.encode_keyspace(keyspace, KeyMode::Raw));

        let mut request = new_raw_batch_get_request(request_keys, self.cf.clone());
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(Collect)
            .plan();

        let mut values_by_key = HashMap::with_capacity(keys.len());
        let pairs = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("batch_get", start.elapsed());
        for pair in pairs? {
            let KvPair(key, value) = pair.truncate_keyspace(keyspace);
            values_by_key.insert(key, value);
        }

        Ok(keys
            .into_iter()
            .map(|key| values_by_key.get(&key).cloned())
            .collect())
    }

    /// Create a new 'get key ttl' request.
    ///
    /// Once resolved this request will result in the fetching of the alive time left for the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// # use tikv_client::{Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.get_key_ttl_secs(key);
    /// let result: `Option<u64>` = req.await.unwrap();
    /// # });
    pub async fn get_key_ttl_secs(&self, key: impl Into<Key>) -> Result<Option<u64>> {
        debug!("invoking raw get_key_ttl_secs request");
        let key: Key = key.into();
        crate::stats::observe_rawkv_kv_size_bytes("key", key.0.len());
        let key = key.encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut request = new_raw_get_key_ttl_request(key, self.cf.clone());
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await
    }

    /// Create a new 'put' request.
    ///
    /// Once resolved this request will result in the setting of the value associated with the given key.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let val = "TiKV".to_owned();
    /// let req = client.put(key, val);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.put_with_ttl(key, value, 0).await
    }

    /// Create a new 'put' request with an explicit TTL in seconds.
    ///
    /// Once resolved this request stores `value` at `key` and asks TiKV to expire the entry after
    /// `ttl_secs` seconds. Passing `0` keeps the entry without an expiration.
    ///
    /// If this client was created through [`with_atomic_for_cas`](Client::with_atomic_for_cas),
    /// the request also uses TiKV's atomic/CAS write path.
    pub async fn put_with_ttl(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl_secs: u64,
    ) -> Result<()> {
        debug!("invoking raw put request");
        let start = Instant::now();
        let key: Key = key.into();
        let value: Value = value.into();
        crate::stats::observe_rawkv_kv_size_bytes("key", key.0.len());
        crate::stats::observe_rawkv_kv_size_bytes("value", value.len());
        let key = key.encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut request = new_raw_put_request(key, value, self.cf.clone(), ttl_secs, self.atomic);
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        let result = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("batch_put", start.elapsed());
        result?;
        Ok(())
    }

    /// Create a new 'batch put' request.
    ///
    /// Once resolved this request will result in the setting of the values associated with the given keys.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Result, KvPair, Key, Value, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let kvpair1 = ("PD".to_owned(), "Go".to_owned());
    /// let kvpair2 = ("TiKV".to_owned(), "Rust".to_owned());
    /// let iterable = vec![kvpair1, kvpair2];
    /// let req = client.batch_put(iterable);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_put(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
    ) -> Result<()> {
        self.batch_put_with_ttl(pairs, std::iter::repeat(0)).await
    }

    /// Create a new 'batch put' request with explicit TTLs in seconds.
    ///
    /// `ttls` follows the same contract as TiKV's raw batch-put API: an empty iterator stores all
    /// pairs without TTL, a single TTL is reused for every pair, and otherwise the iterator must
    /// yield one TTL per pair. Any mismatch is rejected before the request is dispatched.
    ///
    /// If this client was created through [`with_atomic_for_cas`](Client::with_atomic_for_cas),
    /// the request also uses TiKV's atomic/CAS write path.
    pub async fn batch_put_with_ttl(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
        ttls: impl IntoIterator<Item = u64>,
    ) -> Result<()> {
        debug!("invoking raw batch_put request");
        let start = Instant::now();
        let pairs = pairs
            .into_iter()
            .map(|pair| pair.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let mut request =
            new_raw_batch_put_request(pairs, ttls.into_iter(), self.cf.clone(), self.atomic);
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .extract_error()
            .plan();
        let result = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("batch_put", start.elapsed());
        result?;
        Ok(())
    }

    /// Create a new 'delete' request.
    ///
    /// Once resolved this request will result in the deletion of the given key.
    ///
    /// It does not return an error if the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let req = client.delete(key);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&self, key: impl Into<Key>) -> Result<()> {
        debug!("invoking raw delete request");
        let start = Instant::now();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut request = new_raw_delete_request(key, self.cf.clone(), self.atomic);
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        let result = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("delete", start.elapsed());
        result?;
        Ok(())
    }

    /// Create a new 'batch delete' request.
    ///
    /// Once resolved this request will result in the deletion of the given keys.
    ///
    /// It does not return an error if some of the keys do not exist and will delete the others.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Config, RawClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let req = client.batch_delete(keys);
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn batch_delete(&self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<()> {
        debug!("invoking raw batch_delete request");
        let start = Instant::now();
        let result: Result<()> = async {
            self.assert_non_atomic()?;
            let keys = keys
                .into_iter()
                .map(|k| k.into().encode_keyspace(self.keyspace, KeyMode::Raw));
            let mut request = new_raw_batch_delete_request(keys, self.cf.clone());
            self.apply_request_context_to_request(&mut request);
            let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
                .retry_multi_region(self.backoff.clone())
                .extract_error()
                .plan();
            plan.execute().await?;
            Ok(())
        }
        .await;
        crate::stats::observe_rawkv_cmd_seconds("batch_delete", start.elapsed());
        result
    }

    /// Create a new 'delete range' request.
    ///
    /// Once resolved this request will result in the deletion of all keys lying in the given range.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.delete_range(inclusive_range.into_owned());
    /// let result: () = req.await.unwrap();
    /// # });
    /// ```
    pub async fn delete_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        debug!("invoking raw delete_range request");
        let start = Instant::now();
        let result: Result<()> = async {
            self.assert_non_atomic()?;
            let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
            let mut request = new_raw_delete_range_request(range, self.cf.clone());
            self.apply_request_context_to_request(&mut request);
            let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
                .retry_multi_region(self.backoff.clone())
                .extract_error()
                .plan();
            plan.execute().await?;
            Ok(())
        }
        .await;

        let label = if result.is_ok() {
            "delete_range"
        } else {
            "delete_range_error"
        };
        crate::stats::observe_rawkv_cmd_seconds(label, start.elapsed());
        result
    }

    /// Create a new 'scan' request.
    ///
    /// Once resolved this request will result in a `Vec` of key-value pairs that lies in the specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{KvPair, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan(inclusive_range.into_owned(), 2);
    /// let result: Vec<KvPair> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan(&self, range: impl Into<BoundRange>, limit: u32) -> Result<Vec<KvPair>> {
        debug!("invoking raw scan request");
        self.scan_inner(range.into(), limit, false, false).await
    }

    /// Create a new 'scan' request but scans in "reverse" direction.
    ///
    /// Once resolved this request will result in a `Vec` of key-value pairs that lies in the specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// Reverse Scan queries continuous kv pairs in range [endKey, startKey),
    /// from startKey(upperBound) to endKey(lowerBound) in reverse order, up to limit pairs.
    /// The returned keys are in reversed lexicographical order.
    /// If you want to include the startKey or exclude the endKey, push a '\0' to the key.
    /// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{KvPair, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_reverse(inclusive_range.into_owned(), 2);
    /// let result: Vec<KvPair> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw reverse scan request");
        self.scan_inner(range.into(), limit, false, true).await
    }

    /// Create a new 'scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_keys(inclusive_range.into_owned(), 2);
    /// let result: Vec<Key> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys(&self, range: impl Into<BoundRange>, limit: u32) -> Result<Vec<Key>> {
        debug!("invoking raw scan_keys request");
        Ok(self
            .scan_inner(range, limit, true, false)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new 'scan' request that only returns the keys in reverse order.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by the key.
    ///
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let req = client.scan_keys_reverse(inclusive_range.into_owned(), 2);
    /// let result: Vec<Key> = req.await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<Vec<Key>> {
        debug!("invoking raw scan_keys_reverse request");
        Ok(self
            .scan_inner(range, limit, true, true)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Do checksum of continuous kv pairs in range [startKey, endKey).
    ///
    /// If endKey is empty, it means unbounded.
    ///
    /// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
    /// (startKey, endKey], you can write:
    /// `checksum(push(startKey, '\0'), push(endKey, '\0'))`.
    pub async fn checksum(&self, range: impl Into<BoundRange>) -> Result<super::RawChecksum> {
        debug!("invoking raw checksum request");
        let start = Instant::now();
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut request = new_raw_checksum_request(range);
        self.apply_request_context_to_request(&mut request);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .retry_multi_region(self.backoff.clone())
            .extract_error()
            .merge(Collect)
            .plan();
        let result = plan.execute().await;
        crate::stats::observe_rawkv_cmd_seconds("raw_checksum", start.elapsed());
        result
    }

    /// Create a new 'batch scan' request.
    ///
    /// Once resolved this request will result in a set of scanners over the given keys.
    ///
    /// **Warning**: This method is experimental.
    ///
    /// `each_limit` limits the maximum number of returned key-value pairs for **each input range**
    /// (including when a range spans multiple regions). Results are concatenated in the same order
    /// as the input ranges.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let req = client.batch_scan(iterable, 2);
    /// let result = req.await;
    /// # });
    /// ```
    pub async fn batch_scan(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw batch_scan request");
        self.batch_scan_inner(ranges, each_limit, false).await
    }

    /// Create a new 'batch scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a set of scanners over the given keys.
    ///
    /// **Warning**: This method is experimental.
    ///
    /// `each_limit` limits the maximum number of returned keys for **each input range** (including
    /// when a range spans multiple regions). Results are concatenated in the same order as the
    /// input ranges.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, RawClient, IntoOwnedRange};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let req = client.batch_scan_keys(iterable, 2);
    /// let result = req.await;
    /// # });
    /// ```
    pub async fn batch_scan_keys(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
    ) -> Result<Vec<Key>> {
        debug!("invoking raw batch_scan_keys request");
        Ok(self
            .batch_scan_inner(ranges, each_limit, true)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new *atomic* 'compare and set' request.
    ///
    /// Once resolved this request will result in an atomic `compare and set'
    /// operation for the given key.
    ///
    /// If the value retrived is equal to `current_value`, `new_value` is
    /// written.
    ///
    /// # Return Value
    ///
    /// A tuple is returned if successful: the previous value and whether the
    /// value is swapped
    pub async fn compare_and_swap(
        &self,
        key: impl Into<Key>,
        previous_value: impl Into<Option<Value>>,
        new_value: impl Into<Value>,
    ) -> Result<(Option<Value>, bool)> {
        debug!("invoking raw compare_and_swap request");
        self.assert_atomic()?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut req = new_cas_request(
            key,
            new_value.into(),
            previous_value.into(),
            self.cf.clone(),
        );
        self.apply_request_context_to_request(&mut req);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await
    }

    /// Executes a raw coprocessor request over one or more raw-key ranges.
    ///
    /// `copr_name` and `copr_version_req` identify the TiKV coprocessor plugin to run. The version
    /// requirement must parse as a [`semver::VersionReq`].
    ///
    /// `request_builder` is invoked once per region shard with that region's metadata and the
    /// shard-local ranges in this client's keyspace view (that is, any internal keyspace prefix
    /// has already been removed). The callback must return the serialized request payload for that
    /// shard.
    ///
    /// The result keeps the same shard-local ranges paired with each response payload.
    pub async fn coprocessor(
        &self,
        copr_name: impl Into<String>,
        copr_version_req: impl Into<String>,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        request_builder: impl Fn(metapb::Region, Vec<Range<Key>>) -> Vec<u8> + Send + Sync + 'static,
    ) -> Result<Vec<(Vec<Range<Key>>, Vec<u8>)>> {
        let copr_version_req = copr_version_req.into();
        semver::VersionReq::from_str(&copr_version_req)?;
        let ranges = ranges
            .into_iter()
            .map(|range| range.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let keyspace = self.keyspace;
        let request_builder = move |region, ranges: Vec<Range<Key>>| {
            request_builder(
                region,
                ranges
                    .into_iter()
                    .map(|range| range.truncate_keyspace(keyspace))
                    .collect(),
            )
        };
        let mut req = new_raw_coprocessor_request(
            copr_name.into(),
            copr_version_req,
            ranges,
            request_builder,
        );
        self.apply_request_context_to_request(&mut req);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .preserve_shard()
            .retry_multi_region(self.backoff.clone())
            .post_process_default()
            .plan();
        Ok(plan
            .execute()
            .await?
            .into_iter()
            .map(|(ranges, data)| (ranges.truncate_keyspace(keyspace), data))
            .collect())
    }

    async fn scan_inner(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
        key_only: bool,
        reverse: bool,
    ) -> Result<Vec<KvPair>> {
        let label = if reverse {
            "raw_reverse_scan"
        } else {
            "raw_scan"
        };
        let start = Instant::now();
        let result: Result<Vec<KvPair>> = async {
            if limit > MAX_RAW_KV_SCAN_LIMIT {
                return Err(Error::MaxScanLimitExceeded {
                    limit,
                    max_limit: MAX_RAW_KV_SCAN_LIMIT,
                });
            }
            if limit == 0 {
                return Ok(Vec::new());
            }
            let backoff = DEFAULT_STORE_BACKOFF;
            let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
            let (range_start, range_end) = range.into_keys();

            // An empty end key means "unbounded" in TiKV APIs, so normalize it to `None` to avoid
            // prematurely terminating multi-region scans.
            let range_end = range_end.filter(|key| !key.is_empty());

            if let Some(range_end) = range_end.as_ref() {
                if &range_start >= range_end {
                    return Ok(Vec::new());
                }
            }

            let mut result = Vec::new();
            let mut remaining = limit;

            if reverse {
                // Reverse scan requires an explicit upper bound because locating the last region is
                // not implemented.
                let Some(mut current_upper) = range_end.clone() else {
                    return Err(Error::Unimplemented);
                };
                if current_upper.is_empty() {
                    return Err(Error::Unimplemented);
                }

                while remaining > 0 && current_upper > range_start {
                    let scan_args = ScanInnerArgs {
                        from_key: range_start.clone(),
                        to_key: Some(current_upper.clone()),
                        limit: remaining,
                        key_only,
                        reverse,
                        backoff: backoff.clone(),
                    };
                    let (res, next_upper) = self.retryable_scan(scan_args).await?;

                    let mut kvs = res
                        .map(|r| r.kvs.into_iter().map(Into::into).collect::<Vec<KvPair>>())
                        .unwrap_or_default();
                    if !kvs.is_empty() {
                        remaining -= kvs.len() as u32;
                        result.append(&mut kvs);
                    }

                    if next_upper.is_empty() || next_upper <= range_start {
                        break;
                    }
                    if next_upper >= current_upper {
                        return Err(Error::StringError(
                            "raw reverse scan returned a non-advancing range".to_owned(),
                        ));
                    }
                    current_upper = next_upper;
                }
            } else {
                let mut current_start = range_start;
                while remaining > 0 {
                    let scan_args = ScanInnerArgs {
                        from_key: current_start.clone(),
                        to_key: range_end.clone(),
                        limit: remaining,
                        key_only,
                        reverse,
                        backoff: backoff.clone(),
                    };
                    let (res, next_start) = self.retryable_scan(scan_args).await?;

                    let mut kvs = res
                        .map(|r| r.kvs.into_iter().map(Into::into).collect::<Vec<KvPair>>())
                        .unwrap_or_default();
                    if !kvs.is_empty() {
                        remaining -= kvs.len() as u32;
                        result.append(&mut kvs);
                    }

                    if next_start.is_empty()
                        || range_end
                            .as_ref()
                            .is_some_and(|range_end| range_end <= &next_start)
                    {
                        break;
                    }
                    if next_start <= current_start {
                        return Err(Error::StringError(
                            "raw scan returned a non-advancing range".to_owned(),
                        ));
                    }
                    current_start = next_start;
                }
            }

            // limit is a soft limit, so we need check the number of results
            result.truncate(limit as usize);

            // truncate the prefix of keys
            let result = result.truncate_keyspace(self.keyspace);

            Ok(result)
        }
        .await;
        crate::stats::observe_rawkv_cmd_seconds(label, start.elapsed());
        result
    }

    async fn retryable_scan(
        &self,
        mut scan_args: ScanInnerArgs,
    ) -> Result<(Option<RawScanResponse>, Key)> {
        let from_key = scan_args.from_key;
        let to_key = scan_args.to_key;
        loop {
            let region = if scan_args.reverse {
                let Some(to_key) = to_key.as_ref() else {
                    return Err(Error::Unimplemented);
                };
                self.rpc.clone().region_for_end_key(to_key).await?
            } else {
                self.rpc.clone().region_for_key(&from_key).await?
            };
            let store = self.rpc.clone().store_for_id(region.id()).await?;
            let mut request = new_raw_scan_request(
                (from_key.clone(), to_key.clone()).into(),
                scan_args.limit,
                scan_args.key_only,
                scan_args.reverse,
                self.cf.clone(),
            );
            self.apply_request_context_to_request(&mut request);
            let resp = self.do_store_scan(store.clone(), request).await;
            return match resp {
                Ok(mut r) => {
                    if let Some(err) = r.region_error() {
                        let status =
                            plan::handle_region_error(self.rpc.clone(), err.clone(), store.clone())
                                .await?;
                        if status {
                            continue;
                        } else if let Some(duration) = scan_args.backoff.next_delay_duration() {
                            crate::util::sleep_backoff(duration).await;
                            continue;
                        } else {
                            return Err(RegionError(Box::new(err)));
                        }
                    }
                    let next_key = if scan_args.reverse {
                        region.start_key()
                    } else {
                        region.end_key()
                    };
                    Ok((Some(r), next_key))
                }
                Err(err) => Err(err),
            };
        }
    }

    async fn do_store_scan(
        &self,
        store: RegionStore,
        scan_request: RawScanRequest,
    ) -> Result<RawScanResponse> {
        crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, scan_request)
            .single_region_with_store(store.clone())
            .await?
            .plan()
            .execute()
            .await
    }

    async fn batch_scan_inner(
        &self,
        ranges: impl IntoIterator<Item = impl Into<BoundRange>>,
        each_limit: u32,
        key_only: bool,
    ) -> Result<Vec<KvPair>> {
        if each_limit > MAX_RAW_KV_SCAN_LIMIT {
            return Err(Error::MaxScanLimitExceeded {
                limit: each_limit,
                max_limit: MAX_RAW_KV_SCAN_LIMIT,
            });
        }

        if each_limit == 0 {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        for range in ranges {
            let mut scanned = self.scan_inner(range, each_limit, key_only, false).await?;
            result.append(&mut scanned);
        }
        Ok(result)
    }

    fn assert_non_atomic(&self) -> Result<()> {
        if !self.atomic {
            Ok(())
        } else {
            Err(Error::UnsupportedMode)
        }
    }

    fn assert_atomic(&self) -> Result<()> {
        if self.atomic {
            Ok(())
        } else {
            Err(Error::UnsupportedMode)
        }
    }
}

#[derive(Clone)]
struct ScanInnerArgs {
    from_key: Key,
    to_key: Option<Key>,
    limit: u32,
    key_only: bool,
    reverse: bool,
    backoff: Backoff,
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::Result;

    fn new_mock_raw_client_with_scan_data(
        data: Arc<Vec<(Vec<u8>, Vec<u8>)>>,
    ) -> Client<MockPdClient> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook({
            let data = data.clone();
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::RawBatchScanRequest>().is_some() {
                    panic!("unexpected raw batch scan request");
                }

                let req = req
                    .downcast_ref::<kvrpcpb::RawScanRequest>()
                    .expect("expected raw scan request");

                let region_id = req.context.as_ref().map(|ctx| ctx.region_id);
                let (region_start, region_end) = match region_id {
                    Some(1) => (Vec::new(), vec![10]),
                    Some(2) => (vec![10], vec![250, 250]),
                    Some(3) => (vec![250, 250], Vec::new()),
                    Some(other) => panic!("unexpected region id: {other}"),
                    None => (Vec::new(), Vec::new()),
                };

                let (range_start, range_end) = if req.reverse {
                    (req.end_key.clone(), req.start_key.clone())
                } else {
                    (req.start_key.clone(), req.end_key.clone())
                };

                let effective_start = std::cmp::max(range_start, region_start);
                let effective_end = match (range_end.is_empty(), region_end.is_empty()) {
                    (true, true) => Vec::new(),
                    (true, false) => region_end,
                    (false, true) => range_end,
                    (false, false) => std::cmp::min(range_end, region_end),
                };

                let kvs = if req.reverse {
                    data.iter()
                        .rev()
                        .filter(|(key, _)| key.as_slice() >= effective_start.as_slice())
                        .filter(|(key, _)| {
                            effective_end.is_empty() || key.as_slice() < effective_end.as_slice()
                        })
                        .take(req.limit as usize)
                        .map(|(key, value)| kvrpcpb::KvPair {
                            error: None,
                            key: key.clone(),
                            value: if req.key_only {
                                Vec::new()
                            } else {
                                value.clone()
                            },
                            commit_ts: 0,
                        })
                        .collect()
                } else {
                    data.iter()
                        .filter(|(key, _)| key.as_slice() >= effective_start.as_slice())
                        .filter(|(key, _)| {
                            effective_end.is_empty() || key.as_slice() < effective_end.as_slice()
                        })
                        .take(req.limit as usize)
                        .map(|(key, value)| kvrpcpb::KvPair {
                            error: None,
                            key: key.clone(),
                            value: if req.key_only {
                                Vec::new()
                            } else {
                                value.clone()
                            },
                            commit_ts: 0,
                        })
                        .collect()
                };

                let resp = kvrpcpb::RawScanResponse {
                    region_error: None,
                    kvs,
                };
                Ok(Box::new(resp) as Box<dyn Any>)
            }
        })));

        Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
    }

    #[tokio::test]
    async fn test_raw_client_close_is_idempotent_and_blocks_future_requests() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert!(!client.is_closed());

        client.close().await;
        client.close().await;

        assert!(client.is_closed());

        let err = client.get(vec![1]).await.unwrap_err();
        assert!(matches!(err, Error::StringError(msg) if msg == "client is closed"));
    }

    #[tokio::test]
    async fn test_batch_scan_each_limit_is_per_range_across_regions() -> Result<()> {
        let data: Arc<Vec<(Vec<u8>, Vec<u8>)>> = Arc::new(vec![
            (vec![1], b"v1".to_vec()),
            (vec![2], b"v2".to_vec()),
            (vec![10], b"v10".to_vec()),
            (vec![11], b"v11".to_vec()),
            (vec![12], b"v12".to_vec()),
        ]);
        let client = new_mock_raw_client_with_scan_data(data);

        let res = client.batch_scan(vec![.., ..], 3).await?;
        let keys: Vec<Vec<u8>> = res.into_iter().map(|pair| pair.0.into()).collect();
        assert_eq!(
            keys,
            vec![vec![1], vec![2], vec![10], vec![1], vec![2], vec![10]]
        );

        let keys = client.batch_scan_keys(vec![.., ..], 3).await?;
        let keys: Vec<Vec<u8>> = keys.into_iter().map(Into::into).collect();
        assert_eq!(
            keys,
            vec![vec![1], vec![2], vec![10], vec![1], vec![2], vec![10]]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_unbounded_range_terminates_at_last_region() -> Result<()> {
        let client = new_mock_raw_client_with_scan_data(Arc::new(vec![
            (vec![1], b"v1".to_vec()),
            (vec![2], b"v2".to_vec()),
            (vec![10], b"v10".to_vec()),
            (vec![11], b"v11".to_vec()),
            (vec![12], b"v12".to_vec()),
        ]));

        let res = tokio::time::timeout(Duration::from_secs(1), client.scan(.., 10))
            .await
            .expect("scan should not hang")?;
        let keys: Vec<Vec<u8>> = res.into_iter().map(|pair| pair.0.into()).collect();
        assert_eq!(keys, vec![vec![1], vec![2], vec![10], vec![11], vec![12]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_reverse_across_regions_returns_descending_keys() -> Result<()> {
        let client = new_mock_raw_client_with_scan_data(Arc::new(vec![
            (vec![1], b"v1".to_vec()),
            (vec![2], b"v2".to_vec()),
            (vec![10], b"v10".to_vec()),
            (vec![11], b"v11".to_vec()),
            (vec![12], b"v12".to_vec()),
        ]));

        let res = client.scan_reverse(..=vec![12], 10).await?;
        let keys: Vec<Vec<u8>> = res.into_iter().map(|pair| pair.0.into()).collect();
        assert_eq!(keys, vec![vec![12], vec![11], vec![10], vec![2], vec![1]]);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_keys_reverse_across_regions_returns_descending_keys() -> Result<()> {
        let client = new_mock_raw_client_with_scan_data(Arc::new(vec![
            (vec![1], b"v1".to_vec()),
            (vec![2], b"v2".to_vec()),
            (vec![10], b"v10".to_vec()),
            (vec![11], b"v11".to_vec()),
            (vec![12], b"v12".to_vec()),
        ]));

        let res = client.scan_keys_reverse(..=vec![12], 10).await?;
        let keys: Vec<Vec<u8>> = res.into_iter().map(Into::into).collect();
        assert_eq!(keys, vec![vec![12], vec![11], vec![10], vec![2], vec![1]]);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_reverse_respects_exclusive_upper_bound() -> Result<()> {
        let client = new_mock_raw_client_with_scan_data(Arc::new(vec![
            (vec![1], b"v1".to_vec()),
            (vec![2], b"v2".to_vec()),
            (vec![10], b"v10".to_vec()),
            (vec![11], b"v11".to_vec()),
            (vec![12], b"v12".to_vec()),
        ]));

        let res = client.scan_reverse(..vec![12], 10).await?;
        let keys: Vec<Vec<u8>> = res.into_iter().map(|pair| pair.0.into()).collect();
        assert_eq!(keys, vec![vec![11], vec![10], vec![2], vec![1]]);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_reverse_zero_limit_without_upper_bound_returns_empty() -> Result<()> {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_captured = dispatch_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                Err(Error::StringError(
                    "zero-limit reverse scan should not dispatch".to_owned(),
                ))
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert!(client.scan_reverse(.., 0).await?.is_empty());
        assert!(client.scan_keys_reverse(.., 0).await?.is_empty());
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_reverse_requires_bounded_upper() -> Result<()> {
        let client = new_mock_raw_client_with_scan_data(Arc::new(vec![(vec![1], b"v1".to_vec())]));
        let err = client.scan_reverse(.., 10).await.unwrap_err();
        assert!(matches!(err, Error::Unimplemented));
        Ok(())
    }

    #[tokio::test]
    async fn test_min_safe_ts_uses_store_safe_ts_request() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::StoreSafeTsRequest>()
                    .expect("expected store safe-ts request");
                let range = req.key_range.as_ref().expect("expected key_range");
                assert!(range.start_key.is_empty());
                assert!(range.end_key.is_empty());

                calls_captured.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::StoreSafeTsResponse { safe_ts: 42 };
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert_eq!(client.min_safe_ts().await?, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Served from cache.
        assert_eq!(client.min_safe_ts().await?, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_min_safe_ts_with_txn_scope_filters_stores_by_zone_label() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::StoreSafeTsRequest>()
                    .expect("expected store safe-ts request");
                let range = req.key_range.as_ref().expect("expected key_range");
                assert!(range.start_key.is_empty());
                assert!(range.end_key.is_empty());

                calls_captured.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::StoreSafeTsResponse { safe_ts: 42 };
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "dc1".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "dc2".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert_eq!(client.min_safe_ts_with_txn_scope("dc1").await?, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(client.min_safe_ts_with_txn_scope("dc2").await?, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(client.min_safe_ts_with_txn_scope("dc3").await?, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(client.min_safe_ts_with_txn_scope("global").await?, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_min_safe_ts_cache_is_monotonic() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>()
                    .expect("expected store safe-ts request");

                let call = calls_captured.fetch_add(1, Ordering::SeqCst);
                let safe_ts = if call == 0 { 100 } else { 80 };
                Ok(Box::new(kvrpcpb::StoreSafeTsResponse { safe_ts }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert_eq!(client.min_safe_ts().await?, 100);
        client.safe_ts.refresh().await?;

        // A regression should be ignored.
        assert_eq!(client.min_safe_ts().await?, 100);
        Ok(())
    }

    #[tokio::test]
    async fn test_min_safe_ts_cache_does_not_drop_to_zero_on_store_error() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>()
                    .expect("expected store safe-ts request");

                let call = calls_captured.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    Ok(Box::new(kvrpcpb::StoreSafeTsResponse { safe_ts: 42 }) as Box<dyn Any>)
                } else {
                    Err(Error::Unimplemented)
                }
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        assert_eq!(client.min_safe_ts().await?, 42);
        client.safe_ts.refresh().await?;
        assert_eq!(client.min_safe_ts().await?, 42);
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_get_values_preserves_key_order_and_includes_missing_keys() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::RawBatchGetRequest>()
                    .expect("expected raw batch get request");

                let resp = kvrpcpb::RawBatchGetResponse {
                    region_error: None,
                    pairs: vec![
                        kvrpcpb::KvPair {
                            key: vec![13],
                            value: vec![3],
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: vec![11],
                            value: vec![1],
                            ..Default::default()
                        },
                    ],
                };

                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let values = client
            .batch_get_values(vec![vec![11], vec![12], vec![13]])
            .await?;
        assert_eq!(values, vec![Some(vec![1]), None, Some(vec![3])]);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_put_with_ttl() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::RawBatchPutRequest>().is_some() {
                    let resp = kvrpcpb::RawBatchPutResponse {
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    unreachable!()
                }
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Enable { keyspace_id: 0 }),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Enable { keyspace_id: 0 },
        };
        let pairs = vec![
            KvPair(vec![11].into(), vec![12]),
            KvPair(vec![11].into(), vec![12]),
        ];
        let ttls = vec![0, 0];
        assert!(client.batch_put_with_ttl(pairs, ttls).await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_put_with_ttl_allows_single_ttl_for_all_pairs() -> Result<()> {
        let seen_keys = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let seen_keys_cloned = seen_keys.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::RawBatchPutRequest =
                    req.downcast_ref().expect("expected raw batch put request");
                assert_eq!(
                    req.ttls.len(),
                    req.pairs.len(),
                    "ttls must be replicated per pair before dispatch"
                );
                assert!(req.ttls.iter().all(|ttl| *ttl == 7));

                let mut keys = seen_keys_cloned.lock().unwrap();
                keys.extend(req.pairs.iter().map(|pair| pair.key.clone()));

                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let pairs = vec![
            KvPair(vec![1].into(), vec![1]),
            KvPair(vec![11].into(), vec![2]),
            KvPair(vec![251].into(), vec![3]),
        ];
        client.batch_put_with_ttl(pairs, std::iter::once(7)).await?;

        let mut keys = seen_keys.lock().unwrap().clone();
        keys.sort();
        assert_eq!(keys, vec![vec![1], vec![11], vec![251]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_atomic_batch_put_with_ttl_propagates_for_cas_and_replicated_ttls() -> Result<()> {
        let seen_keys = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let seen_keys_captured = seen_keys.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::RawBatchPutRequest =
                    req.downcast_ref().expect("expected raw batch put request");
                assert!(req.for_cas);
                assert_eq!(
                    req.ttls.len(),
                    req.pairs.len(),
                    "ttls must be replicated per pair before dispatch"
                );
                assert!(req.ttls.iter().all(|ttl| *ttl == 7));
                assert_eq!(req.cf, "write");

                let mut keys = seen_keys_captured.lock().unwrap();
                keys.extend(req.pairs.iter().map(|pair| pair.key.clone()));

                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Write),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_atomic_for_cas();

        let pairs = vec![
            KvPair(vec![1].into(), vec![1]),
            KvPair(vec![11].into(), vec![2]),
            KvPair(vec![251].into(), vec![3]),
        ];
        client.batch_put_with_ttl(pairs, std::iter::once(7)).await?;

        let mut keys = seen_keys.lock().unwrap().clone();
        keys.sort();
        assert_eq!(keys, vec![vec![1], vec![11], vec![251]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_put_with_ttl_rejects_mismatched_ttl_count() -> Result<()> {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_cloned = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::RawBatchPutRequest>()
                    .expect("expected raw batch put request");
                dispatch_calls_cloned.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::RawBatchPutResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let pairs = vec![
            KvPair(vec![1].into(), vec![1]),
            KvPair(vec![11].into(), vec![2]),
            KvPair(vec![251].into(), vec![3]),
        ];

        let err = client
            .batch_put_with_ttl(pairs, vec![5, 6])
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::StringError(ref msg) if msg.contains("raw batch put ttls length mismatch")),
            "unexpected error: {err:?}"
        );

        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_coprocessor() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawCoprocessorRequest>() {
                    assert_eq!(req.copr_name, "example");
                    assert_eq!(req.copr_version_req, "0.1.0");
                    let resp = kvrpcpb::RawCoprocessorResponse {
                        data: req.data.clone(),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    unreachable!()
                }
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Enable { keyspace_id: 0 }),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Enable { keyspace_id: 0 },
        };
        let resps = client
            .coprocessor(
                "example",
                "0.1.0",
                vec![vec![5]..vec![15], vec![20]..vec![]],
                |region, ranges| format!("{:?}:{:?}", region.id, ranges).into_bytes(),
            )
            .await?;
        let resps: Vec<_> = resps
            .into_iter()
            .map(|(ranges, data)| (ranges, String::from_utf8(data).unwrap()))
            .collect();
        assert_eq!(
            resps,
            vec![(
                vec![
                    Key::from(vec![5])..Key::from(vec![15]),
                    Key::from(vec![20])..Key::from(vec![])
                ],
                "2:[Key(05)..Key(0F), Key(14)..Key()]".to_string(),
            ),]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_raw_coprocessor_rejects_invalid_version_requirement() -> Result<()> {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_cloned = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::RawCoprocessorRequest>()
                    .expect("expected raw coprocessor request");
                dispatch_calls_cloned.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::RawCoprocessorResponse::default()) as Box<dyn Any>)
            },
        )));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let err = client
            .coprocessor(
                "example",
                "definitely not a semver req",
                vec![vec![1]..vec![2]],
                |_region, _ranges| Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidSemver(_)),
            "unexpected error: {err:?}"
        );
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[test]
    fn test_pd_client_getter_returns_handle() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::default()));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client.clone(),
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };
        assert!(Arc::ptr_eq(&client.pd_client(), &pd_client));
    }

    #[tokio::test]
    async fn test_delete_range_empty_keys_multi_region() -> Result<()> {
        let seen_ranges = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
        let seen_ranges_captured = seen_ranges.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawDeleteRangeRequest>()
                    .expect("expected raw delete_range request");
                seen_ranges_captured
                    .lock()
                    .unwrap()
                    .push((req.start_key.clone(), req.end_key.clone()));
                Ok(Box::new(kvrpcpb::RawDeleteRangeResponse {
                    error: "".to_owned(),
                    region_error: None,
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        client.delete_range(..).await?;

        let mut seen_ranges = seen_ranges.lock().unwrap().clone();
        seen_ranges.sort();
        assert_eq!(
            seen_ranges,
            vec![
                (vec![], vec![10]),
                (vec![10], vec![250, 250]),
                (vec![250, 250], vec![]),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_range_rejects_atomic_mode() {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_captured = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                Err(Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let client = client.with_atomic_for_cas();
        let err = client.delete_range(..).await.unwrap_err();
        assert!(matches!(err, Error::UnsupportedMode));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_atomic_mode_propagates_for_cas_to_put_and_delete() -> Result<()> {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_captured = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawPutRequest>() {
                    assert_eq!(req.key, vec![42]);
                    assert_eq!(req.value, vec![100]);
                    assert_eq!(req.ttl, 0);
                    assert_eq!(req.cf, "write");
                    assert!(req.for_cas);
                    dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawPutResponse::default()) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::RawDeleteRequest>() {
                    assert_eq!(req.key, vec![42]);
                    assert_eq!(req.cf, "write");
                    assert!(req.for_cas);
                    dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawDeleteResponse::default()) as Box<dyn Any>);
                }

                unreachable!("unexpected request type");
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Write),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_atomic_for_cas();

        client.put(vec![42], vec![100]).await?;
        client.delete(vec![42]).await?;

        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_atomic_put_with_ttl_propagates_for_cas_and_explicit_ttl() -> Result<()> {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_captured = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawPutRequest>()
                    .expect("expected raw put request");
                assert_eq!(req.key, vec![42]);
                assert_eq!(req.value, vec![100]);
                assert_eq!(req.ttl, 9);
                assert_eq!(req.cf, "lock");
                assert!(req.for_cas);
                dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::RawPutResponse::default()) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Lock),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_atomic_for_cas();

        client.put_with_ttl(vec![42], vec![100], 9).await?;

        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_compare_and_swap_requires_atomic_mode() {
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_captured = dispatch_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_captured.fetch_add(1, Ordering::SeqCst);
                Err(Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        let err = client
            .compare_and_swap(vec![42], None::<Value>, vec![100])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::UnsupportedMode));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_compare_and_swap_sets_previous_not_exist_and_processes_response() -> Result<()> {
        let expected_key = vec![42];
        let expected_value = vec![100];

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawCasRequest>()
                    .expect("expected raw cas request");
                assert_eq!(req.key, expected_key);
                assert_eq!(req.value, expected_value);
                assert!(req.previous_not_exist);

                Ok(Box::new(kvrpcpb::RawCasResponse {
                    previous_not_exist: true,
                    succeed: true,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_atomic_for_cas();

        let (previous, swapped) = client
            .compare_and_swap(vec![42], None::<Value>, vec![100])
            .await?;
        assert_eq!(previous, None);
        assert!(swapped);
        Ok(())
    }

    #[tokio::test]
    async fn test_compare_and_swap_propagates_kv_error() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::RawCasRequest>()
                    .expect("expected raw cas request");
                Ok(Box::new(kvrpcpb::RawCasResponse {
                    error: "injected cas error".to_owned(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_atomic_for_cas();

        let err = client
            .compare_and_swap(vec![42], None::<Value>, vec![100])
            .await
            .unwrap_err();
        match err {
            Error::KvError { message } => assert_eq!(message, "injected cas error"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_raw_client_trace_fields_propagate_to_context() -> Result<()> {
        let trace_id = b"trace-raw-123".to_vec();
        let flags = TraceControlFlags::IMMEDIATE_LOG.with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
        let flags_bits = flags.bits();

        let get_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let checksum_calls = Arc::new(AtomicUsize::new(0));
        let cas_calls = Arc::new(AtomicUsize::new(0));
        let coprocessor_calls = Arc::new(AtomicUsize::new(0));

        let expected_trace_id = trace_id.clone();
        let get_calls_cloned = get_calls.clone();
        let scan_calls_cloned = scan_calls.clone();
        let checksum_calls_cloned = checksum_calls.clone();
        let cas_calls_cloned = cas_calls.clone();
        let coprocessor_calls_cloned = coprocessor_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.trace_id, expected_trace_id);
                    assert_eq!(ctx.trace_control_flags, flags_bits);
                    get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawGetResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawScanRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.trace_id, expected_trace_id);
                    assert_eq!(ctx.trace_control_flags, flags_bits);
                    scan_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawScanResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawChecksumRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.trace_id, expected_trace_id);
                    assert_eq!(ctx.trace_control_flags, flags_bits);
                    checksum_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawChecksumResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawCasRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.trace_id, expected_trace_id);
                    assert_eq!(ctx.trace_control_flags, flags_bits);
                    cas_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawCasResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawCoprocessorRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.trace_id, expected_trace_id);
                    assert_eq!(ctx.trace_control_flags, flags_bits);
                    coprocessor_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RawCoprocessorResponse::default()) as Box<dyn Any>);
                }
                unreachable!("unexpected request type");
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        }
        .with_trace_id(trace_id)
        .with_trace_control_flags(flags);

        let _ = client.get(vec![1]).await?;
        let _ = client.scan(vec![1]..vec![2], 1).await?;
        let _ = client.checksum(vec![1]..vec![2]).await?;
        let _ = client
            .coprocessor(
                "test",
                "0.1.0",
                vec![vec![1]..vec![2]],
                |_region, _ranges| Vec::new(),
            )
            .await?;
        let atomic_client = client.with_atomic_for_cas();
        let _ = atomic_client
            .compare_and_swap(vec![42], None::<Value>, vec![100])
            .await?;

        assert!(get_calls.load(Ordering::SeqCst) > 0);
        assert!(scan_calls.load(Ordering::SeqCst) > 0);
        assert!(checksum_calls.load(Ordering::SeqCst) > 0);
        assert!(cas_calls.load(Ordering::SeqCst) > 0);
        assert!(coprocessor_calls.load(Ordering::SeqCst) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_raw_client_trace_id_falls_back_to_task_local() -> Result<()> {
        let trace_id = b"trace-raw-task-local".to_vec();
        let expected_trace_id = trace_id.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawGetRequest>()
                    .expect("expected raw get request");
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.trace_id, expected_trace_id);

                Ok(Box::<kvrpcpb::RawGetResponse>::default() as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        crate::trace::with_trace_id(trace_id, async move {
            let _ = client.get(vec![1]).await?;
            Ok::<(), Error>(())
        })
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_client_trace_control_flags_fall_back_to_task_local() -> Result<()> {
        let flags = TraceControlFlags::IMMEDIATE_LOG
            .with(TraceControlFlags::TIKV_CATEGORY_REQUEST)
            .with(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
        let expected_flags = flags.bits();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawGetRequest>()
                    .expect("expected raw get request");
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.trace_control_flags, expected_flags);

                Ok(Box::<kvrpcpb::RawGetResponse>::default() as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        crate::trace::with_trace_control_flags(flags, async move {
            let _ = client.get(vec![1]).await?;
            Ok::<(), Error>(())
        })
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_raw_client_request_metadata_falls_back_to_task_local() -> Result<()> {
        let expected_request_source = "internal_gc_stats".to_owned();
        let expected_resource_group_name = "rg-raw".to_owned();
        let expected_resource_group_name_for_hook = expected_resource_group_name.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::RawGetRequest>()
                    .expect("expected raw get request");
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.request_source, expected_request_source);
                assert_eq!(
                    ctx.resource_control_context
                        .as_ref()
                        .map(|ctx| ctx.resource_group_name.as_str()),
                    Some(expected_resource_group_name_for_hook.as_str())
                );

                Ok(Box::<kvrpcpb::RawGetResponse>::default() as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            trace_id: None,
            trace_control_flags: TraceControlFlags::default(),
            keyspace: Keyspace::Disable,
        };

        crate::request_context::with_internal_source_and_task_type("gc", "stats", async move {
            crate::request_context::with_resource_group_name(
                expected_resource_group_name,
                async move {
                    let _ = client.get(vec![1]).await?;
                    Ok::<(), Error>(())
                },
            )
            .await
        })
        .await?;

        Ok(())
    }
}
