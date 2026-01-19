// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use core::ops::Range;

use std::str::FromStr;
use std::sync::Arc;

use log::debug;
use tokio::time::sleep;

use super::RawChecksum;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::common::Error;
use crate::config::Config;
use crate::interceptor::RpcContextInfo;
use crate::interceptor::RpcInterceptor;
use crate::interceptor::RpcInterceptorChain;
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
use crate::store::{HasRegionError, RegionStore};
use crate::Backoff;
use crate::BoundRange;
use crate::ColumnFamily;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::Error::RegionError;
use crate::Key;
use crate::KvPair;
use crate::Result;
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
    cluster_id: u64,
    rpc: Arc<PdC>,
    cf: Option<ColumnFamily>,
    backoff: Backoff,
    /// Whether to use the [`atomic mode`](Client::with_atomic_for_cas).
    atomic: bool,
    keyspace: Keyspace,
    request_context: crate::RequestContext,
}

impl<PdC: PdClient> Clone for Client<PdC> {
    fn clone(&self) -> Self {
        Self {
            cluster_id: self.cluster_id,
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
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
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// let _client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default()).await
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
    /// # use tikv_client::{Config, RawClient, Result};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<()> {
    /// let _client = RawClient::new_with_config(
    ///     vec!["192.168.0.100"],
    ///     Config::default().with_timeout(Duration::from_secs(60)),
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        let enable_codec = config.keyspace.is_some();
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let rpc =
            Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), enable_codec).await?);
        let cluster_id = rpc.cluster_id().await;
        let keyspace = match config.keyspace {
            Some(keyspace) => {
                let keyspace = rpc.load_keyspace(&keyspace).await?;
                Keyspace::try_enable(keyspace.id)?
            }
            None => Keyspace::Disable,
        };
        Ok(Client {
            cluster_id,
            rpc,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace,
            request_context: crate::RequestContext::default(),
        })
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
    /// # use tikv_client::{ColumnFamily, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// let client = RawClient::new(vec!["192.168.0.100"]).await?.with_cf(ColumnFamily::Write);
    /// // Fetch a value at "foo" from the Write CF.
    /// let _value = client.get("foo".to_owned()).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_cf(&self, cf: ColumnFamily) -> Self {
        Client {
            cluster_id: self.cluster_id,
            rpc: self.rpc.clone(),
            cf: Some(cf),
            backoff: self.backoff.clone(),
            atomic: self.atomic,
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
        }
    }

    /// Set the [`Backoff`] strategy for retrying requests.
    /// The default strategy is [`DEFAULT_REGION_BACKOFF`].
    /// See [`Backoff`] for more information.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{RawClient, Result};
    /// # use tikv_client::backoff::DEFAULT_REGION_BACKOFF;
    /// # async fn example() -> Result<()> {
    /// let client = RawClient::new(vec!["192.168.0.100"])
    ///     .await?
    ///     .with_backoff(DEFAULT_REGION_BACKOFF);
    /// let _value = client.get("foo".to_owned()).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_backoff(&self, backoff: Backoff) -> Self {
        Client {
            cluster_id: self.cluster_id,
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff,
            atomic: self.atomic,
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
        }
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
            cluster_id: self.cluster_id,
            rpc: self.rpc.clone(),
            cf: self.cf.clone(),
            backoff: self.backoff.clone(),
            atomic: true,
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
        }
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Returns the PD cluster ID this client is connected to.
    #[must_use]
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Set `kvrpcpb::Context.request_source` for all requests created by this client.
    #[must_use]
    pub fn with_request_source(&self, source: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_request_source(source);
        cloned
    }

    /// Set `kvrpcpb::Context.resource_group_tag` for all requests created by this client.
    #[must_use]
    pub fn with_resource_group_tag(&self, tag: impl Into<Vec<u8>>) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_resource_group_tag(tag);
        cloned
    }

    /// Set `kvrpcpb::Context.resource_control_context.resource_group_name` for all requests created
    /// by this client.
    #[must_use]
    pub fn with_resource_group_name(&self, name: impl Into<String>) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_resource_group_name(name);
        cloned
    }

    /// Set `kvrpcpb::Context.priority` for all requests created by this client.
    #[must_use]
    pub fn with_priority(&self, priority: CommandPriority) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_priority(priority);
        cloned
    }

    /// Set `kvrpcpb::Context.disk_full_opt` for all requests created by this client.
    #[must_use]
    pub fn with_disk_full_opt(&self, disk_full_opt: DiskFullOpt) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_disk_full_opt(disk_full_opt);
        cloned
    }

    /// Set `kvrpcpb::Context.txn_source` for all requests created by this client.
    #[must_use]
    pub fn with_txn_source(&self, txn_source: u64) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_txn_source(txn_source);
        cloned
    }

    /// Set `kvrpcpb::Context.resource_control_context.override_priority` for all requests created by this client.
    #[must_use]
    pub fn with_resource_control_override_priority(&self, override_priority: u64) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned
            .request_context
            .with_resource_control_override_priority(override_priority);
        cloned
    }

    /// Set `kvrpcpb::Context.resource_control_context.penalty` for all requests created by this client.
    #[must_use]
    pub fn with_resource_control_penalty(
        &self,
        penalty: impl Into<crate::resource_manager::Consumption>,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned
            .request_context
            .with_resource_control_penalty(penalty);
        cloned
    }

    /// Set a resource group tagger for all requests created by this client.
    ///
    /// If a fixed resource group tag is set via [`with_resource_group_tag`](Self::with_resource_group_tag),
    /// it takes precedence over this tagger.
    #[must_use]
    pub fn with_resource_group_tagger(
        &self,
        tagger: impl Fn(&RpcContextInfo, &crate::kvrpcpb::Context) -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned
            .request_context
            .with_resource_group_tagger(Arc::new(tagger));
        cloned
    }

    /// Replace the RPC interceptor chain for all requests created by this client.
    #[must_use]
    pub fn with_rpc_interceptor(&self, interceptor: Arc<dyn RpcInterceptor>) -> Self {
        let mut chain = RpcInterceptorChain::new();
        chain.link(interceptor);
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.with_rpc_interceptors(chain);
        cloned
    }

    /// Add an RPC interceptor for all requests created by this client.
    ///
    /// If another interceptor with the same name exists, it is replaced.
    #[must_use]
    pub fn with_added_rpc_interceptor(&self, interceptor: Arc<dyn RpcInterceptor>) -> Self {
        let mut cloned = self.clone();
        cloned.request_context = cloned.request_context.add_rpc_interceptor(interceptor);
        cloned
    }

    fn with_request_context<R: crate::store::Request>(&self, request: R) -> R {
        self.request_context.apply_to(request)
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
    /// # use tikv_client::{RawClient, Result, Value};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let key = "TiKV".to_owned();
    /// let result: Option<Value> = client.get(key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking raw get request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = self.with_request_context(new_raw_get_request(key, self.cf.clone()));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)
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
    /// # use tikv_client::{KvPair, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let result: Vec<KvPair> = client.batch_get(keys).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking raw batch_get request");
        let keys = keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let request = self.with_request_context(new_raw_batch_get_request(keys, self.cf.clone()));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(Collect)
            .plan();
        plan.execute()
            .await
            .truncate_keyspace(self.keyspace)
            .map(|r| {
                r.into_iter()
                    .map(|pair| pair.truncate_keyspace(self.keyspace))
                    .collect()
            })
    }

    /// Create a new 'get key ttl' request.
    ///
    /// Once resolved this request will result in the fetching of the alive time left for the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let key = "TiKV".to_owned();
    /// let result: Option<u64> = client.get_key_ttl_secs(key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_key_ttl_secs(&self, key: impl Into<Key>) -> Result<Option<u64>> {
        debug!("invoking raw get_key_ttl_secs request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = self.with_request_context(new_raw_get_key_ttl_request(key, self.cf.clone()));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)
    }

    /// Create a new 'put' request.
    ///
    /// Once resolved this request will result in the setting of the value associated with the given key.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let key = "TiKV".to_owned();
    /// let val = "TiKV".to_owned();
    /// client.put(key, val).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.put_with_ttl(key, value, 0).await
    }

    pub async fn put_with_ttl(
        &self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        ttl_secs: u64,
    ) -> Result<()> {
        debug!("invoking raw put request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request = self.with_request_context(new_raw_put_request(
            key,
            value.into(),
            self.cf.clone(),
            ttl_secs,
            self.atomic,
        ));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;
        Ok(())
    }

    /// Create a new 'batch put' request.
    ///
    /// Once resolved this request will result in the setting of the values associated with the given keys.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let kvpair1 = ("PD".to_owned(), "Go".to_owned());
    /// let kvpair2 = ("TiKV".to_owned(), "Rust".to_owned());
    /// let iterable = vec![kvpair1, kvpair2];
    /// client.batch_put(iterable).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_put(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
    ) -> Result<()> {
        self.batch_put_with_ttl(pairs, std::iter::repeat(0)).await
    }

    pub async fn batch_put_with_ttl(
        &self,
        pairs: impl IntoIterator<Item = impl Into<KvPair>>,
        ttls: impl IntoIterator<Item = u64>,
    ) -> Result<()> {
        debug!("invoking raw batch_put request");
        let pairs = pairs
            .into_iter()
            .map(|pair| pair.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let request = self.with_request_context(new_raw_batch_put_request(
            pairs,
            ttls.into_iter(),
            self.cf.clone(),
            self.atomic,
        ));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;
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
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let key = "TiKV".to_owned();
    /// client.delete(key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, key: impl Into<Key>) -> Result<()> {
        debug!("invoking raw delete request");
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let request =
            self.with_request_context(new_raw_delete_request(key, self.cf.clone(), self.atomic));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;
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
    /// # use tikv_client::{RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// client.batch_delete(keys).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_delete(&self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<()> {
        debug!("invoking raw batch_delete request");
        self.assert_non_atomic()?;
        let keys = keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(self.keyspace, KeyMode::Raw));
        let request =
            self.with_request_context(new_raw_batch_delete_request(keys, self.cf.clone()));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;
        Ok(())
    }

    /// Create a new 'delete range' request.
    ///
    /// Once resolved this request will result in the deletion of all keys lying in the given range.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{IntoOwnedRange, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range = "TiKV"..="TiDB";
    /// client.delete_range(inclusive_range.into_owned()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        debug!("invoking raw delete_range request");
        self.assert_non_atomic()?;
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let (start_key, end_key) = range.clone().into_keys();
        if let Some(end_key) = &end_key {
            if start_key >= *end_key {
                // TiKV rejects delete-range requests with an empty/invalid range. Treat them as
                // a no-op for ergonomics and parity with other range-based APIs.
                return Ok(());
            }
        }
        let request =
            self.with_request_context(new_raw_delete_range_request(range, self.cf.clone()));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;
        Ok(())
    }

    /// Compute a checksum over the keys in the given range.
    ///
    /// The checksum is computed by TiKV and aggregated client-side across regions.
    ///
    /// TiKV uses the CRC64-ECMA algorithm (init/xorout of all 1s) and computes `CRC64(key || value)`
    /// for each key/value pair, then xors all per-pair checksums within the range. In API v2
    /// keyspace mode, `key`
    /// refers to the encoded key bytes stored in TiKV (including the 4-byte keyspace prefix).
    /// `total_bytes` is the sum of `len(encoded_key) + len(value)` across pairs.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{IntoOwnedRange, RawChecksum, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// let client = RawClient::new(vec!["127.0.0.1:2379"]).await?;
    /// let checksum: RawChecksum = client.checksum(("a".."z").into_owned()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn checksum(&self, range: impl Into<BoundRange>) -> Result<RawChecksum> {
        debug!("invoking raw checksum request");
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let (start_key, end_key) = range.clone().into_keys();
        if let Some(end_key) = &end_key {
            if start_key >= *end_key {
                return Ok(RawChecksum::default());
            }
        }

        let request = self.with_request_context(new_raw_checksum_request(range));
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(Collect)
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)
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
    /// # use tikv_client::{IntoOwnedRange, KvPair, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let result: Vec<KvPair> = client.scan(inclusive_range.into_owned(), 2).await?;
    /// # Ok(())
    /// # }
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
    /// Reverse Scan queries continuous kv pairs in range [startKey, endKey),
    /// from startKey(lowerBound) to endKey(upperBound) in reverse order, up to limit pairs.
    /// The returned keys are in reversed lexicographical order.
    /// If you want to include the endKey or exclude the startKey, push a '\0' to the key.
    /// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{IntoOwnedRange, KvPair, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let result: Vec<KvPair> = client.scan_reverse(inclusive_range.into_owned(), 2).await?;
    /// # Ok(())
    /// # }
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
    /// # use tikv_client::{IntoOwnedRange, Key, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let result: Vec<Key> = client.scan_keys(inclusive_range.into_owned(), 2).await?;
    /// # Ok(())
    /// # }
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
    /// # use tikv_client::{IntoOwnedRange, Key, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range = "TiKV"..="TiDB";
    /// let result: Vec<Key> = client
    ///     .scan_keys_reverse(inclusive_range.into_owned(), 2)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scan_keys_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<Vec<Key>> {
        debug!("invoking raw scan_keys request");
        Ok(self
            .scan_inner(range, limit, true, true)
            .await?
            .into_iter()
            .map(KvPair::into_key)
            .collect())
    }

    /// Create a new 'batch scan' request.
    ///
    /// Once resolved this request will result in a set of scanners over the given keys.
    /// This is equivalent to calling [`scan`](Self::scan) for each range and concatenating the
    /// results (in the same order as the input ranges).
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{IntoOwnedRange, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let _result = client.batch_scan(iterable, 2).await?;
    /// # Ok(())
    /// # }
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
    /// This is equivalent to calling [`scan_keys`](Self::scan_keys) for each range and
    /// concatenating the results (in the same order as the input ranges).
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{IntoOwnedRange, Key, RawClient, Result};
    /// # async fn example() -> Result<()> {
    /// # let client = RawClient::new(vec!["192.168.0.100"]).await?;
    /// let inclusive_range1 = "TiDB"..="TiKV";
    /// let inclusive_range2 = "TiKV"..="TiSpark";
    /// let iterable = vec![inclusive_range1.into_owned(), inclusive_range2.into_owned()];
    /// let result: Vec<Key> = client.batch_scan_keys(iterable, 2).await?;
    /// # Ok(())
    /// # }
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
        let req = new_cas_request(
            key,
            new_value.into(),
            previous_value.into(),
            self.cf.clone(),
        );
        let req = self.with_request_context(req);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .retry_multi_region(self.backoff.clone())
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)
    }

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
        let req = new_raw_coprocessor_request(
            copr_name.into(),
            copr_version_req,
            ranges,
            request_builder,
        );
        let req = self.with_request_context(req);
        let plan = crate::request::PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .preserve_shard()
            .retry_multi_region(self.backoff.clone())
            .post_process_default()
            .plan();
        Ok(plan
            .execute()
            .await
            .truncate_keyspace(self.keyspace)?
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
        if limit > MAX_RAW_KV_SCAN_LIMIT {
            return Err(Error::MaxScanLimitExceeded {
                limit,
                max_limit: MAX_RAW_KV_SCAN_LIMIT,
            });
        }
        // For raw clients, retry/backoff is user-configurable via `RawClient::with_backoff`.
        let backoff = self.backoff.clone();
        let mut range = range.into().encode_keyspace(self.keyspace, KeyMode::Raw);
        let mut result = Vec::new();
        let mut current_limit = limit;
        let (start_key, end_key) = range.clone().into_keys();
        let mut current_key: Key = start_key;

        while current_limit > 0 {
            let scan_args = ScanInnerArgs {
                start_key: current_key.clone(),
                end_key: end_key.clone(),
                limit: current_limit,
                key_only,
                reverse,
                backoff: backoff.clone(),
            };
            let (res, next_key) = self.retryable_scan(scan_args).await?;

            let mut kvs = res
                .map(|r| r.kvs.into_iter().map(Into::into).collect::<Vec<KvPair>>())
                .unwrap_or(Vec::new());

            if !kvs.is_empty() {
                current_limit -= kvs.len() as u32;
                result.append(&mut kvs);
            }
            if end_key.clone().is_some_and(|ek| ek <= next_key) {
                break;
            } else {
                current_key = next_key;
                range = BoundRange::new(std::ops::Bound::Included(current_key.clone()), range.to);
            }
        }

        // limit is a soft limit, so we need check the number of results
        result.truncate(limit as usize);

        // truncate the prefix of keys
        let result = result.truncate_keyspace(self.keyspace);

        Ok(result)
    }

    async fn retryable_scan(
        &self,
        mut scan_args: ScanInnerArgs,
    ) -> Result<(Option<RawScanResponse>, Key)> {
        let start_key = scan_args.start_key;
        let end_key = scan_args.end_key;
        loop {
            let region = self.rpc.clone().region_for_key(&start_key).await?;
            let store = self.rpc.clone().store_for_id(region.id()).await?;
            let request = new_raw_scan_request(
                (start_key.clone(), end_key.clone()).into(),
                scan_args.limit,
                scan_args.key_only,
                scan_args.reverse,
                self.cf.clone(),
            );
            let request = self.with_request_context(request);
            let resp = self.do_store_scan(store.clone(), request.clone()).await;
            return match resp {
                Ok(mut r) => {
                    if let Some(err) = r.region_error() {
                        let status =
                            plan::handle_region_error(self.rpc.clone(), err.clone(), store.clone())
                                .await?;
                        if status {
                            continue;
                        } else if let Some(duration) = scan_args.backoff.next_delay_duration() {
                            sleep(duration).await;
                            continue;
                        } else {
                            return Err(RegionError(Box::new(err)));
                        }
                    }
                    Ok((Some(r), region.end_key()))
                }
                Err(err) => Err(err),
            };
        }
    }

    async fn do_store_scan(
        &self,
        mut store: RegionStore,
        scan_request: RawScanRequest,
    ) -> Result<RawScanResponse> {
        let scan_request = self.with_request_context(scan_request);
        store.request_context = self.request_context.clone();
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

        let results = futures::future::try_join_all(
            ranges
                .into_iter()
                .map(|range| self.scan_inner(range.into(), each_limit, key_only, false)),
        )
        .await?;
        Ok(results.into_iter().flatten().collect())
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
    start_key: Key,
    end_key: Option<Key>,
    limit: u32,
    key_only: bool,
    reverse: bool,
    backoff: Backoff,
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::Result;
    use tonic::Status;

    #[test]
    fn test_cluster_id_accessor() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            cluster_id: 42,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        assert_eq!(client.cluster_id(), 42);
    }

    #[tokio::test]
    async fn test_keyspace_encodes_raw_get_request_key() -> Result<()> {
        let keyspace_id = 4242;
        let expected_key = vec![b'r', 0, 16, 146, b'k', b'e', b'y'];

        let expected_key_cloned = expected_key.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() {
                    assert_eq!(req.key, expected_key_cloned);
                    Ok(Box::new(kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type: {:?}", req.type_id());
                }
            },
        )));
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Enable { keyspace_id },
            request_context: crate::RequestContext::default(),
        };

        let value = client.get("key".to_owned()).await?;
        assert!(value.is_none());
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
            cluster_id: 0,
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Enable { keyspace_id: 0 },
            request_context: crate::RequestContext::default(),
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
            cluster_id: 0,
            rpc: pd_client,
            cf: Some(ColumnFamily::Default),
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Enable { keyspace_id: 0 },
            request_context: crate::RequestContext::default(),
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
    async fn test_request_source_and_resource_group_tag() -> Result<()> {
        let expected_source = "unit-test".to_owned();
        let expected_tag = vec![1_u8, 2, 3];
        let expected_group_name = "unit-test-group".to_owned();
        let expected_disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull;
        let expected_txn_source = 42_u64;

        let hook_source = expected_source.clone();
        let hook_tag = expected_tag.clone();
        let hook_group_name = expected_group_name.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() {
                    let ctx = req.context.as_ref().expect("context should be set");
                    assert_eq!(ctx.request_source, hook_source);
                    assert_eq!(ctx.resource_group_tag, hook_tag);
                    assert_eq!(ctx.disk_full_opt, i32::from(expected_disk_full_opt));
                    assert_eq!(ctx.txn_source, expected_txn_source);
                    let resource_ctl_ctx = ctx
                        .resource_control_context
                        .as_ref()
                        .expect("resource_control_context should be set");
                    assert_eq!(resource_ctl_ctx.resource_group_name, hook_group_name);

                    let resp = kvrpcpb::RawGetResponse {
                        not_found: true,
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    unreachable!()
                }
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        }
        .with_request_source(expected_source)
        .with_resource_group_tag(expected_tag)
        .with_resource_group_name(expected_group_name)
        .with_disk_full_opt(expected_disk_full_opt)
        .with_txn_source(expected_txn_source);

        assert_eq!(client.get(vec![1_u8]).await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_scan_each_limit_is_per_range() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawScanRequest>() else {
                    unreachable!()
                };
                let ctx = req.context.as_ref().expect("context should be set");
                let (region_start, region_end) = match ctx.region_id {
                    1 => (vec![], vec![10]),
                    2 => (vec![10], vec![250, 250]),
                    3 => (vec![250, 250], vec![]),
                    _ => unreachable!("unexpected region_id: {}", ctx.region_id),
                };

                // Simulate a tiny ordered dataset across regions:
                // - region 1: [1], [2]
                // - region 2: [10], [11], [12], [13]
                let data: &[(Vec<u8>, Vec<u8>)] = &[
                    (vec![1], vec![1]),
                    (vec![2], vec![2]),
                    (vec![10], vec![10]),
                    (vec![11], vec![11]),
                    (vec![12], vec![12]),
                    (vec![13], vec![13]),
                ];

                let start = if req.start_key < region_start {
                    region_start.clone()
                } else {
                    req.start_key.clone()
                };
                let mut end = req.end_key.clone();
                if end.is_empty() || (!region_end.is_empty() && end > region_end) {
                    end = region_end.clone();
                }

                let mut kvs = Vec::new();
                for (k, v) in data {
                    if k.as_slice() < start.as_slice() {
                        continue;
                    }
                    if !end.is_empty() && k.as_slice() >= end.as_slice() {
                        break;
                    }

                    kvs.push(kvrpcpb::KvPair {
                        key: k.clone(),
                        value: if req.key_only { vec![] } else { v.clone() },
                        ..Default::default()
                    });
                    if kvs.len() >= req.limit as usize {
                        break;
                    }
                }

                Ok(Box::new(kvrpcpb::RawScanResponse {
                    kvs,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let ranges = vec![vec![1_u8]..vec![14_u8], vec![10_u8]..vec![13_u8]];
        let each_limit = 3;

        let pairs = client.batch_scan(ranges.clone(), each_limit).await?;
        assert_eq!(pairs.len(), 6);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|p| p.0.into()).collect();
        assert_eq!(
            keys,
            vec![
                vec![1_u8],
                vec![2_u8],
                vec![10_u8],
                vec![10_u8],
                vec![11_u8],
                vec![12_u8],
            ]
        );

        let keys = client.batch_scan_keys(ranges, each_limit).await?;
        let keys: Vec<Vec<u8>> = keys.into_iter().map(Into::into).collect();
        assert_eq!(
            keys,
            vec![
                vec![1_u8],
                vec![2_u8],
                vec![10_u8],
                vec![10_u8],
                vec![11_u8],
                vec![12_u8],
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_request_context_setters_and_interceptors() -> Result<()> {
        let expected_source = "unit-test-context".to_owned();
        let expected_tag = vec![9_u8, 9, 9];
        let expected_priority = CommandPriority::High;
        let expected_override_priority = 16_u64;

        let interceptor1_called = Arc::new(AtomicBool::new(false));
        let interceptor2_called = Arc::new(AtomicBool::new(false));

        let interceptor1_called_cloned = interceptor1_called.clone();
        let interceptor2_called_cloned = interceptor2_called.clone();
        let interceptor1 =
            crate::interceptor::rpc_interceptor("unit_test.interceptor1", move |info, ctx| {
                assert_eq!(info.label, "raw_get");
                interceptor1_called_cloned.store(true, Ordering::SeqCst);
                ctx.request_source = "from-interceptor".to_owned();
            });
        let interceptor2 =
            crate::interceptor::rpc_interceptor("unit_test.interceptor2", move |info, _| {
                assert_eq!(info.label, "raw_get");
                interceptor2_called_cloned.store(true, Ordering::SeqCst);
            });

        let tag_expected_source = expected_source.clone();
        let tag_expected_tag = expected_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawGetRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.request_source, "from-interceptor");
                assert_eq!(ctx.resource_group_tag, tag_expected_tag);
                assert_eq!(ctx.priority, i32::from(expected_priority));
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("resource_control_context should be set");
                assert_eq!(
                    resource_ctl_ctx.override_priority,
                    expected_override_priority
                );
                assert!(resource_ctl_ctx.penalty.is_some());

                Ok(Box::new(kvrpcpb::RawGetResponse {
                    not_found: true,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        }
        .with_request_source(expected_source.clone())
        .with_priority(expected_priority)
        .with_resource_control_override_priority(expected_override_priority)
        .with_resource_control_penalty(crate::resource_manager::Consumption::default())
        .with_resource_group_tagger(move |info, ctx| {
            assert_eq!(info.label, "raw_get");
            assert_eq!(ctx.request_source, tag_expected_source);
            expected_tag.clone()
        })
        .with_rpc_interceptor(interceptor1)
        .with_added_rpc_interceptor(interceptor2);

        assert_eq!(client.get(vec![1_u8]).await?, None);
        assert!(interceptor1_called.load(Ordering::SeqCst));
        assert!(interceptor2_called.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_keys_wrappers_return_empty_for_zero_limit() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        assert!(client
            .scan_keys(vec![1_u8]..vec![2_u8], 0)
            .await?
            .is_empty());
        assert!(client
            .scan_keys_reverse(vec![1_u8]..vec![2_u8], 0)
            .await?
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_limit_exceeded_errors() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let err = client
            .scan(vec![1_u8]..vec![2_u8], MAX_RAW_KV_SCAN_LIMIT + 1)
            .await
            .expect_err("expected MaxScanLimitExceeded");
        assert!(matches!(err, Error::MaxScanLimitExceeded { .. }));

        let err = client
            .batch_scan(vec![vec![1_u8]..vec![2_u8]], MAX_RAW_KV_SCAN_LIMIT + 1)
            .await
            .expect_err("expected MaxScanLimitExceeded");
        assert!(matches!(err, Error::MaxScanLimitExceeded { .. }));
    }

    #[tokio::test]
    async fn test_checksum_empty_range_returns_default_without_rpc() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let checksum = client.checksum(vec![2_u8]..vec![2_u8]).await?;
        assert_eq!(checksum, RawChecksum::default());
        Ok(())
    }

    #[tokio::test]
    async fn test_compare_and_swap_requires_atomic_mode() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let err = client
            .compare_and_swap(vec![1_u8], None, vec![2_u8])
            .await
            .expect_err("expected UnsupportedMode");
        assert!(matches!(err, Error::UnsupportedMode));
    }

    #[tokio::test]
    async fn test_compare_and_swap_atomic_success() -> Result<()> {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::RawCasRequest>() else {
                    unreachable!("unexpected request type");
                };
                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.region_id, 1);
                Ok(Box::new(kvrpcpb::RawCasResponse {
                    succeed: true,
                    previous_not_exist: false,
                    previous_value: vec![7],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: DEFAULT_REGION_BACKOFF,
            atomic: true,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let (prev, swapped) = client
            .compare_and_swap(vec![1_u8], Some(vec![6_u8]), vec![7_u8])
            .await?;
        assert_eq!(prev, Some(vec![7_u8]));
        assert!(swapped);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_scan_region_error_resolved_retries_immediately() -> Result<()> {
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls_for_hook = scan_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawScanRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = scan_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Ok(Box::new(kvrpcpb::RawScanResponse {
                        region_error: Some(errorpb::Error {
                            not_leader: Some(errorpb::NotLeader {
                                leader: Some(metapb::Peer {
                                    store_id: 42,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::new(kvrpcpb::RawScanResponse::default()) as Box<dyn Any>)
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: Backoff::no_backoff(),
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let _ = client.scan(vec![1_u8]..vec![2_u8], 1).await?;
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_scan_region_error_backoffs_then_retries() -> Result<()> {
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls_for_hook = scan_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawScanRequest>() else {
                    unreachable!("unexpected request type");
                };
                let call = scan_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    return Ok(Box::new(kvrpcpb::RawScanResponse {
                        region_error: Some(errorpb::Error {
                            stale_command: Some(errorpb::StaleCommand::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::new(kvrpcpb::RawScanResponse::default()) as Box<dyn Any>)
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: Backoff::no_jitter_backoff(0, 0, 1),
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let _ = client.scan(vec![1_u8]..vec![2_u8], 1).await?;
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_retryable_scan_region_error_without_backoff_returns_err() {
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls_for_hook = scan_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawScanRequest>() else {
                    unreachable!("unexpected request type");
                };
                scan_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::RawScanResponse {
                    region_error: Some(errorpb::Error {
                        stale_command: Some(errorpb::StaleCommand::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: Backoff::no_backoff(),
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        let err = client
            .scan(vec![1_u8]..vec![2_u8], 1)
            .await
            .expect_err("expected RegionError");
        assert!(matches!(err, Error::RegionError(_)));
        assert_eq!(scan_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retryable_scan_propagates_store_scan_error() {
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls_for_hook = scan_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(_) = req.downcast_ref::<kvrpcpb::RawScanRequest>() else {
                    unreachable!("unexpected request type");
                };
                scan_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                Err(Error::GrpcAPI(Status::unavailable("grpc error")))
            },
        )));

        let client = Client {
            cluster_id: 0,
            rpc: pd_client,
            cf: None,
            backoff: Backoff::no_backoff(),
            atomic: false,
            keyspace: Keyspace::Disable,
            request_context: crate::RequestContext::default(),
        };

        assert!(client.scan(vec![1_u8]..vec![2_u8], 1).await.is_err());
        assert_eq!(scan_calls.load(Ordering::SeqCst), 1);
    }
}
