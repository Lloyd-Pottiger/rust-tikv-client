// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use log::debug;
use log::info;

use crate::backoff::{DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
use crate::config::Config;
use crate::interceptor::RpcContextInfo;
use crate::interceptor::RpcInterceptor;
use crate::interceptor::RpcInterceptorChain;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::pdpb::Timestamp;
use crate::request::plan::CleanupLocksResult;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::timestamp::TimestampExt;
use crate::transaction::lock::ResolveLocksOptions;
use crate::transaction::lowering::new_scan_lock_request;
use crate::transaction::lowering::new_unsafe_destroy_range_request;
use crate::transaction::ResolveLocksContext;
use crate::transaction::Snapshot;
use crate::transaction::Transaction;
use crate::transaction::TransactionOptions;
use crate::Backoff;
use crate::BoundRange;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::RequestContext;
use crate::Result;

/// The TiKV transactional `Client` is used to interact with TiKV using transactional requests.
///
/// Transactions support optimistic and pessimistic modes. For more details see the SIG-transaction
/// [docs](https://github.com/tikv/sig-transaction/tree/master/doc/tikv#optimistic-and-pessimistic-transactions).
///
/// Begin a [`Transaction`] by calling [`begin_optimistic`](Client::begin_optimistic) or
/// [`begin_pessimistic`](Client::begin_pessimistic). A transaction must be rolled back or committed.
///
/// Besides transactions, the client provides some further functionality:
/// - `gc`: trigger a GC process which clears stale data in the cluster.
/// - `current_timestamp`: get the current `Timestamp` from PD.
/// - `snapshot`: get a [`Snapshot`] of the database at a specified timestamp.
///   A `Snapshot` is a read-only transaction.
///
/// The returned results of transactional requests are [`Future`](std::future::Future)s that must be
/// awaited to execute.
pub struct Client {
    pd: Arc<PdRpcClient>,
    keyspace: Keyspace,
    request_context: RequestContext,
    txn_latches: Option<Arc<super::LatchesScheduler>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            pd: self.pd.clone(),
            keyspace: self.keyspace,
            request_context: self.request_context.clone(),
            txn_latches: self.txn_latches.clone(),
        }
    }
}

impl Client {
    /// Create a transactional [`Client`] and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Client> {
        // debug!("creating transactional client");
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    /// Create a transactional [`Client`] with a custom configuration, and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::time::Duration;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new_with_config(
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
    ) -> Result<Client> {
        debug!("creating new transactional client");
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        let keyspace = match config.keyspace {
            Some(keyspace) => {
                let keyspace = pd.load_keyspace(&keyspace).await?;
                Keyspace::Enable {
                    keyspace_id: keyspace.id,
                }
            }
            None => Keyspace::Disable,
        };
        Ok(Client {
            pd,
            keyspace,
            request_context: RequestContext::default(),
            txn_latches: None,
        })
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

    /// Enable in-process transaction local latches for commits.
    ///
    /// This can reduce client-side write conflicts by serializing commits with overlapping keys.
    /// It is only applied for *optimistic, non-pipelined* transactions (matching client-go's
    /// behavior).
    #[must_use]
    pub fn with_txn_local_latches(&self, capacity: usize) -> Self {
        let mut cloned = self.clone();
        cloned.txn_latches = Some(super::LatchesScheduler::new(capacity));
        cloned
    }

    /// Creates a new optimistic [`Transaction`].
    ///
    /// Use the transaction to issue requests like [`get`](Transaction::get) or
    /// [`put`](Transaction::put).
    ///
    /// Write operations do not lock data in TiKV, thus the commit request may fail due to a write
    /// conflict.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_optimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_optimistic(&self) -> Result<Transaction> {
        debug!("creating new optimistic transaction");
        let timestamp = self.current_timestamp().await?;
        Ok(self.new_transaction(timestamp, TransactionOptions::new_optimistic()))
    }

    /// Creates a new pessimistic [`Transaction`].
    ///
    /// Write operations will lock the data until committed, thus commit requests should not suffer
    /// from write conflicts.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_pessimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_pessimistic(&self) -> Result<Transaction> {
        debug!("creating new pessimistic transaction");
        let timestamp = self.current_timestamp().await?;
        Ok(self.new_transaction(timestamp, TransactionOptions::new_pessimistic()))
    }

    /// Create a new customized [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient, TransactionOptions};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client
    ///     .begin_with_options(TransactionOptions::default().use_async_commit())
    ///     .await
    ///     .unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_with_options(&self, options: TransactionOptions) -> Result<Transaction> {
        debug!("creating new customized transaction");
        options.validate()?;
        let timestamp = self.current_timestamp().await?;
        Ok(self.new_transaction(timestamp, options))
    }

    /// Create a new [`Snapshot`] at the given [`Timestamp`].
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        debug!("creating new snapshot");
        Snapshot::new(self.new_transaction(timestamp, options.read_only()))
    }

    /// Retrieve the current [`Timestamp`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let timestamp = client.current_timestamp().await.unwrap();
    /// # });
    /// ```
    pub async fn current_timestamp(&self) -> Result<Timestamp> {
        self.pd.clone().get_timestamp().await
    }

    /// Retrieve a minimum timestamp from PD (`GetMinTs`).
    ///
    /// This timestamp is a control-plane value intended for *stale reads* (replica reads that may
    /// be slightly behind the leader). A common pattern is:
    /// 1) write data and get a commit timestamp
    /// 2) wait until `current_min_timestamp >= commit_ts`
    /// 3) perform a stale snapshot read at `current_min_timestamp`
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Result, TransactionClient, TransactionOptions};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["127.0.0.1:2379"]).await?;
    /// let ts = client.current_min_timestamp().await?;
    /// let mut snapshot = client.snapshot(ts, TransactionOptions::new_optimistic().stale_read(true));
    /// let _ = snapshot.get("k".to_owned()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn current_min_timestamp(&self) -> Result<Timestamp> {
        self.pd.clone().get_min_ts().await
    }

    /// Request garbage collection (GC) of the TiKV cluster.
    ///
    /// GC deletes MVCC records whose timestamp is lower than the given `safepoint`. We must guarantee
    ///  that all transactions started before this timestamp had committed. We can keep an active
    /// transaction list in application to decide which is the minimal start timestamp of them.
    ///
    /// For each key, the last mutation record (unless it's a deletion) before `safepoint` is retained.
    ///
    /// GC is performed by:
    /// 1. resolving all locks with timestamp <= `safepoint`
    /// 2. updating PD's known safepoint
    ///
    /// This is a simplified version of [GC in TiDB](https://docs.pingcap.com/tidb/stable/garbage-collection-overview).
    /// We skip the second step "delete ranges" which is an optimization for TiDB.
    pub async fn gc(&self, safepoint: Timestamp) -> Result<bool> {
        debug!("invoking transactional gc request");

        let options = ResolveLocksOptions::default();
        self.cleanup_locks(.., &safepoint, options).await?;

        // update safepoint to PD
        let res: bool = self
            .pd
            .clone()
            .update_safepoint(safepoint.version())
            .await?;
        if !res {
            info!("new safepoint != user-specified safepoint");
        }
        Ok(res)
    }

    pub async fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        debug!("invoking cleanup async commit locks");
        // scan all locks with ts <= safepoint
        let ctx = ResolveLocksContext::default();
        let backoff = Backoff::equal_jitter_backoff(100, 10000, 50);
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = self.request_context.apply_to(new_scan_lock_request(
            range,
            safepoint,
            options.batch_size,
        ));
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .cleanup_locks(ctx.clone(), options, backoff, self.keyspace)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    // For test.
    // Note: `batch_size` must be >= expected number of locks.
    #[cfg(feature = "integration-tests")]
    pub async fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<crate::proto::kvrpcpb::LockInfo>> {
        use crate::request::TruncateKeyspace;

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = self
            .request_context
            .apply_to(new_scan_lock_request(range, safepoint, batch_size));
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Cleans up all keys in a range and quickly reclaim disk space.
    ///
    /// The range can span over multiple regions.
    ///
    /// Note that the request will directly delete data from RocksDB, and all MVCC will be erased.
    ///
    /// This interface is intended for special scenarios that resemble operations like "drop table" or "drop database" in TiDB.
    pub async fn unsafe_destroy_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = self
            .request_context
            .apply_to(new_unsafe_destroy_range_request(range));
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    fn new_transaction(&self, timestamp: Timestamp, options: TransactionOptions) -> Transaction {
        Transaction::new(
            timestamp,
            self.pd.clone(),
            options,
            self.keyspace,
            self.request_context.clone(),
            self.txn_latches.clone(),
        )
    }
}
