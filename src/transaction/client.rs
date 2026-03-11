// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use log::debug;
use log::info;

use crate::backoff::{DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
use crate::config::Config;
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
use crate::transaction::requests::new_store_safe_ts_request_all;
use crate::transaction::BoundLockResolver;
use crate::transaction::LockResolver;
use crate::transaction::ResolveLocksContext;
use crate::transaction::Snapshot;
use crate::transaction::Transaction;
use crate::transaction::TransactionOptions;
use crate::Backoff;
use crate::BoundRange;
use crate::Result;

/// Protobuf-generated lock information returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::LockInfo as ProtoLockInfo;

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
pub struct Client<PdC: PdClient = PdRpcClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    resolve_locks_ctx: ResolveLocksContext,
}

impl<PdC: PdClient> Clone for Client<PdC> {
    fn clone(&self) -> Self {
        Self {
            pd: self.pd.clone(),
            keyspace: self.keyspace,
            resolve_locks_ctx: self.resolve_locks_ctx.clone(),
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
            Some(name) => {
                let keyspace = pd.load_keyspace(&name).await?;
                Keyspace::Enable {
                    keyspace_id: keyspace.id,
                }
            }
            None => Keyspace::Disable,
        };
        Ok(Client {
            pd,
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
        })
    }

    /// Create a transactional [`Client`] that uses API V2 without adding or removing any API V2
    /// keyspace/key-mode prefix, with a custom configuration.
    ///
    /// This is intended for **server-side embedding** use cases. `config.keyspace` must be unset.
    pub async fn new_with_config_api_v2_no_prefix<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Client> {
        if config.keyspace.is_some() {
            return Err(crate::Error::StringError(
                "config.keyspace must be unset when using api-v2-no-prefix mode".to_owned(),
            ));
        }

        debug!("creating new transactional client (api-v2-no-prefix)");
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        Ok(Client {
            pd,
            keyspace: Keyspace::ApiV2NoPrefix,
            resolve_locks_ctx: ResolveLocksContext::default(),
        })
    }

    /// Returns the TiKV cluster ID.
    pub fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    /// Returns a handle to the underlying PD RPC client.
    #[must_use]
    pub fn pd_client(&self) -> Arc<PdRpcClient> {
        self.pd.clone()
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Returns a [`LockResolver`] handle associated with this client.
    ///
    /// The returned resolver shares the resolve-lock caches with this client.
    #[must_use]
    pub fn lock_resolver(&self) -> LockResolver {
        LockResolver::new(self.resolve_locks_ctx.clone())
    }

    /// Returns a [`BoundLockResolver`] handle associated with this client.
    ///
    /// The returned resolver binds this client's PD client and keyspace. It also shares the
    /// resolve-lock caches with this client.
    #[must_use]
    pub fn bound_lock_resolver(&self) -> BoundLockResolver<PdC> {
        BoundLockResolver::new(
            self.pd.clone(),
            self.keyspace,
            self.resolve_locks_ctx.clone(),
        )
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
    pub async fn begin_optimistic(&self) -> Result<Transaction<PdC>> {
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
    pub async fn begin_pessimistic(&self) -> Result<Transaction<PdC>> {
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
    pub async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction<PdC>> {
        debug!("creating new customized transaction");
        let txn_scope = options
            .txn_scope_as_deref()
            .map(|txn_scope| txn_scope.to_owned());
        let timestamp = match txn_scope.as_deref() {
            None => self.current_timestamp().await?,
            Some(scope) => self.current_timestamp_with_txn_scope(scope).await?,
        };
        Ok(self.new_transaction(timestamp, options))
    }

    /// Create a new customized [`Transaction`] in the given transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator
    /// (`dc_location=""`). Otherwise `txn_scope` is passed through as PD `dc_location` to request
    /// a local TSO.
    pub async fn begin_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        options: TransactionOptions,
    ) -> Result<Transaction<PdC>> {
        let txn_scope = txn_scope.as_ref();
        debug!(
            "creating new customized transaction with txn_scope={}",
            txn_scope
        );
        let timestamp = self.current_timestamp_with_txn_scope(txn_scope).await?;
        Ok(self.new_transaction(timestamp, options.txn_scope(txn_scope)))
    }

    /// Create a new customized [`Transaction`] with an explicit start timestamp.
    ///
    /// This does not contact PD to fetch a timestamp. The provided `timestamp` is used as the
    /// transaction's start timestamp (`start_ts`).
    #[must_use]
    pub fn begin_with_start_timestamp(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Transaction<PdC> {
        debug!("creating new customized transaction with explicit start timestamp");
        self.new_transaction(timestamp, options)
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

    /// Retrieve the current [`Timestamp`] for the given transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator
    /// (`dc_location=""`). Otherwise `txn_scope` is passed through as PD `dc_location` to request
    /// a local TSO, matching client-go `CurrentTimestamp(txnScope)` behavior.
    pub async fn current_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
    ) -> Result<Timestamp> {
        let txn_scope = txn_scope.as_ref();
        let dc_location = if txn_scope.is_empty() || txn_scope == "global" {
            String::new()
        } else {
            txn_scope.to_owned()
        };
        PdClient::get_timestamp_with_dc_location(self.pd.clone(), dc_location).await
    }

    /// Get the cluster-wide minimum `safe_ts` across all TiKV stores.
    ///
    /// This value is a best-effort signal used by stale reads: if it is non-zero, reads at
    /// timestamps less than or equal to the returned `safe_ts` can be served from replicas (subject
    /// to per-region `safe_ts`).
    ///
    /// Returns `0` when the minimum safe-ts cannot be determined (for example, if any store is
    /// unreachable).
    pub async fn min_safe_ts(&self) -> Result<u64> {
        let req = new_store_safe_ts_request_all();
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
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

    /// Clean up locks in the given key range up to the provided `safepoint`.
    ///
    /// This is primarily intended for GC-like workflows. When `options.async_commit_only` is set,
    /// only async-commit locks are processed (other lock types are ignored).
    pub async fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        debug!(
            "invoking cleanup locks (async_commit_only={})",
            options.async_commit_only
        );
        // scan all locks with ts <= safepoint
        let ctx = self.resolve_locks_ctx.clone();
        let backoff = Backoff::equal_jitter_backoff(100, 10000, 50);
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, options.batch_size);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .preserve_shard()
            .cleanup_locks(ctx.clone(), options, backoff, self.keyspace)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    // Note: `batch_size` must be >= expected number of locks.
    pub async fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, batch_size);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This method retries until either all locks are resolved or the provided
    /// `backoff` is exhausted. The `timestamp` is used as the caller start
    /// timestamp when checking transaction status.
    pub async fn resolve_locks(
        &self,
        locks: Vec<ProtoLockInfo>,
        timestamp: Timestamp,
        backoff: Backoff,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let mut lock_resolver = self.bound_lock_resolver();
        let live_locks = lock_resolver
            .resolve_locks(
                locks.encode_keyspace(self.keyspace, KeyMode::Txn),
                timestamp,
                backoff,
                false,
            )
            .await?;
        Ok(live_locks.truncate_keyspace(self.keyspace))
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
        let req = new_unsafe_destroy_range_request(range);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    fn new_transaction(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Transaction<PdC> {
        Transaction::new_with_resolve_locks_ctx(
            timestamp,
            self.pd.clone(),
            options,
            self.keyspace,
            self.resolve_locks_ctx.clone(),
        )
    }
}

impl Client {
    /// Create a new [`Snapshot`](Snapshot) at the given [`Timestamp`](Timestamp).
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        debug!("creating new snapshot");
        Snapshot::new(self.new_transaction(timestamp, options.read_only()))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::request::Keyspace;
    use crate::timestamp::TimestampExt;
    use crate::transaction::HeartbeatOption;
    use crate::transaction::ResolveLocksContext;
    use crate::Backoff;
    use crate::Timestamp;
    use crate::TransactionOptions;

    use super::Client;

    #[tokio::test]
    async fn test_begin_with_start_timestamp_uses_provided_start_ts_without_pd_tso() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let start_ts = Timestamp {
            physical: 0,
            logical: 42,
            ..Default::default()
        };
        let start_version = start_ts.version();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, start_version);

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut transaction = client.begin_with_start_timestamp(
            start_ts,
            TransactionOptions::new_optimistic().drop_check(crate::CheckLevel::None),
        );
        assert_eq!(pd_client.get_timestamp_call_count(), 0);

        let value = transaction.get("k".to_owned()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        assert_eq!(pd_client.get_timestamp_call_count(), 0);
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_current_timestamp_with_txn_scope_maps_global_and_empty_to_global_dc_location() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let _ts = client.current_timestamp_with_txn_scope("").await.unwrap();
        let _ts = client
            .current_timestamp_with_txn_scope("global")
            .await
            .unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["".to_owned(), "".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_begin_with_txn_scope_uses_pd_dc_location() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 0);

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut transaction = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic().drop_check(crate::CheckLevel::None),
            )
            .await
            .unwrap();
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned()]
        );

        let value = transaction.get("k".to_owned()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_begin_with_options_respects_txn_scope() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let _txn = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .txn_scope("dc1")
                    .drop_check(crate::CheckLevel::None),
            )
            .await
            .unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_begin_with_txn_scope_propagates_to_commit_tso_requests() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_local_txn_scope_disables_async_commit_and_one_pc() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert!(
                    !req.use_async_commit,
                    "local scope must not use async-commit"
                );
                assert!(!req.try_one_pc, "local scope must not use 1pc");
                assert_eq!(
                    req.min_commit_ts, 0,
                    "local scope must not seed min_commit_ts"
                );
                assert_eq!(
                    req.max_commit_ts, 0,
                    "local scope must not set max_commit_ts"
                );
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic()
                    .use_async_commit()
                    .try_one_pc()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);

        assert_eq!(
            pd_client.get_timestamp_call_count(),
            2,
            "local scope must not fetch extra PD TSO during prewrite"
        );
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_local_txn_scope_uses_dc_location_for_pessimistic_for_update_ts() {
        let start_version = 7;
        let expected_for_update_ts = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(req.for_update_ts, expected_for_update_ts);
                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_pessimistic()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.lock_keys(vec!["k".to_owned()]).await.unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_delegates_to_bound_lock_resolver() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(_req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 50,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 50);
                    assert!(
                        req.keys.is_empty(),
                        "non-lite resolve should not send key list"
                    );
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let client = Client {
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
        };

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let live_locks = client
            .resolve_locks(
                vec![lock],
                Timestamp::from_version(42),
                Backoff::no_backoff(),
            )
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);

        let mut lock_resolver = client.lock_resolver();
        assert!(lock_resolver.resolving().await.is_empty());
    }
}
