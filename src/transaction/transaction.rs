// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter;
use std::sync::atomic;
use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::Instant;

use derive_new::new;
use fail::fail_point;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use tokio::time::Duration;

use super::requests::CollectPessimisticLock;
use super::ReadLockTracker;
use super::ResolveLocksContext;
use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::kv::HexRepr;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::Collect;
use crate::request::CollectError;
use crate::request::CollectSingle;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::Merge;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::RetryOptions;
use crate::request::TruncateKeyspace;
use crate::timestamp::TimestampExt;
use crate::transaction::buffer::Buffer;
use crate::transaction::lowering::*;
use crate::BoundRange;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::Error;
use crate::IsolationLevel;
use crate::Key;
use crate::KvPair;
use crate::ReplicaReadAdjuster;
use crate::ReplicaReadType;
use crate::Result;
use crate::Value;

/// Hook for validating schema versions during commit.
///
/// This mirrors the client-go v2 `SchemaLeaseChecker` concept. When configured via
/// [`Transaction::set_schema_ver`] and [`Transaction::set_schema_lease_checker`], async-commit/1PC
/// will invoke this check before calculating `max_commit_ts`.
pub trait SchemaLeaseChecker: Send + Sync {
    /// Check whether the schema has changed between the transaction's start schema version and the
    /// schema version at `txn_ts`.
    fn check_by_schema_ver(&self, txn_ts: Timestamp, start_schema_ver: i64) -> Result<()>;
}

/// An undo-able set of actions on the dataset.
///
/// Create a transaction using a [`TransactionClient`](crate::TransactionClient), then run actions
/// (such as `get`, or `put`) on the transaction. Reads are executed immediately, writes are
/// buffered locally. Once complete, `commit` the transaction. Behind the scenes, the client will
/// perform a two phase commit and return success as soon as the writes are guaranteed to be
/// committed (some finalisation may continue in the background after the return, but no data can be
/// lost).
///
/// TiKV transactions use multi-version concurrency control. All reads logically happen at the start
/// of the transaction (at the start timestamp, `start_ts`). Once a transaction is commited, a
/// its writes atomically become visible to other transactions at (logically) the commit timestamp.
///
/// In other words, a transaction can read data that was committed at `commit_ts` < its `start_ts`,
/// and its writes are readable by transactions with `start_ts` >= its `commit_ts`.
///
/// Mutations are buffered locally and sent to the TiKV cluster at the time of commit.
/// In a pessimistic transaction, all write operations and `xxx_for_update` operations will immediately
/// acquire locks from TiKV. Such a lock blocks other transactions from writing to that key.
/// A lock exists until the transaction is committed or rolled back, or the lock reaches its time to
/// live (TTL).
///
/// For details, the [SIG-Transaction](https://github.com/tikv/sig-transaction)
/// provides materials explaining designs and implementations of TiKV transactions.
///
/// # Examples
///
/// ```rust,no_run
/// # use tikv_client::{Config, TransactionClient};
/// # use futures::prelude::*;
/// # futures::executor::block_on(async {
/// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
/// let mut txn = client.begin_optimistic().await.unwrap();
/// let foo = txn.get("foo".to_owned()).await.unwrap().unwrap();
/// txn.put("bar".to_owned(), foo).await.unwrap();
/// txn.commit().await.unwrap();
/// # });
/// ```
pub struct Transaction<PdC: PdClient = PdRpcClient> {
    status: Arc<AtomicU8>,
    timestamp: Timestamp,
    buffer: Buffer,
    read_lock_tracker: ReadLockTracker,
    rpc: Arc<PdC>,
    resolve_locks_ctx: ResolveLocksContext,
    options: TransactionOptions,
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    schema_ver: Option<i64>,
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
    keyspace: Keyspace,
    is_heartbeat_started: bool,
    start_instant: Instant,
}

#[derive(Clone, Default)]
struct SnapshotReadContext {
    replica_read: ReplicaReadType,
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    stale_read: bool,
    not_fill_cache: bool,
    task_id: u64,
    max_execution_duration_ms: u64,
    busy_threshold_ms: u32,
    priority: CommandPriority,
    isolation_level: IsolationLevel,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_name: Option<String>,
    request_source: Option<String>,
}

fn normalize_busy_threshold_ms(threshold: Duration) -> u32 {
    let millis = threshold.as_millis();
    if millis == 0 || millis > u128::from(u32::MAX) {
        0
    } else {
        millis as u32
    }
}

impl<PdC: PdClient> Transaction<PdC> {
    pub(crate) fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
    ) -> Transaction<PdC> {
        Self::new_with_resolve_locks_ctx(
            timestamp,
            rpc,
            options,
            keyspace,
            ResolveLocksContext::default(),
        )
    }

    pub(crate) fn new_with_resolve_locks_ctx(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
        resolve_locks_ctx: ResolveLocksContext,
    ) -> Transaction<PdC> {
        let status = if options.read_only {
            TransactionStatus::ReadOnly
        } else {
            TransactionStatus::Active
        };
        Transaction {
            status: Arc::new(AtomicU8::new(status as u8)),
            timestamp,
            buffer: Buffer::new(options.is_pessimistic()),
            read_lock_tracker: ReadLockTracker::default(),
            rpc,
            resolve_locks_ctx,
            options,
            replica_read_adjuster: None,
            schema_ver: None,
            schema_lease_checker: None,
            keyspace,
            is_heartbeat_started: false,
            start_instant: std::time::Instant::now(),
        }
    }

    fn snapshot_read_context(&self) -> SnapshotReadContext {
        if self.options.read_only {
            SnapshotReadContext {
                replica_read: self.options.replica_read,
                replica_read_adjuster: self.replica_read_adjuster.clone(),
                stale_read: self.options.stale_read,
                not_fill_cache: self.options.not_fill_cache,
                task_id: self.options.task_id,
                max_execution_duration_ms: self.options.max_execution_duration_ms,
                busy_threshold_ms: self.options.busy_threshold_ms,
                priority: self.options.priority,
                isolation_level: self.options.isolation_level,
                resource_group_tag: self.options.resource_group_tag.clone(),
                resource_group_name: self.options.resource_group_name.clone(),
                request_source: self.options.request_source.clone(),
            }
        } else {
            SnapshotReadContext {
                priority: self.options.priority,
                resource_group_tag: self.options.resource_group_tag.clone(),
                resource_group_name: self.options.resource_group_name.clone(),
                request_source: self.options.request_source.clone(),
                ..SnapshotReadContext::default()
            }
        }
    }

    fn apply_snapshot_read_context(ctx: &mut Option<kvrpcpb::Context>, opts: SnapshotReadContext) {
        let ctx = ctx.get_or_insert_with(kvrpcpb::Context::default);
        ctx.not_fill_cache = opts.not_fill_cache;
        ctx.task_id = opts.task_id;
        ctx.max_execution_duration_ms = opts.max_execution_duration_ms;
        ctx.busy_threshold_ms = opts.busy_threshold_ms;
        ctx.priority = opts.priority as i32;
        ctx.isolation_level = opts.isolation_level as i32;
        ctx.resource_group_tag = opts.resource_group_tag.unwrap_or_default();
        ctx.resource_control_context =
            opts.resource_group_name
                .map(|resource_group_name| kvrpcpb::ResourceControlContext {
                    resource_group_name,
                    ..Default::default()
                });
        if let Some(request_source) = opts.request_source {
            ctx.request_source = request_source;
        }
        if opts.stale_read {
            ctx.stale_read = true;
            ctx.replica_read = false;
        } else {
            ctx.stale_read = false;
            ctx.replica_read = opts.replica_read.is_follower_read();
        }
    }

    /// Set replica read behavior.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_replica_read(&mut self, read_type: ReplicaReadType) {
        self.options.replica_read = read_type;
    }

    /// Set a replica read adjuster for point/batch gets.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_replica_read_adjuster<F>(&mut self, adjuster: F)
    where
        F: Fn(usize) -> ReplicaReadType + Send + Sync + 'static,
    {
        self.replica_read_adjuster = Some(Arc::new(adjuster));
    }

    /// Clear the replica read adjuster.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn clear_replica_read_adjuster(&mut self) {
        self.replica_read_adjuster = None;
    }

    /// Set the busy threshold for read requests.
    ///
    /// This maps to client-go `KVSnapshot.SetLoadBasedReplicaReadThreshold` and writes to
    /// `kvrpcpb::Context.busy_threshold_ms`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_load_based_replica_read_threshold(&mut self, threshold: Duration) {
        self.options.busy_threshold_ms = normalize_busy_threshold_ms(threshold);
    }

    /// Set the schema version used for schema validity checks during commit.
    ///
    /// The schema validity check is only performed when a schema lease checker is also configured
    /// via [`Transaction::set_schema_lease_checker`].
    pub fn set_schema_ver(&mut self, schema_ver: i64) {
        self.schema_ver = Some(schema_ver);
    }

    /// Clear the configured schema version.
    pub fn clear_schema_ver(&mut self) {
        self.schema_ver = None;
    }

    /// Set a schema lease checker used to validate schema changes during commit.
    ///
    /// The checker is only consulted when a schema version is also configured via
    /// [`Transaction::set_schema_ver`].
    pub fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        self.schema_lease_checker = Some(checker);
    }

    /// Clear the configured schema lease checker.
    pub fn clear_schema_lease_checker(&mut self) {
        self.schema_lease_checker = None;
    }

    /// Create a new 'get' request
    ///
    /// Once resolved this request will result in the fetching of the value associated with the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Option<Value> = txn.get(key).await.unwrap();
    /// # });
    /// ```
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking transactional get request");
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let mut snapshot_ctx = self.snapshot_read_context();
        if snapshot_ctx.replica_read.is_follower_read() {
            if let Some(adjuster) = snapshot_ctx.replica_read_adjuster.as_ref() {
                snapshot_ctx.replica_read = (adjuster)(1);
            }
        }
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let replica_read = snapshot_ctx.replica_read;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();

        self.buffer
            .get_or_else(key, |key| async move {
                let mut request = new_get_request(key, timestamp.clone());
                Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);

                let plan_builder = PlanBuilder::new(rpc, keyspace, request).resolve_lock_for_read(
                    resolve_locks_ctx,
                    timestamp,
                    retry_options.lock_backoff,
                    keyspace,
                    lock_tracker,
                );
                let plan_builder = if replica_read.is_follower_read()
                    || enable_load_based_replica_read
                {
                    plan_builder
                        .retry_multi_region_with_replica_read(DEFAULT_REGION_BACKOFF, replica_read)
                } else {
                    plan_builder.retry_multi_region(DEFAULT_REGION_BACKOFF)
                };
                let plan = plan_builder
                    .merge(CollectSingle)
                    .post_process_default()
                    .plan();
                plan.execute().await
            })
            .await
    }

    /// Create a `get for update` request.
    ///
    /// The request reads and "locks" a key. It is similar to `SELECT ... FOR
    /// UPDATE` in TiDB, and has different behavior in optimistic and
    /// pessimistic transactions.
    ///
    /// # Optimistic transaction
    ///
    /// It reads at the "start timestamp" and caches the value, just like normal
    /// get requests. The lock is written in prewrite and commit, so it cannot
    /// prevent concurrent transactions from writing the same key, but can only
    /// prevent itself from committing.
    ///
    /// # Pessimistic transaction
    ///
    /// It reads at the "current timestamp" and thus does not cache the value.
    /// So following read requests won't be affected by the `get_for_udpate`.
    /// A lock will be acquired immediately with this request, which prevents
    /// concurrent transactions from mutating the keys.
    ///
    /// The "current timestamp" (also called `for_update_ts` of the request) is fetched from PD.
    ///
    /// Note: The behavior of this command under pessimistic transaction does not follow snapshot.
    /// It reads the latest value (using current timestamp), and the value is not cached in the
    /// local buffer. So normal `get`-like commands after `get_for_update` will not be influenced,
    /// they still read values at the transaction's `start_ts`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Value = txn.get_for_update(key).await.unwrap().unwrap();
    /// // now the key "TiKV" is locked, other transactions cannot modify it
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking transactional get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let key = key.into();
            self.lock_keys(iter::once(key.clone())).await?;
            self.get(key).await
        } else {
            let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
            let mut pairs = self.pessimistic_lock(iter::once(key), true).await?;
            debug_assert!(pairs.len() <= 1);
            match pairs.pop() {
                Some(pair) => Ok(Some(pair.1)),
                None => Ok(None),
            }
        }
    }

    /// Check whether a key exists.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let exists = txn.key_exists("k1".to_owned()).await.unwrap();
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        debug!("invoking transactional key_exists request");
        Ok(self.get(key).await?.is_some())
    }

    /// Create a new 'batch get' request.
    ///
    /// Once resolved this request will result in the fetching of the values associated with the
    /// given keys.
    ///
    /// Non-existent entries will not appear in the result. The order of the keys is not retained in
    /// the result.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let result: HashMap<Key, Value> = txn
    ///     .batch_get(keys)
    ///     .await
    ///     .unwrap()
    ///     .map(|pair| (pair.0, pair.1))
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional batch_get request");
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keys = keys
            .into_iter()
            .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = self.options.retry_options.clone();
        let snapshot_ctx = self.snapshot_read_context();
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();

        self.buffer
            .batch_get_or_else(keys, move |keys| {
                let keys: Vec<Key> = keys.collect();
                let key_count = keys.len();
                let mut snapshot_ctx = snapshot_ctx.clone();
                if snapshot_ctx.replica_read.is_follower_read() {
                    if let Some(adjuster) = snapshot_ctx.replica_read_adjuster.as_ref() {
                        snapshot_ctx.replica_read = (adjuster)(key_count);
                    }
                }
                let replica_read = snapshot_ctx.replica_read;

                async move {
                    let mut request = crate::transaction::requests::new_batch_get_request(
                        keys.into_iter().map(Into::into).collect(),
                        timestamp.version(),
                    );
                    Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);

                    let plan_builder = PlanBuilder::new(rpc, keyspace, request)
                        .resolve_lock_for_read(
                            resolve_locks_ctx,
                            timestamp,
                            retry_options.lock_backoff,
                            keyspace,
                            lock_tracker,
                        );
                    let plan_builder =
                        if replica_read.is_follower_read() || enable_load_based_replica_read {
                            plan_builder.retry_multi_region_with_replica_read(
                                retry_options.region_backoff,
                                replica_read,
                            )
                        } else {
                            plan_builder.retry_multi_region(retry_options.region_backoff)
                        };
                    let plan = plan_builder.merge(Collect).plan();
                    plan.execute()
                        .await
                        .map(|r| r.into_iter().map(Into::into).collect())
                }
            })
            .await
            .map(move |pairs| pairs.map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Create a new 'batch get for update' request.
    ///
    /// Similar to [`get_for_update`](Transaction::get_for_update), but it works
    /// for a batch of keys.
    ///
    /// Non-existent entries will not appear in the result. The order of the
    /// keys is not retained in the result.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient, KvPair};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let keys = vec!["foo".to_owned(), "bar".to_owned()];
    /// let result: Vec<KvPair> = txn
    ///     .batch_get_for_update(keys)
    ///     .await
    ///     .unwrap();
    /// // now "foo" and "bar" are both locked
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get_for_update(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking transactional batch_get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let keys: Vec<Key> = keys.into_iter().map(|k| k.into()).collect();
            self.lock_keys(keys.clone()).await?;
            Ok(self.batch_get(keys).await?.collect())
        } else {
            let keyspace = self.keyspace;
            let keys = keys
                .into_iter()
                .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
            let pairs = self
                .pessimistic_lock(keys, true)
                .await?
                .truncate_keyspace(keyspace);
            Ok(pairs)
        }
    }

    /// Create a new 'scan' request.
    ///
    /// Once resolved this request will result in a `Vec` of all key-value pairs that lie in the
    /// specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, KvPair, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let result: Vec<KvPair> = txn
    ///     .scan(key1..key2, 10)
    ///     .await
    ///     .unwrap()
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional scan request");
        self.scan_inner(range, limit, false, false).await
    }

    /// Create a new 'scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` keys are returned, ordered by key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, KvPair, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let result: Vec<Key> = txn
    ///     .scan_keys(key1..key2, 10)
    ///     .await
    ///     .unwrap()
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking transactional scan_keys request");
        Ok(self
            .scan_inner(range, limit, true, false)
            .await?
            .map(KvPair::into_key))
    }

    /// Create a 'scan_reverse' request.
    ///
    /// Similar to [`scan`](Transaction::scan), but scans in the reverse direction.
    pub async fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional scan_reverse request");
        self.scan_inner(range, limit, false, true).await
    }

    /// Create a 'scan_keys_reverse' request.
    ///
    /// Similar to [`scan`](Transaction::scan_keys), but scans in the reverse direction.
    pub async fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking transactional scan_keys_reverse request");
        Ok(self
            .scan_inner(range, limit, true, true)
            .await?
            .map(KvPair::into_key))
    }

    /// Sets the value associated with the given key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.put(key, val);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        trace!("invoking transactional put request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        if self.is_pessimistic() {
            self.pessimistic_lock(iter::once(key.clone()), false)
                .await?;
        }
        self.buffer.put(key, value.into());
        Ok(())
    }

    /// Inserts the value associated with the given key.
    ///
    /// Similar to [`put'], but it has an additional constraint that the key should not exist
    /// before this operation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.insert(key, val);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn insert(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        debug!("invoking transactional insert request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        if self.buffer.get(&key).is_some() {
            return Err(Error::DuplicateKeyInsertion);
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once((key.clone(), kvrpcpb::Assertion::NotExist)),
                false,
            )
            .await?;
        }
        self.buffer.insert(key, value.into());
        Ok(())
    }

    /// Deletes the given key and its value from the database.
    ///
    /// Deleting a non-existent key will not result in an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// txn.delete(key);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        debug!("invoking transactional delete request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        if self.is_pessimistic() {
            self.pessimistic_lock(iter::once(key.clone()), false)
                .await?;
        }
        self.buffer.delete(key);
        Ok(())
    }

    /// Batch mutate the database.
    ///
    /// Only `Put` and `Delete` are supported.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, TransactionClient, transaction::Mutation};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let mutations = vec![
    ///     Mutation::Delete("k0".to_owned().into()),
    ///     Mutation::Put("k1".to_owned().into(), b"v1".to_vec()),
    /// ];
    /// txn.batch_mutate(mutations).await.unwrap();
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_mutate(
        &mut self,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<()> {
        debug!("invoking transactional batch mutate request");
        self.check_allow_operation().await?;
        let mutations: Vec<Mutation> = mutations
            .into_iter()
            .map(|mutation| mutation.encode_keyspace(self.keyspace, KeyMode::Txn))
            .collect();
        if self.is_pessimistic() {
            self.pessimistic_lock(mutations.iter().map(|m| m.key().clone()), false)
                .await?;
            for m in mutations {
                self.buffer.mutate(m);
            }
        } else {
            for m in mutations.into_iter() {
                self.buffer.mutate(m);
            }
        }
        Ok(())
    }

    /// Lock the given keys without mutating their values.
    ///
    /// In optimistic mode, write conflicts are not checked until commit.
    /// So use this command to indicate that
    /// "I do not want to commit if the value associated with this key has been modified".
    /// It's useful to avoid the *write skew* anomaly.
    ///
    /// In pessimistic mode, it is similar to [`batch_get_for_update`](Transaction::batch_get_for_update),
    /// except that it does not read values.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// txn.lock_keys(vec!["TiKV".to_owned(), "Rust".to_owned()]);
    /// // ... Do some actions.
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn lock_keys(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys request");
        self.check_allow_operation().await?;
        let keyspace = self.keyspace;
        let keys = keys
            .into_iter()
            .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
        match self.options.kind {
            TransactionKind::Optimistic => {
                for key in keys {
                    self.buffer.lock(key);
                }
            }
            TransactionKind::Pessimistic(_) => {
                self.pessimistic_lock(keys, false).await?;
            }
        }
        Ok(())
    }

    /// Commits the actions of the transaction. On success, we return the commit timestamp (or
    /// `None` if there was nothing to commit).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, Timestamp, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// // ... Do some actions.
    /// let result: Timestamp = txn.commit().await.unwrap().unwrap();
    /// # });
    /// ```
    pub async fn commit(&mut self) -> Result<Option<Timestamp>> {
        debug!("commiting transaction");
        if !self.transit_status(
            |status| {
                matches!(
                    status,
                    TransactionStatus::StartedCommit | TransactionStatus::Active
                )
            },
            TransactionStatus::StartedCommit,
        ) {
            return Err(Error::OperationAfterCommitError);
        }

        let primary_key = self.buffer.get_primary_key();
        let mutations = self.buffer.to_proto_mutations();
        if mutations.is_empty() {
            assert!(primary_key.is_none());
            return Ok(None);
        }

        self.start_auto_heartbeat().await;

        let primary_key = match &self.options.kind {
            TransactionKind::Optimistic => None,
            TransactionKind::Pessimistic(_) => primary_key,
        };

        let mut committer = Committer::new(
            primary_key,
            mutations,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.keyspace,
            self.buffer.get_write_size() as u64,
            self.start_instant,
        );
        committer.resolve_locks_ctx = self.resolve_locks_ctx.clone();
        committer.schema_ver = self.schema_ver;
        committer.schema_lease_checker = self.schema_lease_checker.clone();
        let res = committer.commit().await;

        if res.is_ok() {
            self.set_status(TransactionStatus::Committed);
        }
        res
    }

    /// Rollback the transaction.
    ///
    /// If it succeeds, all mutations made by this transaction will be discarded.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, Timestamp, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// // ... Do some actions.
    /// txn.rollback().await.unwrap();
    /// # });
    /// ```
    pub async fn rollback(&mut self) -> Result<()> {
        debug!("rolling back transaction");
        if !self.transit_status(
            |status| {
                matches!(
                    status,
                    TransactionStatus::StartedRollback
                        | TransactionStatus::Active
                        | TransactionStatus::StartedCommit
                )
            },
            TransactionStatus::StartedRollback,
        ) {
            return Err(Error::OperationAfterCommitError);
        }

        let primary_key = self.buffer.get_primary_key();
        let mutations = self.buffer.to_proto_mutations();
        let mut committer = Committer::new(
            primary_key,
            mutations,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.keyspace,
            self.buffer.get_write_size() as u64,
            self.start_instant,
        );
        committer.resolve_locks_ctx = self.resolve_locks_ctx.clone();
        committer.schema_ver = self.schema_ver;
        committer.schema_lease_checker = self.schema_lease_checker.clone();
        let res = committer.rollback().await;

        if res.is_ok() {
            self.set_status(TransactionStatus::Rolledback);
        }
        res
    }

    /// Get the start timestamp of this transaction.
    pub fn start_timestamp(&self) -> Timestamp {
        self.timestamp.clone()
    }

    /// Send a heart beat message to keep the transaction alive on the server and update its TTL.
    ///
    /// Returns the TTL set on the transaction's locks by TiKV.
    #[doc(hidden)]
    pub async fn send_heart_beat(&mut self) -> Result<u64> {
        debug!("sending heart_beat");
        self.check_allow_operation().await?;
        let primary_key = match self.buffer.get_primary_key() {
            Some(k) => k,
            None => return Err(Error::NoPrimaryKey),
        };
        let request = new_heart_beat_request(
            self.timestamp.clone(),
            primary_key,
            self.start_instant.elapsed().as_millis() as u64 + MAX_TTL,
        );
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .resolve_lock_in_context(
                self.resolve_locks_ctx.clone(),
                self.timestamp.clone(),
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await
    }

    async fn scan_inner(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
        key_only: bool,
        reverse: bool,
    ) -> Result<impl Iterator<Item = KvPair>> {
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let snapshot_ctx = self.snapshot_read_context();
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let replica_read = snapshot_ctx.replica_read;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();

        self.buffer
            .scan_and_fetch(
                range,
                limit,
                !key_only,
                reverse,
                move |new_range, new_limit| async move {
                    let mut request = new_scan_request(
                        new_range,
                        timestamp.clone(),
                        new_limit,
                        key_only,
                        reverse,
                    );
                    Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);

                    let plan_builder = PlanBuilder::new(rpc, keyspace, request)
                        .resolve_lock_for_read(
                            resolve_locks_ctx,
                            timestamp,
                            retry_options.lock_backoff,
                            keyspace,
                            lock_tracker,
                        );
                    let plan_builder =
                        if replica_read.is_follower_read() || enable_load_based_replica_read {
                            plan_builder.retry_multi_region_with_replica_read(
                                retry_options.region_backoff,
                                replica_read,
                            )
                        } else {
                            plan_builder.retry_multi_region(retry_options.region_backoff)
                        };
                    let plan = plan_builder.merge(Collect).plan();
                    plan.execute()
                        .await
                        .map(|r| r.into_iter().map(Into::into).collect())
                },
            )
            .await
            .map(move |pairs| pairs.map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Pessimistically lock the keys, and optionally retrieve corresponding values.
    /// If a key does not exist, the corresponding pair will not appear in the result.
    ///
    /// Once resolved it acquires locks on the keys in TiKV.
    /// A lock prevents other transactions from mutating the entry until it is released.
    ///
    /// # Panics
    ///
    /// Only valid for pessimistic transactions, panics if called on an optimistic transaction.
    async fn pessimistic_lock(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
    ) -> Result<Vec<KvPair>> {
        debug!("acquiring pessimistic lock");
        assert!(
            matches!(self.options.kind, TransactionKind::Pessimistic(_)),
            "`pessimistic_lock` is only valid to use with pessimistic transactions"
        );

        let mut locks: Vec<(Key, kvrpcpb::Assertion)> = keys
            .into_iter()
            .map(|lock| {
                let assertion = lock.assertion();
                let key = lock.key();
                (key, assertion)
            })
            .collect();
        if locks.is_empty() {
            return Ok(vec![]);
        }

        // Match client-go: sort and deduplicate keys to keep the lock request deterministic and
        // avoid locking the same key twice in a single call (which can also affect is_first_lock).
        locks.sort_by(|a, b| a.0.cmp(&b.0));
        locks.dedup_by(|a, b| {
            if a.0 == b.0 {
                a.1 = a.1.max(b.1);
                true
            } else {
                false
            }
        });

        // we do not set the primary key here, because pessimistic lock request
        // can fail, in which case the keys may not be part of the transaction.
        let existing_primary_key = self.buffer.get_primary_key();
        let is_first_lock = existing_primary_key.is_none() && locks.len() == 1;
        let primary_lock = existing_primary_key.unwrap_or_else(|| locks[0].0.clone());
        let for_update_ts = self.rpc.clone().get_timestamp().await?;
        self.options.push_for_update_ts(for_update_ts.clone());
        let elapsed = self.start_instant.elapsed().as_millis() as u64;
        let lock_ttl = elapsed.saturating_add(MAX_TTL);
        let lock_wait_start = Instant::now();
        let primary_in_request = locks.iter().any(|(key, _)| key == &primary_lock);

        let pairs = if primary_in_request && locks.len() > 1 {
            let primary_region = self.rpc.region_for_key(&primary_lock).await?;
            let mut primary_locks = Vec::new();
            let mut secondary_locks = Vec::new();
            for lock in &locks {
                if primary_region.contains(&lock.0) {
                    primary_locks.push(lock.clone());
                } else {
                    secondary_locks.push(lock.clone());
                }
            }

            if primary_locks.is_empty() || secondary_locks.is_empty() {
                let request = new_pessimistic_lock_request(
                    locks.clone().into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                self.execute_pessimistic_lock_request(
                    request,
                    for_update_ts.clone(),
                    need_value,
                    lock_wait_start,
                )
                .await?
            } else {
                let primary_request = new_pessimistic_lock_request(
                    primary_locks.into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                let primary_request_keys = primary_request
                    .mutations
                    .iter()
                    .map(|mutation| Key::from(mutation.key.clone()))
                    .collect::<Vec<_>>();
                let primary_pairs = self
                    .execute_pessimistic_lock_request(
                        primary_request,
                        for_update_ts.clone(),
                        need_value,
                        lock_wait_start,
                    )
                    .await?;

                let secondary_request = new_pessimistic_lock_request(
                    secondary_locks.into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                let secondary_pairs = match self
                    .execute_pessimistic_lock_request(
                        secondary_request,
                        for_update_ts.clone(),
                        need_value,
                        lock_wait_start,
                    )
                    .await
                {
                    Ok(pairs) => pairs,
                    Err(err) => {
                        self.pessimistic_lock_rollback(
                            primary_request_keys.iter().cloned(),
                            self.timestamp.clone(),
                            for_update_ts,
                        )
                        .await?;
                        return Err(err);
                    }
                };

                let mut pairs = primary_pairs;
                pairs.extend(secondary_pairs);
                pairs
            }
        } else {
            let request = new_pessimistic_lock_request(
                locks.clone().into_iter(),
                primary_lock.clone(),
                self.timestamp.clone(),
                lock_ttl,
                for_update_ts.clone(),
                need_value,
                is_first_lock,
            );
            self.execute_pessimistic_lock_request(
                request,
                for_update_ts.clone(),
                need_value,
                lock_wait_start,
            )
            .await?
        };

        self.buffer.primary_key_or(&primary_lock);
        self.start_auto_heartbeat().await;
        for (key, _assertion) in locks {
            self.buffer.lock(key);
        }
        Ok(pairs)
    }

    async fn execute_pessimistic_lock_request(
        &mut self,
        mut request: kvrpcpb::PessimisticLockRequest,
        for_update_ts: Timestamp,
        need_value: bool,
        lock_wait_start: Instant,
    ) -> Result<Vec<KvPair>> {
        fn collect_lock_errors(error: &Error, locks: &mut Vec<kvrpcpb::LockInfo>) -> bool {
            match error {
                Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                    let mut has_non_lock_error = false;
                    for err in errors {
                        has_non_lock_error |= collect_lock_errors(err, locks);
                    }
                    has_non_lock_error
                }
                Error::ResolveLockError(live_locks) => {
                    locks.extend(live_locks.clone());
                    false
                }
                Error::KeyError(key_error) => {
                    if let Some(lock) = &key_error.locked {
                        locks.push(lock.clone());
                        false
                    } else {
                        true
                    }
                }
                _ => true,
            }
        }

        let lock_wait_timeout = self.options.lock_wait_timeout;

        loop {
            request.wait_timeout = lock_wait_timeout.effective_wait_timeout_ms(lock_wait_start);

            let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request.clone())
                .preserve_shard()
                .retry_multi_region_preserve_results(
                    self.options.retry_options.region_backoff.clone(),
                )
                .plan();
            let results = plan.execute().await?;

            let mut success = Vec::new();
            let mut errors = Vec::new();
            for result in results {
                match result {
                    Ok(result) => success.push(result),
                    Err(err) => errors.push(err),
                }
            }

            if errors.is_empty() {
                return CollectPessimisticLock::new(need_value)
                    .merge(success.into_iter().map(Ok).collect());
            }

            let success_keys = success
                .into_iter()
                .flat_map(|crate::request::ResponseWithShard(_resp, mutations)| {
                    mutations
                        .into_iter()
                        .map(|mutation| Key::from(mutation.key))
                })
                .collect::<Vec<_>>();
            if !success_keys.is_empty() {
                self.pessimistic_lock_rollback(
                    success_keys.into_iter(),
                    self.timestamp.clone(),
                    for_update_ts.clone(),
                )
                .await?;
            }

            let mut locks = Vec::new();
            let mut first_non_lock_error = None;
            for (idx, err) in errors.iter().enumerate() {
                if collect_lock_errors(err, &mut locks) && first_non_lock_error.is_none() {
                    first_non_lock_error = Some(idx);
                }
            }

            if let Some(idx) = first_non_lock_error {
                return Err(errors.swap_remove(idx));
            }
            if locks.is_empty() {
                return Err(errors.swap_remove(0));
            }

            let resolve_result = super::resolve_locks_with_options(
                self.resolve_locks_ctx.clone(),
                locks,
                Timestamp::default(),
                self.rpc.clone(),
                self.keyspace,
                true,
            )
            .await?;

            if resolve_result.live_locks.is_empty() {
                continue;
            }

            if lock_wait_timeout.is_no_wait() {
                return Err(Error::StringError(
                    "lock acquire failed and no wait is set".to_owned(),
                ));
            }
            if lock_wait_timeout.is_timed_out(lock_wait_start) {
                return Err(Error::StringError("lock wait timeout".to_owned()));
            }
        }
    }

    /// Rollback pessimistic lock
    async fn pessimistic_lock_rollback(
        &mut self,
        keys: impl Iterator<Item = Key>,
        start_version: Timestamp,
        for_update_ts: Timestamp,
    ) -> Result<()> {
        debug!("rollback pessimistic lock");

        let keys: Vec<_> = keys.into_iter().collect();
        if keys.is_empty() {
            return Ok(());
        }

        let req = new_pessimistic_rollback_request(
            keys.clone().into_iter(),
            start_version.clone(),
            for_update_ts,
        );
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .resolve_lock_in_context(
                self.resolve_locks_ctx.clone(),
                start_version,
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await?;

        for key in keys {
            self.buffer.unlock(&key);
        }
        Ok(())
    }

    /// Checks if the transaction can perform arbitrary operations.
    async fn check_allow_operation(&self) -> Result<()> {
        match self.get_status() {
            TransactionStatus::ReadOnly | TransactionStatus::Active => Ok(()),
            TransactionStatus::Committed
            | TransactionStatus::Rolledback
            | TransactionStatus::StartedCommit
            | TransactionStatus::StartedRollback
            | TransactionStatus::Dropped => Err(Error::OperationAfterCommitError),
        }
    }

    fn is_pessimistic(&self) -> bool {
        matches!(self.options.kind, TransactionKind::Pessimistic(_))
    }

    async fn start_auto_heartbeat(&mut self) {
        debug!("starting auto_heartbeat");
        if !self.options.heartbeat_option.is_auto_heartbeat() || self.is_heartbeat_started {
            return;
        }
        self.is_heartbeat_started = true;

        let status = self.status.clone();
        let primary_key = self
            .buffer
            .get_primary_key()
            .expect("Primary key should exist");
        let start_ts = self.timestamp.clone();
        let region_backoff = self.options.retry_options.region_backoff.clone();
        let rpc = self.rpc.clone();
        let heartbeat_interval = match self.options.heartbeat_option {
            HeartbeatOption::NoHeartbeat => DEFAULT_HEARTBEAT_INTERVAL,
            HeartbeatOption::FixedTime(heartbeat_interval) => heartbeat_interval,
        };
        let start_instant = self.start_instant;
        let keyspace = self.keyspace;

        let heartbeat_task = async move {
            loop {
                tokio::time::sleep(heartbeat_interval).await;
                {
                    let status: TransactionStatus = status.load(atomic::Ordering::Acquire).into();
                    if matches!(
                        status,
                        TransactionStatus::Rolledback
                            | TransactionStatus::Committed
                            | TransactionStatus::Dropped
                    ) {
                        break;
                    }
                }
                let request = new_heart_beat_request(
                    start_ts.clone(),
                    primary_key.clone(),
                    start_instant.elapsed().as_millis() as u64 + MAX_TTL,
                );
                let plan = PlanBuilder::new(rpc.clone(), keyspace, request)
                    .retry_multi_region(region_backoff.clone())
                    .merge(CollectSingle)
                    .plan();
                plan.execute().await?;
            }
            Ok::<(), Error>(())
        };

        tokio::spawn(async {
            if let Err(err) = heartbeat_task.await {
                log::error!("Error: While sending heartbeat. {}", err);
            }
        });
    }

    fn get_status(&self) -> TransactionStatus {
        self.status.load(atomic::Ordering::Acquire).into()
    }

    fn set_status(&self, status: TransactionStatus) {
        self.status.store(status as u8, atomic::Ordering::Release);
    }

    fn transit_status<F>(&self, check_status: F, next: TransactionStatus) -> bool
    where
        F: Fn(TransactionStatus) -> bool,
    {
        let mut current = self.get_status();
        while check_status(current) {
            if current == next {
                return true;
            }
            match self.status.compare_exchange_weak(
                current as u8,
                next as u8,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(x) => current = x.into(),
            }
        }
        false
    }
}

impl<PdC: PdClient> Drop for Transaction<PdC> {
    fn drop(&mut self) {
        debug!("dropping transaction");
        if std::thread::panicking() {
            return;
        }
        if self.get_status() == TransactionStatus::Active {
            match self.options.check_level {
                CheckLevel::Panic => {
                    panic!("Dropping an active transaction. Consider commit or rollback it.")
                }
                CheckLevel::Warn => {
                    warn!("Dropping an active transaction. Consider commit or rollback it.")
                }

                CheckLevel::None => {}
            }
        }
        self.set_status(TransactionStatus::Dropped);
    }
}

/// The default max TTL of a lock in milliseconds. Also called `ManagedLockTTL` in TiDB.
const MAX_TTL: u64 = 20000;
/// The default TTL of a lock in milliseconds.
const DEFAULT_LOCK_TTL: u64 = 3000;
/// The default heartbeat interval
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(MAX_TTL / 2);
/// Default safe window for async commit / 1PC max-commit-ts calculation.
///
/// This matches the default in client-go.
const DEFAULT_ASYNC_COMMIT_SAFE_WINDOW: Duration = Duration::from_secs(2);
/// TiKV recommends each RPC packet should be less than around 1MB. We keep KV size of
/// each request below 16KB.
pub const TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;
const TTL_FACTOR: f64 = 6000.0;

/// Optimistic or pessimistic transaction.
#[derive(Clone, PartialEq, Debug)]
pub enum TransactionKind {
    Optimistic,
    /// Argument is the transaction's for_update_ts
    Pessimistic(Timestamp),
}

/// Options for configuring a transaction.
///
/// `TransactionOptions` has a builder-style API.
#[derive(Clone, PartialEq, Debug)]
pub struct TransactionOptions {
    /// Optimistic or pessimistic (default) transaction.
    kind: TransactionKind,
    /// Read from replicas other than the leader (read-only snapshots only).
    replica_read: ReplicaReadType,
    /// Mark reads as stale read (read-only snapshots only).
    stale_read: bool,
    /// Read requests should not fill block cache (read-only snapshots only).
    not_fill_cache: bool,
    /// A hint for TiKV to schedule tasks more fairly (read-only snapshots only).
    task_id: u64,
    /// Server-side maximum execution duration for read requests (read-only snapshots only).
    max_execution_duration_ms: u64,
    /// TiKV may reject requests early if estimated wait time exceeds the threshold (read-only snapshots only).
    busy_threshold_ms: u32,
    /// Command priority for requests.
    priority: CommandPriority,
    /// Isolation level for read requests (read-only snapshots only).
    isolation_level: IsolationLevel,
    /// Resource group tag for requests.
    resource_group_tag: Option<Vec<u8>>,
    /// Resource group name for requests.
    resource_group_name: Option<String>,
    /// The source tag for metrics (`kvrpcpb::Context.request_source`).
    request_source: Option<String>,
    /// The source of the current transaction (`kvrpcpb::Context.txn_source`).
    txn_source: u64,
    /// Whether operations are allowed on different disk usage levels (`kvrpcpb::Context.disk_full_opt`).
    disk_full_opt: DiskFullOpt,
    /// Whether to force syncing logs for transactional write requests (`kvrpcpb::Context.sync_log`).
    sync_log: bool,
    /// Server-side maximum execution duration for transactional write requests (`kvrpcpb::Context.max_execution_duration_ms`).
    max_write_execution_duration_ms: u64,
    /// Try using 1pc rather than 2pc (default is to always use 2pc).
    try_one_pc: bool,
    /// Try to use async commit (default is not to).
    async_commit: bool,
    /// Whether the transaction only needs causal consistency (does not guarantee linearizability).
    ///
    /// When enabled, async-commit/1PC does not fetch a fresh PD TSO to seed `min_commit_ts`.
    causal_consistency: bool,
    /// Is the transaction read only? (Default is no).
    read_only: bool,
    /// How to retry in the event of certain errors.
    retry_options: RetryOptions,
    /// Lock wait timeout for pessimistic lock requests (`kvrpcpb::PessimisticLockRequest.wait_timeout`).
    ///
    /// Only effective for pessimistic transactions.
    lock_wait_timeout: LockWaitTimeout,
    /// What to do if the transaction is dropped without an attempt to commit or rollback
    check_level: CheckLevel,
    #[doc(hidden)]
    heartbeat_option: HeartbeatOption,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeartbeatOption {
    NoHeartbeat,
    FixedTime(Duration),
}

/// Lock wait timeout for pessimistic lock requests.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LockWaitTimeout {
    /// Use TiKV's default lock wait timeout (`wait_timeout = 0`).
    Default,
    /// Do not wait when encountering locks (`wait_timeout = -1`).
    NoWait,
    /// Always wait when encountering locks (`wait_timeout = i64::MAX`).
    AlwaysWait,
    /// Wait for at most the given duration.
    Wait(Duration),
}

impl LockWaitTimeout {
    fn effective_wait_timeout_ms(self, wait_start: Instant) -> i64 {
        match self {
            LockWaitTimeout::Default => 0,
            LockWaitTimeout::NoWait => -1,
            LockWaitTimeout::AlwaysWait => i64::MAX,
            LockWaitTimeout::Wait(duration) => {
                if duration.is_zero() {
                    return -1;
                }
                let elapsed_ms = wait_start.elapsed().as_millis();
                let remaining_ms = duration.as_millis().saturating_sub(elapsed_ms);
                if remaining_ms == 0 {
                    return -1;
                }
                i64::try_from(remaining_ms).unwrap_or(i64::MAX)
            }
        }
    }

    fn is_no_wait(self) -> bool {
        match self {
            LockWaitTimeout::NoWait => true,
            LockWaitTimeout::Wait(duration) => duration.is_zero(),
            LockWaitTimeout::Default | LockWaitTimeout::AlwaysWait => false,
        }
    }

    fn is_timed_out(self, wait_start: Instant) -> bool {
        match self {
            LockWaitTimeout::Wait(duration) if !duration.is_zero() => {
                wait_start.elapsed() >= duration
            }
            LockWaitTimeout::Default
            | LockWaitTimeout::NoWait
            | LockWaitTimeout::AlwaysWait
            | LockWaitTimeout::Wait(_) => false,
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        Self::new_pessimistic()
    }
}

impl TransactionOptions {
    /// Default options for an optimistic transaction.
    pub fn new_optimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Optimistic,
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            not_fill_cache: false,
            task_id: 0,
            max_execution_duration_ms: 0,
            busy_threshold_ms: 0,
            priority: CommandPriority::Normal,
            isolation_level: IsolationLevel::Si,
            resource_group_tag: None,
            resource_group_name: None,
            request_source: None,
            txn_source: 0,
            disk_full_opt: DiskFullOpt::NotAllowedOnFull,
            sync_log: false,
            max_write_execution_duration_ms: 0,
            try_one_pc: false,
            async_commit: false,
            causal_consistency: false,
            read_only: false,
            retry_options: RetryOptions::default_optimistic(),
            lock_wait_timeout: LockWaitTimeout::Default,
            check_level: CheckLevel::Panic,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Default options for a pessimistic transaction.
    pub fn new_pessimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Pessimistic(Timestamp::from_version(0)),
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            not_fill_cache: false,
            task_id: 0,
            max_execution_duration_ms: 0,
            busy_threshold_ms: 0,
            priority: CommandPriority::Normal,
            isolation_level: IsolationLevel::Si,
            resource_group_tag: None,
            resource_group_name: None,
            request_source: None,
            txn_source: 0,
            disk_full_opt: DiskFullOpt::NotAllowedOnFull,
            sync_log: false,
            max_write_execution_duration_ms: 0,
            try_one_pc: false,
            async_commit: false,
            causal_consistency: false,
            read_only: false,
            retry_options: RetryOptions::default_pessimistic(),
            lock_wait_timeout: LockWaitTimeout::AlwaysWait,
            check_level: CheckLevel::Panic,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Try to use async commit.
    #[must_use]
    pub fn use_async_commit(mut self) -> TransactionOptions {
        self.async_commit = true;
        self
    }

    /// Try to use 1pc.
    #[must_use]
    pub fn try_one_pc(mut self) -> TransactionOptions {
        self.try_one_pc = true;
        self
    }

    /// Set whether the transaction uses causal consistency instead of linearizability.
    ///
    /// When enabled, async-commit/1PC does not fetch a fresh PD TSO to seed `min_commit_ts`.
    ///
    /// Default is `false` (linearizability is guaranteed).
    #[must_use]
    pub fn causal_consistency(mut self, enabled: bool) -> TransactionOptions {
        self.causal_consistency = enabled;
        self
    }

    /// Set whether operations are allowed when TiKV disk is full.
    #[must_use]
    pub fn disk_full_opt(mut self, opt: DiskFullOpt) -> TransactionOptions {
        self.disk_full_opt = opt;
        self
    }

    /// Set the source of the transaction.
    #[must_use]
    pub fn txn_source(mut self, source: u64) -> TransactionOptions {
        self.txn_source = source;
        self
    }

    /// Set the request source label for TiKV metrics.
    #[must_use]
    pub fn request_source(mut self, source: impl Into<String>) -> TransactionOptions {
        self.request_source = Some(source.into());
        self
    }

    /// Set whether transactional write requests should force TiKV to sync logs.
    ///
    /// This option writes to `kvrpcpb::Context.sync_log`.
    #[must_use]
    pub fn sync_log(mut self, enabled: bool) -> TransactionOptions {
        self.sync_log = enabled;
        self
    }

    /// Set the server-side maximum execution duration for transactional write requests.
    ///
    /// This option writes to `kvrpcpb::Context.max_execution_duration_ms` for 2PC prewrite and
    /// commit requests.
    #[must_use]
    pub fn max_write_execution_duration(mut self, duration: Duration) -> TransactionOptions {
        self.max_write_execution_duration_ms =
            duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self
    }

    /// Set the lock wait timeout for pessimistic lock requests.
    ///
    /// This option writes to `kvrpcpb::PessimisticLockRequest.wait_timeout`.
    #[must_use]
    pub fn lock_wait_timeout(mut self, timeout: LockWaitTimeout) -> TransactionOptions {
        self.lock_wait_timeout = timeout;
        self
    }

    /// Make the transaction read only.
    #[must_use]
    pub fn read_only(mut self) -> TransactionOptions {
        self.read_only = true;
        self
    }

    /// Configure replica read behavior.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn replica_read(mut self, read_type: ReplicaReadType) -> TransactionOptions {
        self.replica_read = read_type;
        self
    }

    /// Enable stale reads for read-only snapshots.
    ///
    /// When enabled, read requests will set `kvrpcpb::Context.stale_read = true`.
    /// If replica read routing is still set to `ReplicaReadType::Leader`, this also switches it to
    /// `ReplicaReadType::Mixed`, matching client-go's `EnableStaleWithMixedReplicaRead` behavior.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn stale_read(mut self) -> TransactionOptions {
        self.stale_read = true;
        if self.replica_read == ReplicaReadType::Leader {
            self.replica_read = ReplicaReadType::Mixed;
        }
        self
    }

    /// Set whether read requests should fill TiKV block cache.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn not_fill_cache(mut self, not_fill_cache: bool) -> TransactionOptions {
        self.not_fill_cache = not_fill_cache;
        self
    }

    /// Set task ID hint for TiKV.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn task_id(mut self, task_id: u64) -> TransactionOptions {
        self.task_id = task_id;
        self
    }

    /// Set the server-side maximum execution duration for read requests.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn max_execution_duration(mut self, duration: Duration) -> TransactionOptions {
        self.max_execution_duration_ms = duration.as_millis().min(u128::from(u64::MAX)) as u64;
        self
    }

    /// Set the busy threshold for read requests.
    ///
    /// If set, TiKV can reject the request with a `ServerIsBusy` error before processing when the
    /// estimated waiting duration exceeds the threshold.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn busy_threshold(mut self, threshold: Duration) -> TransactionOptions {
        self.busy_threshold_ms = normalize_busy_threshold_ms(threshold);
        self
    }

    /// Set the priority for requests.
    ///
    /// This option writes to `kvrpcpb::Context.priority`.
    #[must_use]
    pub fn priority(mut self, priority: CommandPriority) -> TransactionOptions {
        self.priority = priority;
        self
    }

    /// Set the isolation level for read requests.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> TransactionOptions {
        self.isolation_level = isolation_level;
        self
    }

    /// Set resource group tag for requests.
    ///
    /// This option writes to `kvrpcpb::Context.resource_group_tag`.
    #[must_use]
    pub fn resource_group_tag(mut self, tag: Vec<u8>) -> TransactionOptions {
        self.resource_group_tag = Some(tag);
        self
    }

    /// Set resource group name for requests.
    ///
    /// This option writes to `kvrpcpb::Context.resource_control_context.resource_group_name`.
    #[must_use]
    pub fn resource_group_name(mut self, name: impl Into<String>) -> TransactionOptions {
        self.resource_group_name = Some(name.into());
        self
    }

    /// Don't automatically resolve locks and retry if keys are locked.
    #[must_use]
    pub fn no_resolve_locks(mut self) -> TransactionOptions {
        self.retry_options.lock_backoff = Backoff::no_backoff();
        self
    }

    /// Don't automatically resolve regions with PD if we have outdated region information.
    #[must_use]
    pub fn no_resolve_regions(mut self) -> TransactionOptions {
        self.retry_options.region_backoff = Backoff::no_backoff();
        self
    }

    /// Set RetryOptions.
    #[must_use]
    pub fn retry_options(mut self, options: RetryOptions) -> TransactionOptions {
        self.retry_options = options;
        self
    }

    /// Set the behavior when dropping a transaction without an attempt to commit or rollback it.
    #[must_use]
    pub fn drop_check(mut self, level: CheckLevel) -> TransactionOptions {
        self.check_level = level;
        self
    }

    fn apply_write_context(&self, ctx: &mut Option<kvrpcpb::Context>) {
        let ctx = ctx.get_or_insert_with(kvrpcpb::Context::default);
        ctx.disk_full_opt = self.disk_full_opt as i32;
        ctx.txn_source = self.txn_source;
        ctx.sync_log = self.sync_log;
        ctx.priority = self.priority as i32;
        ctx.max_execution_duration_ms = self.max_write_execution_duration_ms;
        if let Some(tag) = &self.resource_group_tag {
            ctx.resource_group_tag = tag.clone();
        }
        ctx.resource_control_context =
            self.resource_group_name
                .as_ref()
                .map(|resource_group_name| kvrpcpb::ResourceControlContext {
                    resource_group_name: resource_group_name.clone(),
                    ..Default::default()
                });
        if let Some(request_source) = &self.request_source {
            ctx.request_source = request_source.clone();
        }
    }

    fn push_for_update_ts(&mut self, for_update_ts: Timestamp) {
        match &mut self.kind {
            TransactionKind::Optimistic => unreachable!(),
            TransactionKind::Pessimistic(old_for_update_ts) => {
                self.kind = TransactionKind::Pessimistic(Timestamp::from_version(std::cmp::max(
                    old_for_update_ts.version(),
                    for_update_ts.version(),
                )));
            }
        }
    }

    #[must_use]
    pub fn heartbeat_option(mut self, heartbeat_option: HeartbeatOption) -> TransactionOptions {
        self.heartbeat_option = heartbeat_option;
        self
    }

    // Returns true if these options describe a pessimistic transaction.
    pub fn is_pessimistic(&self) -> bool {
        match self.kind {
            TransactionKind::Pessimistic(_) => true,
            TransactionKind::Optimistic => false,
        }
    }
}

/// Determines what happens when a transaction is dropped without being rolled back or committed.
///
/// The default is to panic.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum CheckLevel {
    /// The program will panic.
    ///
    /// Note that if the thread is already panicking, then we will not double-panic and abort, but
    /// just ignore the issue.
    Panic,
    /// Log a warning.
    Warn,
    /// Do nothing
    None,
}

impl HeartbeatOption {
    pub fn is_auto_heartbeat(&self) -> bool {
        !matches!(self, HeartbeatOption::NoHeartbeat)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Mutation {
    Put(Key, Value),
    Delete(Key),
}

impl Mutation {
    pub fn key(&self) -> &Key {
        match self {
            Mutation::Put(key, _) => key,
            Mutation::Delete(key) => key,
        }
    }
}

/// A struct wrapping the details of two-phase commit protocol (2PC).
///
/// The two phases are `prewrite` and `commit`.
/// Generally, the `prewrite` phase is to send data to all regions and write them.
/// The `commit` phase is to mark all written data as successfully committed.
///
/// The committer implements `prewrite`, `commit` and `rollback` functions.
#[allow(clippy::too_many_arguments)]
#[derive(new)]
struct Committer<PdC: PdClient = PdRpcClient> {
    primary_key: Option<Key>,
    mutations: Vec<kvrpcpb::Mutation>,
    start_version: Timestamp,
    rpc: Arc<PdC>,
    #[new(default)]
    resolve_locks_ctx: ResolveLocksContext,
    options: TransactionOptions,
    keyspace: Keyspace,
    #[new(default)]
    undetermined: bool,
    write_size: u64,
    start_instant: Instant,
    #[new(default)]
    schema_ver: Option<i64>,
    #[new(default)]
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
}

impl<PdC: PdClient> Committer<PdC> {
    async fn commit(mut self) -> Result<Option<Timestamp>> {
        debug!("committing");

        if self.primary_key.is_none() {
            let primary_key = self
                .mutations
                .iter()
                .filter(|m| m.op != kvrpcpb::Op::CheckNotExists as i32)
                .min_by(|a, b| a.key.cmp(&b.key))
                .or_else(|| self.mutations.iter().min_by(|a, b| a.key.cmp(&b.key)))
                .map(|m| Key::from(m.key.clone()));
            self.primary_key = match primary_key {
                Some(primary_key) => Some(primary_key),
                None => return Ok(None),
            };
        }

        let min_commit_ts = self.prewrite().await?;

        fail_point!("after-prewrite", |_| {
            Err(Error::StringError(
                "failpoint: after-prewrite return error".to_owned(),
            ))
        });

        // If we didn't use 1pc, prewrite will set `try_one_pc` to false.
        if self.options.try_one_pc {
            return Ok(min_commit_ts);
        }

        let commit_ts = if self.options.async_commit {
            match min_commit_ts {
                Some(ts) => ts,
                None => {
                    return Err(Error::StringError(
                        "invalid min_commit_ts after async-commit prewrite".to_owned(),
                    ));
                }
            }
        } else {
            match self.commit_primary_with_retry().await {
                Ok(commit_ts) => commit_ts,
                Err(e) => {
                    return if self.undetermined {
                        Err(Error::UndeterminedError(Box::new(e)))
                    } else {
                        Err(e)
                    };
                }
            }
        };
        tokio::spawn(self.commit_secondary(commit_ts.clone()).map(|res| {
            if let Err(e) = res {
                log::warn!("Failed to commit secondary keys: {}", e);
            }
        }));
        Ok(Some(commit_ts))
    }

    async fn prewrite(&mut self) -> Result<Option<Timestamp>> {
        debug!("prewriting");
        let primary_lock = self.primary_key.clone().unwrap();
        let elapsed = self.start_instant.elapsed().as_millis() as u64;
        let lock_ttl = self.calc_txn_lock_ttl();
        let mut request = match &self.options.kind {
            TransactionKind::Optimistic => new_prewrite_request(
                self.mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl + elapsed,
            ),
            TransactionKind::Pessimistic(for_update_ts) => new_pessimistic_prewrite_request(
                self.mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl + elapsed,
                for_update_ts.clone(),
            ),
        };

        request.use_async_commit = self.options.async_commit;
        request.try_one_pc = self.options.try_one_pc;
        request.secondaries = self
            .mutations
            .iter()
            .filter(|m| self.primary_key.as_ref().unwrap() != m.key.as_ref())
            .map(|m| m.key.clone())
            .collect();
        self.options.apply_write_context(&mut request.context);

        let commit_ts_may_be_calculated = self.options.async_commit || self.options.try_one_pc;
        if commit_ts_may_be_calculated {
            let mut min_commit_ts = self.start_version.version().saturating_add(1);
            if let TransactionKind::Pessimistic(for_update_ts) = &self.options.kind {
                min_commit_ts = min_commit_ts.max(for_update_ts.version().saturating_add(1));
            }

            if !self.options.causal_consistency {
                // Match client-go's default (linearizable) behavior: when using async-commit or
                // 1PC, seed `min_commit_ts` from a fresh PD TSO so the final commit TS is
                // guaranteed to be newer than any existing reader snapshot TS.
                let latest_ts = self.rpc.clone().get_timestamp().await?;
                min_commit_ts = min_commit_ts.max(latest_ts.version().saturating_add(1));
            }

            request.min_commit_ts = min_commit_ts;

            let current_ts = self
                .start_version
                .version()
                .saturating_add(elapsed.saturating_mul(1_u64 << 18));
            if let (Some(checker), Some(schema_ver)) =
                (self.schema_lease_checker.as_ref(), self.schema_ver)
            {
                checker.check_by_schema_ver(Timestamp::from_version(current_ts), schema_ver)?;
            }
            let safe_window_ms =
                u64::try_from(DEFAULT_ASYNC_COMMIT_SAFE_WINDOW.as_millis()).unwrap_or(u64::MAX);
            request.max_commit_ts =
                current_ts.saturating_add(safe_window_ms.saturating_mul(1_u64 << 18));
        }

        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .resolve_lock_with_pessimistic_region_in_context(
                self.resolve_locks_ctx.clone(),
                self.start_version.clone(),
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
                true,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .merge(CollectError)
            .extract_error()
            .plan();
        let response = plan.execute().await?;

        if self.options.try_one_pc && response.len() == 1 {
            if response[0].one_pc_commit_ts == 0 {
                if response[0].min_commit_ts != 0 {
                    return Err(Error::StringError(
                        "MinCommitTs must be 0 when 1pc falls back to 2pc".to_owned(),
                    ));
                }
                warn!(
                    "1pc failed and fallbacks to normal commit procedure, start_ts: {}",
                    self.start_version.version()
                );
                self.options.try_one_pc = false;
                self.options.async_commit = false;
                return Ok(None);
            }

            return Ok(Timestamp::try_from_version(response[0].one_pc_commit_ts));
        }

        if response.iter().any(|r| r.one_pc_commit_ts != 0) {
            return Err(Error::StringError(format!(
                "prewrite returned one_pc_commit_ts for non-1pc transaction, start_ts: {}",
                self.start_version.version()
            )));
        }

        self.options.try_one_pc = false;

        let has_zero_min_commit_ts = response.iter().any(|r| r.min_commit_ts == 0);
        let max_min_commit_ts = response.iter().map(|r| r.min_commit_ts).max().unwrap_or(0);
        let min_commit_ts = Timestamp::try_from_version(max_min_commit_ts);

        if self.options.async_commit && (has_zero_min_commit_ts || min_commit_ts.is_none()) {
            warn!(
                "async commit cannot proceed since the returned min_commit_ts is zero, fallback to normal path, start_ts: {}",
                self.start_version.version()
            );
            self.options.async_commit = false;
            return Ok(None);
        }

        Ok(min_commit_ts)
    }

    /// Commits the primary key and returns the commit version
    async fn commit_primary(&mut self) -> Result<Timestamp> {
        debug!("committing primary");
        let primary_key = self.primary_key.clone().into_iter();
        let commit_version = self.rpc.clone().get_timestamp().await?;
        let mut req = new_commit_request(
            primary_key,
            self.start_version.clone(),
            commit_version.clone(),
        );
        self.options.apply_write_context(&mut req.context);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .resolve_lock_in_context(
                self.resolve_locks_ctx.clone(),
                self.start_version.clone(),
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();
        plan.execute()
            .inspect_err(|e| {
                debug!(
                    "commit primary error: {:?}, start_ts: {}",
                    e,
                    self.start_version.version()
                );
                // We don't know whether the transaction is committed or not if we fail to receive
                // the response. Then, we mark the transaction as undetermined and propagate the
                // error to the user.
                if let Error::Grpc(_) = e {
                    self.undetermined = true;
                }
            })
            .await?;

        Ok(commit_version)
    }

    fn handle_commit_primary_extracted_errors(&self, mut errors: Vec<Error>) -> Result<()> {
        let err = errors.pop().ok_or_else(|| Error::InternalError {
            message: "commit primary returned extracted errors but the vector was empty".to_owned(),
        })?;
        match err {
            Error::KeyError(key_err) => {
                if let Some(expired) = key_err.commit_ts_expired {
                    // Ref: https://github.com/tikv/client-go/blob/tidb-8.5/txnkv/transaction/commit.go
                    info!(
                        "2PC commit_ts rejected by TiKV, retry with a newer commit_ts, start_ts: {}",
                        self.start_version.version()
                    );

                    let primary_key = self.primary_key.as_ref().ok_or_else(|| {
                        Error::InternalError {
                            message: "commit primary returned commit_ts_expired but primary key is missing"
                                .to_owned(),
                        }
                    })?;
                    if primary_key != expired.key.as_ref() {
                        error!(
                            "2PC commit_ts rejected by TiKV, but the key is not the primary key, start_ts: {}, key: {}, primary: {:?}",
                            self.start_version.version(),
                            HexRepr(&expired.key),
                            primary_key
                        );
                        return Err(Error::StringError(
                            "2PC commitTS rejected by TiKV, but the key is not the primary key"
                                .to_string(),
                        ));
                    }

                    // Do not retry for a txn which has a too large min_commit_ts.
                    // 3600000 << 18 = 943718400000
                    if expired
                        .min_commit_ts
                        .saturating_sub(expired.attempted_commit_ts)
                        > 943718400000
                    {
                        let msg = format!(
                            "2PC min_commit_ts is too large, we got min_commit_ts: {}, and attempted_commit_ts: {}",
                            expired.min_commit_ts, expired.attempted_commit_ts
                        );
                        return Err(Error::StringError(msg));
                    }
                    Ok(())
                } else {
                    Err(Error::KeyError(key_err))
                }
            }
            other => Err(other),
        }
    }

    async fn commit_primary_with_retry(&mut self) -> Result<Timestamp> {
        loop {
            match self.commit_primary().await {
                Ok(commit_version) => return Ok(commit_version),
                Err(Error::ExtractedErrors(errors)) => {
                    self.handle_commit_primary_extracted_errors(errors)?;
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn commit_secondary(self, commit_version: Timestamp) -> Result<()> {
        debug!("committing secondary");
        let start_version = self.start_version.clone();
        let mutations_len = self.mutations.len();
        let primary_only = mutations_len == 1;
        #[cfg(not(feature = "integration-tests"))]
        let mutations = self.mutations.into_iter();

        #[cfg(feature = "integration-tests")]
        let mutations = self.mutations.into_iter().take({
            // Truncate mutation to a new length as `percent/100`.
            // Return error when truncate to zero.
            let fp = || -> Result<usize> {
                let mut new_len = mutations_len;
                fail_point!("before-commit-secondary", |percent| {
                    let percent = percent.unwrap().parse::<usize>().unwrap();
                    new_len = mutations_len * percent / 100;
                    if new_len == 0 {
                        Err(Error::StringError(
                            "failpoint: before-commit-secondary return error".to_owned(),
                        ))
                    } else {
                        debug!(
                            "failpoint: before-commit-secondary truncate mutation {} -> {}",
                            mutations_len, new_len
                        );
                        Ok(new_len)
                    }
                });
                Ok(new_len)
            };
            fp()?
        });

        let mut req = if self.options.async_commit {
            let keys = mutations.map(|m| m.key.into());
            new_commit_request(keys, start_version.clone(), commit_version)
        } else if primary_only {
            return Ok(());
        } else {
            let primary_key = self.primary_key.unwrap();
            let keys = mutations
                .map(|m| m.key.into())
                .filter(|key| &primary_key != key);
            new_commit_request(keys, start_version.clone(), commit_version)
        };
        self.options.apply_write_context(&mut req.context);
        let plan = PlanBuilder::new(self.rpc, self.keyspace, req)
            .resolve_lock_in_context(
                self.resolve_locks_ctx.clone(),
                start_version,
                self.options.retry_options.lock_backoff,
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff)
            .extract_error()
            .plan();
        plan.execute().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        debug!("rolling back");
        if self.options.kind == TransactionKind::Optimistic && self.mutations.is_empty() {
            return Ok(());
        }
        let keys = self
            .mutations
            .into_iter()
            .map(|mutation| mutation.key.into());
        let start_version = self.start_version.clone();
        match self.options.kind {
            TransactionKind::Optimistic => {
                let req = new_batch_rollback_request(keys, start_version.clone());
                let plan = PlanBuilder::new(self.rpc, self.keyspace, req)
                    .resolve_lock_in_context(
                        self.resolve_locks_ctx.clone(),
                        start_version.clone(),
                        self.options.retry_options.lock_backoff,
                        self.keyspace,
                    )
                    .retry_multi_region(self.options.retry_options.region_backoff)
                    .extract_error()
                    .plan();
                plan.execute().await?;
            }
            TransactionKind::Pessimistic(for_update_ts) => {
                let req =
                    new_pessimistic_rollback_request(keys, start_version.clone(), for_update_ts);
                let plan = PlanBuilder::new(self.rpc, self.keyspace, req)
                    .resolve_lock_in_context(
                        self.resolve_locks_ctx.clone(),
                        start_version.clone(),
                        self.options.retry_options.lock_backoff,
                        self.keyspace,
                    )
                    .retry_multi_region(self.options.retry_options.region_backoff)
                    .extract_error()
                    .plan();
                plan.execute().await?;
            }
        }
        Ok(())
    }

    fn calc_txn_lock_ttl(&mut self) -> u64 {
        let mut lock_ttl = DEFAULT_LOCK_TTL;
        if self.write_size > TXN_COMMIT_BATCH_SIZE {
            let size_mb = self.write_size as f64 / 1024.0 / 1024.0;
            lock_ttl = (TTL_FACTOR * size_mb.sqrt()) as u64;
            lock_ttl = lock_ttl.clamp(DEFAULT_LOCK_TTL, MAX_TTL);
        }
        lock_ttl
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
enum TransactionStatus {
    /// The transaction is read-only [`Snapshot`](super::Snapshot), no need to commit or rollback or panic on drop.
    ReadOnly = 0,
    /// The transaction have not been committed or rolled back.
    Active = 1,
    /// The transaction has committed.
    Committed = 2,
    /// The transaction has tried to commit. Only `commit` is allowed.
    StartedCommit = 3,
    /// The transaction has rolled back.
    Rolledback = 4,
    /// The transaction has tried to rollback. Only `rollback` is allowed.
    StartedRollback = 5,
    /// The transaction has been dropped.
    Dropped = 6,
}

impl From<u8> for TransactionStatus {
    fn from(num: u8) -> Self {
        match num {
            0 => TransactionStatus::ReadOnly,
            1 => TransactionStatus::Active,
            2 => TransactionStatus::Committed,
            3 => TransactionStatus::StartedCommit,
            4 => TransactionStatus::Rolledback,
            5 => TransactionStatus::StartedRollback,
            6 => TransactionStatus::Dropped,
            _ => panic!("Unknown transaction status {}", num),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::io;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;

    use async_trait::async_trait;
    use fail::FailScenario;

    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::pd::PdClient;
    use crate::proto::kvrpcpb;
    use crate::proto::pdpb::Timestamp;
    use crate::request::Keyspace;
    use crate::timestamp::TimestampExt;
    use crate::transaction::HeartbeatOption;
    use crate::CheckLevel;
    use crate::CommandPriority;
    use crate::DiskFullOpt;
    use crate::Error;
    use crate::IsolationLevel;
    use crate::Key;
    use crate::ReplicaReadType;
    use crate::Transaction;
    use crate::TransactionOptions;

    #[tokio::test]
    async fn test_resolve_lock_for_read_committed_locks_propagates_to_context() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    if attempt == 0 {
                        assert!(ctx.committed_locks.is_empty());
                        assert!(ctx.resolved_locks.is_empty());

                        let mut resp = kvrpcpb::GetResponse::default();
                        let mut key_err = kvrpcpb::KeyError::default();
                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = b"k".to_vec();
                        lock.primary_lock = b"k".to_vec();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100;
                        lock.txn_size = 1;
                        lock.lock_type = kvrpcpb::Op::Put as i32;
                        key_err.locked = Some(lock);
                        resp.error = Some(key_err);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    assert_eq!(ctx.committed_locks, vec![1]);
                    assert!(ctx.resolved_locks.is_empty());

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 5;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type in resolve-lock-for-read test");
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let value = snapshot.get(b"k".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_resolve_lock_for_read_resolved_locks_propagates_to_context() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    if attempt == 0 {
                        assert!(ctx.committed_locks.is_empty());
                        assert!(ctx.resolved_locks.is_empty());

                        let mut resp = kvrpcpb::GetResponse::default();
                        let mut key_err = kvrpcpb::KeyError::default();
                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = b"k".to_vec();
                        lock.primary_lock = b"k".to_vec();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100;
                        lock.txn_size = 1;
                        lock.lock_type = kvrpcpb::Op::Put as i32;
                        key_err.locked = Some(lock);
                        resp.error = Some(key_err);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    assert!(ctx.committed_locks.is_empty());
                    assert_eq!(ctx.resolved_locks, vec![1]);

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 20;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type in resolve-lock-for-read test");
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let value = snapshot.get(b"k".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_resolve_lock_for_read_reuses_resolved_txn_cache_across_calls() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let check_txn_status_calls = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_calls = Arc::new(AtomicUsize::new(0));

        let get_calls_captured = get_calls.clone();
        let check_txn_status_calls_captured = check_txn_status_calls.clone();
        let pessimistic_rollback_calls_captured = pessimistic_rollback_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = get_calls_captured.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    match attempt {
                        0 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert!(ctx.resolved_locks.is_empty());

                            let mut resp = kvrpcpb::GetResponse::default();
                            let mut key_err = kvrpcpb::KeyError::default();
                            let mut lock = kvrpcpb::LockInfo::default();
                            lock.key = b"k1".to_vec();
                            lock.primary_lock = b"k1".to_vec();
                            lock.lock_version = 1;
                            lock.lock_ttl = 100;
                            lock.txn_size = 1;
                            lock.lock_type = kvrpcpb::Op::Put as i32;
                            key_err.locked = Some(lock);
                            resp.error = Some(key_err);
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        1 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert_eq!(ctx.resolved_locks, vec![1]);

                            let mut resp = kvrpcpb::GetResponse::default();
                            resp.value = b"v1".to_vec();
                            resp.not_found = false;
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        2 => {
                            // Ensure the read lock tracker state persists across calls, and the
                            // lock resolver can reuse the resolved-txn cache for later reads.
                            assert!(ctx.committed_locks.is_empty());
                            assert_eq!(ctx.resolved_locks, vec![1]);

                            let mut resp = kvrpcpb::GetResponse::default();
                            let mut key_err = kvrpcpb::KeyError::default();
                            let mut lock = kvrpcpb::LockInfo::default();
                            lock.key = b"k2".to_vec();
                            lock.primary_lock = b"k1".to_vec();
                            lock.lock_version = 1;
                            lock.lock_for_update_ts = 11;
                            lock.lock_ttl = 100;
                            lock.txn_size = 1;
                            lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
                            key_err.locked = Some(lock);
                            resp.error = Some(key_err);
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        3 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert_eq!(ctx.resolved_locks, vec![1]);

                            let mut resp = kvrpcpb::GetResponse::default();
                            resp.value = b"v2".to_vec();
                            resp.not_found = false;
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        _ => panic!("unexpected get attempt: {attempt}"),
                    }
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    check_txn_status_calls_captured.fetch_add(1, Ordering::SeqCst);
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 20;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.keys, vec![b"k2".to_vec()]);
                    assert_eq!(req.start_version, 1);
                    assert_eq!(req.for_update_ts, 11);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }

                panic!("unexpected request type in resolve-lock-for-read cache reuse test");
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let value1 = snapshot.get(b"k1".to_vec()).await.unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let value2 = snapshot.get(b"k2".to_vec()).await.unwrap();
        assert_eq!(value2, Some(b"v2".to_vec()));

        assert_eq!(get_calls.load(Ordering::SeqCst), 4);
        assert_eq!(check_txn_status_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_lock_for_read_reuses_resolved_txn_cache_across_snapshots() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let check_txn_status_calls = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_calls = Arc::new(AtomicUsize::new(0));

        let get_calls_captured = get_calls.clone();
        let check_txn_status_calls_captured = check_txn_status_calls.clone();
        let pessimistic_rollback_calls_captured = pessimistic_rollback_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = get_calls_captured.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    match attempt {
                        0 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert!(ctx.resolved_locks.is_empty());

                            let mut resp = kvrpcpb::GetResponse::default();
                            let mut key_err = kvrpcpb::KeyError::default();
                            let mut lock = kvrpcpb::LockInfo::default();
                            lock.key = b"k1".to_vec();
                            lock.primary_lock = b"k1".to_vec();
                            lock.lock_version = 1;
                            lock.lock_ttl = 100;
                            lock.txn_size = 1;
                            lock.lock_type = kvrpcpb::Op::Put as i32;
                            key_err.locked = Some(lock);
                            resp.error = Some(key_err);
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        1 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert_eq!(ctx.resolved_locks, vec![1]);

                            let mut resp = kvrpcpb::GetResponse::default();
                            resp.value = b"v1".to_vec();
                            resp.not_found = false;
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        2 => {
                            // New snapshot → lock-tracker context starts empty, but resolved-txn
                            // status should be cached by the shared ResolveLocksContext.
                            assert!(ctx.committed_locks.is_empty());
                            assert!(ctx.resolved_locks.is_empty());

                            let mut resp = kvrpcpb::GetResponse::default();
                            let mut key_err = kvrpcpb::KeyError::default();
                            let mut lock = kvrpcpb::LockInfo::default();
                            lock.key = b"k2".to_vec();
                            lock.primary_lock = b"k1".to_vec();
                            lock.lock_version = 1;
                            lock.lock_for_update_ts = 11;
                            lock.lock_ttl = 100;
                            lock.txn_size = 1;
                            lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
                            key_err.locked = Some(lock);
                            resp.error = Some(key_err);
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        3 => {
                            assert!(ctx.committed_locks.is_empty());
                            assert_eq!(ctx.resolved_locks, vec![1]);

                            let mut resp = kvrpcpb::GetResponse::default();
                            resp.value = b"v2".to_vec();
                            resp.not_found = false;
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        _ => panic!("unexpected get attempt: {attempt}"),
                    }
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    check_txn_status_calls_captured.fetch_add(1, Ordering::SeqCst);
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 20;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.keys, vec![b"k2".to_vec()]);
                    assert_eq!(req.start_version, 1);
                    assert_eq!(req.for_update_ts, 11);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }

                panic!("unexpected request type in resolve-lock-for-read snapshot cache test");
            },
        )));

        let resolve_locks_ctx = crate::transaction::ResolveLocksContext::default();

        let mut snapshot1 = Transaction::new_with_resolve_locks_ctx(
            Timestamp::from_version(10),
            pd_client.clone(),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
            resolve_locks_ctx.clone(),
        );
        let value1 = snapshot1.get(b"k1".to_vec()).await.unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let mut snapshot2 = Transaction::new_with_resolve_locks_ctx(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
            resolve_locks_ctx,
        );
        let value2 = snapshot2.get(b"k2".to_vec()).await.unwrap();
        assert_eq!(value2, Some(b"v2".to_vec()));

        assert_eq!(get_calls.load(Ordering::SeqCst), 4);
        assert_eq!(check_txn_status_calls.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_lock_for_read_batch_get_committed_locks_propagates_to_context() {
        let batch_get_calls = Arc::new(AtomicUsize::new(0));
        let batch_get_calls_cloned = batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let attempt = batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    if attempt == 0 {
                        assert!(ctx.committed_locks.is_empty());
                        assert!(ctx.resolved_locks.is_empty());

                        let mut resp = kvrpcpb::BatchGetResponse::default();
                        let mut key_err = kvrpcpb::KeyError::default();
                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = b"k".to_vec();
                        lock.primary_lock = b"k".to_vec();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100;
                        lock.txn_size = 1;
                        lock.lock_type = kvrpcpb::Op::Put as i32;
                        key_err.locked = Some(lock);
                        resp.error = Some(key_err);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    assert_eq!(ctx.committed_locks, vec![1]);
                    assert!(ctx.resolved_locks.is_empty());

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = b"k".to_vec();
                    pair.value = b"v".to_vec();
                    let mut resp = kvrpcpb::BatchGetResponse::default();
                    resp.pairs = vec![pair];
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 5;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type in resolve-lock-for-read batch_get test");
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let pairs: Vec<_> = snapshot
            .batch_get(vec![b"k".to_vec()])
            .await
            .unwrap()
            .collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"k".to_vec().into());
        assert_eq!(pairs[0].1, b"v".to_vec());
        assert_eq!(batch_get_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_resolve_lock_for_read_scan_resolved_locks_propagates_to_context() {
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls_cloned = scan_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::ScanRequest>() {
                    let attempt = scan_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    let ctx = req.context.as_ref().expect("context");

                    if attempt == 0 {
                        assert!(ctx.committed_locks.is_empty());
                        assert!(ctx.resolved_locks.is_empty());

                        let mut resp = kvrpcpb::ScanResponse::default();
                        let mut key_err = kvrpcpb::KeyError::default();
                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = b"k".to_vec();
                        lock.primary_lock = b"k".to_vec();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100;
                        lock.txn_size = 1;
                        lock.lock_type = kvrpcpb::Op::Put as i32;
                        key_err.locked = Some(lock);
                        resp.error = Some(key_err);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    assert!(ctx.committed_locks.is_empty());
                    assert_eq!(ctx.resolved_locks, vec![1]);

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = b"k".to_vec();
                    pair.value = b"v".to_vec();
                    let mut resp = kvrpcpb::ScanResponse::default();
                    resp.pairs = vec![pair];
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req
                    .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                    .is_some()
                {
                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 20;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type in resolve-lock-for-read scan test");
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let start: Key = b"a".to_vec().into();
        let end: Key = b"z".to_vec().into();
        let pairs: Vec<_> = snapshot.scan(start..end, 1).await.unwrap().collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"k".to_vec().into());
        assert_eq!(pairs[0].1, b"v".to_vec());
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_follower() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(false));
        let stale_read = Arc::new(AtomicBool::new(false));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let stale_read_cloned = stale_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);
                stale_read_cloned.store(ctx.stale_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 51);
        assert!(replica_read.load(Ordering::SeqCst));
        assert!(!stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_learner() {
        let store_id = Arc::new(AtomicU64::new(0));

        let store_id_cloned = store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Learner),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 61);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(false));
        let stale_read = Arc::new(AtomicBool::new(false));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let stale_read_cloned = stale_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);
                stale_read_cloned.store(ctx.stale_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 51);
        assert!(replica_read.load(Ordering::SeqCst));
        assert!(!stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_grpc_error_rotates_replicas() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    return Err(Error::GrpcAPI(tonic::Status::unavailable("unavailable")));
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 61);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_skips_unreachable_store_after_grpc_error() {
        let store_ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let store_61_attempts = Arc::new(AtomicUsize::new(0));

        let store_ids_captured = store_ids.clone();
        let store_61_attempts_captured = store_61_attempts.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                store_ids_captured
                    .lock()
                    .expect("mutex poisoned")
                    .push(peer.store_id);

                match peer.store_id {
                    51 => Err(Error::GrpcAPI(tonic::Status::deadline_exceeded(
                        "deadline exceeded",
                    ))),
                    61 => {
                        let attempt = store_61_attempts_captured.fetch_add(1, Ordering::SeqCst);
                        if attempt == 0 {
                            let mut not_leader = crate::proto::errorpb::NotLeader::default();
                            not_leader.leader = Some(crate::proto::metapb::Peer {
                                store_id: 41,
                                ..Default::default()
                            });
                            let mut region_error = crate::proto::errorpb::Error::default();
                            region_error.not_leader = Some(not_leader);

                            let resp = kvrpcpb::GetResponse {
                                region_error: Some(region_error),
                                ..Default::default()
                            };
                            Ok(Box::new(resp) as Box<dyn Any>)
                        } else {
                            Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
                        }
                    }
                    _ => Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>),
                }
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        let store_ids = store_ids.lock().expect("mutex poisoned").clone();
        assert_eq!(store_ids, vec![51, 61, 61]);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_prefer_leader_defaults_to_leader() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(false));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::PreferLeader),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 41);
        assert!(!replica_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_transaction_set_replica_read_affects_routing() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(false));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        snapshot.set_replica_read(ReplicaReadType::Follower);

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 51);
        assert!(replica_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_adjuster_can_disable_replica_read_for_point_get() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(true));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower),
            Keyspace::Disable,
        );
        snapshot.set_replica_read_adjuster(|_| ReplicaReadType::Leader);

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 41);
        assert!(!replica_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_adjuster_can_disable_replica_read_for_batch_get() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(true));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::BatchGetRequest>()
                    .expect("expected batch get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");
                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::BatchGetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower),
            Keyspace::Disable,
        );
        snapshot.set_replica_read_adjuster(|key_count| {
            if key_count >= 2 {
                ReplicaReadType::Leader
            } else {
                ReplicaReadType::Follower
            }
        });

        let keys = vec![Key::from(vec![0]), Key::from(vec![1])];
        let _ = snapshot.batch_get(keys).await.unwrap().collect::<Vec<_>>();

        assert_eq!(store_id.load(Ordering::SeqCst), 41);
        assert!(!replica_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_prefer_leader_grpc_error_falls_back_to_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    return Err(Error::GrpcAPI(tonic::Status::unavailable("unavailable")));
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::PreferLeader),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 41);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_prefer_leader_server_busy_falls_back_to_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(crate::proto::errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::PreferLeader),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 41);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_server_is_busy_retries_same_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(crate::proto::errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_max_timestamp_not_synced_retries_same_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            max_timestamp_not_synced: Some(
                                crate::proto::errorpb::MaxTimestampNotSynced::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_read_index_not_ready_retries_same_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            read_index_not_ready: Some(
                                crate::proto::errorpb::ReadIndexNotReady::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_proposal_in_merging_mode_retries_same_replica() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            proposal_in_merging_mode: Some(
                                crate::proto::errorpb::ProposalInMergingMode::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_leader_server_is_busy_with_threshold_fallbacks_to_mixed() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(crate::proto::errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Leader),
            Keyspace::Disable,
        );
        snapshot.set_load_based_replica_read_threshold(Duration::from_millis(1));
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 41);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 51);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_leader_server_is_busy_without_threshold_retries_leader() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(crate::proto::errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Leader),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 41);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
    }

    #[tokio::test]
    async fn test_snapshot_stale_read_disables_replica_read_flag() {
        let replica_read = Arc::new(AtomicBool::new(true));
        let stale_read = Arc::new(AtomicBool::new(false));

        let replica_read_cloned = replica_read.clone();
        let stale_read_cloned = stale_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);
                stale_read_cloned.store(ctx.stale_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower)
                .stale_read(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert!(!replica_read.load(Ordering::SeqCst));
        assert!(stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_stale_read_defaults_to_mixed_replica_routing() {
        let store_id = Arc::new(AtomicU64::new(0));
        let replica_read = Arc::new(AtomicBool::new(true));
        let stale_read = Arc::new(AtomicBool::new(false));

        let store_id_cloned = store_id.clone();
        let replica_read_cloned = replica_read.clone();
        let stale_read_cloned = stale_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                store_id_cloned.store(peer.store_id, Ordering::SeqCst);
                replica_read_cloned.store(ctx.replica_read, Ordering::SeqCst);
                stale_read_cloned.store(ctx.stale_read, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .stale_read(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 51);
        assert!(!replica_read.load(Ordering::SeqCst));
        assert!(stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_meets_lock_falls_back_to_leader() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::GetRequest>() {
                    let req = req
                        .downcast_ref::<kvrpcpb::GetRequest>()
                        .expect("expected get request");
                    let ctx = req.context.as_ref().expect("context");
                    let peer = ctx.peer.as_ref().expect("peer");

                    let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        first_store_id_captured.store(peer.store_id, Ordering::SeqCst);

                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = req.key.clone();
                        lock.primary_lock = req.key.clone();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100; // not expired under MockPdClient's Timestamp::default()

                        let resp = kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(lock),
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>);
                }

                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
    }

    #[tokio::test]
    async fn test_snapshot_stale_read_meets_lock_disables_stale_read_and_falls_back_to_leader() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));
        let first_stale_read = Arc::new(AtomicBool::new(false));
        let second_stale_read = Arc::new(AtomicBool::new(true));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let first_stale_read_captured = first_stale_read.clone();
        let second_stale_read_captured = second_stale_read.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::GetRequest>() {
                    let req = req
                        .downcast_ref::<kvrpcpb::GetRequest>()
                        .expect("expected get request");
                    let ctx = req.context.as_ref().expect("context");
                    let peer = ctx.peer.as_ref().expect("peer");

                    let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                        first_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);

                        let mut lock = kvrpcpb::LockInfo::default();
                        lock.key = req.key.clone();
                        lock.primary_lock = req.key.clone();
                        lock.lock_version = 1;
                        lock.lock_ttl = 100; // not expired under MockPdClient's Timestamp::default()

                        let resp = kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(lock),
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }

                    second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    second_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>);
                }

                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .stale_read(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
        assert!(first_stale_read.load(Ordering::SeqCst));
        assert!(!second_stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_stale_read_data_is_not_ready_retries_on_leader_and_disables_stale_read()
    {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));
        let first_stale_read = Arc::new(AtomicBool::new(false));
        let second_stale_read = Arc::new(AtomicBool::new(true));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let first_stale_read_captured = first_stale_read.clone();
        let second_stale_read_captured = second_stale_read.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    first_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);

                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            data_is_not_ready: Some(
                                crate::proto::errorpb::DataIsNotReady::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                second_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .stale_read(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
        assert!(first_stale_read.load(Ordering::SeqCst));
        assert!(!second_stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_stale_read_leader_server_is_busy_keeps_stale_read_on_later_retries() {
        let get_count = Arc::new(AtomicUsize::new(0));
        let first_store_id = Arc::new(AtomicU64::new(0));
        let second_store_id = Arc::new(AtomicU64::new(0));
        let third_store_id = Arc::new(AtomicU64::new(0));
        let first_stale_read = Arc::new(AtomicBool::new(false));
        let second_stale_read = Arc::new(AtomicBool::new(true));
        let third_stale_read = Arc::new(AtomicBool::new(false));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let third_store_id_captured = third_store_id.clone();
        let first_stale_read_captured = first_stale_read.clone();
        let second_stale_read_captured = second_stale_read.clone();
        let third_stale_read_captured = third_stale_read.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let peer = ctx.peer.as_ref().expect("peer");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    first_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    first_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);

                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            data_is_not_ready: Some(
                                crate::proto::errorpb::DataIsNotReady::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if attempt == 1 {
                    second_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                    second_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);

                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(crate::proto::errorpb::ServerIsBusy::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                third_store_id_captured.store(peer.store_id, Ordering::SeqCst);
                third_stale_read_captured.store(ctx.stale_read, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .stale_read(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 3);
        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
        assert_eq!(third_store_id.load(Ordering::SeqCst), 51);
        assert!(first_stale_read.load(Ordering::SeqCst));
        assert!(!second_stale_read.load(Ordering::SeqCst));
        assert!(third_stale_read.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_retries_on_max_timestamp_not_synced_region_error() {
        let get_count = Arc::new(AtomicUsize::new(0));

        let get_count_captured = get_count.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                req.downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");

                let attempt = get_count_captured.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    let resp = kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            max_timestamp_not_synced: Some(
                                crate::proto::errorpb::MaxTimestampNotSynced::default(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(get_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_snapshot_not_fill_cache_propagates_to_context() {
        let not_fill_cache = Arc::new(AtomicBool::new(false));

        let not_fill_cache_cloned = not_fill_cache.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                not_fill_cache_cloned.store(ctx.not_fill_cache, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .not_fill_cache(true),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert!(not_fill_cache.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_snapshot_task_id_propagates_to_context() {
        let task_id = Arc::new(AtomicU64::new(0));

        let task_id_cloned = task_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                task_id_cloned.store(ctx.task_id, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only().task_id(42),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(task_id.load(Ordering::SeqCst), 42);
    }

    #[tokio::test]
    async fn test_not_fill_cache_task_id_ignored_for_read_write_transactions() {
        let not_fill_cache = Arc::new(AtomicBool::new(true));
        let task_id = Arc::new(AtomicU64::new(u64::MAX));

        let not_fill_cache_cloned = not_fill_cache.clone();
        let task_id_cloned = task_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                not_fill_cache_cloned.store(ctx.not_fill_cache, Ordering::SeqCst);
                task_id_cloned.store(ctx.task_id, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .not_fill_cache(true)
                .task_id(42)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = txn.get(key).await.unwrap();

        assert!(!not_fill_cache.load(Ordering::SeqCst));
        assert_eq!(task_id.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_snapshot_priority_propagates_to_context() {
        let priority = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let priority_cloned = priority.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                priority_cloned.store(ctx.priority, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .priority(CommandPriority::High),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(
            priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
    }

    #[tokio::test]
    async fn test_snapshot_isolation_level_propagates_to_context() {
        let isolation_level = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let isolation_level_cloned = isolation_level.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                isolation_level_cloned.store(ctx.isolation_level, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .isolation_level(IsolationLevel::RcCheckTs),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(
            isolation_level.load(Ordering::SeqCst),
            IsolationLevel::RcCheckTs as i32
        );
    }

    #[tokio::test]
    async fn test_snapshot_resource_group_tag_propagates_to_context() {
        let seen_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let tag = b"rg-tag".to_vec();

        let seen_tag_cloned = seen_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                *seen_tag_cloned.lock().unwrap() = ctx.resource_group_tag.clone();

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .resource_group_tag(tag.clone()),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(*seen_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_snapshot_resource_group_name_propagates_to_context() {
        let seen_name = Arc::new(std::sync::Mutex::new(String::new()));
        let name = "rg-name".to_string();

        let seen_name_cloned = seen_name.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                let resource_control_context = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("resource_control_context");
                *seen_name_cloned.lock().unwrap() =
                    resource_control_context.resource_group_name.clone();

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .resource_group_name(name.clone()),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(*seen_name.lock().unwrap(), name);
    }

    #[tokio::test]
    async fn test_snapshot_max_execution_duration_propagates_to_context() {
        let seen_timeout_ms = Arc::new(AtomicU64::new(0));

        let seen_timeout_ms_cloned = seen_timeout_ms.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_timeout_ms_cloned.store(ctx.max_execution_duration_ms, Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .max_execution_duration(Duration::from_millis(123)),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(seen_timeout_ms.load(Ordering::SeqCst), 123);
    }

    #[tokio::test]
    async fn test_snapshot_busy_threshold_propagates_to_context() {
        let seen_busy_threshold_ms = Arc::new(AtomicU64::new(0));

        let seen_busy_threshold_ms_cloned = seen_busy_threshold_ms.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_busy_threshold_ms_cloned
                    .store(u64::from(ctx.busy_threshold_ms), Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .busy_threshold(Duration::from_millis(321)),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(seen_busy_threshold_ms.load(Ordering::SeqCst), 321);
    }

    #[tokio::test]
    async fn test_snapshot_busy_threshold_too_large_disables() {
        let seen_busy_threshold_ms = Arc::new(AtomicU64::new(0));

        let seen_busy_threshold_ms_cloned = seen_busy_threshold_ms.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_busy_threshold_ms_cloned
                    .store(u64::from(ctx.busy_threshold_ms), Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let too_large = u64::from(u32::MAX) + 1;
        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .busy_threshold(Duration::from_millis(too_large)),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(seen_busy_threshold_ms.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_snapshot_set_load_based_replica_read_threshold_propagates_to_context() {
        let seen_busy_threshold_ms = Arc::new(AtomicU64::new(0));

        let seen_busy_threshold_ms_cloned = seen_busy_threshold_ms.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_busy_threshold_ms_cloned
                    .store(u64::from(ctx.busy_threshold_ms), Ordering::SeqCst);

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        snapshot.set_load_based_replica_read_threshold(Duration::from_millis(222));

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(seen_busy_threshold_ms.load(Ordering::SeqCst), 222);
    }

    #[tokio::test]
    async fn test_snapshot_request_source_propagates_to_context() {
        let seen_source = Arc::new(std::sync::Mutex::new(String::new()));
        let source = "snapshot-source".to_string();

        let seen_source_cloned = seen_source.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                *seen_source_cloned.lock().unwrap() = ctx.request_source.clone();

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .request_source(source.clone()),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(*seen_source.lock().unwrap(), source);
    }

    #[tokio::test]
    async fn test_txn_source_disk_full_opt_request_source_propagates_to_commit_requests() {
        let prewrite_txn_source = Arc::new(AtomicU64::new(0));
        let commit_txn_source = Arc::new(AtomicU64::new(0));
        let prewrite_disk_full_opt = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let commit_disk_full_opt = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let prewrite_request_source = Arc::new(std::sync::Mutex::new(String::new()));
        let commit_request_source = Arc::new(std::sync::Mutex::new(String::new()));
        let prewrite_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let commit_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let prewrite_sync_log = Arc::new(AtomicBool::new(false));
        let commit_sync_log = Arc::new(AtomicBool::new(false));
        let prewrite_max_execution_duration_ms = Arc::new(AtomicU64::new(0));
        let commit_max_execution_duration_ms = Arc::new(AtomicU64::new(0));
        let prewrite_resource_group_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let commit_resource_group_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let prewrite_resource_group_name = Arc::new(std::sync::Mutex::new(String::new()));
        let commit_resource_group_name = Arc::new(std::sync::Mutex::new(String::new()));
        let request_source = "txn-source".to_string();
        let resource_group_tag = b"rg-tag".to_vec();
        let resource_group_name = "rg-name".to_string();

        let prewrite_txn_source_cloned = prewrite_txn_source.clone();
        let commit_txn_source_cloned = commit_txn_source.clone();
        let prewrite_disk_full_opt_cloned = prewrite_disk_full_opt.clone();
        let commit_disk_full_opt_cloned = commit_disk_full_opt.clone();
        let prewrite_request_source_cloned = prewrite_request_source.clone();
        let commit_request_source_cloned = commit_request_source.clone();
        let prewrite_priority_cloned = prewrite_priority.clone();
        let commit_priority_cloned = commit_priority.clone();
        let prewrite_sync_log_cloned = prewrite_sync_log.clone();
        let commit_sync_log_cloned = commit_sync_log.clone();
        let prewrite_max_execution_duration_ms_cloned = prewrite_max_execution_duration_ms.clone();
        let commit_max_execution_duration_ms_cloned = commit_max_execution_duration_ms.clone();
        let prewrite_resource_group_tag_cloned = prewrite_resource_group_tag.clone();
        let commit_resource_group_tag_cloned = commit_resource_group_tag.clone();
        let prewrite_resource_group_name_cloned = prewrite_resource_group_name.clone();
        let commit_resource_group_name_cloned = commit_resource_group_name.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    prewrite_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    prewrite_disk_full_opt_cloned.store(ctx.disk_full_opt, Ordering::SeqCst);
                    *prewrite_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    prewrite_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    prewrite_sync_log_cloned.store(ctx.sync_log, Ordering::SeqCst);
                    prewrite_max_execution_duration_ms_cloned
                        .store(ctx.max_execution_duration_ms, Ordering::SeqCst);
                    *prewrite_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    *prewrite_resource_group_name_cloned.lock().unwrap() = ctx
                        .resource_control_context
                        .as_ref()
                        .expect("resource_control_context")
                        .resource_group_name
                        .clone();
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    commit_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    commit_disk_full_opt_cloned.store(ctx.disk_full_opt, Ordering::SeqCst);
                    *commit_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    commit_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    commit_sync_log_cloned.store(ctx.sync_log, Ordering::SeqCst);
                    commit_max_execution_duration_ms_cloned
                        .store(ctx.max_execution_duration_ms, Ordering::SeqCst);
                    *commit_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    *commit_resource_group_name_cloned.lock().unwrap() = ctx
                        .resource_control_context
                        .as_ref()
                        .expect("resource_control_context")
                        .resource_group_name
                        .clone();
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
                .txn_source(42)
                .request_source(request_source.clone())
                .sync_log(true)
                .max_write_execution_duration(Duration::from_millis(987))
                .resource_group_tag(resource_group_tag.clone())
                .resource_group_name(resource_group_name.clone())
                .priority(CommandPriority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(prewrite_txn_source.load(Ordering::SeqCst), 42);
        assert_eq!(commit_txn_source.load(Ordering::SeqCst), 42);
        assert_eq!(
            prewrite_disk_full_opt.load(Ordering::SeqCst),
            DiskFullOpt::AllowedOnAlmostFull as i32
        );
        assert_eq!(
            commit_disk_full_opt.load(Ordering::SeqCst),
            DiskFullOpt::AllowedOnAlmostFull as i32
        );
        assert_eq!(*prewrite_request_source.lock().unwrap(), request_source);
        assert_eq!(*commit_request_source.lock().unwrap(), request_source);
        assert_eq!(
            prewrite_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert_eq!(
            commit_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert!(prewrite_sync_log.load(Ordering::SeqCst));
        assert!(commit_sync_log.load(Ordering::SeqCst));
        assert_eq!(
            prewrite_max_execution_duration_ms.load(Ordering::SeqCst),
            987
        );
        assert_eq!(commit_max_execution_duration_ms.load(Ordering::SeqCst), 987);
        assert_eq!(
            *prewrite_resource_group_tag.lock().unwrap(),
            resource_group_tag
        );
        assert_eq!(
            *commit_resource_group_tag.lock().unwrap(),
            resource_group_tag
        );
        assert_eq!(
            *prewrite_resource_group_name.lock().unwrap(),
            resource_group_name
        );
        assert_eq!(
            *commit_resource_group_name.lock().unwrap(),
            resource_group_name
        );
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    async fn test_optimistic_heartbeat(#[case] keyspace: Keyspace) -> Result<(), io::Error> {
        let scenario = FailScenario::setup();
        fail::cfg("after-prewrite", "sleep(1500)").unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let heartbeats_cloned = heartbeats.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>().is_some() {
                    heartbeats_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        let key1 = "key1".to_owned();
        let mut heartbeat_txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1))),
            keyspace,
        );
        heartbeat_txn.put(key1.clone(), "foo").await.unwrap();
        let heartbeat_txn_handle = tokio::task::spawn_blocking(move || {
            assert!(futures::executor::block_on(heartbeat_txn.commit()).is_ok())
        });
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        heartbeat_txn_handle.await.unwrap();
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        scenario.teardown();
        Ok(())
    }

    #[derive(Clone)]
    struct FixedTimestampPdClient {
        inner: Arc<MockPdClient>,
        timestamp: Timestamp,
        timestamp_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PdClient for FixedTimestampPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: crate::region::RegionWithLeader,
        ) -> crate::Result<crate::store::RegionStore> {
            self.inner.clone().map_region_to_store(region).await
        }

        async fn region_for_key(
            &self,
            key: &crate::Key,
        ) -> crate::Result<crate::region::RegionWithLeader> {
            self.inner.region_for_key(key).await
        }

        async fn region_for_id(
            &self,
            id: crate::region::RegionId,
        ) -> crate::Result<crate::region::RegionWithLeader> {
            self.inner.region_for_id(id).await
        }

        async fn get_timestamp(self: Arc<Self>) -> crate::Result<Timestamp> {
            self.timestamp_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.timestamp.clone())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> crate::Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(
            &self,
            _keyspace: &str,
        ) -> crate::Result<crate::proto::keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> crate::Result<Vec<crate::store::Store>> {
            self.inner.all_stores().await
        }

        async fn update_leader(
            &self,
            _ver_id: crate::region::RegionVerId,
            _leader: crate::proto::metapb::Peer,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: crate::region::RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: crate::region::StoreId) {}
    }

    struct FailingSchemaLeaseChecker {
        calls: Arc<AtomicUsize>,
        expected_schema_ver: i64,
        min_check_ts: u64,
    }

    impl super::SchemaLeaseChecker for FailingSchemaLeaseChecker {
        fn check_by_schema_ver(
            &self,
            txn_ts: Timestamp,
            start_schema_ver: i64,
        ) -> crate::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(start_schema_ver, self.expected_schema_ver);
            assert!(txn_ts.version() >= self.min_check_ts);
            Err(Error::StringError("schema changed".to_owned()))
        }
    }

    #[tokio::test]
    async fn test_async_commit_schema_lease_check_blocks_prewrite() {
        let calls = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let start_ts_version = start_ts.version();

        let checker = Arc::new(FailingSchemaLeaseChecker {
            calls: calls.clone(),
            expected_schema_ver: 42,
            min_check_ts: start_ts_version,
        });

        let client = MockKvClient::with_dispatch_hook(|req: &dyn Any| {
            panic!("unexpected request type: {:?}", req.type_id());
        });
        let pd_client = Arc::new(MockPdClient::new(client));

        let mut txn = Transaction::new(
            start_ts,
            pd_client,
            TransactionOptions::new_optimistic()
                .use_async_commit()
                .causal_consistency(true)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_schema_ver(42);
        txn.set_schema_lease_checker(checker);
        txn.put("key".to_owned(), "value").await.unwrap();

        let err = txn
            .commit()
            .await
            .expect_err("expected schema lease check error");
        assert!(matches!(
            err,
            Error::StringError(message) if message == "schema changed"
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_commit_primary_extracted_errors_empty_is_internal_error() {
        let client = MockKvClient::with_dispatch_hook(|req: &dyn Any| {
            panic!("unexpected request type: {:?}", req.type_id());
        });
        let pd_client = Arc::new(MockPdClient::new(client));

        let committer = super::Committer::new(
            Some(vec![1].into()),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: vec![1],
                value: vec![42],
                ..Default::default()
            }],
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic(),
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let err = committer
            .handle_commit_primary_extracted_errors(Vec::new())
            .expect_err("expected internal error");
        assert!(matches!(err, Error::InternalError { .. }));
    }

    #[tokio::test]
    async fn test_committer_one_pc_fallbacks_to_two_pc() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let commit_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };

        let start_ts_version = start_ts.version();
        let commit_ts_version = commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.try_one_pc);
                assert!(req.min_commit_ts > 0);
                assert!(req.max_commit_ts > 0);

                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: 0,
                    min_commit_ts: 0,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.keys, vec![vec![1]]);
                assert_eq!(req.start_version, start_ts_version);
                assert_eq!(req.commit_version, commit_ts_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: commit_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic().try_one_pc();
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_committer_async_commit_fallbacks_to_two_pc_when_min_commit_ts_is_zero() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let commit_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };

        let start_ts_version = start_ts.version();
        let commit_ts_version = commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.use_async_commit);
                assert!(req.min_commit_ts > 0);
                assert!(req.max_commit_ts > 0);

                let resp = kvrpcpb::PrewriteResponse {
                    min_commit_ts: 0,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.keys, vec![vec![1]]);
                assert_eq!(req.start_version, start_ts_version);
                assert_eq!(req.commit_version, commit_ts_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: commit_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic().use_async_commit();
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_committer_one_pc_success_returns_one_pc_commit_ts() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let one_pc_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };

        let one_pc_commit_ts_version = one_pc_commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.try_one_pc);
                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: one_pc_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic().try_one_pc();
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), one_pc_commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 0);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_committer_derives_primary_key_from_mutations_when_missing() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let one_pc_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };

        let one_pc_commit_ts_version = one_pc_commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.try_one_pc);
                assert_eq!(req.primary_lock, vec![1]);
                assert_eq!(req.secondaries, vec![vec![2]]);
                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: one_pc_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let mutations = vec![
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: vec![2],
                value: vec![42],
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put.into(),
                key: vec![1],
                value: vec![43],
                ..Default::default()
            },
        ];

        let options = TransactionOptions::new_optimistic().try_one_pc();
        let committer = super::Committer::new(
            None,
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), one_pc_commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_optimistic_commit_selects_smallest_key_as_primary() {
        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let one_pc_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };
        let one_pc_commit_ts_version = one_pc_commit_ts.version();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert!(req.try_one_pc);
                assert_eq!(req.primary_lock, vec![1]);
                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: one_pc_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let mut txn = Transaction::new(
            start_ts,
            pd_client,
            TransactionOptions::new_optimistic()
                .try_one_pc()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![2u8], b"v2".to_vec()).await.unwrap();
        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), one_pc_commit_ts_version);
    }

    #[tokio::test]
    async fn test_committer_one_pc_success_skips_pd_tso_when_causal_consistency_enabled() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let one_pc_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };

        let start_ts_version = start_ts.version();
        let one_pc_commit_ts_version = one_pc_commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.try_one_pc);
                assert_eq!(req.min_commit_ts, start_ts_version.saturating_add(1));
                assert!(req.max_commit_ts > 0);

                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: one_pc_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic()
            .try_one_pc()
            .causal_consistency(true);
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), one_pc_commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 0);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_committer_async_commit_success_uses_min_commit_ts() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));
        let commit_notify = Arc::new(tokio::sync::Notify::new());

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let async_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };

        let start_ts_version = start_ts.version();
        let async_commit_ts_version = async_commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let commit_notify_captured = commit_notify.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.use_async_commit);
                let resp = kvrpcpb::PrewriteResponse {
                    min_commit_ts: async_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.keys, vec![vec![1]]);
                assert_eq!(req.start_version, start_ts_version);
                assert_eq!(req.commit_version, async_commit_ts_version);
                commit_notify_captured.notify_one();
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic().use_async_commit();
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), async_commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 1);

        tokio::time::timeout(Duration::from_secs(1), commit_notify.notified())
            .await
            .expect("expected async commit request to be dispatched");
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_committer_async_commit_success_skips_pd_tso_when_causal_consistency_enabled() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));
        let commit_notify = Arc::new(tokio::sync::Notify::new());

        let start_ts = Timestamp {
            physical: 1,
            logical: 0,
            ..Default::default()
        };
        let pd_ts = Timestamp {
            physical: 2,
            logical: 0,
            ..Default::default()
        };
        let async_commit_ts = Timestamp {
            physical: 3,
            logical: 0,
            ..Default::default()
        };

        let start_ts_version = start_ts.version();
        let async_commit_ts_version = async_commit_ts.version();

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();
        let commit_notify_captured = commit_notify.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert!(req.use_async_commit);
                assert_eq!(req.min_commit_ts, start_ts_version.saturating_add(1));
                assert!(req.max_commit_ts > 0);

                let resp = kvrpcpb::PrewriteResponse {
                    min_commit_ts: async_commit_ts_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.keys, vec![vec![1]]);
                assert_eq!(req.start_version, start_ts_version);
                assert_eq!(req.commit_version, async_commit_ts_version);
                commit_notify_captured.notify_one();
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: pd_ts,
            timestamp_calls: timestamp_calls.clone(),
        });

        let primary_key: Key = vec![1].into();
        let mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put.into(),
            key: vec![1],
            value: vec![42],
            ..Default::default()
        }];

        let options = TransactionOptions::new_optimistic()
            .use_async_commit()
            .causal_consistency(true);
        let committer = super::Committer::new(
            Some(primary_key),
            mutations,
            start_ts,
            pd_client,
            options,
            Keyspace::Disable,
            0,
            Instant::now(),
        );

        let commit_result = committer
            .commit()
            .await
            .unwrap()
            .expect("expected commit_ts");
        assert_eq!(commit_result.version(), async_commit_ts_version);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(timestamp_calls.load(Ordering::SeqCst), 0);

        tokio::time::timeout(Duration::from_secs(1), commit_notify.notified())
            .await
            .expect("expected async commit request to be dispatched");
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_request_fields_match_client_go() {
        const ELAPSED_LOWER_BOUND: Duration = Duration::from_secs(5);
        const FOR_UPDATE_TS_VERSION: u64 = 42;
        const EXPECTED_MIN_COMMIT_TS: u64 = FOR_UPDATE_TS_VERSION + 1;

        let lock_requests = Arc::new(AtomicUsize::new(0));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.for_update_ts, FOR_UPDATE_TS_VERSION);
                assert_eq!(req.min_commit_ts, EXPECTED_MIN_COMMIT_TS);
                assert_eq!(req.wait_timeout, i64::MAX);
                assert!(req.lock_ttl >= super::MAX_TTL + ELAPSED_LOWER_BOUND.as_millis() as u64);

                match attempt {
                    0 => assert!(req.is_first_lock),
                    1 => assert!(!req.is_first_lock),
                    2 => {
                        assert!(!req.is_first_lock);
                        assert_eq!(req.mutations.len(), 2);
                    }
                    _ => panic!("unexpected pessimistic lock request count {attempt}"),
                }

                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: Timestamp::from_version(FOR_UPDATE_TS_VERSION),
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let options = TransactionOptions::new_pessimistic()
            .heartbeat_option(HeartbeatOption::NoHeartbeat)
            .drop_check(CheckLevel::None);

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client.clone(),
            options.clone(),
            Keyspace::Disable,
        );
        txn.start_instant = Instant::now() - ELAPSED_LOWER_BOUND;
        txn.lock_keys(vec!["k1".to_owned()]).await.unwrap();
        txn.lock_keys(vec!["k2".to_owned()]).await.unwrap();

        let mut multi_key_txn =
            Transaction::new(Timestamp::default(), pd_client, options, Keyspace::Disable);
        multi_key_txn.start_instant = Instant::now() - ELAPSED_LOWER_BOUND;
        multi_key_txn
            .lock_keys(vec!["k3".to_owned(), "k4".to_owned()])
            .await
            .unwrap();

        assert_eq!(lock_requests.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_no_wait_returns_error_on_live_lock() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.wait_timeout, -1);
                let resp = kvrpcpb::PessimisticLockResponse {
                    errors: vec![kvrpcpb::KeyError {
                        locked: Some(kvrpcpb::LockInfo {
                            key: vec![1],
                            primary_lock: vec![1],
                            lock_version: 7,
                            lock_ttl: 100,
                            lock_type: kvrpcpb::Op::Put as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                check_txn_status_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.caller_start_ts, 0);
                let resp = kvrpcpb::CheckTxnStatusResponse {
                    lock_ttl: 100,
                    lock_info: Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 7,
                        lock_ttl: 100,
                        lock_type: kvrpcpb::Op::Put as i32,
                        ..Default::default()
                    }),
                    action: kvrpcpb::Action::NoAction as i32,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: Timestamp::from_version(42),
            timestamp_calls,
        });

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .lock_wait_timeout(crate::LockWaitTimeout::NoWait)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = txn
            .lock_keys(vec![vec![1u8]])
            .await
            .expect_err("expected no-wait lock error");
        match err {
            Error::StringError(message) => {
                assert_eq!(message, "lock acquire failed and no wait is set");
            }
            err => panic!("unexpected error: {err:?}"),
        }

        assert_eq!(lock_requests.load(Ordering::SeqCst), 1);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_selects_smallest_key_as_primary() {
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(req.primary_lock, vec![1]);
                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: Timestamp::from_version(42),
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.lock_keys(vec![vec![5u8], vec![1u8]]).await.unwrap();
    }

    #[tokio::test]
    async fn test_pessimistic_lock_deduplicates_keys_and_sets_first_lock() {
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert!(req.is_first_lock);
                assert_eq!(req.primary_lock, vec![1]);
                assert_eq!(req.mutations.len(), 1);
                assert_eq!(req.mutations[0].key, vec![1]);
                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: Timestamp::from_version(42),
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.lock_keys(vec![vec![1u8], vec![1u8]]).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pessimistic_lock_locks_primary_region_first() {
        let stage = Arc::new(AtomicUsize::new(0));
        let stage_captured = stage.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let store_id = req
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.peer.as_ref())
                    .map(|peer| peer.store_id)
                    .unwrap_or(0);
                match store_id {
                    41 => {
                        assert_eq!(stage_captured.load(Ordering::SeqCst), 0);
                        stage_captured.store(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(100));
                        stage_captured.store(2, Ordering::SeqCst);
                    }
                    43 => {
                        assert_eq!(stage_captured.load(Ordering::SeqCst), 2);
                    }
                    other => panic!("unexpected store id {other}"),
                }
                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(FixedTimestampPdClient {
            inner: Arc::new(MockPdClient::new(client)),
            timestamp: Timestamp::from_version(42),
            timestamp_calls: Arc::new(AtomicUsize::new(0)),
        });

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.lock_keys(vec![vec![1u8], vec![250u8, 250u8, 1u8]])
            .await
            .unwrap();

        assert_eq!(stage.load(Ordering::SeqCst), 2);
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    async fn test_pessimistic_heartbeat(#[case] keyspace: Keyspace) -> Result<(), io::Error> {
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let heartbeats_cloned = heartbeats.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>().is_some() {
                    heartbeats_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if req
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .is_some()
                {
                    Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        let key1 = "key1".to_owned();
        let mut heartbeat_txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1))),
            keyspace,
        );
        heartbeat_txn.put(key1.clone(), "foo").await.unwrap();
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        let heartbeat_txn_handle = tokio::spawn(async move {
            assert!(heartbeat_txn.commit().await.is_ok());
        });
        heartbeat_txn_handle.await.unwrap();
        Ok(())
    }
}
