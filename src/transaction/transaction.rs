// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::iter;
use std::sync::atomic;
use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::Instant;

use derive_new::new;
use fail::fail_point;
use futures::{FutureExt, TryFutureExt};
use log::warn;
use log::{debug, trace};
use tokio::time::Duration;

use crate::backoff::Backoff;
use crate::interceptor::RpcContextInfo;
use crate::interceptor::RpcInterceptor;
use crate::interceptor::RpcInterceptorChain;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::Collect;
use crate::request::CollectError;
use crate::request::CollectSingle;
use crate::request::CollectWithShard;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::ReadRouting;
use crate::request::RetryOptions;
use crate::request::TruncateKeyspace;
use crate::timestamp::TimestampExt;
use crate::transaction::buffer::Buffer;
use crate::transaction::buffer::ReadValueState;
use crate::transaction::lowering::*;
use crate::transaction::FlagsOp;
use crate::transaction::KeyFlags;
use crate::BoundRange;
use crate::CommandPriority;
use crate::DiskFullOpt;
use crate::Error;
use crate::Key;
use crate::KvPair;
use crate::ReplicaReadType;
use crate::RequestContext;
use crate::Result;
use crate::Value;
use crate::ValueEntry;
use crate::{BatchGetOption, BatchGetOptions, GetOption, GetOptions};

/// Options for TiKV's pipelined DML transaction protocol.
///
/// Pipelined transactions flush mutations to TiKV incrementally (via `kvrpcpb::FlushRequest`)
/// during execution and then commit by committing the primary key and resolving the remaining
/// locks by range. This reduces peak memory usage and the latency of the final commit step for
/// very large write transactions.
///
/// Notes (aligned with client-go v2):
/// - Only supported for *optimistic* transactions.
/// - Incompatible with async-commit and 1PC.
#[derive(Clone, Debug, PartialEq)]
pub struct PipelinedTxnOptions {
    flush_concurrency: usize,
    resolve_lock_concurrency: usize,
    write_throttle_ratio: f64,
    #[doc(hidden)]
    min_flush_keys: usize,
}

impl Default for PipelinedTxnOptions {
    fn default() -> Self {
        Self {
            flush_concurrency: 4,
            resolve_lock_concurrency: 4,
            write_throttle_ratio: 0.0,
            min_flush_keys: 10_000,
        }
    }
}

impl PipelinedTxnOptions {
    /// Create pipelined transaction options with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum number of concurrent region requests per flush generation.
    #[must_use]
    pub fn flush_concurrency(mut self, concurrency: usize) -> Self {
        self.flush_concurrency = concurrency;
        self
    }

    /// Maximum number of concurrent region tasks when resolving flushed locks (commit/rollback).
    #[must_use]
    pub fn resolve_lock_concurrency(mut self, concurrency: usize) -> Self {
        self.resolve_lock_concurrency = concurrency;
        self
    }

    /// Throttle write speed by inserting sleeps between flushes (ratio in `[0, 1)`).
    ///
    /// `0.0` means no throttling.
    #[must_use]
    pub fn write_throttle_ratio(mut self, ratio: f64) -> Self {
        self.write_throttle_ratio = ratio;
        self
    }

    /// Set `min_flush_keys` for tests.
    ///
    /// This is a test-only escape hatch to exercise pipelined flush/resolve logic with small
    /// transactions. It is not part of the stable public API surface.
    #[cfg(any(test, feature = "integration-tests"))]
    #[doc(hidden)]
    #[must_use]
    pub fn min_flush_keys_for_test(mut self, min_flush_keys: usize) -> Self {
        self.min_flush_keys = min_flush_keys;
        self
    }

    pub(crate) fn min_flush_keys(&self) -> usize {
        self.min_flush_keys
    }

    pub(crate) fn flush_concurrency_limit(&self) -> usize {
        self.flush_concurrency
    }

    pub(crate) fn resolve_lock_concurrency_limit(&self) -> usize {
        self.resolve_lock_concurrency
    }
}

/// Options for `PessimisticLockRequest` (used by locking APIs such as `lock_keys` and
/// `get_for_update` in pessimistic transactions).
#[derive(Clone, Debug, PartialEq)]
pub struct LockOptions {
    wait_timeout_ms: i64,
    return_values: bool,
    check_existence: bool,
    lock_only_if_exists: bool,
    wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
}

impl Default for LockOptions {
    fn default() -> Self {
        Self {
            wait_timeout_ms: 0,
            return_values: false,
            check_existence: false,
            lock_only_if_exists: false,
            wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal,
        }
    }
}

impl LockOptions {
    /// Create default lock options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Time to wait (milliseconds) when encountering locks on TiKV side.
    ///
    /// This sets `kvrpcpb::PessimisticLockRequest.wait_timeout`.
    #[must_use]
    pub fn wait_timeout_ms(mut self, timeout_ms: i64) -> Self {
        self.wait_timeout_ms = timeout_ms;
        self
    }

    /// Whether TiKV should return values for locked keys.
    ///
    /// This sets `kvrpcpb::PessimisticLockRequest.return_values`.
    #[must_use]
    pub fn return_values(mut self, return_values: bool) -> Self {
        self.return_values = return_values;
        self
    }

    /// Whether TiKV should return existence information for locked keys.
    ///
    /// This sets `kvrpcpb::PessimisticLockRequest.check_existence`.
    #[must_use]
    pub fn check_existence(mut self, check_existence: bool) -> Self {
        self.check_existence = check_existence;
        self
    }

    /// Only acquire lock when the record exists.
    ///
    /// This sets `kvrpcpb::PessimisticLockRequest.lock_only_if_exists`.
    #[must_use]
    pub fn lock_only_if_exists(mut self, lock_only_if_exists: bool) -> Self {
        self.lock_only_if_exists = lock_only_if_exists;
        self
    }

    /// Configure wake-up mode when waiting for another lock.
    ///
    /// This sets `kvrpcpb::PessimisticLockRequest.wake_up_mode`.
    #[must_use]
    pub fn wake_up_mode(mut self, wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode) -> Self {
        self.wake_up_mode = wake_up_mode;
        self
    }

    pub(crate) fn wait_timeout_ms_value(&self) -> i64 {
        self.wait_timeout_ms
    }

    pub(crate) fn return_values_value(&self) -> bool {
        self.return_values
    }

    pub(crate) fn check_existence_value(&self) -> bool {
        self.check_existence
    }

    pub(crate) fn lock_only_if_exists_value(&self) -> bool {
        self.lock_only_if_exists
    }

    pub(crate) fn wake_up_mode_value(&self) -> kvrpcpb::PessimisticLockWakeUpMode {
        self.wake_up_mode
    }
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
/// # use tikv_client::{Result, TransactionClient};
/// # async fn example() -> Result<()> {
/// let client = TransactionClient::new(vec!["192.168.0.100"]).await?;
/// let mut txn = client.begin_optimistic().await?;
/// if let Some(foo) = txn.get("foo".to_owned()).await? {
///     txn.put("bar".to_owned(), foo).await?;
/// }
/// txn.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction<PdC: PdClient = PdRpcClient> {
    status: Arc<AtomicU8>,
    timestamp: Timestamp,
    buffer: Buffer,
    rpc: Arc<PdC>,
    options: TransactionOptions,
    keyspace: Keyspace,
    request_context: RequestContext,
    txn_latches: Option<Arc<super::LatchesScheduler>>,
    pipelined: Option<super::pipelined::PipelinedTxnState>,
    is_heartbeat_started: bool,
    has_pessimistic_lock: bool,
    start_instant: Instant,
}

impl<PdC: PdClient> Transaction<PdC> {
    pub(crate) fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
        request_context: RequestContext,
        txn_latches: Option<Arc<super::LatchesScheduler>>,
    ) -> Transaction<PdC> {
        let status = if options.read_only {
            TransactionStatus::ReadOnly
        } else {
            TransactionStatus::Active
        };
        let pipelined = options
            .pipelined
            .clone()
            .map(super::pipelined::PipelinedTxnState::new);
        Transaction {
            status: Arc::new(AtomicU8::new(status as u8)),
            timestamp,
            buffer: Buffer::new(options.is_pessimistic()),
            rpc,
            options,
            keyspace,
            request_context,
            txn_latches,
            pipelined,
            is_heartbeat_started: false,
            has_pessimistic_lock: false,
            start_instant: std::time::Instant::now(),
        }
    }

    /// Set `kvrpcpb::Context.request_source` for all requests sent by this transaction.
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.request_context = self.request_context.with_request_source(source);
    }

    /// Set `kvrpcpb::Context.resource_group_tag` for all requests sent by this transaction.
    pub fn set_resource_group_tag(&mut self, tag: impl Into<Vec<u8>>) {
        self.request_context = self.request_context.with_resource_group_tag(tag);
    }

    /// Set `kvrpcpb::Context.resource_control_context.resource_group_name` for all requests sent by this transaction.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.request_context = self.request_context.with_resource_group_name(name);
    }

    /// Set `kvrpcpb::Context.priority` for all requests sent by this transaction.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.request_context = self.request_context.with_priority(priority);
    }

    /// Set `kvrpcpb::Context.disk_full_opt` for all requests sent by this transaction.
    pub fn set_disk_full_opt(&mut self, disk_full_opt: DiskFullOpt) {
        self.request_context = self.request_context.with_disk_full_opt(disk_full_opt);
    }

    /// Set `kvrpcpb::Context.txn_source` for all requests sent by this transaction.
    pub fn set_txn_source(&mut self, txn_source: u64) {
        self.request_context = self.request_context.with_txn_source(txn_source);
    }

    /// Set the assertion level for this transaction.
    ///
    /// Assertion level controls whether TiKV enforces `kvrpcpb::Mutation.assertion` checks and
    /// how strict those checks are.
    pub fn set_assertion_level(&mut self, assertion_level: kvrpcpb::AssertionLevel) {
        self.options.assertion_level = assertion_level;
    }

    /// Set the minimum commit timestamp (PD TSO) required for `commit()`.
    ///
    /// When set, `commit()` will keep fetching new timestamps from PD until the chosen commit
    /// timestamp is >= `tso` or `commit_wait_until_tso_timeout()` elapses.
    ///
    /// This is aligned with client-go's `Txn.SetCommitWaitUntilTSO`.
    pub fn set_commit_wait_until_tso(&mut self, tso: u64) {
        self.options.commit_wait_until_tso = (tso != 0).then_some(tso);
    }

    /// Get the minimum commit timestamp (PD TSO) required for `commit()`.
    ///
    /// Returns `0` when unset.
    pub fn commit_wait_until_tso(&self) -> u64 {
        self.options.commit_wait_until_tso.unwrap_or(0)
    }

    /// Set the timeout for `commit()` to wait for `commit_wait_until_tso()`.
    pub fn set_commit_wait_until_tso_timeout(&mut self, timeout: Duration) {
        self.options.commit_wait_until_tso_timeout = timeout;
    }

    /// Get the timeout for `commit()` to wait for `commit_wait_until_tso()`.
    pub fn commit_wait_until_tso_timeout(&self) -> Duration {
        self.options.commit_wait_until_tso_timeout
    }

    /// Set `kvrpcpb::Mutation.assertion` for a key.
    ///
    /// The assertion is sent on `PrewriteRequest` and `FlushRequest` mutations (depending on the
    /// transaction protocol). It takes effect only when the transaction's assertion level is not
    /// `Off`.
    pub fn set_key_assertion(&mut self, key: impl Into<Key>, assertion: kvrpcpb::Assertion) {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        self.buffer.set_assertion(key.clone(), assertion);
        self.pipelined_record_key_change(&key);
    }

    /// Apply per-key flag operations.
    ///
    /// The flags influence how mutations/requests are lowered (e.g. assertions, insert semantics,
    /// prewrite-only keys).
    pub fn update_key_flags(
        &mut self,
        key: impl Into<Key>,
        ops: impl IntoIterator<Item = FlagsOp>,
    ) {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        self.buffer.update_key_flags(key.clone(), ops);
        self.pipelined_record_key_change(&key);
    }

    /// Returns the current flags for a key in this transaction.
    pub fn key_flags(&self, key: impl Into<Key>) -> KeyFlags {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        self.buffer.key_flags(&key)
    }

    /// Set `kvrpcpb::Context.resource_control_context.override_priority` for all requests sent by this transaction.
    pub fn set_resource_control_override_priority(&mut self, override_priority: u64) {
        self.request_context = self
            .request_context
            .with_resource_control_override_priority(override_priority);
    }

    /// Set `kvrpcpb::Context.resource_control_context.penalty` for all requests sent by this transaction.
    pub fn set_resource_control_penalty(
        &mut self,
        penalty: impl Into<crate::resource_manager::Consumption>,
    ) {
        self.request_context = self.request_context.with_resource_control_penalty(penalty);
    }

    /// Set a resource group tagger for all requests sent by this transaction.
    ///
    /// If a fixed resource group tag is set via [`set_resource_group_tag`](Self::set_resource_group_tag),
    /// it takes precedence over this tagger.
    pub fn set_resource_group_tagger(
        &mut self,
        tagger: impl Fn(&RpcContextInfo, &crate::kvrpcpb::Context) -> Vec<u8> + Send + Sync + 'static,
    ) {
        self.request_context = self
            .request_context
            .with_resource_group_tagger(Arc::new(tagger));
    }

    /// Replace the RPC interceptor chain for all requests sent by this transaction.
    pub fn set_rpc_interceptor(&mut self, interceptor: Arc<dyn RpcInterceptor>) {
        let mut chain = RpcInterceptorChain::new();
        chain.link(interceptor);
        self.request_context = self.request_context.with_rpc_interceptors(chain);
    }

    /// Add an RPC interceptor for all requests sent by this transaction.
    ///
    /// If another interceptor with the same name exists, it is replaced.
    pub fn add_rpc_interceptor(&mut self, interceptor: Arc<dyn RpcInterceptor>) {
        self.request_context = self.request_context.add_rpc_interceptor(interceptor);
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
    /// # use tikv_client::{Result, TransactionClient, Value};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key = "TiKV".to_owned();
    /// let _result: Option<Value> = txn.get(key).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking transactional get request");
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let request_context = self.request_context.clone();
        let read_routing = ReadRouting::new(self.options.replica_read, self.options.stale_read)
            .with_seed(timestamp.version() as u32)
            .with_request_source(request_context.request_source());

        self.buffer
            .get_or_else(key, |key| async move {
                let request = request_context.apply_to(new_get_request(key, timestamp));
                let RetryOptions {
                    region_backoff,
                    lock_backoff,
                } = retry_options;
                let plan = PlanBuilder::new(rpc, keyspace, request)
                    .with_request_context(request_context)
                    .with_read_routing(read_routing)
                    .resolve_lock(lock_backoff, keyspace)
                    .retry_multi_region(region_backoff)
                    .merge(CollectSingle)
                    .post_process_default()
                    .plan();
                plan.execute().await
            })
            .await
    }

    /// Get the value associated with the given key with read options.
    ///
    /// When `GetOption::ReturnCommitTs` is set, TiKV will return the commit timestamp of the value.
    /// The returned [`ValueEntry`] contains the value and its commit timestamp (`0` means
    /// "unknown / not requested").
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Result, TransactionClient, ValueEntry, with_return_commit_ts};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key = "TiKV".to_owned();
    /// let _entry: Option<ValueEntry> =
    ///     txn.get_with_options(key, &[with_return_commit_ts().into()]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_with_options(
        &mut self,
        key: impl Into<Key>,
        options: &[GetOption],
    ) -> Result<Option<ValueEntry>> {
        trace!("invoking transactional get request with options");
        self.check_allow_operation().await?;

        let mut opts = GetOptions::default();
        opts.apply(options);
        let need_commit_ts = opts.return_commit_ts();

        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let key = key.into().encode_keyspace(keyspace, KeyMode::Txn);
        let retry_options = self.options.retry_options.clone();
        let request_context = self.request_context.clone();
        let read_routing = ReadRouting::new(self.options.replica_read, self.options.stale_read)
            .with_seed(timestamp.version() as u32)
            .with_request_source(request_context.request_source());

        // Fast path: local mutations always return commit_ts=0; cached reads can be served when
        // commit_ts is not requested (or already cached).
        match self.buffer.read_value_state(&key) {
            ReadValueState::Local(value) => {
                return Ok(value.map(|value| ValueEntry::new(value, 0)));
            }
            ReadValueState::Cached(None) => return Ok(None),
            ReadValueState::Cached(Some(value)) if !need_commit_ts => {
                return Ok(Some(ValueEntry::new(value, 0)));
            }
            ReadValueState::Cached(Some(value)) => {
                if let Some(commit_ts) = self.buffer.cached_commit_ts(&key) {
                    if commit_ts != 0 {
                        return Ok(Some(ValueEntry::new(value, commit_ts)));
                    }
                }
            }
            ReadValueState::Undetermined => {}
        }

        let request = request_context.apply_to(new_get_request_with_need_commit_ts(
            key.clone(),
            timestamp,
            need_commit_ts,
        ));
        let plan = PlanBuilder::new(rpc, keyspace, request)
            .with_request_context(request_context)
            .with_read_routing(read_routing)
            .resolve_lock(retry_options.lock_backoff, keyspace)
            .retry_multi_region(retry_options.region_backoff)
            .merge(CollectSingle)
            .plan();
        let resp = plan.execute().await?;

        let entry = if resp.not_found {
            None
        } else {
            Some(ValueEntry::new(
                resp.value,
                if need_commit_ts { resp.commit_ts } else { 0 },
            ))
        };

        // Keep buffer cache consistent with the fetched result.
        let (value, commit_ts) = match &entry {
            Some(entry) => (Some(entry.value.clone()), entry.commit_ts),
            None => (None, 0),
        };
        self.buffer.update_read_cache(key, value, commit_ts)?;

        Ok(entry)
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
    /// # use tikv_client::{Result, TransactionClient, Value};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_pessimistic().await?;
    /// let key = "TiKV".to_owned();
    /// let _result: Option<Value> = txn.get_for_update(key).await?;
    /// // now the key "TiKV" is locked, other transactions cannot modify it
    /// // Finish the transaction...
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        self.get_for_update_with_options(key, LockOptions::new())
            .await
    }

    /// Same as [`get_for_update`](Transaction::get_for_update), but allows configuring the
    /// underlying pessimistic lock request.
    pub async fn get_for_update_with_options(
        &mut self,
        key: impl Into<Key>,
        options: LockOptions,
    ) -> Result<Option<Value>> {
        debug!("invoking transactional get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let key = key.into();
            self.lock_keys(iter::once(key.clone())).await?;
            self.get(key).await
        } else {
            let options = options.return_values(true);
            let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
            let mut pairs = self.pessimistic_lock(iter::once(key), options).await?;
            debug_assert!(pairs.len() <= 1);
            match pairs.pop() {
                Some(pair) => Ok(Some(pair.value)),
                None => Ok(None),
            }
        }
    }

    /// Check whether a key exists.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_pessimistic().await?;
    /// let _exists = txn.key_exists("k1".to_owned()).await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
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
    /// # use std::collections::HashMap;
    /// # use tikv_client::{Key, Result, TransactionClient, Value};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let _result: HashMap<Key, Value> = txn
    ///     .batch_get(keys)
    ///     .await?
    ///     .map(|pair| (pair.key, pair.value))
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
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
        let request_context = self.request_context.clone();
        let read_routing = ReadRouting::new(self.options.replica_read, self.options.stale_read)
            .with_seed(timestamp.version() as u32)
            .with_request_source(request_context.request_source());

        self.buffer
            .batch_get_or_else(keys, move |keys| async move {
                let request = request_context.apply_to(new_batch_get_request(keys, timestamp));
                let plan = PlanBuilder::new(rpc, keyspace, request)
                    .with_request_context(request_context)
                    .with_read_routing(read_routing)
                    .resolve_lock(retry_options.lock_backoff, keyspace)
                    .retry_multi_region(retry_options.region_backoff)
                    .merge(Collect)
                    .plan();
                plan.execute()
                    .await
                    .map(|r| r.into_iter().map(Into::into).collect())
            })
            .await
            .map(move |pairs| pairs.map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Create a new 'batch get' request with read options.
    ///
    /// When `BatchGetOption::ReturnCommitTs` is set, TiKV will return the commit timestamp of each
    /// returned value.
    ///
    /// Non-existent entries will not appear in the result. The order of the keys is not retained
    /// in the result.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use std::collections::HashMap;
    /// # use tikv_client::{Key, Result, TransactionClient, ValueEntry, with_return_commit_ts};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let _result: HashMap<Key, ValueEntry> =
    ///     txn.batch_get_with_options(keys, &[with_return_commit_ts().into()])
    ///         .await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[BatchGetOption],
    ) -> Result<HashMap<Key, ValueEntry>> {
        debug!("invoking transactional batch_get request with options");
        self.check_allow_operation().await?;

        let mut opts = BatchGetOptions::default();
        opts.apply(options);
        let need_commit_ts = opts.return_commit_ts();

        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let retry_options = self.options.retry_options.clone();
        let request_context = self.request_context.clone();
        let read_routing = ReadRouting::new(self.options.replica_read, self.options.stale_read)
            .with_seed(timestamp.version() as u32)
            .with_request_source(request_context.request_source());

        let mut out = HashMap::new();
        let mut remote_keys = Vec::new();

        for key in keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(keyspace, KeyMode::Txn))
        {
            match self.buffer.read_value_state(&key) {
                ReadValueState::Local(Some(value)) => {
                    let out_key = key.clone().truncate_keyspace(keyspace);
                    out.insert(out_key, ValueEntry::new(value, 0));
                }
                ReadValueState::Local(None) | ReadValueState::Cached(None) => {}
                ReadValueState::Cached(Some(value)) if !need_commit_ts => {
                    let out_key = key.clone().truncate_keyspace(keyspace);
                    out.insert(out_key, ValueEntry::new(value, 0));
                }
                ReadValueState::Cached(Some(value)) => {
                    if let Some(commit_ts) = self.buffer.cached_commit_ts(&key) {
                        if commit_ts != 0 {
                            let out_key = key.clone().truncate_keyspace(keyspace);
                            out.insert(out_key, ValueEntry::new(value, commit_ts));
                            continue;
                        }
                    }
                    remote_keys.push(key);
                }
                ReadValueState::Undetermined => remote_keys.push(key),
            }
        }

        if remote_keys.is_empty() {
            return Ok(out);
        }

        let request = request_context.apply_to(new_batch_get_request_with_need_commit_ts(
            remote_keys.into_iter(),
            timestamp,
            need_commit_ts,
        ));
        let plan = PlanBuilder::new(rpc, keyspace, request)
            .with_request_context(request_context)
            .with_read_routing(read_routing)
            .resolve_lock(retry_options.lock_backoff, keyspace)
            .retry_multi_region(retry_options.region_backoff)
            .merge(Collect)
            .plan();
        let pairs = plan.execute().await?;

        for pair in pairs {
            let commit_ts = if need_commit_ts { pair.commit_ts } else { 0 };
            self.buffer
                .update_read_cache(pair.key.clone(), Some(pair.value.clone()), commit_ts)?;

            let out_key = pair.key.truncate_keyspace(keyspace);
            out.insert(out_key, ValueEntry::new(pair.value, commit_ts));
        }

        Ok(out)
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
    /// # use tikv_client::{KvPair, Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_pessimistic().await?;
    /// let keys = vec!["foo".to_owned(), "bar".to_owned()];
    /// let _result: Vec<KvPair> = txn.batch_get_for_update(keys).await?;
    /// // now "foo" and "bar" are both locked
    /// // Finish the transaction...
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_get_for_update(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        self.batch_get_for_update_with_options(keys, LockOptions::new())
            .await
    }

    /// Same as [`batch_get_for_update`](Transaction::batch_get_for_update), but allows configuring
    /// the underlying pessimistic lock request.
    pub async fn batch_get_for_update_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: LockOptions,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking transactional batch_get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let keys: Vec<Key> = keys.into_iter().map(|k| k.into()).collect();
            self.lock_keys(keys.clone()).await?;
            Ok(self.batch_get(keys).await?.collect())
        } else {
            let options = options.return_values(true);
            let keyspace = self.keyspace;
            let keys = keys
                .into_iter()
                .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
            let pairs = self
                .pessimistic_lock(keys, options)
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
    /// # use tikv_client::{Key, KvPair, Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let _result: Vec<KvPair> = txn.scan(key1..key2, 10).await?.collect();
    /// // Finish the transaction...
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
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
    /// # use tikv_client::{Key, Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let _result: Vec<Key> = txn.scan_keys(key1..key2, 10).await?.collect();
    /// // Finish the transaction...
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
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
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.put(key, val).await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        trace!("invoking transactional put request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let pipelined_key = key.clone();
        if self.is_pessimistic() {
            self.pessimistic_lock(iter::once(key.clone()), LockOptions::new())
                .await?;
        }
        self.buffer.put(key, value.into());
        self.pipelined_record_key_change(&pipelined_key);
        self.pipelined_maybe_flush().await?;
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
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.insert(key, val).await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn insert(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        debug!("invoking transactional insert request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let pipelined_key = key.clone();
        if self.buffer.get(&key).is_some() {
            return Err(Error::DuplicateKeyInsertion);
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once((key.clone(), kvrpcpb::Assertion::NotExist)),
                LockOptions::new(),
            )
            .await?;
        }
        self.buffer.insert(key, value.into());
        self.pipelined_record_key_change(&pipelined_key);
        self.pipelined_maybe_flush().await?;
        Ok(())
    }

    /// Deletes the given key and its value from the database.
    ///
    /// Deleting a non-existent key will not result in an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let key = "foo".to_owned();
    /// txn.delete(key).await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        debug!("invoking transactional delete request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let pipelined_key = key.clone();
        if self.is_pessimistic() {
            self.pessimistic_lock(iter::once(key.clone()), LockOptions::new())
                .await?;
        }
        self.buffer.delete(key);
        self.pipelined_record_key_change(&pipelined_key);
        self.pipelined_maybe_flush().await?;
        Ok(())
    }

    /// Batch mutate the database.
    ///
    /// Only `Put` and `Delete` are supported.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{transaction::Mutation, Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// let mutations = vec![
    ///     Mutation::Delete("k0".to_owned().into()),
    ///     Mutation::Put("k1".to_owned().into(), b"v1".to_vec()),
    /// ];
    /// txn.batch_mutate(mutations).await?;
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
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
            self.pessimistic_lock(
                mutations.iter().map(|m| m.key().clone()),
                LockOptions::new(),
            )
            .await?;
            for m in mutations {
                let key = m.key().clone();
                self.buffer.mutate(m);
                self.pipelined_record_key_change(&key);
            }
        } else {
            for m in mutations.into_iter() {
                let key = m.key().clone();
                self.buffer.mutate(m);
                self.pipelined_record_key_change(&key);
            }
        }
        self.pipelined_maybe_flush().await?;
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
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// txn.lock_keys(vec!["TiKV".to_owned(), "Rust".to_owned()]).await?;
    /// // ... Do some actions.
    /// txn.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn lock_keys(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        self.lock_keys_with_options(keys, LockOptions::new()).await
    }

    /// Same as [`lock_keys`](Transaction::lock_keys), but allows configuring the underlying
    /// pessimistic lock request.
    pub async fn lock_keys_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: LockOptions,
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
                    let pipelined_key = key.clone();
                    self.buffer.lock(key);
                    self.pipelined_record_key_change(&pipelined_key);
                }
            }
            TransactionKind::Pessimistic(_) => {
                self.pessimistic_lock(keys, options).await?;
            }
        }
        self.pipelined_maybe_flush().await?;
        Ok(())
    }

    /// Commits the actions of the transaction. On success, we return the commit timestamp (or
    /// `None` if there was nothing to commit).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// // ... Do some actions.
    /// let _commit_ts = txn.commit().await?;
    /// # Ok(())
    /// # }
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
        let prewrite_mutations = self.buffer.to_proto_mutations(self.options.assertion_level);
        if prewrite_mutations.is_empty() {
            assert!(primary_key.is_none());
            return Ok(None);
        }
        let commit_keys: Vec<Vec<u8>> = prewrite_mutations
            .iter()
            .filter(|mutation| {
                if mutation.op == kvrpcpb::Op::CheckNotExists as i32 {
                    return false;
                }
                let key: &Key = (&mutation.key).into();
                !self.buffer.key_flags(key).has_prewrite_only()
            })
            .map(|mutation| mutation.key.clone())
            .collect();
        let primary_key = if commit_keys.is_empty() {
            primary_key
        } else {
            let primary_key =
                primary_key.filter(|pk| commit_keys.iter().any(|k| pk == <&Key>::from(k)));
            primary_key.or_else(|| commit_keys.first().map(|k| Key::from(k.clone())))
        };

        if self.options.is_pipelined() {
            let primary_key = primary_key.clone().unwrap_or_else(|| {
                Key::from(
                    prewrite_mutations
                        .first()
                        .expect("checked above")
                        .key
                        .clone(),
                )
            });
            if commit_keys.is_empty() {
                let Some(pipelined) = &mut self.pipelined else {
                    return Err(Error::InvalidPipelinedTransaction {
                        message: "pipelined transaction state is missing".to_owned(),
                    });
                };
                pipelined
                    .flush_all(
                        self.rpc.clone(),
                        self.keyspace,
                        self.timestamp.version(),
                        Vec::<u8>::from(primary_key),
                        self.options.assertion_level,
                        &self.request_context,
                        &self.options.retry_options,
                    )
                    .await?;
                self.set_status(TransactionStatus::Committed);
                return Ok(None);
            }
            let res = self.commit_pipelined(primary_key).await;
            if res.is_ok() {
                self.set_status(TransactionStatus::Committed);
            }
            return res;
        }

        if commit_keys.is_empty() {
            let primary_key = primary_key
                .or_else(|| prewrite_mutations.first().map(|m| Key::from(m.key.clone())));
            let mut committer = Committer::new(
                primary_key,
                prewrite_mutations,
                commit_keys,
                self.timestamp.clone(),
                self.rpc.clone(),
                self.options.clone(),
                self.keyspace,
                self.buffer.get_write_size() as u64,
                self.start_instant,
            );
            committer.request_context = self.request_context.clone();
            let _ = committer.prewrite().await?;
            self.set_status(TransactionStatus::Committed);
            return Ok(None);
        }

        // Match client-go behavior:
        // - local latches only apply to optimistic, non-pipelined transactions
        // - pessimistic transactions bypass latches
        // - pipelined transactions bypass latches (handled separately)
        let latch_guard = if !self.is_pessimistic() && !self.options.is_pipelined() {
            match self.txn_latches.clone() {
                Some(latches) => {
                    let start_ts = self.timestamp.version();
                    let guard = latches.lock(start_ts, commit_keys.clone()).await;
                    if guard.is_stale() {
                        return Err(Error::WriteConflictInLatch { start_ts });
                    }
                    Some(guard)
                }
                None => None,
            }
        } else {
            None
        };

        self.start_auto_heartbeat(
            primary_key
                .clone()
                .expect("primary key must exist when committing"),
        )
        .await;

        let mut committer = Committer::new(
            primary_key,
            prewrite_mutations,
            commit_keys,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.keyspace,
            self.buffer.get_write_size() as u64,
            self.start_instant,
        );
        committer.request_context = self.request_context.clone();

        let res = committer.commit().await;

        if res.is_ok() {
            self.set_status(TransactionStatus::Committed);
        }
        if let (Ok(Some(commit_ts)), Some(guard)) = (&res, &latch_guard) {
            guard.set_commit_ts(commit_ts.version());
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
    /// # use tikv_client::{Result, TransactionClient};
    /// # async fn example() -> Result<()> {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await?;
    /// let mut txn = client.begin_optimistic().await?;
    /// // ... Do some actions.
    /// txn.rollback().await?;
    /// # Ok(())
    /// # }
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

        if self.options.is_pipelined() {
            let res = self.rollback_pipelined().await;
            if res.is_ok() {
                self.set_status(TransactionStatus::Rolledback);
            }
            return res;
        }

        let primary_key = self.buffer.get_primary_key();
        let prewrite_mutations = self.buffer.to_proto_mutations(self.options.assertion_level);
        let commit_keys: Vec<Vec<u8>> = prewrite_mutations
            .iter()
            .filter(|mutation| {
                if mutation.op == kvrpcpb::Op::CheckNotExists as i32 {
                    return false;
                }
                let key: &Key = (&mutation.key).into();
                !self.buffer.key_flags(key).has_prewrite_only()
            })
            .map(|mutation| mutation.key.clone())
            .collect();
        if commit_keys.is_empty() {
            self.set_status(TransactionStatus::Rolledback);
            return Ok(());
        }
        let mut committer = Committer::new(
            primary_key,
            prewrite_mutations,
            commit_keys,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.keyspace,
            self.buffer.get_write_size() as u64,
            self.start_instant,
        );
        committer.request_context = self.request_context.clone();

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
        let request = self.request_context.apply_to(request);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
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
        let request_context = self.request_context.clone();
        let read_routing = ReadRouting::new(self.options.replica_read, self.options.stale_read)
            .with_seed(timestamp.version() as u32)
            .with_request_source(request_context.request_source());

        self.buffer
            .scan_and_fetch(
                range,
                limit,
                !key_only,
                reverse,
                move |new_range, new_limit| async move {
                    let request = request_context.apply_to(new_scan_request(
                        new_range, timestamp, new_limit, key_only, reverse,
                    ));
                    let plan = PlanBuilder::new(rpc, keyspace, request)
                        .with_request_context(request_context)
                        .with_read_routing(read_routing)
                        .resolve_lock(retry_options.lock_backoff, keyspace)
                        .retry_multi_region(retry_options.region_backoff)
                        .merge(Collect)
                        .plan();
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
    async fn pessimistic_lock(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        options: LockOptions,
    ) -> Result<Vec<KvPair>> {
        debug!("acquiring pessimistic lock");
        if !matches!(self.options.kind, TransactionKind::Pessimistic(_)) {
            return Err(Error::InvalidTransactionType);
        }

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

        if options.wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock
            && locks.len() != 1
        {
            return Err(Error::InvalidPessimisticLockOptions {
                message: "WakeUpModeForceLock only supports a single key per request".to_owned(),
            });
        }

        for (key, assertion) in &mut locks {
            if *assertion == kvrpcpb::Assertion::None
                && self.buffer.key_flags(key).has_presume_key_not_exists()
            {
                *assertion = kvrpcpb::Assertion::NotExist;
            }
        }

        let first_key = locks[0].0.clone();
        // we do not set the primary key here, because pessimistic lock request
        // can fail, in which case the keys may not be part of the transaction.
        let primary_lock = self
            .buffer
            .get_primary_key()
            .unwrap_or_else(|| first_key.clone());
        let for_update_ts = self.rpc.clone().get_timestamp().await?;
        self.options.push_for_update_ts(for_update_ts.clone());
        let is_first_lock = !self.has_pessimistic_lock && locks.len() == 1;
        let request = new_pessimistic_lock_request(
            locks.clone().into_iter(),
            primary_lock,
            self.timestamp.clone(),
            MAX_TTL,
            for_update_ts.clone(),
            is_first_lock,
            options,
        );
        let request = self.request_context.apply_to(request);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .preserve_shard()
            .retry_multi_region_preserve_results(self.options.retry_options.region_backoff.clone())
            .merge(CollectWithShard)
            .plan();
        match plan.execute().await {
            Ok(pairs) => {
                // primary key will be set here if needed
                self.buffer.primary_key_or(&first_key);
                self.has_pessimistic_lock = true;

                self.start_auto_heartbeat(
                    self.buffer
                        .get_primary_key()
                        .expect("Primary key should exist"),
                )
                .await;

                for (key, _) in locks {
                    self.buffer.lock(key);
                }

                Ok(pairs)
            }
            Err(Error::PessimisticLockError {
                inner,
                success_keys,
            }) => {
                if !success_keys.is_empty() {
                    let keys = success_keys.iter().cloned().map(Key::from);
                    self.pessimistic_lock_rollback(keys, self.timestamp.clone(), for_update_ts)
                        .await?;
                }
                Err(Error::PessimisticLockError {
                    inner,
                    success_keys,
                })
            }
            Err(err) => Err(err),
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
            start_version,
            for_update_ts,
        );
        let req = self.request_context.apply_to(req);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(self.keyspace)?;

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

    fn pipelined_record_key_change(&mut self, key: &Key) {
        let Some(pipelined) = &mut self.pipelined else {
            return;
        };
        let mutation = self
            .buffer
            .mutation_for_key(key, self.options.assertion_level);
        pipelined.record_mutation(key.clone(), mutation);
    }

    async fn pipelined_maybe_flush(&mut self) -> Result<()> {
        let Some(pipelined) = &mut self.pipelined else {
            return Ok(());
        };
        let Some(primary_key) = self.buffer.get_primary_key() else {
            return Ok(());
        };
        pipelined
            .maybe_flush(
                self.rpc.clone(),
                self.keyspace,
                self.timestamp.version(),
                primary_key.into(),
                self.options.assertion_level,
                &self.request_context,
                &self.options.retry_options,
            )
            .await
    }

    async fn commit_pipelined(&mut self, primary_key: Key) -> Result<Option<Timestamp>> {
        let Some(pipelined) = &mut self.pipelined else {
            return Err(Error::InvalidPipelinedTransaction {
                message: "pipelined transaction state is missing".to_owned(),
            });
        };

        let primary_key_bytes = Vec::<u8>::from(primary_key.clone());
        pipelined
            .flush_all(
                self.rpc.clone(),
                self.keyspace,
                self.timestamp.version(),
                primary_key_bytes,
                self.options.assertion_level,
                &self.request_context,
                &self.options.retry_options,
            )
            .await?;
        let range = pipelined.flushed_range();

        let commit_version = fetch_commit_timestamp(self.rpc.clone(), &self.options).await?;
        let req = new_commit_request_with_primary_key(
            iter::once(primary_key.clone()),
            Some(primary_key),
            self.timestamp.clone(),
            commit_version.clone(),
        );
        let req = self.request_context.apply_to(req);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();

        match plan.execute().await {
            Ok(_) => {}
            Err(e) => {
                if matches!(e, Error::Grpc(_)) {
                    return Err(Error::UndeterminedError(Box::new(e)));
                }
                return Err(e);
            }
        }

        if let Some(range) = range {
            let resolve_concurrency = self
                .options
                .pipelined
                .as_ref()
                .map(|opts| opts.resolve_lock_concurrency_limit())
                .unwrap_or(1);
            let resolve_ctx = self
                .request_context
                .with_request_source(super::pipelined::PIPELINED_REQUEST_SOURCE);
            let pd_client = self.rpc.clone();
            let keyspace = self.keyspace;
            let start_ts = self.timestamp.version();
            let commit_ts = commit_version.version();
            tokio::spawn(async move {
                if let Err(e) = super::pipelined::PipelinedTxnState::resolve_flushed_locks(
                    pd_client,
                    keyspace,
                    resolve_ctx,
                    start_ts,
                    commit_ts,
                    range,
                    resolve_concurrency,
                )
                .await
                {
                    warn!(
                        "Failed to resolve flushed locks after pipelined commit (start_ts={}, commit_ts={}): {}",
                        start_ts, commit_ts, e
                    );
                }
            });
        }

        Ok(Some(commit_version))
    }

    async fn rollback_pipelined(&mut self) -> Result<()> {
        let Some(pipelined) = &mut self.pipelined else {
            return Err(Error::InvalidPipelinedTransaction {
                message: "pipelined transaction state is missing".to_owned(),
            });
        };

        pipelined.cancel();
        if let Err(e) = pipelined.flush_wait().await {
            warn!("pipelined flush wait failed during rollback: {}", e);
        }

        if let Some(range) = pipelined.flushed_range() {
            let resolve_concurrency = self
                .options
                .pipelined
                .as_ref()
                .map(|opts| opts.resolve_lock_concurrency_limit())
                .unwrap_or(1);
            let resolve_ctx = self
                .request_context
                .with_request_source(super::pipelined::PIPELINED_REQUEST_SOURCE);
            let pd_client = self.rpc.clone();
            let keyspace = self.keyspace;
            let start_ts = self.timestamp.version();
            tokio::spawn(async move {
                if let Err(e) = super::pipelined::PipelinedTxnState::resolve_flushed_locks(
                    pd_client,
                    keyspace,
                    resolve_ctx,
                    start_ts,
                    0,
                    range,
                    resolve_concurrency,
                )
                .await
                {
                    warn!(
                        "Failed to resolve flushed locks after pipelined rollback (start_ts={}): {}",
                        start_ts, e
                    );
                }
            });
        }

        Ok(())
    }

    fn is_txn_not_found_heartbeat_error(err: &Error) -> bool {
        match err {
            Error::TxnNotFound { .. } => true,
            Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                !errors.is_empty()
                    && errors
                        .iter()
                        .all(|err| Self::is_txn_not_found_heartbeat_error(err))
            }
            Error::UndeterminedError(inner) => Self::is_txn_not_found_heartbeat_error(inner),
            _ => false,
        }
    }

    async fn start_auto_heartbeat(&mut self, primary_key: Key) {
        debug!("starting auto_heartbeat");
        if !self.options.heartbeat_option.is_auto_heartbeat() || self.is_heartbeat_started {
            return;
        }
        self.is_heartbeat_started = true;

        let status = self.status.clone();
        let start_ts = self.timestamp.clone();
        let region_backoff = self.options.retry_options.region_backoff.clone();
        let rpc = self.rpc.clone();
        let heartbeat_interval = match self.options.heartbeat_option {
            HeartbeatOption::NoHeartbeat => DEFAULT_HEARTBEAT_INTERVAL,
            HeartbeatOption::FixedTime(heartbeat_interval) => heartbeat_interval,
        };
        let start_instant = self.start_instant;
        let keyspace = self.keyspace;
        let request_context = self.request_context.clone();

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
                let request = request_context.apply_to(request);
                let plan = PlanBuilder::new(rpc.clone(), keyspace, request)
                    .retry_multi_region(region_backoff.clone())
                    .merge(CollectSingle)
                    .plan();
                match plan.execute().await {
                    Ok(_) => {}
                    Err(err) if Self::is_txn_not_found_heartbeat_error(&err) => {
                        debug!("auto_heartbeat stopped: {}", err);
                        break;
                    }
                    Err(err) => return Err(err),
                }
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
/// TiKV recommends each RPC packet should be less than around 1MB. We keep KV size of
/// each request below 16KB.
pub const TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;
const TTL_FACTOR: f64 = 6000.0;
/// The number of logical bits in a PD timestamp (used in its `u64` encoding).
///
/// This must match `TimestampExt`'s encoding in `src/timestamp.rs`.
const PD_TS_LOGICAL_BITS: u64 = 18;
/// The default safe window for async commit / 1PC max commit TS.
///
/// This is aligned with `client-go`'s default `tikv-client.async-commit.safe-window` (2s).
const DEFAULT_MAX_COMMIT_TS_SAFE_WINDOW: Duration = Duration::from_secs(2);
/// Default timeout for commit-wait TSO (`Transaction::set_commit_wait_until_tso_timeout`).
const DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT: Duration = Duration::from_secs(1);
/// Poll interval while waiting for commit timestamp to reach `commit_wait_until_tso`.
const COMMIT_WAIT_UNTIL_TSO_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
    /// Try using 1pc rather than 2pc (default is to always use 2pc).
    try_one_pc: bool,
    /// Try to use async commit (default is not to).
    async_commit: bool,
    /// Is the transaction read only? (Default is no).
    read_only: bool,
    /// Which replica to read from for snapshot reads (e.g. `get`, `scan`, `batch_get`).
    replica_read: ReplicaReadType,
    /// If enabled, perform snapshot reads as *stale reads* (see TiKV stale read).
    ///
    /// This is intended for read-only / analytical workloads. Enabling this for a read-write
    /// transaction can break the intuitive "reads reflect the transaction snapshot" expectation.
    stale_read: bool,
    /// How to retry in the event of certain errors.
    retry_options: RetryOptions,
    /// What to do if the transaction is dropped without an attempt to commit or rollback
    check_level: CheckLevel,
    /// Controls TiKV assertion enforcement (`kvrpcpb::Mutation.assertion`).
    assertion_level: kvrpcpb::AssertionLevel,
    /// Safe window added to current timestamp when computing `max_commit_ts` for async commit / 1PC.
    ///
    /// This is a control-plane knob for the async-commit / 1PC protocol. If set too small, TiKV may
    /// reject prewrite due to `max_commit_ts` constraints.
    max_commit_ts_safe_window: Duration,
    /// Ensure the final commit timestamp (PD TSO) is >= this value.
    ///
    /// This is aligned with client-go's `Txn.SetCommitWaitUntilTSO`. `None` means disabled.
    commit_wait_until_tso: Option<u64>,
    /// Timeout for waiting `commit_wait_until_tso` to be reached.
    commit_wait_until_tso_timeout: Duration,
    #[doc(hidden)]
    heartbeat_option: HeartbeatOption,
    pipelined: Option<PipelinedTxnOptions>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeartbeatOption {
    NoHeartbeat,
    FixedTime(Duration),
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        Self::new_pessimistic()
    }
}

impl TransactionOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        let Some(opts) = &self.pipelined else {
            return Ok(());
        };
        if self.is_pessimistic() {
            return Err(Error::InvalidPipelinedTransaction {
                message: "pipelined transactions are only supported for optimistic transactions"
                    .to_owned(),
            });
        }
        if self.try_one_pc || self.async_commit {
            return Err(Error::InvalidPipelinedTransaction {
                message: "pipelined transactions cannot enable 1PC or async-commit".to_owned(),
            });
        }
        if opts.flush_concurrency == 0 {
            return Err(Error::InvalidPipelinedTransaction {
                message: "flush_concurrency must be > 0".to_owned(),
            });
        }
        if opts.resolve_lock_concurrency == 0 {
            return Err(Error::InvalidPipelinedTransaction {
                message: "resolve_lock_concurrency must be > 0".to_owned(),
            });
        }
        if !opts.write_throttle_ratio.is_finite()
            || opts.write_throttle_ratio < 0.0
            || opts.write_throttle_ratio >= 1.0
        {
            return Err(Error::InvalidPipelinedTransaction {
                message: format!(
                    "write_throttle_ratio must be in [0, 1), got {}",
                    opts.write_throttle_ratio
                ),
            });
        }
        if opts.min_flush_keys == 0 {
            return Err(Error::InvalidPipelinedTransaction {
                message: "min_flush_keys must be > 0".to_owned(),
            });
        }
        Ok(())
    }

    /// Default options for an optimistic transaction.
    pub fn new_optimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Optimistic,
            try_one_pc: false,
            async_commit: false,
            read_only: false,
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            retry_options: RetryOptions::default_optimistic(),
            check_level: CheckLevel::Panic,
            assertion_level: kvrpcpb::AssertionLevel::Off,
            max_commit_ts_safe_window: DEFAULT_MAX_COMMIT_TS_SAFE_WINDOW,
            commit_wait_until_tso: None,
            commit_wait_until_tso_timeout: DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
            pipelined: None,
        }
    }

    /// Default options for a pessimistic transaction.
    pub fn new_pessimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Pessimistic(Timestamp::from_version(0)),
            try_one_pc: false,
            async_commit: false,
            read_only: false,
            replica_read: ReplicaReadType::Leader,
            stale_read: false,
            retry_options: RetryOptions::default_pessimistic(),
            check_level: CheckLevel::Panic,
            assertion_level: kvrpcpb::AssertionLevel::Off,
            max_commit_ts_safe_window: DEFAULT_MAX_COMMIT_TS_SAFE_WINDOW,
            commit_wait_until_tso: None,
            commit_wait_until_tso_timeout: DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
            pipelined: None,
        }
    }

    /// Configure TiKV assertion enforcement for this transaction.
    #[must_use]
    pub fn assertion_level(
        mut self,
        assertion_level: kvrpcpb::AssertionLevel,
    ) -> TransactionOptions {
        self.assertion_level = assertion_level;
        self
    }

    /// Try to use async commit.
    #[must_use]
    pub fn use_async_commit(mut self) -> TransactionOptions {
        if self.pipelined.is_some() {
            return self;
        }
        self.async_commit = true;
        self
    }

    /// Try to use 1pc.
    #[must_use]
    pub fn try_one_pc(mut self) -> TransactionOptions {
        if self.pipelined.is_some() {
            return self;
        }
        self.try_one_pc = true;
        self
    }

    /// Make the transaction read only.
    #[must_use]
    pub fn read_only(mut self) -> TransactionOptions {
        self.read_only = true;
        self.pipelined = None;
        self
    }

    /// Enable the pipelined DML protocol (see [`PipelinedTxnOptions`]).
    ///
    /// Enabling this will disable 1PC and async-commit.
    #[must_use]
    pub fn use_pipelined_txn(mut self, options: PipelinedTxnOptions) -> TransactionOptions {
        self.try_one_pc = false;
        self.async_commit = false;
        self.read_only = false;
        self.pipelined = Some(options);
        self
    }

    /// Configure where snapshot reads are sent (leader/follower/learner...).
    #[must_use]
    pub fn replica_read(mut self, replica_read: ReplicaReadType) -> TransactionOptions {
        self.replica_read = replica_read;
        self
    }

    /// Enable/disable *stale reads* for snapshot reads.
    ///
    /// When enabled, read requests will set `kvrpcpb::Context.stale_read=true` and may be served by
    /// a non-leader replica.
    #[must_use]
    pub fn stale_read(mut self, stale_read: bool) -> TransactionOptions {
        self.stale_read = stale_read;
        self
    }

    /// Configure the safe window used to compute `max_commit_ts` for async commit / 1PC.
    ///
    /// TiKV uses `max_commit_ts` to bound how far in the future a transaction is allowed to commit.
    /// A safe window helps avoid commit TS violations due to clock skew / scheduling delays.
    #[must_use]
    pub fn max_commit_ts_safe_window(mut self, safe_window: Duration) -> TransactionOptions {
        self.max_commit_ts_safe_window = safe_window;
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

    fn push_for_update_ts(&mut self, for_update_ts: Timestamp) {
        match &mut self.kind {
            TransactionKind::Optimistic => {
                self.kind = TransactionKind::Pessimistic(for_update_ts);
            }
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

    pub(crate) fn is_pipelined(&self) -> bool {
        self.pipelined.is_some()
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
    prewrite_mutations: Vec<kvrpcpb::Mutation>,
    commit_keys: Vec<Vec<u8>>,
    start_version: Timestamp,
    rpc: Arc<PdC>,
    options: TransactionOptions,
    keyspace: Keyspace,
    #[new(default)]
    request_context: RequestContext,
    #[new(default)]
    undetermined: bool,
    write_size: u64,
    start_instant: Instant,
}

async fn fetch_commit_timestamp<PdC: PdClient>(
    rpc: Arc<PdC>,
    options: &TransactionOptions,
) -> Result<Timestamp> {
    let Some(wait_until_tso) = options.commit_wait_until_tso else {
        return rpc.clone().get_timestamp().await;
    };

    let timeout = options.commit_wait_until_tso_timeout;
    let start = tokio::time::Instant::now();
    loop {
        let ts = rpc.clone().get_timestamp().await?;
        if ts.version() >= wait_until_tso {
            return Ok(ts);
        }
        if start.elapsed() >= timeout {
            return Err(Error::CommitTsLag {
                commit_ts: ts.version(),
                wait_until_tso,
            });
        }
        tokio::time::sleep(COMMIT_WAIT_UNTIL_TSO_POLL_INTERVAL).await;
    }
}

impl<PdC: PdClient> Committer<PdC> {
    fn add_physical_ms(version: u64, delta_ms: u64) -> u64 {
        version.saturating_add(delta_ms.saturating_mul(1_u64 << PD_TS_LOGICAL_BITS))
    }

    fn extract_physical_ms(version: u64) -> u64 {
        version >> PD_TS_LOGICAL_BITS
    }

    fn min_commit_ts(&self) -> u64 {
        let start_ts = self.start_version.version();
        let mut min_commit_ts = start_ts.saturating_add(1);
        if let TransactionKind::Pessimistic(for_update_ts) = &self.options.kind {
            min_commit_ts = min_commit_ts.max(for_update_ts.version().saturating_add(1));
        }
        min_commit_ts
    }

    fn max_commit_ts(&self, elapsed_ms: u64) -> u64 {
        let current_ts = Self::add_physical_ms(self.start_version.version(), elapsed_ms);
        let safe_window_ms = self
            .options
            .max_commit_ts_safe_window
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        Self::add_physical_ms(current_ts, safe_window_ms)
    }

    async fn commit_primary_checked(&mut self) -> Result<Timestamp> {
        match self.commit_primary().await {
            Ok(commit_ts) => Ok(commit_ts),
            Err(e) => {
                if self.undetermined {
                    Err(Error::UndeterminedError(Box::new(e)))
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn commit(mut self) -> Result<Option<Timestamp>> {
        debug!("committing");

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
                Some(min_commit_ts) => min_commit_ts,
                None => {
                    // TiKV returns `min_commit_ts = 0` when async commit can't proceed.
                    // Fall back to normal 2PC and acquire commit TS from PD.
                    self.options.async_commit = false;
                    self.commit_primary_checked().await?
                }
            }
        } else {
            self.commit_primary_checked().await?
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
        crate::trace::trace_event(
            crate::trace::CATEGORY_TXN_2PC,
            "txn.prewrite.start",
            [
                (
                    "start_ts",
                    crate::trace::TraceValue::U64(self.start_version.version()),
                ),
                (
                    "mutation_count",
                    crate::trace::TraceValue::U64(self.prewrite_mutations.len() as u64),
                ),
                (
                    "async_commit",
                    crate::trace::TraceValue::Bool(self.options.async_commit),
                ),
                (
                    "try_one_pc",
                    crate::trace::TraceValue::Bool(self.options.try_one_pc),
                ),
                (
                    "pessimistic",
                    crate::trace::TraceValue::Bool(self.options.is_pessimistic()),
                ),
            ],
        );
        let primary_lock = self.primary_key.clone().unwrap();
        let elapsed_ms = self.start_instant.elapsed().as_millis() as u64;
        let lock_ttl = self.calc_txn_lock_ttl().saturating_add(elapsed_ms);
        let mut request = match &self.options.kind {
            TransactionKind::Optimistic => new_prewrite_request(
                self.prewrite_mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl,
            ),
            TransactionKind::Pessimistic(for_update_ts) => new_pessimistic_prewrite_request(
                self.prewrite_mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl,
                for_update_ts.clone(),
            ),
        };

        request.use_async_commit = self.options.async_commit;
        request.try_one_pc = self.options.try_one_pc;
        request.min_commit_ts = self.min_commit_ts();
        let primary_key = self
            .primary_key
            .as_ref()
            .expect("prewrite requires primary key");
        request.secondaries = self
            .commit_keys
            .iter()
            .filter(|key| primary_key != <&Key>::from(*key))
            .cloned()
            .collect();
        if self.options.async_commit || self.options.try_one_pc {
            request.max_commit_ts = self.max_commit_ts(elapsed_ms);

            // ref: https://github.com/pingcap/tidb/issues/33641
            // Make sure the TTL satisfies:
            // `max_commit_ts.physical < start_ts.physical + ttl`.
            if self.options.async_commit && self.options.is_pessimistic() {
                let safe_ttl_ms = Self::extract_physical_ms(request.max_commit_ts)
                    .saturating_sub(Self::extract_physical_ms(self.start_version.version()))
                    .saturating_add(1);
                request.lock_ttl = request.lock_ttl.max(safe_ttl_ms);
            }
        }

        request.assertion_level = self.options.assertion_level.into();

        let request = self.request_context.apply_to(request);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .merge(CollectError)
            .extract_error()
            .plan();
        let response = match plan.execute().await {
            Ok(response) => response,
            Err(Error::MultipleKeyErrors(mut errors)) if errors.len() == 1 => {
                return Err(errors.pop().expect("len checked"));
            }
            Err(Error::MultipleKeyErrors(errors)) => return Err(Error::MultipleKeyErrors(errors)),
            Err(Error::ExtractedErrors(mut errors)) if errors.len() == 1 => {
                return Err(errors.pop().expect("len checked"));
            }
            Err(Error::ExtractedErrors(errors)) => return Err(Error::MultipleKeyErrors(errors)),
            Err(err) => return Err(err),
        };

        if self.options.try_one_pc && response.len() == 1 {
            if response[0].one_pc_commit_ts != 0 {
                return Ok(Timestamp::try_from_version(response[0].one_pc_commit_ts));
            }
            if response[0].min_commit_ts != 0 {
                return Err(Error::StringError(
                    "invalid PrewriteResponse: min_commit_ts must be 0 when 1PC falls back to 2PC"
                        .to_owned(),
                ));
            }
            self.options.async_commit = false;
        }

        self.options.try_one_pc = false;

        let min_commit_ts = response
            .iter()
            .map(|r| {
                assert_eq!(r.one_pc_commit_ts, 0);
                r.min_commit_ts
            })
            .max()
            .and_then(Timestamp::try_from_version);

        Ok(min_commit_ts)
    }

    /// Commits the primary key and returns the commit version
    async fn commit_primary(&mut self) -> Result<Timestamp> {
        debug!("committing primary");
        let primary_key = self.primary_key.clone();
        let keys = primary_key.clone().into_iter();
        let commit_version = fetch_commit_timestamp(self.rpc.clone(), &self.options).await?;
        let req = new_commit_request_with_primary_key(
            keys,
            primary_key,
            self.start_version.clone(),
            commit_version.clone(),
        );
        let req = self.request_context.apply_to(req);
        let plan = PlanBuilder::new(self.rpc.clone(), self.keyspace, req)
            .with_request_context(self.request_context.clone())
            .resolve_lock(
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();
        plan.execute()
            .inspect_err(|e| {
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

    async fn commit_secondary(self, commit_version: Timestamp) -> Result<()> {
        debug!("committing secondary");
        let keys_len = self.commit_keys.len();
        let primary_only = keys_len == 1;
        #[cfg(not(feature = "integration-tests"))]
        let keys = self.commit_keys.into_iter();

        #[cfg(feature = "integration-tests")]
        let keys = self.commit_keys.into_iter().take({
            // Truncate mutation to a new length as `percent/100`.
            // Return error when truncate to zero.
            let fp = || -> Result<usize> {
                let mut new_len = keys_len;
                fail_point!("before-commit-secondary", |percent| {
                    let percent = percent.unwrap().parse::<usize>().unwrap();
                    new_len = keys_len * percent / 100;
                    if new_len == 0 {
                        Err(Error::StringError(
                            "failpoint: before-commit-secondary return error".to_owned(),
                        ))
                    } else {
                        debug!(
                            "failpoint: before-commit-secondary truncate mutation {} -> {}",
                            keys_len, new_len
                        );
                        Ok(new_len)
                    }
                });
                Ok(new_len)
            };
            fp()?
        });

        let req = if self.options.async_commit {
            let keys = keys.map(Key::from);
            new_commit_request(keys, self.start_version, commit_version)
        } else if primary_only {
            return Ok(());
        } else {
            let primary_key = self.primary_key.unwrap();
            let keys = keys.map(Key::from).filter(|key| &primary_key != key);
            new_commit_request(keys, self.start_version, commit_version)
        };
        let req = self.request_context.apply_to(req);
        let keyspace = self.keyspace;
        let plan = PlanBuilder::new(self.rpc, keyspace, req)
            .with_request_context(self.request_context.clone())
            .resolve_lock(self.options.retry_options.lock_backoff, keyspace)
            .retry_multi_region(self.options.retry_options.region_backoff)
            .extract_error()
            .plan();
        plan.execute().await.truncate_keyspace(keyspace)?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        debug!("rolling back");
        if self.commit_keys.is_empty() {
            return Ok(());
        }
        let keyspace = self.keyspace;
        let keys = self.commit_keys.into_iter().map(Key::from);
        match self.options.kind {
            TransactionKind::Optimistic => {
                let req = new_batch_rollback_request(keys, self.start_version);
                let req = self.request_context.apply_to(req);
                let plan = PlanBuilder::new(self.rpc, keyspace, req)
                    .with_request_context(self.request_context.clone())
                    .resolve_lock(self.options.retry_options.lock_backoff, keyspace)
                    .retry_multi_region(self.options.retry_options.region_backoff)
                    .extract_error()
                    .plan();
                plan.execute().await.truncate_keyspace(keyspace)?;
            }
            TransactionKind::Pessimistic(for_update_ts) => {
                let req = new_pessimistic_rollback_request(keys, self.start_version, for_update_ts);
                let req = self.request_context.apply_to(req);
                let plan = PlanBuilder::new(self.rpc, keyspace, req)
                    .with_request_context(self.request_context.clone())
                    .resolve_lock(self.options.retry_options.lock_backoff, keyspace)
                    .retry_multi_region(self.options.retry_options.region_backoff)
                    .extract_error()
                    .plan();
                plan.execute().await.truncate_keyspace(keyspace)?;
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use fail::FailScenario;
    use serial_test::serial;

    use crate::interceptor::override_priority_if_unset;
    use crate::interceptor::rpc_interceptor;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::pd::PdClient;
    use crate::proto::errorpb;
    use crate::proto::keyspacepb;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::proto::pdpb::Timestamp;
    use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
    use crate::request::EncodeKeyspace;
    use crate::request::KeyMode;
    use crate::request::Keyspace;
    use crate::resource_manager;
    use crate::store::{RegionStore, Store};
    use crate::timestamp::TimestampExt;
    use crate::transaction::CheckLevel;
    use crate::transaction::FlagsOp;
    use crate::transaction::HeartbeatOption;
    use crate::transaction::LockOptions;
    use crate::with_return_commit_ts;
    use crate::CommandPriority;
    use crate::Error;
    use crate::Key;
    use crate::ReplicaReadType;
    use crate::RequestContext;
    use crate::Transaction;
    use crate::TransactionOptions;
    use crate::ValueEntry;

    #[test]
    fn heartbeat_txn_not_found_error_is_detected() {
        let err = Error::TxnNotFound { start_ts: 7 };
        assert!(Transaction::<MockPdClient>::is_txn_not_found_heartbeat_error(&err));

        let err = Error::MultipleKeyErrors(vec![Error::TxnNotFound { start_ts: 7 }]);
        assert!(Transaction::<MockPdClient>::is_txn_not_found_heartbeat_error(&err));

        let err = Error::MultipleKeyErrors(vec![
            Error::TxnNotFound { start_ts: 7 },
            Error::Unimplemented,
        ]);
        assert!(
            !Transaction::<MockPdClient>::is_txn_not_found_heartbeat_error(&err),
            "should not suppress non-TxnNotFound errors"
        );

        let err = Error::UndeterminedError(Box::new(Error::TxnNotFound { start_ts: 7 }));
        assert!(Transaction::<MockPdClient>::is_txn_not_found_heartbeat_error(&err));
    }

    #[tokio::test]
    async fn test_keyspace_encodes_commit_request_primary_key() -> crate::Result<()> {
        let keyspace_id = 4242;
        let keyspace = Keyspace::Enable { keyspace_id };

        let expected_key = vec![b'x', 0, 16, 146, b'k', b'e', b'y'];
        let expected_key_cloned = expected_key.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert_eq!(req.keys, vec![expected_key_cloned.clone()]);
                    assert_eq!(req.primary_key, expected_key_cloned);
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type: {:?}", req.type_id());
                }
            },
        )));

        let mut committer = super::Committer::<MockPdClient>::new(
            Some(crate::Key::from(expected_key)),
            vec![],
            vec![],
            Timestamp::from_version(42),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            keyspace,
            0,
            std::time::Instant::now(),
        );

        committer.commit_primary().await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_get_respects_no_resolve_regions() -> Result<(), io::Error> {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let dispatches_cloned = dispatches.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::GetRequest>().is_none() {
                    unreachable!("unexpected request type: {:?}", req.type_id());
                }

                let attempt = dispatches_cloned.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.region_error = Some(errorpb::Error {
                        stale_command: Some(errorpb::StaleCommand::default()),
                        ..Default::default()
                    });
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>)
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(42),
            pd_client,
            TransactionOptions::new_optimistic()
                .no_resolve_regions()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let res = txn.get("key".to_owned()).await;
        assert!(matches!(res, Err(Error::RegionError(_))));
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_with_options_sets_need_commit_ts_and_returns_commit_ts() -> crate::Result<()>
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cloned = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    unreachable!("unexpected request type: {:?}", req.type_id());
                };

                let call = calls_cloned.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    assert!(!req.need_commit_ts);
                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else {
                    assert!(req.need_commit_ts);
                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.commit_ts = 77;
                    Ok(Box::new(resp) as Box<dyn Any>)
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(42),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let value = txn.get("k".to_owned()).await?;
        assert_eq!(value, Some(b"v".to_vec()));

        let entry = txn
            .get_with_options("k".to_owned(), &[with_return_commit_ts().into()])
            .await?;
        assert_eq!(entry, Some(ValueEntry::new(b"v".to_vec(), 77)));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_get_with_options_preserves_commit_ts_and_decodes_keyspace(
    ) -> crate::Result<()> {
        let keyspace = Keyspace::Enable { keyspace_id: 42 };
        let encoded_a = Key::from("a".to_owned()).encode_keyspace(keyspace, KeyMode::Txn);
        let encoded_b = Key::from("b".to_owned()).encode_keyspace(keyspace, KeyMode::Txn);
        let encoded_d = Key::from("d".to_owned()).encode_keyspace(keyspace, KeyMode::Txn);

        let expected_keys = vec![
            Vec::<u8>::from(encoded_a.clone()),
            Vec::<u8>::from(encoded_b.clone()),
            Vec::<u8>::from(encoded_d.clone()),
        ];

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() else {
                    unreachable!("unexpected request type: {:?}", req.type_id());
                };
                assert!(req.need_commit_ts);
                assert_eq!(req.keys, expected_keys);

                let resp = kvrpcpb::BatchGetResponse {
                    pairs: vec![
                        kvrpcpb::KvPair {
                            key: Vec::<u8>::from(encoded_a.clone()),
                            value: b"a1".to_vec(),
                            commit_ts: 11,
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: Vec::<u8>::from(encoded_b.clone()),
                            value: b"b1".to_vec(),
                            commit_ts: 22,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(42),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            keyspace,
            RequestContext::default(),
            None,
        );

        let entries = txn
            .batch_get_with_options(
                ["a".to_owned(), "b".to_owned(), "d".to_owned()],
                &[with_return_commit_ts().into()],
            )
            .await?;

        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries.get(&Key::from("a".to_owned())),
            Some(&ValueEntry::new(b"a1".to_vec(), 11))
        );
        assert_eq!(
            entries.get(&Key::from("b".to_owned())),
            Some(&ValueEntry::new(b"b1".to_vec(), 22))
        );
        assert!(!entries.contains_key(&Key::from("d".to_owned())));

        Ok(())
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
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
            RequestContext::default(),
            None,
        );
        heartbeat_txn.put(key1.clone(), "foo").await.unwrap();
        let handle = tokio::runtime::Handle::current();
        let heartbeat_txn_handle = tokio::task::spawn_blocking(move || {
            let mut heartbeat_txn = heartbeat_txn;
            assert!(handle.block_on(heartbeat_txn.commit()).is_ok());
        });
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        heartbeat_txn_handle.await.unwrap();
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        scenario.teardown();
        Ok(())
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
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
            RequestContext::default(),
            None,
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

    #[tokio::test]
    async fn test_pessimistic_lock_request_propagates_lock_options_and_is_first_lock() {
        let lock_calls = Arc::new(AtomicUsize::new(0));
        let lock_calls_cloned = lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() else {
                    unreachable!("unexpected request type in lock options test");
                };
                let call = lock_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.wait_timeout, 123);
                assert!(req.check_existence);
                assert!(req.lock_only_if_exists);
                assert_eq!(
                    req.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal as i32
                );
                assert!(!req.return_values);
                assert_eq!(req.min_commit_ts, req.for_update_ts.saturating_add(1));
                if call == 0 {
                    assert!(req.is_first_lock);
                } else {
                    assert!(!req.is_first_lock);
                }
                Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let options = LockOptions::new()
            .wait_timeout_ms(123)
            .check_existence(true)
            .lock_only_if_exists(true)
            .wake_up_mode(kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal);

        txn.lock_keys_with_options(vec![vec![1u8]], options.clone())
            .await
            .unwrap();
        txn.lock_keys_with_options(vec![vec![2u8]], options)
            .await
            .unwrap();
        assert_eq!(lock_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_infers_not_exist_assertion_from_presume_key_not_exists() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() else {
                    unreachable!("unexpected request type in presumeKNE lock test");
                };
                assert_eq!(req.mutations.len(), 1);
                assert_eq!(
                    req.mutations[0].assertion,
                    kvrpcpb::Assertion::NotExist as i32
                );
                Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        txn.update_key_flags(vec![1u8], [FlagsOp::SetPresumeKeyNotExists]);
        txn.lock_keys_with_options(vec![vec![1u8]], LockOptions::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_pessimistic_lock_rolls_back_success_keys_on_partial_failure() {
        let rollback_reqs: Arc<Mutex<Vec<kvrpcpb::PessimisticRollbackRequest>>> =
            Arc::new(Mutex::new(Vec::new()));
        let rollback_reqs_cloned = rollback_reqs.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    let region_id = req
                        .context
                        .as_ref()
                        .map(|ctx| ctx.region_id)
                        .unwrap_or_default();
                    match region_id {
                        1 => Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>),
                        2 => {
                            let mut key_error = kvrpcpb::KeyError::default();
                            key_error.conflict = Some(kvrpcpb::WriteConflict {
                                start_ts: req.start_version,
                                conflict_ts: 0,
                                conflict_commit_ts: 0,
                                key: req.mutations[0].key.clone(),
                                primary: req.primary_lock.clone(),
                                reason: 0,
                            });

                            let mut resp = kvrpcpb::PessimisticLockResponse::default();
                            resp.errors.push(key_error);
                            Ok(Box::new(resp) as Box<dyn Any>)
                        }
                        _ => unreachable!("unexpected region id in partial rollback test"),
                    }
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                {
                    rollback_reqs_cloned.lock().unwrap().push(req.clone());
                    Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in partial rollback test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let err = txn
            .lock_keys_with_options(vec![vec![1u8], vec![20u8]], LockOptions::new())
            .await
            .expect_err("lock must fail");
        assert!(err.is_write_conflict(), "{err:?}");

        let rollback_reqs = rollback_reqs.lock().unwrap().clone();
        assert_eq!(rollback_reqs.len(), 1);
        let rollback = &rollback_reqs[0];
        assert_eq!(rollback.start_version, 10);
        assert_eq!(rollback.for_update_ts, 0);
        assert_eq!(rollback.keys, vec![vec![1u8]]);
    }

    #[tokio::test]
    async fn test_get_for_update_force_lock_mode_reads_value_from_results() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() else {
                    unreachable!("unexpected request type in force-lock test");
                };
                assert_eq!(
                    req.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                );

                let mut resp = kvrpcpb::PessimisticLockResponse::default();
                resp.results.push(kvrpcpb::PessimisticLockKeyResult {
                    r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32,
                    value: b"v1".to_vec(),
                    existence: true,
                    locked_with_conflict_ts: 0,
                    skip_resolving_lock: false,
                });
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let value = txn
            .get_for_update_with_options(
                vec![1u8],
                LockOptions::new()
                    .wake_up_mode(kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value, b"v1".to_vec());
    }

    #[tokio::test]
    async fn test_force_lock_mode_rejects_multi_key_requests() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            unreachable!("force-lock validation must happen before RPC dispatch")
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let err = txn
            .lock_keys_with_options(
                vec![vec![1u8], vec![2u8]],
                LockOptions::new()
                    .wake_up_mode(kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock),
            )
            .await
            .expect_err("force-lock must reject multi-key lock requests");
        assert!(
            matches!(err, crate::Error::InvalidPessimisticLockOptions { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn test_async_commit_fallback_to_2pc_when_min_commit_ts_is_zero() -> Result<(), io::Error>
    {
        let expected_safe_window = Duration::from_millis(1234);
        let commit_reqs: Arc<Mutex<Vec<Vec<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
        let prewrite_fields: Arc<Mutex<Option<(u64, u64)>>> = Arc::new(Mutex::new(None));

        let commit_reqs_cloned = commit_reqs.clone();
        let prewrite_fields_cloned = prewrite_fields.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    *prewrite_fields_cloned.lock().unwrap() =
                        Some((req.min_commit_ts, req.max_commit_ts));
                    let mut resp = kvrpcpb::PrewriteResponse::default();
                    resp.min_commit_ts = 0;
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    commit_reqs_cloned.lock().unwrap().push(req.keys.clone());
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in async commit fallback test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .use_async_commit()
                .max_commit_ts_safe_window(expected_safe_window),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.put("k2".to_owned(), "v2").await.unwrap();
        txn.commit().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if commit_reqs.lock().unwrap().len() >= 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let commit_reqs = commit_reqs.lock().unwrap().clone();
        assert_eq!(commit_reqs.len(), 2);
        assert_eq!(commit_reqs[0].len(), 1);
        assert_eq!(commit_reqs[1].len(), 1);
        assert_ne!(commit_reqs[0][0], commit_reqs[1][0]);

        let (min_commit_ts, max_commit_ts) = prewrite_fields.lock().unwrap().unwrap();
        assert!(min_commit_ts > Timestamp::default().version());
        assert!(max_commit_ts > 0);
        let max_commit_ts_physical_ms = max_commit_ts >> super::PD_TS_LOGICAL_BITS;
        assert!(max_commit_ts_physical_ms >= expected_safe_window.as_millis() as u64);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_commit_uses_min_commit_ts_when_available() -> Result<(), io::Error> {
        let expected_min_commit_ts = 42_u64;

        let commit_reqs: Arc<Mutex<Vec<Vec<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
        let commit_reqs_cloned = commit_reqs.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    let mut resp = kvrpcpb::PrewriteResponse::default();
                    resp.min_commit_ts = expected_min_commit_ts;
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    commit_reqs_cloned.lock().unwrap().push(req.keys.clone());
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in async commit success test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().use_async_commit(),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.put("k2".to_owned(), "v2").await.unwrap();
        let commit_ts = txn.commit().await.unwrap().unwrap();
        assert_eq!(commit_ts.version(), expected_min_commit_ts);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !commit_reqs.lock().unwrap().is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let commit_reqs = commit_reqs.lock().unwrap().clone();
        assert_eq!(commit_reqs.len(), 1);
        assert_eq!(commit_reqs[0].len(), 2);

        Ok(())
    }

    fn assert_request_context(
        ctx: &kvrpcpb::Context,
        expected_source: &str,
        expected_tag: &[u8],
        expected_group_name: &str,
    ) {
        assert!(
            ctx.request_source.ends_with(expected_source),
            "unexpected request_source: got={}, expected suffix={}",
            ctx.request_source,
            expected_source
        );
        assert_eq!(ctx.resource_group_tag, expected_tag);
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .expect("missing resource_control_context")
                .resource_group_name,
            expected_group_name
        );
    }

    #[tokio::test]
    async fn test_txn_request_context_applied_to_get_prewrite_commit() -> Result<(), io::Error> {
        let expected_source = "txn-ut";
        let expected_tag = b"tag-ut".to_vec();
        let expected_group_name = "rg-ut";
        let expected_disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull;
        let expected_txn_source = 42_u64;

        let hook_source = expected_source.to_owned();
        let hook_tag = expected_tag.clone();
        let hook_group_name = expected_group_name.to_owned();
        let hook_disk_full_opt = expected_disk_full_opt;
        let hook_txn_source = expected_txn_source;

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let ctx = req.context.as_ref().expect("missing context on GetRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.not_found = true;
                    Ok(Box::new(resp) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on PrewriteRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on CommitRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in request context test");
                }
            },
        )));

        let request_context = RequestContext::default()
            .with_request_source(expected_source)
            .with_resource_group_tag(expected_tag)
            .with_resource_group_name(expected_group_name)
            .with_disk_full_opt(expected_disk_full_opt)
            .with_txn_source(expected_txn_source);

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            request_context,
            None,
        );

        let got = txn.get("k".to_owned()).await.unwrap();
        assert!(got.is_none());

        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.commit().await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_txn_request_context_applied_to_lock_resolution() -> Result<(), io::Error> {
        let expected_source = "txn-ut-lock";
        let expected_tag = b"tag-ut-lock".to_vec();
        let expected_group_name = "rg-ut-lock";

        let hook_source = expected_source.to_owned();
        let hook_tag = expected_tag.clone();
        let hook_group_name = expected_group_name.to_owned();
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let ctx = req.context.as_ref().expect("missing context on GetRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);

                    let call = get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        let mut resp = kvrpcpb::GetResponse::default();
                        let mut key_error = kvrpcpb::KeyError::default();
                        key_error.locked = Some(kvrpcpb::LockInfo {
                            key: req.key.clone(),
                            primary_lock: req.key.clone(),
                            lock_version: 1,
                            lock_ttl: 0,
                            ..Default::default()
                        });
                        resp.error = Some(key_error);
                        Ok(Box::new(resp) as Box<dyn Any>)
                    } else {
                        let mut resp = kvrpcpb::GetResponse::default();
                        resp.not_found = true;
                        Ok(Box::new(resp) as Box<dyn Any>)
                    }
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CleanupRequest>() {
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on CleanupRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);
                    Ok(Box::<kvrpcpb::CleanupResponse>::default() as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on ResolveLockRequest");
                    assert_request_context(ctx, &hook_source, &hook_tag, &hook_group_name);
                    Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in lock resolution context test");
                }
            },
        )));

        let request_context = RequestContext::default()
            .with_request_source(expected_source)
            .with_resource_group_tag(expected_tag)
            .with_resource_group_name(expected_group_name);

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            request_context,
            None,
        );

        let got = txn.get(vec![1]).await.unwrap();
        assert!(got.is_none());
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_resource_group_tagger_sees_patched_request_source_on_retry(
    ) -> Result<(), io::Error> {
        let input_request_source = "unit-test-source";

        type CallLog = Vec<(bool, String, Vec<u8>, u64)>;
        let calls: Arc<Mutex<CallLog>> = Arc::new(Mutex::new(Vec::new()));
        let calls_cloned = calls.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_cloned = call_count.clone();

        let pd_client = Arc::new(ReplicaReadPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    unreachable!("unexpected request type in tagger retry test");
                };
                let ctx = req.context.as_ref().expect("missing context on GetRequest");
                let store_id = ctx.peer.as_ref().map(|p| p.store_id).unwrap_or(0);
                calls_cloned.lock().unwrap().push((
                    ctx.is_retry_request,
                    ctx.request_source.clone(),
                    ctx.resource_group_tag.clone(),
                    store_id,
                ));

                let call = call_count_cloned.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.region_error = Some(crate::proto::errorpb::Error::default());
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = true;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let request_context = RequestContext::default()
            .with_request_source(input_request_source)
            .with_resource_group_name("rg");

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Follower)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            request_context,
            None,
        );

        txn.set_resource_group_tagger(|info, ctx| {
            format!("{}:{}", info.attempt, ctx.request_source).into_bytes()
        });

        let got = txn.get("k".to_owned()).await.unwrap();
        assert!(got.is_none());

        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            (
                false,
                format!("follower_{input_request_source}"),
                format!("0:follower_{input_request_source}").into_bytes(),
                2,
            )
        );
        assert_eq!(
            calls[1],
            (
                true,
                format!("retry_follower_leader_{input_request_source}"),
                format!("1:retry_follower_leader_{input_request_source}").into_bytes(),
                1,
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resource_control_override_priority_if_unset_respects_explicit_override(
    ) -> Result<(), io::Error> {
        let ctxs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let ctxs_cloned = ctxs.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    unreachable!("unexpected request type in override priority test");
                };
                let ctx = req.context.as_ref().expect("missing context on GetRequest");
                let override_priority = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("missing resource_control_context")
                    .override_priority;
                ctxs_cloned.lock().unwrap().push(override_priority);

                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = true;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let base_request_context = RequestContext::default().with_resource_group_name("rg");

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .read_only()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            base_request_context.clone().with_rpc_interceptors({
                let mut chain = crate::interceptor::RpcInterceptorChain::new();
                chain.link(override_priority_if_unset(7));
                chain
            }),
            None,
        );
        let _ = txn.get("k".to_owned()).await.unwrap();

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            base_request_context
                .with_resource_control_override_priority(99)
                .with_rpc_interceptors({
                    let mut chain = crate::interceptor::RpcInterceptorChain::new();
                    chain.link(override_priority_if_unset(7));
                    chain
                }),
            None,
        );
        let _ = txn.get("k".to_owned()).await.unwrap();

        let ctxs = ctxs.lock().unwrap().clone();
        assert_eq!(ctxs, vec![7, 99]);
        Ok(())
    }

    #[tokio::test]
    async fn test_fixed_resource_group_tag_takes_precedence_over_tagger() -> Result<(), io::Error> {
        let expected_tag = b"fixed-tag".to_vec();
        let expected_tag_for_hook = expected_tag.clone();
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let tagger_calls_cloned = tagger_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    unreachable!("unexpected request type in fixed tag test");
                };
                let ctx = req.context.as_ref().expect("missing context on GetRequest");
                assert_eq!(ctx.resource_group_tag, expected_tag_for_hook);
                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = true;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let request_context = RequestContext::default()
            .with_resource_group_name("rg")
            .with_resource_group_tag(expected_tag.clone());

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            request_context,
            None,
        );

        txn.set_resource_group_tagger(move |_, _| {
            tagger_calls_cloned.fetch_add(1, Ordering::SeqCst);
            b"tagger-tag".to_vec()
        });

        let _ = txn.get("k".to_owned()).await.unwrap();
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_rpc_interceptor_can_override_priority_penalty_and_tag() -> Result<(), io::Error> {
        let expected_penalty = resource_manager::Consumption {
            r_r_u: 1.0,
            w_r_u: 2.0,
            ..Default::default()
        };
        let expected_penalty_for_hook = expected_penalty.clone();
        let expected_penalty_for_interceptor = expected_penalty.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    unreachable!("unexpected request type in interceptor test");
                };
                let ctx = req.context.as_ref().expect("missing context on GetRequest");
                assert_eq!(ctx.priority, i32::from(CommandPriority::High));
                assert_eq!(ctx.resource_group_tag, b"it".to_vec());
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .as_ref()
                    .expect("missing resource_control_context");
                assert_eq!(resource_ctl_ctx.resource_group_name, "rg");
                assert_eq!(resource_ctl_ctx.override_priority, 99);
                assert_eq!(
                    resource_ctl_ctx.penalty.as_ref(),
                    Some(&expected_penalty_for_hook)
                );

                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = true;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let request_context = RequestContext::default()
            .with_resource_group_name("rg")
            .with_priority(CommandPriority::Low)
            .with_resource_control_override_priority(99)
            .with_resource_control_penalty(resource_manager::Consumption {
                r_r_u: 0.0,
                w_r_u: 0.0,
                ..Default::default()
            })
            .with_resource_group_tagger(Arc::new(|_, _| b"tagger".to_vec()))
            .add_rpc_interceptor(override_priority_if_unset(7))
            .add_rpc_interceptor(rpc_interceptor("ut", move |_, ctx| {
                ctx.priority = CommandPriority::High.into();
                ctx.resource_group_tag = b"it".to_vec();
                let resource_ctl_ctx = ctx
                    .resource_control_context
                    .get_or_insert(kvrpcpb::ResourceControlContext::default());
                resource_ctl_ctx.penalty = Some(expected_penalty_for_interceptor.clone());
            }));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            request_context,
            None,
        );

        let _ = txn.get("k".to_owned()).await.unwrap();
        Ok(())
    }

    #[derive(Clone)]
    struct ReplicaReadPdClient {
        client: MockKvClient,
        region: RegionWithLeader,
    }

    impl ReplicaReadPdClient {
        fn new(client: MockKvClient) -> Self {
            let leader = metapb::Peer {
                id: 101,
                store_id: 1,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let follower = metapb::Peer {
                id: 102,
                store_id: 2,
                role: metapb::PeerRole::Voter as i32,
                is_witness: false,
            };
            let learner = metapb::Peer {
                id: 103,
                store_id: 3,
                role: metapb::PeerRole::Learner as i32,
                is_witness: false,
            };
            let witness = metapb::Peer {
                id: 104,
                store_id: 4,
                role: metapb::PeerRole::Voter as i32,
                is_witness: true,
            };

            let mut region = metapb::Region::default();
            region.id = 1;
            region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 0,
            });
            region.peers = vec![leader.clone(), follower, learner, witness];

            Self {
                client,
                region: RegionWithLeader {
                    region,
                    leader: Some(leader),
                },
            }
        }
    }

    #[async_trait]
    impl PdClient for ReplicaReadPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: RegionWithLeader,
        ) -> crate::Result<RegionStore> {
            Ok(RegionStore::new(region, Arc::new(self.client.clone())))
        }

        async fn region_for_key(&self, _key: &crate::Key) -> crate::Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn region_for_id(&self, _id: RegionId) -> crate::Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> crate::Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn get_min_ts(self: Arc<Self>) -> crate::Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> crate::Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> crate::Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                id: 0,
                name: keyspace.to_owned(),
                state: keyspacepb::KeyspaceState::Enabled as i32,
                ..Default::default()
            })
        }

        async fn all_stores(&self) -> crate::Result<Vec<Store>> {
            Ok(vec![Store::new(Arc::new(self.client.clone()))])
        }

        async fn update_leader(
            &self,
            _ver_id: RegionVerId,
            _leader: metapb::Peer,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    #[tokio::test]
    async fn test_replica_read_peer_selection_and_context_fields() -> Result<(), io::Error> {
        struct Case {
            name: &'static str,
            replica_read: ReplicaReadType,
            expected_store_id: u64,
            expected_replica_read: bool,
            expected_stale_read: bool,
        }

        let cases = [
            Case {
                name: "leader",
                replica_read: ReplicaReadType::Leader,
                expected_store_id: 1,
                expected_replica_read: false,
                expected_stale_read: false,
            },
            Case {
                name: "follower",
                replica_read: ReplicaReadType::Follower,
                expected_store_id: 2,
                expected_replica_read: true,
                expected_stale_read: false,
            },
            Case {
                name: "learner",
                replica_read: ReplicaReadType::Learner,
                expected_store_id: 3,
                expected_replica_read: true,
                expected_stale_read: false,
            },
            Case {
                name: "mixed",
                replica_read: ReplicaReadType::Mixed,
                expected_store_id: 3,
                expected_replica_read: true,
                expected_stale_read: false,
            },
            Case {
                name: "prefer-leader",
                replica_read: ReplicaReadType::PreferLeader,
                expected_store_id: 1,
                expected_replica_read: false,
                expected_stale_read: false,
            },
        ];

        for case in cases {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let seen_cloned = seen.clone();
            let pd_client = Arc::new(ReplicaReadPdClient::new(MockKvClient::with_dispatch_hook(
                move |req: &dyn Any| {
                    let req = req
                        .downcast_ref::<kvrpcpb::GetRequest>()
                        .expect("expected GetRequest");
                    let ctx = req.context.as_ref().expect("missing context on GetRequest");
                    let peer = ctx.peer.as_ref().expect("missing peer in context");
                    seen_cloned.lock().unwrap().push((
                        peer.store_id,
                        ctx.replica_read,
                        ctx.stale_read,
                    ));

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.not_found = true;
                    Ok(Box::new(resp) as Box<dyn Any>)
                },
            )));

            let mut txn = Transaction::new(
                Timestamp::default(),
                pd_client,
                TransactionOptions::new_optimistic()
                    .replica_read(case.replica_read)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat)
                    .drop_check(crate::CheckLevel::None),
                Keyspace::Disable,
                RequestContext::default(),
                None,
            );

            let got = txn.get("k".to_owned()).await.unwrap();
            assert!(got.is_none(), "case={}", case.name);

            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1, "case={}", case.name);
            assert_eq!(
                seen[0],
                (
                    case.expected_store_id,
                    case.expected_replica_read,
                    case.expected_stale_read,
                ),
                "case={}",
                case.name
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_stale_read_meet_lock_forces_leader_reread() -> Result<(), io::Error> {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_cloned = seen.clone();

        let pd_client = Arc::new(ReplicaReadPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let ctx = req.context.as_ref().expect("missing context on GetRequest");
                    let peer = ctx.peer.as_ref().expect("missing peer in context");
                    seen_cloned.lock().unwrap().push((
                        peer.store_id,
                        ctx.replica_read,
                        ctx.stale_read,
                    ));

                    let call = get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        let mut resp = kvrpcpb::GetResponse::default();
                        let mut key_error = kvrpcpb::KeyError::default();
                        key_error.locked = Some(kvrpcpb::LockInfo {
                            key: req.key.clone(),
                            primary_lock: req.key.clone(),
                            lock_version: 1,
                            lock_ttl: 0,
                            ..Default::default()
                        });
                        resp.error = Some(key_error);
                        Ok(Box::new(resp) as Box<dyn Any>)
                    } else {
                        let mut resp = kvrpcpb::GetResponse::default();
                        resp.not_found = true;
                        Ok(Box::new(resp) as Box<dyn Any>)
                    }
                } else if req.downcast_ref::<kvrpcpb::CleanupRequest>().is_some() {
                    Ok(Box::<kvrpcpb::CleanupResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in stale-read lock fallback test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .stale_read(true)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        let got = txn.get(vec![1]).await.unwrap();
        assert!(got.is_none());
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[0],
            (3, false, true),
            "first attempt uses stale_read on replica"
        );
        assert_eq!(
            seen[1],
            (1, false, false),
            "after lock resolution, falls back to leader read"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pipelined_flush_commit_and_resolve_locks() -> Result<(), io::Error> {
        let expected_source = "external_pdml";
        let expected_disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull;
        let expected_txn_source = 42_u64;
        let flush_generations = Arc::new(Mutex::new(Vec::<u64>::new()));
        let flush_generations_cloned = flush_generations.clone();
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_calls_cloned = resolve_calls.clone();

        let hook_disk_full_opt = expected_disk_full_opt;
        let hook_txn_source = expected_txn_source;
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::FlushRequest>() {
                    assert_eq!(req.start_ts, 10);
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on FlushRequest");
                    assert_eq!(ctx.request_source, expected_source);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    flush_generations_cloned
                        .lock()
                        .unwrap()
                        .push(req.generation);
                    Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.start_version, 10);
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on ResolveLockRequest");
                    assert_eq!(ctx.request_source, expected_source);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    resolve_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in pipelined txn test");
                }
            },
        )));

        let pipelined = crate::transaction::PipelinedTxnOptions::new()
            .flush_concurrency(1)
            .resolve_lock_concurrency(1)
            .min_flush_keys_for_test(1);

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .use_pipelined_txn(pipelined)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default()
                .with_disk_full_opt(expected_disk_full_opt)
                .with_txn_source(expected_txn_source),
            None,
        );

        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.put("k2".to_owned(), "v2").await.unwrap();
        txn.commit().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while resolve_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resolve lock task did not run");

        let gens = flush_generations.lock().unwrap().clone();
        assert_eq!(gens, vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_pipelined_rollback_resolves_locks() -> Result<(), io::Error> {
        let expected_source = "external_pdml";
        let expected_disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull;
        let expected_txn_source = 42_u64;
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let flush_calls_cloned = flush_calls.clone();
        let resolve_calls = Arc::new(AtomicUsize::new(0));
        let resolve_calls_cloned = resolve_calls.clone();

        let hook_disk_full_opt = expected_disk_full_opt;
        let hook_txn_source = expected_txn_source;
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::FlushRequest>() {
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on FlushRequest");
                    assert_eq!(ctx.request_source, expected_source);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    unreachable!("commit must not be sent in rollback test");
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.start_version, 10);
                    assert_eq!(req.commit_version, 0);
                    let ctx = req
                        .context
                        .as_ref()
                        .expect("missing context on ResolveLockRequest");
                    assert_eq!(ctx.request_source, expected_source);
                    assert_eq!(ctx.disk_full_opt, i32::from(hook_disk_full_opt));
                    assert_eq!(ctx.txn_source, hook_txn_source);
                    resolve_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in pipelined rollback test");
                }
            },
        )));

        let pipelined = crate::transaction::PipelinedTxnOptions::new()
            .flush_concurrency(1)
            .resolve_lock_concurrency(1)
            .min_flush_keys_for_test(1);

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .use_pipelined_txn(pipelined)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default()
                .with_disk_full_opt(expected_disk_full_opt)
                .with_txn_source(expected_txn_source),
            None,
        );

        txn.put("k1".to_owned(), "v1").await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while flush_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("flush did not run");

        txn.rollback().await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while resolve_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resolve lock task did not run");

        Ok(())
    }

    #[tokio::test]
    async fn test_prewrite_propagates_assertion_level_and_mutation_assertions() {
        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let prewrite_calls_cloned = prewrite_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    prewrite_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.assertion_level, kvrpcpb::AssertionLevel::Strict as i32);
                    assert_eq!(req.mutations.len(), 1);
                    assert_eq!(
                        req.mutations[0].assertion,
                        kvrpcpb::Assertion::NotExist as i32
                    );
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in assertion propagation test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .assertion_level(kvrpcpb::AssertionLevel::Strict)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.set_key_assertion("k1".to_owned(), kvrpcpb::Assertion::NotExist);

        txn.commit().await.unwrap();
        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_prewrite_put_becomes_insert_with_presume_key_not_exists() {
        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let prewrite_calls_cloned = prewrite_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    prewrite_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.mutations.len(), 1);
                    assert_eq!(req.mutations[0].op, kvrpcpb::Op::Insert as i32);
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in presumeKNE test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.update_key_flags("k1".to_owned(), [FlagsOp::SetPresumeKeyNotExists]);
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_delete_your_writes_presume_key_not_exists_is_prewrite_only_and_skips_commit() {
        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let prewrite_calls_cloned = prewrite_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    prewrite_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.mutations.len(), 1);
                    assert_eq!(req.mutations[0].op, kvrpcpb::Op::CheckNotExists as i32);
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    panic!("unexpected CommitRequest for prewrite-only transaction");
                } else {
                    unreachable!("unexpected request type in prewrite-only test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        txn.update_key_flags("k1".to_owned(), [FlagsOp::SetPresumeKeyNotExists]);
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.delete("k1".to_owned()).await.unwrap();

        assert!(txn.key_flags("k1".to_owned()).has_prewrite_only());

        let res = txn.commit().await.unwrap();
        assert!(res.is_none());

        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_commit_reselects_primary_key_when_first_mutation_is_prewrite_only() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    assert_eq!(req.primary_lock, b"k2".to_vec());
                    assert_eq!(req.mutations.len(), 2);
                    assert!(req.mutations.iter().any(|m| {
                        m.key == b"k1".to_vec() && m.op == kvrpcpb::Op::CheckNotExists as i32
                    }));
                    assert!(req
                        .mutations
                        .iter()
                        .any(|m| m.key == b"k2".to_vec() && m.op == kvrpcpb::Op::Put as i32));
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert_eq!(req.keys, vec![b"k2".to_vec()]);
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in primary reselect test");
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        txn.update_key_flags("k1".to_owned(), [FlagsOp::SetPresumeKeyNotExists]);
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.delete("k1".to_owned()).await.unwrap();
        txn.put("k2".to_owned(), "v2").await.unwrap();

        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_pipelined_flush_propagates_assertion_level_and_mutation_assertions() {
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let flush_calls_cloned = flush_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::FlushRequest>() {
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.assertion_level, kvrpcpb::AssertionLevel::Strict as i32);
                    assert_eq!(req.mutations.len(), 1);
                    assert_eq!(req.mutations[0].assertion, kvrpcpb::Assertion::Exist as i32);
                    Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some() {
                    Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
                } else {
                    unreachable!("unexpected request type in pipelined assertion test");
                }
            },
        )));

        let pipelined = crate::transaction::PipelinedTxnOptions::new()
            .flush_concurrency(1)
            .resolve_lock_concurrency(1)
            .min_flush_keys_for_test(1);

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .assertion_level(kvrpcpb::AssertionLevel::Strict)
                .use_pipelined_txn(pipelined)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        txn.set_key_assertion("k1".to_owned(), kvrpcpb::Assertion::Exist);
        txn.put("k1".to_owned(), "v1").await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while flush_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("flush did not run");
        txn.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_returns_assertion_failed_error_from_prewrite() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() else {
                    unreachable!("unexpected request type in assertion error test");
                };
                assert_eq!(req.assertion_level, kvrpcpb::AssertionLevel::Strict as i32);

                let mut key_error = kvrpcpb::KeyError::default();
                key_error.assertion_failed = Some(kvrpcpb::AssertionFailed {
                    start_ts: req.start_version,
                    key: req.mutations[0].key.clone(),
                    assertion: kvrpcpb::Assertion::Exist as i32,
                    existing_start_ts: 0,
                    existing_commit_ts: 0,
                });

                let mut resp = kvrpcpb::PrewriteResponse::default();
                resp.errors.push(key_error);
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .assertion_level(kvrpcpb::AssertionLevel::Strict)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.put("k1".to_owned(), "v1").await.unwrap();
        txn.set_key_assertion("k1".to_owned(), kvrpcpb::Assertion::Exist);

        let err = txn.commit().await.expect_err("commit must fail");
        assert!(matches!(err, crate::Error::AssertionFailed(_)), "{err:?}");
    }

    #[tokio::test]
    async fn test_commit_returns_key_exists_error_from_prewrite() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() else {
                    unreachable!("unexpected request type in key-exists error test");
                };
                assert_eq!(req.mutations.len(), 1);
                assert_eq!(req.mutations[0].op, kvrpcpb::Op::Insert as i32);

                let mut key_error = kvrpcpb::KeyError::default();
                key_error.already_exist = Some(kvrpcpb::AlreadyExist {
                    key: req.mutations[0].key.clone(),
                });

                let mut resp = kvrpcpb::PrewriteResponse::default();
                resp.errors.push(key_error);
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.insert("k1".to_owned(), "v1").await.unwrap();

        let err = txn.commit().await.expect_err("commit must fail");
        assert!(matches!(err, crate::Error::KeyExists(_)), "{err:?}");
    }

    struct CommitWaitPdClient {
        client: MockKvClient,
        region: RegionWithLeader,
        ts_versions: Vec<u64>,
        ts_calls: AtomicUsize,
    }

    impl CommitWaitPdClient {
        fn new(client: MockKvClient, ts_versions: Vec<u64>) -> Self {
            assert!(!ts_versions.is_empty());

            let leader = metapb::Peer {
                id: 100,
                store_id: 1,
                ..Default::default()
            };
            let mut region = metapb::Region::default();
            region.id = 1;
            region.region_epoch = Some(metapb::RegionEpoch {
                conf_ver: 0,
                version: 0,
            });
            region.peers = vec![leader.clone()];

            Self {
                client,
                region: RegionWithLeader {
                    region,
                    leader: Some(leader),
                },
                ts_versions,
                ts_calls: AtomicUsize::new(0),
            }
        }

        fn ts_calls(&self) -> usize {
            self.ts_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl PdClient for CommitWaitPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: RegionWithLeader,
        ) -> crate::Result<RegionStore> {
            Ok(RegionStore::new(region, Arc::new(self.client.clone())))
        }

        async fn region_for_key(&self, _key: &crate::Key) -> crate::Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn region_for_id(&self, _id: RegionId) -> crate::Result<RegionWithLeader> {
            Ok(self.region.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> crate::Result<Timestamp> {
            let idx = self.ts_calls.fetch_add(1, Ordering::SeqCst);
            let version = self
                .ts_versions
                .get(idx)
                .copied()
                .unwrap_or_else(|| *self.ts_versions.last().expect("non-empty"));
            Ok(Timestamp::from_version(version))
        }

        async fn get_min_ts(self: Arc<Self>) -> crate::Result<Timestamp> {
            Ok(Timestamp::default())
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> crate::Result<bool> {
            Ok(true)
        }

        async fn load_keyspace(&self, keyspace: &str) -> crate::Result<keyspacepb::KeyspaceMeta> {
            Ok(keyspacepb::KeyspaceMeta {
                id: 0,
                name: keyspace.to_owned(),
                state: keyspacepb::KeyspaceState::Enabled as i32,
                ..Default::default()
            })
        }

        async fn all_stores(&self) -> crate::Result<Vec<Store>> {
            Ok(vec![Store::new(Arc::new(self.client.clone()))])
        }

        async fn update_leader(
            &self,
            _ver_id: RegionVerId,
            _leader: metapb::Peer,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    #[tokio::test]
    async fn test_commit_wait_until_tso_retries_until_target_is_reached() -> crate::Result<()> {
        let start_ts = 10_u64;
        let wait_until_tso = start_ts + 200;

        let commit_reqs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let commit_reqs_cloned = commit_reqs.clone();
        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_reqs_cloned.lock().unwrap().push(req.commit_version);
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            } else {
                unreachable!("unexpected request type in commit-wait test");
            }
        });

        let pd_client = Arc::new(CommitWaitPdClient::new(
            kv_client,
            vec![start_ts + 100, start_ts + 201],
        ));
        let mut txn = Transaction::new(
            Timestamp::from_version(start_ts),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );

        assert_eq!(txn.commit_wait_until_tso(), 0);
        assert_eq!(txn.commit_wait_until_tso_timeout(), Duration::from_secs(1));
        txn.set_commit_wait_until_tso(wait_until_tso);
        assert_eq!(txn.commit_wait_until_tso(), wait_until_tso);

        txn.put("k1".to_owned(), "v1".to_owned()).await?;
        let commit_ts = txn
            .commit()
            .await?
            .expect("non-empty txn should return commit ts");
        assert_eq!(commit_ts.version(), start_ts + 201);
        assert_eq!(pd_client.ts_calls(), 2);
        assert_eq!(*commit_reqs.lock().unwrap(), vec![start_ts + 201]);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_wait_until_tso_times_out() -> crate::Result<()> {
        let start_ts = 10_u64;
        let wait_until_tso = start_ts + 100;

        let commit_reqs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let commit_reqs_cloned = commit_reqs.clone();
        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_reqs_cloned.lock().unwrap().push(req.commit_version);
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            } else {
                unreachable!("unexpected request type in commit-wait timeout test");
            }
        });

        let pd_client = Arc::new(CommitWaitPdClient::new(kv_client, vec![start_ts + 10]));
        let mut txn = Transaction::new(
            Timestamp::from_version(start_ts),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
            RequestContext::default(),
            None,
        );
        txn.set_commit_wait_until_tso(wait_until_tso);
        txn.set_commit_wait_until_tso_timeout(Duration::from_millis(20));
        txn.put("k1".to_owned(), "v1".to_owned()).await?;

        let err = txn.commit().await.expect_err("commit must time out");
        assert!(
            matches!(err, Error::CommitTsLag { .. }),
            "unexpected error: {err:?}"
        );
        assert!(commit_reqs.lock().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn test_pipelined_options_validation() {
        let opts = TransactionOptions::new_pessimistic()
            .use_pipelined_txn(crate::transaction::PipelinedTxnOptions::new());
        let err = opts
            .validate()
            .expect_err("pipelined pessimistic txn must fail validation");
        assert!(matches!(
            err,
            crate::Error::InvalidPipelinedTransaction { .. }
        ));
    }
}
