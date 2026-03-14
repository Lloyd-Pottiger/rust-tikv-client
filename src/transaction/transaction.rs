// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{HashMap, HashSet};
use std::iter;
use std::sync::atomic::{self, AtomicU64, AtomicU8};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use derive_new::new;
use fail::fail_point;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use serde_derive::Serialize;
use tokio::task::JoinHandle;
use tokio::time::Duration;

use super::latches::TxnLocalLatches;
use super::lock::ResolveLockDetailCollector;
use super::requests::CollectPessimisticLock;
use super::requests::ResolveLockRangeRequest;
use super::LockResolverRpcContext;
use super::ReadLockTracker;
use super::ResolveLockDetail;
use super::ResolveLocksContext;
use super::Variables;
use crate::backoff::Backoff;
use crate::gc_safe_point::GcSafePointCache;
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
use crate::request::Process;
use crate::request::ProcessResponse;
use crate::request::RetryOptions;
use crate::request::TruncateKeyspace;
use crate::rpc_interceptor::RpcInterceptors;
use crate::store::Request;
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
use crate::StoreLabel;
use crate::Value;

/// A callback used to fill `kvrpcpb::Context.resource_group_tag` when no explicit tag is configured.
///
/// The input is the request label (for example, `"kv_get"` or `"kv_commit"`).
type ResourceGroupTagger = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;
type CommitTsUpperBoundCheck = Arc<dyn Fn(u64) -> bool + Send + Sync>;
type CommitCallback = Arc<dyn Fn(&str, Option<&Error>) + Send + Sync>;

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

/// How strict to enforce mutation assertions during prewrite/flush.
///
/// This maps to client-go `KVTxn.SetAssertionLevel`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum AssertionLevel {
    /// No assertion.
    #[default]
    Off = 0,
    /// Assertion is enabled, but not enforced when it might affect performance.
    Fast = 1,
    /// Assertion is enabled and enforced.
    Strict = 2,
}

impl From<AssertionLevel> for kvrpcpb::AssertionLevel {
    fn from(level: AssertionLevel) -> Self {
        match level {
            AssertionLevel::Off => kvrpcpb::AssertionLevel::Off,
            AssertionLevel::Fast => kvrpcpb::AssertionLevel::Fast,
            AssertionLevel::Strict => kvrpcpb::AssertionLevel::Strict,
        }
    }
}

/// Specifies the policy when prewrite encounters locks.
///
/// This maps to client-go `KVTxn.SetPrewriteEncounterLockPolicy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PrewriteEncounterLockPolicy {
    /// Try to resolve locks and retry prewrite (default).
    #[default]
    TryResolve,
    /// Do not resolve locks; return lock errors directly.
    NoResolve,
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
#[derive(Debug)]
struct AggressiveLockingState {
    primary_key_at_start: Option<Key>,
    last_primary_key: Option<Key>,
    primary_key: Option<Key>,
    last_attempt_start: Instant,
    attempt_start: Instant,
    max_locked_with_conflict_ts: u64,
    last_retry_unnecessary_locks: HashMap<Key, u64>,
    current_locked_keys: HashMap<Key, u64>,
}

#[derive(Debug, Default)]
struct PessimisticLockRequestResult {
    pairs: Vec<KvPair>,
    locked_keys_actual_for_update_ts: Vec<(Key, u64)>,
    max_locked_with_conflict_ts: u64,
}

pub struct Transaction<PdC: PdClient = PdRpcClient> {
    status: Arc<AtomicU8>,
    timestamp: Timestamp,
    commit_ts: Option<Timestamp>,
    buffer: Buffer,
    aggressive_locking: Option<AggressiveLockingState>,
    for_update_ts_checks: HashMap<Key, u64>,
    read_lock_tracker: ReadLockTracker,
    pipelined: Option<PipelinedState>,
    rpc: Arc<PdC>,
    resolve_locks_ctx: ResolveLocksContext,
    gc_safe_point: GcSafePointCache<PdC>,
    resolve_lock_detail: Arc<ResolveLockDetailCollector>,
    options: TransactionOptions,
    rpc_interceptors: RpcInterceptors,
    vars: Variables,
    resource_group_tagger: Option<ResourceGroupTagger>,
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    schema_ver: Option<i64>,
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
    commit_callback: Option<CommitCallback>,
    commit_wait_until_tso: u64,
    commit_wait_until_tso_timeout: Duration,
    commit_ts_upper_bound_check: Option<CommitTsUpperBoundCheck>,
    keyspace: Keyspace,
    txn_latches: Option<Arc<TxnLocalLatches>>,
    is_heartbeat_started: bool,
    heartbeat_generation: Arc<AtomicU64>,
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

#[derive(Debug, Serialize)]
struct TxnInfo {
    txn_scope: String,
    start_ts: u64,
    commit_ts: u64,
    txn_commit_mode: &'static str,
    async_commit_fallback: bool,
    one_pc_fallback: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    pipelined: bool,
    flush_wait_ms: i64,
}

#[derive(Clone, Copy)]
struct GetWithCommitTsProcessor;

impl Process<kvrpcpb::GetResponse> for GetWithCommitTsProcessor {
    type Out = Option<(Value, u64)>;

    fn process(&self, input: Result<kvrpcpb::GetResponse>) -> Result<Self::Out> {
        let input = input?;
        if input.not_found {
            Ok(None)
        } else if input.commit_ts == 0 {
            Err(Error::CommitTsRequiredButNotReturned)
        } else {
            Ok(Some((input.value, input.commit_ts)))
        }
    }
}

#[derive(Clone, Copy)]
struct CollectBatchGetWithCommitTs;

impl Merge<kvrpcpb::BatchGetResponse> for CollectBatchGetWithCommitTs {
    type Out = Vec<(KvPair, u64)>;

    fn merge(&self, input: Vec<Result<kvrpcpb::BatchGetResponse>>) -> Result<Self::Out> {
        let mut out = Vec::new();
        for resp in input {
            let resp = resp?;
            for pair in resp.pairs {
                let commit_ts = pair.commit_ts;
                if commit_ts == 0 {
                    return Err(Error::CommitTsRequiredButNotReturned);
                }
                out.push((KvPair::from(pair), commit_ts));
            }
        }
        Ok(out)
    }
}

fn normalize_busy_threshold_ms(threshold: Duration) -> u32 {
    let millis = threshold.as_millis();
    if millis == 0 || millis > u128::from(u32::MAX) {
        0
    } else {
        millis as u32
    }
}

async fn get_timestamp_for_txn_scope<PdC: PdClient>(
    rpc: Arc<PdC>,
    txn_scope: Option<&str>,
) -> Result<Timestamp> {
    match txn_scope {
        Some(dc_location) => {
            rpc.get_timestamp_with_dc_location(dc_location.to_owned())
                .await
        }
        None => rpc.get_timestamp().await,
    }
}

fn is_write_conflict_error(error: &Error) -> bool {
    match error {
        Error::WriteConflict(_) => true,
        Error::WriteConflictInLatch { .. } => true,
        Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
            errors.iter().any(is_write_conflict_error)
        }
        Error::PessimisticLockError { inner, .. } => is_write_conflict_error(inner),
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PessimisticLockMode {
    Exclusive,
    Shared,
}

impl<PdC: PdClient> Transaction<PdC> {
    #[cfg(test)]
    pub(crate) fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
    ) -> Transaction<PdC> {
        let gc_safe_point = GcSafePointCache::new(rpc.clone(), keyspace);
        Self::new_with_resolve_locks_ctx(
            timestamp,
            rpc,
            options,
            keyspace,
            ResolveLocksContext::default(),
            gc_safe_point,
            None,
        )
    }

    pub(crate) fn new_with_resolve_locks_ctx(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
        resolve_locks_ctx: ResolveLocksContext,
        gc_safe_point: GcSafePointCache<PdC>,
        txn_latches: Option<Arc<TxnLocalLatches>>,
    ) -> Transaction<PdC> {
        let status = if options.read_only {
            TransactionStatus::ReadOnly
        } else {
            TransactionStatus::Active
        };
        let read_lock_tracker = if options.pipelined_txn.is_some() {
            // Match client-go: pipelined reads skip locks with `lock.ts == start_ts` via resolved-locks.
            // This prevents the transaction from trying to resolve its own flushed locks.
            ReadLockTracker::new_with_resolved_locks([timestamp.version()])
        } else {
            ReadLockTracker::default()
        };
        Transaction {
            status: Arc::new(AtomicU8::new(status as u8)),
            timestamp,
            commit_ts: None,
            buffer: Buffer::new(options.is_pessimistic()),
            aggressive_locking: None,
            for_update_ts_checks: HashMap::new(),
            read_lock_tracker,
            pipelined: options
                .pipelined_txn
                .as_ref()
                .map(|_| PipelinedState::new()),
            rpc,
            resolve_locks_ctx,
            gc_safe_point,
            resolve_lock_detail: Arc::new(ResolveLockDetailCollector::default()),
            options,
            rpc_interceptors: Default::default(),
            vars: Variables::default(),
            resource_group_tagger: None,
            replica_read_adjuster: None,
            schema_ver: None,
            schema_lease_checker: None,
            commit_callback: None,
            commit_wait_until_tso: 0,
            commit_wait_until_tso_timeout: DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT,
            commit_ts_upper_bound_check: None,
            keyspace,
            txn_latches,
            is_heartbeat_started: false,
            heartbeat_generation: Arc::new(AtomicU64::new(0)),
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

    fn lock_resolver_rpc_context(&self) -> LockResolverRpcContext {
        let mut ctx = self
            .options
            .lock_resolver_rpc_context(self.resource_group_tagger.clone());
        ctx.rpc_interceptors = self.rpc_interceptors.clone();
        ctx.resolve_lock_detail = Some(self.resolve_lock_detail.clone());
        ctx
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

    /// Enable or disable stale reads for read-only snapshots.
    ///
    /// When enabled, read requests will set `kvrpcpb::Context.stale_read = true`.
    /// If replica read routing is still set to `ReplicaReadType::Leader`, this also switches it to
    /// `ReplicaReadType::Mixed`, matching client-go's `EnableStaleWithMixedReplicaRead` behavior.
    ///
    /// This maps to client-go `KVSnapshot.SetIsStalenessReadOnly`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.options.stale_read = stale_read;
        if stale_read && self.options.replica_read == ReplicaReadType::Leader {
            self.options.replica_read = ReplicaReadType::Mixed;
        }
    }

    /// Set labels to filter target stores for replica reads.
    ///
    /// This maps to client-go `KVSnapshot.SetMatchStoreLabels`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_labels(&mut self, labels: impl IntoIterator<Item = StoreLabel>) {
        self.options.match_store_labels = Arc::new(labels.into_iter().collect());
    }

    /// Set store ids to filter target stores for replica reads.
    ///
    /// This maps to client-go `tikv.WithMatchStores` / `locate.WithMatchStores`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_ids(&mut self, store_ids: impl IntoIterator<Item = u64>) {
        self.options.match_store_ids = Arc::new(store_ids.into_iter().collect());
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

    /// Set whether read requests should fill TiKV block cache.
    ///
    /// This maps to client-go `KVSnapshot.SetNotFillCache`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.options.not_fill_cache = not_fill_cache;
    }

    /// Set task ID hint for TiKV.
    ///
    /// This maps to client-go `KVSnapshot.SetTaskID`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.options.task_id = task_id;
    }

    /// Set server-side maximum execution duration for read requests.
    ///
    /// This option writes to `kvrpcpb::Context.max_execution_duration_ms`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_max_execution_duration(&mut self, duration: Duration) {
        self.options.max_execution_duration_ms =
            duration.as_millis().min(u128::from(u64::MAX)) as u64;
    }

    /// Set the priority for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetPriority`.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.options.priority = priority;
    }

    /// Set the isolation level for read requests.
    ///
    /// This maps to client-go `KVSnapshot.SetIsolationLevel`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.options.isolation_level = isolation_level;
    }

    /// Set resource group tag for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTag`.
    pub fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.options.resource_group_tag = Some(tag);
    }

    /// Set a resource group tagger used to fill `kvrpcpb::Context.resource_group_tag`.
    ///
    /// The tagger is invoked only when no explicit resource group tag is configured via
    /// [`Transaction::set_resource_group_tag`] / [`TransactionOptions::resource_group_tag`], matching
    /// client-go behavior.
    ///
    /// The tagger input is the request label (for example, `"kv_get"` or `"kv_commit"`).
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTagger` / `KVTxn.SetResourceGroupTagger`.
    pub fn set_resource_group_tagger<F>(&mut self, tagger: F)
    where
        F: Fn(&str) -> Vec<u8> + Send + Sync + 'static,
    {
        self.resource_group_tagger = Some(Arc::new(tagger));
    }

    /// Clear the configured resource group tagger.
    pub fn clear_resource_group_tagger(&mut self) {
        self.resource_group_tagger = None;
    }

    /// Set resource group name for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupName`.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.options.resource_group_name = Some(name.into());
    }

    /// Set request source for requests.
    ///
    /// This option writes to `kvrpcpb::Context.request_source`.
    ///
    /// For client-go compatible formatting (internal/external prefixes and optional explicit type),
    /// use [`RequestSource`](crate::RequestSource).
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.options.request_source = Some(source.into());
    }

    /// Set whether current operation is allowed in each TiKV disk usage level.
    ///
    /// This maps to client-go `KVTxn.SetDiskFullOpt` and writes to `kvrpcpb::Context.disk_full_opt`.
    pub fn set_disk_full_opt(&mut self, opt: DiskFullOpt) {
        self.options.disk_full_opt = opt;
    }

    /// Set the source of the transaction.
    ///
    /// This maps to client-go `KVTxn.SetTxnSource` and writes to `kvrpcpb::Context.txn_source`.
    pub fn set_txn_source(&mut self, txn_source: u64) {
        self.options.txn_source = txn_source;
    }

    /// Enable forcing TiKV to always sync logs for transactional write requests.
    ///
    /// This maps to client-go `KVTxn.EnableForceSyncLog` and writes to `kvrpcpb::Context.sync_log`.
    pub fn enable_force_sync_log(&mut self) {
        self.options.sync_log = true;
    }

    /// Set whether TiKV should sync logs for transactional write requests.
    ///
    /// This option writes to `kvrpcpb::Context.sync_log`.
    pub fn set_sync_log(&mut self, enabled: bool) {
        self.options.sync_log = enabled;
    }

    /// Set the server-side maximum execution duration for transactional write requests.
    ///
    /// This option writes to `kvrpcpb::Context.max_execution_duration_ms`.
    pub fn set_max_write_execution_duration(&mut self, duration: Duration) {
        self.options.max_write_execution_duration_ms =
            duration.as_millis().min(u128::from(u64::MAX)) as u64;
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

    /// Set the KV variables used by this transaction.
    ///
    /// This maps to client-go `KVTxn.SetVars`.
    pub fn set_vars(&mut self, vars: Variables) {
        self.vars = vars;
    }

    /// Get the KV variables used by this transaction.
    ///
    /// This maps to client-go `KVTxn.GetVars`.
    #[must_use]
    pub fn vars(&self) -> &Variables {
        &self.vars
    }

    /// Get lock-resolution runtime stats accumulated by this transaction.
    ///
    /// This mirrors client-go `KVTxn.GetResolveLockDetail` and `KVSnapshot.GetResolveLockDetail`.
    #[must_use]
    pub fn resolve_lock_detail(&self) -> ResolveLockDetail {
        self.resolve_lock_detail.snapshot()
    }

    /// Set the geographical scope of the transaction.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator.
    /// Otherwise `txn_scope` is passed through as PD `dc_location` to request a local TSO.
    ///
    /// This maps to client-go `KVTxn.SetScope`.
    pub fn set_txn_scope(&mut self, txn_scope: impl AsRef<str>) {
        let txn_scope = txn_scope.as_ref();
        self.options.txn_scope = if txn_scope.is_empty() || txn_scope == "global" {
            None
        } else {
            Some(txn_scope.to_owned())
        };
    }

    /// Returns the geographical scope of the transaction.
    ///
    /// This maps to client-go `KVTxn.GetScope`.
    #[must_use]
    pub fn txn_scope(&self) -> &str {
        self.options.txn_scope.as_deref().unwrap_or("global")
    }

    /// Enable or disable async commit.
    ///
    /// This maps to client-go `KVTxn.SetEnableAsyncCommit`.
    pub fn set_enable_async_commit(&mut self, enabled: bool) {
        self.options.async_commit = enabled;
    }

    /// Enable or disable 1PC.
    ///
    /// This maps to client-go `KVTxn.SetEnable1PC`.
    pub fn set_enable_one_pc(&mut self, enabled: bool) {
        self.options.try_one_pc = enabled;
    }

    /// Set whether the transaction uses causal consistency instead of linearizability.
    ///
    /// When enabled, async-commit/1PC does not fetch a fresh PD TSO to seed `min_commit_ts`.
    ///
    /// This maps to client-go `KVTxn.SetCausalConsistency`.
    pub fn set_causal_consistency(&mut self, enabled: bool) {
        self.options.causal_consistency = enabled;
    }

    /// Set how strict to enforce mutation assertions during prewrite/flush.
    ///
    /// This maps to client-go `KVTxn.SetAssertionLevel`.
    pub fn set_assertion_level(&mut self, assertion_level: AssertionLevel) {
        self.options.assertion_level = assertion_level;
    }

    /// Set the policy for handling locks encountered during prewrite.
    ///
    /// When set to [`PrewriteEncounterLockPolicy::NoResolve`], prewrite returns lock errors directly
    /// without attempting lock resolution.
    ///
    /// This maps to client-go `KVTxn.SetPrewriteEncounterLockPolicy`.
    pub fn set_prewrite_encounter_lock_policy(&mut self, policy: PrewriteEncounterLockPolicy) {
        self.options.prewrite_encounter_lock_policy = policy;
    }

    /// Set the minimum commit timestamp constraint for the transaction.
    ///
    /// When set, the commit timestamp returned by PD must be strictly greater than
    /// `commit_wait_until_tso`.
    ///
    /// This maps to client-go `KVTxn.SetCommitWaitUntilTSO`.
    pub fn set_commit_wait_until_tso(&mut self, commit_wait_until_tso: u64) {
        self.commit_wait_until_tso = self.commit_wait_until_tso.max(commit_wait_until_tso);
    }

    /// Returns the commit-wait constraint configured by [`Transaction::set_commit_wait_until_tso`].
    ///
    /// A value of `0` means "no commit-wait constraint".
    #[must_use]
    pub fn commit_wait_until_tso(&self) -> u64 {
        self.commit_wait_until_tso
    }

    /// Set the maximum time allowed for PD TSO to catch up to the commit-wait target timestamp.
    ///
    /// This maps to client-go `KVTxn.SetCommitWaitUntilTSOTimeout`.
    pub fn set_commit_wait_until_tso_timeout(&mut self, timeout: Duration) {
        self.commit_wait_until_tso_timeout = timeout;
    }

    /// Returns the commit-wait timeout configured by
    /// [`Transaction::set_commit_wait_until_tso_timeout`].
    #[must_use]
    pub fn commit_wait_until_tso_timeout(&self) -> Duration {
        self.commit_wait_until_tso_timeout
    }

    /// Get a timestamp version that can be used as the commit timestamp for this transaction.
    ///
    /// Unlike a direct PD TSO request, this also respects the commit-wait constraint configured by
    /// [`Transaction::set_commit_wait_until_tso`].
    ///
    /// This maps to client-go `KVTxn.GetTimestampForCommit`.
    pub async fn get_timestamp_for_commit(&mut self) -> Result<u64> {
        let first_attempt =
            get_timestamp_for_txn_scope(self.rpc.clone(), self.options.txn_scope.as_deref())
                .await?;
        let first_attempt_version = first_attempt.version();

        if self.commit_wait_until_tso == 0 || first_attempt_version > self.commit_wait_until_tso {
            self.check_commit_ts_upper_bound(first_attempt_version)?;
            return Ok(first_attempt_version);
        }

        let max_sleep = self.commit_wait_until_tso_timeout;
        if max_sleep.is_zero() {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', retry timeout: {:?}, attempts: 1, last attempted commit TS: {}",
                first_attempt_version,
                self.commit_wait_until_tso,
                max_sleep,
                first_attempt_version
            )));
        }

        // Match client-go: if PD lags too far behind (clock drift exceeds the allowed timeout),
        // fail fast rather than waiting.
        let first_physical = Timestamp::from_version(first_attempt_version).physical;
        let expected_physical = Timestamp::from_version(self.commit_wait_until_tso).physical;
        let interval_ms =
            u64::try_from(expected_physical.saturating_sub(first_physical)).unwrap_or(0);
        let interval = Duration::from_millis(interval_ms);
        if interval > max_sleep {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', clock drift {:?} exceeds maximum allowed timeout {:?}",
                first_attempt_version,
                self.commit_wait_until_tso,
                interval,
                max_sleep
            )));
        }

        let deadline = Instant::now() + max_sleep;
        let mut backoff = Backoff::no_jitter_backoff(2, 500, 32);
        let mut attempts = 1_usize;
        let mut last_attempt = first_attempt;

        while last_attempt.version() <= self.commit_wait_until_tso {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline.duration_since(now);
            let mut delay = backoff.next_delay_duration().unwrap_or(remaining);
            if delay > remaining {
                delay = remaining;
            }
            if delay.is_zero() {
                break;
            }
            tokio::time::sleep(delay).await;

            attempts += 1;
            last_attempt =
                get_timestamp_for_txn_scope(self.rpc.clone(), self.options.txn_scope.as_deref())
                    .await?;
        }

        if last_attempt.version() <= self.commit_wait_until_tso {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', retry timeout: {:?}, attempts: {}, last attempted commit TS: {}",
                first_attempt_version,
                self.commit_wait_until_tso,
                max_sleep,
                attempts,
                last_attempt.version()
            )));
        }

        self.check_commit_ts_upper_bound(last_attempt.version())?;
        Ok(last_attempt.version())
    }

    fn check_commit_ts_upper_bound(&self, commit_ts: u64) -> Result<()> {
        let Some(checker) = self.commit_ts_upper_bound_check.as_ref() else {
            return Ok(());
        };

        if (checker)(commit_ts) {
            Ok(())
        } else {
            Err(Error::StringError(format!(
                "check commit ts upper bound fail, start_ts: {}, comm: {}",
                self.timestamp.version(),
                commit_ts
            )))
        }
    }

    /// Set a commit-ts upper bound checker for this transaction.
    ///
    /// When set, the checker is invoked with the chosen commit timestamp (TSO version). If it
    /// returns false, the commit is aborted with an error.
    ///
    /// This maps to client-go `KVTxn.SetCommitTSUpperBoundCheck`.
    ///
    /// Note: client-go disables async-commit and 1PC when this checker is set. This client matches
    /// that behavior during commit.
    pub fn set_commit_ts_upper_bound_check<F>(&mut self, checker: F)
    where
        F: Fn(u64) -> bool + Send + Sync + 'static,
    {
        self.commit_ts_upper_bound_check = Some(Arc::new(checker));
    }

    /// Clear the configured commit-ts upper bound checker.
    pub fn clear_commit_ts_upper_bound_check(&mut self) {
        self.commit_ts_upper_bound_check = None;
    }

    /// Set a callback invoked when [`Transaction::commit`] finishes.
    ///
    /// The callback receives:
    /// - `info`: a JSON string containing the commit info (client-go `TxnInfo` compatible); and
    /// - `err`: the commit error (if any).
    ///
    /// This maps to client-go `KVTxn.SetCommitCallback`.
    pub fn set_commit_callback<F>(&mut self, callback: F)
    where
        F: Fn(&str, Option<&Error>) + Send + Sync + 'static,
    {
        self.commit_callback = Some(Arc::new(callback));
    }

    /// Clear the commit callback.
    pub fn clear_commit_callback(&mut self) {
        self.commit_callback = None;
    }

    /// Set an RPC interceptor for this transaction.
    ///
    /// The interceptor is applied to all TiKV RPC requests initiated by this transaction,
    /// including lock resolution and read RPCs.
    ///
    /// This maps to client-go `KVTxn.SetRPCInterceptor`.
    pub fn set_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.rpc_interceptors = Arc::new(vec![Arc::new(interceptor)]);
    }

    /// Add an RPC interceptor.
    ///
    /// Interceptors are executed in an "onion model": interceptors added earlier execute earlier,
    /// but return later. If multiple interceptors with the same name are added, only the last one
    /// is kept.
    ///
    /// This maps to client-go `KVTxn.AddRPCInterceptor`.
    pub fn add_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        let interceptor: Arc<dyn crate::RpcInterceptor> = Arc::new(interceptor);
        let mut interceptors = self.rpc_interceptors.as_ref().clone();
        interceptors.retain(|existing| existing.name() != interceptor.name());
        interceptors.push(interceptor);
        self.rpc_interceptors = Arc::new(interceptors);
    }

    /// Clear all configured RPC interceptors.
    pub fn clear_rpc_interceptors(&mut self) {
        self.rpc_interceptors = Default::default();
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
        self.check_visibility().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
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
        let match_store_ids = self.options.match_store_ids.clone();
        let match_store_labels = self.options.match_store_labels.clone();
        let resource_group_tag_set = self.options.resource_group_tag.is_some();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let pipelined_has_flushed = self
            .pipelined
            .as_ref()
            .map(|state| state.generation > 0)
            .unwrap_or(false);
        let pipelined_flushed_deletes = self
            .pipelined
            .as_ref()
            .map(|state| state.flushed_deletes.clone());
        let pipelined_flushed_range = self.pipelined.as_ref().and_then(|state| {
            match (&state.flushed_range_start, &state.flushed_range_end) {
                (Some(start), Some(end)) => Some((start.clone(), end.clone())),
                _ => None,
            }
        });
        let pipelined_flushing_puts = self
            .pipelined
            .as_ref()
            .and_then(|state| state.flushing_puts.clone());
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

        self.buffer
            .get_or_else(key, |key| async move {
                if pipelined_has_flushed {
                    if pipelined_flushed_deletes.as_ref().is_some_and(|deleted| {
                        let deleted = match deleted.lock() {
                            Ok(guard) => guard,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        deleted.contains(&key)
                    }) {
                        return Ok(None);
                    }
                    if let Some(puts) = pipelined_flushing_puts.as_ref() {
                        if let Some(value) = puts.get(&key) {
                            return Ok(Some(value.clone()));
                        }
                    }

                    if pipelined_flushed_range
                        .as_ref()
                        .is_some_and(|(start, end)| &key >= start && &key < end)
                    {
                        let mut request = new_buffer_batch_get_request(
                            iter::once(key.clone()),
                            timestamp.clone(),
                        );
                        Self::apply_snapshot_read_context(
                            &mut request.context,
                            snapshot_ctx.clone(),
                        );
                        if !resource_group_tag_set {
                            if let Some(tagger) = resource_group_tagger.as_ref() {
                                let tag = (tagger)(request.label());
                                let ctx = request
                                    .context
                                    .get_or_insert_with(kvrpcpb::Context::default);
                                ctx.resource_group_tag = tag;
                            }
                        }

                        let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                            rpc.clone(),
                            keyspace,
                            request,
                            lock_resolver_rpc_context.rpc_interceptors.clone(),
                        )
                        .resolve_lock_for_read(
                            resolve_locks_ctx.clone(),
                            timestamp.clone(),
                            lock_backoff.clone(),
                            killed.clone(),
                            keyspace,
                            true,
                            lock_tracker.clone(),
                            lock_resolver_rpc_context.clone(),
                        );
                        let plan_builder =
                            if replica_read.is_follower_read() || enable_load_based_replica_read {
                                plan_builder
                                    .retry_multi_region_with_replica_read_and_match_stores(
                                        region_backoff.clone(),
                                        replica_read,
                                        match_store_ids.clone(),
                                        match_store_labels.clone(),
                                    )
                                    .with_killed(killed.clone())
                            } else {
                                plan_builder
                                    .retry_multi_region(region_backoff.clone())
                                    .with_killed(killed.clone())
                            };
                        let plan = plan_builder.merge(Collect).plan();
                        let mut pairs = plan.execute().await?;
                        if let Some(pair) = pairs.pop() {
                            return Ok(Some(pair.1));
                        }
                    }
                }

                let mut request = new_get_request(key, timestamp.clone());
                Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);
                if !resource_group_tag_set {
                    if let Some(tagger) = resource_group_tagger.as_ref() {
                        let tag = (tagger)(request.label());
                        let ctx = request
                            .context
                            .get_or_insert_with(kvrpcpb::Context::default);
                        ctx.resource_group_tag = tag;
                    }
                }

                let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                    rpc,
                    keyspace,
                    request,
                    lock_resolver_rpc_context.rpc_interceptors.clone(),
                )
                .resolve_lock_for_read(
                    resolve_locks_ctx,
                    timestamp,
                    lock_backoff.clone(),
                    killed.clone(),
                    keyspace,
                    true,
                    lock_tracker,
                    lock_resolver_rpc_context,
                );
                let plan_builder =
                    if replica_read.is_follower_read() || enable_load_based_replica_read {
                        plan_builder
                            .retry_multi_region_with_replica_read_and_match_stores(
                                region_backoff,
                                replica_read,
                                match_store_ids,
                                match_store_labels,
                            )
                            .with_killed(killed)
                    } else {
                        plan_builder
                            .retry_multi_region(region_backoff)
                            .with_killed(killed)
                    };
                let plan = plan_builder
                    .merge(CollectSingle)
                    .post_process_default()
                    .plan();
                plan.execute().await
            })
            .await
    }

    /// Get the value associated with the given key and its commit timestamp.
    ///
    /// This is only supported for **read-only snapshots** (`TransactionOptions::read_only()`) and
    /// sets `kvrpcpb::GetRequest.need_commit_ts = true`.
    ///
    /// Returns [`Error::CommitTsRequiredButNotReturned`] if TiKV does not return a commit
    /// timestamp for an existing key.
    pub async fn get_with_commit_ts(
        &mut self,
        key: impl Into<Key>,
    ) -> Result<Option<(Value, u64)>> {
        trace!("invoking transactional get_with_commit_ts request");
        self.check_allow_operation().await?;
        self.check_visibility().await?;
        if !self.options.read_only {
            return Err(Error::StringError(
                "get_with_commit_ts is only supported for read-only snapshots".to_owned(),
            ));
        }

        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
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
        let match_store_ids = self.options.match_store_ids.clone();
        let match_store_labels = self.options.match_store_labels.clone();
        let resource_group_tag_set = self.options.resource_group_tag.is_some();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

        let mut request = new_get_request(key, timestamp.clone());
        request.need_commit_ts = true;
        Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);
        if !resource_group_tag_set {
            if let Some(tagger) = resource_group_tagger.as_ref() {
                let tag = (tagger)(request.label());
                let ctx = request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }

        let plan_builder = PlanBuilder::new_with_rpc_interceptors(
            rpc,
            keyspace,
            request,
            lock_resolver_rpc_context.rpc_interceptors.clone(),
        )
        .resolve_lock_for_read(
            resolve_locks_ctx,
            timestamp,
            lock_backoff,
            killed.clone(),
            keyspace,
            true,
            lock_tracker,
            lock_resolver_rpc_context,
        );
        let plan_builder = if replica_read.is_follower_read() || enable_load_based_replica_read {
            plan_builder
                .retry_multi_region_with_replica_read_and_match_stores(
                    region_backoff,
                    replica_read,
                    match_store_ids,
                    match_store_labels,
                )
                .with_killed(killed)
        } else {
            plan_builder
                .retry_multi_region(region_backoff)
                .with_killed(killed)
        };

        let plan = plan_builder.merge(CollectSingle).plan();
        let plan = ProcessResponse {
            inner: plan,
            processor: GetWithCommitTsProcessor,
        };
        plan.execute().await
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

    /// Create a `get for share` request.
    ///
    /// This is similar to [`Transaction::get_for_update`], but acquires a shared pessimistic lock
    /// (`SharedPessimisticLock`) for pessimistic transactions, similar to `SELECT ... LOCK IN SHARE
    /// MODE` in TiDB.
    pub async fn get_for_share(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking transactional get_for_share request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let key = key.into();
            self.lock_keys_in_share_mode(iter::once(key.clone()))
                .await?;
            self.get(key).await
        } else {
            let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
            let mut pairs = self
                .pessimistic_lock_in_share_mode_with_wait_timeout(
                    iter::once(key),
                    true,
                    self.options.lock_wait_timeout,
                )
                .await?;
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
        self.check_visibility().await?;
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keys = keys
            .into_iter()
            .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = RetryOptions::new(self.region_backoff(), self.lock_backoff());
        let killed = self.vars.killed.clone();
        let snapshot_ctx = self.snapshot_read_context();
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();
        let match_store_ids = self.options.match_store_ids.clone();
        let match_store_labels = self.options.match_store_labels.clone();
        let resource_group_tag_set = self.options.resource_group_tag.is_some();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let pipelined_has_flushed = self
            .pipelined
            .as_ref()
            .map(|state| state.generation > 0)
            .unwrap_or(false);
        let pipelined_flushed_deletes = self
            .pipelined
            .as_ref()
            .map(|state| state.flushed_deletes.clone());
        let pipelined_flushed_range = self.pipelined.as_ref().and_then(|state| {
            match (&state.flushed_range_start, &state.flushed_range_end) {
                (Some(start), Some(end)) => Some((start.clone(), end.clone())),
                _ => None,
            }
        });
        let pipelined_flushing_puts = self
            .pipelined
            .as_ref()
            .and_then(|state| state.flushing_puts.clone());
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

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
                let match_store_ids = match_store_ids.clone();
                let match_store_labels = match_store_labels.clone();

                async move {
                    let mut keys = keys;
                    let mut buffer_pairs = Vec::new();
                    if let Some(puts) = pipelined_flushing_puts.as_ref() {
                        let mut remaining = Vec::new();
                        for key in keys {
                            if let Some(value) = puts.get(&key) {
                                buffer_pairs.push(KvPair(key, value.clone()));
                            } else {
                                remaining.push(key);
                            }
                        }
                        keys = remaining;
                    }

                    if pipelined_has_flushed {
                        if let Some(deleted) = pipelined_flushed_deletes.as_ref() {
                            let deleted = match deleted.lock() {
                                Ok(guard) => guard,
                                Err(poisoned) => poisoned.into_inner(),
                            };
                            keys.retain(|key| !deleted.contains(key));
                        }
                        if keys.is_empty() {
                            return Ok(buffer_pairs);
                        }

                        let buffer_lookup_keys = pipelined_flushed_range
                            .as_ref()
                            .map(|(start, end)| {
                                keys.iter()
                                    .filter(|key| *key >= start && *key < end)
                                    .cloned()
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();

                        if !buffer_lookup_keys.is_empty() {
                            let mut buffer_request =
                                crate::transaction::requests::new_buffer_batch_get_request(
                                    buffer_lookup_keys.iter().cloned().map(Into::into).collect(),
                                    timestamp.version(),
                                );
                            Self::apply_snapshot_read_context(
                                &mut buffer_request.context,
                                snapshot_ctx.clone(),
                            );
                            if !resource_group_tag_set {
                                if let Some(tagger) = resource_group_tagger.as_ref() {
                                    let tag = (tagger)(buffer_request.label());
                                    let ctx = buffer_request
                                        .context
                                        .get_or_insert_with(kvrpcpb::Context::default);
                                    ctx.resource_group_tag = tag;
                                }
                            }

                            let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                                rpc.clone(),
                                keyspace,
                                buffer_request,
                                lock_resolver_rpc_context.rpc_interceptors.clone(),
                            )
                            .resolve_lock_for_read(
                                resolve_locks_ctx.clone(),
                                timestamp.clone(),
                                retry_options.lock_backoff.clone(),
                                killed.clone(),
                                keyspace,
                                false,
                                lock_tracker.clone(),
                                lock_resolver_rpc_context.clone(),
                            );
                            let plan_builder = if replica_read.is_follower_read()
                                || enable_load_based_replica_read
                            {
                                plan_builder
                                    .retry_multi_region_with_replica_read_and_match_stores(
                                        retry_options.region_backoff.clone(),
                                        replica_read,
                                        match_store_ids.clone(),
                                        match_store_labels.clone(),
                                    )
                                    .with_killed(killed.clone())
                            } else {
                                plan_builder
                                    .retry_multi_region(retry_options.region_backoff.clone())
                                    .with_killed(killed.clone())
                            };
                            let plan = plan_builder.merge(Collect).plan();
                            let mut remote_pairs: Vec<KvPair> =
                                plan.execute().await?.into_iter().map(Into::into).collect();
                            buffer_pairs.append(&mut remote_pairs);
                        }

                        let buffer_keys = buffer_pairs
                            .iter()
                            .map(|pair| pair.0.clone())
                            .collect::<HashSet<_>>();
                        let snapshot_keys = keys
                            .into_iter()
                            .filter(|key| !buffer_keys.contains(key))
                            .collect::<Vec<_>>();
                        if snapshot_keys.is_empty() {
                            return Ok(buffer_pairs);
                        }

                        let mut snapshot_request =
                            crate::transaction::requests::new_batch_get_request(
                                snapshot_keys.into_iter().map(Into::into).collect(),
                                timestamp.version(),
                            );
                        Self::apply_snapshot_read_context(
                            &mut snapshot_request.context,
                            snapshot_ctx,
                        );
                        if !resource_group_tag_set {
                            if let Some(tagger) = resource_group_tagger.as_ref() {
                                let tag = (tagger)(snapshot_request.label());
                                let ctx = snapshot_request
                                    .context
                                    .get_or_insert_with(kvrpcpb::Context::default);
                                ctx.resource_group_tag = tag;
                            }
                        }

                        let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                            rpc,
                            keyspace,
                            snapshot_request,
                            lock_resolver_rpc_context.rpc_interceptors.clone(),
                        )
                        .resolve_lock_for_read(
                            resolve_locks_ctx,
                            timestamp,
                            retry_options.lock_backoff,
                            killed.clone(),
                            keyspace,
                            false,
                            lock_tracker,
                            lock_resolver_rpc_context,
                        );
                        let plan_builder =
                            if replica_read.is_follower_read() || enable_load_based_replica_read {
                                plan_builder
                                    .retry_multi_region_with_replica_read_and_match_stores(
                                        retry_options.region_backoff,
                                        replica_read,
                                        match_store_ids,
                                        match_store_labels,
                                    )
                                    .with_killed(killed.clone())
                            } else {
                                plan_builder
                                    .retry_multi_region(retry_options.region_backoff)
                                    .with_killed(killed.clone())
                            };
                        let plan = plan_builder.merge(Collect).plan();
                        let snapshot_pairs: Vec<KvPair> =
                            plan.execute().await?.into_iter().map(Into::into).collect();
                        buffer_pairs.extend(snapshot_pairs);
                        return Ok(buffer_pairs);
                    }

                    let mut request = crate::transaction::requests::new_batch_get_request(
                        keys.into_iter().map(Into::into).collect(),
                        timestamp.version(),
                    );
                    Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);
                    if !resource_group_tag_set {
                        if let Some(tagger) = resource_group_tagger.as_ref() {
                            let tag = (tagger)(request.label());
                            let ctx = request
                                .context
                                .get_or_insert_with(kvrpcpb::Context::default);
                            ctx.resource_group_tag = tag;
                        }
                    }

                    let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                        rpc,
                        keyspace,
                        request,
                        lock_resolver_rpc_context.rpc_interceptors.clone(),
                    )
                    .resolve_lock_for_read(
                        resolve_locks_ctx,
                        timestamp,
                        retry_options.lock_backoff,
                        killed.clone(),
                        keyspace,
                        false,
                        lock_tracker,
                        lock_resolver_rpc_context,
                    );
                    let plan_builder =
                        if replica_read.is_follower_read() || enable_load_based_replica_read {
                            plan_builder
                                .retry_multi_region_with_replica_read_and_match_stores(
                                    retry_options.region_backoff,
                                    replica_read,
                                    match_store_ids,
                                    match_store_labels,
                                )
                                .with_killed(killed.clone())
                        } else {
                            plan_builder
                                .retry_multi_region(retry_options.region_backoff)
                                .with_killed(killed.clone())
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

    /// Get the values associated with the given keys and their commit timestamps.
    ///
    /// This is only supported for **read-only snapshots** (`TransactionOptions::read_only()`) and
    /// sets `kvrpcpb::BatchGetRequest.need_commit_ts = true`.
    ///
    /// Returns [`Error::CommitTsRequiredButNotReturned`] if TiKV does not return a commit timestamp
    /// for an existing key.
    ///
    /// Non-existent entries will not appear in the result. The order of the keys is not retained
    /// in the result.
    pub async fn batch_get_with_commit_ts(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = (KvPair, u64)>> {
        debug!("invoking transactional batch_get_with_commit_ts request");
        self.check_allow_operation().await?;
        self.check_visibility().await?;
        if !self.options.read_only {
            return Err(Error::StringError(
                "batch_get_with_commit_ts is only supported for read-only snapshots".to_owned(),
            ));
        }

        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keys = keys
            .into_iter()
            .map(|k| k.into().encode_keyspace(keyspace, KeyMode::Txn))
            .collect::<Vec<_>>();
        let key_count = keys.len();
        let retry_options = RetryOptions::new(self.region_backoff(), self.lock_backoff());
        let killed = self.vars.killed.clone();
        let mut snapshot_ctx = self.snapshot_read_context();
        if snapshot_ctx.replica_read.is_follower_read() {
            if let Some(adjuster) = snapshot_ctx.replica_read_adjuster.as_ref() {
                snapshot_ctx.replica_read = (adjuster)(key_count);
            }
        }
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let replica_read = snapshot_ctx.replica_read;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();
        let match_store_ids = self.options.match_store_ids.clone();
        let match_store_labels = self.options.match_store_labels.clone();
        let resource_group_tag_set = self.options.resource_group_tag.is_some();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

        let mut request = crate::transaction::requests::new_batch_get_request(
            keys.into_iter().map(Into::into).collect(),
            timestamp.version(),
        );
        request.need_commit_ts = true;
        Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);
        if !resource_group_tag_set {
            if let Some(tagger) = resource_group_tagger.as_ref() {
                let tag = (tagger)(request.label());
                let ctx = request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }

        let plan_builder = PlanBuilder::new_with_rpc_interceptors(
            rpc,
            keyspace,
            request,
            lock_resolver_rpc_context.rpc_interceptors.clone(),
        )
        .resolve_lock_for_read(
            resolve_locks_ctx,
            timestamp,
            retry_options.lock_backoff,
            killed.clone(),
            keyspace,
            false,
            lock_tracker,
            lock_resolver_rpc_context,
        );
        let plan_builder = if replica_read.is_follower_read() || enable_load_based_replica_read {
            plan_builder
                .retry_multi_region_with_replica_read_and_match_stores(
                    retry_options.region_backoff,
                    replica_read,
                    match_store_ids,
                    match_store_labels,
                )
                .with_killed(killed)
        } else {
            plan_builder
                .retry_multi_region(retry_options.region_backoff)
                .with_killed(killed)
        };
        let plan = plan_builder.merge(CollectBatchGetWithCommitTs).plan();
        plan.execute().await.map(move |pairs| {
            pairs
                .into_iter()
                .map(move |(pair, commit_ts)| (pair.truncate_keyspace(keyspace), commit_ts))
        })
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

    /// Create a new 'batch get for share' request.
    ///
    /// This is similar to [`Transaction::batch_get_for_update`], but acquires shared pessimistic
    /// locks (`SharedPessimisticLock`) for pessimistic transactions.
    pub async fn batch_get_for_share(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking transactional batch_get_for_share request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let keys: Vec<Key> = keys.into_iter().map(|k| k.into()).collect();
            self.lock_keys_in_share_mode(keys.clone()).await?;
            Ok(self.batch_get(keys).await?.collect())
        } else {
            let keyspace = self.keyspace;
            let lock_wait_timeout = self.options.lock_wait_timeout;
            let keys = keys
                .into_iter()
                .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
            let pairs = self
                .pessimistic_lock_in_share_mode_with_wait_timeout(keys, true, lock_wait_timeout)
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
    /// Note: this operation is not supported for pipelined transactions.
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
    /// Note: this operation is not supported for pipelined transactions.
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
    ///
    /// Note: this operation is not supported for pipelined transactions.
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
    ///
    /// Note: this operation is not supported for pipelined transactions.
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
        if self.options.pipelined_txn.is_some() {
            let _ = self.flush(false).await?;
        }
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
        if self.options.pipelined_txn.is_some()
            && self
                .pipelined
                .as_ref()
                .is_some_and(|state| state.generation > 0)
            && !self.buffer.has_mutation_or_lock(&key)
        {
            let pipelined_flushed_deletes = self
                .pipelined
                .as_ref()
                .map(|state| state.flushed_deletes.clone());
            if pipelined_flushed_deletes.as_ref().is_some_and(|deleted| {
                let deleted = match deleted.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                deleted.contains(&key)
            }) {
                // The key has been flushed as a delete (or CheckNotExist); treat it as non-existent.
            } else {
                let pipelined_flushing_puts = self
                    .pipelined
                    .as_ref()
                    .and_then(|state| state.flushing_puts.clone());
                if pipelined_flushing_puts
                    .as_ref()
                    .is_some_and(|puts| puts.contains_key(&key))
                {
                    return Err(Error::DuplicateKeyInsertion);
                }

                let pipelined_flushed_range = self.pipelined.as_ref().and_then(|state| {
                    match (&state.flushed_range_start, &state.flushed_range_end) {
                        (Some(start), Some(end)) => Some((start.clone(), end.clone())),
                        _ => None,
                    }
                });

                if pipelined_flushed_range
                    .as_ref()
                    .is_some_and(|(start, end)| &key >= start && &key < end)
                {
                    let timestamp = self.timestamp.clone();
                    let rpc = self.rpc.clone();
                    let keyspace = self.keyspace;
                    let lock_backoff = self.lock_backoff();
                    let region_backoff = self.region_backoff();
                    let killed = self.vars.killed.clone();
                    let snapshot_ctx = self.snapshot_read_context();
                    let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
                    let replica_read = snapshot_ctx.replica_read;
                    let lock_tracker = self.read_lock_tracker.clone();
                    let resolve_locks_ctx = self.resolve_locks_ctx.clone();
                    let match_store_ids = self.options.match_store_ids.clone();
                    let match_store_labels = self.options.match_store_labels.clone();
                    let resource_group_tag_set = self.options.resource_group_tag.is_some();
                    let resource_group_tagger = self.resource_group_tagger.clone();
                    let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

                    let mut request =
                        new_buffer_batch_get_request(iter::once(key.clone()), timestamp.clone());
                    Self::apply_snapshot_read_context(&mut request.context, snapshot_ctx);
                    if !resource_group_tag_set {
                        if let Some(tagger) = resource_group_tagger.as_ref() {
                            let tag = (tagger)(request.label());
                            let ctx = request
                                .context
                                .get_or_insert_with(kvrpcpb::Context::default);
                            ctx.resource_group_tag = tag;
                        }
                    }

                    let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                        rpc,
                        keyspace,
                        request,
                        lock_resolver_rpc_context.rpc_interceptors.clone(),
                    )
                    .resolve_lock_for_read(
                        resolve_locks_ctx,
                        timestamp,
                        lock_backoff,
                        killed.clone(),
                        keyspace,
                        true,
                        lock_tracker,
                        lock_resolver_rpc_context,
                    );
                    let plan_builder =
                        if replica_read.is_follower_read() || enable_load_based_replica_read {
                            plan_builder
                                .retry_multi_region_with_replica_read_and_match_stores(
                                    region_backoff,
                                    replica_read,
                                    match_store_ids,
                                    match_store_labels,
                                )
                                .with_killed(killed)
                        } else {
                            plan_builder
                                .retry_multi_region(region_backoff)
                                .with_killed(killed)
                        };
                    let plan = plan_builder.merge(Collect).plan();
                    let pairs = plan.execute().await?;
                    if !pairs.is_empty() {
                        return Err(Error::DuplicateKeyInsertion);
                    }
                }
            }
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once((key.clone(), kvrpcpb::Assertion::NotExist)),
                false,
            )
            .await?;
        }
        self.buffer.insert(key, value.into());
        if self.options.pipelined_txn.is_some() {
            let _ = self.flush(false).await?;
        }
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
        if self.options.pipelined_txn.is_some() {
            let _ = self.flush(false).await?;
        }
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
        if self.options.pipelined_txn.is_some() {
            let _ = self.flush(false).await?;
        }
        Ok(())
    }

    /// Flush buffered mutations to TiKV.
    ///
    /// This is only supported for pipelined transactions enabled via
    /// [`TransactionOptions::pipelined`] / [`TransactionOptions::pipelined_txn`].
    ///
    /// The returned boolean indicates whether a flush was triggered.
    pub async fn flush(&mut self, force: bool) -> Result<bool> {
        self.check_allow_operation().await?;
        self.options.validate()?;
        self.vars.check_killed()?;

        let Some(pipelined) = self.options.pipelined_txn else {
            return Err(Error::StringError(
                "flush is only supported for pipelined transactions".to_owned(),
            ));
        };

        let mutation_count = self.buffer.mutation_count();
        let write_size = self.buffer.mutation_size();
        if mutation_count == 0 {
            return Ok(false);
        }

        let should_flush = self
            .pipelined
            .as_ref()
            .ok_or_else(|| {
                crate::internal_err!("pipelined txn options are set but pipelined state is missing")
            })?
            .should_flush(force, mutation_count, write_size);
        if !should_flush {
            return Ok(false);
        }
        debug!(
            "triggering transactional flush (force={force}, mutation_count={mutation_count}, write_size={write_size})"
        );

        // If the mutable buffer is too large, block until the previous flush finishes.
        let is_flushing = self
            .pipelined
            .as_ref()
            .ok_or_else(|| {
                crate::internal_err!("pipelined txn options are set but pipelined state is missing")
            })?
            .is_flushing();
        if is_flushing {
            self.pipelined
                .as_mut()
                .ok_or_else(|| {
                    crate::internal_err!(
                        "pipelined txn options are set but pipelined state is missing"
                    )
                })?
                .flush_wait()
                .await?;
        }

        // Match client-go: pipelined flush requires a primary key (a non-check mutation).
        if self
            .pipelined
            .as_ref()
            .and_then(|state| state.primary_key.clone())
            .is_none()
            && self.buffer.get_primary_key().is_none()
        {
            return Err(Error::StringError(
                "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
            ));
        }

        let mutations = self.buffer.take_mutations();
        debug_assert!(!mutations.is_empty());

        let (primary_key, generation, flush_ewma) = {
            let state = self.pipelined.as_mut().ok_or_else(|| {
                crate::internal_err!("pipelined txn options are set but pipelined state is missing")
            })?;
            let primary_key = state.primary_key_or_init(&mutations)?;
            state.record_flushed_mutations(&mutations);
            state.record_flushing_puts(&mutations);
            let generation = state.next_generation();
            let flush_ewma = state.flush_duration_ewma.clone();
            (primary_key, generation, flush_ewma)
        };

        self.start_auto_heartbeat().await?;

        let mut flush_request = new_flush_request(
            mutations,
            primary_key,
            self.timestamp.clone(),
            self.timestamp.version().saturating_add(1),
            generation,
            MAX_TTL,
            self.options.assertion_level.into(),
        );
        self.options.apply_write_context(&mut flush_request.context);
        if let Some(ctx) = flush_request.context.as_mut() {
            ctx.request_source = PIPELINED_REQUEST_SOURCE.to_owned();
        }
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(flush_request.label());
                let ctx = flush_request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }

        let flush_pd = self.rpc.clone();
        let flush_keyspace = self.keyspace;
        let flush_resolve_locks_ctx = self.resolve_locks_ctx.clone();
        let flush_start_version = self.timestamp.clone();
        let flush_lock_backoff = self.lock_backoff();
        let flush_region_backoff = self.region_backoff();
        let flush_killed = self.vars.killed.clone();
        let flush_concurrency = pipelined.flush_concurrency();
        let flush_lock_resolver_rpc_context = self.lock_resolver_rpc_context();
        let write_throttle_ratio = pipelined.write_throttle_ratio();

        let flush_handle = tokio::spawn(async move {
            throttle_pipelined_flush(flush_ewma.clone(), write_throttle_ratio).await;

            let start = Instant::now();
            let plan = PlanBuilder::new_with_rpc_interceptors(
                flush_pd,
                flush_keyspace,
                flush_request,
                flush_lock_resolver_rpc_context.rpc_interceptors.clone(),
            )
            .resolve_lock_in_context(
                flush_resolve_locks_ctx,
                flush_start_version,
                flush_lock_backoff,
                flush_killed.clone(),
                flush_keyspace,
                flush_lock_resolver_rpc_context,
            )
            .retry_multi_region_with_concurrency(flush_region_backoff, flush_concurrency)
            .with_killed(flush_killed)
            .merge(CollectError)
            .extract_error()
            .plan();
            let result = plan.execute().await.map(|_| ());

            let sample_ms = start.elapsed().as_millis() as f64;
            let mut flush_ewma = match flush_ewma.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            flush_ewma.observe(sample_ms);

            result
        });
        self.pipelined
            .as_mut()
            .ok_or_else(|| {
                crate::internal_err!("pipelined txn options are set but pipelined state is missing")
            })?
            .flushing = Some(flush_handle);

        Ok(true)
    }

    /// Wait for an outstanding pipelined flush (if any) to complete.
    pub async fn flush_wait(&mut self) -> Result<()> {
        debug!("invoking transactional flush_wait");
        self.check_allow_operation().await?;
        if let Some(state) = self.pipelined.as_mut() {
            state.flush_wait().await?;
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
        self.lock_keys_impl(keys, self.options.lock_wait_timeout, false)
            .await
    }

    /// Lock the given keys without mutating their values, using the provided lock wait timeout.
    ///
    /// In optimistic mode, this behaves the same as [`Transaction::lock_keys`].
    ///
    /// In pessimistic mode, the `lock_wait_timeout` controls how long TiKV will wait when
    /// encountering conflicting locks (`kvrpcpb::PessimisticLockRequest.wait_timeout`).
    ///
    /// This maps to client-go `KVTxn.LockKeysWithWaitTime`.
    pub async fn lock_keys_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys_with_wait_timeout request");
        self.lock_keys_impl(keys, lock_wait_timeout, false).await
    }

    /// Lock the given keys in share mode without mutating their values.
    ///
    /// In optimistic mode, this behaves the same as [`Transaction::lock_keys`]. Shared locks are not
    /// supported for optimistic transactions.
    ///
    /// In pessimistic mode, this issues a `SharedPessimisticLock` request in TiKV. Note that shared
    /// locks cannot be used as the transaction primary key, so the transaction must already have a
    /// primary key selected (for example by locking a key in exclusive mode or by writing a key)
    /// before acquiring shared locks.
    pub async fn lock_keys_in_share_mode(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys_in_share_mode request");
        self.lock_keys_impl(keys, self.options.lock_wait_timeout, true)
            .await
    }

    /// Lock the given keys in share mode without mutating their values, using the provided lock
    /// wait timeout.
    pub async fn lock_keys_in_share_mode_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys_in_share_mode_with_wait_timeout request");
        self.lock_keys_impl(keys, lock_wait_timeout, true).await
    }

    /// Start aggressive locking mode for pessimistic transactions.
    ///
    /// Aggressive locking keeps pessimistic locks acquired during a statement attempt so they can be reused
    /// when the statement is retried, avoiding redundant lock RPCs.
    ///
    /// Note: If the transaction primary key is not selected when aggressive locking starts, this client may
    /// reset and reselect the primary key between retries to match client-go. When the primary key changes,
    /// previously acquired locks are re-locked rather than skipped.
    ///
    /// This mirrors client-go `KVTxn.StartAggressiveLocking`.
    pub fn start_aggressive_locking(&mut self) -> Result<()> {
        match self.get_status() {
            TransactionStatus::ReadOnly | TransactionStatus::Active => {}
            TransactionStatus::Committed
            | TransactionStatus::Rolledback
            | TransactionStatus::StartedCommit
            | TransactionStatus::StartedRollback
            | TransactionStatus::Dropped => return Err(Error::OperationAfterCommitError),
        }
        if !self.is_pessimistic() {
            return Err(Error::InvalidTransactionType);
        }
        if self.aggressive_locking.is_some() {
            return Err(Error::StringError(
                "aggressive locking is already started".to_owned(),
            ));
        }
        let now = Instant::now();
        self.aggressive_locking = Some(AggressiveLockingState {
            primary_key_at_start: self.buffer.get_primary_key(),
            last_primary_key: None,
            primary_key: None,
            last_attempt_start: now,
            attempt_start: now,
            max_locked_with_conflict_ts: 0,
            last_retry_unnecessary_locks: HashMap::new(),
            current_locked_keys: HashMap::new(),
        });
        Ok(())
    }

    /// Returns whether aggressive locking mode is enabled.
    ///
    /// This mirrors client-go `KVTxn.IsInAggressiveLockingMode`.
    #[must_use]
    pub fn is_in_aggressive_locking_mode(&self) -> bool {
        self.aggressive_locking.is_some()
    }

    /// Prepare for retrying a statement under aggressive locking mode.
    ///
    /// This rolls back locks that were acquired in the previous attempt but were not reused in the
    /// current attempt, then promotes the current attempt's locks as reusable for the next attempt.
    pub async fn retry_aggressive_locking(&mut self) -> Result<()> {
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            return Err(Error::InvalidTransactionType);
        }
        if self.aggressive_locking.is_none() {
            return Err(Error::StringError(
                "aggressive locking is not started".to_owned(),
            ));
        }

        self.cleanup_aggressive_locking_redundant_locks().await?;

        let state = self
            .aggressive_locking
            .as_mut()
            .ok_or_else(|| Error::StringError("aggressive locking is not started".to_owned()))?;
        let leftover_last_retry_unnecessary_locks =
            std::mem::take(&mut state.last_retry_unnecessary_locks);
        let mut next_last_retry_unnecessary_locks = std::mem::take(&mut state.current_locked_keys);
        for (key, expected_for_update_ts) in leftover_last_retry_unnecessary_locks {
            next_last_retry_unnecessary_locks
                .entry(key)
                .or_insert(expected_for_update_ts);
        }
        state.last_retry_unnecessary_locks = next_last_retry_unnecessary_locks;
        state.last_attempt_start = state.attempt_start;
        state.attempt_start = Instant::now();
        if state.primary_key_at_start.is_none() && state.primary_key.is_some() {
            state.last_primary_key = state.primary_key.take();
            self.buffer.clear_primary_key();
        }
        Ok(())
    }

    /// Finish aggressive locking mode.
    ///
    /// Any redundant locks from the previous attempt are rolled back.
    pub async fn done_aggressive_locking(&mut self) -> Result<()> {
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            return Err(Error::InvalidTransactionType);
        }
        if self.aggressive_locking.is_none() {
            return Err(Error::StringError(
                "aggressive locking is not started".to_owned(),
            ));
        }
        self.cleanup_aggressive_locking_redundant_locks().await?;
        if let Some(state) = self.aggressive_locking.as_ref() {
            for (key, expected_for_update_ts) in state
                .current_locked_keys
                .iter()
                .chain(state.last_retry_unnecessary_locks.iter())
            {
                self.for_update_ts_checks
                    .entry(key.clone())
                    .or_insert(*expected_for_update_ts);
            }
        }
        self.aggressive_locking = None;
        Ok(())
    }

    /// Cancel aggressive locking mode and roll back locks acquired during the aggressive-locking
    /// attempts.
    pub async fn cancel_aggressive_locking(&mut self) -> Result<()> {
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            return Err(Error::InvalidTransactionType);
        }

        let Some(state) = self.aggressive_locking.as_ref() else {
            return Err(Error::StringError(
                "aggressive locking is not started".to_owned(),
            ));
        };

        let primary_key_at_start = state.primary_key_at_start.clone();
        let current_primary_key = self.buffer.get_primary_key();

        let mut keys: HashSet<Key> = state
            .last_retry_unnecessary_locks
            .keys()
            .chain(state.current_locked_keys.keys())
            .cloned()
            .collect();
        if keys.is_empty() {
            if primary_key_at_start.is_none() {
                self.buffer.clear_primary_key();
                self.stop_auto_heartbeat();
            }
            self.aggressive_locking = None;
            return Ok(());
        }

        // When the transaction already had a primary key before aggressive locking started, keep
        // it stable and avoid rolling it back here.
        if primary_key_at_start.is_some() {
            if let Some(primary) = current_primary_key.as_ref() {
                keys.remove(primary);
            }
        }

        let for_update_ts = match &self.options.kind {
            TransactionKind::Pessimistic(for_update_ts) => for_update_ts.clone(),
            TransactionKind::Optimistic => return Err(Error::InvalidTransactionType),
        };

        self.pessimistic_lock_rollback(keys.into_iter(), self.timestamp.clone(), for_update_ts)
            .await?;

        if primary_key_at_start.is_none() {
            self.buffer.clear_primary_key();
            self.stop_auto_heartbeat();
        }

        self.aggressive_locking = None;
        Ok(())
    }

    async fn lock_keys_impl(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        lock_wait_timeout: LockWaitTimeout,
        in_share_mode: bool,
    ) -> Result<()> {
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
                let _ = if in_share_mode {
                    self.pessimistic_lock_with_wait_timeout_and_mode(
                        keys,
                        false,
                        lock_wait_timeout,
                        PessimisticLockMode::Shared,
                        true,
                    )
                    .await?
                } else {
                    self.pessimistic_lock_with_wait_timeout_and_mode(
                        keys,
                        false,
                        lock_wait_timeout,
                        PessimisticLockMode::Exclusive,
                        true,
                    )
                    .await?
                };
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
        if self.aggressive_locking.is_some() {
            return Err(Error::StringError(
                "aggressive locking is pending; call done_aggressive_locking or cancel_aggressive_locking first"
                    .to_owned(),
            ));
        }
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

        self.options.validate()?;

        let mut flush_wait_ms = 0i64;
        if let Some(state) = self.pipelined.as_mut() {
            let start = Instant::now();
            state.flush_wait().await?;
            let elapsed_ms = start.elapsed().as_millis();
            flush_wait_ms = elapsed_ms.min(u128::from(i64::MAX as u64)) as i64;
        }

        let mutations = self.buffer.to_proto_mutations();
        let buffer_primary_key = self.buffer.get_primary_key();
        let has_flushed_range = self
            .pipelined
            .as_ref()
            .map(PipelinedState::has_flushed_range)
            .unwrap_or(false);
        if mutations.is_empty() && !has_flushed_range {
            assert!(buffer_primary_key.is_none());
            return Ok(None);
        }

        let primary_key = if self.options.pipelined_txn.is_some() {
            let state = self.pipelined.as_mut().ok_or_else(|| {
                crate::internal_err!("pipelined txn options are set but pipelined state is missing")
            })?;
            // Initialize the pipelined primary key if this is the first (commit-time) flush.
            if state.primary_key.is_none() {
                let _ = state.primary_key_or_init(&mutations)?;
            }
            state.primary_key.clone()
        } else {
            match &self.options.kind {
                TransactionKind::Optimistic => None,
                TransactionKind::Pessimistic(_) => buffer_primary_key,
            }
        };

        let mut latch_guard = if matches!(self.options.kind, TransactionKind::Optimistic)
            && self.options.pipelined_txn.is_none()
        {
            match self.txn_latches.clone() {
                Some(latches) => {
                    let latch_keys = mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect();
                    Some(latches.lock(self.timestamp.version(), latch_keys).await)
                }
                None => None,
            }
        } else {
            None
        };

        if latch_guard.as_ref().is_some_and(|guard| guard.is_stale()) {
            let start_ts = latch_guard.as_ref().unwrap().start_ts();
            let guard = latch_guard.take().unwrap();
            guard.unlock().await;
            return Err(Error::WriteConflictInLatch { start_ts });
        }

        if let Err(err) = self.start_auto_heartbeat().await {
            if let Some(guard) = latch_guard.take() {
                guard.unlock().await;
            }
            return Err(err);
        }

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
        committer.for_update_ts_constraints = self.for_update_ts_checks.clone();
        committer.rpc_interceptors = self.rpc_interceptors.clone();
        committer.vars = self.vars.clone();
        if let Some(state) = self.pipelined.as_ref() {
            committer.pipelined_generation = state.generation;
            committer.pipelined_range_start = state.flushed_range_start.clone();
            committer.pipelined_range_end = state.flushed_range_end.clone();
            committer.pipelined_flush_duration_ewma = Some(state.flush_duration_ewma.clone());
        }
        committer.resolve_locks_ctx = self.resolve_locks_ctx.clone();
        committer.resolve_lock_detail = Some(self.resolve_lock_detail.clone());
        committer.resource_group_tagger = self.resource_group_tagger.clone();
        committer.schema_ver = self.schema_ver;
        committer.schema_lease_checker = self.schema_lease_checker.clone();
        committer.commit_wait_until_tso = self.commit_wait_until_tso;
        committer.commit_wait_until_tso_timeout = self.commit_wait_until_tso_timeout;
        committer.commit_ts_upper_bound_check = self.commit_ts_upper_bound_check.clone();
        let (res, mode_info) = committer.commit_with_mode_info().await;

        if let Some(guard) = latch_guard {
            if let Ok(Some(commit_ts)) = &res {
                guard.set_commit_ts(commit_ts.version());
            }
            guard.unlock().await;
        }

        if let Some(callback) = self.commit_callback.as_ref() {
            let commit_ts = match &res {
                Ok(Some(ts)) => ts.version(),
                _ => 0,
            };
            let info = TxnInfo {
                txn_scope: self.txn_scope().to_owned(),
                start_ts: self.timestamp.version(),
                commit_ts,
                txn_commit_mode: mode_info.txn_commit_mode(),
                async_commit_fallback: mode_info.async_commit_fallback(),
                one_pc_fallback: mode_info.one_pc_fallback(),
                error: res.as_ref().err().map(|err| err.to_string()),
                pipelined: self.options.pipelined_txn.is_some(),
                flush_wait_ms,
            };
            let info_str = serde_json::to_string(&info).unwrap_or_else(|err| {
                format!("failed to serialize transaction commit info: {err}; info: {info:?}")
            });
            callback(&info_str, res.as_ref().err());
        }

        if let Ok(Some(commit_ts)) = &res {
            self.commit_ts = Some(commit_ts.clone());
        }
        if res.is_ok() {
            self.set_status(TransactionStatus::Committed);
        } else {
            // Stop the background heartbeat task. On commit failure the transaction remains in
            // `StartedCommit`, which would otherwise keep sending heartbeats indefinitely.
            self.stop_auto_heartbeat();
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
        if self.aggressive_locking.is_some() {
            return Err(Error::StringError(
                "aggressive locking is pending; call done_aggressive_locking or cancel_aggressive_locking first"
                    .to_owned(),
            ));
        }
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

        // Stop the background heartbeat task early. Pipelined transactions may broadcast status
        // after successful heartbeats; once rollback starts, we don't want further heartbeats to
        // keep the transaction alive.
        self.stop_auto_heartbeat();

        if let Some(state) = self.pipelined.as_mut() {
            // Match client-go: wait for in-flight pipelined flushes to finish on rollback to avoid
            // racing with the flush task.
            state.flush_wait().await?;
        }

        let pipelined_flushed_range = self.pipelined.as_ref().and_then(|state| {
            state
                .flushed_range_start
                .clone()
                .zip(state.flushed_range_end.clone())
        });
        let pipelined_resolve_concurrency = self
            .options
            .pipelined_txn
            .as_ref()
            .map(PipelinedTxnOptions::resolve_lock_concurrency);

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
        committer.rpc_interceptors = self.rpc_interceptors.clone();
        committer.vars = self.vars.clone();
        committer.resolve_locks_ctx = self.resolve_locks_ctx.clone();
        committer.resolve_lock_detail = Some(self.resolve_lock_detail.clone());
        committer.resource_group_tagger = self.resource_group_tagger.clone();
        committer.schema_ver = self.schema_ver;
        committer.schema_lease_checker = self.schema_lease_checker.clone();

        let res = committer.rollback().await;

        if res.is_ok() {
            self.set_status(TransactionStatus::Rolledback);
        }
        if let (Some((range_start, range_end)), Some(resolve_concurrency)) =
            (pipelined_flushed_range, pipelined_resolve_concurrency)
        {
            let rpc = self.rpc.clone();
            let start_ts = self.timestamp.version();
            let status = kvrpcpb::TxnStatus {
                start_ts,
                min_commit_ts: start_ts.saturating_add(1),
                commit_ts: 0,
                rolled_back: true,
                is_completed: false,
            };
            tokio::spawn(async move {
                if let Err(err) = rpc.broadcast_txn_status_to_all_stores(vec![status]).await {
                    debug!("broadcast_txn_status_to_all_stores failed: {err}");
                }
            });

            spawn_pipelined_resolve_locks_and_broadcast_completion(
                self.rpc.clone(),
                self.keyspace,
                self.rpc_interceptors.clone(),
                self.options.clone(),
                self.resource_group_tagger.clone(),
                start_ts,
                0,
                true,
                range_start,
                range_end,
                self.region_backoff(),
                self.vars.killed.clone(),
                resolve_concurrency,
            );
        }
        res
    }

    /// Get the start timestamp of this transaction.
    pub fn start_timestamp(&self) -> Timestamp {
        self.timestamp.clone()
    }

    /// Get the start timestamp version of this transaction.
    ///
    /// This maps to client-go `KVTxn.StartTS`.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.timestamp.version()
    }

    /// Get the commit timestamp of this transaction (if committed).
    #[must_use]
    pub fn commit_timestamp(&self) -> Option<Timestamp> {
        self.commit_ts.clone()
    }

    /// Get the commit timestamp version of this transaction.
    ///
    /// Returns 0 when the transaction is not committed.
    ///
    /// This maps to client-go `KVTxn.CommitTS`.
    #[must_use]
    pub fn commit_ts(&self) -> u64 {
        self.commit_ts.as_ref().map(|ts| ts.version()).unwrap_or(0)
    }

    /// Returns whether this transaction has only performed read operations so far.
    ///
    /// This maps to client-go `KVTxn.IsReadOnly`.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        if self.options.read_only {
            return true;
        }
        let has_flushed_range = self
            .pipelined
            .as_ref()
            .map(PipelinedState::has_flushed_range)
            .unwrap_or(false);
        self.buffer.mutation_count() == 0 && !has_flushed_range
    }

    /// Returns whether the transaction is valid.
    ///
    /// A transaction becomes invalid after commit or rollback. This maps to client-go `KVTxn.Valid`.
    #[must_use]
    pub fn valid(&self) -> bool {
        matches!(
            self.get_status(),
            TransactionStatus::ReadOnly | TransactionStatus::Active
        )
    }

    /// Returns the number of buffered entries in this transaction.
    ///
    /// This maps to client-go `KVTxn.Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.mutation_count()
    }

    /// Returns true if the transaction has no buffered entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the buffered size (sum of key and value lengths) of this transaction.
    ///
    /// This maps to client-go `KVTxn.Size`.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.buffer.mutation_size()
    }

    /// Set a hook that is triggered when the local mutation buffer memory footprint changes.
    ///
    /// This maps to client-go `KVTxn.SetMemoryFootprintChangeHook`.
    pub fn set_memory_footprint_change_hook<F>(&mut self, hook: F)
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        self.buffer.set_memory_footprint_change_hook(Arc::new(hook));
    }

    /// Clear the memory footprint change hook.
    pub fn clear_memory_footprint_change_hook(&mut self) {
        self.buffer.clear_memory_footprint_change_hook();
    }

    /// Returns whether the memory footprint change hook is set.
    ///
    /// This maps to client-go `KVTxn.MemHookSet`.
    #[must_use]
    pub fn mem_hook_set(&self) -> bool {
        self.buffer.mem_hook_set()
    }

    /// Returns the current memory footprint of the local mutation buffer.
    ///
    /// This maps to client-go `KVTxn.Mem`.
    #[must_use]
    pub fn mem(&self) -> u64 {
        self.buffer.mem()
    }

    /// Send a heart beat message to keep the transaction alive on the server and update its TTL.
    ///
    /// Returns the TTL set on the transaction's locks by TiKV.
    #[doc(hidden)]
    pub async fn send_heart_beat(&mut self) -> Result<u64> {
        debug!("sending heart_beat");
        self.check_allow_operation().await?;
        self.vars.check_killed()?;
        let primary_key = match self.buffer.get_primary_key() {
            Some(k) => k,
            None => return Err(Error::NoPrimaryKey),
        };
        let mut request = new_heart_beat_request(
            self.timestamp.clone(),
            primary_key,
            self.start_instant.elapsed().as_millis() as u64 + MAX_TTL,
        );
        self.options.apply_write_context(&mut request.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(request.label());
                let ctx = request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let plan = PlanBuilder::new_with_rpc_interceptors(
            self.rpc.clone(),
            self.keyspace,
            request,
            lock_resolver_rpc_context.rpc_interceptors.clone(),
        )
        .resolve_lock_in_context(
            self.resolve_locks_ctx.clone(),
            self.timestamp.clone(),
            lock_backoff,
            killed.clone(),
            self.keyspace,
            lock_resolver_rpc_context,
        )
        .retry_multi_region(region_backoff)
        .with_killed(killed)
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
        self.check_visibility().await?;
        if self.options.pipelined_txn.is_some() {
            return Err(Error::StringError(
                "scan is not supported for pipelined transactions".to_owned(),
            ));
        }
        let timestamp = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let snapshot_ctx = self.snapshot_read_context();
        let enable_load_based_replica_read = snapshot_ctx.busy_threshold_ms > 0;
        let replica_read = snapshot_ctx.replica_read;
        let lock_tracker = self.read_lock_tracker.clone();
        let resolve_locks_ctx = self.resolve_locks_ctx.clone();
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let match_store_ids = self.options.match_store_ids.clone();
        let match_store_labels = self.options.match_store_labels.clone();
        let resource_group_tag_set = self.options.resource_group_tag.is_some();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

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
                    if !resource_group_tag_set {
                        if let Some(tagger) = resource_group_tagger.as_ref() {
                            let tag = (tagger)(request.label());
                            let ctx = request
                                .context
                                .get_or_insert_with(kvrpcpb::Context::default);
                            ctx.resource_group_tag = tag;
                        }
                    }

                    let plan_builder = PlanBuilder::new_with_rpc_interceptors(
                        rpc,
                        keyspace,
                        request,
                        lock_resolver_rpc_context.rpc_interceptors.clone(),
                    )
                    .resolve_lock_for_read(
                        resolve_locks_ctx,
                        timestamp,
                        lock_backoff,
                        killed.clone(),
                        keyspace,
                        false,
                        lock_tracker,
                        lock_resolver_rpc_context,
                    );
                    let plan_builder =
                        if replica_read.is_follower_read() || enable_load_based_replica_read {
                            plan_builder.retry_multi_region_with_replica_read_and_match_stores(
                                region_backoff,
                                replica_read,
                                match_store_ids,
                                match_store_labels,
                            )
                        } else {
                            plan_builder.retry_multi_region(region_backoff)
                        };
                    let plan = plan_builder.with_killed(killed).merge(Collect).plan();
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
    /// Returns [`Error::InvalidTransactionType`] if called on an optimistic transaction.
    async fn pessimistic_lock(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
    ) -> Result<Vec<KvPair>> {
        self.pessimistic_lock_with_wait_timeout(keys, need_value, self.options.lock_wait_timeout)
            .await
    }

    async fn pessimistic_lock_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<Vec<KvPair>> {
        self.pessimistic_lock_with_wait_timeout_and_mode(
            keys,
            need_value,
            lock_wait_timeout,
            PessimisticLockMode::Exclusive,
            false,
        )
        .await
    }

    async fn pessimistic_lock_in_share_mode_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<Vec<KvPair>> {
        self.pessimistic_lock_with_wait_timeout_and_mode(
            keys,
            need_value,
            lock_wait_timeout,
            PessimisticLockMode::Shared,
            false,
        )
        .await
    }

    async fn pessimistic_lock_with_wait_timeout_and_mode(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
        lock_wait_timeout: LockWaitTimeout,
        mode: PessimisticLockMode,
        track_aggressive_locking: bool,
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

        if mode == PessimisticLockMode::Shared && self.buffer.get_primary_key().is_none() {
            return Err(Error::StringError(
                "pessimistic lock in share mode requires primary key to be selected".to_owned(),
            ));
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

        if mode == PessimisticLockMode::Exclusive
            && locks
                .iter()
                .any(|(key, _)| self.buffer.is_locked_in_share_mode(key))
        {
            return Err(Error::StringError(
                "upgrading a shared lock to an exclusive lock is not supported".to_owned(),
            ));
        }

        // we do not set the primary key here, because pessimistic lock request
        // can fail, in which case the keys may not be part of the transaction.
        let existing_primary_key = self.buffer.get_primary_key();
        let is_first_lock = existing_primary_key.is_none() && locks.len() == 1;
        let primary_lock = match (mode, existing_primary_key) {
            (PessimisticLockMode::Exclusive, Some(primary)) => primary,
            (PessimisticLockMode::Exclusive, None) => {
                let mut selected = locks[0].0.clone();
                if track_aggressive_locking {
                    if let Some(state) = self.aggressive_locking.as_mut() {
                        if state.primary_key_at_start.is_none() {
                            if let Some(last_primary_key) = state.last_primary_key.as_ref() {
                                if locks
                                    .binary_search_by(|(key, _assertion)| key.cmp(last_primary_key))
                                    .is_ok()
                                {
                                    selected = last_primary_key.clone();
                                }
                            }
                            state.primary_key = Some(selected.clone());
                        }
                    }
                }
                selected
            }
            (PessimisticLockMode::Shared, Some(primary)) => primary,
            (PessimisticLockMode::Shared, None) => {
                return Err(Error::StringError(
                    "pessimistic lock in share mode requires primary key to be selected".to_owned(),
                ));
            }
        };

        let fresh_for_update_ts =
            get_timestamp_for_txn_scope(self.rpc.clone(), self.options.txn_scope.as_deref())
                .await?;
        self.options.push_for_update_ts(fresh_for_update_ts);
        let for_update_ts = match &self.options.kind {
            TransactionKind::Pessimistic(for_update_ts) => for_update_ts.clone(),
            TransactionKind::Optimistic => return Err(Error::InvalidTransactionType),
        };

        let locks_for_request = if need_value {
            locks.clone()
        } else if track_aggressive_locking {
            if let Some(state) = self.aggressive_locking.as_mut() {
                let can_try_skip = state.primary_key.is_none()
                    || state.last_primary_key.as_ref() == state.primary_key.as_ref();
                let last_attempt_locks_may_expire =
                    state.last_attempt_start.elapsed() >= Duration::from_millis(MAX_TTL);
                let mut filtered = Vec::with_capacity(locks.len());
                for (key, assertion) in &locks {
                    if state.current_locked_keys.contains_key(key) {
                        continue;
                    }
                    if let Some(expected_for_update_ts) =
                        state.last_retry_unnecessary_locks.get(key).copied()
                    {
                        if for_update_ts.version() < expected_for_update_ts {
                            return Err(Error::StringError(format!(
                                "txn {} retrying aggressive locking with for_update_ts ({}) less than previous expected_for_update_ts ({expected_for_update_ts})",
                                self.timestamp.version(),
                                for_update_ts.version(),
                            )));
                        }
                        if can_try_skip && !last_attempt_locks_may_expire {
                            state.last_retry_unnecessary_locks.remove(key);
                            state
                                .current_locked_keys
                                .insert(key.clone(), expected_for_update_ts);
                            continue;
                        }
                        filtered.push((key.clone(), *assertion));
                        continue;
                    }
                    if can_try_skip
                        && !last_attempt_locks_may_expire
                        && self.buffer.has_mutation_or_lock(key)
                    {
                        continue;
                    }
                    filtered.push((key.clone(), *assertion));
                }
                filtered
            } else {
                locks.clone()
            }
        } else {
            locks.clone()
        };
        if locks_for_request.is_empty() {
            self.buffer.primary_key_or(&primary_lock);
            self.start_auto_heartbeat().await?;
            for (key, _assertion) in locks {
                match mode {
                    PessimisticLockMode::Exclusive => self.buffer.lock(key),
                    PessimisticLockMode::Shared => self.buffer.lock_shared(key),
                }
            }
            return Ok(vec![]);
        }

        let elapsed = self.start_instant.elapsed().as_millis() as u64;
        let lock_ttl = elapsed.saturating_add(MAX_TTL);
        let lock_wait_start = Instant::now();
        let primary_in_request = locks_for_request
            .iter()
            .any(|(key, _)| key == &primary_lock);

        let apply_pessimistic_lock_mode = |request: &mut kvrpcpb::PessimisticLockRequest| {
            if mode == PessimisticLockMode::Shared {
                for mutation in &mut request.mutations {
                    mutation.op = kvrpcpb::Op::SharedPessimisticLock.into();
                }
            }
        };

        let use_force_lock_wake_up_mode = track_aggressive_locking
            && !need_value
            && self.aggressive_locking.is_some()
            && mode == PessimisticLockMode::Exclusive
            && locks_for_request.len() == 1;
        let apply_pessimistic_lock_wake_up_mode =
            |request: &mut kvrpcpb::PessimisticLockRequest| {
                if use_force_lock_wake_up_mode {
                    request.wake_up_mode =
                        kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock.into();
                }
            };

        let mut locked_keys_actual_for_update_ts = Vec::new();
        let mut max_locked_with_conflict_ts = 0u64;

        let pairs = if primary_in_request && locks_for_request.len() > 1 {
            let primary_region = self.rpc.region_for_key(&primary_lock).await?;
            let mut primary_locks = Vec::new();
            let mut secondary_locks = Vec::new();
            for lock in &locks_for_request {
                if primary_region.contains(&lock.0) {
                    primary_locks.push(lock.clone());
                } else {
                    secondary_locks.push(lock.clone());
                }
            }

            if primary_locks.is_empty() || secondary_locks.is_empty() {
                let mut request = new_pessimistic_lock_request(
                    locks_for_request.clone().into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                apply_pessimistic_lock_mode(&mut request);
                apply_pessimistic_lock_wake_up_mode(&mut request);
                let result = self
                    .execute_pessimistic_lock_request(
                        request,
                        for_update_ts.clone(),
                        lock_wait_timeout,
                        lock_wait_start,
                        mode,
                        track_aggressive_locking,
                    )
                    .await?;
                max_locked_with_conflict_ts = std::cmp::max(
                    max_locked_with_conflict_ts,
                    result.max_locked_with_conflict_ts,
                );
                locked_keys_actual_for_update_ts.extend(result.locked_keys_actual_for_update_ts);
                result.pairs
            } else {
                let mut primary_request = new_pessimistic_lock_request(
                    primary_locks.into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                apply_pessimistic_lock_mode(&mut primary_request);
                apply_pessimistic_lock_wake_up_mode(&mut primary_request);
                let primary_request_keys = primary_request
                    .mutations
                    .iter()
                    .map(|mutation| Key::from(mutation.key.clone()))
                    .collect::<Vec<_>>();
                let primary_result = self
                    .execute_pessimistic_lock_request(
                        primary_request,
                        for_update_ts.clone(),
                        lock_wait_timeout,
                        lock_wait_start,
                        mode,
                        track_aggressive_locking,
                    )
                    .await?;
                max_locked_with_conflict_ts = std::cmp::max(
                    max_locked_with_conflict_ts,
                    primary_result.max_locked_with_conflict_ts,
                );
                locked_keys_actual_for_update_ts
                    .extend(primary_result.locked_keys_actual_for_update_ts);
                let primary_pairs = primary_result.pairs;

                let mut secondary_request = new_pessimistic_lock_request(
                    secondary_locks.into_iter(),
                    primary_lock.clone(),
                    self.timestamp.clone(),
                    lock_ttl,
                    for_update_ts.clone(),
                    need_value,
                    is_first_lock,
                );
                apply_pessimistic_lock_mode(&mut secondary_request);
                apply_pessimistic_lock_wake_up_mode(&mut secondary_request);
                let secondary_pairs = match self
                    .execute_pessimistic_lock_request(
                        secondary_request,
                        for_update_ts.clone(),
                        lock_wait_timeout,
                        lock_wait_start,
                        mode,
                        track_aggressive_locking,
                    )
                    .await
                {
                    Ok(result) => {
                        max_locked_with_conflict_ts = std::cmp::max(
                            max_locked_with_conflict_ts,
                            result.max_locked_with_conflict_ts,
                        );
                        locked_keys_actual_for_update_ts
                            .extend(result.locked_keys_actual_for_update_ts);
                        result.pairs
                    }
                    Err(err) => {
                        let keep_locks_on_error = track_aggressive_locking
                            && !need_value
                            && self.aggressive_locking.is_some()
                            && is_write_conflict_error(&err);
                        if keep_locks_on_error {
                            let primary_changed = self
                                .aggressive_locking
                                .as_ref()
                                .map(|state| {
                                    state.primary_key.is_some()
                                        && state.last_primary_key.as_ref()
                                            != state.primary_key.as_ref()
                                })
                                .unwrap_or(false);
                            if primary_changed {
                                self.stop_auto_heartbeat();
                            }
                            self.buffer.primary_key_or(&primary_lock);
                            self.start_auto_heartbeat().await?;
                            if let Some(state) = self.aggressive_locking.as_mut() {
                                let expected_for_update_ts = for_update_ts.version();
                                for key in primary_request_keys.iter() {
                                    state
                                        .current_locked_keys
                                        .entry(key.clone())
                                        .or_insert(expected_for_update_ts);
                                    state.last_retry_unnecessary_locks.remove(key);
                                }
                            }
                            for key in primary_request_keys {
                                match mode {
                                    PessimisticLockMode::Exclusive => self.buffer.lock(key),
                                    PessimisticLockMode::Shared => self.buffer.lock_shared(key),
                                }
                            }
                            return Err(err);
                        }
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
            let mut request = new_pessimistic_lock_request(
                locks_for_request.clone().into_iter(),
                primary_lock.clone(),
                self.timestamp.clone(),
                lock_ttl,
                for_update_ts.clone(),
                need_value,
                is_first_lock,
            );
            apply_pessimistic_lock_mode(&mut request);
            apply_pessimistic_lock_wake_up_mode(&mut request);
            let result = self
                .execute_pessimistic_lock_request(
                    request,
                    for_update_ts.clone(),
                    lock_wait_timeout,
                    lock_wait_start,
                    mode,
                    track_aggressive_locking,
                )
                .await?;
            max_locked_with_conflict_ts = std::cmp::max(
                max_locked_with_conflict_ts,
                result.max_locked_with_conflict_ts,
            );
            locked_keys_actual_for_update_ts.extend(result.locked_keys_actual_for_update_ts);
            result.pairs
        };

        let primary_changed = if track_aggressive_locking && !need_value {
            self.aggressive_locking
                .as_ref()
                .map(|state| {
                    state.primary_key.is_some()
                        && state.last_primary_key.as_ref() != state.primary_key.as_ref()
                })
                .unwrap_or(false)
        } else {
            false
        };
        if primary_changed {
            self.stop_auto_heartbeat();
        }

        self.buffer.primary_key_or(&primary_lock);
        self.start_auto_heartbeat().await?;
        if track_aggressive_locking && !need_value {
            if let Some(state) = self.aggressive_locking.as_mut() {
                state.max_locked_with_conflict_ts = std::cmp::max(
                    state.max_locked_with_conflict_ts,
                    max_locked_with_conflict_ts,
                );
                let expected_for_update_ts = for_update_ts.version();
                for (key, _assertion) in &locks_for_request {
                    let actual_for_update_ts = locked_keys_actual_for_update_ts
                        .iter()
                        .find(|(locked_key, _)| locked_key == key)
                        .map(|(_, actual_ts)| *actual_ts)
                        .unwrap_or(expected_for_update_ts);
                    state
                        .current_locked_keys
                        .entry(key.clone())
                        .or_insert(actual_for_update_ts);
                    state.last_retry_unnecessary_locks.remove(key);
                }
            }
        }
        for (key, _assertion) in locks {
            match mode {
                PessimisticLockMode::Exclusive => self.buffer.lock(key),
                PessimisticLockMode::Shared => self.buffer.lock_shared(key),
            }
        }
        Ok(pairs)
    }

    async fn execute_pessimistic_lock_request(
        &mut self,
        mut request: kvrpcpb::PessimisticLockRequest,
        for_update_ts: Timestamp,
        lock_wait_timeout: LockWaitTimeout,
        lock_wait_start: Instant,
        mode: PessimisticLockMode,
        track_aggressive_locking: bool,
    ) -> Result<PessimisticLockRequestResult> {
        let need_value = request.return_values;
        fn collect_lock_errors(
            error: &Error,
            locks: &mut Vec<kvrpcpb::LockInfo>,
            skipped_recently_updated_locks: &mut bool,
        ) -> bool {
            const SKIP_RESOLVE_THRESHOLD_MS: u64 = 300;

            fn should_skip_lock(lock: &kvrpcpb::LockInfo) -> bool {
                let duration = lock.duration_to_last_update_ms;
                duration > 0 && duration < SKIP_RESOLVE_THRESHOLD_MS
            }

            fn extend_lock_infos(
                locks: &mut Vec<kvrpcpb::LockInfo>,
                lock: &kvrpcpb::LockInfo,
                skipped_recently_updated_locks: &mut bool,
            ) {
                if lock.shared_lock_infos.is_empty() {
                    if should_skip_lock(lock) {
                        *skipped_recently_updated_locks = true;
                        return;
                    }
                    locks.push(lock.clone());
                } else {
                    for shared_lock in &lock.shared_lock_infos {
                        if should_skip_lock(shared_lock) {
                            *skipped_recently_updated_locks = true;
                            continue;
                        }
                        locks.push(shared_lock.clone());
                    }
                }
            }

            match error {
                Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                    let mut has_non_lock_error = false;
                    for err in errors {
                        has_non_lock_error |=
                            collect_lock_errors(err, locks, skipped_recently_updated_locks);
                    }
                    has_non_lock_error
                }
                Error::ResolveLockError(live_locks) => {
                    for lock in live_locks {
                        extend_lock_infos(locks, lock, skipped_recently_updated_locks);
                    }
                    false
                }
                Error::KeyError(key_error) => {
                    if let Some(lock) = &key_error.locked {
                        extend_lock_infos(locks, lock, skipped_recently_updated_locks);
                        false
                    } else {
                        true
                    }
                }
                _ => true,
            }
        }

        fn extract_deadlock_error(error: &Error) -> Option<crate::DeadlockError> {
            match error {
                Error::Deadlock(deadlock) => Some(deadlock.clone()),
                Error::KeyError(key_error) => {
                    key_error.deadlock.clone().map(crate::DeadlockError::from)
                }
                Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                    errors.iter().find_map(extract_deadlock_error)
                }
                Error::PessimisticLockError { inner, .. } => extract_deadlock_error(inner),
                _ => None,
            }
        }

        fn contains_already_exist_error(error: &Error) -> bool {
            match error {
                Error::KeyError(key_error) => key_error.already_exist.is_some(),
                Error::MultipleKeyErrors(errors) | Error::ExtractedErrors(errors) => {
                    errors.iter().any(contains_already_exist_error)
                }
                Error::PessimisticLockError { inner, .. } => contains_already_exist_error(inner),
                _ => false,
            }
        }

        self.options.apply_write_context(&mut request.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(request.label());
                let ctx = request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }

        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();

        loop {
            request.wait_timeout = lock_wait_timeout.effective_wait_timeout_ms(lock_wait_start);

            let plan = PlanBuilder::new_with_rpc_interceptors(
                self.rpc.clone(),
                self.keyspace,
                request.clone(),
                self.rpc_interceptors.clone(),
            )
            .preserve_shard()
            .retry_multi_region_preserve_results(region_backoff.clone())
            .with_killed(killed.clone())
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

            let mut locked_keys_actual_for_update_ts = Vec::new();
            let mut max_locked_with_conflict_ts = 0u64;
            if request.wake_up_mode
                == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
            {
                for crate::request::ResponseWithShard(resp, mutations) in &success {
                    if resp.results.is_empty() {
                        return Err(Error::StringError(
                            "pessimistic lock response missing results for force lock request"
                                .to_owned(),
                        ));
                    }
                    if resp.results.len() != mutations.len() {
                        return Err(Error::StringError(format!(
                            "pessimistic lock response results length {} does not match mutations length {}",
                            resp.results.len(),
                            mutations.len(),
                        )));
                    }
                    for (mutation, result) in mutations.iter().zip(resp.results.iter()) {
                        let is_failed = match result.r#type {
                            x if x
                                == kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32 =>
                            {
                                false
                            }
                            x if x
                                == kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                                    as i32 =>
                            {
                                false
                            }
                            x if x
                                == kvrpcpb::PessimisticLockKeyResultType::LockResultFailed as i32 =>
                            {
                                true
                            }
                            other => {
                                return Err(Error::StringError(format!(
                                    "unknown pessimistic lock key result type: {other}",
                                )));
                            }
                        };
                        if is_failed {
                            return Err(Error::StringError(
                                "pessimistic lock force lock request failed".to_owned(),
                            ));
                        }

                        let locked_with_conflict_ts = result.locked_with_conflict_ts;
                        if locked_with_conflict_ts != 0
                            && locked_with_conflict_ts <= request.for_update_ts
                        {
                            return Err(Error::StringError(format!(
                                "pessimistic lock request to key {:?} returns locked_with_conflict_ts({}) not greater than requested for_update_ts({})",
                                mutation.key,
                                locked_with_conflict_ts,
                                request.for_update_ts,
                            )));
                        }
                        max_locked_with_conflict_ts =
                            std::cmp::max(max_locked_with_conflict_ts, locked_with_conflict_ts);
                        let actual_for_update_ts =
                            std::cmp::max(request.for_update_ts, locked_with_conflict_ts);
                        locked_keys_actual_for_update_ts
                            .push((Key::from(mutation.key.clone()), actual_for_update_ts));
                    }
                }
            }

            self.options.max_locked_with_conflict_ts = std::cmp::max(
                self.options.max_locked_with_conflict_ts,
                max_locked_with_conflict_ts,
            );

            if errors.is_empty() {
                let pairs = CollectPessimisticLock::new(need_value)
                    .merge(success.into_iter().map(Ok).collect())?;
                return Ok(PessimisticLockRequestResult {
                    pairs,
                    locked_keys_actual_for_update_ts,
                    max_locked_with_conflict_ts,
                });
            }

            let success_keys = success
                .into_iter()
                .flat_map(|crate::request::ResponseWithShard(_resp, mutations)| {
                    mutations
                        .into_iter()
                        .map(|mutation| Key::from(mutation.key))
                })
                .collect::<Vec<_>>();

            if errors.iter().any(contains_already_exist_error) {
                if !success_keys.is_empty() {
                    self.pessimistic_lock_rollback(
                        success_keys.iter().cloned(),
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await?;
                }
                return Err(Error::DuplicateKeyInsertion);
            }

            if let Some(deadlock) = errors.iter().find_map(extract_deadlock_error) {
                if !success_keys.is_empty() {
                    self.pessimistic_lock_rollback(
                        success_keys.iter().cloned(),
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await?;
                }
                return Err(Error::Deadlock(deadlock));
            }

            let mut locks = Vec::new();
            let mut skipped_recently_updated_locks = false;
            let mut first_non_lock_error = None;
            for (idx, err) in errors.iter().enumerate() {
                if collect_lock_errors(err, &mut locks, &mut skipped_recently_updated_locks)
                    && first_non_lock_error.is_none()
                {
                    first_non_lock_error = Some(idx);
                }
            }

            if let Some(idx) = first_non_lock_error {
                let err = errors.swap_remove(idx);
                let keep_locks_on_error = track_aggressive_locking
                    && !need_value
                    && self.aggressive_locking.is_some()
                    && is_write_conflict_error(&err);
                if keep_locks_on_error {
                    if !success_keys.is_empty() {
                        let primary_changed = self
                            .aggressive_locking
                            .as_ref()
                            .map(|state| {
                                state.primary_key.is_some()
                                    && state.last_primary_key.as_ref() != state.primary_key.as_ref()
                            })
                            .unwrap_or(false);
                        if primary_changed {
                            self.stop_auto_heartbeat();
                        }
                        self.buffer
                            .primary_key_or(&Key::from(request.primary_lock.clone()));
                        self.start_auto_heartbeat().await?;
                        if let Some(state) = self.aggressive_locking.as_mut() {
                            let expected_for_update_ts = for_update_ts.version();
                            for key in success_keys.iter() {
                                state
                                    .current_locked_keys
                                    .entry(key.clone())
                                    .or_insert(expected_for_update_ts);
                                state.last_retry_unnecessary_locks.remove(key);
                            }
                        }
                        for key in success_keys.iter().cloned() {
                            match mode {
                                PessimisticLockMode::Exclusive => self.buffer.lock(key),
                                PessimisticLockMode::Shared => self.buffer.lock_shared(key),
                            }
                        }
                    }
                    return Err(err);
                }

                if !success_keys.is_empty() {
                    self.pessimistic_lock_rollback(
                        success_keys.iter().cloned(),
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await?;
                }
                return Err(err);
            }
            if locks.is_empty() {
                if !success_keys.is_empty() {
                    self.pessimistic_lock_rollback(
                        success_keys.iter().cloned(),
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await?;
                }
                if skipped_recently_updated_locks {
                    if lock_wait_timeout.is_no_wait() {
                        return Err(Error::LockAcquireFailAndNoWaitSet);
                    }
                    if lock_wait_timeout.is_timed_out(lock_wait_start) {
                        return Err(Error::LockWaitTimeout);
                    }
                    continue;
                }
                return Err(errors.swap_remove(0));
            }

            if !success_keys.is_empty() {
                self.pessimistic_lock_rollback(
                    success_keys.iter().cloned(),
                    self.timestamp.clone(),
                    for_update_ts.clone(),
                )
                .await?;
            }

            let lock_resolver_rpc_context = self.lock_resolver_rpc_context();

            let resolve_result = super::resolve_locks_with_options(
                self.resolve_locks_ctx.clone(),
                locks,
                Timestamp::default(),
                self.rpc.clone(),
                self.keyspace,
                true,
                lock_backoff.clone(),
                killed.clone(),
                lock_resolver_rpc_context,
            )
            .await?;

            if resolve_result.live_locks.is_empty() {
                continue;
            }

            if lock_wait_timeout.is_no_wait() {
                return Err(Error::LockAcquireFailAndNoWaitSet);
            }
            if lock_wait_timeout.is_timed_out(lock_wait_start) {
                return Err(Error::LockWaitTimeout);
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

        let effective_for_update_ts = Timestamp::from_version(std::cmp::max(
            for_update_ts.version(),
            self.options.max_locked_with_conflict_ts,
        ));
        let req = new_pessimistic_rollback_request(
            keys.clone().into_iter(),
            start_version.clone(),
            effective_for_update_ts,
        );
        let mut req = req;
        self.options.apply_write_context(&mut req.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(req.label());
                let ctx = req.context.get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let plan = PlanBuilder::new_with_rpc_interceptors(
            self.rpc.clone(),
            self.keyspace,
            req,
            lock_resolver_rpc_context.rpc_interceptors.clone(),
        )
        .resolve_lock_in_context(
            self.resolve_locks_ctx.clone(),
            start_version,
            lock_backoff,
            killed.clone(),
            self.keyspace,
            lock_resolver_rpc_context,
        )
        .retry_multi_region(region_backoff)
        .with_killed(killed)
        .extract_error()
        .plan();
        plan.execute().await?;

        for key in keys {
            self.buffer.unlock(&key);
        }
        Ok(())
    }

    async fn cleanup_aggressive_locking_redundant_locks(&mut self) -> Result<()> {
        let keys_to_rollback = {
            let Some(state) = self.aggressive_locking.as_ref() else {
                return Ok(());
            };
            if state.last_retry_unnecessary_locks.is_empty() {
                return Ok(());
            }

            state
                .last_retry_unnecessary_locks
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        if keys_to_rollback.is_empty() {
            return Ok(());
        }

        let for_update_ts = match &self.options.kind {
            TransactionKind::Pessimistic(for_update_ts) => for_update_ts.clone(),
            TransactionKind::Optimistic => return Err(Error::InvalidTransactionType),
        };

        self.pessimistic_lock_rollback(
            keys_to_rollback.iter().cloned(),
            self.timestamp.clone(),
            for_update_ts,
        )
        .await?;
        if let Some(state) = self.aggressive_locking.as_mut() {
            for key in keys_to_rollback {
                state.last_retry_unnecessary_locks.remove(&key);
            }
        }
        Ok(())
    }

    fn stop_auto_heartbeat(&mut self) {
        self.heartbeat_generation
            .fetch_add(1, atomic::Ordering::SeqCst);
        self.is_heartbeat_started = false;
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

    async fn check_visibility(&self) -> Result<()> {
        self.gc_safe_point
            .check_visibility(self.timestamp.version())
            .await
    }

    fn is_pessimistic(&self) -> bool {
        matches!(self.options.kind, TransactionKind::Pessimistic(_))
    }

    fn lock_backoff(&self) -> Backoff {
        let weight = self.vars.backoff_weight_factor();
        self.options
            .retry_options
            .lock_backoff
            .clone()
            .with_base_delay_ms(self.vars.lock_fast_base_delay_ms())
            .scaled_max_attempts(weight)
    }

    fn region_backoff(&self) -> Backoff {
        self.options
            .retry_options
            .region_backoff
            .clone()
            .scaled_max_attempts(self.vars.backoff_weight_factor())
    }

    async fn start_auto_heartbeat(&mut self) -> Result<()> {
        debug!("starting auto_heartbeat");
        if !self.options.heartbeat_option.is_auto_heartbeat() || self.is_heartbeat_started {
            return Ok(());
        }

        let primary_key = self
            .pipelined
            .as_ref()
            .and_then(|state| state.primary_key.clone())
            .or_else(|| self.buffer.get_primary_key())
            .ok_or_else(|| {
                crate::internal_err!("auto heartbeat requested without a primary key")
            })?;

        let heartbeat_generation_id = self
            .heartbeat_generation
            .fetch_add(1, atomic::Ordering::SeqCst)
            .wrapping_add(1);
        self.is_heartbeat_started = true;

        let status = self.status.clone();
        let start_ts = self.timestamp.clone();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let heartbeat_generation = self.heartbeat_generation.clone();
        let rpc = self.rpc.clone();
        let rpc_interceptors = self.rpc_interceptors.clone();
        let broadcast_pipelined_txn_status = self.options.pipelined_txn.is_some();
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
                if let Some(killed) = killed.as_ref() {
                    if killed.load(atomic::Ordering::SeqCst) != 0 {
                        break;
                    }
                }
                if heartbeat_generation.load(atomic::Ordering::SeqCst) != heartbeat_generation_id {
                    break;
                }
                let request = new_heart_beat_request(
                    start_ts.clone(),
                    primary_key.clone(),
                    start_instant.elapsed().as_millis() as u64 + MAX_TTL,
                );
                let plan = PlanBuilder::new_with_rpc_interceptors(
                    rpc.clone(),
                    keyspace,
                    request,
                    rpc_interceptors.clone(),
                )
                .retry_multi_region(region_backoff.clone())
                .with_killed(killed.clone())
                .merge(CollectSingle)
                .plan();
                plan.execute().await?;

                if broadcast_pipelined_txn_status {
                    let rpc = rpc.clone();
                    let status = kvrpcpb::TxnStatus {
                        start_ts: start_ts.version(),
                        min_commit_ts: start_ts.version().saturating_add(1),
                        commit_ts: 0,
                        rolled_back: false,
                        is_completed: false,
                    };
                    tokio::spawn(async move {
                        if let Err(err) = rpc.broadcast_txn_status_to_all_stores(vec![status]).await
                        {
                            debug!("broadcast_txn_status_to_all_stores failed: {err}");
                        }
                    });
                }
            }
            Ok::<(), Error>(())
        };

        tokio::spawn(async {
            if let Err(err) = heartbeat_task.await {
                log::error!("Error: While sending heartbeat. {}", err);
            }
        });
        Ok(())
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
/// Default maximum time allowed for PD TSO to catch up to the commit-wait target timestamp.
///
/// This matches client-go `KVTxn.commitWaitUntilTSOTimeout` default.
const DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT: Duration = Duration::from_secs(1);
/// Default safe window for async commit / 1PC max-commit-ts calculation.
///
/// This matches the default in client-go.
const DEFAULT_ASYNC_COMMIT_SAFE_WINDOW: Duration = Duration::from_secs(2);
/// TiKV recommends each RPC packet should be less than around 1MB. We keep KV size of
/// each request below 16KB.
pub const TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;
const TTL_FACTOR: f64 = 6000.0;
const PIPELINED_REQUEST_SOURCE: &str = "pipelined_flush";

const PIPELINED_MIN_FLUSH_KEYS: u64 = 10_000;
const PIPELINED_MIN_FLUSH_MEM_SIZE: u64 = 16 * 1024 * 1024; // 16MB
const PIPELINED_FORCE_FLUSH_MEM_SIZE_THRESHOLD: u64 = 128 * 1024 * 1024; // 128MB
const PIPELINED_FLUSH_EWMA_AGE: f64 = 10.0;
const PIPELINED_BROADCAST_GRACE_PERIOD: Duration = Duration::from_secs(5);

fn pipelined_min_flush_keys() -> u64 {
    fail_point!("pipelined_memdb_min_flush_keys", |val| {
        val.map(|val| val.parse::<u64>().unwrap())
            .unwrap_or(PIPELINED_MIN_FLUSH_KEYS)
    });
    PIPELINED_MIN_FLUSH_KEYS
}

fn pipelined_min_flush_mem_size() -> u64 {
    fail_point!("pipelined_memdb_min_flush_size", |val| {
        val.map(|val| val.parse::<u64>().unwrap())
            .unwrap_or(PIPELINED_MIN_FLUSH_MEM_SIZE)
    });
    PIPELINED_MIN_FLUSH_MEM_SIZE
}

fn pipelined_force_flush_mem_size_threshold() -> u64 {
    fail_point!("pipelined_memdb_force_flush_size_threshold", |val| {
        val.map(|val| val.parse::<u64>().unwrap())
            .unwrap_or(PIPELINED_FORCE_FLUSH_MEM_SIZE_THRESHOLD)
    });
    PIPELINED_FORCE_FLUSH_MEM_SIZE_THRESHOLD
}

fn pipelined_broadcast_grace_period() -> Duration {
    fail_point!("pipelined_broadcast_grace_period_ms", |val| {
        val.map(|val| Duration::from_millis(val.parse::<u64>().unwrap()))
            .unwrap_or(PIPELINED_BROADCAST_GRACE_PERIOD)
    });
    PIPELINED_BROADCAST_GRACE_PERIOD
}

#[derive(Debug, Default)]
struct FlushDurationEwma {
    value_ms: f64,
}

impl FlushDurationEwma {
    fn value_ms(&self) -> f64 {
        self.value_ms
    }

    fn observe(&mut self, sample_ms: f64) {
        if self.value_ms == 0.0 {
            self.value_ms = sample_ms;
        } else {
            self.value_ms = (self.value_ms * (PIPELINED_FLUSH_EWMA_AGE - 1.0) + sample_ms)
                / PIPELINED_FLUSH_EWMA_AGE;
        }
    }
}

#[derive(Debug)]
struct PipelinedState {
    primary_key: Option<Key>,
    generation: u64,
    flushing: Option<JoinHandle<Result<()>>>,
    flushing_puts: Option<Arc<HashMap<Key, Value>>>,
    flushed_deletes: Arc<Mutex<HashSet<Key>>>,
    flushed_range_start: Option<Key>,
    flushed_range_end: Option<Key>,
    flush_duration_ewma: Arc<Mutex<FlushDurationEwma>>,
}

impl PipelinedState {
    fn new() -> PipelinedState {
        PipelinedState {
            primary_key: None,
            generation: 0,
            flushing: None,
            flushing_puts: None,
            flushed_deletes: Arc::new(Mutex::new(HashSet::new())),
            flushed_range_start: None,
            flushed_range_end: None,
            flush_duration_ewma: Arc::new(Mutex::new(FlushDurationEwma::default())),
        }
    }

    fn has_flushed_range(&self) -> bool {
        self.flushed_range_start.is_some() && self.flushed_range_end.is_some()
    }

    fn primary_key_or_init(&mut self, mutations: &[kvrpcpb::Mutation]) -> Result<Key> {
        if let Some(primary_key) = self.primary_key.clone() {
            return Ok(primary_key);
        }

        let primary_key = mutations
            .iter()
            .filter(|m| m.op != kvrpcpb::Op::CheckNotExists as i32)
            .min_by(|a, b| a.key.cmp(&b.key))
            .map(|m| Key::from(m.key.clone()))
            .ok_or_else(|| {
                Error::StringError(
                    "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
                )
            })?;
        self.primary_key = Some(primary_key.clone());
        Ok(primary_key)
    }

    fn is_flushing(&self) -> bool {
        self.flushing.is_some()
    }

    fn should_flush(&self, force: bool, mutation_count: usize, write_size: u64) -> bool {
        if force {
            return mutation_count > 0;
        }

        let min_keys = pipelined_min_flush_keys();
        let min_size = pipelined_min_flush_mem_size();
        let force_size = pipelined_force_flush_mem_size_threshold();

        if write_size < min_size
            || (u64::try_from(mutation_count).unwrap_or(u64::MAX) < min_keys
                && write_size < force_size)
        {
            return false;
        }

        if self.is_flushing() && write_size < force_size {
            return false;
        }

        true
    }

    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.generation
    }

    fn record_flushing_puts(&mut self, mutations: &[kvrpcpb::Mutation]) {
        let puts = mutations
            .iter()
            .filter_map(|m| match m.op {
                op if op == kvrpcpb::Op::Put as i32 || op == kvrpcpb::Op::Insert as i32 => {
                    Some((Key::from(m.key.clone()), m.value.clone()))
                }
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        self.flushing_puts = if puts.is_empty() {
            None
        } else {
            Some(Arc::new(puts))
        };
    }

    fn record_flushed_mutations(&mut self, mutations: &[kvrpcpb::Mutation]) {
        let mut flushed_deletes = match self.flushed_deletes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut range_start: Option<Key> = None;
        let mut range_end_inclusive: Option<Key> = None;

        for m in mutations {
            match m.op {
                op if op == kvrpcpb::Op::Del as i32 || op == kvrpcpb::Op::CheckNotExists as i32 => {
                    flushed_deletes.insert(Key::from(m.key.clone()));
                }
                op if op == kvrpcpb::Op::Put as i32 || op == kvrpcpb::Op::Insert as i32 => {
                    flushed_deletes.remove(&Key::from(m.key.clone()));
                }
                _ => {}
            }

            if m.op == kvrpcpb::Op::CheckNotExists as i32 {
                continue;
            }

            let key = Key::from(m.key.clone());
            if range_start.as_ref().map_or(true, |start| &key < start) {
                range_start = Some(key.clone());
            }
            if range_end_inclusive.as_ref().map_or(true, |end| &key > end) {
                range_end_inclusive = Some(key);
            }
        }

        let Some(range_start) = range_start else {
            return;
        };
        let Some(range_end_inclusive) = range_end_inclusive else {
            return;
        };
        let range_end = range_end_inclusive.next_key();

        self.flushed_range_start = match self.flushed_range_start.take() {
            Some(existing) => Some(existing.min(range_start)),
            None => Some(range_start),
        };
        self.flushed_range_end = match self.flushed_range_end.take() {
            Some(existing) => Some(existing.max(range_end)),
            None => Some(range_end),
        };
    }

    async fn flush_wait(&mut self) -> Result<()> {
        let Some(handle) = self.flushing.take() else {
            return Ok(());
        };
        let result = match handle.await {
            Ok(result) => result,
            Err(err) => Err(Error::InternalError {
                message: format!("pipelined flush task failed: {err}"),
            }),
        };
        self.flushing_puts = None;
        result?;
        Ok(())
    }
}

async fn throttle_pipelined_flush(ewma: Arc<Mutex<FlushDurationEwma>>, write_throttle_ratio: f64) {
    if write_throttle_ratio == 0.0 {
        return;
    }

    let expected_flush_ms = {
        let ewma = match ewma.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        ewma.value_ms()
    };
    if expected_flush_ms == 0.0 {
        return;
    }

    let sleep_ms =
        (write_throttle_ratio / (1.0 - write_throttle_ratio) * expected_flush_ms).round() as u64;
    if sleep_ms == 0 {
        return;
    }

    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
}

#[allow(clippy::too_many_arguments)]
fn spawn_pipelined_resolve_locks_and_broadcast_completion<PdC: PdClient + 'static>(
    rpc: Arc<PdC>,
    keyspace: Keyspace,
    rpc_interceptors: RpcInterceptors,
    options: TransactionOptions,
    resource_group_tagger: Option<ResourceGroupTagger>,
    start_ts: u64,
    commit_ts: u64,
    rolled_back: bool,
    range_start: Key,
    range_end: Key,
    region_backoff: Backoff,
    killed: Option<Arc<atomic::AtomicU32>>,
    resolve_concurrency: usize,
) {
    let resolve_status = if rolled_back { "rollback" } else { "commit" };

    let mut resolve_request = ResolveLockRangeRequest::new(
        super::requests::new_resolve_lock_request(start_ts, commit_ts, false),
        range_start,
        range_end,
    );
    options.apply_write_context(&mut resolve_request.inner_mut().context);
    if let Some(ctx) = resolve_request.inner_mut().context.as_mut() {
        ctx.request_source = PIPELINED_REQUEST_SOURCE.to_owned();
    }
    if options.resource_group_tag.is_none() {
        if let Some(tagger) = resource_group_tagger.as_ref() {
            let tag = (tagger)(resolve_request.label());
            let ctx = resolve_request
                .inner_mut()
                .context
                .get_or_insert_with(kvrpcpb::Context::default);
            ctx.resource_group_tag = tag;
        }
    }

    tokio::spawn(async move {
        let plan = PlanBuilder::new_with_rpc_interceptors(
            rpc.clone(),
            keyspace,
            resolve_request,
            rpc_interceptors,
        )
        .retry_multi_region_with_concurrency(region_backoff, resolve_concurrency)
        .with_killed(killed)
        .merge(CollectError)
        .extract_error()
        .plan();
        if let Err(err) = plan.execute().await {
            log::warn!(
                "Failed to resolve pipelined locks ({resolve_status}): {}",
                err
            );
            return;
        }

        tokio::time::sleep(pipelined_broadcast_grace_period()).await;

        let status = kvrpcpb::TxnStatus {
            start_ts,
            min_commit_ts: 0,
            commit_ts,
            rolled_back,
            is_completed: true,
        };
        if let Err(err) = rpc.broadcast_txn_status_to_all_stores(vec![status]).await {
            debug!("broadcast_txn_status_to_all_stores failed: {err}");
        }
    });
}

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
    /// The maximum `locked_with_conflict_ts` returned by TiKV for this transaction.
    ///
    /// This is used to choose a safe `for_update_ts` when rolling back pessimistic locks.
    max_locked_with_conflict_ts: u64,
    /// The geographical scope of the transaction.
    ///
    /// When set, PD timestamps used by the transaction (for example, `for_update_ts`, commit-ts,
    /// and async/1PC `min_commit_ts` seeding) will be requested with this as PD
    /// `TsoRequest.dc_location` (client-go `txnScope` / "local TSO").
    ///
    /// When `None`, the transaction uses the global TSO allocator.
    txn_scope: Option<String>,
    /// Read from replicas other than the leader (read-only snapshots only).
    replica_read: ReplicaReadType,
    /// Filter replica reads to stores with the given ids (read-only snapshots only).
    match_store_ids: Arc<Vec<u64>>,
    /// Filter replica reads to stores matching the given labels (read-only snapshots only).
    match_store_labels: Arc<Vec<StoreLabel>>,
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
    /// How strict to enforce mutation assertions during prewrite/flush requests.
    ///
    /// This maps to client-go `KVTxn.SetAssertionLevel`.
    assertion_level: AssertionLevel,
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
    /// Specifies the policy when prewrite encounters locks.
    ///
    /// This maps to client-go `KVTxn.SetPrewriteEncounterLockPolicy`.
    prewrite_encounter_lock_policy: PrewriteEncounterLockPolicy,
    /// Options for pipelined DML transactions.
    pipelined_txn: Option<PipelinedTxnOptions>,
    /// Lock wait timeout for pessimistic lock requests (`kvrpcpb::PessimisticLockRequest.wait_timeout`).
    ///
    /// Only effective for pessimistic transactions.
    lock_wait_timeout: LockWaitTimeout,
    /// What to do if the transaction is dropped without an attempt to commit or rollback
    check_level: CheckLevel,
    #[doc(hidden)]
    heartbeat_option: HeartbeatOption,
}

/// Options for configuring pipelined DML transactions.
///
/// This maps to client-go `TxnOptions.PipelinedTxn`.
///
/// When enabled, buffered mutations are automatically flushed to TiKV in the background once the
/// in-memory mutation buffer grows large enough (client-go parity). You can also trigger a flush
/// explicitly via [`Transaction::flush`] (asynchronous) and wait for completion via
/// [`Transaction::flush_wait`].
///
/// Reads (`get`/`batch_get`) remain usable while a flush is in-flight: they first consult the
/// in-memory flushing buffer, then use `BufferBatchGet` to read the transaction's flushed locks
/// (client-go `BatchGetBufferTier`), and fall back to normal snapshot reads for missing keys.
///
/// Range scans (`scan*`) are not supported for pipelined transactions (client-go parity:
/// `PipelinedMemDB` does not support iterators).
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct PipelinedTxnOptions {
    flush_concurrency: usize,
    resolve_lock_concurrency: usize,
    /// Write throttle ratio in `[0, 1)`.
    ///
    /// - `0.0` means no throttle.
    /// - Values closer to `1.0` yield more throttling.
    ///
    /// The implementation sleeps before flush requests based on an EWMA of recent flush duration so
    /// that `T_sleep / (T_sleep + T_flush) ≈ write_throttle_ratio` (client-go parity).
    write_throttle_ratio: f64,
}

impl PipelinedTxnOptions {
    /// Default pipelined flush concurrency.
    ///
    /// This matches client-go `defaultPipelinedFlushConcurrency`.
    pub const DEFAULT_FLUSH_CONCURRENCY: usize = 128;

    /// Default pipelined resolve-lock concurrency.
    ///
    /// This matches client-go `defaultPipelinedResolveLockConcurrency`.
    pub const DEFAULT_RESOLVE_LOCK_CONCURRENCY: usize = 8;

    /// Default pipelined write throttle ratio.
    ///
    /// This matches client-go `defaultPipelinedWriteThrottleRatio`.
    pub const DEFAULT_WRITE_THROTTLE_RATIO: f64 = 0.0;

    /// Create pipelined transaction options.
    ///
    /// Returns an error if any parameter is invalid.
    pub fn new(
        flush_concurrency: usize,
        resolve_lock_concurrency: usize,
        write_throttle_ratio: f64,
    ) -> Result<PipelinedTxnOptions> {
        if flush_concurrency == 0 {
            return Err(Error::StringError(
                "pipelined txn flush concurrency should be greater than 0".to_owned(),
            ));
        }
        if resolve_lock_concurrency == 0 {
            return Err(Error::StringError(
                "pipelined txn resolve lock concurrency should be greater than 0".to_owned(),
            ));
        }
        if !(0.0..1.0).contains(&write_throttle_ratio) {
            return Err(Error::StringError(format!(
                "invalid write throttle ratio: {write_throttle_ratio}"
            )));
        }
        Ok(PipelinedTxnOptions {
            flush_concurrency,
            resolve_lock_concurrency,
            write_throttle_ratio,
        })
    }

    pub fn flush_concurrency(&self) -> usize {
        self.flush_concurrency
    }

    pub fn resolve_lock_concurrency(&self) -> usize {
        self.resolve_lock_concurrency
    }

    pub fn write_throttle_ratio(&self) -> f64 {
        self.write_throttle_ratio
    }
}

impl Default for PipelinedTxnOptions {
    fn default() -> Self {
        Self {
            flush_concurrency: Self::DEFAULT_FLUSH_CONCURRENCY,
            resolve_lock_concurrency: Self::DEFAULT_RESOLVE_LOCK_CONCURRENCY,
            write_throttle_ratio: Self::DEFAULT_WRITE_THROTTLE_RATIO,
        }
    }
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
            max_locked_with_conflict_ts: 0,
            txn_scope: None,
            replica_read: ReplicaReadType::Leader,
            match_store_ids: Arc::new(Vec::new()),
            match_store_labels: Arc::new(Vec::new()),
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
            assertion_level: AssertionLevel::Off,
            try_one_pc: false,
            async_commit: false,
            causal_consistency: false,
            read_only: false,
            retry_options: RetryOptions::default_optimistic(),
            prewrite_encounter_lock_policy: PrewriteEncounterLockPolicy::TryResolve,
            pipelined_txn: None,
            lock_wait_timeout: LockWaitTimeout::Default,
            check_level: CheckLevel::Panic,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Default options for a pessimistic transaction.
    pub fn new_pessimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Pessimistic(Timestamp::from_version(0)),
            max_locked_with_conflict_ts: 0,
            txn_scope: None,
            replica_read: ReplicaReadType::Leader,
            match_store_ids: Arc::new(Vec::new()),
            match_store_labels: Arc::new(Vec::new()),
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
            assertion_level: AssertionLevel::Off,
            try_one_pc: false,
            async_commit: false,
            causal_consistency: false,
            read_only: false,
            retry_options: RetryOptions::default_pessimistic(),
            prewrite_encounter_lock_policy: PrewriteEncounterLockPolicy::TryResolve,
            pipelined_txn: None,
            lock_wait_timeout: LockWaitTimeout::AlwaysWait,
            check_level: CheckLevel::Panic,
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Set the geographical scope of the transaction.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator.
    /// Otherwise `txn_scope` is passed through as PD `dc_location` to request a local TSO.
    #[must_use]
    pub fn txn_scope(mut self, txn_scope: impl AsRef<str>) -> TransactionOptions {
        let txn_scope = txn_scope.as_ref();
        self.txn_scope = if txn_scope.is_empty() || txn_scope == "global" {
            None
        } else {
            Some(txn_scope.to_owned())
        };
        self
    }

    pub(crate) fn is_global_txn_scope(&self) -> bool {
        self.txn_scope.is_none()
    }

    pub(crate) fn txn_scope_as_deref(&self) -> Option<&str> {
        self.txn_scope.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.pipelined_txn.is_some() && (self.try_one_pc || self.async_commit) {
            return Err(Error::StringError(
                "pipelined txn does not support async-commit or 1pc".to_owned(),
            ));
        }
        Ok(())
    }

    /// Enable a pipelined DML transaction with default parameters.
    ///
    /// This maps to client-go `tikv.WithDefaultPipelinedTxn`.
    ///
    /// Note: pipelined transactions do not support async-commit or 1PC.
    #[must_use]
    pub fn pipelined(mut self) -> TransactionOptions {
        self.pipelined_txn = Some(PipelinedTxnOptions::default());
        self
    }

    /// Enable a pipelined DML transaction with custom parameters.
    ///
    /// Note: pipelined transactions do not support async-commit or 1PC.
    #[must_use]
    pub fn pipelined_txn(mut self, options: PipelinedTxnOptions) -> TransactionOptions {
        self.pipelined_txn = Some(options);
        self
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

    /// Set how strict to enforce mutation assertions during prewrite/flush.
    ///
    /// This maps to client-go `KVTxn.SetAssertionLevel`.
    #[must_use]
    pub fn assertion_level(mut self, assertion_level: AssertionLevel) -> TransactionOptions {
        self.assertion_level = assertion_level;
        self
    }

    /// Set the policy for handling locks encountered during prewrite.
    ///
    /// When set to [`PrewriteEncounterLockPolicy::NoResolve`], prewrite returns lock errors directly
    /// without attempting lock resolution.
    ///
    /// This maps to client-go `KVTxn.SetPrewriteEncounterLockPolicy`.
    #[must_use]
    pub fn prewrite_encounter_lock_policy(
        mut self,
        policy: PrewriteEncounterLockPolicy,
    ) -> TransactionOptions {
        self.prewrite_encounter_lock_policy = policy;
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
    ///
    /// This option writes to `kvrpcpb::Context.request_source`.
    ///
    /// For client-go compatible formatting (internal/external prefixes and optional explicit type),
    /// use [`RequestSource`](crate::RequestSource).
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

    /// Set labels to filter target stores for replica reads.
    ///
    /// This maps to client-go `KVSnapshot.SetMatchStoreLabels`.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn match_store_labels(
        mut self,
        labels: impl IntoIterator<Item = StoreLabel>,
    ) -> TransactionOptions {
        self.match_store_labels = Arc::new(labels.into_iter().collect());
        self
    }

    /// Set store ids to filter target stores for replica reads.
    ///
    /// This maps to client-go `tikv.WithMatchStores` / `locate.WithMatchStores`.
    ///
    /// This option is only effective for read-only snapshots created via
    /// [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    #[must_use]
    pub fn match_store_ids(
        mut self,
        store_ids: impl IntoIterator<Item = u64>,
    ) -> TransactionOptions {
        self.match_store_ids = Arc::new(store_ids.into_iter().collect());
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

    fn lock_resolver_rpc_context(
        &self,
        resource_group_tagger: Option<ResourceGroupTagger>,
    ) -> LockResolverRpcContext {
        let mut context = None;
        self.apply_write_context(&mut context);
        LockResolverRpcContext {
            context,
            resource_group_tag_set: self.resource_group_tag.is_some(),
            resource_group_tagger,
            resolve_lock_detail: None,
            rpc_interceptors: Default::default(),
            meet_lock_callback: None,
        }
    }

    fn push_for_update_ts(&mut self, for_update_ts: Timestamp) {
        let old_version = match &self.kind {
            TransactionKind::Optimistic => {
                debug_assert!(
                    false,
                    "push_for_update_ts called on optimistic transaction options"
                );
                return;
            }
            TransactionKind::Pessimistic(old_for_update_ts) => old_for_update_ts.version(),
        };

        let max_version = std::cmp::max(old_version, for_update_ts.version());
        self.kind = TransactionKind::Pessimistic(Timestamp::from_version(max_version));
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
    #[new(default)]
    resolve_lock_detail: Option<Arc<ResolveLockDetailCollector>>,
    options: TransactionOptions,
    #[new(default)]
    for_update_ts_constraints: HashMap<Key, u64>,
    #[new(default)]
    rpc_interceptors: RpcInterceptors,
    #[new(default)]
    vars: Variables,
    #[new(default)]
    resource_group_tagger: Option<ResourceGroupTagger>,
    keyspace: Keyspace,
    #[new(default)]
    undetermined: bool,
    write_size: u64,
    start_instant: Instant,
    #[new(default)]
    schema_ver: Option<i64>,
    #[new(default)]
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
    #[new(default)]
    commit_wait_until_tso: u64,
    #[new(value = "DEFAULT_COMMIT_WAIT_UNTIL_TSO_TIMEOUT")]
    commit_wait_until_tso_timeout: Duration,
    #[new(default)]
    commit_ts_upper_bound_check: Option<CommitTsUpperBoundCheck>,
    #[new(default)]
    pipelined_generation: u64,
    #[new(default)]
    pipelined_range_start: Option<Key>,
    #[new(default)]
    pipelined_range_end: Option<Key>,
    #[new(default)]
    pipelined_flush_duration_ewma: Option<Arc<Mutex<FlushDurationEwma>>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct CommitModeInfo {
    has_tried_async_commit: bool,
    has_tried_one_pc: bool,
    is_async_commit: bool,
    is_one_pc: bool,
}

impl CommitModeInfo {
    fn txn_commit_mode(&self) -> &'static str {
        if self.is_one_pc {
            "1pc"
        } else if self.is_async_commit {
            "async_commit"
        } else {
            "2pc"
        }
    }

    fn async_commit_fallback(&self) -> bool {
        self.has_tried_async_commit && !self.is_async_commit
    }

    fn one_pc_fallback(&self) -> bool {
        self.has_tried_one_pc && !self.is_one_pc
    }
}

impl<PdC: PdClient> Committer<PdC> {
    fn lock_backoff(&self) -> Backoff {
        let weight = self.vars.backoff_weight_factor();
        self.options
            .retry_options
            .lock_backoff
            .clone()
            .with_base_delay_ms(self.vars.lock_fast_base_delay_ms())
            .scaled_max_attempts(weight)
    }

    fn region_backoff(&self) -> Backoff {
        self.options
            .retry_options
            .region_backoff
            .clone()
            .scaled_max_attempts(self.vars.backoff_weight_factor())
    }

    fn lock_resolver_rpc_context(&self) -> LockResolverRpcContext {
        let mut ctx = self
            .options
            .lock_resolver_rpc_context(self.resource_group_tagger.clone());
        ctx.rpc_interceptors = self.rpc_interceptors.clone();
        ctx.resolve_lock_detail = self.resolve_lock_detail.clone();
        ctx
    }

    #[cfg(test)]
    async fn commit(self) -> Result<Option<Timestamp>> {
        let (res, _mode_info) = self.commit_with_mode_info().await;
        res
    }

    async fn commit_with_mode_info(mut self) -> (Result<Option<Timestamp>>, CommitModeInfo) {
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
                None => return (Ok(None), CommitModeInfo::default()),
            };
        }

        if self.options.pipelined_txn.is_some() {
            let res = self.commit_pipelined().await;
            return (res, CommitModeInfo::default());
        }

        // Match client-go: async-commit / 1PC are disabled for local transactions and when a
        // commit-ts upper bound checker is configured.
        if !self.options.is_global_txn_scope() || self.commit_ts_upper_bound_check.is_some() {
            self.options.try_one_pc = false;
            self.options.async_commit = false;
        }

        // Keep parity with client-go by reporting fallbacks only when we actually tried to use the
        // commit mode (not when it is disabled by configuration).
        let has_tried_async_commit = self.options.async_commit;
        let has_tried_one_pc = self.options.try_one_pc;

        let min_commit_ts = match self.prewrite().await {
            Ok(min_commit_ts) => min_commit_ts,
            Err(err) => {
                let info = CommitModeInfo {
                    has_tried_async_commit,
                    has_tried_one_pc,
                    is_async_commit: self.options.async_commit,
                    is_one_pc: self.options.try_one_pc,
                };
                return (Err(err), info);
            }
        };

        let _info_after_prewrite = CommitModeInfo {
            has_tried_async_commit,
            has_tried_one_pc,
            is_async_commit: self.options.async_commit,
            is_one_pc: self.options.try_one_pc,
        };
        fail_point!("after-prewrite", |_| {
            (
                Err(Error::StringError(
                    "failpoint: after-prewrite return error".to_owned(),
                )),
                _info_after_prewrite,
            )
        });

        // If we didn't use 1pc, prewrite will set `try_one_pc` to false.
        if self.options.try_one_pc {
            let info = CommitModeInfo {
                has_tried_async_commit,
                has_tried_one_pc,
                is_async_commit: self.options.async_commit,
                is_one_pc: self.options.try_one_pc,
            };
            return (Ok(min_commit_ts), info);
        }

        let commit_ts = if self.options.async_commit {
            match min_commit_ts {
                Some(ts) => ts,
                None => {
                    let err = Error::StringError(
                        "invalid min_commit_ts after async-commit prewrite".to_owned(),
                    );
                    let info = CommitModeInfo {
                        has_tried_async_commit,
                        has_tried_one_pc,
                        is_async_commit: self.options.async_commit,
                        is_one_pc: self.options.try_one_pc,
                    };
                    return (Err(err), info);
                }
            }
        } else {
            match self.commit_primary_with_retry().await {
                Ok(commit_ts) => commit_ts,
                Err(e) => {
                    let err = if self.undetermined {
                        Error::UndeterminedError(Box::new(e))
                    } else {
                        e
                    };
                    let info = CommitModeInfo {
                        has_tried_async_commit,
                        has_tried_one_pc,
                        is_async_commit: self.options.async_commit,
                        is_one_pc: self.options.try_one_pc,
                    };
                    return (Err(err), info);
                }
            }
        };

        let info = CommitModeInfo {
            has_tried_async_commit,
            has_tried_one_pc,
            is_async_commit: self.options.async_commit,
            is_one_pc: self.options.try_one_pc,
        };
        tokio::spawn(self.commit_secondary(commit_ts.clone()).map(|res| {
            if let Err(e) = res {
                log::warn!("Failed to commit secondary keys: {}", e);
            }
        }));
        (Ok(Some(commit_ts)), info)
    }

    async fn commit_pipelined(mut self) -> Result<Option<Timestamp>> {
        debug!("committing (pipelined)");

        let Some(pipelined) = self.options.pipelined_txn else {
            return Err(Error::InternalError {
                message: "commit_pipelined called without pipelined options".to_owned(),
            });
        };

        if self.options.try_one_pc || self.options.async_commit {
            return Err(Error::StringError(
                "pipelined txn does not support async-commit or 1pc".to_owned(),
            ));
        }

        // Match client-go: pipelined flush requires a primary key.
        let primary_key = self.primary_key.clone().ok_or_else(|| {
            Error::StringError(
                "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
            )
        })?;

        let mut range_start = self.pipelined_range_start.clone();
        let mut range_end = self.pipelined_range_end.clone();

        let mut pending_range_start: Option<Key> = None;
        let mut pending_range_end_inclusive: Option<Key> = None;
        for m in &self.mutations {
            if m.op == kvrpcpb::Op::CheckNotExists as i32 {
                continue;
            }
            let key = Key::from(m.key.clone());
            if pending_range_start
                .as_ref()
                .map_or(true, |start| &key < start)
            {
                pending_range_start = Some(key.clone());
            }
            if pending_range_end_inclusive
                .as_ref()
                .map_or(true, |end| &key > end)
            {
                pending_range_end_inclusive = Some(key);
            }
        }
        let pending_range_end = pending_range_end_inclusive.map(Key::next_key);

        if let Some(pending_range_start) = pending_range_start {
            range_start = match range_start.take() {
                Some(existing) => Some(existing.min(pending_range_start)),
                None => Some(pending_range_start),
            };
        }
        if let Some(pending_range_end) = pending_range_end {
            range_end = match range_end.take() {
                Some(existing) => Some(existing.max(pending_range_end)),
                None => Some(pending_range_end),
            };
        }

        let (range_start, range_end) = match (range_start, range_end) {
            (Some(range_start), Some(range_end)) => (range_start, range_end),
            _ => {
                return Err(Error::StringError(
                    "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
                ))
            }
        };

        if !self.mutations.is_empty() {
            let generation = self.pipelined_generation.saturating_add(1);
            let pipelined_ewma = self.pipelined_flush_duration_ewma.clone();
            if let Some(ewma) = pipelined_ewma.as_ref() {
                throttle_pipelined_flush(ewma.clone(), pipelined.write_throttle_ratio()).await;
            }

            let mutations = std::mem::take(&mut self.mutations);
            let mut flush_request = new_flush_request(
                mutations,
                primary_key.clone(),
                self.start_version.clone(),
                self.start_version.version().saturating_add(1),
                generation,
                MAX_TTL,
                self.options.assertion_level.into(),
            );
            self.options.apply_write_context(&mut flush_request.context);
            if let Some(ctx) = flush_request.context.as_mut() {
                ctx.request_source = PIPELINED_REQUEST_SOURCE.to_owned();
            }
            if self.options.resource_group_tag.is_none() {
                if let Some(tagger) = self.resource_group_tagger.as_ref() {
                    let tag = (tagger)(flush_request.label());
                    let ctx = flush_request
                        .context
                        .get_or_insert_with(kvrpcpb::Context::default);
                    ctx.resource_group_tag = tag;
                }
            }

            let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
            let flush_lock_backoff = self.lock_backoff();
            let flush_region_backoff = self.region_backoff();
            let flush_killed = self.vars.killed.clone();
            let flush_plan = PlanBuilder::new_with_rpc_interceptors(
                self.rpc.clone(),
                self.keyspace,
                flush_request,
                self.rpc_interceptors.clone(),
            )
            .resolve_lock_in_context(
                self.resolve_locks_ctx.clone(),
                self.start_version.clone(),
                flush_lock_backoff,
                flush_killed.clone(),
                self.keyspace,
                lock_resolver_rpc_context,
            )
            .retry_multi_region_with_concurrency(
                flush_region_backoff,
                pipelined.flush_concurrency(),
            )
            .with_killed(flush_killed)
            .merge(CollectError)
            .extract_error()
            .plan();
            let start = Instant::now();
            let flush_result = flush_plan.execute().await;
            if let Some(ewma) = pipelined_ewma.as_ref() {
                let sample_ms = start.elapsed().as_millis() as f64;
                let mut ewma = match ewma.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                ewma.observe(sample_ms);
            }
            if let Err(err) = flush_result {
                let start_ts = self.start_version.version();
                let rpc = self.rpc.clone();
                let status = kvrpcpb::TxnStatus {
                    start_ts,
                    min_commit_ts: start_ts.saturating_add(1),
                    commit_ts: 0,
                    rolled_back: true,
                    is_completed: false,
                };
                tokio::spawn(async move {
                    if let Err(err) = rpc.broadcast_txn_status_to_all_stores(vec![status]).await {
                        debug!("broadcast_txn_status_to_all_stores failed: {err}");
                    }
                });

                spawn_pipelined_resolve_locks_and_broadcast_completion(
                    self.rpc.clone(),
                    self.keyspace,
                    self.rpc_interceptors.clone(),
                    self.options.clone(),
                    self.resource_group_tagger.clone(),
                    start_ts,
                    0,
                    true,
                    range_start.clone(),
                    range_end.clone(),
                    self.region_backoff(),
                    self.vars.killed.clone(),
                    pipelined.resolve_lock_concurrency(),
                );
                return Err(err);
            }
        }

        let commit_ts = match self.commit_primary_with_retry().await {
            Ok(commit_ts) => commit_ts,
            Err(e) => {
                if self.undetermined {
                    return Err(Error::UndeterminedError(Box::new(e)));
                }

                let start_ts = self.start_version.version();
                let rpc = self.rpc.clone();
                let status = kvrpcpb::TxnStatus {
                    start_ts,
                    min_commit_ts: start_ts.saturating_add(1),
                    commit_ts: 0,
                    rolled_back: true,
                    is_completed: false,
                };
                tokio::spawn(async move {
                    if let Err(err) = rpc.broadcast_txn_status_to_all_stores(vec![status]).await {
                        debug!("broadcast_txn_status_to_all_stores failed: {err}");
                    }
                });

                spawn_pipelined_resolve_locks_and_broadcast_completion(
                    self.rpc.clone(),
                    self.keyspace,
                    self.rpc_interceptors.clone(),
                    self.options.clone(),
                    self.resource_group_tagger.clone(),
                    start_ts,
                    0,
                    true,
                    range_start.clone(),
                    range_end.clone(),
                    self.region_backoff(),
                    self.vars.killed.clone(),
                    pipelined.resolve_lock_concurrency(),
                );
                return Err(e);
            }
        };

        spawn_pipelined_resolve_locks_and_broadcast_completion(
            self.rpc.clone(),
            self.keyspace,
            self.rpc_interceptors.clone(),
            self.options.clone(),
            self.resource_group_tagger.clone(),
            self.start_version.version(),
            commit_ts.version(),
            false,
            range_start,
            range_end,
            self.region_backoff(),
            self.vars.killed.clone(),
            pipelined.resolve_lock_concurrency(),
        );

        Ok(Some(commit_ts))
    }

    async fn prewrite(&mut self) -> Result<Option<Timestamp>> {
        debug!("prewriting");
        self.vars.check_killed()?;
        let primary_lock = self
            .primary_key
            .clone()
            .ok_or_else(|| Error::InternalError {
                message: "primary key should be set before prewrite".to_owned(),
            })?;
        let primary_lock_key = primary_lock.clone();
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
        if matches!(self.options.kind, TransactionKind::Pessimistic(_))
            && !self.for_update_ts_constraints.is_empty()
        {
            request.for_update_ts_constraints = self
                .mutations
                .iter()
                .enumerate()
                .filter_map(|(index, mutation)| {
                    let key = Key::from(mutation.key.clone());
                    self.for_update_ts_constraints
                        .get(&key)
                        .map(|expected_for_update_ts| {
                            kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                                index: index as u32,
                                expected_for_update_ts: *expected_for_update_ts,
                            }
                        })
                })
                .collect();
        }

        request.use_async_commit = self.options.async_commit;
        request.try_one_pc = self.options.try_one_pc;
        request.assertion_level = self.options.assertion_level as i32;
        request.secondaries = self
            .mutations
            .iter()
            .filter(|m| primary_lock_key.as_ref() != m.key.as_ref())
            .map(|m| m.key.clone())
            .collect();
        self.options.apply_write_context(&mut request.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(request.label());
                let ctx = request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }

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
                let latest_ts = get_timestamp_for_txn_scope(
                    self.rpc.clone(),
                    self.options.txn_scope.as_deref(),
                )
                .await?;
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

        let killed = self.vars.killed.clone();
        let response = match self.options.prewrite_encounter_lock_policy {
            PrewriteEncounterLockPolicy::TryResolve => {
                let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
                let lock_backoff = self.lock_backoff();
                let region_backoff = self.region_backoff();
                let plan = PlanBuilder::new_with_rpc_interceptors(
                    self.rpc.clone(),
                    self.keyspace,
                    request,
                    self.rpc_interceptors.clone(),
                )
                .resolve_lock_with_pessimistic_region_in_context(
                    self.resolve_locks_ctx.clone(),
                    self.start_version.clone(),
                    lock_backoff,
                    killed.clone(),
                    self.keyspace,
                    true,
                    lock_resolver_rpc_context,
                )
                .retry_multi_region(region_backoff)
                .with_killed(killed.clone())
                .merge(CollectError)
                .extract_error()
                .plan();
                plan.execute().await?
            }
            PrewriteEncounterLockPolicy::NoResolve => {
                let region_backoff = self.region_backoff();
                let plan = PlanBuilder::new_with_rpc_interceptors(
                    self.rpc.clone(),
                    self.keyspace,
                    request,
                    self.rpc_interceptors.clone(),
                )
                .retry_multi_region(region_backoff)
                .with_killed(killed.clone())
                .merge(CollectError)
                .extract_error()
                .plan();
                plan.execute().await?
            }
        };

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

    fn check_commit_ts_upper_bound(&self, commit_ts: u64) -> Result<()> {
        let Some(checker) = self.commit_ts_upper_bound_check.as_ref() else {
            return Ok(());
        };

        if (checker)(commit_ts) {
            Ok(())
        } else {
            Err(Error::StringError(format!(
                "check commit ts upper bound fail, start_ts: {}, comm: {}",
                self.start_version.version(),
                commit_ts
            )))
        }
    }

    async fn get_timestamp_for_commit_inner(&mut self) -> Result<Timestamp> {
        let first_attempt =
            get_timestamp_for_txn_scope(self.rpc.clone(), self.options.txn_scope.as_deref())
                .await?;
        let first_attempt_version = first_attempt.version();

        if self.commit_wait_until_tso == 0 || first_attempt_version > self.commit_wait_until_tso {
            self.check_commit_ts_upper_bound(first_attempt_version)?;
            return Ok(first_attempt);
        }

        let max_sleep = self.commit_wait_until_tso_timeout;
        if max_sleep.is_zero() {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', retry timeout: {:?}, attempts: 1, last attempted commit TS: {}",
                first_attempt_version,
                self.commit_wait_until_tso,
                max_sleep,
                first_attempt_version
            )));
        }

        // Match client-go: if PD lags too far behind (clock drift exceeds the allowed timeout),
        // fail fast rather than waiting.
        let first_physical = Timestamp::from_version(first_attempt_version).physical;
        let expected_physical = Timestamp::from_version(self.commit_wait_until_tso).physical;
        let interval_ms =
            u64::try_from(expected_physical.saturating_sub(first_physical)).unwrap_or(0);
        let interval = Duration::from_millis(interval_ms);
        if interval > max_sleep {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', clock drift {:?} exceeds maximum allowed timeout {:?}",
                first_attempt_version,
                self.commit_wait_until_tso,
                interval,
                max_sleep
            )));
        }

        let deadline = Instant::now() + max_sleep;
        let mut backoff = Backoff::no_jitter_backoff(2, 500, 32);
        let mut attempts = 1_usize;
        let mut last_attempt = first_attempt;

        while last_attempt.version() <= self.commit_wait_until_tso {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let remaining = deadline.duration_since(now);
            let mut delay = backoff.next_delay_duration().unwrap_or(remaining);
            if delay > remaining {
                delay = remaining;
            }
            if delay.is_zero() {
                break;
            }
            tokio::time::sleep(delay).await;

            attempts += 1;
            last_attempt =
                get_timestamp_for_txn_scope(self.rpc.clone(), self.options.txn_scope.as_deref())
                    .await?;
        }

        if last_attempt.version() <= self.commit_wait_until_tso {
            return Err(Error::StringError(format!(
                "PD TSO '{}' lags the expected timestamp '{}', retry timeout: {:?}, attempts: {}, last attempted commit TS: {}",
                first_attempt_version,
                self.commit_wait_until_tso,
                max_sleep,
                attempts,
                last_attempt.version()
            )));
        }

        self.check_commit_ts_upper_bound(last_attempt.version())?;
        Ok(last_attempt)
    }

    /// Commits the primary key and returns the commit version
    async fn commit_primary(&mut self) -> Result<Timestamp> {
        debug!("committing primary");
        let primary_key = self.primary_key.clone().into_iter();
        let commit_version = self.get_timestamp_for_commit_inner().await?;
        let mut req = new_commit_request(
            primary_key,
            self.start_version.clone(),
            commit_version.clone(),
        );
        self.options.apply_write_context(&mut req.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(req.label());
                let ctx = req.context.get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let plan = PlanBuilder::new_with_rpc_interceptors(
            self.rpc.clone(),
            self.keyspace,
            req,
            self.rpc_interceptors.clone(),
        )
        .resolve_lock_in_context(
            self.resolve_locks_ctx.clone(),
            self.start_version.clone(),
            lock_backoff,
            killed.clone(),
            self.keyspace,
            lock_resolver_rpc_context,
        )
        .retry_multi_region(region_backoff)
        .with_killed(killed)
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
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
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
            let Some(primary_key) = self.primary_key else {
                return Err(Error::InternalError {
                    message: "primary key missing for secondary commit".to_owned(),
                });
            };
            let keys = mutations
                .map(|m| m.key.into())
                .filter(|key| &primary_key != key);
            new_commit_request(keys, start_version.clone(), commit_version)
        };
        self.options.apply_write_context(&mut req.context);
        if self.options.resource_group_tag.is_none() {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                let tag = (tagger)(req.label());
                let ctx = req.context.get_or_insert_with(kvrpcpb::Context::default);
                ctx.resource_group_tag = tag;
            }
        }
        let plan = PlanBuilder::new_with_rpc_interceptors(
            self.rpc,
            self.keyspace,
            req,
            self.rpc_interceptors.clone(),
        )
        .resolve_lock_in_context(
            self.resolve_locks_ctx.clone(),
            start_version,
            lock_backoff,
            killed.clone(),
            self.keyspace,
            lock_resolver_rpc_context,
        )
        .retry_multi_region(region_backoff)
        .with_killed(killed)
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
        let lock_backoff = self.lock_backoff();
        let region_backoff = self.region_backoff();
        let killed = self.vars.killed.clone();
        let lock_resolver_rpc_context = self.lock_resolver_rpc_context();
        let keys = self
            .mutations
            .into_iter()
            .map(|mutation| mutation.key.into());
        let start_version = self.start_version.clone();
        match self.options.kind.clone() {
            TransactionKind::Optimistic => {
                let mut req = new_batch_rollback_request(keys, start_version.clone());
                self.options.apply_write_context(&mut req.context);
                if self.options.resource_group_tag.is_none() {
                    if let Some(tagger) = self.resource_group_tagger.as_ref() {
                        let tag = (tagger)(req.label());
                        let ctx = req.context.get_or_insert_with(kvrpcpb::Context::default);
                        ctx.resource_group_tag = tag;
                    }
                }
                let plan = PlanBuilder::new_with_rpc_interceptors(
                    self.rpc,
                    self.keyspace,
                    req,
                    self.rpc_interceptors.clone(),
                )
                .resolve_lock_in_context(
                    self.resolve_locks_ctx.clone(),
                    start_version.clone(),
                    lock_backoff,
                    killed.clone(),
                    self.keyspace,
                    lock_resolver_rpc_context.clone(),
                )
                .retry_multi_region(region_backoff)
                .with_killed(killed)
                .extract_error()
                .plan();
                plan.execute().await?;
            }
            TransactionKind::Pessimistic(for_update_ts) => {
                let effective_for_update_ts = Timestamp::from_version(std::cmp::max(
                    for_update_ts.version(),
                    self.options.max_locked_with_conflict_ts,
                ));
                let mut req = new_pessimistic_rollback_request(
                    keys,
                    start_version.clone(),
                    effective_for_update_ts,
                );
                self.options.apply_write_context(&mut req.context);
                if self.options.resource_group_tag.is_none() {
                    if let Some(tagger) = self.resource_group_tagger.as_ref() {
                        let tag = (tagger)(req.label());
                        let ctx = req.context.get_or_insert_with(kvrpcpb::Context::default);
                        ctx.resource_group_tag = tag;
                    }
                }
                let plan = PlanBuilder::new_with_rpc_interceptors(
                    self.rpc,
                    self.keyspace,
                    req,
                    self.rpc_interceptors.clone(),
                )
                .resolve_lock_in_context(
                    self.resolve_locks_ctx.clone(),
                    start_version.clone(),
                    lock_backoff,
                    killed.clone(),
                    self.keyspace,
                    lock_resolver_rpc_context.clone(),
                )
                .retry_multi_region(region_backoff)
                .with_killed(killed)
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

    use super::{
        AssertionLevel, PessimisticLockMode, PrewriteEncounterLockPolicy, PIPELINED_MIN_FLUSH_KEYS,
        PIPELINED_MIN_FLUSH_MEM_SIZE,
    };

    use crate::gc_safe_point::GcSafePointCache;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::pd::PdClient;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
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
    use crate::PipelinedTxnOptions;
    use crate::ReplicaReadType;
    use crate::StoreLabel;
    use crate::Transaction;
    use crate::TransactionOptions;

    fn ok_pessimistic_lock_response_for_request(
        request: &kvrpcpb::PessimisticLockRequest,
    ) -> kvrpcpb::PessimisticLockResponse {
        let mut resp = kvrpcpb::PessimisticLockResponse::default();
        if request.wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32 {
            resp.results = std::iter::repeat_with(|| kvrpcpb::PessimisticLockKeyResult {
                r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal.into(),
                existence: true,
                ..Default::default()
            })
            .take(request.mutations.len())
            .collect();
        }
        resp
    }

    struct RecordingInterceptor {
        name: &'static str,
        id: &'static str,
        events: Arc<Mutex<Vec<String>>>,
        expected_target: Option<&'static str>,
        expected_label: Option<&'static str>,
        set_priority: Option<CommandPriority>,
    }

    impl RecordingInterceptor {
        fn new(
            name: &'static str,
            id: &'static str,
            events: Arc<Mutex<Vec<String>>>,
            expected_target: Option<&'static str>,
            expected_label: Option<&'static str>,
            set_priority: Option<CommandPriority>,
        ) -> Self {
            Self {
                name,
                id,
                events,
                expected_target,
                expected_label,
                set_priority,
            }
        }
    }

    impl crate::RpcInterceptor for RecordingInterceptor {
        fn name(&self) -> &str {
            self.name
        }

        fn before(&self, request: &mut crate::RpcRequest<'_>) {
            if let Some(target) = self.expected_target {
                assert_eq!(request.target(), target);
            }
            if let Some(label) = self.expected_label {
                assert_eq!(request.label(), label);
            }
            if let Some(priority) = self.set_priority {
                request.set_priority(priority);
            }
            self.events
                .lock()
                .unwrap()
                .push(format!("before:{}:{}", self.id, request.label()));
        }

        fn after(&self, request: &crate::RpcRequest<'_>, result: crate::RpcCallResult<'_>) {
            if let Some(target) = self.expected_target {
                assert_eq!(request.target(), target);
            }
            if let Some(label) = self.expected_label {
                assert_eq!(request.label(), label);
            }

            let result = match result {
                crate::RpcCallResult::Ok => "ok",
                crate::RpcCallResult::Err(_) => "err",
            };
            self.events.lock().unwrap().push(format!(
                "after:{}:{}:{}",
                self.id,
                request.label(),
                result
            ));
        }
    }

    #[test]
    fn test_pipelined_txn_options_validation() {
        assert!(matches!(
            PipelinedTxnOptions::new(0, 1, 0.0),
            Err(Error::StringError(msg))
                if msg == "pipelined txn flush concurrency should be greater than 0"
        ));
        assert!(matches!(
            PipelinedTxnOptions::new(1, 0, 0.0),
            Err(Error::StringError(msg))
                if msg == "pipelined txn resolve lock concurrency should be greater than 0"
        ));
        assert!(matches!(
            PipelinedTxnOptions::new(1, 1, -0.1),
            Err(Error::StringError(msg)) if msg.contains("invalid write throttle ratio")
        ));
        assert!(matches!(
            PipelinedTxnOptions::new(1, 1, 1.0),
            Err(Error::StringError(msg)) if msg.contains("invalid write throttle ratio")
        ));
        assert!(PipelinedTxnOptions::new(1, 1, 0.0).is_ok());
    }

    #[tokio::test]
    async fn test_get_checks_visibility_before_sending_requests() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn Any| {
                panic!("unexpected kv request when checking gc safe point visibility");
            },
        )));

        let gc_safe_point = GcSafePointCache::new(pd_client.clone(), Keyspace::Disable);
        gc_safe_point.observe_safe_point(50).await;

        let resolve_locks_ctx = crate::transaction::ResolveLocksContext::default();
        let mut snapshot = Transaction::new_with_resolve_locks_ctx(
            Timestamp::from_version(40),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
            resolve_locks_ctx,
            gc_safe_point,
            None,
        );

        let err = snapshot.get(b"k".to_vec()).await.unwrap_err();
        assert!(matches!(
            err,
            Error::TxnAbortedByGc {
                start_ts: 40,
                safe_point: 50
            }
        ));
    }

    #[tokio::test]
    async fn test_pipelined_flush_force_triggers_flush_and_increments_generation() {
        let flushed = Arc::new(Mutex::new(Vec::<kvrpcpb::FlushRequest>::new()));
        let flushed_cloned = flushed.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::FlushRequest>() {
                    flushed_cloned.lock().unwrap().push(req.clone());
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_assertion_level(AssertionLevel::Strict);

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();

        assert!(!txn.flush(false).await.unwrap());
        assert_eq!(
            txn.pipelined
                .as_ref()
                .expect("pipelined state must exist")
                .generation,
            0
        );
        assert!(flushed.lock().unwrap().is_empty());

        assert!(txn.flush(true).await.unwrap());
        assert_eq!(
            txn.pipelined
                .as_ref()
                .expect("pipelined state must exist")
                .generation,
            1
        );
        txn.flush_wait().await.unwrap();

        let flushed = flushed.lock().unwrap().clone();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].start_ts, 5);
        assert_eq!(flushed[0].min_commit_ts, 6);
        assert_eq!(flushed[0].generation, 1);
        assert_eq!(flushed[0].lock_ttl, 20000);
        assert_eq!(flushed[0].primary_key, vec![1u8]);
        assert_eq!(
            flushed[0].context.as_ref().unwrap().request_source,
            "pipelined_flush"
        );
        assert_eq!(flushed[0].assertion_level, AssertionLevel::Strict as i32);
    }

    #[tokio::test]
    async fn test_pipelined_auto_flush_triggers_flush_when_thresholds_met() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_assertion_level(AssertionLevel::Strict);

        let value_len = usize::try_from(PIPELINED_MIN_FLUSH_MEM_SIZE / PIPELINED_MIN_FLUSH_KEYS)
            .unwrap()
            .saturating_add(1);
        let value = vec![0u8; value_len];

        for i in 0..PIPELINED_MIN_FLUSH_KEYS {
            txn.put((i as u32).to_be_bytes().to_vec(), value.clone())
                .await
                .unwrap();
        }

        assert_eq!(
            txn.pipelined
                .as_ref()
                .expect("pipelined state must exist")
                .generation,
            1
        );

        {
            let pipelined = txn.pipelined.as_mut().expect("pipelined state must exist");
            pipelined
                .flushing
                .as_ref()
                .expect("expected auto flush to spawn task")
                .abort();
        }

        let err = txn
            .flush_wait()
            .await
            .expect_err("expected aborted flush to return error");
        match err {
            Error::InternalError { message } => {
                assert!(message.contains("pipelined flush task failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_flush_rejects_non_pipelined_transaction() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert!(matches!(
            txn.flush(false).await,
            Err(Error::StringError(msg)) if msg == "flush is only supported for pipelined transactions"
        ));
    }

    #[tokio::test]
    async fn test_pipelined_flush_rejects_async_commit_and_one_pc() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .use_async_commit()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert!(matches!(
            txn.flush(true).await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .try_one_pc()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert!(matches!(
            txn.flush(true).await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
    }

    #[tokio::test]
    async fn test_pipelined_flush_returns_internal_error_when_state_missing() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined_txn(PipelinedTxnOptions::new(1, 1, 0.0).unwrap())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        txn.pipelined = None;

        let err = txn
            .flush(true)
            .await
            .expect_err("expected missing pipelined state to return error");
        match err {
            Error::InternalError { message } => {
                assert!(message
                    .contains("pipelined txn options are set but pipelined state is missing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pipelined_commit_returns_internal_error_when_state_missing() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined_txn(PipelinedTxnOptions::new(1, 1, 0.0).unwrap())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        txn.pipelined = None;

        let err = txn
            .commit()
            .await
            .expect_err("expected missing pipelined state to return error");
        match err {
            Error::InternalError { message } => {
                assert!(message
                    .contains("pipelined txn options are set but pipelined state is missing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pipelined_get_after_flush_reads_remote_buffer() {
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let buffer_batch_get_calls = Arc::new(AtomicUsize::new(0));

        let flush_calls_cloned = flush_calls.clone();
        let buffer_batch_get_calls_cloned = buffer_batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BufferBatchGetRequest>() {
                    buffer_batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 5);
                    assert_eq!(req.keys, vec![vec![1u8]]);

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = vec![1u8];
                    pair.value = b"v1".to_vec();

                    let resp = kvrpcpb::BufferBatchGetResponse {
                        pairs: vec![pair],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        assert!(txn.flush(true).await.unwrap());
        txn.flush_wait().await.unwrap();

        assert_eq!(txn.get(vec![1u8]).await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(flush_calls.load(Ordering::SeqCst), 1);
        assert_eq!(buffer_batch_get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pipelined_insert_after_flush_detects_duplicate_in_remote_buffer() {
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let buffer_batch_get_calls = Arc::new(AtomicUsize::new(0));

        let flush_calls_cloned = flush_calls.clone();
        let buffer_batch_get_calls_cloned = buffer_batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BufferBatchGetRequest>() {
                    buffer_batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 5);
                    assert_eq!(req.keys, vec![vec![1u8]]);

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = vec![1u8];
                    pair.value = b"v1".to_vec();

                    let resp = kvrpcpb::BufferBatchGetResponse {
                        pairs: vec![pair],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        assert!(txn.flush(true).await.unwrap());
        txn.flush_wait().await.unwrap();

        let err = txn
            .insert(vec![1u8], b"v2".to_vec())
            .await
            .expect_err("expected insert to fail on duplicate key");
        assert!(matches!(err, Error::DuplicateKeyInsertion));
        assert_eq!(flush_calls.load(Ordering::SeqCst), 1);
        assert_eq!(buffer_batch_get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pipelined_get_outside_flushed_range_skips_remote_buffer() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if req
                    .downcast_ref::<kvrpcpb::BufferBatchGetRequest>()
                    .is_some()
                {
                    return Err(Error::StringError(
                        "BufferBatchGet should be skipped for keys outside flushed range"
                            .to_owned(),
                    ));
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    assert_eq!(req.version, 5);
                    assert_eq!(req.key, vec![2u8]);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"v2".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        assert!(txn.flush(true).await.unwrap());
        txn.flush_wait().await.unwrap();

        assert_eq!(txn.get(vec![2u8]).await.unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_pipelined_batch_get_after_flush_merges_buffer_and_snapshot() {
        let buffer_batch_get_calls = Arc::new(AtomicUsize::new(0));
        let snapshot_batch_get_calls = Arc::new(AtomicUsize::new(0));

        let buffer_batch_get_calls_cloned = buffer_batch_get_calls.clone();
        let snapshot_batch_get_calls_cloned = snapshot_batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::BufferBatchGetRequest>() {
                    buffer_batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 5);
                    assert_eq!(req.keys, vec![vec![1u8]]);

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = vec![1u8];
                    pair.value = b"v1".to_vec();

                    let resp = kvrpcpb::BufferBatchGetResponse {
                        pairs: vec![pair],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    snapshot_batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 5);
                    assert_eq!(req.keys, vec![vec![2u8]]);

                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = vec![2u8];
                    pair.value = b"v2".to_vec();

                    let resp = kvrpcpb::BatchGetResponse {
                        pairs: vec![pair],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        assert!(txn.flush(true).await.unwrap());
        txn.flush_wait().await.unwrap();

        let result: std::collections::HashMap<Key, Vec<u8>> = txn
            .batch_get(vec![vec![1u8], vec![2u8]])
            .await
            .unwrap()
            .map(|pair| (pair.0, pair.1))
            .collect();

        assert_eq!(result.get(&Key::from(vec![1u8])), Some(&b"v1".to_vec()));
        assert_eq!(result.get(&Key::from(vec![2u8])), Some(&b"v2".to_vec()));

        assert_eq!(buffer_batch_get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot_batch_get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pipelined_get_does_not_block_on_in_flight_flush() {
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let flush_calls_cloned = flush_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined_txn(PipelinedTxnOptions::new(1, 1, 0.5).unwrap())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        let ewma = txn
            .pipelined
            .as_ref()
            .expect("pipelined state must exist")
            .flush_duration_ewma
            .clone();
        ewma.lock().unwrap().observe(200.0);

        assert!(txn.flush(true).await.unwrap());
        let value = tokio::time::timeout(Duration::from_millis(100), txn.get(vec![1u8]))
            .await
            .expect("get timed out while flush is in-flight")
            .unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));
        assert_eq!(flush_calls.load(Ordering::SeqCst), 0);

        txn.flush_wait().await.unwrap();
        assert_eq!(flush_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pipelined_batch_get_does_not_block_on_in_flight_flush() {
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let flush_calls_cloned = flush_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    flush_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined_txn(PipelinedTxnOptions::new(1, 1, 0.5).unwrap())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        let ewma = txn
            .pipelined
            .as_ref()
            .expect("pipelined state must exist")
            .flush_duration_ewma
            .clone();
        ewma.lock().unwrap().observe(200.0);

        assert!(txn.flush(true).await.unwrap());
        let pairs = tokio::time::timeout(Duration::from_millis(100), async {
            txn.batch_get(vec![vec![1u8]])
                .await
                .unwrap()
                .map(|pair| (pair.0, pair.1))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .await
        .expect("batch_get timed out while flush is in-flight");
        assert_eq!(pairs.get(&Key::from(vec![1u8])), Some(&b"v1".to_vec()));
        assert_eq!(flush_calls.load(Ordering::SeqCst), 0);

        txn.flush_wait().await.unwrap();
        assert_eq!(flush_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pipelined_scan_is_not_supported() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert!(matches!(
            txn.scan(vec![0u8]..vec![2u8], 10).await,
            Err(Error::StringError(msg)) if msg == "scan is not supported for pipelined transactions"
        ));
    }

    #[tokio::test]
    async fn test_pipelined_commit_uses_flush_and_resolve_lock_range() {
        let scenario = FailScenario::setup();
        fail::cfg("pipelined_broadcast_grace_period_ms", "return(0)").unwrap();

        let flushed = Arc::new(Mutex::new(Vec::<kvrpcpb::FlushRequest>::new()));
        let committed = Arc::new(Mutex::new(Vec::<kvrpcpb::CommitRequest>::new()));
        let resolved = Arc::new(Mutex::new(Vec::<kvrpcpb::ResolveLockRequest>::new()));

        let (resolve_tx, mut resolve_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let broadcasts = Arc::new(AtomicUsize::new(0));
        let broadcasts_captured = broadcasts.clone();
        let broadcast_notified = Arc::new(tokio::sync::Notify::new());
        let broadcast_notified_captured = broadcast_notified.clone();

        let flushed_cloned = flushed.clone();
        let committed_cloned = committed.clone();
        let resolved_cloned = resolved.clone();

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::FlushRequest>() {
                    flushed_cloned.lock().unwrap().push(req.clone());
                    return Ok(Box::new(kvrpcpb::FlushResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    committed_cloned.lock().unwrap().push(req.clone());
                    return Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolved_cloned.lock().unwrap().push(req.clone());
                    let _ = resolve_tx.send(());
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>() {
                    assert!(
                        req.context.is_some(),
                        "BroadcastTxnStatusRequest should populate context"
                    );
                    assert_eq!(req.txn_status.len(), 1);
                    let status = &req.txn_status[0];
                    assert_eq!(status.start_ts, 5);
                    assert_eq!(status.min_commit_ts, 0);
                    assert_eq!(status.commit_ts, 100);
                    assert!(!status.rolled_back);
                    assert!(status.is_completed);

                    let count = broadcasts_captured.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 2 {
                        broadcast_notified_captured.notify_one();
                    }
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            }))
            .with_tso_sequence(100),
        );

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let start_ts = Timestamp::from_version(5);
        let options = TransactionOptions::new_optimistic()
            .pipelined()
            .heartbeat_option(HeartbeatOption::NoHeartbeat);
        let mut txn = Transaction::new(start_ts.clone(), pd_client, options, Keyspace::Disable);

        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        txn.put(vec![10u8], b"v2".to_vec()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().unwrap();
        assert_eq!(commit_ts.version(), 100);

        // Pipelined resolve-lock runs in a background task.
        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..2 {
                resolve_rx
                    .recv()
                    .await
                    .expect("resolve-lock request should be sent");
            }
        })
        .await
        .expect("timed out waiting for pipelined resolve-lock");

        tokio::time::timeout(Duration::from_secs(2), broadcast_notified.notified())
            .await
            .expect("completion broadcast should be dispatched to all stores");
        assert_eq!(broadcasts.load(Ordering::SeqCst), 2);

        let flushed = flushed.lock().unwrap().clone();
        assert_eq!(flushed.len(), 2, "expected one flush per region");
        for req in &flushed {
            assert_eq!(req.start_ts, 5);
            assert_eq!(req.min_commit_ts, 6);
            assert_eq!(req.generation, 1);
            assert_eq!(req.lock_ttl, 20000);
            assert_eq!(req.primary_key, vec![1u8]);
            assert_eq!(
                req.context.as_ref().unwrap().request_source,
                "pipelined_flush"
            );
        }

        let committed = committed.lock().unwrap().clone();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].start_version, 5);
        assert_eq!(committed[0].commit_version, 100);
        assert_eq!(committed[0].keys, vec![vec![1u8]]);

        let resolved = resolved.lock().unwrap().clone();
        assert_eq!(resolved.len(), 2, "expected resolve-lock per region");
        for req in &resolved {
            assert_eq!(req.start_version, 5);
            assert_eq!(req.commit_version, 100);
            assert_eq!(
                req.context.as_ref().unwrap().request_source,
                "pipelined_flush"
            );
        }

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_pipelined_commit_failure_resolves_flushed_locks_and_broadcasts_completion() {
        use tokio::sync::Notify;

        let scenario = FailScenario::setup();
        fail::cfg("pipelined_broadcast_grace_period_ms", "return(0)").unwrap();

        let initial_broadcasts = Arc::new(AtomicUsize::new(0));
        let initial_broadcasts_captured = initial_broadcasts.clone();
        let completed_broadcasts = Arc::new(AtomicUsize::new(0));
        let completed_broadcasts_captured = completed_broadcasts.clone();
        let resolved = Arc::new(AtomicUsize::new(0));
        let resolved_captured = resolved.clone();
        let notified = Arc::new(Notify::new());
        let notified_captured = notified.clone();

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::FlushRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    return Err(Error::StringError("commit primary failed".to_owned()));
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.start_version, 5);
                    assert_eq!(req.commit_version, 0);
                    assert_eq!(
                        req.context.as_ref().unwrap().request_source,
                        "pipelined_flush"
                    );
                    resolved_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>() {
                    assert!(
                        req.context.is_some(),
                        "BroadcastTxnStatusRequest should populate context"
                    );
                    assert_eq!(req.txn_status.len(), 1);
                    let status = &req.txn_status[0];
                    assert_eq!(status.start_ts, 5);
                    assert_eq!(status.commit_ts, 0);
                    assert!(status.rolled_back);

                    if status.is_completed {
                        assert_eq!(status.min_commit_ts, 0);
                        let count =
                            completed_broadcasts_captured.fetch_add(1, Ordering::SeqCst) + 1;
                        if count == 2 {
                            notified_captured.notify_one();
                        }
                    } else {
                        assert_eq!(status.min_commit_ts, 6);
                        initial_broadcasts_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }

                Err(Error::StringError("unexpected request".to_owned()))
            }))
            .with_tso_sequence(100),
        );

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();
        txn.put(vec![11u8], b"v2".to_vec()).await.unwrap();

        txn.commit().await.expect_err("commit primary should fail");

        tokio::time::timeout(Duration::from_secs(2), notified.notified())
            .await
            .expect("completion broadcast should be dispatched to all stores");
        assert_eq!(initial_broadcasts.load(Ordering::SeqCst), 2);
        assert_eq!(completed_broadcasts.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.load(Ordering::SeqCst), 2);

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_pipelined_commit_rejects_async_commit_and_one_pc() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .use_async_commit()
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        assert!(matches!(
            txn.put(vec![1u8], b"v1".to_vec()).await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert!(matches!(
            txn.commit().await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("unexpected request".to_owned()))
        })));
        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .try_one_pc()
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        assert!(matches!(
            txn.put(vec![1u8], b"v1".to_vec()).await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert!(matches!(
            txn.commit().await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
    }

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
    async fn test_resolve_lock_detail_records_time_for_snapshot_reads() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_captured = get_calls.clone();

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::GetRequest>().is_some() {
                    let attempt = get_calls_captured.fetch_add(1, Ordering::SeqCst);

                    if attempt == 0 {
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

                panic!("unexpected request type in resolve-lock-detail test");
            }))
            .with_get_timestamp_delay(Duration::from_millis(5)),
        );

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let value = snapshot.get(b"k".to_vec()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        let detail = snapshot.resolve_lock_detail();
        assert!(detail.resolve_lock_time >= Duration::from_millis(5));
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
        let gc_safe_point = GcSafePointCache::new(pd_client.clone(), Keyspace::Disable);

        let mut snapshot1 = Transaction::new_with_resolve_locks_ctx(
            Timestamp::from_version(10),
            pd_client.clone(),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
            resolve_locks_ctx.clone(),
            gc_safe_point.clone(),
            None,
        );
        let value1 = snapshot1.get(b"k1".to_vec()).await.unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let mut snapshot2 = Transaction::new_with_resolve_locks_ctx(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
            resolve_locks_ctx,
            gc_safe_point,
            None,
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
    async fn test_snapshot_replica_read_mixed_match_store_labels_routes_to_label_matched_store() {
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

        pd_client
            .insert_store_meta(metapb::Store {
                id: 41,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 51,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 61,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed)
                .match_store_labels(vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }]),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 61);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_match_store_ids_routes_to_store() {
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
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed)
                .match_store_ids(vec![41]),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 41);
        assert_eq!(pd_client.store_meta_by_id_call_count(), 0);
    }

    #[tokio::test]
    async fn test_snapshot_replica_read_mixed_match_store_ids_and_labels_filters_store_meta_queries(
    ) {
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

        pd_client
            .insert_store_meta(metapb::Store {
                id: 41,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-west".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 51,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 61,
                labels: vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .read_only()
                .replica_read(ReplicaReadType::Mixed)
                .match_store_ids(vec![41, 51])
                .match_store_labels(vec![StoreLabel {
                    key: "zone".to_owned(),
                    value: "us-east".to_owned(),
                }]),
            Keyspace::Disable,
        );
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(store_id.load(Ordering::SeqCst), 51);
        assert_eq!(pd_client.store_meta_by_id_call_count(), 2);
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
        assert_eq!(store_ids, vec![51, 61, 41]);
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
        let first_busy_threshold_ms = Arc::new(AtomicU64::new(0));
        let second_busy_threshold_ms = Arc::new(AtomicU64::new(0));

        let get_count_captured = get_count.clone();
        let first_store_id_captured = first_store_id.clone();
        let second_store_id_captured = second_store_id.clone();
        let first_stale_read_captured = first_stale_read.clone();
        let second_stale_read_captured = second_stale_read.clone();
        let first_busy_threshold_ms_captured = first_busy_threshold_ms.clone();
        let second_busy_threshold_ms_captured = second_busy_threshold_ms.clone();
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
                        first_busy_threshold_ms_captured
                            .store(u64::from(ctx.busy_threshold_ms), Ordering::SeqCst);

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
                    second_busy_threshold_ms_captured
                        .store(u64::from(ctx.busy_threshold_ms), Ordering::SeqCst);
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
        snapshot.set_load_based_replica_read_threshold(Duration::from_millis(222));
        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(first_store_id.load(Ordering::SeqCst), 51);
        assert_eq!(second_store_id.load(Ordering::SeqCst), 41);
        assert!(first_stale_read.load(Ordering::SeqCst));
        assert!(!second_stale_read.load(Ordering::SeqCst));
        assert_eq!(first_busy_threshold_ms.load(Ordering::SeqCst), 222);
        assert_eq!(second_busy_threshold_ms.load(Ordering::SeqCst), 0);
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
        assert_eq!(third_store_id.load(Ordering::SeqCst), 61);
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
    async fn test_snapshot_setters_propagate_to_context() {
        let seen_ctx = Arc::new(Mutex::new(None::<kvrpcpb::Context>));

        let seen_ctx_cloned = seen_ctx.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                *seen_ctx_cloned.lock().unwrap() = Some(ctx.clone());

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let tag = b"rg-tag".to_vec();
        let resource_group_name = "rg-name".to_string();
        let request_source = "request-source".to_string();

        snapshot.set_not_fill_cache(true);
        snapshot.set_task_id(42);
        snapshot.set_max_execution_duration(Duration::from_millis(987));
        snapshot.set_priority(CommandPriority::High);
        snapshot.set_isolation_level(IsolationLevel::RcCheckTs);
        snapshot.set_resource_group_tag(tag.clone());
        snapshot.set_resource_group_name(resource_group_name.clone());
        snapshot.set_request_source(request_source.clone());

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        let ctx = seen_ctx
            .lock()
            .unwrap()
            .clone()
            .expect("expected context to be captured");
        assert!(ctx.not_fill_cache);
        assert_eq!(ctx.task_id, 42);
        assert_eq!(ctx.max_execution_duration_ms, 987);
        assert_eq!(ctx.priority, CommandPriority::High as i32);
        assert_eq!(ctx.isolation_level, IsolationLevel::RcCheckTs as i32);
        assert_eq!(ctx.resource_group_tag, tag);
        let resource_control_context = ctx
            .resource_control_context
            .as_ref()
            .expect("resource_control_context");
        assert_eq!(
            resource_control_context.resource_group_name,
            resource_group_name
        );
        assert_eq!(ctx.request_source, request_source);
    }

    #[tokio::test]
    async fn test_snapshot_set_stale_read_toggles_context_flag() {
        let seen_flags = Arc::new(Mutex::new(Vec::<(bool, bool)>::new()));

        let seen_flags_cloned = seen_flags.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_flags_cloned
                    .lock()
                    .unwrap()
                    .push((ctx.stale_read, ctx.replica_read));

                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        snapshot.set_stale_read(true);
        let _ = snapshot.get(vec![0]).await.unwrap();

        snapshot.set_stale_read(false);
        let _ = snapshot.get(vec![1]).await.unwrap();

        assert_eq!(
            *seen_flags.lock().unwrap(),
            vec![(true, false), (false, true)]
        );
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
    async fn test_snapshot_resource_group_tagger_applies_when_tag_unset() {
        let seen_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let tag = b"rg-tagger".to_vec();

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

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        snapshot.set_resource_group_tagger(move |_label| {
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(*seen_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_snapshot_resource_group_tagger_skipped_when_tag_set() {
        let seen_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
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

        let calls_cloned = calls.clone();
        let mut snapshot = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        snapshot.set_resource_group_tagger(move |_label| {
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            b"tagger".to_vec()
        });
        snapshot.set_resource_group_tag(tag.clone());

        let key: Key = vec![0].into();
        let _ = snapshot.get(key).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 0);
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

    #[tokio::test]
    async fn test_rpc_interceptor_onion_order_and_target() {
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let events_cloned = events.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::StringError("unexpected request".to_owned()));
                };
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.priority, CommandPriority::High as i32);

                events_cloned
                    .lock()
                    .unwrap()
                    .push("dispatch:kv_get".to_owned());

                let mut resp = kvrpcpb::GetResponse::default();
                resp.value = b"v".to_vec();
                resp.not_found = false;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.add_rpc_interceptor(RecordingInterceptor::new(
            "a",
            "a",
            events.clone(),
            Some("mock://41"),
            Some("kv_get"),
            Some(CommandPriority::High),
        ));
        txn.add_rpc_interceptor(RecordingInterceptor::new(
            "b",
            "b",
            events.clone(),
            Some("mock://41"),
            Some("kv_get"),
            None,
        ));

        let value = txn.get(vec![1_u8]).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        assert_eq!(
            events.lock().unwrap().clone(),
            vec![
                "before:a:kv_get".to_owned(),
                "before:b:kv_get".to_owned(),
                "dispatch:kv_get".to_owned(),
                "after:b:kv_get:ok".to_owned(),
                "after:a:kv_get:ok".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn test_rpc_interceptor_add_dedup_by_name_last_wins() {
        let events = Arc::new(Mutex::new(Vec::<String>::new()));

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::GetRequest>().is_none() {
                    return Err(Error::StringError("unexpected request".to_owned()));
                }
                Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn Any>)
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.add_rpc_interceptor(RecordingInterceptor::new(
            "dup",
            "first",
            events.clone(),
            None,
            None,
            None,
        ));
        txn.add_rpc_interceptor(RecordingInterceptor::new(
            "dup",
            "second",
            events.clone(),
            None,
            None,
            None,
        ));

        txn.get(vec![1_u8]).await.unwrap();
        assert_eq!(
            events.lock().unwrap().clone(),
            vec![
                "before:second:kv_get".to_owned(),
                "after:second:kv_get:ok".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn test_rpc_interceptor_applies_to_commit_requests() {
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let events_cloned = events.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.priority, CommandPriority::High as i32);
                    events_cloned
                        .lock()
                        .unwrap()
                        .push("dispatch:kv_prewrite".to_owned());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.priority, CommandPriority::High as i32);
                    events_cloned
                        .lock()
                        .unwrap()
                        .push("dispatch:kv_commit".to_owned());
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_rpc_interceptor(RecordingInterceptor::new(
            "rec",
            "rec",
            events.clone(),
            Some("mock://41"),
            None,
            Some(CommandPriority::High),
        ));

        txn.put(vec![1_u8], b"v".to_vec()).await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            events.lock().unwrap().clone(),
            vec![
                "before:rec:kv_prewrite".to_owned(),
                "dispatch:kv_prewrite".to_owned(),
                "after:rec:kv_prewrite:ok".to_owned(),
                "before:rec:kv_commit".to_owned(),
                "dispatch:kv_commit".to_owned(),
                "after:rec:kv_commit:ok".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn test_transaction_write_context_setters_propagate_to_commit_requests() {
        let prewrite_txn_source = Arc::new(AtomicU64::new(0));
        let commit_txn_source = Arc::new(AtomicU64::new(0));
        let prewrite_disk_full_opt = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let commit_disk_full_opt = Arc::new(std::sync::atomic::AtomicI32::new(-1));
        let prewrite_sync_log = Arc::new(AtomicBool::new(false));
        let commit_sync_log = Arc::new(AtomicBool::new(false));
        let prewrite_max_execution_duration_ms = Arc::new(AtomicU64::new(0));
        let commit_max_execution_duration_ms = Arc::new(AtomicU64::new(0));

        let prewrite_txn_source_cloned = prewrite_txn_source.clone();
        let commit_txn_source_cloned = commit_txn_source.clone();
        let prewrite_disk_full_opt_cloned = prewrite_disk_full_opt.clone();
        let commit_disk_full_opt_cloned = commit_disk_full_opt.clone();
        let prewrite_sync_log_cloned = prewrite_sync_log.clone();
        let commit_sync_log_cloned = commit_sync_log.clone();
        let prewrite_max_execution_duration_ms_cloned = prewrite_max_execution_duration_ms.clone();
        let commit_max_execution_duration_ms_cloned = commit_max_execution_duration_ms.clone();

        let start_version = 7;
        let commit_version = 8;

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    prewrite_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    prewrite_disk_full_opt_cloned.store(ctx.disk_full_opt, Ordering::SeqCst);
                    prewrite_sync_log_cloned.store(ctx.sync_log, Ordering::SeqCst);
                    prewrite_max_execution_duration_ms_cloned
                        .store(ctx.max_execution_duration_ms, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert_eq!(req.start_version, start_version);
                    assert_eq!(req.commit_version, commit_version);

                    let ctx = req.context.as_ref().expect("context");
                    commit_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    commit_disk_full_opt_cloned.store(ctx.disk_full_opt, Ordering::SeqCst);
                    commit_sync_log_cloned.store(ctx.sync_log, Ordering::SeqCst);
                    commit_max_execution_duration_ms_cloned
                        .store(ctx.max_execution_duration_ms, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            }))
            .with_tso_sequence(commit_version),
        );

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client,
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_txn_source(42);
        txn.set_disk_full_opt(DiskFullOpt::AllowedOnAlmostFull);
        txn.enable_force_sync_log();
        txn.set_max_write_execution_duration(Duration::from_millis(987));

        txn.put("key".to_owned(), "value".to_owned()).await.unwrap();
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
        assert!(prewrite_sync_log.load(Ordering::SeqCst));
        assert!(commit_sync_log.load(Ordering::SeqCst));
        assert_eq!(
            prewrite_max_execution_duration_ms.load(Ordering::SeqCst),
            987
        );
        assert_eq!(commit_max_execution_duration_ms.load(Ordering::SeqCst), 987);
    }

    #[tokio::test]
    async fn test_txn_resource_group_tagger_applies_to_commit_requests() {
        let prewrite_resource_group_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let commit_resource_group_tag = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let tag = b"rg-tagger".to_vec();

        let prewrite_resource_group_tag_cloned = prewrite_resource_group_tag.clone();
        let commit_resource_group_tag_cloned = commit_resource_group_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    *prewrite_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    *commit_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_resource_group_tagger(move |_label| {
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(*prewrite_resource_group_tag.lock().unwrap(), tag);
        assert_eq!(*commit_resource_group_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_txn_resource_group_tagger_applies_to_rollback_requests() {
        let rollback_txn_source = Arc::new(AtomicU64::new(0));
        let rollback_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let rollback_request_source = Arc::new(Mutex::new(String::new()));
        let rollback_resource_group_tag = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let request_source = "txn-rollback".to_string();
        let tag = b"rg-rollback".to_vec();

        let rollback_txn_source_cloned = rollback_txn_source.clone();
        let rollback_priority_cloned = rollback_priority.clone();
        let rollback_request_source_cloned = rollback_request_source.clone();
        let rollback_resource_group_tag_cloned = rollback_resource_group_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::BatchRollbackRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    rollback_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    rollback_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    *rollback_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    *rollback_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    return Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
                .txn_source(7)
                .request_source(request_source.clone())
                .priority(CommandPriority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_resource_group_tagger(move |label| {
            assert_eq!(label, "kv_batch_rollback");
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.rollback().await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(rollback_txn_source.load(Ordering::SeqCst), 7);
        assert_eq!(
            rollback_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert_eq!(*rollback_request_source.lock().unwrap(), request_source);
        assert_eq!(*rollback_resource_group_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_txn_resource_group_tagger_applies_to_pessimistic_lock_requests() {
        let lock_txn_source = Arc::new(AtomicU64::new(0));
        let lock_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let lock_request_source = Arc::new(Mutex::new(String::new()));
        let lock_resource_group_tag = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let request_source = "txn-pessimistic-lock".to_string();
        let tag = b"rg-lock".to_vec();

        let lock_txn_source_cloned = lock_txn_source.clone();
        let lock_priority_cloned = lock_priority.clone();
        let lock_request_source_cloned = lock_request_source.clone();
        let lock_resource_group_tag_cloned = lock_resource_group_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    lock_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    lock_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    *lock_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    *lock_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    let resp = kvrpcpb::PessimisticLockResponse {
                        values: vec![b"v".to_vec(); req.mutations.len()],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
                .txn_source(9)
                .request_source(request_source.clone())
                .priority(CommandPriority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_resource_group_tagger(move |label| {
            assert_eq!(label, "kv_pessimistic_lock");
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });

        let key: Key = vec![0].into();
        let _ = txn.get_for_update(key).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(lock_txn_source.load(Ordering::SeqCst), 9);
        assert_eq!(
            lock_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert_eq!(*lock_request_source.lock().unwrap(), request_source);
        assert_eq!(*lock_resource_group_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_txn_resource_group_tagger_applies_to_pessimistic_lock_rollback_requests() {
        let rollback_txn_source = Arc::new(AtomicU64::new(0));
        let rollback_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let rollback_request_source = Arc::new(Mutex::new(String::new()));
        let rollback_resource_group_tag = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let request_source = "txn-lock-rollback".to_string();
        let tag = b"rg-lock-rollback".to_vec();

        let rollback_txn_source_cloned = rollback_txn_source.clone();
        let rollback_priority_cloned = rollback_priority.clone();
        let rollback_request_source_cloned = rollback_request_source.clone();
        let rollback_resource_group_tag_cloned = rollback_resource_group_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    rollback_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    rollback_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    *rollback_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    *rollback_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
                .txn_source(11)
                .request_source(request_source.clone())
                .priority(CommandPriority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_resource_group_tagger(move |label| {
            assert_eq!(label, "kv_pessimistic_rollback");
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });

        let key: Key = vec![0].into();
        txn.pessimistic_lock_rollback(
            std::iter::once(key),
            Timestamp::default(),
            Timestamp::default(),
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(rollback_txn_source.load(Ordering::SeqCst), 11);
        assert_eq!(
            rollback_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert_eq!(*rollback_request_source.lock().unwrap(), request_source);
        assert_eq!(*rollback_resource_group_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_txn_resource_group_tagger_applies_to_txn_heart_beat_requests() {
        let heart_beat_txn_source = Arc::new(AtomicU64::new(0));
        let heart_beat_priority = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let heart_beat_request_source = Arc::new(Mutex::new(String::new()));
        let heart_beat_resource_group_tag = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let request_source = "txn-heartbeat".to_string();
        let tag = b"rg-heartbeat".to_vec();

        let heart_beat_txn_source_cloned = heart_beat_txn_source.clone();
        let heart_beat_priority_cloned = heart_beat_priority.clone();
        let heart_beat_request_source_cloned = heart_beat_request_source.clone();
        let heart_beat_resource_group_tag_cloned = heart_beat_resource_group_tag.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    heart_beat_txn_source_cloned.store(ctx.txn_source, Ordering::SeqCst);
                    heart_beat_priority_cloned.store(ctx.priority, Ordering::SeqCst);
                    *heart_beat_request_source_cloned.lock().unwrap() = ctx.request_source.clone();
                    *heart_beat_resource_group_tag_cloned.lock().unwrap() =
                        ctx.resource_group_tag.clone();
                    let resp = kvrpcpb::TxnHeartBeatResponse {
                        lock_ttl: 99,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let calls_cloned = calls.clone();
        let tag_cloned = tag.clone();
        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .txn_source(13)
                .request_source(request_source.clone())
                .priority(CommandPriority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_resource_group_tagger(move |label| {
            assert_eq!(label, "kv_txn_heart_beat");
            calls_cloned.fetch_add(1, Ordering::SeqCst);
            tag_cloned.clone()
        });
        txn.put("key".to_owned(), "value").await.unwrap();
        let ttl = txn.send_heart_beat().await.unwrap();

        assert_eq!(ttl, 99);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(heart_beat_txn_source.load(Ordering::SeqCst), 13);
        assert_eq!(
            heart_beat_priority.load(Ordering::SeqCst),
            CommandPriority::High as i32
        );
        assert_eq!(*heart_beat_request_source.lock().unwrap(), request_source);
        assert_eq!(*heart_beat_resource_group_tag.lock().unwrap(), tag);
    }

    #[tokio::test]
    async fn test_start_auto_heartbeat_without_primary_key_returns_error() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = txn
            .start_auto_heartbeat()
            .await
            .expect_err("auto heartbeat requires a primary key");
        assert!(matches!(err, Error::InternalError { .. }));
        assert!(!txn.is_heartbeat_started);
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

    #[tokio::test]
    async fn test_pipelined_auto_heartbeat_broadcasts_txn_status_to_all_stores() {
        use tokio::sync::Notify;

        let broadcasts = Arc::new(AtomicUsize::new(0));
        let broadcasts_captured = broadcasts.clone();
        let notified = Arc::new(Notify::new());
        let notified_captured = notified.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>() {
                    assert!(
                        req.context.is_some(),
                        "BroadcastTxnStatusRequest should populate context"
                    );
                    assert_eq!(req.txn_status.len(), 1);
                    let status = &req.txn_status[0];
                    assert_eq!(status.start_ts, 5);
                    assert_eq!(status.min_commit_ts, 6);
                    assert_eq!(status.commit_ts, 0);
                    assert!(!status.rolled_back);
                    assert!(!status.is_completed);

                    let count = broadcasts_captured.fetch_add(1, Ordering::SeqCst) + 1;
                    if count == 2 {
                        notified_captured.notify_one();
                    }
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(50)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.put(b"k".to_vec(), b"v".to_vec()).await.unwrap();
        txn.start_auto_heartbeat().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), notified.notified())
            .await
            .expect("broadcast should be dispatched to all stores");

        txn.stop_auto_heartbeat();
        assert_eq!(broadcasts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_pipelined_rollback_broadcasts_txn_status_to_all_stores_when_flushed_range_present(
    ) {
        use tokio::sync::Notify;

        let scenario = FailScenario::setup();
        fail::cfg("pipelined_broadcast_grace_period_ms", "return(0)").unwrap();

        let initial_broadcasts = Arc::new(AtomicUsize::new(0));
        let initial_broadcasts_captured = initial_broadcasts.clone();
        let completed_broadcasts = Arc::new(AtomicUsize::new(0));
        let completed_broadcasts_captured = completed_broadcasts.clone();
        let resolved = Arc::new(AtomicUsize::new(0));
        let resolved_captured = resolved.clone();
        let notified = Arc::new(Notify::new());
        let notified_captured = notified.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.start_version, 5);
                    assert_eq!(req.commit_version, 0);
                    assert_eq!(
                        req.context.as_ref().unwrap().request_source,
                        "pipelined_flush"
                    );
                    resolved_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>() {
                    assert!(
                        req.context.is_some(),
                        "BroadcastTxnStatusRequest should populate context"
                    );
                    assert_eq!(req.txn_status.len(), 1);
                    let status = &req.txn_status[0];
                    assert_eq!(status.start_ts, 5);
                    assert_eq!(status.commit_ts, 0);
                    assert!(status.rolled_back);

                    if status.is_completed {
                        assert_eq!(status.min_commit_ts, 0);
                        let count =
                            completed_broadcasts_captured.fetch_add(1, Ordering::SeqCst) + 1;
                        if count == 2 {
                            notified_captured.notify_one();
                        }
                    } else {
                        assert_eq!(status.min_commit_ts, 6);
                        initial_broadcasts_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                ..Default::default()
            })
            .await;

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_optimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.pipelined
            .as_mut()
            .expect("pipelined state must exist")
            .flushed_range_start = Some(Key::from(b"a".to_vec()));
        txn.pipelined
            .as_mut()
            .expect("pipelined state must exist")
            .flushed_range_end = Some(Key::from(b"z".to_vec()));

        txn.rollback().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), notified.notified())
            .await
            .expect("completion broadcast should be dispatched to all stores");
        assert_eq!(initial_broadcasts.load(Ordering::SeqCst), 2);
        assert_eq!(completed_broadcasts.load(Ordering::SeqCst), 2);
        assert_eq!(resolved.load(Ordering::SeqCst), 1);

        scenario.teardown();
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

        async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> crate::Result<u64> {
            Ok(safepoint)
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
    async fn test_transaction_commit_callback_receives_client_go_txn_info_json() {
        let start_version = 7;
        let commit_version = 8;

        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));
        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

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

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(commit_version));
        let seen = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
        let seen_cloned = seen.clone();

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );

        txn.set_commit_callback(move |info, err| {
            seen_cloned
                .lock()
                .unwrap()
                .push((info.to_owned(), err.map(|err| err.to_string())));
        });

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);

        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let (info_json, err) = &seen[0];
        assert!(err.is_none());

        let info: serde_json::Value = serde_json::from_str(info_json).unwrap();
        assert_eq!(
            info.get("txn_scope").and_then(|value| value.as_str()),
            Some("global")
        );
        assert_eq!(
            info.get("start_ts").and_then(|value| value.as_u64()),
            Some(start_version)
        );
        assert_eq!(
            info.get("commit_ts").and_then(|value| value.as_u64()),
            Some(commit_version)
        );
        assert_eq!(
            info.get("txn_commit_mode").and_then(|value| value.as_str()),
            Some("2pc")
        );
        assert_eq!(
            info.get("async_commit_fallback")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            info.get("one_pc_fallback")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        assert!(info.get("error").is_none());
        assert_eq!(
            info.get("pipelined").and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            info.get("flush_wait_ms").and_then(|value| value.as_i64()),
            Some(0)
        );
    }

    #[tokio::test]
    async fn test_commit_wait_until_tso_waits_for_pd_tso() {
        let start_version = 7;
        let first_commit_version = 8;
        let commit_wait_until = 10;
        let expected_commit_version = 11;

        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, expected_commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(first_commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        assert_eq!(txn.start_ts(), start_version);
        assert_eq!(txn.commit_ts(), 0);
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        assert!(!txn.is_read_only());
        txn.set_commit_wait_until_tso(commit_wait_until);
        txn.set_commit_wait_until_tso_timeout(Duration::from_millis(50));

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), expected_commit_version);
        assert_eq!(txn.commit_ts(), expected_commit_version);
        assert_eq!(
            txn.commit_timestamp()
                .expect("expected commit ts stored on transaction")
                .version(),
            expected_commit_version
        );
        assert!(!txn.valid());
        assert_eq!(pd_client.get_timestamp_call_count(), 4);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_get_timestamp_for_commit_respects_commit_wait_until_tso() {
        let start_version = 7;
        let first_commit_version = 8;
        let commit_wait_until = 10;
        let expected_commit_version = 11;

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::default()).with_tso_sequence(first_commit_version),
        );

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_commit_wait_until_tso(commit_wait_until);
        txn.set_commit_wait_until_tso_timeout(Duration::from_millis(50));

        let commit_ts = txn.get_timestamp_for_commit().await.unwrap();
        assert_eq!(commit_ts, expected_commit_version);
        assert_eq!(pd_client.get_timestamp_call_count(), 4);
    }

    #[tokio::test]
    async fn test_transaction_is_read_only_reflects_buffer_mutations() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert!(txn.is_read_only());

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        assert!(!txn.is_read_only());
    }

    #[tokio::test]
    async fn test_transaction_len_and_size_reflects_buffered_mutations() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert_eq!(txn.commit_ts(), 0);
        assert_eq!(txn.len(), 0);
        assert_eq!(txn.size(), 0);

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        assert_eq!(txn.len(), 1);
        assert_eq!(txn.size(), 2);

        txn.lock_keys(vec!["l".to_owned()]).await.unwrap();
        assert_eq!(txn.len(), 2);
        assert_eq!(txn.size(), 3);

        txn.delete("k".to_owned()).await.unwrap();
        assert_eq!(txn.len(), 2);
        assert_eq!(txn.size(), 2);
    }

    #[tokio::test]
    async fn test_transaction_memory_footprint_change_hook_reports_mem() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let mem_reported = Arc::new(AtomicU64::new(0));
        let mem_reported_cloned = mem_reported.clone();

        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert!(!txn.mem_hook_set());

        txn.set_memory_footprint_change_hook(move |mem| {
            mem_reported_cloned.store(mem, Ordering::SeqCst);
        });
        assert!(txn.mem_hook_set());
        assert_eq!(mem_reported.load(Ordering::SeqCst), 0);

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        let reported = mem_reported.load(Ordering::SeqCst);
        assert!(reported > 0);
        assert_eq!(reported, txn.mem());
        assert_eq!(txn.mem(), txn.size());

        txn.clear_memory_footprint_change_hook();
        assert!(!txn.mem_hook_set());
    }

    #[test]
    fn test_transaction_vars_set_and_get() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert_eq!(txn.vars().backoff_lock_fast_ms, 10);
        assert_eq!(txn.vars().backoff_weight, 2);
        assert!(txn.vars().killed.is_none());

        let vars = crate::Variables {
            backoff_lock_fast_ms: 123,
            backoff_weight: 7,
            killed: Some(Arc::new(std::sync::atomic::AtomicU32::new(0))),
        };
        txn.set_vars(vars.clone());

        assert_eq!(txn.vars().backoff_lock_fast_ms, 123);
        assert_eq!(txn.vars().backoff_weight, 7);
        assert!(txn.vars().killed.is_some());
    }

    #[tokio::test]
    async fn test_commit_wait_until_tso_timeout_returns_error() {
        let start_version = 7;
        let first_commit_version = 8;
        let commit_wait_until = 10;

        let prewrite_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                panic!("commit request should not be sent when commit-wait times out");
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(first_commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        txn.set_commit_wait_until_tso(commit_wait_until);
        txn.set_commit_wait_until_tso_timeout(Duration::ZERO);

        let err = txn
            .commit()
            .await
            .expect_err("expected commit-wait timeout error");
        assert!(
            matches!(err, Error::StringError(message) if message.contains("PD TSO '8' lags the expected timestamp '10'"))
        );
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_commit_ts_upper_bound_check_aborts_commit() {
        let start_version = 7;
        let first_commit_version = 8;

        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let prewrite_count_captured = prewrite_count.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                panic!("commit request should not be sent when commit-ts upper bound check fails");
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(first_commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        txn.set_commit_ts_upper_bound_check(|commit_ts| commit_ts < 8);

        let err = txn
            .commit()
            .await
            .expect_err("expected upper bound check error");
        assert!(
            matches!(err, Error::StringError(message) if message.contains("check commit ts upper bound fail"))
        );
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_commit_ts_upper_bound_check_disables_async_commit_and_one_pc() {
        let start_version = 7;
        let first_commit_version = 8;

        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert!(!req.use_async_commit);
                assert!(!req.try_one_pc);
                assert_eq!(req.min_commit_ts, 0);
                assert_eq!(req.max_commit_ts, 0);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, first_commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(first_commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_commit_ts_upper_bound_check(|_| true);
        txn.set_enable_async_commit(true);
        txn.set_enable_one_pc(true);
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), first_commit_version);
        assert_eq!(
            pd_client.get_timestamp_call_count(),
            1,
            "upper bound check must disable async/1PC min_commit_ts seeding"
        );
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_transaction_set_txn_scope_affects_pd_tso_selection() {
        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        assert_eq!(txn.txn_scope(), "global");
        txn.set_txn_scope("dc1");
        assert_eq!(txn.txn_scope(), "dc1");

        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();
        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_transaction_set_enable_async_commit_one_pc_and_causal_consistency() {
        let start_version = 7;
        let one_pc_commit_version = 9;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert_eq!(req.start_version, start_version);
                assert!(req.use_async_commit);
                assert!(req.try_one_pc);
                assert_eq!(req.min_commit_ts, start_version.saturating_add(1));
                assert!(req.max_commit_ts > 0);

                let resp = kvrpcpb::PrewriteResponse {
                    one_pc_commit_ts: one_pc_commit_version,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_enable_async_commit(true);
        txn.set_enable_one_pc(true);
        txn.set_causal_consistency(true);

        txn.put(vec![1u8], vec![42u8]).await.unwrap();
        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), one_pc_commit_version);
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_transaction_assertion_level_propagates_to_prewrite_request() {
        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.assertion_level, AssertionLevel::Strict as i32);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(commit_version));
        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client,
            TransactionOptions::new_optimistic()
                .assertion_level(AssertionLevel::Strict)
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);
    }

    #[tokio::test]
    async fn test_prewrite_encounter_lock_policy_no_resolve_skips_lock_resolution_rpcs() {
        let start_version = 7;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                assert_eq!(req.start_version, start_version);
                let resp = kvrpcpb::PrewriteResponse {
                    errors: vec![kvrpcpb::KeyError {
                        locked: Some(kvrpcpb::LockInfo {
                            key: vec![1],
                            primary_lock: vec![1],
                            lock_version: start_version.saturating_sub(1),
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

            if req
                .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                .is_some()
                || req.downcast_ref::<kvrpcpb::ResolveLockRequest>().is_some()
            {
                panic!(
                    "lock resolution RPC should not be sent when NoResolve policy is configured"
                );
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client));
        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client,
            TransactionOptions::new_optimistic()
                .drop_check(CheckLevel::None)
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::Disable,
        );
        txn.set_prewrite_encounter_lock_policy(PrewriteEncounterLockPolicy::NoResolve);
        txn.put(vec![1u8], b"v1".to_vec()).await.unwrap();

        txn.commit()
            .await
            .expect_err("expected commit to fail on lock without resolving");
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
    async fn test_pessimistic_lock_on_optimistic_transaction_returns_error() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = txn
            .pessimistic_lock(std::iter::once(Key::from(b"k".to_vec())), false)
            .await
            .expect_err("pessimistic_lock should reject optimistic transactions");
        assert!(matches!(err, Error::InvalidTransactionType));
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

                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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
        assert!(matches!(err, Error::LockAcquireFailAndNoWaitSet));

        assert_eq!(lock_requests.load(Ordering::SeqCst), 1);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_keys_with_wait_timeout_overrides_pessimistic_lock_request_wait_timeout() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                match attempt {
                    0 => assert_eq!(req.wait_timeout, -1),
                    1 => assert_eq!(req.wait_timeout, i64::MAX),
                    _ => panic!("unexpected lock request count {attempt}"),
                }
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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

        txn.lock_keys_with_wait_timeout(vec![b"k1".to_vec()], crate::LockWaitTimeout::NoWait)
            .await
            .unwrap();
        txn.lock_keys(vec![b"k2".to_vec()]).await.unwrap();

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_lock_keys_in_share_mode_requires_primary_key_for_pessimistic_txn() {
        let mut txn = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = txn
            .lock_keys_in_share_mode(vec![b"k1".to_vec()])
            .await
            .expect_err("shared lock without primary key should error");
        match err {
            Error::StringError(message) => {
                assert!(message.contains(
                    "pessimistic lock in share mode requires primary key to be selected"
                ));
            }
            err => panic!("unexpected error: {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_lock_keys_in_share_mode_sends_shared_pessimistic_lock_and_forbids_upgrade() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.mutations.len(), 1);
                match attempt {
                    0 => assert_eq!(req.mutations[0].op, kvrpcpb::Op::PessimisticLock as i32),
                    1 => {
                        assert_eq!(
                            req.mutations[0].op,
                            kvrpcpb::Op::SharedPessimisticLock as i32
                        );
                    }
                    _ => panic!("unexpected lock request count {attempt}"),
                }
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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

        txn.lock_keys(vec![b"k2".to_vec()]).await.unwrap();
        txn.lock_keys_in_share_mode(vec![b"k1".to_vec()])
            .await
            .unwrap();

        let mutations = txn.buffer.to_proto_mutations();
        let mut lock_op = None;
        let mut shared_lock_op = None;
        for mutation in mutations {
            if mutation.key == b"k1".to_vec() {
                shared_lock_op = Some(mutation.op);
            } else if mutation.key == b"k2".to_vec() {
                lock_op = Some(mutation.op);
            }
        }
        assert_eq!(shared_lock_op, Some(kvrpcpb::Op::SharedLock as i32));
        assert_eq!(lock_op, Some(kvrpcpb::Op::Lock as i32));

        let err = txn
            .lock_keys(vec![b"k1".to_vec()])
            .await
            .expect_err("upgrading shared lock should error");
        match err {
            Error::StringError(message) => {
                assert!(message
                    .contains("upgrading a shared lock to an exclusive lock is not supported"))
            }
            err => panic!("unexpected error: {err:?}"),
        }

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_get_for_share_sends_shared_pessimistic_lock_and_returns_value() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.mutations.len(), 1);
                match attempt {
                    0 => {
                        assert_eq!(req.mutations[0].key, b"k2".to_vec());
                        assert_eq!(req.mutations[0].op, kvrpcpb::Op::PessimisticLock as i32);
                        assert!(!req.return_values);
                        let resp = ok_pessimistic_lock_response_for_request(req);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    1 => {
                        assert_eq!(req.mutations[0].key, b"k1".to_vec());
                        assert_eq!(
                            req.mutations[0].op,
                            kvrpcpb::Op::SharedPessimisticLock as i32
                        );
                        assert!(req.return_values);
                        let resp = kvrpcpb::PessimisticLockResponse {
                            values: vec![b"v1".to_vec()],
                            not_founds: vec![false],
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    _ => panic!("unexpected lock request count {attempt}"),
                }
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

        txn.lock_keys(vec![b"k2".to_vec()]).await.unwrap();
        let value = txn
            .get_for_share(b"k1".to_vec())
            .await
            .unwrap()
            .expect("expected value for get_for_share");
        assert_eq!(value, b"v1".to_vec());

        let mutations = txn.buffer.to_proto_mutations();
        assert!(mutations.iter().any(|mutation| {
            mutation.key == b"k1".to_vec() && mutation.op == kvrpcpb::Op::SharedLock as i32
        }));

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_batch_get_for_share_sends_shared_pessimistic_lock_and_returns_values() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                match attempt {
                    0 => {
                        assert_eq!(req.mutations.len(), 1);
                        assert_eq!(req.mutations[0].key, b"k2".to_vec());
                        assert_eq!(req.mutations[0].op, kvrpcpb::Op::PessimisticLock as i32);
                        assert!(!req.return_values);
                        let resp = ok_pessimistic_lock_response_for_request(req);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    1 => {
                        assert_eq!(req.mutations.len(), 2);
                        for mutation in &req.mutations {
                            assert_eq!(mutation.op, kvrpcpb::Op::SharedPessimisticLock as i32);
                        }
                        assert!(req.return_values);

                        let resp = kvrpcpb::PessimisticLockResponse {
                            values: vec![b"v1".to_vec(), b"v3".to_vec()],
                            not_founds: vec![false, false],
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    _ => panic!("unexpected lock request count {attempt}"),
                }
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

        txn.lock_keys(vec![b"k2".to_vec()]).await.unwrap();
        let pairs = txn
            .batch_get_for_share(vec![b"k1".to_vec(), b"k3".to_vec()])
            .await
            .unwrap();
        assert!(pairs.contains(&crate::KvPair::new(b"k1".to_vec(), b"v1".to_vec())));
        assert!(pairs.contains(&crate::KvPair::new(b"k3".to_vec(), b"v3".to_vec())));

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_wait_timeout_returns_error() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req
                .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                .is_some()
            {
                lock_requests_captured.fetch_add(1, Ordering::SeqCst);
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
                .lock_wait_timeout(crate::LockWaitTimeout::Wait(Duration::from_millis(1)))
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let request = crate::transaction::lowering::new_pessimistic_lock_request(
            std::iter::once(Key::from(vec![1u8])),
            Key::from(vec![1u8]),
            Timestamp::default(),
            100,
            Timestamp::default(),
            false,
            true,
        );

        let lock_wait_start = Instant::now() - Duration::from_millis(10);
        let err = txn
            .execute_pessimistic_lock_request(
                request,
                Timestamp::default(),
                crate::LockWaitTimeout::Wait(Duration::from_millis(1)),
                lock_wait_start,
                PessimisticLockMode::Exclusive,
                false,
            )
            .await
            .expect_err("expected wait-timeout lock error");
        assert!(matches!(err, Error::LockWaitTimeout));

        assert_eq!(lock_requests.load(Ordering::SeqCst), 1);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_deadlock_returns_error() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req
                .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                .is_some()
            {
                lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::PessimisticLockResponse {
                    errors: vec![kvrpcpb::KeyError {
                        deadlock: Some(kvrpcpb::Deadlock {
                            lock_ts: 7,
                            lock_key: vec![1],
                            deadlock_key_hash: 42,
                            wait_chain: Vec::new(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req
                .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                .is_some()
            {
                check_txn_status_requests_captured.fetch_add(1, Ordering::SeqCst);
                panic!("check_txn_status must not be invoked for deadlock errors");
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
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let request = crate::transaction::lowering::new_pessimistic_lock_request(
            std::iter::once(Key::from(vec![1u8])),
            Key::from(vec![1u8]),
            Timestamp::default(),
            100,
            Timestamp::default(),
            false,
            true,
        );

        let err = txn
            .execute_pessimistic_lock_request(
                request,
                Timestamp::default(),
                crate::LockWaitTimeout::Default,
                Instant::now(),
                PessimisticLockMode::Exclusive,
                false,
            )
            .await
            .expect_err("expected deadlock error");

        match err {
            Error::Deadlock(deadlock) => {
                assert_eq!(deadlock.lock_ts(), 7);
                assert_eq!(deadlock.deadlock_key_hash(), 42);
                assert_eq!(deadlock.wait_chain_len(), 0);
            }
            other => panic!("expected deadlock error, got {other:?}"),
        }

        assert_eq!(lock_requests.load(Ordering::SeqCst), 1);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_already_exists_returns_error() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req
                .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                .is_some()
            {
                lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::PessimisticLockResponse {
                    errors: vec![kvrpcpb::KeyError {
                        already_exist: Some(kvrpcpb::AlreadyExist { key: vec![1] }),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req
                .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                .is_some()
            {
                check_txn_status_requests_captured.fetch_add(1, Ordering::SeqCst);
                panic!("check_txn_status must not be invoked for already-exist errors");
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
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let request = crate::transaction::lowering::new_pessimistic_lock_request(
            std::iter::once(Key::from(vec![1u8])),
            Key::from(vec![1u8]),
            Timestamp::default(),
            100,
            Timestamp::default(),
            false,
            true,
        );

        let err = txn
            .execute_pessimistic_lock_request(
                request,
                Timestamp::default(),
                crate::LockWaitTimeout::Default,
                Instant::now(),
                PessimisticLockMode::Exclusive,
                false,
            )
            .await
            .expect_err("expected already-exist error");
        assert!(matches!(err, Error::DuplicateKeyInsertion));

        assert_eq!(lock_requests.load(Ordering::SeqCst), 1);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_optimistic_insert_commit_already_exists_returns_error() {
        let start_version = 7;
        let commit_version = 8;

        let prewrite_requests = Arc::new(AtomicUsize::new(0));
        let prewrite_requests_captured = prewrite_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.mutations.len(), 1);
                assert_eq!(req.mutations[0].op, kvrpcpb::Op::Insert as i32);

                let resp = kvrpcpb::PrewriteResponse {
                    errors: vec![kvrpcpb::KeyError {
                        already_exist: Some(kvrpcpb::AlreadyExist {
                            key: req.mutations[0].key.clone(),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                panic!("commit request must not be sent for already-exist prewrite errors");
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(commit_version));

        let mut txn = Transaction::new(
            Timestamp::from_version(start_version),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.insert("k".to_owned(), "v".to_owned()).await.unwrap();

        let err = txn
            .commit()
            .await
            .expect_err("expected already-exist error");
        assert!(
            matches!(err, Error::DuplicateKeyInsertion),
            "expected duplicate key insertion, got {err:?}"
        );
        assert_eq!(prewrite_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_skips_resolve_for_recently_updated_lock() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let attempt = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                match attempt {
                    0 => {
                        let resp = kvrpcpb::PessimisticLockResponse {
                            errors: vec![kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: vec![1],
                                    primary_lock: vec![1],
                                    lock_version: 7,
                                    lock_ttl: 100,
                                    lock_type: kvrpcpb::Op::Put as i32,
                                    duration_to_last_update_ms: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    1 => {
                        let resp = ok_pessimistic_lock_response_for_request(req);
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    _ => panic!("unexpected lock request count {attempt}"),
                }
            }

            if req
                .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
                .is_some()
            {
                check_txn_status_requests_captured.fetch_add(1, Ordering::SeqCst);
                panic!("check_txn_status must not be invoked for recently updated locks");
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
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let request = crate::transaction::lowering::new_pessimistic_lock_request(
            std::iter::once(Key::from(vec![1u8])),
            Key::from(vec![1u8]),
            Timestamp::default(),
            100,
            Timestamp::default(),
            false,
            true,
        );

        let result = txn
            .execute_pessimistic_lock_request(
                request,
                Timestamp::default(),
                crate::LockWaitTimeout::Wait(Duration::from_secs(1)),
                Instant::now(),
                PessimisticLockMode::Exclusive,
                false,
            )
            .await
            .expect("expected the lock request to eventually succeed");
        assert!(result.pairs.is_empty());

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_flattens_shared_lock_wrapper() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let check_txn_status_requests = Arc::new(AtomicUsize::new(0));
        let resolve_lock_requests = Arc::new(AtomicUsize::new(0));
        let timestamp_calls = Arc::new(AtomicUsize::new(0));

        let expected_disk_full_opt = DiskFullOpt::AllowedOnAlmostFull as i32;
        let expected_txn_source = 42_u64;
        let expected_priority = CommandPriority::High as i32;
        let expected_max_execution_duration_ms = 321_u64;
        let expected_resource_group_tag = b"rg-tag".to_vec();
        let expected_resource_group_name = "rg-name".to_owned();
        let expected_request_source = "request-source".to_owned();

        let embedded_lock_key = vec![1_u8];
        let embedded_lock_key_hook = embedded_lock_key.clone();
        let embedded_primary_lock = vec![99_u8];
        let embedded_primary_lock_hook = embedded_primary_lock.clone();

        let lock_requests_captured = lock_requests.clone();
        let check_txn_status_requests_captured = check_txn_status_requests.clone();
        let resolve_lock_requests_captured = resolve_lock_requests.clone();
        let expected_resource_group_tag_hook = expected_resource_group_tag.clone();
        let expected_resource_group_name_hook = expected_resource_group_name.clone();
        let expected_request_source_hook = expected_request_source.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let call = lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    let embedded = kvrpcpb::LockInfo {
                        key: embedded_lock_key_hook.clone(),
                        primary_lock: embedded_primary_lock_hook.clone(),
                        lock_version: 7,
                        lock_ttl: 100,
                        txn_size: 1,
                        lock_type: kvrpcpb::Op::Put as i32,
                        ..Default::default()
                    };
                    let wrapper = kvrpcpb::LockInfo {
                        lock_type: kvrpcpb::Op::SharedLock as i32,
                        shared_lock_infos: vec![embedded],
                        ..Default::default()
                    };
                    let resp = kvrpcpb::PessimisticLockResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(wrapper),
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                check_txn_status_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.primary_key, embedded_primary_lock_hook);
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.disk_full_opt, expected_disk_full_opt);
                assert_eq!(ctx.txn_source, expected_txn_source);
                assert!(ctx.sync_log);
                assert_eq!(ctx.priority, expected_priority);
                assert_eq!(
                    ctx.max_execution_duration_ms,
                    expected_max_execution_duration_ms
                );
                assert_eq!(ctx.resource_group_tag, expected_resource_group_tag_hook);
                assert_eq!(ctx.request_source, expected_request_source_hook);
                assert_eq!(
                    ctx.resource_control_context
                        .as_ref()
                        .expect("resource control context")
                        .resource_group_name,
                    expected_resource_group_name_hook
                );
                assert_eq!(ctx.region_id, 2);
                assert_eq!(ctx.peer.as_ref().expect("peer").store_id, 42);
                let resp = kvrpcpb::CheckTxnStatusResponse {
                    commit_version: 5,
                    action: kvrpcpb::Action::NoAction as i32,
                    ..Default::default()
                };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                resolve_lock_requests_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, 7);
                assert_eq!(req.commit_version, 5);
                assert_eq!(req.keys, vec![embedded_lock_key_hook.clone()]);
                let ctx = req.context.as_ref().expect("context");
                assert_eq!(ctx.disk_full_opt, expected_disk_full_opt);
                assert_eq!(ctx.txn_source, expected_txn_source);
                assert!(ctx.sync_log);
                assert_eq!(ctx.priority, expected_priority);
                assert_eq!(
                    ctx.max_execution_duration_ms,
                    expected_max_execution_duration_ms
                );
                assert_eq!(ctx.resource_group_tag, expected_resource_group_tag_hook);
                assert_eq!(ctx.request_source, expected_request_source_hook);
                assert_eq!(
                    ctx.resource_control_context
                        .as_ref()
                        .expect("resource control context")
                        .resource_group_name,
                    expected_resource_group_name_hook
                );
                assert_eq!(ctx.region_id, 1);
                assert_eq!(ctx.peer.as_ref().expect("peer").store_id, 41);
                return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
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
                .disk_full_opt(DiskFullOpt::AllowedOnAlmostFull)
                .txn_source(expected_txn_source)
                .sync_log(true)
                .priority(CommandPriority::High)
                .max_write_execution_duration(Duration::from_millis(
                    expected_max_execution_duration_ms,
                ))
                .resource_group_tag(expected_resource_group_tag.clone())
                .resource_group_name(expected_resource_group_name.clone())
                .request_source(expected_request_source.clone())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.lock_keys(vec![embedded_lock_key]).await.unwrap();

        assert_eq!(lock_requests.load(Ordering::SeqCst), 2);
        assert_eq!(check_txn_status_requests.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_pessimistic_lock_selects_smallest_key_as_primary() {
        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(req.primary_lock, vec![1]);
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
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
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    Ok(Box::new(resp) as Box<dyn Any>)
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

    #[tokio::test]
    async fn test_snapshot_get_with_commit_ts_returns_commit_ts() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert!(req.need_commit_ts);

                let mut resp = kvrpcpb::GetResponse::default();
                resp.value = b"v".to_vec();
                resp.not_found = false;
                resp.commit_ts = 123;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let value = snapshot.get_with_commit_ts(b"k".to_vec()).await.unwrap();
        assert_eq!(value, Some((b"v".to_vec(), 123)));
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_get_with_commit_ts_returns_error_when_commit_ts_not_returned() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                assert!(req.need_commit_ts);

                let mut resp = kvrpcpb::GetResponse::default();
                resp.value = b"v".to_vec();
                resp.not_found = false;
                resp.commit_ts = 0;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = snapshot
            .get_with_commit_ts(b"k".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::CommitTsRequiredButNotReturned));
    }

    #[tokio::test]
    async fn test_get_with_commit_ts_requires_read_only_snapshots() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            panic!("should not send requests when get_with_commit_ts is not supported");
        })));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert!(matches!(
            txn.get_with_commit_ts(b"k".to_vec()).await,
            Err(Error::StringError(msg))
                if msg == "get_with_commit_ts is only supported for read-only snapshots"
        ));
    }

    #[tokio::test]
    async fn test_snapshot_batch_get_with_commit_ts_returns_commit_ts() {
        let batch_get_calls = Arc::new(AtomicUsize::new(0));
        let batch_get_calls_cloned = batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::BatchGetRequest>()
                    .expect("expected batch get request");
                batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert!(req.need_commit_ts);

                let mut resp = kvrpcpb::BatchGetResponse::default();
                for key in &req.keys {
                    let mut pair = kvrpcpb::KvPair::default();
                    pair.key = key.clone();
                    pair.value = b"v".to_vec();
                    pair.commit_ts = 123;
                    resp.pairs.push(pair);
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let mut results = snapshot
            .batch_get_with_commit_ts(vec![b"k1".to_vec(), b"k2".to_vec()])
            .await
            .unwrap()
            .collect::<Vec<_>>();
        results.sort_by_key(|(pair, _)| pair.0.clone());

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].0,
            crate::KvPair::new(b"k1".to_vec(), b"v".to_vec())
        );
        assert_eq!(results[0].1, 123);
        assert_eq!(
            results[1].0,
            crate::KvPair::new(b"k2".to_vec(), b"v".to_vec())
        );
        assert_eq!(results[1].1, 123);
        assert_eq!(batch_get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_batch_get_with_commit_ts_returns_error_when_commit_ts_not_returned() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::BatchGetRequest>()
                    .expect("expected batch get request");
                assert!(req.need_commit_ts);

                let mut resp = kvrpcpb::BatchGetResponse::default();
                let mut pair = kvrpcpb::KvPair::default();
                pair.key = req.keys[0].clone();
                pair.value = b"v".to_vec();
                pair.commit_ts = 0;
                resp.pairs.push(pair);
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let err = snapshot
            .batch_get_with_commit_ts(vec![b"k1".to_vec()])
            .await
            .err()
            .expect("expected error");
        assert!(matches!(err, Error::CommitTsRequiredButNotReturned));
    }

    #[tokio::test]
    async fn test_batch_get_with_commit_ts_requires_read_only_snapshots() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            panic!("should not send requests when batch_get_with_commit_ts is not supported");
        })));

        let mut txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert!(matches!(
            txn.batch_get_with_commit_ts(vec![b"k".to_vec()]).await,
            Err(Error::StringError(msg))
                if msg == "batch_get_with_commit_ts is only supported for read-only snapshots"
        ));
    }

    #[tokio::test]
    async fn test_aggressive_locking_single_key_uses_force_lock_wake_up_mode() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                lock_requests_captured.lock().unwrap().push(req.clone());
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            Err(Error::StringError("unexpected request".to_owned()))
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(100));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 1);
        assert_eq!(
            lock_requests[0].wake_up_mode,
            kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
        );
    }

    #[tokio::test]
    async fn test_aggressive_locking_records_locked_with_conflict_ts_for_prewrite_constraints() {
        let prewrite_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PrewriteRequest>::new()));
        let prewrite_requests_captured = prewrite_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(
                    req.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                );
                assert_eq!(req.for_update_ts, 100);
                assert_eq!(req.mutations.len(), 1);
                let mut resp = kvrpcpb::PessimisticLockResponse::default();
                resp.results.push(kvrpcpb::PessimisticLockKeyResult {
                    r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                        .into(),
                    existence: true,
                    locked_with_conflict_ts: 200,
                    ..Default::default()
                });
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_requests_captured.lock().unwrap().push(req.clone());
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }
            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }
            Err(Error::StringError("unexpected request".to_owned()))
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(100));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();
        txn.done_aggressive_locking().await.unwrap();

        txn.commit().await.unwrap();

        let prewrite_requests = prewrite_requests.lock().unwrap().clone();
        assert_eq!(prewrite_requests.len(), 1);

        let req = &prewrite_requests[0];
        assert_eq!(req.for_update_ts, 100);
        assert_eq!(
            req.for_update_ts_constraints,
            vec![kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                index: 0,
                expected_for_update_ts: 200,
            }]
        );
    }

    #[tokio::test]
    async fn test_aggressive_locking_cancel_uses_max_locked_with_conflict_ts_for_rollback() {
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));
        let rollback_requests_captured = rollback_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(
                    req.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                );
                assert_eq!(req.for_update_ts, 100);
                let mut resp = kvrpcpb::PessimisticLockResponse::default();
                resp.results.push(kvrpcpb::PessimisticLockKeyResult {
                    r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                        .into(),
                    existence: true,
                    locked_with_conflict_ts: 200,
                    ..Default::default()
                });
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                rollback_requests_captured.lock().unwrap().push(req.clone());
                return Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>);
            }
            Err(Error::StringError("unexpected request".to_owned()))
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(100));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();
        txn.cancel_aggressive_locking().await.unwrap();

        let rollback_requests = rollback_requests.lock().unwrap().clone();
        assert_eq!(rollback_requests.len(), 1);
        assert_eq!(rollback_requests[0].for_update_ts, 200);
    }

    #[tokio::test]
    async fn test_aggressive_locking_retry_rejects_for_update_ts_before_expected_ts() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let lock_requests_captured = lock_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                lock_requests_captured.lock().unwrap().push(req.clone());
                let mut resp = kvrpcpb::PessimisticLockResponse::default();
                resp.results.push(kvrpcpb::PessimisticLockKeyResult {
                    r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                        .into(),
                    existence: true,
                    locked_with_conflict_ts: 200,
                    ..Default::default()
                });
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            Err(Error::StringError("unexpected request".to_owned()))
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(100));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();
        txn.retry_aggressive_locking().await.unwrap();

        let err = txn
            .lock_keys(vec![b"k1".to_vec()])
            .await
            .expect_err("expected error");
        assert!(
            matches!(err, Error::StringError(msg) if msg == "txn 5 retrying aggressive locking with for_update_ts (101) less than previous expected_for_update_ts (200)")
        );

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_aggressive_locking_skips_redundant_lock_requests_and_rolls_back_unneeded_locks() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));

        let lock_requests_captured = lock_requests.clone();
        let rollback_requests_captured = rollback_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    lock_requests_captured.lock().unwrap().push(req.clone());
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_requests_captured.lock().unwrap().push(req.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec(), b"k2".to_vec()])
            .await
            .unwrap();

        txn.retry_aggressive_locking().await.unwrap();

        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();

        txn.done_aggressive_locking().await.unwrap();
        assert!(!txn.is_in_aggressive_locking_mode());

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 1);
        let locked_keys = lock_requests[0]
            .mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(locked_keys, vec![b"k1".to_vec(), b"k2".to_vec()]);

        let rollback_requests = rollback_requests.lock().unwrap().clone();
        assert_eq!(rollback_requests.len(), 1);
        assert_eq!(rollback_requests[0].keys, vec![b"k2".to_vec()]);
    }

    #[tokio::test]
    async fn test_aggressive_locking_primary_key_change_forces_relock() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));

        let lock_requests_captured = lock_requests.clone();
        let rollback_requests_captured = rollback_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    lock_requests_captured.lock().unwrap().push(req.clone());
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_requests_captured.lock().unwrap().push(req.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k2".to_vec(), b"k3".to_vec()])
            .await
            .unwrap();
        txn.retry_aggressive_locking().await.unwrap();
        txn.lock_keys(vec![b"k3".to_vec()]).await.unwrap();
        txn.done_aggressive_locking().await.unwrap();

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 2);
        assert_eq!(lock_requests[0].primary_lock, b"k2".to_vec());
        assert_eq!(lock_requests[1].primary_lock, b"k3".to_vec());

        let locked_keys = lock_requests[1]
            .mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(locked_keys, vec![b"k3".to_vec()]);

        let rollback_requests = rollback_requests.lock().unwrap().clone();
        assert_eq!(rollback_requests.len(), 1);
        assert_eq!(rollback_requests[0].keys, vec![b"k2".to_vec()]);

        assert_eq!(
            txn.buffer.get_primary_key(),
            Some(Key::from(b"k3".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_aggressive_locking_prefers_previous_primary_key_when_possible() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));

        let lock_requests_captured = lock_requests.clone();
        let rollback_requests_captured = rollback_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    lock_requests_captured.lock().unwrap().push(req.clone());
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_requests_captured.lock().unwrap().push(req.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k2".to_vec(), b"k3".to_vec()])
            .await
            .unwrap();
        txn.retry_aggressive_locking().await.unwrap();
        txn.lock_keys(vec![b"k1".to_vec(), b"k2".to_vec()])
            .await
            .unwrap();
        txn.done_aggressive_locking().await.unwrap();

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 2);

        // Primary stays as `k2` (from the previous attempt) even though `k1` sorts first.
        assert_eq!(lock_requests[1].primary_lock, b"k2".to_vec());

        // We only need to lock the new key (`k1`) because `k2` can be reused.
        let locked_keys = lock_requests[1]
            .mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(locked_keys, vec![b"k1".to_vec()]);

        let rollback_requests = rollback_requests.lock().unwrap().clone();
        assert_eq!(rollback_requests.len(), 1);
        assert_eq!(rollback_requests[0].keys, vec![b"k3".to_vec()]);
    }

    #[tokio::test]
    async fn test_aggressive_locking_does_not_skip_locks_when_previous_attempt_may_expire() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));

        let lock_requests_captured = lock_requests.clone();
        let rollback_requests_captured = rollback_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    lock_requests_captured.lock().unwrap().push(req.clone());
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_requests_captured.lock().unwrap().push(req.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec(), b"k2".to_vec()])
            .await
            .unwrap();
        txn.retry_aggressive_locking().await.unwrap();

        if let Some(state) = txn.aggressive_locking.as_mut() {
            state.last_attempt_start = Instant::now()
                - Duration::from_millis(super::MAX_TTL).saturating_add(Duration::from_millis(1));
        }

        // Even though `k1` was locked in the previous attempt, we should re-lock it when the
        // previous attempt's locks may have expired.
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();
        txn.done_aggressive_locking().await.unwrap();

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 2);
        let locked_keys = lock_requests[1]
            .mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(locked_keys, vec![b"k1".to_vec()]);
    }

    #[tokio::test]
    async fn test_cancel_aggressive_locking_rolls_back_locks_and_clears_primary_key() {
        let lock_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticLockRequest>::new()));
        let rollback_requests =
            Arc::new(Mutex::new(Vec::<kvrpcpb::PessimisticRollbackRequest>::new()));

        let lock_requests_captured = lock_requests.clone();
        let rollback_requests_captured = rollback_requests.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    lock_requests_captured.lock().unwrap().push(req.clone());
                    let resp = ok_pessimistic_lock_response_for_request(req);
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_requests_captured.lock().unwrap().push(req.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                Err(Error::StringError("unexpected request".to_owned()))
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();

        assert_eq!(
            txn.buffer.get_primary_key(),
            Some(Key::from(b"k1".to_vec()))
        );

        txn.cancel_aggressive_locking().await.unwrap();
        assert!(!txn.is_in_aggressive_locking_mode());
        assert_eq!(txn.buffer.get_primary_key(), None);

        let lock_requests = lock_requests.lock().unwrap().clone();
        assert_eq!(lock_requests.len(), 1);

        let rollback_requests = rollback_requests.lock().unwrap().clone();
        assert_eq!(rollback_requests.len(), 1);
        assert_eq!(rollback_requests[0].keys, vec![b"k1".to_vec()]);
    }

    #[tokio::test]
    async fn test_aggressive_locking_populates_prewrite_for_update_ts_constraints() {
        let prewrite_requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PrewriteRequest>::new()));
        let prewrite_requests_captured = prewrite_requests.clone();

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                let resp = ok_pessimistic_lock_response_for_request(req);
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_requests_captured.lock().unwrap().push(req.clone());
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }
            if req.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }
            Err(Error::StringError("unexpected request".to_owned()))
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(100));

        let mut txn = Transaction::new(
            Timestamp::from_version(5),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        txn.start_aggressive_locking().unwrap();
        txn.lock_keys(vec![b"k1".to_vec()]).await.unwrap();
        txn.retry_aggressive_locking().await.unwrap();
        txn.lock_keys(vec![b"k1".to_vec(), b"k2".to_vec()])
            .await
            .unwrap();
        txn.done_aggressive_locking().await.unwrap();

        txn.commit().await.unwrap();

        let prewrite_requests = prewrite_requests.lock().unwrap().clone();
        assert_eq!(prewrite_requests.len(), 1);

        let req = &prewrite_requests[0];
        assert_eq!(req.for_update_ts, 101);
        assert_eq!(
            req.for_update_ts_constraints,
            vec![
                kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                    index: 0,
                    expected_for_update_ts: 100,
                },
                kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                    index: 1,
                    expected_for_update_ts: 101,
                }
            ]
        );
    }
}
