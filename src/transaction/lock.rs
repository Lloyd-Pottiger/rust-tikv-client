// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fail::fail_point;
use log::debug;
use log::error;
use log::warn;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::pd::PdClient;

use crate::proto::kvrpcpb;
use crate::proto::kvrpcpb::TxnInfo;
use crate::proto::pdpb::Timestamp;
use crate::region::RegionVerId;
use crate::request::plan::handle_region_error;
use crate::request::plan::is_grpc_error;
use crate::request::Collect;
use crate::request::CollectSingle;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::rpc_interceptor::RpcInterceptors;
use crate::store::RegionStore;
use crate::store::Request;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::transaction::requests::new_check_secondary_locks_request;
use crate::transaction::requests::new_check_txn_status_request;
use crate::transaction::requests::SecondaryLocksStatus;
use crate::transaction::requests::TransactionStatus;
use crate::transaction::requests::TransactionStatusKind;
use crate::Error;
use crate::Result;

fn check_killed(killed: &Option<Arc<AtomicU32>>) -> Result<()> {
    let Some(killed) = killed.as_ref() else {
        return Ok(());
    };
    let killed_signal = killed.load(Ordering::SeqCst);
    if killed_signal == 0 {
        Ok(())
    } else {
        Err(Error::StringError(format!(
            "query interrupted by signal {killed_signal}"
        )))
    }
}

fn format_key_for_log(key: &[u8]) -> String {
    let prefix_len = key.len().min(16);
    format!("len={}, prefix={:?}", key.len(), &key[..prefix_len])
}

// `client-go` treats both `PessimisticLock` and `SharedPessimisticLock` as pessimistic.
const SHARED_LOCK_TYPE: i32 = kvrpcpb::Op::SharedLock as i32;
const SHARED_PESSIMISTIC_LOCK_TYPE: i32 = kvrpcpb::Op::SharedPessimisticLock as i32;

fn is_pessimistic_lock(lock_type: i32) -> bool {
    lock_type == kvrpcpb::Op::PessimisticLock as i32 || lock_type == SHARED_PESSIMISTIC_LOCK_TYPE
}

fn is_shared_lock(lock_type: i32) -> bool {
    lock_type == SHARED_LOCK_TYPE
}

fn pessimistic_rollback_for_update_ts(lock: &kvrpcpb::LockInfo) -> u64 {
    if lock.lock_for_update_ts == 0 {
        // Match client-go behavior: lock info from mismatch paths may miss for_update_ts.
        u64::MAX
    } else {
        lock.lock_for_update_ts
    }
}

fn select_check_txn_status_error(mut errors: Vec<Error>) -> Error {
    // Retry plans can preserve intermediate non-key errors before the final key error.
    // For `CheckTxnStatus`, we need key-error details (`txn_not_found` / `primary_mismatch`)
    // whenever available, regardless of vector ordering.
    if let Some(index) = errors
        .iter()
        .rposition(|err| matches!(err, Error::KeyError(_)))
    {
        return errors.swap_remove(index);
    }

    errors
        .pop()
        .unwrap_or_else(|| empty_extracted_errors("check_txn_status"))
}

fn empty_extracted_errors(operation: &str) -> Error {
    Error::StringError(format!("{operation} returned empty extracted errors"))
}

enum TxnStatusFromLock {
    Status(Arc<TransactionStatus>),
    PessimisticRollbackRequired,
}

/// The result of a one-shot lock-resolve attempt.
///
/// This mirrors the result shape of client-go `LockResolver.ResolveLocksWithOpts`/`ResolveLocks`:
/// `ms_before_txn_expired` is returned so the caller can choose whether to sleep/backoff and retry.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ResolveLocksResult {
    /// Locks that are still live after this attempt.
    pub live_locks: Vec<kvrpcpb::LockInfo>,
    /// The minimum remaining time (in milliseconds) before any still-live lock expires.
    ///
    /// Returns 0 when there is no live lock or the lock is already expired.
    pub ms_before_txn_expired: i64,
}

/// The result of resolving locks for read.
///
/// This mirrors client-go `LockResolver.ResolveLocksForRead` behavior:
/// - `resolved_locks` contains transaction IDs that can be ignored by subsequent reads
///   (`kvrpcpb::Context.resolved_locks`).
/// - `committed_locks` contains transaction IDs that can be accessed by subsequent reads
///   (`kvrpcpb::Context.committed_locks`).
/// - `ms_before_txn_expired` is the minimum remaining TTL (in milliseconds) among any still-live
///   locks (0 when there is no live lock or the lock is already expired).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ResolveLocksForReadResult {
    pub ms_before_txn_expired: i64,
    pub live_locks: Vec<kvrpcpb::LockInfo>,
    pub resolved_locks: Vec<u64>,
    pub committed_locks: Vec<u64>,
}

/// Runtime stats for lock resolution.
///
/// This mirrors client-go `util.ResolveLockDetail` and currently tracks only the total
/// wall-clock time spent resolving locks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResolveLockDetail {
    pub resolve_lock_time: Duration,
}

#[derive(Debug, Default)]
pub(crate) struct ResolveLockDetailCollector {
    resolve_lock_time_ns: AtomicU64,
}

impl ResolveLockDetailCollector {
    pub(crate) fn add_resolve_lock_time(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.resolve_lock_time_ns
            .fetch_add(nanos, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ResolveLockDetail {
        let nanos = self.resolve_lock_time_ns.load(Ordering::Relaxed);
        ResolveLockDetail {
            resolve_lock_time: Duration::from_nanos(nanos),
        }
    }
}

struct ResolveLockTimeGuard {
    detail: Option<Arc<ResolveLockDetailCollector>>,
    started_at: Instant,
}

impl ResolveLockTimeGuard {
    fn new(detail: Option<Arc<ResolveLockDetailCollector>>) -> Self {
        Self {
            detail,
            started_at: Instant::now(),
        }
    }
}

impl Drop for ResolveLockTimeGuard {
    fn drop(&mut self) {
        let Some(detail) = self.detail.as_ref() else {
            return;
        };
        detail.add_resolve_lock_time(self.started_at.elapsed());
    }
}

/// A decoded lock returned by TiKV.
///
/// This is a thin wrapper around [`kvrpcpb::LockInfo`] that provides convenient accessors and
/// helper predicates, mirroring client-go `txnkv.Lock` and `txnkv.NewLock`.
#[derive(Clone, Debug, PartialEq)]
pub struct Lock {
    inner: kvrpcpb::LockInfo,
}

/// Create a [`Lock`] from a protobuf [`kvrpcpb::LockInfo`].
///
/// This mirrors client-go `txnkv.NewLock`.
#[must_use]
pub fn new_lock(lock: kvrpcpb::LockInfo) -> Lock {
    Lock::new(lock)
}

impl Lock {
    #[must_use]
    pub fn new(inner: kvrpcpb::LockInfo) -> Lock {
        Lock { inner }
    }

    #[must_use]
    pub fn as_proto(&self) -> &kvrpcpb::LockInfo {
        &self.inner
    }

    #[must_use]
    pub fn into_proto(self) -> kvrpcpb::LockInfo {
        self.inner
    }

    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.inner.key
    }

    #[must_use]
    pub fn primary(&self) -> &[u8] {
        &self.inner.primary_lock
    }

    /// The transaction start TS (`lock_version`) of this lock.
    #[must_use]
    pub fn txn_id(&self) -> u64 {
        self.inner.lock_version
    }

    #[must_use]
    pub fn ttl(&self) -> u64 {
        self.inner.lock_ttl
    }

    #[must_use]
    pub fn txn_size(&self) -> u64 {
        self.inner.txn_size
    }

    #[must_use]
    pub fn lock_type(&self) -> i32 {
        self.inner.lock_type
    }

    #[must_use]
    pub fn use_async_commit(&self) -> bool {
        self.inner.use_async_commit
    }

    #[must_use]
    pub fn lock_for_update_ts(&self) -> u64 {
        self.inner.lock_for_update_ts
    }

    #[must_use]
    pub fn min_commit_ts(&self) -> u64 {
        self.inner.min_commit_ts
    }

    #[must_use]
    pub fn secondaries(&self) -> &[Vec<u8>] {
        &self.inner.secondaries
    }

    #[must_use]
    pub fn is_txn_file(&self) -> bool {
        self.inner.is_txn_file
    }

    /// Returns `true` if this lock is pessimistic (`PessimisticLock` or `SharedPessimisticLock`).
    #[must_use]
    pub fn is_pessimistic(&self) -> bool {
        is_pessimistic_lock(self.inner.lock_type)
    }

    /// Returns `true` if this lock is a shared-lock wrapper (`SharedLock`).
    #[must_use]
    pub fn is_shared(&self) -> bool {
        is_shared_lock(self.inner.lock_type)
    }
}

impl AsRef<kvrpcpb::LockInfo> for Lock {
    fn as_ref(&self) -> &kvrpcpb::LockInfo {
        self.as_proto()
    }
}

impl From<kvrpcpb::LockInfo> for Lock {
    fn from(lock: kvrpcpb::LockInfo) -> Lock {
        Lock::new(lock)
    }
}

impl From<Lock> for kvrpcpb::LockInfo {
    fn from(lock: Lock) -> kvrpcpb::LockInfo {
        lock.into_proto()
    }
}

impl std::fmt::Display for Lock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lock_type = match self.inner.lock_type {
            x if x == kvrpcpb::Op::Put as i32 => kvrpcpb::Op::Put.as_str_name().to_owned(),
            x if x == kvrpcpb::Op::Del as i32 => kvrpcpb::Op::Del.as_str_name().to_owned(),
            x if x == kvrpcpb::Op::Lock as i32 => kvrpcpb::Op::Lock.as_str_name().to_owned(),
            x if x == kvrpcpb::Op::Rollback as i32 => {
                kvrpcpb::Op::Rollback.as_str_name().to_owned()
            }
            x if x == kvrpcpb::Op::Insert as i32 => kvrpcpb::Op::Insert.as_str_name().to_owned(),
            x if x == kvrpcpb::Op::PessimisticLock as i32 => {
                kvrpcpb::Op::PessimisticLock.as_str_name().to_owned()
            }
            x if x == kvrpcpb::Op::CheckNotExists as i32 => {
                kvrpcpb::Op::CheckNotExists.as_str_name().to_owned()
            }
            x if x == kvrpcpb::Op::SharedLock as i32 => {
                kvrpcpb::Op::SharedLock.as_str_name().to_owned()
            }
            x if x == kvrpcpb::Op::SharedPessimisticLock as i32 => {
                kvrpcpb::Op::SharedPessimisticLock.as_str_name().to_owned()
            }
            other => format!("unknown({other})"),
        };

        write!(
            f,
            "key: {}, primary: {}, txnStartTS: {}, lockForUpdateTS:{}, minCommitTs:{}, ttl: {}, type: {}, UseAsyncCommit: {}, txnSize: {}",
            format_key_for_log(&self.inner.key),
            format_key_for_log(&self.inner.primary_lock),
            self.inner.lock_version,
            self.inner.lock_for_update_ts,
            self.inner.min_commit_ts,
            self.inner.lock_ttl,
            lock_type,
            self.inner.use_async_commit,
            self.inner.txn_size,
        )
    }
}

#[derive(Debug, Default)]
struct ReadLockTrackerState {
    resolved_locks: HashSet<u64>,
    committed_locks: HashSet<u64>,
    async_cleanup_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for ReadLockTrackerState {
    fn drop(&mut self) {
        for task in &self.async_cleanup_tasks {
            task.abort();
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReadLockTracker {
    inner: Arc<RwLock<ReadLockTrackerState>>,
}

impl ReadLockTracker {
    pub(crate) fn new_with_resolved_locks(resolved_locks: impl IntoIterator<Item = u64>) -> Self {
        let mut state = ReadLockTrackerState::default();
        state.resolved_locks.extend(resolved_locks);
        Self {
            inner: Arc::new(RwLock::new(state)),
        }
    }

    pub(crate) async fn snapshot(&self) -> (Vec<u64>, Vec<u64>) {
        let state = self.inner.read().await;
        let resolved_locks = state.resolved_locks.iter().copied().collect();
        let committed_locks = state.committed_locks.iter().copied().collect();
        (resolved_locks, committed_locks)
    }

    pub(crate) async fn extend(&self, resolved_locks: Vec<u64>, committed_locks: Vec<u64>) {
        if resolved_locks.is_empty() && committed_locks.is_empty() {
            return;
        }

        let mut state = self.inner.write().await;
        state.resolved_locks.extend(resolved_locks);
        state.committed_locks.extend(committed_locks);
    }

    pub(crate) async fn track_cleanup_task(&self, task: tokio::task::JoinHandle<()>) {
        let mut state = self.inner.write().await;
        state
            .async_cleanup_tasks
            .retain(|existing| !existing.is_finished());
        state.async_cleanup_tasks.push(task);
    }

    pub(crate) async fn abort_cleanup_tasks(&self) {
        let mut state = self.inner.write().await;
        for task in &state.async_cleanup_tasks {
            task.abort();
        }
        state.async_cleanup_tasks.clear();
    }
}

/// _Resolves_ the given locks. Returns locks still live. When there is no live locks, all the given locks are resolved.
///
/// If a key has a lock, the latest status of the key is unknown. We need to "resolve" the lock,
/// which means the key is finally either committed or rolled back, before we read the value of
/// the key. We first use `CheckTxnStatus` to get the transaction's final status (committed or
/// rolled back), then resolve the remaining locks:
/// - non-pessimistic locks are resolved by `ResolveLock`;
/// - pessimistic locks are resolved by `PessimisticRollback`.
#[cfg(test)]
pub async fn resolve_locks(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<Vec<kvrpcpb::LockInfo> /* live_locks */> {
    Ok(resolve_locks_with_options(
        ResolveLocksContext::default(),
        locks,
        timestamp,
        pd_client,
        keyspace,
        false,
        OPTIMISTIC_BACKOFF,
        None,
        LockResolverRpcContext::default(),
    )
    .await?
    .live_locks)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_with_options(
    ctx: ResolveLocksContext,
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    pessimistic_region_resolve: bool,
    lock_backoff: Backoff,
    killed: Option<Arc<AtomicU32>>,
    rpc_context: LockResolverRpcContext,
) -> Result<ResolveLocksResult> {
    let _resolve_lock_time_guard =
        ResolveLockTimeGuard::new(rpc_context.resolve_lock_detail.clone());
    let resolve_lock_lite_threshold = pd_client.resolve_lock_lite_threshold();

    debug!("resolving locks");
    let ts = pd_client.clone().get_timestamp().await?;
    let caller_start_ts = timestamp.version();
    let current_ts = ts.version();

    let mut live_locks = Vec::new();
    let mut ms_before_txn_expired: Option<i64> = None;
    let mut lock_resolver = LockResolver::new(ctx);
    lock_resolver.rpc_context = rpc_context;

    // records the commit version of each primary lock (representing the status of the transaction)
    let mut commit_versions: HashMap<u64, u64> = HashMap::new();
    // Keep separate cleaned-region caches to match client-go behavior:
    // pessimistic rollback region dedupe must not suppress normal resolve-lock dedupe.
    let mut clean_regions: HashMap<u64, HashSet<RegionVerId>> = HashMap::new();
    let mut pessimistic_clean_regions: HashMap<u64, HashSet<RegionVerId>> = HashMap::new();
    // We must check txn status for *all* locks, not only TTL-expired ones.
    //
    // TTL only indicates whether a lock is *possibly* orphaned; it does not mean the transaction
    // is still running. A transaction may already be committed/rolled back while its locks are
    // still visible (e.g. cleanup/resolve hasn't finished, retries after region errors, etc.).
    // If we only resolve TTL-expired locks, we can unnecessarily sleep/backoff until TTL even
    // though `CheckTxnStatus` would already report `Committed`/`RolledBack`.
    //
    // This matches the client-go `LockResolver.ResolveLocksWithOpts` flow: query txn status for
    // each encountered lock, then resolve immediately when the status is final.
    for lock in locks {
        if is_shared_lock(lock.lock_type) {
            return Err(Error::StringError(
                "misuse of resolve_locks: trying to resolve a shared lock directly".to_owned(),
            ));
        }
        let region_ver_id = pd_client
            .region_for_key(&lock.key.clone().into())
            .await?
            .ver_id();
        let is_pessimistic = is_pessimistic_lock(lock.lock_type);

        let cleaned_regions = if is_pessimistic {
            &pessimistic_clean_regions
        } else {
            &clean_regions
        };
        // Skip if the region has already been resolved for this lock type.
        if cleaned_regions
            .get(&lock.lock_version)
            .map(|regions| regions.contains(&region_ver_id))
            .unwrap_or(false)
        {
            continue;
        }

        let commit_version = match commit_versions.get(&lock.lock_version) {
            Some(&commit_version) => Some(commit_version),
            None => {
                let status = lock_resolver
                    .get_txn_status_from_lock(
                        lock_backoff.clone(),
                        killed.clone(),
                        &lock,
                        caller_start_ts,
                        current_ts,
                        false,
                        pd_client.clone(),
                        keyspace,
                    )
                    .await?;
                let status = match status {
                    TxnStatusFromLock::Status(status) => status,
                    TxnStatusFromLock::PessimisticRollbackRequired => {
                        if pessimistic_region_resolve {
                            if let Some(cleaned_region) = rollback_pessimistic_lock_with_retry(
                                &lock,
                                &lock_resolver.rpc_context,
                                pd_client.clone(),
                                keyspace,
                                lock_backoff.clone(),
                                killed.clone(),
                            )
                            .await?
                            {
                                pessimistic_clean_regions
                                    .entry(lock.lock_version)
                                    .or_default()
                                    .insert(cleaned_region);
                            }
                        } else {
                            rollback_pessimistic_lock(
                                &lock,
                                &lock_resolver.rpc_context,
                                pd_client.clone(),
                                keyspace,
                            )
                            .await?;
                        }
                        continue;
                    }
                };
                match &status.kind {
                    TransactionStatusKind::Committed(ts) => {
                        let commit_version = ts.version();
                        commit_versions.insert(lock.lock_version, commit_version);
                        Some(commit_version)
                    }
                    TransactionStatusKind::RolledBack => {
                        commit_versions.insert(lock.lock_version, 0);
                        Some(0)
                    }
                    TransactionStatusKind::Locked(ttl, lock_info) => {
                        live_locks.push(lock_info.clone());
                        let mut ms_before_lock_expired =
                            lock_until_expired_ms(lock_info.lock_version, *ttl, ts.clone());
                        if ms_before_lock_expired <= 0 {
                            ms_before_lock_expired = 0;
                        }
                        ms_before_txn_expired = Some(match ms_before_txn_expired {
                            None => ms_before_lock_expired,
                            Some(prev) => prev.min(ms_before_lock_expired),
                        });
                        None
                    }
                }
            }
        };

        if let Some(commit_version) = commit_version {
            // Match client-go `resolve`: pessimistic locks should be handled by
            // `PessimisticRollback`, not `ResolveLock`.
            if is_pessimistic {
                if pessimistic_region_resolve {
                    if let Some(cleaned_region) = rollback_pessimistic_lock_with_retry(
                        &lock,
                        &lock_resolver.rpc_context,
                        pd_client.clone(),
                        keyspace,
                        lock_backoff.clone(),
                        killed.clone(),
                    )
                    .await?
                    {
                        pessimistic_clean_regions
                            .entry(lock.lock_version)
                            .or_default()
                            .insert(cleaned_region);
                    }
                } else {
                    rollback_pessimistic_lock(
                        &lock,
                        &lock_resolver.rpc_context,
                        pd_client.clone(),
                        keyspace,
                    )
                    .await?;
                }
                continue;
            }

            let resolve_lite = lock.txn_size < resolve_lock_lite_threshold;
            // The primary lock has been resolved by CheckTxnStatus already.
            if resolve_lite && lock.key == lock.primary_lock {
                continue;
            }
            let cleaned_region = resolve_lock_with_retry(
                &lock.key,
                lock.lock_version,
                commit_version,
                lock.is_txn_file,
                resolve_lite,
                &lock_resolver.rpc_context,
                pd_client.clone(),
                keyspace,
                lock_backoff.clone(),
                killed.clone(),
            )
            .await?;
            if !resolve_lite {
                clean_regions
                    .entry(lock.lock_version)
                    .or_default()
                    .insert(cleaned_region);
            }
        }
    }
    Ok(ResolveLocksResult {
        live_locks,
        ms_before_txn_expired: ms_before_txn_expired.unwrap_or(0),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_locks_for_read(
    ctx: ResolveLocksContext,
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    lock_backoff: Backoff,
    killed: Option<Arc<AtomicU32>>,
    force_resolve_lock_lite: bool,
    lock_tracker: Option<ReadLockTracker>,
    rpc_context: LockResolverRpcContext,
) -> Result<ResolveLocksForReadResult> {
    let _resolve_lock_time_guard =
        ResolveLockTimeGuard::new(rpc_context.resolve_lock_detail.clone());
    let resolve_lock_lite_threshold = pd_client.resolve_lock_lite_threshold();

    debug!("resolving locks for read");
    let ts = pd_client.clone().get_timestamp().await?;
    let caller_start_ts = timestamp.version();
    let current_ts = ts.version();

    let mut ms_before_txn_expired: Option<i64> = None;
    let mut live_locks = Vec::new();
    let mut resolved_locks = Vec::new();
    let mut committed_locks = Vec::new();
    let mut lock_resolver = LockResolver::new(ctx);
    lock_resolver.rpc_context = rpc_context;

    for lock in locks {
        if is_shared_lock(lock.lock_type) {
            return Err(Error::StringError(
                "misuse of resolve_locks_for_read: trying to resolve a shared lock directly"
                    .to_owned(),
            ));
        }

        let status = lock_resolver
            .get_txn_status_from_lock(
                lock_backoff.clone(),
                killed.clone(),
                &lock,
                caller_start_ts,
                current_ts,
                false,
                pd_client.clone(),
                keyspace,
            )
            .await?;

        let status = match status {
            TxnStatusFromLock::Status(status) => status,
            TxnStatusFromLock::PessimisticRollbackRequired => {
                rollback_pessimistic_lock(
                    &lock,
                    &lock_resolver.rpc_context,
                    pd_client.clone(),
                    keyspace,
                )
                .await?;
                resolved_locks.push(lock.lock_version);
                continue;
            }
        };

        // Match client-go `resolveLocks` for read: `MinCommitTSPushed` means the lock can
        // be ignored for this snapshot read.
        if status.action == kvrpcpb::Action::MinCommitTsPushed {
            resolved_locks.push(lock.lock_version);
            continue;
        }

        let is_rolled_back = matches!(
            status.action,
            kvrpcpb::Action::NoAction
                | kvrpcpb::Action::LockNotExistRollback
                | kvrpcpb::Action::TtlExpireRollback
        ) && matches!(status.kind, TransactionStatusKind::RolledBack);

        let commit_version = match &status.kind {
            TransactionStatusKind::Committed(commit_ts) => commit_ts.version(),
            TransactionStatusKind::RolledBack if is_rolled_back => 0,
            TransactionStatusKind::Locked(ttl, _) => {
                let mut ms_before_lock_expired =
                    lock_until_expired_ms(lock.lock_version, *ttl, ts.clone());
                if ms_before_lock_expired <= 0 {
                    ms_before_lock_expired = 0;
                }
                ms_before_txn_expired = Some(match ms_before_txn_expired {
                    None => ms_before_lock_expired,
                    Some(prev) => prev.min(ms_before_lock_expired),
                });
                live_locks.push(lock);
                continue;
            }
            _ => {
                // `CheckTxnStatus` returned an undetermined "rolled back" response shape.
                // Keep parity with client-go by not marking the lock as resolved for read;
                // the caller should retry until the status is determined or lock expires.
                live_locks.push(lock);
                ms_before_txn_expired = Some(match ms_before_txn_expired {
                    None => 0,
                    Some(prev) => prev.min(0),
                });
                continue;
            }
        };

        if commit_version > 0 && commit_version <= caller_start_ts {
            committed_locks.push(lock.lock_version);
        } else {
            resolved_locks.push(lock.lock_version);
        }

        // For read requests, resolve locks asynchronously and ignore best-effort cleanup errors.
        // The `committed_locks` / `resolved_locks` context allows read retry to proceed without
        // waiting for the cleanup to finish.
        if is_pessimistic_lock(lock.lock_type) {
            rollback_pessimistic_lock(
                &lock,
                &lock_resolver.rpc_context,
                pd_client.clone(),
                keyspace,
            )
            .await?;
            continue;
        }

        // Match client-go point-read behavior: callers can force ResolveLock lite regardless of
        // `txn_size`/threshold to reduce cleanup overhead.
        let resolve_lite = force_resolve_lock_lite || lock.txn_size < resolve_lock_lite_threshold;
        if resolve_lite && lock.key == lock.primary_lock {
            continue;
        }

        let key = lock.key.clone();
        let start_version = lock.lock_version;
        let is_txn_file = lock.is_txn_file;
        let rpc_context = lock_resolver.rpc_context.clone();
        let pd_client = pd_client.clone();
        let lock_backoff = lock_backoff.clone();
        let killed = killed.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = resolve_lock_with_retry(
                &key,
                start_version,
                commit_version,
                is_txn_file,
                resolve_lite,
                &rpc_context,
                pd_client,
                keyspace,
                lock_backoff,
                killed,
            )
            .await
            {
                warn!("async resolve_lock_for_read failed: {err}");
            }
        });
        if let Some(lock_tracker) = lock_tracker.as_ref() {
            lock_tracker.track_cleanup_task(task).await;
        }
    }

    Ok(ResolveLocksForReadResult {
        ms_before_txn_expired: ms_before_txn_expired.unwrap_or(0),
        live_locks,
        resolved_locks,
        committed_locks,
    })
}

#[allow(clippy::too_many_arguments)]
async fn resolve_lock_with_retry(
    #[allow(clippy::ptr_arg)] key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    resolve_lite: bool,
    rpc_context: &LockResolverRpcContext,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    mut backoff: Backoff,
    killed: Option<Arc<AtomicU32>>,
) -> Result<RegionVerId> {
    debug!("resolving locks with retry");
    let mut attempt = 0;
    loop {
        attempt += 1;
        debug!("resolving locks: attempt {}", attempt);
        let store = pd_client.clone().store_for_key(key.into()).await?;
        let ver_id = store.region_with_leader.ver_id();
        let mut request =
            requests::new_resolve_lock_request(start_version, commit_version, is_txn_file);
        rpc_context.apply_to_request(&mut request);
        if resolve_lite {
            request.keys = vec![key.clone()];
        }
        let plan_builder = match crate::request::PlanBuilder::new_with_rpc_interceptors(
            pd_client.clone(),
            keyspace,
            request,
            rpc_context.rpc_interceptors.clone(),
        )
        .single_region_with_store(store.clone())
        .await
        {
            Ok(plan_builder) => plan_builder,
            Err(Error::LeaderNotFound { region }) => {
                pd_client.invalidate_region_cache(region.clone()).await;
                match backoff.next_delay_duration() {
                    Some(duration) => {
                        check_killed(&killed)?;
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(Error::LeaderNotFound { region }),
                }
            }
            Err(err) => return Err(err),
        };
        let plan = plan_builder.extract_error().plan();
        match plan.execute().await {
            Ok(_) => {
                return Ok(ver_id);
            }
            // Retry on region error
            Err(Error::ExtractedErrors(mut errors)) => {
                // ResolveLockResponse can have at most 1 error
                match errors.pop() {
                    Some(Error::RegionError(e)) => match backoff.next_delay_duration() {
                        Some(duration) => {
                            let region_error_resolved =
                                handle_region_error(pd_client.clone(), *e, store.clone()).await?;
                            if !region_error_resolved {
                                check_killed(&killed)?;
                                sleep(duration).await;
                            }
                            continue;
                        }
                        None => return Err(Error::RegionError(e)),
                    },
                    Some(Error::KeyError(key_err)) => {
                        // Keyspace is not truncated here because we need full key info for logging.
                        error!(
                            "resolve_lock error, unexpected resolve err: {:?}, lock: {{key: {}, start_version: {}, commit_version: {}, is_txn_file: {}}}",
                            key_err,
                            format_key_for_log(key),
                            start_version,
                            commit_version,
                            is_txn_file,
                        );
                        return Err(Error::KeyError(key_err));
                    }
                    Some(e) => return Err(e),
                    None => return Err(empty_extracted_errors("resolve_lock")),
                }
            }
            Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                Some(duration) => {
                    pd_client.invalidate_region_cache(ver_id.clone()).await;
                    if let Ok(store_id) = store.region_with_leader.get_store_id() {
                        pd_client.invalidate_store_cache(store_id).await;
                    }
                    check_killed(&killed)?;
                    sleep(duration).await;
                    continue;
                }
                None => return Err(e),
            },
            Err(e) => return Err(e),
        }
    }
}

async fn rollback_pessimistic_lock(
    lock: &kvrpcpb::LockInfo,
    rpc_context: &LockResolverRpcContext,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<()> {
    // Match client-go `resolvePessimisticLock`: primary key is considered resolved by
    // CheckTxnStatus, so no extra rollback request is needed here.
    if lock.key == lock.primary_lock {
        return Ok(());
    }

    let for_update_ts = pessimistic_rollback_for_update_ts(lock);
    let mut request = requests::new_pessimistic_rollback_request(
        vec![lock.key.clone()],
        lock.lock_version,
        for_update_ts,
    );
    rpc_context.apply_to_request(&mut request);
    let plan = crate::request::PlanBuilder::new_with_rpc_interceptors(
        pd_client.clone(),
        keyspace,
        request,
        rpc_context.rpc_interceptors.clone(),
    )
    .retry_multi_region(DEFAULT_REGION_BACKOFF)
    .extract_error()
    .plan();
    let _ = plan.execute().await?;
    Ok(())
}

async fn rollback_pessimistic_lock_with_retry(
    lock: &kvrpcpb::LockInfo,
    rpc_context: &LockResolverRpcContext,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    mut backoff: Backoff,
    killed: Option<Arc<AtomicU32>>,
) -> Result<Option<RegionVerId>> {
    // Match client-go `resolvePessimisticLock`: primary key is considered resolved by
    // CheckTxnStatus, so no extra rollback request is needed here.
    if lock.key == lock.primary_lock {
        return Ok(None);
    }

    let for_update_ts = pessimistic_rollback_for_update_ts(lock);
    loop {
        let store = pd_client.clone().store_for_key((&lock.key).into()).await?;
        let ver_id = store.region_with_leader.ver_id();
        let mut request = requests::new_pessimistic_rollback_request(
            Vec::new(),
            lock.lock_version,
            for_update_ts,
        );
        rpc_context.apply_to_request(&mut request);
        let plan_builder = match crate::request::PlanBuilder::new_with_rpc_interceptors(
            pd_client.clone(),
            keyspace,
            request,
            rpc_context.rpc_interceptors.clone(),
        )
        .single_region_with_store(store.clone())
        .await
        {
            Ok(plan_builder) => plan_builder,
            Err(Error::LeaderNotFound { region }) => {
                pd_client.invalidate_region_cache(region.clone()).await;
                match backoff.next_delay_duration() {
                    Some(duration) => {
                        check_killed(&killed)?;
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(Error::LeaderNotFound { region }),
                }
            }
            Err(err) => return Err(err),
        };
        let plan = plan_builder.extract_error().plan();
        match plan.execute().await {
            Ok(_) => return Ok(Some(ver_id)),
            Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                Some(Error::RegionError(e)) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        let region_error_resolved =
                            handle_region_error(pd_client.clone(), *e, store.clone()).await?;
                        if !region_error_resolved {
                            check_killed(&killed)?;
                            sleep(duration).await;
                        }
                        continue;
                    }
                    None => return Err(Error::RegionError(e)),
                },
                Some(Error::KeyError(key_err)) => {
                    // Keyspace is not truncated here because we need full key info for logging.
                    error!(
                        "pessimistic_rollback error, unexpected resolve err: {:?}, lock: {{key: {}, start_version: {}, for_update_ts: {}}}",
                        key_err,
                        format_key_for_log(&lock.key),
                        lock.lock_version,
                        for_update_ts,
                    );
                    return Err(Error::KeyError(key_err));
                }
                Some(err) => return Err(err),
                None => return Err(empty_extracted_errors("pessimistic_rollback")),
            },
            Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                Some(duration) => {
                    pd_client.invalidate_region_cache(ver_id.clone()).await;
                    if let Ok(store_id) = store.region_with_leader.get_store_id() {
                        pd_client.invalidate_store_cache(store_id).await;
                    }
                    check_killed(&killed)?;
                    sleep(duration).await;
                    continue;
                }
                None => return Err(e),
            },
            Err(err) => return Err(err),
        }
    }
}

const RESOLVED_CACHE_SIZE: usize = 2048;

#[derive(Default)]
struct ResolvedTxnStatusCache {
    map: HashMap<u64, Arc<TransactionStatus>>,
    order: VecDeque<u64>,
}

impl ResolvedTxnStatusCache {
    fn get(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        self.map.get(&txn_id).cloned()
    }

    fn remove(&mut self, txn_id: u64) {
        if self.map.remove(&txn_id).is_some() {
            self.order.retain(|&id| id != txn_id);
        }
    }

    fn insert(&mut self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
        // Preserve insertion order (FIFO), matching client-go `ResolvedCacheSize` behavior.
        if let Some(existing) = self.map.get(&txn_id).cloned() {
            let has_same_determined_status = existing.is_cacheable()
                && txn_status.is_cacheable()
                && match (&existing.kind, &txn_status.kind) {
                    (
                        TransactionStatusKind::Committed(existing),
                        TransactionStatusKind::Committed(new),
                    ) => existing.version() == new.version(),
                    (TransactionStatusKind::RolledBack, TransactionStatusKind::RolledBack) => true,
                    _ => false,
                };

            if !has_same_determined_status {
                error!(
                    "resolved txn status cache conflict for txn_id={txn_id}, existing={existing:?}, new={txn_status:?}"
                );
                self.remove(txn_id);
            }
            return;
        }

        if self.map.len() >= RESOLVED_CACHE_SIZE {
            while let Some(evict) = self.order.pop_front() {
                if self.map.remove(&evict).is_some() {
                    break;
                }
            }
        }

        self.order.push_back(txn_id);
        self.map.insert(txn_id, txn_status);
    }
}

/// Shared lock-resolution state reused across requests.
///
/// This is a cheap-to-clone handle (`Arc`-backed) used by [`LockResolver`] and transactional reads
/// to cache determined transaction statuses and to deduplicate lock cleanup work. Clones share the
/// same underlying state.
#[derive(Default, Clone)]
pub struct ResolveLocksContext {
    // Record the status of each transaction.
    resolved: Arc<RwLock<ResolvedTxnStatusCache>>,
    clean_regions: Arc<RwLock<HashMap<u64, HashSet<RegionVerId>>>>,
    resolving: Arc<RwLock<ResolvingLocksState>>,
}

#[derive(Default)]
struct ResolvingLocksState {
    // caller_start_ts -> token -> resolving locks
    resolving: HashMap<u64, Vec<Option<Vec<ResolvingLock>>>>,
}

/// A lock currently being resolved by the client.
///
/// This is intended for debugging and introspection, matching client-go
/// `LockResolver.Resolving`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvingLock {
    pub txn_id: u64,
    pub lock_txn_id: u64,
    pub key: Vec<u8>,
    pub primary: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
/// Options for GC-style lock cleanup.
///
/// These options are used by [`TransactionClient::cleanup_locks`](crate::TransactionClient::cleanup_locks)
/// (and by extension [`TransactionClient::gc`](crate::TransactionClient::gc)).
pub struct ResolveLocksOptions {
    /// When `true`, only async-commit locks are processed.
    ///
    /// Non-async-commit locks are ignored.
    pub async_commit_only: bool,
    /// The per-batch `ScanLock` limit used when scanning locks.
    pub batch_size: u32,
}

type ResourceGroupTagger = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct LockResolverRpcContext {
    pub(crate) context: Option<kvrpcpb::Context>,
    pub(crate) resource_group_tag_set: bool,
    pub(crate) resource_group_tagger: Option<ResourceGroupTagger>,
    pub(crate) resolve_lock_detail: Option<Arc<ResolveLockDetailCollector>>,
    pub(crate) rpc_interceptors: RpcInterceptors,
}

impl LockResolverRpcContext {
    pub(crate) fn apply_to_kv_context(&self, label: &str, ctx: &mut kvrpcpb::Context) {
        if let Some(template) = self.context.as_ref() {
            ctx.disk_full_opt = template.disk_full_opt;
            ctx.txn_source = template.txn_source;
            ctx.sync_log = template.sync_log;
            ctx.priority = template.priority;
            ctx.max_execution_duration_ms = template.max_execution_duration_ms;
            ctx.resource_control_context = template.resource_control_context.clone();
            if self.resource_group_tag_set {
                ctx.resource_group_tag = template.resource_group_tag.clone();
            }
            if !template.request_source.is_empty() {
                ctx.request_source = template.request_source.clone();
            }
        }

        if !self.resource_group_tag_set {
            if let Some(tagger) = self.resource_group_tagger.as_ref() {
                ctx.resource_group_tag = (tagger)(label);
            }
        }
    }

    pub(crate) fn apply_to_request<R: Request>(&self, request: &mut R) {
        let label = request.label();
        if let Some(ctx) = request.context_mut() {
            self.apply_to_kv_context(label, ctx);
        }
    }
}

impl Default for ResolveLocksOptions {
    fn default() -> Self {
        Self {
            async_commit_only: false,
            batch_size: 1024,
        }
    }
}

impl ResolveLocksContext {
    pub async fn get_resolved(&self, txn_id: u64) -> Option<Arc<TransactionStatus>> {
        self.resolved.read().await.get(txn_id)
    }

    /// Save a transaction status for `txn_id` in the resolved-status cache.
    ///
    /// Only cacheable, determined statuses (`Committed` or cacheable `RolledBack`) are stored.
    /// Non-cacheable statuses (for example `Locked` or `LockNotExistDoNothing`) are ignored.
    pub async fn save_resolved(&self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
        if !txn_status.is_cacheable() {
            warn!(
                "ignored non-cacheable txn status saved to resolved cache for txn_id={}: {:?}",
                txn_id, txn_status
            );
            return;
        }
        self.resolved.write().await.insert(txn_id, txn_status);
    }

    pub async fn is_region_cleaned(&self, txn_id: u64, region: &RegionVerId) -> bool {
        self.clean_regions
            .read()
            .await
            .get(&txn_id)
            .map(|regions| regions.contains(region))
            .unwrap_or(false)
    }

    pub async fn save_cleaned_region(&self, txn_id: u64, region: RegionVerId) {
        self.clean_regions
            .write()
            .await
            .entry(txn_id)
            .or_insert_with(HashSet::new)
            .insert(region);
    }
}

pub struct LockResolver {
    ctx: ResolveLocksContext,
    rpc_context: LockResolverRpcContext,
    read_lock_tracker: ReadLockTracker,
}

/// A [`LockResolver`] with a bound PD client and keyspace.
///
/// This is an ergonomic wrapper that avoids repeatedly passing `pd_client` and `keyspace` to each
/// lock-resolver call (similar to client-go store-level lock resolver usage).
pub struct BoundLockResolver<PdC: PdClient> {
    pd_client: Arc<PdC>,
    keyspace: Keyspace,
    inner: LockResolver,
}

impl<PdC: PdClient> BoundLockResolver<PdC> {
    /// Creates a bound lock resolver using the provided PD client and keyspace.
    ///
    /// The provided [`ResolveLocksContext`] is used for resolved-status caching and resolving-lock
    /// bookkeeping.
    pub fn new(pd_client: Arc<PdC>, keyspace: Keyspace, ctx: ResolveLocksContext) -> Self {
        Self {
            pd_client,
            keyspace,
            inner: LockResolver::new(ctx),
        }
    }

    /// Returns the bound PD client.
    #[must_use]
    pub fn pd_client(&self) -> Arc<PdC> {
        self.pd_client.clone()
    }

    /// Returns the bound keyspace.
    #[must_use]
    pub fn keyspace(&self) -> Keyspace {
        self.keyspace
    }

    /// Returns the underlying [`LockResolver`].
    #[must_use]
    pub fn into_inner(self) -> LockResolver {
        self.inner
    }

    /// Cancels background cleanup tasks spawned by [`BoundLockResolver::resolve_locks_for_read`].
    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Records the locks being resolved for a given `caller_start_ts` and returns a token.
    pub async fn record_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> usize {
        self.inner
            .record_resolving_locks(locks, caller_start_ts)
            .await
    }

    /// Updates the recorded resolving-lock information for `caller_start_ts` and `token`.
    pub async fn update_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
        token: usize,
    ) {
        self.inner
            .update_resolving_locks(locks, caller_start_ts, token)
            .await
    }

    /// Removes the recorded resolving-lock information for `caller_start_ts` and `token`.
    pub async fn resolve_locks_done(&self, caller_start_ts: u64, token: usize) {
        self.inner.resolve_locks_done(caller_start_ts, token).await
    }

    /// Returns the locks currently being resolved.
    pub async fn resolving(&self) -> Vec<ResolvingLock> {
        self.inner.resolving().await
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This retries until either all locks are resolved or the provided `backoff` is exhausted.
    /// The `timestamp` is used as the caller start timestamp when checking transaction status.
    pub async fn resolve_locks(
        &self,
        locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        backoff: Backoff,
        pessimistic_region_resolve: bool,
    ) -> Result<Vec<kvrpcpb::LockInfo>> {
        self.inner
            .resolve_locks(
                locks,
                timestamp,
                backoff,
                self.pd_client.clone(),
                self.keyspace,
                pessimistic_region_resolve,
            )
            .await
    }

    /// Performs a one-shot lock resolve attempt and returns the outcome.
    ///
    /// This does not perform the caller-side sleep loop: the returned `ms_before_txn_expired` can
    /// be used by the caller to decide whether to backoff/sleep and retry.
    pub async fn resolve_locks_once(
        &self,
        locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        pessimistic_region_resolve: bool,
    ) -> Result<ResolveLocksResult> {
        self.inner
            .resolve_locks_once(
                locks,
                timestamp,
                self.pd_client.clone(),
                self.keyspace,
                pessimistic_region_resolve,
            )
            .await
    }

    /// Resolves locks for read and returns any that remain live.
    ///
    /// This is a public wrapper around the internal resolve-lock-for-read path used by
    /// transactional reads (mirroring client-go `LockResolver.ResolveLocksForRead`).
    ///
    /// Note: non-pessimistic lock cleanup is performed asynchronously in a background task.
    pub async fn resolve_locks_for_read(
        &self,
        locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        force_resolve_lock_lite: bool,
    ) -> Result<ResolveLocksForReadResult> {
        self.inner
            .resolve_locks_for_read(
                locks,
                timestamp,
                self.pd_client.clone(),
                self.keyspace,
                force_resolve_lock_lite,
            )
            .await
    }

    /// Get the transaction status for `txn_id` (start TS) and `primary` key.
    pub async fn get_txn_status(
        &self,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
    ) -> Result<Arc<TransactionStatus>> {
        self.inner
            .get_txn_status(
                self.pd_client.clone(),
                self.keyspace,
                txn_id,
                primary,
                caller_start_ts,
            )
            .await
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    pub async fn cleanup_locks(
        &self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
    ) -> Result<()> {
        self.inner
            .cleanup_locks(store, locks, self.pd_client.clone(), self.keyspace)
            .await
    }

    /// Resolve locks in a batch (GC-only).
    ///
    /// This mirrors client-go `LockResolver.BatchResolveLocks` and is an alias for
    /// [`BoundLockResolver::cleanup_locks`].
    pub async fn batch_resolve_locks(
        &self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
    ) -> Result<()> {
        self.cleanup_locks(store, locks).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_txn_status(
        &self,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        is_txn_file: bool,
    ) -> Result<Arc<TransactionStatus>> {
        self.inner
            .check_txn_status(
                self.pd_client.clone(),
                self.keyspace,
                txn_id,
                primary,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
                force_sync_commit,
                resolving_pessimistic_lock,
                is_txn_file,
            )
            .await
    }
}

impl LockResolver {
    pub fn new(ctx: ResolveLocksContext) -> Self {
        Self {
            ctx,
            rpc_context: LockResolverRpcContext::default(),
            read_lock_tracker: ReadLockTracker::default(),
        }
    }

    pub(crate) fn set_rpc_context(&mut self, rpc_context: LockResolverRpcContext) {
        self.rpc_context = rpc_context;
    }

    /// Records the locks being resolved for a given `caller_start_ts` and returns a token.
    ///
    /// This mirrors client-go `LockResolver.RecordResolvingLocks`.
    pub async fn record_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
    ) -> usize {
        let resolving: Vec<ResolvingLock> = locks
            .iter()
            .map(|lock| ResolvingLock {
                txn_id: caller_start_ts,
                lock_txn_id: lock.lock_version,
                key: lock.key.clone(),
                primary: lock.primary_lock.clone(),
            })
            .collect();

        let mut state = self.ctx.resolving.write().await;
        let tokens = state.resolving.entry(caller_start_ts).or_default();
        let token = tokens.len();
        tokens.push(Some(resolving));
        token
    }

    /// Updates the recorded resolving-lock information for `caller_start_ts` and `token`.
    ///
    /// This mirrors client-go `LockResolver.UpdateResolvingLocks`.
    pub async fn update_resolving_locks(
        &self,
        locks: &[kvrpcpb::LockInfo],
        caller_start_ts: u64,
        token: usize,
    ) {
        let resolving: Vec<ResolvingLock> = locks
            .iter()
            .map(|lock| ResolvingLock {
                txn_id: caller_start_ts,
                lock_txn_id: lock.lock_version,
                key: lock.key.clone(),
                primary: lock.primary_lock.clone(),
            })
            .collect();

        let mut state = self.ctx.resolving.write().await;
        let Some(tokens) = state.resolving.get_mut(&caller_start_ts) else {
            return;
        };
        let Some(slot) = tokens.get_mut(token) else {
            return;
        };
        *slot = Some(resolving);
    }

    /// Removes the recorded resolving-lock information for `caller_start_ts` and `token`.
    ///
    /// This mirrors client-go `LockResolver.ResolveLocksDone`.
    pub async fn resolve_locks_done(&self, caller_start_ts: u64, token: usize) {
        let mut state = self.ctx.resolving.write().await;
        let Some(tokens) = state.resolving.get_mut(&caller_start_ts) else {
            return;
        };
        let Some(slot) = tokens.get_mut(token) else {
            return;
        };
        *slot = None;

        if tokens.iter().all(|slot| slot.is_none()) {
            state.resolving.remove(&caller_start_ts);
        }
    }

    /// Returns the locks currently being resolved.
    ///
    /// This mirrors client-go `LockResolver.Resolving`.
    pub async fn resolving(&self) -> Vec<ResolvingLock> {
        let state = self.ctx.resolving.read().await;
        state
            .resolving
            .values()
            .flat_map(|tokens| tokens.iter())
            .filter_map(|slot| slot.as_ref())
            .flat_map(|locks| locks.iter().cloned())
            .collect()
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This retries until either all locks are resolved or the provided `backoff` is exhausted.
    /// The `timestamp` is used as the caller start timestamp when checking transaction status.
    pub async fn resolve_locks(
        &self,
        mut locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        mut backoff: Backoff,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        pessimistic_region_resolve: bool,
    ) -> Result<Vec<kvrpcpb::LockInfo>> {
        if locks.is_empty() {
            return Ok(locks);
        }

        let ctx = self.ctx.clone();
        let rpc_context = self.rpc_context.clone();
        let caller_start_ts = timestamp.version();
        let mut resolving_record_token: Option<usize> = None;

        loop {
            let token = match resolving_record_token {
                Some(token) => {
                    self.update_resolving_locks(&locks, caller_start_ts, token)
                        .await;
                    token
                }
                None => {
                    let token = self.record_resolving_locks(&locks, caller_start_ts).await;
                    resolving_record_token = Some(token);
                    token
                }
            };

            let resolve_result = match resolve_locks_with_options(
                ctx.clone(),
                locks,
                timestamp.clone(),
                pd_client.clone(),
                keyspace,
                pessimistic_region_resolve,
                backoff.clone(),
                None,
                rpc_context.clone(),
            )
            .await
            {
                Ok(resolve_result) => resolve_result,
                Err(err) => {
                    self.resolve_locks_done(caller_start_ts, token).await;
                    return Err(err);
                }
            };

            let ms_before_txn_expired = resolve_result.ms_before_txn_expired;
            locks = resolve_result.live_locks;
            if locks.is_empty() {
                self.resolve_locks_done(caller_start_ts, token).await;
                return Ok(locks);
            }

            match backoff.next_delay_duration() {
                None => {
                    self.resolve_locks_done(caller_start_ts, token).await;
                    return Ok(locks);
                }
                Some(delay_duration) => {
                    let delay_duration = if ms_before_txn_expired > 0 {
                        delay_duration.min(Duration::from_millis(ms_before_txn_expired as u64))
                    } else {
                        delay_duration
                    };
                    sleep(delay_duration).await;
                }
            }
        }
    }

    /// Performs a one-shot lock resolve attempt and returns the outcome.
    ///
    /// This does not perform the caller-side sleep loop: the returned `ms_before_txn_expired` can
    /// be used by the caller to decide whether to backoff/sleep and retry.
    pub async fn resolve_locks_once(
        &self,
        locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        pessimistic_region_resolve: bool,
    ) -> Result<ResolveLocksResult> {
        resolve_locks_with_options(
            self.ctx.clone(),
            locks,
            timestamp,
            pd_client,
            keyspace,
            pessimistic_region_resolve,
            OPTIMISTIC_BACKOFF,
            None,
            self.rpc_context.clone(),
        )
        .await
    }

    /// Resolves locks for read and returns the classification information.
    ///
    /// This is a public wrapper around the internal resolve-lock-for-read path used by
    /// transactional reads (mirroring client-go `LockResolver.ResolveLocksForRead`).
    ///
    /// Note: non-pessimistic lock cleanup is performed asynchronously in a background task.
    pub async fn resolve_locks_for_read(
        &self,
        locks: Vec<kvrpcpb::LockInfo>,
        timestamp: Timestamp,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        force_resolve_lock_lite: bool,
    ) -> Result<ResolveLocksForReadResult> {
        resolve_locks_for_read(
            self.ctx.clone(),
            locks,
            timestamp,
            pd_client,
            keyspace,
            OPTIMISTIC_BACKOFF,
            None,
            force_resolve_lock_lite,
            Some(self.read_lock_tracker.clone()),
            self.rpc_context.clone(),
        )
        .await
    }

    /// Cancels background cleanup tasks spawned by [`LockResolver::resolve_locks_for_read`].
    ///
    /// This mirrors client-go `LockResolver.Close`.
    pub async fn close(&self) {
        self.read_lock_tracker.abort_cleanup_tasks().await;
    }

    /// Get the transaction status for `txn_id` (start TS) and `primary` key.
    ///
    /// This is a convenience wrapper around [`LockResolver::check_txn_status`] that fetches a
    /// `current_ts` from PD and sets `rollback_if_not_exist=true`, matching client-go
    /// `LockResolver.GetTxnStatus`.
    pub async fn get_txn_status(
        &self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
    ) -> Result<Arc<TransactionStatus>> {
        let current_ts = pd_client.clone().get_timestamp().await?.version();
        self.check_txn_status(
            pd_client,
            keyspace,
            txn_id,
            primary,
            caller_start_ts,
            current_ts,
            true,
            false,
            false,
            false,
        )
        .await
    }

    /// Resolve locks in a batch (GC-only).
    ///
    /// This mirrors client-go `LockResolver.BatchResolveLocks` and is an alias for
    /// [`LockResolver::cleanup_locks`].
    pub async fn batch_resolve_locks(
        &self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
    ) -> Result<()> {
        self.cleanup_locks(store, locks, pd_client, keyspace).await
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    ///
    /// Note: Will rollback RUNNING transactions. ONLY use in GC.
    pub async fn cleanup_locks(
        &self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
    ) -> Result<()> {
        if locks.is_empty() {
            return Ok(());
        }

        fail_point!("before-cleanup-locks", |_| { Ok(()) });

        let region = store.region_with_leader.ver_id();

        let mut txn_infos = HashMap::new();
        for l in locks {
            if is_shared_lock(l.lock_type) {
                return Err(Error::StringError(
                    "misuse of cleanup_locks: trying to resolve a shared lock directly".to_owned(),
                ));
            }
            let txn_id = l.lock_version;
            let is_pessimistic = is_pessimistic_lock(l.lock_type);
            let seen_non_pessimistic_txn = !is_pessimistic && txn_infos.contains_key(&txn_id);
            // `clean_regions` comes from `BatchResolveLock` (non-pessimistic path) and should not
            // suppress explicit `PessimisticRollback` for later pessimistic locks of the same txn.
            let region_already_cleaned =
                !is_pessimistic && self.ctx.is_region_cleaned(txn_id, &region).await;
            if seen_non_pessimistic_txn || region_already_cleaned {
                continue;
            }

            // Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
            let mut status = match self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    txn_id,
                    l.primary_lock.clone(),
                    0,
                    u64::MAX,
                    true,
                    false,
                    is_pessimistic,
                    l.is_txn_file,
                )
                .await
            {
                Ok(status) => status,
                Err(Error::KeyError(key_err))
                    if is_pessimistic && key_err.primary_mismatch.is_some() =>
                {
                    debug!(
                        "cleanup_locks got primary_mismatch on pessimistic lock, rollback directly; lock: {}",
                        format_key_for_log(&l.key)
                    );
                    rollback_pessimistic_lock(&l, &self.rpc_context, pd_client.clone(), keyspace)
                        .await?;
                    continue;
                }
                Err(err) => return Err(err),
            };

            if is_pessimistic {
                // Match client-go `BatchResolveLocks`: pessimistic locks are cleaned by
                // `PessimisticRollback` instead of being packed into `TxnInfo` for
                // `BatchResolveLock`.
                rollback_pessimistic_lock(&l, &self.rpc_context, pd_client.clone(), keyspace)
                    .await?;
                continue;
            }

            // If the transaction uses async commit, check_txn_status will reject rolling back the primary lock.
            // Then we need to check the secondary locks to determine the final status of the transaction.
            if let TransactionStatusKind::Locked(_, lock_info) = &status.kind {
                if !lock_info.use_async_commit {
                    error!(
                        "cleanup_locks got locked status without async-commit metadata. txn_id:{}",
                        txn_id
                    );
                    return Err(Error::ResolveLockError(vec![lock_info.clone()]));
                }
                let secondary_status = self
                    .check_all_secondaries(
                        pd_client.clone(),
                        keyspace,
                        lock_info.secondaries.clone(),
                        txn_id,
                        lock_info.min_commit_ts,
                    )
                    .await?;
                debug!(
                    "secondary status, txn_id:{}, commit_ts:{:?}, min_commit_version:{}, fallback_2pc:{}",
                    txn_id,
                    secondary_status
                        .commit_ts
                        .as_ref()
                        .map_or(0, |ts| ts.version()),
                    secondary_status.min_commit_ts,
                    secondary_status.fallback_2pc,
                );

                if secondary_status.fallback_2pc {
                    debug!("fallback to 2pc, txn_id:{}, check_txn_status again", txn_id);
                    status = self
                        .check_txn_status(
                            pd_client.clone(),
                            keyspace,
                            txn_id,
                            l.primary_lock,
                            0,
                            u64::MAX,
                            true,
                            true,
                            is_pessimistic,
                            l.is_txn_file,
                        )
                        .await?;
                } else {
                    let commit_ts = match (
                        secondary_status.missing_lock,
                        secondary_status.commit_ts.as_ref(),
                    ) {
                        (true, Some(commit_ts)) => commit_ts.version(),
                        (true, None) => 0,
                        (false, Some(commit_ts)) => commit_ts.version(),
                        (false, None) => secondary_status.min_commit_ts,
                    };
                    txn_infos.insert(txn_id, (commit_ts, l.is_txn_file));
                    continue;
                }
            }

            match &status.kind {
                TransactionStatusKind::Locked(_, lock_info) => {
                    error!(
                        "cleanup_locks fail to clean locks, this result is not expected. txn_id:{}",
                        txn_id
                    );
                    return Err(Error::ResolveLockError(vec![lock_info.clone()]));
                }
                TransactionStatusKind::Committed(ts) => {
                    txn_infos.insert(txn_id, (ts.version(), l.is_txn_file))
                }
                TransactionStatusKind::RolledBack => txn_infos.insert(txn_id, (0, l.is_txn_file)),
            };
        }

        debug!(
            "batch resolve locks, region:{:?}, txn:{:?}",
            store.region_with_leader.ver_id(),
            txn_infos
        );
        let mut txn_ids = Vec::with_capacity(txn_infos.len());
        let mut txn_info_vec = Vec::with_capacity(txn_infos.len());
        for (txn_id, (commit_ts, is_txn_file)) in txn_infos.into_iter() {
            txn_ids.push(txn_id);
            let mut txn_info = TxnInfo::default();
            txn_info.txn = txn_id;
            txn_info.status = commit_ts;
            txn_info.is_txn_file = is_txn_file;
            txn_info_vec.push(txn_info);
        }
        let cleaned_region = self
            .send_batch_resolve_lock_request(
                pd_client.clone(),
                keyspace,
                store.clone(),
                txn_info_vec,
            )
            .await?;
        for txn_id in txn_ids {
            self.ctx
                .save_cleaned_region(txn_id, cleaned_region.clone())
                .await;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn check_txn_status(
        &self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        is_txn_file: bool,
    ) -> Result<Arc<TransactionStatus>> {
        if let Some(txn_status) = self.ctx.get_resolved(txn_id).await {
            return Ok(txn_status);
        }

        // CheckTxnStatus may meet the following cases:
        // 1. LOCK
        // 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
        // 1.2 Lock TTL -- active transaction holding the lock.
        // 2. NO LOCK
        // 2.1 Txn Committed
        // 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
        // 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.
        let mut req = new_check_txn_status_request(
            primary,
            txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
            is_txn_file,
        );
        self.rpc_context.apply_to_request(&mut req);
        let plan = crate::request::PlanBuilder::new_with_rpc_interceptors(
            pd_client.clone(),
            keyspace,
            req,
            self.rpc_context.rpc_interceptors.clone(),
        )
        .retry_multi_region(DEFAULT_REGION_BACKOFF)
        .merge(CollectSingle)
        .extract_error()
        .post_process_default()
        .plan();
        let mut status: TransactionStatus = match plan.execute().await {
            Ok(status) => status,
            Err(Error::ExtractedErrors(errors)) | Err(Error::MultipleKeyErrors(errors)) => {
                match select_check_txn_status_error(errors) {
                    Error::KeyError(key_err) => {
                        if let Some(txn_not_found) = key_err.txn_not_found {
                            return Err(Error::TxnNotFound(txn_not_found));
                        }
                        // `get_txn_status_from_lock` needs the original lock key and `for_update_ts` to
                        // perform a pessimistic rollback when this error is `primary_mismatch`.
                        return Err(Error::KeyError(key_err));
                    }
                    err => return Err(err),
                }
            }
            Err(err) => return Err(err),
        };

        let current = pd_client.clone().get_timestamp().await?;
        status.check_ttl(current);
        let res = Arc::new(status);
        if res.is_cacheable() {
            self.ctx.save_resolved(txn_id, res.clone()).await;
        }
        Ok(res)
    }

    async fn check_all_secondaries(
        &self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keys: Vec<Vec<u8>>,
        txn_id: u64,
        primary_min_commit_ts: u64,
    ) -> Result<SecondaryLocksStatus> {
        let expected_keys = keys.len();
        let mut req = new_check_secondary_locks_request(keys, txn_id);
        self.rpc_context.apply_to_request(&mut req);
        let plan = crate::request::PlanBuilder::new_with_rpc_interceptors(
            pd_client.clone(),
            keyspace,
            req,
            self.rpc_context.rpc_interceptors.clone(),
        )
        .retry_multi_region(DEFAULT_REGION_BACKOFF)
        .extract_error()
        .merge(Collect)
        .plan();
        let mut secondary_status = plan.execute().await?;
        secondary_status.missing_lock = secondary_status.locked_keys < expected_keys;
        if let Some(commit_ts) = secondary_status.commit_ts.as_ref() {
            if commit_ts.version() < primary_min_commit_ts {
                return Err(Error::StringError(format!(
                    "check_secondary_locks commit_ts {} is less than primary min_commit_ts {}",
                    commit_ts.version(),
                    primary_min_commit_ts,
                )));
            }
        }
        secondary_status.min_commit_ts = secondary_status.min_commit_ts.max(primary_min_commit_ts);
        Ok(secondary_status)
    }

    async fn send_batch_resolve_lock_request(
        &self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        store: RegionStore,
        txn_infos: Vec<TxnInfo>,
    ) -> Result<RegionVerId> {
        let mut backoff = DEFAULT_REGION_BACKOFF;
        loop {
            let ver_id = store.region_with_leader.ver_id();
            let mut request = requests::new_batch_resolve_lock_request(txn_infos.clone());
            self.rpc_context.apply_to_request(&mut request);
            let plan_builder = match crate::request::PlanBuilder::new_with_rpc_interceptors(
                pd_client.clone(),
                keyspace,
                request,
                self.rpc_context.rpc_interceptors.clone(),
            )
            .single_region_with_store(store.clone())
            .await
            {
                Ok(plan_builder) => plan_builder,
                Err(Error::LeaderNotFound { region }) => {
                    pd_client.invalidate_region_cache(region.clone()).await;
                    match backoff.next_delay_duration() {
                        Some(duration) => {
                            sleep(duration).await;
                            continue;
                        }
                        None => return Err(Error::LeaderNotFound { region }),
                    }
                }
                Err(err) => return Err(err),
            };
            let plan = plan_builder.extract_error().plan();
            match plan.execute().await {
                Ok(_) => return Ok(ver_id),
                Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                    Some(Error::RegionError(e)) => match backoff.next_delay_duration() {
                        Some(duration) => {
                            let region_error_resolved =
                                handle_region_error(pd_client.clone(), *e, store.clone()).await?;
                            if !region_error_resolved {
                                sleep(duration).await;
                            }
                            continue;
                        }
                        None => return Err(Error::RegionError(e)),
                    },
                    Some(err) => return Err(err),
                    None => return Err(empty_extracted_errors("batch_resolve_locks")),
                },
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        pd_client.invalidate_region_cache(ver_id.clone()).await;
                        if let Ok(store_id) = store.region_with_leader.get_store_id() {
                            pd_client.invalidate_store_cache(store_id).await;
                        }
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(err) => return Err(err),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_txn_status_from_lock(
        &self,
        mut backoff: Backoff,
        killed: Option<Arc<AtomicU32>>,
        lock: &kvrpcpb::LockInfo,
        caller_start_ts: u64,
        current_ts: u64,
        force_sync_commit: bool,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
    ) -> Result<TxnStatusFromLock> {
        let current_ts = if lock.lock_ttl == 0 {
            // NOTE: lock_ttl = 0 is a special protocol!!!
            // When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
            // In this case, TiKV use lock TTL = 0 to notify client, and client should resolve the lock!
            // Set current_ts to max uint64 to make the lock expired.
            u64::MAX
        } else {
            current_ts
        };

        let mut force_sync_commit = force_sync_commit;
        let mut rollback_if_not_exist = false;
        loop {
            match self
                .check_txn_status(
                    pd_client.clone(),
                    keyspace,
                    lock.lock_version,
                    lock.primary_lock.clone(),
                    caller_start_ts,
                    current_ts,
                    rollback_if_not_exist,
                    force_sync_commit,
                    is_pessimistic_lock(lock.lock_type),
                    lock.is_txn_file,
                )
                .await
            {
                Ok(status) => {
                    let expired_async_commit_primary = match &status.kind {
                        TransactionStatusKind::Locked(_, primary_lock)
                            if primary_lock.use_async_commit
                                && status.is_expired
                                && !force_sync_commit =>
                        {
                            Some(primary_lock.clone())
                        }
                        _ => None,
                    };

                    // Match client-go `resolve`: expired async-commit locks must be
                    // determined via secondary checks (or force-sync fallback), not
                    // returned as live locks indefinitely.
                    if let Some(primary_lock) = expired_async_commit_primary {
                        let secondary_status = self
                            .check_all_secondaries(
                                pd_client.clone(),
                                keyspace,
                                primary_lock.secondaries.clone(),
                                lock.lock_version,
                                primary_lock.min_commit_ts,
                            )
                            .await?;
                        if secondary_status.fallback_2pc {
                            self.ctx.resolved.write().await.remove(lock.lock_version);
                            force_sync_commit = true;
                            continue;
                        }

                        let commit_version = match (
                            secondary_status.missing_lock,
                            secondary_status.commit_ts.as_ref(),
                        ) {
                            (true, Some(commit_ts)) => commit_ts.version(),
                            (true, None) => 0,
                            (false, Some(commit_ts)) => commit_ts.version(),
                            (false, None) => secondary_status.min_commit_ts,
                        };
                        let mut resolved_status = status.as_ref().clone();
                        resolved_status.kind = if commit_version == 0 {
                            TransactionStatusKind::RolledBack
                        } else {
                            TransactionStatusKind::Committed(Timestamp::from_version(
                                commit_version,
                            ))
                        };
                        resolved_status.is_expired = false;
                        let resolved_status = Arc::new(resolved_status);
                        if resolved_status.is_cacheable() {
                            self.ctx
                                .save_resolved(lock.lock_version, resolved_status.clone())
                                .await;
                        }
                        return Ok(TxnStatusFromLock::Status(resolved_status));
                    }

                    return Ok(TxnStatusFromLock::Status(status));
                }
                Err(Error::TxnNotFound(txn_not_found)) => {
                    let current = pd_client.clone().get_timestamp().await?;
                    if lock_until_expired_ms(lock.lock_version, lock.lock_ttl, current) <= 0 {
                        warn!(
                            "lock txn not found, lock has expired, lock {:?}, caller_start_ts {}, current_ts {}",
                            lock, caller_start_ts, current_ts
                        );
                        rollback_if_not_exist = true;
                        continue;
                    } else if is_pessimistic_lock(lock.lock_type) {
                        let status = TransactionStatus {
                            kind: TransactionStatusKind::Locked(lock.lock_ttl, lock.clone()),
                            action: kvrpcpb::Action::NoAction,
                            is_expired: false,
                        };
                        return Ok(TxnStatusFromLock::Status(Arc::new(status)));
                    }

                    if let Some(duration) = backoff.next_delay_duration() {
                        check_killed(&killed)?;
                        sleep(duration).await;
                        continue;
                    }
                    return Err(Error::TxnNotFound(txn_not_found));
                }
                Err(Error::KeyError(key_err))
                    if is_pessimistic_lock(lock.lock_type)
                        && key_err.primary_mismatch.is_some() =>
                {
                    debug!(
                        "check_txn_status got primary_mismatch on pessimistic lock, defer rollback to resolve flow; lock: {}",
                        format_key_for_log(&lock.key)
                    );
                    return Ok(TxnStatusFromLock::PessimisticRollbackRequired);
                }
                Err(err) => return Err(err),
            }
        }
    }
}

pub trait HasLocks {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        Vec::new()
    }
}

// Return duration in milliseconds until lock expired.
// If the lock has expired, return a negative value.
pub fn lock_until_expired_ms(lock_version: u64, ttl: u64, current: Timestamp) -> i64 {
    Timestamp::from_version(lock_version).physical + ttl as i64 - current.physical
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use fail::FailScenario;
    use serial_test::serial;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;

    #[test]
    fn test_lock_wrapper_accessors_and_predicates() {
        let mut proto = kvrpcpb::LockInfo::default();
        proto.key = b"k".to_vec();
        proto.primary_lock = b"p".to_vec();
        proto.lock_version = 42;
        proto.lock_ttl = 10;
        proto.txn_size = 16;
        proto.use_async_commit = true;
        proto.lock_for_update_ts = 7;
        proto.min_commit_ts = 8;
        proto.secondaries = vec![b"s1".to_vec(), b"s2".to_vec()];
        proto.is_txn_file = true;

        proto.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        let lock = new_lock(proto.clone());

        assert_eq!(lock.key(), b"k");
        assert_eq!(lock.primary(), b"p");
        assert_eq!(lock.txn_id(), 42);
        assert_eq!(lock.ttl(), 10);
        assert_eq!(lock.txn_size(), 16);
        assert!(lock.use_async_commit());
        assert_eq!(lock.lock_for_update_ts(), 7);
        assert_eq!(lock.min_commit_ts(), 8);
        assert_eq!(lock.secondaries(), &[b"s1".to_vec(), b"s2".to_vec()]);
        assert!(lock.is_txn_file());
        assert!(lock.is_pessimistic());
        assert!(!lock.is_shared());
        assert_eq!(lock.as_proto(), &proto);

        let proto_roundtrip: kvrpcpb::LockInfo = lock.clone().into();
        assert_eq!(proto_roundtrip, proto);
    }

    #[test]
    fn test_lock_wrapper_display_includes_key_len_prefix() {
        let mut proto = kvrpcpb::LockInfo::default();
        proto.key = vec![9; 20];
        proto.primary_lock = vec![10; 1];
        proto.lock_version = 1;
        proto.lock_for_update_ts = 2;
        proto.min_commit_ts = 3;
        proto.lock_ttl = 4;
        proto.lock_type = kvrpcpb::Op::Put as i32;
        proto.txn_size = 5;
        let lock = new_lock(proto);

        let display = lock.to_string();
        assert!(display.contains("len=20"), "display={display}");
        assert!(display.contains("txnStartTS: 1"), "display={display}");
        assert!(display.contains("ttl: 4"), "display={display}");
        assert!(display.contains("type: Put"), "display={display}");
    }

    #[test]
    fn test_lock_wrapper_shared_predicates() {
        let mut proto = kvrpcpb::LockInfo::default();
        proto.lock_type = kvrpcpb::Op::SharedPessimisticLock as i32;
        let lock = new_lock(proto.clone());
        assert!(lock.is_pessimistic());
        assert!(!lock.is_shared());

        proto.lock_type = kvrpcpb::Op::SharedLock as i32;
        let lock = new_lock(proto);
        assert!(!lock.is_pessimistic());
        assert!(lock.is_shared());
    }

    #[tokio::test]
    async fn test_resolve_locks_context_resolved_cache_is_bounded_fifo() {
        let ctx = ResolveLocksContext::default();
        let status = Arc::new(TransactionStatus {
            kind: TransactionStatusKind::RolledBack,
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        });

        for txn_id in 0..(RESOLVED_CACHE_SIZE as u64 + 1) {
            ctx.save_resolved(txn_id, status.clone()).await;
        }

        assert!(
            ctx.get_resolved(0).await.is_none(),
            "oldest entry should be evicted once cache exceeds RESOLVED_CACHE_SIZE"
        );
        assert!(ctx.get_resolved(1).await.is_some());
        assert!(ctx.get_resolved(RESOLVED_CACHE_SIZE as u64).await.is_some());
    }

    #[tokio::test]
    async fn test_resolve_locks_context_resolved_cache_rejects_conflicting_determined_status() {
        let ctx = ResolveLocksContext::default();

        let committed = Arc::new(TransactionStatus {
            kind: TransactionStatusKind::Committed(Timestamp::from_version(10)),
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        });
        let rolled_back = Arc::new(TransactionStatus {
            kind: TransactionStatusKind::RolledBack,
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        });

        ctx.save_resolved(7, committed.clone()).await;
        assert!(matches!(
            &ctx.get_resolved(7).await.unwrap().kind,
            TransactionStatusKind::Committed(ts) if ts.version() == 10
        ));

        ctx.save_resolved(7, rolled_back).await;
        assert!(
            ctx.get_resolved(7).await.is_none(),
            "conflicting determined statuses should invalidate the cache entry"
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_context_save_resolved_ignores_non_cacheable_statuses() {
        let ctx = ResolveLocksContext::default();

        let mut lock_info = kvrpcpb::LockInfo::default();
        lock_info.key = vec![1];
        lock_info.primary_lock = vec![1];
        lock_info.lock_version = 7;
        lock_info.lock_ttl = 100;
        lock_info.lock_type = kvrpcpb::Op::Put as i32;

        ctx.save_resolved(
            7,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::Locked(100, lock_info),
                action: kvrpcpb::Action::NoAction,
                is_expired: false,
            }),
        )
        .await;
        assert!(
            ctx.get_resolved(7).await.is_none(),
            "locked status should not be stored in resolved cache"
        );

        ctx.save_resolved(
            8,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::RolledBack,
                action: kvrpcpb::Action::LockNotExistDoNothing,
                is_expired: false,
            }),
        )
        .await;
        assert!(
            ctx.get_resolved(8).await.is_none(),
            "LockNotExistDoNothing should not be stored in resolved cache"
        );

        ctx.save_resolved(
            9,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::Committed(Timestamp::from_version(10)),
                action: kvrpcpb::Action::NoAction,
                is_expired: false,
            }),
        )
        .await;
        assert!(
            ctx.get_resolved(9).await.is_some(),
            "cacheable committed status should be stored in resolved cache"
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_resolved_cache_committed_async_commit_skips_check_secondary_locks()
    {
        let txn_id = 1;
        let commit_version = 10;
        let lock_key = vec![10];

        let ctx = ResolveLocksContext::default();
        ctx.save_resolved(
            txn_id,
            Arc::new(TransactionStatus {
                kind: TransactionStatusKind::Committed(Timestamp::from_version(commit_version)),
                action: kvrpcpb::Action::NoAction,
                is_expired: false,
            }),
        )
        .await;

        let resolve_lock_calls = Arc::new(AtomicUsize::new(0));
        let resolve_lock_calls_captured = resolve_lock_calls.clone();
        let lock_key_captured = lock_key.clone();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    panic!("CheckTxnStatus should be skipped when txn status is cached");
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    panic!("CheckSecondaryLocks should be skipped when txn status is cached");
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert!(req.txn_infos.is_empty());
                    assert_eq!(req.start_version, txn_id);
                    assert_eq!(req.commit_version, commit_version);
                    assert_eq!(req.keys, vec![lock_key_captured.clone()]);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = lock_key;
        lock.primary_lock = vec![1];
        lock.lock_version = txn_id;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;
        lock.use_async_commit = true;

        let result = resolve_locks_with_options(
            ctx,
            vec![lock],
            Timestamp::from_version(5),
            client,
            Keyspace::Disable,
            false,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(result.live_locks.is_empty());
        assert_eq!(resolve_lock_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_lock_resolver_rpc_context_merges_without_clobbering_region_fields() {
        let mut ctx = kvrpcpb::Context::default();
        ctx.region_id = 123;
        ctx.peer = Some(crate::proto::metapb::Peer {
            store_id: 456,
            ..Default::default()
        });

        let mut template = kvrpcpb::Context::default();
        template.disk_full_opt = crate::DiskFullOpt::AllowedOnAlmostFull as i32;
        template.txn_source = 42;
        template.sync_log = true;
        template.priority = crate::CommandPriority::High as i32;
        template.max_execution_duration_ms = 321;
        template.resource_control_context = Some(kvrpcpb::ResourceControlContext {
            resource_group_name: "rg-name".to_owned(),
            ..Default::default()
        });
        template.resource_group_tag = b"explicit-tag".to_vec();
        template.request_source = "request-source".to_owned();

        let rpc_context = LockResolverRpcContext {
            context: Some(template),
            resource_group_tag_set: true,
            resource_group_tagger: None,
            resolve_lock_detail: None,
            rpc_interceptors: Default::default(),
        };
        rpc_context.apply_to_kv_context("kv_check_txn_status", &mut ctx);

        assert_eq!(
            ctx.disk_full_opt,
            crate::DiskFullOpt::AllowedOnAlmostFull as i32
        );
        assert_eq!(ctx.txn_source, 42);
        assert!(ctx.sync_log);
        assert_eq!(ctx.priority, crate::CommandPriority::High as i32);
        assert_eq!(ctx.max_execution_duration_ms, 321);
        assert_eq!(
            ctx.resource_control_context
                .as_ref()
                .expect("resource control context")
                .resource_group_name,
            "rg-name"
        );
        assert_eq!(ctx.resource_group_tag, b"explicit-tag".to_vec());
        assert_eq!(ctx.request_source, "request-source");

        assert_eq!(ctx.region_id, 123);
        assert_eq!(
            ctx.peer.as_ref().expect("peer").store_id,
            456,
            "region routing fields should not be clobbered by the template context"
        );
    }

    #[tokio::test]
    async fn test_lock_resolver_rpc_context_resource_group_tagger_applies_when_tag_unset() {
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let tagger_calls_captured = tagger_calls.clone();

        let mut template = kvrpcpb::Context::default();
        template.disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull as i32;
        template.txn_source = 7;
        template.sync_log = true;
        template.priority = crate::CommandPriority::High as i32;
        template.max_execution_duration_ms = 999;
        template.resource_control_context = Some(kvrpcpb::ResourceControlContext {
            resource_group_name: "rg-lock-resolver".to_owned(),
            ..Default::default()
        });
        template.request_source = "src-lock-resolver".to_owned();

        let rpc_context = LockResolverRpcContext {
            context: Some(template),
            resource_group_tag_set: false,
            resource_group_tagger: Some(Arc::new(move |label: &str| {
                tagger_calls_captured.fetch_add(1, Ordering::SeqCst);
                label.as_bytes().to_vec()
            })),
            resolve_lock_detail: None,
            rpc_interceptors: Default::default(),
        };

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(
                        ctx.disk_full_opt,
                        crate::DiskFullOpt::AllowedOnAlreadyFull as i32
                    );
                    assert_eq!(ctx.txn_source, 7);
                    assert!(ctx.sync_log);
                    assert_eq!(ctx.priority, crate::CommandPriority::High as i32);
                    assert_eq!(ctx.max_execution_duration_ms, 999);
                    assert_eq!(ctx.request_source, "src-lock-resolver");
                    assert_eq!(
                        ctx.resource_control_context
                            .as_ref()
                            .expect("resource control context")
                            .resource_group_name,
                        "rg-lock-resolver"
                    );
                    assert_eq!(ctx.resource_group_tag, b"kv_check_txn_status".to_vec());
                    assert_eq!(ctx.region_id, 1);
                    assert_eq!(
                        ctx.peer.as_ref().expect("peer").store_id,
                        41,
                        "region routing fields should not be clobbered by the template context"
                    );

                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(
                        ctx.disk_full_opt,
                        crate::DiskFullOpt::AllowedOnAlreadyFull as i32
                    );
                    assert_eq!(ctx.txn_source, 7);
                    assert!(ctx.sync_log);
                    assert_eq!(ctx.priority, crate::CommandPriority::High as i32);
                    assert_eq!(ctx.max_execution_duration_ms, 999);
                    assert_eq!(ctx.request_source, "src-lock-resolver");
                    assert_eq!(
                        ctx.resource_control_context
                            .as_ref()
                            .expect("resource control context")
                            .resource_group_name,
                        "rg-lock-resolver"
                    );
                    assert_eq!(ctx.resource_group_tag, b"kv_resolve_lock".to_vec());
                    assert_eq!(ctx.region_id, 1);
                    assert_eq!(
                        ctx.peer.as_ref().expect("peer").store_id,
                        41,
                        "region routing fields should not be clobbered by the template context"
                    );

                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            false,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            rpc_context,
        )
        .await
        .unwrap();
        assert!(result.live_locks.is_empty());
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_lock_resolver_rpc_context_resource_group_tagger_skipped_when_tag_set() {
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let tagger_calls_captured = tagger_calls.clone();

        let mut template = kvrpcpb::Context::default();
        template.disk_full_opt = crate::DiskFullOpt::AllowedOnAlmostFull as i32;
        template.txn_source = 8;
        template.sync_log = true;
        template.priority = crate::CommandPriority::High as i32;
        template.max_execution_duration_ms = 123;
        template.resource_group_tag = b"explicit-tag".to_vec();

        let rpc_context = LockResolverRpcContext {
            context: Some(template),
            resource_group_tag_set: true,
            resource_group_tagger: Some(Arc::new(move |_label: &str| {
                tagger_calls_captured.fetch_add(1, Ordering::SeqCst);
                b"tagger".to_vec()
            })),
            resolve_lock_detail: None,
            rpc_interceptors: Default::default(),
        };

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.resource_group_tag, b"explicit-tag".to_vec());
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.resource_group_tag, b"explicit-tag".to_vec());
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            false,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            rpc_context,
        )
        .await
        .unwrap();
        assert!(result.live_locks.is_empty());
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_lock_resolver_rpc_context_applies_to_check_secondary_locks_requests() {
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let tagger_calls_captured = tagger_calls.clone();

        let mut template = kvrpcpb::Context::default();
        template.disk_full_opt = crate::DiskFullOpt::AllowedOnAlreadyFull as i32;
        template.txn_source = 123;
        template.sync_log = true;
        template.priority = crate::CommandPriority::High as i32;
        template.max_execution_duration_ms = 456;
        template.resource_control_context = Some(kvrpcpb::ResourceControlContext {
            resource_group_name: "rg-check-secondary".to_owned(),
            ..Default::default()
        });
        template.request_source = "src-check-secondary".to_owned();

        let rpc_context = LockResolverRpcContext {
            context: Some(template),
            resource_group_tag_set: false,
            resource_group_tagger: Some(Arc::new(move |label: &str| {
                tagger_calls_captured.fetch_add(1, Ordering::SeqCst);
                label.as_bytes().to_vec()
            })),
            resolve_lock_detail: None,
            rpc_interceptors: Default::default(),
        };

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(
                        ctx.disk_full_opt,
                        crate::DiskFullOpt::AllowedOnAlreadyFull as i32
                    );
                    assert_eq!(ctx.txn_source, 123);
                    assert!(ctx.sync_log);
                    assert_eq!(ctx.priority, crate::CommandPriority::High as i32);
                    assert_eq!(ctx.max_execution_duration_ms, 456);
                    assert_eq!(ctx.request_source, "src-check-secondary");
                    assert_eq!(
                        ctx.resource_control_context
                            .as_ref()
                            .expect("resource control context")
                            .resource_group_name,
                        "rg-check-secondary"
                    );
                    assert_eq!(
                        ctx.resource_group_tag,
                        b"kv_check_secondary_locks_request".to_vec()
                    );
                    assert_eq!(ctx.region_id, 1);
                    assert_eq!(
                        ctx.peer.as_ref().expect("peer").store_id,
                        41,
                        "region routing fields should not be clobbered by the template context"
                    );

                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 200,
                        locks: vec![kvrpcpb::LockInfo {
                            use_async_commit: true,
                            min_commit_ts: 150,
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver.set_rpc_context(rpc_context);
        let status = lock_resolver
            .check_all_secondaries(client, Keyspace::Disable, vec![vec![3]], 7, 150)
            .await
            .unwrap();
        assert_eq!(
            status
                .commit_ts
                .as_ref()
                .expect("commit_ts should be present")
                .version(),
            200
        );
        assert!(!status.missing_lock);
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_get_txn_status_wires_defaults_and_uses_cache() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.primary_key, b"p".to_vec());
                    assert_eq!(req.lock_ts, 7);
                    assert_eq!(req.caller_start_ts, 9);
                    assert_eq!(req.current_ts, 42);
                    assert!(req.rollback_if_not_exist);
                    assert!(!req.force_sync_commit);
                    assert!(!req.resolving_pessimistic_lock);
                    assert!(req.verify_is_primary);
                    assert!(!req.is_txn_file);

                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 50;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let pd_client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp::from_version(42),
        ));

        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let status = lock_resolver
            .get_txn_status(pd_client.clone(), Keyspace::Disable, 7, b"p".to_vec(), 9)
            .await
            .unwrap();
        assert!(matches!(
            &status.kind,
            TransactionStatusKind::Committed(ts) if ts.version() == 50
        ));

        let _ = lock_resolver
            .get_txn_status(pd_client, Keyspace::Disable, 7, b"p".to_vec(), 9)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_bound_lock_resolver_get_txn_status_binds_pd_and_keyspace() {
        let keyspace = Keyspace::ApiV2NoPrefix;
        let expected_api_version = keyspace.api_version() as i32;
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.primary_key, b"p".to_vec());
                    assert_eq!(req.lock_ts, 7);
                    assert_eq!(req.caller_start_ts, 9);
                    assert_eq!(req.current_ts, 42);
                    assert!(req.rollback_if_not_exist);
                    assert!(!req.force_sync_commit);
                    assert!(!req.resolving_pessimistic_lock);
                    assert!(req.verify_is_primary);
                    assert!(!req.is_txn_file);
                    assert_eq!(
                        req.context.as_ref().expect("context").api_version,
                        expected_api_version
                    );

                    let mut resp = kvrpcpb::CheckTxnStatusResponse::default();
                    resp.action = kvrpcpb::Action::NoAction as i32;
                    resp.commit_version = 50;
                    resp.lock_ttl = 0;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let pd_client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp::from_version(42),
        ));

        let lock_resolver =
            BoundLockResolver::new(pd_client, keyspace, ResolveLocksContext::default());
        let status = lock_resolver
            .get_txn_status(7, b"p".to_vec(), 9)
            .await
            .unwrap();
        assert!(matches!(
            &status.kind,
            TransactionStatusKind::Committed(ts) if ts.version() == 50
        ));

        let _ = lock_resolver
            .get_txn_status(7, b"p".to_vec(), 9)
            .await
            .unwrap();
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_resolving_records_updates_and_cleans_up() {
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());

        let mut lock1 = kvrpcpb::LockInfo::default();
        lock1.key = vec![1];
        lock1.primary_lock = vec![9];
        lock1.lock_version = 10;

        let mut lock2 = kvrpcpb::LockInfo::default();
        lock2.key = vec![2];
        lock2.primary_lock = vec![9];
        lock2.lock_version = 11;

        let locks = vec![lock1, lock2.clone()];
        let token = lock_resolver.record_resolving_locks(&locks, 42).await;

        let mut resolving = lock_resolver.resolving().await;
        resolving.sort_by_key(|lock| (lock.txn_id, lock.lock_txn_id));
        assert_eq!(
            resolving,
            vec![
                ResolvingLock {
                    txn_id: 42,
                    lock_txn_id: 10,
                    key: vec![1],
                    primary: vec![9],
                },
                ResolvingLock {
                    txn_id: 42,
                    lock_txn_id: 11,
                    key: vec![2],
                    primary: vec![9],
                },
            ],
        );

        lock_resolver
            .update_resolving_locks(&[lock2], 42, token)
            .await;

        let mut resolving = lock_resolver.resolving().await;
        resolving.sort_by_key(|lock| (lock.txn_id, lock.lock_txn_id));
        assert_eq!(
            resolving,
            vec![ResolvingLock {
                txn_id: 42,
                lock_txn_id: 11,
                key: vec![2],
                primary: vec![9],
            }],
        );

        lock_resolver.resolve_locks_done(42, token).await;
        assert!(lock_resolver.resolving().await.is_empty());
    }

    #[tokio::test]
    async fn test_lock_resolver_resolve_locks_returns_live_locks_when_backoff_exhausted() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
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
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![2];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let live_locks = lock_resolver
            .resolve_locks(
                vec![lock],
                Timestamp::from_version(42),
                Backoff::no_backoff(),
                client,
                Keyspace::Disable,
                false,
            )
            .await
            .unwrap();

        assert_eq!(live_locks.len(), 1);
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert!(lock_resolver.resolving().await.is_empty());
    }

    #[tokio::test]
    async fn test_bound_lock_resolver_resolve_locks_returns_live_locks_when_backoff_exhausted() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
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
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let lock_resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let live_locks = lock_resolver
            .resolve_locks(
                vec![lock],
                Timestamp::from_version(42),
                Backoff::no_backoff(),
                false,
            )
            .await
            .unwrap();

        assert_eq!(live_locks.len(), 1);
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert!(lock_resolver.resolving().await.is_empty());
    }

    struct TimestampedPdClient {
        inner: Arc<MockPdClient>,
        timestamp: Timestamp,
        resolve_lock_lite_threshold: u64,
    }

    impl TimestampedPdClient {
        fn new(inner: Arc<MockPdClient>, timestamp: Timestamp) -> Self {
            Self {
                inner,
                timestamp,
                resolve_lock_lite_threshold: crate::config::DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD,
            }
        }

        fn new_with_resolve_lock_lite_threshold(
            inner: Arc<MockPdClient>,
            timestamp: Timestamp,
            resolve_lock_lite_threshold: u64,
        ) -> Self {
            Self {
                inner,
                timestamp,
                resolve_lock_lite_threshold,
            }
        }
    }

    #[async_trait]
    impl PdClient for TimestampedPdClient {
        type KvClient = MockKvClient;

        fn resolve_lock_lite_threshold(&self) -> u64 {
            self.resolve_lock_lite_threshold
        }

        async fn map_region_to_store(
            self: Arc<Self>,
            region: crate::region::RegionWithLeader,
        ) -> Result<RegionStore> {
            self.inner.clone().map_region_to_store(region).await
        }

        async fn region_for_key(
            &self,
            key: &crate::Key,
        ) -> Result<crate::region::RegionWithLeader> {
            self.inner.region_for_key(key).await
        }

        async fn region_for_id(
            &self,
            id: crate::region::RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.inner.region_for_id(id).await
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Ok(self.timestamp.clone())
        }

        async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
            self.inner.clone().update_safepoint(safepoint).await
        }

        async fn load_keyspace(
            &self,
            keyspace: &str,
        ) -> Result<crate::proto::keyspacepb::KeyspaceMeta> {
            self.inner.load_keyspace(keyspace).await
        }

        async fn all_stores(&self) -> Result<Vec<crate::store::Store>> {
            self.inner.all_stores().await
        }

        async fn update_leader(
            &self,
            ver_id: crate::region::RegionVerId,
            leader: crate::proto::metapb::Peer,
        ) -> Result<()> {
            self.inner.update_leader(ver_id, leader).await
        }

        async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
            self.inner.invalidate_region_cache(ver_id).await
        }

        async fn invalidate_store_cache(&self, store_id: crate::region::StoreId) {
            self.inner.invalidate_store_cache(store_id).await
        }
    }

    #[test]
    fn test_select_check_txn_status_error_prefers_key_error() {
        let selected = select_check_txn_status_error(vec![
            Error::KeyError(Box::default()),
            Error::RegionError(Box::default()),
        ]);
        assert!(matches!(selected, Error::KeyError(_)));
    }

    #[test]
    fn test_select_check_txn_status_error_falls_back_to_last_non_key_error() {
        let selected = select_check_txn_status_error(vec![
            Error::RegionError(Box::default()),
            Error::Unimplemented,
        ]);
        assert!(matches!(selected, Error::Unimplemented));
    }

    #[test]
    fn test_select_check_txn_status_error_handles_empty_error_list() {
        let selected = select_check_txn_status_error(Vec::new());
        match selected {
            Error::StringError(message) => {
                assert_eq!(message, "check_txn_status returned empty extracted errors");
            }
            other => panic!("unexpected error type: {other:?}"),
        }
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial]
    async fn test_resolve_lock_with_retry(#[case] keyspace: Keyspace) {
        let _scenario = FailScenario::setup();

        const MAX_REGION_ERROR_RETRIES: u32 = 10;
        let backoff = Backoff::no_jitter_backoff(0, 0, MAX_REGION_ERROR_RETRIES);

        // Test resolve lock within retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES),
        )
        .unwrap();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                fail::fail_point!("region-error", |_| {
                    let resp = kvrpcpb::ResolveLockResponse {
                        region_error: Some(errorpb::Error::default()),
                        ..Default::default()
                    };
                    Ok(Box::new(resp) as Box<dyn Any>)
                });
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));

        let key = vec![1];
        let rpc_context = LockResolverRpcContext::default();
        let region1 = MockPdClient::region1();
        let resolved_region = resolve_lock_with_retry(
            &key,
            1,
            2,
            false,
            false,
            &rpc_context,
            client.clone(),
            keyspace,
            backoff.clone(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(region1.ver_id(), resolved_region);

        // Test resolve lock over retry limit
        fail::cfg(
            "region-error",
            &format!("{}*return", MAX_REGION_ERROR_RETRIES + 1),
        )
        .unwrap();
        let key = vec![100];
        resolve_lock_with_retry(
            &key,
            3,
            4,
            false,
            false,
            &rpc_context,
            client,
            keyspace,
            backoff,
            None,
        )
        .await
        .expect_err("should return error");
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_locks_resolves_committed_even_if_ttl_not_expired() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100; // not expired under MockPdClient's Timestamp::default()

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_live_lock_returns_ms_before_txn_expired() {
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
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
            },
        )));
        let client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp {
                physical: 0,
                logical: 0,
                ..Default::default()
            },
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            false,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(resolve_result.live_locks.len(), 1);
        assert_eq!(resolve_result.ms_before_txn_expired, 100);
    }

    #[tokio::test]
    async fn test_resolve_locks_expired_async_commit_lock_checks_secondaries() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_locks_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_locks_count_captured = check_secondary_locks_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert!(!req.force_sync_commit);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 1,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: 7,
                            use_async_commit: true,
                            min_commit_ts: 150,
                            secondaries: vec![vec![3]],
                            ..Default::default()
                        }),
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_locks_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.keys, vec![vec![3]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 200,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 200);
                    assert!(
                        req.keys.is_empty(),
                        "non-lite resolve should not send key list"
                    );
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp {
                physical: 100,
                logical: 0,
                ..Default::default()
            },
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(check_secondary_locks_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_expired_async_commit_lock_missing_secondary_rolls_back_when_commit_ts_zero(
    ) {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_locks_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_locks_count_captured = check_secondary_locks_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert!(!req.force_sync_commit);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 1,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: 7,
                            use_async_commit: true,
                            min_commit_ts: 150,
                            secondaries: vec![vec![3]],
                            ..Default::default()
                        }),
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_locks_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.keys, vec![vec![3]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse::default();
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 0);
                    assert!(
                        req.keys.is_empty(),
                        "non-lite resolve should not send key list"
                    );
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp {
                physical: 100,
                logical: 0,
                ..Default::default()
            },
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(check_secondary_locks_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_expired_async_commit_lock_fallbacks_to_force_sync() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_locks_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_locks_count_captured = check_secondary_locks_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    let call = check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        assert!(!req.force_sync_commit);
                        let resp = kvrpcpb::CheckTxnStatusResponse {
                            lock_ttl: 1,
                            lock_info: Some(kvrpcpb::LockInfo {
                                lock_version: 9,
                                use_async_commit: true,
                                min_commit_ts: 151,
                                secondaries: vec![vec![4]],
                                ..Default::default()
                            }),
                            action: kvrpcpb::Action::NoAction as i32,
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    assert!(req.force_sync_commit);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 250,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_locks_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 9);
                    assert_eq!(req.keys, vec![vec![4]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            use_async_commit: false,
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 9);
                    assert_eq!(req.commit_version, 250);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));
        let client = Arc::new(TimestampedPdClient::new(
            base_client,
            Timestamp {
                physical: 100,
                logical: 0,
                ..Default::default()
            },
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 9;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 2);
        assert_eq!(check_secondary_locks_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_resolve_lock_lite_sends_keys() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_locks_resolve_lock_lite_threshold_can_disable_lite() {
        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert!(
                        req.keys.is_empty(),
                        "resolve-lock-lite should be disabled when threshold is 0"
                    );
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let client = Arc::new(TimestampedPdClient::new_with_resolve_lock_lite_threshold(
            base_client,
            Timestamp::default(),
            0,
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
    }

    #[tokio::test]
    async fn test_lock_resolver_resolve_locks_once_uses_resolve_lock_lite_keys() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.keys, vec![vec![1u8]]);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let result = lock_resolver
            .resolve_locks_once(
                vec![lock],
                Timestamp::default(),
                client,
                Keyspace::Disable,
                false,
            )
            .await
            .unwrap();

        assert_eq!(result.ms_before_txn_expired, 0);
        assert!(result.live_locks.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_locks_for_read_force_resolve_lock_lite_sends_keys_even_when_threshold_disabled(
    ) {
        let (resolve_lock_keys_tx, resolve_lock_keys_rx) =
            tokio::sync::oneshot::channel::<Vec<Vec<u8>>>();
        let resolve_lock_keys_tx = Arc::new(Mutex::new(Some(resolve_lock_keys_tx)));

        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook({
            let resolve_lock_keys_tx = resolve_lock_keys_tx.clone();
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if let Some(tx) = resolve_lock_keys_tx.lock().unwrap().take() {
                        let _ = tx.send(req.keys.clone());
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            }
        })));

        let client = Arc::new(TimestampedPdClient::new_with_resolve_lock_lite_threshold(
            base_client,
            Timestamp::default(),
            0,
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 256;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let lock_tracker = ReadLockTracker::default();

        let _result = resolve_locks_for_read(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            true,
            Some(lock_tracker.clone()),
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();

        let keys = tokio::time::timeout(std::time::Duration::from_secs(1), resolve_lock_keys_rx)
            .await
            .expect("resolve_lock_for_read should issue resolve-lock cleanup request")
            .expect("resolve-lock request should report the `keys` field");
        assert_eq!(keys, vec![vec![1]]);
    }

    #[tokio::test]
    async fn test_lock_resolver_resolve_locks_for_read_detaches_cleanup_task() {
        let (resolve_lock_keys_tx, resolve_lock_keys_rx) =
            tokio::sync::oneshot::channel::<Vec<Vec<u8>>>();
        let resolve_lock_keys_tx = Arc::new(Mutex::new(Some(resolve_lock_keys_tx)));

        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook({
            let resolve_lock_keys_tx = resolve_lock_keys_tx.clone();
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if let Some(tx) = resolve_lock_keys_tx.lock().unwrap().take() {
                        let _ = tx.send(req.keys.clone());
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            }
        })));

        let client = Arc::new(TimestampedPdClient::new_with_resolve_lock_lite_threshold(
            base_client,
            Timestamp::default(),
            0,
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 256;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let result = lock_resolver
            .resolve_locks_for_read(
                vec![lock],
                Timestamp::default(),
                client,
                Keyspace::Disable,
                true,
            )
            .await
            .unwrap();

        assert_eq!(result.ms_before_txn_expired, 0);
        assert!(result.live_locks.is_empty());
        assert_eq!(result.resolved_locks, vec![1]);
        assert!(result.committed_locks.is_empty());

        let keys = tokio::time::timeout(std::time::Duration::from_secs(1), resolve_lock_keys_rx)
            .await
            .expect("resolve_locks_for_read should issue resolve-lock cleanup request")
            .expect("resolve-lock request should report the `keys` field");
        assert_eq!(keys, vec![vec![1]]);

        assert_eq!(
            lock_resolver
                .read_lock_tracker
                .inner
                .read()
                .await
                .async_cleanup_tasks
                .len(),
            1,
            "resolve_locks_for_read should track the background cleanup task"
        );

        lock_resolver.close().await;
        assert_eq!(
            lock_resolver
                .read_lock_tracker
                .inner
                .read()
                .await
                .async_cleanup_tasks
                .len(),
            0,
            "close should abort and clear background cleanup tasks"
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_for_read_min_commit_ts_pushed_marks_resolved_without_cleanup() {
        let txn_id = 7;

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut lock_info = kvrpcpb::LockInfo::default();
                    lock_info.lock_version = txn_id;
                    lock_info.use_async_commit = false;
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        action: kvrpcpb::Action::MinCommitTsPushed as i32,
                        lock_info: Some(lock_info),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("MinCommitTsPushed should not trigger resolve-lock cleanup");
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    panic!("MinCommitTsPushed should not trigger secondary checks");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![2];
        lock.primary_lock = vec![1];
        lock.lock_version = txn_id;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let ctx = ResolveLocksContext::default();
        let lock_tracker = ReadLockTracker::default();

        let result = resolve_locks_for_read(
            ctx.clone(),
            vec![lock],
            Timestamp::from_version(5),
            client,
            Keyspace::Disable,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            false,
            Some(lock_tracker),
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.ms_before_txn_expired, 0);
        assert!(result.live_locks.is_empty());
        assert_eq!(result.resolved_locks, vec![txn_id]);
        assert!(result.committed_locks.is_empty());
        assert!(ctx.get_resolved(txn_id).await.is_none());
    }

    #[tokio::test]
    async fn test_resolve_locks_for_read_expired_async_commit_lock_checks_secondaries_and_marks_committed(
    ) {
        let txn_id = 7;

        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_locks_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_locks_count_captured = check_secondary_locks_count.clone();

        let (resolve_lock_req_tx, resolve_lock_req_rx) =
            tokio::sync::oneshot::channel::<(u64, u64, Vec<Vec<u8>>)>();
        let resolve_lock_req_tx = Arc::new(Mutex::new(Some(resolve_lock_req_tx)));

        let base_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook({
            let resolve_lock_req_tx = resolve_lock_req_tx.clone();
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert!(!req.force_sync_commit);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 1,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: txn_id,
                            use_async_commit: true,
                            min_commit_ts: 150,
                            secondaries: vec![vec![3]],
                            ..Default::default()
                        }),
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_locks_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, txn_id);
                    assert_eq!(req.keys, vec![vec![3]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        commit_ts: 200,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if let Some(tx) = resolve_lock_req_tx.lock().unwrap().take() {
                        let _ = tx.send((req.start_version, req.commit_version, req.keys.clone()));
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            }
        })));
        let client = Arc::new(TimestampedPdClient::new_with_resolve_lock_lite_threshold(
            base_client,
            Timestamp {
                physical: 100,
                logical: 0,
                ..Default::default()
            },
            0,
        ));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = txn_id;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let ctx = ResolveLocksContext::default();
        let lock_resolver = LockResolver::new(ctx.clone());
        let result = lock_resolver
            .resolve_locks_for_read(
                vec![lock],
                Timestamp::from_version(250),
                client,
                Keyspace::Disable,
                true,
            )
            .await
            .unwrap();

        assert_eq!(result.ms_before_txn_expired, 0);
        assert!(result.live_locks.is_empty());
        assert!(result.resolved_locks.is_empty());
        assert_eq!(result.committed_locks, vec![txn_id]);

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(check_secondary_locks_count.load(Ordering::SeqCst), 1);

        let (start_version, commit_version, keys) =
            tokio::time::timeout(std::time::Duration::from_secs(1), resolve_lock_req_rx)
                .await
                .expect("resolve_locks_for_read should issue resolve-lock cleanup request")
                .expect("resolve-lock request should report request metadata");
        assert_eq!(start_version, txn_id);
        assert_eq!(commit_version, 200);
        assert_eq!(keys, vec![vec![1]]);

        let resolved = ctx
            .get_resolved(txn_id)
            .await
            .expect("expired async-commit lock resolution should cache committed status");
        assert!(
            matches!(
                &resolved.kind,
                TransactionStatusKind::Committed(ts) if ts.version() == 200
            ),
            "cached status should include commit ts returned by secondary checks"
        );

        lock_resolver.close().await;
    }

    #[tokio::test]
    async fn test_resolve_locks_for_read_classifies_committed_resolved_and_live_locks() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    match req.lock_ts {
                        1 => {
                            let resp = kvrpcpb::CheckTxnStatusResponse {
                                commit_version: 5,
                                action: kvrpcpb::Action::NoAction as i32,
                                ..Default::default()
                            };
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        2 => {
                            let resp = kvrpcpb::CheckTxnStatusResponse {
                                commit_version: 15,
                                action: kvrpcpb::Action::NoAction as i32,
                                ..Default::default()
                            };
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        3 => {
                            let resp = kvrpcpb::CheckTxnStatusResponse {
                                commit_version: 0,
                                action: kvrpcpb::Action::NoAction as i32,
                                ..Default::default()
                            };
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        4 => {
                            let mut lock_info = kvrpcpb::LockInfo::default();
                            lock_info.lock_version = req.lock_ts;
                            lock_info.use_async_commit = false;
                            let resp = kvrpcpb::CheckTxnStatusResponse {
                                lock_ttl: 100,
                                action: kvrpcpb::Action::NoAction as i32,
                                lock_info: Some(lock_info),
                                ..Default::default()
                            };
                            return Ok(Box::new(resp) as Box<dyn Any>);
                        }
                        other => panic!("unexpected check_txn_status lock_ts: {other}"),
                    }
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut committed_visible = kvrpcpb::LockInfo::default();
        committed_visible.key = vec![1];
        committed_visible.primary_lock = vec![1];
        committed_visible.lock_version = 1;
        committed_visible.lock_type = kvrpcpb::Op::Put as i32;

        let mut committed_in_future = kvrpcpb::LockInfo::default();
        committed_in_future.key = vec![2];
        committed_in_future.primary_lock = vec![2];
        committed_in_future.lock_version = 2;
        committed_in_future.lock_type = kvrpcpb::Op::Put as i32;

        let mut rolled_back = kvrpcpb::LockInfo::default();
        rolled_back.key = vec![3];
        rolled_back.primary_lock = vec![3];
        rolled_back.lock_version = 3;
        rolled_back.lock_type = kvrpcpb::Op::Put as i32;

        let mut live = kvrpcpb::LockInfo::default();
        live.key = vec![4];
        live.primary_lock = vec![4];
        live.lock_version = 4;
        live.lock_type = kvrpcpb::Op::Put as i32;

        let ctx = ResolveLocksContext::default();
        let result = resolve_locks_for_read(
            ctx.clone(),
            vec![
                committed_visible,
                committed_in_future,
                rolled_back,
                live.clone(),
            ],
            Timestamp::from_version(10),
            client,
            Keyspace::Disable,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            true,
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();

        assert_eq!(result.ms_before_txn_expired, 100);
        assert!(result.live_locks.iter().any(|lock| lock.lock_version == 4));
        assert_eq!(result.resolved_locks, vec![2, 3]);
        assert_eq!(result.committed_locks, vec![1]);

        let committed_visible_status = ctx
            .get_resolved(1)
            .await
            .expect("committed status should be cached");
        assert!(
            matches!(
                &committed_visible_status.kind,
                TransactionStatusKind::Committed(ts) if ts.version() == 5
            ),
            "committed status should include commit ts"
        );

        let committed_in_future_status = ctx
            .get_resolved(2)
            .await
            .expect("committed status should be cached");
        assert!(
            matches!(
                &committed_in_future_status.kind,
                TransactionStatusKind::Committed(ts) if ts.version() == 15
            ),
            "committed status should include commit ts"
        );

        let rolled_back_status = ctx
            .get_resolved(3)
            .await
            .expect("rolled back status should be cached");
        assert!(
            matches!(&rolled_back_status.kind, TransactionStatusKind::RolledBack),
            "rolled back status should be cached"
        );
        assert!(
            ctx.get_resolved(4).await.is_none(),
            "live locks should not be stored in resolved cache"
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_resolve_lock_lite_skips_primary() {
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count_captured = resolve_lock_count.clone();

        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 1;
        lock.lock_ttl = 100;
        lock.txn_size = 1;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_shared_lock_returns_error() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn Any| panic!("shared lock should be rejected before sending requests"),
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = SHARED_LOCK_TYPE;

        let err = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .expect_err("shared lock wrapper should not be resolved directly");
        match err {
            Error::StringError(msg) => {
                assert!(msg.contains("misuse of resolve_locks"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_rolls_back_pessimistic_lock() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![2],
                        lock_version: 7,
                        lock_for_update_ts: 9,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 9);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 9;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_region_resolve_rolls_back_region() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![2],
                        lock_version: 7,
                        lock_for_update_ts: 9,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert!(req.keys.is_empty());
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 9);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 9;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_rolls_back_shared_pessimistic_lock() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![2],
                        lock_version: 7,
                        lock_for_update_ts: 9,
                        lock_type: SHARED_PESSIMISTIC_LOCK_TYPE,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 9);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 9;
        lock.lock_ttl = 100;
        lock.lock_type = SHARED_PESSIMISTIC_LOCK_TYPE;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_final_status_uses_pessimistic_rollback() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_shared_pessimistic_final_status_uses_pessimistic_rollback() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = SHARED_PESSIMISTIC_LOCK_TYPE;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_final_status_primary_lock_skips_rollback() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 0);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_final_status_primary_then_secondary_rolls_back_once() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![2]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut primary_lock = kvrpcpb::LockInfo::default();
        primary_lock.key = vec![1];
        primary_lock.primary_lock = vec![1];
        primary_lock.lock_version = 7;
        primary_lock.lock_for_update_ts = 11;
        primary_lock.lock_ttl = 100;
        primary_lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let mut secondary_lock = kvrpcpb::LockInfo::default();
        secondary_lock.key = vec![2];
        secondary_lock.primary_lock = vec![1];
        secondary_lock.lock_version = 7;
        secondary_lock.lock_for_update_ts = 11;
        secondary_lock.lock_ttl = 100;
        secondary_lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(
            vec![primary_lock, secondary_lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
        )
        .await
        .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_non_lite_defaults_to_key_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![2]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![2];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        lock.txn_size = 16;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_non_lite_rolls_back_region_once() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert!(req.keys.is_empty());
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock_1 = kvrpcpb::LockInfo::default();
        lock_1.key = vec![2];
        lock_1.primary_lock = vec![1];
        lock_1.lock_version = 7;
        lock_1.lock_for_update_ts = 11;
        lock_1.lock_ttl = 100;
        lock_1.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        // Keep txn_size non-lite while region-scoped rollback is enabled by option.
        lock_1.txn_size = 16;

        let mut lock_2 = kvrpcpb::LockInfo::default();
        lock_2.key = vec![3];
        lock_2.primary_lock = vec![1];
        lock_2.lock_version = 7;
        lock_2.lock_for_update_ts = 11;
        lock_2.lock_ttl = 100;
        lock_2.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        lock_2.txn_size = 16;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock_1, lock_2],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_mixed_lock_types_keep_separate_region_cleanup() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 9,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert!(req.keys.is_empty());
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 9);
                    assert!(req.keys.is_empty());
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut pessimistic_lock = kvrpcpb::LockInfo::default();
        pessimistic_lock.key = vec![2];
        pessimistic_lock.primary_lock = vec![1];
        pessimistic_lock.lock_version = 7;
        pessimistic_lock.lock_for_update_ts = 11;
        pessimistic_lock.lock_ttl = 100;
        pessimistic_lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        pessimistic_lock.txn_size = 16;

        let mut prewrite_lock = kvrpcpb::LockInfo::default();
        prewrite_lock.key = vec![3];
        prewrite_lock.primary_lock = vec![1];
        prewrite_lock.lock_version = 7;
        prewrite_lock.lock_ttl = 100;
        prewrite_lock.lock_type = kvrpcpb::Op::Put as i32;
        prewrite_lock.txn_size = 16;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![pessimistic_lock, prewrite_lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_pessimistic_non_lite_region_rollback_retries_on_region_error() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert!(req.keys.is_empty());
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    let attempt =
                        pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let resp = kvrpcpb::PessimisticRollbackResponse {
                            region_error: Some(errorpb::Error::default()),
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![2];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        lock.txn_size = 16;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 2);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_non_pessimistic_returns_error() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![2],
                        lock_version: 7,
                        lock_type: kvrpcpb::Op::Put as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let err = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .expect_err("non-pessimistic primary_mismatch should surface as key error");
        match err {
            Error::KeyError(key_err) => assert!(key_err.primary_mismatch.is_some()),
            other => panic!("unexpected error type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_primary_lock_skips_rollback() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 7,
                        lock_for_update_ts: 9,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 9;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_region_resolve_primary_lock_skips_rollback() {
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![1],
                        lock_version: 7,
                        lock_for_update_ts: 9,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 9;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let resolve_result = resolve_locks_with_options(
            ResolveLocksContext::default(),
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
            Backoff::no_jitter_backoff(0, 0, 10),
            None,
            LockResolverRpcContext::default(),
        )
        .await
        .unwrap();
        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_resolve_locks_primary_mismatch_uses_max_for_update_ts_when_missing() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![1],
                        primary_lock: vec![2],
                        lock_version: 7,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 0;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let live_locks = resolve_locks(vec![lock], Timestamp::default(), client, Keyspace::Disable)
            .await
            .unwrap();
        assert!(live_locks.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_locks_pessimistic_lock_uses_pessimistic_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cleanup_locks_mixed_lock_types_same_txn_keeps_pessimistic_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 9,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![2]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        assert_eq!(req.txn_infos.len(), 1);
                        assert_eq!(req.txn_infos[0].txn, 7);
                        assert_eq!(req.txn_infos[0].status, 9);
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut non_pessimistic_lock = kvrpcpb::LockInfo::default();
        non_pessimistic_lock.key = vec![1];
        non_pessimistic_lock.primary_lock = vec![1];
        non_pessimistic_lock.lock_version = 7;
        non_pessimistic_lock.lock_ttl = 100;
        non_pessimistic_lock.lock_type = kvrpcpb::Op::Put as i32;

        let mut pessimistic_lock = kvrpcpb::LockInfo::default();
        pessimistic_lock.key = vec![2];
        pessimistic_lock.primary_lock = vec![1];
        pessimistic_lock.lock_version = 7;
        pessimistic_lock.lock_for_update_ts = 11;
        pessimistic_lock.lock_ttl = 100;
        pessimistic_lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&non_pessimistic_lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(
                store,
                vec![non_pessimistic_lock, pessimistic_lock],
                client,
                Keyspace::Disable,
            )
            .await
            .unwrap();

        // Committed txn status should be cached and reused across lock types.
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cleanup_locks_mixed_lock_types_across_calls_keeps_pessimistic_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 9,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![2]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        assert_eq!(req.txn_infos.len(), 1);
                        assert_eq!(req.txn_infos[0].txn, 7);
                        assert_eq!(req.txn_infos[0].status, 9);
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut non_pessimistic_lock = kvrpcpb::LockInfo::default();
        non_pessimistic_lock.key = vec![1];
        non_pessimistic_lock.primary_lock = vec![1];
        non_pessimistic_lock.lock_version = 7;
        non_pessimistic_lock.lock_ttl = 100;
        non_pessimistic_lock.lock_type = kvrpcpb::Op::Put as i32;

        let mut pessimistic_lock = kvrpcpb::LockInfo::default();
        pessimistic_lock.key = vec![2];
        pessimistic_lock.primary_lock = vec![1];
        pessimistic_lock.lock_version = 7;
        pessimistic_lock.lock_for_update_ts = 11;
        pessimistic_lock.lock_ttl = 100;
        pessimistic_lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&non_pessimistic_lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());

        lock_resolver
            .cleanup_locks(
                store.clone(),
                vec![non_pessimistic_lock],
                client.clone(),
                Keyspace::Disable,
            )
            .await
            .unwrap();
        lock_resolver
            .cleanup_locks(store, vec![pessimistic_lock], client, Keyspace::Disable)
            .await
            .unwrap();

        // Txn status is cached across calls, but pessimistic rollback must still execute.
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cleanup_locks_lock_not_exist_do_nothing_is_not_cached() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert!(req.txn_infos.is_empty());
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());

        lock_resolver
            .cleanup_locks(
                store.clone(),
                vec![lock.clone()],
                client.clone(),
                Keyspace::Disable,
            )
            .await
            .unwrap();
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        // `LockNotExistDoNothing` is not a determined final status and should not be cached.
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 2);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cleanup_locks_shared_pessimistic_lock_uses_pessimistic_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = SHARED_PESSIMISTIC_LOCK_TYPE;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cleanup_locks_shared_lock_returns_error() {
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn Any| panic!("shared lock should be rejected before sending requests"),
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = SHARED_LOCK_TYPE;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let err = lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .expect_err("shared lock wrapper should not be cleaned directly");
        match err {
            Error::StringError(msg) => {
                assert!(msg.contains("misuse of cleanup_locks"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_cleanup_locks_batch_resolve_retries_on_region_error() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let batch_resolve_attempts = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let batch_resolve_attempts_captured = batch_resolve_attempts.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 0,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.txn_infos.len(), 1);
                    let attempt = batch_resolve_attempts_captured.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        let resp = kvrpcpb::ResolveLockResponse {
                            region_error: Some(errorpb::Error::default()),
                            ..Default::default()
                        };
                        return Ok(Box::new(resp) as Box<dyn Any>);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(batch_resolve_attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cleanup_locks_pessimistic_lock_uses_max_for_update_ts_when_missing() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 0;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cleanup_locks_async_primary_without_secondaries_uses_primary_min_commit_ts() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        lock_info: Some(kvrpcpb::LockInfo {
                            use_async_commit: true,
                            min_commit_ts: 123,
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    assert_eq!(req.txn_infos.len(), 1);
                    assert_eq!(req.txn_infos[0].txn, 7);
                    assert_eq!(req.txn_infos[0].status, 123);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    return Ok(
                        Box::<kvrpcpb::CheckSecondaryLocksResponse>::default() as Box<dyn Any>
                    );
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cleanup_locks_async_commit_missing_secondary_rolls_back_when_commit_ts_zero() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_secondary_locks_count = Arc::new(AtomicUsize::new(0));
        let batch_resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let check_secondary_locks_count_captured = check_secondary_locks_count.clone();
        let batch_resolve_lock_count_captured = batch_resolve_lock_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.current_ts, u64::MAX);
                    assert!(req.rollback_if_not_exist);
                    assert!(!req.force_sync_commit);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        lock_info: Some(kvrpcpb::LockInfo {
                            lock_version: 7,
                            use_async_commit: true,
                            min_commit_ts: 123,
                            secondaries: vec![vec![3]],
                            ..Default::default()
                        }),
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_locks_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.keys, vec![vec![3]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse::default();
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    batch_resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.txn_infos.len(), 1);
                    assert_eq!(req.txn_infos[0].txn, 7);
                    assert_eq!(req.txn_infos[0].status, 0);
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(check_secondary_locks_count.load(Ordering::SeqCst), 1);
        assert_eq!(batch_resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cleanup_locks_locked_non_async_status_returns_error() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        lock_info: Some(kvrpcpb::LockInfo {
                            use_async_commit: false,
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    panic!("non-async lock should not trigger check_secondaries");
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!("non-async locked status should not reach batch resolve");
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        let err = lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .expect_err("non-async locked status should return error");
        match err {
            Error::ResolveLockError(locks) => assert_eq!(locks.len(), 1),
            other => panic!("unexpected error: {other:?}"),
        }

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cleanup_locks_primary_mismatch_rolls_back_pessimistic_lock() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let mut mismatch = kvrpcpb::PrimaryMismatch::default();
                    mismatch.lock_info = Some(kvrpcpb::LockInfo {
                        key: vec![9],
                        primary_lock: vec![10],
                        lock_version: 123,
                        lock_type: kvrpcpb::Op::PessimisticLock as i32,
                        ..Default::default()
                    });
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(mismatch),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    assert_eq!(req.keys, vec![vec![1]]);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.for_update_ts, 11);
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 1);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_cleanup_locks_pessimistic_primary_lock_skips_rollback() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let pessimistic_rollback_count = Arc::new(AtomicUsize::new(0));
        let non_empty_batch_resolve_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let pessimistic_rollback_count_captured = pessimistic_rollback_count.clone();
        let non_empty_batch_resolve_count_captured = non_empty_batch_resolve_count.clone();
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        action: kvrpcpb::Action::LockNotExistDoNothing as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    pessimistic_rollback_count_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    if !req.txn_infos.is_empty() {
                        non_empty_batch_resolve_count_captured.fetch_add(1, Ordering::SeqCst);
                    }
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![1];
        lock.lock_version = 7;
        lock.lock_for_update_ts = 11;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;

        let store = client
            .clone()
            .store_for_key(&lock.key.clone().into())
            .await
            .unwrap();
        let lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 0);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }
}
