// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

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
use crate::store::RegionStore;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::transaction::requests::new_check_secondary_locks_request;
use crate::transaction::requests::new_check_txn_status_request;
use crate::transaction::requests::SecondaryLocksStatus;
use crate::transaction::requests::TransactionStatus;
use crate::transaction::requests::TransactionStatusKind;
use crate::Error;
use crate::Result;

fn format_key_for_log(key: &[u8]) -> String {
    let prefix_len = key.len().min(16);
    format!("len={}, prefix={:?}", key.len(), &key[..prefix_len])
}

// `client-go` treats both `PessimisticLock` and `SharedPessimisticLock` as pessimistic.
// The generated Rust proto in this repo currently lacks `SharedPessimisticLock`, so keep
// the numeric value for forward-compatible lock-type handling.
const SHARED_LOCK_TYPE: i32 = 7;
const SHARED_PESSIMISTIC_LOCK_TYPE: i32 = 8;

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

#[derive(Debug)]
pub(crate) struct ResolveLocksResult {
    pub(crate) live_locks: Vec<kvrpcpb::LockInfo>,
    // The minimum remaining time (in milliseconds) before any still-live lock expires.
    // Returns 0 when there is no live lock or the lock is already expired.
    pub(crate) ms_before_txn_expired: i64,
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
    Ok(
        resolve_locks_with_options(locks, timestamp, pd_client, keyspace, false)
            .await?
            .live_locks,
    )
}

pub(crate) async fn resolve_locks_with_options(
    locks: Vec<kvrpcpb::LockInfo>,
    timestamp: Timestamp,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    pessimistic_region_resolve: bool,
) -> Result<ResolveLocksResult> {
    const RESOLVE_LOCK_LITE_THRESHOLD: u64 = 16;

    debug!("resolving locks");
    let ts = pd_client.clone().get_timestamp().await?;
    let caller_start_ts = timestamp.version();
    let current_ts = ts.version();

    let mut live_locks = Vec::new();
    let mut ms_before_txn_expired: Option<i64> = None;
    let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());

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
                        OPTIMISTIC_BACKOFF,
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
                                pd_client.clone(),
                                keyspace,
                                OPTIMISTIC_BACKOFF,
                            )
                            .await?
                            {
                                pessimistic_clean_regions
                                    .entry(lock.lock_version)
                                    .or_default()
                                    .insert(cleaned_region);
                            }
                        } else {
                            rollback_pessimistic_lock(&lock, pd_client.clone(), keyspace).await?;
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
                        pd_client.clone(),
                        keyspace,
                        OPTIMISTIC_BACKOFF,
                    )
                    .await?
                    {
                        pessimistic_clean_regions
                            .entry(lock.lock_version)
                            .or_default()
                            .insert(cleaned_region);
                    }
                } else {
                    rollback_pessimistic_lock(&lock, pd_client.clone(), keyspace).await?;
                }
                continue;
            }

            let resolve_lite = lock.txn_size < RESOLVE_LOCK_LITE_THRESHOLD;
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
                pd_client.clone(),
                keyspace,
                OPTIMISTIC_BACKOFF,
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
async fn resolve_lock_with_retry(
    #[allow(clippy::ptr_arg)] key: &Vec<u8>,
    start_version: u64,
    commit_version: u64,
    is_txn_file: bool,
    resolve_lite: bool,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    mut backoff: Backoff,
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
        if resolve_lite {
            request.keys = vec![key.clone()];
        }
        let plan_builder =
            match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
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
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
) -> Result<()> {
    // Match client-go `resolvePessimisticLock`: primary key is considered resolved by
    // CheckTxnStatus, so no extra rollback request is needed here.
    if lock.key == lock.primary_lock {
        return Ok(());
    }

    let for_update_ts = pessimistic_rollback_for_update_ts(lock);
    let request = requests::new_pessimistic_rollback_request(
        vec![lock.key.clone()],
        lock.lock_version,
        for_update_ts,
    );
    let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
        .retry_multi_region(DEFAULT_REGION_BACKOFF)
        .extract_error()
        .plan();
    let _ = plan.execute().await?;
    Ok(())
}

async fn rollback_pessimistic_lock_with_retry(
    lock: &kvrpcpb::LockInfo,
    pd_client: Arc<impl PdClient>,
    keyspace: Keyspace,
    mut backoff: Backoff,
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
        let request = requests::new_pessimistic_rollback_request(
            Vec::new(),
            lock.lock_version,
            for_update_ts,
        );
        let plan_builder =
            match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
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
            Ok(_) => return Ok(Some(ver_id)),
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
                    sleep(duration).await;
                    continue;
                }
                None => return Err(e),
            },
            Err(err) => return Err(err),
        }
    }
}

#[derive(Default, Clone)]
pub struct ResolveLocksContext {
    // Record the status of each transaction.
    pub(crate) resolved: Arc<RwLock<HashMap<u64, Arc<TransactionStatus>>>>,
    pub(crate) clean_regions: Arc<RwLock<HashMap<u64, HashSet<RegionVerId>>>>,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolveLocksOptions {
    pub async_commit_only: bool,
    pub batch_size: u32,
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
        self.resolved.read().await.get(&txn_id).cloned()
    }

    pub async fn save_resolved(&mut self, txn_id: u64, txn_status: Arc<TransactionStatus>) {
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

    pub async fn save_cleaned_region(&mut self, txn_id: u64, region: RegionVerId) {
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
}

impl LockResolver {
    pub fn new(ctx: ResolveLocksContext) -> Self {
        Self { ctx }
    }

    /// _Cleanup_ the given locks. Returns whether all the given locks are resolved.
    ///
    /// Note: Will rollback RUNNING transactions. ONLY use in GC.
    pub async fn cleanup_locks(
        &mut self,
        store: RegionStore,
        locks: Vec<kvrpcpb::LockInfo>,
        pd_client: Arc<impl PdClient>, // TODO: make pd_client a member of LockResolver
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
                    rollback_pessimistic_lock(&l, pd_client.clone(), keyspace).await?;
                    continue;
                }
                Err(err) => return Err(err),
            };

            if is_pessimistic {
                // Match client-go `BatchResolveLocks`: pessimistic locks are cleaned by
                // `PessimisticRollback` instead of being packed into `TxnInfo` for
                // `BatchResolveLock`.
                rollback_pessimistic_lock(&l, pd_client.clone(), keyspace).await?;
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
                    let commit_ts = if let Some(commit_ts) = &secondary_status.commit_ts {
                        commit_ts.version()
                    } else {
                        secondary_status.min_commit_ts
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
            .batch_resolve_locks(pd_client.clone(), keyspace, store.clone(), txn_info_vec)
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
        &mut self,
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
        let req = new_check_txn_status_request(
            primary,
            txn_id,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
            is_txn_file,
        );
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
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
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        keys: Vec<Vec<u8>>,
        txn_id: u64,
        primary_min_commit_ts: u64,
    ) -> Result<SecondaryLocksStatus> {
        let req = new_check_secondary_locks_request(keys, txn_id);
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(Collect)
            .plan();
        let mut secondary_status = plan.execute().await?;
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

    async fn batch_resolve_locks(
        &mut self,
        pd_client: Arc<impl PdClient>,
        keyspace: Keyspace,
        store: RegionStore,
        txn_infos: Vec<TxnInfo>,
    ) -> Result<RegionVerId> {
        let mut backoff = DEFAULT_REGION_BACKOFF;
        loop {
            let ver_id = store.region_with_leader.ver_id();
            let request = requests::new_batch_resolve_lock_request(txn_infos.clone());
            let plan_builder =
                match crate::request::PlanBuilder::new(pd_client.clone(), keyspace, request)
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
        &mut self,
        mut backoff: Backoff,
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
                            self.ctx.resolved.write().await.remove(&lock.lock_version);
                            force_sync_commit = true;
                            continue;
                        }

                        let commit_version = secondary_status
                            .commit_ts
                            .as_ref()
                            .map_or(secondary_status.min_commit_ts, |ts| ts.version());
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

    use async_trait::async_trait;
    use fail::FailScenario;
    use serial_test::serial;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;

    struct TimestampedPdClient {
        inner: Arc<MockPdClient>,
        timestamp: Timestamp,
    }

    impl TimestampedPdClient {
        fn new(inner: Arc<MockPdClient>, timestamp: Timestamp) -> Self {
            Self { inner, timestamp }
        }
    }

    #[async_trait]
    impl PdClient for TimestampedPdClient {
        type KvClient = MockKvClient;

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

        async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
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
        let region1 = MockPdClient::region1();
        let resolved_region = resolve_lock_with_retry(
            &key,
            1,
            2,
            false,
            false,
            client.clone(),
            keyspace,
            backoff.clone(),
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
        resolve_lock_with_retry(&key, 3, 4, false, false, client, keyspace, backoff)
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
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            false,
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
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
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
            vec![lock_1, lock_2],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
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
            vec![pessimistic_lock, prewrite_lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
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
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
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
            vec![lock],
            Timestamp::default(),
            client,
            Keyspace::Disable,
            true,
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());

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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());

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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
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
        let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
        lock_resolver
            .cleanup_locks(store, vec![lock], client, Keyspace::Disable)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(pessimistic_rollback_count.load(Ordering::SeqCst), 0);
        assert_eq!(non_empty_batch_resolve_count.load(Ordering::SeqCst), 0);
    }
}
