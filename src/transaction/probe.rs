// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Test probe helpers.
//!
//! These utilities are intended to make it easier to port client-go tests that
//! rely on exported `*Probe` types (for example, `txnlock/test_probe.go`).

use std::sync::Arc;

use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::timestamp::TimestampExt;
use crate::transaction::BoundLockResolver;
use crate::transaction::Lock;
use crate::transaction::TransactionStatusKind;
use crate::transaction::TxnStatus;
use crate::Error;
use crate::Result;

const NON_ASYNC_COMMIT_LOCK_ERROR_MESSAGE: &str =
    "check_secondary_locks receives a non-async-commit lock";

fn non_async_commit_lock_error() -> Error {
    Error::InternalError {
        message: NON_ASYNC_COMMIT_LOCK_ERROR_MESSAGE.to_owned(),
    }
}

/// Exposes lock utilities for testing.
///
/// This mirrors client-go `txnlock.LockProbe`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LockProbe;

impl LockProbe {
    /// Returns a locked transaction status with the provided async-commit metadata.
    ///
    /// This mirrors client-go `LockProbe.NewLockStatus`.
    #[must_use]
    pub fn new_lock_status(
        self,
        keys: Vec<Vec<u8>>,
        use_async_commit: bool,
        min_commit_ts: u64,
    ) -> TxnStatus {
        let mut lock = kvrpcpb::LockInfo::default();
        lock.secondaries = keys;
        lock.use_async_commit = use_async_commit;
        lock.min_commit_ts = min_commit_ts;

        TxnStatus {
            kind: TransactionStatusKind::Locked(1, lock),
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        }
    }

    /// Returns the lock key carried in a locked transaction status.
    ///
    /// This mirrors client-go `LockProbe.GetPrimaryKeyFromTxnStatus`.
    #[must_use]
    pub fn primary_key_from_txn_status(self, status: &TxnStatus) -> Vec<u8> {
        match &status.kind {
            TransactionStatusKind::Locked(_, lock) => lock.key.clone(),
            _ => Vec::new(),
        }
    }

    /// Returns the secondary keys carried in a locked transaction status.
    ///
    /// This mirrors client-go `LockResolverProbe.GetSecondariesFromTxnStatus`.
    #[must_use]
    pub fn secondaries_from_txn_status(self, status: &TxnStatus) -> Vec<Vec<u8>> {
        match &status.kind {
            TransactionStatusKind::Locked(_, lock) => lock.secondaries.clone(),
            _ => Vec::new(),
        }
    }
}

/// Wraps a [`BoundLockResolver`] and exposes additional helpers for tests.
///
/// This is inspired by client-go `txnlock.LockResolverProbe`.
#[doc(hidden)]
pub struct LockResolverProbe<PdC: PdClient> {
    inner: BoundLockResolver<PdC>,
}

impl<PdC: PdClient> LockResolverProbe<PdC> {
    /// Creates a new probe for the provided resolver.
    #[must_use]
    pub fn new(inner: BoundLockResolver<PdC>) -> Self {
        Self { inner }
    }

    /// Returns the underlying resolver.
    #[must_use]
    pub fn into_inner(self) -> BoundLockResolver<PdC> {
        self.inner
    }

    /// Sets a callback invoked when the resolver sees locks.
    ///
    /// This mirrors client-go `LockResolverProbe.SetMeetLockCallback`.
    #[doc(hidden)]
    pub fn set_meet_lock_callback<F>(&mut self, callback: F)
    where
        F: Fn(&[kvrpcpb::LockInfo]) + Send + Sync + 'static,
    {
        self.inner.set_meet_lock_callback(callback);
    }

    /// Clears the meet-lock callback.
    #[doc(hidden)]
    pub fn clear_meet_lock_callback(&mut self) {
        self.inner.clear_meet_lock_callback();
    }

    /// Returns `true` if `err` represents a transaction-not-found error.
    ///
    /// This mirrors client-go `LockResolverProbe.IsErrorNotFound`.
    #[must_use]
    pub fn is_error_not_found(&self, err: &Error) -> bool {
        matches!(err, Error::TxnNotFound(_))
    }

    /// Returns `true` if `err` represents an async-commit fallback due to observing a
    /// non-async-commit secondary lock.
    ///
    /// This mirrors client-go `LockResolverProbe.IsNonAsyncCommitLock`.
    #[must_use]
    pub fn is_non_async_commit_lock(&self, err: &Error) -> bool {
        matches!(
            err,
            Error::InternalError { message }
                if message == NON_ASYNC_COMMIT_LOCK_ERROR_MESSAGE
        )
    }

    /// Sends `CheckTxnStatus` and returns the decoded status.
    ///
    /// This mirrors client-go `LockResolverProbe.GetTxnStatus`.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_txn_status(
        &self,
        txn_id: u64,
        primary: Vec<u8>,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        lock_info: Option<Lock>,
    ) -> Result<TxnStatus> {
        let resolving_pessimistic_lock = lock_info.as_ref().is_some_and(Lock::is_pessimistic);
        let is_txn_file = lock_info.as_ref().is_some_and(Lock::is_txn_file);
        let status = self
            .inner
            .check_txn_status(
                txn_id,
                primary,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
                force_sync_commit,
                resolving_pessimistic_lock,
                is_txn_file,
            )
            .await?;
        Ok((*status).clone())
    }

    /// Queries TiKV for a txn's status for the provided lock.
    ///
    /// This mirrors client-go `LockResolverProbe.GetTxnStatusFromLock`.
    pub async fn get_txn_status_from_lock(
        &self,
        lock: Lock,
        caller_start_ts: u64,
        force_sync_commit: bool,
    ) -> Result<TxnStatus> {
        let lock = lock.into_proto();
        let status = self
            .inner
            .get_txn_status_from_lock_for_probe(&lock, caller_start_ts, force_sync_commit)
            .await?;
        Ok((*status).clone())
    }

    /// Returns the secondary keys carried in a locked transaction status.
    ///
    /// This mirrors client-go `LockResolverProbe.GetSecondariesFromTxnStatus`.
    #[must_use]
    pub fn secondaries_from_txn_status(&self, status: &TxnStatus) -> Vec<Vec<u8>> {
        match &status.kind {
            TransactionStatusKind::Locked(_, lock) => lock.secondaries.clone(),
            _ => Vec::new(),
        }
    }

    /// Checks the secondary locks of an async-commit transaction.
    ///
    /// This mirrors client-go `LockResolverProbe.CheckAllSecondaries`.
    pub async fn check_all_secondaries(&self, lock: Lock, status: &TxnStatus) -> Result<()> {
        let primary_lock = match &status.kind {
            TransactionStatusKind::Locked(_, lock) => lock,
            _ => {
                return Err(Error::StringError(
                    "check_all_secondaries requires locked txn status".to_owned(),
                ));
            }
        };

        let lock = lock.into_proto();
        let secondary_status = self
            .inner
            .check_all_secondaries_for_probe(
                primary_lock.secondaries.clone(),
                lock.lock_version,
                primary_lock.min_commit_ts,
            )
            .await?;
        if secondary_status.fallback_2pc {
            return Err(non_async_commit_lock_error());
        }
        Ok(())
    }

    /// Resolves an async-commit lock using secondary checks.
    ///
    /// This mirrors client-go `LockResolverProbe.ResolveAsyncCommitLock` and resolves locks
    /// synchronously.
    pub async fn resolve_async_commit_lock(&self, lock: Lock, status: TxnStatus) -> Result<()> {
        let primary_lock = match &status.kind {
            TransactionStatusKind::Locked(_, lock) => lock.clone(),
            _ => {
                return Err(Error::StringError(
                    "resolve_async_commit_lock requires locked txn status".to_owned(),
                ));
            }
        };

        let lock = lock.into_proto();
        let secondary_status = self
            .inner
            .check_all_secondaries_for_probe(
                primary_lock.secondaries.clone(),
                lock.lock_version,
                primary_lock.min_commit_ts,
            )
            .await?;
        if secondary_status.fallback_2pc {
            return Err(non_async_commit_lock_error());
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

        let mut resolved_status = status.clone();
        resolved_status.kind = if commit_version == 0 {
            TransactionStatusKind::RolledBack
        } else {
            TransactionStatusKind::Committed(crate::proto::pdpb::Timestamp::from_version(
                commit_version,
            ))
        };
        resolved_status.is_expired = false;
        let resolved_status = Arc::new(resolved_status);
        self.inner
            .save_resolved_for_probe(lock.lock_version, resolved_status)
            .await;

        let mut keys = secondary_status.resolve_keys;
        keys.push(lock.primary_lock);
        keys.push(lock.key);
        self.inner
            .resolve_lock_keys_for_probe(
                keys,
                lock.lock_version,
                commit_version,
                lock.is_txn_file,
                OPTIMISTIC_BACKOFF,
                None,
            )
            .await?;
        Ok(())
    }

    /// Resolves a single lock.
    ///
    /// This mirrors client-go `LockResolverProbe.ResolveLock`.
    pub async fn resolve_lock(&self, lock: Lock) -> Result<()> {
        let lock = lock.into_proto();
        self.inner
            .resolve_lock_for_probe(&lock, 0, OPTIMISTIC_BACKOFF, None)
            .await
    }

    /// Resolves a single pessimistic lock.
    ///
    /// This mirrors client-go `LockResolverProbe.ResolvePessimisticLock`.
    pub async fn resolve_pessimistic_lock(&self, lock: Lock) -> Result<()> {
        let lock = lock.into_proto();
        self.inner.resolve_pessimistic_lock_for_probe(&lock).await
    }

    /// Forces resolving the given lock by setting `lock_ttl = 0` before invoking lock resolution.
    ///
    /// This mirrors the helper used by client-go tests (`ForceResolveLock`).
    pub async fn force_resolve_lock(&self, lock: Lock) -> Result<()> {
        let mut lock = lock.into_proto();
        lock.lock_ttl = 0;
        let result = self
            .inner
            .resolve_locks_once(
                vec![lock],
                crate::proto::pdpb::Timestamp::from_version(0),
                false,
            )
            .await?;
        if result.live_locks.is_empty() {
            Ok(())
        } else {
            Err(Error::ResolveLockError(result.live_locks))
        }
    }

    /// Sets the resolving lock status for the underlying resolver.
    ///
    /// This mirrors client-go `LockResolverProbe.SetResolving`.
    pub async fn set_resolving(&self, caller_start_ts: u64, locks: Vec<Lock>) {
        let locks: Vec<kvrpcpb::LockInfo> = locks.into_iter().map(Lock::into_proto).collect();
        let _ = self
            .inner
            .record_resolving_locks(&locks, caller_start_ts)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::request::Keyspace;
    use crate::transaction::ResolveLocksContext;

    #[test]
    fn test_lock_probe_new_lock_status_carries_async_commit_metadata() {
        let status = LockProbe.new_lock_status(vec![b"k2".to_vec()], true, 42);
        let (ttl, lock) = match status.kind {
            TransactionStatusKind::Locked(ttl, lock) => (ttl, lock),
            other => panic!("unexpected status kind: {other:?}"),
        };
        assert_eq!(ttl, 1);
        assert_eq!(lock.secondaries, vec![b"k2".to_vec()]);
        assert!(lock.use_async_commit);
        assert_eq!(lock.min_commit_ts, 42);
    }

    #[test]
    fn test_lock_probe_primary_key_from_txn_status_reads_locked_key() {
        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = b"p".to_vec();
        let status = TxnStatus {
            kind: TransactionStatusKind::Locked(1, lock),
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        };
        assert_eq!(
            LockProbe.primary_key_from_txn_status(&status),
            b"p".to_vec()
        );
    }

    #[test]
    fn test_lock_probe_secondaries_from_txn_status_reads_locked_secondaries() {
        let mut lock = kvrpcpb::LockInfo::default();
        lock.secondaries = vec![b"s1".to_vec(), b"s2".to_vec()];
        let status = TxnStatus {
            kind: TransactionStatusKind::Locked(1, lock),
            action: kvrpcpb::Action::NoAction,
            is_expired: false,
        };
        assert_eq!(
            LockProbe.secondaries_from_txn_status(&status),
            vec![b"s1".to_vec(), b"s2".to_vec()]
        );
    }

    #[test]
    fn test_lock_resolver_probe_is_error_not_found_matches_txn_not_found() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn std::any::Any| Err(Error::Unimplemented),
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        assert!(probe.is_error_not_found(&Error::TxnNotFound(kvrpcpb::TxnNotFound::default())));
        assert!(!probe.is_error_not_found(&Error::Unimplemented));
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_resolve_lock_issues_resolve_lock_lite() {
        let resolve_lock_calls = Arc::new(AtomicUsize::new(0));
        let resolve_lock_calls_captured = resolve_lock_calls.clone();
        let expected_key = b"k".to_vec();
        let expected_key_captured = expected_key.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn std::any::Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 42);
                    assert_eq!(req.commit_version, 0);
                    assert_eq!(req.keys, vec![expected_key_captured.clone()]);
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
                }
                Err(Error::Unimplemented)
            },
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = expected_key;
        lock.primary_lock = b"p".to_vec();
        lock.lock_version = 42;
        lock.txn_size = 1;
        probe
            .resolve_lock(Lock::new(lock))
            .await
            .expect("resolve_lock should succeed");

        assert_eq!(resolve_lock_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_resolve_pessimistic_lock_issues_pessimistic_rollback() {
        let rollback_calls = Arc::new(AtomicUsize::new(0));
        let rollback_calls_captured = rollback_calls.clone();
        let expected_key = b"k".to_vec();
        let expected_key_captured = expected_key.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn std::any::Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>() {
                    rollback_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 42);
                    assert_eq!(req.for_update_ts, u64::MAX);
                    assert_eq!(req.keys, vec![expected_key_captured.clone()]);
                    return Ok(Box::new(kvrpcpb::PessimisticRollbackResponse::default()));
                }
                Err(Error::Unimplemented)
            },
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = expected_key;
        lock.primary_lock = b"p".to_vec();
        lock.lock_version = 42;
        lock.lock_type = kvrpcpb::Op::PessimisticLock as i32;
        probe
            .resolve_pessimistic_lock(Lock::new(lock))
            .await
            .expect("resolve_pessimistic_lock should succeed");

        assert_eq!(rollback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_check_all_secondaries_reports_non_async_commit_lock() {
        let check_secondary_calls = Arc::new(AtomicUsize::new(0));
        let check_secondary_calls_captured = check_secondary_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn std::any::Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.keys, vec![vec![1]]);
                    let resp = kvrpcpb::CheckSecondaryLocksResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            lock_version: 7,
                            key: vec![1],
                            use_async_commit: false,
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn std::any::Any>);
                }
                Err(Error::Unimplemented)
            },
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;

        let status = LockProbe.new_lock_status(vec![vec![1]], true, 0);
        let err = probe
            .check_all_secondaries(Lock::new(lock), &status)
            .await
            .expect_err("check_all_secondaries should surface non-async-commit fallback");
        assert!(probe.is_non_async_commit_lock(&err));
        assert_eq!(check_secondary_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_resolve_async_commit_lock_resolves_only_returned_locks() {
        let check_secondary_calls = Arc::new(AtomicUsize::new(0));
        let resolve_lock_calls = Arc::new(AtomicUsize::new(0));

        let check_secondary_calls_captured = check_secondary_calls.clone();
        let resolve_lock_calls_captured = resolve_lock_calls.clone();

        let commit_ts = 1000;

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn std::any::Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>() {
                    check_secondary_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    let resp = if req.keys == vec![vec![1]] {
                        kvrpcpb::CheckSecondaryLocksResponse {
                            commit_ts,
                            locks: vec![kvrpcpb::LockInfo {
                                lock_version: 7,
                                key: vec![1],
                                use_async_commit: true,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }
                    } else if req.keys == vec![vec![11]] {
                        kvrpcpb::CheckSecondaryLocksResponse {
                            commit_ts,
                            locks: Vec::new(),
                            ..Default::default()
                        }
                    } else {
                        panic!("unexpected secondary keys: {:?}", req.keys);
                    };
                    return Ok(Box::new(resp) as Box<dyn std::any::Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, commit_ts);
                    assert_eq!(req.keys, vec![vec![1], vec![2]]);
                    let resp = kvrpcpb::ResolveLockResponse::default();
                    return Ok(Box::new(resp) as Box<dyn std::any::Any>);
                }
                Err(Error::Unimplemented)
            },
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let status = LockProbe.new_lock_status(vec![vec![1], vec![11]], true, 0);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.use_async_commit = true;

        probe
            .resolve_async_commit_lock(Lock::new(lock), status)
            .await
            .expect("resolve_async_commit_lock should succeed");

        assert_eq!(check_secondary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(resolve_lock_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_get_txn_status_from_lock_decodes_committed() {
        let check_txn_status_calls = Arc::new(AtomicUsize::new(0));
        let check_txn_status_calls_captured = check_txn_status_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn std::any::Any| {
                if let Some(_req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_calls_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 10,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn std::any::Any>);
                }
                Err(Error::Unimplemented)
            },
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let status = probe
            .get_txn_status_from_lock(Lock::new(lock), 0, false)
            .await
            .expect("get_txn_status_from_lock should succeed");
        assert!(
            matches!(
                &status.kind,
                TransactionStatusKind::Committed(ts) if ts.version() == 10
            ),
            "expected committed status with commit_ts=10, got: {status:?}"
        );
        assert_eq!(check_txn_status_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_resolver_probe_set_resolving_records_locks() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn std::any::Any| Err(Error::Unimplemented),
        )));
        let resolver =
            BoundLockResolver::new(pd_client, Keyspace::Disable, ResolveLocksContext::default());
        let probe = LockResolverProbe::new(resolver);

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = b"k".to_vec();
        lock.primary_lock = b"p".to_vec();
        lock.lock_version = 42;

        probe.set_resolving(7, vec![Lock::new(lock)]).await;

        let resolver = probe.into_inner();
        let resolving = resolver.resolving().await;
        assert_eq!(resolving.len(), 1);
        assert_eq!(resolving[0].txn_id, 7);
        assert_eq!(resolving[0].lock_txn_id, 42);
        assert_eq!(resolving[0].key, b"k".to_vec());
        assert_eq!(resolving[0].primary, b"p".to_vec());
    }
}
