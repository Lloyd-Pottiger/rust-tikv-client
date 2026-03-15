// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Test probe helpers.
//!
//! These utilities are intended to make it easier to port client-go tests that
//! rely on exported `*Probe` types (for example, `txnlock/test_probe.go`).

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
}
