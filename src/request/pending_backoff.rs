// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::proto::errorpb;
use crate::region::StoreId;

/// Pending backoffs for replica-read fast retry.
///
/// This ports the essential behavior of client-go's `baseReplicaSelector.pendingBackoffs`:
/// - when we "fast retry" (switch peer without sleeping) we record a pending backoff for the
///   current store;
/// - when we later retry on the same store we apply/consume the pending backoff;
/// - when there is no candidate store, we apply the "largest" pending backoff (by base delay).
#[derive(Clone, Default, Debug)]
pub(crate) struct PendingBackoffs {
    inner: Arc<Mutex<HashMap<StoreId, PendingBackoff>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingBackoffKind {
    #[cfg(test)]
    RegionScheduling,
    #[cfg(test)]
    TikvRpc,
    #[cfg(test)]
    TikvDiskFull,
    TikvServerBusy,
}

impl PendingBackoffKind {
    #[cfg(test)]
    fn base_delay_rank(self) -> u32 {
        // We only need a stable ordering for selecting "largest base" (client-go behavior).
        match self {
            PendingBackoffKind::RegionScheduling => 1,
            PendingBackoffKind::TikvRpc => 2,
            PendingBackoffKind::TikvDiskFull => 3,
            PendingBackoffKind::TikvServerBusy => 4,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingBackoff {
    pub(crate) kind: PendingBackoffKind,
    pub(crate) error: errorpb::Error,
}

impl PendingBackoffs {
    pub(crate) fn add(
        &self,
        store_id: Option<StoreId>,
        kind: PendingBackoffKind,
        error: errorpb::Error,
    ) {
        let store_id = store_id.unwrap_or(0);
        let mut guard = self.inner.lock().expect("pending backoffs lock poisoned");
        guard.insert(store_id, PendingBackoff { kind, error });
    }

    pub(crate) fn take_for_retry(&self, store_id: Option<StoreId>) -> Option<PendingBackoff> {
        let store_id = store_id.unwrap_or(0);
        let mut guard = self.inner.lock().expect("pending backoffs lock poisoned");
        guard.remove(&store_id)
    }

    #[cfg(test)]
    pub(crate) fn peek_for_no_candidate(&self) -> Option<PendingBackoff> {
        let guard = self.inner.lock().expect("pending backoffs lock poisoned");
        guard
            .values()
            .max_by_key(|b| b.kind.base_delay_rank())
            .cloned()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("pending backoffs lock poisoned")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_backoffs_match_client_go_pending_backoff_test() {
        let pending = PendingBackoffs::default();

        assert!(pending.take_for_retry(None).is_none());
        assert!(pending.take_for_retry(Some(1)).is_none());
        assert!(pending.peek_for_no_candidate().is_none());

        pending.add(
            None,
            PendingBackoffKind::RegionScheduling,
            errorpb::Error {
                message: "err-0".to_owned(),
                ..Default::default()
            },
        );
        assert_eq!(pending.len(), 1);

        pending.add(
            Some(1),
            PendingBackoffKind::TikvRpc,
            errorpb::Error {
                message: "err-1".to_owned(),
                ..Default::default()
            },
        );
        assert_eq!(pending.len(), 2);

        pending.add(
            Some(2),
            PendingBackoffKind::TikvDiskFull,
            errorpb::Error {
                message: "err-2".to_owned(),
                ..Default::default()
            },
        );
        assert_eq!(pending.len(), 3);

        // Same store id overwrites the previous pending backoff.
        pending.add(
            Some(1),
            PendingBackoffKind::TikvServerBusy,
            errorpb::Error {
                message: "err-3".to_owned(),
                ..Default::default()
            },
        );
        assert_eq!(pending.len(), 3);

        let err = pending
            .take_for_retry(None)
            .expect("expected pending backoff")
            .error;
        assert_eq!(err.message, "err-0");
        assert_eq!(pending.len(), 2);

        assert!(pending.take_for_retry(Some(10)).is_none());
        assert_eq!(pending.len(), 2);

        let err = pending
            .peek_for_no_candidate()
            .expect("expected pending backoff for no-candidate")
            .error;
        assert_eq!(err.message, "err-3");
    }
}
