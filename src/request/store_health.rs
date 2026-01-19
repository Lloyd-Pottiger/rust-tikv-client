// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::region::StoreId;

/// Best-effort per-store health tracking used for replica read routing.
///
/// This intentionally starts small (slow-store avoidance) and is designed to be extended as we
/// port more client-go replica selector semantics (score, fast retry, etc).
#[derive(Clone, Default, Debug)]
pub(crate) struct StoreHealthMap {
    // store_id -> slow_until_epoch_ms
    inner: Arc<RwLock<HashMap<StoreId, i64>>>,
}

impl StoreHealthMap {
    pub(crate) fn now_epoch_ms() -> i64 {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(0)
    }

    pub(crate) fn is_slow(&self, store_id: StoreId, now_epoch_ms: i64) -> bool {
        let guard = match self.inner.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard
            .get(&store_id)
            .copied()
            .is_some_and(|until| until > now_epoch_ms)
    }

    pub(crate) fn is_slow_now(&self, store_id: StoreId) -> bool {
        self.is_slow(store_id, Self::now_epoch_ms())
    }

    pub(crate) fn mark_slow_for(&self, store_id: StoreId, duration: Duration) {
        let now = Self::now_epoch_ms();
        let slow_for_ms = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        let slow_until = now.saturating_add(slow_for_ms);

        let mut guard = match self.inner.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        let entry = guard.entry(store_id).or_insert(0);
        *entry = (*entry).max(slow_until);
    }

    #[allow(dead_code)]
    pub(crate) fn clear_slow(&self, store_id: StoreId) {
        let mut guard = match self.inner.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.remove(&store_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_slow_and_clear() {
        let health = StoreHealthMap::default();
        let store_id = 42;

        assert!(!health.is_slow(store_id, 0));
        health.mark_slow_for(store_id, Duration::from_secs(1));
        assert!(health.is_slow(store_id, StoreHealthMap::now_epoch_ms()));

        health.clear_slow(store_id);
        assert!(!health.is_slow(store_id, StoreHealthMap::now_epoch_ms()));
    }
}
