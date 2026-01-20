// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::region::StoreId;

#[derive(Clone, Copy, Debug, Default)]
struct StoreHealthEntry {
    slow_until_epoch_ms: i64,
    tikv_side_slow_score: i32,
    tikv_side_feedback_seq_no: u64,
}

/// Best-effort per-store health tracking used for replica read routing.
#[derive(Clone, Default, Debug)]
pub struct StoreHealthMap {
    inner: Arc<RwLock<HashMap<StoreId, StoreHealthEntry>>>,
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
            .is_some_and(|entry| entry.slow_until_epoch_ms > now_epoch_ms)
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
        let entry = guard.entry(store_id).or_default();
        entry.slow_until_epoch_ms = entry.slow_until_epoch_ms.max(slow_until);
    }

    pub(crate) fn record_tikv_health_feedback(
        &self,
        store_id: StoreId,
        feedback_seq_no: u64,
        slow_score: i32,
    ) {
        let mut guard = match self.inner.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        let entry = guard.entry(store_id).or_default();
        if feedback_seq_no >= entry.tikv_side_feedback_seq_no {
            entry.tikv_side_feedback_seq_no = feedback_seq_no;
            entry.tikv_side_slow_score = slow_score;
        }
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) fn tikv_side_slow_score(&self, store_id: StoreId) -> Option<i32> {
        let guard = self.inner.read().ok()?;
        guard.get(&store_id).map(|entry| entry.tikv_side_slow_score)
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

    #[test]
    fn record_health_feedback_is_monotonic_by_seq_no() {
        let health = StoreHealthMap::default();
        let store_id = 7;

        health.record_tikv_health_feedback(store_id, 10, 1);
        assert_eq!(health.tikv_side_slow_score(store_id), Some(1));

        health.record_tikv_health_feedback(store_id, 9, 50);
        assert_eq!(health.tikv_side_slow_score(store_id), Some(1));

        health.record_tikv_health_feedback(store_id, 11, 2);
        assert_eq!(health.tikv_side_slow_score(store_id), Some(2));
    }
}
