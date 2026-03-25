// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, Notify};

const LATCH_LIST_COUNT: usize = 5;
const EXPIRE_DURATION_MS: u64 = 30 * 60 * 1000;
const PHYSICAL_SHIFT_BITS: u32 = 18;

#[derive(Debug, Default)]
struct LatchNode {
    max_commit_ts: u64,
    holder: Option<Arc<TxnLatchLock>>,
}

#[derive(Debug, Default)]
struct LatchSlot {
    nodes: HashMap<Vec<u8>, LatchNode>,
    waiting: Vec<Arc<TxnLatchLock>>,
}

impl LatchSlot {
    fn recycle(&mut self, current_ts: u64) {
        let current_physical = current_ts >> PHYSICAL_SHIFT_BITS;
        self.nodes.retain(|_key, node| {
            if node.holder.is_some() {
                return true;
            }

            let max_commit_physical = node.max_commit_ts >> PHYSICAL_SHIFT_BITS;
            current_physical.saturating_sub(max_commit_physical) < EXPIRE_DURATION_MS
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcquireResult {
    Success,
    Locked,
    Stale,
}

#[derive(Debug)]
pub(crate) struct TxnLocalLatches {
    slots: Vec<Mutex<LatchSlot>>,
}

impl TxnLocalLatches {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1).next_power_of_two();
        let slots = (0..capacity)
            .map(|_| Mutex::new(LatchSlot::default()))
            .collect();
        Self { slots }
    }

    pub(crate) async fn lock(
        self: &Arc<Self>,
        start_ts: u64,
        mut keys: Vec<Vec<u8>>,
    ) -> TxnLatchGuard {
        let started_at = Instant::now();
        keys.sort();
        keys.dedup();

        let required_slots = keys.iter().map(|key| self.slot_id(key)).collect();
        let lock = Arc::new(TxnLatchLock::new(start_ts, keys, required_slots));

        let acquire_result = self.acquire(&lock).await;
        if acquire_result == AcquireResult::Locked {
            lock.notify.notified().await;
        }
        debug_assert!(!lock.is_locked());
        let elapsed = started_at.elapsed();
        if !elapsed.is_zero() {
            crate::stats::observe_local_latch_wait_seconds(elapsed);
        }

        TxnLatchGuard {
            latches: self.clone(),
            lock,
        }
    }

    #[cfg(test)]
    async fn recycle_all(&self, current_ts: u64) {
        for slot in &self.slots {
            slot.lock().await.recycle(current_ts);
        }
    }

    fn slot_id(&self, key: &[u8]) -> usize {
        let hash = murmur3_32(key);
        (hash as usize) & (self.slots.len() - 1)
    }

    async fn acquire(&self, lock: &Arc<TxnLatchLock>) -> AcquireResult {
        if lock.is_stale.load(Ordering::Acquire) {
            return AcquireResult::Stale;
        }

        loop {
            let acquired_count = lock.acquired_count.load(Ordering::Acquire);
            if acquired_count >= lock.keys.len() {
                return AcquireResult::Success;
            }

            let key = &lock.keys[acquired_count];
            let slot_id = lock.required_slots[acquired_count];
            match self.acquire_slot(lock, slot_id, key).await {
                AcquireResult::Success => continue,
                other => return other,
            }
        }
    }

    async fn acquire_slot(
        &self,
        lock: &Arc<TxnLatchLock>,
        slot_id: usize,
        key: &[u8],
    ) -> AcquireResult {
        let mut slot = self.slots[slot_id].lock().await;
        if slot.nodes.len() >= LATCH_LIST_COUNT {
            slot.recycle(lock.start_ts);
        }

        let node = match slot.nodes.get_mut(key) {
            Some(node) => node,
            None => {
                slot.nodes.insert(
                    key.to_vec(),
                    LatchNode {
                        max_commit_ts: 0,
                        holder: Some(lock.clone()),
                    },
                );
                lock.acquired_count.fetch_add(1, Ordering::Release);
                return AcquireResult::Success;
            }
        };

        if node.max_commit_ts > lock.start_ts {
            lock.is_stale.store(true, Ordering::Release);
            return AcquireResult::Stale;
        }

        if node.holder.is_none() {
            node.holder = Some(lock.clone());
            lock.acquired_count.fetch_add(1, Ordering::Release);
            return AcquireResult::Success;
        }

        slot.waiting.push(lock.clone());
        AcquireResult::Locked
    }

    async fn release_slot(
        &self,
        lock: &Arc<TxnLatchLock>,
        slot_id: usize,
        key: &[u8],
    ) -> Option<Arc<TxnLatchLock>> {
        let mut slot = self.slots[slot_id].lock().await;
        let node_max_commit_ts = {
            let node = slot.nodes.get_mut(key)?;
            debug_assert!(node
                .holder
                .as_ref()
                .is_some_and(|holder| Arc::ptr_eq(holder, lock)));

            node.max_commit_ts = node
                .max_commit_ts
                .max(lock.commit_ts.load(Ordering::Acquire));
            node.holder = None;
            node.max_commit_ts
        };

        let waiting_idx = slot.waiting.iter().position(|waiting| {
            let acquired_count = waiting.acquired_count.load(Ordering::Acquire);
            acquired_count < waiting.keys.len() && waiting.keys[acquired_count].as_slice() == key
        })?;

        let next_lock = slot.waiting.remove(waiting_idx);
        if node_max_commit_ts > next_lock.start_ts {
            next_lock.is_stale.store(true, Ordering::Release);
            if let Some(node) = slot.nodes.get_mut(key) {
                node.holder = Some(next_lock.clone());
            }
            next_lock.acquired_count.fetch_add(1, Ordering::Release);
        }

        Some(next_lock)
    }

    async fn unlock_lock(&self, lock: Arc<TxnLatchLock>) {
        let mut wakeup_list = Vec::new();
        let acquired_count = lock.acquired_count.load(Ordering::Acquire);
        for idx in (0..acquired_count).rev() {
            let key = &lock.keys[idx];
            let slot_id = lock.required_slots[idx];
            if let Some(next_lock) = self.release_slot(&lock, slot_id, key).await {
                wakeup_list.push(next_lock);
            }
        }
        lock.acquired_count.store(0, Ordering::Release);

        for lock in wakeup_list {
            if self.acquire(&lock).await != AcquireResult::Locked {
                lock.notify.notify_one();
            }
        }
    }
}

#[derive(Debug)]
struct TxnLatchLock {
    keys: Vec<Vec<u8>>,
    required_slots: Vec<usize>,
    start_ts: u64,
    commit_ts: AtomicU64,
    acquired_count: AtomicUsize,
    is_stale: AtomicBool,
    notify: Notify,
}

impl TxnLatchLock {
    fn new(start_ts: u64, keys: Vec<Vec<u8>>, required_slots: Vec<usize>) -> Self {
        Self {
            keys,
            required_slots,
            start_ts,
            commit_ts: AtomicU64::new(0),
            acquired_count: AtomicUsize::new(0),
            is_stale: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn is_locked(&self) -> bool {
        !self.is_stale.load(Ordering::Acquire)
            && self.acquired_count.load(Ordering::Acquire) != self.keys.len()
    }
}

#[derive(Debug)]
pub(crate) struct TxnLatchGuard {
    latches: Arc<TxnLocalLatches>,
    lock: Arc<TxnLatchLock>,
}

impl TxnLatchGuard {
    pub(crate) fn is_stale(&self) -> bool {
        self.lock.is_stale.load(Ordering::Acquire)
    }

    pub(crate) fn start_ts(&self) -> u64 {
        self.lock.start_ts
    }

    pub(crate) fn set_commit_ts(&self, commit_ts: u64) {
        self.lock.commit_ts.store(commit_ts, Ordering::Release);
    }

    pub(crate) async fn unlock(self) {
        self.latches.unlock_lock(self.lock).await;
    }
}

fn murmur3_32(bytes: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash: u32 = 0;

    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);

        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = chunks.remainder();
    let mut k1: u32 = 0;
    match tail.len() {
        3 => {
            k1 ^= u32::from(tail[2]) << 16;
            k1 ^= u32::from(tail[1]) << 8;
            k1 ^= u32::from(tail[0]);
        }
        2 => {
            k1 ^= u32::from(tail[1]) << 8;
            k1 ^= u32::from(tail[0]);
        }
        1 => {
            k1 ^= u32::from(tail[0]);
        }
        _ => {}
    }

    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= bytes.len() as u32;

    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;

    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::gc_safe_point::GcSafePointCache;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::request::Keyspace;
    use crate::timestamp::TimestampExt;
    use crate::transaction::{ResolveLocksContext, Transaction, TransactionOptions};
    use crate::Error;
    use crate::Timestamp;
    use serial_test::serial;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_wakeup_marks_stale_and_allows_restart() {
        let latches = Arc::new(TxnLocalLatches::new(256));

        let keys_a = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let keys_b = vec![b"d".to_vec(), b"e".to_vec(), b"a".to_vec(), b"c".to_vec()];

        let guard_a = latches.lock(1, keys_a).await;
        assert!(!guard_a.is_stale());

        let latches_b = latches.clone();
        let keys_b_for_task = keys_b.clone();
        let handle_b = tokio::spawn(async move { latches_b.lock(2, keys_b_for_task).await });

        tokio::task::yield_now().await;
        assert!(!handle_b.is_finished());

        guard_a.set_commit_ts(3);
        guard_a.unlock().await;

        let guard_b = timeout(Duration::from_secs(1), handle_b)
            .await
            .expect("lock b should be woken")
            .expect("lock b task should succeed");
        assert!(guard_b.is_stale());

        guard_b.unlock().await;

        let guard_b_retry = latches.lock(4, keys_b).await;
        assert!(!guard_b_retry.is_stale());
        guard_b_retry.unlock().await;
    }

    #[tokio::test]
    async fn test_first_acquire_can_fail_with_stale() {
        let latches = Arc::new(TxnLocalLatches::new(256));

        let keys = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let guard_a = latches.lock(1, keys.clone()).await;
        guard_a.set_commit_ts(3);
        guard_a.unlock().await;

        let guard_b = latches.lock(2, keys).await;
        assert!(guard_b.is_stale());
        guard_b.unlock().await;
    }

    #[tokio::test]
    async fn test_recycle_removes_old_nodes() {
        let latches = Arc::new(TxnLocalLatches::new(8));

        let base_physical_ms = 42_000u64;
        let start_ts = (base_physical_ms << PHYSICAL_SHIFT_BITS) + 1;
        let commit_ts = start_ts + 1;
        let current_ts = ((base_physical_ms + EXPIRE_DURATION_MS) << PHYSICAL_SHIFT_BITS) + 3;

        let guard = latches
            .lock(start_ts, vec![b"a".to_vec(), b"b".to_vec()])
            .await;
        guard.set_commit_ts(commit_ts);
        guard.unlock().await;

        let mut any_non_empty = false;
        for slot in &latches.slots {
            if !slot.lock().await.nodes.is_empty() {
                any_non_empty = true;
                break;
            }
        }
        assert!(any_non_empty, "expected at least one slot to contain nodes");

        latches.recycle_all(current_ts).await;

        for slot in &latches.slots {
            assert!(slot.lock().await.nodes.is_empty());
        }
    }

    #[tokio::test]
    async fn test_transaction_commit_returns_write_conflict_in_latch_without_dispatch() {
        let latches = Arc::new(TxnLocalLatches::new(256));

        let guard_a = latches.lock(1, vec![b"k".to_vec()]).await;
        assert!(!guard_a.is_stale());

        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_cloned = dispatch_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_cloned.fetch_add(1, Ordering::SeqCst);
                panic!("commit should not dispatch when latch conflict is detected");
            },
        )));

        let latches_for_txn = latches.clone();
        let handle = tokio::spawn(async move {
            let gc_safe_point = GcSafePointCache::new(pd_client.clone(), Keyspace::Disable);
            let mut txn = Transaction::new_with_resolve_locks_ctx(
                Timestamp::from_version(2),
                pd_client,
                TransactionOptions::new_optimistic().drop_check(crate::CheckLevel::None),
                Keyspace::Disable,
                ResolveLocksContext::default(),
                gc_safe_point,
                Some(latches_for_txn),
            );
            txn.put(b"k".to_vec(), b"v".to_vec()).await.unwrap();
            txn.commit().await.unwrap_err()
        });

        tokio::task::yield_now().await;
        assert!(!handle.is_finished());

        guard_a.set_commit_ts(3);
        guard_a.unlock().await;

        let err = timeout(Duration::from_secs(1), handle)
            .await
            .expect("commit should finish once latch is released")
            .expect("commit task should succeed");

        match err {
            Error::WriteConflictInLatch { start_ts } => assert_eq!(start_ts, 2),
            other => panic!("expected WriteConflictInLatch, got {other:?}"),
        }

        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);

        let guard_c = timeout(Duration::from_secs(1), latches.lock(4, vec![b"k".to_vec()]))
            .await
            .expect("subsequent latch acquisition should not block");
        assert!(!guard_c.is_stale());
        guard_c.unlock().await;
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_lock_records_local_latch_wait_metrics() {
        let metric = "tikv_client_local_latch_wait_seconds";
        let sample_count = || {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| family.get_name() == metric)
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_histogram().get_sample_count())
                .unwrap_or(0)
        };

        let before = sample_count();

        let latches = Arc::new(TxnLocalLatches::new(256));
        let guard_a = latches.lock(1, vec![b"a".to_vec()]).await;

        let latches_b = latches.clone();
        let handle_b = tokio::spawn(async move { latches_b.lock(2, vec![b"a".to_vec()]).await });

        tokio::task::yield_now().await;
        assert!(!handle_b.is_finished());

        guard_a.unlock().await;

        let guard_b = timeout(Duration::from_secs(1), handle_b)
            .await
            .expect("lock b should be woken")
            .expect("lock b task should succeed");
        guard_b.unlock().await;

        let after = sample_count();
        assert!(
            after > before,
            "expected local_latch_wait_seconds to observe samples"
        );
    }
}
