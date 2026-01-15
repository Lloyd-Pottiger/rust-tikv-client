// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
//
// This module implements TiDB/TiKV-style *local transaction latches*.
//
// Local latches are purely client-side concurrency control:
// - They serialize transaction commits with overlapping keys inside a process.
// - They can mark a transaction "stale" if it waited behind a transaction that committed
//   with a newer commit_ts than this transaction's start_ts.
//
// The algorithm is adapted from client-go (`client-go/internal/latch/*`), keeping the same core
// semantics (sorted keys, hashed slots, wakeup scheduling, and stale detection via max_commit_ts).

use std::cmp::max;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::Notify;

// Recycling constants aligned with client-go.
const EXPIRE_DURATION: Duration = Duration::from_secs(2 * 60);
const CHECK_INTERVAL: Duration = Duration::from_secs(60);
const CHECK_COUNTER: usize = 50_000;
const LATCH_LIST_COUNT: usize = 5;

/// The number of logical bits in a PD timestamp (used in its `u64` encoding).
///
/// Must match `TimestampExt`'s encoding.
const PD_TS_LOGICAL_BITS: u64 = 18;

fn extract_physical_ms(ts: u64) -> u64 {
    ts >> PD_TS_LOGICAL_BITS
}

fn tso_sub(ts1: u64, ts2: u64) -> Duration {
    let t1 = extract_physical_ms(ts1);
    let t2 = extract_physical_ms(ts2);
    Duration::from_millis(t1.saturating_sub(t2))
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum AcquireResult {
    Success,
    Locked,
    Stale,
}

/// A lock handle returned from [`LatchesScheduler::lock`].
///
/// The caller must release it (by dropping the guard) to avoid deadlocking other transactions.
pub(crate) struct LatchGuard {
    scheduler: Arc<LatchesScheduler>,
    lock: Arc<LatchLock>,
}

impl LatchGuard {
    pub(crate) fn is_stale(&self) -> bool {
        self.lock.is_stale()
    }

    pub(crate) fn set_commit_ts(&self, commit_ts: u64) {
        self.lock.set_commit_ts(commit_ts);
    }
}

impl Drop for LatchGuard {
    fn drop(&mut self) {
        self.scheduler.unlock(self.lock.clone());
    }
}

/// Local latches scheduler (client-side only).
///
/// Internally it runs a background task which processes unlock events, releases latches, and
/// wakes waiting transactions.
pub(crate) struct LatchesScheduler {
    latches: Latches,
    unlock_tx: mpsc::UnboundedSender<Arc<LatchLock>>,
}

impl LatchesScheduler {
    pub(crate) fn new(size: usize) -> Arc<Self> {
        assert!(size > 0, "latches size must be > 0");
        let latches = Latches::new(size);
        let (unlock_tx, mut unlock_rx) = mpsc::unbounded_channel::<Arc<LatchLock>>();

        let scheduler = Arc::new(Self { latches, unlock_tx });

        let scheduler_cloned = scheduler.clone();
        tokio::spawn(async move {
            let mut counter = 0usize;
            let mut last_recycle_ts = 0u64;
            while let Some(lock) = unlock_rx.recv().await {
                let wakeups = scheduler_cloned.latches.release(&lock);
                if !wakeups.is_empty() {
                    scheduler_cloned.wakeup(wakeups);
                }

                let commit_ts = lock.commit_ts();
                if commit_ts > lock.start_ts()
                    && (tso_sub(commit_ts, last_recycle_ts) > CHECK_INTERVAL
                        || counter > CHECK_COUNTER)
                {
                    let latches = scheduler_cloned.latches.clone();
                    tokio::spawn(async move {
                        latches.recycle(commit_ts);
                    });
                    last_recycle_ts = commit_ts;
                    counter = 0;
                }
                counter += 1;
            }
        });

        scheduler
    }

    pub(crate) async fn lock(self: &Arc<Self>, start_ts: u64, keys: Vec<Vec<u8>>) -> LatchGuard {
        let lock = self.latches.gen_lock(start_ts, keys);
        if self.latches.acquire(&lock) == AcquireResult::Locked {
            lock.wait().await;
        }
        debug_assert!(!lock.is_locked());
        LatchGuard {
            scheduler: self.clone(),
            lock,
        }
    }

    fn unlock(&self, lock: Arc<LatchLock>) {
        // Best-effort: if the receiver is closed (e.g. scheduler dropped), ignore.
        let _ = self.unlock_tx.send(lock);
    }

    fn wakeup(&self, wakeups: Vec<Arc<LatchLock>>) {
        for lock in wakeups {
            if self.latches.acquire(&lock) != AcquireResult::Locked {
                lock.wake();
            }
        }
    }
}

#[derive(Clone)]
struct Latches {
    slots: Arc<Vec<Slot>>,
}

impl Latches {
    fn new(size: usize) -> Self {
        let size = size.next_power_of_two();
        let slots = (0..size).map(|_| Slot::default()).collect();
        Self {
            slots: Arc::new(slots),
        }
    }

    fn gen_lock(&self, start_ts: u64, mut keys: Vec<Vec<u8>>) -> Arc<LatchLock> {
        keys.sort();
        keys.dedup();
        let required_slots = keys.iter().map(|k| self.slot_id(k)).collect();
        Arc::new(LatchLock::new(start_ts, keys, required_slots))
    }

    fn slot_id(&self, key: &[u8]) -> usize {
        // Keep the same distribution properties as client-go by using Murmur3 (x86_32).
        let hash = murmur3_x86_32(key, 0);
        (hash as usize) & (self.slots.len() - 1)
    }

    fn acquire(&self, lock: &Arc<LatchLock>) -> AcquireResult {
        if lock.is_stale() {
            return AcquireResult::Stale;
        }
        while lock.acquired_count() < lock.required_len() {
            let status = self.acquire_slot(lock);
            if status != AcquireResult::Success {
                return status;
            }
        }
        AcquireResult::Success
    }

    fn acquire_slot(&self, lock: &Arc<LatchLock>) -> AcquireResult {
        let idx = lock.acquired_count();
        let key = lock.key_at(idx);
        let slot_id = lock.slot_at(idx);
        let slot = &self.slots[slot_id];

        let mut inner = slot.inner.lock().unwrap();

        // Best-effort recycling to cap per-slot memory usage.
        if inner.nodes.len() >= LATCH_LIST_COUNT {
            inner.recycle(lock.start_ts());
        }

        let node = inner.nodes.entry(key.to_vec()).or_insert_with(|| Node {
            max_commit_ts: 0,
            value: None,
        });

        if node.max_commit_ts > lock.start_ts() {
            lock.mark_stale();
            return AcquireResult::Stale;
        }

        if node.value.is_none() {
            node.value = Some(lock.clone());
            lock.inc_acquired();
            return AcquireResult::Success;
        }

        inner.waiting.push(lock.clone());
        AcquireResult::Locked
    }

    fn release(&self, lock: &Arc<LatchLock>) -> Vec<Arc<LatchLock>> {
        let mut wakeups = Vec::new();
        while lock.acquired_count() > 0 {
            if let Some(next) = self.release_slot(lock) {
                wakeups.push(next);
            }
        }
        wakeups
    }

    fn release_slot(&self, lock: &Arc<LatchLock>) -> Option<Arc<LatchLock>> {
        let idx = lock
            .dec_acquired()
            .expect("release_slot called with acquired_count=0");
        let key = lock.key_at(idx);
        let slot_id = lock.slot_at(idx);
        let slot = &self.slots[slot_id];

        let mut inner = slot.inner.lock().unwrap();
        let node_max_commit_ts = {
            let node = inner
                .nodes
                .get_mut(key)
                .expect("latch node must exist when releasing");
            match &node.value {
                Some(owner) if Arc::ptr_eq(owner, lock) => {}
                _ => panic!("release_slot wrong lock owner"),
            }
            node.max_commit_ts = max(node.max_commit_ts, lock.commit_ts());
            node.value = None;
            node.max_commit_ts
        };

        if inner.waiting.is_empty() {
            return None;
        }

        // Wake up the first waiting lock blocked on this key.
        let mut idx_in_waiting = None;
        for (i, w) in inner.waiting.iter().enumerate() {
            let w_idx = w.acquired_count();
            if w_idx < w.required_len() && w.key_at(w_idx) == key {
                idx_in_waiting = Some(i);
                break;
            }
        }
        let Some(i) = idx_in_waiting else {
            return None;
        };
        let next = inner.waiting.remove(i);

        // Stale detection: if the max_commit_ts for this key is newer than the waiting txn's
        // start_ts, mark it stale and grant it the latch so it can release promptly.
        if node_max_commit_ts > next.start_ts() {
            let node = inner
                .nodes
                .get_mut(key)
                .expect("latch node must exist when granting to stale lock");
            node.value = Some(next.clone());
            next.inc_acquired();
            next.mark_stale();
        }
        Some(next)
    }

    fn recycle(&self, current_ts: u64) {
        for slot in self.slots.iter() {
            let mut inner = slot.inner.lock().unwrap();
            inner.recycle(current_ts);
        }
    }
}

#[derive(Default)]
struct Slot {
    inner: Mutex<SlotInner>,
}

#[derive(Default)]
struct SlotInner {
    nodes: HashMap<Vec<u8>, Node>,
    waiting: Vec<Arc<LatchLock>>,
}

impl SlotInner {
    fn recycle(&mut self, current_ts: u64) {
        self.nodes.retain(|_key, node| {
            if node.value.is_some() {
                return true;
            }
            tso_sub(current_ts, node.max_commit_ts) < EXPIRE_DURATION
        });
    }
}

struct Node {
    max_commit_ts: u64,
    value: Option<Arc<LatchLock>>,
}

struct LatchLock {
    start_ts: u64,
    keys: Vec<Vec<u8>>,
    required_slots: Vec<usize>,

    acquired_count: AtomicUsize,
    commit_ts: AtomicU64,
    stale: AtomicBool,
    notify: Notify,
}

impl LatchLock {
    fn new(start_ts: u64, keys: Vec<Vec<u8>>, required_slots: Vec<usize>) -> Self {
        Self {
            start_ts,
            keys,
            required_slots,
            acquired_count: AtomicUsize::new(0),
            commit_ts: AtomicU64::new(0),
            stale: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn start_ts(&self) -> u64 {
        self.start_ts
    }

    fn required_len(&self) -> usize {
        self.required_slots.len()
    }

    fn key_at(&self, idx: usize) -> &[u8] {
        &self.keys[idx]
    }

    fn slot_at(&self, idx: usize) -> usize {
        self.required_slots[idx]
    }

    fn acquired_count(&self) -> usize {
        self.acquired_count.load(Ordering::Acquire)
    }

    fn inc_acquired(&self) {
        self.acquired_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement acquired_count and return the *new* index to release (old_count - 1).
    fn dec_acquired(&self) -> Option<usize> {
        let prev = self
            .acquired_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_sub(1));
        match prev {
            Ok(old) => Some(old - 1),
            Err(_) => None,
        }
    }

    fn is_stale(&self) -> bool {
        self.stale.load(Ordering::Acquire)
    }

    fn mark_stale(&self) {
        self.stale.store(true, Ordering::Release);
    }

    fn is_locked(&self) -> bool {
        !self.is_stale() && self.acquired_count() != self.required_len()
    }

    fn set_commit_ts(&self, commit_ts: u64) {
        self.commit_ts.store(commit_ts, Ordering::Release);
    }

    fn commit_ts(&self) -> u64 {
        self.commit_ts.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        self.notify.notified().await;
    }

    fn wake(&self) {
        self.notify.notify_one();
    }
}

// Minimal Murmur3 x86_32 implementation for stable slot hashing.
fn murmur3_x86_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let len = data.len() as u32;
    let nblocks = data.len() / 4;

    let mut h1 = seed;

    // body
    for i in 0..nblocks {
        let block = &data[i * 4..i * 4 + 4];
        let mut k1 = u32::from_le_bytes([block[0], block[1], block[2], block[3]]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    // tail
    let tail = &data[nblocks * 4..];
    let mut k1 = 0u32;
    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
        }
        1 => {
            k1 ^= tail[0] as u32;
        }
        _ => {}
    }
    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;
    fmix32(h1)
}

fn fmix32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn latch_wakeup_and_stale() {
        let sched = LatchesScheduler::new(256);

        let g1 = sched
            .lock(10, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .await;
        assert!(!g1.is_stale());

        let sched2 = sched.clone();
        let t2 = tokio::spawn(async move {
            let g2 = sched2
                .lock(
                    15,
                    vec![b"d".to_vec(), b"e".to_vec(), b"a".to_vec(), b"c".to_vec()],
                )
                .await;
            g2
        });

        // Commit txn1 and release.
        g1.set_commit_ts(20);
        drop(g1);

        let g2 = t2.await.unwrap();
        assert!(g2.is_stale());
        drop(g2);
    }

    #[tokio::test]
    async fn latch_non_overlapping_no_wait() {
        let sched = LatchesScheduler::new(64);
        let g1 = sched.lock(10, vec![b"a".to_vec()]).await;
        let g2 = sched.lock(11, vec![b"b".to_vec()]).await;
        assert!(!g1.is_stale());
        assert!(!g2.is_stale());
        drop(g1);
        drop(g2);
    }
}
