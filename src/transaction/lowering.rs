// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides constructor functions for requests which take arguments as high-level
//! types (i.e., the types from the client crate) and converts these to the types used in the
//! generated protobuf code, then calls the low-level ctor functions in the requests module.

use std::iter::Iterator;

use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::stats;
use crate::timestamp::TimestampExt;
use crate::transaction::requests;
use crate::BoundRange;
use crate::Key;

/// Builds a transactional get request for a single crate-native key at `timestamp`.
pub fn new_get_request(key: Key, timestamp: Timestamp) -> kvrpcpb::GetRequest {
    requests::new_get_request(key.into(), timestamp.version())
}

/// Builds a transactional batch-get request for crate-native keys at `timestamp`.
pub fn new_batch_get_request(
    keys: impl Iterator<Item = Key>,
    timestamp: Timestamp,
) -> kvrpcpb::BatchGetRequest {
    requests::new_batch_get_request(keys.map(Into::into).collect(), timestamp.version())
}

/// Builds a buffer batch-get request for the in-memory pessimistic lock buffer at `timestamp`.
pub fn new_buffer_batch_get_request(
    keys: impl Iterator<Item = Key>,
    timestamp: Timestamp,
) -> kvrpcpb::BufferBatchGetRequest {
    requests::new_buffer_batch_get_request(keys.map(Into::into).collect(), timestamp.version())
}

/// Builds a transactional scan request from a crate-native key range.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_scan_request(
    range: BoundRange,
    timestamp: Timestamp,
    limit: u32,
    key_only: bool,
    reverse: bool,
) -> kvrpcpb::ScanRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_scan_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        timestamp.version(),
        limit,
        key_only,
        reverse,
    )
}

/// Builds a prewrite request from already-lowered mutations plus crate-native lock metadata.
pub fn new_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
) -> kvrpcpb::PrewriteRequest {
    requests::new_prewrite_request(
        mutations,
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
    )
}

/// Builds a pessimistic prewrite request from already-lowered mutations plus crate-native lock
/// metadata.
pub fn new_pessimistic_prewrite_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
    for_update_ts: Timestamp,
) -> kvrpcpb::PrewriteRequest {
    requests::new_pessimistic_prewrite_request(
        mutations,
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
        for_update_ts.version(),
    )
}

/// Builds a commit request for the provided keys and transaction timestamps.
pub fn new_commit_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
    commit_version: Timestamp,
) -> kvrpcpb::CommitRequest {
    requests::new_commit_request(
        keys.map(Into::into).collect(),
        start_version.version(),
        commit_version.version(),
    )
}

/// Builds a rollback request for the provided keys at `start_version`.
pub fn new_batch_rollback_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
) -> kvrpcpb::BatchRollbackRequest {
    requests::new_batch_rollback_request(keys.map(Into::into).collect(), start_version.version())
}

/// Builds a pessimistic rollback request for the provided keys and `for_update_ts`.
pub fn new_pessimistic_rollback_request(
    keys: impl Iterator<Item = Key>,
    start_version: Timestamp,
    for_update_ts: Timestamp,
) -> kvrpcpb::PessimisticRollbackRequest {
    requests::new_pessimistic_rollback_request(
        keys.map(Into::into).collect(),
        start_version.version(),
        for_update_ts.version(),
    )
}

/// Describes a key that can be converted into a pessimistic lock mutation.
///
/// The crate accepts bare [`Key`] values, which imply [`kvrpcpb::Assertion::None`], and
/// `(Key, Assertion)` pairs when the caller needs to set an explicit assertion.
pub trait PessimisticLock: Clone {
    /// Returns the key to lock.
    fn key(self) -> Key;

    /// Returns the assertion to encode on the generated lock mutation.
    fn assertion(&self) -> kvrpcpb::Assertion;
}

impl PessimisticLock for Key {
    fn key(self) -> Key {
        self
    }

    fn assertion(&self) -> kvrpcpb::Assertion {
        kvrpcpb::Assertion::None
    }
}

impl PessimisticLock for (Key, kvrpcpb::Assertion) {
    fn key(self) -> Key {
        self.0
    }

    fn assertion(&self) -> kvrpcpb::Assertion {
        self.1
    }
}

/// Builds a pessimistic lock request from crate-native keys or `(key, assertion)` pairs.
pub fn new_pessimistic_lock_request(
    locks: impl Iterator<Item = impl PessimisticLock>,
    primary_lock: Key,
    start_version: Timestamp,
    lock_ttl: u64,
    for_update_ts: Timestamp,
    need_value: bool,
    is_first_lock: bool,
) -> kvrpcpb::PessimisticLockRequest {
    requests::new_pessimistic_lock_request(
        locks
            .map(|pl| {
                let mut mutation = kvrpcpb::Mutation::default();
                mutation.op = kvrpcpb::Op::PessimisticLock.into();
                mutation.assertion = pl.assertion().into();
                mutation.key = pl.key().into();
                mutation
            })
            .collect(),
        primary_lock.into(),
        start_version.version(),
        lock_ttl,
        for_update_ts.version(),
        need_value,
        is_first_lock,
    )
}

/// Builds a scan-lock request for a crate-native key range and GC safepoint.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_scan_lock_request(
    range: BoundRange,
    safepoint: &Timestamp,
    limit: u32,
) -> kvrpcpb::ScanLockRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_scan_lock_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        safepoint.version(),
        limit,
    )
}

/// Builds a transaction heart-beat request that extends the TTL of `primary_lock`.
pub fn new_heart_beat_request(
    start_ts: Timestamp,
    primary_lock: Key,
    ttl: u64,
) -> kvrpcpb::TxnHeartBeatRequest {
    requests::new_heart_beat_request(start_ts.version(), primary_lock.into(), ttl)
}

/// Builds a flush request for async-commit/pipelined write state.
///
/// When `assertion_level` is not [`kvrpcpb::AssertionLevel::Off`], this also updates the
/// client-side assertion metrics for the supplied mutations.
pub fn new_flush_request(
    mutations: Vec<kvrpcpb::Mutation>,
    primary_key: Key,
    start_ts: Timestamp,
    min_commit_ts: u64,
    generation: u64,
    lock_ttl: u64,
    assertion_level: kvrpcpb::AssertionLevel,
) -> kvrpcpb::FlushRequest {
    if assertion_level != kvrpcpb::AssertionLevel::Off {
        stats::inc_prewrite_assertion_count_for_mutations(&mutations);
    }
    requests::new_flush_request(
        mutations,
        primary_key.into(),
        start_ts.version(),
        min_commit_ts,
        generation,
        lock_ttl,
        assertion_level,
    )
}

/// Builds a split-region request from crate-native split keys.
pub fn new_split_region_request(
    split_keys: impl Iterator<Item = Key>,
    is_raw_kv: bool,
) -> kvrpcpb::SplitRegionRequest {
    requests::new_split_region_request(split_keys.map(Into::into).collect(), is_raw_kv)
}

/// Builds an unsafe destroy-range request from a crate-native key range.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_unsafe_destroy_range_request(range: BoundRange) -> kvrpcpb::UnsafeDestroyRangeRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_unsafe_destroy_range_request(start_key.into(), end_key.unwrap_or_default().into())
}

/// Builds a delete-range request from a crate-native key range.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_delete_range_request(
    range: BoundRange,
    notify_only: bool,
) -> kvrpcpb::DeleteRangeRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_delete_range_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        notify_only,
    )
}

/// Builds a prepare-flashback request for the provided crate-native key range.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_prepare_flashback_to_version_request(
    range: BoundRange,
    start_ts: u64,
    version: u64,
) -> kvrpcpb::PrepareFlashbackToVersionRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_prepare_flashback_to_version_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        start_ts,
        version,
    )
}

/// Builds a flashback request for the provided crate-native key range.
///
/// An unbounded end in `range` is encoded as TiKV's empty end key sentinel.
pub fn new_flashback_to_version_request(
    range: BoundRange,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) -> kvrpcpb::FlashbackToVersionRequest {
    let (start_key, end_key) = range.into_keys();
    requests::new_flashback_to_version_request(
        start_key.into(),
        end_key.unwrap_or_default().into(),
        version,
        start_ts,
        commit_ts,
    )
}

/// Builds a compaction request that starts at `start_key` for the specified table identifiers.
pub fn new_compact_request(
    start_key: Key,
    physical_table_id: i64,
    logical_table_id: i64,
    keyspace_id: u32,
) -> kvrpcpb::CompactRequest {
    requests::new_compact_request(
        start_key.into(),
        physical_table_id,
        logical_table_id,
        keyspace_id,
    )
}

/// Builds a lock-observer registration request for transactions up to `max_ts`.
pub fn new_register_lock_observer_request(max_ts: u64) -> kvrpcpb::RegisterLockObserverRequest {
    requests::new_register_lock_observer_request(max_ts)
}

/// Builds a lock-observer status check request for transactions up to `max_ts`.
pub fn new_check_lock_observer_request(max_ts: u64) -> kvrpcpb::CheckLockObserverRequest {
    requests::new_check_lock_observer_request(max_ts)
}

/// Builds a lock-observer removal request for transactions up to `max_ts`.
pub fn new_remove_lock_observer_request(max_ts: u64) -> kvrpcpb::RemoveLockObserverRequest {
    requests::new_remove_lock_observer_request(max_ts)
}

/// Builds a physical scan-lock request beginning at `start_key`.
pub fn new_physical_scan_lock_request(
    max_ts: u64,
    start_key: Key,
    limit: u32,
) -> kvrpcpb::PhysicalScanLockRequest {
    requests::new_physical_scan_lock_request(max_ts, start_key.into(), limit)
}

/// Builds a request that fetches the current lock-wait table snapshot.
pub fn new_get_lock_wait_info_request() -> kvrpcpb::GetLockWaitInfoRequest {
    requests::new_get_lock_wait_info_request()
}

/// Builds a request that fetches historical lock-wait samples.
pub fn new_get_lock_wait_history_request() -> kvrpcpb::GetLockWaitHistoryRequest {
    requests::new_get_lock_wait_history_request()
}

/// Builds a TiFlash system-table request for the provided SQL query.
pub fn new_get_ti_flash_system_table_request(sql: String) -> kvrpcpb::TiFlashSystemTableRequest {
    requests::new_get_ti_flash_system_table_request(sql)
}
