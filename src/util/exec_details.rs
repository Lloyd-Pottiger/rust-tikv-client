// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::proto::kvrpcpb;
use crate::proto::resource_manager;
use crate::transaction::ResolveLockDetail;
use crate::util::format_bytes;

tokio::task_local! {
    static TASK_EXEC_DETAILS: Arc<ExecDetails>;
}

tokio::task_local! {
    static TASK_TRACE_EXEC_DETAILS_ENABLED: bool;
}

/// Marker keys for execution details stored in task-local context.
///
/// These mirror the exported `context.Context` keys in client-go `util/execdetails.go`. The Rust
/// client uses Tokio task-locals instead of `context.Context`, but these marker types are kept to
/// make it easier to port client-go code that passes keys around.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommitDetailCtxKey;

/// Marker key for lock-keys details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LockKeysDetailCtxKey;

/// Marker key for exec details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExecDetailsKey;

/// Marker key for RU details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RUDetailsCtxKey;

#[derive(Clone, Copy, Debug, Default)]
struct TaskTrafficKind {
    is_mpp: bool,
    is_cross_zone: bool,
}

tokio::task_local! {
    static TASK_TRAFFIC_KIND: TaskTrafficKind;
}

/// Server-side execution details returned by TiKV.
///
/// This mirrors the public shape of client-go `util.TiKVExecDetails`, but keeps the sub-details as
/// value types so empty/default details remain cheap to store and merge in Rust.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TiKVExecDetails {
    /// Time-related execution details.
    pub time_detail: TimeDetail,
    /// Scan-related execution details.
    pub scan_detail: ScanDetail,
    /// Write-path execution details.
    pub write_detail: WriteDetail,
}

impl TiKVExecDetails {
    /// Build execution details from a TiKV `ExecDetailsV2` protobuf message.
    #[must_use]
    pub fn from_proto(details: Option<&kvrpcpb::ExecDetailsV2>) -> Self {
        let mut exec_details = Self::default();
        exec_details.merge_from_proto(details);
        exec_details
    }

    /// Merge another execution detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.time_detail.merge(&other.time_detail);
        self.scan_detail.merge(&other.scan_detail);
        self.write_detail.merge(&other.write_detail);
    }

    /// Merge execution details from a TiKV `ExecDetailsV2` protobuf message.
    pub fn merge_from_proto(&mut self, details: Option<&kvrpcpb::ExecDetailsV2>) {
        let Some(details) = details else {
            return;
        };

        self.time_detail.merge_from_proto(
            details.time_detail_v2.as_ref(),
            details.time_detail.as_ref(),
        );
        self.scan_detail
            .merge_from_proto(details.scan_detail_v2.as_ref());
        self.write_detail
            .merge_from_proto(details.write_detail.as_ref());
    }

    /// Whether all sub-details are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.time_detail.is_empty() && self.scan_detail.is_empty() && self.write_detail.is_empty()
    }
}

impl From<&kvrpcpb::ExecDetailsV2> for TiKVExecDetails {
    fn from(details: &kvrpcpb::ExecDetailsV2) -> Self {
        Self::from_proto(Some(details))
    }
}

impl fmt::Display for TiKVExecDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::with_capacity(3);

        if !self.time_detail.is_empty() {
            parts.push(self.time_detail.to_string());
        }
        if !self.scan_detail.is_empty() {
            parts.push(self.scan_detail.to_string());
        }
        if !self.write_detail.is_empty() {
            parts.push(self.write_detail.to_string());
        }

        f.write_str(&parts.join(", "))
    }
}

/// Diagnose information for a single TiKV request.
///
/// This mirrors client-go `util.ReqDetailInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReqDetailInfo {
    /// End-to-end wall time spent on the request.
    pub req_total_time: Duration,
    /// Region ID targeted by the request.
    pub region: u64,
    /// Store address that served the request.
    pub store_addr: String,
    /// Server-side execution details returned by TiKV.
    pub exec_details: TiKVExecDetails,
}

/// Extra diagnostics for commit-ts lag introduced by `wait_until`.
///
/// This mirrors client-go `util.CommitTSLagDetails`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitTSLagDetails {
    /// Total wait time spent waiting for PD TSO to catch up.
    pub wait_time: Duration,
    /// Number of backoff rounds taken while waiting.
    pub backoff_count: u32,
    /// The first lagging TSO observed from PD.
    pub first_lag_ts: u64,
    /// The minimum TSO the commit was waiting for.
    pub wait_until_ts: u64,
}

impl CommitTSLagDetails {
    /// Merge another lag-detail snapshot into `self`.
    ///
    /// Like client-go, snapshots with `first_lag_ts == 0` are treated as "no lag happened" and
    /// do not affect the accumulated state.
    pub fn merge(&mut self, other: &Self) {
        if other.first_lag_ts == 0 {
            return;
        }

        self.wait_time += other.wait_time;
        self.backoff_count = self.backoff_count.saturating_add(other.backoff_count);
        // Keep the last lag observation after merge, matching client-go.
        self.first_lag_ts = other.first_lag_ts;
        self.wait_until_ts = other.wait_until_ts;
    }
}

/// Client-side commit execution details collected across retries.
///
/// This mirrors the public shape of client-go `util.CommitDetails`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitDetails {
    /// Time spent fetching commit-ts from PD.
    pub get_commit_ts_time: Duration,
    /// Time spent fetching a fresh read timestamp from PD.
    pub get_latest_ts_time: Duration,
    /// Lag diagnostics for `wait_until` commit-ts constraints.
    pub lag_details: CommitTSLagDetails,
    /// Time spent in the prewrite phase.
    pub prewrite_time: Duration,
    /// Time spent waiting for prewrite binlog writes.
    pub wait_prewrite_binlog_time: Duration,
    /// Time spent in the commit phase.
    pub commit_time: Duration,
    /// Time spent waiting on local latches.
    pub local_latch_time: Duration,
    /// Total backoff time used across prewrite + commit phases.
    pub commit_backoff_time: i64,
    /// Backoff types seen while prewriting.
    pub prewrite_backoff_types: Vec<String>,
    /// Backoff types seen while committing.
    pub commit_backoff_types: Vec<String>,
    /// Slowest prewrite request observed so far.
    pub slowest_prewrite: ReqDetailInfo,
    /// Slowest primary-commit request observed so far.
    pub commit_primary: ReqDetailInfo,
    /// Number of written keys.
    pub write_keys: usize,
    /// Total written payload size in bytes.
    pub write_size: usize,
    /// Number of regions touched by prewrite.
    pub prewrite_region_num: i32,
    /// Transaction retry count accumulated across attempts.
    pub txn_retry: usize,
    /// Time spent resolving locks while committing.
    pub resolve_lock: ResolveLockDetail,
    /// Number of prewrite RPCs issued.
    pub prewrite_req_num: usize,
}

impl CommitDetails {
    /// Merge another commit-detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.get_commit_ts_time += other.get_commit_ts_time;
        self.get_latest_ts_time += other.get_latest_ts_time;
        self.lag_details.merge(&other.lag_details);
        self.prewrite_time += other.prewrite_time;
        self.wait_prewrite_binlog_time += other.wait_prewrite_binlog_time;
        self.commit_time += other.commit_time;
        self.local_latch_time += other.local_latch_time;
        self.commit_backoff_time = self
            .commit_backoff_time
            .saturating_add(other.commit_backoff_time);
        self.write_keys = self.write_keys.saturating_add(other.write_keys);
        self.write_size = self.write_size.saturating_add(other.write_size);
        self.prewrite_region_num = self
            .prewrite_region_num
            .saturating_add(other.prewrite_region_num);
        self.txn_retry = self.txn_retry.saturating_add(other.txn_retry);
        self.prewrite_req_num = self.prewrite_req_num.saturating_add(other.prewrite_req_num);
        self.resolve_lock.resolve_lock_time += other.resolve_lock.resolve_lock_time;

        self.prewrite_backoff_types
            .extend(other.prewrite_backoff_types.iter().cloned());
        if self.slowest_prewrite.req_total_time < other.slowest_prewrite.req_total_time {
            self.slowest_prewrite = other.slowest_prewrite.clone();
        }

        self.commit_backoff_types
            .extend(other.commit_backoff_types.iter().cloned());
        if self.commit_primary.req_total_time < other.commit_primary.req_total_time {
            self.commit_primary = other.commit_primary.clone();
        }
    }

    /// Merge prewrite request diagnostics into `self`, retaining only the slowest request.
    pub fn merge_prewrite_req_details(
        &mut self,
        req_duration: Duration,
        region_id: u64,
        store_addr: impl Into<String>,
        exec_details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if req_duration > self.slowest_prewrite.req_total_time {
            self.slowest_prewrite = ReqDetailInfo {
                req_total_time: req_duration,
                region: region_id,
                store_addr: store_addr.into(),
                exec_details: TiKVExecDetails::from_proto(exec_details),
            };
        }
    }

    /// Merge primary-commit request diagnostics into `self`, retaining only the slowest request.
    pub fn merge_commit_req_details(
        &mut self,
        req_duration: Duration,
        region_id: u64,
        store_addr: impl Into<String>,
        exec_details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if req_duration > self.commit_primary.req_total_time {
            self.commit_primary = ReqDetailInfo {
                req_total_time: req_duration,
                region: region_id,
                store_addr: store_addr.into(),
                exec_details: TiKVExecDetails::from_proto(exec_details),
            };
        }
    }

    /// Placeholder for flush-phase request details.
    ///
    /// client-go exposes the helper but leaves it empty today; keep the same behavior until the
    /// Rust client collects flush-specific diagnostics.
    pub fn merge_flush_req_details(
        &mut self,
        _req_duration: Duration,
        _region_id: u64,
        _store_addr: impl Into<String>,
        _exec_details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
    }
}

/// Client-side pessimistic-lock execution details collected across retries.
///
/// This mirrors the public shape of client-go `util.LockKeysDetails`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LockKeysDetails {
    /// Total time spent locking keys.
    pub total_time: Duration,
    /// Number of regions touched by the lock requests.
    pub region_num: i32,
    /// Number of keys requested to lock.
    pub lock_keys: i32,
    /// Number of newly locked keys in aggressive-locking mode.
    pub aggressive_lock_new_count: usize,
    /// Number of lock derivations reused in aggressive-locking mode.
    pub aggressive_lock_derived_count: usize,
    /// Number of keys that encountered conflicts while locking.
    pub locked_with_conflict_count: usize,
    /// Time spent resolving encountered locks.
    pub resolve_lock: ResolveLockDetail,
    /// Total backoff time used while locking.
    pub backoff_time: i64,
    /// Backoff types seen while locking.
    pub backoff_types: Vec<String>,
    /// Slowest lock request wall time.
    pub slowest_req_total_time: Duration,
    /// Region ID of the slowest lock request.
    pub slowest_region: u64,
    /// Store address of the slowest lock request.
    pub slowest_store_addr: String,
    /// TiKV execution details associated with the slowest lock request.
    pub slowest_exec_details: TiKVExecDetails,
    /// Total lock RPC time.
    pub lock_rpc_time: i64,
    /// Total number of lock RPCs.
    pub lock_rpc_count: i64,
    /// Number of retry rounds. Like client-go, each merge counts as one extra retry.
    pub retry_count: usize,
}

impl LockKeysDetails {
    /// Merge another lock-detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.total_time += other.total_time;
        self.region_num = self.region_num.saturating_add(other.region_num);
        self.lock_keys = self.lock_keys.saturating_add(other.lock_keys);
        self.aggressive_lock_new_count = self
            .aggressive_lock_new_count
            .saturating_add(other.aggressive_lock_new_count);
        self.aggressive_lock_derived_count = self
            .aggressive_lock_derived_count
            .saturating_add(other.aggressive_lock_derived_count);
        self.locked_with_conflict_count = self
            .locked_with_conflict_count
            .saturating_add(other.locked_with_conflict_count);
        self.resolve_lock.resolve_lock_time += other.resolve_lock.resolve_lock_time;
        self.backoff_time = self.backoff_time.saturating_add(other.backoff_time);
        self.lock_rpc_time = self.lock_rpc_time.saturating_add(other.lock_rpc_time);
        self.lock_rpc_count = self.lock_rpc_count.saturating_add(other.lock_rpc_count);
        self.backoff_types
            .extend(other.backoff_types.iter().cloned());
        self.retry_count = self.retry_count.saturating_add(1);

        if self.slowest_req_total_time < other.slowest_req_total_time {
            self.slowest_req_total_time = other.slowest_req_total_time;
            self.slowest_region = other.slowest_region;
            self.slowest_store_addr = other.slowest_store_addr.clone();
            self.slowest_exec_details = other.slowest_exec_details.clone();
        }
    }

    /// Merge lock-request diagnostics into `self`, retaining only the slowest request.
    pub fn merge_req_details(
        &mut self,
        req_duration: Duration,
        region_id: u64,
        store_addr: impl Into<String>,
        exec_details: Option<&kvrpcpb::ExecDetailsV2>,
    ) {
        if req_duration > self.slowest_req_total_time {
            self.slowest_req_total_time = req_duration;
            self.slowest_region = region_id;
            self.slowest_store_addr = store_addr.into();
            self.slowest_exec_details = TiKVExecDetails::from_proto(exec_details);
        }
    }
}

/// Time-related execution details returned by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimeDetail {
    /// Time spent processing the request payload on the TiKV side.
    pub process_time: Duration,
    /// Time spent while a task is suspended/yielded.
    pub suspend_time: Duration,
    /// Time spent waiting in TiKV queues or other off-CPU waits.
    pub wait_time: Duration,
    /// Time spent reading key/value data.
    pub kv_read_wall_time: Duration,
    /// Time spent processing the request in TiKV gRPC handling.
    ///
    /// Current repo protobufs do not populate this field, but client-go exposes it publicly, so
    /// the Rust compatibility type keeps it for API parity and future proto upgrades.
    pub kv_grpc_process_time: Duration,
    /// Time spent waiting before TiKV begins sending the gRPC response.
    ///
    /// Current repo protobufs do not populate this field, but client-go exposes it publicly, so
    /// the Rust compatibility type keeps it for API parity and future proto upgrades.
    pub kv_grpc_wait_time: Duration,
    /// Total wall-clock time spent handling the RPC in TiKV.
    pub total_rpc_wall_time: Duration,
}

impl TimeDetail {
    /// Merge another time detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.process_time = self.process_time.saturating_add(other.process_time);
        self.suspend_time = self.suspend_time.saturating_add(other.suspend_time);
        self.wait_time = self.wait_time.saturating_add(other.wait_time);
        self.kv_read_wall_time = self
            .kv_read_wall_time
            .saturating_add(other.kv_read_wall_time);
        self.kv_grpc_process_time = self
            .kv_grpc_process_time
            .saturating_add(other.kv_grpc_process_time);
        self.kv_grpc_wait_time = self
            .kv_grpc_wait_time
            .saturating_add(other.kv_grpc_wait_time);
        self.total_rpc_wall_time = self
            .total_rpc_wall_time
            .saturating_add(other.total_rpc_wall_time);
    }

    /// Merge time detail fields from TiKV protobufs.
    ///
    /// This follows client-go's precedence: prefer `TimeDetailV2`, otherwise fall back to the
    /// legacy `TimeDetail`.
    pub fn merge_from_proto(
        &mut self,
        time_detail_v2: Option<&kvrpcpb::TimeDetailV2>,
        time_detail: Option<&kvrpcpb::TimeDetail>,
    ) {
        if let Some(detail) = time_detail_v2 {
            self.wait_time = self
                .wait_time
                .saturating_add(Duration::from_nanos(detail.wait_wall_time_ns));
            self.process_time = self
                .process_time
                .saturating_add(Duration::from_nanos(detail.process_wall_time_ns));
            self.suspend_time = self
                .suspend_time
                .saturating_add(Duration::from_nanos(detail.process_suspend_wall_time_ns));
            self.kv_read_wall_time = self
                .kv_read_wall_time
                .saturating_add(Duration::from_nanos(detail.kv_read_wall_time_ns));
            self.total_rpc_wall_time = self
                .total_rpc_wall_time
                .saturating_add(Duration::from_nanos(detail.total_rpc_wall_time_ns));
            return;
        }

        let Some(detail) = time_detail else {
            return;
        };

        self.wait_time = self
            .wait_time
            .saturating_add(Duration::from_millis(detail.wait_wall_time_ms));
        self.process_time = self
            .process_time
            .saturating_add(Duration::from_millis(detail.process_wall_time_ms));
        self.kv_read_wall_time = self
            .kv_read_wall_time
            .saturating_add(Duration::from_millis(detail.kv_read_wall_time_ms));
        self.total_rpc_wall_time = self
            .total_rpc_wall_time
            .saturating_add(Duration::from_nanos(detail.total_rpc_wall_time_ns));
    }

    /// Whether this detail contains any non-zero measurements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.process_time.is_zero()
            && self.suspend_time.is_zero()
            && self.wait_time.is_zero()
            && self.kv_read_wall_time.is_zero()
            && self.kv_grpc_process_time.is_zero()
            && self.kv_grpc_wait_time.is_zero()
            && self.total_rpc_wall_time.is_zero()
    }
}

impl fmt::Display for TimeDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        let mut parts = Vec::with_capacity(7);
        push_duration_part(&mut parts, "total_process_time", self.process_time);
        push_duration_part(&mut parts, "total_suspend_time", self.suspend_time);
        push_duration_part(&mut parts, "total_wait_time", self.wait_time);
        push_duration_part(
            &mut parts,
            "total_kv_read_wall_time",
            self.kv_read_wall_time,
        );
        push_duration_part(
            &mut parts,
            "tikv_grpc_process_time",
            self.kv_grpc_process_time,
        );
        push_duration_part(&mut parts, "tikv_grpc_wait_time", self.kv_grpc_wait_time);
        push_duration_part(&mut parts, "tikv_wall_time", self.total_rpc_wall_time);

        write!(f, "time_detail: {{{}}}", parts.join(", "))
    }
}

/// Scan-related execution details returned by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScanDetail {
    /// Approximate number of MVCC versions visited while scanning.
    pub total_keys: u64,
    /// Number of visible user keys processed.
    pub processed_keys: u64,
    /// Total size of processed key/value pairs.
    pub processed_keys_size: u64,
    /// RocksDB tombstones skipped during iteration.
    pub rocksdb_delete_skipped_count: u64,
    /// Internal RocksDB keys skipped during iteration.
    pub rocksdb_key_skipped_count: u64,
    /// RocksDB block cache hits.
    pub rocksdb_block_cache_hit_count: u64,
    /// RocksDB block reads requiring I/O.
    pub rocksdb_block_read_count: u64,
    /// Total bytes read from RocksDB blocks.
    pub rocksdb_block_read_byte: u64,
    /// Time spent on RocksDB block reads.
    pub rocksdb_block_read_duration: Duration,
    /// Time spent getting an engine snapshot.
    pub get_snapshot_duration: Duration,
}

impl ScanDetail {
    /// Merge another scan detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.total_keys = self.total_keys.saturating_add(other.total_keys);
        self.processed_keys = self.processed_keys.saturating_add(other.processed_keys);
        self.processed_keys_size = self
            .processed_keys_size
            .saturating_add(other.processed_keys_size);
        self.rocksdb_delete_skipped_count = self
            .rocksdb_delete_skipped_count
            .saturating_add(other.rocksdb_delete_skipped_count);
        self.rocksdb_key_skipped_count = self
            .rocksdb_key_skipped_count
            .saturating_add(other.rocksdb_key_skipped_count);
        self.rocksdb_block_cache_hit_count = self
            .rocksdb_block_cache_hit_count
            .saturating_add(other.rocksdb_block_cache_hit_count);
        self.rocksdb_block_read_count = self
            .rocksdb_block_read_count
            .saturating_add(other.rocksdb_block_read_count);
        self.rocksdb_block_read_byte = self
            .rocksdb_block_read_byte
            .saturating_add(other.rocksdb_block_read_byte);
        self.rocksdb_block_read_duration = self
            .rocksdb_block_read_duration
            .saturating_add(other.rocksdb_block_read_duration);
        self.get_snapshot_duration = self
            .get_snapshot_duration
            .saturating_add(other.get_snapshot_duration);
    }

    /// Merge scan details from a TiKV `ScanDetailV2` protobuf message.
    pub fn merge_from_proto(&mut self, scan_detail: Option<&kvrpcpb::ScanDetailV2>) {
        let Some(detail) = scan_detail else {
            return;
        };

        self.total_keys = self.total_keys.saturating_add(detail.total_versions);
        self.processed_keys = self
            .processed_keys
            .saturating_add(detail.processed_versions);
        self.processed_keys_size = self
            .processed_keys_size
            .saturating_add(detail.processed_versions_size);
        self.rocksdb_delete_skipped_count = self
            .rocksdb_delete_skipped_count
            .saturating_add(detail.rocksdb_delete_skipped_count);
        self.rocksdb_key_skipped_count = self
            .rocksdb_key_skipped_count
            .saturating_add(detail.rocksdb_key_skipped_count);
        self.rocksdb_block_cache_hit_count = self
            .rocksdb_block_cache_hit_count
            .saturating_add(detail.rocksdb_block_cache_hit_count);
        self.rocksdb_block_read_count = self
            .rocksdb_block_read_count
            .saturating_add(detail.rocksdb_block_read_count);
        self.rocksdb_block_read_byte = self
            .rocksdb_block_read_byte
            .saturating_add(detail.rocksdb_block_read_byte);
        self.rocksdb_block_read_duration = self
            .rocksdb_block_read_duration
            .saturating_add(Duration::from_nanos(detail.rocksdb_block_read_nanos));
        self.get_snapshot_duration = self
            .get_snapshot_duration
            .saturating_add(Duration::from_nanos(detail.get_snapshot_nanos));
    }

    /// Whether this detail contains any non-zero measurements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_keys == 0
            && self.processed_keys == 0
            && self.processed_keys_size == 0
            && self.rocksdb_delete_skipped_count == 0
            && self.rocksdb_key_skipped_count == 0
            && self.rocksdb_block_cache_hit_count == 0
            && self.rocksdb_block_read_count == 0
            && self.rocksdb_block_read_byte == 0
            && self.rocksdb_block_read_duration.is_zero()
            && self.get_snapshot_duration.is_zero()
    }
}

impl fmt::Display for ScanDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        let mut parts = Vec::with_capacity(10);
        if self.processed_keys > 0 {
            parts.push(format!("total_process_keys: {}", self.processed_keys));
        }
        if self.processed_keys_size > 0 {
            parts.push(format!(
                "total_process_keys_size: {}",
                self.processed_keys_size
            ));
        }
        if self.total_keys > 0 {
            parts.push(format!("total_keys: {}", self.total_keys));
        }
        if !self.get_snapshot_duration.is_zero() {
            parts.push(format!(
                "get_snapshot_time: {}",
                format_duration(self.get_snapshot_duration)
            ));
        }

        let mut rocksdb_parts = Vec::with_capacity(3);
        if self.rocksdb_delete_skipped_count > 0 {
            rocksdb_parts.push(format!(
                "delete_skipped_count: {}",
                self.rocksdb_delete_skipped_count
            ));
        }
        if self.rocksdb_key_skipped_count > 0 {
            rocksdb_parts.push(format!(
                "key_skipped_count: {}",
                self.rocksdb_key_skipped_count
            ));
        }

        let mut block_parts = Vec::with_capacity(4);
        if self.rocksdb_block_cache_hit_count > 0 {
            block_parts.push(format!(
                "cache_hit_count: {}",
                self.rocksdb_block_cache_hit_count
            ));
        }
        if self.rocksdb_block_read_count > 0 {
            block_parts.push(format!("read_count: {}", self.rocksdb_block_read_count));
        }
        if self.rocksdb_block_read_byte > 0 {
            let bytes = i64::try_from(self.rocksdb_block_read_byte).unwrap_or(i64::MAX);
            block_parts.push(format!("read_byte: {}", format_bytes(bytes)));
        }
        if !self.rocksdb_block_read_duration.is_zero() {
            block_parts.push(format!(
                "read_time: {}",
                format_duration(self.rocksdb_block_read_duration)
            ));
        }

        rocksdb_parts.push(format!("block: {{{}}}", block_parts.join(", ")));
        parts.push(format!("rocksdb: {{{}}}", rocksdb_parts.join(", ")));

        write!(f, "scan_detail: {{{}}}", parts.join(", "))
    }
}

/// Write-path execution details returned by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteDetail {
    /// Wait duration in the store loop.
    pub store_batch_wait_duration: Duration,
    /// Wait duration before sending the proposal to peers.
    pub propose_send_wait_duration: Duration,
    /// Total time spent persisting the Raft log.
    pub persist_log_duration: Duration,
    /// Wait time until the Raft log write leader begins to write.
    pub raft_db_write_leader_wait_duration: Duration,
    /// Time spent syncing the Raft log to disk.
    pub raft_db_sync_log_duration: Duration,
    /// Time spent writing the Raft log to the memtable.
    pub raft_db_write_memtable_duration: Duration,
    /// Time spent waiting for peers to confirm the proposal.
    pub commit_log_duration: Duration,
    /// Wait duration in the apply loop.
    pub apply_batch_wait_duration: Duration,
    /// Total time spent applying the log.
    pub apply_log_duration: Duration,
    /// Wait time until the KV RocksDB mutex is acquired.
    pub apply_mutex_lock_duration: Duration,
    /// Wait time until becoming the KV RocksDB write leader.
    pub apply_write_leader_wait_duration: Duration,
    /// Time spent writing the KV WAL.
    pub apply_write_wal_duration: Duration,
    /// Time spent writing the KV memtable.
    pub apply_write_memtable_duration: Duration,
    /// Scheduler latch wait time.
    pub scheduler_latch_wait_duration: Duration,
    /// Scheduler processing time.
    pub scheduler_process_duration: Duration,
    /// Scheduler throttle wait time.
    pub scheduler_throttle_duration: Duration,
    /// Waiter-manager time for pessimistic locking.
    pub scheduler_pessimistic_lock_wait_duration: Duration,
}

impl WriteDetail {
    /// Merge another write detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.store_batch_wait_duration = self
            .store_batch_wait_duration
            .saturating_add(other.store_batch_wait_duration);
        self.propose_send_wait_duration = self
            .propose_send_wait_duration
            .saturating_add(other.propose_send_wait_duration);
        self.persist_log_duration = self
            .persist_log_duration
            .saturating_add(other.persist_log_duration);
        self.raft_db_write_leader_wait_duration = self
            .raft_db_write_leader_wait_duration
            .saturating_add(other.raft_db_write_leader_wait_duration);
        self.raft_db_sync_log_duration = self
            .raft_db_sync_log_duration
            .saturating_add(other.raft_db_sync_log_duration);
        self.raft_db_write_memtable_duration = self
            .raft_db_write_memtable_duration
            .saturating_add(other.raft_db_write_memtable_duration);
        self.commit_log_duration = self
            .commit_log_duration
            .saturating_add(other.commit_log_duration);
        self.apply_batch_wait_duration = self
            .apply_batch_wait_duration
            .saturating_add(other.apply_batch_wait_duration);
        self.apply_log_duration = self
            .apply_log_duration
            .saturating_add(other.apply_log_duration);
        self.apply_mutex_lock_duration = self
            .apply_mutex_lock_duration
            .saturating_add(other.apply_mutex_lock_duration);
        self.apply_write_leader_wait_duration = self
            .apply_write_leader_wait_duration
            .saturating_add(other.apply_write_leader_wait_duration);
        self.apply_write_wal_duration = self
            .apply_write_wal_duration
            .saturating_add(other.apply_write_wal_duration);
        self.apply_write_memtable_duration = self
            .apply_write_memtable_duration
            .saturating_add(other.apply_write_memtable_duration);
        self.scheduler_latch_wait_duration = self
            .scheduler_latch_wait_duration
            .saturating_add(other.scheduler_latch_wait_duration);
        self.scheduler_process_duration = self
            .scheduler_process_duration
            .saturating_add(other.scheduler_process_duration);
        self.scheduler_throttle_duration = self
            .scheduler_throttle_duration
            .saturating_add(other.scheduler_throttle_duration);
        self.scheduler_pessimistic_lock_wait_duration = self
            .scheduler_pessimistic_lock_wait_duration
            .saturating_add(other.scheduler_pessimistic_lock_wait_duration);
    }

    /// Merge write details from a TiKV protobuf message.
    pub fn merge_from_proto(&mut self, write_detail: Option<&kvrpcpb::WriteDetail>) {
        let Some(detail) = write_detail else {
            return;
        };

        self.store_batch_wait_duration = self
            .store_batch_wait_duration
            .saturating_add(Duration::from_nanos(detail.store_batch_wait_nanos));
        self.propose_send_wait_duration = self
            .propose_send_wait_duration
            .saturating_add(Duration::from_nanos(detail.propose_send_wait_nanos));
        self.persist_log_duration = self
            .persist_log_duration
            .saturating_add(Duration::from_nanos(detail.persist_log_nanos));
        self.raft_db_write_leader_wait_duration = self
            .raft_db_write_leader_wait_duration
            .saturating_add(Duration::from_nanos(detail.raft_db_write_leader_wait_nanos));
        self.raft_db_sync_log_duration = self
            .raft_db_sync_log_duration
            .saturating_add(Duration::from_nanos(detail.raft_db_sync_log_nanos));
        self.raft_db_write_memtable_duration = self
            .raft_db_write_memtable_duration
            .saturating_add(Duration::from_nanos(detail.raft_db_write_memtable_nanos));
        self.commit_log_duration = self
            .commit_log_duration
            .saturating_add(Duration::from_nanos(detail.commit_log_nanos));
        self.apply_batch_wait_duration = self
            .apply_batch_wait_duration
            .saturating_add(Duration::from_nanos(detail.apply_batch_wait_nanos));
        self.apply_log_duration = self
            .apply_log_duration
            .saturating_add(Duration::from_nanos(detail.apply_log_nanos));
        self.apply_mutex_lock_duration = self
            .apply_mutex_lock_duration
            .saturating_add(Duration::from_nanos(detail.apply_mutex_lock_nanos));
        self.apply_write_leader_wait_duration = self
            .apply_write_leader_wait_duration
            .saturating_add(Duration::from_nanos(detail.apply_write_leader_wait_nanos));
        self.apply_write_wal_duration = self
            .apply_write_wal_duration
            .saturating_add(Duration::from_nanos(detail.apply_write_wal_nanos));
        self.apply_write_memtable_duration = self
            .apply_write_memtable_duration
            .saturating_add(Duration::from_nanos(detail.apply_write_memtable_nanos));
        self.scheduler_latch_wait_duration = self
            .scheduler_latch_wait_duration
            .saturating_add(Duration::from_nanos(detail.latch_wait_nanos));
        self.scheduler_process_duration = self
            .scheduler_process_duration
            .saturating_add(Duration::from_nanos(detail.process_nanos));
        self.scheduler_throttle_duration = self
            .scheduler_throttle_duration
            .saturating_add(Duration::from_nanos(detail.throttle_nanos));
        self.scheduler_pessimistic_lock_wait_duration = self
            .scheduler_pessimistic_lock_wait_duration
            .saturating_add(Duration::from_nanos(detail.pessimistic_lock_wait_nanos));
    }

    /// Whether this detail contains any non-zero measurements.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store_batch_wait_duration.is_zero()
            && self.propose_send_wait_duration.is_zero()
            && self.persist_log_duration.is_zero()
            && self.raft_db_write_leader_wait_duration.is_zero()
            && self.raft_db_sync_log_duration.is_zero()
            && self.raft_db_write_memtable_duration.is_zero()
            && self.commit_log_duration.is_zero()
            && self.apply_batch_wait_duration.is_zero()
            && self.apply_log_duration.is_zero()
            && self.apply_mutex_lock_duration.is_zero()
            && self.apply_write_leader_wait_duration.is_zero()
            && self.apply_write_wal_duration.is_zero()
            && self.apply_write_memtable_duration.is_zero()
            && self.scheduler_latch_wait_duration.is_zero()
            && self.scheduler_process_duration.is_zero()
            && self.scheduler_throttle_duration.is_zero()
            && self.scheduler_pessimistic_lock_wait_duration.is_zero()
    }
}

impl fmt::Display for WriteDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        let mut scheduler_parts = vec![format!(
            "process: {}",
            format_duration(self.scheduler_process_duration)
        )];
        if !self.scheduler_latch_wait_duration.is_zero() {
            scheduler_parts.push(format!(
                "latch_wait: {}",
                format_duration(self.scheduler_latch_wait_duration)
            ));
        }
        if !self.scheduler_pessimistic_lock_wait_duration.is_zero() {
            scheduler_parts.push(format!(
                "pessimistic_lock_wait: {}",
                format_duration(self.scheduler_pessimistic_lock_wait_duration)
            ));
        }
        if !self.scheduler_throttle_duration.is_zero() {
            scheduler_parts.push(format!(
                "throttle: {}",
                format_duration(self.scheduler_throttle_duration)
            ));
        }

        write!(
            f,
            "write_detail: {{store_batch_wait: {}, propose_send_wait: {}, persist_log: {{total: {}, write_leader_wait: {}, sync_log: {}, write_memtable: {}}}, commit_log: {}, apply_batch_wait: {}, apply: {{total: {}, mutex_lock: {}, write_leader_wait: {}, write_wal: {}, write_memtable: {}}}, scheduler: {{{}}}}}",
            format_duration(self.store_batch_wait_duration),
            format_duration(self.propose_send_wait_duration),
            format_duration(self.persist_log_duration),
            format_duration(self.raft_db_write_leader_wait_duration),
            format_duration(self.raft_db_sync_log_duration),
            format_duration(self.raft_db_write_memtable_duration),
            format_duration(self.commit_log_duration),
            format_duration(self.apply_batch_wait_duration),
            format_duration(self.apply_log_duration),
            format_duration(self.apply_mutex_lock_duration),
            format_duration(self.apply_write_leader_wait_duration),
            format_duration(self.apply_write_wal_duration),
            format_duration(self.apply_write_memtable_duration),
            scheduler_parts.join(", "),
        )
    }
}

/// Traffic-related counters collected alongside execution details.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct TrafficDetails {
    unpacked_bytes_sent_kv_total: AtomicI64,
    unpacked_bytes_received_kv_total: AtomicI64,
    unpacked_bytes_sent_kv_cross_zone: AtomicI64,
    unpacked_bytes_received_kv_cross_zone: AtomicI64,
    unpacked_bytes_sent_mpp_total: AtomicI64,
    unpacked_bytes_received_mpp_total: AtomicI64,
    unpacked_bytes_sent_mpp_cross_zone: AtomicI64,
    unpacked_bytes_received_mpp_cross_zone: AtomicI64,
}

impl Clone for TrafficDetails {
    fn clone(&self) -> Self {
        Self {
            unpacked_bytes_sent_kv_total: AtomicI64::new(self.unpacked_bytes_sent_kv_total()),
            unpacked_bytes_received_kv_total: AtomicI64::new(
                self.unpacked_bytes_received_kv_total(),
            ),
            unpacked_bytes_sent_kv_cross_zone: AtomicI64::new(
                self.unpacked_bytes_sent_kv_cross_zone(),
            ),
            unpacked_bytes_received_kv_cross_zone: AtomicI64::new(
                self.unpacked_bytes_received_kv_cross_zone(),
            ),
            unpacked_bytes_sent_mpp_total: AtomicI64::new(self.unpacked_bytes_sent_mpp_total()),
            unpacked_bytes_received_mpp_total: AtomicI64::new(
                self.unpacked_bytes_received_mpp_total(),
            ),
            unpacked_bytes_sent_mpp_cross_zone: AtomicI64::new(
                self.unpacked_bytes_sent_mpp_cross_zone(),
            ),
            unpacked_bytes_received_mpp_cross_zone: AtomicI64::new(
                self.unpacked_bytes_received_mpp_cross_zone(),
            ),
        }
    }
}

impl TrafficDetails {
    /// Add KV traffic counters.
    pub fn add_kv_bytes(
        &self,
        sent_total: i64,
        received_total: i64,
        sent_cross_zone: i64,
        received_cross_zone: i64,
    ) {
        saturating_fetch_add_i64(&self.unpacked_bytes_sent_kv_total, sent_total);
        saturating_fetch_add_i64(&self.unpacked_bytes_received_kv_total, received_total);
        saturating_fetch_add_i64(&self.unpacked_bytes_sent_kv_cross_zone, sent_cross_zone);
        saturating_fetch_add_i64(
            &self.unpacked_bytes_received_kv_cross_zone,
            received_cross_zone,
        );
    }

    /// Add MPP traffic counters.
    pub fn add_mpp_bytes(
        &self,
        sent_total: i64,
        received_total: i64,
        sent_cross_zone: i64,
        received_cross_zone: i64,
    ) {
        saturating_fetch_add_i64(&self.unpacked_bytes_sent_mpp_total, sent_total);
        saturating_fetch_add_i64(&self.unpacked_bytes_received_mpp_total, received_total);
        saturating_fetch_add_i64(&self.unpacked_bytes_sent_mpp_cross_zone, sent_cross_zone);
        saturating_fetch_add_i64(
            &self.unpacked_bytes_received_mpp_cross_zone,
            received_cross_zone,
        );
    }

    /// Merge another traffic snapshot into `self`.
    pub fn merge(&self, other: &Self) {
        self.add_kv_bytes(
            other.unpacked_bytes_sent_kv_total(),
            other.unpacked_bytes_received_kv_total(),
            other.unpacked_bytes_sent_kv_cross_zone(),
            other.unpacked_bytes_received_kv_cross_zone(),
        );
        self.add_mpp_bytes(
            other.unpacked_bytes_sent_mpp_total(),
            other.unpacked_bytes_received_mpp_total(),
            other.unpacked_bytes_sent_mpp_cross_zone(),
            other.unpacked_bytes_received_mpp_cross_zone(),
        );
    }

    /// Total unpacked KV bytes sent.
    #[must_use]
    pub fn unpacked_bytes_sent_kv_total(&self) -> i64 {
        self.unpacked_bytes_sent_kv_total.load(Ordering::Relaxed)
    }

    /// Total unpacked KV bytes received.
    #[must_use]
    pub fn unpacked_bytes_received_kv_total(&self) -> i64 {
        self.unpacked_bytes_received_kv_total
            .load(Ordering::Relaxed)
    }

    /// Total unpacked cross-zone KV bytes sent.
    #[must_use]
    pub fn unpacked_bytes_sent_kv_cross_zone(&self) -> i64 {
        self.unpacked_bytes_sent_kv_cross_zone
            .load(Ordering::Relaxed)
    }

    /// Total unpacked cross-zone KV bytes received.
    #[must_use]
    pub fn unpacked_bytes_received_kv_cross_zone(&self) -> i64 {
        self.unpacked_bytes_received_kv_cross_zone
            .load(Ordering::Relaxed)
    }

    /// Total unpacked MPP bytes sent.
    #[must_use]
    pub fn unpacked_bytes_sent_mpp_total(&self) -> i64 {
        self.unpacked_bytes_sent_mpp_total.load(Ordering::Relaxed)
    }

    /// Total unpacked MPP bytes received.
    #[must_use]
    pub fn unpacked_bytes_received_mpp_total(&self) -> i64 {
        self.unpacked_bytes_received_mpp_total
            .load(Ordering::Relaxed)
    }

    /// Total unpacked cross-zone MPP bytes sent.
    #[must_use]
    pub fn unpacked_bytes_sent_mpp_cross_zone(&self) -> i64 {
        self.unpacked_bytes_sent_mpp_cross_zone
            .load(Ordering::Relaxed)
    }

    /// Total unpacked cross-zone MPP bytes received.
    #[must_use]
    pub fn unpacked_bytes_received_mpp_cross_zone(&self) -> i64 {
        self.unpacked_bytes_received_mpp_cross_zone
            .load(Ordering::Relaxed)
    }
}

/// Lightweight client-side execution counters collected across retries and RPC waits.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ExecDetails {
    backoff_count: AtomicU64,
    backoff_duration_ns: AtomicU64,
    wait_kv_resp_duration_ns: AtomicU64,
    wait_pd_resp_duration_ns: AtomicU64,
    traffic_details: TrafficDetails,
}

impl Clone for ExecDetails {
    fn clone(&self) -> Self {
        let clone = Self::default();
        clone.add_backoff_count(self.backoff_count());
        clone.add_backoff_duration(self.backoff_duration());
        clone.add_wait_kv_response(self.wait_kv_resp_duration());
        clone.add_wait_pd_response(self.wait_pd_resp_duration());
        clone.traffic_details.merge(&self.traffic_details);
        clone
    }
}

impl ExecDetails {
    /// Create an empty execution-detail accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `count` to the backoff counter.
    pub fn add_backoff_count(&self, count: u64) {
        saturating_fetch_add_u64(&self.backoff_count, count);
    }

    /// Add a backoff sleep duration.
    pub fn add_backoff_duration(&self, duration: Duration) {
        saturating_fetch_add_u64(&self.backoff_duration_ns, duration_to_nanos(duration));
    }

    /// Add a backoff event (count + duration).
    pub fn add_backoff(&self, duration: Duration) {
        self.add_backoff_count(1);
        self.add_backoff_duration(duration);
    }

    /// Add time spent waiting for a TiKV RPC response.
    pub fn add_wait_kv_response(&self, duration: Duration) {
        saturating_fetch_add_u64(&self.wait_kv_resp_duration_ns, duration_to_nanos(duration));
    }

    /// Add time spent waiting for a PD RPC response.
    pub fn add_wait_pd_response(&self, duration: Duration) {
        saturating_fetch_add_u64(&self.wait_pd_resp_duration_ns, duration_to_nanos(duration));
    }

    /// Merge another execution-detail accumulator into `self`.
    pub fn merge(&self, other: &Self) {
        self.add_backoff_count(other.backoff_count());
        self.add_backoff_duration(other.backoff_duration());
        self.add_wait_kv_response(other.wait_kv_resp_duration());
        self.add_wait_pd_response(other.wait_pd_resp_duration());
        self.traffic_details.merge(&other.traffic_details);
    }

    /// Total number of backoff sleeps.
    #[must_use]
    pub fn backoff_count(&self) -> u64 {
        self.backoff_count.load(Ordering::Relaxed)
    }

    /// Total backoff sleep time.
    #[must_use]
    pub fn backoff_duration(&self) -> Duration {
        Duration::from_nanos(self.backoff_duration_ns.load(Ordering::Relaxed))
    }

    /// Total time spent waiting for KV responses.
    #[must_use]
    pub fn wait_kv_resp_duration(&self) -> Duration {
        Duration::from_nanos(self.wait_kv_resp_duration_ns.load(Ordering::Relaxed))
    }

    /// Total time spent waiting for PD responses.
    #[must_use]
    pub fn wait_pd_resp_duration(&self) -> Duration {
        Duration::from_nanos(self.wait_pd_resp_duration_ns.load(Ordering::Relaxed))
    }

    /// Traffic counters associated with this execution accumulator.
    #[must_use]
    pub fn traffic_details(&self) -> &TrafficDetails {
        &self.traffic_details
    }
}

/// Resource unit (RU) consumption details.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct RUDetails {
    read_ru: f64,
    write_ru: f64,
    ru_wait_duration: Duration,
}

impl RUDetails {
    /// Create an empty RU detail accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create RU details with explicit initial values.
    #[must_use]
    pub fn new_with(read_ru: f64, write_ru: f64, wait_duration: Duration) -> Self {
        Self {
            read_ru,
            write_ru,
            ru_wait_duration: wait_duration,
        }
    }

    /// Merge another RU detail snapshot into `self`.
    pub fn merge(&mut self, other: &Self) {
        self.read_ru += other.read_ru;
        self.write_ru += other.write_ru;
        self.ru_wait_duration = self.ru_wait_duration.saturating_add(other.ru_wait_duration);
    }

    /// Update RU details from a resource-manager consumption snapshot.
    pub fn update(
        &mut self,
        consumption: Option<&resource_manager::Consumption>,
        wait_duration: Duration,
    ) {
        let Some(consumption) = consumption else {
            return;
        };

        self.read_ru += consumption.r_r_u;
        self.write_ru += consumption.w_r_u;
        self.ru_wait_duration = self.ru_wait_duration.saturating_add(wait_duration);
    }

    /// Total read RU.
    #[must_use]
    pub fn rru(&self) -> f64 {
        self.read_ru
    }

    /// Total write RU.
    #[must_use]
    pub fn wru(&self) -> f64 {
        self.write_ru
    }

    /// Total time spent waiting for RU tokens.
    #[must_use]
    pub fn ru_wait_duration(&self) -> Duration {
        self.ru_wait_duration
    }
}

impl fmt::Display for RUDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RRU:{:.6}, WRU:{:.6}, WaitDuration:{}",
            self.read_ru,
            self.write_ru,
            format_duration(self.ru_wait_duration)
        )
    }
}

/// Runs `future` with a task-local execution-detail accumulator.
pub fn with_exec_details<T, F>(details: Arc<ExecDetails>, future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_EXEC_DETAILS.scope(details, future)
}

/// Returns the task-local execution-detail accumulator, if present.
#[must_use]
pub fn exec_details() -> Option<Arc<ExecDetails>> {
    TASK_EXEC_DETAILS.try_with(|details| details.clone()).ok()
}

/// Runs `future` with trace-exec-details enabled for the current Tokio task.
pub fn with_trace_exec_details<T, F>(future: F) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_TRACE_EXEC_DETAILS_ENABLED.scope(true, future)
}

/// Returns whether trace-exec-details is enabled in the current Tokio task.
#[must_use]
pub fn trace_exec_details_enabled() -> bool {
    TASK_TRACE_EXEC_DETAILS_ENABLED
        .try_with(|enabled| *enabled)
        .unwrap_or(false)
}

pub(crate) async fn scope_task_exec_details<T, F>(
    details: Option<Arc<ExecDetails>>,
    trace_enabled: bool,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    match (details, trace_enabled) {
        (Some(details), true) => with_exec_details(details, with_trace_exec_details(future)).await,
        (Some(details), false) => with_exec_details(details, future).await,
        (None, true) => with_trace_exec_details(future).await,
        (None, false) => future.await,
    }
}

pub(crate) fn scope_task_traffic_kind<T, F>(
    is_mpp: bool,
    is_cross_zone: bool,
    future: F,
) -> impl Future<Output = T>
where
    F: Future<Output = T>,
{
    TASK_TRAFFIC_KIND.scope(
        TaskTrafficKind {
            is_mpp,
            is_cross_zone,
        },
        future,
    )
}

pub(crate) fn task_traffic_kind() -> (bool, bool) {
    TASK_TRAFFIC_KIND
        .try_with(|kind| (kind.is_mpp, kind.is_cross_zone))
        .unwrap_or((false, false))
}

pub(crate) fn record_task_local_backoff(duration: Duration) {
    if let Some(details) = exec_details() {
        details.add_backoff(duration);
    }
}

pub(crate) fn record_task_local_wait_kv_response(duration: Duration) {
    if let Some(details) = exec_details() {
        details.add_wait_kv_response(duration);
    }
}

pub(crate) fn record_task_local_wait_pd_response(duration: Duration) {
    if let Some(details) = exec_details() {
        details.add_wait_pd_response(duration);
    }
}

pub(crate) fn record_task_local_kv_traffic(sent_bytes: i64, received_bytes: i64) {
    if let Some(details) = exec_details() {
        let kind = TASK_TRAFFIC_KIND.try_with(|kind| *kind).unwrap_or_default();
        let (sent_cross_zone, received_cross_zone) = if kind.is_cross_zone {
            (sent_bytes, received_bytes)
        } else {
            (0, 0)
        };
        if kind.is_mpp {
            details.traffic_details().add_mpp_bytes(
                sent_bytes,
                received_bytes,
                sent_cross_zone,
                received_cross_zone,
            );
        } else {
            details.traffic_details().add_kv_bytes(
                sent_bytes,
                received_bytes,
                sent_cross_zone,
                received_cross_zone,
            );
        }
    }
}

fn push_duration_part(parts: &mut Vec<String>, name: &str, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    parts.push(format!("{name}: {}", format_duration(duration)));
}

/// Format a duration using the same precision-pruning rules as client-go `util.FormatDuration`.
#[must_use]
pub fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    if nanos <= 1_000 {
        if nanos == 1_000 {
            return "1us".to_owned();
        }
        return format!("{nanos}ns");
    }

    let (unit_size, unit_name) = if nanos >= 1_000_000_000 {
        (1_000_000_000_f64, "s")
    } else if nanos >= 1_000_000 {
        (1_000_000_f64, "ms")
    } else {
        (1_000_f64, "us")
    };

    let value = nanos as f64 / unit_size;
    let decimals = if value < 10.0 { 2 } else { 1 };
    let factor = 10_f64.powi(decimals);
    let rounded = (value * factor).round() / factor;

    format!("{}{}", trim_float(rounded, decimals as usize), unit_name)
}

fn trim_float(value: f64, decimals: usize) -> String {
    let mut text = format!("{value:.decimals$}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn saturating_fetch_add_u64(slot: &AtomicU64, value: u64) {
    let _ = slot.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn saturating_fetch_add_i64(slot: &AtomicI64, value: i64) {
    let _ = slot.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn sample_exec_details_v2(wait_ms: u64, process_ms: u64) -> kvrpcpb::ExecDetailsV2 {
        kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                wait_wall_time_ns: wait_ms * 1_000_000,
                process_wall_time_ns: process_ms * 1_000_000,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_commit_ts_lag_details_merge() {
        let mut details = CommitTSLagDetails {
            wait_time: Duration::from_millis(3),
            backoff_count: 1,
            first_lag_ts: 10,
            wait_until_ts: 20,
        };

        details.merge(&CommitTSLagDetails::default());
        assert_eq!(details.wait_time, Duration::from_millis(3));
        assert_eq!(details.backoff_count, 1);
        assert_eq!(details.first_lag_ts, 10);
        assert_eq!(details.wait_until_ts, 20);

        details.merge(&CommitTSLagDetails {
            wait_time: Duration::from_millis(5),
            backoff_count: 2,
            first_lag_ts: 30,
            wait_until_ts: 40,
        });

        assert_eq!(details.wait_time, Duration::from_millis(8));
        assert_eq!(details.backoff_count, 3);
        assert_eq!(details.first_lag_ts, 30);
        assert_eq!(details.wait_until_ts, 40);
    }

    #[test]
    fn test_commit_details_merge() {
        let mut left = CommitDetails {
            get_commit_ts_time: Duration::from_millis(10),
            get_latest_ts_time: Duration::from_millis(5),
            lag_details: CommitTSLagDetails {
                wait_time: Duration::from_millis(1),
                backoff_count: 1,
                first_lag_ts: 11,
                wait_until_ts: 12,
            },
            prewrite_time: Duration::from_millis(20),
            wait_prewrite_binlog_time: Duration::from_millis(3),
            commit_time: Duration::from_millis(15),
            local_latch_time: Duration::from_millis(2),
            commit_backoff_time: 100,
            prewrite_backoff_types: vec!["txnLock".to_owned()],
            commit_backoff_types: vec!["regionMiss".to_owned()],
            slowest_prewrite: ReqDetailInfo {
                req_total_time: Duration::from_millis(5),
                region: 1,
                store_addr: "s1".to_owned(),
                ..Default::default()
            },
            commit_primary: ReqDetailInfo {
                req_total_time: Duration::from_millis(3),
                region: 2,
                store_addr: "s2".to_owned(),
                ..Default::default()
            },
            write_keys: 100,
            write_size: 2_000,
            prewrite_region_num: 4,
            txn_retry: 1,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: Duration::from_nanos(50),
            },
            prewrite_req_num: 2,
        };
        let right = CommitDetails {
            get_commit_ts_time: Duration::from_millis(12),
            get_latest_ts_time: Duration::from_millis(6),
            lag_details: CommitTSLagDetails {
                wait_time: Duration::from_millis(2),
                backoff_count: 3,
                first_lag_ts: 21,
                wait_until_ts: 22,
            },
            prewrite_time: Duration::from_millis(25),
            wait_prewrite_binlog_time: Duration::from_millis(4),
            commit_time: Duration::from_millis(18),
            local_latch_time: Duration::from_millis(3),
            commit_backoff_time: 200,
            prewrite_backoff_types: vec!["tikvRPC".to_owned()],
            commit_backoff_types: vec!["txnLock".to_owned()],
            slowest_prewrite: ReqDetailInfo {
                req_total_time: Duration::from_millis(8),
                region: 10,
                store_addr: "s10".to_owned(),
                ..Default::default()
            },
            commit_primary: ReqDetailInfo {
                req_total_time: Duration::from_millis(6),
                region: 20,
                store_addr: "s20".to_owned(),
                ..Default::default()
            },
            write_keys: 150,
            write_size: 3_000,
            prewrite_region_num: 5,
            txn_retry: 2,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: Duration::from_nanos(60),
            },
            prewrite_req_num: 3,
        };

        left.merge(&right);

        assert_eq!(left.get_commit_ts_time, Duration::from_millis(22));
        assert_eq!(left.get_latest_ts_time, Duration::from_millis(11));
        assert_eq!(left.lag_details.wait_time, Duration::from_millis(3));
        assert_eq!(left.lag_details.backoff_count, 4);
        assert_eq!(left.lag_details.first_lag_ts, 21);
        assert_eq!(left.lag_details.wait_until_ts, 22);
        assert_eq!(left.prewrite_time, Duration::from_millis(45));
        assert_eq!(left.wait_prewrite_binlog_time, Duration::from_millis(7));
        assert_eq!(left.commit_time, Duration::from_millis(33));
        assert_eq!(left.local_latch_time, Duration::from_millis(5));
        assert_eq!(left.write_keys, 250);
        assert_eq!(left.write_size, 5_000);
        assert_eq!(left.prewrite_region_num, 9);
        assert_eq!(left.txn_retry, 3);
        assert_eq!(
            left.resolve_lock.resolve_lock_time,
            Duration::from_nanos(110)
        );
        assert_eq!(left.commit_backoff_time, 300);
        assert_eq!(
            left.prewrite_backoff_types,
            vec!["txnLock".to_owned(), "tikvRPC".to_owned()]
        );
        assert_eq!(
            left.commit_backoff_types,
            vec!["regionMiss".to_owned(), "txnLock".to_owned()]
        );
        assert_eq!(
            left.slowest_prewrite.req_total_time,
            Duration::from_millis(8)
        );
        assert_eq!(left.slowest_prewrite.region, 10);
        assert_eq!(left.slowest_prewrite.store_addr, "s10");
        assert_eq!(left.commit_primary.req_total_time, Duration::from_millis(6));
        assert_eq!(left.commit_primary.region, 20);
        assert_eq!(left.commit_primary.store_addr, "s20");
        assert_eq!(left.prewrite_req_num, 5);
    }

    #[test]
    fn test_commit_details_merge_req_helpers_and_clone() {
        let exec_details = sample_exec_details_v2(2, 3);
        let mut details = CommitDetails::default();

        details.merge_prewrite_req_details(
            Duration::from_millis(5),
            1,
            "store-1",
            Some(&exec_details),
        );
        details.merge_commit_req_details(
            Duration::from_millis(4),
            2,
            "store-2",
            Some(&exec_details),
        );
        details.merge_prewrite_req_details(Duration::from_millis(3), 9, "store-9", None);
        details.merge_flush_req_details(Duration::from_secs(1), 99, "noop", Some(&exec_details));

        let cloned = details.clone();

        assert_eq!(
            cloned.slowest_prewrite.req_total_time,
            Duration::from_millis(5)
        );
        assert_eq!(cloned.slowest_prewrite.region, 1);
        assert_eq!(cloned.slowest_prewrite.store_addr, "store-1");
        assert_eq!(
            cloned.slowest_prewrite.exec_details,
            TiKVExecDetails::from_proto(Some(&exec_details))
        );
        assert_eq!(
            cloned.commit_primary.req_total_time,
            Duration::from_millis(4)
        );
        assert_eq!(cloned.commit_primary.region, 2);
        assert_eq!(cloned.commit_primary.store_addr, "store-2");
    }

    #[test]
    fn test_commit_details_merge_keeps_existing_slower_requests() {
        let mut left = CommitDetails {
            slowest_prewrite: ReqDetailInfo {
                req_total_time: Duration::from_millis(10),
                region: 1,
                ..Default::default()
            },
            commit_primary: ReqDetailInfo {
                req_total_time: Duration::from_millis(10),
                region: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let right = CommitDetails {
            slowest_prewrite: ReqDetailInfo {
                req_total_time: Duration::from_millis(5),
                region: 3,
                ..Default::default()
            },
            commit_primary: ReqDetailInfo {
                req_total_time: Duration::from_millis(5),
                region: 4,
                ..Default::default()
            },
            ..Default::default()
        };

        left.merge(&right);

        assert_eq!(left.slowest_prewrite.region, 1);
        assert_eq!(left.commit_primary.region, 2);
    }

    #[test]
    fn test_lock_keys_details_merge() {
        let mut left = LockKeysDetails {
            total_time: Duration::from_millis(10),
            region_num: 2,
            lock_keys: 5,
            aggressive_lock_new_count: 1,
            aggressive_lock_derived_count: 2,
            locked_with_conflict_count: 3,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: Duration::from_nanos(100),
            },
            backoff_time: 200,
            backoff_types: vec!["txnLock".to_owned()],
            slowest_req_total_time: Duration::from_millis(5),
            slowest_region: 10,
            slowest_store_addr: "store1".to_owned(),
            lock_rpc_time: 300,
            lock_rpc_count: 4,
            retry_count: 1,
            ..Default::default()
        };
        let right = LockKeysDetails {
            total_time: Duration::from_millis(20),
            region_num: 3,
            lock_keys: 7,
            aggressive_lock_new_count: 4,
            aggressive_lock_derived_count: 5,
            locked_with_conflict_count: 6,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: Duration::from_nanos(150),
            },
            backoff_time: 250,
            backoff_types: vec!["regionMiss".to_owned()],
            slowest_req_total_time: Duration::from_millis(8),
            slowest_region: 20,
            slowest_store_addr: "store2".to_owned(),
            lock_rpc_time: 350,
            lock_rpc_count: 5,
            retry_count: 2,
            ..Default::default()
        };

        left.merge(&right);

        assert_eq!(left.total_time, Duration::from_millis(30));
        assert_eq!(left.region_num, 5);
        assert_eq!(left.lock_keys, 12);
        assert_eq!(left.aggressive_lock_new_count, 5);
        assert_eq!(left.aggressive_lock_derived_count, 7);
        assert_eq!(left.locked_with_conflict_count, 9);
        assert_eq!(
            left.resolve_lock.resolve_lock_time,
            Duration::from_nanos(250)
        );
        assert_eq!(left.backoff_time, 450);
        assert_eq!(left.lock_rpc_time, 650);
        assert_eq!(left.lock_rpc_count, 9);
        assert_eq!(left.retry_count, 2);
        assert_eq!(
            left.backoff_types,
            vec!["txnLock".to_owned(), "regionMiss".to_owned()]
        );
        assert_eq!(left.slowest_req_total_time, Duration::from_millis(8));
        assert_eq!(left.slowest_region, 20);
        assert_eq!(left.slowest_store_addr, "store2");
    }

    #[test]
    fn test_lock_keys_details_merge_keeps_existing_slowest_request() {
        let mut left = LockKeysDetails {
            slowest_req_total_time: Duration::from_millis(10),
            slowest_region: 1,
            ..Default::default()
        };
        let right = LockKeysDetails {
            slowest_req_total_time: Duration::from_millis(5),
            slowest_region: 2,
            ..Default::default()
        };

        left.merge(&right);

        assert_eq!(left.slowest_req_total_time, Duration::from_millis(10));
        assert_eq!(left.slowest_region, 1);
    }

    #[test]
    fn test_lock_keys_details_merge_req_details_and_clone() {
        let exec_details = sample_exec_details_v2(4, 5);
        let mut details = LockKeysDetails::default();

        details.merge_req_details(Duration::from_millis(5), 10, "store-a", Some(&exec_details));
        details.merge_req_details(Duration::from_millis(3), 20, "store-b", None);

        let cloned = details.clone();

        assert_eq!(cloned.slowest_req_total_time, Duration::from_millis(5));
        assert_eq!(cloned.slowest_region, 10);
        assert_eq!(cloned.slowest_store_addr, "store-a");
        assert_eq!(
            cloned.slowest_exec_details,
            TiKVExecDetails::from_proto(Some(&exec_details))
        );
    }

    #[test]
    fn test_time_detail_prefers_v2_proto_fields() {
        let mut detail = TimeDetail::default();
        detail.merge_from_proto(
            Some(&kvrpcpb::TimeDetailV2 {
                wait_wall_time_ns: 2_000_000,
                process_wall_time_ns: 3_000_000,
                process_suspend_wall_time_ns: 4_000_000,
                kv_read_wall_time_ns: 5_000_000,
                total_rpc_wall_time_ns: 6_000_000,
                ..Default::default()
            }),
            Some(&kvrpcpb::TimeDetail {
                wait_wall_time_ms: 200,
                process_wall_time_ms: 300,
                kv_read_wall_time_ms: 400,
                total_rpc_wall_time_ns: 500_000_000,
            }),
        );

        assert_eq!(detail.wait_time, Duration::from_millis(2));
        assert_eq!(detail.process_time, Duration::from_millis(3));
        assert_eq!(detail.suspend_time, Duration::from_millis(4));
        assert_eq!(detail.kv_read_wall_time, Duration::from_millis(5));
        assert_eq!(detail.total_rpc_wall_time, Duration::from_millis(6));
        assert_eq!(
            detail.to_string(),
            "time_detail: {total_process_time: 3ms, total_suspend_time: 4ms, total_wait_time: 2ms, total_kv_read_wall_time: 5ms, tikv_wall_time: 6ms}"
        );
    }

    #[test]
    fn test_scan_detail_merge_from_proto_and_display() {
        let mut detail = ScanDetail::default();
        detail.merge_from_proto(Some(&kvrpcpb::ScanDetailV2 {
            processed_versions: 7,
            processed_versions_size: 128,
            total_versions: 9,
            rocksdb_delete_skipped_count: 2,
            rocksdb_key_skipped_count: 3,
            rocksdb_block_cache_hit_count: 4,
            rocksdb_block_read_count: 5,
            rocksdb_block_read_byte: 64,
            rocksdb_block_read_nanos: 6_000_000,
            get_snapshot_nanos: 7_000_000,
            read_index_propose_wait_nanos: 0,
            read_index_confirm_wait_nanos: 0,
            read_pool_schedule_wait_nanos: 0,
        }));
        detail.merge(&ScanDetail {
            processed_keys: 1,
            rocksdb_block_read_byte: 32,
            ..Default::default()
        });

        assert_eq!(detail.processed_keys, 8);
        assert_eq!(detail.rocksdb_block_read_byte, 96);
        assert_eq!(
            detail.to_string(),
            "scan_detail: {total_process_keys: 8, total_process_keys_size: 128, total_keys: 9, get_snapshot_time: 7ms, rocksdb: {delete_skipped_count: 2, key_skipped_count: 3, block: {cache_hit_count: 4, read_count: 5, read_byte: 96 Bytes, read_time: 6ms}}}"
        );
    }

    #[test]
    fn test_write_detail_merge_from_proto_and_display() {
        let mut detail = WriteDetail::default();
        detail.merge_from_proto(Some(&kvrpcpb::WriteDetail {
            store_batch_wait_nanos: 1_000_000,
            propose_send_wait_nanos: 2_000_000,
            persist_log_nanos: 3_000_000,
            raft_db_write_leader_wait_nanos: 4_000_000,
            raft_db_sync_log_nanos: 5_000_000,
            raft_db_write_memtable_nanos: 6_000_000,
            commit_log_nanos: 7_000_000,
            apply_batch_wait_nanos: 8_000_000,
            apply_log_nanos: 9_000_000,
            apply_mutex_lock_nanos: 10_000_000,
            apply_write_leader_wait_nanos: 11_000_000,
            apply_write_wal_nanos: 12_000_000,
            apply_write_memtable_nanos: 13_000_000,
            latch_wait_nanos: 14_000_000,
            process_nanos: 15_000_000,
            throttle_nanos: 16_000_000,
            pessimistic_lock_wait_nanos: 17_000_000,
        }));
        detail.merge(&WriteDetail {
            scheduler_process_duration: Duration::from_millis(1),
            ..Default::default()
        });

        assert_eq!(detail.scheduler_process_duration, Duration::from_millis(16));
        assert_eq!(
            detail.to_string(),
            "write_detail: {store_batch_wait: 1ms, propose_send_wait: 2ms, persist_log: {total: 3ms, write_leader_wait: 4ms, sync_log: 5ms, write_memtable: 6ms}, commit_log: 7ms, apply_batch_wait: 8ms, apply: {total: 9ms, mutex_lock: 10ms, write_leader_wait: 11ms, write_wal: 12ms, write_memtable: 13ms}, scheduler: {process: 16ms, latch_wait: 14ms, pessimistic_lock_wait: 17ms, throttle: 16ms}}"
        );
    }

    #[test]
    fn test_tikv_exec_details_from_proto_and_merge() {
        let first = kvrpcpb::ExecDetailsV2 {
            time_detail: Some(kvrpcpb::TimeDetail {
                wait_wall_time_ms: 1,
                process_wall_time_ms: 2,
                kv_read_wall_time_ms: 3,
                total_rpc_wall_time_ns: 4_000_000,
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                processed_versions: 5,
                processed_versions_size: 6,
                total_versions: 7,
                rocksdb_delete_skipped_count: 0,
                rocksdb_key_skipped_count: 0,
                rocksdb_block_cache_hit_count: 0,
                rocksdb_block_read_count: 0,
                rocksdb_block_read_byte: 0,
                rocksdb_block_read_nanos: 0,
                get_snapshot_nanos: 0,
                read_index_propose_wait_nanos: 0,
                read_index_confirm_wait_nanos: 0,
                read_pool_schedule_wait_nanos: 0,
            }),
            write_detail: None,
            time_detail_v2: None,
        };
        let second = kvrpcpb::ExecDetailsV2 {
            time_detail: None,
            scan_detail_v2: None,
            write_detail: Some(kvrpcpb::WriteDetail {
                store_batch_wait_nanos: 1_000_000,
                ..Default::default()
            }),
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                wait_wall_time_ns: 1_000_000,
                process_wall_time_ns: 1_000_000,
                process_suspend_wall_time_ns: 0,
                kv_read_wall_time_ns: 0,
                total_rpc_wall_time_ns: 0,
                ..Default::default()
            }),
        };

        let mut details = TiKVExecDetails::from_proto(Some(&first));
        details.merge(&TiKVExecDetails::from(&second));

        assert_eq!(details.time_detail.wait_time, Duration::from_millis(2));
        assert_eq!(details.time_detail.process_time, Duration::from_millis(3));
        assert_eq!(details.scan_detail.processed_keys, 5);
        assert_eq!(
            details.write_detail.store_batch_wait_duration,
            Duration::from_millis(1)
        );
        assert!(details.to_string().contains("time_detail: {"));
        assert!(details.to_string().contains("scan_detail: {"));
        assert!(details.to_string().contains("write_detail: {"));
    }

    #[test]
    fn test_ru_details_update_merge_and_display() {
        let mut details = RUDetails::new_with(1.5, 2.5, Duration::from_millis(3));
        details.update(
            Some(&resource_manager::Consumption {
                r_r_u: 4.5,
                w_r_u: 5.5,
                ..Default::default()
            }),
            Duration::from_millis(7),
        );

        let clone = details.clone();
        details.merge(&RUDetails::new_with(1.0, 2.0, Duration::from_millis(5)));

        assert_eq!(clone.rru(), 6.0);
        assert_eq!(clone.wru(), 8.0);
        assert_eq!(clone.ru_wait_duration(), Duration::from_millis(10));
        assert_eq!(details.rru(), 7.0);
        assert_eq!(details.wru(), 10.0);
        assert_eq!(details.ru_wait_duration(), Duration::from_millis(15));
        assert_eq!(
            details.to_string(),
            "RRU:7.000000, WRU:10.000000, WaitDuration:15ms"
        );
    }

    #[tokio::test]
    async fn test_exec_details_task_local_scope_restores_outer_value_and_flag() {
        assert!(exec_details().is_none());
        assert!(!trace_exec_details_enabled());

        let outer = Arc::new(ExecDetails::default());
        let inner = Arc::new(ExecDetails::default());

        with_exec_details(outer.clone(), async {
            assert!(Arc::ptr_eq(&exec_details().unwrap(), &outer));
            assert!(!trace_exec_details_enabled());

            with_trace_exec_details(async {
                assert!(trace_exec_details_enabled());
                with_exec_details(inner.clone(), async {
                    assert!(Arc::ptr_eq(&exec_details().unwrap(), &inner));
                    record_task_local_backoff(Duration::from_millis(2));
                })
                .await;
                assert!(trace_exec_details_enabled());
                assert!(Arc::ptr_eq(&exec_details().unwrap(), &outer));
            })
            .await;

            assert!(!trace_exec_details_enabled());
            assert!(Arc::ptr_eq(&exec_details().unwrap(), &outer));
        })
        .await;

        assert!(exec_details().is_none());
        assert!(!trace_exec_details_enabled());
        assert_eq!(inner.backoff_count(), 1);
        assert_eq!(inner.backoff_duration(), Duration::from_millis(2));
        assert_eq!(outer.backoff_count(), 0);
    }

    #[tokio::test]
    async fn test_scope_task_exec_details_inherits_both_values() {
        let details = Arc::new(ExecDetails::default());

        scope_task_exec_details(Some(details.clone()), true, async {
            assert!(Arc::ptr_eq(&exec_details().unwrap(), &details));
            assert!(trace_exec_details_enabled());
            record_task_local_wait_kv_response(Duration::from_millis(3));
            record_task_local_wait_pd_response(Duration::from_millis(5));
        })
        .await;

        assert_eq!(details.wait_kv_resp_duration(), Duration::from_millis(3));
        assert_eq!(details.wait_pd_resp_duration(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn test_record_task_local_kv_traffic_respects_task_local_traffic_kind() {
        let details = Arc::new(ExecDetails::default());

        with_exec_details(details.clone(), async {
            // Default scope records KV totals only.
            record_task_local_kv_traffic(10, 20);

            // Cross-zone KV.
            scope_task_traffic_kind(false, true, async {
                record_task_local_kv_traffic(1, 2);
            })
            .await;

            // Local MPP.
            scope_task_traffic_kind(true, false, async {
                record_task_local_kv_traffic(3, 4);
            })
            .await;

            // Cross-zone MPP.
            scope_task_traffic_kind(true, true, async {
                record_task_local_kv_traffic(5, 6);
            })
            .await;
        })
        .await;

        assert_eq!(details.traffic_details().unpacked_bytes_sent_kv_total(), 11);
        assert_eq!(
            details.traffic_details().unpacked_bytes_received_kv_total(),
            22
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_sent_kv_cross_zone(),
            1
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_received_kv_cross_zone(),
            2
        );
        assert_eq!(details.traffic_details().unpacked_bytes_sent_mpp_total(), 8);
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_received_mpp_total(),
            10
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_sent_mpp_cross_zone(),
            5
        );
        assert_eq!(
            details
                .traffic_details()
                .unpacked_bytes_received_mpp_cross_zone(),
            6
        );
    }

    #[test]
    fn test_exec_details_merge_and_traffic_details_merge() {
        let left = ExecDetails::default();
        left.add_backoff(Duration::from_millis(2));
        left.add_wait_kv_response(Duration::from_millis(3));
        left.traffic_details().add_kv_bytes(10, 20, 1, 2);

        let right = ExecDetails::default();
        right.add_backoff(Duration::from_millis(4));
        right.add_wait_pd_response(Duration::from_millis(5));
        right.traffic_details().add_mpp_bytes(30, 40, 3, 4);

        left.merge(&right);

        assert_eq!(left.backoff_count(), 2);
        assert_eq!(left.backoff_duration(), Duration::from_millis(6));
        assert_eq!(left.wait_kv_resp_duration(), Duration::from_millis(3));
        assert_eq!(left.wait_pd_resp_duration(), Duration::from_millis(5));
        assert_eq!(left.traffic_details().unpacked_bytes_sent_kv_total(), 10);
        assert_eq!(
            left.traffic_details().unpacked_bytes_received_kv_total(),
            20
        );
        assert_eq!(left.traffic_details().unpacked_bytes_sent_mpp_total(), 30);
        assert_eq!(
            left.traffic_details().unpacked_bytes_received_mpp_total(),
            40
        );
    }
}
