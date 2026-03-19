// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::time::Duration;

use crate::proto::kvrpcpb;
use crate::proto::resource_manager;
use crate::util::format_bytes;

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

fn push_duration_part(parts: &mut Vec<String>, name: &str, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    parts.push(format!("{name}: {}", format_duration(duration)));
}

fn format_duration(duration: Duration) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
