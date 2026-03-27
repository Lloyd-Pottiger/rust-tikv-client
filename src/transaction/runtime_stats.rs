// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::proto::kvrpcpb;

/// Runtime stats collected by a [`Snapshot`](crate::Snapshot).
///
/// This mirrors client-go `txnkv.SnapshotRuntimeStats` and can be attached to a snapshot via
/// [`Snapshot::set_runtime_stats`](crate::Snapshot::set_runtime_stats).
///
/// When attached, the client records:
/// - per-RPC counts and total wall time (client-side),
/// - backoff counts and total backoff sleep time (client-side),
/// - exec details (server-side `ExecDetailsV2`) for supported read RPCs.
#[derive(Debug, Default)]
pub struct SnapshotRuntimeStats {
    inner: Mutex<SnapshotRuntimeStatsInner>,
}

#[derive(Debug, Default)]
struct SnapshotRuntimeStatsInner {
    rpc: BTreeMap<&'static str, RpcRuntimeStats>,
    backoff: BTreeMap<&'static str, BackoffRuntimeStats>,
    time_detail: SnapshotTimeDetail,
    scan_detail: SnapshotScanDetail,
}

/// Aggregated per-RPC runtime stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RpcRuntimeStats {
    /// Total number of RPC calls observed.
    pub count: u64,
    /// Sum of observed RPC wall times.
    pub total_time: Duration,
}

/// Aggregated backoff runtime stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BackoffRuntimeStats {
    /// Total number of backoff sleeps observed.
    pub count: u64,
    /// Sum of observed backoff sleep durations.
    pub total_time: Duration,
}

/// Aggregated server-side time details.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotTimeDetail {
    /// Time spent processing requests on the TiKV side.
    pub total_process_time: Duration,
    /// Time spent waiting (queueing, etc.) on the TiKV side.
    pub total_wait_time: Duration,
}

/// Aggregated server-side scan details.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SnapshotScanDetail {
    /// Total number of MVCC versions TiKV reports as processed.
    pub total_process_keys: u64,
    /// Total size, in bytes, of processed MVCC versions.
    pub total_process_keys_size: u64,
    /// Total number of keys TiKV reports scanning.
    pub total_keys: u64,
    /// Time spent obtaining RocksDB snapshots on the TiKV side.
    pub get_snapshot_time: Duration,

    /// Number of delete tombstones skipped during RocksDB iteration.
    pub rocksdb_delete_skipped_count: u64,
    /// Number of internal RocksDB keys skipped during iteration.
    pub rocksdb_key_skipped_count: u64,
    /// Number of RocksDB block-cache hits reported by TiKV.
    pub rocksdb_block_cache_hit_count: u64,
    /// Number of RocksDB block reads reported by TiKV.
    pub rocksdb_block_read_count: u64,
    /// Total bytes read from RocksDB blocks.
    pub rocksdb_block_read_byte: u64,
}

impl SnapshotRuntimeStats {
    pub(crate) fn record_rpc(&self, label: &'static str, duration: Duration) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let stats = inner.rpc.entry(label).or_default();
        stats.count = stats.count.saturating_add(1);
        stats.total_time = stats.total_time.saturating_add(duration);
    }

    pub(crate) fn record_backoff(&self, label: &'static str, duration: Duration) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let stats = inner.backoff.entry(label).or_default();
        stats.count = stats.count.saturating_add(1);
        stats.total_time = stats.total_time.saturating_add(duration);
    }

    pub(crate) fn merge_exec_details_v2(&self, details: &kvrpcpb::ExecDetailsV2) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(time_v2) = details.time_detail_v2.as_ref() {
            inner.time_detail.total_process_time = inner
                .time_detail
                .total_process_time
                .saturating_add(Duration::from_nanos(time_v2.process_wall_time_ns));
            inner.time_detail.total_wait_time = inner
                .time_detail
                .total_wait_time
                .saturating_add(Duration::from_nanos(time_v2.wait_wall_time_ns));
        } else if let Some(time_v1) = details.time_detail.as_ref() {
            inner.time_detail.total_process_time = inner
                .time_detail
                .total_process_time
                .saturating_add(Duration::from_millis(time_v1.process_wall_time_ms));
            inner.time_detail.total_wait_time = inner
                .time_detail
                .total_wait_time
                .saturating_add(Duration::from_millis(time_v1.wait_wall_time_ms));
        }

        if let Some(scan) = details.scan_detail_v2.as_ref() {
            inner.scan_detail.total_process_keys = inner
                .scan_detail
                .total_process_keys
                .saturating_add(scan.processed_versions);
            inner.scan_detail.total_process_keys_size = inner
                .scan_detail
                .total_process_keys_size
                .saturating_add(scan.processed_versions_size);
            inner.scan_detail.total_keys = inner
                .scan_detail
                .total_keys
                .saturating_add(scan.total_versions);
            inner.scan_detail.get_snapshot_time = inner
                .scan_detail
                .get_snapshot_time
                .saturating_add(Duration::from_nanos(scan.get_snapshot_nanos));

            inner.scan_detail.rocksdb_delete_skipped_count = inner
                .scan_detail
                .rocksdb_delete_skipped_count
                .saturating_add(scan.rocksdb_delete_skipped_count);
            inner.scan_detail.rocksdb_key_skipped_count = inner
                .scan_detail
                .rocksdb_key_skipped_count
                .saturating_add(scan.rocksdb_key_skipped_count);
            inner.scan_detail.rocksdb_block_cache_hit_count = inner
                .scan_detail
                .rocksdb_block_cache_hit_count
                .saturating_add(scan.rocksdb_block_cache_hit_count);
            inner.scan_detail.rocksdb_block_read_count = inner
                .scan_detail
                .rocksdb_block_read_count
                .saturating_add(scan.rocksdb_block_read_count);
            inner.scan_detail.rocksdb_block_read_byte = inner
                .scan_detail
                .rocksdb_block_read_byte
                .saturating_add(scan.rocksdb_block_read_byte);
        }
    }

    /// Get a snapshot of all per-RPC runtime stats observed so far.
    ///
    /// The map is keyed by request labels (e.g. `"kv_get"`).
    #[must_use]
    pub fn rpc_stats_map(&self) -> BTreeMap<&'static str, RpcRuntimeStats> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.rpc.clone()
    }

    /// Get a snapshot of all backoff runtime stats observed so far.
    ///
    /// The map is keyed by backoff labels (e.g. `"grpc"`).
    #[must_use]
    pub fn backoff_stats_map(&self) -> BTreeMap<&'static str, BackoffRuntimeStats> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.backoff.clone()
    }

    /// Clear all collected stats.
    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *inner = SnapshotRuntimeStatsInner::default();
    }

    /// Get the aggregated time details observed so far.
    #[must_use]
    pub fn time_detail(&self) -> SnapshotTimeDetail {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.time_detail
    }

    /// Get the aggregated scan details observed so far.
    #[must_use]
    pub fn scan_detail(&self) -> SnapshotScanDetail {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.scan_detail
    }

    /// Get runtime stats for a single RPC label.
    #[must_use]
    pub fn rpc_stats(&self, label: &str) -> Option<RpcRuntimeStats> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.rpc.get(label).copied()
    }

    /// Get the aggregated RPC count for a single command type.
    ///
    /// This mirrors client-go `SnapshotRuntimeStats::GetCmdRPCCount`, while keeping the Rust
    /// implementation label-based internally.
    #[must_use]
    pub fn cmd_rpc_count(&self, cmd_type: crate::CmdType) -> u64 {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .rpc
            .iter()
            .filter(|(label, _)| crate::CmdType::from_label(label) == cmd_type)
            .map(|(_, stats)| stats.count)
            .sum()
    }

    /// Get runtime stats for a single backoff label.
    #[must_use]
    pub fn backoff_stats(&self, label: &str) -> Option<BackoffRuntimeStats> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.backoff.get(label).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_runtime_stats_reports_rpc_count_by_cmd_type() {
        let stats = SnapshotRuntimeStats::default();
        stats.record_rpc("kv_get", Duration::from_millis(1));
        stats.record_rpc("kv_get", Duration::from_millis(2));
        stats.record_rpc("raw_get", Duration::from_millis(3));

        assert_eq!(stats.cmd_rpc_count(crate::CmdType::Get), 2);
        assert_eq!(stats.cmd_rpc_count(crate::CmdType::RawGet), 1);
        assert_eq!(stats.cmd_rpc_count(crate::CmdType::Commit), 0);
        assert_eq!(stats.cmd_rpc_count(crate::CmdType::Unknown), 0);
    }

    #[test]
    fn test_snapshot_runtime_stats_reset_clears_all_stats() {
        let stats = SnapshotRuntimeStats::default();
        stats.record_rpc("kv_get", Duration::from_millis(1));
        stats.record_backoff("grpc", Duration::from_millis(2));

        assert_eq!(stats.rpc_stats("kv_get").unwrap().count, 1);
        assert_eq!(stats.backoff_stats("grpc").unwrap().count, 1);
        assert_eq!(stats.rpc_stats_map().len(), 1);
        assert_eq!(stats.backoff_stats_map().len(), 1);

        stats.reset();

        assert!(stats.rpc_stats("kv_get").is_none());
        assert!(stats.backoff_stats("grpc").is_none());
        assert!(stats.rpc_stats_map().is_empty());
        assert!(stats.backoff_stats_map().is_empty());
        assert_eq!(stats.time_detail(), SnapshotTimeDetail::default());
        assert_eq!(stats.scan_detail(), SnapshotScanDetail::default());
    }
}
