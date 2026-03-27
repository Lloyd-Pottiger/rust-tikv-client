use std::sync::Arc;
use std::time::Duration;

use tikv_client::{
    BackoffRuntimeStats, CmdType, FnRpcInterceptor, ReplicaReadType, RpcRuntimeStats,
    SnapshotCacheEntry, SnapshotRuntimeStats, SnapshotScanDetail, SnapshotTimeDetail, StoreLabel,
    SyncSnapshot, Timestamp, TimestampExt, TraceControlFlags,
};

#[test]
fn transaction_runtime_stats_public_api_exposes_stats_types_and_defaults() {
    let mut rpc = RpcRuntimeStats::default();
    rpc.count = 2;
    rpc.total_time = Duration::from_millis(5);
    assert_eq!(rpc.count, 2);
    assert_eq!(rpc.total_time, Duration::from_millis(5));

    let mut backoff = BackoffRuntimeStats::default();
    backoff.count = 3;
    backoff.total_time = Duration::from_millis(7);
    assert_eq!(backoff.count, 3);
    assert_eq!(backoff.total_time, Duration::from_millis(7));

    let mut time_detail = SnapshotTimeDetail::default();
    time_detail.total_process_time = Duration::from_millis(11);
    time_detail.total_wait_time = Duration::from_millis(13);
    assert_eq!(time_detail.total_process_time, Duration::from_millis(11));
    assert_eq!(time_detail.total_wait_time, Duration::from_millis(13));

    let mut scan_detail = SnapshotScanDetail::default();
    scan_detail.total_process_keys = 17;
    scan_detail.total_process_keys_size = 19;
    scan_detail.total_keys = 23;
    scan_detail.get_snapshot_time = Duration::from_millis(29);
    scan_detail.rocksdb_delete_skipped_count = 31;
    scan_detail.rocksdb_key_skipped_count = 37;
    scan_detail.rocksdb_block_cache_hit_count = 41;
    scan_detail.rocksdb_block_read_count = 43;
    scan_detail.rocksdb_block_read_byte = 47;
    assert_eq!(scan_detail.total_process_keys, 17);
    assert_eq!(scan_detail.total_process_keys_size, 19);
    assert_eq!(scan_detail.total_keys, 23);
    assert_eq!(scan_detail.get_snapshot_time, Duration::from_millis(29));
    assert_eq!(scan_detail.rocksdb_delete_skipped_count, 31);
    assert_eq!(scan_detail.rocksdb_key_skipped_count, 37);
    assert_eq!(scan_detail.rocksdb_block_cache_hit_count, 41);
    assert_eq!(scan_detail.rocksdb_block_read_count, 43);
    assert_eq!(scan_detail.rocksdb_block_read_byte, 47);

    let stats = SnapshotRuntimeStats::default();
    assert!(stats.rpc_stats_map().is_empty());
    assert!(stats.backoff_stats_map().is_empty());
    assert_eq!(stats.time_detail(), SnapshotTimeDetail::default());
    assert_eq!(stats.scan_detail(), SnapshotScanDetail::default());
    assert_eq!(stats.rpc_stats("kv_get"), None);
    assert_eq!(stats.cmd_rpc_count(CmdType::Get), 0);
    assert_eq!(stats.backoff_stats("region_miss"), None);
    stats.reset();
}

#[test]
fn transaction_runtime_stats_public_api_exposes_snapshot_cache_entry_and_timestamp_ext() {
    let entry = SnapshotCacheEntry {
        value: Some(b"value".to_vec()),
        commit_ts: 42,
    };
    assert_eq!(entry.value.as_deref(), Some(b"value".as_slice()));
    assert_eq!(entry.commit_ts, 42);

    let ts = <Timestamp as TimestampExt>::from_version(123);
    assert_eq!(ts.version(), 123);

    assert!(<Timestamp as TimestampExt>::try_from_version(0).is_none());
    assert_eq!(
        <Timestamp as TimestampExt>::try_from_version(456)
            .expect("non-zero version should produce timestamp")
            .version(),
        456
    );
}

#[test]
fn transaction_runtime_stats_public_api_exposes_sync_snapshot_method_surface() {
    let _: Option<SyncSnapshot> = None;

    let _ = |snapshot: &mut SyncSnapshot, key: Vec<u8>| snapshot.get(key);
    let _ = |snapshot: &mut SyncSnapshot, key: Vec<u8>| snapshot.get_with_commit_ts(key);
    let _ = |snapshot: &mut SyncSnapshot, key: Vec<u8>| snapshot.key_exists(key);
    let _ = |snapshot: &mut SyncSnapshot, keys: Vec<Vec<u8>>| snapshot.batch_get(keys);
    let _ =
        |snapshot: &mut SyncSnapshot, keys: Vec<Vec<u8>>| snapshot.batch_get_with_commit_ts(keys);
    let _ = |snapshot: &mut SyncSnapshot, range: std::ops::Range<Vec<u8>>, limit: u32| {
        snapshot.scan(range, limit)
    };
    let _ = |snapshot: &mut SyncSnapshot, range: std::ops::Range<Vec<u8>>, limit: u32| {
        snapshot.scan_keys(range, limit)
    };
    let _ = |snapshot: &mut SyncSnapshot, range: std::ops::Range<Vec<u8>>, limit: u32| {
        snapshot.scan_reverse(range, limit)
    };
    let _ = |snapshot: &mut SyncSnapshot, range: std::ops::Range<Vec<u8>>, limit: u32| {
        snapshot.scan_keys_reverse(range, limit)
    };
    let _ = |snapshot: &mut SyncSnapshot, start_key: Vec<u8>, upper_bound: Vec<u8>| {
        let _ = snapshot.iter(start_key, upper_bound);
    };
    let _ = |snapshot: &mut SyncSnapshot, start_key: Vec<u8>, lower_bound: Vec<u8>| {
        let _ = snapshot.iter_reverse(start_key, lower_bound);
    };

    let _ = SyncSnapshot::vars;
    let _ = SyncSnapshot::resolve_lock_detail;
    let _ = |snapshot: &mut SyncSnapshot, stats: Option<Arc<SnapshotRuntimeStats>>| {
        snapshot.set_runtime_stats(stats)
    };
    let _ = SyncSnapshot::runtime_stats;
    let _ = SyncSnapshot::txn_scope;
    let _ = SyncSnapshot::is_internal;
    let _ = SyncSnapshot::snap_cache_hit_count;
    let _ = SyncSnapshot::snap_cache_size;
    let _ = SyncSnapshot::snap_cache;
    let _ = SyncSnapshot::key_only;
    let _ = SyncSnapshot::kv_read_timeout;
    let _ = SyncSnapshot::scan_batch_size;

    let _ = |snapshot: &mut SyncSnapshot, vars: tikv_client::Variables| snapshot.set_vars(vars);
    let _ = |snapshot: &mut SyncSnapshot, stats: Timestamp| snapshot.set_snapshot_ts(stats);
    let _ = SyncSnapshot::set_pipelined;
    let _ = |snapshot: &mut SyncSnapshot, scope: String| snapshot.set_txn_scope(scope);
    let _ = |snapshot: &mut SyncSnapshot, scope: String| snapshot.set_read_replica_scope(scope);
    let _ = |snapshot: &mut SyncSnapshot, entries: Vec<(Vec<u8>, SnapshotCacheEntry)>| {
        snapshot.update_snapshot_cache(entries)
    };
    let _ = |snapshot: &mut SyncSnapshot, keys: Vec<Vec<u8>>| snapshot.clean_cache(keys);
    let _ = |snapshot: &mut SyncSnapshot, interceptor: FnRpcInterceptor| {
        snapshot.set_rpc_interceptor(interceptor)
    };
    let _ = |snapshot: &mut SyncSnapshot, interceptor: FnRpcInterceptor| {
        snapshot.add_rpc_interceptor(interceptor)
    };
    let _ = SyncSnapshot::clear_rpc_interceptors;
    let _ = SyncSnapshot::set_replica_read;
    let _ = |snapshot: &mut SyncSnapshot, adjuster: fn(usize) -> ReplicaReadType| {
        snapshot.set_replica_read_adjuster(adjuster)
    };
    let _ = SyncSnapshot::set_load_based_replica_read_threshold;
    let _ = |snapshot: &mut SyncSnapshot, labels: Vec<StoreLabel>| {
        snapshot.set_match_store_labels(labels)
    };
    let _ =
        |snapshot: &mut SyncSnapshot, store_ids: Vec<u64>| snapshot.set_match_store_ids(store_ids);
    let _ = SyncSnapshot::set_stale_read;
    let _ = SyncSnapshot::set_not_fill_cache;
    let _ = SyncSnapshot::set_key_only;
    let _ = SyncSnapshot::set_task_id;
    let _ = SyncSnapshot::set_max_execution_duration;
    let _ = SyncSnapshot::set_kv_read_timeout;
    let _ = SyncSnapshot::set_priority;
    let _ = SyncSnapshot::set_isolation_level;
    let _ = SyncSnapshot::set_sample_step;
    let _ = SyncSnapshot::set_scan_batch_size;
    let _ = SyncSnapshot::set_resource_group_tag;
    let _ = |snapshot: &mut SyncSnapshot, tagger: fn(&str) -> Vec<u8>| {
        snapshot.set_resource_group_tagger(tagger)
    };
    let _ = SyncSnapshot::clear_resource_group_tagger;
    let _ = |snapshot: &mut SyncSnapshot, name: String| snapshot.set_resource_group_name(name);
    let _ = |snapshot: &mut SyncSnapshot, source: String| snapshot.set_request_source(source);
    let _ = SyncSnapshot::set_trace_id;
    let _ = SyncSnapshot::clear_trace_id;
    let _ = SyncSnapshot::set_trace_control_flags;

    let _ = ReplicaReadType::PreferLeader;
    let _ = TraceControlFlags::default();
}
