use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tikv_client::{
    GcOptions, ProtoCompactResponse, ProtoLockInfo, ProtoTiFlashSystemTableResponse,
    ProtoWaitForEntry, Timestamp, TransactionClient, Variables, MAX_TXN_TIME_USE,
};

async fn scan_locks_entry(
    client: &TransactionClient,
    safepoint: &Timestamp,
    range: std::ops::Range<Vec<u8>>,
    batch_size: u32,
) -> tikv_client::Result<Vec<ProtoLockInfo>> {
    client.scan_locks(safepoint, range, batch_size).await
}

async fn compact_entry(
    client: &TransactionClient,
    physical_table_id: i64,
    logical_table_id: i64,
    start_key: Vec<u8>,
) -> tikv_client::Result<Vec<ProtoCompactResponse>> {
    client
        .compact(physical_table_id, logical_table_id, start_key)
        .await
}

async fn tiflash_system_table_entry(
    client: &TransactionClient,
    sql: String,
) -> tikv_client::Result<Vec<ProtoTiFlashSystemTableResponse>> {
    client.tiflash_system_table(sql).await
}

async fn check_lock_observer_entry(
    client: &TransactionClient,
    max_ts: u64,
) -> tikv_client::Result<(bool, Vec<ProtoLockInfo>)> {
    client.check_lock_observer(max_ts).await
}

async fn physical_scan_lock_entry(
    client: &TransactionClient,
    max_ts: u64,
    start_key: Vec<u8>,
    limit: u32,
) -> tikv_client::Result<Vec<ProtoLockInfo>> {
    client.physical_scan_lock(max_ts, start_key, limit).await
}

async fn lock_wait_info_entry(
    client: &TransactionClient,
) -> tikv_client::Result<Vec<ProtoWaitForEntry>> {
    client.lock_wait_info().await
}

async fn lock_wait_history_entry(
    client: &TransactionClient,
) -> tikv_client::Result<Vec<ProtoWaitForEntry>> {
    client.lock_wait_history().await
}

#[test]
fn crate_root_exports_transaction_proto_aliases_and_store_level_helpers() {
    let _: Option<ProtoLockInfo> = None;
    let _: Option<ProtoCompactResponse> = None;
    let _: Option<ProtoTiFlashSystemTableResponse> = None;
    let _: Option<ProtoWaitForEntry> = None;

    let _ = ProtoLockInfo::default();
    let _ = ProtoCompactResponse::default();
    let _ = ProtoTiFlashSystemTableResponse::default();
    let _ = ProtoWaitForEntry::default();

    let _ = scan_locks_entry;
    let _ = compact_entry;
    let _ = tiflash_system_table_entry;
    let _ = check_lock_observer_entry;
    let _ = physical_scan_lock_entry;
    let _ = lock_wait_info_entry;
    let _ = lock_wait_history_entry;
}

#[test]
fn crate_root_exports_transaction_vars_alias_and_constants() {
    assert_eq!(MAX_TXN_TIME_USE, 24 * 60 * 60 * 1000);

    let gc_options = GcOptions::new().with_concurrency(4);
    assert_eq!(gc_options.concurrency, 4);
    assert_eq!(GcOptions::default().concurrency, 8);

    let vars = Variables::default();
    assert_eq!(vars.backoff_lock_fast_ms, 10);
    assert_eq!(vars.backoff_weight, 2);
    assert!(vars.killed.is_none());

    let killed = Arc::new(AtomicU32::new(7));
    let vars = Variables::with_killed_flag(killed.clone());
    assert_eq!(vars.backoff_lock_fast_ms, 10);
    assert_eq!(vars.backoff_weight, 2);
    assert_eq!(
        vars.killed
            .as_ref()
            .expect("with_killed_flag should keep the signal")
            .load(Ordering::SeqCst),
        7
    );

    let vars = Variables::new(Some(killed.clone()));
    assert_eq!(
        vars.killed
            .as_ref()
            .expect("new(Some(_)) should keep the signal")
            .load(Ordering::SeqCst),
        7
    );
}
