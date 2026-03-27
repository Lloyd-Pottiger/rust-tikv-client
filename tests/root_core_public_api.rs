use std::convert::TryFrom;
use std::time::Duration;

use tikv_client::{
    BoundRange, ColumnFamily, IntoOwnedRange, Key, KvPair, PdRpcClient, RawChecksum, RawClient,
    Snapshot, SyncSnapshot, SyncTransaction, SyncTransactionClient, Timestamp, TimestampExt,
    Transaction, TransactionClient, TransactionOptions, Value,
};

async fn raw_get_entry(client: &RawClient, key: Vec<u8>) -> tikv_client::Result<Option<Value>> {
    client.get(key).await
}

async fn raw_scan_entry(
    client: &RawClient,
    range: std::ops::Range<Vec<u8>>,
    limit: u32,
) -> tikv_client::Result<Vec<KvPair>> {
    client.scan(range, limit).await
}

async fn raw_checksum_entry(
    client: &RawClient,
    range: std::ops::Range<Vec<u8>>,
) -> tikv_client::Result<RawChecksum> {
    client.checksum(range).await
}

async fn transaction_get_entry(
    txn: &mut Transaction,
    key: Vec<u8>,
) -> tikv_client::Result<Option<Value>> {
    txn.get(key).await
}

async fn transaction_put_entry(
    txn: &mut Transaction,
    key: Vec<u8>,
    value: Vec<u8>,
) -> tikv_client::Result<()> {
    txn.put(key, value).await
}

async fn snapshot_get_entry(
    snapshot: &mut Snapshot,
    key: Vec<u8>,
) -> tikv_client::Result<Option<Value>> {
    snapshot.get(key).await
}

async fn snapshot_batch_get_entry(
    snapshot: &mut Snapshot,
    keys: Vec<Vec<u8>>,
) -> tikv_client::Result<impl Iterator<Item = KvPair>> {
    snapshot.batch_get(keys).await
}

async fn transaction_current_timestamp_entry(
    client: &TransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.current_timestamp().await
}

async fn transaction_current_timestamp_with_txn_scope_entry(
    client: &TransactionClient,
    txn_scope: &str,
) -> tikv_client::Result<Timestamp> {
    client.current_timestamp_with_txn_scope(txn_scope).await
}

async fn transaction_current_all_tso_keyspace_group_min_ts_entry(
    client: &TransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.current_all_tso_keyspace_group_min_ts().await
}

async fn transaction_external_timestamp_entry(
    client: &TransactionClient,
) -> tikv_client::Result<u64> {
    client.external_timestamp().await
}

async fn transaction_set_external_timestamp_entry(
    client: &TransactionClient,
    timestamp: u64,
) -> tikv_client::Result<()> {
    client.set_external_timestamp(timestamp).await
}

async fn transaction_validate_read_ts_entry(
    client: &TransactionClient,
    read_ts: u64,
    is_stale_read: bool,
) -> tikv_client::Result<()> {
    client.validate_read_ts(read_ts, is_stale_read).await
}

async fn transaction_validate_read_ts_with_txn_scope_entry(
    client: &TransactionClient,
    txn_scope: &str,
    read_ts: u64,
    is_stale_read: bool,
) -> tikv_client::Result<()> {
    client
        .validate_read_ts_with_txn_scope(txn_scope, read_ts, is_stale_read)
        .await
}

fn transaction_set_low_resolution_timestamp_update_interval_entry(
    client: &TransactionClient,
    update_interval: Duration,
) -> tikv_client::Result<()> {
    client.set_low_resolution_timestamp_update_interval(update_interval)
}

async fn transaction_low_resolution_timestamp_entry(
    client: &TransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.low_resolution_timestamp().await
}

async fn transaction_low_resolution_timestamp_with_txn_scope_entry(
    client: &TransactionClient,
    txn_scope: &str,
) -> tikv_client::Result<Timestamp> {
    client
        .low_resolution_timestamp_with_txn_scope(txn_scope)
        .await
}

async fn transaction_stale_timestamp_entry(
    client: &TransactionClient,
    prev_seconds: u64,
) -> tikv_client::Result<Timestamp> {
    client.stale_timestamp(prev_seconds).await
}

async fn transaction_stale_timestamp_with_txn_scope_entry(
    client: &TransactionClient,
    txn_scope: &str,
    prev_seconds: u64,
) -> tikv_client::Result<Timestamp> {
    client
        .stale_timestamp_with_txn_scope(txn_scope, prev_seconds)
        .await
}

fn sync_transaction_current_timestamp_entry(
    client: &SyncTransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.current_timestamp()
}

fn sync_transaction_current_timestamp_with_txn_scope_entry(
    client: &SyncTransactionClient,
    txn_scope: &str,
) -> tikv_client::Result<Timestamp> {
    client.current_timestamp_with_txn_scope(txn_scope)
}

fn sync_transaction_current_all_tso_keyspace_group_min_ts_entry(
    client: &SyncTransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.current_all_tso_keyspace_group_min_ts()
}

fn sync_transaction_external_timestamp_entry(
    client: &SyncTransactionClient,
) -> tikv_client::Result<u64> {
    client.external_timestamp()
}

fn sync_transaction_set_external_timestamp_entry(
    client: &SyncTransactionClient,
    timestamp: u64,
) -> tikv_client::Result<()> {
    client.set_external_timestamp(timestamp)
}

fn sync_transaction_validate_read_ts_entry(
    client: &SyncTransactionClient,
    read_ts: u64,
    is_stale_read: bool,
) -> tikv_client::Result<()> {
    client.validate_read_ts(read_ts, is_stale_read)
}

fn sync_transaction_validate_read_ts_with_txn_scope_entry(
    client: &SyncTransactionClient,
    txn_scope: &str,
    read_ts: u64,
    is_stale_read: bool,
) -> tikv_client::Result<()> {
    client.validate_read_ts_with_txn_scope(txn_scope, read_ts, is_stale_read)
}

fn sync_transaction_set_low_resolution_timestamp_update_interval_entry(
    client: &SyncTransactionClient,
    update_interval: Duration,
) -> tikv_client::Result<()> {
    client.set_low_resolution_timestamp_update_interval(update_interval)
}

fn sync_transaction_low_resolution_timestamp_entry(
    client: &SyncTransactionClient,
) -> tikv_client::Result<Timestamp> {
    client.low_resolution_timestamp()
}

fn sync_transaction_low_resolution_timestamp_with_txn_scope_entry(
    client: &SyncTransactionClient,
    txn_scope: &str,
) -> tikv_client::Result<Timestamp> {
    client.low_resolution_timestamp_with_txn_scope(txn_scope)
}

fn sync_transaction_stale_timestamp_entry(
    client: &SyncTransactionClient,
    prev_seconds: u64,
) -> tikv_client::Result<Timestamp> {
    client.stale_timestamp(prev_seconds)
}

fn sync_transaction_stale_timestamp_with_txn_scope_entry(
    client: &SyncTransactionClient,
    txn_scope: &str,
    prev_seconds: u64,
) -> tikv_client::Result<Timestamp> {
    client.stale_timestamp_with_txn_scope(txn_scope, prev_seconds)
}

#[test]
fn crate_root_exports_core_client_constructor_and_method_surface() {
    let _ = RawClient::<PdRpcClient>::new::<String>;
    let _ = RawClient::<PdRpcClient>::new_with_config::<String>;
    let _: fn(&RawClient, ColumnFamily) -> RawClient = RawClient::with_cf;
    let _ = raw_get_entry;
    let _ = raw_scan_entry;
    let _ = raw_checksum_entry;

    let _ = TransactionClient::<PdRpcClient>::new::<String>;
    let _ = TransactionClient::<PdRpcClient>::new_with_config::<String>;
    let _ = TransactionClient::<PdRpcClient>::new_with_config_api_v2_no_prefix::<String>;
    let _ = TransactionClient::<PdRpcClient>::begin_optimistic;
    let _ = TransactionClient::<PdRpcClient>::begin_pessimistic;
    let _ = TransactionClient::<PdRpcClient>::begin_with_options;
    let _ = TransactionClient::<PdRpcClient>::begin_with_start_timestamp;
    let _ = TransactionClient::<PdRpcClient>::snapshot;

    let _ = SyncTransactionClient::new::<String>;
    let _ = SyncTransactionClient::new_with_config::<String>;
    let _ = SyncTransactionClient::new_with_config_api_v2_no_prefix::<String>;
    let _ = SyncTransactionClient::begin_optimistic;
    let _ = SyncTransactionClient::begin_pessimistic;
    let _ = SyncTransactionClient::begin_with_options;
    let _ = SyncTransactionClient::begin_with_start_timestamp;
    let _ = SyncTransactionClient::snapshot;
}

#[test]
fn crate_root_exports_transaction_and_snapshot_aliases() {
    let _ = TransactionOptions::new_optimistic;
    let _ = TransactionOptions::new_pessimistic;

    let _ = transaction_get_entry;
    let _ = transaction_put_entry;
    let _ = snapshot_get_entry;
    let _ = snapshot_batch_get_entry;

    let _ = |txn: &mut SyncTransaction, key: Vec<u8>| txn.get(key);
    let _ = |txn: &mut SyncTransaction, key: Vec<u8>, value: Vec<u8>| txn.put(key, value);
    let _ = |snapshot: &mut SyncSnapshot, key: Vec<u8>| snapshot.get(key);
    let _ = |snapshot: &mut SyncSnapshot, keys: Vec<Vec<u8>>| snapshot.batch_get(keys);
}

#[test]
fn crate_root_exports_transaction_client_timestamp_helpers() {
    let _ = transaction_current_timestamp_entry;
    let _ = transaction_current_timestamp_with_txn_scope_entry;
    let _ = transaction_current_all_tso_keyspace_group_min_ts_entry;
    let _ = transaction_external_timestamp_entry;
    let _ = transaction_set_external_timestamp_entry;
    let _ = transaction_validate_read_ts_entry;
    let _ = transaction_validate_read_ts_with_txn_scope_entry;
    let _ = transaction_set_low_resolution_timestamp_update_interval_entry;
    let _ = transaction_low_resolution_timestamp_entry;
    let _ = transaction_low_resolution_timestamp_with_txn_scope_entry;
    let _ = transaction_stale_timestamp_entry;
    let _ = transaction_stale_timestamp_with_txn_scope_entry;

    let _ = sync_transaction_current_timestamp_entry;
    let _ = sync_transaction_current_timestamp_with_txn_scope_entry;
    let _ = sync_transaction_current_all_tso_keyspace_group_min_ts_entry;
    let _ = sync_transaction_external_timestamp_entry;
    let _ = sync_transaction_set_external_timestamp_entry;
    let _ = sync_transaction_validate_read_ts_entry;
    let _ = sync_transaction_validate_read_ts_with_txn_scope_entry;
    let _ = sync_transaction_set_low_resolution_timestamp_update_interval_entry;
    let _ = sync_transaction_low_resolution_timestamp_entry;
    let _ = sync_transaction_low_resolution_timestamp_with_txn_scope_entry;
    let _ = sync_transaction_stale_timestamp_entry;
    let _ = sync_transaction_stale_timestamp_with_txn_scope_entry;
}

#[test]
fn crate_root_exports_kv_range_and_timestamp_types() {
    let key = Key::from(vec![1, 2, 3]);
    let value: Value = b"value".to_vec();
    let pair = KvPair::new(key.clone(), value.clone());
    assert_eq!(pair.key(), &key);
    assert_eq!(pair.value(), &value);

    let owned_range: BoundRange = (&b"a"[..], Some(&b"z"[..])).into_owned();
    let expected: BoundRange = (b"a".to_vec(), Some(b"z".to_vec())).into();
    assert_eq!(owned_range, expected);

    let cf = ColumnFamily::try_from("write").expect("write column family should parse");
    assert_eq!(cf, ColumnFamily::Write);

    let checksum = RawChecksum {
        crc64_xor: 7,
        total_kvs: 11,
        total_bytes: 13,
    };
    assert_eq!(checksum.total_kvs, 11);
    assert_eq!(checksum.total_bytes, 13);

    let version = (5_u64 << 18) | 9;
    let ts = <Timestamp as TimestampExt>::from_version(version);
    assert_eq!(ts.version(), version);
    assert_eq!(
        <Timestamp as TimestampExt>::try_from_version(version)
            .expect("non-zero version should produce timestamp")
            .version(),
        version
    );
    assert!(<Timestamp as TimestampExt>::try_from_version(0).is_none());
}
