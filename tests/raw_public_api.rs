use std::convert::TryFrom;
use std::ops::Range;
use std::sync::Arc;

use tikv_client::{raw, Backoff, Error, Key, KvPair, TraceControlFlags, Value};

async fn min_safe_ts_with_txn_scope_entry(
    client: &raw::Client,
    txn_scope: String,
) -> tikv_client::Result<u64> {
    client.min_safe_ts_with_txn_scope(txn_scope).await
}

async fn close_addr_entry(client: &raw::Client, address: &str) -> bool {
    client.close_addr(address).await
}

async fn close_entry(client: &raw::Client) {
    client.close().await
}

async fn min_safe_ts_entry(client: &raw::Client) -> tikv_client::Result<u64> {
    client.min_safe_ts().await
}

async fn get_entry(client: &raw::Client, key: Vec<u8>) -> tikv_client::Result<Option<Value>> {
    client.get(key).await
}

async fn batch_get_entry(
    client: &raw::Client,
    keys: Vec<Vec<u8>>,
) -> tikv_client::Result<Vec<KvPair>> {
    client.batch_get(keys).await
}

async fn batch_get_values_entry(
    client: &raw::Client,
    keys: Vec<Vec<u8>>,
) -> tikv_client::Result<Vec<Option<Value>>> {
    client.batch_get_values(keys).await
}

async fn put_entry(client: &raw::Client, key: Vec<u8>, value: Vec<u8>) -> tikv_client::Result<()> {
    client.put(key, value).await
}

async fn put_with_ttl_entry(
    client: &raw::Client,
    key: Vec<u8>,
    value: Vec<u8>,
    ttl: u64,
) -> tikv_client::Result<()> {
    client.put_with_ttl(key, value, ttl).await
}

async fn batch_put_with_ttl_entry(
    client: &raw::Client,
    pairs: Vec<KvPair>,
    ttls: Vec<u64>,
) -> tikv_client::Result<()> {
    client.batch_put_with_ttl(pairs, ttls).await
}

async fn get_key_ttl_secs_entry(
    client: &raw::Client,
    key: Vec<u8>,
) -> tikv_client::Result<Option<u64>> {
    client.get_key_ttl_secs(key).await
}

async fn batch_delete_entry(client: &raw::Client, keys: Vec<Vec<u8>>) -> tikv_client::Result<()> {
    client.batch_delete(keys).await
}

async fn delete_range_entry(
    client: &raw::Client,
    range: std::ops::Range<Vec<u8>>,
) -> tikv_client::Result<()> {
    client.delete_range(range).await
}

async fn scan_entry(
    client: &raw::Client,
    range: std::ops::Range<Vec<u8>>,
    limit: u32,
) -> tikv_client::Result<Vec<KvPair>> {
    client.scan(range, limit).await
}

async fn batch_scan_entry(
    client: &raw::Client,
    ranges: Vec<std::ops::Range<Vec<u8>>>,
    each_limit: u32,
) -> tikv_client::Result<Vec<KvPair>> {
    client.batch_scan(ranges, each_limit).await
}

async fn batch_scan_keys_entry(
    client: &raw::Client,
    ranges: Vec<std::ops::Range<Vec<u8>>>,
    each_limit: u32,
) -> tikv_client::Result<Vec<Key>> {
    client.batch_scan_keys(ranges, each_limit).await
}

async fn checksum_entry(
    client: &raw::Client,
    range: std::ops::Range<Vec<u8>>,
) -> tikv_client::Result<raw::RawChecksum> {
    client.checksum(range).await
}

async fn compare_and_swap_entry(
    client: &raw::Client,
    key: Vec<u8>,
    previous_value: Option<Vec<u8>>,
    new_value: Vec<u8>,
) -> tikv_client::Result<(Option<Value>, bool)> {
    client
        .compare_and_swap(key, previous_value, new_value)
        .await
}

async fn coprocessor_entry(
    client: &raw::Client,
    copr_name: String,
    copr_version_req: String,
    ranges: Vec<std::ops::Range<Vec<u8>>>,
) -> tikv_client::Result<Vec<(Vec<Range<Key>>, Vec<u8>)>> {
    client
        .coprocessor(copr_name, copr_version_req, ranges, |_region, _ranges| {
            Vec::new()
        })
        .await
}

#[test]
fn raw_module_exports_column_family_and_checksum_helpers() {
    let _: Option<raw::Client> = None;
    let _ = raw::ColumnFamily::Default;
    let _ = raw::ColumnFamily::Lock;
    let _ = raw::ColumnFamily::Write;
    let _: Option<raw::RawChecksum> = None;

    let cf = raw::ColumnFamily::try_from("write").expect("valid column family");
    assert_eq!(cf, raw::ColumnFamily::Write);
    assert_eq!(cf.to_string(), "write");

    let checksum = raw::RawChecksum {
        crc64_xor: 11,
        total_kvs: 13,
        total_bytes: 17,
    };
    assert_eq!(checksum.crc64_xor, 11);
    assert_eq!(checksum.total_kvs, 13);
    assert_eq!(checksum.total_bytes, 17);

    assert!(matches!(
        raw::ColumnFamily::try_from("raft"),
        Err(Error::ColumnFamilyError(name)) if name == "raft"
    ));
}

#[test]
fn raw_module_exports_client_entrypoints() {
    let _ = raw::Client::<tikv_client::PdRpcClient>::new::<String>;
    let _ = raw::Client::<tikv_client::PdRpcClient>::new_with_config::<String>;
    let _: fn(&raw::Client) -> u64 = raw::Client::cluster_id;
    let _ = close_addr_entry;
    let _: fn(&raw::Client, raw::ColumnFamily) -> raw::Client = raw::Client::with_cf;
    let _: fn(&raw::Client, Backoff) -> raw::Client = raw::Client::with_backoff;
    let _ = close_entry;
    let _: fn(&raw::Client) -> bool = raw::Client::is_closed;
    let _: fn(&raw::Client, Vec<u8>) -> raw::Client = raw::Client::with_trace_id;
    let _: fn(&raw::Client) -> raw::Client = raw::Client::without_trace_id;
    let _: fn(&raw::Client, TraceControlFlags) -> raw::Client =
        raw::Client::with_trace_control_flags;
    let _: fn(&raw::Client) -> Arc<tikv_client::PdRpcClient> = raw::Client::pd_client;
    let _ = min_safe_ts_entry;
    let _: fn(&raw::Client) -> raw::Client = raw::Client::with_atomic_for_cas;

    let _ = min_safe_ts_with_txn_scope_entry;
    let _ = get_entry;
    let _ = batch_get_entry;
    let _ = batch_get_values_entry;
    let _ = put_entry;
    let _ = put_with_ttl_entry;
    let _ = batch_put_with_ttl_entry;
    let _ = get_key_ttl_secs_entry;
    let _ = batch_delete_entry;
    let _ = delete_range_entry;
    let _ = scan_entry;
    let _ = batch_scan_entry;
    let _ = batch_scan_keys_entry;
    let _ = checksum_entry;
    let _ = compare_and_swap_entry;
    let _ = coprocessor_entry;
}

#[test]
fn raw_lowering_helpers_build_expected_requests() {
    let get =
        raw::lowering::new_raw_get_request(Key::from(vec![1, 2]), Some(raw::ColumnFamily::Lock));
    assert_eq!(get.key, vec![1, 2]);
    assert_eq!(get.cf, "lock");

    let batch_get = raw::lowering::new_raw_batch_get_request(
        vec![Key::from(vec![3]), Key::from(vec![4, 5])].into_iter(),
        Some(raw::ColumnFamily::Write),
    );
    assert_eq!(batch_get.keys, vec![vec![3], vec![4, 5]]);
    assert_eq!(batch_get.cf, "write");

    let put = raw::lowering::new_raw_put_request(
        Key::from(vec![6]),
        b"value".to_vec(),
        Some(raw::ColumnFamily::Default),
        9,
        true,
    );
    assert_eq!(put.key, vec![6]);
    assert_eq!(put.value, b"value".to_vec());
    assert_eq!(put.cf, "default");
    assert_eq!(put.ttl, 9);
    assert!(put.for_cas);

    let batch_put = raw::lowering::new_raw_batch_put_request(
        vec![
            KvPair::new(vec![7], b"v1".to_vec()),
            KvPair::new(vec![8], b"v2".to_vec()),
        ]
        .into_iter(),
        vec![11, 12, 13].into_iter(),
        Some(raw::ColumnFamily::Lock),
        true,
    );
    assert_eq!(batch_put.pairs.len(), 2);
    assert_eq!(batch_put.ttls, vec![11, 12]);
    assert_eq!(batch_put.cf, "lock");
    assert!(batch_put.for_cas);

    let delete = raw::lowering::new_raw_delete_request(
        Key::from(vec![9]),
        Some(raw::ColumnFamily::Write),
        true,
    );
    assert_eq!(delete.key, vec![9]);
    assert_eq!(delete.cf, "write");
    assert!(delete.for_cas);

    let delete_range = raw::lowering::new_raw_delete_range_request(
        (vec![10]..vec![20]).into(),
        Some(raw::ColumnFamily::Default),
    );
    assert_eq!(delete_range.start_key, vec![10]);
    assert_eq!(delete_range.end_key, vec![20]);
    assert_eq!(delete_range.cf, "default");

    let scan = raw::lowering::new_raw_scan_request(
        (vec![21]..vec![29]).into(),
        30,
        true,
        true,
        Some(raw::ColumnFamily::Lock),
    );
    assert_eq!(scan.start_key, vec![29]);
    assert_eq!(scan.end_key, vec![21]);
    assert_eq!(scan.limit, 30);
    assert!(scan.key_only);
    assert!(scan.reverse);
    assert_eq!(scan.cf, "lock");

    let batch_scan = raw::lowering::new_raw_batch_scan_request(
        vec![(vec![30]..vec![31]).into(), (vec![32]..vec![33]).into()].into_iter(),
        5,
        false,
        Some(raw::ColumnFamily::Write),
    );
    assert_eq!(batch_scan.ranges.len(), 2);
    assert_eq!(batch_scan.each_limit, 5);
    assert!(!batch_scan.key_only);
    assert_eq!(batch_scan.cf, "write");

    let checksum = raw::lowering::new_raw_checksum_request((vec![34]..vec![35]).into());
    assert_eq!(checksum.ranges.len(), 1);
    assert_eq!(checksum.ranges[0].start_key, vec![34]);
    assert_eq!(checksum.ranges[0].end_key, vec![35]);

    let cas = raw::lowering::new_cas_request(
        Key::from(vec![36]),
        b"next".to_vec(),
        Some(b"prev".to_vec()),
        Some(raw::ColumnFamily::Default),
    );
    assert_eq!(cas.key, vec![36]);
    assert_eq!(cas.value, b"next".to_vec());
    assert!(!cas.previous_not_exist);
    assert_eq!(cas.previous_value, b"prev".to_vec());
    assert_eq!(cas.cf, "default");

    let root_get = tikv_client::raw_lowering::new_raw_get_request(Key::from(vec![37]), None);
    assert_eq!(root_get.key, vec![37]);
    assert_eq!(root_get.cf, "");
}
