use tikv_client::rawkv;

#[test]
fn rawkv_module_exports_types() {
    let _: Option<rawkv::Client> = None;
    let _ = rawkv::ColumnFamily::Default;
    let _: Option<rawkv::RawChecksum> = None;
    let _ = rawkv::MAX_RAW_KV_SCAN_LIMIT;

    assert_eq!(rawkv::MAX_RAW_KV_SCAN_LIMIT, 10240);
}
