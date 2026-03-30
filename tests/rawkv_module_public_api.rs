use tikv_client::rawkv;

#[test]
fn rawkv_module_exports_types() {
    let _: Option<rawkv::Client> = None;
    let _ = rawkv::ColumnFamily::Default;
    let _: Option<rawkv::RawChecksum> = None;
    let _ = rawkv::MAX_RAW_KV_SCAN_LIMIT;
    let _: fn(&rawkv::Client, bool) -> rawkv::Client = rawkv::Client::set_atomic_for_cas;
    let _: fn(&rawkv::Client, rawkv::ColumnFamily) -> rawkv::Client =
        rawkv::Client::set_column_family;

    assert_eq!(rawkv::MAX_RAW_KV_SCAN_LIMIT, 10240);
}
