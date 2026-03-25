use tikv_client::rawkv;

#[test]
fn rawkv_module_exports_types() {
    let _: Option<rawkv::Client> = None;
    let _ = rawkv::ColumnFamily::Default;
    let _: Option<rawkv::RawChecksum> = None;
}
