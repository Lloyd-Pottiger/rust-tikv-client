use tikv_client::tikv;

#[test]
fn tikv_module_exports_kvstore_and_backoffer() {
    let _: Option<tikv::KVStore> = None;
    let _ = tikv::Backoffer::no_backoff();
}
