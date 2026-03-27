use tikv_client::config;

#[test]
fn config_retry_module_exports_backoff_helpers() {
    let _ = config::retry::Backoffer::no_backoff();
    let _ = config::retry::Config::no_backoff();

    let _ = config::retry::new_backoffer(100);
    let _ = config::retry::new_backoffer_with_vars(100, None);
    let _ = config::retry::new_gc_resolve_lock_max_backoffer();
    let _ = config::retry::new_noop_backoff();

    let _ = config::retry::bo_region_miss();
    let _ = config::retry::bo_tikv_rpc();
    let _ = config::retry::bo_tiflash_rpc();
    let _ = config::retry::bo_txn_lock();
    let _ = config::retry::bo_pd_rpc();
}
