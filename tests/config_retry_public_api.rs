use tikv_client::config;

#[test]
fn config_retry_module_exports_backoff_helpers() {
    let _ = config::retry::Backoffer::no_backoff();
    let _: config::retry::TxnStartKey = config::retry::txn_start_key();

    let _: config::retry::BackoffJitter = config::retry::NO_JITTER;
    let _: config::retry::BackoffJitter = config::retry::FULL_JITTER;
    let _: config::retry::BackoffJitter = config::retry::EQUAL_JITTER;
    let _: config::retry::BackoffJitter = config::retry::DECORR_JITTER;
    let cfg = config::retry::new_backoff_fn_cfg(2, 7, config::retry::NO_JITTER);
    let _: config::retry::BackoffFnCfg = cfg;
    let _ = cfg.to_backoff(3);
    let mut named = config::retry::new_config("tikvRPC", cfg, tikv_client::Error::Unimplemented);
    let _: config::retry::Config = named.clone();
    assert_eq!(named.base(), 2);
    assert_eq!(named.to_string(), "tikvRPC");
    named.set_backoff_fn_cfg(config::retry::new_backoff_fn_cfg(
        3,
        9,
        config::retry::FULL_JITTER,
    ));
    named.set_errors(tikv_client::Error::InternalError {
        message: "retry failed".to_owned(),
    });
    let _ = named.to_backoff();
    let _ = named.to_backoff_with_vars(&tikv_client::Variables::default());
    let _ = config::retry::Config::no_backoff().to_backoff();

    let _ = config::retry::new_backoffer(100);
    let _ = config::retry::new_backoffer_with_vars(100, None);
    let _ = config::retry::new_gc_resolve_lock_max_backoffer();
    let _ = config::retry::new_noop_backoff();

    let _: config::retry::Config = config::retry::bo_region_miss();
    let _: config::retry::Config = config::retry::bo_region_scheduling();
    let _: config::retry::Config = config::retry::bo_tikv_rpc();
    let _: config::retry::Config = config::retry::bo_tiflash_rpc();
    let _: config::retry::Config = config::retry::bo_txn_lock();
    let _: config::retry::Config = config::retry::bo_pd_rpc();

    let _: config::retry::Config = config::retry::bo_tikv_server_busy();
    let _: config::retry::Config = config::retry::bo_tiflash_server_busy();
    let _: config::retry::Config = config::retry::bo_tikv_disk_full();
    let _: config::retry::Config = config::retry::bo_region_recovery_in_progress();
    let _: config::retry::Config = config::retry::bo_txn_not_found();
    let _: config::retry::Config = config::retry::bo_stale_cmd();
    let _: config::retry::Config = config::retry::bo_max_ts_not_synced();
    let _: config::retry::Config = config::retry::bo_commit_ts_lag();
    let _: config::retry::Config = config::retry::bo_region_not_initialized();
    let _: config::retry::Config = config::retry::bo_is_witness();
    let _: config::retry::Config = config::retry::bo_txn_lock_fast();
    let _ = config::retry::bo_region_miss().to_backoff();
    let _ =
        config::retry::bo_txn_lock_fast().to_backoff_with_vars(&tikv_client::Variables::default());
    let _ = config::retry::bo_txn_lock_fast_with_vars(&tikv_client::Variables::default());
}

#[tokio::test]
async fn config_retry_module_exposes_task_local_txn_start_ts() {
    assert_eq!(config::retry::txn_start_ts(), None);

    config::retry::with_txn_start_ts(42, async {
        assert_eq!(config::retry::txn_start_ts(), Some(42));
    })
    .await;

    assert_eq!(config::retry::txn_start_ts(), None);
}
