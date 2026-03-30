use tikv_client::config;
use tikv_client::proto::{errorpb, metapb};
use tikv_client::Error;

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

    let _: fn(&errorpb::Error) -> bool = config::retry::is_fake_region_error;
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

#[test]
fn config_retry_module_region_error_helper_detects_fake_epoch_not_match() {
    let fake_epoch_not_match = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch::default()),
        ..Default::default()
    };
    assert!(config::retry::is_fake_region_error(&fake_epoch_not_match));

    let real_epoch_not_match = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: 42,
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    assert!(!config::retry::is_fake_region_error(&real_epoch_not_match));
}

#[tokio::test]
async fn config_retry_module_may_backoff_for_region_error_matches_client_go_semantics() {
    let real_epoch_not_match = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch {
            current_regions: vec![metapb::Region {
                id: 7,
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let mut immediate_retry_backoff = config::retry::Backoffer::no_jitter_backoff(1, 1, 1);
    config::retry::may_backoff_for_region_error(
        &real_epoch_not_match,
        &mut immediate_retry_backoff,
    )
    .await
    .unwrap();
    assert_eq!(
        immediate_retry_backoff.next_delay_duration(),
        Some(std::time::Duration::from_millis(1))
    );

    let fake_epoch_not_match = errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch::default()),
        ..Default::default()
    };
    let mut delayed_retry_backoff = config::retry::Backoffer::no_jitter_backoff(1, 1, 1);
    config::retry::may_backoff_for_region_error(&fake_epoch_not_match, &mut delayed_retry_backoff)
        .await
        .unwrap();
    assert_eq!(delayed_retry_backoff.next_delay_duration(), None);

    let exhausted = config::retry::may_backoff_for_region_error(
        &errorpb::Error {
            not_leader: Some(errorpb::NotLeader::default()),
            ..Default::default()
        },
        &mut config::retry::Backoffer::no_backoff(),
    )
    .await;
    assert!(matches!(exhausted, Err(Error::RegionError(_))));
}
