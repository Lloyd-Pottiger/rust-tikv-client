use std::net::SocketAddr;
use std::time::Duration;

use tikv_client::config;

#[test]
fn config_module_exports_types_and_global_helpers() {
    let _ = config::GrpcCompressionType::None;
    let _: Duration = config::DEF_STORE_LIVENESS_TIMEOUT;
    let _: u32 = config::DEF_GRPC_INITIAL_WINDOW_SIZE;
    let _: u32 = config::DEF_GRPC_INITIAL_CONN_WINDOW_SIZE;
    let _: i64 = config::DEF_MAX_CONCURRENCY_REQUEST_LIMIT;
    let _: &str = config::DEF_BATCH_POLICY;
    let _: &str = config::BATCH_POLICY_BASIC;
    let _: &str = config::BATCH_POLICY_STANDARD;
    let _: &str = config::BATCH_POLICY_POSITIVE;
    let _: &str = config::BATCH_POLICY_CUSTOM;
    assert_eq!(config::DEF_BATCH_POLICY, config::BATCH_POLICY_STANDARD);

    let security = config::Security::new("ca", "cert", "key", std::iter::empty::<&str>());
    let _ = security.apply_to_config(config::Config::default());

    let _ = config::Config::default()
        .with_timeout(Duration::from_secs(1))
        .with_enable_replica_selector_v2(true)
        .with_grpc_custom_dns_server("8.8.8.8:53".parse::<SocketAddr>().unwrap())
        .with_grpc_custom_dns_domain("cluster.local");

    let config = config::Config::default().with_enable_replica_selector_v2(false);
    assert!(!config.enable_replica_selector_v2);

    let parsed = config::parse_path("tikv://127.0.0.1:2379?disableGC=true&keyspaceName=test")
        .expect("parse_path");
    assert_eq!(parsed.pd_addrs, vec!["127.0.0.1:2379"]);
    assert!(parsed.disable_gc);
    assert_eq!(parsed.keyspace_name.as_deref(), Some("test"));

    let _: Option<config::GlobalConfigRestore> = None;
    let restore = config::update_global_config(|_cfg| {});
    restore.restore();

    let _: fn() -> config::Config = config::get_global_config;
    let _: fn(config::Config) = config::set_global_config;
    let _: fn(Duration, Duration) = config::set_region_cache_ttl_with_jitter;
    let _: fn(Duration) = config::set_store_liveness_timeout;
}

#[test]
fn config_module_exposes_recent_parity_builders_and_fields() {
    let copr_cache = config::CoprocessorCacheConfig {
        capacity_mb: 64,
        admission_max_ranges: 12,
        admission_max_result_mb: 8,
        admission_min_process_ms: 9,
    };
    let _: config::CoprocessorCacheConfig = copr_cache.clone();

    let config = config::Config::default()
        .with_pd_server_timeout(Duration::from_secs(9))
        .with_commit_timeout(Duration::from_secs(41))
        .with_async_commit_keys_limit(512)
        .with_async_commit_total_key_size_limit(8192)
        .with_async_commit_safe_window(Duration::from_secs(3))
        .with_async_commit_allowed_clock_drift(Duration::from_millis(750))
        .with_batch_rpc_policy(config::BATCH_POLICY_BASIC)
        .with_batch_rpc_overload_threshold(321)
        .with_batch_rpc_wait_size(16)
        .with_batch_rpc_max_wait_time(Duration::from_millis(5))
        .with_copr_req_timeout(Duration::from_secs(7))
        .with_copr_cache(copr_cache.clone())
        .with_enable_chunk_rpc(false)
        .with_grpc_shared_buffer_pool(true)
        .with_max_concurrency_request_limit(42)
        .with_resolve_lock_lite_threshold(24);

    assert_eq!(config.pd_server_timeout, Duration::from_secs(9));
    assert_eq!(config.commit_timeout, Duration::from_secs(41));
    assert_eq!(config.async_commit_keys_limit, 512);
    assert_eq!(config.async_commit_total_key_size_limit, 8192);
    assert_eq!(config.async_commit_safe_window, Duration::from_secs(3));
    assert_eq!(
        config.async_commit_allowed_clock_drift,
        Duration::from_millis(750)
    );
    assert_eq!(config.batch_rpc_policy, config::BATCH_POLICY_BASIC);
    assert_eq!(config.batch_rpc_overload_threshold, 321);
    assert_eq!(config.batch_rpc_wait_size, 16);
    assert_eq!(config.batch_rpc_max_wait_time, Duration::from_millis(5));
    assert_eq!(config.copr_req_timeout, Duration::from_secs(7));
    assert_eq!(config.copr_cache, copr_cache);
    assert!(!config.enable_chunk_rpc);
    assert!(config.grpc_shared_buffer_pool);
    assert_eq!(config.max_concurrency_request_limit, 42);
    assert_eq!(config.resolve_lock_lite_threshold, 24);
}
