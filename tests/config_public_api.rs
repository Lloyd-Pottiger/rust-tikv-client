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
