use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tikv_client::config;

#[test]
fn config_module_exports_types_and_global_helpers() {
    let _: Option<config::ParsedTikvPath> = None;
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

#[test]
fn config_module_exports_extended_builder_knobs() {
    let config = config::Config::default()
        .with_security("ca.pem", "cert.pem", "key.pem")
        .with_store_limit(8)
        .with_committer_concurrency(12)
        .with_max_txn_ttl(Duration::from_secs(90))
        .with_tso_max_pending_count(99)
        .with_grpc_max_decoding_message_size(1024 * 1024)
        .with_grpc_connection_count(4)
        .with_grpc_compression_type(config::GrpcCompressionType::Gzip)
        .with_enable_batch_rpc(false)
        .with_enable_forwarding(true)
        .with_batch_rpc_max_batch_size(256)
        .with_grpc_connect_timeout(Duration::from_secs(2))
        .with_grpc_keepalive_time(Duration::from_secs(3))
        .with_grpc_keepalive_timeout(Duration::from_secs(4))
        .with_grpc_initial_window_size(32)
        .with_grpc_initial_conn_window_size(64)
        .with_default_keyspace()
        .with_resolve_lock_lite_threshold(31)
        .with_ttl_refreshed_txn_size(2048)
        .with_region_cache_ttl(Duration::from_secs(11))
        .with_region_cache_ttl_jitter(Duration::from_secs(2))
        .with_enable_region_cache_preload(true);

    assert_eq!(config.ca_path, Some(PathBuf::from("ca.pem")));
    assert_eq!(config.cert_path, Some(PathBuf::from("cert.pem")));
    assert_eq!(config.key_path, Some(PathBuf::from("key.pem")));
    assert_eq!(config.store_limit, 8);
    assert_eq!(config.committer_concurrency, 12);
    assert_eq!(config.max_txn_ttl, Duration::from_secs(90));
    assert_eq!(config.tso_max_pending_count, 99);
    assert_eq!(config.grpc_max_decoding_message_size, 1024 * 1024);
    assert_eq!(config.grpc_connection_count, 4);
    assert_eq!(
        config.grpc_compression_type,
        config::GrpcCompressionType::Gzip
    );
    assert!(!config.enable_batch_rpc);
    assert!(config.enable_forwarding);
    assert_eq!(config.batch_rpc_max_batch_size, 256);
    assert_eq!(config.grpc_connect_timeout, Duration::from_secs(2));
    assert_eq!(config.grpc_keepalive_time, Duration::from_secs(3));
    assert_eq!(config.grpc_keepalive_timeout, Duration::from_secs(4));
    assert_eq!(config.grpc_initial_window_size, 32);
    assert_eq!(config.grpc_initial_conn_window_size, 64);
    assert_eq!(config.keyspace.as_deref(), Some("DEFAULT"));
    assert_eq!(config.resolve_lock_lite_threshold, 31);
    assert_eq!(config.ttl_refreshed_txn_size, 2048);
    assert_eq!(config.region_cache_ttl, Duration::from_secs(11));
    assert_eq!(config.region_cache_ttl_jitter, Duration::from_secs(2));
    assert!(config.enable_region_cache_preload);

    let _: fn(&config::Config) -> tikv_client::Result<tikv_client::SecurityManager> =
        config::Config::security_manager;
    let _: fn(&config::Security) -> tikv_client::Result<tikv_client::SecurityManager> =
        config::Security::security_manager;
}

#[test]
fn config_module_exports_scope_and_liveness_builders() {
    let config = config::Config::default()
        .with_keyspace("tenant-a")
        .with_zone_label("zone-a")
        .with_txn_scope("sh")
        .with_health_feedback_update_interval(Duration::from_secs(5))
        .with_store_liveness_update_interval(Duration::from_secs(7))
        .with_store_liveness_timeout(Duration::from_secs(9))
        .with_txn_local_latches_capacity(128);

    assert_eq!(config.keyspace.as_deref(), Some("tenant-a"));
    assert_eq!(config.zone_label.as_deref(), Some("zone-a"));
    assert_eq!(config.txn_scope.as_deref(), Some("sh"));
    assert_eq!(
        config.health_feedback_update_interval,
        Duration::from_secs(5)
    );
    assert_eq!(
        config.store_liveness_update_interval,
        Duration::from_secs(7)
    );
    assert_eq!(config.store_liveness_timeout, Duration::from_secs(9));
    assert_eq!(config.txn_local_latches_capacity, 128);

    let restore = config::update_global_config(|cfg| {
        *cfg = cfg.clone().with_txn_scope("bj");
    });
    assert_eq!(config::get_txn_scope_from_global_config(), "bj");
    restore.restore();
}

#[test]
fn config_module_exposes_validate_and_combined_custom_dns_builder() {
    let dns_server = SocketAddr::from(([8, 8, 8, 8], 53));
    let config = config::Config::default().with_grpc_custom_dns(dns_server, "cluster.local");

    let _: fn(&config::Config) -> tikv_client::Result<()> = config::Config::validate;
    assert_eq!(config.grpc_custom_dns_server, Some(dns_server));
    assert_eq!(
        config.grpc_custom_dns_domain.as_deref(),
        Some("cluster.local")
    );
    config.validate().expect("valid custom dns config");
}

#[test]
fn config_module_validate_rejects_custom_dns_server_without_port() {
    let config =
        config::Config::default().with_grpc_custom_dns(SocketAddr::from(([127, 0, 0, 1], 0)), "");

    assert_eq!(config.grpc_custom_dns_domain, None);

    let err = config.validate().expect_err("port 0 should be rejected");
    assert!(matches!(
        err,
        tikv_client::Error::StringError(message)
            if message.contains("grpc-custom-dns-server port should be greater than 0")
    ));
}

#[test]
fn security_materialization_helpers_are_publicly_usable() {
    config::Config::default()
        .security_manager()
        .expect("default config should materialize an insecure security manager");
    config::Security::default()
        .security_manager()
        .expect("empty standalone security should also materialize");

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    let missing = std::env::temp_dir().join(format!("tikv-client-missing-security-{nonce}.pem"));

    let err = match tikv_client::SecurityManager::load(&missing, &missing, missing.clone()) {
        Ok(_) => panic!("loading a security manager from missing PEM files should fail"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());
}

#[test]
fn security_materialization_accepts_valid_pem_contents() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ca = dir.path().join("ca.pem");
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");

    let cert_pem = include_bytes!("fixtures/security-test-cert.pem");
    let key_pem = include_bytes!("fixtures/security-test-key.pem");
    std::fs::write(&ca, cert_pem).expect("write ca");
    std::fs::write(&cert, cert_pem).expect("write cert");
    std::fs::write(&key, key_pem).expect("write key");

    config::Config::default()
        .with_security(&ca, &cert, &key)
        .security_manager()
        .expect("valid PEM should materialize a TLS security manager");
    config::Security::new(&ca, &cert, &key, std::iter::empty::<String>())
        .security_manager()
        .expect("standalone security helper should materialize valid PEM too");
}

#[test]
fn security_materialization_rejects_missing_ca_for_ca_only_tls() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after unix epoch")
        .as_nanos();
    let missing = std::env::temp_dir().join(format!("tikv-client-missing-ca-only-{nonce}.pem"));

    let mut config = config::Config::default();
    config.ca_path = Some(missing.clone());
    let err = match config.security_manager() {
        Ok(_) => panic!("ca-only TLS should validate and reject a missing CA file"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());

    let err = match (config::Security {
        ca_path: Some(missing),
        ..config::Security::default()
    })
    .security_manager()
    {
        Ok(_) => panic!("standalone ca-only TLS should reject a missing CA file"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());
}

#[test]
fn security_materialization_rejects_invalid_pem_contents() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ca = dir.path().join("ca.pem");
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");

    std::fs::write(&ca, b"not a pem").expect("write ca");
    std::fs::write(&cert, b"not a pem").expect("write cert");
    std::fs::write(&key, b"not a pem").expect("write key");

    let err = match config::Config::default()
        .with_security(&ca, &cert, &key)
        .security_manager()
    {
        Ok(_) => panic!("invalid PEM should be rejected eagerly"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());

    let err = match config::Security::new(&ca, &cert, &key, std::iter::empty::<String>())
        .security_manager()
    {
        Ok(_) => panic!("standalone security helper should reject invalid PEM too"),
        Err(err) => err,
    };
    assert!(!err.to_string().is_empty());
}
