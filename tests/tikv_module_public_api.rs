use std::sync::Arc;
use std::time::Duration;

use tikv_client::tikv;

#[test]
fn tikv_module_exports_kvstore_and_backoffer() {
    let _: Option<tikv::KVStore> = None;
    let _ = tikv::Backoffer::no_backoff();
    let _: Option<tikv::Store> = None;
    let _: Option<tikv::Variables> = None;
}

#[test]
fn tikv_module_exports_backoff_helpers() {
    let _: Option<tikv::BackoffConfig> = None;

    let _ = tikv::new_backoffer(100);
    let _ = tikv::new_backoffer_with_vars(100, None);
    let _ = tikv::new_gc_resolve_lock_max_backoffer();
    let _ = tikv::new_noop_backoff();
    let _ = tikv::txn_start_key();

    let _ = tikv::bo_region_miss();
    let _ = tikv::bo_tikv_rpc();
    let _ = tikv::bo_tiflash_rpc();
    let _ = tikv::bo_txn_lock();
    let _ = tikv::bo_pd_rpc();
}

#[test]
fn tikv_module_exports_region_ver_id_constructor() {
    let _: fn(u64, u64, u64) -> tikv::RegionVerId = tikv::new_region_ver_id;

    assert_eq!(
        tikv::new_region_ver_id(11, 22, 33),
        tikv::RegionVerId {
            id: 11,
            conf_ver: 22,
            ver: 33,
        }
    );
}

#[test]
fn tikv_module_exports_global_helpers() {
    let _: fn(Duration, Duration) = tikv::set_region_cache_ttl_with_jitter;
    let _: fn(Duration) = tikv::set_store_liveness_timeout;
    #[allow(deprecated)]
    let _: fn(Duration) = tikv::set_region_cache_ttl;
    let _: fn() -> u32 = tikv::load_shutting_down;
    let _: fn(u32) = tikv::store_shutting_down;
    let _: fn(fn(&mut tikv_client::PdRegionMetaCircuitBreakerSettings)) =
        tikv::change_pd_region_meta_circuit_breaker_settings;

    let _: fn() = tikv::enable_resource_control;
    let _: fn() = tikv::disable_resource_control;
    let _: fn(Arc<dyn tikv_client::ResourceGroupKvInterceptor>) =
        tikv::set_resource_control_interceptor;
    let _: fn() = tikv::unset_resource_control_interceptor;
}

#[test]
fn tikv_module_exports_resource_control_info_accessors() {
    let _: fn(tikv_client::ResourceControlRequestInfo) -> u64 =
        tikv_client::ResourceControlRequestInfo::write_bytes;
    let _: fn(tikv_client::ResourceControlResponseInfo) -> u64 =
        tikv_client::ResourceControlResponseInfo::read_bytes;
    let _: fn(tikv_client::ResourceControlResponseInfo) -> Duration =
        tikv_client::ResourceControlResponseInfo::kv_cpu;

    let request = tikv_client::ResourceControlRequestInfo::new("kv_commit", 123, 7);
    assert_eq!(request.write_bytes(), 0);

    let response = tikv_client::ResourceControlResponseInfo::new(456);
    assert_eq!(response.read_bytes(), 0);
    assert_eq!(response.kv_cpu(), Duration::ZERO);
}

#[test]
fn tikv_module_exports_codec_prefix_helpers() {
    let _: u8 = tikv::CODEC_V2_RAW_KEYSPACE_PREFIX;
    let _: u8 = tikv::CODEC_V2_TXN_KEYSPACE_PREFIX;
    let v2 = tikv::codec_v2_prefixes();
    assert_eq!(v2.len(), 2);
    assert_eq!(v2[0], &[tikv::CODEC_V2_RAW_KEYSPACE_PREFIX]);
    assert_eq!(v2[1], &[tikv::CODEC_V2_TXN_KEYSPACE_PREFIX]);
    assert_eq!(tikv::codec_v1_exclude_prefixes(), v2);
}

#[test]
fn tikv_module_exports_region_cache_helpers_and_keyrange() {
    let _: Option<tikv::RegionVerId> = None;
    let _: Option<tikv::KeyLocation> = None;
    let _: Option<tikv::KeyRange> = None;
    let _: tikv::BatchLocateKeyRangesOpt = tikv::with_need_buckets();
    let _: tikv::BatchLocateKeyRangesOpt = tikv::with_need_region_has_leader_peer();

    fn assert_new_region_cache_signature<C>()
    where
        C: tikv_client::RetryClientTrait + Send + Sync,
    {
        let _: fn(Arc<C>) -> tikv::RegionCache<C> = tikv::new_region_cache::<C>;
    }

    assert_new_region_cache_signature::<tikv_client::RetryClient>();
}

#[test]
fn tikv_module_exports_label_filters() {
    let _: tikv::LabelFilter = tikv::label_filter_all_node;
    let _: fn(&tikv_client::proto::metapb::Store) -> tikv_client::tikvrpc::EndpointType =
        tikv::get_store_type_by_meta;

    let tiflash_labels = vec![tikv_client::StoreLabel {
        key: tikv_client::tikvrpc::ENGINE_LABEL_KEY.to_owned(),
        value: tikv_client::tikvrpc::ENGINE_LABEL_TIFLASH.to_owned(),
    }];
    assert!(tikv::label_filter_all_tiflash_node(&tiflash_labels));
    assert!(tikv::label_filter_no_tiflash_write_node(&tiflash_labels));
    assert!(!tikv::label_filter_only_tiflash_write_node(&tiflash_labels));

    let tiflash_write_labels = vec![
        tikv_client::StoreLabel {
            key: tikv_client::tikvrpc::ENGINE_LABEL_KEY.to_owned(),
            value: tikv_client::tikvrpc::ENGINE_LABEL_TIFLASH.to_owned(),
        },
        tikv_client::StoreLabel {
            key: tikv_client::tikvrpc::ENGINE_ROLE_LABEL_KEY.to_owned(),
            value: tikv_client::tikvrpc::ENGINE_ROLE_WRITE.to_owned(),
        },
    ];
    assert!(tikv::label_filter_all_tiflash_node(&tiflash_write_labels));
    assert!(tikv::label_filter_only_tiflash_write_node(
        &tiflash_write_labels
    ));
    assert!(!tikv::label_filter_no_tiflash_write_node(
        &tiflash_write_labels
    ));

    let non_tiflash_labels = vec![tikv_client::StoreLabel {
        key: tikv_client::tikvrpc::ENGINE_LABEL_KEY.to_owned(),
        value: "not-tiflash".to_owned(),
    }];
    assert!(!tikv::label_filter_all_tiflash_node(&non_tiflash_labels));
    assert!(!tikv::label_filter_only_tiflash_write_node(
        &non_tiflash_labels
    ));
    assert!(!tikv::label_filter_no_tiflash_write_node(
        &non_tiflash_labels
    ));
    assert!(tikv::label_filter_all_node(&non_tiflash_labels));

    let tiflash_store = tikv_client::proto::metapb::Store {
        labels: tiflash_labels,
        ..Default::default()
    };
    assert_eq!(
        tikv::get_store_type_by_meta(&tiflash_store),
        tikv_client::tikvrpc::EndpointType::TiFlash
    );
}
