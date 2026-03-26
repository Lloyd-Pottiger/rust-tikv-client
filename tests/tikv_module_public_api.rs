use std::sync::Arc;
use std::time::Duration;

use tikv_client::tikv;

#[test]
fn tikv_module_exports_kvstore_and_backoffer() {
    let _: Option<tikv::KVStore> = None;
    let _ = tikv::Backoffer::no_backoff();
}

#[test]
fn tikv_module_exports_global_helpers() {
    let _: fn(Duration, Duration) = tikv::set_region_cache_ttl_with_jitter;
    let _: fn(Duration) = tikv::set_store_liveness_timeout;
    #[allow(deprecated)]
    let _: fn(Duration) = tikv::set_region_cache_ttl;

    let _: fn() = tikv::enable_resource_control;
    let _: fn() = tikv::disable_resource_control;
    let _: fn(Arc<dyn tikv_client::ResourceGroupKvInterceptor>) =
        tikv::set_resource_control_interceptor;
    let _: fn() = tikv::unset_resource_control_interceptor;
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
