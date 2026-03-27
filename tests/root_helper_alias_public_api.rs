use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use tikv_client::{
    change_pd_region_meta_circuit_breaker_settings, coprocessor_lowering,
    decode_comparable_uvarint, decode_comparable_varint, disable_resource_control,
    enable_resource_control, encode_comparable_uvarint, encode_comparable_varint,
    set_resource_control_interceptor, transaction_lowering, unset_resource_control_interceptor,
    PdRegionMetaCircuitBreakerSettings, ResourceControlRequestInfo,
    ResourceControlRequestWaitResult, ResourceControlResponseInfo,
    ResourceControlResponseWaitResult, ResourceGroupKvInterceptor, Timestamp, TimestampExt,
};

#[derive(Default)]
struct TestInterceptor;

#[async_trait]
impl ResourceGroupKvInterceptor for TestInterceptor {
    async fn on_request_wait(
        &self,
        _resource_group_name: &str,
        _request: &ResourceControlRequestInfo,
    ) -> tikv_client::Result<ResourceControlRequestWaitResult> {
        Ok(ResourceControlRequestWaitResult::default())
    }

    async fn on_response_wait(
        &self,
        _resource_group_name: &str,
        _request: &ResourceControlRequestInfo,
        _response: &ResourceControlResponseInfo,
    ) -> tikv_client::Result<ResourceControlResponseWaitResult> {
        Ok(ResourceControlResponseWaitResult::default())
    }
}

#[test]
fn crate_root_exports_comparable_varint_helpers() {
    let mut encoded = Vec::new();
    encode_comparable_varint(&mut encoded, -7);
    let (rest, decoded) = decode_comparable_varint(&encoded).expect("decode comparable varint");
    assert!(rest.is_empty());
    assert_eq!(decoded, -7);

    encoded.clear();
    encode_comparable_uvarint(&mut encoded, 42);
    let (rest, decoded) = decode_comparable_uvarint(&encoded).expect("decode comparable uvarint");
    assert!(rest.is_empty());
    assert_eq!(decoded, 42);
}

#[test]
fn crate_root_exports_resource_control_and_circuit_breaker_hooks() {
    let _: fn() = enable_resource_control;
    let _: fn() = disable_resource_control;
    let _: fn(Arc<dyn ResourceGroupKvInterceptor>) = set_resource_control_interceptor;
    let _: fn() = unset_resource_control_interceptor;

    let defaults = PdRegionMetaCircuitBreakerSettings::default();
    assert_eq!(defaults.error_rate_window, Duration::from_secs(30));
    assert_eq!(defaults.min_qps_for_open, 10);

    change_pd_region_meta_circuit_breaker_settings(|settings| {
        settings.min_qps_for_open = defaults.min_qps_for_open + 1;
        settings.half_open_success_count = defaults.half_open_success_count + 1;
    });
    change_pd_region_meta_circuit_breaker_settings(|settings| *settings = defaults);

    set_resource_control_interceptor(Arc::new(TestInterceptor));
    enable_resource_control();
    disable_resource_control();
    unset_resource_control_interceptor();
}

#[test]
fn crate_root_exports_lowering_alias_modules() {
    let start_ts = <Timestamp as TimestampExt>::from_version(42);

    let cop_request = coprocessor_lowering::new_coprocessor_request(
        7,
        b"payload".to_vec(),
        vec!["a".to_owned().."b".to_owned()],
        start_ts.clone(),
    );
    assert_eq!(cop_request.tp, 7);
    assert_eq!(cop_request.start_ts, 42);
    assert_eq!(cop_request.ranges.len(), 1);
    assert_eq!(cop_request.ranges[0].start, b"a".to_vec());
    assert_eq!(cop_request.ranges[0].end, b"b".to_vec());

    let get = transaction_lowering::new_get_request(b"k".to_vec().into(), start_ts);
    assert_eq!(get.version, 42);
    assert_eq!(get.key, b"k".to_vec());
}
