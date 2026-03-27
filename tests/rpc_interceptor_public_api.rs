use std::sync::Arc;
use std::time::Duration;

use tikv_client::{
    chain_rpc_interceptors, Error, FnRpcInterceptor, ProtoResourceConsumption,
    ResourceControlRequestWaitResult, ResourceControlResponseWaitResult, RpcCallResult,
    RpcInterceptor, RpcInterceptorChain, RpcRequest,
};

fn assert_interceptor<T: RpcInterceptor>(_interceptor: &T) {}

#[test]
fn rpc_interceptor_public_api_exposes_rpc_request_methods() {
    let _ = RpcRequest::target;
    let _ = RpcRequest::label;
    let _ = RpcRequest::cmd_type;
    let _ = RpcRequest::replica_read;
    let _ = RpcRequest::set_replica_read;
    let _ = RpcRequest::stale_read;
    let _ = RpcRequest::set_stale_read;
    let _ = RpcRequest::not_fill_cache;
    let _ = RpcRequest::set_not_fill_cache;
    let _ = RpcRequest::task_id;
    let _ = RpcRequest::set_task_id;
    let _ = RpcRequest::busy_threshold_ms;
    let _ = RpcRequest::set_busy_threshold_ms;
    let _ = RpcRequest::priority;
    let _ = RpcRequest::set_priority;
    let _ = RpcRequest::request_source;
    let _ = |req: &mut RpcRequest<'_>, value: String| req.set_request_source(value);
    let _ = RpcRequest::resource_group_name;
    let _ = |req: &mut RpcRequest<'_>, value: String| req.set_resource_group_name(value);
    let _ = RpcRequest::resource_control_penalty;
    let _ = RpcRequest::set_resource_control_penalty;
    let _ = RpcRequest::resource_control_override_priority;
    let _ = RpcRequest::set_resource_control_override_priority;
    let _ = RpcRequest::txn_source;
    let _ = RpcRequest::set_txn_source;
    let _ = RpcRequest::disk_full_opt;
    let _ = RpcRequest::set_disk_full_opt;
    let _ = RpcRequest::sync_log;
    let _ = RpcRequest::set_sync_log;
    let _ = RpcRequest::max_execution_duration_ms;
    let _ = RpcRequest::set_max_execution_duration_ms;
    let _ = RpcRequest::trace_control_flags;
    let _ = RpcRequest::set_trace_control_flags;
    let _ = RpcRequest::resource_group_tag;
    let _ = |req: &mut RpcRequest<'_>, value: Vec<u8>| req.set_resource_group_tag(value);
}

#[test]
fn rpc_interceptor_public_api_exposes_builders_and_result_types() {
    let interceptor = FnRpcInterceptor::new("demo")
        .on_before(|_req| {})
        .on_after(|_req, result| if let RpcCallResult::Err(_err) = result {});
    assert_interceptor(&interceptor);
    assert_eq!(RpcInterceptor::name(&interceptor), "demo");

    let mut chain = RpcInterceptorChain::new();
    chain
        .link(FnRpcInterceptor::new("first"))
        .link_arc(Arc::new(FnRpcInterceptor::new("second")));
    assert_interceptor(&chain);
    assert_eq!(RpcInterceptor::name(&chain), "interceptor-chain");

    let first: Arc<dyn RpcInterceptor> = Arc::new(FnRpcInterceptor::new("first"));
    let rest: [Arc<dyn RpcInterceptor>; 1] = [Arc::new(FnRpcInterceptor::new("second"))];
    let chained = chain_rpc_interceptors(first, rest);
    assert_interceptor(&chained);

    let _: RpcCallResult<'_> = RpcCallResult::Ok;
    let err = Error::Unimplemented;
    let result = RpcCallResult::Err(&err);
    match result {
        RpcCallResult::Ok => panic!("expected Err variant"),
        RpcCallResult::Err(inner) => assert!(matches!(inner, Error::Unimplemented)),
    }
}

#[test]
fn rpc_interceptor_public_api_exposes_resource_control_wait_results() {
    let consumption = ProtoResourceConsumption {
        r_r_u: 1.5,
        w_r_u: 2.5,
        ..Default::default()
    };
    let penalty = ProtoResourceConsumption {
        read_bytes: 11.0,
        write_bytes: 22.0,
        ..Default::default()
    };

    let request = ResourceControlRequestWaitResult {
        consumption: Some(consumption.clone()),
        penalty: Some(penalty.clone()),
        wait_duration: Duration::from_millis(7),
        priority: 9,
    };
    assert_eq!(request.consumption.as_ref(), Some(&consumption));
    assert_eq!(request.penalty.as_ref(), Some(&penalty));
    assert_eq!(request.wait_duration, Duration::from_millis(7));
    assert_eq!(request.priority, 9);

    let response = ResourceControlResponseWaitResult {
        consumption: Some(consumption),
        wait_duration: Duration::from_millis(3),
    };
    assert!(response.consumption.is_some());
    assert_eq!(response.wait_duration, Duration::from_millis(3));

    let _: ResourceControlRequestWaitResult = Default::default();
    let _: ResourceControlResponseWaitResult = Default::default();
}
