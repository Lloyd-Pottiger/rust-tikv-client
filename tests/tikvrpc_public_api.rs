use tikv_client::tikvrpc;

#[test]
fn tikvrpc_module_exports_request_and_response_wrappers() {
    let _: Option<tikvrpc::Request> = None;

    #[derive(Debug)]
    struct MockResponse {
        value: u64,
    }

    let response = MockResponse { value: 42 };
    let wrapper = tikvrpc::Response::new("kv_get", &response);
    assert_eq!(wrapper.label(), "kv_get");
    let _ = wrapper.cmd_type();
    assert_eq!(wrapper.downcast_ref::<MockResponse>().unwrap().value, 42);
    assert!(wrapper.downcast_ref::<u32>().is_none());
}
