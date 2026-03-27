use tikv_client::tikvrpc;

#[test]
fn tikvrpc_module_exports_request_and_response_wrappers() {
    let _: tikvrpc::CmdType = tikvrpc::CmdType::Get;
    let _: Option<tikvrpc::Request> = None;

    #[derive(Debug)]
    struct MockResponse {
        value: u64,
    }

    let response = MockResponse { value: 42 };
    let wrapper = tikvrpc::Response::new("kv_get", &response);
    assert_eq!(wrapper.label(), "kv_get");
    assert_eq!(wrapper.cmd_type(), tikvrpc::CmdType::Get);
    assert_eq!(wrapper.downcast_ref::<MockResponse>().unwrap().value, 42);
    assert!(wrapper.downcast_ref::<u32>().is_none());

    assert_eq!(
        tikvrpc::CmdType::from_label("kv_get"),
        tikvrpc::CmdType::Get
    );
    assert_eq!(
        tikvrpc::CmdType::from_label("not-a-real-label"),
        tikvrpc::CmdType::Unknown
    );
    assert_eq!(tikvrpc::CmdType::Commit.as_str(), "Commit");
}
