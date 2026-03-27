use tikv_client::util;

#[test]
fn util_request_source_public_api_exposes_context_key_markers() {
    let _: util::RequestSourceTypeKey = util::RequestSourceTypeKey;
    let _: util::RequestSourceTypeKeyType = util::RequestSourceTypeKey;
    let _: util::RequestSourceKey = util::RequestSourceKey;
    let _: util::RequestSourceKeyType = util::RequestSourceKey;
    let _: util::SessionID = util::SessionID;

    let _ = util::build_request_source(true, "gc", util::EXPLICIT_TYPE_EMPTY);
    std::mem::drop(util::with_internal_source_type("gc", async {}));
    std::mem::drop(util::with_internal_source_and_task_type(
        "gc",
        "stats",
        async {},
    ));
    let _ = util::request_source();
    std::mem::drop(util::with_resource_group_name("rg", async {}));
    let _ = util::resource_group_name();
    let _ = util::is_internal_request("internal_gc");
}
