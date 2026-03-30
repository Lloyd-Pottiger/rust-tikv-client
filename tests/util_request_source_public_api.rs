use tikv_client::util;

#[test]
fn util_request_source_public_api_exposes_context_key_markers() {
    let _: util::RequestSourceTypeKey = util::RequestSourceTypeKey;
    let _: util::RequestSourceTypeKeyType = util::RequestSourceTypeKey;
    let _: util::RequestSourceKey = util::RequestSourceKey;
    let _: util::RequestSourceKeyType = util::RequestSourceKey;
    let _: util::SessionID = util::SessionID;

    let _: &str = util::INTERNAL_TXN_OTHERS;
    let _: &str = util::INTERNAL_TXN_GC;
    let _: &str = util::INTERNAL_TXN_META;
    let _: &str = util::INTERNAL_TXN_STATS;
    let _: &str = util::EXPLICIT_TYPE_EMPTY;
    let _: &str = util::EXPLICIT_TYPE_LIGHTNING;
    let _: &str = util::EXPLICIT_TYPE_BR;
    let _: &str = util::EXPLICIT_TYPE_DUMPLING;
    let _: &str = util::EXPLICIT_TYPE_BACKGROUND;
    let _: &str = util::EXPLICIT_TYPE_DDL;
    let _: &str = util::EXPLICIT_TYPE_STATS;
    let _: &str = util::EXPLICIT_TYPE_IMPORT;
    let _: &[&str] = util::EXPLICIT_TYPE_LIST;
    let _: &str = util::INTERNAL_REQUEST;
    let _: &str = util::INTERNAL_REQUEST_PREFIX;
    let _: &str = util::EXTERNAL_REQUEST;
    let _: &str = util::SOURCE_UNKNOWN;

    let _ = util::build_request_source(true, "gc", util::EXPLICIT_TYPE_EMPTY);
    std::mem::drop(util::with_request_source("external_gc", async {}));
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

    assert_eq!(util::INTERNAL_TXN_META, util::INTERNAL_TXN_OTHERS);
    assert!(util::EXPLICIT_TYPE_LIST.contains(&util::EXPLICIT_TYPE_BACKGROUND));
    assert!(util::is_internal_request(util::INTERNAL_REQUEST_PREFIX));
}
