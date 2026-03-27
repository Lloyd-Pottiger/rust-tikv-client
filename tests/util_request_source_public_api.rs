use tikv_client::util;

#[test]
fn util_request_source_public_api_exposes_context_key_markers() {
    let _: util::RequestSourceTypeKey = util::RequestSourceTypeKey;
    let _: util::RequestSourceTypeKeyType = util::RequestSourceTypeKey;
    let _: util::RequestSourceKey = util::RequestSourceKey;
    let _: util::RequestSourceKeyType = util::RequestSourceKey;
    let _: util::SessionID = util::SessionID;
}
