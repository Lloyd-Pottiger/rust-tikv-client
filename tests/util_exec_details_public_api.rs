use tikv_client::util;

#[test]
fn util_exec_details_public_api_exposes_context_key_markers() {
    let _: util::CommitDetailCtxKey = util::CommitDetailCtxKey;
    let _: util::LockKeysDetailCtxKey = util::LockKeysDetailCtxKey;
    let _: util::ExecDetailsKey = util::ExecDetailsKey;
    let _: util::RUDetailsCtxKey = util::RUDetailsCtxKey;

    std::mem::drop(util::with_trace_exec_details(async {}));
    let _ = util::trace_exec_details_enabled();
}
