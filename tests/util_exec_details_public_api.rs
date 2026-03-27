use tikv_client::util;

#[test]
fn util_exec_details_public_api_exposes_context_key_markers() {
    let _: util::CommitDetailCtxKey = util::CommitDetailCtxKey;
    let _: util::LockKeysDetailCtxKey = util::LockKeysDetailCtxKey;
    let _: util::ExecDetailsKey = util::ExecDetailsKey;
    let _: util::RUDetailsCtxKey = util::RUDetailsCtxKey;

    let ru_details = std::sync::Arc::new(util::RUDetails::new());
    std::mem::drop(util::with_ru_details(ru_details, async {}));
    let _ = util::ru_details();

    std::mem::drop(util::with_trace_exec_details(async {}));
    let _ = util::trace_exec_details_enabled();
}

#[test]
fn util_exec_details_public_api_exposes_constructor_helpers() {
    let _: fn(Option<&tikv_client::proto::kvrpcpb::ExecDetailsV2>) -> util::TiKVExecDetails =
        util::new_tikv_exec_details;
    let _: fn() -> util::RUDetails = util::new_ru_details;
    let _: fn(f64, f64, std::time::Duration) -> util::RUDetails = util::new_ru_details_with;

    let exec_details = util::new_tikv_exec_details(None);
    assert_eq!(exec_details, util::TiKVExecDetails::default());

    let ru_details = util::new_ru_details_with(1.5, 2.5, std::time::Duration::from_millis(3));
    assert_eq!(ru_details.rru(), 1.5);
    assert_eq!(ru_details.wru(), 2.5);
    assert_eq!(
        ru_details.ru_wait_duration(),
        std::time::Duration::from_millis(3)
    );
}
