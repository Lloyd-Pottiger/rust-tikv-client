use tikv_client::error;

#[test]
fn error_module_exports_error_types() {
    let _ = error::Error::Unimplemented;
    let _ = error::Error::CommitTsLag {
        message: String::new(),
    };
    let _: fn(&error::Error) -> bool = error::is_error_commit_ts_lag;
    let _: fn(&error::Error) -> bool = error::is_error_undetermined;
    let _: fn(&error::Error) -> bool = error::is_err_key_exist;
    let _: fn(&error::Error) -> bool = error::is_err_write_conflict;

    let _: Option<error::DeadlockError> = None;
    let _: Option<error::WriteConflictError> = None;
    let _: Option<error::AssertionFailedError> = None;
    let _: Option<error::TokenLimitError> = None;
    let _: Option<error::PdServerTimeoutError> = None;
}

#[test]
fn error_module_exports_result_alias() {
    fn assert_result<T>(_result: error::Result<T>) {}
    assert_result::<()>(Ok(()));
}

#[test]
fn error_module_exports_proto_aliases() {
    let _: Option<error::ProtoRegionError> = None;
    let _: Option<error::ProtoKeyError> = None;
    let _: Option<error::ProtoDeadlock> = None;
    let _: Option<error::ProtoWriteConflict> = None;
    let _: Option<error::ProtoAssertionFailed> = None;
    let _: fn(&error::ProtoKeyError) -> String = error::extract_debug_info_str_from_key_error;
}

#[test]
fn error_module_extract_debug_info_helper_handles_missing_debug_info() {
    let key_error = error::ProtoKeyError::default();
    assert_eq!(
        error::extract_debug_info_str_from_key_error(&key_error),
        String::new()
    );
}
