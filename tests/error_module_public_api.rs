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
}

#[test]
fn error_module_exports_result_alias() {
    fn assert_result<T>(_result: error::Result<T>) {}
    assert_result::<()>(Ok(()));
}
