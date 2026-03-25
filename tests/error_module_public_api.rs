use tikv_client::error;

#[test]
fn error_module_exports_error_types() {
    let _ = error::Error::Unimplemented;

    let _: Option<error::DeadlockError> = None;
    let _: Option<error::WriteConflictError> = None;
    let _: Option<error::AssertionFailedError> = None;
}

#[test]
fn error_module_exports_result_alias() {
    fn assert_result<T>(_result: error::Result<T>) {}
    assert_result::<()>(Ok(()));
}
