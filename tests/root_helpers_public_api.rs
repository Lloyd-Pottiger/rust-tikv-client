use tikv_client::proto::kvrpcpb;
use tikv_client::{
    extract_debug_info_str_from_key_error, is_err_key_exist, is_err_write_conflict,
    is_error_commit_ts_lag, is_error_undetermined, load_shutting_down, store_shutting_down, Error,
    ProtoAssertionFailed, ProtoDeadlock, ProtoKeyError, ProtoRegionError, ProtoWriteConflict,
    SecurityManager,
};

struct ShuttingDownGuard(u32);

impl Drop for ShuttingDownGuard {
    fn drop(&mut self) {
        store_shutting_down(self.0);
    }
}

fn push_shutting_down(v: u32) -> ShuttingDownGuard {
    let prev = load_shutting_down();
    store_shutting_down(v);
    ShuttingDownGuard(prev)
}

#[test]
fn crate_root_exports_common_proto_aliases_and_helpers() {
    let _: Option<SecurityManager> = None;
    let _: Option<ProtoAssertionFailed> = None;
    let _: Option<ProtoDeadlock> = None;
    let _: Option<ProtoKeyError> = None;
    let _: Option<ProtoRegionError> = None;
    let _: Option<ProtoWriteConflict> = None;

    let _: fn(&Error) -> bool = is_error_commit_ts_lag;
    let _: fn(&Error) -> bool = is_error_undetermined;
    let _: fn(&Error) -> bool = is_err_key_exist;
    let _: fn(&Error) -> bool = is_err_write_conflict;

    let key_error = kvrpcpb::KeyError::default();
    assert_eq!(extract_debug_info_str_from_key_error(&key_error), "");

    let key_error = kvrpcpb::KeyError {
        debug_info: Some(kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: b"abc".to_vec(),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let debug_info = extract_debug_info_str_from_key_error(&key_error);
    assert!(!debug_info.is_empty());
    assert!(debug_info.contains("\"key\":\"YWJj\""));
}

#[test]
fn crate_root_exports_shutting_down_helpers() {
    let _guard = push_shutting_down(0);

    store_shutting_down(7);
    assert_eq!(load_shutting_down(), 7);

    store_shutting_down(0);
    assert_eq!(load_shutting_down(), 0);
}
