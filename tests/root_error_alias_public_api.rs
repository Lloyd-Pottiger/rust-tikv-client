use tikv_client::proto::{deadlock, kvrpcpb};
use tikv_client::{
    AssertionFailedError, DeadlockError, PdServerTimeoutError, ProtoAssertionFailed, ProtoDeadlock,
    ProtoWriteConflict, TokenLimitError, WriteConflictError,
};

#[test]
fn crate_root_exports_proto_backed_error_aliases() {
    let deadlock = DeadlockError::new(ProtoDeadlock {
        lock_ts: 11,
        deadlock_key: b"dead".to_vec(),
        deadlock_key_hash: 22,
        wait_chain: vec![deadlock::WaitForEntry::default()],
        ..Default::default()
    });
    assert_eq!(deadlock.lock_ts(), 11);
    assert_eq!(deadlock.deadlock_key(), b"dead");
    assert_eq!(deadlock.deadlock_key_hash(), 22);
    assert_eq!(deadlock.wait_chain_len(), 1);
    assert_eq!(deadlock.deadlock().lock_ts, 11);

    let conflict = WriteConflictError::new(ProtoWriteConflict {
        start_ts: 33,
        conflict_ts: 44,
        conflict_commit_ts: 55,
        key: b"k".to_vec(),
        primary: b"p".to_vec(),
        reason: kvrpcpb::write_conflict::Reason::PessimisticRetry.into(),
        ..Default::default()
    });
    assert_eq!(conflict.start_ts(), 33);
    assert_eq!(conflict.conflict_ts(), 44);
    assert_eq!(conflict.conflict_commit_ts(), 55);
    assert_eq!(conflict.key(), b"k");
    assert_eq!(conflict.primary(), b"p");
    assert_eq!(
        conflict.reason(),
        kvrpcpb::write_conflict::Reason::PessimisticRetry
    );
    assert_eq!(conflict.write_conflict().start_ts, 33);

    let assertion = AssertionFailedError::new(ProtoAssertionFailed {
        start_ts: 66,
        key: b"assert".to_vec(),
        assertion: kvrpcpb::Assertion::Exist.into(),
        existing_start_ts: 77,
        existing_commit_ts: 88,
        ..Default::default()
    });
    assert_eq!(assertion.start_ts(), 66);
    assert_eq!(assertion.key(), b"assert");
    assert_eq!(assertion.assertion(), Some(kvrpcpb::Assertion::Exist));
    assert_eq!(assertion.existing_start_ts(), 77);
    assert_eq!(assertion.existing_commit_ts(), 88);
    assert_eq!(assertion.assertion_failed().start_ts, 66);
}

#[test]
fn crate_root_exports_simple_error_aliases() {
    let token_limit = TokenLimitError::new(99);
    assert_eq!(token_limit.store_id(), 99);

    let timeout = PdServerTimeoutError::new("pd timeout");
    assert_eq!(timeout.message(), "pd timeout");
}
