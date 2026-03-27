use tikv_client::proto::kvrpcpb;
use tikv_client::redact;

struct RedactModeGuard(redact::RedactMode);

impl Drop for RedactModeGuard {
    fn drop(&mut self) {
        redact::set_redact_mode(self.0);
    }
}

fn push_redact_mode(mode: redact::RedactMode) -> RedactModeGuard {
    let prev = if redact::need_redact() {
        redact::RedactMode::Enable
    } else {
        redact::RedactMode::Disable
    };
    redact::set_redact_mode(mode);
    RedactModeGuard(prev)
}

#[test]
fn redact_module_exposes_redaction_helpers() {
    let _guard = push_redact_mode(redact::RedactMode::Disable);

    assert!(!redact::need_redact());
    assert_eq!(redact::key(b"\x01\xab"), "01AB");
    assert_eq!(redact::key_bytes(b"\x01\xab"), b"01AB".to_vec());

    redact::set_redact_mode(redact::RedactMode::Enable);
    assert!(redact::need_redact());

    let mut key_error = kvrpcpb::KeyError {
        locked: Some(kvrpcpb::LockInfo {
            primary_lock: b"primary".to_vec(),
            key: b"key".to_vec(),
            ..Default::default()
        }),
        txn_not_found: Some(kvrpcpb::TxnNotFound {
            primary_key: b"missing-primary".to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    };
    redact::redact_key_error_if_necessary(&mut key_error);

    assert_eq!(redact::key(b"\x01\xab"), "?");
    assert_eq!(redact::key_bytes(b"\x01\xab"), vec![b'?']);
    assert_eq!(
        key_error.locked.expect("locked info").primary_lock,
        vec![b'?']
    );
    assert_eq!(
        key_error.txn_not_found.expect("txn not found").primary_key,
        vec![b'?']
    );
}
