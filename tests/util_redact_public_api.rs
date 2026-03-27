use tikv_client::proto::kvrpcpb;
use tikv_client::util;

struct RedactModeGuard(util::redact::RedactMode);

impl Drop for RedactModeGuard {
    fn drop(&mut self) {
        util::redact::set_redact_mode(self.0);
    }
}

fn push_redact_mode(mode: util::redact::RedactMode) -> RedactModeGuard {
    let prev = if util::redact::need_redact() {
        util::redact::RedactMode::Enable
    } else {
        util::redact::RedactMode::Disable
    };
    util::redact::set_redact_mode(mode);
    RedactModeGuard(prev)
}

#[test]
fn util_redact_public_api_exposes_redaction_helpers() {
    let _guard = push_redact_mode(util::redact::RedactMode::Disable);

    assert!(!util::redact::need_redact());
    assert_eq!(util::redact::key(b"\x01\xab"), "01AB");
    assert_eq!(util::redact::key_bytes(b"\x01\xab"), b"01AB".to_vec());

    util::redact::set_redact_mode(util::redact::RedactMode::Enable);
    assert!(util::redact::need_redact());

    let mut key_error = kvrpcpb::KeyError {
        locked: Some(kvrpcpb::LockInfo {
            primary_lock: b"primary".to_vec(),
            key: b"key".to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    };
    util::redact::redact_key_error_if_necessary(&mut key_error);

    assert_eq!(util::redact::key(b"\x01\xab"), "?");
    assert_eq!(util::redact::key_bytes(b"\x01\xab"), vec![b'?']);
    assert_eq!(
        key_error.locked.expect("locked info").primary_lock,
        vec![b'?']
    );
}
