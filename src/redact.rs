//! Redaction helpers for keys and other sensitive values.
//!
//! This module mirrors the high-level client-go `util/redact` surface. It provides a process-wide
//! redaction mode plus helpers for formatting keys and protobuf fields without leaking raw key
//! material into logs or error messages.

use std::sync::atomic::{AtomicU8, Ordering};

use crate::kv::HexRepr;
use crate::proto::kvrpcpb;

/// Redaction mode for key material in logs / errors.
///
/// This mirrors the client-go `util/redact` surface at a high level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedactMode {
    Disable = 0,
    Enable = 1,
}

static REDACT_MODE: AtomicU8 = AtomicU8::new(RedactMode::Disable as u8);

/// Returns whether redaction is enabled.
pub fn need_redact() -> bool {
    REDACT_MODE.load(Ordering::Acquire) == (RedactMode::Enable as u8)
}

/// Set the global redaction mode.
pub fn set_redact_mode(mode: RedactMode) {
    REDACT_MODE.store(mode as u8, Ordering::Release);
}

/// Render a key for logging.
///
/// - When redaction is enabled, returns `"?"`.
/// - Otherwise, returns the upper-case hex representation (client-go parity).
pub fn key(key: &[u8]) -> String {
    if need_redact() {
        "?".to_owned()
    } else {
        format!("{}", HexRepr(key))
    }
}

/// Render a key for logging as bytes.
///
/// - When redaction is enabled, returns `[b'?']`.
/// - Otherwise, returns the upper-case hex representation bytes (client-go parity).
pub fn key_bytes(key: &[u8]) -> Vec<u8> {
    if need_redact() {
        vec![b'?']
    } else {
        let mut out = Vec::with_capacity(key.len().saturating_mul(2));
        for &byte in key {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push(HEX[(byte >> 4) as usize]);
            out.push(HEX[(byte & 0x0f) as usize]);
        }
        out
    }
}

/// Redact key fields inside `kvrpcpb::KeyError` in-place (when redaction is enabled).
///
/// This mirrors client-go `util/redact.RedactKeyErrIfNecessary`.
pub fn redact_key_error_if_necessary(err: &mut kvrpcpb::KeyError) {
    if !need_redact() {
        return;
    }

    let marker = vec![b'?'];

    if let Some(info) = err.locked.as_mut() {
        redact_lock_info(info, &marker);
    }
    if let Some(conflict) = err.conflict.as_mut() {
        if !conflict.key.is_empty() {
            conflict.key = marker.clone();
        }
        if !conflict.primary.is_empty() {
            conflict.primary = marker.clone();
        }
    }
    if let Some(already_exist) = err.already_exist.as_mut() {
        if !already_exist.key.is_empty() {
            already_exist.key = marker.clone();
        }
    }
    if let Some(deadlock) = err.deadlock.as_mut() {
        if !deadlock.lock_key.is_empty() {
            deadlock.lock_key = marker.clone();
        }
        if !deadlock.deadlock_key.is_empty() {
            deadlock.deadlock_key = marker.clone();
        }
        for entry in &mut deadlock.wait_chain {
            if !entry.key.is_empty() {
                entry.key = marker.clone();
            }
        }
    }
    if let Some(expired) = err.commit_ts_expired.as_mut() {
        if !expired.key.is_empty() {
            expired.key = marker.clone();
        }
    }
    if let Some(not_found) = err.txn_not_found.as_mut() {
        if !not_found.primary_key.is_empty() {
            not_found.primary_key = marker.clone();
        }
    }
    if let Some(lock_not_found) = err.txn_lock_not_found.as_mut() {
        if !lock_not_found.key.is_empty() {
            lock_not_found.key = marker.clone();
        }
    }
    if let Some(assertion_failed) = err.assertion_failed.as_mut() {
        if !assertion_failed.key.is_empty() {
            assertion_failed.key = marker.clone();
        }
    }
    if let Some(primary_mismatch) = err.primary_mismatch.as_mut() {
        if let Some(info) = primary_mismatch.lock_info.as_mut() {
            redact_lock_info(info, &marker);
        }
    }
    if let Some(debug_info) = err.debug_info.as_mut() {
        for mvcc_info in &mut debug_info.mvcc_info {
            if !mvcc_info.key.is_empty() {
                mvcc_info.key = marker.clone();
            }
            if let Some(mvcc) = mvcc_info.mvcc.as_mut() {
                if let Some(lock) = mvcc.lock.as_mut() {
                    if !lock.primary.is_empty() {
                        lock.primary = marker.clone();
                    }
                    if !lock.short_value.is_empty() {
                        lock.short_value = marker.clone();
                    }
                    for secondary in &mut lock.secondaries {
                        *secondary = marker.clone();
                    }
                }
                for write in &mut mvcc.writes {
                    if !write.short_value.is_empty() {
                        write.short_value = marker.clone();
                    }
                }
                for value in &mut mvcc.values {
                    if !value.value.is_empty() {
                        value.value = marker.clone();
                    }
                }
            }
        }
    }
}

fn redact_lock_info(info: &mut kvrpcpb::LockInfo, marker: &Vec<u8>) {
    if !info.primary_lock.is_empty() {
        info.primary_lock = marker.clone();
    }
    if !info.key.is_empty() {
        info.key = marker.clone();
    }
    for secondary in &mut info.secondaries {
        *secondary = marker.clone();
    }
    for shared in &mut info.shared_lock_infos {
        redact_lock_info(shared, marker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RedactGuard {
        mode: RedactMode,
    }

    impl Drop for RedactGuard {
        fn drop(&mut self) {
            set_redact_mode(self.mode);
        }
    }

    fn set_redact_mode_scoped(mode: RedactMode) -> RedactGuard {
        let prev = if need_redact() {
            RedactMode::Enable
        } else {
            RedactMode::Disable
        };
        set_redact_mode(mode);
        RedactGuard { mode: prev }
    }

    #[test]
    fn test_key_and_key_bytes_match_client_go_hex_and_redaction() {
        let _guard = set_redact_mode_scoped(RedactMode::Disable);
        assert!(!need_redact());
        assert_eq!(key(&[0x01, 0xAB]), "01AB");
        assert_eq!(key_bytes(&[0x01, 0xAB]), b"01AB".to_vec());

        set_redact_mode(RedactMode::Enable);
        assert!(need_redact());
        assert_eq!(key(&[0x01, 0xAB]), "?");
        assert_eq!(key_bytes(&[0x01, 0xAB]), vec![b'?']);
    }

    #[test]
    fn test_redact_key_error_if_necessary_redacts_nested_keys() {
        let _guard = set_redact_mode_scoped(RedactMode::Enable);

        let mut err = kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                primary_lock: b"p".to_vec(),
                key: b"k".to_vec(),
                secondaries: vec![b"s".to_vec()],
                shared_lock_infos: vec![kvrpcpb::LockInfo {
                    primary_lock: b"sp".to_vec(),
                    key: b"sk".to_vec(),
                    secondaries: vec![b"ss".to_vec()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            conflict: Some(kvrpcpb::WriteConflict {
                key: b"ck".to_vec(),
                primary: b"cp".to_vec(),
                ..Default::default()
            }),
            already_exist: Some(kvrpcpb::AlreadyExist {
                key: b"ek".to_vec(),
            }),
            deadlock: Some(kvrpcpb::Deadlock {
                lock_key: b"lk".to_vec(),
                deadlock_key: b"dk".to_vec(),
                wait_chain: vec![crate::proto::deadlock::WaitForEntry {
                    key: b"wk".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                key: b"xk".to_vec(),
                ..Default::default()
            }),
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                primary_key: b"npk".to_vec(),
                ..Default::default()
            }),
            txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                key: b"tlk".to_vec(),
            }),
            debug_info: Some(kvrpcpb::DebugInfo {
                mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                    key: b"di_k".to_vec(),
                    mvcc: Some(kvrpcpb::MvccInfo {
                        lock: Some(kvrpcpb::MvccLock {
                            primary: b"di_p".to_vec(),
                            short_value: b"di_sv".to_vec(),
                            secondaries: vec![b"di_s".to_vec()],
                            ..Default::default()
                        }),
                        writes: vec![kvrpcpb::MvccWrite {
                            short_value: b"di_wsv".to_vec(),
                            ..Default::default()
                        }],
                        values: vec![kvrpcpb::MvccValue {
                            value: b"di_v".to_vec(),
                            ..Default::default()
                        }],
                    }),
                }],
            }),
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                key: b"ak".to_vec(),
                ..Default::default()
            }),
            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                lock_info: Some(kvrpcpb::LockInfo {
                    primary_lock: b"mp".to_vec(),
                    key: b"mk".to_vec(),
                    secondaries: vec![b"ms".to_vec()],
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };

        redact_key_error_if_necessary(&mut err);

        let marker = vec![b'?'];
        let locked = err.locked.unwrap();
        assert_eq!(locked.primary_lock, marker);
        assert_eq!(locked.key, vec![b'?']);
        assert_eq!(locked.secondaries, vec![vec![b'?']]);
        assert_eq!(locked.shared_lock_infos.len(), 1);
        assert_eq!(locked.shared_lock_infos[0].primary_lock, vec![b'?']);
        assert_eq!(locked.shared_lock_infos[0].key, vec![b'?']);
        assert_eq!(locked.shared_lock_infos[0].secondaries, vec![vec![b'?']]);

        let conflict = err.conflict.unwrap();
        assert_eq!(conflict.key, vec![b'?']);
        assert_eq!(conflict.primary, vec![b'?']);

        assert_eq!(err.already_exist.unwrap().key, vec![b'?']);

        let deadlock = err.deadlock.unwrap();
        assert_eq!(deadlock.lock_key, vec![b'?']);
        assert_eq!(deadlock.deadlock_key, vec![b'?']);
        assert_eq!(deadlock.wait_chain.len(), 1);
        assert_eq!(deadlock.wait_chain[0].key, vec![b'?']);

        assert_eq!(err.commit_ts_expired.unwrap().key, vec![b'?']);
        assert_eq!(err.txn_not_found.unwrap().primary_key, vec![b'?']);
        assert_eq!(err.txn_lock_not_found.unwrap().key, vec![b'?']);
        assert_eq!(err.assertion_failed.unwrap().key, vec![b'?']);

        let mismatch = err.primary_mismatch.unwrap();
        let mismatch_lock = mismatch.lock_info.unwrap();
        assert_eq!(mismatch_lock.primary_lock, vec![b'?']);
        assert_eq!(mismatch_lock.key, vec![b'?']);
        assert_eq!(mismatch_lock.secondaries, vec![vec![b'?']]);

        let debug_info = err.debug_info.unwrap();
        assert_eq!(debug_info.mvcc_info.len(), 1);
        let debug_mvcc = &debug_info.mvcc_info[0];
        assert_eq!(debug_mvcc.key, vec![b'?']);
        let mvcc = debug_mvcc.mvcc.as_ref().expect("expected mvcc info");
        let lock = mvcc.lock.as_ref().expect("expected mvcc lock");
        assert_eq!(lock.primary, vec![b'?']);
        assert_eq!(lock.short_value, vec![b'?']);
        assert_eq!(lock.secondaries, vec![vec![b'?']]);
        assert_eq!(mvcc.writes.len(), 1);
        assert_eq!(mvcc.writes[0].short_value, vec![b'?']);
        assert_eq!(mvcc.values.len(), 1);
        assert_eq!(mvcc.values[0].value, vec![b'?']);
    }
}
