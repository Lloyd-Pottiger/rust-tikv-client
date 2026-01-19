// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Test-only local timestamp oracle.
//!
//! This ports the core behavior of client-go's `oracles/localOracle` used in unit tests:
//! - `GetTimestamp`: `time_to_ts(now)` plus an in-ms logical counter to ensure uniqueness.
//! - `IsExpired`: `now >= lock_ts + ttl_ms`.
//! - `UntilExpired`: `physical(lock_ts) + ttl_ms - physical(now)`.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

#[derive(Default)]
struct LocalOracle {
    state: Mutex<State>,
    hook_current_time: Mutex<Option<SystemTime>>,
}

#[derive(Default)]
struct State {
    last_ts: u64,
    n: u64,
}

impl LocalOracle {
    fn now(&self) -> SystemTime {
        self.hook_current_time
            .lock()
            .unwrap()
            .unwrap_or_else(SystemTime::now)
    }

    fn set_current_time_for_test(&self, t: SystemTime) {
        *self.hook_current_time.lock().unwrap() = Some(t);
    }

    fn get_timestamp(&self) -> u64 {
        let now = self.now();
        let ts = crate::timestamp::time_to_ts(now).expect("time_to_ts should not overflow");

        let mut state = self.state.lock().unwrap();
        if state.last_ts == ts {
            state.n += 1;
            return ts + state.n;
        }

        state.last_ts = ts;
        state.n = 0;
        ts
    }

    fn is_expired(&self, lock_ts: u64, ttl_ms: u64) -> bool {
        let now = self.now();
        let expire = crate::timestamp::get_time_from_ts(lock_ts) + Duration::from_millis(ttl_ms);
        now >= expire
    }

    fn until_expired(&self, lock_ts: u64, ttl_ms: u64) -> i64 {
        let now = self.now();
        crate::timestamp::extract_physical(lock_ts) + ttl_ms as i64
            - crate::timestamp::get_physical(now).expect("SystemTime must be representable")
    }
}

#[test]
fn local_oracle_generates_unique_timestamps() {
    let o = LocalOracle::default();
    let mut seen = HashSet::new();
    for _ in 0..100_000 {
        seen.insert(o.get_timestamp());
    }
    assert_eq!(seen.len(), 100_000);
}

#[test]
fn local_oracle_is_expired_matches_client_go_test() {
    let o = LocalOracle::default();
    let start = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
    o.set_current_time_for_test(start);
    let ts = o.get_timestamp();

    o.set_current_time_for_test(start + Duration::from_millis(10));
    assert!(o.is_expired(ts, 5));
    assert!(!o.is_expired(ts, 200));
}

#[test]
fn local_oracle_until_expired_matches_client_go_test() {
    let o = LocalOracle::default();
    let start = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
    o.set_current_time_for_test(start);
    let ts = o.get_timestamp();

    o.set_current_time_for_test(start + Duration::from_millis(10));
    assert_eq!(o.until_expired(ts, 6), -4);
    assert_eq!(o.until_expired(ts, 14), 4);
}
