// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Stale timestamp helpers for snapshot / stale reads (test-only).
//!
//! This ports the core semantics from client-go's `oracles/pdOracle`:
//! - `UntilExpired`: `physical(lock_ts) + ttl_ms - physical(last_ts)`
//! - `GetStaleTimestamp`: estimate current physical time from last TS + elapsed-since-arrival, then
//!   return a TS about `prev_second` seconds before that estimated time.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
enum StaleTimestampError {
    #[error("get stale timestamp fail, txnScope: {txn_scope}")]
    MissingLastTs { txn_scope: String },
    #[error("invalid prevSecond {prev_second}")]
    InvalidPrevSecond { prev_second: u64 },
    #[error("failed to compose stale ts: {message}")]
    Compose { message: String },
}

#[derive(Clone, Copy, Debug)]
struct LastTso {
    ts: u64,
    arrival: Instant,
}

#[derive(Default)]
struct StaleTsOracle {
    last_tso: Mutex<HashMap<String, LastTso>>,
}

impl StaleTsOracle {
    fn normalize_txn_scope(txn_scope: &str) -> &str {
        if txn_scope.is_empty() {
            crate::GLOBAL_TXN_SCOPE
        } else {
            txn_scope
        }
    }

    fn set_last_ts_for_test(&self, txn_scope: &str, ts: u64) {
        let txn_scope = Self::normalize_txn_scope(txn_scope);
        self.last_tso.lock().unwrap().insert(
            txn_scope.to_owned(),
            LastTso {
                ts,
                arrival: Instant::now(),
            },
        );
    }

    fn get_last_ts(&self, txn_scope: &str) -> Result<LastTso, StaleTimestampError> {
        let txn_scope = Self::normalize_txn_scope(txn_scope);
        self.last_tso
            .lock()
            .unwrap()
            .get(txn_scope)
            .copied()
            .ok_or_else(|| StaleTimestampError::MissingLastTs {
                txn_scope: txn_scope.to_owned(),
            })
    }

    fn until_expired(&self, lock_ts: u64, ttl_ms: u64, txn_scope: &str) -> i64 {
        let Ok(last) = self.get_last_ts(txn_scope) else {
            return 0;
        };
        crate::timestamp::extract_physical(lock_ts) + ttl_ms as i64
            - crate::timestamp::extract_physical(last.ts)
    }

    fn get_stale_timestamp(
        &self,
        txn_scope: &str,
        prev_second: u64,
    ) -> Result<u64, StaleTimestampError> {
        let last = self.get_last_ts(txn_scope)?;
        self.get_stale_timestamp_with_last(last, prev_second)
    }

    fn get_stale_timestamp_with_last(
        &self,
        last: LastTso,
        prev_second: u64,
    ) -> Result<u64, StaleTimestampError> {
        let physical_ms = crate::timestamp::extract_physical(last.ts);
        if physical_ms < 0 {
            return Err(StaleTimestampError::InvalidPrevSecond { prev_second });
        }

        let physical_unix_secs = (physical_ms as u64) / 1000;
        if physical_unix_secs <= prev_second {
            return Err(StaleTimestampError::InvalidPrevSecond { prev_second });
        }

        let elapsed_ms: i128 = last
            .arrival
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(i128::MAX);
        let prev_ms: i128 = (prev_second as i128) * 1000;
        let stale_physical_ms: i64 = (physical_ms as i128 + elapsed_ms - prev_ms)
            .try_into()
            .map_err(|_| StaleTimestampError::InvalidPrevSecond { prev_second })?;

        crate::timestamp::compose_ts(stale_physical_ms, 0)
            .map_err(|e| StaleTimestampError::Compose {
                message: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    #[test]
    fn until_expired_matches_client_go_test() {
        let lock_after_ms: u64 = 10;
        let lock_exp_ms: u64 = 15;

        let o = StaleTsOracle::default();

        let start = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let last_ts = crate::timestamp::time_to_ts(start).unwrap();
        o.set_last_ts_for_test(crate::GLOBAL_TXN_SCOPE, last_ts);

        let lock_ts = crate::timestamp::time_to_ts(start + Duration::from_millis(lock_after_ms))
            .unwrap()
            + 1;
        let wait_ms = o.until_expired(lock_ts, lock_exp_ms, crate::GLOBAL_TXN_SCOPE);
        assert_eq!(wait_ms, (lock_after_ms + lock_exp_ms) as i64);
    }

    #[test]
    fn get_stale_timestamp_matches_client_go_test() {
        let o = StaleTsOracle::default();

        let start = SystemTime::now();
        let last_ts = crate::timestamp::time_to_ts(start).unwrap();
        o.set_last_ts_for_test(crate::GLOBAL_TXN_SCOPE, last_ts);

        let ts = o
            .get_stale_timestamp(crate::GLOBAL_TXN_SCOPE, 10)
            .unwrap();
        let stale_time = crate::timestamp::get_time_from_ts(ts);

        let expected = start - Duration::from_secs(10);
        let diff = match stale_time.duration_since(expected) {
            Ok(d) => d,
            Err(e) => e.duration(),
        };
        assert!(
            diff <= Duration::from_secs(2),
            "unexpected stale ts drift: diff={diff:?}, stale_time={stale_time:?}, expected={expected:?}",
        );

        let err = o
            .get_stale_timestamp(crate::GLOBAL_TXN_SCOPE, 1_000_000_000_000)
            .expect_err("expected invalid prevSecond error");
        assert!(err.to_string().contains("invalid prevSecond"));

        let err = o
            .get_stale_timestamp(crate::GLOBAL_TXN_SCOPE, u64::MAX)
            .expect_err("expected invalid prevSecond error");
        assert!(err.to_string().contains("invalid prevSecond"));
    }
}

