// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Read-ts validation for snapshot / stale reads (test-only).
//!
//! This ports the core semantics of client-go's `oracles/pdOracle.ValidateReadTS`:
//! - reject "latest stale read" sentinel (`u64::MAX`) for stale read
//! - detect obviously-invalid timestamps (MaxInt64 <= ts < MaxUint64)
//! - validate `read_ts <= current_ts` using a singleflight GetTS to avoid stampeding PD
//! - retry once to avoid false-positives when the singleflight returns an older TS
//! - cancellation of one waiter must not cancel the shared GetTS

use std::collections::HashMap;
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

static ENABLE_TS_VALIDATION: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
enum ReadTsValidationError {
    #[error("MaxInt64 <= read_ts < MaxUint64, read_ts={read_ts}")]
    InvalidReadTs { read_ts: u64 },
    #[error("latest stale read is unavailable")]
    LatestStaleRead,
    #[error("read_ts {read_ts} is in the future of current_ts {current_ts}")]
    FutureTsRead { read_ts: u64, current_ts: u64 },
    #[error("context canceled")]
    Canceled,
    #[error("failed to fetch current ts for validation: {message}")]
    GetTimestamp { message: String },
}

#[async_trait]
trait TsoProvider: Send + Sync + 'static {
    async fn get_timestamp(&self) -> Result<u64, String>;
}

#[derive(Clone, Default)]
struct CancelToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancelToken {
    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

async fn cancelled(token: Option<&CancelToken>) {
    if let Some(token) = token {
        token.cancelled().await;
    } else {
        pending::<()>().await;
    }
}

#[derive(Default)]
struct InFlight {
    result: Mutex<Option<Result<u64, ReadTsValidationError>>>,
    notify: Notify,
}

impl InFlight {
    fn new() -> Self {
        Self::default()
    }

    async fn set_result(&self, result: Result<u64, ReadTsValidationError>) {
        *self.result.lock().await = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self, cancel: Option<&CancelToken>) -> Result<u64, ReadTsValidationError> {
        loop {
            if let Some(res) = { self.result.lock().await.clone() } {
                return res;
            }

            tokio::select! {
                _ = self.notify.notified() => {},
                _ = cancelled(cancel) => return Err(ReadTsValidationError::Canceled),
            }
        }
    }
}

struct ReadTsValidator<P: TsoProvider> {
    provider: Arc<P>,
    low_resolution_ts: Arc<AtomicU64>,
    inflight: Arc<Mutex<HashMap<String, Arc<InFlight>>>>,
}

impl<P: TsoProvider> ReadTsValidator<P> {
    fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            low_resolution_ts: Arc::new(AtomicU64::new(0)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn set_ts_validation(enabled: bool) {
        ENABLE_TS_VALIDATION.store(enabled, Ordering::SeqCst);
    }

    fn low_resolution_ts(&self) -> u64 {
        self.low_resolution_ts.load(Ordering::SeqCst)
    }

    fn observe_low_resolution_ts(&self, ts: u64) {
        // Atomically "fetch max".
        let mut prev = self.low_resolution_ts.load(Ordering::SeqCst);
        while ts > prev {
            match self.low_resolution_ts.compare_exchange(
                prev,
                ts,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(v) => prev = v,
            }
        }
    }

    async fn get_timestamp(&self) -> Result<u64, ReadTsValidationError> {
        let ts = self
            .provider
            .get_timestamp()
            .await
            .map_err(|e| ReadTsValidationError::GetTimestamp { message: e })?;
        self.observe_low_resolution_ts(ts);
        Ok(ts)
    }

    fn should_skip_retry() -> bool {
        fail::eval("validate_read_ts_retry_get_ts", |arg| arg)
            .flatten()
            .as_deref()
            .is_some_and(|s| s == "skip")
    }

    async fn get_current_ts_for_validation(
        &self,
        cancel: Option<&CancelToken>,
        txn_scope: &str,
    ) -> Result<u64, ReadTsValidationError> {
        let txn_scope = if txn_scope.is_empty() {
            crate::GLOBAL_TXN_SCOPE
        } else {
            txn_scope
        };

        let inflight = {
            let mut map = self.inflight.lock().await;
            if let Some(existing) = map.get(txn_scope) {
                existing.clone()
            } else {
                let inflight = Arc::new(InFlight::new());
                map.insert(txn_scope.to_owned(), inflight.clone());

                let provider = self.provider.clone();
                let low_resolution_ts = self.low_resolution_ts.clone();
                let map_ref = self.inflight.clone();
                let scope_key = txn_scope.to_owned();
                let inflight_ref = inflight.clone();

                tokio::spawn(async move {
                    let result = provider
                        .get_timestamp()
                        .await
                        .map_err(|e| ReadTsValidationError::GetTimestamp { message: e });
                    if let Ok(ts) = result {
                        // Align with client-go: update low-resolution ts as soon as GetTS succeeds.
                        let mut prev = low_resolution_ts.load(Ordering::SeqCst);
                        while ts > prev {
                            match low_resolution_ts.compare_exchange(
                                prev,
                                ts,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => break,
                                Err(v) => prev = v,
                            }
                        }
                    }

                    // Used by tests to block returning the GetTS result while still having updated low-res ts.
                    fail::fail_point!("get_current_ts_for_validation_before_return");

                    inflight_ref.set_result(result).await;
                    map_ref.lock().await.remove(&scope_key);
                });

                inflight
            }
        };

        inflight.wait(cancel).await
    }

    async fn validate_read_ts(
        &self,
        cancel: Option<&CancelToken>,
        read_ts: u64,
        stale_read: bool,
        txn_scope: &str,
    ) -> Result<(), ReadTsValidationError> {
        if !ENABLE_TS_VALIDATION.load(Ordering::SeqCst) {
            return Ok(());
        }

        // For a mistake we've seen in client-go: `MaxInt64 <= ts < MaxUint64`.
        if read_ts == u64::MAX {
            return if stale_read {
                Err(ReadTsValidationError::LatestStaleRead)
            } else {
                Ok(())
            };
        }
        if read_ts >= i64::MAX as u64 && read_ts < u64::MAX {
            return Err(ReadTsValidationError::InvalidReadTs { read_ts });
        }

        let mut retrying = false;
        loop {
            let latest_low_res = self.low_resolution_ts();
            if latest_low_res == 0 || read_ts > latest_low_res {
                let current_ts = self
                    .get_current_ts_for_validation(cancel, txn_scope)
                    .await?;
                if read_ts > current_ts {
                    if !retrying && !Self::should_skip_retry() {
                        retrying = true;
                        continue;
                    }
                    return Err(ReadTsValidationError::FutureTsRead {
                        read_ts,
                        current_ts,
                    });
                }
            }
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;
    use std::time::Duration;

    use serial_test::serial;

    use super::*;

    struct Defer<F: FnOnce()> {
        f: Option<F>,
    }

    impl<F: FnOnce()> Defer<F> {
        fn new(f: F) -> Self {
            Self { f: Some(f) }
        }
    }

    impl<F: FnOnce()> Drop for Defer<F> {
        fn drop(&mut self) {
            let Some(f) = self.f.take() else {
                return;
            };
            f();
        }
    }

    #[derive(Default)]
    struct MockPdClient {
        logical: AtomicI64,
    }

    impl MockPdClient {
        fn advance(&self, by: i64) {
            self.logical.fetch_add(by, Ordering::SeqCst);
        }

        fn store(&self, v: i64) {
            self.logical.store(v, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl TsoProvider for MockPdClient {
        async fn get_timestamp(&self) -> Result<u64, String> {
            let logical = self.logical.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(logical as u64)
        }
    }

    struct MockPdClientWithPause {
        inner: MockPdClient,
        gate: Arc<tokio::sync::Mutex<()>>,
        paused: std::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
    }

    impl Default for MockPdClientWithPause {
        fn default() -> Self {
            Self {
                inner: MockPdClient::default(),
                gate: Arc::new(tokio::sync::Mutex::new(())),
                paused: std::sync::Mutex::new(None),
            }
        }
    }

    impl MockPdClientWithPause {
        async fn pause(&self) {
            let guard = self.gate.clone().lock_owned().await;
            *self.paused.lock().unwrap() = Some(guard);
        }

        fn resume(&self) {
            self.paused.lock().unwrap().take();
        }

        fn store(&self, v: i64) {
            self.inner.store(v);
        }
    }

    #[async_trait]
    impl TsoProvider for MockPdClientWithPause {
        async fn get_timestamp(&self) -> Result<u64, String> {
            let _guard = self.gate.lock().await;
            self.inner.get_timestamp().await
        }
    }

    #[tokio::test]
    #[serial]
    async fn validate_read_ts_matches_client_go_test() {
        let scenario = fail::FailScenario::setup();
        ReadTsValidator::<MockPdClient>::set_ts_validation(true);
        let _guard = Defer::new(move || {
            ReadTsValidator::<MockPdClient>::set_ts_validation(false);
            scenario.teardown();
        });

        let pd = Arc::new(MockPdClient::default());
        let validator = ReadTsValidator::new(pd.clone());

        // MaxUint64: stale read rejects, normal read allows.
        assert!(matches!(
            validator
                .validate_read_ts(None, u64::MAX, true, crate::GLOBAL_TXN_SCOPE)
                .await,
            Err(ReadTsValidationError::LatestStaleRead)
        ));
        assert!(validator
            .validate_read_ts(None, u64::MAX, false, crate::GLOBAL_TXN_SCOPE)
            .await
            .is_ok());

        let ts = validator.get_timestamp().await.unwrap();
        assert!(ts >= 1);

        validator
            .validate_read_ts(None, 1, true, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();

        // readTS == previous ts + 1: should pass by fetching current ts.
        let ts = validator.get_timestamp().await.unwrap();
        validator
            .validate_read_ts(None, ts + 1, true, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();

        // readTS == previous ts + 2: first check fails with current ts (prev+1), then retries once.
        let ts = validator.get_timestamp().await.unwrap();
        validator
            .validate_read_ts(None, ts + 2, true, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();

        // readTS newer than previous ts + 3: should fail (only retries once).
        let ts = validator.get_timestamp().await.unwrap();
        assert!(matches!(
            validator
                .validate_read_ts(None, ts + 3, true, crate::GLOBAL_TXN_SCOPE)
                .await,
            Err(ReadTsValidationError::FutureTsRead { .. })
        ));

        // Simulate another client advanced PD's logical.
        let ts = validator.get_timestamp().await.unwrap();
        pd.advance(2);
        validator
            .validate_read_ts(None, ts + 3, true, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn validate_read_ts_rejects_invalid_ts_range() {
        let scenario = fail::FailScenario::setup();
        ReadTsValidator::<MockPdClient>::set_ts_validation(true);
        let _guard = Defer::new(move || {
            ReadTsValidator::<MockPdClient>::set_ts_validation(false);
            scenario.teardown();
        });

        let pd = Arc::new(MockPdClient::default());
        let validator = ReadTsValidator::new(pd);

        let invalid = i64::MAX as u64;
        assert!(matches!(
            validator
                .validate_read_ts(None, invalid, true, crate::GLOBAL_TXN_SCOPE)
                .await,
            Err(ReadTsValidationError::InvalidReadTs { read_ts }) if read_ts == invalid
        ));
    }

    #[tokio::test]
    #[serial]
    async fn validate_read_ts_normal_read_matches_client_go_test() {
        let scenario = fail::FailScenario::setup();
        ReadTsValidator::<MockPdClient>::set_ts_validation(true);
        let _guard = Defer::new(move || {
            ReadTsValidator::<MockPdClient>::set_ts_validation(false);
            scenario.teardown();
        });

        let pd = Arc::new(MockPdClient::default());
        let validator = ReadTsValidator::new(pd);

        let ts = validator.get_timestamp().await.unwrap();
        assert!(ts >= 1);

        validator
            .validate_read_ts(None, ts, false, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();

        // Loads `ts + 1` then retries `ts + 2` and passes.
        validator
            .validate_read_ts(None, ts + 2, false, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();

        // Loads `ts + 3` then `ts + 4` and fails.
        assert!(matches!(
            validator
                .validate_read_ts(None, ts + 5, false, crate::GLOBAL_TXN_SCOPE)
                .await,
            Err(ReadTsValidationError::FutureTsRead { read_ts, .. }) if read_ts == ts + 5
        ));

        // Loads `ts + 5` and passes.
        validator
            .validate_read_ts(None, ts + 5, false, crate::GLOBAL_TXN_SCOPE)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn validate_read_ts_singleflight_reuses_get_ts_and_cancellation_does_not_propagate() {
        let scenario = fail::FailScenario::setup();
        ReadTsValidator::<MockPdClientWithPause>::set_ts_validation(true);
        let _guard = Defer::new(move || {
            ReadTsValidator::<MockPdClientWithPause>::set_ts_validation(false);
            fail::remove("validate_read_ts_retry_get_ts");
            scenario.teardown();
        });

        fail::cfg("validate_read_ts_retry_get_ts", "return(skip)").unwrap();

        let pd = Arc::new(MockPdClientWithPause::default());
        let validator = Arc::new(ReadTsValidator::new(pd.clone()));

        // Cancel the 0-th/1-st validation call in some rounds (align with client-go test).
        let cancel_indices: [isize; 4] = [-1, -1, 0, 1];

        for (case_index, ts) in [100_u64, 200, 300, 400].into_iter().enumerate() {
            let cancel_index = cancel_indices[case_index];

            pd.pause().await;

            let cancelable = CancelToken::default();

            let make_token = |idx: usize| -> Option<CancelToken> {
                if cancel_index == idx as isize {
                    Some(cancelable.clone())
                } else {
                    None
                }
            };

            let read_ts = [ts - 2, ts + 2, ts - 1, ts + 1, ts];
            let expected_ok = [true, false, true, false, true];

            let mut handles = Vec::new();
            for (idx, &read_ts) in read_ts.iter().enumerate() {
                let token = make_token(idx);
                let validator = validator.clone();
                handles.push((
                    idx,
                    tokio::spawn(async move {
                        validator
                            .validate_read_ts(
                                token.as_ref(),
                                read_ts,
                                true,
                                crate::GLOBAL_TXN_SCOPE,
                            )
                            .await
                    }),
                ));
            }

            // Ensure all validations are blocked on the paused GetTS.
            tokio::time::sleep(Duration::from_millis(50)).await;
            for (_, h) in &handles {
                assert!(!h.is_finished(), "expected validation to be blocked");
            }

            cancelable.cancel();

            // Cancelled call should return; others remain blocked.
            if cancel_index >= 0 {
                let pos = handles
                    .iter()
                    .position(|(idx, _)| *idx == cancel_index as usize)
                    .expect("canceled validation handle must exist");
                let (_, handle) = handles.swap_remove(pos);
                let err = handle
                    .await
                    .expect("validation task join")
                    .expect_err("expected cancellation error");
                assert!(matches!(err, ReadTsValidationError::Canceled));
            }
            for (_, h) in &handles {
                assert!(!h.is_finished(), "non-canceled call should still block");
            }

            pd.store((ts - 1) as i64);
            pd.resume();

            // Collect the rest results.
            for (idx, h) in handles.into_iter() {
                let res = h.await.expect("validation task join");
                if expected_ok[idx] {
                    res.unwrap();
                } else {
                    assert!(res.is_err(), "expected validation to fail");
                    assert!(!matches!(res, Err(ReadTsValidationError::Canceled)));
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn validate_read_ts_retry_avoids_false_positive_from_singleflight_reuse() {
        let scenario = fail::FailScenario::setup();
        ReadTsValidator::<MockPdClient>::set_ts_validation(true);
        let _guard = Defer::new(move || {
            ReadTsValidator::<MockPdClient>::set_ts_validation(false);
            fail::remove("get_current_ts_for_validation_before_return");
            scenario.teardown();
        });

        let pd = Arc::new(MockPdClient::default());
        let validator = Arc::new(ReadTsValidator::new(pd.clone()));

        let ts = validator.get_timestamp().await.unwrap();
        assert_eq!(validator.low_resolution_ts(), ts);

        // Block the singleflight return after the GetTS has already updated low-res ts.
        fail::cfg("get_current_ts_for_validation_before_return", "pause").unwrap();

        let v1 = validator.clone();
        let h1 = tokio::spawn(async move {
            v1.validate_read_ts(None, ts + 1, false, crate::GLOBAL_TXN_SCOPE)
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!h1.is_finished(), "expected first validation to be blocked");

        // Wait until low-res ts is updated to `ts + 1` while the validation result is still blocked.
        tokio::time::timeout(Duration::from_secs(1), async {
            while validator.low_resolution_ts() < ts + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("low resolution ts must be updated");
        assert_eq!(validator.low_resolution_ts(), ts + 1);

        // Simulate another client advanced PD and got a new timestamp.
        pd.advance(10);
        let next_ts = pd.get_timestamp().await.unwrap();
        assert!(next_ts > ts + 1);
        assert_eq!(validator.low_resolution_ts(), ts + 1);

        let v2 = validator.clone();
        let h2 = tokio::spawn(async move {
            v2.validate_read_ts(None, next_ts, false, crate::GLOBAL_TXN_SCOPE)
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !h1.is_finished(),
            "first validation should still be blocked"
        );
        assert!(
            !h2.is_finished(),
            "second validation should still be blocked"
        );

        fail::remove("get_current_ts_for_validation_before_return");
        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();
    }
}
