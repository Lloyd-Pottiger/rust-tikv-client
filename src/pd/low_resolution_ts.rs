// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Low-resolution timestamp cache and adaptive update interval (test-only).
//!
//! This ports client-go `oracles/pdOracle` low-resolution timestamp semantics:
//! - `SetLowResolutionTimestampUpdateInterval`: update configured interval; update adaptive interval
//!   immediately only when the adaptive mechanism is not taking effect (or when shrinking).
//! - `updateTS` loop: periodically refresh per-scope cached timestamps (driven by a ticker).
//! - adaptive interval state machine (`nextUpdateInterval`) and trigger (`adjust...`).
//! - `setLastTS` must always push (monotonic; concurrent `GetTimestamp` cannot regress cached TS).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

const MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL: Duration = Duration::from_millis(500);
const ADAPTIVE_UPDATE_TS_INTERVAL_SHRINKING_PRESERVE: Duration = Duration::from_millis(100);
const ADAPTIVE_UPDATE_TS_INTERVAL_BLOCK_RECOVER_THRESHOLD: Duration = Duration::from_millis(200);
const ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND: Duration = Duration::from_millis(20);
const ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdaptiveUpdateTsIntervalState {
    None,
    Normal,
    Adapting,
    Recovering,
    Unadjustable,
}

#[derive(Debug)]
struct AdaptiveUpdateIntervalState {
    inner: Mutex<AdaptiveUpdateIntervalInner>,
    last_short_staleness_read_time_ms: AtomicI64,
}

#[derive(Debug)]
struct AdaptiveUpdateIntervalInner {
    last_tick_ms: i64,
    state: AdaptiveUpdateTsIntervalState,
}

#[async_trait]
trait TsoProvider: Send + Sync + 'static {
    async fn get_timestamp(&self, txn_scope: &str) -> Result<u64, String>;
}

struct LowResolutionTsOracle<P: TsoProvider> {
    provider: Arc<P>,
    last_ts: Mutex<HashMap<String, Arc<AtomicU64>>>,
    last_ts_update_interval_ns: AtomicU64,
    adaptive_last_ts_update_interval_ns: AtomicU64,
    adaptive_state: AdaptiveUpdateIntervalState,
    shrink_interval_tx: mpsc::Sender<Duration>,
}

impl<P: TsoProvider> LowResolutionTsOracle<P> {
    fn new(
        provider: Arc<P>,
        update_interval: Duration,
        now_ms: i64,
    ) -> (Arc<Self>, mpsc::Receiver<Duration>) {
        let (shrink_interval_tx, shrink_interval_rx) = mpsc::channel(1);
        let oracle = Arc::new(Self {
            provider,
            last_ts: Mutex::new(HashMap::new()),
            last_ts_update_interval_ns: AtomicU64::new(update_interval.as_nanos() as u64),
            adaptive_last_ts_update_interval_ns: AtomicU64::new(update_interval.as_nanos() as u64),
            adaptive_state: AdaptiveUpdateIntervalState {
                inner: Mutex::new(AdaptiveUpdateIntervalInner {
                    last_tick_ms: now_ms,
                    state: AdaptiveUpdateTsIntervalState::None,
                }),
                last_short_staleness_read_time_ms: AtomicI64::new(0),
            },
            shrink_interval_tx,
        });

        (oracle, shrink_interval_rx)
    }

    fn normalize_txn_scope(txn_scope: &str) -> &str {
        if txn_scope.is_empty() {
            crate::GLOBAL_TXN_SCOPE
        } else {
            txn_scope
        }
    }

    fn configured_update_interval(&self) -> Duration {
        Duration::from_nanos(self.last_ts_update_interval_ns.load(Ordering::SeqCst))
    }

    fn adaptive_update_interval(&self) -> Duration {
        Duration::from_nanos(
            self.adaptive_last_ts_update_interval_ns
                .load(Ordering::SeqCst),
        )
    }

    fn store_configured_update_interval(&self, interval: Duration) -> Duration {
        Duration::from_nanos(
            self.last_ts_update_interval_ns
                .swap(interval.as_nanos() as u64, Ordering::SeqCst),
        )
    }

    fn store_adaptive_update_interval(&self, interval: Duration) {
        self.adaptive_last_ts_update_interval_ns
            .store(interval.as_nanos() as u64, Ordering::SeqCst);
    }

    fn try_send_shrink_signal(&self, required_staleness: Duration) {
        let _ = self.shrink_interval_tx.try_send(required_staleness);
    }

    async fn get_timestamp(&self, txn_scope: &str) -> Result<u64, String> {
        let txn_scope = Self::normalize_txn_scope(txn_scope);
        let ts = self.provider.get_timestamp(txn_scope).await?;
        self.set_last_ts(ts, txn_scope);
        Ok(ts)
    }

    fn get_low_resolution_timestamp(&self, txn_scope: &str) -> Result<u64, String> {
        let txn_scope = Self::normalize_txn_scope(txn_scope);
        let map = self.last_ts.lock().unwrap();
        let Some(cell) = map.get(txn_scope) else {
            return Err(format!("invalid txnScope = {txn_scope}"));
        };
        Ok(cell.load(Ordering::SeqCst))
    }

    fn set_last_ts(&self, ts: u64, txn_scope: &str) {
        let txn_scope = Self::normalize_txn_scope(txn_scope);

        let cell = {
            let mut map = self.last_ts.lock().unwrap();
            map.entry(txn_scope.to_owned())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone()
        };

        // Atomically "fetch max".
        let mut prev = cell.load(Ordering::SeqCst);
        while ts > prev {
            match cell.compare_exchange(prev, ts, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(v) => prev = v,
            }
        }
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        new_update_interval: Duration,
    ) -> Result<(), String> {
        if new_update_interval.is_zero() {
            return Err("updateInterval must be > 0".to_owned());
        }

        let _guard = self.adaptive_state.inner.lock().unwrap();

        let prev_configured = self.store_configured_update_interval(new_update_interval);
        let prev_adaptive = self.adaptive_update_interval();

        if new_update_interval == prev_configured {
            return Ok(());
        }

        // If the adaptive update interval is synced with the configured one, treat it as "not taking effect".
        // Also shrink immediately when the new configured interval is smaller than the current adaptive interval.
        if prev_adaptive == prev_configured || new_update_interval < prev_adaptive {
            self.store_adaptive_update_interval(new_update_interval);
        }

        Ok(())
    }

    fn required_staleness(read_ts: u64, current_ts: u64) -> Duration {
        let current_ms = crate::timestamp::extract_physical(current_ts);
        let read_ms = crate::timestamp::extract_physical(read_ts);
        if current_ms <= read_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((current_ms - read_ms) as u64)
        }
    }

    fn adjust_update_low_resolution_ts_interval_with_requested_staleness(
        &self,
        read_ts: u64,
        current_ts: u64,
        now_ms: i64,
    ) {
        let required_staleness = Self::required_staleness(read_ts, current_ts);
        let current_update_interval = self.adaptive_update_interval();

        if required_staleness
            <= current_update_interval + ADAPTIVE_UPDATE_TS_INTERVAL_BLOCK_RECOVER_THRESHOLD
        {
            let last = self
                .adaptive_state
                .last_short_staleness_read_time_ms
                .load(Ordering::SeqCst);
            if last < now_ms {
                let _ = self
                    .adaptive_state
                    .last_short_staleness_read_time_ms
                    .compare_exchange(last, now_ms, Ordering::SeqCst, Ordering::SeqCst);
            }
        }

        if required_staleness <= current_update_interval
            && current_update_interval > MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL
        {
            let required_staleness = required_staleness.max(Duration::from_millis(1));
            self.try_send_shrink_signal(required_staleness);
        }
    }

    fn next_update_interval(&self, now_ms: i64, required_staleness: Duration) -> Duration {
        let mut inner = self.adaptive_state.inner.lock().unwrap();

        let configured_interval = self.configured_update_interval();
        let prev_adaptive_interval = self.adaptive_update_interval();
        let last_reach_ms = self
            .adaptive_state
            .last_short_staleness_read_time_ms
            .load(Ordering::SeqCst);

        let since_last_reach = if now_ms <= last_reach_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((now_ms - last_reach_ms) as u64)
        };

        let mut next_interval = prev_adaptive_interval;
        let mut next_state = None;

        if configured_interval <= MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL {
            next_interval = configured_interval;
            next_state = Some(AdaptiveUpdateTsIntervalState::Unadjustable);
        } else if !required_staleness.is_zero() {
            if required_staleness < prev_adaptive_interval
                && prev_adaptive_interval > MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL
            {
                next_interval = required_staleness
                    .saturating_sub(ADAPTIVE_UPDATE_TS_INTERVAL_SHRINKING_PRESERVE)
                    .max(MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL);
                next_state = Some(AdaptiveUpdateTsIntervalState::Adapting);
            }
        } else if prev_adaptive_interval != configured_interval
            && since_last_reach < ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING
        {
            next_interval = prev_adaptive_interval;
            next_state = Some(AdaptiveUpdateTsIntervalState::Adapting);
        } else if configured_interval > MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL
            && prev_adaptive_interval == configured_interval
        {
            next_interval = prev_adaptive_interval;
            next_state = Some(AdaptiveUpdateTsIntervalState::Normal);
        } else if prev_adaptive_interval != configured_interval
            && since_last_reach >= ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING
        {
            let since_last_tick_ms = (now_ms - inner.last_tick_ms).max(0) as u64;
            let grow_ns = ((since_last_tick_ms as f64 / 1000.0)
                * ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND.as_nanos() as f64)
                as u64;
            next_interval =
                (prev_adaptive_interval + Duration::from_nanos(grow_ns)).min(configured_interval);
            next_state = Some(AdaptiveUpdateTsIntervalState::Recovering);

            if next_interval == configured_interval
                && configured_interval > MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL
            {
                next_state = Some(AdaptiveUpdateTsIntervalState::Normal);
            }
        }

        self.store_adaptive_update_interval(next_interval);
        if let Some(state) = next_state {
            inner.state = state;
        }

        next_interval
    }

    fn start_update_loop(
        self: Arc<Self>,
        mut shrink_interval_rx: mpsc::Receiver<Duration>,
        mut stop_rx: oneshot::Receiver<()>,
        now_ms: Arc<dyn Fn() -> i64 + Send + Sync + 'static>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut current_interval = self.configured_update_interval();
            let mut ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + current_interval,
                current_interval,
            );

            loop {
                tokio::select! {
                    _ = &mut stop_rx => return,
                    _ = ticker.tick() => {
                        let now_ms = now_ms();
                        let new_interval = self.next_update_interval(now_ms, Duration::ZERO);
                        self.update_cached_timestamps(now_ms).await;

                        if new_interval != current_interval {
                            current_interval = new_interval;
                            ticker = tokio::time::interval_at(
                                tokio::time::Instant::now() + current_interval,
                                current_interval,
                            );
                        }
                    }
                    Some(required_staleness) = shrink_interval_rx.recv() => {
                        let now_ms = now_ms();
                        let new_interval = self.next_update_interval(now_ms, required_staleness);
                        if new_interval != current_interval {
                            current_interval = new_interval;
                            ticker = tokio::time::interval_at(
                                tokio::time::Instant::now() + current_interval,
                                current_interval,
                            );
                        }
                    }
                }
            }
        })
    }

    async fn update_cached_timestamps(&self, now_ms: i64) {
        let scopes: Vec<String> = { self.last_ts.lock().unwrap().keys().cloned().collect() };

        for scope in scopes {
            let Ok(ts) = self.provider.get_timestamp(&scope).await else {
                continue;
            };
            self.set_last_ts(ts, &scope);
        }

        let mut inner = self.adaptive_state.inner.lock().unwrap();
        inner.last_tick_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_NOW_MS: i64 = 1_700_000_000_000;

    fn assert_in_epsilon(expected: Duration, actual: Duration, epsilon: f64) {
        let expected_s = expected.as_secs_f64();
        let actual_s = actual.as_secs_f64();
        let rel = ((actual_s - expected_s) / expected_s).abs();
        assert!(
            rel <= epsilon,
            "expected ~{expected:?}, got {actual:?} (rel={rel})"
        );
    }

    #[derive(Default)]
    struct MockPdClient {
        logical: AtomicU64,
    }

    #[async_trait]
    impl TsoProvider for MockPdClient {
        async fn get_timestamp(&self, _txn_scope: &str) -> Result<u64, String> {
            Ok(self.logical.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn must_notify_shrinking(shrink_rx: &mut mpsc::Receiver<Duration>, expected: Duration) {
        match shrink_rx.try_recv() {
            Ok(v) => assert_eq!(v, expected),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                panic!("expected shrink notification but channel is empty");
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("shrink channel disconnected");
            }
        }
    }

    fn must_no_notify(shrink_rx: &mut mpsc::Receiver<Duration>) {
        match shrink_rx.try_recv() {
            Ok(v) => panic!("expected no notification, got {v:?}"),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("shrink channel disconnected");
            }
        }
    }

    #[test]
    fn adaptive_update_interval_matches_client_go_test() {
        let pd = Arc::new(MockPdClient::default());
        let (o, mut shrink_rx) =
            LowResolutionTsOracle::new(pd, Duration::from_secs(2), BASE_NOW_MS);

        let mut now_ms = BASE_NOW_MS;

        let mock_ts = |before_now: Duration, now_ms: i64| -> u64 {
            let physical_ms = now_ms - before_now.as_millis() as i64;
            crate::timestamp::compose_ts(physical_ms, 1).unwrap()
        };

        now_ms += 2_000;
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_secs(2)
        );
        now_ms += 2_000;
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_secs(2)
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Normal
        );

        now_ms += 1_000;
        // A read requesting a staleness larger than 2s: nothing special will happen.
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(Duration::from_secs(3), now_ms),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_no_notify(&mut shrink_rx);
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_secs(2)
        );

        now_ms += 1_000;
        // A read requesting a staleness less than 2s triggers immediate shrinking.
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(Duration::from_secs(1), now_ms),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_notify_shrinking(&mut shrink_rx, Duration::from_secs(1));
        let mut expected_interval =
            Duration::from_secs(1) - ADAPTIVE_UPDATE_TS_INTERVAL_SHRINKING_PRESERVE;
        assert_eq!(
            o.next_update_interval(now_ms, Duration::from_secs(1)),
            expected_interval
        );
        {
            let inner = o.adaptive_state.inner.lock().unwrap();
            assert_eq!(inner.state, AdaptiveUpdateTsIntervalState::Adapting);
        }
        assert_eq!(
            o.adaptive_state
                .last_short_staleness_read_time_ms
                .load(Ordering::SeqCst),
            now_ms
        );

        // Let reads with short staleness continue happening.
        now_ms += ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING.as_millis() as i64 / 2;
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(Duration::from_secs(1), now_ms),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_no_notify(&mut shrink_rx);
        assert_eq!(
            o.adaptive_state
                .last_short_staleness_read_time_ms
                .load(Ordering::SeqCst),
            now_ms
        );

        // Delay before recovering has not elapsed since last short-staleness read.
        now_ms += ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING.as_millis() as i64 / 2
            + Duration::from_secs(1).as_millis() as i64;
        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms - 1_000;
        }
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            expected_interval
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Adapting
        );

        // Delay before recovering has elapsed.
        now_ms += ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING.as_millis() as i64 / 2;
        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms - 1_000;
        }
        expected_interval += ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND;
        assert_in_epsilon(
            expected_interval,
            o.next_update_interval(now_ms, Duration::ZERO),
            1e-3,
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Recovering
        );
        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms;
        }

        now_ms += Duration::from_secs(2).as_millis() as i64;
        // No effect if the required staleness didn't trigger the threshold.
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(
                expected_interval + ADAPTIVE_UPDATE_TS_INTERVAL_BLOCK_RECOVER_THRESHOLD * 2,
                now_ms,
            ),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_no_notify(&mut shrink_rx);
        expected_interval += ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND * 2;
        assert_in_epsilon(
            expected_interval,
            o.next_update_interval(now_ms, Duration::ZERO),
            1e-3,
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Recovering
        );

        // Block recovering when the required staleness is close enough to the current interval.
        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms;
        }
        now_ms += Duration::from_secs(1).as_millis() as i64;
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(
                expected_interval + ADAPTIVE_UPDATE_TS_INTERVAL_BLOCK_RECOVER_THRESHOLD / 2,
                now_ms,
            ),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_no_notify(&mut shrink_rx);
        assert_in_epsilon(
            expected_interval,
            o.next_update_interval(now_ms, Duration::ZERO),
            1e-3,
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Adapting
        );

        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms;
        }
        now_ms += Duration::from_secs(1).as_millis() as i64;
        assert_in_epsilon(
            expected_interval,
            o.next_update_interval(now_ms, Duration::ZERO),
            1e-3,
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Adapting
        );

        // Now delay + 1s has elapsed. Continue recovering.
        now_ms += ADAPTIVE_UPDATE_TS_INTERVAL_DELAY_BEFORE_RECOVERING.as_millis() as i64;
        {
            let mut inner = o.adaptive_state.inner.lock().unwrap();
            inner.last_tick_ms = now_ms - 1_000;
        }
        expected_interval += ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND;
        assert_in_epsilon(
            expected_interval,
            o.next_update_interval(now_ms, Duration::ZERO),
            1e-3,
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Recovering
        );

        // Recover back to configured.
        loop {
            {
                let mut inner = o.adaptive_state.inner.lock().unwrap();
                inner.last_tick_ms = now_ms;
            }
            now_ms += Duration::from_secs(1).as_millis() as i64;
            expected_interval += ADAPTIVE_UPDATE_TS_INTERVAL_RECOVER_PER_SECOND;
            if expected_interval >= Duration::from_secs(2) {
                break;
            }
            assert_in_epsilon(
                expected_interval,
                o.next_update_interval(now_ms, Duration::ZERO),
                1e-3,
            );
            assert_eq!(
                o.adaptive_state.inner.lock().unwrap().state,
                AdaptiveUpdateTsIntervalState::Recovering
            );
        }
        expected_interval = Duration::from_secs(2);
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            expected_interval
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Normal
        );

        // Manual configuration changes when adaptive is not taking effect.
        o.set_low_resolution_timestamp_update_interval(Duration::from_secs(1))
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), Duration::from_secs(1));
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_secs(1)
        );

        o.set_low_resolution_timestamp_update_interval(Duration::from_secs(2))
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), Duration::from_secs(2));
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_secs(2)
        );

        // Adaptive is taking effect; config changes don't immediately affect actual update interval.
        now_ms += 1_000;
        o.adjust_update_low_resolution_ts_interval_with_requested_staleness(
            mock_ts(Duration::from_secs(1), now_ms),
            mock_ts(Duration::ZERO, now_ms),
            now_ms,
        );
        must_notify_shrinking(&mut shrink_rx, Duration::from_secs(1));
        expected_interval = Duration::from_secs(1) - ADAPTIVE_UPDATE_TS_INTERVAL_SHRINKING_PRESERVE;
        assert_eq!(
            o.next_update_interval(now_ms, Duration::from_secs(1)),
            expected_interval
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Adapting
        );

        o.set_low_resolution_timestamp_update_interval(Duration::from_secs(3))
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), expected_interval);
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            expected_interval
        );
        o.set_low_resolution_timestamp_update_interval(Duration::from_secs(1))
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), expected_interval);
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            expected_interval
        );

        // ...unless set to a value shorter than the current adaptive interval.
        o.set_low_resolution_timestamp_update_interval(Duration::from_millis(800))
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), Duration::from_millis(800));
        assert_eq!(
            o.next_update_interval(now_ms, Duration::ZERO),
            Duration::from_millis(800)
        );
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Normal
        );

        // Too short configured interval: unadjustable.
        let too_short = MIN_ALLOWED_ADAPTIVE_UPDATE_TS_INTERVAL / 2;
        o.set_low_resolution_timestamp_update_interval(too_short)
            .unwrap();
        assert_eq!(o.adaptive_update_interval(), too_short);
        assert_eq!(o.next_update_interval(now_ms, Duration::ZERO), too_short);
        assert_eq!(
            o.adaptive_state.inner.lock().unwrap().state,
            AdaptiveUpdateTsIntervalState::Unadjustable
        );
    }

    #[tokio::test(start_paused = true)]
    async fn set_low_resolution_timestamp_update_interval_drives_update_loop() {
        let pd = Arc::new(MockPdClient::default());
        let (o, shrink_rx) = LowResolutionTsOracle::new(pd, Duration::from_millis(50), BASE_NOW_MS);

        o.set_low_resolution_timestamp_update_interval(Duration::from_millis(50))
            .unwrap();

        // Seed the timestamp.
        o.get_timestamp(crate::GLOBAL_TXN_SCOPE).await.unwrap();

        let low_res = o
            .get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
            .unwrap();
        let ts = o.get_timestamp(crate::GLOBAL_TXN_SCOPE).await.unwrap();
        assert!(ts > low_res);

        let (stop_tx, stop_rx) = oneshot::channel();
        let base = tokio::time::Instant::now();
        let now_ms = Arc::new(move || {
            let elapsed = tokio::time::Instant::now().duration_since(base);
            BASE_NOW_MS + elapsed.as_millis() as i64
        });

        let _update_loop = o.clone().start_update_loop(shrink_rx, stop_rx, now_ms);
        tokio::task::yield_now().await;

        // Each tuple: (new_interval, previous_interval)
        let cases = [
            (Duration::from_millis(50), Duration::from_millis(50)),
            (Duration::from_millis(150), Duration::from_millis(50)),
            (Duration::from_millis(500), Duration::from_millis(150)),
        ];

        let mut prev_interval = Duration::from_millis(50);
        for (new_interval, expected_prev_interval) in cases {
            assert_eq!(prev_interval, expected_prev_interval);
            o.set_low_resolution_timestamp_update_interval(new_interval)
                .unwrap();

            // Interval takes effect after at most one tick of the previous interval.
            let before = o
                .get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                .unwrap();
            tokio::time::advance(prev_interval - Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(
                o.get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                    .unwrap(),
                before
            );
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            let after_prev_tick = o
                .get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                .unwrap();
            assert!(after_prev_tick > before);

            // Next tick should be scheduled at the new interval.
            tokio::time::advance(new_interval - Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(
                o.get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                    .unwrap(),
                after_prev_tick
            );
            tokio::time::advance(Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert!(
                o.get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                    .unwrap()
                    > after_prev_tick
            );

            prev_interval = new_interval;
        }

        let _ = stop_tx.send(());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn set_last_ts_always_pushes() {
        let pd = Arc::new(MockPdClient::default());
        let (o, _shrink_rx) = LowResolutionTsOracle::new(pd, Duration::from_secs(2), BASE_NOW_MS);

        let task_count = 50;
        let iterations = 200;

        let mut handles = Vec::with_capacity(task_count);
        for _ in 0..task_count {
            let o = o.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..iterations {
                    let ts = o.get_timestamp(crate::GLOBAL_TXN_SCOPE).await.unwrap();
                    let last = o
                        .get_low_resolution_timestamp(crate::GLOBAL_TXN_SCOPE)
                        .unwrap();
                    assert!(
                        last >= ts,
                        "lastTS must never be smaller than the returned ts (last={last}, ts={ts})"
                    );
                    tokio::task::yield_now().await;
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }
}
