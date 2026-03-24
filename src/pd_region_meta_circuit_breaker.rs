//! PD region metadata circuit breaker.
//!
//! This mirrors client-go `locate.ChangePDRegionMetaCircuitBreakerSettings`: it allows callers to
//! tune a global circuit breaker guarding PD region metadata calls (GetRegion/GetPrevRegion/
//! GetRegionByID/ScanRegions). The goal is to reduce pressure on PD during outages by
//! short-circuiting repeated failures.
//!
//! The implementation is intentionally lightweight and dependency-free:
//! - Track success/failure samples in a sliding window.
//! - Open the circuit when the request rate is high enough and the error rate is high.
//! - After a cool-down, transition to half-open and require N consecutive successes to close.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use lazy_static::lazy_static;

use crate::Error;
use crate::Result;

const FAILURE_RATE_TO_OPEN: f64 = 0.5;

/// Settings for the PD region metadata circuit breaker.
///
/// This is a Rust mapping of `github.com/tikv/pd/client/pkg/circuitbreaker.Settings`, as used by
/// client-go for region metadata calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PdRegionMetaCircuitBreakerSettings {
    /// Sliding window used to compute error rate.
    pub error_rate_window: Duration,
    /// Minimum requests-per-second within the window before the circuit breaker may open.
    pub min_qps_for_open: u64,
    /// Duration to keep the circuit open before allowing half-open probes.
    pub cool_down_interval: Duration,
    /// Number of successful half-open probes required to close the circuit.
    pub half_open_success_count: u64,
}

impl Default for PdRegionMetaCircuitBreakerSettings {
    fn default() -> Self {
        Self {
            error_rate_window: Duration::from_secs(30),
            min_qps_for_open: 10,
            cool_down_interval: Duration::from_secs(10),
            half_open_success_count: 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    at: Instant,
    ok: bool,
}

#[derive(Debug, Default)]
struct SlidingWindow {
    samples: VecDeque<Sample>,
    total: u64,
    failures: u64,
}

impl SlidingWindow {
    fn clear(&mut self) {
        self.samples.clear();
        self.total = 0;
        self.failures = 0;
    }

    fn record(&mut self, at: Instant, ok: bool) {
        self.samples.push_back(Sample { at, ok });
        self.total = self.total.saturating_add(1);
        if !ok {
            self.failures = self.failures.saturating_add(1);
        }
    }

    fn trim(&mut self, now: Instant, window: Duration) {
        if window.is_zero() {
            self.clear();
            return;
        }

        while let Some(front) = self.samples.front() {
            if now.duration_since(front.at) <= window {
                break;
            }
            let Some(sample) = self.samples.pop_front() else {
                break;
            };
            self.total = self.total.saturating_sub(1);
            if !sample.ok {
                self.failures = self.failures.saturating_sub(1);
            }
        }
    }

    fn qps(&self, window: Duration) -> f64 {
        if window.is_zero() {
            return 0.0;
        }
        let secs = window.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        (self.total as f64) / secs
    }

    fn failure_rate(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        (self.failures as f64) / (self.total as f64)
    }
}

#[derive(Debug)]
enum CircuitState {
    Closed,
    Open { opened_at: Instant },
    HalfOpen { successes: u64, in_flight: bool },
}

impl Default for CircuitState {
    fn default() -> Self {
        CircuitState::Closed
    }
}

#[derive(Debug, Default)]
struct CircuitBreakerInner {
    settings: PdRegionMetaCircuitBreakerSettings,
    state: CircuitState,
    window: SlidingWindow,
}

#[derive(Clone, Copy, Debug)]
struct CallToken {
    half_open: bool,
}

/// A circuit breaker used for PD region metadata calls.
pub(crate) struct PdRegionMetaCircuitBreaker {
    inner: Mutex<CircuitBreakerInner>,
}

impl PdRegionMetaCircuitBreaker {
    pub(crate) fn new(settings: PdRegionMetaCircuitBreakerSettings) -> Self {
        Self {
            inner: Mutex::new(CircuitBreakerInner {
                settings: normalize_settings(settings),
                state: CircuitState::Closed,
                window: SlidingWindow::default(),
            }),
        }
    }

    pub(crate) fn change_settings<F>(&self, apply: F)
    where
        F: FnOnce(&mut PdRegionMetaCircuitBreakerSettings),
    {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        apply(&mut guard.settings);
        guard.settings = normalize_settings(guard.settings);
    }

    pub(crate) async fn execute<F, Fut, T>(&self, call: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let token = self.before_call()?;
        let result = call().await;
        self.after_call(token, result.is_ok());
        result
    }

    fn before_call(&self) -> Result<CallToken> {
        let now = Instant::now();
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let settings = guard.settings;
        guard.window.trim(now, settings.error_rate_window);

        let state = std::mem::replace(&mut guard.state, CircuitState::Closed);
        match state {
            CircuitState::Closed => {
                guard.state = CircuitState::Closed;
                Ok(CallToken { half_open: false })
            }
            CircuitState::Open { opened_at } => {
                if now.duration_since(opened_at) < settings.cool_down_interval {
                    guard.state = CircuitState::Open { opened_at };
                    Err(open_error())
                } else {
                    guard.state = CircuitState::HalfOpen {
                        successes: 0,
                        in_flight: true,
                    };
                    Ok(CallToken { half_open: true })
                }
            }
            CircuitState::HalfOpen {
                successes,
                in_flight,
            } => {
                if in_flight {
                    guard.state = CircuitState::HalfOpen {
                        successes,
                        in_flight,
                    };
                    Err(open_error())
                } else {
                    guard.state = CircuitState::HalfOpen {
                        successes,
                        in_flight: true,
                    };
                    Ok(CallToken { half_open: true })
                }
            }
        }
    }

    fn after_call(&self, token: CallToken, ok: bool) {
        let now = Instant::now();
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let settings = guard.settings;
        guard.window.trim(now, settings.error_rate_window);

        if token.half_open {
            let state = std::mem::replace(&mut guard.state, CircuitState::Closed);
            let CircuitState::HalfOpen { successes, .. } = state else {
                guard.state = state;
                return;
            };

            if ok {
                let successes = successes.saturating_add(1);
                if successes >= settings.half_open_success_count {
                    guard.state = CircuitState::Closed;
                    guard.window.clear();
                } else {
                    guard.state = CircuitState::HalfOpen {
                        successes,
                        in_flight: false,
                    };
                }
            } else {
                guard.state = CircuitState::Open { opened_at: now };
                guard.window.clear();
            }
            return;
        }

        let state = std::mem::replace(&mut guard.state, CircuitState::Closed);
        if !matches!(state, CircuitState::Closed) {
            guard.state = state;
            return;
        }

        guard.window.record(now, ok);
        if settings.error_rate_window.is_zero() {
            return;
        }

        let qps = guard.window.qps(settings.error_rate_window);
        if qps < settings.min_qps_for_open as f64 {
            return;
        }

        let failure_rate = guard.window.failure_rate();
        if failure_rate < FAILURE_RATE_TO_OPEN {
            return;
        }

        if guard.window.failures == 0 {
            return;
        }

        guard.state = CircuitState::Open { opened_at: now };
        guard.window.clear();
    }
}

fn normalize_settings(
    settings: PdRegionMetaCircuitBreakerSettings,
) -> PdRegionMetaCircuitBreakerSettings {
    PdRegionMetaCircuitBreakerSettings {
        half_open_success_count: settings.half_open_success_count.max(1),
        ..settings
    }
}

fn open_error() -> Error {
    Error::InternalError {
        message: "pd region meta circuit breaker is open".to_owned(),
    }
}

lazy_static! {
    static ref DEFAULT_PD_REGION_META_CIRCUIT_BREAKER: Arc<PdRegionMetaCircuitBreaker> = Arc::new(
        PdRegionMetaCircuitBreaker::new(PdRegionMetaCircuitBreakerSettings::default())
    );
}

/// Update settings for the global PD region metadata circuit breaker.
///
/// This is the Rust equivalent of client-go `ChangePDRegionMetaCircuitBreakerSettings`.
pub fn change_pd_region_meta_circuit_breaker_settings<F>(apply: F)
where
    F: FnOnce(&mut PdRegionMetaCircuitBreakerSettings),
{
    DEFAULT_PD_REGION_META_CIRCUIT_BREAKER.change_settings(apply);
}

pub(crate) fn default_pd_region_meta_circuit_breaker() -> Arc<PdRegionMetaCircuitBreaker> {
    DEFAULT_PD_REGION_META_CIRCUIT_BREAKER.clone()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Notify;
    use tokio::time::sleep;

    use super::*;

    fn settings_open_on_first_failure() -> PdRegionMetaCircuitBreakerSettings {
        PdRegionMetaCircuitBreakerSettings {
            error_rate_window: Duration::from_millis(50),
            min_qps_for_open: 1,
            cool_down_interval: Duration::from_secs(3600),
            half_open_success_count: 1,
        }
    }

    #[tokio::test]
    async fn circuit_breaker_opens_and_short_circuits_calls() {
        let breaker = PdRegionMetaCircuitBreaker::new(settings_open_on_first_failure());
        let calls = Arc::new(AtomicUsize::new(0));

        let calls_clone = calls.clone();
        let err = breaker
            .execute(|| async move {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Err::<(), Error>(Error::StringError("boom".to_owned()))
            })
            .await
            .expect_err("expected failure");
        assert!(matches!(err, Error::StringError(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let calls_clone = calls.clone();
        let err = breaker
            .execute(|| async move {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Error>(())
            })
            .await
            .expect_err("expected circuit breaker to reject call");
        assert!(matches!(err, Error::InternalError { .. }));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "rejected calls must not run the underlying operation"
        );
    }

    #[tokio::test]
    async fn circuit_breaker_allows_single_half_open_probe() -> Result<()> {
        let breaker = Arc::new(PdRegionMetaCircuitBreaker::new(
            PdRegionMetaCircuitBreakerSettings {
                cool_down_interval: Duration::ZERO,
                half_open_success_count: 100,
                ..settings_open_on_first_failure()
            },
        ));

        // open the circuit
        let _ = breaker
            .execute(|| async { Err::<(), Error>(Error::StringError("boom".to_owned())) })
            .await;

        let entered = Arc::new(Notify::new());
        let entered_task = entered.clone();
        let breaker_task = breaker.clone();
        let probe = tokio::spawn(async move {
            breaker_task
                .execute(|| async move {
                    entered_task.notify_one();
                    sleep(Duration::from_millis(30)).await;
                    Ok::<(), Error>(())
                })
                .await
        });

        entered.notified().await;

        let err = breaker
            .execute(|| async { Ok::<(), Error>(()) })
            .await
            .expect_err("expected concurrent probe to be rejected");
        assert!(matches!(err, Error::InternalError { .. }));

        probe.await.expect("probe task")?;
        Ok(())
    }
}
