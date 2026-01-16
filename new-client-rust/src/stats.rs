// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Internal request metrics.
//!
//! This module is intentionally tiny: it provides a `RequestStats` helper used by the request
//! dispatch path to record per-RPC stats.
//!
//! The Prometheus integration is optional (feature `prometheus`). When disabled, this module
//! compiles to a no-op implementation.

#[cfg(feature = "prometheus")]
mod imp {
    use std::sync::OnceLock;
    use std::time::Duration;
    use std::time::Instant;

    use log::warn;
    use prometheus::register_histogram;
    use prometheus::register_histogram_vec;
    use prometheus::register_int_counter_vec;
    use prometheus::Histogram;
    use prometheus::HistogramVec;
    use prometheus::IntCounterVec;

    use crate::Result;

    struct Metrics {
        tikv_request_duration: Option<HistogramVec>,
        tikv_request_total: Option<IntCounterVec>,
        tikv_failed_request_duration: Option<HistogramVec>,
        tikv_failed_request_total: Option<IntCounterVec>,
        backoff_sleep_duration: Option<HistogramVec>,

        pd_request_duration: Option<HistogramVec>,
        pd_request_total: Option<IntCounterVec>,
        pd_failed_request_duration: Option<HistogramVec>,
        pd_failed_request_total: Option<IntCounterVec>,
        pd_tso_batch_size: Option<Histogram>,
    }

    static METRICS: OnceLock<Metrics> = OnceLock::new();

    fn metrics() -> &'static Metrics {
        METRICS.get_or_init(Metrics::register)
    }

    impl Metrics {
        fn register_histogram_vec(
            name: &'static str,
            help: &'static str,
            labels: &'static [&'static str],
        ) -> Option<HistogramVec> {
            match register_histogram_vec!(name, help, labels) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("failed to register prometheus histogram vec {name}: {e:?}");
                    None
                }
            }
        }

        fn register_int_counter_vec(
            name: &'static str,
            help: &'static str,
            labels: &'static [&'static str],
        ) -> Option<IntCounterVec> {
            match register_int_counter_vec!(name, help, labels) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("failed to register prometheus counter vec {name}: {e:?}");
                    None
                }
            }
        }

        fn register_histogram(name: &'static str, help: &'static str) -> Option<Histogram> {
            match register_histogram!(name, help) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("failed to register prometheus histogram {name}: {e:?}");
                    None
                }
            }
        }

        fn register() -> Metrics {
            Metrics {
                tikv_request_duration: Self::register_histogram_vec(
                    "tikv_request_duration_seconds",
                    "Bucketed histogram of TiKV requests duration",
                    &["type"],
                ),
                tikv_request_total: Self::register_int_counter_vec(
                    "tikv_request_total",
                    "Total number of requests sent to TiKV",
                    &["type"],
                ),
                tikv_failed_request_duration: Self::register_histogram_vec(
                    "tikv_failed_request_duration_seconds",
                    "Bucketed histogram of failed TiKV requests duration",
                    &["type"],
                ),
                tikv_failed_request_total: Self::register_int_counter_vec(
                    "tikv_failed_request_total",
                    "Total number of failed requests sent to TiKV",
                    &["type"],
                ),
                backoff_sleep_duration: Self::register_histogram_vec(
                    "tikv_backoff_sleep_duration_seconds",
                    "Bucketed histogram of TiKV client backoff sleep duration",
                    &["type"],
                ),
                pd_request_duration: Self::register_histogram_vec(
                    "pd_request_duration_seconds",
                    "Bucketed histogram of PD requests duration",
                    &["type"],
                ),
                pd_request_total: Self::register_int_counter_vec(
                    "pd_request_total",
                    "Total number of requests sent to PD",
                    &["type"],
                ),
                pd_failed_request_duration: Self::register_histogram_vec(
                    "pd_failed_request_duration_seconds",
                    "Bucketed histogram of failed PD requests duration",
                    &["type"],
                ),
                pd_failed_request_total: Self::register_int_counter_vec(
                    "pd_failed_request_total",
                    "Total number of failed requests sent to PD",
                    &["type"],
                ),
                pd_tso_batch_size: Self::register_histogram(
                    "pd_tso_batch_size",
                    "Bucketed histogram of TSO request batch size",
                ),
            }
        }
    }

    pub struct RequestStats {
        start: Instant,
        cmd: &'static str,
        duration: Option<&'static HistogramVec>,
        failed_duration: Option<&'static HistogramVec>,
        failed_counter: Option<&'static IntCounterVec>,
    }

    impl RequestStats {
        fn new(
            cmd: &'static str,
            duration: Option<&'static HistogramVec>,
            counter: Option<&'static IntCounterVec>,
            failed_duration: Option<&'static HistogramVec>,
            failed_counter: Option<&'static IntCounterVec>,
        ) -> Self {
            if let Some(counter) = counter {
                counter.with_label_values(&[cmd]).inc();
            }
            Self {
                start: Instant::now(),
                cmd,
                duration,
                failed_duration,
                failed_counter,
            }
        }

        pub fn done<R>(&self, r: Result<R>) -> Result<R> {
            if r.is_ok() {
                if let Some(duration) = self.duration {
                    duration
                        .with_label_values(&[self.cmd])
                        .observe(duration_to_sec(self.start.elapsed()));
                }
            } else {
                if let Some(failed_duration) = self.failed_duration {
                    failed_duration
                        .with_label_values(&[self.cmd])
                        .observe(duration_to_sec(self.start.elapsed()));
                }
                if let Some(failed_counter) = self.failed_counter {
                    failed_counter.with_label_values(&[self.cmd]).inc();
                }
            }
            r
        }
    }

    pub fn tikv_stats(cmd: &'static str) -> RequestStats {
        let metrics = metrics();
        RequestStats::new(
            cmd,
            metrics.tikv_request_duration.as_ref(),
            metrics.tikv_request_total.as_ref(),
            metrics.tikv_failed_request_duration.as_ref(),
            metrics.tikv_failed_request_total.as_ref(),
        )
    }

    pub fn pd_stats(cmd: &'static str) -> RequestStats {
        let metrics = metrics();
        RequestStats::new(
            cmd,
            metrics.pd_request_duration.as_ref(),
            metrics.pd_request_total.as_ref(),
            metrics.pd_failed_request_duration.as_ref(),
            metrics.pd_failed_request_total.as_ref(),
        )
    }

    #[allow(dead_code)]
    pub fn observe_tso_batch(batch_size: usize) {
        if let Some(h) = metrics().pd_tso_batch_size.as_ref() {
            h.observe(batch_size as f64);
        }
    }

    pub(crate) fn ensure_metrics_registered() {
        let _ = metrics();
    }

    pub(crate) fn observe_backoff_sleep(kind: &'static str, duration: Duration) {
        if let Some(h) = metrics().backoff_sleep_duration.as_ref() {
            h.with_label_values(&[kind])
                .observe(duration_to_sec(duration));
        }
    }

    /// Convert Duration to seconds.
    #[inline]
    fn duration_to_sec(d: Duration) -> f64 {
        let nanos = f64::from(d.subsec_nanos());
        // In most cases, we can't have so large Duration, so here just panic if overflow now.
        d.as_secs() as f64 + (nanos / 1_000_000_000.0)
    }
}

#[cfg(not(feature = "prometheus"))]
mod imp {
    use crate::Result;

    #[derive(Debug, Default)]
    pub struct RequestStats;

    impl RequestStats {
        pub fn done<R>(&self, r: Result<R>) -> Result<R> {
            r
        }
    }

    pub fn tikv_stats(_cmd: &'static str) -> RequestStats {
        RequestStats
    }

    pub fn pd_stats(_cmd: &'static str) -> RequestStats {
        RequestStats
    }

    #[allow(dead_code)]
    pub fn observe_tso_batch(_batch_size: usize) {}

    #[allow(dead_code)]
    pub(crate) fn observe_backoff_sleep(_kind: &'static str, _duration: std::time::Duration) {}

    #[allow(dead_code)]
    pub(crate) fn ensure_metrics_registered() {}
}

pub use imp::*;
