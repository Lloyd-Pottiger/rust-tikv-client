// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;
use std::time::Instant;

use log::warn;
use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounterVec;
use prometheus::Opts;

use crate::Result;

pub struct RequestStats {
    start: Instant,
    cmd: &'static str,
    duration: Option<&'static HistogramVec>,
    failed_duration: Option<&'static HistogramVec>,
    failed_counter: Option<&'static IntCounterVec>,
}

impl RequestStats {
    pub fn new(
        cmd: &'static str,
        duration: &'static HistogramVec,
        counter: &'static IntCounterVec,
        failed_duration: &'static HistogramVec,
        failed_counter: &'static IntCounterVec,
    ) -> Self {
        Self::new_optional(
            cmd,
            Some(duration),
            Some(counter),
            Some(failed_duration),
            Some(failed_counter),
        )
    }

    fn new_optional(
        cmd: &'static str,
        duration: Option<&'static HistogramVec>,
        counter: Option<&'static IntCounterVec>,
        failed_duration: Option<&'static HistogramVec>,
        failed_counter: Option<&'static IntCounterVec>,
    ) -> Self {
        if let Some(counter) = counter {
            counter.with_label_values(&[cmd]).inc();
        }
        RequestStats {
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
    match (
        TIKV_REQUEST_DURATION_HISTOGRAM_VEC.as_ref(),
        TIKV_REQUEST_COUNTER_VEC.as_ref(),
        TIKV_FAILED_REQUEST_DURATION_HISTOGRAM_VEC.as_ref(),
        TIKV_FAILED_REQUEST_COUNTER_VEC.as_ref(),
    ) {
        (Some(duration), Some(counter), Some(failed_duration), Some(failed_counter)) => {
            RequestStats::new(cmd, duration, counter, failed_duration, failed_counter)
        }
        (duration, counter, failed_duration, failed_counter) => {
            RequestStats::new_optional(cmd, duration, counter, failed_duration, failed_counter)
        }
    }
}

pub fn pd_stats(cmd: &'static str) -> RequestStats {
    match (
        PD_REQUEST_DURATION_HISTOGRAM_VEC.as_ref(),
        PD_REQUEST_COUNTER_VEC.as_ref(),
        PD_FAILED_REQUEST_DURATION_HISTOGRAM_VEC.as_ref(),
        PD_FAILED_REQUEST_COUNTER_VEC.as_ref(),
    ) {
        (Some(duration), Some(counter), Some(failed_duration), Some(failed_counter)) => {
            RequestStats::new(cmd, duration, counter, failed_duration, failed_counter)
        }
        (duration, counter, failed_duration, failed_counter) => {
            RequestStats::new_optional(cmd, duration, counter, failed_duration, failed_counter)
        }
    }
}

#[allow(dead_code)]
pub fn observe_tso_batch(batch_size: usize) {
    if let Some(histogram) = PD_TSO_BATCH_SIZE_HISTOGRAM.as_ref() {
        histogram.observe(batch_size as f64);
    }
}

fn register_histogram_vec(
    name: &'static str,
    help: &'static str,
    label_names: &'static [&'static str],
) -> Option<HistogramVec> {
    let metric = match HistogramVec::new(HistogramOpts::new(name, help), label_names) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus histogram vec {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus histogram vec {name}: {err}");
        return None;
    }
    Some(metric)
}

fn register_int_counter_vec(
    name: &'static str,
    help: &'static str,
    label_names: &'static [&'static str],
) -> Option<IntCounterVec> {
    let metric = match IntCounterVec::new(Opts::new(name, help), label_names) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus int counter vec {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus int counter vec {name}: {err}");
        return None;
    }
    Some(metric)
}

fn register_histogram(name: &'static str, help: &'static str) -> Option<Histogram> {
    let metric = match Histogram::with_opts(HistogramOpts::new(name, help)) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus histogram {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus histogram {name}: {err}");
        return None;
    }
    Some(metric)
}

lazy_static::lazy_static! {
    static ref TIKV_REQUEST_DURATION_HISTOGRAM_VEC: Option<HistogramVec> = register_histogram_vec(
        "tikv_request_duration_seconds",
        "Bucketed histogram of TiKV requests duration",
        &["type"],
    );
    static ref TIKV_REQUEST_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_request_total",
        "Total number of requests sent to TiKV",
        &["type"],
    );
    static ref TIKV_FAILED_REQUEST_DURATION_HISTOGRAM_VEC: Option<HistogramVec> = register_histogram_vec(
        "tikv_failed_request_duration_seconds",
        "Bucketed histogram of failed TiKV requests duration",
        &["type"],
    );
    static ref TIKV_FAILED_REQUEST_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_failed_request_total",
        "Total number of failed requests sent to TiKV",
        &["type"],
    );
    static ref PD_REQUEST_DURATION_HISTOGRAM_VEC: Option<HistogramVec> = register_histogram_vec(
        "pd_request_duration_seconds",
        "Bucketed histogram of PD requests duration",
        &["type"],
    );
    static ref PD_REQUEST_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "pd_request_total",
        "Total number of requests sent to PD",
        &["type"],
    );
    static ref PD_FAILED_REQUEST_DURATION_HISTOGRAM_VEC: Option<HistogramVec> = register_histogram_vec(
        "pd_failed_request_duration_seconds",
        "Bucketed histogram of failed PD requests duration",
        &["type"],
    );
    static ref PD_FAILED_REQUEST_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "pd_failed_request_total",
        "Total number of failed requests sent to PD",
        &["type"],
    );
    static ref PD_TSO_BATCH_SIZE_HISTOGRAM: Option<Histogram> = register_histogram(
        "pd_tso_batch_size",
        "Bucketed histogram of TSO request batch size",
    );
}

/// Convert Duration to seconds.
#[inline]
fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    // In most cases, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}
