// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;
use std::time::Instant;

use log::warn;
use prometheus::Gauge;
use prometheus::GaugeVec;
use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounter;
use prometheus::IntCounterVec;
use prometheus::Opts;

use crate::proto::coprocessor;
use crate::proto::errorpb;
use crate::proto::kvrpcpb;
use crate::request_context::is_internal_request_source;
use crate::Error;
use crate::Result;

#[derive(Clone, Copy, Debug)]
struct TikvClientRequestLabels {
    store_id: u64,
    stale_read: bool,
    internal: bool,
}

pub struct RequestStats {
    start: Instant,
    cmd: &'static str,
    duration: Option<&'static HistogramVec>,
    failed_duration: Option<&'static HistogramVec>,
    failed_counter: Option<&'static IntCounterVec>,
    tikv_client_request_seconds: Option<&'static HistogramVec>,
    tikv_client_rpc_net_latency_seconds: Option<&'static HistogramVec>,
    tikv_client_request_labels: Option<TikvClientRequestLabels>,
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
            tikv_client_request_seconds: None,
            tikv_client_rpc_net_latency_seconds: None,
            tikv_client_request_labels: None,
        }
    }

    fn with_tikv_client_request_labels(
        mut self,
        labels: Option<TikvClientRequestLabels>,
    ) -> RequestStats {
        self.tikv_client_request_seconds = TIKV_CLIENT_RUST_REQUEST_SECONDS_HISTOGRAM_VEC.as_ref();
        self.tikv_client_rpc_net_latency_seconds =
            TIKV_CLIENT_RUST_RPC_NET_LATENCY_SECONDS_HISTOGRAM_VEC.as_ref();
        self.tikv_client_request_labels = labels;
        self
    }

    pub fn done<R: Any>(&self, r: Result<R>) -> Result<R> {
        let elapsed = self.start.elapsed();
        let elapsed_sec = duration_to_sec(elapsed);

        if let (Some(duration), Some(labels)) = (
            self.tikv_client_request_seconds,
            self.tikv_client_request_labels,
        ) {
            let stale_read = bool_label_value(labels.stale_read);
            let internal = bool_label_value(labels.internal);
            let mut buf = [0u8; 20];
            let store = u64_label_value(labels.store_id, &mut buf);
            duration
                .with_label_values(&[self.cmd, store, stale_read, internal])
                .observe(elapsed_sec);
        }

        match &r {
            Ok(resp) => {
                if let Some(duration) = self.duration {
                    duration.with_label_values(&[self.cmd]).observe(elapsed_sec);
                }

                if let (Some(counter), Some(labels)) = (
                    TIKV_CLIENT_RUST_REGION_ERROR_COUNTER_VEC.as_ref(),
                    self.tikv_client_request_labels,
                ) {
                    if let Some(region_error) = region_error_from_response(resp as &dyn Any) {
                        let mut buf = [0u8; 20];
                        let store = u64_label_value(labels.store_id, &mut buf);
                        counter
                            .with_label_values(&[region_error_label(region_error), store])
                            .inc();
                    }
                }

                if let (Some(histogram), Some(labels)) = (
                    self.tikv_client_rpc_net_latency_seconds,
                    self.tikv_client_request_labels,
                ) {
                    if let Some(details) = exec_details_v2_from_response(resp as &dyn Any) {
                        let total_rpc_wall_time_ns = details
                            .time_detail_v2
                            .as_ref()
                            .map(|detail| detail.total_rpc_wall_time_ns)
                            .or_else(|| {
                                details
                                    .time_detail
                                    .as_ref()
                                    .map(|detail| detail.total_rpc_wall_time_ns)
                            })
                            .unwrap_or(0);
                        if total_rpc_wall_time_ns > 0 {
                            let net_latency = elapsed
                                .saturating_sub(Duration::from_nanos(total_rpc_wall_time_ns));
                            let internal = bool_label_value(labels.internal);
                            let mut buf = [0u8; 20];
                            let store = u64_label_value(labels.store_id, &mut buf);
                            histogram
                                .with_label_values(&[store, internal])
                                .observe(duration_to_sec(net_latency));
                        }
                    }
                }
            }
            Err(err) => {
                if let Some(failed_duration) = self.failed_duration {
                    failed_duration
                        .with_label_values(&[self.cmd])
                        .observe(elapsed_sec);
                }
                if let Some(failed_counter) = self.failed_counter {
                    failed_counter.with_label_values(&[self.cmd]).inc();
                }

                if let (Some(counter), Some(labels)) = (
                    TIKV_CLIENT_RUST_RPC_ERROR_COUNTER_VEC.as_ref(),
                    self.tikv_client_request_labels,
                ) {
                    if is_grpc_error(err) {
                        let mut buf = [0u8; 20];
                        let store = u64_label_value(labels.store_id, &mut buf);
                        counter
                            .with_label_values(&[rpc_error_label(err), store])
                            .inc();
                    }
                }
            }
        };

        r
    }
}

pub(crate) fn tikv_stats_with_context(
    cmd: &'static str,
    context: Option<&kvrpcpb::Context>,
) -> RequestStats {
    let labels = context.map(|ctx| TikvClientRequestLabels {
        store_id: ctx.peer.as_ref().map(|peer| peer.store_id).unwrap_or(0),
        stale_read: ctx.stale_read,
        internal: is_internal_request_source(&ctx.request_source),
    });
    tikv_stats(cmd).with_tikv_client_request_labels(labels)
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

pub(crate) fn region_cache_operation(op: &'static str, ok: bool) {
    let result = if ok { "ok" } else { "err" };
    if let Some(counter) = TIKV_CLIENT_RUST_REGION_CACHE_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[op, result]).inc();
    }
}

pub(crate) fn observe_load_region_cache(op: &'static str, elapsed: Duration) {
    if let Some(histogram) = TIKV_CLIENT_RUST_LOAD_REGION_CACHE_SECONDS_HISTOGRAM_VEC.as_ref() {
        histogram
            .with_label_values(&[op])
            .observe(duration_to_sec(elapsed));
    }
}

pub(crate) fn inc_load_region_total(tag: &'static str, reason: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_LOAD_REGION_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[tag, reason]).inc();
    }
}

pub(crate) fn inc_stale_region_from_pd_counter() {
    if let Some(counter) = TIKV_CLIENT_RUST_STALE_REGION_FROM_PD_COUNTER.as_ref() {
        counter.inc();
    }
}

pub(crate) fn inc_gc_unsafe_destroy_range_failures(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_GC_UNSAFE_DESTROY_RANGE_FAILURES_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_lock_resolver_actions(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_LOCK_RESOLVER_ACTIONS_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_prewrite_assertion_count_for_mutations(mutations: &[kvrpcpb::Mutation]) {
    let Some(counter) = TIKV_CLIENT_RUST_PREWRITE_ASSERTION_COUNT_COUNTER_VEC.as_ref() else {
        return;
    };

    if mutations.is_empty() {
        return;
    }

    let mut none = 0_u64;
    let mut exist = 0_u64;
    let mut not_exist = 0_u64;
    let mut unknown = 0_u64;

    for mutation in mutations {
        match kvrpcpb::Assertion::try_from(mutation.assertion) {
            Ok(kvrpcpb::Assertion::None) => none += 1,
            Ok(kvrpcpb::Assertion::Exist) => exist += 1,
            Ok(kvrpcpb::Assertion::NotExist) => not_exist += 1,
            Err(_) => unknown += 1,
        }
    }

    if none > 0 {
        counter.with_label_values(&["none"]).inc_by(none);
    }
    if exist > 0 {
        counter.with_label_values(&["exist"]).inc_by(exist);
    }
    if not_exist > 0 {
        counter.with_label_values(&["not-exist"]).inc_by(not_exist);
    }
    if unknown > 0 {
        counter.with_label_values(&["unknown"]).inc_by(unknown);
    }
}

pub(crate) fn set_feedback_slow_score(store_id: u64, slow_score: i32) {
    let Some(gauge) = TIKV_CLIENT_RUST_FEEDBACK_SLOW_SCORE_GAUGE_VEC.as_ref() else {
        return;
    };
    let mut buf = [0u8; 20];
    let store = u64_label_value(store_id, &mut buf);
    gauge.with_label_values(&[store]).set(f64::from(slow_score));
}

pub(crate) fn inc_health_feedback_ops_counter(scope: u64, label: &'static str) {
    let Some(counter) = TIKV_CLIENT_RUST_HEALTH_FEEDBACK_OPS_COUNTER_VEC.as_ref() else {
        return;
    };
    let mut buf = [0u8; 20];
    let scope = u64_label_value(scope, &mut buf);
    counter.with_label_values(&[scope, label]).inc();
}

pub(crate) fn observe_backoff_seconds(label: &str, duration: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_BACKOFF_SECONDS_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    let label = normalize_backoff_label(label);
    histogram
        .with_label_values(&[label])
        .observe(duration_to_sec(duration));
}

pub(crate) fn observe_request_retry_times(retry_times: u32) {
    if retry_times == 0 {
        return;
    }
    if let Some(histogram) = TIKV_CLIENT_RUST_REQUEST_RETRY_TIMES_HISTOGRAM.as_ref() {
        histogram.observe(f64::from(retry_times));
    }
}

pub(crate) fn observe_stale_read_hit_miss(is_stale_read: bool, retry_times: u32) {
    if !is_stale_read {
        return;
    }
    let Some(counter) = TIKV_CLIENT_RUST_STALE_READ_COUNTER_VEC.as_ref() else {
        return;
    };
    let result = if retry_times == 0 { "hit" } else { "miss" };
    counter.with_label_values(&[result]).inc();
}

pub(crate) fn inc_replica_selector_failure_counter(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_REPLICA_SELECTOR_FAILURE_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_async_send_req_total(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_ASYNC_SEND_REQ_TOTAL_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn observe_batch_pending_requests(target: &str, pending_requests: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_BATCH_PENDING_REQUESTS_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    if pending_requests == 0 {
        return;
    }
    histogram
        .with_label_values(&[target])
        .observe(pending_requests as f64);
}

pub(crate) fn observe_batch_requests(target: &str, batch_size: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_BATCH_REQUESTS_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    if batch_size == 0 {
        return;
    }
    histogram
        .with_label_values(&[target])
        .observe(batch_size as f64);
}

pub(crate) fn observe_batch_client_wait_connection_establish(elapsed: Duration) {
    if let Some(histogram) = TIKV_CLIENT_RUST_BATCH_CLIENT_WAIT_ESTABLISH_HISTOGRAM.as_ref() {
        histogram.observe(duration_to_sec(elapsed));
    }
}

pub(crate) fn observe_batch_client_unavailable(elapsed: Duration) {
    if let Some(histogram) = TIKV_CLIENT_RUST_BATCH_CLIENT_UNAVAILABLE_SECONDS_HISTOGRAM.as_ref() {
        histogram.observe(duration_to_sec(elapsed));
    }
}

pub(crate) fn observe_batch_client_reset(elapsed: Duration) {
    if let Some(histogram) = TIKV_CLIENT_RUST_BATCH_CLIENT_RESET_SECONDS_HISTOGRAM.as_ref() {
        histogram.observe(duration_to_sec(elapsed));
    }
}

pub(crate) fn inc_batch_client_no_available_connection() {
    if let Some(counter) = TIKV_CLIENT_RUST_BATCH_CLIENT_NO_AVAILABLE_CONNECTION_COUNTER.as_ref() {
        counter.inc();
    }
}

pub(crate) fn set_range_task_stats(task: &str, completed_regions: usize, failed_regions: usize) {
    let Some(gauge) = TIKV_CLIENT_RUST_RANGE_TASK_STATS_GAUGE_VEC.as_ref() else {
        return;
    };

    gauge
        .with_label_values(&[task, "completed-regions"])
        .set(completed_regions as f64);
    gauge
        .with_label_values(&[task, "failed-regions"])
        .set(failed_regions as f64);
}

pub(crate) fn add_range_task_stats(task: &str, completed_regions: usize, failed_regions: usize) {
    let Some(gauge) = TIKV_CLIENT_RUST_RANGE_TASK_STATS_GAUGE_VEC.as_ref() else {
        return;
    };

    if completed_regions != 0 {
        gauge
            .with_label_values(&[task, "completed-regions"])
            .add(completed_regions as f64);
    }
    if failed_regions != 0 {
        gauge
            .with_label_values(&[task, "failed-regions"])
            .add(failed_regions as f64);
    }
}

pub(crate) fn observe_range_task_push_duration(task: &str, elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_RANGE_TASK_PUSH_DURATION_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram
        .with_label_values(&[task])
        .observe(duration_to_sec(elapsed));
}

pub(crate) fn inc_commit_txn_counter(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_COMMIT_TXN_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_async_commit_txn_counter(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_ASYNC_COMMIT_TXN_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_one_pc_txn_counter(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_ONE_PC_TXN_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn add_aggressive_locking_count(label: &'static str, count: usize) {
    let Some(counter) = TIKV_CLIENT_RUST_AGGRESSIVE_LOCKING_COUNT_COUNTER_VEC.as_ref() else {
        return;
    };
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    if count == 0 {
        return;
    }
    counter.with_label_values(&[label]).inc_by(count);
}

pub(crate) fn inc_lock_cleanup_task_total(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_LOCK_CLEANUP_TASK_TOTAL_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_load_safepoint_total(label: &'static str) {
    if let Some(counter) = TIKV_CLIENT_RUST_LOAD_SAFEPOINT_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[label]).inc();
    }
}

pub(crate) fn inc_validate_read_ts_from_pd_count() {
    if let Some(counter) = TIKV_CLIENT_RUST_VALIDATE_READ_TS_FROM_PD_COUNT.as_ref() {
        counter.inc();
    }
}

pub(crate) fn set_low_resolution_tso_update_interval_seconds(update_interval: Duration) {
    if let Some(gauge) = TIKV_CLIENT_RUST_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS_GAUGE.as_ref()
    {
        gauge.set(update_interval.as_secs_f64());
    }
}

pub(crate) fn inc_txn_write_conflict_counter() {
    if let Some(counter) = TIKV_CLIENT_RUST_TXN_WRITE_CONFLICT_COUNTER.as_ref() {
        counter.inc();
    }
}

pub(crate) fn observe_txn_cmd_duration_seconds(
    label: &'static str,
    internal: bool,
    elapsed: Duration,
) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_CMD_DURATION_SECONDS_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram
        .with_label_values(&[label, bool_label_value(internal)])
        .observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_txn_regions_num(label: &'static str, internal: bool, regions: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_REGIONS_NUM_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram
        .with_label_values(&[label, bool_label_value(internal)])
        .observe(regions as f64);
}

pub(crate) fn observe_txn_commit_backoff_seconds(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_COMMIT_BACKOFF_SECONDS_HISTOGRAM.as_ref() else {
        return;
    };
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_txn_commit_backoff_count(count: u64) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_COMMIT_BACKOFF_COUNT_HISTOGRAM.as_ref() else {
        return;
    };
    histogram.observe(count as f64);
}

pub(crate) fn observe_txn_heart_beat_seconds(label: &'static str, elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_HEART_BEAT_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram
        .with_label_values(&[label])
        .observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_txn_ttl_manager(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_TTL_MANAGER_HISTOGRAM.as_ref() else {
        return;
    };
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) fn inc_ttl_lifetime_reach_total() {
    let Some(counter) = TIKV_CLIENT_RUST_TTL_LIFETIME_REACH_COUNTER.as_ref() else {
        return;
    };
    counter.inc();
}

pub(crate) fn observe_txn_lag_commit_ts_wait_seconds(result: &'static str, elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_LAG_COMMIT_TS_WAIT_SECONDS_HISTOGRAM_VEC.as_ref()
    else {
        return;
    };
    histogram
        .with_label_values(&[result])
        .observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_txn_lag_commit_ts_attempt_count(result: &'static str, attempts: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_LAG_COMMIT_TS_ATTEMPT_COUNT_HISTOGRAM_VEC.as_ref()
    else {
        return;
    };
    if attempts == 0 {
        return;
    }
    histogram
        .with_label_values(&[result])
        .observe(attempts as f64);
}

pub(crate) fn observe_txn_write_kv_num(internal: bool, write_keys: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_WRITE_KV_NUM_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    if write_keys == 0 {
        return;
    }
    histogram
        .with_label_values(&[bool_label_value(internal)])
        .observe(write_keys as f64);
}

pub(crate) fn observe_txn_write_size_bytes(internal: bool, write_size: u64) {
    let Some(histogram) = TIKV_CLIENT_RUST_TXN_WRITE_SIZE_BYTES_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    if write_size == 0 {
        return;
    }
    histogram
        .with_label_values(&[bool_label_value(internal)])
        .observe(write_size as f64);
}

pub(crate) fn observe_local_latch_wait_seconds(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_LOCAL_LATCH_WAIT_SECONDS_HISTOGRAM.as_ref() else {
        return;
    };
    if elapsed.is_zero() {
        return;
    }
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_pipelined_flush_len(mutation_count: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_PIPELINED_FLUSH_LEN_HISTOGRAM.as_ref() else {
        return;
    };
    if mutation_count == 0 {
        return;
    }
    histogram.observe(mutation_count as f64);
}

pub(crate) fn observe_pipelined_flush_size(write_size: u64) {
    let Some(histogram) = TIKV_CLIENT_RUST_PIPELINED_FLUSH_SIZE_HISTOGRAM.as_ref() else {
        return;
    };
    if write_size == 0 {
        return;
    }
    histogram.observe(write_size as f64);
}

pub(crate) fn observe_pipelined_flush_duration(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_PIPELINED_FLUSH_DURATION_HISTOGRAM.as_ref() else {
        return;
    };
    if elapsed.is_zero() {
        return;
    }
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_pipelined_flush_throttle_seconds(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_PIPELINED_FLUSH_THROTTLE_SECONDS_HISTOGRAM.as_ref()
    else {
        return;
    };
    if elapsed.is_zero() {
        return;
    }
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) struct TxnCmdTimer {
    label: &'static str,
    internal: bool,
    start: Instant,
}

impl TxnCmdTimer {
    pub(crate) fn new(label: &'static str, internal: bool) -> Self {
        Self {
            label,
            internal,
            start: Instant::now(),
        }
    }
}

impl Drop for TxnCmdTimer {
    fn drop(&mut self) {
        observe_txn_cmd_duration_seconds(self.label, self.internal, self.start.elapsed());
    }
}

pub(crate) struct TxnLagCommitTsTimer {
    start: Instant,
    attempts: usize,
    ok: bool,
}

impl TxnLagCommitTsTimer {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            attempts: 1,
            ok: false,
        }
    }

    pub(crate) fn inc_attempts(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    pub(crate) fn attempts(&self) -> usize {
        self.attempts
    }

    pub(crate) fn mark_ok(&mut self) {
        self.ok = true;
    }
}

impl Drop for TxnLagCommitTsTimer {
    fn drop(&mut self) {
        let result = if self.ok { "ok" } else { "err" };
        observe_txn_lag_commit_ts_attempt_count(result, self.attempts);
        observe_txn_lag_commit_ts_wait_seconds(result, self.start.elapsed());
    }
}

pub(crate) fn observe_rawkv_cmd_seconds(label: &'static str, elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_RAWKV_CMD_SECONDS_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram
        .with_label_values(&[label])
        .observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_rawkv_kv_size_bytes(label: &'static str, bytes: usize) {
    let Some(histogram) = TIKV_CLIENT_RUST_RAWKV_KV_SIZE_BYTES_HISTOGRAM_VEC.as_ref() else {
        return;
    };
    histogram.with_label_values(&[label]).observe(bytes as f64);
}

pub(crate) fn inc_safe_ts_update_counter(result: &'static str, store: &str) {
    if let Some(counter) = TIKV_CLIENT_RUST_SAFE_TS_UPDATE_COUNTER_VEC.as_ref() {
        counter.with_label_values(&[result, store]).inc();
    }
}

pub(crate) fn set_min_safe_ts_gap_seconds(store: &str, gap_seconds: f64) {
    if let Some(gauge) = TIKV_CLIENT_RUST_MIN_SAFE_TS_GAP_SECONDS_GAUGE_VEC.as_ref() {
        gauge.with_label_values(&[store]).set(gap_seconds);
    }
}

#[allow(dead_code)]
pub fn observe_tso_batch(batch_size: usize) {
    if let Some(histogram) = PD_TSO_BATCH_SIZE_HISTOGRAM.as_ref() {
        histogram.observe(batch_size as f64);
    }
}

pub(crate) fn observe_ts_future_wait_seconds(elapsed: Duration) {
    let Some(histogram) = TIKV_CLIENT_RUST_TS_FUTURE_WAIT_SECONDS_HISTOGRAM.as_ref() else {
        return;
    };
    histogram.observe(duration_to_sec(elapsed));
}

pub(crate) fn observe_kv_request_traffic_metrics(
    label: &'static str,
    context: Option<&kvrpcpb::Context>,
    is_cross_zone: bool,
    sent_bytes: i64,
    received_bytes: i64,
    response_ok: bool,
) {
    fn location_label_value(is_cross_zone: bool) -> &'static str {
        if is_cross_zone {
            "cross-zone"
        } else {
            "local"
        }
    }

    fn direction_label_value(is_in: bool) -> &'static str {
        if is_in {
            "in"
        } else {
            "out"
        }
    }

    fn replica_label_value(is_replica_read: bool) -> &'static str {
        if is_replica_read {
            "follower"
        } else {
            "leader"
        }
    }

    fn bytes_to_u64(bytes: i64) -> u64 {
        if bytes <= 0 {
            0
        } else {
            u64::try_from(bytes).unwrap_or(u64::MAX)
        }
    }

    fn is_read_request_label(label: &str) -> bool {
        matches!(
            label,
            "kv_get"
                | "kv_batch_get"
                | "kv_buffer_batch_get"
                | "kv_scan"
                | "coprocessor"
                | "batch_coprocessor"
                | "coprocessor_stream"
                | "raw_get"
                | "raw_batch_get"
                | "raw_get_key_ttl"
                | "raw_scan"
                | "raw_batch_scan"
                | "raw_coprocessor"
                | "raw_checksum"
        )
    }

    let Some(context) = context else {
        return;
    };

    let location = location_label_value(is_cross_zone);

    if context.stale_read {
        if let Some(counter) = TIKV_CLIENT_RUST_STALE_READ_REQ_COUNTER_VEC.as_ref() {
            counter.with_label_values(&[location]).inc();
        }
        if let Some(counter) = TIKV_CLIENT_RUST_STALE_READ_BYTES_COUNTER_VEC.as_ref() {
            counter
                .with_label_values(&[location, direction_label_value(false)])
                .inc_by(bytes_to_u64(sent_bytes));
            if response_ok {
                counter
                    .with_label_values(&[location, direction_label_value(true)])
                    .inc_by(bytes_to_u64(received_bytes));
            }
        }
    }

    if response_ok && is_read_request_label(label) {
        let Some(histogram) = TIKV_CLIENT_RUST_READ_REQUEST_BYTES_HISTOGRAM_VEC.as_ref() else {
            return;
        };

        let total = bytes_to_u64(sent_bytes).saturating_add(bytes_to_u64(received_bytes));
        if total == 0 {
            return;
        }

        histogram
            .with_label_values(&[replica_label_value(context.replica_read), location])
            .observe(total as f64);
    }
}

fn is_grpc_error(error: &Error) -> bool {
    matches!(error, Error::Grpc(_) | Error::GrpcAPI(_))
}

fn rpc_error_label(error: &Error) -> &'static str {
    match error {
        Error::Grpc(_) => "grpc-transport",
        Error::GrpcAPI(status) => match status.code() {
            tonic::Code::Cancelled => "grpc-canceled",
            tonic::Code::DeadlineExceeded => "grpc-deadline-exceeded",
            tonic::Code::Unavailable => "grpc-unavailable",
            tonic::Code::Unimplemented => "grpc-unimplemented",
            tonic::Code::Unknown => "grpc-unknown",
            tonic::Code::Internal => "grpc-internal",
            tonic::Code::InvalidArgument => "grpc-invalid-argument",
            tonic::Code::NotFound => "grpc-not-found",
            tonic::Code::AlreadyExists => "grpc-already-exists",
            tonic::Code::PermissionDenied => "grpc-permission-denied",
            tonic::Code::ResourceExhausted => "grpc-resource-exhausted",
            tonic::Code::FailedPrecondition => "grpc-failed-precondition",
            tonic::Code::Aborted => "grpc-aborted",
            tonic::Code::OutOfRange => "grpc-out-of-range",
            tonic::Code::Unauthenticated => "grpc-unauthenticated",
            tonic::Code::DataLoss => "grpc-data-loss",
            tonic::Code::Ok => "grpc-ok",
        },
        _ => "unknown",
    }
}

fn region_error_label(e: &errorpb::Error) -> &'static str {
    if e.not_leader.is_some() {
        return "not_leader";
    }
    if e.region_not_found.is_some() {
        return "region_not_found";
    }
    if e.key_not_in_region.is_some() {
        return "key_not_in_region";
    }
    if e.epoch_not_match.is_some() {
        return "epoch_not_match";
    }
    if let Some(busy) = e.server_is_busy.as_ref() {
        if busy.reason.contains("deadline is exceeded") {
            return "deadline_exceeded";
        }
        return "server_is_busy";
    }
    if e.stale_command.is_some() {
        return "stale_command";
    }
    if e.store_not_match.is_some() {
        return "store_not_match";
    }
    if e.raft_entry_too_large.is_some() {
        return "raft_entry_too_large";
    }
    if e.max_timestamp_not_synced.is_some() {
        return "max_timestamp_not_synced";
    }
    if e.read_index_not_ready.is_some() {
        return "read_index_not_ready";
    }
    if e.proposal_in_merging_mode.is_some() {
        return "proposal_in_merging_mode";
    }
    if e.data_is_not_ready.is_some() {
        return "data_is_not_ready";
    }
    if e.region_not_initialized.is_some() {
        return "region_not_initialized";
    }
    if e.disk_full.is_some() {
        return "disk_full";
    }
    if e.recovery_in_progress.is_some() {
        return "recovery_in_progress";
    }
    if e.flashback_in_progress.is_some() {
        return "flashback_in_progress";
    }
    if e.flashback_not_prepared.is_some() {
        return "flashback_not_prepared";
    }
    if e.is_witness.is_some() {
        return "peer_is_witness";
    }
    if e.message.contains("Deadline is exceeded") {
        return "deadline_exceeded";
    }
    if e.mismatch_peer_id.is_some() {
        return "mismatch_peer_id";
    }
    if e.bucket_version_not_match.is_some() {
        return "bucket_version_not_match";
    }
    if e.message.contains("invalid max_ts update") {
        return "invalid_max_ts_update";
    }
    "unknown"
}

fn region_error_from_response(response: &dyn Any) -> Option<&errorpb::Error> {
    macro_rules! downcast_region_error {
        ($($ty:ty),* $(,)?) => {
            $(
                if let Some(resp) = response.downcast_ref::<$ty>() {
                    return resp.region_error.as_ref();
                }
            )*
        };
    }

    downcast_region_error!(
        kvrpcpb::GetResponse,
        kvrpcpb::ScanResponse,
        kvrpcpb::PrewriteResponse,
        kvrpcpb::FlushResponse,
        kvrpcpb::CommitResponse,
        kvrpcpb::CleanupResponse,
        kvrpcpb::PessimisticLockResponse,
        kvrpcpb::ImportResponse,
        kvrpcpb::BatchRollbackResponse,
        kvrpcpb::PessimisticRollbackResponse,
        kvrpcpb::BatchGetResponse,
        kvrpcpb::BufferBatchGetResponse,
        kvrpcpb::ScanLockResponse,
        kvrpcpb::ResolveLockResponse,
        kvrpcpb::TxnHeartBeatResponse,
        kvrpcpb::CheckTxnStatusResponse,
        kvrpcpb::CheckSecondaryLocksResponse,
        kvrpcpb::DeleteRangeResponse,
        kvrpcpb::PrepareFlashbackToVersionResponse,
        kvrpcpb::FlashbackToVersionResponse,
        kvrpcpb::SplitRegionResponse,
        kvrpcpb::GcResponse,
        kvrpcpb::UnsafeDestroyRangeResponse,
        kvrpcpb::RawGetResponse,
        kvrpcpb::RawBatchGetResponse,
        kvrpcpb::RawGetKeyTtlResponse,
        kvrpcpb::RawPutResponse,
        kvrpcpb::RawBatchPutResponse,
        kvrpcpb::RawDeleteResponse,
        kvrpcpb::RawBatchDeleteResponse,
        kvrpcpb::RawDeleteRangeResponse,
        kvrpcpb::RawScanResponse,
        kvrpcpb::RawBatchScanResponse,
        kvrpcpb::RawCasResponse,
        kvrpcpb::RawCoprocessorResponse,
        kvrpcpb::RawChecksumResponse,
        kvrpcpb::GetLockWaitInfoResponse,
        kvrpcpb::GetLockWaitHistoryResponse,
    );

    if let Some(resp) = response.downcast_ref::<coprocessor::Response>() {
        if let Some(error) = resp.region_error.as_ref() {
            return Some(error);
        }
        for batch in &resp.batch_responses {
            if let Some(error) = batch.region_error.as_ref() {
                return Some(error);
            }
        }
        return None;
    }

    None
}

fn exec_details_v2_from_response(response: &dyn Any) -> Option<&kvrpcpb::ExecDetailsV2> {
    if let Some(resp) = response.downcast_ref::<kvrpcpb::GetResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::BatchRollbackResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::CheckSecondaryLocksResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::CheckTxnStatusResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::ResolveLockResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    if let Some(resp) = response.downcast_ref::<kvrpcpb::ScanLockResponse>() {
        return resp.exec_details_v2.as_ref();
    }
    None
}

fn normalize_backoff_label(label: &str) -> &str {
    match label {
        // Match client-go `BackoffHistogramRPC` label usage.
        "grpc" => "tikvRPC",
        // Region metadata changes / cache invalidations.
        "region" => "regionMiss",
        // Match client-go `BoTxnLockFast` label.
        "txnLockFast" => "tikvLockFast",
        other => other,
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

fn register_histogram_vec_with_buckets(
    name: &'static str,
    help: &'static str,
    label_names: &'static [&'static str],
    buckets: Vec<f64>,
) -> Option<HistogramVec> {
    let opts = HistogramOpts::new(name, help).buckets(buckets);
    let metric = match HistogramVec::new(opts, label_names) {
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

fn register_gauge_vec(
    name: &'static str,
    help: &'static str,
    label_names: &'static [&'static str],
) -> Option<GaugeVec> {
    let metric = match GaugeVec::new(Opts::new(name, help), label_names) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus gauge vec {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus gauge vec {name}: {err}");
        return None;
    }
    Some(metric)
}

fn register_gauge(name: &'static str, help: &'static str) -> Option<Gauge> {
    let metric = match Gauge::with_opts(Opts::new(name, help)) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus gauge {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus gauge {name}: {err}");
        return None;
    }
    Some(metric)
}

fn register_int_counter(name: &'static str, help: &'static str) -> Option<IntCounter> {
    let metric = match IntCounter::with_opts(Opts::new(name, help)) {
        Ok(metric) => metric,
        Err(err) => {
            warn!("failed to build prometheus int counter {name}: {err}");
            return None;
        }
    };
    if let Err(err) = prometheus::register(Box::new(metric.clone())) {
        warn!("failed to register prometheus int counter {name}: {err}");
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

fn register_histogram_with_buckets(
    name: &'static str,
    help: &'static str,
    buckets: Vec<f64>,
) -> Option<Histogram> {
    let metric = match Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets)) {
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

    static ref TIKV_CLIENT_RUST_REQUEST_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_request_seconds";
        let help = "Bucketed histogram of sending request duration.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 24) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(
            name,
            help,
            &["type", "store", "stale_read", "scope"],
            buckets,
        )
    };

    static ref TIKV_CLIENT_RUST_RPC_NET_LATENCY_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_rpc_net_latency_seconds";
        let help = "Bucketed histogram of estimated network latency between the client and TiKV.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 24) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["store", "scope"], buckets)
    };

    static ref TIKV_CLIENT_RUST_RPC_ERROR_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_rpc_err_total",
        "Counter of rpc errors.",
        &["type", "store"],
    );

    static ref TIKV_CLIENT_RUST_LOCK_RESOLVER_ACTIONS_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_lock_resolver_actions_total",
        "Counter of lock resolver actions.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_PREWRITE_ASSERTION_COUNT_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_prewrite_assertion_count",
        "Counter of assertions used in prewrite requests.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_FEEDBACK_SLOW_SCORE_GAUGE_VEC: Option<GaugeVec> = register_gauge_vec(
        "tikv_client_rust_feedback_slow_score",
        "Slow scores of each tikv node that is calculated by TiKV and sent to the client by health feedback.",
        &["store"],
    );

    static ref TIKV_CLIENT_RUST_HEALTH_FEEDBACK_OPS_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_health_feedback_ops_counter",
        "Counter of operations about TiKV health feedback.",
        &["scope", "type"],
    );

    static ref TIKV_CLIENT_RUST_REGION_ERROR_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_region_err_total",
        "Counter of region errors.",
        &["type", "store"],
    );

    static ref TIKV_CLIENT_RUST_BACKOFF_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_backoff_seconds";
        let help = "Total backoff seconds for a single request backoff loop.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 29) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };

    static ref TIKV_CLIENT_RUST_REQUEST_RETRY_TIMES_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_request_retry_times";
        let help = "Bucketed histogram of how many times a request retries.";
        let buckets = vec![1.0, 2.0, 3.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0];
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_ASYNC_SEND_REQ_TOTAL_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_async_send_req_total",
            "Counter of async send req by request plan.",
            &["result"],
        );

    static ref TIKV_CLIENT_RUST_REPLICA_SELECTOR_FAILURE_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_replica_selector_failure_counter",
            "Counter of the reason why the replica selector cannot yield a potential leader.",
            &["type"],
        );

    static ref TIKV_CLIENT_RUST_BATCH_PENDING_REQUESTS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_batch_pending_requests";
        let help = "Number of requests pending in the batch channel.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 11) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["target"], buckets)
    };

    static ref TIKV_CLIENT_RUST_BATCH_REQUESTS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_batch_requests";
        let help = "Number of requests in one batch.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 11) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["target"], buckets)
    };

    static ref TIKV_CLIENT_RUST_BATCH_CLIENT_WAIT_ESTABLISH_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_batch_client_wait_connection_establish";
        let help = "Batch client wait new connection establish.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 28) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_BATCH_CLIENT_UNAVAILABLE_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_batch_client_unavailable_seconds";
        let help = "Batch client unavailable.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 28) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_BATCH_CLIENT_RESET_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_batch_client_reset";
        let help = "Batch client recycle connection and reconnect duration.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 28) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_BATCH_CLIENT_NO_AVAILABLE_CONNECTION_COUNTER: Option<IntCounter> = register_int_counter(
        "tikv_client_rust_batch_client_no_available_connection_total",
        "Counter of no available batch client.",
    );

    static ref TIKV_CLIENT_RUST_RANGE_TASK_STATS_GAUGE_VEC: Option<GaugeVec> = register_gauge_vec(
        "tikv_client_rust_range_task_stats",
        "Stat of range tasks.",
        &["type", "result"],
    );

    static ref TIKV_CLIENT_RUST_RANGE_TASK_PUSH_DURATION_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_range_task_push_duration";
        let help = "Duration to push sub tasks to range task workers.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 20) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };

    static ref TIKV_CLIENT_RUST_LOAD_SAFEPOINT_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_load_safepoint_total",
            "Counter of load safepoint.",
            &["type"],
        );

    static ref TIKV_CLIENT_RUST_VALIDATE_READ_TS_FROM_PD_COUNT: Option<IntCounter> =
        register_int_counter(
            "tikv_client_rust_validate_read_ts_from_pd_count",
            "Counter of validating read ts by getting a timestamp from PD",
        );

    static ref TIKV_CLIENT_RUST_LOW_RESOLUTION_TSO_UPDATE_INTERVAL_SECONDS_GAUGE: Option<Gauge> =
        register_gauge(
            "tikv_client_rust_low_resolution_tso_update_interval_seconds",
            "The actual working update interval for the low resolution TSO. As there are adaptive mechanism internally, this value may differ from the config.",
        );

    static ref TIKV_CLIENT_RUST_TS_FUTURE_WAIT_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_ts_future_wait_seconds";
        let help = "Bucketed histogram of seconds cost for waiting timestamp future.";
        let buckets = match prometheus::exponential_buckets(0.000005, 2.0, 30) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_WRITE_CONFLICT_COUNTER: Option<IntCounter> = register_int_counter(
        "tikv_client_rust_txn_write_conflict_counter",
        "Counter of txn write conflict",
    );

    static ref TIKV_CLIENT_RUST_AGGRESSIVE_LOCKING_COUNT_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_aggressive_locking_count",
            "Counter of keys locked in aggressive locking mode",
            &["type"],
        );

    static ref TIKV_CLIENT_RUST_LOCK_CLEANUP_TASK_TOTAL_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_lock_cleanup_task_total",
            "Failure statistic of secondary lock cleanup task.",
            &["type"],
        );

    static ref TIKV_CLIENT_RUST_COMMIT_TXN_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_commit_txn_counter",
        "Counter of 2PC transactions.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_ASYNC_COMMIT_TXN_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_async_commit_txn_counter",
        "Counter of async commit transactions.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_ONE_PC_TXN_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_one_pc_txn_counter",
        "Counter of 1PC transactions.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_TXN_CMD_DURATION_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_cmd_duration_seconds";
        let help = "Bucketed histogram of processing time of txn cmds.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 29) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type", "scope"], buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_COMMIT_BACKOFF_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_txn_commit_backoff_seconds";
        let help = "Bucketed histogram of the total backoff duration in committing a transaction.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 22) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_COMMIT_BACKOFF_COUNT_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_txn_commit_backoff_count";
        let help = "Bucketed histogram of the backoff count in committing a transaction.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 12) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_REGIONS_NUM_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_regions_num";
        let help = "Number of regions in a transaction.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 25) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type", "scope"], buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_WRITE_KV_NUM_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_write_kv_num";
        let help = "Count of kv pairs to write in a transaction.";
        let buckets = match prometheus::exponential_buckets(1.0, 4.0, 17) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["scope"], buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_WRITE_SIZE_BYTES_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_write_size_bytes";
        let help = "Size of kv pairs to write in a transaction.";
        let buckets = match prometheus::exponential_buckets(16.0, 4.0, 17) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["scope"], buckets)
    };

    static ref TIKV_CLIENT_RUST_LOCAL_LATCH_WAIT_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_local_latch_wait_seconds";
        let help = "Wait time of a get local latch.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 20) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_PIPELINED_FLUSH_LEN_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_pipelined_flush_len";
        let help = "Bucketed histogram of length of pipelined flushed memdb";
        let buckets = match prometheus::exponential_buckets(1000.0, 2.0, 16) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_PIPELINED_FLUSH_SIZE_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_pipelined_flush_size";
        let help = "Bucketed histogram of size of pipelined flushed memdb";
        let buckets = match prometheus::exponential_buckets(16.0 * 1024.0 * 1024.0, 1.2, 13) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_PIPELINED_FLUSH_DURATION_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_pipelined_flush_duration";
        let help = "Flush time of pipelined memdb.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 28) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_PIPELINED_FLUSH_THROTTLE_SECONDS_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_pipelined_flush_throttle_seconds";
        let help = "Throttle durations of pipelined flushes.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 28) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_HEART_BEAT_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_heart_beat";
        let help = "Bucketed histogram of the txn_heartbeat request duration.";
        let buckets = match prometheus::exponential_buckets(0.001, 2.0, 20) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_TTL_MANAGER_HISTOGRAM: Option<Histogram> = {
        let name = "tikv_client_rust_txn_ttl_manager";
        let help = "Bucketed histogram of the txn ttl manager lifetime duration.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 20) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_with_buckets(name, help, buckets)
    };

    static ref TIKV_CLIENT_RUST_TTL_LIFETIME_REACH_COUNTER: Option<IntCounter> = register_int_counter(
        "tikv_client_rust_ttl_lifetime_reach_total",
        "Counter of ttlManager live too long.",
    );

    static ref TIKV_CLIENT_RUST_TXN_LAG_COMMIT_TS_WAIT_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_lag_commit_ts_wait_seconds";
        let help = "Bucketed histogram of seconds waiting commit TSO lag.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 16) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["result"], buckets)
    };

    static ref TIKV_CLIENT_RUST_TXN_LAG_COMMIT_TS_ATTEMPT_COUNT_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_txn_lag_commit_ts_attempt_count";
        let help = "Bucketed histogram of attempts to get the lagging TSO in one commit.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 6) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["result"], buckets)
    };

    static ref TIKV_CLIENT_RUST_RAWKV_CMD_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_rawkv_cmd_seconds";
        let help = "Bucketed histogram of processing time of rawkv cmds.";
        let buckets = match prometheus::exponential_buckets(0.0005, 2.0, 29) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };

    static ref TIKV_CLIENT_RUST_RAWKV_KV_SIZE_BYTES_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_rawkv_kv_size_bytes";
        let help = "Size of key/value to put, in bytes.";
        let buckets = match prometheus::exponential_buckets(1.0, 2.0, 30) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };

    static ref TIKV_CLIENT_RUST_SAFE_TS_UPDATE_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_safets_update_counter",
            "Counter of tikv safe_ts being updated.",
            &["result", "store"],
        );

    static ref TIKV_CLIENT_RUST_MIN_SAFE_TS_GAP_SECONDS_GAUGE_VEC: Option<GaugeVec> =
        register_gauge_vec(
            "tikv_client_rust_min_safets_gap_seconds",
            "The minimal (non-zero) SafeTS gap for each store.",
            &["store"],
        );

    static ref TIKV_CLIENT_RUST_READ_REQUEST_BYTES_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_read_request_bytes";
        let help = "Bucketed histogram of total bytes sent/received for read requests.";
        let buckets = match prometheus::exponential_buckets(256.0, 2.0, 22) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type", "result"], buckets)
    };

    static ref TIKV_CLIENT_RUST_STALE_READ_REQ_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_stale_read_req_counter",
        "Total number of stale read requests.",
        &["type"],
    );

    static ref TIKV_CLIENT_RUST_STALE_READ_BYTES_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_stale_read_bytes",
        "Bytes sent/received for stale read requests.",
        &["result", "direction"],
    );

    static ref TIKV_CLIENT_RUST_STALE_READ_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_stale_read_counter",
        "Counter of stale read hit/miss.",
        &["result"],
    );

    static ref TIKV_CLIENT_RUST_STALE_REGION_FROM_PD_COUNTER: Option<IntCounter> = register_int_counter(
        "tikv_client_rust_stale_region_from_pd",
        "Counter of stale region from PD",
    );

    static ref TIKV_CLIENT_RUST_GC_UNSAFE_DESTROY_RANGE_FAILURES_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_gc_unsafe_destroy_range_failures",
            "Counter of unsafe destroy range failures.",
            &["type"],
        );

    static ref TIKV_CLIENT_RUST_REGION_CACHE_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_region_cache_operations_total",
        "Counter of region cache operations.",
        &["type", "result"],
    );

    static ref TIKV_CLIENT_RUST_LOAD_REGION_COUNTER_VEC: Option<IntCounterVec> =
        register_int_counter_vec(
            "tikv_client_rust_load_region_total",
            "Counter of loading region.",
            &["type", "reason"],
        );

    static ref TIKV_CLIENT_RUST_LOAD_REGION_CACHE_SECONDS_HISTOGRAM_VEC: Option<HistogramVec> = {
        let name = "tikv_client_rust_load_region_cache_seconds";
        let help = "Load region information duration.";
        let buckets = match prometheus::exponential_buckets(0.0001, 2.0, 20) {
            Ok(buckets) => buckets,
            Err(err) => {
                warn!("failed to build prometheus histogram buckets {name}: {err}");
                return None;
            }
        };
        register_histogram_vec_with_buckets(name, help, &["type"], buckets)
    };
}

#[inline]
fn bool_label_value(v: bool) -> &'static str {
    if v {
        "true"
    } else {
        "false"
    }
}

#[inline]
fn u64_label_value(mut v: u64, buf: &mut [u8; 20]) -> &str {
    if v == 0 {
        buf[0] = b'0';
        return "0";
    }

    let mut i = buf.len();
    while v > 0 {
        let digit = (v % 10) as u8;
        v /= 10;
        i = i.saturating_sub(1);
        buf[i] = b'0' + digit;
    }

    std::str::from_utf8(&buf[i..]).unwrap_or("0")
}

/// Convert Duration to seconds.
#[inline]
fn duration_to_sec(d: Duration) -> f64 {
    let nanos = f64::from(d.subsec_nanos());
    // In most cases, we can't have so large Duration, so here just panic if overflow now.
    d.as_secs() as f64 + (nanos / 1_000_000_000.0)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serial_test::serial;

    use super::{
        add_aggressive_locking_count, add_range_task_stats, inc_async_commit_txn_counter,
        inc_async_send_req_total, inc_batch_client_no_available_connection, inc_commit_txn_counter,
        inc_gc_unsafe_destroy_range_failures, inc_health_feedback_ops_counter,
        inc_load_region_total, inc_load_safepoint_total, inc_lock_cleanup_task_total,
        inc_lock_resolver_actions, inc_one_pc_txn_counter,
        inc_prewrite_assertion_count_for_mutations, inc_replica_selector_failure_counter,
        inc_safe_ts_update_counter, inc_stale_region_from_pd_counter, inc_ttl_lifetime_reach_total,
        inc_validate_read_ts_from_pd_count, observe_backoff_seconds, observe_batch_client_reset,
        observe_batch_client_unavailable, observe_batch_client_wait_connection_establish,
        observe_batch_pending_requests, observe_batch_requests, observe_kv_request_traffic_metrics,
        observe_load_region_cache, observe_local_latch_wait_seconds,
        observe_pipelined_flush_duration, observe_pipelined_flush_len,
        observe_pipelined_flush_size, observe_pipelined_flush_throttle_seconds,
        observe_range_task_push_duration, observe_rawkv_cmd_seconds, observe_rawkv_kv_size_bytes,
        observe_request_retry_times, observe_stale_read_hit_miss, observe_ts_future_wait_seconds,
        observe_txn_cmd_duration_seconds, observe_txn_commit_backoff_count,
        observe_txn_commit_backoff_seconds, observe_txn_heart_beat_seconds,
        observe_txn_lag_commit_ts_attempt_count, observe_txn_lag_commit_ts_wait_seconds,
        observe_txn_ttl_manager, observe_txn_write_kv_num, observe_txn_write_size_bytes,
        region_cache_operation, set_feedback_slow_score,
        set_low_resolution_tso_update_interval_seconds, set_min_safe_ts_gap_seconds,
        set_range_task_stats, tikv_stats_with_context,
    };
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::proto::{errorpb, kvrpcpb as kvrpcpb_alias};
    use crate::Error;

    fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
        metric
            .get_label()
            .iter()
            .find(|pair| pair.get_name() == name)
            .map(|pair| pair.get_value())
    }

    #[test]
    #[serial(metrics)]
    fn test_tikv_client_request_seconds_histogram_records_labels() {
        let mut ctx = kvrpcpb::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 9_876_543_210,
            ..Default::default()
        });
        ctx.stale_read = true;
        ctx.request_source = "internal_unit_test".to_owned();

        let cmd = "unit_test_cmd_request_seconds";
        let stats = tikv_stats_with_context(cmd, Some(&ctx));
        let _ = stats.done::<()>(Ok(()));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_request_seconds")
            .expect("request_seconds histogram not registered");

        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some(cmd)
                && label_value(metric, "store") == Some("9876543210")
                && label_value(metric, "stale_read") == Some("true")
                && label_value(metric, "scope") == Some("true")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(found, "expected histogram metric with labels not found");
    }

    #[test]
    #[serial(metrics)]
    fn test_stale_read_metrics_record_traffic_counters() {
        let mut ctx = kvrpcpb::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 1,
            ..Default::default()
        });
        ctx.stale_read = true;

        observe_kv_request_traffic_metrics("kv_get", Some(&ctx), true, 12, 34, true);

        let families = prometheus::gather();

        let req_counter_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_stale_read_req_counter")
            .expect("stale read req counter not registered");
        let req_counter_found = req_counter_family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("cross-zone")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(
            req_counter_found,
            "expected stale read req counter metric with labels not found"
        );

        let bytes_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_stale_read_bytes")
            .expect("stale read bytes counter not registered");
        let bytes_out_found = bytes_family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("cross-zone")
                && label_value(metric, "direction") == Some("out")
                && metric.get_counter().get_value() >= 12.0
        });
        let bytes_in_found = bytes_family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("cross-zone")
                && label_value(metric, "direction") == Some("in")
                && metric.get_counter().get_value() >= 34.0
        });
        assert!(
            bytes_out_found && bytes_in_found,
            "expected stale read bytes metrics with labels not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_request_retry_times_histogram_records_observations() {
        let before_sum = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_request_retry_times")
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_sum())
                        .sum::<f64>()
                })
                .unwrap_or(0.0);
            family
        };

        observe_request_retry_times(123);

        let after_sum = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_request_retry_times")
                .expect("request retry times histogram not registered");
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_sum())
                .sum::<f64>()
        };

        assert!(
            after_sum >= before_sum + 123.0,
            "expected request retry times histogram sample sum to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_stale_read_counter_records_hit_and_miss() {
        fn counter_value(families: &[prometheus::proto::MetricFamily], result: &str) -> f64 {
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_stale_read_counter")
                .and_then(|family| {
                    family
                        .get_metric()
                        .iter()
                        .find(|metric| label_value(metric, "result") == Some(result))
                        .map(|metric| metric.get_counter().get_value())
                })
                .unwrap_or(0.0)
        }

        let before = prometheus::gather();
        let before_hit = counter_value(&before, "hit");
        let before_miss = counter_value(&before, "miss");

        for _ in 0..50 {
            observe_stale_read_hit_miss(true, 0);
        }
        for _ in 0..70 {
            observe_stale_read_hit_miss(true, 1);
        }
        observe_stale_read_hit_miss(false, 0);
        observe_stale_read_hit_miss(false, 10);

        let after = prometheus::gather();
        let after_hit = counter_value(&after, "hit");
        let after_miss = counter_value(&after, "miss");

        assert!(
            after_hit >= before_hit + 50.0,
            "expected stale read hit counter to increase"
        );
        assert!(
            after_miss >= before_miss + 70.0,
            "expected stale read miss counter to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_batch_metrics_helpers_record_metrics() {
        let target = "unit_test_target_batch_metrics";
        observe_batch_pending_requests(target, 10);
        observe_batch_requests(target, 5);

        let families = prometheus::gather();

        let pending_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_batch_pending_requests")
            .expect("batch_pending_requests histogram not registered");
        let pending_found = pending_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target)
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            pending_found,
            "expected batch_pending_requests metric with labels not found"
        );

        let requests_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_batch_requests")
            .expect("batch_requests histogram not registered");
        let requests_found = requests_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target)
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            requests_found,
            "expected batch_requests metric with labels not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_batch_client_wait_establish_histogram_records_observations() {
        let before_sum = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_batch_client_wait_connection_establish"
                })
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_sum())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        };

        observe_batch_client_wait_connection_establish(Duration::from_secs(5));

        let after_sum = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_batch_client_wait_connection_establish"
                })
                .expect("batch_client_wait_connection_establish histogram not registered");
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_sum())
                .sum::<f64>()
        };

        assert!(
            after_sum >= before_sum + 5.0,
            "expected batch client wait-establish histogram sample sum to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_batch_client_unavailable_histogram_records_observations() {
        let before_sum = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_batch_client_unavailable_seconds"
                })
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_sum())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        };

        observe_batch_client_unavailable(Duration::from_secs(5));

        let after_sum = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_batch_client_unavailable_seconds"
                })
                .expect("batch_client_unavailable_seconds histogram not registered");
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_sum())
                .sum::<f64>()
        };

        assert!(
            after_sum >= before_sum + 5.0,
            "expected batch client unavailable histogram sample sum to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_batch_client_reset_histogram_records_observations() {
        let before_sum = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_batch_client_reset")
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .map(|metric| metric.get_histogram().get_sample_sum())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        };

        observe_batch_client_reset(Duration::from_secs(5));

        let after_sum = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_batch_client_reset")
                .expect("batch_client_reset histogram not registered");
            family
                .get_metric()
                .iter()
                .map(|metric| metric.get_histogram().get_sample_sum())
                .sum::<f64>()
        };

        assert!(
            after_sum >= before_sum + 5.0,
            "expected batch client reset histogram sample sum to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_batch_client_no_available_connection_counter_increments() {
        let before = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| {
                    family.get_name()
                        == "tikv_client_rust_batch_client_no_available_connection_total"
                })
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        for _ in 0..200 {
            inc_batch_client_no_available_connection();
        }

        let after = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| {
                    family.get_name()
                        == "tikv_client_rust_batch_client_no_available_connection_total"
                })
                .expect("batch client no-available-connection counter not registered");
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        assert!(
            after >= before + 200.0,
            "expected no-available-connection counter to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_write_conflict_counter_increments() {
        let before = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_txn_write_conflict_counter")
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        for idx in 0..200_u64 {
            let conflict = kvrpcpb::WriteConflict {
                start_ts: idx,
                ..Default::default()
            };
            let _ = crate::WriteConflictError::new(conflict);
        }

        let after = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_txn_write_conflict_counter")
                .expect("txn_write_conflict_counter not registered");
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        assert!(
            after >= before + 200.0,
            "expected txn_write_conflict_counter to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_commit_counters_record_labels() {
        inc_commit_txn_counter("ok");
        inc_commit_txn_counter("err");
        inc_async_commit_txn_counter("ok");
        inc_async_commit_txn_counter("err");
        inc_one_pc_txn_counter("ok");
        inc_one_pc_txn_counter("err");
        inc_one_pc_txn_counter("fallback");

        let families = prometheus::gather();

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_commit_txn_counter")
            .expect("commit_txn_counter not registered");
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("ok")),
            "expected commit_txn_counter ok label"
        );
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("err")),
            "expected commit_txn_counter err label"
        );

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_async_commit_txn_counter")
            .expect("async_commit_txn_counter not registered");
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("ok")),
            "expected async_commit_txn_counter ok label"
        );
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("err")),
            "expected async_commit_txn_counter err label"
        );

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_one_pc_txn_counter")
            .expect("one_pc_txn_counter not registered");
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("ok")),
            "expected one_pc_txn_counter ok label"
        );
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("err")),
            "expected one_pc_txn_counter err label"
        );
        assert!(
            family
                .get_metric()
                .iter()
                .any(|metric| label_value(metric, "type") == Some("fallback")),
            "expected one_pc_txn_counter fallback label"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_aggressive_locking_count_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_aggressive_locking_count")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("new");
        add_aggressive_locking_count("new", 3);
        let after = counter_value("new");
        assert!(
            after >= before + 3.0,
            "expected aggressive_locking_count(new) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_async_send_req_total_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_async_send_req_total")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "result") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("ok");
        inc_async_send_req_total("ok");
        let after = counter_value("ok");
        assert!(
            after >= before + 1.0,
            "expected async_send_req_total(ok) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_lock_cleanup_task_total_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_lock_cleanup_task_total")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("commit");
        inc_lock_cleanup_task_total("commit");
        let after = counter_value("commit");
        assert!(
            after >= before + 1.0,
            "expected lock_cleanup_task_total(commit) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_rawkv_cmd_seconds_histogram_records_labels() {
        observe_rawkv_cmd_seconds("get", Duration::from_millis(1));
        observe_rawkv_cmd_seconds("delete_range_error", Duration::from_millis(2));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_rawkv_cmd_seconds")
            .expect("rawkv_cmd_seconds histogram not registered");

        let get_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("get")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            get_found,
            "expected rawkv_cmd_seconds get label metric not found"
        );

        let err_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("delete_range_error")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            err_found,
            "expected rawkv_cmd_seconds delete_range_error label metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_rawkv_kv_size_bytes_histogram_records_labels() {
        observe_rawkv_kv_size_bytes("key", 123);
        observe_rawkv_kv_size_bytes("value", 456);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_rawkv_kv_size_bytes")
            .expect("rawkv_kv_size_bytes histogram not registered");

        let key_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("key")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            key_found,
            "expected rawkv_kv_size_bytes key label metric not found"
        );

        let value_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("value")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            value_found,
            "expected rawkv_kv_size_bytes value label metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_cmd_duration_seconds_histogram_records_labels() {
        observe_txn_cmd_duration_seconds("get", true, Duration::from_millis(5));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_cmd_duration_seconds")
            .expect("txn_cmd_duration_seconds histogram not registered");

        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("get")
                && label_value(metric, "scope") == Some("true")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(found, "expected histogram metric with labels not found");
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_commit_backoff_histograms_record_observations() {
        fn histogram_sample(name: &str) -> (u64, f64) {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == name)
                .and_then(|family| family.get_metric().first())
                .map(|metric| {
                    let histogram = metric.get_histogram();
                    (histogram.get_sample_count(), histogram.get_sample_sum())
                })
                .unwrap_or((0, 0.0))
        }

        let (seconds_count_before, seconds_sum_before) =
            histogram_sample("tikv_client_rust_txn_commit_backoff_seconds");
        let (count_count_before, count_sum_before) =
            histogram_sample("tikv_client_rust_txn_commit_backoff_count");

        observe_txn_commit_backoff_seconds(Duration::from_millis(3));
        observe_txn_commit_backoff_count(4);

        let (seconds_count_after, seconds_sum_after) =
            histogram_sample("tikv_client_rust_txn_commit_backoff_seconds");
        let (count_count_after, count_sum_after) =
            histogram_sample("tikv_client_rust_txn_commit_backoff_count");

        assert!(
            seconds_count_after >= seconds_count_before + 1,
            "expected txn_commit_backoff_seconds histogram to record observations"
        );
        assert!(
            seconds_sum_after > seconds_sum_before,
            "expected txn_commit_backoff_seconds histogram sum to increase"
        );
        assert!(
            count_count_after >= count_count_before + 1,
            "expected txn_commit_backoff_count histogram to record observations"
        );
        assert!(
            count_sum_after >= count_sum_before + 4.0,
            "expected txn_commit_backoff_count histogram sum to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_heart_beat_histogram_records_labels() {
        observe_txn_heart_beat_seconds("ok", Duration::from_millis(5));
        observe_txn_heart_beat_seconds("err", Duration::from_millis(7));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_heart_beat")
            .expect("txn_heart_beat histogram not registered");

        let ok_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("ok")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            ok_found,
            "expected txn_heart_beat ok label metric not found"
        );

        let err_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("err")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            err_found,
            "expected txn_heart_beat err label metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_ttl_manager_histogram_records_observations() {
        let before = prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_ttl_manager")
            .and_then(|family| family.get_metric().first())
            .map(|metric| metric.get_histogram().get_sample_count())
            .unwrap_or(0);

        observe_txn_ttl_manager(Duration::from_millis(12));

        let after = prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_ttl_manager")
            .and_then(|family| family.get_metric().first())
            .map(|metric| metric.get_histogram().get_sample_count())
            .unwrap_or(0);

        assert!(
            after >= before + 1,
            "expected txn_ttl_manager histogram to record observations"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_ttl_lifetime_reach_total_counter_increments() {
        fn counter_value() -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_ttl_lifetime_reach_total")
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value();
        inc_ttl_lifetime_reach_total();
        let after = counter_value();
        assert!(
            after >= before + 1.0,
            "expected ttl_lifetime_reach_total counter to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_lag_commit_ts_histograms_record_labels() {
        observe_txn_lag_commit_ts_wait_seconds("ok", Duration::from_millis(5));
        observe_txn_lag_commit_ts_wait_seconds("err", Duration::from_millis(7));
        observe_txn_lag_commit_ts_attempt_count("ok", 1);
        observe_txn_lag_commit_ts_attempt_count("err", 3);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_lag_commit_ts_wait_seconds")
            .expect("txn_lag_commit_ts_wait_seconds histogram not registered");

        let ok_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("ok")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            ok_found,
            "expected txn_lag_commit_ts_wait_seconds ok label metric not found"
        );

        let err_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("err")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            err_found,
            "expected txn_lag_commit_ts_wait_seconds err label metric not found"
        );

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_lag_commit_ts_attempt_count")
            .expect("txn_lag_commit_ts_attempt_count histogram not registered");

        let ok_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("ok")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            ok_found,
            "expected txn_lag_commit_ts_attempt_count ok label metric not found"
        );

        let err_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("err")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            err_found,
            "expected txn_lag_commit_ts_attempt_count err label metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_txn_write_histograms_record_labels() {
        observe_txn_write_kv_num(true, 1);
        observe_txn_write_kv_num(false, 2);
        observe_txn_write_size_bytes(true, 16);
        observe_txn_write_size_bytes(false, 64);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_write_kv_num")
            .expect("txn_write_kv_num histogram not registered");

        let internal_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "scope") == Some("true")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            internal_found,
            "expected txn_write_kv_num internal label metric not found"
        );

        let general_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "scope") == Some("false")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            general_found,
            "expected txn_write_kv_num general label metric not found"
        );

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_txn_write_size_bytes")
            .expect("txn_write_size_bytes histogram not registered");

        let internal_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "scope") == Some("true")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            internal_found,
            "expected txn_write_size_bytes internal label metric not found"
        );

        let general_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "scope") == Some("false")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            general_found,
            "expected txn_write_size_bytes general label metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_local_latch_wait_histogram_records_samples() {
        observe_local_latch_wait_seconds(Duration::from_millis(5));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_local_latch_wait_seconds")
            .expect("local_latch_wait_seconds histogram not registered");

        let observed = family
            .get_metric()
            .first()
            .map(|metric| metric.get_histogram().get_sample_count())
            .unwrap_or(0);
        assert!(observed >= 1, "expected local_latch_wait_seconds sample");
    }

    #[test]
    #[serial(metrics)]
    fn test_pipelined_flush_histograms_record_samples() {
        observe_pipelined_flush_len(1234);
        observe_pipelined_flush_size(16 * 1024 * 1024);
        observe_pipelined_flush_duration(Duration::from_millis(5));
        observe_pipelined_flush_throttle_seconds(Duration::from_millis(7));

        let sample_count = |families: &[prometheus::proto::MetricFamily], name: &str| -> u64 {
            families
                .iter()
                .find(|family| family.get_name() == name)
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_histogram().get_sample_count())
                .unwrap_or(0)
        };

        let families = prometheus::gather();
        for name in [
            "tikv_client_rust_pipelined_flush_len",
            "tikv_client_rust_pipelined_flush_size",
            "tikv_client_rust_pipelined_flush_duration",
            "tikv_client_rust_pipelined_flush_throttle_seconds",
        ] {
            assert!(sample_count(&families, name) >= 1, "expected {name} sample");
        }
    }

    #[test]
    #[serial(metrics)]
    fn test_safets_update_counter_records_labels() {
        inc_safe_ts_update_counter("success", "unit_test_store_ok");
        inc_safe_ts_update_counter("skip", "unit_test_store_skip");
        inc_safe_ts_update_counter("fail", "unit_test_store_fail");

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_safets_update_counter")
            .expect("safets_update_counter not registered");

        let ok_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("success")
                && label_value(metric, "store") == Some("unit_test_store_ok")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(ok_found, "expected safets_update_counter ok label metric");

        let skip_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("skip")
                && label_value(metric, "store") == Some("unit_test_store_skip")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(
            skip_found,
            "expected safets_update_counter skip label metric"
        );

        let fail_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("fail")
                && label_value(metric, "store") == Some("unit_test_store_fail")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(
            fail_found,
            "expected safets_update_counter fail label metric"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_load_safepoint_total_counter_records_labels() {
        inc_load_safepoint_total("ok");
        inc_load_safepoint_total("fail");
        inc_load_safepoint_total("ok_compatible");
        inc_load_safepoint_total("fail_compatible");

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_load_safepoint_total")
            .expect("load_safepoint_total not registered");

        for label in ["ok", "fail", "ok_compatible", "fail_compatible"] {
            assert!(
                family.get_metric().iter().any(|metric| {
                    label_value(metric, "type") == Some(label)
                        && metric.get_counter().get_value() >= 1.0
                }),
                "expected load_safepoint_total {label} label"
            );
        }
    }

    #[test]
    #[serial(metrics)]
    fn test_validate_read_ts_from_pd_count_counter_increments() {
        fn counter_value() -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_validate_read_ts_from_pd_count"
                })
                .and_then(|family| {
                    family
                        .get_metric()
                        .get(0)
                        .map(|metric| metric.get_counter().get_value())
                })
                .unwrap_or(0.0)
        }

        let before = counter_value();
        inc_validate_read_ts_from_pd_count();
        let after = counter_value();

        assert!(
            after >= before + 1.0,
            "expected validate_read_ts_from_pd_count to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_ts_future_wait_seconds_histogram_records_observations() {
        fn sample_count() -> u64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_ts_future_wait_seconds")
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_histogram().get_sample_count())
                .unwrap_or(0)
        }

        let before = sample_count();
        observe_ts_future_wait_seconds(Duration::from_micros(10));
        let after = sample_count();
        assert!(
            after >= before + 1,
            "expected ts_future_wait_seconds histogram to record observations"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_stale_region_from_pd_counter_increments() {
        fn counter_value() -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_stale_region_from_pd")
                .and_then(|family| {
                    family
                        .get_metric()
                        .get(0)
                        .map(|metric| metric.get_counter().get_value())
                })
                .unwrap_or(0.0)
        }

        let before = counter_value();
        inc_stale_region_from_pd_counter();
        let after = counter_value();

        assert!(
            after >= before + 1.0,
            "expected stale_region_from_pd counter to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_gc_unsafe_destroy_range_failures_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_gc_unsafe_destroy_range_failures"
                })
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.has_counter()
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("get_stores");
        inc_gc_unsafe_destroy_range_failures("get_stores");
        let after = counter_value("get_stores");
        assert!(
            after >= before + 1.0,
            "expected gc_unsafe_destroy_range_failures(get_stores) to increase"
        );

        let before = counter_value("send");
        inc_gc_unsafe_destroy_range_failures("send");
        let after = counter_value("send");
        assert!(
            after >= before + 1.0,
            "expected gc_unsafe_destroy_range_failures(send) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_lock_resolver_actions_total_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_lock_resolver_actions_total")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("resolve");
        inc_lock_resolver_actions("resolve");
        let after = counter_value("resolve");
        assert!(
            after >= before + 1.0,
            "expected lock_resolver_actions_total(resolve) to increase"
        );

        let before = counter_value("query_txn_status");
        inc_lock_resolver_actions("query_txn_status");
        let after = counter_value("query_txn_status");
        assert!(
            after >= before + 1.0,
            "expected lock_resolver_actions_total(query_txn_status) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_prewrite_assertion_count_counter_records_mutation_assertions() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_prewrite_assertion_count")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.has_counter()
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let mutations = vec![
            kvrpcpb::Mutation {
                assertion: kvrpcpb::Assertion::None as i32,
                ..Default::default()
            },
            kvrpcpb::Mutation {
                assertion: kvrpcpb::Assertion::Exist as i32,
                ..Default::default()
            },
            kvrpcpb::Mutation {
                assertion: kvrpcpb::Assertion::NotExist as i32,
                ..Default::default()
            },
            kvrpcpb::Mutation {
                assertion: 42,
                ..Default::default()
            },
        ];

        let before_none = counter_value("none");
        let before_exist = counter_value("exist");
        let before_not_exist = counter_value("not-exist");
        let before_unknown = counter_value("unknown");

        inc_prewrite_assertion_count_for_mutations(&mutations);

        let after_none = counter_value("none");
        let after_exist = counter_value("exist");
        let after_not_exist = counter_value("not-exist");
        let after_unknown = counter_value("unknown");

        assert!(
            after_none >= before_none + 1.0,
            "expected prewrite_assertion_count(none) to increase"
        );
        assert!(
            after_exist >= before_exist + 1.0,
            "expected prewrite_assertion_count(exist) to increase"
        );
        assert!(
            after_not_exist >= before_not_exist + 1.0,
            "expected prewrite_assertion_count(not-exist) to increase"
        );
        assert!(
            after_unknown >= before_unknown + 1.0,
            "expected prewrite_assertion_count(unknown) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_feedback_slow_score_gauge_sets_value_for_store() {
        set_feedback_slow_score(42, 81);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_feedback_slow_score")
            .expect("feedback_slow_score gauge not registered");

        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("42") && metric.get_gauge().get_value() == 81.0
        });
        assert!(found, "expected feedback_slow_score gauge for store 42");
    }

    #[test]
    #[serial(metrics)]
    fn test_health_feedback_ops_counter_increments_for_scope_and_type() {
        fn counter_value(scope: &str, ty: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_health_feedback_ops_counter")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "scope") == Some(scope)
                            && label_value(metric, "type") == Some(ty)
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("7", "tick");
        inc_health_feedback_ops_counter(7, "tick");
        let after = counter_value("7", "tick");
        assert!(
            after >= before + 1.0,
            "expected health_feedback_ops_counter(7, tick) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_range_task_stats_gauge_records_labels_and_values() {
        set_range_task_stats("delete-range", 0, 0);
        add_range_task_stats("delete-range", 12, 3);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_range_task_stats")
            .expect("range_task_stats gauge not registered");

        let completed_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("delete-range")
                && label_value(metric, "result") == Some("completed-regions")
                && metric.get_gauge().get_value() >= 12.0
        });
        let failed_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("delete-range")
                && label_value(metric, "result") == Some("failed-regions")
                && metric.get_gauge().get_value() >= 3.0
        });

        assert!(
            completed_found && failed_found,
            "expected range_task_stats gauge labels not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_range_task_push_duration_histogram_records_observations() {
        observe_range_task_push_duration("delete-range", Duration::from_millis(12));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_range_task_push_duration")
            .expect("range_task_push_duration histogram not registered");

        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("delete-range")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            found,
            "expected range_task_push_duration histogram metric with labels not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_low_resolution_tso_update_interval_seconds_gauge_records_value() {
        set_low_resolution_tso_update_interval_seconds(Duration::from_millis(1500));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| {
                family.get_name() == "tikv_client_rust_low_resolution_tso_update_interval_seconds"
            })
            .expect("low_resolution_tso_update_interval_seconds gauge not registered");

        let value = family
            .get_metric()
            .get(0)
            .map(|metric| metric.get_gauge().get_value())
            .unwrap_or(0.0);
        assert!(
            (value - 1.5).abs() < 1e-6,
            "expected low_resolution_tso_update_interval_seconds gauge to be set"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_min_safets_gap_seconds_gauge_records_labels() {
        set_min_safe_ts_gap_seconds("unit_test_store_gap", 123.0);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_min_safets_gap_seconds")
            .expect("min_safets_gap_seconds gauge not registered");

        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("unit_test_store_gap")
                && (metric.get_gauge().get_value() - 123.0).abs() < 1e-6
        });
        assert!(
            found,
            "expected min_safets_gap_seconds gauge with label not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_read_request_bytes_histogram_records_labels() {
        let mut ctx = kvrpcpb::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 1,
            ..Default::default()
        });
        ctx.replica_read = true;

        observe_kv_request_traffic_metrics("kv_get", Some(&ctx), false, 12, 34, true);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_read_request_bytes")
            .expect("read request bytes histogram not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("follower")
                && label_value(metric, "result") == Some("local")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            found,
            "expected read request bytes histogram metric not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_rpc_net_latency_histogram_records_labels() {
        let mut ctx = kvrpcpb_alias::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 11_223_344_556,
            ..Default::default()
        });
        ctx.request_source = "internal_unit_test".to_owned();

        let stats = tikv_stats_with_context("unit_test_cmd_rpc_net_latency", Some(&ctx));

        std::thread::sleep(Duration::from_millis(1));

        let mut resp = kvrpcpb_alias::GetResponse::default();
        resp.exec_details_v2 = Some(kvrpcpb_alias::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb_alias::TimeDetailV2 {
                total_rpc_wall_time_ns: 1,
                ..Default::default()
            }),
            ..Default::default()
        });
        let _ = stats.done(Ok(resp));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_rpc_net_latency_seconds")
            .expect("rpc net latency histogram not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("11223344556")
                && label_value(metric, "scope") == Some("true")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(found, "expected rpc net latency histogram metric not found");
    }

    #[test]
    #[serial(metrics)]
    fn test_region_cache_metrics_helpers_record_metrics() {
        let op = "unit_test_region_cache_op";
        region_cache_operation(op, true);
        observe_load_region_cache(op, Duration::from_millis(1));

        let families = prometheus::gather();

        let counter_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_region_cache_operations_total")
            .expect("region cache counter not registered");
        let counter_found = counter_family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some(op)
                && label_value(metric, "result") == Some("ok")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(
            counter_found,
            "expected region cache counter metric with labels not found"
        );

        let histogram_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_load_region_cache_seconds")
            .expect("load_region_cache histogram not registered");
        let hist_found = histogram_family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some(op)
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            hist_found,
            "expected load region cache histogram metric with labels not found"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_load_region_total_counter_increments() {
        fn counter_value(tag: &str, reason: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_rust_load_region_total")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(tag)
                            && label_value(metric, "reason") == Some(reason)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("ByKey", "Missing");
        inc_load_region_total("ByKey", "Missing");
        let after = counter_value("ByKey", "Missing");
        assert!(
            after >= before + 1.0,
            "expected load_region_total(ByKey,Missing) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_replica_selector_failure_counter_increments() {
        fn counter_value(label: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_rust_replica_selector_failure_counter"
                })
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value("exhausted");
        inc_replica_selector_failure_counter("exhausted");
        let after = counter_value("exhausted");
        assert!(
            after >= before + 1.0,
            "expected replica_selector_failure_counter(exhausted) to increase"
        );
    }

    #[test]
    #[serial(metrics)]
    fn test_rpc_error_counter_records_grpc_status_code() {
        let mut ctx = kvrpcpb::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 12_345_678_901,
            ..Default::default()
        });

        let stats = tikv_stats_with_context("unit_test_cmd_rpc_err_total", Some(&ctx));
        let err = Error::GrpcAPI(tonic::Status::new(tonic::Code::Unavailable, "boom"));
        let _ = stats.done::<()>(Err(err));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_rpc_err_total")
            .expect("rpc error counter not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("grpc-unavailable")
                && label_value(metric, "store") == Some("12345678901")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(found, "expected rpc error counter metric not found");
    }

    #[test]
    #[serial(metrics)]
    fn test_region_error_counter_records_region_error_label() {
        let mut ctx = kvrpcpb_alias::Context::default();
        ctx.peer = Some(metapb::Peer {
            store_id: 98_765_432_109,
            ..Default::default()
        });

        let mut resp = kvrpcpb_alias::GetResponse::default();
        resp.region_error = Some(errorpb::Error {
            not_leader: Some(errorpb::NotLeader {
                region_id: 42,
                leader: None,
            }),
            ..Default::default()
        });

        let stats = tikv_stats_with_context("unit_test_cmd_region_err_total", Some(&ctx));
        let _ = stats.done(Ok(resp));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_region_err_total")
            .expect("region error counter not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("not_leader")
                && label_value(metric, "store") == Some("98765432109")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(found, "expected region error counter metric not found");
    }

    #[test]
    #[serial(metrics)]
    fn test_backoff_seconds_histogram_records_mapped_label() {
        observe_backoff_seconds("grpc", Duration::from_millis(1));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_backoff_seconds")
            .expect("backoff histogram not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "type") == Some("tikvRPC")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(found, "expected backoff histogram metric not found");
    }
}
