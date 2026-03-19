// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::time::Duration;
use std::time::Instant;

use log::warn;
use prometheus::Histogram;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
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
            tikv_client_request_labels: None,
        }
    }

    fn with_tikv_client_request_labels(
        mut self,
        labels: Option<TikvClientRequestLabels>,
    ) -> RequestStats {
        self.tikv_client_request_seconds = TIKV_CLIENT_RUST_REQUEST_SECONDS_HISTOGRAM_VEC.as_ref();
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

pub(crate) fn tikv_stats_for_kv_request(cmd: &'static str, request: &dyn Any) -> RequestStats {
    tikv_stats_with_context(cmd, kv_context_from_request(request))
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

#[allow(dead_code)]
pub fn observe_tso_batch(batch_size: usize) {
    if let Some(histogram) = PD_TSO_BATCH_SIZE_HISTOGRAM.as_ref() {
        histogram.observe(batch_size as f64);
    }
}

pub(crate) fn kv_context_from_request(request: &dyn Any) -> Option<&kvrpcpb::Context> {
    macro_rules! downcast_kv_context {
        ($($ty:ty),* $(,)?) => {
            $(
                if let Some(req) = request.downcast_ref::<$ty>() {
                    return req.context.as_ref();
                }
            )*
        };
    }

    downcast_kv_context!(
        kvrpcpb::RawGetRequest,
        kvrpcpb::RawBatchGetRequest,
        kvrpcpb::RawGetKeyTtlRequest,
        kvrpcpb::RawPutRequest,
        kvrpcpb::RawBatchPutRequest,
        kvrpcpb::RawDeleteRequest,
        kvrpcpb::RawBatchDeleteRequest,
        kvrpcpb::RawScanRequest,
        kvrpcpb::RawBatchScanRequest,
        kvrpcpb::RawDeleteRangeRequest,
        kvrpcpb::RawCasRequest,
        kvrpcpb::RawCoprocessorRequest,
        kvrpcpb::RawChecksumRequest,
        kvrpcpb::GetRequest,
        kvrpcpb::ScanRequest,
        kvrpcpb::PrewriteRequest,
        kvrpcpb::CommitRequest,
        kvrpcpb::CleanupRequest,
        kvrpcpb::BatchGetRequest,
        kvrpcpb::BatchRollbackRequest,
        kvrpcpb::FlushRequest,
        kvrpcpb::PessimisticRollbackRequest,
        kvrpcpb::ResolveLockRequest,
        kvrpcpb::ScanLockRequest,
        kvrpcpb::PessimisticLockRequest,
        kvrpcpb::TxnHeartBeatRequest,
        kvrpcpb::CheckTxnStatusRequest,
        kvrpcpb::CheckSecondaryLocksRequest,
        kvrpcpb::BufferBatchGetRequest,
        kvrpcpb::GcRequest,
        kvrpcpb::DeleteRangeRequest,
        kvrpcpb::PrepareFlashbackToVersionRequest,
        kvrpcpb::FlashbackToVersionRequest,
        kvrpcpb::SplitRegionRequest,
        kvrpcpb::UnsafeDestroyRangeRequest,
        kvrpcpb::RegisterLockObserverRequest,
        kvrpcpb::CheckLockObserverRequest,
        kvrpcpb::RemoveLockObserverRequest,
        kvrpcpb::PhysicalScanLockRequest,
        kvrpcpb::GetLockWaitInfoRequest,
        kvrpcpb::GetLockWaitHistoryRequest,
        kvrpcpb::GetHealthFeedbackRequest,
        kvrpcpb::BroadcastTxnStatusRequest,
    );

    if let Some(req) = request.downcast_ref::<coprocessor::Request>() {
        return req.context.as_ref();
    }
    if let Some(req) = request.downcast_ref::<coprocessor::BatchRequest>() {
        return req.context.as_ref();
    }

    None
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

    static ref TIKV_CLIENT_RUST_RPC_ERROR_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_rpc_err_total",
        "Counter of rpc errors.",
        &["type", "store"],
    );

    static ref TIKV_CLIENT_RUST_REGION_ERROR_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_region_err_total",
        "Counter of region errors.",
        &["type", "store"],
    );

    static ref TIKV_CLIENT_RUST_REGION_CACHE_COUNTER_VEC: Option<IntCounterVec> = register_int_counter_vec(
        "tikv_client_rust_region_cache_operations_total",
        "Counter of region cache operations.",
        &["type", "result"],
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
fn u64_label_value<'a>(mut v: u64, buf: &'a mut [u8; 20]) -> &'a str {
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

    use super::{observe_load_region_cache, region_cache_operation, tikv_stats_with_context};
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
}
