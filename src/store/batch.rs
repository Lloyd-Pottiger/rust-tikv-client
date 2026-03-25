use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::time::Instant;

use futures::future::BoxFuture;
use futures::stream;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use log::warn;
use serde_derive::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::Body;
use tonic::codegen::Bytes;
use tonic::codegen::StdError;
use tonic::IntoStreamingRequest;
use tonic::Status;

use crate::proto::kvrpcpb;
use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::stats::{
    inc_batch_wait_overload, inc_panic_total, observe_batch_best_size, observe_batch_client_reset,
    observe_batch_client_unavailable, observe_batch_client_wait_connection_establish,
    observe_batch_head_arrival_interval_seconds, observe_batch_more_requests_total,
    observe_batch_pending_requests, observe_batch_recv_loop_duration_seconds,
    observe_batch_recv_tail_latency_seconds, observe_batch_request_duration_seconds,
    observe_batch_requests, observe_batch_send_loop_duration_seconds,
    observe_batch_send_tail_latency_seconds,
};
use crate::Error;
use crate::Result;

const BATCH_TAIL_LATENCY_THRESHOLD: Duration = Duration::from_millis(20);

#[derive(Clone, Debug)]
pub(crate) struct BatchDispatchResult {
    pub(crate) cmd: tikvpb::batch_commands_response::response::Cmd,
    pub(crate) health_feedback: Option<kvrpcpb::HealthFeedback>,
}

type BatchResponse = std::result::Result<BatchDispatchResult, Error>;
type BatchInbound = std::result::Result<tikvpb::BatchCommandsResponse, Status>;
type BatchInboundStream = futures::stream::BoxStream<'static, BatchInbound>;
type ReconnectFn = Arc<
    dyn Fn(
            mpsc::Receiver<tikvpb::BatchCommandsRequest>,
        ) -> BoxFuture<'static, Result<BatchInboundStream>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct BatchCommandsClient {
    inner: Arc<BatchCommandsClientInner>,
}

struct BatchCommandsClientInner {
    outbound: Arc<RwLock<mpsc::Sender<tikvpb::BatchCommandsRequest>>>,
    inflight: Arc<Mutex<HashMap<u64, oneshot::Sender<BatchResponse>>>>,
    next_id: AtomicU64,
    reader: JoinHandle<()>,
    stream_error: Arc<Mutex<Option<Status>>>,
    target: String,
}

impl Drop for BatchCommandsClientInner {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

struct OutboundRequestState {
    receiver: mpsc::Receiver<tikvpb::BatchCommandsRequest>,
    pending: Option<tikvpb::BatchCommandsRequest>,
    target: String,
    transport_layer_load: Arc<AtomicU64>,
    overload_threshold: u64,
    max_requests: usize,
    batch_wait_size: usize,
    max_wait_time: Duration,
    last_head_received_at: Option<tokio::time::Instant>,
    avg_batch_wait_size: f64,
    turbo_batch_trigger: TurboBatchTrigger,
}

const TURBO_BATCH_ALWAYS: u8 = 0;
const TURBO_BATCH_TIME_BASED: u8 = 1;
const TURBO_BATCH_PROB_BASED: u8 = 2;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
struct TurboBatchOptions {
    #[serde(rename = "v")]
    v: u8,
    #[serde(rename = "n", default)]
    n: u32,
    #[serde(rename = "t", default)]
    t: f64,
    #[serde(rename = "w", default)]
    w: f64,
    #[serde(rename = "p", default)]
    p: f64,
    #[serde(rename = "q", default)]
    q: f64,
}

#[derive(Clone, Copy, Debug)]
struct TurboBatchTrigger {
    opts: TurboBatchOptions,
    max_arrival_interval_secs: f64,
    est_arrival_interval_secs: f64,
    est_fetch_more_prob: f64,
}

impl TurboBatchTrigger {
    fn new(opts: TurboBatchOptions) -> Self {
        TurboBatchTrigger {
            opts,
            max_arrival_interval_secs: 0.0,
            est_arrival_interval_secs: 0.0,
            est_fetch_more_prob: 0.0,
        }
    }

    fn turbo_wait_time(&self) -> Duration {
        if self.opts.t <= 0.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(self.opts.t)
    }

    fn need_fetch_more(&mut self, req_arrival_interval: Duration) -> bool {
        match self.opts.v {
            TURBO_BATCH_TIME_BASED => {
                let turbo_wait_secs = self.opts.t;
                if turbo_wait_secs <= 0.0 {
                    return false;
                }

                let mut this_interval = req_arrival_interval.as_secs_f64();
                if self.max_arrival_interval_secs == 0.0 {
                    self.max_arrival_interval_secs = turbo_wait_secs * self.opts.n as f64;
                }
                if self.max_arrival_interval_secs > 0.0
                    && this_interval > self.max_arrival_interval_secs
                {
                    this_interval = self.max_arrival_interval_secs;
                }

                if self.est_arrival_interval_secs == 0.0 {
                    self.est_arrival_interval_secs = this_interval;
                } else {
                    self.est_arrival_interval_secs = self.opts.w * this_interval
                        + (1.0 - self.opts.w) * self.est_arrival_interval_secs;
                }

                self.est_arrival_interval_secs < turbo_wait_secs * self.opts.p
            }
            TURBO_BATCH_PROB_BASED => {
                let turbo_wait_secs = self.opts.t;
                if turbo_wait_secs <= 0.0 {
                    return false;
                }

                let this_prob = if req_arrival_interval.as_secs_f64() < turbo_wait_secs {
                    1.0
                } else {
                    0.0
                };
                self.est_fetch_more_prob =
                    self.opts.w * this_prob + (1.0 - self.opts.w) * self.est_fetch_more_prob;
                self.est_fetch_more_prob > self.opts.p
            }
            _ => true,
        }
    }

    fn preferred_batch_wait_size(
        &self,
        avg_batch_wait_size: f64,
        def_batch_wait_size: usize,
    ) -> usize {
        if self.opts.v == TURBO_BATCH_ALWAYS {
            return def_batch_wait_size;
        }

        let floored = avg_batch_wait_size.floor();
        let mut batch_wait_size = floored.max(0.0) as usize;
        let fraction = avg_batch_wait_size - floored;
        if fraction >= self.opts.q {
            batch_wait_size = batch_wait_size.saturating_add(1);
        }
        batch_wait_size
    }
}

fn new_turbo_batch_trigger_from_policy(policy: &str) -> (TurboBatchTrigger, bool) {
    let policy = policy.trim();
    let opts = match policy {
        "basic" => TurboBatchOptions::default(),
        "standard" => TurboBatchOptions {
            v: TURBO_BATCH_TIME_BASED,
            t: 0.0001,
            n: 5,
            w: 0.2,
            p: 0.8,
            q: 0.8,
        },
        "positive" => TurboBatchOptions {
            v: TURBO_BATCH_ALWAYS,
            t: 0.0001,
            ..TurboBatchOptions::default()
        },
        _ => {
            let raw_opts = policy.strip_prefix("custom").unwrap_or(policy).trim();
            match serde_json::from_str::<TurboBatchOptions>(raw_opts) {
                Ok(opts) => return (TurboBatchTrigger::new(opts), true),
                Err(_) => {
                    return (
                        TurboBatchTrigger::new(TurboBatchOptions {
                            v: TURBO_BATCH_TIME_BASED,
                            t: 0.0001,
                            n: 5,
                            w: 0.2,
                            p: 0.8,
                            q: 0.8,
                        }),
                        false,
                    )
                }
            }
        }
    };
    (TurboBatchTrigger::new(opts), true)
}

async fn fetch_more_pending_requests(
    state: &mut OutboundRequestState,
    head: &mut tikvpb::BatchCommandsRequest,
    batch_wait_size: usize,
    max_wait_time: Duration,
) {
    if max_wait_time.is_zero() || batch_wait_size == 0 {
        return;
    }

    let deadline = tokio::time::Instant::now() + max_wait_time;
    while head.requests.len() < batch_wait_size && head.requests.len() < state.max_requests {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.duration_since(now);
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, state.receiver.recv()).await {
            Ok(Some(mut next)) => {
                if head.requests.len() + next.requests.len() <= state.max_requests {
                    head.requests.append(&mut next.requests);
                    head.request_ids.append(&mut next.request_ids);
                } else {
                    state.pending = Some(next);
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Do an additional non-blocking try with a single yield, mirroring client-go's
    // "try best to fetch more" behavior.
    let mut yielded = false;
    while head.requests.len() < state.max_requests {
        match state.receiver.try_recv() {
            Ok(mut next) => {
                if head.requests.len() + next.requests.len() <= state.max_requests {
                    head.requests.append(&mut next.requests);
                    head.request_ids.append(&mut next.request_ids);
                } else {
                    state.pending = Some(next);
                    break;
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                if yielded {
                    break;
                }
                tokio::task::yield_now().await;
                yielded = true;
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn outbound_stream(
    receiver: mpsc::Receiver<tikvpb::BatchCommandsRequest>,
    max_requests: usize,
    batch_wait_size: usize,
    max_wait_time: Duration,
    batch_policy: String,
    overload_threshold: u64,
    transport_layer_load: Arc<AtomicU64>,
    target: String,
) -> impl Stream<Item = tikvpb::BatchCommandsRequest> + Send {
    let max_requests = max_requests.max(1);
    let batch_wait_size = batch_wait_size.min(max_requests);
    let (turbo_batch_trigger, _ok) = new_turbo_batch_trigger_from_policy(&batch_policy);
    let state = OutboundRequestState {
        receiver,
        pending: None,
        target,
        transport_layer_load,
        overload_threshold,
        max_requests,
        batch_wait_size,
        max_wait_time,
        last_head_received_at: None,
        avg_batch_wait_size: batch_wait_size as f64,
        turbo_batch_trigger,
    };
    stream::unfold(state, move |mut state| async move {
        let send_loop_started_at = tokio::time::Instant::now();

        let mut head = if let Some(request) = state.pending.take() {
            request
        } else {
            state.receiver.recv().await?
        };

        let head_received_at = tokio::time::Instant::now();
        observe_batch_send_loop_duration_seconds(
            &state.target,
            "wait_head",
            head_received_at.duration_since(send_loop_started_at),
        );
        let head_arrival_interval = state
            .last_head_received_at
            .map(|prev| head_received_at.duration_since(prev))
            .unwrap_or(Duration::ZERO);
        state.last_head_received_at = Some(head_received_at);
        observe_batch_head_arrival_interval_seconds(&state.target, head_arrival_interval);

        while head.requests.len() < max_requests {
            match state.receiver.try_recv() {
                Ok(mut next) => {
                    if head.requests.len() + next.requests.len() <= max_requests {
                        head.requests.append(&mut next.requests);
                        head.request_ids.append(&mut next.request_ids);
                    } else {
                        state.pending = Some(next);
                        break;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        if head.requests.len() < state.max_requests {
            let is_overload = state.max_wait_time > Duration::ZERO
                && state.transport_layer_load.load(Ordering::Relaxed) > state.overload_threshold;
            if is_overload {
                inc_batch_wait_overload();
                let batch_wait_size = state.batch_wait_size;
                let max_wait_time = state.max_wait_time;
                fetch_more_pending_requests(&mut state, &mut head, batch_wait_size, max_wait_time)
                    .await;
            } else {
                let turbo_wait_time = state.turbo_batch_trigger.turbo_wait_time();
                if !turbo_wait_time.is_zero()
                    && !head_arrival_interval.is_zero()
                    && state
                        .turbo_batch_trigger
                        .need_fetch_more(head_arrival_interval)
                {
                    let preferred_wait_size = state
                        .turbo_batch_trigger
                        .preferred_batch_wait_size(state.avg_batch_wait_size, state.batch_wait_size)
                        .min(state.max_requests);
                    let before_len = head.requests.len();
                    fetch_more_pending_requests(
                        &mut state,
                        &mut head,
                        preferred_wait_size,
                        turbo_wait_time,
                    )
                    .await;
                    let more_requests = head.requests.len().saturating_sub(before_len);
                    observe_batch_more_requests_total(&state.target, more_requests);
                }
            }
        }

        observe_batch_send_loop_duration_seconds(
            &state.target,
            "wait_more",
            send_loop_started_at.elapsed(),
        );

        let length = head.requests.len();
        state.avg_batch_wait_size = 0.2 * length as f64 + 0.8 * state.avg_batch_wait_size;
        observe_batch_best_size(&state.target, state.avg_batch_wait_size);

        let batch_size = head.requests.len();
        observe_batch_requests(&state.target, batch_size);
        let pending_in_channel = state.receiver.len();
        let pending_buffered = state
            .pending
            .as_ref()
            .map(|req| req.requests.len())
            .unwrap_or(0);
        observe_batch_pending_requests(
            &state.target,
            batch_size
                .saturating_add(pending_in_channel)
                .saturating_add(pending_buffered),
        );

        observe_batch_send_loop_duration_seconds(
            &state.target,
            "send",
            send_loop_started_at.elapsed(),
        );

        Some((head, state))
    })
}

impl BatchCommandsClient {
    pub(crate) async fn connect<T>(
        client: TikvClient<T>,
        max_outbound_requests: usize,
        batch_policy: String,
        overload_threshold: u64,
        batch_wait_size: usize,
        max_wait_time: Duration,
        target: String,
    ) -> Result<Self>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        T::Error: Into<StdError> + Send + Sync,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        Self::connect_inner(
            client,
            max_outbound_requests,
            batch_policy,
            overload_threshold,
            batch_wait_size,
            max_wait_time,
            target,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect_with_forwarded_host<T>(
        client: TikvClient<T>,
        max_outbound_requests: usize,
        batch_policy: String,
        overload_threshold: u64,
        batch_wait_size: usize,
        max_wait_time: Duration,
        target: String,
        forwarded_host: String,
    ) -> Result<Self>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        T::Error: Into<StdError> + Send + Sync,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        Self::connect_inner(
            client,
            max_outbound_requests,
            batch_policy,
            overload_threshold,
            batch_wait_size,
            max_wait_time,
            target,
            Some(forwarded_host),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_inner<T>(
        client: TikvClient<T>,
        max_outbound_requests: usize,
        batch_policy: String,
        overload_threshold: u64,
        batch_wait_size: usize,
        max_wait_time: Duration,
        target: String,
        forwarded_host: Option<String>,
    ) -> Result<Self>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody> + Clone + Send + Sync + 'static,
        T::Future: Send,
        T::Error: Into<StdError> + Send + Sync,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let connector: ReconnectFn = {
            let client = client.clone();
            let target = target.clone();
            let batch_policy = batch_policy.clone();
            let transport_layer_load = Arc::clone(&transport_layer_load);
            let forwarded_host = forwarded_host.clone();
            Arc::new(move |outbound_rx| {
                let client = client.clone();
                let target = target.clone();
                let batch_policy = batch_policy.clone();
                let transport_layer_load = Arc::clone(&transport_layer_load);
                let forwarded_host = forwarded_host.clone();
                async move {
                    let outbound_stream = outbound_stream(
                        outbound_rx,
                        max_outbound_requests,
                        batch_wait_size,
                        max_wait_time,
                        batch_policy,
                        overload_threshold,
                        transport_layer_load,
                        target,
                    );
                    let mut req = outbound_stream.into_streaming_request();
                    if let Some(host) = forwarded_host.as_deref() {
                        crate::store::apply_forwarded_host_metadata_value(&mut req, host)?;
                    }
                    let response = client
                        .clone()
                        .batch_commands(req)
                        .await
                        .map_err(Error::GrpcAPI)?;
                    Ok(response.into_inner().boxed())
                }
                .boxed()
            })
        };

        let (outbound_tx, outbound_rx) = mpsc::channel(1024);
        let started_at = Instant::now();
        let inbound = connector(outbound_rx).await;
        observe_batch_client_wait_connection_establish(started_at.elapsed());
        let inbound = inbound?;
        Self::new_with_inbound(
            outbound_tx,
            inbound,
            transport_layer_load,
            Some(connector),
            target,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_inbound_for_test(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: impl Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>>
            + Send
            + 'static,
    ) -> Result<Self> {
        Self::new_with_inbound(
            outbound,
            inbound.boxed(),
            Arc::new(AtomicU64::new(0)),
            None,
            "test".to_owned(),
        )
    }

    #[cfg(test)]
    fn new_with_inbound_and_reconnector_for_test(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: impl Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>>
            + Send
            + 'static,
        reconnector: ReconnectFn,
    ) -> Result<Self> {
        Self::new_with_inbound(
            outbound,
            inbound.boxed(),
            Arc::new(AtomicU64::new(0)),
            Some(reconnector),
            "unit_test".to_owned(),
        )
    }

    fn new_with_inbound(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: BatchInboundStream,
        transport_layer_load: Arc<AtomicU64>,
        reconnect: Option<ReconnectFn>,
        target: String,
    ) -> Result<Self> {
        let outbound = Arc::new(RwLock::new(outbound));
        let inflight: Arc<Mutex<HashMap<u64, oneshot::Sender<BatchResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let inflight_reader = inflight.clone();
        let stream_error = Arc::new(Mutex::new(None));
        let stream_error_reader = stream_error.clone();
        let reconnect_reader = reconnect.clone();
        let outbound_reader = outbound.clone();
        let target_reader = target.clone();
        let transport_layer_load_reader = Arc::clone(&transport_layer_load);

        let reader = tokio::spawn(async move {
            let mut inbound = inbound;

            loop {
                let stream_err = match std::panic::AssertUnwindSafe(async {
                    loop {
                        let recv_started_at = Instant::now();
                        let next = inbound.next().await;
                        let recv_elapsed = recv_started_at.elapsed();
                        observe_batch_recv_loop_duration_seconds(&target_reader, "recv", recv_elapsed);
                        if recv_elapsed > BATCH_TAIL_LATENCY_THRESHOLD {
                            observe_batch_recv_tail_latency_seconds(&target_reader, recv_elapsed);
                        }
                        match next {
                            Some(Ok(message)) => {
                                transport_layer_load_reader
                                    .store(message.transport_layer_load, Ordering::Relaxed);
                                let request_ids_len = message.request_ids.len();
                                let responses_len = message.responses.len();
                                if request_ids_len != responses_len {
                                    warn!(
                                        "batch_commands response mismatch: request_ids={}, responses={}",
                                    request_ids_len, responses_len
                                );
                            }

                            let health_feedback = message.health_feedback.clone();
                            let mut responses = message.responses.into_iter();
                            for request_id in message.request_ids.into_iter() {
                                let Some(response) = responses.next() else {
                                    let sender = {
                                        let mut inflight = inflight_reader
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner());
                                        inflight.remove(&request_id)
                                    };
                                    let Some(sender) = sender else {
                                        continue;
                                    };
                                    let _ = sender.send(Err(Error::GrpcAPI(Status::internal(
                                        format!(
                                            "batch_commands response missing response for request_id={request_id} (request_ids={request_ids_len}, responses={responses_len})",
                                        ),
                                    ))));
                                    continue;
                                };

                                let sender = {
                                    let mut inflight =
                                        inflight_reader.lock().unwrap_or_else(|e| e.into_inner());
                                    inflight.remove(&request_id)
                                };
                                let Some(sender) = sender else {
                                    continue;
                                };

                                let result = response
                                    .cmd
                                    .ok_or_else(|| {
                                        Error::StringError(
                                            "batch_commands response missing cmd".to_owned(),
                                        )
                                    })
                                    .map(|cmd| BatchDispatchResult {
                                        cmd,
                                        health_feedback: health_feedback.clone(),
                                    });
                                let _ = sender.send(result);
                            }
                            if responses.next().is_some() {
                                warn!(
                                    "batch_commands response has extra responses: request_ids={}, responses={}",
                                    request_ids_len, responses_len
                                );
                            }
                            observe_batch_recv_loop_duration_seconds(
                                &target_reader,
                                "process",
                                recv_started_at.elapsed(),
                            );
                            }
                            Some(Err(status)) => break status,
                            None => break Status::unavailable("batch_commands stream ended"),
                        }
                    }
                })
                .catch_unwind()
                .await
                {
                    Ok(status) => status,
                    Err(panic) => {
                        inc_panic_total("batch-recv-loop");
                        let message = if let Some(message) =
                            panic.as_ref().downcast_ref::<&'static str>()
                        {
                            (*message).to_owned()
                        } else if let Some(message) = panic.as_ref().downcast_ref::<String>() {
                            message.clone()
                        } else {
                            "non-string panic payload".to_owned()
                        };
                        warn!(
                            "batch_commands reader loop panicked: target={}, payload={}",
                            target_reader, message
                        );
                        Status::internal("batch_commands reader loop panicked")
                    }
                };

                {
                    let mut guard = stream_error_reader
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    *guard = Some(stream_err.clone());
                }

                {
                    let mut inflight = inflight_reader.lock().unwrap_or_else(|e| e.into_inner());
                    for (_, sender) in inflight.drain() {
                        let _ = sender.send(Err(Error::GrpcAPI(stream_err.clone())));
                    }
                }

                let Some(reconnect) = reconnect_reader.as_ref() else {
                    break;
                };

                let unavailable_started_at = Instant::now();
                let mut retry_backoff = Duration::from_millis(200);
                loop {
                    let attempt_started_at = Instant::now();
                    let (outbound_tx, outbound_rx) = mpsc::channel(1024);

                    match reconnect(outbound_rx).await {
                        Ok(new_inbound) => {
                            observe_batch_client_unavailable(unavailable_started_at.elapsed());
                            observe_batch_client_reset(attempt_started_at.elapsed());

                            let mut guard = stream_error_reader
                                .lock()
                                .unwrap_or_else(|e| e.into_inner());
                            *guard = None;
                            drop(guard);

                            let mut guard =
                                outbound_reader.write().unwrap_or_else(|e| e.into_inner());
                            *guard = outbound_tx;
                            drop(guard);

                            inbound = new_inbound;
                            break;
                        }
                        Err(err) => {
                            warn!(
                                "batch_commands stream reconnect failed: target={}, err={err:?}",
                                target_reader
                            );
                            crate::util::sleep_backoff(retry_backoff).await;
                            retry_backoff = (retry_backoff * 2).min(Duration::from_secs(10));
                        }
                    }
                }
            }
        });

        let inner = BatchCommandsClientInner {
            outbound,
            inflight,
            next_id: AtomicU64::new(1),
            reader,
            stream_error,
            target,
        };
        Ok(BatchCommandsClient {
            inner: Arc::new(inner),
        })
    }

    pub(crate) async fn dispatch(
        &self,
        cmd: tikvpb::batch_commands_request::request::Cmd,
        timeout: Duration,
    ) -> Result<BatchDispatchResult> {
        if let Some(status) = self
            .inner
            .stream_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return Err(Error::GrpcAPI(status));
        }

        let request_started_at = Instant::now();
        let request_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        {
            let mut inflight = self
                .inner
                .inflight
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            inflight.insert(request_id, sender);
        }
        let _guard = InflightGuard {
            request_id,
            inflight: self.inner.inflight.clone(),
        };

        let request = tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request { cmd: Some(cmd) }],
            request_ids: vec![request_id],
        };

        let outbound = self
            .inner
            .outbound
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let send_started_at = Instant::now();
        let send_result = outbound.send(request).await;
        let send_elapsed = send_started_at.elapsed();
        if send_elapsed > BATCH_TAIL_LATENCY_THRESHOLD {
            observe_batch_send_tail_latency_seconds(&self.inner.target, send_elapsed);
        }
        observe_batch_request_duration_seconds("send", request_started_at.elapsed());
        if send_result.is_err() {
            let status =
                Status::unavailable("batch_commands stream is unavailable (sender dropped)");
            let mut guard = self
                .inner
                .stream_error
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = Some(status.clone());
            return Err(Error::GrpcAPI(status));
        }

        let recv_started_at = Instant::now();
        let result = match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::GrpcAPI(Status::unavailable(
                "batch_commands response channel closed",
            ))),
            Err(_) => Err(Error::GrpcAPI(Status::deadline_exceeded(
                "batch_commands request timed out",
            ))),
        };
        observe_batch_request_duration_seconds("recv", recv_started_at.elapsed());
        observe_batch_request_duration_seconds("done", request_started_at.elapsed());
        result
    }
}

struct InflightGuard {
    request_id: u64,
    inflight: Arc<Mutex<HashMap<u64, oneshot::Sender<BatchResponse>>>>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let mut inflight = self.inflight.lock().unwrap_or_else(|e| e.into_inner());
        inflight.remove(&self.request_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use serial_test::serial;
    use tonic::body::BoxBody;
    use tonic::codegen::http;
    use tonic::codegen::Service;

    #[derive(Clone)]
    struct CaptureService {
        seen_headers: Arc<Mutex<Option<http::HeaderMap>>>,
    }

    impl Service<http::Request<BoxBody>> for CaptureService {
        type Response = http::Response<BoxBody>;
        type Error = Infallible;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(std::result::Result::Ok(()))
        }

        fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
            let seen_headers = Arc::clone(&self.seen_headers);
            Box::pin(async move {
                *seen_headers.lock().unwrap() = Some(req.headers().clone());

                let resp = http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "12")
                    .body(tonic::body::empty_body())
                    .unwrap();
                std::result::Result::Ok(resp)
            })
        }
    }

    fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
        metric
            .get_label()
            .iter()
            .find(|pair| pair.get_name() == name)
            .map(|pair| pair.get_value())
    }

    fn client_with_channels(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: mpsc::Receiver<std::result::Result<tikvpb::BatchCommandsResponse, Status>>,
    ) -> BatchCommandsClient {
        let inbound_stream = stream::unfold(inbound, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        BatchCommandsClient::new_with_inbound_for_test(outbound, inbound_stream).unwrap()
    }

    #[tokio::test]
    async fn test_batch_commands_connect_with_forwarded_host_sets_metadata_header() {
        let forwarded_host = "127.0.0.1:20160".to_owned();

        let seen_headers = Arc::new(Mutex::new(None));
        let service = CaptureService {
            seen_headers: Arc::clone(&seen_headers),
        };
        let client = TikvClient::new(service);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            BatchCommandsClient::connect_with_forwarded_host(
                client,
                128,
                "basic".to_owned(),
                0,
                8,
                Duration::ZERO,
                "test".to_owned(),
                forwarded_host.clone(),
            ),
        )
        .await
        .expect("connect_with_forwarded_host should not hang");
        assert!(
            result.is_err(),
            "expected connect_with_forwarded_host to return a grpc error"
        );

        let headers = seen_headers
            .lock()
            .unwrap()
            .clone()
            .expect("expected request headers captured");
        let value = headers
            .get(super::super::forwarding::FORWARD_METADATA_KEY)
            .expect("expected forwarded host metadata header");
        assert_eq!(value.to_str().unwrap(), forwarded_host);
    }

    #[tokio::test]
    async fn test_batch_commands_connect_does_not_set_forwarded_host_metadata_from_task_locals() {
        let seen_headers = Arc::new(Mutex::new(None));
        let service = CaptureService {
            seen_headers: Arc::clone(&seen_headers),
        };
        let client = TikvClient::new(service);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            super::super::scope_forwarded_host(
                "127.0.0.1:20161".to_owned(),
                BatchCommandsClient::connect(
                    client,
                    128,
                    "basic".to_owned(),
                    0,
                    8,
                    Duration::ZERO,
                    "test".to_owned(),
                ),
            ),
        )
        .await
        .expect("connect should not hang");
        assert!(result.is_err(), "expected connect to return a grpc error");

        let headers = seen_headers
            .lock()
            .unwrap()
            .clone()
            .expect("expected request headers captured");
        assert!(
            headers
                .get(super::super::forwarding::FORWARD_METADATA_KEY)
                .is_none(),
            "base batch stream should not include forwarded host metadata"
        );
    }

    #[tokio::test]
    async fn test_batch_commands_client_matches_out_of_order_responses() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let t1 = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };
        let t2 = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let req1 = out_rx.recv().await.expect("first batch request");
        let req2 = out_rx.recv().await.expect("second batch request");
        let id1 = *req1.request_ids.first().expect("request id");
        let id2 = *req2.request_ids.first().expect("request id");

        let response_for = |request_id| tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };

        in_tx
            .send(Ok(response_for(id2)))
            .await
            .expect("send response 2");
        in_tx
            .send(Ok(response_for(id1)))
            .await
            .expect("send response 1");

        let r1 = t1.await.expect("task 1").expect("task 1 ok");
        let r2 = t2.await.expect("task 2").expect("task 2 ok");
        assert!(matches!(
            r1.cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));
        assert!(matches!(
            r2.cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));
    }

    #[tokio::test]
    async fn test_outbound_stream_batches_ready_requests() {
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            8,
            0,
            Duration::ZERO,
            "basic".to_owned(),
            200,
            transport_layer_load,
            "unit_test_ready".to_owned(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");
        out_tx.send(mk_req(2)).await.expect("send req 2");

        let merged = stream.next().await.expect("merged request");
        assert_eq!(merged.requests.len(), 2);
        assert_eq!(merged.request_ids, vec![1, 2]);

        for req in merged.requests.into_iter() {
            match req.cmd.expect("cmd should be present") {
                tikvpb::batch_commands_request::request::Cmd::Empty(_) => {}
                other => panic!("unexpected cmd in merged request: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_outbound_stream_respects_batch_limit() {
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            2,
            0,
            Duration::ZERO,
            "basic".to_owned(),
            200,
            transport_layer_load,
            "unit_test_limit".to_owned(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");
        out_tx.send(mk_req(2)).await.expect("send req 2");
        out_tx.send(mk_req(3)).await.expect("send req 3");

        let first = stream.next().await.expect("first batch");
        assert_eq!(first.request_ids, vec![1, 2]);

        let second = stream.next().await.expect("second batch");
        assert_eq!(second.request_ids, vec![3]);
    }

    #[tokio::test]
    async fn test_outbound_stream_waits_for_more_requests_when_configured() {
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(201));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            8,
            2,
            Duration::from_millis(100),
            "basic".to_owned(),
            200,
            transport_layer_load,
            "unit_test_wait".to_owned(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");

        let next = stream.next();
        tokio::pin!(next);

        tokio::select! {
            batch = &mut next => {
                let ids = batch.map(|batch| batch.request_ids);
                panic!("expected outbound stream to wait for more requests, but got {ids:?}");
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }

        out_tx.send(mk_req(2)).await.expect("send req 2");

        let batch = next.await.expect("first batch");
        assert_eq!(batch.request_ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_outbound_stream_does_not_wait_when_not_overloaded_and_policy_basic() {
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            8,
            2,
            Duration::from_millis(100),
            "basic".to_owned(),
            200,
            transport_layer_load,
            "unit_test_no_wait".to_owned(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");

        let batch = tokio::time::timeout(Duration::from_millis(20), stream.next())
            .await
            .expect("expected stream to yield without waiting")
            .expect("batch");
        assert_eq!(batch.request_ids, vec![1]);
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_outbound_stream_turbo_policy_waits_for_more_requests() {
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            8,
            2,
            Duration::ZERO,
            "custom {\"v\":0,\"t\":0.05}".to_owned(),
            200,
            transport_layer_load,
            "unit_test_turbo_wait".to_owned(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");
        let first = stream.next().await.expect("first batch");
        assert_eq!(first.request_ids, vec![1]);

        tokio::time::sleep(Duration::from_millis(1)).await;

        out_tx.send(mk_req(2)).await.expect("send req 2");
        let next = stream.next();
        tokio::pin!(next);

        tokio::select! {
            batch = &mut next => {
                let ids = batch.map(|batch| batch.request_ids);
                panic!("expected outbound stream to wait for more requests, but got {ids:?}");
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }

        out_tx.send(mk_req(3)).await.expect("send req 3");

        let batch = next.await.expect("second batch");
        assert_eq!(batch.request_ids, vec![2, 3]);

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_more_requests_total")
            .expect("batch_more_requests_total summary not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some("unit_test_turbo_wait")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            found,
            "expected batch_more_requests_total summary to record observations"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_outbound_stream_overload_wait_increments_batch_wait_overload_counter() {
        let target = "unit_test_target_batch_metrics_overload_wait".to_owned();
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(500));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            8,
            2,
            Duration::from_millis(50),
            "basic".to_owned(),
            200,
            transport_layer_load,
            target,
        ));

        let before = prometheus::gather();
        let before_value = before
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_wait_overload")
            .map(|family| {
                family
                    .get_metric()
                    .iter()
                    .map(|metric| metric.get_counter().get_value())
                    .sum::<f64>()
            })
            .unwrap_or(0.0);

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");
        tokio::time::sleep(Duration::from_millis(1)).await;
        out_tx.send(mk_req(2)).await.expect("send req 2");

        let batch = stream.next().await.expect("first batch");
        assert_eq!(batch.request_ids, vec![1, 2]);

        let after = prometheus::gather();
        let after_value = after
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_wait_overload")
            .map(|family| {
                family
                    .get_metric()
                    .iter()
                    .map(|metric| metric.get_counter().get_value())
                    .sum::<f64>()
            })
            .unwrap_or(0.0);

        assert!(
            after_value >= before_value + 1.0,
            "expected batch_wait_overload counter to increase"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_outbound_stream_records_batch_metrics() {
        let target = "unit_test_target_batch_metrics_outbound_stream".to_owned();
        let (out_tx, out_rx) = mpsc::channel(8);
        let transport_layer_load = Arc::new(AtomicU64::new(0));
        let mut stream = Box::pin(outbound_stream(
            out_rx,
            2,
            0,
            Duration::ZERO,
            "basic".to_owned(),
            200,
            transport_layer_load,
            target.clone(),
        ));

        let mk_req = |request_id| tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![request_id],
        };

        out_tx.send(mk_req(1)).await.expect("send req 1");
        out_tx.send(mk_req(2)).await.expect("send req 2");
        out_tx.send(mk_req(3)).await.expect("send req 3");

        let _ = stream.next().await.expect("first batch");
        let _ = stream.next().await.expect("second batch");

        let families = prometheus::gather();

        let requests_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_requests")
            .expect("batch_requests histogram not registered");
        let requests_found = requests_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            requests_found,
            "expected batch_requests metrics for target not found"
        );

        let pending_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_pending_requests")
            .expect("batch_pending_requests histogram not registered");
        let pending_found = pending_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            pending_found,
            "expected batch_pending_requests metrics for target not found"
        );

        let send_loop_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_send_loop_duration_seconds")
            .expect("batch_send_loop_duration_seconds summary not registered");
        let send_loop_found = send_loop_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && label_value(metric, "step") == Some("wait_head")
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            send_loop_found,
            "expected batch_send_loop_duration_seconds summary for target not found"
        );

        let head_interval_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_head_arrival_interval_seconds")
            .expect("batch_head_arrival_interval_seconds summary not registered");
        let head_interval_found = head_interval_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            head_interval_found,
            "expected batch_head_arrival_interval_seconds summary for target not found"
        );

        let best_size_family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_best_size")
            .expect("batch_best_size summary not registered");
        let best_size_found = best_size_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            best_size_found,
            "expected batch_best_size summary for target not found"
        );
    }

    #[tokio::test]
    async fn test_batch_commands_client_timeout_returns_deadline_exceeded() {
        let (out_tx, _out_rx) = mpsc::channel(8);
        let (_in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let err = client
            .dispatch(
                tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                ),
                Duration::from_millis(10),
            )
            .await
            .expect_err("expected timeout error");

        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::DeadlineExceeded),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_batch_commands_client_stream_error_fails_inflight_requests() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let _req = out_rx.recv().await.expect("batch request");

        in_tx
            .send(Err(Status::unavailable("boom")))
            .await
            .expect("send stream error");

        let err = task
            .await
            .expect("task join")
            .expect_err("expected stream error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_batch_commands_client_reader_panic_records_panic_total() {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;

        fn counter_value(families: &[prometheus::proto::MetricFamily], label: &str) -> f64 {
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_panic_total")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "type") == Some(label)
                            && metric.get_counter().get_value() > 0.0
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let before = counter_value(&prometheus::gather(), "batch-recv-loop");

        let (start_tx, start_rx) = oneshot::channel::<()>();
        let mut start_rx = start_rx;
        let inbound = stream::poll_fn(move |cx| match Pin::new(&mut start_rx).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => panic!("unit_test_batch_recv_loop_panic"),
        });

        let (out_tx, mut out_rx) = mpsc::channel(8);
        let client = BatchCommandsClient::new_with_inbound_for_test(out_tx, inbound).unwrap();

        let task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let _req = out_rx.recv().await.expect("batch request");
        let _ = start_tx.send(());

        let err = task
            .await
            .expect("task join")
            .expect_err("expected panic error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Internal),
            other => panic!("unexpected error: {other:?}"),
        }

        let after = counter_value(&prometheus::gather(), "batch-recv-loop");
        assert!(
            after >= before + 1.0,
            "expected panic_total(batch-recv-loop) to increase"
        );
    }

    #[tokio::test]
    async fn test_batch_commands_client_stream_error_fails_fast_for_future_requests() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let _req = out_rx.recv().await.expect("batch request");
        in_tx
            .send(Err(Status::unavailable("boom")))
            .await
            .expect("send stream error");

        let err = task
            .await
            .expect("task join")
            .expect_err("expected stream error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("unexpected error: {other:?}"),
        }

        let err = tokio::time::timeout(
            Duration::from_millis(100),
            client.dispatch(
                tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                ),
                Duration::from_secs(30),
            ),
        )
        .await
        .expect("second dispatch should not hang")
        .expect_err("expected stream error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("unexpected error: {other:?}"),
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(50), out_rx.recv())
                .await
                .is_err(),
            "stream error should prevent sending new batch requests"
        );
    }

    #[tokio::test]
    async fn test_batch_commands_client_reconnects_after_stream_error() -> Result<()> {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let inbound_stream = stream::unfold(in_rx, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });

        let (reconnect_in_tx, reconnect_in_rx) = mpsc::channel(8);
        let (reconnect_out_tx, reconnect_out_rx) =
            tokio::sync::oneshot::channel::<mpsc::Receiver<tikvpb::BatchCommandsRequest>>();

        let reconnect_out_tx = Arc::new(Mutex::new(Some(reconnect_out_tx)));
        let reconnect_in_rx = Arc::new(Mutex::new(Some(reconnect_in_rx)));
        let reconnector: ReconnectFn = {
            let reconnect_out_tx = Arc::clone(&reconnect_out_tx);
            let reconnect_in_rx = Arc::clone(&reconnect_in_rx);
            Arc::new(move |outbound_rx| {
                let reconnect_out_tx = Arc::clone(&reconnect_out_tx);
                let reconnect_in_rx = Arc::clone(&reconnect_in_rx);
                Box::pin(async move {
                    if let Some(tx) = reconnect_out_tx.lock().unwrap().take() {
                        let _ = tx.send(outbound_rx);
                    }

                    let inbound_rx = reconnect_in_rx
                        .lock()
                        .unwrap()
                        .take()
                        .expect("reconnect inbound receiver");
                    let inbound_stream = stream::unfold(inbound_rx, |mut rx| async move {
                        rx.recv().await.map(|message| (message, rx))
                    });
                    Ok(inbound_stream.boxed())
                })
            })
        };

        let client = BatchCommandsClient::new_with_inbound_and_reconnector_for_test(
            out_tx,
            inbound_stream,
            reconnector,
        )?;

        let task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let _req = out_rx.recv().await.expect("batch request");
        in_tx
            .send(Err(Status::unavailable("boom")))
            .await
            .expect("send stream error");

        let err = task
            .await
            .expect("task join")
            .expect_err("expected stream error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
            other => panic!("unexpected error: {other:?}"),
        }

        let mut reconnect_out_rx = tokio::time::timeout(Duration::from_secs(1), reconnect_out_rx)
            .await
            .expect("reconnect should create a new outbound channel")
            .expect("reconnect outbound channel");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let is_healthy = client
                    .inner
                    .stream_error
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .is_none();
                if is_healthy {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnect should clear stream error");

        let dispatch = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let req = tokio::time::timeout(Duration::from_secs(1), reconnect_out_rx.recv())
            .await
            .expect("dispatch should send via reconnected batch stream")
            .expect("batch request");
        let request_id = *req.request_ids.first().expect("request id");

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        reconnect_in_tx
            .send(Ok(response))
            .await
            .expect("send response");

        let result = dispatch.await.expect("dispatch join")?;
        assert!(matches!(
            result.cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_commands_client_response_mismatch_fails_missing_request_id() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let t1 = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };
        let t2 = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let req1 = out_rx.recv().await.expect("batch request");
        let req2 = out_rx.recv().await.expect("batch request");
        let id1 = *req1.request_ids.first().expect("request id");
        let id2 = *req2.request_ids.first().expect("request id");

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![id1, id2],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let r1 = t1.await.expect("task 1");
        let r2 = t2.await.expect("task 2");

        let ok = r1.as_ref().ok().or_else(|| r2.as_ref().ok());
        assert!(
            ok.is_some_and(|result| matches!(
                result.cmd,
                tikvpb::batch_commands_response::response::Cmd::Empty(_)
            )),
            "expected one request to receive the empty response"
        );

        let err = r1
            .err()
            .or_else(|| r2.err())
            .expect("missing response should error");
        match err {
            Error::GrpcAPI(status) => assert_eq!(status.code(), tonic::Code::Internal),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_batch_commands_client_records_batch_recv_tail_latency_seconds_histogram() {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let request_task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let req = out_rx.recv().await.expect("batch request");
        let request_id = *req.request_ids.first().expect("request id");

        tokio::time::sleep(Duration::from_millis(30)).await;

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let result = request_task.await.expect("request task");
        assert!(matches!(
            result.expect("dispatch response").cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_recv_tail_latency_seconds")
            .expect("batch_recv_tail_latency_seconds histogram not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some("test")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            found,
            "expected batch_recv_tail_latency_seconds histogram to record observations"
        );

        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_recv_loop_duration_seconds")
            .expect("batch_recv_loop_duration_seconds summary not registered");
        let recv_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some("test")
                && label_value(metric, "step") == Some("recv")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            recv_found,
            "expected batch_recv_loop_duration_seconds(recv) summary to record observations"
        );
        let process_found = family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some("test")
                && label_value(metric, "step") == Some("process")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            process_found,
            "expected batch_recv_loop_duration_seconds(process) summary to record observations"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_batch_commands_client_records_batch_send_tail_latency_seconds_histogram() {
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let (in_tx, in_rx) = mpsc::channel(8);

        out_tx
            .send(tikvpb::BatchCommandsRequest::default())
            .await
            .expect("fill outbound channel");

        let client = client_with_channels(out_tx, in_rx);

        let request_task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(30)).await;
        out_rx.recv().await.expect("drain dummy request");

        let req = out_rx.recv().await.expect("batch request");
        let request_id = *req.request_ids.first().expect("request id");

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let result = request_task.await.expect("request task");
        assert!(matches!(
            result.expect("dispatch response").cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));

        let families = prometheus::gather();
        let family = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_batch_send_tail_latency_seconds")
            .expect("batch_send_tail_latency_seconds histogram not registered");
        let found = family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some("test")
                && metric.get_histogram().get_sample_count() >= 1
        });
        assert!(
            found,
            "expected batch_send_tail_latency_seconds histogram to record observations"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_batch_commands_client_records_batch_request_duration_seconds_summary() {
        fn sample_count(families: &[prometheus::proto::MetricFamily], step: &str) -> u64 {
            families
                .iter()
                .find(|family| family.get_name() == "tikv_client_batch_request_duration_seconds")
                .map(|family| {
                    family
                        .get_metric()
                        .iter()
                        .filter(|metric| label_value(metric, "step") == Some(step))
                        .map(|metric| metric.get_histogram().get_sample_count())
                        .sum()
                })
                .unwrap_or(0)
        }

        let before = prometheus::gather();
        let before_send = sample_count(&before, "send");
        let before_recv = sample_count(&before, "recv");
        let before_done = sample_count(&before, "done");

        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let client = client_with_channels(out_tx, in_rx);

        let request_task = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .dispatch(
                        tikvpb::batch_commands_request::request::Cmd::Empty(
                            tikvpb::BatchCommandsEmptyRequest::default(),
                        ),
                        Duration::from_secs(1),
                    )
                    .await
            })
        };

        let req = out_rx.recv().await.expect("batch request");
        let request_id = *req.request_ids.first().expect("request id");

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let result = request_task.await.expect("request task");
        assert!(matches!(
            result.expect("dispatch response").cmd,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));

        let after = prometheus::gather();
        let after_send = sample_count(&after, "send");
        let after_recv = sample_count(&after, "recv");
        let after_done = sample_count(&after, "done");

        assert!(
            after_send >= before_send + 1,
            "expected send sample count to increase"
        );
        assert!(
            after_recv >= before_recv + 1,
            "expected recv sample count to increase"
        );
        assert!(
            after_done >= before_done + 1,
            "expected done sample count to increase"
        );
    }
}
