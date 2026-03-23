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
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::IntoStreamingRequest;
use tonic::Status;

use crate::proto::kvrpcpb;
use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::stats::{
    observe_batch_client_reset, observe_batch_client_unavailable,
    observe_batch_client_wait_connection_establish, observe_batch_pending_requests,
    observe_batch_requests,
};
use crate::Error;
use crate::Result;

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
}

fn outbound_stream(
    receiver: mpsc::Receiver<tikvpb::BatchCommandsRequest>,
    max_requests: usize,
    target: String,
) -> impl Stream<Item = tikvpb::BatchCommandsRequest> + Send {
    let max_requests = max_requests.max(1);
    let state = OutboundRequestState {
        receiver,
        pending: None,
        target,
    };
    stream::unfold(state, move |mut state| async move {
        let mut head = if let Some(request) = state.pending.take() {
            request
        } else {
            state.receiver.recv().await?
        };

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

        Some((head, state))
    })
}

impl BatchCommandsClient {
    pub(crate) async fn connect(
        client: TikvClient<Channel>,
        max_outbound_requests: usize,
        target: String,
    ) -> Result<Self> {
        let connector: ReconnectFn = {
            let client = client.clone();
            let target = target.clone();
            Arc::new(move |outbound_rx| {
                let client = client.clone();
                let target = target.clone();
                async move {
                    let outbound_stream =
                        outbound_stream(outbound_rx, max_outbound_requests, target);
                    let req = outbound_stream.into_streaming_request();
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
        Self::new_with_inbound(outbound_tx, inbound, Some(connector), target)
    }

    #[cfg(test)]
    pub(crate) fn new_with_inbound_for_test(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: impl Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>>
            + Send
            + 'static,
    ) -> Result<Self> {
        Self::new_with_inbound(outbound, inbound.boxed(), None, "test".to_owned())
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
            Some(reconnector),
            "unit_test".to_owned(),
        )
    }

    fn new_with_inbound(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: BatchInboundStream,
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

        let reader = tokio::spawn(async move {
            let mut inbound = inbound;

            loop {
                let stream_err = loop {
                    match inbound.next().await {
                        Some(Ok(message)) => {
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
                        }
                        Some(Err(status)) => break status,
                        None => break Status::unavailable("batch_commands stream ended"),
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
                            tokio::time::sleep(retry_backoff).await;
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
        if outbound.send(request).await.is_err() {
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

        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::GrpcAPI(Status::unavailable(
                "batch_commands response channel closed",
            ))),
            Err(_) => Err(Error::GrpcAPI(Status::deadline_exceeded(
                "batch_commands request timed out",
            ))),
        }
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
    use serial_test::serial;

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
        let mut stream = Box::pin(outbound_stream(out_rx, 8, "unit_test_ready".to_owned()));

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
        let mut stream = Box::pin(outbound_stream(out_rx, 2, "unit_test_limit".to_owned()));

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
    #[serial(metrics)]
    async fn test_outbound_stream_records_batch_metrics() {
        let target = "unit_test_target_batch_metrics_outbound_stream".to_owned();
        let (out_tx, out_rx) = mpsc::channel(8);
        let mut stream = Box::pin(outbound_stream(out_rx, 2, target.clone()));

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
            .find(|family| family.get_name() == "tikv_client_rust_batch_requests")
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
            .find(|family| family.get_name() == "tikv_client_rust_batch_pending_requests")
            .expect("batch_pending_requests histogram not registered");
        let pending_found = pending_family.get_metric().iter().any(|metric| {
            label_value(metric, "target") == Some(target.as_str())
                && metric.get_histogram().get_sample_count() >= 2
        });
        assert!(
            pending_found,
            "expected batch_pending_requests metrics for target not found"
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
}
