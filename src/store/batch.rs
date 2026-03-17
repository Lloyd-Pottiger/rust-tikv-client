use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream;
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
use crate::Error;
use crate::Result;

const BATCH_COMMANDS_OUTBOUND_MAX_REQUESTS: usize = 128;

#[derive(Clone, Debug)]
pub(crate) struct BatchDispatchResult {
    pub(crate) cmd: tikvpb::batch_commands_response::response::Cmd,
    pub(crate) health_feedback: Option<kvrpcpb::HealthFeedback>,
}

type BatchResponse = std::result::Result<BatchDispatchResult, Error>;

#[derive(Clone)]
pub(crate) struct BatchCommandsClient {
    inner: Arc<BatchCommandsClientInner>,
}

struct BatchCommandsClientInner {
    outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
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
}

fn outbound_stream(
    receiver: mpsc::Receiver<tikvpb::BatchCommandsRequest>,
    max_requests: usize,
) -> impl Stream<Item = tikvpb::BatchCommandsRequest> + Send {
    let max_requests = max_requests.max(1);
    let state = OutboundRequestState {
        receiver,
        pending: None,
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

        Some((head, state))
    })
}

impl BatchCommandsClient {
    pub(crate) async fn connect(client: TikvClient<Channel>) -> Result<Self> {
        let (outbound_tx, outbound_rx) = mpsc::channel(1024);
        let outbound_stream = outbound_stream(outbound_rx, BATCH_COMMANDS_OUTBOUND_MAX_REQUESTS);

        let req = outbound_stream.into_streaming_request();
        let response = client
            .clone()
            .batch_commands(req)
            .await
            .map_err(Error::GrpcAPI)?;
        Self::new_with_inbound(outbound_tx, response.into_inner())
    }

    #[cfg(test)]
    pub(crate) fn new_with_inbound_for_test(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: impl Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>>
            + Send
            + 'static,
    ) -> Result<Self> {
        Self::new_with_inbound(outbound, inbound)
    }

    fn new_with_inbound(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: impl Stream<Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>>
            + Send
            + 'static,
    ) -> Result<Self> {
        let inflight: Arc<Mutex<HashMap<u64, oneshot::Sender<BatchResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let inflight_reader = inflight.clone();
        let stream_error = Arc::new(Mutex::new(None));
        let stream_error_reader = stream_error.clone();

        let reader = tokio::spawn(async move {
            let mut inbound = Box::pin(inbound);
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
                                    let mut inflight =
                                        inflight_reader.lock().unwrap_or_else(|e| e.into_inner());
                                    inflight.remove(&request_id)
                                };
                                let Some(sender) = sender else {
                                    continue;
                                };
                                let _ = sender.send(Err(Error::GrpcAPI(Status::internal(format!(
                                    "batch_commands response missing response for request_id={request_id} (request_ids={request_ids_len}, responses={responses_len})",
                                )))));
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

            let mut guard = stream_error_reader
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = Some(stream_err.clone());
            drop(guard);

            let mut inflight = inflight_reader.lock().unwrap_or_else(|e| e.into_inner());
            for (_, sender) in inflight.drain() {
                let _ = sender.send(Err(Error::GrpcAPI(stream_err.clone())));
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

        if self.inner.outbound.send(request).await.is_err() {
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

    fn client_with_channels(
        outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
        inbound: mpsc::Receiver<std::result::Result<tikvpb::BatchCommandsResponse, Status>>,
    ) -> BatchCommandsClient {
        let inbound_stream = stream::unfold(inbound, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        BatchCommandsClient::new_with_inbound(outbound, inbound_stream).unwrap()
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
        let mut stream = Box::pin(outbound_stream(out_rx, 8));

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
        let mut stream = Box::pin(outbound_stream(out_rx, 2));

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
