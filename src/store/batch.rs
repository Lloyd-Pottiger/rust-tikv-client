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

use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Error;
use crate::Result;

type BatchResponse = std::result::Result<tikvpb::batch_commands_response::response::Cmd, Error>;

#[derive(Clone)]
pub(crate) struct BatchCommandsClient {
    inner: Arc<BatchCommandsClientInner>,
}

struct BatchCommandsClientInner {
    outbound: mpsc::Sender<tikvpb::BatchCommandsRequest>,
    inflight: Arc<Mutex<HashMap<u64, oneshot::Sender<BatchResponse>>>>,
    next_id: AtomicU64,
    reader: JoinHandle<()>,
}

impl Drop for BatchCommandsClientInner {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl BatchCommandsClient {
    pub(crate) async fn connect(client: TikvClient<Channel>) -> Result<Self> {
        let (outbound_tx, outbound_rx) = mpsc::channel(1024);
        let outbound_stream = stream::unfold(outbound_rx, |mut rx| async move {
            rx.recv().await.map(|request| (request, rx))
        });

        let req = outbound_stream.into_streaming_request();
        let response = client
            .clone()
            .batch_commands(req)
            .await
            .map_err(Error::GrpcAPI)?;
        Self::new_with_inbound(outbound_tx, response.into_inner())
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

        let reader = tokio::spawn(async move {
            let mut inbound = Box::pin(inbound);
            let stream_err = loop {
                match inbound.next().await {
                    Some(Ok(message)) => {
                        if message.request_ids.len() != message.responses.len() {
                            warn!(
                                "batch_commands response mismatch: request_ids={}, responses={}",
                                message.request_ids.len(),
                                message.responses.len()
                            );
                        }

                        for (request_id, response) in message
                            .request_ids
                            .into_iter()
                            .zip(message.responses.into_iter())
                        {
                            let sender = {
                                let mut inflight =
                                    inflight_reader.lock().unwrap_or_else(|e| e.into_inner());
                                inflight.remove(&request_id)
                            };
                            let Some(sender) = sender else {
                                continue;
                            };

                            let result = response.cmd.ok_or_else(|| {
                                Error::StringError("batch_commands response missing cmd".to_owned())
                            });
                            let _ = sender.send(result);
                        }
                    }
                    Some(Err(status)) => break status,
                    None => break Status::unavailable("batch_commands stream ended"),
                }
            };

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
        };
        Ok(BatchCommandsClient {
            inner: Arc::new(inner),
        })
    }

    pub(crate) async fn dispatch(
        &self,
        cmd: tikvpb::batch_commands_request::request::Cmd,
        timeout: Duration,
    ) -> Result<tikvpb::batch_commands_response::response::Cmd> {
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

        self.inner.outbound.send(request).await.map_err(|_| {
            Error::GrpcAPI(Status::unavailable(
                "batch_commands stream is unavailable (sender dropped)",
            ))
        })?;

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
            r1,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));
        assert!(matches!(
            r2,
            tikvpb::batch_commands_response::response::Cmd::Empty(_)
        ));
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
}
