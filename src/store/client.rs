// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use log::debug;
use tonic::codec::CompressionEncoding;
use tonic::codegen::Body;
use tonic::codegen::Bytes;
use tonic::codegen::StdError;
use tonic::transport::Channel;

use super::batch::BatchCommandsClient;
use super::Request;
use crate::proto::kvrpcpb;
use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Error;
use crate::GrpcCompressionType;
use crate::Result;
use crate::SecurityManager;

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    grpc_max_decoding_message_size: usize,
    grpc_compression_type: GrpcCompressionType,
    enable_batch_rpc: bool,
}

fn build_tikv_client<T>(
    channel: T,
    grpc_max_decoding_message_size: usize,
    grpc_compression_type: GrpcCompressionType,
) -> TikvClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    let mut client =
        TikvClient::new(channel).max_decoding_message_size(grpc_max_decoding_message_size);
    if grpc_compression_type == GrpcCompressionType::Gzip {
        client = client
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
    }
    client
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        let timeout = self.timeout;
        let grpc_max_decoding_message_size = self.grpc_max_decoding_message_size;
        let grpc_compression_type = self.grpc_compression_type;
        let enable_batch_rpc = self.enable_batch_rpc;

        let rpc_client = self
            .security_mgr
            .connect(address, move |channel| {
                build_tikv_client(
                    channel,
                    grpc_max_decoding_message_size,
                    grpc_compression_type,
                )
            })
            .await?;

        Ok(KvRpcClient::new(rpc_client, timeout, enable_batch_rpc).await)
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_client: TikvClient<Channel>,
    timeout: Duration,
    batch: Option<BatchCommandsClient>,
}

impl KvRpcClient {
    async fn new(
        rpc_client: TikvClient<Channel>,
        timeout: Duration,
        enable_batch_rpc: bool,
    ) -> KvRpcClient {
        let batch = if enable_batch_rpc {
            match BatchCommandsClient::connect(rpc_client.clone()).await {
                Ok(client) => Some(client),
                Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => None,
                Err(err) => {
                    debug!("failed to init batch_commands stream: {err:?}");
                    None
                }
            }
        } else {
            None
        };

        KvRpcClient {
            rpc_client,
            timeout,
            batch,
        }
    }

    async fn try_dispatch_batch(&self, request: &dyn Request) -> Result<Option<Box<dyn Any>>> {
        let Some(batch) = self.batch.as_ref() else {
            return Ok(None);
        };

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::GetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Get(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::Get(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for get: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::BatchGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BatchGet(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::BatchGet(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for batch_get: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::ScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Scan(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::Scan(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for scan: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawGet(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::RawGet(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for raw_get: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchGetRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchGet(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::RawBatchGet(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for raw_batch_get: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawScan(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::RawScan(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for raw_scan: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchScanRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchScan(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::RawBatchScan(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for raw_batch_scan: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawCoprocessorRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawCoprocessor(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(tikvpb::batch_commands_response::response::Cmd::RawCoprocessor(resp)) => {
                    Ok(Some(Box::new(resp) as Box<dyn Any>))
                }
                Ok(other) => Err(Error::StringError(format!(
                    "unexpected batch_commands response for raw_coprocessor: {other:?}"
                ))),
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        Ok(None)
    }
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        if let Some(resp) = self.try_dispatch_batch(request).await? {
            return Ok(resp);
        }
        request.dispatch(&self.rpc_client, self.timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use futures::stream;
    use tokio::sync::mpsc;
    use tonic::body::BoxBody;
    use tonic::codegen::http;
    use tonic::codegen::Service;

    use super::*;

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

    #[tokio::test]
    async fn test_build_tikv_client_compression_none_does_not_set_grpc_encoding() {
        let seen_headers = Arc::new(Mutex::new(None));
        let service = CaptureService {
            seen_headers: Arc::clone(&seen_headers),
        };
        let mut client = build_tikv_client(service, 4 * 1024 * 1024, GrpcCompressionType::None);

        let mut req = crate::proto::kvrpcpb::GetRequest::default();
        req.key = vec![1];
        let _ = client.kv_get(req).await;

        let headers = seen_headers
            .lock()
            .unwrap()
            .clone()
            .expect("request should be captured");
        assert!(headers.get("grpc-encoding").is_none());
    }

    #[tokio::test]
    async fn test_build_tikv_client_compression_gzip_sets_grpc_encoding() {
        let seen_headers = Arc::new(Mutex::new(None));
        let service = CaptureService {
            seen_headers: Arc::clone(&seen_headers),
        };
        let mut client = build_tikv_client(service, 4 * 1024 * 1024, GrpcCompressionType::Gzip);

        let mut req = crate::proto::kvrpcpb::GetRequest::default();
        req.key = vec![1];
        let _ = client.kv_get(req).await;

        let headers = seen_headers
            .lock()
            .unwrap()
            .clone()
            .expect("request should be captured");
        assert_eq!(
            headers
                .get("grpc-encoding")
                .expect("grpc-encoding should be set for gzip")
                .as_bytes(),
            b"gzip"
        );
        let accept = headers
            .get("grpc-accept-encoding")
            .expect("grpc-accept-encoding should be set for gzip")
            .to_str()
            .unwrap();
        assert!(accept.contains("gzip"));
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_coprocessor_via_batch_commands() -> Result<()> {
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (in_tx, in_rx) = mpsc::channel(8);
        let inbound_stream = stream::unfold(in_rx, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        let batch = BatchCommandsClient::new_with_inbound_for_test(out_tx, inbound_stream)?;

        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let rpc_client = TikvClient::new(channel);
        let client = KvRpcClient {
            rpc_client,
            timeout: Duration::from_secs(1),
            batch: Some(batch),
        };

        let mut request = kvrpcpb::RawCoprocessorRequest::default();
        request.copr_name = "example".to_owned();
        request.copr_version_req = "0.1.0".to_owned();
        request.data = b"ping".to_vec();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_coprocessor dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawCoprocessor(sent_req) => {
                assert_eq!(sent_req.copr_name, "example");
                assert_eq!(sent_req.copr_version_req, "0.1.0");
                assert_eq!(sent_req.data, b"ping".to_vec());
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_coprocessor: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::RawCoprocessor(
                        kvrpcpb::RawCoprocessorResponse {
                            data: b"pong".to_vec(),
                            ..Default::default()
                        },
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::RawCoprocessorResponse>()
            .map_err(|_| Error::StringError("expected raw_coprocessor response".to_owned()))?;
        assert_eq!(resp.data, b"pong".to_vec());
        Ok(())
    }
}
