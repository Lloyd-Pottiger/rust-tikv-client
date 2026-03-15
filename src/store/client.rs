// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use tonic::codec::CompressionEncoding;
use tonic::codegen::Body;
use tonic::codegen::Bytes;
use tonic::codegen::StdError;
use tonic::transport::Channel;

use super::batch::BatchCommandsClient;
use super::Request;
use crate::pd::HealthFeedbackObserver;
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

#[derive(Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
    grpc_max_decoding_message_size: usize,
    grpc_compression_type: GrpcCompressionType,
    enable_batch_rpc: bool,
    health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
}

impl TikvConnect {
    pub fn new(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        grpc_max_decoding_message_size: usize,
        grpc_compression_type: GrpcCompressionType,
        enable_batch_rpc: bool,
    ) -> TikvConnect {
        TikvConnect {
            security_mgr,
            timeout,
            grpc_max_decoding_message_size,
            grpc_compression_type,
            enable_batch_rpc,
            health_feedback_observer: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn set_health_feedback_observer(&self, observer: Weak<dyn HealthFeedbackObserver>) {
        *self
            .health_feedback_observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(observer);
    }
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
        let health_feedback_observer = Arc::clone(&self.health_feedback_observer);

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

        Ok(KvRpcClient::new(
            rpc_client,
            timeout,
            enable_batch_rpc,
            health_feedback_observer,
        )
        .await)
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
    health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
}

impl KvRpcClient {
    async fn new(
        rpc_client: TikvClient<Channel>,
        timeout: Duration,
        enable_batch_rpc: bool,
        health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
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
            health_feedback_observer,
        }
    }

    fn observe_health_feedback(&self, feedback: Option<&kvrpcpb::HealthFeedback>) {
        let Some(feedback) = feedback else {
            return;
        };

        let observer = self
            .health_feedback_observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        let Some(observer) = observer else {
            return;
        };
        observer.observe_health_feedback(feedback);
    }

    async fn try_dispatch_batch(&self, request: &dyn Request) -> Result<Option<Box<dyn Any>>> {
        let Some(batch) = self.batch.as_ref() else {
            return Ok(None);
        };

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::GetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Get(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Get(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for get: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::BatchGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BatchGet(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::BatchGet(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for batch_get: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::ScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Scan(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Scan(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for scan: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::ScanLockRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::ScanLock(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::ScanLock(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for scan_lock: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::CheckTxnStatus(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::CheckTxnStatus(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for check_txn_status: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
        {
            let cmd =
                tikvpb::batch_commands_request::request::Cmd::CheckSecondaryLocks(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::CheckSecondaryLocks(
                            resp,
                        ) => Ok(Some(Box::new(resp) as Box<dyn Any>)),
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for check_secondary_locks: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::TxnHeartBeatRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::TxnHeartBeat(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::TxnHeartBeat(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for txn_heart_beat: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::ResolveLockRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::ResolveLock(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::ResolveLock(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for resolve_lock: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
        {
            let cmd =
                tikvpb::batch_commands_request::request::Cmd::PessimisticRollback(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::PessimisticRollback(
                            resp,
                        ) => Ok(Some(Box::new(resp) as Box<dyn Any>)),
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for pessimistic_rollback: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::GetHealthFeedback(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => match result.cmd {
                    tikvpb::batch_commands_response::response::Cmd::GetHealthFeedback(resp) => {
                        let health_feedback = result.health_feedback.or(resp.health_feedback);
                        self.observe_health_feedback(health_feedback.as_ref());
                        let resp = kvrpcpb::GetHealthFeedbackResponse {
                            region_error: resp.region_error,
                            health_feedback,
                        };
                        Ok(Some(Box::new(resp) as Box<dyn Any>))
                    }
                    other => Err(Error::StringError(format!(
                        "unexpected batch_commands response for get_health_feedback: {other:?}"
                    ))),
                },
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BroadcastTxnStatus(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::BroadcastTxnStatus(
                            resp,
                        ) => Ok(Some(Box::new(resp) as Box<dyn Any>)),
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for broadcast_txn_status: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawGet(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawGet(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_get: {other:?}"
                        ))),
                    }
                }
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
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawBatchGet(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_batch_get: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawPutRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawPut(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawPut(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_put: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchPutRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchPut(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawBatchPut(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_batch_put: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawDeleteRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawDelete(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawDelete(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_delete: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchDeleteRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchDelete(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawBatchDelete(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_batch_delete: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawDeleteRangeRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawDeleteRange(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawDeleteRange(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_delete_range: {other:?}"
                        ))),
                    }
                }
                Err(Error::GrpcAPI(_)) => Ok(None),
                Err(err) => Err(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawScan(req.clone());
            return match batch.dispatch(cmd, self.timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawScan(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_scan: {other:?}"
                        ))),
                    }
                }
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
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawBatchScan(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_batch_scan: {other:?}"
                        ))),
                    }
                }
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
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::RawCoprocessor(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for raw_coprocessor: {other:?}"
                        ))),
                    }
                }
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
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

    type BatchInbound = std::result::Result<tikvpb::BatchCommandsResponse, tonic::Status>;

    fn new_kv_rpc_client_with_batch_for_test() -> Result<(
        KvRpcClient,
        mpsc::Receiver<tikvpb::BatchCommandsRequest>,
        mpsc::Sender<BatchInbound>,
    )> {
        let (out_tx, out_rx) = mpsc::channel(8);
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
            health_feedback_observer: Arc::new(Mutex::new(None)),
        };

        Ok((client, out_rx, in_tx))
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_coprocessor_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

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

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_put_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawPutRequest::default();
        request.key = b"k".to_vec();
        request.value = b"v".to_vec();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_put dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawPut(sent_req) => {
                assert_eq!(sent_req.key, b"k".to_vec());
                assert_eq!(sent_req.value, b"v".to_vec());
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_put: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawPut(
                    kvrpcpb::RawPutResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::RawPutResponse>()
            .map_err(|_| Error::StringError("expected raw_put response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_delete_range_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawDeleteRangeRequest::default();
        request.start_key = b"a".to_vec();
        request.end_key = b"z".to_vec();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_delete_range dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawDeleteRange(sent_req) => {
                assert_eq!(sent_req.start_key, b"a".to_vec());
                assert_eq!(sent_req.end_key, b"z".to_vec());
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_delete_range: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::RawDeleteRange(
                        kvrpcpb::RawDeleteRangeResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::RawDeleteRangeResponse>()
            .map_err(|_| Error::StringError("expected raw_delete_range response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_scan_lock_via_batch_commands() -> Result<()> {
        struct MockObserver {
            calls: AtomicUsize,
        }

        impl HealthFeedbackObserver for MockObserver {
            fn observe_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
                assert_eq!(feedback.store_id, 42);
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }

        let observer = Arc::new(MockObserver {
            calls: AtomicUsize::new(0),
        });
        let observer_dyn: Arc<dyn HealthFeedbackObserver> = observer.clone();
        let health_feedback_observer = Arc::new(Mutex::new(Some(Arc::downgrade(&observer_dyn))));

        let (mut client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;
        client.health_feedback_observer = health_feedback_observer;

        let mut request = kvrpcpb::ScanLockRequest::default();
        request.max_version = 42;
        request.start_key = b"a".to_vec();
        request.end_key = b"z".to_vec();
        request.limit = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "scan_lock dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::ScanLock(sent_req) => {
                assert_eq!(sent_req.max_version, 42);
                assert_eq!(sent_req.start_key, b"a".to_vec());
                assert_eq!(sent_req.end_key, b"z".to_vec());
                assert_eq!(sent_req.limit, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for scan_lock: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::ScanLock(
                    kvrpcpb::ScanLockResponse {
                        locks: vec![kvrpcpb::LockInfo {
                            key: b"k".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: Some(kvrpcpb::HealthFeedback {
                store_id: 42,
                feedback_seq_no: 7,
                slow_score: 1,
            }),
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::ScanLockResponse>()
            .map_err(|_| Error::StringError("expected scan_lock response".to_owned()))?;
        assert_eq!(resp.locks.len(), 1);
        assert_eq!(resp.locks[0].key, b"k".to_vec());
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_check_txn_status_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::CheckTxnStatusRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.primary_key = b"p".to_vec();
        request.lock_ts = 10;
        request.caller_start_ts = 20;
        request.current_ts = 30;
        request.rollback_if_not_exist = true;
        request.force_sync_commit = true;
        request.resolving_pessimistic_lock = true;
        request.verify_is_primary = true;
        request.is_txn_file = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "check_txn_status dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::CheckTxnStatus(sent_req) => {
                assert_eq!(sent_req.primary_key, b"p".to_vec());
                assert_eq!(sent_req.lock_ts, 10);
                assert_eq!(sent_req.caller_start_ts, 20);
                assert_eq!(sent_req.current_ts, 30);
                assert!(sent_req.rollback_if_not_exist);
                assert!(sent_req.force_sync_commit);
                assert!(sent_req.resolving_pessimistic_lock);
                assert!(sent_req.verify_is_primary);
                assert!(sent_req.is_txn_file);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for check_txn_status: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::CheckTxnStatus(
                        kvrpcpb::CheckTxnStatusResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::CheckTxnStatusResponse>()
            .map_err(|_| Error::StringError("expected check_txn_status response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_check_secondary_locks_via_batch_commands() -> Result<()>
    {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::CheckSecondaryLocksRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        request.start_version = 99;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "check_secondary_locks dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::CheckSecondaryLocks(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.start_version, 99);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for check_secondary_locks: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::CheckSecondaryLocks(
                        kvrpcpb::CheckSecondaryLocksResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::CheckSecondaryLocksResponse>()
            .map_err(|_| {
                Error::StringError("expected check_secondary_locks response".to_owned())
            })?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_txn_heart_beat_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::TxnHeartBeatRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.primary_lock = b"p".to_vec();
        request.start_version = 7;
        request.advise_lock_ttl = 123;
        request.is_txn_file = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "txn_heart_beat dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::TxnHeartBeat(sent_req) => {
                assert_eq!(sent_req.primary_lock, b"p".to_vec());
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.advise_lock_ttl, 123);
                assert!(sent_req.is_txn_file);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for txn_heart_beat: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::TxnHeartBeat(
                        kvrpcpb::TxnHeartBeatResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::TxnHeartBeatResponse>()
            .map_err(|_| Error::StringError("expected txn_heart_beat response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_resolve_lock_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::ResolveLockRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.start_version = 7;
        request.commit_version = 8;
        request.txn_infos = vec![kvrpcpb::TxnInfo {
            txn: 1,
            status: 2,
            is_txn_file: true,
        }];
        request.keys = vec![b"k".to_vec()];
        request.is_txn_file = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "resolve_lock dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::ResolveLock(sent_req) => {
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.commit_version, 8);
                assert_eq!(sent_req.txn_infos.len(), 1);
                assert_eq!(sent_req.txn_infos[0].txn, 1);
                assert_eq!(sent_req.txn_infos[0].status, 2);
                assert!(sent_req.txn_infos[0].is_txn_file);
                assert_eq!(sent_req.keys, vec![b"k".to_vec()]);
                assert!(sent_req.is_txn_file);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for resolve_lock: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::ResolveLock(
                    kvrpcpb::ResolveLockResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::ResolveLockResponse>()
            .map_err(|_| Error::StringError("expected resolve_lock response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_pessimistic_rollback_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::PessimisticRollbackRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.start_version = 42;
        request.for_update_ts = 43;
        request.keys = vec![b"k".to_vec()];

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "pessimistic_rollback dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::PessimisticRollback(sent_req) => {
                assert_eq!(sent_req.start_version, 42);
                assert_eq!(sent_req.for_update_ts, 43);
                assert_eq!(sent_req.keys, vec![b"k".to_vec()]);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for pessimistic_rollback: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::PessimisticRollback(
                        kvrpcpb::PessimisticRollbackResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::PessimisticRollbackResponse>()
            .map_err(|_| Error::StringError("expected pessimistic_rollback response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_get_health_feedback_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let request = kvrpcpb::GetHealthFeedbackRequest {
            context: Some(kvrpcpb::Context::default()),
        };

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "get_health_feedback dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::GetHealthFeedback(_sent_req) => {}
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for get_health_feedback: {other:?}"
                )));
            }
        }

        let feedback = kvrpcpb::HealthFeedback {
            store_id: 42,
            feedback_seq_no: 7,
            slow_score: 81,
        };
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::GetHealthFeedback(
                        kvrpcpb::GetHealthFeedbackResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: Some(feedback.clone()),
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::GetHealthFeedbackResponse>()
            .map_err(|_| Error::StringError("expected get_health_feedback response".to_owned()))?;
        let seen = resp.health_feedback.as_ref().ok_or_else(|| {
            Error::StringError("expected health feedback to be present".to_owned())
        })?;
        assert_eq!(seen.store_id, 42);
        assert_eq!(seen.feedback_seq_no, 7);
        assert_eq!(seen.slow_score, 81);
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_broadcast_txn_status_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let request = kvrpcpb::BroadcastTxnStatusRequest {
            context: Some(kvrpcpb::Context::default()),
            txn_status: vec![
                kvrpcpb::TxnStatus {
                    start_ts: 1,
                    min_commit_ts: 2,
                    commit_ts: 0,
                    rolled_back: false,
                    is_completed: false,
                },
                kvrpcpb::TxnStatus {
                    start_ts: 10,
                    min_commit_ts: 0,
                    commit_ts: 11,
                    rolled_back: false,
                    is_completed: true,
                },
            ],
        };

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "broadcast_txn_status dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::BroadcastTxnStatus(sent_req) => {
                assert_eq!(sent_req.txn_status.len(), 2);
                assert_eq!(sent_req.txn_status[0].start_ts, 1);
                assert_eq!(sent_req.txn_status[0].min_commit_ts, 2);
                assert_eq!(sent_req.txn_status[1].start_ts, 10);
                assert_eq!(sent_req.txn_status[1].commit_ts, 11);
                assert!(sent_req.txn_status[1].is_completed);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for broadcast_txn_status: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::BroadcastTxnStatus(
                        kvrpcpb::BroadcastTxnStatusResponse {},
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::BroadcastTxnStatusResponse>()
            .map_err(|_| Error::StringError("expected broadcast_txn_status response".to_owned()))?;
        Ok(())
    }
}
