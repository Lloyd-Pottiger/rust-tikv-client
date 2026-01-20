// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Status;

use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::StoreHealthMap;
use crate::{internal_err, Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BatchCommandKind {
    RawGet,
    RawBatchGet,
    RawPut,
    RawBatchPut,
    RawDelete,
    RawBatchDelete,
    RawScan,
    RawBatchScan,
    RawDeleteRange,
    RawCoprocessor,

    Get,
    Scan,
    Prewrite,
    Commit,
    Cleanup,
    BatchGet,
    BatchRollback,
    PessimisticRollback,
    ResolveLock,
    ScanLock,
    Flush,
    BufferBatchGet,
    PessimisticLock,
    TxnHeartBeat,
    CheckTxnStatus,
    CheckSecondaryLocks,
    Gc,
    DeleteRange,
    BroadcastTxnStatus,

    GetHealthFeedback,
}

impl BatchCommandKind {
    fn from_request(req: &tikvpb::batch_commands_request::Request) -> Option<Self> {
        use tikvpb::batch_commands_request::request::Cmd;

        match req.cmd.as_ref()? {
            Cmd::RawGet(_) => Some(Self::RawGet),
            Cmd::RawBatchGet(_) => Some(Self::RawBatchGet),
            Cmd::RawPut(_) => Some(Self::RawPut),
            Cmd::RawBatchPut(_) => Some(Self::RawBatchPut),
            Cmd::RawDelete(_) => Some(Self::RawDelete),
            Cmd::RawBatchDelete(_) => Some(Self::RawBatchDelete),
            Cmd::RawScan(_) => Some(Self::RawScan),
            Cmd::RawBatchScan(_) => Some(Self::RawBatchScan),
            Cmd::RawDeleteRange(_) => Some(Self::RawDeleteRange),
            Cmd::RawCoprocessor(_) => Some(Self::RawCoprocessor),

            Cmd::Get(_) => Some(Self::Get),
            Cmd::Scan(_) => Some(Self::Scan),
            Cmd::Prewrite(_) => Some(Self::Prewrite),
            Cmd::Commit(_) => Some(Self::Commit),
            Cmd::Cleanup(_) => Some(Self::Cleanup),
            Cmd::BatchGet(_) => Some(Self::BatchGet),
            Cmd::BatchRollback(_) => Some(Self::BatchRollback),
            Cmd::PessimisticRollback(_) => Some(Self::PessimisticRollback),
            Cmd::ResolveLock(_) => Some(Self::ResolveLock),
            Cmd::ScanLock(_) => Some(Self::ScanLock),
            Cmd::Flush(_) => Some(Self::Flush),
            Cmd::BufferBatchGet(_) => Some(Self::BufferBatchGet),
            Cmd::PessimisticLock(_) => Some(Self::PessimisticLock),
            Cmd::TxnHeartBeat(_) => Some(Self::TxnHeartBeat),
            Cmd::CheckTxnStatus(_) => Some(Self::CheckTxnStatus),
            Cmd::CheckSecondaryLocks(_) => Some(Self::CheckSecondaryLocks),
            Cmd::Gc(_) => Some(Self::Gc),
            Cmd::DeleteRange(_) => Some(Self::DeleteRange),
            Cmd::BroadcastTxnStatus(_) => Some(Self::BroadcastTxnStatus),
            Cmd::GetHealthFeedback(_) => Some(Self::GetHealthFeedback),

            // Not used by this client yet.
            _ => None,
        }
    }
}

struct Inflight {
    kind: BatchCommandKind,
    resp_tx: oneshot::Sender<Result<Box<dyn Any + Send>>>,
}

struct Outbound {
    request_id: u64,
    request: tikvpb::batch_commands_request::Request,
}

#[derive(Clone)]
pub(crate) struct BatchCommandsClient {
    outbound_tx: mpsc::Sender<Outbound>,
    inflight: Arc<Mutex<HashMap<u64, Inflight>>>,
    next_request_id: Arc<AtomicU64>,
}

impl BatchCommandsClient {
    pub(crate) fn new(rpc_client: TikvClient<Channel>, store_health: StoreHealthMap) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(1024);
        let inflight = Arc::new(Mutex::new(HashMap::new()));

        let state = Arc::new(BatchCommandsState {
            rpc_client,
            store_health,
            inflight: inflight.clone(),
        });
        tokio::spawn(run_batch_send_loop(state, outbound_rx));

        Self {
            outbound_tx,
            inflight,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) async fn dispatch(
        &self,
        kind: BatchCommandKind,
        request: tikvpb::batch_commands_request::Request,
        timeout: Duration,
    ) -> Result<Box<dyn Any + Send>> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();

        {
            let mut guard = self.inflight.lock().await;
            guard.insert(request_id, Inflight { kind, resp_tx });
        }

        if self
            .outbound_tx
            .send(Outbound {
                request_id,
                request,
            })
            .await
            .is_err()
        {
            let mut guard = self.inflight.lock().await;
            guard.remove(&request_id);
            return Err(Error::GrpcAPI(Status::unavailable(
                "batch commands send loop terminated",
            )));
        }

        match tokio::time::timeout(timeout, resp_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => Err(err.into()),
            Err(_) => {
                let mut guard = self.inflight.lock().await;
                guard.remove(&request_id);
                Err(Error::GrpcAPI(Status::deadline_exceeded(
                    "batch commands request timeout",
                )))
            }
        }
    }
}

struct BatchCommandsState {
    rpc_client: TikvClient<Channel>,
    store_health: StoreHealthMap,
    inflight: Arc<Mutex<HashMap<u64, Inflight>>>,
}

struct BatchConnection {
    request_tx: mpsc::Sender<tikvpb::BatchCommandsRequest>,
}

async fn run_batch_send_loop(
    state: Arc<BatchCommandsState>,
    mut outbound_rx: mpsc::Receiver<Outbound>,
) {
    let mut connection: Option<BatchConnection> = None;

    while let Some(outbound) = outbound_rx.recv().await {
        loop {
            if connection.is_none() {
                connection = match connect_batch_stream(state.clone()).await {
                    Ok(conn) => Some(conn),
                    Err(status) => {
                        fail_inflight(&state.inflight, outbound.request_id, status).await;
                        break;
                    }
                };
            }

            let Some(conn) = &connection else {
                break;
            };

            let req = tikvpb::BatchCommandsRequest {
                requests: vec![outbound.request.clone()],
                request_ids: vec![outbound.request_id],
            };

            match conn.request_tx.send(req).await {
                Ok(()) => break,
                Err(_) => {
                    connection = None;
                    continue;
                }
            }
        }
    }
}

async fn connect_batch_stream(
    state: Arc<BatchCommandsState>,
) -> std::result::Result<BatchConnection, Status> {
    let (request_tx, request_rx) = mpsc::channel(1024);

    // TiKV's `BatchCommands` stream may not become ready until the client sends at least one
    // request message. Prime the stream with an `Empty` request so the server can start the stream
    // even when the client has not enqueued any real requests yet.
    //
    // We use `request_id=0` which is never produced by `BatchCommandsClient` (it starts at 1), so
    // any response can be safely ignored.
    let _ = request_tx
        .send(tikvpb::BatchCommandsRequest {
            requests: vec![tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            }],
            request_ids: vec![0],
        })
        .await;
    let request_stream = ReceiverStream::new(request_rx);

    let mut client = state.rpc_client.clone();
    let response_stream = client.batch_commands(request_stream).await?.into_inner();

    tokio::spawn(run_batch_recv_loop(state, response_stream));

    Ok(BatchConnection { request_tx })
}

async fn run_batch_recv_loop(
    state: Arc<BatchCommandsState>,
    mut response_stream: tonic::codec::Streaming<tikvpb::BatchCommandsResponse>,
) {
    loop {
        match response_stream.message().await {
            Ok(Some(resp)) => {
                if let Some(health_feedback) = resp.health_feedback {
                    state.store_health.record_tikv_health_feedback(
                        health_feedback.store_id,
                        health_feedback.feedback_seq_no,
                        health_feedback.slow_score,
                    );
                }

                for (request_id, response) in resp.request_ids.into_iter().zip(resp.responses) {
                    let inflight = {
                        let mut guard = state.inflight.lock().await;
                        guard.remove(&request_id)
                    };

                    let Some(inflight) = inflight else {
                        continue;
                    };

                    let result = decode_response(inflight.kind, response);
                    let _ = inflight.resp_tx.send(result);
                }
            }
            Ok(None) => {
                fail_all_inflight(
                    &state.inflight,
                    Status::unavailable("batch commands stream closed"),
                )
                .await;
                return;
            }
            Err(status) => {
                fail_all_inflight(&state.inflight, status).await;
                return;
            }
        }
    }
}

async fn fail_inflight(inflight: &Mutex<HashMap<u64, Inflight>>, request_id: u64, status: Status) {
    let entry = {
        let mut guard = inflight.lock().await;
        guard.remove(&request_id)
    };
    if let Some(entry) = entry {
        let _ = entry.resp_tx.send(Err(Error::GrpcAPI(status)));
    }
}

async fn fail_all_inflight(inflight: &Mutex<HashMap<u64, Inflight>>, status: Status) {
    let entries = {
        let mut guard = inflight.lock().await;
        std::mem::take(&mut *guard)
    };
    for (_, entry) in entries {
        let _ = entry.resp_tx.send(Err(Error::GrpcAPI(status.clone())));
    }
}

fn decode_response(
    kind: BatchCommandKind,
    response: tikvpb::batch_commands_response::Response,
) -> Result<Box<dyn Any + Send>> {
    use tikvpb::batch_commands_response::response::Cmd;

    let Some(cmd) = response.cmd else {
        return Err(internal_err!("batch response missing cmd"));
    };

    match (kind, cmd) {
        (BatchCommandKind::RawGet, Cmd::RawGet(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawBatchGet, Cmd::RawBatchGet(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawPut, Cmd::RawPut(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawBatchPut, Cmd::RawBatchPut(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawDelete, Cmd::RawDelete(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawBatchDelete, Cmd::RawBatchDelete(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawScan, Cmd::RawScan(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawBatchScan, Cmd::RawBatchScan(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawDeleteRange, Cmd::RawDeleteRange(r)) => Ok(Box::new(r)),
        (BatchCommandKind::RawCoprocessor, Cmd::RawCoprocessor(r)) => Ok(Box::new(r)),

        (BatchCommandKind::Get, Cmd::Get(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Scan, Cmd::Scan(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Prewrite, Cmd::Prewrite(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Commit, Cmd::Commit(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Cleanup, Cmd::Cleanup(r)) => Ok(Box::new(r)),
        (BatchCommandKind::BatchGet, Cmd::BatchGet(r)) => Ok(Box::new(r)),
        (BatchCommandKind::BatchRollback, Cmd::BatchRollback(r)) => Ok(Box::new(r)),
        (BatchCommandKind::PessimisticRollback, Cmd::PessimisticRollback(r)) => Ok(Box::new(r)),
        (BatchCommandKind::ResolveLock, Cmd::ResolveLock(r)) => Ok(Box::new(r)),
        (BatchCommandKind::ScanLock, Cmd::ScanLock(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Flush, Cmd::Flush(r)) => Ok(Box::new(r)),
        (BatchCommandKind::BufferBatchGet, Cmd::BufferBatchGet(r)) => Ok(Box::new(r)),
        (BatchCommandKind::PessimisticLock, Cmd::PessimisticLock(r)) => Ok(Box::new(r)),
        (BatchCommandKind::TxnHeartBeat, Cmd::TxnHeartBeat(r)) => Ok(Box::new(r)),
        (BatchCommandKind::CheckTxnStatus, Cmd::CheckTxnStatus(r)) => Ok(Box::new(r)),
        (BatchCommandKind::CheckSecondaryLocks, Cmd::CheckSecondaryLocks(r)) => Ok(Box::new(r)),
        (BatchCommandKind::Gc, Cmd::Gc(r)) => Ok(Box::new(r)),
        (BatchCommandKind::DeleteRange, Cmd::DeleteRange(r)) => Ok(Box::new(r)),
        (BatchCommandKind::BroadcastTxnStatus, Cmd::BroadcastTxnStatus(r)) => Ok(Box::new(r)),

        (BatchCommandKind::GetHealthFeedback, Cmd::GetHealthFeedback(r)) => Ok(Box::new(r)),

        (expected, _) => Err(internal_err!(format!(
            "batch response mismatch: expected={expected:?}"
        ))),
    }
}

pub(crate) fn batch_kind_for_request(
    req: &tikvpb::batch_commands_request::Request,
) -> Result<BatchCommandKind> {
    BatchCommandKind::from_request(req).ok_or_else(|| {
        Error::GrpcAPI(Status::new(
            Code::Unimplemented,
            "request is not supported by batch commands",
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Endpoint;
    use tonic::transport::Server;
    use tonic::Status;

    #[tonic::async_trait]
    trait BatchCommandsService: Send + Sync + 'static {
        type BatchCommandsStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<tikvpb::BatchCommandsResponse, Status>,
            > + Send
            + 'static;

        async fn batch_commands(
            &self,
            request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
        ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>;
    }

    #[derive(Debug)]
    struct BatchCommandsServer<T: BatchCommandsService> {
        inner: Arc<T>,
    }

    impl<T: BatchCommandsService> BatchCommandsServer<T> {
        fn new(inner: T) -> Self {
            Self {
                inner: Arc::new(inner),
            }
        }
    }

    impl<T: BatchCommandsService> Clone for BatchCommandsServer<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }

    impl<T, B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for BatchCommandsServer<T>
    where
        T: BatchCommandsService,
        B: tonic::codegen::Body + Send + 'static,
        B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
        type Error = Infallible;
        type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut tonic::codegen::Context<'_>,
        ) -> tonic::codegen::Poll<std::result::Result<(), Self::Error>> {
            tonic::codegen::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: tonic::codegen::http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();

            match req.uri().path() {
                "/tikvpb.Tikv/BatchCommands" => {
                    struct BatchCommandsSvc<T: BatchCommandsService>(pub Arc<T>);

                    impl<T: BatchCommandsService>
                        tonic::server::StreamingService<tikvpb::BatchCommandsRequest>
                        for BatchCommandsSvc<T>
                    {
                        type Response = tikvpb::BatchCommandsResponse;
                        type ResponseStream = T::BatchCommandsStream;
                        type Future = tonic::codegen::BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;

                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.batch_commands(request).await };
                            Box::pin(fut)
                        }
                    }

                    let fut = async move {
                        let method = BatchCommandsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec);
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(tonic::codegen::http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(tonic::codegen::empty_body())
                        .unwrap())
                }),
            }
        }
    }

    impl<T: BatchCommandsService> tonic::server::NamedService for BatchCommandsServer<T> {
        const NAME: &'static str = "tikvpb.Tikv";
    }

    async fn start_batch_commands_server<T: BatchCommandsService>(
        handler: T,
    ) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("server local_addr");
        let incoming = TcpListenerStream::new(listener);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            Server::builder()
                .add_service(BatchCommandsServer::new(handler))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve test server");
        });

        (addr, shutdown_tx)
    }

    async fn connect_test_client(addr: SocketAddr) -> TikvClient<Channel> {
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        TikvClient::new(channel)
    }

    async fn expect_prime_empty(
        inbound: &mut tonic::Streaming<tikvpb::BatchCommandsRequest>,
    ) -> std::result::Result<(), Status> {
        let Some(first) = inbound.message().await? else {
            return Err(Status::invalid_argument("missing prime request"));
        };
        if first.request_ids != [0] {
            return Err(Status::invalid_argument("unexpected prime request_id"));
        }
        let Some(first_req) = first.requests.into_iter().next() else {
            return Err(Status::invalid_argument("missing prime request cmd"));
        };
        match first_req.cmd {
            Some(tikvpb::batch_commands_request::request::Cmd::Empty(_)) => Ok(()),
            _ => Err(Status::invalid_argument("unexpected prime cmd")),
        }
    }

    fn make_raw_get_batch_request(key: Vec<u8>) -> tikvpb::batch_commands_request::Request {
        tikvpb::batch_commands_request::Request {
            cmd: Some(tikvpb::batch_commands_request::request::Cmd::RawGet(
                crate::proto::kvrpcpb::RawGetRequest {
                    key,
                    ..Default::default()
                },
            )),
        }
    }

    fn make_raw_get_batch_response(value: Vec<u8>) -> tikvpb::batch_commands_response::Response {
        tikvpb::batch_commands_response::Response {
            cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawGet(
                crate::proto::kvrpcpb::RawGetResponse {
                    value,
                    not_found: false,
                    ..Default::default()
                },
            )),
        }
    }

    #[tokio::test]
    async fn batch_commands_primes_stream_and_dispatches() -> Result<()> {
        struct Echo;

        #[tonic::async_trait]
        impl BatchCommandsService for Echo {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    while let Ok(Some(req)) = inbound.message().await {
                        for (request_id, request) in
                            req.request_ids.into_iter().zip(req.requests.into_iter())
                        {
                            let key = match request.cmd {
                                Some(tikvpb::batch_commands_request::request::Cmd::RawGet(req)) => {
                                    req.key
                                }
                                _ => Vec::new(),
                            };
                            let resp = tikvpb::BatchCommandsResponse {
                                request_ids: vec![request_id],
                                responses: vec![make_raw_get_batch_response(key)],
                                ..Default::default()
                            };
                            if tx.send(Ok(resp)).await.is_err() {
                                return;
                            }
                        }
                    }
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let (addr, shutdown_tx) = start_batch_commands_server(Echo).await;
        let rpc = connect_test_client(addr).await;
        let store_health = StoreHealthMap::default();
        let client = BatchCommandsClient::new(rpc, store_health);

        let resp = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![1, 2, 3]),
                Duration::from_secs(5),
            )
            .await?;
        let resp = resp
            .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
            .expect("raw get response");
        assert_eq!(resp.value, vec![1, 2, 3]);

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn batch_commands_routes_out_of_order_responses_by_request_id() -> Result<()> {
        struct OutOfOrder;

        #[tonic::async_trait]
        impl BatchCommandsService for OutOfOrder {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    let mut buffered = Vec::new();
                    while let Ok(Some(req)) = inbound.message().await {
                        for (request_id, request) in
                            req.request_ids.into_iter().zip(req.requests.into_iter())
                        {
                            let key = match request.cmd {
                                Some(tikvpb::batch_commands_request::request::Cmd::RawGet(req)) => {
                                    req.key
                                }
                                _ => Vec::new(),
                            };
                            buffered.push((request_id, key));
                        }

                        if buffered.len() >= 2 {
                            let (id2, k2) = buffered.pop().unwrap();
                            let (id1, k1) = buffered.pop().unwrap();

                            let resp2 = tikvpb::BatchCommandsResponse {
                                request_ids: vec![id2],
                                responses: vec![make_raw_get_batch_response(k2)],
                                ..Default::default()
                            };
                            if tx.send(Ok(resp2)).await.is_err() {
                                return;
                            }
                            let resp1 = tikvpb::BatchCommandsResponse {
                                request_ids: vec![id1],
                                responses: vec![make_raw_get_batch_response(k1)],
                                ..Default::default()
                            };
                            let _ = tx.send(Ok(resp1)).await;
                            return;
                        }
                    }
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let (addr, shutdown_tx) = start_batch_commands_server(OutOfOrder).await;
        let rpc = connect_test_client(addr).await;
        let client = BatchCommandsClient::new(rpc, StoreHealthMap::default());

        let f1 = client.dispatch(
            BatchCommandKind::RawGet,
            make_raw_get_batch_request(vec![1]),
            Duration::from_secs(5),
        );
        let f2 = client.dispatch(
            BatchCommandKind::RawGet,
            make_raw_get_batch_request(vec![2]),
            Duration::from_secs(5),
        );
        let (r1, r2) = tokio::try_join!(f1, f2)?;

        let r1 = r1
            .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
            .unwrap();
        let r2 = r2
            .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
            .unwrap();
        assert_eq!(r1.value, vec![1]);
        assert_eq!(r2.value, vec![2]);

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[test]
    fn batch_commands_decode_response_missing_cmd_returns_error() {
        let err = decode_response(
            BatchCommandKind::RawGet,
            tikvpb::batch_commands_response::Response { cmd: None },
        )
        .expect_err("missing cmd should be an error");
        assert!(matches!(err, Error::InternalError { .. }));
    }

    #[tokio::test]
    async fn batch_commands_dispatch_times_out_when_server_never_responds() -> Result<()> {
        struct NeverRespond;

        #[tonic::async_trait]
        impl BatchCommandsService for NeverRespond {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    // Keep the response stream open, but never send a response.
                    let _tx = tx;
                    let _ = inbound.message().await;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let (addr, shutdown_tx) = start_batch_commands_server(NeverRespond).await;
        let rpc = connect_test_client(addr).await;
        let client = BatchCommandsClient::new(rpc, StoreHealthMap::default());

        let err = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![1_u8]),
                Duration::from_millis(50),
            )
            .await
            .expect_err("expected timeout error");
        match err {
            Error::GrpcAPI(status) => {
                assert_eq!(status.code(), Code::DeadlineExceeded);
                assert_eq!(status.message(), "batch commands request timeout");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn batch_commands_reconnects_after_stream_is_closed() -> Result<()> {
        struct CloseAfterOne {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl BatchCommandsService for CloseAfterOne {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    if let Ok(Some(req)) = inbound.message().await {
                        if let Some((request_id, request)) = req
                            .request_ids
                            .into_iter()
                            .zip(req.requests.into_iter())
                            .next()
                        {
                            let key = match request.cmd {
                                Some(tikvpb::batch_commands_request::request::Cmd::RawGet(req)) => {
                                    req.key
                                }
                                _ => Vec::new(),
                            };
                            let resp = tikvpb::BatchCommandsResponse {
                                request_ids: vec![request_id],
                                responses: vec![make_raw_get_batch_response(key)],
                                ..Default::default()
                            };
                            let _ = tx.send(Ok(resp)).await;
                        }
                    }
                    // Closing the sender closes the response stream and forces the client to
                    // reconnect for subsequent requests.
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let (addr, shutdown_tx) = start_batch_commands_server(CloseAfterOne {
            calls: calls.clone(),
        })
        .await;
        let rpc = connect_test_client(addr).await;
        let client = BatchCommandsClient::new(rpc, StoreHealthMap::default());

        let first = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![10]),
                Duration::from_secs(5),
            )
            .await?;
        let first = first
            .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
            .unwrap();
        assert_eq!(first.value, vec![10]);

        let second = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![11]),
                Duration::from_secs(5),
            )
            .await?;
        let second = second
            .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
            .unwrap();
        assert_eq!(second.value, vec![11]);

        // The second request should require a new `BatchCommands` stream.
        assert!(calls.load(Ordering::SeqCst) >= 2);

        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn batch_commands_fails_inflight_when_stream_closes_without_response() -> Result<()> {
        struct CloseWithoutResponse {
            calls: Arc<AtomicUsize>,
        }

        #[tonic::async_trait]
        impl BatchCommandsService for CloseWithoutResponse {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                let call = self.calls.fetch_add(1, Ordering::SeqCst);
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    if call == 0 {
                        // Close the response stream without replying to the first request.
                        let _ = inbound.message().await;
                        return;
                    }

                    let tx = tx;
                    while let Ok(Some(req)) = inbound.message().await {
                        for (request_id, request) in
                            req.request_ids.into_iter().zip(req.requests.into_iter())
                        {
                            let key = match request.cmd {
                                Some(tikvpb::batch_commands_request::request::Cmd::RawGet(req)) => {
                                    req.key
                                }
                                _ => Vec::new(),
                            };
                            let resp = tikvpb::BatchCommandsResponse {
                                request_ids: vec![request_id],
                                responses: vec![make_raw_get_batch_response(key)],
                                ..Default::default()
                            };
                            if tx.send(Ok(resp)).await.is_err() {
                                return;
                            }
                        }
                    }
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let (addr, shutdown_tx) = start_batch_commands_server(CloseWithoutResponse {
            calls: calls.clone(),
        })
        .await;
        let rpc = connect_test_client(addr).await;
        let client = BatchCommandsClient::new(rpc, StoreHealthMap::default());

        let err = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![11_u8]),
                Duration::from_secs(5),
            )
            .await
            .expect_err("expected stream closed error");
        match err {
            Error::GrpcAPI(status) => {
                assert_eq!(status.code(), Code::Unavailable);
                assert_eq!(status.message(), "batch commands stream closed");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        // After reconnecting, subsequent requests should reuse the new stream.
        for v in 12_u8..=15_u8 {
            let resp = client
                .dispatch(
                    BatchCommandKind::RawGet,
                    make_raw_get_batch_request(vec![v]),
                    Duration::from_secs(5),
                )
                .await?;
            let resp = resp
                .downcast::<crate::proto::kvrpcpb::RawGetResponse>()
                .expect("raw get response");
            assert_eq!(resp.value, vec![v]);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let _ = shutdown_tx.send(());
        Ok(())
    }

    #[tokio::test]
    async fn batch_commands_health_feedback_updates_store_health_map() -> Result<()> {
        struct HealthFeedback;

        #[tonic::async_trait]
        impl BatchCommandsService for HealthFeedback {
            type BatchCommandsStream =
                ReceiverStream<std::result::Result<tikvpb::BatchCommandsResponse, Status>>;

            async fn batch_commands(
                &self,
                request: tonic::Request<tonic::Streaming<tikvpb::BatchCommandsRequest>>,
            ) -> std::result::Result<tonic::Response<Self::BatchCommandsStream>, Status>
            {
                let mut inbound = request.into_inner();
                expect_prime_empty(&mut inbound).await?;

                let (tx, rx) = mpsc::channel(16);
                tokio::spawn(async move {
                    let store_id = 42;

                    let Some(req) = inbound.message().await.ok().flatten() else {
                        return;
                    };
                    let Some((request_id, request)) = req
                        .request_ids
                        .into_iter()
                        .zip(req.requests.into_iter())
                        .next()
                    else {
                        return;
                    };

                    let key = match request.cmd {
                        Some(tikvpb::batch_commands_request::request::Cmd::RawGet(req)) => req.key,
                        _ => Vec::new(),
                    };
                    let resp = tikvpb::BatchCommandsResponse {
                        request_ids: vec![request_id],
                        responses: vec![make_raw_get_batch_response(key)],
                        health_feedback: Some(crate::proto::kvrpcpb::HealthFeedback {
                            store_id,
                            feedback_seq_no: 10,
                            slow_score: 1,
                        }),
                        ..Default::default()
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        return;
                    }

                    // Send feedback-only messages to exercise seq-no monotonicity.
                    let _ = tx
                        .send(Ok(tikvpb::BatchCommandsResponse {
                            health_feedback: Some(crate::proto::kvrpcpb::HealthFeedback {
                                store_id,
                                feedback_seq_no: 9,
                                slow_score: 50,
                            }),
                            ..Default::default()
                        }))
                        .await;
                    let _ = tx
                        .send(Ok(tikvpb::BatchCommandsResponse {
                            health_feedback: Some(crate::proto::kvrpcpb::HealthFeedback {
                                store_id,
                                feedback_seq_no: 11,
                                slow_score: 2,
                            }),
                            ..Default::default()
                        }))
                        .await;
                });

                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }
        }

        let (addr, shutdown_tx) = start_batch_commands_server(HealthFeedback).await;
        let rpc = connect_test_client(addr).await;
        let store_health = StoreHealthMap::default();
        let client = BatchCommandsClient::new(rpc, store_health.clone());

        let _ = client
            .dispatch(
                BatchCommandKind::RawGet,
                make_raw_get_batch_request(vec![1]),
                Duration::from_secs(5),
            )
            .await?;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if store_health.tikv_side_slow_score(42) == Some(2) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("health feedback applied");

        let _ = shutdown_tx.send(());
        Ok(())
    }
}
