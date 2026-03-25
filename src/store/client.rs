// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use log::warn;
use tonic::codec::CompressionEncoding;
use tonic::codegen::Body;
use tonic::codegen::Bytes;
use tonic::codegen::StdError;
use tonic::transport::Channel;

use super::batch::BatchCommandsClient;
use super::coprocessor_cache::CoprocessorCache;
use super::Request;
use crate::config::CoprocessorCacheConfig;
use crate::pd::HealthFeedbackObserver;
use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::util::RateLimit;
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
    copr_req_timeout: Duration,
    copr_cache: Option<Arc<CoprocessorCache>>,
    grpc_max_decoding_message_size: usize,
    grpc_compression_type: GrpcCompressionType,
    enable_batch_rpc: bool,
    batch_rpc_max_batch_size: usize,
    batch_rpc_policy: String,
    batch_rpc_overload_threshold: u64,
    batch_rpc_wait_size: usize,
    batch_rpc_max_wait_time: Duration,
    max_concurrency_request_limit: i64,
    health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
}

impl TikvConnect {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        copr_req_timeout: Duration,
        grpc_max_decoding_message_size: usize,
        grpc_compression_type: GrpcCompressionType,
        enable_batch_rpc: bool,
        batch_rpc_max_batch_size: usize,
        batch_rpc_wait_size: usize,
        batch_rpc_max_wait_time: Duration,
    ) -> TikvConnect {
        TikvConnect {
            security_mgr,
            timeout,
            copr_req_timeout,
            copr_cache: None,
            grpc_max_decoding_message_size,
            grpc_compression_type,
            enable_batch_rpc,
            batch_rpc_max_batch_size,
            batch_rpc_policy: crate::config::DEFAULT_BATCH_RPC_POLICY.to_owned(),
            batch_rpc_overload_threshold: crate::config::DEFAULT_BATCH_RPC_OVERLOAD_THRESHOLD,
            batch_rpc_wait_size,
            batch_rpc_max_wait_time,
            max_concurrency_request_limit: crate::config::DEFAULT_MAX_CONCURRENCY_REQUEST_LIMIT,
            health_feedback_observer: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_batch_rpc_policy(mut self, policy: impl Into<String>) -> Self {
        self.batch_rpc_policy = policy.into();
        self
    }

    pub fn with_batch_rpc_overload_threshold(mut self, threshold: u64) -> Self {
        self.batch_rpc_overload_threshold = threshold;
        self
    }

    pub fn with_max_concurrency_request_limit(mut self, limit: i64) -> Self {
        self.max_concurrency_request_limit = limit;
        self
    }

    pub fn with_copr_cache_config(mut self, config: CoprocessorCacheConfig) -> Self {
        if config.capacity_mb > 0 && config.admission_max_result_mb == 0 {
            warn!("invalid coprocessor cache config: admission_max_result_mb=0, disabling cache");
            self.copr_cache = None;
            return self;
        }

        self.copr_cache = CoprocessorCache::from_config(&config).map(Arc::new);
        self
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
        let copr_req_timeout = self.copr_req_timeout;
        let copr_cache = self.copr_cache.clone();
        let grpc_max_decoding_message_size = self.grpc_max_decoding_message_size;
        let grpc_compression_type = self.grpc_compression_type;
        let enable_batch_rpc = self.enable_batch_rpc;
        let batch_rpc_max_batch_size = self.batch_rpc_max_batch_size;
        let batch_rpc_policy = self.batch_rpc_policy.clone();
        let batch_rpc_overload_threshold = self.batch_rpc_overload_threshold;
        let batch_rpc_wait_size = self.batch_rpc_wait_size;
        let batch_rpc_max_wait_time = self.batch_rpc_max_wait_time;
        let max_concurrency_request_limit = self.max_concurrency_request_limit;
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
            copr_req_timeout,
            address.to_owned(),
            enable_batch_rpc,
            batch_rpc_max_batch_size,
            batch_rpc_policy,
            batch_rpc_overload_threshold,
            batch_rpc_wait_size,
            batch_rpc_max_wait_time,
            max_concurrency_request_limit,
            copr_cache,
            health_feedback_observer,
        )
        .await)
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;

    fn timeout(&self) -> Duration {
        Duration::ZERO
    }

    fn store_address(&self) -> Option<&str> {
        None
    }
}

#[derive(Clone)]
pub(crate) struct ForwardedHostKvClient {
    inner: Arc<dyn KvClient + Send + Sync>,
    from_store_id: u64,
    to_store_id: u64,
    forwarded_host: String,
}

impl ForwardedHostKvClient {
    pub(crate) fn new(
        inner: Arc<dyn KvClient + Send + Sync>,
        from_store_id: u64,
        to_store_id: u64,
        forwarded_host: String,
    ) -> ForwardedHostKvClient {
        ForwardedHostKvClient {
            inner,
            from_store_id,
            to_store_id,
            forwarded_host,
        }
    }
}

#[async_trait]
impl KvClient for ForwardedHostKvClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        let req_type = crate::request::plan::kv_cmd_from_label(request.label());
        let result = crate::store::scope_forwarded_host(
            self.forwarded_host.clone(),
            self.inner.dispatch(request),
        )
        .await;

        let label = if result.is_ok() { "ok" } else { "fail" };
        crate::stats::inc_forward_request_counter(
            self.from_store_id,
            self.to_store_id,
            &req_type,
            label,
        );
        result
    }

    fn timeout(&self) -> Duration {
        self.inner.timeout()
    }

    fn store_address(&self) -> Option<&str> {
        self.inner.store_address()
    }
}

#[derive(Clone)]
pub(crate) struct StoreLimitKvClient {
    inner: Arc<dyn KvClient + Send + Sync>,
    store_id: u64,
    store_address: String,
    store_limit_override: i64,
    token_count: Arc<AtomicI64>,
}

impl StoreLimitKvClient {
    pub(crate) fn new(
        inner: Arc<dyn KvClient + Send + Sync>,
        store_id: u64,
        store_address: String,
        store_limit_override: i64,
        token_count: Arc<AtomicI64>,
    ) -> StoreLimitKvClient {
        StoreLimitKvClient {
            inner,
            store_id,
            store_address,
            store_limit_override,
            token_count,
        }
    }

    fn effective_store_limit(&self) -> i64 {
        if self.store_limit_override > 0 {
            self.store_limit_override
        } else {
            crate::store_vars::store_limit()
        }
    }

    fn try_acquire_token(
        &self,
        store_limit: i64,
    ) -> std::result::Result<StoreLimitTokenGuard, Error> {
        let count = self.token_count.load(Ordering::Relaxed);
        if count < store_limit {
            self.token_count.fetch_add(1, Ordering::Relaxed);
            return Ok(StoreLimitTokenGuard {
                token_count: self.token_count.clone(),
            });
        }
        crate::stats::inc_get_store_limit_token_error(&self.store_address, self.store_id);
        Err(crate::TokenLimitError::new(self.store_id).into())
    }
}

struct StoreLimitTokenGuard {
    token_count: Arc<AtomicI64>,
}

impl Drop for StoreLimitTokenGuard {
    fn drop(&mut self) {
        let prev = self.token_count.fetch_sub(1, Ordering::Relaxed);
        if prev <= 0 {
            self.token_count.fetch_add(1, Ordering::Relaxed);
            warn!("release store token failed, count equals to 0");
        }
    }
}

#[async_trait]
impl KvClient for StoreLimitKvClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        let store_limit = self.effective_store_limit();
        if store_limit <= 0 {
            return self.inner.dispatch(request).await;
        }
        let _guard = self.try_acquire_token(store_limit)?;
        self.inner.dispatch(request).await
    }

    fn timeout(&self) -> Duration {
        self.inner.timeout()
    }

    fn store_address(&self) -> Option<&str> {
        Some(&self.store_address)
    }
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_client: TikvClient<Channel>,
    timeout: Duration,
    copr_req_timeout: Duration,
    copr_cache: Option<Arc<CoprocessorCache>>,
    store_address: String,
    grpc_connection_state: Option<Arc<GrpcConnectionStateTracker>>,
    batch: Option<BatchCommandsClient>,
    forwarded_batch_clients: Option<ForwardedBatchCommandsClientPool>,
    max_concurrency_request_limit: Option<RateLimit>,
    health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum TrackedGrpcConnectionState {
    Ready = 0,
    TransientFailure = 1,
}

impl TrackedGrpcConnectionState {
    const fn as_str(self) -> &'static str {
        match self {
            TrackedGrpcConnectionState::Ready => "READY",
            TrackedGrpcConnectionState::TransientFailure => "TRANSIENT_FAILURE",
        }
    }
}

#[derive(Debug)]
struct GrpcConnectionStateTracker {
    connection_id: String,
    store_ip: String,
    state: AtomicU8,
}

impl GrpcConnectionStateTracker {
    fn new(store_address: &str) -> Self {
        let connection_id = format!("{store_address}-0");
        crate::stats::set_grpc_connection_state(&connection_id, store_address, "READY");

        GrpcConnectionStateTracker {
            connection_id,
            store_ip: store_address.to_owned(),
            state: AtomicU8::new(TrackedGrpcConnectionState::Ready as u8),
        }
    }

    fn set_state(&self, state: TrackedGrpcConnectionState) {
        let next = state as u8;
        let prev = self.state.swap(next, Ordering::Relaxed);
        if prev == next {
            return;
        }

        crate::stats::set_grpc_connection_state(
            &self.connection_id,
            &self.store_ip,
            state.as_str(),
        );
    }
}

impl Drop for GrpcConnectionStateTracker {
    fn drop(&mut self) {
        crate::stats::clear_grpc_connection_state(&self.connection_id, &self.store_ip);
    }
}

#[derive(Clone)]
struct ForwardedBatchCommandsClientPool {
    rpc_client: TikvClient<Channel>,
    max_outbound_requests: usize,
    batch_policy: String,
    overload_threshold: u64,
    batch_wait_size: usize,
    max_wait_time: Duration,
    target: String,
    clients: Arc<Mutex<HashMap<String, BatchCommandsClient>>>,
}

impl ForwardedBatchCommandsClientPool {
    async fn get_or_connect(&self, forwarded_host: String) -> Option<BatchCommandsClient> {
        if forwarded_host.is_empty() {
            return None;
        }

        let cached = {
            let guard = self
                .clients
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.get(&forwarded_host).cloned()
        };
        if cached.is_some() {
            return cached;
        }

        let connected = match BatchCommandsClient::connect_with_forwarded_host(
            self.rpc_client.clone(),
            self.max_outbound_requests,
            self.batch_policy.clone(),
            self.overload_threshold,
            self.batch_wait_size,
            self.max_wait_time,
            self.target.clone(),
            forwarded_host.clone(),
        )
        .await
        {
            Ok(client) => client,
            Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                return None
            }
            Err(err) => {
                debug!(
                    "failed to init forwarded batch_commands stream: forwarded_host={}, err={err:?}",
                    forwarded_host
                );
                return None;
            }
        };

        let mut guard = self
            .clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = guard
            .entry(forwarded_host)
            .or_insert_with(|| connected.clone());
        Some(entry.clone())
    }
}

impl KvRpcClient {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        rpc_client: TikvClient<Channel>,
        timeout: Duration,
        copr_req_timeout: Duration,
        store_address: String,
        enable_batch_rpc: bool,
        batch_rpc_max_batch_size: usize,
        batch_rpc_policy: String,
        batch_rpc_overload_threshold: u64,
        batch_rpc_wait_size: usize,
        batch_rpc_max_wait_time: Duration,
        max_concurrency_request_limit: i64,
        copr_cache: Option<Arc<CoprocessorCache>>,
        health_feedback_observer: Arc<Mutex<Option<Weak<dyn HealthFeedbackObserver>>>>,
    ) -> KvRpcClient {
        let max_concurrency_request_limit =
            match max_concurrency_request_limit > 0 && max_concurrency_request_limit != i64::MAX {
                true => usize::try_from(max_concurrency_request_limit)
                    .ok()
                    .map(RateLimit::new),
                false => None,
            };
        let forwarded_batch_config = ForwardedBatchCommandsClientPool {
            rpc_client: rpc_client.clone(),
            max_outbound_requests: batch_rpc_max_batch_size,
            batch_policy: batch_rpc_policy.clone(),
            overload_threshold: batch_rpc_overload_threshold,
            batch_wait_size: batch_rpc_wait_size,
            max_wait_time: batch_rpc_max_wait_time,
            target: store_address.clone(),
            clients: Arc::new(Mutex::new(HashMap::new())),
        };
        let batch = if enable_batch_rpc {
            match BatchCommandsClient::connect(
                rpc_client.clone(),
                batch_rpc_max_batch_size,
                batch_rpc_policy,
                batch_rpc_overload_threshold,
                batch_rpc_wait_size,
                batch_rpc_max_wait_time,
                store_address.clone(),
            )
            .await
            {
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

        let grpc_connection_state = Some(Arc::new(GrpcConnectionStateTracker::new(&store_address)));

        KvRpcClient {
            rpc_client,
            timeout,
            copr_req_timeout,
            copr_cache,
            store_address,
            grpc_connection_state,
            batch,
            forwarded_batch_clients: enable_batch_rpc.then_some(forwarded_batch_config),
            max_concurrency_request_limit,
            health_feedback_observer,
        }
    }

    fn timeout_for_request(&self, request: &dyn Request) -> Duration {
        if let Some(timeout) = request.timeout_override() {
            return timeout;
        }
        if !self.copr_req_timeout.is_zero()
            && matches!(
                request.label(),
                "coprocessor" | "batch_coprocessor" | "coprocessor_stream" | "raw_coprocessor"
            )
        {
            return self.copr_req_timeout;
        }
        self.timeout
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

    fn fallback_from_batch_dispatch_error(err: Error) -> Result<Option<Box<dyn Any>>> {
        match err {
            Error::GrpcAPI(_) => {
                crate::stats::inc_batch_client_no_available_connection();
                Ok(None)
            }
            err => Err(err),
        }
    }

    async fn dispatch_coprocessor_unary_with_cache(
        &self,
        cache: &CoprocessorCache,
        request: &coprocessor::Request,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut request = request.clone();
        let region_id = request
            .context
            .as_ref()
            .map(|ctx| ctx.region_id)
            .filter(|region_id| *region_id != 0);
        let lookup = region_id.and_then(|region_id| cache.prepare_request(&mut request, region_id));

        let response = request.dispatch(&self.rpc_client, timeout).await?;
        let mut response =
            *response
                .downcast::<coprocessor::Response>()
                .map_err(|_| Error::InternalError {
                    message: "expected coprocessor response".to_owned(),
                })?;

        if let (Some(region_id), Some(lookup)) = (region_id, lookup) {
            cache.handle_response(&request, region_id, Some(lookup), &mut response)?;
        }

        Ok(Box::new(response) as Box<dyn Any>)
    }

    async fn try_dispatch_batch(&self, request: &dyn Request) -> Result<Option<Box<dyn Any>>> {
        let Some(base_batch) = self.batch.as_ref() else {
            return Ok(None);
        };
        let timeout = self.timeout_for_request(request);
        let forwarded_host = crate::store::forwarded_host();
        let mut forwarded_batch: Option<BatchCommandsClient> = None;

        macro_rules! batch_for_dispatch {
            () => {{
                if let Some(forwarded_host) = forwarded_host.as_deref() {
                    if forwarded_batch.is_none() {
                        let Some(pool) = self.forwarded_batch_clients.as_ref() else {
                            return Ok(None);
                        };
                        forwarded_batch = pool.get_or_connect(forwarded_host.to_owned()).await;
                    }
                    let Some(batch) = forwarded_batch.as_ref() else {
                        return Ok(None);
                    };
                    batch
                } else {
                    base_batch
                }
            }};
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::GetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Get(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::BatchGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BatchGet(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BatchRollback(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::BatchRollback(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for batch_rollback: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::ScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Scan(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Prewrite(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Prewrite(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for prewrite: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::CommitRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Commit(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Commit(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for commit: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::CleanupRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Cleanup(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Cleanup(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for cleanup: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<coprocessor::Request>() {
            let mut req = req.clone();
            let cache = self.copr_cache.as_ref();
            let region_id = req
                .context
                .as_ref()
                .map(|ctx| ctx.region_id)
                .filter(|region_id| *region_id != 0);
            let lookup = match (cache, region_id) {
                (Some(cache), Some(region_id)) => cache.prepare_request(&mut req, region_id),
                _ => None,
            };

            let cmd = tikvpb::batch_commands_request::request::Cmd::Coprocessor(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Coprocessor(mut resp) => {
                            if let (Some(cache), Some(region_id), Some(lookup)) =
                                (cache, region_id, lookup)
                            {
                                cache.handle_response(&req, region_id, Some(lookup), &mut resp)?;
                            }
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for coprocessor: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::ScanLockRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::ScanLock(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::PessimisticLock(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::PessimisticLock(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for pessimistic_lock: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::CheckTxnStatusRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::CheckTxnStatus(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::CheckSecondaryLocksRequest>()
        {
            let cmd =
                tikvpb::batch_commands_request::request::Cmd::CheckSecondaryLocks(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::TxnHeartBeatRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::TxnHeartBeat(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::ResolveLockRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::ResolveLock(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::GcRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Gc(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Gc(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for gc: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::DeleteRangeRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::DeleteRange(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::DeleteRange(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for delete_range: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::PrepareFlashbackToVersionRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::PrepareFlashbackToVersion(
                req.clone(),
            );
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::PrepareFlashbackToVersion(
                            resp,
                        ) => Ok(Some(Box::new(resp) as Box<dyn Any>)),
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for prepare_flashback_to_version: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::FlashbackToVersionRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::FlashbackToVersion(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::FlashbackToVersion(
                            resp,
                        ) => Ok(Some(Box::new(resp) as Box<dyn Any>)),
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for flashback_to_version: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::FlushRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::Flush(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::Flush(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for flush: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::BufferBatchGetRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BufferBatchGet(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
                Ok(result) => {
                    self.observe_health_feedback(result.health_feedback.as_ref());
                    match result.cmd {
                        tikvpb::batch_commands_response::response::Cmd::BufferBatchGet(resp) => {
                            Ok(Some(Box::new(resp) as Box<dyn Any>))
                        }
                        other => Err(Error::StringError(format!(
                            "unexpected batch_commands response for buffer_batch_get: {other:?}"
                        ))),
                    }
                }
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
        {
            let cmd =
                tikvpb::batch_commands_request::request::Cmd::PessimisticRollback(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::GetHealthFeedbackRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::GetHealthFeedback(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::BroadcastTxnStatus(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawGetRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawGet(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchGetRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchGet(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawPutRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawPut(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchPutRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchPut(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawDeleteRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawDelete(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchDeleteRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchDelete(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawDeleteRangeRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawDeleteRange(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request.as_any().downcast_ref::<kvrpcpb::RawScanRequest>() {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawScan(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawBatchScanRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawBatchScan(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        if let Some(req) = request
            .as_any()
            .downcast_ref::<kvrpcpb::RawCoprocessorRequest>()
        {
            let cmd = tikvpb::batch_commands_request::request::Cmd::RawCoprocessor(req.clone());
            return match batch_for_dispatch!().dispatch(cmd, timeout).await {
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
                Err(err) => Self::fallback_from_batch_dispatch_error(err),
            };
        }

        Ok(None)
    }
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        let _permit = match &self.max_concurrency_request_limit {
            Some(limit) => Some(limit.acquire().await.map_err(|err| Error::InternalError {
                message: err.to_string(),
            })?),
            None => None,
        };

        let timeout = self.timeout_for_request(request);

        let result = async {
            if let Some(resp) = self.try_dispatch_batch(request).await? {
                return Ok(resp);
            }
            if let (Some(cache), Some(cop_req)) = (
                self.copr_cache.as_ref(),
                request.as_any().downcast_ref::<coprocessor::Request>(),
            ) {
                return self
                    .dispatch_coprocessor_unary_with_cache(cache, cop_req, timeout)
                    .await;
            }
            request.dispatch(&self.rpc_client, timeout).await
        }
        .await;

        if let Some(tracker) = self.grpc_connection_state.as_ref() {
            match &result {
                Ok(_) => tracker.set_state(TrackedGrpcConnectionState::Ready),
                Err(Error::GrpcAPI(_)) => {
                    tracker.set_state(TrackedGrpcConnectionState::TransientFailure)
                }
                Err(_) => {}
            }
        }

        result
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn store_address(&self) -> Option<&str> {
        Some(&self.store_address)
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
    use serial_test::serial;
    use tokio::sync::mpsc;
    use tonic::body::BoxBody;
    use tonic::codegen::http;
    use tonic::codegen::Service;

    use super::*;
    use crate::util::RateLimit;

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
    type ForwardedBatchClientForTest = (
        KvRpcClient,
        mpsc::Receiver<tikvpb::BatchCommandsRequest>,
        mpsc::Sender<BatchInbound>,
        mpsc::Receiver<tikvpb::BatchCommandsRequest>,
        mpsc::Sender<BatchInbound>,
    );

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
            copr_req_timeout: Duration::ZERO,
            copr_cache: None,
            store_address: "127.0.0.1:1".to_owned(),
            grpc_connection_state: None,
            batch: Some(batch),
            forwarded_batch_clients: None,
            max_concurrency_request_limit: None,
            health_feedback_observer: Arc::new(Mutex::new(None)),
        };

        Ok((client, out_rx, in_tx))
    }

    fn new_kv_rpc_client_with_forwarded_batch_for_test(
        forwarded_host: String,
    ) -> Result<ForwardedBatchClientForTest> {
        let (base_out_tx, base_out_rx) = mpsc::channel(8);
        let (base_in_tx, base_in_rx) = mpsc::channel(8);
        let base_inbound_stream = stream::unfold(base_in_rx, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        let base_batch =
            BatchCommandsClient::new_with_inbound_for_test(base_out_tx, base_inbound_stream)?;

        let (forward_out_tx, forward_out_rx) = mpsc::channel(8);
        let (forward_in_tx, forward_in_rx) = mpsc::channel(8);
        let forward_inbound_stream = stream::unfold(forward_in_rx, |mut rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        let forward_batch =
            BatchCommandsClient::new_with_inbound_for_test(forward_out_tx, forward_inbound_stream)?;

        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let rpc_client = TikvClient::new(channel);

        let mut clients = HashMap::new();
        clients.insert(forwarded_host.clone(), forward_batch);
        let forwarded_batch_clients = ForwardedBatchCommandsClientPool {
            rpc_client: rpc_client.clone(),
            max_outbound_requests: 128,
            batch_policy: "basic".to_owned(),
            overload_threshold: 0,
            batch_wait_size: 0,
            max_wait_time: Duration::ZERO,
            target: "127.0.0.1:1".to_owned(),
            clients: Arc::new(Mutex::new(clients)),
        };

        let client = KvRpcClient {
            rpc_client,
            timeout: Duration::from_secs(1),
            copr_req_timeout: Duration::ZERO,
            copr_cache: None,
            store_address: "127.0.0.1:1".to_owned(),
            grpc_connection_state: None,
            batch: Some(base_batch),
            forwarded_batch_clients: Some(forwarded_batch_clients),
            max_concurrency_request_limit: None,
            health_feedback_observer: Arc::new(Mutex::new(None)),
        };

        Ok((
            client,
            base_out_rx,
            base_in_tx,
            forward_out_rx,
            forward_in_tx,
        ))
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_kv_rpc_client_records_no_available_connection_on_batch_grpc_error() -> Result<()>
    {
        let before = {
            let families = prometheus::gather();
            families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_batch_client_no_available_connection_total"
                })
                .and_then(|family| family.get_metric().first())
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        let (client, out_rx, _in_tx) = new_kv_rpc_client_with_batch_for_test()?;
        drop(out_rx);

        let mut request = kvrpcpb::GetRequest::default();
        request.key = b"k".to_vec();
        request.version = 7;

        let resp = client.try_dispatch_batch(&request).await?;
        assert!(
            resp.is_none(),
            "expected batch dispatch to fall back to unary"
        );

        let after = {
            let families = prometheus::gather();
            let family = families
                .iter()
                .find(|family| {
                    family.get_name() == "tikv_client_batch_client_no_available_connection_total"
                })
                .expect("batch client no-available-connection counter not registered");
            family
                .get_metric()
                .first()
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        };

        assert!(
            after >= before + 1.0,
            "expected no-available-connection counter to increase"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_try_dispatch_batch_respects_request_timeout_override() -> Result<()> {
        let (client, _out_rx, _in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::GetRequest::default();
        request.key = b"k".to_vec();
        request.version = 7;
        let request = crate::request::RequestWithTimeout::new(request, Duration::from_millis(1));

        let response = tokio::time::timeout(
            Duration::from_millis(50),
            client.try_dispatch_batch(&request),
        )
        .await
        .expect("expected batch dispatch to time out quickly");

        assert!(
            response?.is_none(),
            "expected batch dispatch to fall back to unary"
        );
        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_try_dispatch_batch_uses_coprocessor_timeout_override() -> Result<()> {
        let (mut client, _out_rx, _in_tx) = new_kv_rpc_client_with_batch_for_test()?;
        client.copr_req_timeout = Duration::from_millis(1);

        let request = coprocessor::Request::default();
        let response = tokio::time::timeout(
            Duration::from_millis(50),
            client.try_dispatch_batch(&request),
        )
        .await
        .expect("expected batch dispatch to time out quickly");

        assert!(
            response?.is_none(),
            "expected batch dispatch to fall back to unary"
        );
        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_kv_rpc_client_updates_grpc_connection_state_metric() {
        const STORE_ADDR: &str = "127.0.0.1:2";
        const STORE_URI: &str = "http://127.0.0.1:2";
        let connection_id = format!("{STORE_ADDR}-0");

        fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
            metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == name)
                .map(|pair| pair.get_value())
        }

        fn gauge_value(connection_id: &str, store_ip: &str, state: &str) -> Option<f64> {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_grpc_connection_state")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "connection_id") == Some(connection_id)
                            && label_value(metric, "store_ip") == Some(store_ip)
                            && label_value(metric, "grpc_state") == Some(state)
                    })
                })
                .map(|metric| metric.get_gauge().get_value())
        }

        let rpc_client = TikvClient::new(Channel::from_static(STORE_URI).connect_lazy());
        let client = KvRpcClient::new(
            rpc_client,
            Duration::from_millis(50),
            Duration::ZERO,
            STORE_ADDR.to_owned(),
            false,
            0,
            String::new(),
            0,
            0,
            Duration::ZERO,
            i64::MAX,
            None,
            Arc::new(Mutex::new(None)),
        )
        .await;

        assert_eq!(
            gauge_value(&connection_id, STORE_ADDR, "READY"),
            Some(1.0),
            "expected READY grpc_connection_state gauge to be set when KvRpcClient is created"
        );

        let mut request = kvrpcpb::GetRequest::default();
        request.key = b"k".to_vec();
        request.version = 1;
        request.context = Some(kvrpcpb::Context::default());

        let result = client.dispatch(&request).await;
        assert!(
            result.is_err(),
            "expected dispatch to fail against {STORE_URI}"
        );

        assert_eq!(
            gauge_value(&connection_id, STORE_ADDR, "TRANSIENT_FAILURE"),
            Some(1.0),
            "expected TRANSIENT_FAILURE grpc_connection_state gauge to be set after dispatch error"
        );
    }

    #[tokio::test]
    async fn test_try_dispatch_batch_uses_forwarded_batch_stream_when_forwarded_host_set(
    ) -> Result<()> {
        let forwarded_host = "127.0.0.1:20160".to_owned();
        let (client, mut base_out_rx, base_in_tx, mut forward_out_rx, forward_in_tx) =
            new_kv_rpc_client_with_forwarded_batch_for_test(forwarded_host.clone())?;

        let mut request = kvrpcpb::GetRequest::default();
        request.key = b"k".to_vec();
        request.version = 7;

        // Without forwarded host, we should use the base batch stream.
        let dispatch = tokio::spawn({
            let client = client.clone();
            let request = request.clone();
            async move {
                client
                    .dispatch(&request)
                    .await?
                    .downcast::<kvrpcpb::GetResponse>()
                    .map_err(|_| Error::StringError("expected get response".to_owned()))?;
                Ok::<(), Error>(())
            }
        });

        let sent = tokio::select! {
            sent = base_out_rx.recv() => sent.expect("expected base batch request"),
            sent = forward_out_rx.recv() => {
                return Err(Error::StringError(format!(
                    "unexpected forwarded batch request without forwarded host: {sent:?}"
                )));
            }
        };
        let request_id = sent.request_ids[0];
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Get(
                    kvrpcpb::GetResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        base_in_tx.send(Ok(response)).await.expect("send response");

        dispatch.await.expect("expected join ok")?;

        // With forwarded host, we should use the forwarded batch stream.
        let dispatch = tokio::spawn({
            let client = client.clone();
            let request = request.clone();
            let forwarded_host = forwarded_host.clone();
            async move {
                crate::store::scope_forwarded_host(forwarded_host, client.dispatch(&request))
                    .await?
                    .downcast::<kvrpcpb::GetResponse>()
                    .map_err(|_| Error::StringError("expected get response".to_owned()))?;
                Ok::<(), Error>(())
            }
        });

        let sent = tokio::select! {
            sent = forward_out_rx.recv() => sent.expect("expected forwarded batch request"),
            sent = base_out_rx.recv() => {
                return Err(Error::StringError(format!(
                    "unexpected base batch request with forwarded host: {sent:?}"
                )));
            }
        };
        let request_id = sent.request_ids[0];
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Get(
                    kvrpcpb::GetResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        forward_in_tx
            .send(Ok(response))
            .await
            .expect("send response");

        dispatch.await.expect("expected join ok")?;
        Ok(())
    }

    #[tokio::test]
    async fn test_coprocessor_cache_applies_on_batch_dispatch() -> Result<()> {
        let (mut client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;
        let cache = CoprocessorCache::from_config(&CoprocessorCacheConfig {
            capacity_mb: 1,
            admission_max_ranges: 500,
            admission_max_result_mb: 1,
            admission_min_process_ms: 5,
        })
        .expect("expected enabled cache");
        client.copr_cache = Some(Arc::new(cache));

        let region_id = 7;
        let mut request = coprocessor::Request::default();
        request.tp = 1;
        request.start_ts = 10;
        request.data = vec![1, 2, 3];
        request.ranges = vec![coprocessor::KeyRange {
            start: vec![1],
            end: vec![2],
        }];
        request.context = Some(kvrpcpb::Context {
            region_id,
            ..Default::default()
        });

        let dispatch_one = {
            let client = client.clone();
            let request = request.clone();
            tokio::spawn(async move {
                let resp_any = client.dispatch(&request).await?;
                let resp = *resp_any.downcast::<coprocessor::Response>().map_err(|_| {
                    Error::InternalError {
                        message: "expected coprocessor response".to_owned(),
                    }
                })?;
                Ok::<_, Error>(resp)
            })
        };

        let outbound = out_rx.recv().await.expect("expected outbound request");
        assert_eq!(outbound.request_ids.len(), 1);
        assert_eq!(outbound.requests.len(), 1);
        let request_id = outbound.request_ids[0];

        let sent_request = match outbound.requests[0]
            .cmd
            .as_ref()
            .expect("expected outbound request cmd")
        {
            tikvpb::batch_commands_request::request::Cmd::Coprocessor(req) => req,
            other => panic!("unexpected batch cmd for coprocessor request: {other:?}"),
        };
        assert!(sent_request.is_cache_enabled);
        assert_eq!(sent_request.cache_if_match_version, 0);

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Coprocessor(
                    coprocessor::Response {
                        data: vec![9],
                        can_be_cached: true,
                        cache_last_version: 123,
                        exec_details: Some(kvrpcpb::ExecDetails {
                            time_detail: Some(kvrpcpb::TimeDetail {
                                process_wall_time_ms: 5,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("expected send ok");

        let resp = dispatch_one.await.expect("expected join ok")?;
        assert_eq!(resp.data, vec![9]);
        assert!(!resp.is_cache_hit);

        // Second request should carry cache_if_match_version=123, and a cache hit should fill data.
        let mut request2 = request.clone();
        request2.start_ts = 11;

        let dispatch_two = {
            let client = client.clone();
            tokio::spawn(async move {
                let resp_any = client.dispatch(&request2).await?;
                let resp = *resp_any.downcast::<coprocessor::Response>().map_err(|_| {
                    Error::InternalError {
                        message: "expected coprocessor response".to_owned(),
                    }
                })?;
                Ok::<_, Error>(resp)
            })
        };

        let outbound = out_rx.recv().await.expect("expected outbound request");
        let request_id = outbound.request_ids[0];
        let sent_request = match outbound.requests[0]
            .cmd
            .as_ref()
            .expect("expected outbound request cmd")
        {
            tikvpb::batch_commands_request::request::Cmd::Coprocessor(req) => req,
            other => panic!("unexpected batch cmd for coprocessor request: {other:?}"),
        };
        assert!(sent_request.is_cache_enabled);
        assert_eq!(sent_request.cache_if_match_version, 123);

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Coprocessor(
                    coprocessor::Response {
                        is_cache_hit: true,
                        cache_last_version: 123,
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("expected send ok");

        let resp = dispatch_two.await.expect("expected join ok")?;
        assert!(resp.is_cache_hit);
        assert_eq!(resp.data, vec![9]);
        Ok(())
    }

    #[derive(Clone, Debug)]
    struct TimeoutEchoRequest {
        label: &'static str,
    }

    #[async_trait]
    impl Request for TimeoutEchoRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            timeout: Duration,
        ) -> Result<Box<dyn Any>> {
            Ok(Box::new(timeout) as Box<dyn Any>)
        }

        fn label(&self) -> &'static str {
            self.label
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _leader: &crate::store::RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

        fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
    }

    #[tokio::test]
    async fn test_kv_rpc_client_uses_coprocessor_timeout_for_coprocessor_labels() -> Result<()> {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let rpc_client = TikvClient::new(channel);
        let client = KvRpcClient {
            rpc_client,
            timeout: Duration::from_secs(1),
            copr_req_timeout: Duration::from_secs(9),
            copr_cache: None,
            store_address: "127.0.0.1:1".to_owned(),
            grpc_connection_state: None,
            batch: None,
            forwarded_batch_clients: None,
            max_concurrency_request_limit: None,
            health_feedback_observer: Arc::new(Mutex::new(None)),
        };

        let request = TimeoutEchoRequest {
            label: "batch_coprocessor",
        };
        let timeout = client
            .dispatch(&request)
            .await?
            .downcast::<Duration>()
            .map(|timeout| *timeout)
            .map_err(|_| Error::StringError("expected duration response".to_owned()))?;
        assert_eq!(timeout, Duration::from_secs(9));

        let request = TimeoutEchoRequest { label: "get" };
        let timeout = client
            .dispatch(&request)
            .await?
            .downcast::<Duration>()
            .map(|timeout| *timeout)
            .map_err(|_| Error::StringError("expected duration response".to_owned()))?;
        assert_eq!(timeout, Duration::from_secs(1));

        let request = crate::request::RequestWithTimeout::new(
            TimeoutEchoRequest {
                label: "coprocessor",
            },
            Duration::from_millis(3),
        );
        let timeout = client
            .dispatch(&request)
            .await?
            .downcast::<Duration>()
            .map(|timeout| *timeout)
            .map_err(|_| Error::StringError("expected duration response".to_owned()))?;
        assert_eq!(timeout, Duration::from_millis(3));
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_get_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::GetRequest::default();
        request.key = b"k".to_vec();
        request.version = 7;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "get dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Get(sent_req) => {
                assert_eq!(sent_req.key, b"k".to_vec());
                assert_eq!(sent_req.version, 7);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for get: {other:?}"
                )));
            }
        }

        let mut resp = kvrpcpb::GetResponse::default();
        resp.value = b"v".to_vec();
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Get(resp)),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::GetResponse>()
            .map_err(|_| Error::StringError("expected get response".to_owned()))?;
        assert_eq!(resp.value, b"v".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_batch_get_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::BatchGetRequest::default();
        request.keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        request.version = 7;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "batch_get dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::BatchGet(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.version, 7);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for batch_get: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::BatchGet(
                    kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"k1".to_vec(),
                            value: b"v1".to_vec(),
                            error: None,
                            commit_ts: 0,
                        }],
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::BatchGetResponse>()
            .map_err(|_| Error::StringError("expected batch_get response".to_owned()))?;
        assert_eq!(resp.pairs.len(), 1);
        assert_eq!(resp.pairs[0].key, b"k1".to_vec());
        assert_eq!(resp.pairs[0].value, b"v1".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_batch_rollback_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::BatchRollbackRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        request.start_version = 7;
        request.is_txn_file = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "batch_rollback dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::BatchRollback(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.start_version, 7);
                assert!(sent_req.is_txn_file);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for batch_rollback: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::BatchRollback(
                        kvrpcpb::BatchRollbackResponse::default(),
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
            .downcast::<kvrpcpb::BatchRollbackResponse>()
            .map_err(|_| Error::StringError("expected batch_rollback response".to_owned()))?;
        assert!(resp.region_error.is_none());
        assert!(resp.error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_scan_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::ScanRequest::default();
        request.start_key = b"a".to_vec();
        request.end_key = b"z".to_vec();
        request.version = 7;
        request.limit = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "scan dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Scan(sent_req) => {
                assert_eq!(sent_req.start_key, b"a".to_vec());
                assert_eq!(sent_req.end_key, b"z".to_vec());
                assert_eq!(sent_req.version, 7);
                assert_eq!(sent_req.limit, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for scan: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Scan(
                    kvrpcpb::ScanResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"v".to_vec(),
                            error: None,
                            commit_ts: 0,
                        }],
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::ScanResponse>()
            .map_err(|_| Error::StringError("expected scan response".to_owned()))?;
        assert_eq!(resp.pairs.len(), 1);
        assert_eq!(resp.pairs[0].key, b"a".to_vec());
        assert_eq!(resp.pairs[0].value, b"v".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_prewrite_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::PrewriteRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put as i32,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        }];
        request.primary_lock = b"p".to_vec();
        request.start_version = 7;
        request.lock_ttl = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "prewrite dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Prewrite(sent_req) => {
                assert_eq!(sent_req.primary_lock, b"p".to_vec());
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.lock_ttl, 123);
                assert_eq!(sent_req.mutations.len(), 1);
                assert_eq!(sent_req.mutations[0].key, b"k".to_vec());
                assert_eq!(sent_req.mutations[0].value, b"v".to_vec());
                assert_eq!(sent_req.mutations[0].op, kvrpcpb::Op::Put as i32);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for prewrite: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Prewrite(
                    kvrpcpb::PrewriteResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::PrewriteResponse>()
            .map_err(|_| Error::StringError("expected prewrite response".to_owned()))?;
        assert!(resp.region_error.is_none());
        assert!(resp.errors.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_commit_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::CommitRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.start_version = 7;
        request.keys = vec![b"k".to_vec()];
        request.commit_version = 8;
        request.is_txn_file = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "commit dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Commit(sent_req) => {
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.keys, vec![b"k".to_vec()]);
                assert_eq!(sent_req.commit_version, 8);
                assert!(sent_req.is_txn_file);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for commit: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Commit(
                    kvrpcpb::CommitResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::CommitResponse>()
            .map_err(|_| Error::StringError("expected commit response".to_owned()))?;
        assert!(resp.region_error.is_none());
        assert!(resp.error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_cleanup_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::CleanupRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.key = b"k".to_vec();
        request.start_version = 7;
        request.current_ts = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "cleanup dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Cleanup(sent_req) => {
                assert_eq!(sent_req.key, b"k".to_vec());
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.current_ts, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for cleanup: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Cleanup(
                    kvrpcpb::CleanupResponse {
                        commit_version: 42,
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::CleanupResponse>()
            .map_err(|_| Error::StringError("expected cleanup response".to_owned()))?;
        assert_eq!(resp.commit_version, 42);
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_coprocessor_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = coprocessor::Request::default();
        request.tp = 1;
        request.start_ts = 7;
        request.ranges = vec![coprocessor::KeyRange {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
        }];

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "coprocessor dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Coprocessor(sent_req) => {
                assert_eq!(sent_req.tp, 1);
                assert_eq!(sent_req.start_ts, 7);
                assert_eq!(sent_req.ranges.len(), 1);
                assert_eq!(sent_req.ranges[0].start, b"a".to_vec());
                assert_eq!(sent_req.ranges[0].end, b"z".to_vec());
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for coprocessor: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Coprocessor(
                    coprocessor::Response {
                        data: b"v".to_vec(),
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<coprocessor::Response>()
            .map_err(|_| Error::StringError("expected coprocessor response".to_owned()))?;
        assert_eq!(resp.data, b"v".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_get_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawGetRequest::default();
        request.key = b"k".to_vec();
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_get dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawGet(sent_req) => {
                assert_eq!(sent_req.key, b"k".to_vec());
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_get: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawGet(
                    kvrpcpb::RawGetResponse {
                        value: b"v".to_vec(),
                        not_found: false,
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::RawGetResponse>()
            .map_err(|_| Error::StringError("expected raw_get response".to_owned()))?;
        assert_eq!(resp.value, b"v".to_vec());
        assert!(!resp.not_found);
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_batch_get_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawBatchGetRequest::default();
        request.keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_batch_get dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawBatchGet(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_batch_get: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawBatchGet(
                    kvrpcpb::RawBatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"k1".to_vec(),
                            value: b"v1".to_vec(),
                            error: None,
                            commit_ts: 0,
                        }],
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::RawBatchGetResponse>()
            .map_err(|_| Error::StringError("expected raw_batch_get response".to_owned()))?;
        assert_eq!(resp.pairs.len(), 1);
        assert_eq!(resp.pairs[0].key, b"k1".to_vec());
        assert_eq!(resp.pairs[0].value, b"v1".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_scan_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawScanRequest::default();
        request.start_key = b"a".to_vec();
        request.end_key = b"z".to_vec();
        request.limit = 123;
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_scan dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawScan(sent_req) => {
                assert_eq!(sent_req.start_key, b"a".to_vec());
                assert_eq!(sent_req.end_key, b"z".to_vec());
                assert_eq!(sent_req.limit, 123);
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_scan: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawScan(
                    kvrpcpb::RawScanResponse {
                        kvs: vec![kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"v".to_vec(),
                            error: None,
                            commit_ts: 0,
                        }],
                        ..Default::default()
                    },
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::RawScanResponse>()
            .map_err(|_| Error::StringError("expected raw_scan response".to_owned()))?;
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].key, b"a".to_vec());
        assert_eq!(resp.kvs[0].value, b"v".to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_batch_scan_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawBatchScanRequest::default();
        request.ranges = vec![
            kvrpcpb::KeyRange {
                start_key: b"a".to_vec(),
                end_key: b"m".to_vec(),
            },
            kvrpcpb::KeyRange {
                start_key: b"m".to_vec(),
                end_key: b"z".to_vec(),
            },
        ];
        request.each_limit = 10;
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_batch_scan dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawBatchScan(sent_req) => {
                assert_eq!(sent_req.ranges.len(), 2);
                assert_eq!(sent_req.ranges[0].start_key, b"a".to_vec());
                assert_eq!(sent_req.ranges[0].end_key, b"m".to_vec());
                assert_eq!(sent_req.ranges[1].start_key, b"m".to_vec());
                assert_eq!(sent_req.ranges[1].end_key, b"z".to_vec());
                assert_eq!(sent_req.each_limit, 10);
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_batch_scan: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::RawBatchScan(
                        kvrpcpb::RawBatchScanResponse {
                            kvs: vec![kvrpcpb::KvPair {
                                key: b"a".to_vec(),
                                value: b"v".to_vec(),
                                error: None,
                                commit_ts: 0,
                            }],
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
            .downcast::<kvrpcpb::RawBatchScanResponse>()
            .map_err(|_| Error::StringError("expected raw_batch_scan response".to_owned()))?;
        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].key, b"a".to_vec());
        assert_eq!(resp.kvs[0].value, b"v".to_vec());
        Ok(())
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
    async fn test_kv_rpc_client_dispatches_raw_batch_put_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawBatchPutRequest::default();
        request.pairs = vec![
            kvrpcpb::KvPair {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                error: None,
                commit_ts: 0,
            },
            kvrpcpb::KvPair {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                error: None,
                commit_ts: 0,
            },
        ];
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_batch_put dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawBatchPut(sent_req) => {
                assert_eq!(sent_req.pairs.len(), 2);
                assert_eq!(sent_req.pairs[0].key, b"k1".to_vec());
                assert_eq!(sent_req.pairs[0].value, b"v1".to_vec());
                assert_eq!(sent_req.pairs[1].key, b"k2".to_vec());
                assert_eq!(sent_req.pairs[1].value, b"v2".to_vec());
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_batch_put: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawBatchPut(
                    kvrpcpb::RawBatchPutResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::RawBatchPutResponse>()
            .map_err(|_| Error::StringError("expected raw_batch_put response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_delete_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawDeleteRequest::default();
        request.key = b"k".to_vec();
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_delete dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawDelete(sent_req) => {
                assert_eq!(sent_req.key, b"k".to_vec());
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_delete: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::RawDelete(
                    kvrpcpb::RawDeleteResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::RawDeleteResponse>()
            .map_err(|_| Error::StringError("expected raw_delete response".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_raw_batch_delete_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::RawBatchDeleteRequest::default();
        request.keys = vec![b"k1".to_vec(), b"k2".to_vec()];
        request.cf = "default".to_owned();

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "raw_batch_delete dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::RawBatchDelete(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.cf, "default");
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for raw_batch_delete: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::RawBatchDelete(
                        kvrpcpb::RawBatchDeleteResponse::default(),
                    ),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        resp.downcast::<kvrpcpb::RawBatchDeleteResponse>()
            .map_err(|_| Error::StringError("expected raw_batch_delete response".to_owned()))?;
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
    async fn test_kv_rpc_client_dispatches_pessimistic_lock_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::PessimisticLockRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::PessimisticLock as i32,
            key: b"k".to_vec(),
            ..Default::default()
        }];
        request.primary_lock = b"p".to_vec();
        request.start_version = 7;
        request.lock_ttl = 123;
        request.for_update_ts = 8;
        request.is_first_lock = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "pessimistic_lock dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::PessimisticLock(sent_req) => {
                assert_eq!(sent_req.primary_lock, b"p".to_vec());
                assert_eq!(sent_req.start_version, 7);
                assert_eq!(sent_req.lock_ttl, 123);
                assert_eq!(sent_req.for_update_ts, 8);
                assert!(sent_req.is_first_lock);
                assert_eq!(sent_req.mutations.len(), 1);
                assert_eq!(sent_req.mutations[0].key, b"k".to_vec());
                assert_eq!(
                    sent_req.mutations[0].op,
                    kvrpcpb::Op::PessimisticLock as i32
                );
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for pessimistic_lock: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::PessimisticLock(
                        kvrpcpb::PessimisticLockResponse::default(),
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
            .downcast::<kvrpcpb::PessimisticLockResponse>()
            .map_err(|_| Error::StringError("expected pessimistic_lock response".to_owned()))?;
        assert!(resp.region_error.is_none());
        assert!(resp.errors.is_empty());
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
    async fn test_kv_rpc_client_dispatches_gc_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::GcRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.safe_point = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "gc dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Gc(sent_req) => {
                assert_eq!(sent_req.safe_point, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for gc: {other:?}"
                )));
            }
        }

        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Gc(
                    kvrpcpb::GcResponse::default(),
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::GcResponse>()
            .map_err(|_| Error::StringError("expected gc response".to_owned()))?;
        assert!(resp.region_error.is_none());
        assert!(resp.error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_delete_range_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::DeleteRangeRequest::default();
        request.start_key = b"k1".to_vec();
        request.end_key = b"k3".to_vec();
        request.notify_only = true;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "delete_range dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::DeleteRange(sent_req) => {
                assert_eq!(sent_req.start_key, b"k1".to_vec());
                assert_eq!(sent_req.end_key, b"k3".to_vec());
                assert!(sent_req.notify_only);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for delete_range: {other:?}"
                )));
            }
        }

        let mut resp = kvrpcpb::DeleteRangeResponse::default();
        resp.error = "oops".to_owned();
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::DeleteRange(
                    resp,
                )),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::DeleteRangeResponse>()
            .map_err(|_| Error::StringError("expected delete_range response".to_owned()))?;
        assert_eq!(resp.error, "oops");
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_prepare_flashback_to_version_via_batch_commands(
    ) -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::PrepareFlashbackToVersionRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.start_key = b"k1".to_vec();
        request.end_key = b"k3".to_vec();
        request.start_ts = 42;
        request.version = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "prepare_flashback_to_version dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::PrepareFlashbackToVersion(sent_req) => {
                assert_eq!(sent_req.start_key, b"k1".to_vec());
                assert_eq!(sent_req.end_key, b"k3".to_vec());
                assert_eq!(sent_req.start_ts, 42);
                assert_eq!(sent_req.version, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for prepare_flashback_to_version: {other:?}"
                )));
            }
        }

        let mut resp = kvrpcpb::PrepareFlashbackToVersionResponse::default();
        resp.error = "oops".to_owned();
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(
                    tikvpb::batch_commands_response::response::Cmd::PrepareFlashbackToVersion(resp),
                ),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::PrepareFlashbackToVersionResponse>()
            .map_err(|_| {
                Error::StringError("expected prepare_flashback_to_version response".to_owned())
            })?;
        assert_eq!(resp.error, "oops");
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_flashback_to_version_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let mut request = kvrpcpb::FlashbackToVersionRequest::default();
        request.context = Some(kvrpcpb::Context::default());
        request.start_key = b"k1".to_vec();
        request.end_key = b"k3".to_vec();
        request.start_ts = 42;
        request.commit_ts = 43;
        request.version = 123;

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "flashback_to_version dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };

        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::FlashbackToVersion(sent_req) => {
                assert_eq!(sent_req.start_key, b"k1".to_vec());
                assert_eq!(sent_req.end_key, b"k3".to_vec());
                assert_eq!(sent_req.start_ts, 42);
                assert_eq!(sent_req.commit_ts, 43);
                assert_eq!(sent_req.version, 123);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for flashback_to_version: {other:?}"
                )));
            }
        }

        let mut resp = kvrpcpb::FlashbackToVersionResponse::default();
        resp.error = "oops".to_owned();
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::FlashbackToVersion(resp)),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::FlashbackToVersionResponse>()
            .map_err(|_| Error::StringError("expected flashback_to_version response".to_owned()))?;
        assert_eq!(resp.error, "oops");
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_flush_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let request = kvrpcpb::FlushRequest {
            context: Some(kvrpcpb::Context::default()),
            mutations: vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            }],
            primary_key: b"p".to_vec(),
            start_ts: 42,
            min_commit_ts: 43,
            generation: 7,
            lock_ttl: 1234,
            assertion_level: kvrpcpb::AssertionLevel::Strict as i32,
        };

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "flush dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::Flush(sent_req) => {
                assert_eq!(sent_req.primary_key, b"p".to_vec());
                assert_eq!(sent_req.start_ts, 42);
                assert_eq!(sent_req.min_commit_ts, 43);
                assert_eq!(sent_req.generation, 7);
                assert_eq!(sent_req.lock_ttl, 1234);
                assert_eq!(
                    sent_req.assertion_level,
                    kvrpcpb::AssertionLevel::Strict as i32
                );
                assert_eq!(sent_req.mutations.len(), 1);
                assert_eq!(sent_req.mutations[0].op, kvrpcpb::Op::Put as i32);
                assert_eq!(sent_req.mutations[0].key, b"k1".to_vec());
                assert_eq!(sent_req.mutations[0].value, b"v1".to_vec());
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for flush: {other:?}"
                )));
            }
        }

        let resp = kvrpcpb::FlushResponse {
            errors: vec![kvrpcpb::KeyError {
                abort: "oops".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::Flush(resp)),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::FlushResponse>()
            .map_err(|_| Error::StringError("expected flush response".to_owned()))?;
        assert_eq!(
            resp.errors
                .first()
                .expect("flush response should contain a key error")
                .abort,
            "oops"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatches_buffer_batch_get_via_batch_commands() -> Result<()> {
        let (client, mut out_rx, in_tx) = new_kv_rpc_client_with_batch_for_test()?;

        let request = kvrpcpb::BufferBatchGetRequest {
            context: Some(kvrpcpb::Context::default()),
            keys: vec![b"k1".to_vec(), b"k2".to_vec()],
            version: 7,
        };

        let dispatch = client.dispatch(&request);
        tokio::pin!(dispatch);

        let sent = tokio::select! {
            sent = out_rx.recv() => sent.expect("batch request"),
            result = &mut dispatch => {
                return Err(Error::StringError(format!(
                    "buffer_batch_get dispatch finished before seeing batch request: {result:?}"
                )));
            }
        };
        let request_id = *sent.request_ids.first().expect("request id");
        assert_eq!(sent.request_ids.len(), 1);
        assert_eq!(sent.requests.len(), 1);

        let cmd = sent.requests.into_iter().next().unwrap().cmd.unwrap();
        match cmd {
            tikvpb::batch_commands_request::request::Cmd::BufferBatchGet(sent_req) => {
                assert_eq!(sent_req.keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
                assert_eq!(sent_req.version, 7);
            }
            other => {
                return Err(Error::StringError(format!(
                    "unexpected cmd for buffer_batch_get: {other:?}"
                )));
            }
        }

        let resp = kvrpcpb::BufferBatchGetResponse {
            pairs: vec![kvrpcpb::KvPair {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = tikvpb::BatchCommandsResponse {
            responses: vec![tikvpb::batch_commands_response::Response {
                cmd: Some(tikvpb::batch_commands_response::response::Cmd::BufferBatchGet(resp)),
            }],
            request_ids: vec![request_id],
            transport_layer_load: 0,
            health_feedback: None,
        };
        in_tx.send(Ok(response)).await.expect("send response");

        let resp = dispatch.await?;
        let resp = resp
            .downcast::<kvrpcpb::BufferBatchGetResponse>()
            .map_err(|_| Error::StringError("expected buffer_batch_get response".to_owned()))?;
        assert_eq!(resp.pairs.len(), 1);
        assert_eq!(resp.pairs[0].key, b"k1".to_vec());
        assert_eq!(resp.pairs[0].value, b"v1".to_vec());
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

    #[derive(Clone)]
    struct BlockingKvClient {
        started: Arc<tokio::sync::Notify>,
        proceed: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl KvClient for BlockingKvClient {
        async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn Any>> {
            self.started.notify_one();
            self.proceed.notified().await;
            Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
        }
    }

    struct BlockingRequest {
        started: Arc<tokio::sync::Notify>,
        proceed: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl Request for BlockingRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            _timeout: Duration,
        ) -> Result<Box<dyn Any>> {
            self.started.notify_one();
            self.proceed.notified().await;
            Ok(Box::new(()) as Box<dyn Any>)
        }

        fn label(&self) -> &'static str {
            "blocking"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _leader: &crate::store::RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

        fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
    }

    #[tokio::test]
    async fn test_kv_rpc_client_dispatch_blocks_when_max_concurrency_request_limit_reached() {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let rpc_client = TikvClient::new(channel);

        let client = KvRpcClient {
            rpc_client,
            timeout: Duration::from_secs(1),
            copr_req_timeout: Duration::from_secs(1),
            copr_cache: None,
            store_address: "test".to_owned(),
            grpc_connection_state: None,
            batch: None,
            forwarded_batch_clients: None,
            health_feedback_observer: Arc::new(Mutex::new(None)),
            max_concurrency_request_limit: Some(RateLimit::new(1)),
        };

        let first_started = Arc::new(tokio::sync::Notify::new());
        let first_proceed = Arc::new(tokio::sync::Notify::new());
        let first_request = BlockingRequest {
            started: Arc::clone(&first_started),
            proceed: Arc::clone(&first_proceed),
        };

        let second_started = Arc::new(tokio::sync::Notify::new());
        let second_proceed = Arc::new(tokio::sync::Notify::new());
        let second_request = BlockingRequest {
            started: Arc::clone(&second_started),
            proceed: Arc::clone(&second_proceed),
        };

        let dispatch_one = {
            let client = client.clone();
            async move {
                client
                    .dispatch(&first_request)
                    .await
                    .expect("dispatch one ok");
            }
        };

        let dispatch_two = {
            let client = client.clone();
            async move {
                client
                    .dispatch(&second_request)
                    .await
                    .expect("dispatch two ok");
            }
        };

        let first_task = tokio::spawn(dispatch_one);
        first_started.notified().await;

        let second_task = tokio::spawn(dispatch_two);
        let second_wait =
            tokio::time::timeout(Duration::from_millis(20), second_started.notified());
        assert!(
            second_wait.await.is_err(),
            "second dispatch should be blocked"
        );

        first_proceed.notify_one();
        first_task.await.expect("task one finished");

        second_started.notified().await;
        second_proceed.notify_one();
        second_task.await.expect("task two finished");
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_store_limit_kv_client_rejects_when_limit_exceeded_and_records_metric() {
        fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
            metric
                .get_label()
                .iter()
                .find(|pair| pair.get_name() == name)
                .map(|pair| pair.get_value())
        }

        fn counter_value(address: &str, store: &str) -> f64 {
            prometheus::gather()
                .iter()
                .find(|family| family.get_name() == "tikv_client_get_store_limit_token_error")
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        label_value(metric, "address") == Some(address)
                            && label_value(metric, "store") == Some(store)
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or(0.0)
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        let inner: Arc<dyn KvClient + Send + Sync> = Arc::new(BlockingKvClient {
            started: started.clone(),
            proceed: proceed.clone(),
        });

        let token_count = Arc::new(AtomicI64::new(0));
        let client = Arc::new(StoreLimitKvClient::new(
            inner,
            42,
            "127.0.0.1:20160".to_owned(),
            1,
            token_count.clone(),
        ));

        let first_dispatch = {
            let client = client.clone();
            async move {
                let req = kvrpcpb::GetRequest::default();
                client.dispatch(&req).await
            }
        };

        let second_dispatch = async {
            started.notified().await;
            assert_eq!(token_count.load(Ordering::Relaxed), 1);

            let before = counter_value("127.0.0.1:20160", "42");
            let err = {
                let req = kvrpcpb::GetRequest::default();
                client.dispatch(&req).await.unwrap_err()
            };
            let after = counter_value("127.0.0.1:20160", "42");
            assert!(
                after >= before + 1.0,
                "expected get_store_limit_token_error to increase"
            );

            let Error::TokenLimit(token_err) = err else {
                panic!("expected TokenLimit error, got {err:?}");
            };
            assert_eq!(token_err.store_id(), 42);
            assert!(
                token_err
                    .to_string()
                    .contains("Store token is up to the limit"),
                "unexpected store limit error: {token_err}"
            );

            assert_eq!(token_count.load(Ordering::Relaxed), 1);
            proceed.notify_one();
        };

        let (first_result, ()) = tokio::join!(first_dispatch, second_dispatch);
        first_result.expect("dispatch ok");
        assert_eq!(token_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_store_limit_kv_client_uses_task_local_store_limit_when_override_is_zero() {
        let started = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        let inner = Arc::new(BlockingKvClient {
            started: started.clone(),
            proceed: proceed.clone(),
        });
        let token_count = Arc::new(AtomicI64::new(0));
        let client = Arc::new(StoreLimitKvClient::new(
            inner,
            42,
            "127.0.0.1:20160".to_owned(),
            0,
            token_count.clone(),
        ));

        crate::store_vars::with_store_limit(1, async move {
            let first_dispatch = {
                let client = client.clone();
                async move {
                    let req = kvrpcpb::GetRequest::default();
                    client.dispatch(&req).await
                }
            };

            let second_dispatch = async {
                started.notified().await;
                assert_eq!(token_count.load(Ordering::Relaxed), 1);

                let err = {
                    let req = kvrpcpb::GetRequest::default();
                    client.dispatch(&req).await.unwrap_err()
                };

                let Error::TokenLimit(token_err) = err else {
                    panic!("expected TokenLimit error, got {err:?}");
                };
                assert_eq!(token_err.store_id(), 42);
                assert!(
                    token_err
                        .to_string()
                        .contains("Store token is up to the limit"),
                    "unexpected store limit error: {token_err}"
                );

                assert_eq!(token_count.load(Ordering::Relaxed), 1);
                proceed.notify_one();
            };

            let (first_result, ()) = tokio::join!(first_dispatch, second_dispatch);
            first_result.expect("dispatch ok");
            assert_eq!(token_count.load(Ordering::Relaxed), 0);
        })
        .await;
    }
}
