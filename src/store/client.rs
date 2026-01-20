// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use tonic::transport::Channel;

use super::batch_commands::batch_kind_for_request;
use super::batch_commands::BatchCommandsClient;
use super::Request;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::StoreHealthMap;
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
    store_health: StoreHealthMap,
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
        self.security_mgr
            .connect(address, TikvClient::new)
            .await
            .map(|c| KvRpcClient::new(c, self.timeout, self.store_health.clone()))
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any + Send>>;
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(Clone)]
pub struct KvRpcClient {
    rpc_client: TikvClient<Channel>,
    timeout: Duration,
    batch_client: BatchCommandsClient,
}

impl KvRpcClient {
    pub(crate) fn new(
        rpc_client: TikvClient<Channel>,
        timeout: Duration,
        store_health: StoreHealthMap,
    ) -> Self {
        let batch_client = BatchCommandsClient::new(rpc_client.clone(), store_health);
        Self {
            rpc_client,
            timeout,
            batch_client,
        }
    }
}

#[async_trait]
impl KvClient for KvRpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any + Send>> {
        if let Some(batch_req) = request.batch_request() {
            if let Ok(kind) = batch_kind_for_request(&batch_req) {
                return self
                    .batch_client
                    .dispatch(kind, batch_req, self.timeout)
                    .await;
            }
        }

        request.dispatch(&self.rpc_client, self.timeout).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tonic::transport::Endpoint;

    use super::*;
    use crate::proto::kvrpcpb;
    use crate::proto::tikvpb;
    use crate::store::RegionWithLeader;

    #[derive(Default)]
    struct TestRequest {
        called: AtomicBool,
        timeout_ms: AtomicU64,
        context: Option<kvrpcpb::Context>,
    }

    #[async_trait]
    impl Request for TestRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            timeout: Duration,
        ) -> Result<Box<dyn Any + Send>> {
            self.called.store(true, Ordering::SeqCst);
            self.timeout_ms
                .store(timeout.as_millis() as u64, Ordering::SeqCst);
            Ok(Box::new(42_u64))
        }

        fn label(&self) -> &'static str {
            "test_request"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn context_mut(&mut self) -> &mut kvrpcpb::Context {
            self.context.get_or_insert_with(kvrpcpb::Context::default)
        }

        fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}
    }

    #[tokio::test]
    async fn kv_rpc_client_dispatch_calls_request_dispatch() -> Result<()> {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = KvRpcClient::new(
            TikvClient::new(channel),
            Duration::from_millis(123),
            crate::store::StoreHealthMap::default(),
        );

        let req = TestRequest::default();
        let resp = client.dispatch(&req).await?;
        assert!(req.called.load(Ordering::SeqCst));
        assert_eq!(req.timeout_ms.load(Ordering::SeqCst), 123);
        assert_eq!(*resp.downcast::<u64>().unwrap(), 42);
        Ok(())
    }

    #[tokio::test]
    async fn kv_rpc_client_dispatch_propagates_request_error() {
        struct ErrorRequest;

        #[async_trait]
        impl Request for ErrorRequest {
            async fn dispatch(
                &self,
                _client: &TikvClient<Channel>,
                _timeout: Duration,
            ) -> Result<Box<dyn Any + Send>> {
                Err(crate::Error::Unimplemented)
            }

            fn label(&self) -> &'static str {
                "error_request"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn context_mut(&mut self) -> &mut kvrpcpb::Context {
                unreachable!("context not used")
            }

            fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}
        }

        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = KvRpcClient::new(
            TikvClient::new(channel),
            Duration::from_millis(1),
            crate::store::StoreHealthMap::default(),
        );

        let req = ErrorRequest;
        let err = client
            .dispatch(&req)
            .await
            .expect_err("KvRpcClient must propagate request errors");
        assert!(matches!(err, crate::Error::Unimplemented));
    }

    #[tokio::test]
    async fn tikv_connect_connect_rejects_invalid_address() {
        let connect = TikvConnect::new(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(1),
            crate::store::StoreHealthMap::default(),
        );
        let err = match connect.connect("not a valid address").await {
            Ok(_) => panic!("expected invalid address to fail fast"),
            Err(err) => err,
        };
        assert!(matches!(err, crate::Error::Grpc(_) | crate::Error::Url(_)));
    }

    #[derive(Default)]
    struct UnsupportedBatchRequest {
        unary_called: AtomicBool,
        context: Option<kvrpcpb::Context>,
    }

    #[async_trait]
    impl Request for UnsupportedBatchRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            _timeout: Duration,
        ) -> Result<Box<dyn Any + Send>> {
            self.unary_called.store(true, Ordering::SeqCst);
            Ok(Box::new(7_u64))
        }

        fn label(&self) -> &'static str {
            "unsupported_batch_request"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn batch_request(&self) -> Option<tikvpb::batch_commands_request::Request> {
            // The Rust client does not send Empty requests as "real" batch commands. It should
            // treat it as unsupported and fall back to unary dispatch.
            Some(tikvpb::batch_commands_request::Request {
                cmd: Some(tikvpb::batch_commands_request::request::Cmd::Empty(
                    tikvpb::BatchCommandsEmptyRequest::default(),
                )),
            })
        }

        fn context_mut(&mut self) -> &mut kvrpcpb::Context {
            self.context.get_or_insert_with(kvrpcpb::Context::default)
        }

        fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}
    }

    #[tokio::test]
    async fn kv_rpc_client_dispatch_falls_back_to_unary_when_batch_unimplemented() -> Result<()> {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = KvRpcClient::new(
            TikvClient::new(channel),
            Duration::from_millis(123),
            crate::store::StoreHealthMap::default(),
        );

        let req = UnsupportedBatchRequest::default();
        let resp = client.dispatch(&req).await?;
        assert!(req.unary_called.load(Ordering::SeqCst));
        assert_eq!(*resp.downcast::<u64>().unwrap(), 7);
        Ok(())
    }
}
