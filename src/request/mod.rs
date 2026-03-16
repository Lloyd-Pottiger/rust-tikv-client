// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use derive_new::new;

pub use self::keyspace::EncodeKeyspace;
pub use self::keyspace::KeyMode;
pub use self::keyspace::Keyspace;
pub use self::keyspace::TruncateKeyspace;
pub use self::plan::Collect;
pub use self::plan::CollectError;
pub use self::plan::CollectSingle;
pub use self::plan::CollectWithShard;
pub use self::plan::DefaultProcessor;
pub use self::plan::Dispatch;
pub use self::plan::ExtractError;
pub use self::plan::Merge;
pub use self::plan::MergeResponse;
pub use self::plan::Plan;
pub use self::plan::Process;
pub use self::plan::ProcessResponse;
pub use self::plan::ResolveLock;
pub(crate) use self::plan::ResolveLockForRead;
pub use self::plan::ResponseWithShard;
pub use self::plan::RetryableMultiRegion;
pub use self::plan_builder::PlanBuilder;
pub use self::plan_builder::SingleKey;
pub use self::shard::Batchable;
pub use self::shard::HasNextBatch;
pub use self::shard::NextBatch;
pub use self::shard::RangeRequest;
pub use self::shard::Shardable;
pub(crate) use self::timeout::RequestWithTimeout;
use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::OPTIMISTIC_BACKOFF;
use crate::backoff::PESSIMISTIC_BACKOFF;
use crate::store::Request;
use crate::store::{HasKeyErrors, Store};
use crate::transaction::HasLocks;

mod keyspace;
pub mod plan;
mod plan_builder;
mod shard;
mod timeout;

/// Abstracts any request sent to a TiKV server.
#[async_trait]
pub trait KvRequest: Request + Sized + Clone + Sync + Send + 'static {
    /// The expected response to the request.
    type Response: HasKeyErrors + HasLocks + Clone + Send + 'static;
}

/// For requests or plans which are handled at TiKV store (other than region) level.
pub trait StoreRequest {
    /// Apply the request to specified TiKV store.
    fn apply_store(&mut self, store: &Store);
}

#[derive(Clone, Debug, new, Eq, PartialEq)]
pub struct RetryOptions {
    /// How to retry when there is a region error and we need to resolve regions with PD.
    pub region_backoff: Backoff,
    /// How to retry when a key is locked.
    pub lock_backoff: Backoff,
}

impl RetryOptions {
    pub const fn default_optimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: OPTIMISTIC_BACKOFF,
        }
    }

    pub const fn default_pessimistic() -> RetryOptions {
        RetryOptions {
            region_backoff: DEFAULT_REGION_BACKOFF,
            lock_backoff: PESSIMISTIC_BACKOFF,
        }
    }

    pub const fn none() -> RetryOptions {
        RetryOptions {
            region_backoff: Backoff::no_backoff(),
            lock_backoff: Backoff::no_backoff(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::iter;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use tonic::transport::Channel;

    use super::*;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::proto::pdpb::Timestamp;
    use crate::proto::tikvpb::tikv_client::TikvClient;
    use crate::region::RegionWithLeader;
    use crate::store::region_stream_for_keys;
    use crate::store::HasRegionError;
    use crate::transaction::lowering::new_commit_request;
    use crate::transaction::lowering::new_get_request;
    use crate::Error;
    use crate::Key;
    use crate::Result;

    #[tokio::test]
    async fn test_region_retry() {
        #[derive(Debug, Clone)]
        struct MockRpcResponse;

        impl HasKeyErrors for MockRpcResponse {
            fn key_errors(&mut self) -> Option<Vec<Error>> {
                None
            }
        }

        impl HasRegionError for MockRpcResponse {
            fn region_error(&mut self) -> Option<crate::proto::errorpb::Error> {
                Some(crate::proto::errorpb::Error::default())
            }
        }

        impl HasLocks for MockRpcResponse {}

        #[derive(Clone)]
        struct MockKvRequest {
            test_invoking_count: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Request for MockKvRequest {
            async fn dispatch(&self, _: &TikvClient<Channel>, _: Duration) -> Result<Box<dyn Any>> {
                Ok(Box::new(MockRpcResponse {}))
            }

            fn label(&self) -> &'static str {
                "mock"
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _: kvrpcpb::ApiVersion) {}

            fn set_is_retry_request(&mut self, _: bool) {}
        }

        #[async_trait]
        impl KvRequest for MockKvRequest {
            type Response = MockRpcResponse;
        }

        impl Shardable for MockKvRequest {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &std::sync::Arc<impl crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                crate::Result<(Self::Shard, crate::region::RegionWithLeader)>,
            > {
                // Increases by 1 for each call.
                self.test_invoking_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                region_stream_for_keys(
                    Some(Key::from("mock_key".to_owned())).into_iter(),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, _shard: Self::Shard) {}

            fn apply_store(&mut self, _store: &crate::store::RegionStore) -> crate::Result<()> {
                Ok(())
            }
        }

        let invoking_count = Arc::new(AtomicUsize::new(0));

        let request = MockKvRequest {
            test_invoking_count: invoking_count.clone(),
        };

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| Ok(Box::new(MockRpcResponse) as Box<dyn Any>),
        )));

        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, request)
            .retry_multi_region(Backoff::no_jitter_backoff(1, 1, 3))
            .extract_error()
            .plan();
        let _ = plan.execute().await;

        // Original call plus the 3 retries
        assert_eq!(invoking_count.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_is_retry_request_flag_set_on_retry() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let seen_is_retry_request = Arc::new(std::sync::Mutex::new(Vec::new()));

        let call_count_cloned = call_count.clone();
        let seen_is_retry_request_cloned = seen_is_retry_request.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_is_retry_request_cloned
                    .lock()
                    .unwrap()
                    .push(ctx.is_retry_request);

                let call = call_count_cloned.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let response = if call == 0 {
                    kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error::default()),
                        ..Default::default()
                    }
                } else {
                    kvrpcpb::GetResponse::default()
                };

                Ok(Box::new(response) as Box<dyn Any>)
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_get_request(key, Timestamp::default());
        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, req)
            .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
            .plan();
        let _ = plan.execute().await.unwrap();

        assert_eq!(*seen_is_retry_request.lock().unwrap(), vec![false, true]);
    }

    #[tokio::test]
    async fn test_cluster_id_is_set_on_requests() {
        let seen_cluster_id = Arc::new(std::sync::Mutex::new(Vec::new()));

        let seen_cluster_id_cloned = seen_cluster_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                seen_cluster_id_cloned.lock().unwrap().push(ctx.cluster_id);

                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_get_request(key, Timestamp::default());
        let plan = crate::request::PlanBuilder::new(pd_client, Keyspace::Disable, req)
            .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
            .plan();
        let _ = plan.execute().await.unwrap();

        assert_eq!(*seen_cluster_id.lock().unwrap(), vec![42]);
    }

    #[tokio::test]
    async fn test_keyspace_name_is_set_for_default_keyspace_id() {
        let seen_keyspace_name = Arc::new(std::sync::Mutex::new(String::new()));
        let seen_keyspace_id = Arc::new(std::sync::Mutex::new(0_u32));

        let seen_keyspace_name_cloned = seen_keyspace_name.clone();
        let seen_keyspace_id_cloned = seen_keyspace_id.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::GetRequest>()
                    .expect("expected get request");
                let ctx = req.context.as_ref().expect("context");
                *seen_keyspace_name_cloned.lock().unwrap() = ctx.keyspace_name.clone();
                *seen_keyspace_id_cloned.lock().unwrap() = ctx.keyspace_id;

                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_get_request(key, Timestamp::default());
        let plan =
            crate::request::PlanBuilder::new(pd_client, Keyspace::Enable { keyspace_id: 0 }, req)
                .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
                .plan();
        let _ = plan.execute().await.unwrap();

        assert_eq!(*seen_keyspace_id.lock().unwrap(), 0);
        assert_eq!(*seen_keyspace_name.lock().unwrap(), "DEFAULT");
    }

    #[tokio::test]
    async fn test_extract_error() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_: &dyn Any| {
                Ok(Box::new(kvrpcpb::CommitResponse {
                    error: Some(kvrpcpb::KeyError::default()),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let key: Key = "key".to_owned().into();
        let req = new_commit_request(iter::once(key), Timestamp::default(), Timestamp::default());

        // does not extract error
        let plan =
            crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req.clone())
                .retry_multi_region(OPTIMISTIC_BACKOFF)
                .plan();
        assert!(plan.execute().await.is_ok());

        // extract error
        let plan = crate::request::PlanBuilder::new(pd_client.clone(), Keyspace::Disable, req)
            .retry_multi_region(OPTIMISTIC_BACKOFF)
            .extract_error()
            .plan();
        assert!(plan.execute().await.is_err());
    }
}
