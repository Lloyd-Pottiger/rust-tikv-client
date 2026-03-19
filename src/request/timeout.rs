use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::RegionWithLeader;
use crate::request::{KvRequest, Shardable};
use crate::store::RegionStore;
use crate::store::Request;
use crate::Result;

/// Wraps a request and overrides the gRPC deadline for that request when `timeout` is non-zero.
///
/// This is used to implement per-snapshot KV read timeouts (client-go `KVSnapshot.SetKVReadTimeout`)
/// without changing the global TiKV client timeout.
#[derive(Clone, Debug)]
pub(crate) struct RequestWithTimeout<Req> {
    inner: Req,
    timeout: Duration,
}

impl<Req> RequestWithTimeout<Req> {
    pub(crate) fn new(inner: Req, timeout: Duration) -> RequestWithTimeout<Req> {
        RequestWithTimeout { inner, timeout }
    }
}

#[async_trait]
impl<Req> Request for RequestWithTimeout<Req>
where
    Req: Request + Clone + Sync + Send + 'static,
{
    async fn dispatch(
        &self,
        client: &crate::proto::tikvpb::tikv_client::TikvClient<tonic::transport::Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let timeout = if self.timeout.is_zero() {
            timeout
        } else {
            self.timeout
        };
        self.inner.dispatch(client, timeout).await
    }

    fn label(&self) -> &'static str {
        self.inner.label()
    }

    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        self.inner.set_leader(leader)
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        self.inner.set_api_version(api_version);
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        self.inner.set_is_retry_request(is_retry_request);
    }

    fn timeout_override(&self) -> Option<Duration> {
        (!self.timeout.is_zero()).then_some(self.timeout)
    }

    fn context(&self) -> Option<&kvrpcpb::Context> {
        self.inner.context()
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        self.inner.context_mut()
    }
}

#[async_trait]
impl<Req> KvRequest for RequestWithTimeout<Req>
where
    Req: KvRequest,
{
    type Response = Req::Response;
}

impl<Req> Shardable for RequestWithTimeout<Req>
where
    Req: Shardable,
{
    type Shard = Req::Shard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        self.inner.shards(pd_client)
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.inner.apply_shard(shard);
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.inner.apply_store(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use tonic::transport::Channel;

    use crate::proto::tikvpb::tikv_client::TikvClient;

    #[derive(Clone, Default)]
    struct RecordingRequest {
        seen: Arc<Mutex<Vec<Duration>>>,
    }

    #[async_trait]
    impl Request for RecordingRequest {
        async fn dispatch(
            &self,
            _client: &TikvClient<Channel>,
            timeout: Duration,
        ) -> Result<Box<dyn Any>> {
            self.seen.lock().unwrap().push(timeout);
            Ok(Box::new(()) as Box<dyn Any>)
        }

        fn label(&self) -> &'static str {
            "recording"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
            Ok(())
        }

        fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

        fn set_is_retry_request(&mut self, _is_retry_request: bool) {}

        fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
            None
        }
    }

    fn noop_client() -> TikvClient<Channel> {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        TikvClient::new(channel)
    }

    #[tokio::test]
    async fn test_request_with_timeout_passes_through_when_zero() {
        let request = RecordingRequest::default();
        let wrapped = RequestWithTimeout::new(request.clone(), Duration::ZERO);

        wrapped
            .dispatch(&noop_client(), Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            request.seen.lock().unwrap().as_slice(),
            &[Duration::from_secs(2)]
        );
    }

    #[tokio::test]
    async fn test_request_with_timeout_overrides_when_non_zero() {
        let request = RecordingRequest::default();
        let wrapped = RequestWithTimeout::new(request.clone(), Duration::from_secs(5));

        wrapped
            .dispatch(&noop_client(), Duration::from_secs(2))
            .await
            .unwrap();

        assert_eq!(
            request.seen.lock().unwrap().as_slice(),
            &[Duration::from_secs(5)]
        );
    }
}
