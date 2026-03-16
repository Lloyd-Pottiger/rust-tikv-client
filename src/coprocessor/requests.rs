use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use tonic::transport::Channel;
use tonic::IntoRequest;

use crate::pd::PdClient;
use crate::proto::coprocessor as coprocessor_pb;
use crate::proto::kvrpcpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::region::RegionWithLeader;
use crate::request::KvRequest;
use crate::request::Shardable;
use crate::store::region_stream_for_ranges;
use crate::store::RegionStore;
use crate::store::Request;
use crate::transaction::HasLocks;
use crate::Error;
use crate::Result;

impl KvRequest for coprocessor_pb::Request {
    type Response = coprocessor_pb::Response;
}

#[derive(Clone, Debug)]
pub struct CoprocessorStreamRequest {
    inner: coprocessor_pb::Request,
}

impl CoprocessorStreamRequest {
    pub fn inner(&self) -> &coprocessor_pb::Request {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut coprocessor_pb::Request {
        &mut self.inner
    }

    pub fn into_inner(self) -> coprocessor_pb::Request {
        self.inner
    }
}

impl From<coprocessor_pb::Request> for CoprocessorStreamRequest {
    fn from(inner: coprocessor_pb::Request) -> Self {
        CoprocessorStreamRequest { inner }
    }
}

#[async_trait]
impl Request for CoprocessorStreamRequest {
    async fn dispatch(
        &self,
        client: &TikvClient<Channel>,
        timeout: Duration,
    ) -> Result<Box<dyn Any>> {
        let mut req = self.inner.clone().into_request();
        req.set_timeout(timeout);
        client
            .clone()
            .coprocessor_stream(req)
            .await
            .map(|r| Box::new(r.into_inner()) as Box<dyn Any>)
            .map_err(Error::GrpcAPI)
    }

    fn label(&self) -> &'static str {
        "coprocessor_stream"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_leader(&mut self, leader: &RegionWithLeader) -> Result<()> {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        let leader_peer = leader.leader.as_ref().ok_or(Error::LeaderNotFound {
            region: leader.ver_id(),
        })?;
        ctx.region_id = leader.region.id;
        ctx.region_epoch = leader.region.region_epoch.clone();
        ctx.peer = Some(leader_peer.clone());
        Ok(())
    }

    fn set_api_version(&mut self, api_version: kvrpcpb::ApiVersion) {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        ctx.api_version = api_version.into();
    }

    fn set_is_retry_request(&mut self, is_retry_request: bool) {
        let ctx = self
            .inner
            .context
            .get_or_insert(kvrpcpb::Context::default());
        ctx.is_retry_request = is_retry_request;
    }

    fn context_mut(&mut self) -> Option<&mut kvrpcpb::Context> {
        Some(
            self.inner
                .context
                .get_or_insert(kvrpcpb::Context::default()),
        )
    }
}

fn to_kv_key_range(range: &coprocessor_pb::KeyRange) -> kvrpcpb::KeyRange {
    kvrpcpb::KeyRange {
        start_key: range.start.clone(),
        end_key: range.end.clone(),
    }
}

fn to_coprocessor_key_range(range: kvrpcpb::KeyRange) -> coprocessor_pb::KeyRange {
    coprocessor_pb::KeyRange {
        start: range.start_key,
        end: range.end_key,
    }
}

impl Shardable for coprocessor_pb::Request {
    type Shard = Vec<coprocessor_pb::KeyRange>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut ranges: Vec<kvrpcpb::KeyRange> = self.ranges.iter().map(to_kv_key_range).collect();
        ranges.sort_by(|left, right| {
            left.start_key
                .cmp(&right.start_key)
                .then_with(|| left.end_key.cmp(&right.end_key))
        });
        region_stream_for_ranges(ranges, pd_client.clone())
            .map_ok(|(ranges, region)| {
                let ranges = ranges.into_iter().map(to_coprocessor_key_range).collect();
                (ranges, region)
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.ranges = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
    }
}

impl Shardable for CoprocessorStreamRequest {
    type Shard = Vec<coprocessor_pb::KeyRange>;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        let mut ranges: Vec<kvrpcpb::KeyRange> =
            self.inner.ranges.iter().map(to_kv_key_range).collect();
        ranges.sort_by(|left, right| {
            left.start_key
                .cmp(&right.start_key)
                .then_with(|| left.end_key.cmp(&right.end_key))
        });
        region_stream_for_ranges(ranges, pd_client.clone())
            .map_ok(|(ranges, region)| {
                let ranges = ranges.into_iter().map(to_coprocessor_key_range).collect();
                (ranges, region)
            })
            .boxed()
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.inner.ranges = shard;
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.set_leader(&store.region_with_leader)
    }
}

fn flatten_lock_info(lock: kvrpcpb::LockInfo) -> Vec<kvrpcpb::LockInfo> {
    if lock.shared_lock_infos.is_empty() {
        vec![lock]
    } else {
        lock.shared_lock_infos
    }
}

impl HasLocks for coprocessor_pb::Response {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        let mut locks = Vec::new();

        if let Some(lock) = self.locked.take() {
            locks.extend(flatten_lock_info(lock));
        }

        for response in self.batch_responses.iter_mut() {
            if let Some(lock) = response.locked.take() {
                locks.extend(flatten_lock_info(lock));
            }
        }

        locks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockPdClient;
    use crate::proto::errorpb;
    use crate::store::HasKeyErrors;
    use crate::store::HasRegionError;

    #[tokio::test]
    async fn test_coprocessor_request_shards_split_and_group_by_region() {
        let pd_client = Arc::new(MockPdClient::default());

        let request = coprocessor_pb::Request {
            ranges: vec![
                coprocessor_pb::KeyRange {
                    start: vec![240],
                    end: vec![251],
                },
                coprocessor_pb::KeyRange {
                    start: vec![8],
                    end: vec![12],
                },
                coprocessor_pb::KeyRange {
                    start: vec![1],
                    end: vec![5],
                },
                coprocessor_pb::KeyRange {
                    start: vec![20],
                    end: vec![30],
                },
            ],
            ..Default::default()
        };

        let shards: Vec<(Vec<coprocessor_pb::KeyRange>, RegionWithLeader)> =
            request.shards(&pd_client).try_collect().await.unwrap();

        assert_eq!(shards.len(), 3);

        assert_eq!(shards[0].1.region.id, 1);
        assert_eq!(
            shards[0].0,
            vec![
                coprocessor_pb::KeyRange {
                    start: vec![1],
                    end: vec![5],
                },
                coprocessor_pb::KeyRange {
                    start: vec![8],
                    end: vec![10],
                },
            ]
        );

        assert_eq!(shards[1].1.region.id, 2);
        assert_eq!(
            shards[1].0,
            vec![
                coprocessor_pb::KeyRange {
                    start: vec![10],
                    end: vec![12],
                },
                coprocessor_pb::KeyRange {
                    start: vec![20],
                    end: vec![30],
                },
                coprocessor_pb::KeyRange {
                    start: vec![240],
                    end: vec![250, 250],
                },
            ]
        );

        assert_eq!(shards[2].1.region.id, 3);
        assert_eq!(
            shards[2].0,
            vec![coprocessor_pb::KeyRange {
                start: vec![250, 250],
                end: vec![251],
            }]
        );
    }

    #[tokio::test]
    async fn test_coprocessor_stream_request_shards_split_and_group_by_region() {
        let pd_client = Arc::new(MockPdClient::default());

        let request: CoprocessorStreamRequest = coprocessor_pb::Request {
            ranges: vec![
                coprocessor_pb::KeyRange {
                    start: vec![240],
                    end: vec![251],
                },
                coprocessor_pb::KeyRange {
                    start: vec![8],
                    end: vec![12],
                },
                coprocessor_pb::KeyRange {
                    start: vec![1],
                    end: vec![5],
                },
                coprocessor_pb::KeyRange {
                    start: vec![20],
                    end: vec![30],
                },
            ],
            ..Default::default()
        }
        .into();

        let shards: Vec<(Vec<coprocessor_pb::KeyRange>, RegionWithLeader)> =
            request.shards(&pd_client).try_collect().await.unwrap();

        assert_eq!(shards.len(), 3);
        assert_eq!(shards[0].1.region.id, 1);
        assert_eq!(shards[1].1.region.id, 2);
        assert_eq!(shards[2].1.region.id, 3);
    }

    #[test]
    fn test_coprocessor_stream_request_set_leader_populates_context() {
        let leader = MockPdClient::region1();

        let mut request: CoprocessorStreamRequest = coprocessor_pb::Request::default().into();
        request.set_leader(&leader).unwrap();

        let ctx = request.context_mut().expect("context");
        assert_eq!(ctx.region_id, 1);
        assert!(ctx.region_epoch.is_some());
        assert_eq!(ctx.peer.as_ref().unwrap().store_id, 41);
    }

    #[test]
    fn test_batch_coprocessor_request_set_api_version_populates_context() {
        let mut request = coprocessor_pb::BatchRequest::default();
        request.set_api_version(kvrpcpb::ApiVersion::V2);

        let ctx = request.context_mut().expect("context");
        assert_eq!(ctx.api_version, kvrpcpb::ApiVersion::V2 as i32);
    }

    #[test]
    fn test_coprocessor_response_take_locks_flattens_shared_lock_infos() {
        let mut shared_1 = kvrpcpb::LockInfo::default();
        shared_1.key = b"k1".to_vec();

        let mut shared_2 = kvrpcpb::LockInfo::default();
        shared_2.key = b"k2".to_vec();

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = b"outer".to_vec();
        lock.shared_lock_infos = vec![shared_1.clone(), shared_2.clone()];

        let mut response = coprocessor_pb::Response {
            locked: Some(lock),
            ..Default::default()
        };

        let locks = response.take_locks();
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].key, shared_1.key);
        assert_eq!(locks[1].key, shared_2.key);
        assert!(response.locked.is_none());
    }

    #[test]
    fn test_coprocessor_response_take_locks_includes_batch_responses() {
        let mut top_lock = kvrpcpb::LockInfo::default();
        top_lock.key = b"top".to_vec();

        let mut batch_lock = kvrpcpb::LockInfo::default();
        batch_lock.key = b"batch".to_vec();

        let mut response = coprocessor_pb::Response {
            locked: Some(top_lock),
            batch_responses: vec![coprocessor_pb::StoreBatchTaskResponse {
                locked: Some(batch_lock),
                ..Default::default()
            }],
            ..Default::default()
        };

        let locks = response.take_locks();
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].key, b"top".to_vec());
        assert_eq!(locks[1].key, b"batch".to_vec());
        assert!(response.locked.is_none());
        assert!(response.batch_responses[0].locked.is_none());
    }

    #[test]
    fn test_coprocessor_response_key_errors_includes_batch_responses() {
        let mut response = coprocessor_pb::Response {
            other_error: "top".to_owned(),
            batch_responses: vec![coprocessor_pb::StoreBatchTaskResponse {
                other_error: "batch".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let errors = response.key_errors().expect("errors");
        assert_eq!(errors.len(), 2);
        assert!(matches!(
            errors[0],
            Error::KvError { ref message } if message == "top"
        ));
        assert!(matches!(
            errors[1],
            Error::KvError { ref message } if message == "batch"
        ));
        assert!(response.other_error.is_empty());
        assert!(response.batch_responses[0].other_error.is_empty());
    }

    #[test]
    fn test_coprocessor_response_region_error_includes_batch_responses() {
        let mut response = coprocessor_pb::Response {
            batch_responses: vec![coprocessor_pb::StoreBatchTaskResponse {
                region_error: Some(errorpb::Error {
                    message: "batch".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = response.region_error().expect("region error");
        assert_eq!(err.message, "batch");
        assert!(response.batch_responses[0].region_error.is_none());
    }
}
