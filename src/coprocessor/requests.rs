use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::pd::PdClient;
use crate::proto::coprocessor as coprocessor_pb;
use crate::proto::kvrpcpb;
use crate::region::RegionWithLeader;
use crate::request::KvRequest;
use crate::request::Shardable;
use crate::store::region_stream_for_ranges;
use crate::store::RegionStore;
use crate::store::Request;
use crate::transaction::HasLocks;
use crate::Result;

impl KvRequest for coprocessor_pb::Request {
    type Response = coprocessor_pb::Response;
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

fn flatten_lock_info(lock: kvrpcpb::LockInfo) -> Vec<kvrpcpb::LockInfo> {
    if lock.shared_lock_infos.is_empty() {
        vec![lock]
    } else {
        lock.shared_lock_infos
    }
}

impl HasLocks for coprocessor_pb::Response {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.locked
            .take()
            .into_iter()
            .flat_map(flatten_lock_info)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockPdClient;

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
}
