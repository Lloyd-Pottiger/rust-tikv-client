use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tikv_client::backoff::{
    Backoff, DEFAULT_REGION_BACKOFF, OPTIMISTIC_BACKOFF, PESSIMISTIC_BACKOFF,
};
use tikv_client::proto::{keyspacepb, kvrpcpb, metapb};
use tikv_client::request::{
    self, EncodeKeyspace, HasNextBatch, KeyMode, Keyspace, Merge, NextBatch, RangeRequest,
    RetryOptions, Shardable, TruncateKeyspace,
};
use tikv_client::store::{KvClient, RegionStore, Request, Store};
use tikv_client::{
    Error, Key, PdClient, RegionVerId, RegionWithLeader, Result, StoreId, Timestamp,
};

fn assert_kv_request<T: request::KvRequest>() {}
fn assert_single_key<T: request::SingleKey>() {}
fn assert_shardable<T: request::Shardable>() {}
fn assert_batchable<T: request::Batchable>() {}
fn assert_range_request<T: request::RangeRequest>() {}
fn assert_next_batch<T: request::NextBatch>() {}
fn assert_has_next_batch<T: request::HasNextBatch>() {}
fn assert_store_request<T: request::StoreRequest>() {}
fn assert_plan<T: request::Plan>() {}

macro_rules! impl_test_request {
    ($type_:ty, $label:literal) => {
        #[async_trait]
        impl Request for $type_ {
            async fn dispatch(
                &self,
                _client: &tikv_client::proto::tikvpb::tikv_client::TikvClient<
                    tonic::transport::Channel,
                >,
                _timeout: Duration,
            ) -> Result<Box<dyn Any>> {
                Err(Error::Unimplemented)
            }

            fn label(&self) -> &'static str {
                $label
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn set_leader(&mut self, _leader: &RegionWithLeader) -> Result<()> {
                Ok(())
            }

            fn set_api_version(&mut self, _api_version: kvrpcpb::ApiVersion) {}

            fn set_is_retry_request(&mut self, _is_retry_request: bool) {}
        }
    };
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MacroCollectResponse(u8);

tikv_client::collect_single!(MacroCollectResponse);

#[derive(Clone, Default)]
struct MacroShardableKeyRequest {
    key: Vec<u8>,
}
impl_test_request!(MacroShardableKeyRequest, "macro_key");
tikv_client::shardable_key!(MacroShardableKeyRequest);

#[derive(Clone, Default)]
struct MacroShardableKeysRequest {
    keys: Vec<Vec<u8>>,
}
impl_test_request!(MacroShardableKeysRequest, "macro_keys");
tikv_client::shardable_keys!(MacroShardableKeysRequest);

#[derive(Clone, Default)]
struct MacroRangeRequest {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}
impl_test_request!(MacroRangeRequest, "macro_range");
tikv_client::range_request!(MacroRangeRequest);
tikv_client::shardable_range!(MacroRangeRequest);

#[derive(Clone, Default)]
struct MacroReverseRangeRequest {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    reverse: bool,
}
impl_test_request!(MacroReverseRangeRequest, "macro_reverse_range");
tikv_client::reversible_range_request!(MacroReverseRangeRequest);
tikv_client::shardable_range!(MacroReverseRangeRequest);

#[derive(Clone, Default)]
struct FakeKvClient;

#[async_trait]
impl KvClient for FakeKvClient {
    async fn dispatch(&self, _req: &dyn Request) -> Result<Box<dyn Any>> {
        Err(Error::Unimplemented)
    }
}

#[derive(Default)]
struct FakePdClient;

#[async_trait]
impl PdClient for FakePdClient {
    type KvClient = FakeKvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(
            region,
            Arc::new(FakeKvClient),
            "fake-store".to_owned(),
        ))
    }

    async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        Ok(test_region())
    }

    async fn region_for_id(&self, _id: u64) -> Result<RegionWithLeader> {
        Ok(test_region())
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        Ok(safepoint)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Ok(keyspacepb::KeyspaceMeta {
            name: keyspace.to_owned(),
            ..Default::default()
        })
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![])
    }

    async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: StoreId) {}

    fn cluster_id(&self) -> u64 {
        4242
    }
}

fn test_region() -> RegionWithLeader {
    RegionWithLeader::new(
        metapb::Region {
            id: 7,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 3,
                version: 5,
            }),
            ..Default::default()
        },
        Some(metapb::Peer {
            id: 11,
            store_id: 29,
            ..Default::default()
        }),
    )
}

#[test]
fn request_public_api_exposes_retry_options_and_keyspace_helpers() {
    let _: fn() -> RetryOptions = RetryOptions::default_optimistic;
    let _: fn() -> RetryOptions = RetryOptions::default_pessimistic;
    let _: fn() -> RetryOptions = RetryOptions::none;

    assert_eq!(
        RetryOptions::default_optimistic(),
        RetryOptions::new(DEFAULT_REGION_BACKOFF, OPTIMISTIC_BACKOFF)
    );
    assert_eq!(
        RetryOptions::default_pessimistic(),
        RetryOptions::new(DEFAULT_REGION_BACKOFF, PESSIMISTIC_BACKOFF)
    );
    assert_eq!(
        RetryOptions::none(),
        RetryOptions::new(Backoff::no_backoff(), Backoff::no_backoff())
    );

    assert_eq!(Keyspace::Disable.api_version(), kvrpcpb::ApiVersion::V1);
    assert_eq!(
        Keyspace::Enable { keyspace_id: 9 }.api_version(),
        kvrpcpb::ApiVersion::V2
    );
    assert_eq!(
        Keyspace::ApiV2NoPrefix.api_version(),
        kvrpcpb::ApiVersion::V2
    );

    let keyspace = Keyspace::Enable { keyspace_id: 9 };
    let key = Key::from(vec![1, 2, 3]);
    let encoded = key.clone().encode_keyspace(keyspace, KeyMode::Txn);
    assert_ne!(encoded, key);
    assert_eq!(encoded.truncate_keyspace(keyspace), key);
    assert_eq!(
        key.clone().encode_keyspace(Keyspace::Disable, KeyMode::Raw),
        key
    );
}

#[test]
fn request_public_api_exposes_trait_surface_for_known_requests() {
    assert_kv_request::<kvrpcpb::RawGetRequest>();
    assert_kv_request::<kvrpcpb::RawBatchPutRequest>();
    assert_kv_request::<kvrpcpb::RawScanRequest>();
    assert_kv_request::<kvrpcpb::ScanLockRequest>();
    assert_single_key::<kvrpcpb::RawGetRequest>();
    assert_shardable::<kvrpcpb::RawGetRequest>();
    assert_batchable::<kvrpcpb::RawBatchPutRequest>();
    assert_range_request::<kvrpcpb::RawScanRequest>();
    assert_next_batch::<kvrpcpb::ScanLockRequest>();
    assert_has_next_batch::<kvrpcpb::ScanLockResponse>();
    assert_store_request::<kvrpcpb::CompactRequest>();
    assert_plan::<request::Dispatch<kvrpcpb::RawGetRequest>>();

    let scan = kvrpcpb::RawScanRequest::default();
    assert!(!RangeRequest::is_reverse(&scan));

    let mut next_batch = kvrpcpb::ScanLockRequest {
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        ..Default::default()
    };
    NextBatch::next_batch(&mut next_batch, (b"m".to_vec(), b"z".to_vec()));
    assert_eq!(next_batch.start_key, b"m".to_vec());
    assert_eq!(next_batch.end_key, b"z".to_vec());

    let has_next = kvrpcpb::ScanLockResponse {
        locks: vec![kvrpcpb::LockInfo {
            key: b"k".to_vec(),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        HasNextBatch::has_next_batch(&has_next),
        Some((b"k\0".to_vec(), vec![]))
    );
}

#[tokio::test]
async fn request_public_api_exposes_plan_builder_entrypoint() {
    let builder = request::PlanBuilder::new(
        Arc::new(FakePdClient),
        Keyspace::Enable { keyspace_id: 0 },
        kvrpcpb::GetRequest::default(),
    );
    let plan = builder
        .single_region_with_store(RegionStore::new(
            test_region(),
            Arc::new(FakeKvClient),
            "fake-store".to_owned(),
        ))
        .await
        .expect("single_region_with_store should succeed")
        .plan();

    let context = plan.request.context().expect("context should be present");
    assert_eq!(context.cluster_id, 4242);
    assert_eq!(context.keyspace_id, 0);
    assert_eq!(context.keyspace_name, "DEFAULT");
    assert!(plan.kv_client.is_some());
}

#[test]
fn request_public_api_exports_macros_with_basic_semantics() {
    let merged = request::CollectSingle
        .merge(vec![Ok(MacroCollectResponse(7))])
        .expect("collect_single should extract the only item");
    assert_eq!(merged, MacroCollectResponse(7));

    let mut key_request = MacroShardableKeyRequest {
        key: b"old".to_vec(),
    };
    key_request.apply_shard(vec![b"new".to_vec()]);
    assert_eq!(key_request.key, b"new".to_vec());
    assert_shardable::<MacroShardableKeyRequest>();

    let mut keys_request = MacroShardableKeysRequest {
        keys: vec![b"a".to_vec()],
    };
    keys_request.apply_shard(vec![b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(keys_request.keys, vec![b"b".to_vec(), b"c".to_vec()]);
    assert_shardable::<MacroShardableKeysRequest>();

    let mut range_request = MacroRangeRequest {
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
    };
    range_request.apply_shard((b"c".to_vec(), b"m".to_vec()));
    assert_eq!(range_request.start_key, b"c".to_vec());
    assert_eq!(range_request.end_key, b"m".to_vec());
    assert!(!RangeRequest::is_reverse(&range_request));
    assert_shardable::<MacroRangeRequest>();

    let mut reverse_range_request = MacroReverseRangeRequest {
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        reverse: true,
    };
    reverse_range_request.apply_shard((b"c".to_vec(), b"m".to_vec()));
    assert_eq!(reverse_range_request.start_key, b"m".to_vec());
    assert_eq!(reverse_range_request.end_key, b"c".to_vec());
    assert!(RangeRequest::is_reverse(&reverse_range_request));
    assert_shardable::<MacroReverseRangeRequest>();
}
