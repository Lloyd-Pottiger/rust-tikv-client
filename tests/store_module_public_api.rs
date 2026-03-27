use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use futures::executor::block_on_stream;
use tikv_client::{
    proto::{keyspacepb, kvrpcpb, metapb},
    store, Key, PdClient, RegionStore, RegionVerId, RegionWithLeader, Result, Store, StoreId,
    Timestamp,
};

#[derive(Clone, Default)]
struct FakeKvClient;

#[async_trait]
impl store::KvClient for FakeKvClient {
    async fn dispatch(&self, _req: &dyn store::Request) -> Result<Box<dyn Any>> {
        Err(tikv_client::Error::Unimplemented)
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

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let key_bytes: Vec<u8> = key.clone().into();
        Ok(region_for_first_byte(
            key_bytes.first().copied().unwrap_or_default(),
        ))
    }

    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        Ok(match id {
            1 => test_region(1, vec![0], vec![50], 101),
            _ => test_region(2, vec![50], vec![], 202),
        })
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Key,
        end_key: Option<Key>,
        limit: i32,
    ) -> Result<Vec<RegionWithLeader>> {
        let start_key_bytes: Vec<u8> = start_key.into();
        let start = start_key_bytes.first().copied().unwrap_or_default();
        let end = end_key
            .map(Into::<Vec<u8>>::into)
            .and_then(|key| key.first().copied())
            .unwrap_or(u8::MAX);
        let regions = [
            test_region(1, vec![0], vec![50], 101),
            test_region(2, vec![50], vec![], 202),
        ];
        Ok(regions
            .into_iter()
            .filter(|region| {
                let region_start: Vec<u8> = region.start_key().into();
                let region_end: Vec<u8> = region.end_key().into();
                let region_start = region_start.first().copied().unwrap_or_default();
                let region_end = region_end.first().copied().unwrap_or(u8::MAX);
                region_end > start && region_start < end
            })
            .take(limit.max(0) as usize)
            .collect())
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

fn test_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>, store_id: u64) -> RegionWithLeader {
    RegionWithLeader::new(
        metapb::Region {
            id,
            start_key,
            end_key,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 2,
                version: 3,
            }),
            ..Default::default()
        },
        Some(metapb::Peer {
            id: store_id + 1,
            store_id,
            ..Default::default()
        }),
    )
}

fn region_for_first_byte(first_byte: u8) -> RegionWithLeader {
    if first_byte < 50 {
        test_region(1, vec![0], vec![50], 101)
    } else {
        test_region(2, vec![50], vec![], 202)
    }
}

#[test]
fn store_module_exports_dispatch_traits() {
    fn assert_request_impl<T: store::Request>() {}
    assert_request_impl::<kvrpcpb::GetRequest>();

    fn assert_has_key_errors_impl<T: store::HasKeyErrors>() {}
    assert_has_key_errors_impl::<kvrpcpb::BatchGetResponse>();
}

#[test]
fn store_module_exports_kv_client_trait() {
    fn assert_kv_client_trait_object(_client: &dyn store::KvClient) {}
    let _: fn(&dyn store::KvClient) = assert_kv_client_trait_object;
}

#[test]
fn store_module_exports_region_store_helpers() {
    let region = test_region(1, vec![0], vec![50], 101);
    let store = RegionStore::new(
        region.clone(),
        Arc::new(FakeKvClient),
        "store-101".to_owned(),
    )
    .with_store_meta(metapb::Store {
        id: 101,
        address: "store-101".to_owned(),
        ..Default::default()
    });

    assert_eq!(store.region_with_leader.id(), 1);
    assert_eq!(store.store_address, "store-101");
    let target_store = store
        .target_store
        .expect("store metadata should be attached");
    assert_eq!(target_store.id, 101);
    assert_eq!(target_store.address, "store-101");
}

#[test]
fn store_module_exports_region_stream_helpers() {
    let pd_client = Arc::new(FakePdClient);

    let key_groups = block_on_stream(store::region_stream_for_keys::<Key, Key, _>(
        vec![
            Key::from(vec![10]),
            Key::from(vec![20]),
            Key::from(vec![70]),
        ]
        .into_iter(),
        pd_client.clone(),
    ))
    .collect::<Vec<_>>();
    assert_eq!(key_groups.len(), 2);
    let (keys, region) = key_groups[0]
        .as_ref()
        .expect("first key group should succeed");
    assert_eq!(keys, &vec![Key::from(vec![10]), Key::from(vec![20])]);
    assert_eq!(region.id(), 1);
    let (keys, region) = key_groups[1]
        .as_ref()
        .expect("second key group should succeed");
    assert_eq!(keys, &vec![Key::from(vec![70])]);
    assert_eq!(region.id(), 2);

    let ranges = block_on_stream(store::region_stream_for_range(
        (vec![10], vec![80]),
        pd_client.clone(),
    ))
    .collect::<Vec<_>>();
    assert_eq!(ranges.len(), 2);
    let ((start, end), region) = ranges[0]
        .as_ref()
        .expect("first range intersection should succeed");
    assert_eq!(start, &vec![10]);
    assert_eq!(end, &vec![50]);
    assert_eq!(region.id(), 1);
    let ((start, end), region) = ranges[1]
        .as_ref()
        .expect("second range intersection should succeed");
    assert_eq!(start, &vec![50]);
    assert_eq!(end, &vec![80]);
    assert_eq!(region.id(), 2);

    let grouped_ranges = block_on_stream(store::region_stream_for_ranges(
        vec![
            kvrpcpb::KeyRange {
                start_key: vec![5],
                end_key: vec![10],
            },
            kvrpcpb::KeyRange {
                start_key: vec![20],
                end_key: vec![30],
            },
            kvrpcpb::KeyRange {
                start_key: vec![70],
                end_key: vec![80],
            },
        ],
        pd_client,
    ))
    .collect::<Vec<_>>();
    assert_eq!(grouped_ranges.len(), 2);
    let (ranges, region) = grouped_ranges[0]
        .as_ref()
        .expect("first grouped range batch should succeed");
    assert_eq!(ranges.len(), 2);
    assert_eq!(region.id(), 1);
    let (ranges, region) = grouped_ranges[1]
        .as_ref()
        .expect("second grouped range batch should succeed");
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].start_key, vec![70]);
    assert_eq!(ranges[0].end_key, vec![80]);
    assert_eq!(region.id(), 2);
}
