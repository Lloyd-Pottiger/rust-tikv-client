use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tikv_client::proto::keyspacepb;
use tikv_client::proto::metapb;
use tikv_client::proto::pdpb;
use tikv_client::{
    BucketLocation, Error, Key, KeyLocation, RegionCache, RegionVerId, RegionWithLeader, Result,
    RetryClientTrait,
};

fn region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
    RegionWithLeader {
        region: metapb::Region {
            id,
            start_key,
            end_key,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            ..Default::default()
        },
        leader: None,
    }
}

#[tokio::test]
async fn region_cache_exports_buckets_query_api() {
    struct DummyClient;

    let cache = RegionCache::new_with_ttl(Arc::new(DummyClient), Duration::ZERO, Duration::ZERO);
    let buckets = cache.get_buckets_by_ver_id(&RegionVerId::default()).await;
    assert!(buckets.is_none());
}

#[test]
fn crate_root_exports_bucket_location_and_key_location_types() {
    let location = KeyLocation {
        region: RegionVerId {
            id: 7,
            conf_ver: 11,
            ver: 13,
        },
        start_key: Key::from(vec![1]),
        end_key: Key::from(vec![9]),
        buckets: Some(Arc::new(metapb::Buckets {
            region_id: 7,
            version: 5,
            keys: vec![vec![3], vec![6]],
            ..Default::default()
        })),
    };

    assert!(location.contains(&Key::from(vec![4])));
    assert_eq!(location.bucket_version(), 5);
    assert_eq!(
        location.locate_bucket(&Key::from(vec![2])),
        Some(BucketLocation {
            start_key: vec![1].into(),
            end_key: vec![3].into(),
        })
    );
    assert_eq!(
        location.locate_bucket(&Key::from(vec![8])),
        Some(BucketLocation {
            start_key: vec![6].into(),
            end_key: vec![9].into(),
        })
    );
}

#[tokio::test]
async fn region_cache_exports_locate_range_apis() -> Result<()> {
    struct DummyClient;

    #[async_trait]
    impl RetryClientTrait for DummyClient {
        async fn get_region(self: Arc<Self>, _key: Vec<u8>) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_store(self: Arc<Self>, _id: u64) -> Result<metapb::Store> {
            Err(Error::Unimplemented)
        }

        async fn get_region_by_id(self: Arc<Self>, _region_id: u64) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    let cache = RegionCache::new_with_ttl(Arc::new(DummyClient), Duration::ZERO, Duration::ZERO);
    cache.add_region(region(1, vec![], vec![10])).await;
    cache.add_region(region(2, vec![10], vec![20])).await;
    cache.add_region(region(3, vec![20], vec![])).await;

    let locations = cache
        .locate_key_range(Key::from(vec![2]), Key::from(vec![18]))
        .await?;
    assert_eq!(locations.len(), 2);
    assert_eq!(locations[0].region.id, 1);
    assert_eq!(locations[1].region.id, 2);

    let locations = cache
        .batch_locate_key_ranges(vec![
            tikv_client::tikv::KeyRange::new(vec![1], vec![3]),
            tikv_client::tikv::KeyRange::new(vec![3], vec![8]),
            tikv_client::tikv::KeyRange::new(vec![12], vec![18]),
        ])
        .await?;
    assert_eq!(locations.len(), 2);
    assert_eq!(locations[0].region.id, 1);
    assert_eq!(locations[1].region.id, 2);

    Ok(())
}
