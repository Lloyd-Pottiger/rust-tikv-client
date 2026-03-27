use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::{
    proto::metapb, Error, KvClient, RegionId, RegionStore, RegionVerId, RegionWithLeader, Result,
    Store, StoreId,
};

#[derive(Clone, Default)]
struct FakeKvClient;

#[async_trait]
impl KvClient for FakeKvClient {
    async fn dispatch(&self, _req: &dyn tikv_client::store::Request) -> Result<Box<dyn Any>> {
        Err(Error::Unimplemented)
    }
}

fn test_region() -> RegionWithLeader {
    RegionWithLeader::new(
        metapb::Region {
            id: 7,
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 2,
                version: 3,
            }),
            ..Default::default()
        },
        Some(metapb::Peer {
            store_id: 11,
            ..Default::default()
        }),
    )
}

fn root_kv_client_trait_object(
    client: Arc<dyn KvClient + Send + Sync>,
) -> Arc<dyn KvClient + Send + Sync> {
    client
}

#[test]
fn root_region_reexports_expose_region_identity_helpers() {
    let region_id: RegionId = 7;
    let store_id: StoreId = 11;
    let region = test_region();

    assert_eq!(region.id(), region_id);
    assert_eq!(region.get_store_id().unwrap(), store_id);
    assert_eq!(
        region.ver_id(),
        RegionVerId {
            id: region_id,
            conf_ver: 2,
            ver: 3,
        }
    );
}

#[test]
fn root_store_reexports_expose_region_store_and_store_constructors() {
    let client: Arc<dyn KvClient + Send + Sync> = Arc::new(FakeKvClient);
    let store_meta = metapb::Store {
        id: 11,
        address: "store-1".to_owned(),
        ..Default::default()
    };

    let region_store = RegionStore::new(test_region(), client.clone(), "store-1".to_owned())
        .with_store_meta(store_meta.clone());
    let store = Store::new(store_meta, root_kv_client_trait_object(client));

    assert_eq!(region_store.region_with_leader.id(), 7);
    assert_eq!(region_store.store_address, "store-1");
    assert_eq!(
        region_store.target_store.as_ref().map(|store| store.id),
        Some(11)
    );
    assert_eq!(store.meta.id, 11);
    let _: Arc<dyn KvClient + Send + Sync> = store.client.clone();
}
