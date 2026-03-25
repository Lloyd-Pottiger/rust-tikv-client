use tikv_client::{KvClient, RegionId, RegionStore, RegionVerId, RegionWithLeader, Store, StoreId};

#[test]
fn crate_root_exports_region_metadata_types() {
    let _: RegionId = 0;
    let _: StoreId = 0;
    let _: RegionVerId = RegionVerId::default();
    let _: RegionWithLeader = RegionWithLeader::default();
}

#[test]
fn crate_root_exports_store_metadata_types() {
    let _: Option<Store> = None;
    let _: Option<RegionStore> = None;

    fn assert_kv_client_trait_object(_client: &dyn KvClient) {}
    let _: fn(&dyn KvClient) = assert_kv_client_trait_object;
}
