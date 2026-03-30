use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::proto::{keyspacepb, metapb};
use tikv_client::store::{self, RegionStore};
use tikv_client::{
    Config, Error, Key, PdClient, PdRpcClient, RawClient, RegionCache, RegionVerId,
    RegionWithLeader, Result, Store, StoreId, SyncTransactionClient, Timestamp, TransactionClient,
};

fn assert_pd_client_trait<T: PdClient>() {}
fn assert_retry_client_trait<T: tikv_client::RetryClientTrait>() {}

#[derive(Clone, Default)]
struct FakeKvClient;

#[async_trait]
impl store::KvClient for FakeKvClient {
    async fn dispatch(&self, _req: &dyn store::Request) -> Result<Box<dyn Any>> {
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
        Ok(test_region(1, 101))
    }

    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        Ok(test_region(id, 101))
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
        11
    }
}

fn test_region(id: u64, store_id: u64) -> RegionWithLeader {
    RegionWithLeader::new(
        metapb::Region {
            id,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
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

async fn update_service_gc_safe_point_entry(
    client: &TransactionClient,
    service_id: &str,
    ttl: i64,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client
        .update_service_gc_safe_point(service_id, ttl, safe_point)
        .await
}

async fn update_service_safe_point_v2_entry(
    client: &TransactionClient,
    keyspace_id: u32,
    service_id: &str,
    ttl: i64,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client
        .update_service_safe_point_v2(keyspace_id, service_id, ttl, safe_point)
        .await
}

async fn update_gc_safe_point_v2_entry(
    client: &TransactionClient,
    keyspace_id: u32,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client
        .update_gc_safe_point_v2(keyspace_id, safe_point)
        .await
}

async fn get_gc_safe_point_entry(client: &TransactionClient) -> tikv_client::Result<u64> {
    client.get_gc_safe_point().await
}

async fn get_gc_safe_point_v2_entry(
    client: &TransactionClient,
    keyspace_id: u32,
) -> tikv_client::Result<u64> {
    client.get_gc_safe_point_v2(keyspace_id).await
}

async fn check_visibility_entry(
    client: &TransactionClient,
    start_ts: u64,
) -> tikv_client::Result<()> {
    client.check_visibility(start_ts).await
}

fn sync_update_service_gc_safe_point_entry(
    client: &SyncTransactionClient,
    service_id: &str,
    ttl: i64,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client.update_service_gc_safe_point(service_id, ttl, safe_point)
}

fn sync_update_service_safe_point_v2_entry(
    client: &SyncTransactionClient,
    keyspace_id: u32,
    service_id: &str,
    ttl: i64,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client.update_service_safe_point_v2(keyspace_id, service_id, ttl, safe_point)
}

fn sync_update_gc_safe_point_v2_entry(
    client: &SyncTransactionClient,
    keyspace_id: u32,
    safe_point: u64,
) -> tikv_client::Result<u64> {
    client.update_gc_safe_point_v2(keyspace_id, safe_point)
}

fn sync_get_gc_safe_point_entry(client: &SyncTransactionClient) -> tikv_client::Result<u64> {
    client.get_gc_safe_point()
}

fn sync_get_gc_safe_point_v2_entry(
    client: &SyncTransactionClient,
    keyspace_id: u32,
) -> tikv_client::Result<u64> {
    client.get_gc_safe_point_v2(keyspace_id)
}

fn sync_check_visibility_entry(
    client: &SyncTransactionClient,
    start_ts: u64,
) -> tikv_client::Result<()> {
    client.check_visibility(start_ts)
}

async fn pd_connect_entry(
    pd_endpoints: &[String],
    config: Config,
    enable_codec: bool,
) -> tikv_client::Result<PdRpcClient> {
    PdRpcClient::connect(pd_endpoints, config, enable_codec).await
}

async fn pd_get_timestamp_with_dc_location_entry(
    client: Arc<PdRpcClient>,
    dc_location: &str,
) -> tikv_client::Result<tikv_client::Timestamp> {
    client.get_timestamp_with_dc_location(dc_location).await
}

#[test]
fn crate_root_exports_pd_client_types() {
    let _: fn(&RawClient) -> Arc<PdRpcClient> = RawClient::pd_client;
    let _: fn(&TransactionClient) -> Arc<PdRpcClient> = TransactionClient::pd_client;
    let _: fn(&SyncTransactionClient) -> Arc<PdRpcClient> = SyncTransactionClient::pd_client;
    let _: fn(&TransactionClient) -> Arc<PdRpcClient> = TransactionClient::get_pd_client;
    let _: fn(&SyncTransactionClient) -> Arc<PdRpcClient> = SyncTransactionClient::get_pd_client;
    assert_pd_client_trait::<PdRpcClient>();
}

#[test]
fn crate_root_exports_retry_client_types() {
    assert_retry_client_trait::<tikv_client::RetryClient>();
}

#[test]
fn crate_root_exports_region_cache_accessors() {
    let _: fn(&PdRpcClient) -> &RegionCache = PdRpcClient::region_cache;
    let _: fn(&TransactionClient) -> &RegionCache = TransactionClient::region_cache;
    let _: fn(&SyncTransactionClient) -> &RegionCache = SyncTransactionClient::region_cache;
    let _: fn(&TransactionClient) -> &RegionCache = TransactionClient::get_region_cache;
    let _: fn(&SyncTransactionClient) -> &RegionCache = SyncTransactionClient::get_region_cache;
}

#[test]
fn crate_root_exports_lock_resolver_accessors() {
    let _: fn(&TransactionClient) -> tikv_client::LockResolver = TransactionClient::lock_resolver;
    let _: fn(&SyncTransactionClient) -> tikv_client::LockResolver =
        SyncTransactionClient::lock_resolver;
    let _: fn(&TransactionClient) -> tikv_client::LockResolver =
        TransactionClient::get_lock_resolver;
    let _: fn(&SyncTransactionClient) -> tikv_client::LockResolver =
        SyncTransactionClient::get_lock_resolver;
}

#[test]
fn crate_root_exports_pd_connection_and_tso_entrypoints() {
    let _ = pd_connect_entry;
    let _ = pd_get_timestamp_with_dc_location_entry;
}

#[test]
fn crate_root_exports_pd_locate_entrypoints() {
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_key;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::try_locate_key;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_end_key;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_region_by_id;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_region_by_id_from_pd;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_key_range;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_key_range_with_opts::<
        [tikv_client::BatchLocateKeyRangesOpt; 1],
    >;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::batch_locate_key_ranges;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::batch_locate_key_ranges_with_opts::<
        [tikv_client::BatchLocateKeyRangesOpt; 1],
    >;
}

#[test]
fn crate_root_exports_batch_locate_option_helpers() {
    let _: tikv_client::BatchLocateKeyRangesOpt = tikv_client::with_need_buckets();
    let _: tikv_client::BatchLocateKeyRangesOpt = tikv_client::with_need_region_has_leader_peer();
}

#[test]
fn crate_root_exports_pd_http_accessors() {
    let _: fn(&PdRpcClient) -> Option<&reqwest::Client> = PdRpcClient::pd_http_client;
    let _: fn(&PdRpcClient) -> &[String] = PdRpcClient::pd_http_endpoints;
    let _: fn(&PdRpcClient) -> bool = PdRpcClient::pd_http_uses_https;
    let _: fn(&TransactionClient) -> Option<&reqwest::Client> = TransactionClient::pd_http_client;
    let _: fn(&TransactionClient) -> &[String] = TransactionClient::pd_http_endpoints;
    let _: fn(&TransactionClient) -> bool = TransactionClient::pd_http_uses_https;
    let _: fn(&SyncTransactionClient) -> Option<&reqwest::Client> =
        SyncTransactionClient::pd_http_client;
    let _: fn(&SyncTransactionClient) -> &[String] = SyncTransactionClient::pd_http_endpoints;
    let _: fn(&SyncTransactionClient) -> bool = SyncTransactionClient::pd_http_uses_https;
}

#[test]
fn crate_root_exports_delete_range_capability_accessors() {
    let _: fn(&TransactionClient) -> bool = TransactionClient::supports_delete_range;
    let _: fn(&SyncTransactionClient) -> bool = SyncTransactionClient::supports_delete_range;
}

#[test]
fn crate_root_exports_gc_safe_point_and_visibility_entrypoints() {
    let _ = update_service_gc_safe_point_entry;
    let _ = update_service_safe_point_v2_entry;
    let _ = update_gc_safe_point_v2_entry;
    let _ = get_gc_safe_point_entry;
    let _ = get_gc_safe_point_v2_entry;
    let _ = check_visibility_entry;

    let _ = sync_update_service_gc_safe_point_entry;
    let _ = sync_update_service_safe_point_v2_entry;
    let _ = sync_update_gc_safe_point_v2_entry;
    let _ = sync_get_gc_safe_point_entry;
    let _ = sync_get_gc_safe_point_v2_entry;
    let _ = sync_check_visibility_entry;
}

#[tokio::test]
async fn pd_client_trait_exposes_doc_hidden_cache_hooks() {
    let client = FakePdClient;

    client.add_region_to_cache(test_region(7, 707)).await;
    client
        .on_bucket_version_not_match(RegionVerId::default(), 88, vec![b"bucket".to_vec()])
        .await;

    assert_eq!(client.buckets_version(RegionVerId::default()).await, 0);
}
