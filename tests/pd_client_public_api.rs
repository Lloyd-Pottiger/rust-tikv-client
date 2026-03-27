use std::sync::Arc;

use tikv_client::{
    PdClient, PdRpcClient, RawClient, RegionCache, SyncTransactionClient, TransactionClient,
};

fn assert_pd_client_trait<T: PdClient>() {}
fn assert_retry_client_trait<T: tikv_client::RetryClientTrait>() {}

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

#[test]
fn crate_root_exports_pd_client_types() {
    let _: fn(&RawClient) -> Arc<PdRpcClient> = RawClient::pd_client;
    let _: fn(&TransactionClient) -> Arc<PdRpcClient> = TransactionClient::pd_client;
    let _: fn(&SyncTransactionClient) -> Arc<PdRpcClient> = SyncTransactionClient::pd_client;
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
