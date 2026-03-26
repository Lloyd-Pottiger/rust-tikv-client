use std::sync::Arc;

use tikv_client::{
    PdClient, PdRpcClient, RawClient, RegionCache, SyncTransactionClient, TransactionClient,
};

fn assert_pd_client_trait<T: PdClient>() {}
fn assert_retry_client_trait<T: tikv_client::RetryClientTrait>() {}

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
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::locate_key_range;
    let _ = PdRpcClient::<tikv_client::store::TikvConnect>::batch_locate_key_ranges;
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
