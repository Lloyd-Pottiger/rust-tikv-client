use std::sync::Arc;

use tikv_client::{
    PdClient, PdRpcClient, RawClient, RegionCache, SyncTransactionClient, TransactionClient,
};

fn assert_pd_client_trait<T: PdClient>() {}

#[test]
fn crate_root_exports_pd_client_types() {
    let _: fn(&RawClient) -> Arc<PdRpcClient> = RawClient::pd_client;
    let _: fn(&TransactionClient) -> Arc<PdRpcClient> = TransactionClient::pd_client;
    let _: fn(&SyncTransactionClient) -> Arc<PdRpcClient> = SyncTransactionClient::pd_client;
    assert_pd_client_trait::<PdRpcClient>();
}

#[test]
fn crate_root_exports_region_cache_accessors() {
    let _: fn(&PdRpcClient) -> &RegionCache = PdRpcClient::region_cache;
    let _: fn(&TransactionClient) -> &RegionCache = TransactionClient::region_cache;
}

#[test]
fn crate_root_exports_pd_http_accessors() {
    let _: fn(&PdRpcClient) -> Option<&reqwest::Client> = PdRpcClient::pd_http_client;
    let _: fn(&PdRpcClient) -> &[String] = PdRpcClient::pd_http_endpoints;
    let _: fn(&PdRpcClient) -> bool = PdRpcClient::pd_http_uses_https;
    let _: fn(&TransactionClient) -> Option<&reqwest::Client> = TransactionClient::pd_http_client;
    let _: fn(&TransactionClient) -> &[String] = TransactionClient::pd_http_endpoints;
    let _: fn(&TransactionClient) -> bool = TransactionClient::pd_http_uses_https;
}
