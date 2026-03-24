use std::sync::Arc;

use tikv_client::{PdClient, PdRpcClient, RawClient, SyncTransactionClient, TransactionClient};

fn assert_pd_client_trait<T: PdClient>() {}

#[test]
fn crate_root_exports_pd_client_types() {
    let _: fn(&RawClient) -> Arc<PdRpcClient> = RawClient::pd_client;
    let _: fn(&TransactionClient) -> Arc<PdRpcClient> = TransactionClient::pd_client;
    let _: fn(&SyncTransactionClient) -> Arc<PdRpcClient> = SyncTransactionClient::pd_client;
    assert_pd_client_trait::<PdRpcClient>();
}
