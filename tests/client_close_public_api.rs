use tikv_client::{RawClient, SyncTransactionClient, TransactionClient};

#[test]
fn crate_root_clients_expose_is_closed() {
    let _: fn(&RawClient) -> bool = RawClient::is_closed;
    let _: fn(&TransactionClient) -> bool = TransactionClient::is_closed;
    let _: fn(&SyncTransactionClient) -> bool = SyncTransactionClient::is_closed;
}
