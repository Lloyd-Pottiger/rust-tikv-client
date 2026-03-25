use tikv_client::txnkv;

#[test]
fn txnkv_module_exports_types() {
    let _: Option<txnkv::Client> = None;
    let _: Option<txnkv::Snapshot> = None;
    let _: Option<txnkv::Transaction> = None;
    let _: Option<txnkv::TransactionOptions> = None;
    let _: Option<txnkv::TxnStatus> = None;
}
