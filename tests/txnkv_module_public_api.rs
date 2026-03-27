use tikv_client::txnkv;

#[test]
fn txnkv_module_exports_types() {
    let _: Option<txnkv::Client> = None;
    let _: Option<txnkv::Lock> = None;
    let _: Option<txnkv::LockResolver> = None;
    let _: Option<txnkv::Snapshot> = None;
    let _: Option<txnkv::Transaction> = None;
    let _: Option<txnkv::TransactionOptions> = None;
    let _: Option<txnkv::TxnStatus> = None;

    let _: Option<txnkv::transaction::Transaction> = None;
    let _: Option<txnkv::transaction::TransactionOptions> = None;
    let _: Option<txnkv::transaction::AssertionLevel> = None;
    let _: Option<txnkv::transaction::LockWaitTimeout> = None;
    let _: Option<txnkv::transaction::PipelinedTxnOptions> = None;
    let _: Option<txnkv::transaction::PrewriteEncounterLockPolicy> = None;

    let _: Option<txnkv::txnsnapshot::Snapshot> = None;
    let _: Option<txnkv::txnsnapshot::SyncSnapshot> = None;

    let _: Option<txnkv::txnlock::LockResolver> = None;
    let _: Option<txnkv::txnlock::Lock> = None;
    let _: Option<txnkv::txnlock::LockProbe> = None;
    let _: Option<txnkv::txnlock::LockResolverProbe<tikv_client::PdRpcClient>> = None;
    let _: Option<txnkv::txnlock::ResolveLockDetail> = None;
    let _: Option<txnkv::txnlock::ResolveLocksContext> = None;
    let _: Option<txnkv::txnlock::ResolveLocksForReadResult> = None;
    let _: Option<txnkv::txnlock::ResolveLocksOptions> = None;
    let _: Option<txnkv::txnlock::ResolveLocksResult> = None;
    let _: Option<txnkv::txnlock::ResolvingLock> = None;
    let _: Option<txnkv::txnlock::TxnStatus> = None;

    let _: Option<txnkv::rangetask::DeleteRangeTask> = None;
    let _: Option<txnkv::rangetask::RangeTaskRunner<tikv_client::PdRpcClient>> = None;
    let _: Option<txnkv::rangetask::RangeTaskStat> = None;
    let _: Option<std::sync::Arc<dyn txnkv::rangetask::RangeTaskHandler>> = None;

    let _: Option<txnkv::txnutil::TxnStatus> = None;

    let _: fn(tikv_client::proto::kvrpcpb::LockInfo) -> txnkv::Lock = txnkv::new_lock;

    let proto = tikv_client::proto::kvrpcpb::LockInfo {
        key: b"txnkv-lock".to_vec(),
        primary_lock: b"txnkv-primary".to_vec(),
        lock_version: 3,
        ..Default::default()
    };
    let lock = txnkv::new_lock(proto.clone());
    assert_eq!(lock.as_proto(), &proto);

    fn assert_lock_resolver_probe_constructor<PdC: tikv_client::PdClient>() {
        let _: fn(tikv_client::BoundLockResolver<PdC>) -> txnkv::txnlock::LockResolverProbe<PdC> =
            txnkv::txnlock::LockResolverProbe::new;
    }

    assert_lock_resolver_probe_constructor::<tikv_client::PdRpcClient>();
}
