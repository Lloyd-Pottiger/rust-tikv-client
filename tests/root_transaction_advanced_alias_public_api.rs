use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use tikv_client::{
    proto::kvrpcpb, transaction, AssertionLevel, BinlogExecutor, BoundLockResolver, CheckLevel,
    DeleteRangeTask, Error, LifecycleHooks, Lock, LockResolver, LockWaitTimeout, PdRpcClient,
    RangeTaskHandler, RangeTaskRunner, RangeTaskStat, ResolveLockDetail, ResolveLocksContext,
    ResolveLocksForReadResult, ResolveLocksOptions, ResolveLocksResult, SchemaVer, SyncTransaction,
    Timestamp, TimestampExt, TransactionClient, TxnStatus,
};

async fn run_on_range_entry(
    runner: &RangeTaskRunner<PdRpcClient>,
    range: std::ops::Range<Vec<u8>>,
) -> tikv_client::Result<()> {
    runner.run_on_range(range).await
}

struct TestRangeTaskHandler;

#[async_trait]
impl RangeTaskHandler for TestRangeTaskHandler {
    async fn handle(
        &self,
        _range: tikv_client::BoundRange,
    ) -> (RangeTaskStat, tikv_client::Result<()>) {
        (RangeTaskStat::default(), Ok(()))
    }
}

fn assert_range_task_handler<T: RangeTaskHandler>() {}

fn set_commit_callback_entry(txn: &mut SyncTransaction) {
    txn.set_commit_callback(|_stage, _err: Option<&Error>| {});
}

fn set_commit_ts_upper_bound_check_entry(txn: &mut SyncTransaction) {
    txn.set_commit_ts_upper_bound_check(|ts| ts > 0);
}

#[test]
fn crate_root_exports_transaction_advanced_alias_types_and_methods() {
    let _: Option<DeleteRangeTask> = None;
    let _: Option<RangeTaskRunner<PdRpcClient>> = None;
    let _: Option<RangeTaskStat> = None;
    let _: Option<Lock> = None;
    let _: Option<LockResolver> = None;
    let _: Option<BoundLockResolver<PdRpcClient>> = None;
    let _: Option<ResolveLocksContext> = None;
    let _: Option<ResolveLocksOptions> = None;
    let _: Option<ResolveLocksResult> = None;
    let _: Option<ResolveLocksForReadResult> = None;
    let _: Option<ResolveLockDetail> = None;
    let _: Option<TxnStatus> = None;

    let schema_ver: SchemaVer = 42;
    assert_eq!(schema_ver, 42);

    assert_eq!(AssertionLevel::default(), AssertionLevel::Off);
    let _ = AssertionLevel::Fast;
    let _ = AssertionLevel::Strict;
    let _ = CheckLevel::Warn;
    let _ = CheckLevel::None;
    let _ = LockWaitTimeout::Default;
    let _ = LockWaitTimeout::NoWait;
    let _ = LockWaitTimeout::AlwaysWait;

    let _: fn(ResolveLocksContext) -> LockResolver = LockResolver::new;
    let _: fn(
        Arc<PdRpcClient>,
        tikv_client::request::Keyspace,
        ResolveLocksContext,
    ) -> BoundLockResolver<PdRpcClient> = BoundLockResolver::new;
    let _: fn(&BoundLockResolver<PdRpcClient>) -> Arc<PdRpcClient> = BoundLockResolver::pd_client;
    let _: fn(&BoundLockResolver<PdRpcClient>) -> tikv_client::request::Keyspace =
        BoundLockResolver::keyspace;
    let _: fn(BoundLockResolver<PdRpcClient>) -> LockResolver = BoundLockResolver::into_inner;

    assert_range_task_handler::<TestRangeTaskHandler>();
    let _ = |pd_client: Arc<PdRpcClient>, handler: TestRangeTaskHandler| {
        RangeTaskRunner::<PdRpcClient>::new("root-transaction", pd_client, 1, handler)
    };
    let _: fn(&mut RangeTaskRunner<PdRpcClient>, usize) -> tikv_client::Result<()> =
        RangeTaskRunner::set_regions_per_task;
    let _: fn(&RangeTaskRunner<PdRpcClient>) -> usize = RangeTaskRunner::completed_regions;
    let _: fn(&RangeTaskRunner<PdRpcClient>) -> usize = RangeTaskRunner::failed_regions;
    let _: fn(&RangeTaskRunner<PdRpcClient>) -> &str = RangeTaskRunner::identifier;
    let _ = run_on_range_entry;

    let _ = |client: TransactionClient, range: std::ops::Range<Vec<u8>>, concurrency| {
        DeleteRangeTask::<PdRpcClient>::new(client, range, concurrency)
    };
    let _ = |client: TransactionClient, range: std::ops::Range<Vec<u8>>, concurrency| {
        DeleteRangeTask::<PdRpcClient>::new_notify(client, range, concurrency)
    };
    let _: fn(&DeleteRangeTask) -> bool = DeleteRangeTask::notify_only;
    let _: fn(&DeleteRangeTask) -> usize = DeleteRangeTask::completed_regions;
}

#[test]
fn crate_root_exports_new_lock_constructor() {
    let _: fn(kvrpcpb::LockInfo) -> Lock = tikv_client::new_lock;

    let proto = kvrpcpb::LockInfo {
        key: b"root-lock".to_vec(),
        primary_lock: b"root-primary".to_vec(),
        lock_version: 7,
        lock_ttl: 9,
        ..Default::default()
    };

    let lock = tikv_client::new_lock(proto.clone());
    assert_eq!(lock.as_proto(), &proto);
}

#[test]
fn crate_root_exports_sync_transaction_commit_control_methods() {
    let _ = set_commit_callback_entry;
    let _: fn(&mut SyncTransaction) = SyncTransaction::clear_commit_callback;
    let _: fn(&mut SyncTransaction, Arc<dyn BinlogExecutor>) = SyncTransaction::set_binlog_executor;
    let _: fn(&mut SyncTransaction) = SyncTransaction::clear_binlog_executor;
    let _: fn(&mut SyncTransaction, LifecycleHooks) =
        SyncTransaction::set_background_task_lifecycle_hooks;
    let _: fn(&mut SyncTransaction) = SyncTransaction::clear_background_task_lifecycle_hooks;
    let _: fn(&mut SyncTransaction, u64) = SyncTransaction::set_commit_wait_until_tso;
    let _: fn(&SyncTransaction) -> u64 = SyncTransaction::commit_wait_until_tso;
    let _: fn(&mut SyncTransaction, Duration) = SyncTransaction::set_commit_wait_until_tso_timeout;
    let _: fn(&SyncTransaction) -> Duration = SyncTransaction::commit_wait_until_tso_timeout;
    let _: fn(&mut SyncTransaction, bool) -> tikv_client::Result<()> =
        SyncTransaction::set_pessimistic;
    let _: fn(&mut SyncTransaction) -> tikv_client::Result<u64> =
        SyncTransaction::get_timestamp_for_commit;
    let _ = set_commit_ts_upper_bound_check_entry;
    let _: fn(&mut SyncTransaction) = SyncTransaction::clear_commit_ts_upper_bound_check;
}

#[test]
fn crate_root_exports_transaction_advanced_alias_values() {
    let wait = LockWaitTimeout::Wait(Duration::from_millis(5));
    assert!(
        matches!(wait, LockWaitTimeout::Wait(duration) if duration == Duration::from_millis(5))
    );

    let proto = kvrpcpb::LockInfo {
        primary_lock: b"pk".to_vec(),
        lock_version: Timestamp::from_version(123).version(),
        key: b"k".to_vec(),
        lock_ttl: 456,
        txn_size: 7,
        lock_type: kvrpcpb::Op::PessimisticLock as i32,
        lock_for_update_ts: 11,
        use_async_commit: true,
        min_commit_ts: 13,
        secondaries: vec![b"s".to_vec()],
        is_txn_file: true,
        ..Default::default()
    };

    let lock = Lock::new(proto.clone());
    assert_eq!(lock.as_proto(), &proto);
    assert_eq!(lock.key(), b"k");
    assert_eq!(lock.primary(), b"pk");
    assert_eq!(lock.txn_id(), Timestamp::from_version(123).version());
    assert_eq!(lock.ttl(), 456);
    assert_eq!(lock.txn_size(), 7);
    assert!(lock.use_async_commit());
    assert_eq!(lock.lock_for_update_ts(), 11);
    assert_eq!(lock.min_commit_ts(), 13);
    assert_eq!(lock.secondaries(), &[b"s".to_vec()]);
    assert!(lock.is_txn_file());
    assert!(lock.is_pessimistic());

    let options = ResolveLocksOptions::default();
    assert!(!options.async_commit_only);
    assert_eq!(options.batch_size, 1024);

    let stat = RangeTaskStat {
        completed_regions: 2,
        failed_regions: 1,
    };
    assert_eq!(stat.completed_regions, 2);
    assert_eq!(stat.failed_regions, 1);

    let rolled_back: TxnStatus = transaction::TransactionStatus {
        kind: transaction::TransactionStatusKind::RolledBack,
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    };
    assert!(rolled_back.is_cacheable());

    let mut locked: TxnStatus = transaction::TransactionStatus {
        kind: transaction::TransactionStatusKind::Locked(
            10,
            kvrpcpb::LockInfo {
                lock_version: tikv_client::oracle::compose_ts(100, 0),
                ..Default::default()
            },
        ),
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    };
    assert!(!locked.is_cacheable());
    locked.check_ttl(Timestamp::from_version(tikv_client::oracle::compose_ts(
        110, 0,
    )));
    assert!(locked.is_expired);
}

#[tokio::test]
async fn crate_root_exports_resolve_locks_context_alias_behaviour() {
    let ctx = ResolveLocksContext::default();
    let rolled_back: Arc<TxnStatus> = Arc::new(transaction::TransactionStatus {
        kind: transaction::TransactionStatusKind::RolledBack,
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    });

    ctx.save_resolved(1, rolled_back.clone()).await;

    let cached = ctx.get_resolved(1).await.expect("cacheable status");
    assert!(Arc::ptr_eq(&cached, &rolled_back));
}
