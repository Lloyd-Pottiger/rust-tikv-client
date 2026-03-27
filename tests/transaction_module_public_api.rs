use std::sync::Arc;
use std::time::Duration;

use tikv_client::{
    proto::kvrpcpb, transaction, CommandPriority, DiskFullOpt, IsolationLevel, Key,
    ReplicaReadType, RetryOptions, Timestamp, TimestampExt, TraceControlFlags,
};

async fn begin_with_txn_scope_entry(
    client: &transaction::Client,
    txn_scope: String,
    options: transaction::TransactionOptions,
) -> tikv_client::Result<transaction::Transaction> {
    client.begin_with_txn_scope(txn_scope, options).await
}

async fn cleanup_locks_entry(
    client: &transaction::Client,
    range: std::ops::Range<Vec<u8>>,
    safepoint: &Timestamp,
    options: transaction::ResolveLocksOptions,
) -> tikv_client::Result<tikv_client::request::plan::CleanupLocksResult> {
    client.cleanup_locks(range, safepoint, options).await
}

async fn scan_locks_entry(
    client: &transaction::Client,
    safepoint: &Timestamp,
    range: std::ops::Range<Vec<u8>>,
    batch_size: u32,
) -> tikv_client::Result<Vec<transaction::ProtoLockInfo>> {
    client.scan_locks(safepoint, range, batch_size).await
}

async fn delete_range_entry(
    client: &transaction::Client,
    range: std::ops::Range<Vec<u8>>,
    concurrency: usize,
) -> tikv_client::Result<usize> {
    client.delete_range(range, concurrency).await
}

async fn notify_delete_range_entry(
    client: &transaction::Client,
    range: std::ops::Range<Vec<u8>>,
    concurrency: usize,
) -> tikv_client::Result<usize> {
    client.notify_delete_range(range, concurrency).await
}

async fn run_on_range_entry(
    runner: &transaction::RangeTaskRunner<tikv_client::PdRpcClient>,
    range: std::ops::Range<Vec<u8>>,
) -> tikv_client::Result<()> {
    runner.run_on_range(range).await
}

fn set_request_source_type_entry(txn: &mut transaction::Transaction, source_type: String) {
    txn.set_request_source_type(source_type);
}

fn set_explicit_request_source_type_entry(txn: &mut transaction::Transaction, explicit: String) {
    txn.set_explicit_request_source_type(explicit);
}

fn is_in_aggressive_locking_stage_entry(txn: &transaction::Transaction, key: &Key) -> bool {
    txn.is_in_aggressive_locking_stage(key)
}

fn set_memory_footprint_change_hook_entry(txn: &mut transaction::Transaction) {
    txn.set_memory_footprint_change_hook(|_| {});
}

async fn get_for_share_entry(
    txn: &mut transaction::Transaction,
    key: Vec<u8>,
) -> tikv_client::Result<Option<tikv_client::Value>> {
    txn.get_for_share(key).await
}

async fn batch_get_for_share_entry(
    txn: &mut transaction::Transaction,
    keys: Vec<Vec<u8>>,
) -> tikv_client::Result<Vec<tikv_client::KvPair>> {
    txn.batch_get_for_share(keys).await
}

async fn lock_keys_with_wait_timeout_entry(
    txn: &mut transaction::Transaction,
    keys: Vec<Vec<u8>>,
    wait_timeout: transaction::LockWaitTimeout,
) -> tikv_client::Result<()> {
    txn.lock_keys_with_wait_timeout(keys, wait_timeout).await
}

async fn lock_keys_in_share_mode_entry(
    txn: &mut transaction::Transaction,
    keys: Vec<Vec<u8>>,
) -> tikv_client::Result<()> {
    txn.lock_keys_in_share_mode(keys).await
}

async fn lock_keys_in_share_mode_with_wait_timeout_entry(
    txn: &mut transaction::Transaction,
    keys: Vec<Vec<u8>>,
    wait_timeout: transaction::LockWaitTimeout,
) -> tikv_client::Result<()> {
    txn.lock_keys_in_share_mode_with_wait_timeout(keys, wait_timeout)
        .await
}

async fn retry_aggressive_locking_entry(
    txn: &mut transaction::Transaction,
) -> tikv_client::Result<()> {
    txn.retry_aggressive_locking().await
}

async fn done_aggressive_locking_entry(
    txn: &mut transaction::Transaction,
) -> tikv_client::Result<()> {
    txn.done_aggressive_locking().await
}

async fn cancel_aggressive_locking_entry(
    txn: &mut transaction::Transaction,
) -> tikv_client::Result<()> {
    txn.cancel_aggressive_locking().await
}

#[test]
fn transaction_module_exports_core_types_and_constants() {
    let _: Option<transaction::Client> = None;
    let _: Option<transaction::Transaction> = None;
    let _: Option<transaction::Snapshot> = None;
    let _: Option<transaction::SyncTransactionClient> = None;
    let _: Option<transaction::SyncTransaction> = None;
    let _: Option<transaction::SyncSnapshot> = None;
    let _: Option<transaction::Buffer> = None;
    let _: Option<transaction::DeleteRangeTask> = None;
    let _: Option<transaction::RangeTaskRunner<tikv_client::PdRpcClient>> = None;
    let _: Option<transaction::RangeTaskStat> = None;
    let _: Option<transaction::TransactionOptions> = None;
    let _: Option<transaction::Lock> = None;
    let _: Option<transaction::LockResolver> = None;
    let _: Option<transaction::BoundLockResolver<tikv_client::PdRpcClient>> = None;
    let _: Option<transaction::ResolveLocksContext> = None;
    let _: Option<transaction::ResolveLocksOptions> = None;
    let _: Option<transaction::ResolveLocksResult> = None;
    let _: Option<transaction::ResolveLocksForReadResult> = None;
    let _: Option<transaction::ResolveLockDetail> = None;
    let _: Option<transaction::ResolvingLock> = None;
    let _: Option<transaction::LifecycleHooks> = None;
    let _: Option<Arc<dyn transaction::SchemaLeaseChecker>> = None;
    let _: Option<Arc<dyn transaction::BinlogExecutor>> = None;
    let _: Option<Arc<dyn transaction::KvFilter>> = None;
    let _: Option<transaction::Mutation> = None;
    let _: Option<transaction::TransactionStatus> = None;
    let _: Option<transaction::TransactionStatusKind> = None;
    let _: Option<transaction::LockProbe> = None;
    let _: Option<transaction::LockResolverProbe<tikv_client::PdRpcClient>> = None;
    let _: transaction::SchemaVer = 42;
    let _: transaction::Variables = transaction::DEFAULT_VARS.clone();
    let _ = transaction::DEF_BACKOFF_LOCK_FAST_MS;
    let _ = transaction::DEF_BACKOFF_WEIGHT;
    let _ = transaction::MAX_TXN_TIME_USE;
    let _ = transaction::txn_commit_batch_size;
    let _ = transaction::global_txn_commit_batch_size;
    let _: fn(u64) = transaction::set_txn_commit_batch_size;
    let _ = transaction::with_txn_commit_batch_size(16, std::future::ready(()));
}

#[test]
fn transaction_module_exports_client_and_runner_entrypoints() {
    let _ = transaction::Client::<tikv_client::PdRpcClient>::new::<String>;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::new_with_config::<String>;
    let _ =
        transaction::Client::<tikv_client::PdRpcClient>::new_with_config_api_v2_no_prefix::<String>;
    let _: fn(&transaction::Client) -> u64 = transaction::Client::cluster_id;
    let _: fn(&transaction::Client) -> &tikv_client::RegionCache =
        transaction::Client::region_cache;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::begin_optimistic;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::begin_pessimistic;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::begin_with_options;
    let _ = begin_with_txn_scope_entry;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::begin_with_start_timestamp;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::lock_resolver;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::bound_lock_resolver;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::snapshot;
    let _ = cleanup_locks_entry;
    let _ = scan_locks_entry;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::resolve_locks;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::resolve_locks_once;
    let _ = transaction::Client::<tikv_client::PdRpcClient>::resolve_locks_for_read;
    let _ = delete_range_entry;
    let _ = notify_delete_range_entry;
    let _ = transaction::SyncTransactionClient::new::<String>;
    let _ = transaction::SyncTransactionClient::new_with_config::<String>;
    let _ = transaction::SyncTransactionClient::begin_optimistic;
    let _ = transaction::SyncTransactionClient::begin_pessimistic;
    let _ = transaction::SyncTransactionClient::begin_with_options;
    let _ = transaction::SyncTransactionClient::begin_with_start_timestamp;
    let _ = transaction::SyncTransactionClient::snapshot;
    let _ = |identifier: String, pd_client: Arc<tikv_client::PdRpcClient>, concurrency: usize| {
        transaction::RangeTaskRunner::<tikv_client::PdRpcClient>::new(
            identifier,
            pd_client,
            concurrency,
            |_range| async {
                (
                    transaction::RangeTaskStat::default(),
                    Ok::<(), tikv_client::Error>(()),
                )
            },
        )
    };
    let _ = transaction::RangeTaskRunner::<tikv_client::PdRpcClient>::set_regions_per_task;
    let _: fn(&transaction::RangeTaskRunner<tikv_client::PdRpcClient>) -> usize =
        transaction::RangeTaskRunner::completed_regions;
    let _: fn(&transaction::RangeTaskRunner<tikv_client::PdRpcClient>) -> usize =
        transaction::RangeTaskRunner::failed_regions;
    let _: fn(&transaction::RangeTaskRunner<tikv_client::PdRpcClient>) -> &str =
        transaction::RangeTaskRunner::identifier;
    let _ = run_on_range_entry;
    let _ = |client: transaction::Client, range: std::ops::Range<Vec<u8>>, concurrency| {
        transaction::DeleteRangeTask::<tikv_client::PdRpcClient>::new(client, range, concurrency)
    };
    let _ = |client: transaction::Client, range: std::ops::Range<Vec<u8>>, concurrency| {
        transaction::DeleteRangeTask::<tikv_client::PdRpcClient>::new_notify(
            client,
            range,
            concurrency,
        )
    };
    let _: fn(&transaction::DeleteRangeTask) -> bool = transaction::DeleteRangeTask::notify_only;
    let _: fn(&transaction::DeleteRangeTask) -> usize =
        transaction::DeleteRangeTask::completed_regions;
}

#[test]
fn transaction_module_exports_option_and_filter_helpers() {
    let optimistic = transaction::TransactionOptions::new_optimistic()
        .txn_scope("bj")
        .read_only()
        .replica_read(ReplicaReadType::PreferLeader)
        .match_store_labels(vec![tikv_client::StoreLabel {
            key: "engine".into(),
            value: "tiflash".into(),
        }])
        .match_store_ids(vec![7, 9])
        .stale_read()
        .not_fill_cache(true)
        .task_id(5)
        .max_execution_duration(Duration::from_secs(1))
        .busy_threshold(Duration::from_millis(5))
        .priority(CommandPriority::High)
        .isolation_level(IsolationLevel::Rc)
        .resource_group_tag(vec![1, 2, 3])
        .resource_group_name("rg")
        .no_resolve_locks()
        .no_resolve_regions()
        .retry_options(RetryOptions::default_optimistic())
        .drop_check(transaction::CheckLevel::Warn)
        .heartbeat_option(transaction::HeartbeatOption::FixedTime(
            Duration::from_secs(2),
        ));
    assert!(!optimistic.is_pessimistic());
    assert!(transaction::HeartbeatOption::FixedTime(Duration::from_secs(2)).is_auto_heartbeat());
    assert!(!transaction::HeartbeatOption::NoHeartbeat.is_auto_heartbeat());

    let pessimistic = transaction::TransactionOptions::new_pessimistic()
        .lock_wait_timeout(transaction::LockWaitTimeout::AlwaysWait)
        .max_write_execution_duration(Duration::from_secs(3))
        .drop_check(transaction::CheckLevel::None);
    assert!(pessimistic.is_pessimistic());

    let hooks = transaction::LifecycleHooks::new()
        .with_pre(|| {})
        .with_post(|| {});
    let _ = hooks;

    let flags = transaction::KvFilterKeyFlags::new(transaction::KvFilterMutationOp::Delete, true);
    assert_eq!(flags.op(), transaction::KvFilterMutationOp::Delete);
    assert!(flags.is_locked());

    let _ = transaction::AssertionLevel::Fast;
    let _ = transaction::PrewriteEncounterLockPolicy::TryResolve;
    let _ = transaction::LockWaitTimeout::NoWait;
    let _ = transaction::KvFilterOp::Insert;

    let key = Key::from(vec![b'a']);
    let put = transaction::Mutation::Put(key.clone(), b"value".to_vec());
    assert_eq!(put.key(), &key);
}

#[test]
fn transaction_module_exports_pipelined_and_request_metadata_option_builders() {
    let defaults = transaction::PipelinedTxnOptions::default();
    assert_eq!(
        defaults.flush_concurrency(),
        transaction::PipelinedTxnOptions::DEFAULT_FLUSH_CONCURRENCY
    );
    assert_eq!(
        defaults.resolve_lock_concurrency(),
        transaction::PipelinedTxnOptions::DEFAULT_RESOLVE_LOCK_CONCURRENCY
    );
    assert_eq!(
        defaults.write_throttle_ratio(),
        transaction::PipelinedTxnOptions::DEFAULT_WRITE_THROTTLE_RATIO
    );

    let custom = transaction::PipelinedTxnOptions::new(16, 4, 0.25).expect("custom pipeline");
    assert_eq!(custom.flush_concurrency(), 16);
    assert_eq!(custom.resolve_lock_concurrency(), 4);
    assert_eq!(custom.write_throttle_ratio(), 0.25);
    assert!(transaction::PipelinedTxnOptions::new(0, 4, 0.25).is_err());
    assert!(transaction::PipelinedTxnOptions::new(16, 0, 0.25).is_err());
    assert!(transaction::PipelinedTxnOptions::new(16, 4, 1.0).is_err());

    let base = transaction::TransactionOptions::new_optimistic();
    assert_eq!(
        base.clone().pipelined(),
        base.clone().pipelined_txn(defaults)
    );
    assert_ne!(base.clone(), base.clone().pipelined());
    assert_ne!(base.clone(), base.clone().pipelined_txn(custom));
    assert_ne!(base.clone(), base.clone().try_one_pc());
    assert_ne!(base.clone(), base.clone().causal_consistency(true));
    assert_ne!(
        base.clone(),
        base.clone()
            .assertion_level(transaction::AssertionLevel::Strict)
    );
    assert_ne!(
        base.clone(),
        base.clone()
            .prewrite_encounter_lock_policy(transaction::PrewriteEncounterLockPolicy::NoResolve,)
    );
    assert_ne!(
        base.clone(),
        base.clone()
            .disk_full_opt(DiskFullOpt::AllowedOnAlreadyFull)
    );
    assert_ne!(base.clone(), base.clone().txn_source(42));
    assert_ne!(base.clone(), base.clone().request_source("cdc"));
    assert_ne!(base.clone(), base.clone().trace_id(vec![9, 8, 7]));

    let flags = TraceControlFlags::default()
        .with(TraceControlFlags::IMMEDIATE_LOG)
        .with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
    assert_ne!(base.clone(), base.clone().trace_control_flags(flags));
    assert_ne!(base.clone(), base.sync_log(true));
}

#[test]
fn transaction_module_exports_transaction_instance_method_entrypoints() {
    let _: fn(&mut transaction::Transaction) =
        transaction::Transaction::clear_replica_read_adjuster;
    let _: fn(&mut transaction::Transaction, bool) =
        transaction::Transaction::set_request_source_internal;
    let _ = set_request_source_type_entry;
    let _ = set_explicit_request_source_type_entry;
    let _: fn(&mut transaction::Transaction) = transaction::Transaction::clear_disk_full_opt;
    let _: fn(&mut transaction::Transaction, u64) = transaction::Transaction::set_session_id;
    let _: fn(&mut transaction::Transaction, Arc<dyn transaction::KvFilter>) =
        transaction::Transaction::set_kv_filter;
    let _: fn(&mut transaction::Transaction) = transaction::Transaction::clear_kv_filter;
    let _: fn(&mut transaction::Transaction) = transaction::Transaction::enable_force_sync_log;
    let _: fn(&mut transaction::Transaction, Duration) =
        transaction::Transaction::set_max_write_execution_duration;
    let _: fn(&mut transaction::Transaction, i64) = transaction::Transaction::set_schema_ver;
    let _: fn(&mut transaction::Transaction) = transaction::Transaction::clear_schema_ver;
    let _: fn(&mut transaction::Transaction, Arc<dyn transaction::SchemaLeaseChecker>) =
        transaction::Transaction::set_schema_lease_checker;
    let _: fn(&mut transaction::Transaction) = transaction::Transaction::clear_schema_lease_checker;
    let _: fn(&mut transaction::Transaction, bool) =
        transaction::Transaction::set_enable_async_commit;
    let _: fn(&mut transaction::Transaction, bool) = transaction::Transaction::set_enable_one_pc;
    let _: fn(&mut transaction::Transaction, bool) =
        transaction::Transaction::set_causal_consistency;
    let _: fn(&transaction::Transaction) -> bool = transaction::Transaction::is_causal_consistency;
    let _: fn(&mut transaction::Transaction, transaction::AssertionLevel) =
        transaction::Transaction::set_assertion_level;
    let _: fn(&mut transaction::Transaction, transaction::PrewriteEncounterLockPolicy) =
        transaction::Transaction::set_prewrite_encounter_lock_policy;
    let _: fn(&transaction::Transaction) -> Option<Timestamp> =
        transaction::Transaction::commit_timestamp;
    let _: fn(&transaction::Transaction) -> bool = transaction::Transaction::is_read_only;
    let _: fn(&mut transaction::Transaction) -> tikv_client::Result<u64> =
        transaction::Transaction::staging;
    let _: fn(&mut transaction::Transaction, u64) -> tikv_client::Result<()> =
        transaction::Transaction::release_staging;
    let _: fn(&mut transaction::Transaction, u64) -> tikv_client::Result<()> =
        transaction::Transaction::cleanup_staging;
    let _ = set_memory_footprint_change_hook_entry;
    let _: fn(&mut transaction::Transaction) =
        transaction::Transaction::clear_memory_footprint_change_hook;
    let _: fn(&transaction::Transaction) -> bool = transaction::Transaction::mem_hook_set;
    let _: fn(&transaction::Transaction) -> bool = transaction::Transaction::is_pipelined;
    let _ = is_in_aggressive_locking_stage_entry;

    let _ = get_for_share_entry;
    let _ = batch_get_for_share_entry;
    let _ = lock_keys_with_wait_timeout_entry;
    let _ = lock_keys_in_share_mode_entry;
    let _ = lock_keys_in_share_mode_with_wait_timeout_entry;
    let _: fn(&mut transaction::Transaction) -> tikv_client::Result<()> =
        transaction::Transaction::start_aggressive_locking;
    let _: fn(&transaction::Transaction) -> bool =
        transaction::Transaction::is_in_aggressive_locking_mode;
    let _ = retry_aggressive_locking_entry;
    let _ = done_aggressive_locking_entry;
    let _ = cancel_aggressive_locking_entry;
}

#[test]
fn transaction_module_exports_lock_helpers() {
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

    let lock = transaction::new_lock(proto.clone());
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

    let options = transaction::ResolveLocksOptions::default();
    assert!(!options.async_commit_only);
    assert_eq!(options.batch_size, 1024);

    let stat = transaction::RangeTaskStat {
        completed_regions: 2,
        failed_regions: 1,
    };
    assert_eq!(stat.completed_regions, 2);
    assert_eq!(stat.failed_regions, 1);
}

#[tokio::test]
async fn transaction_module_resolve_lock_context_caches_only_cacheable_statuses() {
    let ctx = transaction::ResolveLocksContext::default();
    let rolled_back = Arc::new(transaction::TransactionStatus {
        kind: transaction::TransactionStatusKind::RolledBack,
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    });
    ctx.save_resolved(1, rolled_back.clone()).await;
    let cached = ctx.get_resolved(1).await.expect("cacheable status");
    assert!(Arc::ptr_eq(&cached, &rolled_back));

    let mut locked = transaction::TransactionStatus {
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
        111, 0,
    )));
    assert!(locked.is_expired);

    ctx.save_resolved(2, Arc::new(locked)).await;
    assert!(ctx.get_resolved(2).await.is_none());

    let region = tikv_client::RegionVerId {
        id: 8,
        conf_ver: 9,
        ver: 10,
    };
    assert!(!ctx.is_region_cleaned(3, &region).await);
    ctx.save_cleaned_region(3, region.clone()).await;
    assert!(ctx.is_region_cleaned(3, &region).await);
}
