use std::sync::Arc;

use async_trait::async_trait;

use tikv_client::{
    BinlogExecutor, BinlogWriteResult, KvFilter, KvFilterOp, LifecycleHooks, PipelinedTxnOptions,
    PrewriteEncounterLockPolicy, ResolvingLock, SchemaLeaseChecker, Timestamp,
};

struct TestSchemaLeaseChecker;

impl SchemaLeaseChecker for TestSchemaLeaseChecker {
    fn check_by_schema_ver(
        &self,
        _txn_ts: Timestamp,
        _start_schema_ver: i64,
    ) -> tikv_client::Result<()> {
        Ok(())
    }
}

struct TestKvFilter;

impl KvFilter for TestKvFilter {
    fn is_unnecessary_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        op: KvFilterOp,
    ) -> tikv_client::Result<bool> {
        Ok(key.is_empty() && value.is_empty() && matches!(op, KvFilterOp::Delete))
    }
}

struct TestBinlogExecutor;

#[async_trait]
impl BinlogExecutor for TestBinlogExecutor {
    async fn prewrite(&self, _primary: Vec<u8>) -> tikv_client::Result<BinlogWriteResult> {
        Ok(BinlogWriteResult::new(false))
    }

    async fn commit(&self, _commit_ts: u64) -> tikv_client::Result<()> {
        Ok(())
    }

    async fn skip(&self) -> tikv_client::Result<()> {
        Ok(())
    }
}

#[test]
fn crate_root_exports_transaction_hook_filter_alias_types() {
    let _: Option<LifecycleHooks> = None;
    let _: Option<Arc<dyn BinlogExecutor>> = None;
    let _: Option<Arc<dyn KvFilter>> = None;
    let _: Option<Arc<dyn SchemaLeaseChecker>> = None;
    let _: Option<PipelinedTxnOptions> = None;
    let _: Option<ResolvingLock> = None;

    let _executor: Arc<dyn BinlogExecutor> = Arc::new(TestBinlogExecutor);
    let _filter: Arc<dyn KvFilter> = Arc::new(TestKvFilter);
    let _checker: Arc<dyn SchemaLeaseChecker> = Arc::new(TestSchemaLeaseChecker);

    let _ = LifecycleHooks::new().with_pre(|| {}).with_post(|| {});
    let _ = KvFilterOp::Put;
    let _ = KvFilterOp::Insert;
    let _ = PrewriteEncounterLockPolicy::TryResolve;
    let _ = PrewriteEncounterLockPolicy::NoResolve;
}

#[test]
fn crate_root_exports_transaction_hook_filter_alias_values() {
    let result = BinlogWriteResult::new(true);
    assert!(result.skipped());
    assert!(!BinlogWriteResult::new(false).skipped());

    let filter = TestKvFilter;
    assert!(filter
        .is_unnecessary_key_value(b"", b"", KvFilterOp::Delete)
        .expect("filter should succeed"));
    assert!(!filter
        .is_unnecessary_key_value(b"k", b"v", KvFilterOp::Put)
        .expect("filter should succeed"));

    let options = PipelinedTxnOptions::new(2, 3, 0.5).expect("valid options");
    assert_eq!(options.flush_concurrency(), 2);
    assert_eq!(options.resolve_lock_concurrency(), 3);
    assert_eq!(options.write_throttle_ratio(), 0.5);

    let resolving = ResolvingLock {
        txn_id: 11,
        lock_txn_id: 13,
        key: b"key".to_vec(),
        primary: b"primary".to_vec(),
    };
    assert_eq!(resolving.txn_id, 11);
    assert_eq!(resolving.lock_txn_id, 13);
    assert_eq!(resolving.key, b"key".to_vec());
    assert_eq!(resolving.primary, b"primary".to_vec());

    let checker = TestSchemaLeaseChecker;
    checker
        .check_by_schema_ver(Timestamp::default(), 42)
        .expect("checker should accept the schema version");
}

#[tokio::test]
async fn crate_root_exports_binlog_executor_alias_methods() {
    let executor = TestBinlogExecutor;
    let result = executor
        .prewrite(b"primary".to_vec())
        .await
        .expect("prewrite should succeed");
    assert!(!result.skipped());
    executor.commit(123).await.expect("commit should succeed");
    executor.skip().await.expect("skip should succeed");
}
