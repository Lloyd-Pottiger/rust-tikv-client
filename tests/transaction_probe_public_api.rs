use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::proto::{keyspacepb, kvrpcpb, metapb};
use tikv_client::request::Keyspace;
use tikv_client::store::{self, RegionStore};
use tikv_client::transaction::{LockProbe, LockResolverProbe, TransactionStatusKind};
use tikv_client::{
    BoundLockResolver, Error, Key, Lock, PdClient, RegionVerId, RegionWithLeader,
    ResolveLocksContext, Result, Store, StoreId, Timestamp, TxnStatus,
};

#[derive(Clone, Default)]
struct FakeKvClient;

#[async_trait]
impl store::KvClient for FakeKvClient {
    async fn dispatch(&self, _req: &dyn store::Request) -> Result<Box<dyn Any>> {
        Err(Error::Unimplemented)
    }
}

#[derive(Default)]
struct FakePdClient;

#[async_trait]
impl PdClient for FakePdClient {
    type KvClient = FakeKvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(
            region,
            Arc::new(FakeKvClient),
            "fake-store".to_owned(),
        ))
    }

    async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        Ok(test_region(1, vec![], vec![], 101))
    }

    async fn region_for_id(&self, id: u64) -> Result<RegionWithLeader> {
        Ok(test_region(id, vec![], vec![], 101))
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        Ok(safepoint)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Ok(keyspacepb::KeyspaceMeta {
            name: keyspace.to_owned(),
            ..Default::default()
        })
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![])
    }

    async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: StoreId) {}

    fn cluster_id(&self) -> u64 {
        7
    }
}

fn test_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>, store_id: u64) -> RegionWithLeader {
    RegionWithLeader::new(
        metapb::Region {
            id,
            start_key,
            end_key,
            region_epoch: Some(metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            ..Default::default()
        },
        Some(metapb::Peer {
            id: store_id + 1,
            store_id,
            ..Default::default()
        }),
    )
}

fn sample_lock() -> Lock {
    Lock::new(kvrpcpb::LockInfo {
        primary_lock: b"pk".to_vec(),
        lock_version: 42,
        key: b"k".to_vec(),
        lock_ttl: 88,
        secondaries: vec![b"s1".to_vec(), b"s2".to_vec()],
        lock_type: kvrpcpb::Op::PessimisticLock as i32,
        min_commit_ts: 64,
        ..Default::default()
    })
}

fn make_probe() -> LockResolverProbe<FakePdClient> {
    let resolver = BoundLockResolver::new(
        Arc::new(FakePdClient),
        Keyspace::Disable,
        ResolveLocksContext::default(),
    );
    LockResolverProbe::new(resolver)
}

async fn get_txn_status_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Option<Lock>,
) -> Result<TxnStatus> {
    probe
        .get_txn_status(42, b"pk".to_vec(), 7, 9, false, false, lock)
        .await
}

async fn get_txn_status_from_lock_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Lock,
) -> Result<TxnStatus> {
    probe.get_txn_status_from_lock(lock, 7, false).await
}

async fn check_all_secondaries_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Lock,
    status: &TxnStatus,
) -> Result<()> {
    probe.check_all_secondaries(lock, status).await
}

async fn resolve_async_commit_lock_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Lock,
    status: TxnStatus,
) -> Result<()> {
    probe.resolve_async_commit_lock(lock, status).await
}

async fn resolve_lock_entry(probe: &LockResolverProbe<FakePdClient>, lock: Lock) -> Result<()> {
    probe.resolve_lock(lock).await
}

async fn resolve_pessimistic_lock_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Lock,
) -> Result<()> {
    probe.resolve_pessimistic_lock(lock).await
}

async fn force_resolve_lock_entry(
    probe: &LockResolverProbe<FakePdClient>,
    lock: Lock,
) -> Result<()> {
    probe.force_resolve_lock(lock).await
}

async fn set_resolving_entry(probe: &LockResolverProbe<FakePdClient>, locks: Vec<Lock>) {
    probe.set_resolving(7, locks).await;
}

#[test]
fn transaction_probe_public_api_exposes_lock_probe_helpers() {
    let status = LockProbe.new_lock_status(vec![b"s1".to_vec(), b"s2".to_vec()], true, 88);
    match &status.kind {
        TransactionStatusKind::Locked(ttl, lock) => {
            assert_eq!(*ttl, 1);
            assert_eq!(lock.secondaries, vec![b"s1".to_vec(), b"s2".to_vec()]);
            assert!(lock.use_async_commit);
            assert_eq!(lock.min_commit_ts, 88);
        }
        other => panic!("expected locked status, got {other:?}"),
    }

    let mut proto = kvrpcpb::LockInfo::default();
    proto.key = b"pk".to_vec();
    proto.secondaries = vec![b"x".to_vec()];
    let locked = TxnStatus {
        kind: TransactionStatusKind::Locked(5, proto),
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    };
    assert_eq!(
        LockProbe.primary_key_from_txn_status(&locked),
        b"pk".to_vec()
    );
    assert_eq!(
        LockProbe.secondaries_from_txn_status(&locked),
        vec![b"x".to_vec()]
    );

    let committed = TxnStatus {
        kind: TransactionStatusKind::Committed(Timestamp::default().into()),
        action: kvrpcpb::Action::NoAction,
        is_expired: false,
    };
    assert!(LockProbe.primary_key_from_txn_status(&committed).is_empty());
    assert!(LockProbe.secondaries_from_txn_status(&committed).is_empty());
}

#[test]
fn transaction_probe_public_api_exposes_lock_resolver_probe_helpers() {
    let resolver = BoundLockResolver::new(
        Arc::new(FakePdClient),
        Keyspace::Disable,
        ResolveLocksContext::default(),
    );
    let probe = LockResolverProbe::new(resolver);
    let _: BoundLockResolver<FakePdClient> = probe.into_inner();

    let mut probe = make_probe();
    probe.set_meet_lock_callback(|locks| {
        let _ = locks.len();
    });
    probe.clear_meet_lock_callback();

    assert!(probe.is_error_not_found(&Error::TxnNotFound(kvrpcpb::TxnNotFound::default(),)));
    assert!(!probe.is_error_not_found(&Error::Unimplemented));

    assert!(probe.is_non_async_commit_lock(&Error::InternalError {
        message: "check_secondary_locks receives a non-async-commit lock".to_owned(),
    }));
    assert!(!probe.is_non_async_commit_lock(&Error::Unimplemented));
}

#[test]
fn transaction_probe_public_api_exposes_async_method_signatures() {
    let _ = get_txn_status_entry;
    let _ = get_txn_status_from_lock_entry;
    let _ = check_all_secondaries_entry;
    let _ = resolve_async_commit_lock_entry;
    let _ = resolve_lock_entry;
    let _ = resolve_pessimistic_lock_entry;
    let _ = force_resolve_lock_entry;
    let _ = set_resolving_entry;

    let _ = sample_lock();
}
