// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Various mock versions of the various clients and other objects.
//!
//! The goal is to be able to test functionality independently of the rest of
//! the system, in particular without requiring a TiKV or PD server, or RPC layer.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use derive_new::new;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::kvrpcpb;
use crate::proto::metapb::RegionEpoch;
use crate::proto::metapb::{self};
use crate::proto::pdpb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::{KvClient, Store};
use crate::timestamp::TimestampExt;
use crate::Config;
use crate::Error;
use crate::Key;
use crate::Result;
use crate::Timestamp;

/// Create a `PdRpcClient` with it's internals replaced with mocks so that the
/// client can be tested without doing any RPC calls.
pub async fn pd_rpc_client() -> PdRpcClient<MockKvConnect, MockCluster> {
    let config = Config::default();
    PdRpcClient::new(
        config.clone(),
        |_| MockKvConnect,
        |sm| {
            futures::future::ok(RetryClient::new_with_cluster(
                sm,
                config.timeout,
                0,
                MockCluster,
            ))
        },
        false,
    )
    .await
    .unwrap()
}

#[allow(clippy::type_complexity)]
#[derive(new, Default, Clone)]
pub struct MockKvClient {
    pub addr: String,
    dispatch: Option<Arc<dyn Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static>>,
}

impl MockKvClient {
    pub fn with_dispatch_hook<F>(dispatch: F) -> MockKvClient
    where
        F: Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    {
        MockKvClient {
            addr: String::new(),
            dispatch: Some(Arc::new(dispatch)),
        }
    }
}

pub struct MockKvConnect;

pub struct MockCluster;

#[derive(new)]
pub struct MockPdClient {
    client: MockKvClient,
    #[new(value = "RwLock::new(HashMap::new())")]
    store_clients: RwLock<HashMap<StoreId, MockKvClient>>,
    #[new(value = "RwLock::new(HashMap::new())")]
    store_metas: RwLock<HashMap<StoreId, metapb::Store>>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    store_meta_by_id_calls: Arc<AtomicUsize>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    get_timestamp_calls: Arc<AtomicUsize>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    get_min_ts_calls: Arc<AtomicUsize>,
    #[new(value = "Mutex::new(Vec::new())")]
    get_timestamp_dc_locations: Mutex<Vec<String>>,
    #[new(value = "Mutex::new(HashMap::new())")]
    slow_store_until: Mutex<HashMap<StoreId, Instant>>,
    #[new(value = "Mutex::new(HashMap::new())")]
    store_estimated_wait_until: Mutex<HashMap<StoreId, Instant>>,
    #[new(value = "Mutex::new(Vec::new())")]
    invalidated_region_ver_ids: Mutex<Vec<RegionVerId>>,
    #[new(value = "Mutex::new(Vec::new())")]
    invalidated_store_ids: Mutex<Vec<StoreId>>,
    #[new(value = "Mutex::new(Vec::new())")]
    added_regions_to_cache: Mutex<Vec<RegionWithLeader>>,
    #[new(value = "Arc::new(AtomicU64::new(0))")]
    tso_version: Arc<AtomicU64>,
    #[new(value = "false")]
    use_tso_sequence: bool,
    #[new(value = "Arc::new(AtomicU64::new(0))")]
    min_ts_version: Arc<AtomicU64>,
    #[new(value = "false")]
    use_min_ts: bool,
    #[new(value = "Arc::new(AtomicU64::new(0))")]
    external_timestamp: Arc<AtomicU64>,
    #[new(value = "Arc::new(AtomicU64::new(crate::config::DEFAULT_TTL_REFRESHED_TXN_SIZE))")]
    ttl_refreshed_txn_size: Arc<AtomicU64>,
    #[new(value = "Arc::new(AtomicUsize::new(crate::config::DEFAULT_COMMITTER_CONCURRENCY))")]
    committer_concurrency: Arc<AtomicUsize>,
    #[new(
        value = "Arc::new(AtomicU64::new(u64::try_from(crate::config::DEFAULT_MAX_TXN_TTL.as_millis()).unwrap_or(u64::MAX)))"
    )]
    max_txn_ttl_ms: Arc<AtomicU64>,
    #[new(value = "42")]
    cluster_id: u64,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    get_external_timestamp_calls: Arc<AtomicUsize>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    set_external_timestamp_calls: Arc<AtomicUsize>,
    #[new(default)]
    get_timestamp_delay: Duration,
    #[new(value = "false")]
    get_timestamp_should_fail: bool,
    #[new(value = "AtomicBool::new(false)")]
    all_stores_should_fail: AtomicBool,
    #[new(value = "AtomicBool::new(false)")]
    enable_forwarding: AtomicBool,
    #[new(value = "AtomicBool::new(false)")]
    closed: AtomicBool,
    #[new(value = "Mutex::new(Vec::new())")]
    scatter_regions_calls: Mutex<Vec<(Vec<RegionId>, Option<String>)>>,
    #[new(value = "Mutex::new(Vec::new())")]
    get_operator_calls: Mutex<Vec<RegionId>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    get_operator_responses: Mutex<VecDeque<pdpb::GetOperatorResponse>>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    get_gc_safe_point_calls: Arc<AtomicUsize>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    get_gc_safe_point_responses: Mutex<VecDeque<u64>>,
    #[new(value = "Mutex::new(Vec::new())")]
    get_gc_safe_point_v2_calls: Mutex<Vec<u32>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    get_gc_safe_point_v2_responses: Mutex<VecDeque<u64>>,
    #[new(value = "Mutex::new(Vec::new())")]
    update_service_gc_safe_point_calls: Mutex<Vec<(String, i64, u64)>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    update_service_gc_safe_point_responses: Mutex<VecDeque<u64>>,
    #[new(value = "Mutex::new(Vec::new())")]
    update_service_safe_point_v2_calls: Mutex<Vec<(u32, String, i64, u64)>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    update_service_safe_point_v2_responses: Mutex<VecDeque<u64>>,
    #[new(value = "Mutex::new(Vec::new())")]
    update_gc_safe_point_v2_calls: Mutex<Vec<(u32, u64)>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    update_gc_safe_point_v2_responses: Mutex<VecDeque<u64>>,
    #[new(value = "Mutex::new(Vec::new())")]
    update_safepoint_calls: Mutex<Vec<u64>>,
    #[new(value = "Mutex::new(VecDeque::new())")]
    update_safepoint_responses: Mutex<VecDeque<u64>>,
}

#[async_trait]
impl KvClient for MockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match &self.dispatch {
            Some(f) => f(req.as_any()),
            None => panic!("no dispatch hook set"),
        }
    }

    fn store_address(&self) -> Option<&str> {
        Some(&self.addr)
    }
}

#[async_trait]
impl KvConnect for MockKvConnect {
    type KvClient = MockKvClient;

    async fn connect(&self, address: &str) -> Result<Self::KvClient> {
        Ok(MockKvClient {
            addr: address.to_owned(),
            dispatch: None,
        })
    }
}

impl MockPdClient {
    pub fn default() -> MockPdClient {
        MockPdClient::new(MockKvClient::default())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::StringError("client is closed".to_owned()));
        }
        Ok(())
    }

    pub fn set_enable_forwarding(&self, enable: bool) {
        self.enable_forwarding.store(enable, Ordering::SeqCst);
    }

    pub async fn insert_store_client(&self, store_id: StoreId, client: MockKvClient) {
        self.store_clients.write().await.insert(store_id, client);
    }

    pub fn set_ttl_refreshed_txn_size(&self, size: u64) {
        self.ttl_refreshed_txn_size.store(size, Ordering::SeqCst);
    }

    pub fn set_committer_concurrency(&self, concurrency: usize) {
        self.committer_concurrency
            .store(concurrency, Ordering::SeqCst);
    }

    pub fn set_max_txn_ttl(&self, ttl: Duration) {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.max_txn_ttl_ms.store(ttl_ms, Ordering::SeqCst);
    }

    pub fn set_all_stores_should_fail(&self, should_fail: bool) {
        self.all_stores_should_fail
            .store(should_fail, Ordering::SeqCst);
    }

    pub async fn insert_store_meta(&self, store: metapb::Store) {
        self.store_metas.write().await.insert(store.id, store);
    }

    pub fn store_meta_by_id_call_count(&self) -> usize {
        self.store_meta_by_id_calls.load(Ordering::SeqCst)
    }

    pub fn get_timestamp_call_count(&self) -> usize {
        self.get_timestamp_calls.load(Ordering::SeqCst)
    }

    pub fn get_min_ts_call_count(&self) -> usize {
        self.get_min_ts_calls.load(Ordering::SeqCst)
    }

    pub fn get_external_timestamp_call_count(&self) -> usize {
        self.get_external_timestamp_calls.load(Ordering::SeqCst)
    }

    pub fn set_external_timestamp_call_count(&self) -> usize {
        self.set_external_timestamp_calls.load(Ordering::SeqCst)
    }

    pub fn get_gc_safe_point_call_count(&self) -> usize {
        self.get_gc_safe_point_calls.load(Ordering::SeqCst)
    }

    pub fn scatter_regions_calls(&self) -> Vec<(Vec<RegionId>, Option<String>)> {
        let calls = match self.scatter_regions_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn get_operator_calls(&self) -> Vec<RegionId> {
        let calls = match self.get_operator_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_get_operator_response(&self, resp: pdpb::GetOperatorResponse) {
        let mut responses = match self.get_operator_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(resp);
    }

    pub fn push_get_gc_safe_point_response(&self, safe_point: u64) {
        let mut responses = match self.get_gc_safe_point_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(safe_point);
    }

    pub fn get_gc_safe_point_v2_calls(&self) -> Vec<u32> {
        let calls = match self.get_gc_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_get_gc_safe_point_v2_response(&self, safe_point: u64) {
        let mut responses = match self.get_gc_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(safe_point);
    }

    pub fn update_service_gc_safe_point_calls(&self) -> Vec<(String, i64, u64)> {
        let calls = match self.update_service_gc_safe_point_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_update_service_gc_safe_point_response(&self, min_safe_point: u64) {
        let mut responses = match self.update_service_gc_safe_point_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(min_safe_point);
    }

    pub fn update_service_safe_point_v2_calls(&self) -> Vec<(u32, String, i64, u64)> {
        let calls = match self.update_service_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_update_service_safe_point_v2_response(&self, min_safe_point: u64) {
        let mut responses = match self.update_service_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(min_safe_point);
    }

    pub fn update_gc_safe_point_v2_calls(&self) -> Vec<(u32, u64)> {
        let calls = match self.update_gc_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_update_gc_safe_point_v2_response(&self, new_safe_point: u64) {
        let mut responses = match self.update_gc_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(new_safe_point);
    }

    pub fn update_safepoint_calls(&self) -> Vec<u64> {
        let calls = match self.update_safepoint_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.clone()
    }

    pub fn push_update_safepoint_response(&self, new_safe_point: u64) {
        let mut responses = match self.update_safepoint_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        responses.push_back(new_safe_point);
    }

    pub fn get_timestamp_dc_locations(&self) -> Vec<String> {
        let locations = match self.get_timestamp_dc_locations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        locations.clone()
    }

    pub fn invalidated_region_ver_ids(&self) -> Vec<RegionVerId> {
        let invalidated = match self.invalidated_region_ver_ids.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        invalidated.clone()
    }

    pub fn invalidated_store_ids(&self) -> Vec<StoreId> {
        let invalidated = match self.invalidated_store_ids.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        invalidated.clone()
    }

    pub fn added_regions_to_cache(&self) -> Vec<RegionWithLeader> {
        let added = match self.added_regions_to_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        added.clone()
    }

    pub fn with_tso_sequence(mut self, start_version: u64) -> MockPdClient {
        self.use_tso_sequence = true;
        self.tso_version.store(start_version, Ordering::SeqCst);
        self
    }

    pub fn with_get_timestamp_delay(mut self, delay: Duration) -> MockPdClient {
        self.get_timestamp_delay = delay;
        self
    }

    pub fn with_get_timestamp_error(mut self) -> MockPdClient {
        self.get_timestamp_should_fail = true;
        self
    }

    pub fn with_min_ts_version(mut self, version: u64) -> MockPdClient {
        self.use_min_ts = true;
        self.min_ts_version.store(version, Ordering::SeqCst);
        self
    }

    pub fn region1() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.start_key = vec![];
        region.region.end_key = vec![10];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        region.region.peers = vec![
            metapb::Peer {
                id: 1,
                store_id: 41,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 2,
                store_id: 51,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 3,
                store_id: 61,
                role: metapb::PeerRole::Learner as i32,
                ..Default::default()
            },
        ];

        let leader = metapb::Peer {
            store_id: 41,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region2() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 2;
        region.region.start_key = vec![10];
        region.region.end_key = vec![250, 250];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        region.region.peers = vec![
            metapb::Peer {
                id: 4,
                store_id: 42,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 5,
                store_id: 52,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 6,
                store_id: 62,
                role: metapb::PeerRole::Learner as i32,
                ..Default::default()
            },
        ];

        let leader = metapb::Peer {
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region3() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 3;
        region.region.start_key = vec![250, 250];
        region.region.end_key = vec![];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        region.region.peers = vec![
            metapb::Peer {
                id: 7,
                store_id: 43,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 8,
                store_id: 53,
                role: metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            metapb::Peer {
                id: 9,
                store_id: 63,
                role: metapb::PeerRole::Learner as i32,
                ..Default::default()
            },
        ];

        let leader = metapb::Peer {
            store_id: 43,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }
}

#[async_trait]
impl PdClient for MockPdClient {
    type KvClient = MockKvClient;

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn ttl_refreshed_txn_size(&self) -> u64 {
        self.ttl_refreshed_txn_size.load(Ordering::SeqCst)
    }

    fn committer_concurrency(&self) -> usize {
        self.committer_concurrency.load(Ordering::SeqCst)
    }

    fn max_txn_ttl(&self) -> Duration {
        Duration::from_millis(self.max_txn_ttl_ms.load(Ordering::SeqCst))
    }

    fn enable_forwarding(&self) -> bool {
        self.enable_forwarding.load(Ordering::SeqCst)
    }

    fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        self.ensure_open()?;
        let store_id = region.get_store_id()?;
        let client = self
            .store_clients
            .read()
            .await
            .get(&store_id)
            .cloned()
            .unwrap_or_else(|| self.client.clone());
        Ok(RegionStore::new(
            region,
            Arc::new(client),
            format!("mock://{store_id}"),
        ))
    }

    async fn store_meta_by_id(&self, store_id: StoreId) -> Result<metapb::Store> {
        self.ensure_open()?;
        self.store_meta_by_id_calls.fetch_add(1, Ordering::SeqCst);
        self.store_metas
            .read()
            .await
            .get(&store_id)
            .cloned()
            .ok_or(Error::Unimplemented)
    }

    fn store_meta_by_id_cached(&self, store_id: StoreId) -> Option<metapb::Store> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let guard = self.store_metas.try_read().ok()?;
        guard.get(&store_id).cloned()
    }

    async fn get_health_feedback(&self, store_id: StoreId) -> Result<kvrpcpb::HealthFeedback> {
        self.ensure_open()?;
        let _ = store_id;
        let mut req = kvrpcpb::GetHealthFeedbackRequest::default();
        req.context = Some(kvrpcpb::Context::default());

        let resp = self.client.dispatch(&req).await?;
        let resp = resp
            .downcast::<kvrpcpb::GetHealthFeedbackResponse>()
            .map_err(|_| {
                Error::StringError("unexpected GetHealthFeedback response type".to_owned())
            })?;
        let resp = *resp;
        if let Some(region_error) = resp.region_error {
            return Err(Error::RegionError(Box::new(region_error)));
        }

        let Some(feedback) = resp.health_feedback else {
            return Err(Error::Unimplemented);
        };
        if feedback.slow_score >= crate::pd::HEALTH_FEEDBACK_SLOW_SCORE_THRESHOLD {
            self.mark_store_slow(feedback.store_id, crate::pd::HEALTH_FEEDBACK_SLOW_STORE_TTL);
        }
        Ok(feedback)
    }

    fn mark_store_slow(&self, store_id: StoreId, duration: Duration) {
        if duration.is_zero() {
            return;
        }

        let until = Instant::now() + duration;
        let mut slow_store_until = match self.slow_store_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if matches!(
            slow_store_until.get(&store_id),
            Some(existing) if *existing >= until
        ) {
            return;
        }
        slow_store_until.insert(store_id, until);
    }

    fn is_store_slow(&self, store_id: StoreId) -> bool {
        let now = Instant::now();
        let mut slow_store_until = match self.slow_store_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match slow_store_until.get(&store_id) {
            Some(until) if *until > now => true,
            Some(_) => {
                slow_store_until.remove(&store_id);
                false
            }
            None => false,
        }
    }

    fn update_store_load_stats(&self, store_id: StoreId, estimated_wait_ms: u32) {
        if estimated_wait_ms == 0 {
            return;
        }

        let estimated_wait = Duration::from_millis(u64::from(estimated_wait_ms));
        let until = Instant::now() + estimated_wait;

        let mut store_estimated_wait_until = match self.store_estimated_wait_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        store_estimated_wait_until.insert(store_id, until);
    }

    fn store_estimated_wait_time(&self, store_id: StoreId) -> Duration {
        let now = Instant::now();
        let mut store_estimated_wait_until = match self.store_estimated_wait_until.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match store_estimated_wait_until.get(&store_id) {
            Some(until) if *until > now => until.duration_since(now),
            Some(_) => {
                store_estimated_wait_until.remove(&store_id);
                Duration::ZERO
            }
            None => Duration::ZERO,
        }
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        let bytes: &[_] = key.into();
        let region = if bytes.is_empty() || bytes < &[10][..] {
            Self::region1()
        } else if bytes >= &[10][..] && bytes < &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };

        Ok(region)
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        let bytes: &[_] = key.into();
        if bytes.is_empty() {
            return Err(Error::Unimplemented);
        }

        let region = if bytes <= &[10][..] {
            Self::region1()
        } else if bytes <= &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };

        Ok(region)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        self.ensure_open()?;
        match id {
            1 => Ok(Self::region1()),
            2 => Ok(Self::region2()),
            3 => Ok(Self::region3()),
            _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
        }
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        self.ensure_open()?;
        if self.all_stores_should_fail.load(Ordering::SeqCst) {
            return Err(Error::StringError("injected all_stores error".to_owned()));
        }
        let metas = self.store_metas.read().await;
        let client = Arc::new(self.client.clone());
        if metas.is_empty() {
            return Ok(vec![Store::new(
                metapb::Store {
                    id: 1,
                    ..Default::default()
                },
                client,
            )]);
        }
        Ok(metas
            .values()
            .cloned()
            .map(|meta| Store::new(meta, client.clone()))
            .collect())
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.ensure_open()?;
        self.get_timestamp_calls.fetch_add(1, Ordering::SeqCst);
        if self.get_timestamp_delay > Duration::from_millis(0) {
            sleep(self.get_timestamp_delay).await;
        }
        if self.get_timestamp_should_fail {
            return Err(Error::StringError(
                "injected get_timestamp error".to_owned(),
            ));
        }
        if self.use_tso_sequence {
            let version = self.tso_version.fetch_add(1, Ordering::SeqCst);
            Ok(Timestamp::from_version(version))
        } else {
            Ok(Timestamp::default())
        }
    }

    async fn get_timestamp_with_dc_location(
        self: Arc<Self>,
        dc_location: String,
    ) -> Result<Timestamp> {
        self.ensure_open()?;
        self.get_timestamp_calls.fetch_add(1, Ordering::SeqCst);
        if self.get_timestamp_delay > Duration::from_millis(0) {
            sleep(self.get_timestamp_delay).await;
        }

        let mut locations = match self.get_timestamp_dc_locations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        locations.push(dc_location);

        if self.get_timestamp_should_fail {
            return Err(Error::StringError(
                "injected get_timestamp error".to_owned(),
            ));
        }
        if self.use_tso_sequence {
            let version = self.tso_version.fetch_add(1, Ordering::SeqCst);
            Ok(Timestamp::from_version(version))
        } else {
            Ok(Timestamp::default())
        }
    }

    async fn get_min_ts(self: Arc<Self>) -> Result<Timestamp> {
        self.ensure_open()?;
        self.get_min_ts_calls.fetch_add(1, Ordering::SeqCst);
        if self.use_min_ts {
            Ok(Timestamp::from_version(
                self.min_ts_version.load(Ordering::SeqCst),
            ))
        } else {
            Ok(Timestamp::default())
        }
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.ensure_open()?;
        self.get_external_timestamp_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(self.external_timestamp.load(Ordering::SeqCst))
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.ensure_open()?;
        self.set_external_timestamp_calls
            .fetch_add(1, Ordering::SeqCst);
        self.external_timestamp.store(timestamp, Ordering::SeqCst);
        Ok(())
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<RegionId>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        self.ensure_open()?;
        let mut calls = match self.scatter_regions_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push((region_ids, group));
        Ok(pdpb::ScatterRegionResponse::default())
    }

    async fn get_operator(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<pdpb::GetOperatorResponse> {
        self.ensure_open()?;
        let mut calls = match self.get_operator_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push(region_id);

        let mut responses = match self.get_operator_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut resp = responses.pop_front().unwrap_or_default();
        if resp.region_id == 0 {
            resp.region_id = region_id;
        }
        Ok(resp)
    }

    async fn get_gc_safe_point(self: Arc<Self>) -> Result<u64> {
        self.ensure_open()?;
        self.get_gc_safe_point_calls.fetch_add(1, Ordering::SeqCst);
        let mut responses = match self.get_gc_safe_point_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or_default())
    }

    async fn get_gc_safe_point_v2(self: Arc<Self>, keyspace_id: u32) -> Result<u64> {
        self.ensure_open()?;
        let mut calls = match self.get_gc_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push(keyspace_id);

        let mut responses = match self.get_gc_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or_default())
    }

    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        let mut calls = match self.update_service_gc_safe_point_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push((service_id, ttl, safe_point));

        let mut responses = match self.update_service_gc_safe_point_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or(safe_point))
    }

    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        let mut calls = match self.update_service_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push((keyspace_id, service_id, ttl, safe_point));

        let mut responses = match self.update_service_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or(safe_point))
    }

    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        self.ensure_open()?;
        let mut calls = match self.update_gc_safe_point_v2_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push((keyspace_id, safe_point));

        let mut responses = match self.update_gc_safe_point_v2_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or(safe_point))
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        self.ensure_open()?;
        let mut calls = match self.update_safepoint_calls.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        calls.push(safepoint);

        let mut responses = match self.update_safepoint_responses.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        Ok(responses.pop_front().unwrap_or(safepoint))
    }

    async fn update_leader(
        &self,
        _ver_id: crate::region::RegionVerId,
        _leader: metapb::Peer,
    ) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        let mut invalidated = match self.invalidated_region_ver_ids.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        invalidated.push(ver_id);
    }

    async fn invalidate_store_cache(&self, store_id: crate::region::StoreId) {
        let mut invalidated = match self.invalidated_store_ids.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        invalidated.push(store_id);
    }

    async fn add_region_to_cache(&self, region: RegionWithLeader) {
        let mut added = match self.added_regions_to_cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        added.push(region);
    }

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::Unimplemented)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::pd::PdClient;
    use crate::proto::pdpb;

    use super::MockPdClient;

    #[test]
    fn test_mock_pd_client_committer_concurrency_is_configurable() {
        let client = MockPdClient::default();
        assert_eq!(
            client.committer_concurrency(),
            crate::config::DEFAULT_COMMITTER_CONCURRENCY
        );
        client.set_committer_concurrency(1);
        assert_eq!(client.committer_concurrency(), 1);
    }

    #[test]
    fn test_mock_pd_client_max_txn_ttl_is_configurable() {
        let client = MockPdClient::default();
        assert_eq!(client.max_txn_ttl(), crate::config::DEFAULT_MAX_TXN_TTL);
        client.set_max_txn_ttl(Duration::from_secs(2));
        assert_eq!(client.max_txn_ttl(), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn test_mock_pd_client_scatter_regions_records_calls() {
        let client = Arc::new(MockPdClient::default());

        client
            .clone()
            .scatter_regions(vec![1, 2], Some("group".to_owned()))
            .await
            .unwrap();

        assert_eq!(
            client.scatter_regions_calls(),
            vec![(vec![1, 2], Some("group".to_owned()))]
        );
    }

    #[tokio::test]
    async fn test_mock_pd_client_get_operator_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Running as i32,
            ..Default::default()
        });

        let resp = client.clone().get_operator(42).await.unwrap();
        assert_eq!(resp.region_id, 42);
        assert_eq!(resp.desc, b"scatter-region".to_vec());
        assert_eq!(resp.status, pdpb::OperatorStatus::Running as i32);

        let resp = client.clone().get_operator(43).await.unwrap();
        assert_eq!(resp.region_id, 43);
        assert_eq!(resp.status, pdpb::OperatorStatus::Success as i32);

        assert_eq!(client.get_operator_calls(), vec![42, 43]);
    }

    #[tokio::test]
    async fn test_mock_pd_client_get_gc_safe_point_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_get_gc_safe_point_response(100);

        let safe_point = client.clone().get_gc_safe_point().await.unwrap();
        assert_eq!(safe_point, 100);
        assert_eq!(client.get_gc_safe_point_call_count(), 1);

        let safe_point = client.clone().get_gc_safe_point().await.unwrap();
        assert_eq!(safe_point, 0);
        assert_eq!(client.get_gc_safe_point_call_count(), 2);
    }

    #[tokio::test]
    async fn test_mock_pd_client_get_gc_safe_point_v2_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_get_gc_safe_point_v2_response(100);

        let safe_point = client.clone().get_gc_safe_point_v2(7).await.unwrap();
        assert_eq!(safe_point, 100);
        assert_eq!(client.get_gc_safe_point_v2_calls(), vec![7]);

        let safe_point = client.clone().get_gc_safe_point_v2(8).await.unwrap();
        assert_eq!(safe_point, 0);
        assert_eq!(client.get_gc_safe_point_v2_calls(), vec![7, 8]);
    }

    #[tokio::test]
    async fn test_mock_pd_client_update_service_gc_safe_point_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_update_service_gc_safe_point_response(100);
        let min_safe_point = client
            .clone()
            .update_service_gc_safe_point("service".to_owned(), 42, 7)
            .await
            .unwrap();

        assert_eq!(min_safe_point, 100);
        assert_eq!(
            client.update_service_gc_safe_point_calls(),
            vec![("service".to_owned(), 42, 7)]
        );

        let min_safe_point = client
            .clone()
            .update_service_gc_safe_point("service".to_owned(), 1, 5)
            .await
            .unwrap();
        assert_eq!(min_safe_point, 5);
    }

    #[tokio::test]
    async fn test_mock_pd_client_update_service_safe_point_v2_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_update_service_safe_point_v2_response(100);
        let min_safe_point = client
            .clone()
            .update_service_safe_point_v2(7, "service".to_owned(), 42, 9)
            .await
            .unwrap();

        assert_eq!(min_safe_point, 100);
        assert_eq!(
            client.update_service_safe_point_v2_calls(),
            vec![(7, "service".to_owned(), 42, 9)]
        );

        let min_safe_point = client
            .clone()
            .update_service_safe_point_v2(7, "service".to_owned(), 1, 5)
            .await
            .unwrap();
        assert_eq!(min_safe_point, 5);
    }

    #[tokio::test]
    async fn test_mock_pd_client_update_gc_safe_point_v2_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_update_gc_safe_point_v2_response(100);
        let new_safe_point = client.clone().update_gc_safe_point_v2(7, 9).await.unwrap();

        assert_eq!(new_safe_point, 100);
        assert_eq!(client.update_gc_safe_point_v2_calls(), vec![(7, 9)]);

        let new_safe_point = client.clone().update_gc_safe_point_v2(7, 5).await.unwrap();
        assert_eq!(new_safe_point, 5);
    }

    #[tokio::test]
    async fn test_mock_pd_client_update_safepoint_records_calls_and_uses_queue() {
        let client = Arc::new(MockPdClient::default());

        client.push_update_safepoint_response(100);
        let new_safe_point = client.clone().update_safepoint(9).await.unwrap();

        assert_eq!(new_safe_point, 100);
        assert_eq!(client.update_safepoint_calls(), vec![9]);

        let new_safe_point = client.clone().update_safepoint(5).await.unwrap();
        assert_eq!(new_safe_point, 5);
    }
}
