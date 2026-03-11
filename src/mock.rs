// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Various mock versions of the various clients and other objects.
//!
//! The goal is to be able to test functionality independently of the rest of
//! the system, in particular without requiring a TiKV or PD server, or RPC layer.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use derive_new::new;
use tokio::sync::RwLock;

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::pd::RetryClient;
use crate::proto::keyspacepb;
use crate::proto::metapb::RegionEpoch;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::{KvClient, Store};
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
    store_metas: RwLock<HashMap<StoreId, metapb::Store>>,
    #[new(value = "Arc::new(AtomicUsize::new(0))")]
    store_meta_by_id_calls: Arc<AtomicUsize>,
    #[new(value = "Mutex::new(HashMap::new())")]
    slow_store_until: Mutex<HashMap<StoreId, Instant>>,
    #[new(value = "Mutex::new(HashMap::new())")]
    store_estimated_wait_until: Mutex<HashMap<StoreId, Instant>>,
}

#[async_trait]
impl KvClient for MockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match &self.dispatch {
            Some(f) => f(req.as_any()),
            None => panic!("no dispatch hook set"),
        }
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

    pub async fn insert_store_meta(&self, store: metapb::Store) {
        self.store_metas.write().await.insert(store.id, store);
    }

    pub fn store_meta_by_id_call_count(&self) -> usize {
        self.store_meta_by_id_calls.load(Ordering::SeqCst)
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

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(region, Arc::new(self.client.clone())))
    }

    async fn store_meta_by_id(&self, store_id: StoreId) -> Result<metapb::Store> {
        self.store_meta_by_id_calls.fetch_add(1, Ordering::SeqCst);
        self.store_metas
            .read()
            .await
            .get(&store_id)
            .cloned()
            .ok_or(Error::Unimplemented)
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

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        match id {
            1 => Ok(Self::region1()),
            2 => Ok(Self::region2()),
            3 => Ok(Self::region3()),
            _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
        }
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![Store::new(Arc::new(self.client.clone()))])
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        Err(Error::Unimplemented)
    }

    async fn update_leader(
        &self,
        _ver_id: crate::region::RegionVerId,
        _leader: metapb::Peer,
    ) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: crate::region::RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: crate::region::StoreId) {}

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::Unimplemented)
    }
}
