use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::backoff::DEFAULT_STORE_BACKOFF;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::StoreId;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::Result;

const GLOBAL_TXN_SCOPE: &str = "global";
const ZONE_LABEL_KEY: &str = "zone";
const SAFE_TS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) struct SafeTsCache<PdC: PdClient> {
    inner: Arc<SafeTsCacheInner<PdC>>,
}

impl<PdC: PdClient> Clone for SafeTsCache<PdC> {
    fn clone(&self) -> Self {
        SafeTsCache {
            inner: self.inner.clone(),
        }
    }
}

struct SafeTsCacheInner<PdC: PdClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    state: RwLock<SafeTsState>,
    started: AtomicBool,
    start_lock: Mutex<()>,
    refresh_lock: Mutex<()>,
}

#[derive(Default)]
struct SafeTsState {
    initialized: bool,
    store_safe_ts: HashMap<StoreId, u64>,
    min_safe_ts: HashMap<String, u64>,
}

impl<PdC: PdClient> SafeTsCache<PdC> {
    pub(crate) fn new(pd: Arc<PdC>, keyspace: Keyspace) -> Self {
        SafeTsCache {
            inner: Arc::new(SafeTsCacheInner {
                pd,
                keyspace,
                state: RwLock::new(SafeTsState::default()),
                started: AtomicBool::new(false),
                start_lock: Mutex::new(()),
                refresh_lock: Mutex::new(()),
            }),
        }
    }

    pub(crate) async fn min_safe_ts(&self) -> Result<u64> {
        self.min_safe_ts_with_txn_scope(GLOBAL_TXN_SCOPE).await
    }

    pub(crate) async fn min_safe_ts_with_txn_scope(&self, txn_scope: &str) -> Result<u64> {
        self.ensure_started().await;

        let txn_scope = if txn_scope.is_empty() || txn_scope == GLOBAL_TXN_SCOPE {
            GLOBAL_TXN_SCOPE
        } else {
            txn_scope
        };

        {
            let state = self.inner.state.read().await;
            if state.initialized {
                return Ok(*state.min_safe_ts.get(txn_scope).unwrap_or(&0));
            }
        }

        // Populate the cache on first use.
        let _ = self.inner.refresh().await;
        let state = self.inner.state.read().await;
        Ok(*state.min_safe_ts.get(txn_scope).unwrap_or(&0))
    }

    #[cfg(test)]
    pub(crate) async fn refresh(&self) -> Result<()> {
        self.ensure_started().await;
        self.inner.refresh().await
    }

    async fn ensure_started(&self) {
        if self.inner.started.load(Ordering::Acquire) {
            return;
        }

        let _guard = self.inner.start_lock.lock().await;
        if self.inner.started.load(Ordering::Relaxed) {
            return;
        }

        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + SAFE_TS_UPDATE_INTERVAL;
            let mut interval = tokio::time::interval_at(start, SAFE_TS_UPDATE_INTERVAL);
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                if let Err(err) = inner.refresh().await {
                    debug!("safe-ts refresh failed: {:?}", err);
                }
            }
        });

        self.inner.started.store(true, Ordering::Release);
    }
}

impl<PdC: PdClient> SafeTsCacheInner<PdC> {
    async fn refresh(&self) -> Result<()> {
        let _guard = self.refresh_lock.lock().await;

        let stores = self.pd.all_stores_for_safe_ts().await?;
        if stores.is_empty() {
            let mut state = self.state.write().await;
            state.initialized = true;
            state.min_safe_ts.clear();
            return Ok(());
        }

        let store_ids: Vec<StoreId> = stores.iter().map(|store| store.meta.id).collect();
        let pd_store_safe_ts = match self.pd.min_resolved_ts_by_stores(&store_ids).await {
            Ok((_min_resolved_ts, store_safe_ts)) if !store_safe_ts.is_empty() => {
                Some(store_safe_ts)
            }
            _ => None,
        };

        let store_safe_ts_updates: Vec<(StoreId, u64)> = match pd_store_safe_ts {
            Some(pd_store_safe_ts) => stores
                .iter()
                .filter_map(|store| {
                    let store_id = store.meta.id;
                    pd_store_safe_ts
                        .get(&store_id)
                        .copied()
                        .map(|safe_ts| (store_id, safe_ts))
                })
                .collect(),
            None => {
                let request = kvrpcpb::StoreSafeTsRequest {
                    key_range: Some(kvrpcpb::KeyRange::default()),
                };
                let plan = PlanBuilder::new(self.pd.clone(), self.keyspace, request)
                    .stores(stores.clone(), DEFAULT_STORE_BACKOFF)
                    .plan();
                let results = plan.execute().await?;

                stores
                    .iter()
                    .zip(results)
                    .filter_map(|(store, result)| {
                        let resp = match result {
                            Ok(resp) => resp,
                            Err(_) => return None,
                        };
                        Some((store.meta.id, resp.safe_ts))
                    })
                    .collect()
            }
        };

        let mut state = self.state.write().await;
        for (store_id, new_safe_ts) in store_safe_ts_updates {
            if new_safe_ts == u64::MAX {
                debug!("skip setting safe-ts to max value (store_id={})", store_id);
                continue;
            }

            let prev_safe_ts = state.store_safe_ts.get(&store_id).copied().unwrap_or(0);
            if prev_safe_ts > new_safe_ts {
                continue;
            }
            state.store_safe_ts.insert(store_id, new_safe_ts);
        }

        let mut scope_to_store_ids: HashMap<String, Vec<StoreId>> = HashMap::new();
        for store in &stores {
            scope_to_store_ids
                .entry(GLOBAL_TXN_SCOPE.to_owned())
                .or_default()
                .push(store.meta.id);

            if let Some(zone) = store
                .meta
                .labels
                .iter()
                .find(|label| label.key == ZONE_LABEL_KEY)
                .map(|label| label.value.clone())
            {
                scope_to_store_ids
                    .entry(zone)
                    .or_default()
                    .push(store.meta.id);
            }
        }

        let mut min_safe_ts = HashMap::with_capacity(scope_to_store_ids.len());
        for (scope, store_ids) in scope_to_store_ids {
            min_safe_ts.insert(scope, compute_min_safe_ts(&state.store_safe_ts, &store_ids));
        }
        state.min_safe_ts = min_safe_ts;
        state.initialized = true;

        Ok(())
    }
}

fn compute_min_safe_ts(store_safe_ts: &HashMap<StoreId, u64>, store_ids: &[StoreId]) -> u64 {
    if store_ids.is_empty() {
        return 0;
    }

    let mut min_safe_ts = u64::MAX;
    for store_id in store_ids {
        let Some(safe_ts) = store_safe_ts.get(store_id).copied() else {
            return 0;
        };
        if safe_ts != 0 && safe_ts < min_safe_ts {
            min_safe_ts = safe_ts;
        }
    }

    if min_safe_ts == u64::MAX {
        0
    } else {
        min_safe_ts
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::SafeTsCache;
    use crate::mock::MockKvClient;
    use crate::pd::PdClient;
    use crate::proto::keyspacepb;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
    use crate::request::Keyspace;
    use crate::store::{RegionStore, Store};
    use crate::{Error, Result, Timestamp};

    #[derive(Clone)]
    struct SafeTsPdClient {
        stores: Vec<Store>,
    }

    #[async_trait]
    impl PdClient for SafeTsPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            _region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Err(Error::Unimplemented)
        }

        async fn region_for_key(&self, _key: &crate::Key) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(self.stores.clone())
        }

        async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
            Ok(self.stores.clone())
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    #[derive(Clone)]
    struct SafeTsFastPathPdClient {
        stores: Vec<Store>,
        store_safe_ts: HashMap<StoreId, u64>,
    }

    #[async_trait]
    impl PdClient for SafeTsFastPathPdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            _region: RegionWithLeader,
        ) -> Result<RegionStore> {
            Err(Error::Unimplemented)
        }

        async fn region_for_key(&self, _key: &crate::Key) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
            Err(Error::Unimplemented)
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(self.stores.clone())
        }

        async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
            Ok(self.stores.clone())
        }

        async fn min_resolved_ts_by_stores(
            &self,
            store_ids: &[StoreId],
        ) -> Result<(u64, HashMap<StoreId, u64>)> {
            let expected_store_ids: Vec<StoreId> =
                self.stores.iter().map(|store| store.meta.id).collect();
            assert_eq!(store_ids, expected_store_ids.as_slice());
            Ok((42, self.store_safe_ts.clone()))
        }

        async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
            Ok(())
        }

        async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

        async fn invalidate_store_cache(&self, _store_id: StoreId) {}
    }

    fn store_for_safe_ts(
        store_id: StoreId,
        labels: Vec<metapb::StoreLabel>,
        safe_ts: u64,
    ) -> Store {
        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>().is_some() {
                let resp = kvrpcpb::StoreSafeTsResponse { safe_ts };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            Err(Error::Unimplemented)
        });

        Store::new(
            metapb::Store {
                id: store_id,
                labels,
                ..Default::default()
            },
            Arc::new(kv_client),
        )
    }

    fn store_for_safe_ts_with_counter(
        store_id: StoreId,
        labels: Vec<metapb::StoreLabel>,
        safe_ts: u64,
        calls: Arc<AtomicUsize>,
    ) -> Store {
        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>().is_some() {
                calls.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::StoreSafeTsResponse { safe_ts };
                return Ok(Box::new(resp) as Box<dyn Any>);
            }
            Err(Error::Unimplemented)
        });

        Store::new(
            metapb::Store {
                id: store_id,
                labels,
                ..Default::default()
            },
            Arc::new(kv_client),
        )
    }

    #[tokio::test]
    async fn test_min_safe_ts_with_txn_scope_includes_tiflash_store() -> Result<()> {
        let tikv_dc1 = store_for_safe_ts(
            1,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
            100,
        );
        let tikv_dc2 = store_for_safe_ts(
            2,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc2".to_owned(),
            }],
            10,
        );
        let tiflash_dc2 = store_for_safe_ts(
            3,
            vec![
                metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                },
                metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "dc2".to_owned(),
                },
            ],
            42,
        );

        let pd_client = Arc::new(SafeTsPdClient {
            stores: vec![tikv_dc1, tikv_dc2, tiflash_dc2],
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);
        safe_ts.refresh().await?;

        assert_eq!(safe_ts.min_safe_ts().await?, 10);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc1").await?, 100);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc2").await?, 10);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("unknown").await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_safe_ts_refresh_prefers_pd_min_resolved_ts_fast_path() -> Result<()> {
        let store_safe_ts_calls = Arc::new(AtomicUsize::new(0));
        let tikv_dc1 = store_for_safe_ts_with_counter(
            1,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
            999,
            store_safe_ts_calls.clone(),
        );
        let tikv_dc2 = store_for_safe_ts_with_counter(
            2,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc2".to_owned(),
            }],
            999,
            store_safe_ts_calls.clone(),
        );

        let mut store_safe_ts = HashMap::new();
        store_safe_ts.insert(1, 100);
        store_safe_ts.insert(2, 10);

        let pd_client = Arc::new(SafeTsFastPathPdClient {
            stores: vec![tikv_dc1, tikv_dc2],
            store_safe_ts,
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);
        safe_ts.refresh().await?;

        assert_eq!(
            store_safe_ts_calls.load(Ordering::SeqCst),
            0,
            "pd fast path should skip StoreSafeTS RPCs"
        );

        assert_eq!(safe_ts.min_safe_ts().await?, 10);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc1").await?, 100);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc2").await?, 10);

        Ok(())
    }
}
