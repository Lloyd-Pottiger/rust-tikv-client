use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::debug;
use tokio::sync::{watch, Mutex, RwLock};

use crate::backoff::DEFAULT_STORE_BACKOFF;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::region::StoreId;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::stats::{inc_safe_ts_update_counter, set_min_safe_ts_gap_seconds};
use crate::Error;
use crate::Result;

const GLOBAL_TXN_SCOPE: &str = "global";
const ZONE_LABEL_KEY: &str = "zone";
const SAFE_TS_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const SAFE_TS_PHYSICAL_SHIFT_BITS: u32 = 18;

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

#[derive(Debug, Default)]
struct SafeTsRefreshState {
    in_flight: bool,
    generation: u64,
    last_error: Option<String>,
}

struct SafeTsCacheInner<PdC: PdClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    state: RwLock<SafeTsState>,
    closed: AtomicBool,
    started: AtomicBool,
    start_lock: Mutex<()>,
    refresh: Mutex<SafeTsRefreshState>,
    refresh_generation: watch::Sender<u64>,
}

#[derive(Default)]
struct SafeTsState {
    initialized: bool,
    store_safe_ts: HashMap<StoreId, u64>,
    min_safe_ts: HashMap<String, u64>,
}

impl<PdC: PdClient> SafeTsCache<PdC> {
    pub(crate) fn new(pd: Arc<PdC>, keyspace: Keyspace) -> Self {
        let (refresh_generation, _rx) = watch::channel(0);
        SafeTsCache {
            inner: Arc::new(SafeTsCacheInner {
                pd,
                keyspace,
                state: RwLock::new(SafeTsState::default()),
                closed: AtomicBool::new(false),
                started: AtomicBool::new(false),
                start_lock: Mutex::new(()),
                refresh: Mutex::new(SafeTsRefreshState::default()),
                refresh_generation,
            }),
        }
    }

    pub(crate) async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        *self.inner.state.write().await = SafeTsState::default();
        let generation = {
            let mut refresh = self.inner.refresh.lock().await;
            refresh.in_flight = false;
            refresh.generation = refresh.generation.wrapping_add(1);
            refresh.last_error = Some("client is closed".to_owned());
            refresh.generation
        };
        let _ = self.inner.refresh_generation.send(generation);
    }

    pub(crate) async fn min_safe_ts(&self) -> Result<u64> {
        self.min_safe_ts_with_txn_scope(GLOBAL_TXN_SCOPE).await
    }

    pub(crate) async fn min_safe_ts_with_txn_scope(&self, txn_scope: &str) -> Result<u64> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::StringError("client is closed".to_owned()));
        }
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
        if self.inner.closed.load(Ordering::Acquire) {
            return;
        }
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
                if inner.closed.load(Ordering::Acquire) {
                    return;
                }
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
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::StringError("client is closed".to_owned()));
        }
        let wait_for_generation = {
            let mut refresh = self.refresh.lock().await;
            if refresh.in_flight {
                Some(refresh.generation)
            } else {
                refresh.in_flight = true;
                None
            }
        };

        if let Some(generation) = wait_for_generation {
            let mut refreshed = self.refresh_generation.subscribe();
            while *refreshed.borrow() == generation {
                // Sender is owned by the cache; treat closure as a best-effort wake-up.
                if refreshed.changed().await.is_err() {
                    break;
                }
            }

            if self.closed.load(Ordering::Acquire) {
                return Err(Error::StringError("client is closed".to_owned()));
            }

            let refresh = self.refresh.lock().await;
            if let Some(err) = refresh.last_error.as_ref() {
                return Err(Error::StringError(err.clone()));
            }
            return Ok(());
        }

        let result = self.refresh_once().await;
        let generation = {
            let mut refresh = self.refresh.lock().await;
            refresh.in_flight = false;
            refresh.generation = refresh.generation.wrapping_add(1);
            refresh.last_error = result.as_ref().err().map(|err| err.to_string());
            refresh.generation
        };
        let _ = self.refresh_generation.send(generation);
        result
    }

    async fn refresh_once(&self) -> Result<()> {
        let stores = self.pd.all_stores_for_safe_ts().await?;
        if stores.is_empty() {
            let mut state = self.state.write().await;
            state.initialized = true;
            state.min_safe_ts.clear();
            return Ok(());
        }

        let store_ids: Vec<StoreId> = stores.iter().map(|store| store.meta.id).collect();
        let (pd_min_resolved_ts, pd_store_safe_ts) =
            match self.pd.min_resolved_ts_by_stores(&store_ids).await {
                Ok((min_resolved_ts, store_safe_ts)) if !store_safe_ts.is_empty() => {
                    (Some(min_resolved_ts), Some(store_safe_ts))
                }
                _ => (None, None),
            };

        let store_safe_ts_updates: Vec<(StoreId, Option<u64>)> = match pd_store_safe_ts {
            Some(pd_store_safe_ts) => {
                let mut updates = Vec::with_capacity(stores.len());
                let mut fallback_stores = Vec::new();

                for store in &stores {
                    let store_id = store.meta.id;
                    if let Some(safe_ts) = pd_store_safe_ts
                        .get(&store_id)
                        .copied()
                        .filter(|safe_ts| is_valid_pd_safe_ts(*safe_ts))
                    {
                        updates.push((store_id, Some(safe_ts)));
                    } else {
                        fallback_stores.push(store.clone());
                        updates.push((store_id, None));
                    }
                }

                if !fallback_stores.is_empty() {
                    let request = kvrpcpb::StoreSafeTsRequest {
                        key_range: Some(kvrpcpb::KeyRange::default()),
                    };
                    let plan = PlanBuilder::new(self.pd.clone(), self.keyspace, request)
                        .stores(fallback_stores.clone(), DEFAULT_STORE_BACKOFF)
                        .plan();
                    let results = plan.execute().await?;

                    let mut fallback_results = HashMap::with_capacity(fallback_stores.len());
                    for (store, result) in fallback_stores.iter().zip(results) {
                        let store_id = store.meta.id;
                        let safe_ts = match result {
                            Ok(resp) => Some(resp.safe_ts),
                            Err(_) => None,
                        };
                        fallback_results.insert(store_id, safe_ts);
                    }

                    for (store_id, safe_ts) in &mut updates {
                        if safe_ts.is_none() {
                            if let Some(fallback_safe_ts) = fallback_results.get(store_id) {
                                *safe_ts = *fallback_safe_ts;
                            }
                        }
                    }
                }

                updates
            }
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
                    .map(|(store, result)| {
                        let safe_ts = match result {
                            Ok(resp) => Some(resp.safe_ts),
                            Err(_) => None,
                        };
                        (store.meta.id, safe_ts)
                    })
                    .collect()
            }
        };

        let now_ms = epoch_millis();
        let mut state = self.state.write().await;
        if let Some(pd_min_resolved_ts) =
            pd_min_resolved_ts.filter(|safe_ts| is_valid_pd_safe_ts(*safe_ts))
        {
            let prev_min_safe_ts = state
                .min_safe_ts
                .get(GLOBAL_TXN_SCOPE)
                .copied()
                .unwrap_or(0);
            if prev_min_safe_ts > pd_min_resolved_ts {
                inc_safe_ts_update_counter("skip", "cluster");
                set_min_safe_ts_gap_seconds(
                    "cluster",
                    safe_ts_gap_seconds(now_ms, prev_min_safe_ts),
                );
            } else {
                inc_safe_ts_update_counter("success", "cluster");
                set_min_safe_ts_gap_seconds(
                    "cluster",
                    safe_ts_gap_seconds(now_ms, pd_min_resolved_ts),
                );
            }
        }

        for (store_id, new_safe_ts) in store_safe_ts_updates {
            let store_id_label = store_id.to_string();
            let Some(new_safe_ts) = new_safe_ts else {
                inc_safe_ts_update_counter("fail", store_id_label.as_str());
                continue;
            };

            if new_safe_ts == u64::MAX {
                debug!("skip setting safe-ts to max value (store_id={})", store_id);
                inc_safe_ts_update_counter("fail", store_id_label.as_str());
                continue;
            }

            let prev_safe_ts = state.store_safe_ts.get(&store_id).copied().unwrap_or(0);
            if prev_safe_ts > new_safe_ts {
                inc_safe_ts_update_counter("skip", store_id_label.as_str());
                set_min_safe_ts_gap_seconds(
                    store_id_label.as_str(),
                    safe_ts_gap_seconds(now_ms, prev_safe_ts),
                );
                continue;
            }

            state.store_safe_ts.insert(store_id, new_safe_ts);
            inc_safe_ts_update_counter("success", store_id_label.as_str());
            set_min_safe_ts_gap_seconds(
                store_id_label.as_str(),
                safe_ts_gap_seconds(now_ms, new_safe_ts),
            );
        }

        let mut zone_to_store_ids: HashMap<String, Vec<StoreId>> = HashMap::new();
        for store in &stores {
            let store_id = store.meta.id;
            if let Some(zone) = store
                .meta
                .labels
                .iter()
                .find(|label| label.key == ZONE_LABEL_KEY)
                .map(|label| label.value.clone())
            {
                zone_to_store_ids.entry(zone).or_default().push(store_id);
            }
        }

        let mut min_safe_ts = HashMap::with_capacity(zone_to_store_ids.len() + 1);
        min_safe_ts.insert(
            GLOBAL_TXN_SCOPE.to_owned(),
            compute_min_safe_ts(&state.store_safe_ts, &store_ids),
        );
        for (scope, store_ids) in zone_to_store_ids {
            min_safe_ts.insert(scope, compute_min_safe_ts(&state.store_safe_ts, &store_ids));
        }
        state.min_safe_ts = min_safe_ts;
        state.initialized = true;

        Ok(())
    }
}

fn is_valid_pd_safe_ts(safe_ts: u64) -> bool {
    safe_ts != 0 && safe_ts != u64::MAX
}

fn epoch_millis() -> i128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i128,
        Err(_) => 0,
    }
}

fn safe_ts_gap_seconds(now_ms: i128, safe_ts: u64) -> f64 {
    let safe_ms = (safe_ts >> SAFE_TS_PHYSICAL_SHIFT_BITS) as i128;
    (now_ms - safe_ms) as f64 / 1000.0
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
    use std::time::Duration;

    use async_trait::async_trait;
    use serial_test::serial;
    use tokio::sync::Notify;

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

    fn label_value<'a>(metric: &'a prometheus::proto::Metric, name: &str) -> Option<&'a str> {
        metric
            .get_label()
            .iter()
            .find(|pair| pair.get_name() == name)
            .map(|pair| pair.get_value())
    }

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

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
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

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
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

    #[derive(Clone)]
    struct BlockingSafeTsPdClient {
        called: Arc<Notify>,
        ready: Arc<Notify>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PdClient for BlockingSafeTsPdClient {
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

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }

        async fn all_stores(&self) -> Result<Vec<Store>> {
            Ok(Vec::new())
        }

        async fn all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.called.notify_one();
            self.ready.notified().await;
            Err(Error::StringError("injected safe-ts error".to_owned()))
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

    fn store_for_safe_ts_scripted(
        store_id: StoreId,
        labels: Vec<metapb::StoreLabel>,
        safe_ts: Vec<u64>,
    ) -> Store {
        let safe_ts = Arc::new(safe_ts);
        let cursor = Arc::new(AtomicUsize::new(0));

        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>().is_some() {
                let idx = cursor.fetch_add(1, Ordering::SeqCst);
                let safe_ts = safe_ts
                    .get(idx)
                    .copied()
                    .or_else(|| safe_ts.last().copied())
                    .unwrap_or(0);
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

    fn store_for_safe_ts_error(store_id: StoreId, labels: Vec<metapb::StoreLabel>) -> Store {
        let kv_client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if req.downcast_ref::<kvrpcpb::StoreSafeTsRequest>().is_some() {
                return Err(Error::StringError("injected safe-ts error".to_owned()));
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

    #[tokio::test]
    async fn test_safe_ts_refresh_dedupes_concurrent_calls() -> Result<()> {
        let store_safe_ts_calls = Arc::new(AtomicUsize::new(0));
        let tikv_dc1 = store_for_safe_ts_with_counter(
            1,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
            100,
            store_safe_ts_calls.clone(),
        );
        let tikv_dc2 = store_for_safe_ts_with_counter(
            2,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc2".to_owned(),
            }],
            10,
            store_safe_ts_calls.clone(),
        );

        let pd_client = Arc::new(SafeTsPdClient {
            stores: vec![tikv_dc1, tikv_dc2],
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);

        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let safe_ts = safe_ts.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                safe_ts.refresh().await
            }));
        }
        barrier.wait().await;

        for handle in handles {
            handle.await.unwrap()?;
        }

        assert_eq!(
            store_safe_ts_calls.load(Ordering::SeqCst),
            2,
            "concurrent refresh calls should be singleflighted"
        );

        assert_eq!(safe_ts.min_safe_ts().await?, 10);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc1").await?, 100);
        assert_eq!(safe_ts.min_safe_ts_with_txn_scope("dc2").await?, 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_safe_ts_refresh_dedupes_concurrent_errors() -> Result<()> {
        let called = Arc::new(Notify::new());
        let ready = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));

        let pd_client = Arc::new(BlockingSafeTsPdClient {
            called: called.clone(),
            ready: ready.clone(),
            calls: calls.clone(),
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);

        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let safe_ts = safe_ts.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                safe_ts.refresh().await
            }));
        }
        barrier.wait().await;

        tokio::time::timeout(Duration::from_secs(1), called.notified())
            .await
            .expect("pd all_stores_for_safe_ts called");

        {
            let refresh = safe_ts.inner.refresh.lock().await;
            assert!(refresh.in_flight, "refresh should be singleflighted");
        }

        ready.notify_waiters();

        for handle in handles {
            let err = tokio::time::timeout(Duration::from_secs(1), handle)
                .await
                .expect("refresh task should finish")
                .unwrap()
                .unwrap_err();
            match err {
                Error::StringError(message) => assert_eq!(message, "injected safe-ts error"),
                other => panic!("expected StringError, got {other:?}"),
            }
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent refresh errors should be singleflighted"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_safe_ts_refresh_records_metrics_success() -> Result<()> {
        let store_safe_ts_calls = Arc::new(AtomicUsize::new(0));
        let tikv_dc1 = store_for_safe_ts_with_counter(
            111,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
            999,
            store_safe_ts_calls.clone(),
        );
        let tikv_dc2 = store_for_safe_ts_with_counter(
            222,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc2".to_owned(),
            }],
            999,
            store_safe_ts_calls.clone(),
        );

        let mut store_safe_ts = HashMap::new();
        store_safe_ts.insert(111, 100);
        store_safe_ts.insert(222, 10);

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

        let families = prometheus::gather();
        let counter = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_safets_update_counter")
            .expect("safets_update_counter not registered");

        let store_111_ok = counter.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("success")
                && label_value(metric, "store") == Some("111")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(store_111_ok, "expected safets success metric for store 111");

        let store_222_ok = counter.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("success")
                && label_value(metric, "store") == Some("222")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(store_222_ok, "expected safets success metric for store 222");

        let gap = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_min_safets_gap_seconds")
            .expect("min_safets_gap_seconds gauge not registered");
        let store_111_gap = gap.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("111")
                && metric.get_gauge().get_value().is_finite()
        });
        assert!(store_111_gap, "expected safets gap metric for store 111");

        let store_222_gap = gap.get_metric().iter().any(|metric| {
            label_value(metric, "store") == Some("222")
                && metric.get_gauge().get_value().is_finite()
        });
        assert!(store_222_gap, "expected safets gap metric for store 222");

        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_safe_ts_refresh_records_metrics_skip() -> Result<()> {
        let tikv_dc1 = store_for_safe_ts_scripted(
            333,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
            vec![100, 10],
        );

        let pd_client = Arc::new(SafeTsPdClient {
            stores: vec![tikv_dc1],
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);

        safe_ts.refresh().await?;
        safe_ts.refresh().await?;

        let families = prometheus::gather();
        let counter = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_safets_update_counter")
            .expect("safets_update_counter not registered");

        let store_333_skip = counter.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("skip")
                && label_value(metric, "store") == Some("333")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(store_333_skip, "expected safets skip metric for store 333");

        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_safe_ts_refresh_records_metrics_fail() -> Result<()> {
        let tikv_dc1 = store_for_safe_ts_error(
            444,
            vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "dc1".to_owned(),
            }],
        );

        let pd_client = Arc::new(SafeTsPdClient {
            stores: vec![tikv_dc1],
        });
        let safe_ts = SafeTsCache::new(pd_client, Keyspace::Disable);
        safe_ts.refresh().await?;

        let families = prometheus::gather();
        let counter = families
            .iter()
            .find(|family| family.get_name() == "tikv_client_safets_update_counter")
            .expect("safets_update_counter not registered");

        let store_444_fail = counter.get_metric().iter().any(|metric| {
            label_value(metric, "result") == Some("fail")
                && label_value(metric, "store") == Some("444")
                && metric.get_counter().get_value() >= 1.0
        });
        assert!(store_444_fail, "expected safets fail metric for store 444");

        Ok(())
    }
}
