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

        let stores = self.pd.all_stores().await?;
        if stores.is_empty() {
            let mut state = self.state.write().await;
            state.initialized = true;
            state.min_safe_ts.clear();
            return Ok(());
        }

        let request = kvrpcpb::StoreSafeTsRequest {
            key_range: Some(kvrpcpb::KeyRange::default()),
        };
        let plan = PlanBuilder::new(self.pd.clone(), self.keyspace, request)
            .stores(stores.clone(), DEFAULT_STORE_BACKOFF)
            .plan();
        let results = plan.execute().await?;

        let mut state = self.state.write().await;
        for (store, result) in stores.iter().zip(results) {
            let store_id = store.meta.id;
            let new_safe_ts = match result {
                Ok(resp) => resp.safe_ts,
                Err(_) => continue,
            };

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
