// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rand::Rng;
use tokio::sync::Notify;
use tokio::sync::RwLock;

use crate::common::Error;
use crate::pd::Cluster;
use crate::pd::RetryClient;
use crate::pd::RetryClientTrait;
use crate::proto::metapb::Store;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats;
use crate::trace::{self, Category, TraceField};
use crate::Key;
use crate::Result;

const MAX_RETRY_WAITING_CONCURRENT_REQUEST: usize = 4;

fn duration_to_millis_saturating(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

fn now_epoch_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    if millis > u128::from(u64::MAX) {
        u64::MAX
    } else {
        millis as u64
    }
}

fn next_ttl_deadline_millis(now_ms: u64, ttl_ms: u64, jitter_ms: u64) -> u64 {
    if ttl_ms == 0 {
        return u64::MAX;
    }

    let jitter = if jitter_ms == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..jitter_ms)
    };
    now_ms.saturating_add(ttl_ms).saturating_add(jitter)
}

fn check_and_maybe_extend_ttl(
    ttl_deadline_ms: &AtomicU64,
    now_ms: u64,
    ttl_ms: u64,
    jitter_ms: u64,
) -> bool {
    if ttl_ms == 0 {
        return true;
    }

    let mut new_deadline = None;
    loop {
        let deadline = ttl_deadline_ms.load(Ordering::Acquire);
        if now_ms > deadline {
            return false;
        }
        // Only extend the TTL when the deadline is close enough to "now". This matches the
        // client-go behavior of skipping updates while within the jitter window.
        if deadline > now_ms.saturating_add(ttl_ms) {
            return true;
        }
        let candidate = *new_deadline
            .get_or_insert_with(|| next_ttl_deadline_millis(now_ms, ttl_ms, jitter_ms));
        if ttl_deadline_ms
            .compare_exchange(deadline, candidate, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
    }
}

struct RegionCacheMap {
    /// RegionVerID -> Region. It stores the concrete region caches.
    /// RegionVerID is the unique identifier of a region *across time*.
    ver_id_to_region: HashMap<RegionVerId, RegionWithLeader>,
    /// RegionVerId -> TTL deadline (epoch milliseconds).
    ///
    /// This mirrors client-go's region cache TTL behavior: entries are considered expired after
    /// being idle for roughly `region_cache_ttl` (plus `region_cache_ttl_jitter`), and refreshed
    /// from PD on next use.
    ver_id_to_ttl_deadline_ms: HashMap<RegionVerId, AtomicU64>,
    /// Start_key -> RegionVerID
    ///
    /// Invariant: there are no intersecting regions in the map at any time.
    key_to_ver_id: BTreeMap<Key, RegionVerId>,
    /// RegionID -> RegionVerID. Note: regions with identical ID doesn't necessarily
    /// mean they are the same, they can be different regions across time.
    id_to_ver_id: HashMap<RegionId, RegionVerId>,
    /// We don't want to spawn multiple queries querying a same region id. If a
    /// request is on its way, others will wait for its completion.
    on_my_way_id: HashMap<RegionId, Arc<Notify>>,
}

impl RegionCacheMap {
    fn new() -> RegionCacheMap {
        RegionCacheMap {
            ver_id_to_region: HashMap::new(),
            ver_id_to_ttl_deadline_ms: HashMap::new(),
            key_to_ver_id: BTreeMap::new(),
            id_to_ver_id: HashMap::new(),
            on_my_way_id: HashMap::new(),
        }
    }
}

pub struct RegionCache<Client = RetryClient<Cluster>> {
    region_cache: Arc<RwLock<RegionCacheMap>>,
    store_cache: Arc<RwLock<HashMap<StoreId, Store>>>,
    inner_client: Arc<Client>,
    region_cache_ttl_ms: u64,
    region_cache_ttl_jitter_ms: u64,
}

struct OnMyWayIdGuard {
    id: RegionId,
    notify: Arc<Notify>,
    region_cache: Arc<RwLock<RegionCacheMap>>,
    active: bool,
}

impl OnMyWayIdGuard {
    fn new(id: RegionId, notify: Arc<Notify>, region_cache: Arc<RwLock<RegionCacheMap>>) -> Self {
        Self {
            id,
            notify,
            region_cache,
            active: true,
        }
    }

    async fn finish(mut self) {
        let mut guard = self.region_cache.write().await;
        guard.on_my_way_id.remove(&self.id);
        drop(guard);
        self.notify.notify_waiters();
        self.active = false;
    }
}

impl Drop for OnMyWayIdGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let id = self.id;
        let notify = self.notify.clone();
        let region_cache = self.region_cache.clone();
        handle.spawn(async move {
            let mut guard = region_cache.write().await;
            guard.on_my_way_id.remove(&id);
            drop(guard);
            notify.notify_waiters();
        });
    }
}

impl<Client> RegionCache<Client> {
    pub fn new_with_ttl(
        inner_client: Arc<Client>,
        region_cache_ttl: Duration,
        region_cache_ttl_jitter: Duration,
    ) -> RegionCache<Client> {
        RegionCache {
            region_cache: Arc::new(RwLock::new(RegionCacheMap::new())),
            store_cache: Arc::new(RwLock::new(HashMap::new())),
            inner_client,
            region_cache_ttl_ms: duration_to_millis_saturating(region_cache_ttl),
            region_cache_ttl_jitter_ms: duration_to_millis_saturating(region_cache_ttl_jitter),
        }
    }
}

impl<C: RetryClientTrait + Send + Sync> RegionCache<C> {
    // Retrieve cache entry by key. If there's no entry, query PD and update cache.
    pub async fn get_region_by_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let trace_enabled = trace::is_category_enabled(Category::RegionCache);
        let region_cache_guard = self.region_cache.read().await;
        let mut expired_ver_id: Option<RegionVerId> = None;
        let res = {
            region_cache_guard
                .key_to_ver_id
                .range(..=key)
                .next_back()
                .map(|(x, y)| (x.clone(), y.clone()))
        };

        if let Some((_, candidate_region_ver_id)) = res {
            if let Some(region) = region_cache_guard
                .ver_id_to_region
                .get(&candidate_region_ver_id)
            {
                if region.contains(key) {
                    if self.region_cache_ttl_ms == 0 {
                        if trace_enabled {
                            trace::trace(
                                Category::RegionCache,
                                "region_cache.get_region_by_key.cache_hit",
                                &[TraceField::u64("regionID", region.id())],
                            );
                        }
                        return Ok(region.clone());
                    }

                    let now_ms = now_epoch_millis();
                    if let Some(ttl) = region_cache_guard
                        .ver_id_to_ttl_deadline_ms
                        .get(&candidate_region_ver_id)
                    {
                        if check_and_maybe_extend_ttl(
                            ttl,
                            now_ms,
                            self.region_cache_ttl_ms,
                            self.region_cache_ttl_jitter_ms,
                        ) {
                            if trace_enabled {
                                trace::trace(
                                    Category::RegionCache,
                                    "region_cache.get_region_by_key.cache_hit",
                                    &[TraceField::u64("regionID", region.id())],
                                );
                            }
                            return Ok(region.clone());
                        }
                    }

                    if trace_enabled {
                        trace::trace(
                            Category::RegionCache,
                            "region_cache.get_region_by_key.cache_expired",
                            &[TraceField::u64("regionID", region.id())],
                        );
                    }
                    expired_ver_id = Some(candidate_region_ver_id);
                }
            }
        }
        drop(region_cache_guard);
        let reason = if expired_ver_id.is_some() {
            "Expired:Normal"
        } else {
            "Missing"
        };
        if let Some(ver_id) = expired_ver_id {
            self.invalidate_region_cache(ver_id).await;
        } else if trace_enabled {
            let key_bytes: &[u8] = key.into();
            let key_len = u64::try_from(key_bytes.len()).unwrap_or(u64::MAX);
            trace::trace(
                Category::RegionCache,
                "region_cache.get_region_by_key.cache_miss",
                &[TraceField::u64("keyLen", key_len)],
            );
        }
        stats::inc_load_region_total("ByKey", reason);
        self.read_through_region_by_key(key.clone()).await
    }

    // Retrieve cache entry by RegionId. If there's no entry, query PD and update cache.
    pub async fn get_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        let trace_enabled = trace::is_category_enabled(Category::RegionCache);
        let started_at = Instant::now();
        for _ in 0..=MAX_RETRY_WAITING_CONCURRENT_REQUEST {
            let region_cache_guard = self.region_cache.read().await;
            let mut expired_ver_id: Option<RegionVerId> = None;

            // check cache
            if let Some(ver_id) = region_cache_guard.id_to_ver_id.get(&id) {
                if let Some(region) = region_cache_guard.ver_id_to_region.get(ver_id) {
                    if self.region_cache_ttl_ms == 0 {
                        if trace_enabled {
                            trace::trace(
                                Category::RegionCache,
                                "region_cache.get_region_by_id.cache_hit",
                                &[TraceField::u64("regionID", id)],
                            );
                        }
                        stats::region_cache_operation("get_region_by_id", true);
                        stats::observe_load_region_cache("get_region_by_id", started_at.elapsed());
                        return Ok(region.clone());
                    }

                    let now_ms = now_epoch_millis();
                    if let Some(ttl) = region_cache_guard.ver_id_to_ttl_deadline_ms.get(ver_id) {
                        if check_and_maybe_extend_ttl(
                            ttl,
                            now_ms,
                            self.region_cache_ttl_ms,
                            self.region_cache_ttl_jitter_ms,
                        ) {
                            if trace_enabled {
                                trace::trace(
                                    Category::RegionCache,
                                    "region_cache.get_region_by_id.cache_hit",
                                    &[TraceField::u64("regionID", id)],
                                );
                            }
                            stats::region_cache_operation("get_region_by_id", true);
                            stats::observe_load_region_cache(
                                "get_region_by_id",
                                started_at.elapsed(),
                            );
                            return Ok(region.clone());
                        }
                    }

                    if trace_enabled {
                        trace::trace(
                            Category::RegionCache,
                            "region_cache.get_region_by_id.cache_expired",
                            &[TraceField::u64("regionID", id)],
                        );
                    }
                    expired_ver_id = Some(ver_id.clone());
                }
            }

            // check concurrent requests
            let notify = region_cache_guard.on_my_way_id.get(&id).cloned();
            let notified = notify.as_ref().map(|notify| notify.notified());
            drop(region_cache_guard);

            let expired = expired_ver_id.is_some();
            if let Some(ver_id) = expired_ver_id {
                self.invalidate_region_cache(ver_id).await;
            }

            if let Some(n) = notified {
                n.await;
                continue;
            } else {
                if trace_enabled {
                    trace::trace(
                        Category::RegionCache,
                        "region_cache.get_region_by_id.cache_miss",
                        &[TraceField::u64("regionID", id)],
                    );
                }
                let reason = if expired { "Expired:Normal" } else { "Missing" };
                stats::inc_load_region_total("ByID", reason);
                let result = self.read_through_region_by_id(id).await;
                stats::region_cache_operation("get_region_by_id", result.is_ok());
                stats::observe_load_region_cache("get_region_by_id", started_at.elapsed());
                return result;
            }
        }
        let err = Error::StringError(format!(
            "Concurrent PD requests failed for {MAX_RETRY_WAITING_CONCURRENT_REQUEST} times"
        ));
        stats::region_cache_operation("get_region_by_id", false);
        stats::observe_load_region_cache("get_region_by_id", started_at.elapsed());
        Err(err)
    }

    pub async fn get_store_by_id(&self, id: StoreId) -> Result<Store> {
        let started_at = Instant::now();
        let store = self.store_cache.read().await.get(&id).cloned();
        let result = match store {
            Some(store) => Ok(store),
            None => self.read_through_store_by_id(id).await,
        };
        stats::region_cache_operation("get_store", result.is_ok());
        stats::observe_load_region_cache("get_store", started_at.elapsed());
        result
    }

    /// Force read through (query from PD) and update cache
    pub async fn read_through_region_by_key(&self, key: Key) -> Result<RegionWithLeader> {
        let trace_enabled = trace::is_category_enabled(Category::RegionCache);
        let started_at = Instant::now();
        if trace_enabled {
            let key_bytes: &[u8] = (&key).into();
            let key_len = u64::try_from(key_bytes.len()).unwrap_or(u64::MAX);
            trace::trace(
                Category::RegionCache,
                "region_cache.read_through_region_by_key.pd_request",
                &[TraceField::u64("keyLen", key_len)],
            );
        }
        let region = match self.inner_client.clone().get_region(key.into()).await {
            Ok(region) => {
                stats::region_cache_operation("get_region_when_miss", true);
                stats::observe_load_region_cache("get_region_when_miss", started_at.elapsed());
                region
            }
            Err(err) => {
                stats::region_cache_operation("get_region_when_miss", false);
                stats::observe_load_region_cache("get_region_when_miss", started_at.elapsed());
                return Err(err);
            }
        };
        if trace_enabled {
            trace::trace(
                Category::RegionCache,
                "region_cache.read_through_region_by_key.pd_response",
                &[TraceField::u64("regionID", region.id())],
            );
        }
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Preload region metadata from PD by scanning regions in key order.
    ///
    /// This is a best-effort helper intended to reduce cold-start latency by warming the local
    /// region cache (similar to client-go `EnablePreload` + `refreshRegionIndex`).
    ///
    /// - `start_key` and `end_key` should be in the same key encoding used by PD/TiKV (the caller
    ///   is responsible for applying any API version / keyspace / memcomparable encoding).
    /// - Regions without leaders are skipped (callers can still fall back to read-through).
    ///
    /// Returns the number of regions added to the cache.
    pub async fn preload_region_index(
        &self,
        mut start_key: Key,
        end_key: Key,
        limit: i32,
    ) -> Result<usize> {
        if limit <= 0 {
            return Ok(0);
        }

        let end_key_bytes: Vec<u8> = end_key.clone().into();
        let mut total = 0usize;

        loop {
            let scan_started_at = Instant::now();
            let regions = self
                .inner_client
                .clone()
                .scan_regions(start_key.clone().into(), end_key_bytes.clone(), limit)
                .await;
            stats::region_cache_operation("scan_regions", regions.is_ok());
            stats::observe_load_region_cache("scan_regions", scan_started_at.elapsed());
            let regions = regions?;
            if regions.is_empty() {
                break;
            }

            for region in &regions {
                if region.leader.is_none() {
                    continue;
                }
                self.add_region(region.clone()).await;
                total += 1;
            }

            let Some(last_end_key) = regions.last().map(RegionWithLeader::end_key) else {
                break;
            };
            if last_end_key.is_empty() {
                break;
            }
            if last_end_key == start_key {
                break;
            }
            if !end_key_bytes.is_empty() && last_end_key >= end_key {
                break;
            }
            start_key = last_end_key;
        }

        Ok(total)
    }

    /// Force read through (query from PD) and update cache
    async fn read_through_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        let trace_enabled = trace::is_category_enabled(Category::RegionCache);
        if trace_enabled {
            trace::trace(
                Category::RegionCache,
                "region_cache.read_through_region_by_id.pd_request",
                &[TraceField::u64("regionID", id)],
            );
        }
        // put a notify to let others know the region id is being queried
        let notify = Arc::new(Notify::new());
        {
            let mut region_cache_guard = self.region_cache.write().await;
            region_cache_guard.on_my_way_id.insert(id, notify.clone());
        }

        let cleanup_guard = OnMyWayIdGuard::new(id, notify.clone(), self.region_cache.clone());

        let result = match self.inner_client.clone().get_region_by_id(id).await {
            Ok(region) => {
                if trace_enabled {
                    trace::trace(
                        Category::RegionCache,
                        "region_cache.read_through_region_by_id.pd_response",
                        &[TraceField::u64("regionID", region.id())],
                    );
                }
                self.add_region(region.clone()).await;
                Ok(region)
            }
            Err(err) => Err(err),
        };

        cleanup_guard.finish().await;

        result
    }

    async fn read_through_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.inner_client.clone().get_store(id).await?;
        self.store_cache.write().await.insert(id, store.clone());
        Ok(store)
    }

    pub async fn add_region(&self, region: RegionWithLeader) {
        // This takes a write lock because we update multiple indices consistently. This runs on
        // region-cache update paths (e.g. cache miss), not the hot-path request routing.
        let mut cache = self.region_cache.write().await;
        let new_ver_id = region.ver_id();
        if let Some(old_ver_id) = cache.id_to_ver_id.get(&new_ver_id.id) {
            // Like client-go, ignore stale region info that arrives out-of-order or from a PD
            // follower that lags behind. These stale entries can otherwise overwrite newer cached
            // regions.
            if old_ver_id.ver > new_ver_id.ver || old_ver_id.conf_ver > new_ver_id.conf_ver {
                stats::inc_stale_region_from_pd_counter();
                return;
            }
        }

        let now_ms = now_epoch_millis();
        let ttl_deadline_ms = next_ttl_deadline_millis(
            now_ms,
            self.region_cache_ttl_ms,
            self.region_cache_ttl_jitter_ms,
        );

        let end_key = region.end_key();
        let mut to_be_removed: HashSet<RegionVerId> = HashSet::new();

        if let Some(ver_id) = cache.id_to_ver_id.get(&new_ver_id.id) {
            if ver_id != &new_ver_id {
                to_be_removed.insert(ver_id.clone());
            }
        }

        let mut search_range = {
            if end_key.is_empty() {
                cache.key_to_ver_id.range(..)
            } else {
                cache.key_to_ver_id.range(..end_key)
            }
        };
        while let Some((_, ver_id_in_cache)) = search_range.next_back() {
            match cache.ver_id_to_region.get(ver_id_in_cache) {
                Some(region_in_cache) => {
                    if region_in_cache.region.end_key > region.region.start_key {
                        if region_in_cache.ver_id().ver > new_ver_id.ver {
                            stats::inc_stale_region_from_pd_counter();
                            return;
                        }
                        to_be_removed.insert(ver_id_in_cache.clone());
                    } else {
                        break;
                    }
                }
                None => {
                    // Inconsistent internal state: key-to-ver-id points to a missing region.
                    // Treat it as stale and remove it.
                    to_be_removed.insert(ver_id_in_cache.clone());
                }
            }
        }

        let mut stale_ver_ids = HashSet::new();
        for ver_id in to_be_removed {
            cache.ver_id_to_ttl_deadline_ms.remove(&ver_id);
            match cache.ver_id_to_region.remove(&ver_id) {
                Some(region_to_remove) => {
                    cache.key_to_ver_id.remove(&region_to_remove.start_key());
                    cache.id_to_ver_id.remove(&region_to_remove.id());
                }
                None => {
                    stale_ver_ids.insert(ver_id);
                }
            }
        }
        if !stale_ver_ids.is_empty() {
            cache
                .key_to_ver_id
                .retain(|_, ver_id| !stale_ver_ids.contains(ver_id));
            cache
                .id_to_ver_id
                .retain(|_, ver_id| !stale_ver_ids.contains(ver_id));
        }
        let ver_id = new_ver_id;
        cache
            .key_to_ver_id
            .insert(region.start_key(), ver_id.clone());
        cache.id_to_ver_id.insert(region.id(), ver_id.clone());
        cache.ver_id_to_region.insert(ver_id.clone(), region);
        cache
            .ver_id_to_ttl_deadline_ms
            .insert(ver_id, AtomicU64::new(ttl_deadline_ms));
    }

    pub async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        let mut cache = self.region_cache.write().await;
        let region_entry = cache.ver_id_to_region.get_mut(&ver_id);
        if let Some(region) = region_entry {
            region.leader = Some(leader);
        }

        Ok(())
    }

    pub async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        stats::region_cache_operation("invalidate_region_from_cache", true);
        trace::trace_if_enabled(
            Category::RegionCache,
            "region_cache.invalidate_region_cache",
            || {
                vec![
                    TraceField::u64("regionID", ver_id.id),
                    TraceField::u64("confVer", ver_id.conf_ver),
                    TraceField::u64("ver", ver_id.ver),
                ]
            },
        );
        let mut cache = self.region_cache.write().await;
        cache.ver_id_to_ttl_deadline_ms.remove(&ver_id);
        if let Some(region) = cache.ver_id_to_region.remove(&ver_id) {
            cache.id_to_ver_id.remove(&region.id());
            cache.key_to_ver_id.remove(&region.start_key());
        } else {
            cache
                .key_to_ver_id
                .retain(|_, cached_ver_id| cached_ver_id != &ver_id);
            cache
                .id_to_ver_id
                .retain(|_, cached_ver_id| cached_ver_id != &ver_id);
        }
    }

    pub async fn invalidate_store_cache(&self, store_id: StoreId) {
        stats::region_cache_operation("invalidate_store_regions", true);
        let mut cache = self.store_cache.write().await;
        cache.remove(&store_id);
    }

    pub async fn read_through_all_stores(&self) -> Result<Vec<Store>> {
        let stores = self
            .inner_client
            .clone()
            .get_all_stores()
            .await?
            .into_iter()
            .filter(is_valid_tikv_store)
            .collect::<Vec<_>>();
        for store in &stores {
            self.store_cache
                .write()
                .await
                .insert(store.id, store.clone());
        }
        Ok(stores)
    }

    pub async fn read_through_all_stores_for_safe_ts(&self) -> Result<Vec<Store>> {
        let stores = self
            .inner_client
            .clone()
            .get_all_stores()
            .await?
            .into_iter()
            .filter(is_valid_safe_ts_store)
            .collect::<Vec<_>>();
        Ok(stores)
    }
}

const ENGINE_LABEL_KEY: &str = "engine";
const ENGINE_LABEL_TIFLASH: &str = "tiflash";
const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";
const ENGINE_LABEL_TIFLASH_MPP: &str = "tiflash_mpp";

fn is_valid_store(store: &metapb::Store) -> bool {
    store.state != metapb::StoreState::Tombstone as i32
}

fn is_valid_safe_ts_store(store: &metapb::Store) -> bool {
    is_valid_store(store) && !is_tiflash_compute_store(store)
}

pub(crate) fn is_tiflash_store(store: &metapb::Store) -> bool {
    store
        .labels
        .iter()
        .any(|label| label.key == ENGINE_LABEL_KEY && label.value == ENGINE_LABEL_TIFLASH)
}

pub(crate) fn is_tiflash_compute_store(store: &metapb::Store) -> bool {
    store.labels.iter().any(|label| {
        label.key == ENGINE_LABEL_KEY
            && (label.value == ENGINE_LABEL_TIFLASH_COMPUTE
                || label.value == ENGINE_LABEL_TIFLASH_MPP)
    })
}

pub(crate) fn is_tiflash_related_store(store: &metapb::Store) -> bool {
    is_tiflash_store(store) || is_tiflash_compute_store(store)
}

fn is_valid_tikv_store(store: &metapb::Store) -> bool {
    is_valid_store(store) && !is_tiflash_related_store(store)
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use serial_test::serial;
    use tokio::sync::Mutex;

    use super::RegionCache;
    use crate::common::Error;
    use crate::pd::RetryClientTrait;
    use crate::proto::keyspacepb;
    use crate::proto::metapb::RegionEpoch;
    use crate::proto::metapb::{self};
    use crate::region::RegionId;
    use crate::region::RegionWithLeader;
    use crate::region_cache::is_tiflash_compute_store;
    use crate::region_cache::is_tiflash_related_store;
    use crate::region_cache::is_tiflash_store;
    use crate::region_cache::is_valid_tikv_store;
    use crate::Key;
    use crate::Result;

    type ScanRegionsCall = (Vec<u8>, Vec<u8>, i32);

    struct TraceHookReset;

    impl Drop for TraceHookReset {
        fn drop(&mut self) {
            crate::trace::set_trace_event_func(None);
            crate::trace::set_is_category_enabled_func(None);
        }
    }

    fn trace_field_u64(fields: &[crate::trace::TraceField], key: &str) -> Option<u64> {
        fields
            .iter()
            .find(|field| field.key == key)
            .and_then(|field| match &field.value {
                crate::trace::TraceValue::U64(value) => Some(*value),
                _ => None,
            })
    }

    fn stale_region_from_pd_counter_value() -> f64 {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_stale_region_from_pd")
            .and_then(|family| {
                family
                    .get_metric()
                    .get(0)
                    .map(|metric| metric.get_counter().get_value())
            })
            .unwrap_or(0.0)
    }

    fn load_region_total_counter_value(tag: &str, reason: &str) -> f64 {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_rust_load_region_total")
            .and_then(|family| {
                family.get_metric().iter().find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|pair| pair.get_name() == "type" && pair.get_value() == tag)
                        && metric
                            .get_label()
                            .iter()
                            .any(|pair| pair.get_name() == "reason" && pair.get_value() == reason)
                })
            })
            .map(|metric| metric.get_counter().get_value())
            .unwrap_or(0.0)
    }

    #[derive(Default)]
    struct MockRetryClient {
        pub regions: Mutex<HashMap<RegionId, RegionWithLeader>>,
        pub stores: Mutex<Vec<metapb::Store>>,
        pub get_region_count: AtomicU64,
        pub get_store_count: AtomicU64,
        pub scan_regions_count: AtomicU64,
        pub scan_regions_calls: Mutex<Vec<ScanRegionsCall>>,
    }

    #[async_trait]
    impl RetryClientTrait for MockRetryClient {
        async fn get_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(_, r)| r.contains(&key.clone().into()))
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id == &&region_id)
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn scan_regions(
            self: Arc<Self>,
            start_key: Vec<u8>,
            end_key: Vec<u8>,
            limit: i32,
        ) -> Result<Vec<RegionWithLeader>> {
            self.scan_regions_count.fetch_add(1, SeqCst);
            self.scan_regions_calls
                .lock()
                .await
                .push((start_key.clone(), end_key.clone(), limit));

            if limit <= 0 {
                return Ok(Vec::new());
            }

            let start_key = Key::from(start_key);
            let end_key = Key::from(end_key);

            let mut regions: Vec<RegionWithLeader> =
                self.regions.lock().await.values().cloned().collect();
            regions.sort_by(|left, right| left.region.start_key.cmp(&right.region.start_key));

            let Some(start_idx) = regions
                .iter()
                .position(|region| region.contains(&start_key))
            else {
                return Ok(Vec::new());
            };

            let mut out = Vec::new();
            for region in regions.into_iter().skip(start_idx) {
                if !end_key.is_empty() && region.start_key() >= end_key {
                    break;
                }
                out.push(region);
                if out.len() >= limit as usize {
                    break;
                }
            }

            Ok(out)
        }

        async fn get_store(
            self: Arc<Self>,
            id: crate::region::StoreId,
        ) -> Result<crate::proto::metapb::Store> {
            self.get_store_count.fetch_add(1, SeqCst);
            self.stores
                .lock()
                .await
                .iter()
                .find(|store| store.id == id)
                .cloned()
                .ok_or_else(|| Error::StringError("MockRetryClient: store not found".to_owned()))
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<crate::proto::metapb::Store>> {
            Ok(self.stores.lock().await.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            Err(Error::Unimplemented)
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
            Err(Error::Unimplemented)
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            Err(Error::Unimplemented)
        }
    }

    #[tokio::test]
    async fn cache_is_used() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);
        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
            },
        );
        retry_client.regions.lock().await.insert(
            2,
            RegionWithLeader {
                region: metapb::Region {
                    id: 2,
                    start_key: vec![101],
                    end_key: vec![],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 2,
                    ..Default::default()
                }),
            },
        );

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // first query, read through
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // should read from cache
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // invalidate, should read through
        cache
            .invalidate_region_cache(cache.get_region_by_id(1).await?.ver_id())
            .await;
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        // update leader should work
        cache
            .update_leader(
                cache.get_region_by_id(2).await?.ver_id(),
                metapb::Peer {
                    store_id: 102,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            cache.get_region_by_id(2).await?.leader.unwrap().store_id,
            102
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_trace_emits_hit_and_miss_events() -> Result<()> {
        let _lock = crate::trace::TRACE_HOOK_TEST_LOCK.lock().await;
        let _reset = TraceHookReset;

        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
            crate::trace::Category,
            String,
            Vec<crate::trace::TraceField>,
        )>::new()));

        let seen_event = seen.clone();
        let event: crate::trace::TraceEventFunc =
            std::sync::Arc::new(move |category, name, fields| {
                if category != crate::trace::Category::RegionCache {
                    return;
                }

                match name {
                    "region_cache.get_region_by_key.cache_miss"
                    | "region_cache.read_through_region_by_key.pd_request" => {
                        if trace_field_u64(fields, "keyLen") != Some(77) {
                            return;
                        }
                    }
                    "region_cache.read_through_region_by_key.pd_response"
                    | "region_cache.get_region_by_key.cache_hit" => {
                        if trace_field_u64(fields, "regionID") != Some(424_242) {
                            return;
                        }
                    }
                    _ => return,
                }

                seen_event
                    .lock()
                    .unwrap()
                    .push((category, name.to_owned(), fields.to_vec()));
            });
        crate::trace::set_trace_event_func(Some(event));

        let enabled: crate::trace::IsCategoryEnabledFunc =
            std::sync::Arc::new(|category| category == crate::trace::Category::RegionCache);
        crate::trace::set_is_category_enabled_func(Some(enabled));

        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);
        retry_client
            .regions
            .lock()
            .await
            .insert(424_242, region(424_242, vec![], vec![]));

        let key: Key = vec![42; 77].into();

        // Miss triggers PD request/response and populates cache.
        cache.get_region_by_key(&key).await?;
        // Hit should be served from cache.
        cache.get_region_by_key(&key).await?;

        let events = seen.lock().unwrap().clone();
        let names: Vec<String> = events.iter().map(|(_, name, _)| name.clone()).collect();
        assert_eq!(
            names,
            vec![
                "region_cache.get_region_by_key.cache_miss",
                "region_cache.read_through_region_by_key.pd_request",
                "region_cache.read_through_region_by_key.pd_response",
                "region_cache.get_region_by_key.cache_hit",
            ]
        );

        assert_eq!(events[0].0, crate::trace::Category::RegionCache);
        assert_eq!(trace_field_u64(&events[0].2, "keyLen"), Some(77));

        assert_eq!(events[2].0, crate::trace::Category::RegionCache);
        assert_eq!(trace_field_u64(&events[2].2, "regionID"), Some(424_242));

        assert_eq!(events[3].0, crate::trace::Category::RegionCache);
        assert_eq!(trace_field_u64(&events[3].2, "regionID"), Some(424_242));

        Ok(())
    }

    #[tokio::test]
    async fn test_preload_region_index_populates_cache_and_skips_no_leader() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
            },
        );
        retry_client.regions.lock().await.insert(
            2,
            RegionWithLeader {
                region: metapb::Region {
                    id: 2,
                    start_key: vec![100],
                    end_key: vec![200],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: None,
            },
        );
        retry_client.regions.lock().await.insert(
            3,
            RegionWithLeader {
                region: metapb::Region {
                    id: 3,
                    start_key: vec![200],
                    end_key: vec![],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 3,
                    ..Default::default()
                }),
            },
        );

        let loaded = cache
            .preload_region_index(Key::EMPTY, Key::EMPTY, 2)
            .await?;
        assert_eq!(loaded, 2);
        assert_eq!(retry_client.scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // Region with leader should be served from cache.
        assert_eq!(cache.get_region_by_id(1).await?.id(), 1);
        assert_eq!(cache.get_region_by_key(&Key::from(vec![1])).await?.id(), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // Region without leader should not be preloaded.
        assert_eq!(cache.get_region_by_id(2).await?.id(), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_ttl_expires_and_refreshes() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_millis(20),
            Duration::ZERO,
        );
        retry_client
            .regions
            .lock()
            .await
            .insert(1, region(1, vec![], vec![]));

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // First query reads through and initializes the cache.
        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // Should read from cache, without contacting PD again.
        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // Wait for the cache entry to expire, then ensure it is refreshed.
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.get_region_by_id(1).await?;
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_region_by_id_does_not_deadlock_when_read_through_fails() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let (r1, r2) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(cache.get_region_by_id(42), cache.get_region_by_id(42))
        })
        .await
        .expect("get_region_by_id should not hang on read-through errors");
        assert!(r1.is_err());
        assert!(r2.is_err());

        let guard = cache.region_cache.read().await;
        assert!(guard.on_my_way_id.is_empty());
    }

    #[tokio::test]
    async fn test_tolerate_inconsistent_cache_maps() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region(1, vec![], vec![10]);
        retry_client.regions.lock().await.insert(1, region1.clone());

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);
        assert_eq!(
            cache.get_region_by_key(&vec![5].into()).await?,
            region1.clone()
        );
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // Corrupt internal maps so lookups must tolerate missing region entries.
        {
            let mut guard = cache.region_cache.write().await;
            guard.ver_id_to_region.clear();
        }

        assert_eq!(cache.get_region_by_key(&vec![5].into()).await?.id(), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        {
            let mut guard = cache.region_cache.write().await;
            guard.ver_id_to_region.clear();
        }

        assert_eq!(cache.get_region_by_id(1).await?.id(), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_disjoint_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;

        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region1);
        expected_cache.insert(vec![10].into(), region2);
        expected_cache.insert(vec![30].into(), region3);

        assert(&cache, &expected_cache).await
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_add_region_ignores_stale_region_from_pd_and_records_metrics() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let mut region_newer = region(1, vec![], vec![10]);
        region_newer.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 2,
        });
        cache.add_region(region_newer).await;

        let before = stale_region_from_pd_counter_value();

        let mut stale_same_id = region(1, vec![], vec![10]);
        stale_same_id.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 1,
        });
        cache.add_region(stale_same_id).await;

        let mut cached_intersecting = region(2, vec![10], vec![20]);
        cached_intersecting.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 5,
        });
        cache.add_region(cached_intersecting).await;

        let mut stale_intersecting = region(3, vec![], vec![15]);
        stale_intersecting.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 3,
        });
        cache.add_region(stale_intersecting).await;

        let after = stale_region_from_pd_counter_value();
        assert!(
            after >= before + 2.0,
            "expected stale_region_from_pd counter to increase"
        );

        let guard = cache.region_cache.read().await;
        let region1_ver = guard.id_to_ver_id.get(&1).expect("region 1 missing");
        assert_eq!(region1_ver.ver, 2);
        assert!(
            !guard.id_to_ver_id.contains_key(&3),
            "expected stale region not to be inserted"
        );
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_region_cache_load_region_total_records_missing_and_expired() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        retry_client
            .regions
            .lock()
            .await
            .insert(1, region(1, vec![], vec![100]));
        retry_client
            .regions
            .lock()
            .await
            .insert(2, region(2, vec![100], vec![]));

        let cache =
            RegionCache::new_with_ttl(retry_client.clone(), Duration::from_secs(1), Duration::ZERO);

        let before_by_key_missing = load_region_total_counter_value("ByKey", "Missing");
        let region1 = cache.get_region_by_key(&vec![10].into()).await?;
        assert_eq!(region1.id(), 1);
        let after_by_key_missing = load_region_total_counter_value("ByKey", "Missing");
        assert!(
            after_by_key_missing >= before_by_key_missing + 1.0,
            "expected load_region_total(ByKey,Missing) to increase"
        );

        {
            let guard = cache.region_cache.read().await;
            let ttl_deadline = guard
                .ver_id_to_ttl_deadline_ms
                .get(&region1.ver_id())
                .expect("region 1 ttl missing");
            ttl_deadline.store(0, SeqCst);
        }

        let before_by_key_expired = load_region_total_counter_value("ByKey", "Expired:Normal");
        let region1 = cache.get_region_by_key(&vec![10].into()).await?;
        assert_eq!(region1.id(), 1);
        let after_by_key_expired = load_region_total_counter_value("ByKey", "Expired:Normal");
        assert!(
            after_by_key_expired >= before_by_key_expired + 1.0,
            "expected load_region_total(ByKey,Expired:Normal) to increase"
        );

        let before_by_id_missing = load_region_total_counter_value("ByID", "Missing");
        let region2 = cache.get_region_by_id(2).await?;
        assert_eq!(region2.id(), 2);
        let after_by_id_missing = load_region_total_counter_value("ByID", "Missing");
        assert!(
            after_by_id_missing >= before_by_id_missing + 1.0,
            "expected load_region_total(ByID,Missing) to increase"
        );

        {
            let guard = cache.region_cache.read().await;
            let ttl_deadline = guard
                .ver_id_to_ttl_deadline_ms
                .get(&region2.ver_id())
                .expect("region 2 ttl missing");
            ttl_deadline.store(0, SeqCst);
        }

        let before_by_id_expired = load_region_total_counter_value("ByID", "Expired:Normal");
        let region2 = cache.get_region_by_id(2).await?;
        assert_eq!(region2.id(), 2);
        let after_by_id_expired = load_region_total_counter_value("ByID", "Expired:Normal");
        assert!(
            after_by_id_expired >= before_by_id_expired + 1.0,
            "expected load_region_total(ByID,Expired:Normal) to increase"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_intersecting_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;
        cache.add_region(region(3, vec![30], vec![40])).await;
        cache.add_region(region(4, vec![50], vec![60])).await;
        cache.add_region(region(5, vec![20], vec![35])).await;

        let mut expected_cache: BTreeMap<Key, _> = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![10].into(), region(2, vec![10], vec![20]));
        expected_cache.insert(vec![20].into(), region(5, vec![20], vec![35]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(6, vec![15], vec![25])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![15].into(), region(6, vec![15], vec![25]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(7, vec![20], vec![])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(8, vec![], vec![15])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(8, vec![], vec![15]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;
    }

    #[tokio::test]
    async fn test_get_region_by_key() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![40]);
        let region4 = region(4, vec![50], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;
        cache.add_region(region4.clone()).await;

        assert_eq!(
            cache.get_region_by_key(&vec![].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![5].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![10].into()).await?,
            region2.clone()
        );
        assert!(cache.get_region_by_key(&vec![20].into()).await.is_err());
        assert!(cache.get_region_by_key(&vec![25].into()).await.is_err());
        assert_eq!(cache.get_region_by_key(&vec![60].into()).await?, region4);
        Ok(())
    }

    // a helper function to assert the cache is in expected state
    async fn assert(
        cache: &RegionCache<MockRetryClient>,
        expected_cache: &BTreeMap<Key, RegionWithLeader>,
    ) {
        let guard = cache.region_cache.read().await;
        let mut actual_keys = guard.ver_id_to_region.values().collect::<Vec<_>>();
        let mut expected_keys = expected_cache.values().collect::<Vec<_>>();
        actual_keys.sort_by_cached_key(|r| r.id());
        expected_keys.sort_by_cached_key(|r| r.id());

        assert_eq!(actual_keys, expected_keys);
        assert_eq!(
            guard.key_to_ver_id.keys().collect::<HashSet<_>>(),
            expected_cache.keys().collect::<HashSet<_>>()
        )
    }

    fn region(id: RegionId, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = id;
        region.region.start_key = start_key;
        region.region.end_key = end_key;
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });
        // We don't care about other fields here

        region
    }

    #[tokio::test]
    async fn test_read_through_all_stores_for_safe_ts_includes_tiflash() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let tikv_store = metapb::Store {
            id: 1,
            state: metapb::StoreState::Up.into(),
            address: "tikv".to_owned(),
            ..Default::default()
        };

        let mut tiflash_store = metapb::Store {
            id: 2,
            state: metapb::StoreState::Up.into(),
            address: "tiflash".to_owned(),
            peer_address: "tiflash-peer".to_owned(),
            ..Default::default()
        };
        tiflash_store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });

        let mut tiflash_compute_store = metapb::Store {
            id: 4,
            state: metapb::StoreState::Up.into(),
            address: "tiflash_compute".to_owned(),
            peer_address: "tiflash_compute-peer".to_owned(),
            ..Default::default()
        };
        tiflash_compute_store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash_compute".to_owned(),
        });

        let tombstone_store = metapb::Store {
            id: 3,
            state: metapb::StoreState::Tombstone.into(),
            address: "tombstone".to_owned(),
            ..Default::default()
        };

        *retry_client.stores.lock().await = vec![
            tikv_store.clone(),
            tiflash_store,
            tiflash_compute_store,
            tombstone_store,
        ];

        let all_stores = cache.read_through_all_stores().await?;
        let mut all_store_ids = all_stores.iter().map(|store| store.id).collect::<Vec<_>>();
        all_store_ids.sort_unstable();
        assert_eq!(all_store_ids, vec![1]);

        let safe_ts_stores = cache.read_through_all_stores_for_safe_ts().await?;
        let mut safe_ts_store_ids = safe_ts_stores
            .iter()
            .map(|store| store.id)
            .collect::<Vec<_>>();
        safe_ts_store_ids.sort_unstable();
        assert_eq!(safe_ts_store_ids, vec![1, 2]);
        assert!(safe_ts_stores
            .iter()
            .any(|store| store.id == 2 && is_tiflash_store(store)));
        assert!(
            !safe_ts_stores.iter().any(|store| store.id == 4),
            "safe-ts store list must exclude tiflash_compute stores"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_invalidate_store_cache_forces_store_meta_refresh() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let store_id = 1;
        let mut store = metapb::Store {
            id: store_id,
            state: metapb::StoreState::Up.into(),
            address: "old".to_owned(),
            ..Default::default()
        };
        *retry_client.stores.lock().await = vec![store.clone()];

        let cached = cache.get_store_by_id(store_id).await?;
        assert_eq!(cached.address, "old");
        assert_eq!(retry_client.get_store_count.load(SeqCst), 1);

        store.address = "new".to_owned();
        *retry_client.stores.lock().await = vec![store.clone()];

        let cached = cache.get_store_by_id(store_id).await?;
        assert_eq!(cached.address, "old");
        assert_eq!(retry_client.get_store_count.load(SeqCst), 1);

        cache.invalidate_store_cache(store_id).await;

        let refreshed = cache.get_store_by_id(store_id).await?;
        assert_eq!(refreshed.address, "new");
        assert_eq!(retry_client.get_store_count.load(SeqCst), 2);

        Ok(())
    }

    #[test]
    fn test_is_valid_tikv_store() {
        let mut store = metapb::Store::default();
        assert!(is_valid_tikv_store(&store));
        assert!(!is_tiflash_store(&store));
        assert!(!is_tiflash_compute_store(&store));
        assert!(!is_tiflash_related_store(&store));

        store.state = metapb::StoreState::Tombstone.into();
        assert!(!is_valid_tikv_store(&store));

        store.state = metapb::StoreState::Up.into();
        assert!(is_valid_tikv_store(&store));

        store.state = 123;
        assert!(is_valid_tikv_store(&store));
        store.state = metapb::StoreState::Up.into();

        store.labels.push(metapb::StoreLabel {
            key: "some_key".to_owned(),
            value: "some_value".to_owned(),
        });
        assert!(is_valid_tikv_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });
        assert!(!is_valid_tikv_store(&store));
        assert!(is_tiflash_store(&store));
        assert!(!is_tiflash_compute_store(&store));
        assert!(is_tiflash_related_store(&store));

        store.labels[1].value = "tiflash_compute".to_string();
        assert!(!is_valid_tikv_store(&store));
        assert!(!is_tiflash_store(&store));
        assert!(is_tiflash_compute_store(&store));
        assert!(is_tiflash_related_store(&store));

        store.labels[1].value = "tiflash_mpp".to_string();
        assert!(!is_valid_tikv_store(&store));
        assert!(!is_tiflash_store(&store));
        assert!(is_tiflash_compute_store(&store));
        assert!(is_tiflash_related_store(&store));

        store.labels[1].value = "other".to_string();
        assert!(is_valid_tikv_store(&store));
        assert!(!is_tiflash_store(&store));
        assert!(!is_tiflash_compute_store(&store));
        assert!(!is_tiflash_related_store(&store));
    }
}
