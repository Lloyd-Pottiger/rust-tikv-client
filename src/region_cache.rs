// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
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
    region_cache: RwLock<RegionCacheMap>,
    store_cache: RwLock<HashMap<StoreId, Store>>,
    inner_client: Arc<Client>,
    region_cache_ttl_ms: u64,
    region_cache_ttl_jitter_ms: u64,
}

impl<Client> RegionCache<Client> {
    pub fn new_with_ttl(
        inner_client: Arc<Client>,
        region_cache_ttl: Duration,
        region_cache_ttl_jitter: Duration,
    ) -> RegionCache<Client> {
        RegionCache {
            region_cache: RwLock::new(RegionCacheMap::new()),
            store_cache: RwLock::new(HashMap::new()),
            inner_client,
            region_cache_ttl_ms: duration_to_millis_saturating(region_cache_ttl),
            region_cache_ttl_jitter_ms: duration_to_millis_saturating(region_cache_ttl_jitter),
        }
    }
}

impl<C: RetryClientTrait> RegionCache<C> {
    // Retrieve cache entry by key. If there's no entry, query PD and update cache.
    pub async fn get_region_by_key(&self, key: &Key) -> Result<RegionWithLeader> {
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
                            return Ok(region.clone());
                        }
                    }

                    expired_ver_id = Some(candidate_region_ver_id);
                }
            }
        }
        drop(region_cache_guard);
        if let Some(ver_id) = expired_ver_id {
            self.invalidate_region_cache(ver_id).await;
        }
        self.read_through_region_by_key(key.clone()).await
    }

    // Retrieve cache entry by RegionId. If there's no entry, query PD and update cache.
    pub async fn get_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        for _ in 0..=MAX_RETRY_WAITING_CONCURRENT_REQUEST {
            let region_cache_guard = self.region_cache.read().await;
            let mut expired_ver_id: Option<RegionVerId> = None;

            // check cache
            if let Some(ver_id) = region_cache_guard.id_to_ver_id.get(&id) {
                if let Some(region) = region_cache_guard.ver_id_to_region.get(ver_id) {
                    if self.region_cache_ttl_ms == 0 {
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
                            return Ok(region.clone());
                        }
                    }

                    expired_ver_id = Some(ver_id.clone());
                }
            }

            // check concurrent requests
            let notify = region_cache_guard.on_my_way_id.get(&id).cloned();
            let notified = notify.as_ref().map(|notify| notify.notified());
            drop(region_cache_guard);

            if let Some(ver_id) = expired_ver_id {
                self.invalidate_region_cache(ver_id).await;
            }

            if let Some(n) = notified {
                n.await;
                continue;
            } else {
                return self.read_through_region_by_id(id).await;
            }
        }
        Err(Error::StringError(format!(
            "Concurrent PD requests failed for {MAX_RETRY_WAITING_CONCURRENT_REQUEST} times"
        )))
    }

    pub async fn get_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.store_cache.read().await.get(&id).cloned();
        match store {
            Some(store) => Ok(store),
            None => self.read_through_store_by_id(id).await,
        }
    }

    /// Force read through (query from PD) and update cache
    pub async fn read_through_region_by_key(&self, key: Key) -> Result<RegionWithLeader> {
        let region = self.inner_client.clone().get_region(key.into()).await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Force read through (query from PD) and update cache
    async fn read_through_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        // put a notify to let others know the region id is being queried
        let notify = Arc::new(Notify::new());
        {
            let mut region_cache_guard = self.region_cache.write().await;
            region_cache_guard.on_my_way_id.insert(id, notify.clone());
        }

        let region = self.inner_client.clone().get_region_by_id(id).await?;
        self.add_region(region.clone()).await;

        // notify others
        {
            let mut region_cache_guard = self.region_cache.write().await;
            notify.notify_waiters();
            region_cache_guard.on_my_way_id.remove(&id);
        }

        Ok(region)
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
        let now_ms = now_epoch_millis();
        let ttl_deadline_ms = next_ttl_deadline_millis(
            now_ms,
            self.region_cache_ttl_ms,
            self.region_cache_ttl_jitter_ms,
        );

        let end_key = region.end_key();
        let mut to_be_removed: HashSet<RegionVerId> = HashSet::new();

        if let Some(ver_id) = cache.id_to_ver_id.get(&region.id()) {
            if ver_id != &region.ver_id() {
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
        let ver_id = region.ver_id();
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

    #[derive(Default)]
    struct MockRetryClient {
        pub regions: Mutex<HashMap<RegionId, RegionWithLeader>>,
        pub stores: Mutex<Vec<metapb::Store>>,
        pub get_region_count: AtomicU64,
        pub get_store_count: AtomicU64,
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
