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
use crate::pd_region_meta_circuit_breaker::default_pd_region_meta_circuit_breaker;
use crate::pd_region_meta_circuit_breaker::PdRegionMetaCircuitBreaker;
use crate::proto::metapb::Store;
use crate::proto::metapb::{self};
use crate::proto::pdpb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats;
use crate::trace::{self, Category, TraceField};
use crate::Key;
use crate::Result;

const MAX_RETRY_WAITING_CONCURRENT_REQUEST: usize = 4;
const DEFAULT_REGIONS_PER_BATCH: i32 = 128;
const MAX_RANGES_PER_BATCH: usize = 16 * DEFAULT_REGIONS_PER_BATCH as usize;

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

fn clamp_bucket_keys_to_region(
    region_start: &[u8],
    region_end: &[u8],
    keys: Vec<Vec<u8>>,
) -> (Vec<Vec<u8>>, bool) {
    if keys.len() < 2 {
        return (vec![region_start.to_vec(), region_end.to_vec()], true);
    }

    let start_matches = keys
        .first()
        .map(|key| key.as_slice() == region_start)
        .unwrap_or(false);
    let end_matches = keys
        .last()
        .map(|key| key.as_slice() == region_end)
        .unwrap_or(false);

    let keys_len = keys.len();
    let region_end_bounded = !region_end.is_empty();
    let mut clamped = !start_matches || !end_matches;

    let mut out = Vec::with_capacity(keys_len);
    out.push(region_start.to_vec());

    for (index, key) in keys.into_iter().enumerate() {
        if index == 0 || index + 1 == keys_len {
            continue;
        }

        // Empty end key means infinity and is only valid for the region end boundary.
        if key.is_empty() {
            clamped = true;
            continue;
        }

        if key.as_slice() <= region_start {
            clamped = true;
            continue;
        }

        if region_end_bounded && key.as_slice() >= region_end {
            clamped = true;
            continue;
        }

        if let Some(prev) = out.last() {
            if key.as_slice() <= prev.as_slice() {
                clamped = true;
                continue;
            }
        }

        out.push(key);
    }

    out.push(region_end.to_vec());

    (out, clamped)
}

/// A key range describing a single bucket within a region.
///
/// When no bucket metadata is available, callers typically treat the whole region as one bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketLocation {
    /// The inclusive start key of the bucket range.
    pub start_key: Key,
    /// The exclusive end key of the bucket range. Empty means unbounded.
    pub end_key: Key,
}

/// Region location metadata for a key range lookup.
///
/// This mirrors client-go `locate.KeyLocation`.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyLocation {
    /// The region identifier and epoch version.
    pub region: RegionVerId,
    /// The inclusive start key of the region range.
    pub start_key: Key,
    /// The exclusive end key of the region range. Empty means unbounded.
    pub end_key: Key,
    /// Cached bucket metadata returned by PD for this region, when available.
    pub buckets: Option<Arc<metapb::Buckets>>,
}

impl KeyLocation {
    /// Return whether `key` falls within this region range.
    #[doc(alias = "Contains")]
    pub fn contains(&self, key: &Key) -> bool {
        self.start_key.as_ref() <= key && (self.end_key.is_empty() || key < &self.end_key)
    }

    fn covers_end_key(&self, end_key: &Key) -> bool {
        end_key.is_empty() || self.end_key.is_empty() || end_key <= &self.end_key
    }

    /// Return the cached bucket version, or `0` when no bucket metadata is attached.
    #[doc(alias = "BucketVersion")]
    pub fn bucket_version(&self) -> u64 {
        self.buckets.as_ref().map_or(0, |buckets| buckets.version)
    }

    /// Locate the bucket range containing `key`.
    ///
    /// If PD has not provided bucket metadata, this returns the whole region range. Sparse bucket
    /// boundaries are clamped to the region edges, matching client-go `LocateBucket`.
    #[doc(alias = "LocateBucket")]
    pub fn locate_bucket(&self, key: &Key) -> Option<BucketLocation> {
        if !self.contains(key) {
            return None;
        }

        let Some(buckets) = self.buckets.as_ref() else {
            return Some(BucketLocation {
                start_key: self.start_key.clone(),
                end_key: self.end_key.clone(),
            });
        };

        if buckets.keys.is_empty() {
            return Some(BucketLocation {
                start_key: self.start_key.clone(),
                end_key: self.end_key.clone(),
            });
        }

        let mut bucket_start = self.start_key.clone();
        for boundary in &buckets.keys {
            let boundary = Key::from(boundary.clone());
            if key < &boundary {
                return Some(BucketLocation {
                    start_key: bucket_start,
                    end_key: boundary,
                });
            }
            bucket_start = boundary;
        }

        Some(BucketLocation {
            start_key: bucket_start,
            end_key: self.end_key.clone(),
        })
    }
}

/// Option bit for locate-range helpers.
///
/// This mirrors client-go `locate.BatchLocateKeyRangesOpt`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchLocateKeyRangesOpt {
    /// Request bucket metadata for each located region when PD can provide it.
    NeedBuckets,
    /// Skip regions that still have no leader after refresh.
    NeedRegionHasLeaderPeer,
}

/// Request PD bucket metadata for locate-range helpers.
///
/// This mirrors client-go `locate.WithNeedBuckets`.
#[doc(alias = "WithNeedBuckets")]
pub const fn with_need_buckets() -> BatchLocateKeyRangesOpt {
    BatchLocateKeyRangesOpt::NeedBuckets
}

/// Skip regions without a leader peer when locating ranges.
///
/// This mirrors client-go `locate.WithNeedRegionHasLeaderPeer`.
#[doc(alias = "WithNeedRegionHasLeaderPeer")]
pub const fn with_need_region_has_leader_peer() -> BatchLocateKeyRangesOpt {
    BatchLocateKeyRangesOpt::NeedRegionHasLeaderPeer
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BatchLocateKeyRangesOptions {
    need_buckets: bool,
    need_region_has_leader_peer: bool,
}

impl BatchLocateKeyRangesOptions {
    fn from_opts<I>(opts: I) -> Self
    where
        I: IntoIterator<Item = BatchLocateKeyRangesOpt>,
    {
        let mut options = Self::default();
        for opt in opts {
            match opt {
                BatchLocateKeyRangesOpt::NeedBuckets => options.need_buckets = true,
                BatchLocateKeyRangesOpt::NeedRegionHasLeaderPeer => {
                    options.need_region_has_leader_peer = true;
                }
            }
        }
        options
    }
}

struct RegionCacheMap {
    /// RegionVerID -> Region. It stores the concrete region caches.
    /// RegionVerID is the unique identifier of a region *across time*.
    ver_id_to_region: HashMap<RegionVerId, RegionWithLeader>,
    /// RegionVerId -> buckets meta.
    ///
    /// Buckets can change without region epoch changes and may be stale.
    ver_id_to_buckets: HashMap<RegionVerId, Arc<metapb::Buckets>>,
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
            ver_id_to_buckets: HashMap::new(),
            ver_id_to_ttl_deadline_ms: HashMap::new(),
            key_to_ver_id: BTreeMap::new(),
            id_to_ver_id: HashMap::new(),
            on_my_way_id: HashMap::new(),
        }
    }
}

/// Cache of region and store metadata fetched from PD.
///
/// This is primarily useful for advanced callers who need to inspect or prewarm
/// region placement information outside the higher-level Raw/Transaction APIs.
pub struct RegionCache<Client = RetryClient<Cluster>> {
    region_cache: Arc<RwLock<RegionCacheMap>>,
    store_cache: Arc<RwLock<HashMap<StoreId, Store>>>,
    inner_client: Arc<Client>,
    region_cache_ttl_ms: u64,
    region_cache_ttl_jitter_ms: u64,
    pd_region_meta_circuit_breaker: Arc<PdRegionMetaCircuitBreaker>,
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
    /// Create an empty region cache with explicit TTL settings.
    ///
    /// `region_cache_ttl` controls the base idle lifetime of cached region entries, while
    /// `region_cache_ttl_jitter` adds per-entry jitter to avoid synchronized expiry bursts.
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
            pd_region_meta_circuit_breaker: default_pd_region_meta_circuit_breaker(),
        }
    }

    pub(crate) fn pd_region_meta_circuit_breaker(&self) -> Arc<PdRegionMetaCircuitBreaker> {
        self.pd_region_meta_circuit_breaker.clone()
    }

    /// Remove all cached region and store metadata.
    ///
    /// This is primarily useful for tests, diagnostics, or callers that need to force subsequent
    /// lookups down the normal read-through path.
    pub async fn clear(&self) {
        let mut region_cache = self.region_cache.write().await;
        *region_cache = RegionCacheMap::new();
        drop(region_cache);

        self.store_cache.write().await.clear();
    }

    /// Return cached buckets metadata for the given region version id.
    ///
    /// Note: buckets are best-effort and may be absent or stale (PD followers can lag and buckets
    /// can change even when the region epoch does not).
    pub async fn get_buckets_by_ver_id(
        &self,
        ver_id: &RegionVerId,
    ) -> Option<Arc<metapb::Buckets>> {
        self.region_cache
            .read()
            .await
            .ver_id_to_buckets
            .get(ver_id)
            .cloned()
    }

    fn cached_region_is_fresh(
        &self,
        cache: &RegionCacheMap,
        ver_id: &RegionVerId,
        region_id: RegionId,
        trace_name: &'static str,
    ) -> bool {
        if self.region_cache_ttl_ms == 0 {
            if trace::is_category_enabled(Category::RegionCache) {
                trace::trace(
                    Category::RegionCache,
                    trace_name,
                    &[TraceField::u64("regionID", region_id)],
                );
            }
            return true;
        }

        let now_ms = now_epoch_millis();
        cache
            .ver_id_to_ttl_deadline_ms
            .get(ver_id)
            .is_some_and(|ttl| {
                check_and_maybe_extend_ttl(
                    ttl,
                    now_ms,
                    self.region_cache_ttl_ms,
                    self.region_cache_ttl_jitter_ms,
                )
            })
    }

    fn key_location_from_parts(
        region: &RegionWithLeader,
        buckets: Option<Arc<metapb::Buckets>>,
    ) -> KeyLocation {
        KeyLocation {
            region: region.ver_id(),
            start_key: region.start_key(),
            end_key: region.end_key(),
            buckets,
        }
    }

    fn key_location_from_cache(cache: &RegionCacheMap, region: &RegionWithLeader) -> KeyLocation {
        let buckets = cache.ver_id_to_buckets.get(&region.ver_id()).cloned();
        Self::key_location_from_parts(region, buckets)
    }

    pub(crate) async fn on_bucket_version_not_match(
        &self,
        ver_id: RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        let mut cache = self.region_cache.write().await;
        let Some(region) = cache.ver_id_to_region.get(&ver_id) else {
            return;
        };

        let should_replace = cache
            .ver_id_to_buckets
            .get(&ver_id)
            .map_or(true, |cached| cached.version < version);
        if should_replace {
            let (keys, clamped) =
                clamp_bucket_keys_to_region(&region.region.start_key, &region.region.end_key, keys);
            if clamped {
                stats::inc_bucket_clamped_counter();
            }

            let region_id = ver_id.id;
            cache.ver_id_to_buckets.insert(
                ver_id,
                Arc::new(metapb::Buckets {
                    region_id,
                    version,
                    keys,
                    ..Default::default()
                }),
            );
        }
    }
}

impl<C: RetryClientTrait + Send + Sync> RegionCache<C> {
    async fn key_location_for_region(&self, region: RegionWithLeader) -> KeyLocation {
        let cache = self.region_cache.read().await;
        Self::key_location_from_cache(&cache, &region)
    }

    async fn key_location_for_region_with_opts(
        &self,
        key: &Key,
        mut region: RegionWithLeader,
        options: BatchLocateKeyRangesOptions,
    ) -> Result<(RegionWithLeader, KeyLocation)> {
        let mut location = self.key_location_for_region(region.clone()).await;
        let needs_refresh = (options.need_buckets && location.buckets.is_none())
            || (options.need_region_has_leader_peer && region.leader.is_none());

        if needs_refresh {
            region = self.read_through_region_by_key(key.clone()).await?;
            location = self.key_location_for_region(region.clone()).await;
        }

        Ok((region, location))
    }

    async fn try_get_region_by_key_from_cache(&self, key: &Key) -> Option<RegionWithLeader> {
        let cache = self.region_cache.read().await;
        let candidate = cache
            .key_to_ver_id
            .range(..=key)
            .next_back()
            .map(|(_, ver_id)| ver_id.clone())?;
        let region = cache.ver_id_to_region.get(&candidate)?;
        if !region.contains(key) {
            return None;
        }
        if !self.cached_region_is_fresh(
            &cache,
            &candidate,
            region.id(),
            "region_cache.try_locate_key.cache_hit",
        ) {
            return None;
        }

        Some(region.clone())
    }

    async fn upgrade_region_from_cache_if_newer(
        &self,
        region: RegionWithLeader,
    ) -> RegionWithLeader {
        let cache = self.region_cache.read().await;
        let region_id = region.id();
        let region_ver_id = region.ver_id();
        let Some(current_ver_id) = cache.id_to_ver_id.get(&region_id) else {
            return region;
        };
        if current_ver_id.ver < region_ver_id.ver
            || current_ver_id.conf_ver < region_ver_id.conf_ver
        {
            return region;
        }
        cache
            .ver_id_to_region
            .get(current_ver_id)
            .cloned()
            .unwrap_or(region)
    }

    async fn try_locate_prev_region_by_end_key(&self, key: &Key) -> Option<KeyLocation> {
        let cache = self.region_cache.read().await;
        let candidate = cache
            .key_to_ver_id
            .range(..key)
            .next_back()
            .map(|(_, ver_id)| ver_id.clone())?;
        let region = cache.ver_id_to_region.get(&candidate)?;
        let region_end_key: &Key = (&region.region.end_key).into();
        if !region_end_key.is_empty() && key > region_end_key {
            return None;
        }
        if !self.cached_region_is_fresh(
            &cache,
            &candidate,
            region.id(),
            "region_cache.locate_end_key.cache_hit",
        ) {
            return None;
        }

        Some(Self::key_location_from_cache(&cache, region))
    }

    /// Return the region covering `key`, loading it from PD on cache miss or expiry.
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

    /// Return the region identified by `id`, loading it from PD on cache miss or expiry.
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

    /// Locate the consecutive regions covering `[start_key, end_key)`.
    ///
    /// This low-level API operates on the region cache's key encoding. Higher-level callers that
    /// rely on API V2 key encoding should prefer [`crate::PdRpcClient::locate_key_range`].
    #[doc(alias = "LocateKeyRange")]
    pub async fn locate_key_range(&self, start_key: Key, end_key: Key) -> Result<Vec<KeyLocation>> {
        self.locate_key_range_with_opts(start_key, end_key, std::iter::empty())
            .await
    }

    /// Locate the consecutive regions covering `[start_key, end_key)` with extra locate options.
    pub async fn locate_key_range_with_opts<I>(
        &self,
        mut start_key: Key,
        end_key: Key,
        opts: I,
    ) -> Result<Vec<KeyLocation>>
    where
        I: IntoIterator<Item = BatchLocateKeyRangesOpt>,
    {
        let options = BatchLocateKeyRangesOptions::from_opts(opts);

        if !end_key.is_empty() && start_key >= end_key {
            return Ok(Vec::new());
        }

        let mut locations = Vec::new();
        loop {
            let region = self.get_region_by_key(&start_key).await?;
            let (region, location) = self
                .key_location_for_region_with_opts(&start_key, region, options)
                .await?;
            let location_end = location.end_key.clone();
            let covers_end = location.covers_end_key(&end_key);

            if !options.need_region_has_leader_peer || region.leader.is_some() {
                locations.push(location);
            }

            if covers_end || location_end == start_key {
                break;
            }
            start_key = location_end;
        }

        Ok(locations)
    }

    /// Locate the region containing `key`.
    ///
    /// This low-level API operates on the region cache's key encoding. Higher-level callers that
    /// rely on API V2 key encoding should prefer [`crate::PdRpcClient::locate_key`].
    #[doc(alias = "LocateKey")]
    pub async fn locate_key(&self, key: Key) -> Result<KeyLocation> {
        let region = self.get_region_by_key(&key).await?;
        Ok(self.key_location_for_region(region).await)
    }

    /// Try locating the region containing `key` from the local cache only.
    ///
    /// Returns `None` if the region is missing or expired. No PD request is issued.
    #[doc(alias = "TryLocateKey")]
    pub async fn try_locate_key(&self, key: Key) -> Option<KeyLocation> {
        let cache = self.region_cache.read().await;
        let candidate = cache
            .key_to_ver_id
            .range(..=&key)
            .next_back()
            .map(|(_, ver_id)| ver_id.clone())?;
        let region = cache.ver_id_to_region.get(&candidate)?;
        if !region.contains(&key) {
            return None;
        }
        if !self.cached_region_is_fresh(
            &cache,
            &candidate,
            region.id(),
            "region_cache.try_locate_key.cache_hit",
        ) {
            return None;
        }

        Some(Self::key_location_from_cache(&cache, region))
    }

    /// Locate the region containing the last key strictly less than `key`.
    ///
    /// This mirrors client-go `LocateEndKey` semantics. The input key must use the region cache's
    /// key encoding.
    #[doc(alias = "LocateEndKey")]
    pub async fn locate_end_key(&self, key: Key) -> Result<KeyLocation> {
        if key.is_empty() {
            return Err(Error::Unimplemented);
        }

        let key_bytes: &[u8] = (&key).into();
        let region = self.get_region_by_key(&key).await?;
        if key_bytes > region.region.start_key.as_slice() {
            return Ok(self.key_location_for_region(region).await);
        }

        if let Some(location) = self.try_locate_prev_region_by_end_key(&key).await {
            return Ok(location);
        }

        let (prev_region, buckets) = self
            .pd_region_meta_circuit_breaker
            .execute(|| {
                self.inner_client
                    .clone()
                    .get_prev_region_with_buckets(key.clone().into())
            })
            .await?;
        let location = Self::key_location_from_parts(
            &prev_region,
            buckets.as_ref().map(|bucket| Arc::new(bucket.clone())),
        );
        self.add_region_with_buckets(prev_region, buckets).await;
        Ok(location)
    }

    /// Locate a region by region id and return its range metadata.
    ///
    /// This low-level API operates on the region cache's key encoding. Higher-level callers that
    /// rely on API V2 key encoding should prefer [`crate::PdRpcClient::locate_region_by_id`].
    #[doc(alias = "LocateRegionByID")]
    pub async fn locate_region_by_id(&self, id: RegionId) -> Result<KeyLocation> {
        let region = self.get_region_by_id(id).await?;
        Ok(self.key_location_for_region(region).await)
    }

    /// Load a region by region id directly from PD without updating the local cache.
    ///
    /// This is mainly useful for diagnostics when callers want to bypass potentially stale cached
    /// metadata. The input/output keys use the region cache's key encoding.
    #[doc(alias = "LocateRegionByIDFromPD")]
    pub async fn locate_region_by_id_from_pd(&self, id: RegionId) -> Result<KeyLocation> {
        let (region, buckets) = self
            .pd_region_meta_circuit_breaker
            .execute(|| self.inner_client.clone().get_region_by_id_with_buckets(id))
            .await?;
        Ok(Self::key_location_from_parts(
            &region,
            buckets.map(Arc::new),
        ))
    }

    /// Locate multiple key ranges and merge adjacent duplicates from the same region.
    ///
    /// This low-level API operates on the region cache's key encoding. Higher-level callers that
    /// rely on API V2 key encoding should prefer [`crate::PdRpcClient::batch_locate_key_ranges`].
    #[doc(alias = "BatchLocateKeyRanges")]
    pub async fn batch_locate_key_ranges(
        &self,
        ranges: Vec<crate::kv::KeyRange>,
    ) -> Result<Vec<KeyLocation>> {
        self.batch_locate_key_ranges_with_opts(ranges, std::iter::empty())
            .await
    }

    /// Locate multiple key ranges and merge adjacent duplicates from the same region, with extra
    /// locate options applied to each region lookup.
    pub async fn batch_locate_key_ranges_with_opts<I>(
        &self,
        ranges: Vec<crate::kv::KeyRange>,
        opts: I,
    ) -> Result<Vec<KeyLocation>>
    where
        I: IntoIterator<Item = BatchLocateKeyRangesOpt> + Clone,
    {
        let options = BatchLocateKeyRangesOptions::from_opts(opts.clone());

        let ranges: Vec<crate::kv::KeyRange> = ranges
            .into_iter()
            .filter(|range| range.end_key.is_empty() || range.start_key < range.end_key)
            .collect();

        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        // When range grouping is used on cold cache, avoid issuing N PD lookups by preloading the
        // region metadata with PD BatchScanRegions requests.
        //
        // Note: PD requires ranges in order. Preheating sorts ranges before issuing PD requests;
        // this only warms the cache and does not affect the final locate output order.
        let ranges_are_sorted = ranges
            .windows(2)
            .all(|pair| pair[0].start_key <= pair[1].start_key);
        let to_send = ranges.len().min(MAX_RANGES_PER_BATCH);

        // Even if the range start hits cache, later regions may still be missing. In that case,
        // preheat only the uncached tail ranges to reduce per-key PD lookups in the main locate
        // path.
        let mut uncached_ranges = Vec::new();
        let mut cached_prefix_regions = Vec::with_capacity(ranges.len());
        for (idx, range) in ranges.iter().enumerate() {
            let mut start_key = range.start_key.clone();
            let end_key = range.end_key.clone();
            let mut cached_prefix = Vec::new();

            loop {
                let Some(region) = self.try_get_region_by_key_from_cache(&start_key).await else {
                    if idx < to_send {
                        uncached_ranges.push(crate::kv::KeyRange {
                            start_key,
                            end_key: end_key.clone(),
                        });
                    }
                    break;
                };
                let location = self.key_location_for_region(region.clone()).await;
                cached_prefix.push(region);

                let next_start_key = location.end_key;
                if next_start_key.is_empty() {
                    break;
                }
                if !end_key.is_empty() && next_start_key >= end_key {
                    break;
                }
                // Defensive guard against non-progressing cached locations.
                if next_start_key <= start_key {
                    if idx < to_send {
                        uncached_ranges.push(crate::kv::KeyRange {
                            start_key,
                            end_key: end_key.clone(),
                        });
                    }
                    break;
                }

                start_key = next_start_key;
            }

            cached_prefix_regions.push(cached_prefix);
        }

        if !uncached_ranges.is_empty() {
            if !ranges_are_sorted {
                uncached_ranges.sort_by(|left, right| left.start_key.cmp(&right.start_key));
            }

            let mut remaining_ranges = uncached_ranges;

            fn regions_have_gap_in_ranges(
                ranges: &[crate::kv::KeyRange],
                regions: &[(RegionWithLeader, Option<metapb::Buckets>)],
                limit: usize,
            ) -> bool {
                if ranges.is_empty() {
                    return false;
                }
                if regions.is_empty() {
                    return true;
                }

                let mut check_idx = 0usize;
                let mut check_key = ranges[0].start_key.clone();

                for (region, _) in regions {
                    let start_key = region.start_key();
                    if start_key > check_key {
                        return true;
                    }

                    let end_key = region.end_key();
                    if end_key.is_empty() {
                        return false;
                    }
                    check_key = end_key;

                    while check_idx < ranges.len()
                        && !ranges[check_idx].end_key.is_empty()
                        && check_key >= ranges[check_idx].end_key
                    {
                        check_idx += 1;
                        if check_idx >= ranges.len() {
                            return false;
                        }
                    }

                    if check_key < ranges[check_idx].start_key {
                        check_key = ranges[check_idx].start_key.clone();
                    }
                }

                if regions.len() >= limit {
                    return false;
                }
                if check_idx < ranges.len() - 1 {
                    return true;
                }
                if check_key.is_empty() {
                    return false;
                }
                if ranges[check_idx].end_key.is_empty() {
                    return true;
                }

                check_key < ranges[check_idx].end_key
            }

            // Drop ranges that are fully covered by `split_key` and continue from there.
            //
            // This mirrors client-go `rangesAfterKey` but is best-effort (linear scan) because
            // callers may pass overlapping or otherwise irregular range lists.
            fn ranges_after_key(
                mut key_ranges: Vec<crate::kv::KeyRange>,
                split_key: &Key,
            ) -> Vec<crate::kv::KeyRange> {
                if key_ranges.is_empty() {
                    return Vec::new();
                }
                if split_key.is_empty() {
                    return Vec::new();
                }

                let last_end = &key_ranges[key_ranges.len() - 1].end_key;
                if !last_end.is_empty() && split_key >= last_end {
                    return Vec::new();
                }

                let mut idx = 0usize;
                while idx < key_ranges.len() {
                    let end = &key_ranges[idx].end_key;
                    if end.is_empty() || split_key < end {
                        break;
                    }
                    idx += 1;
                }
                if idx >= key_ranges.len() {
                    return Vec::new();
                }

                let mut rest = key_ranges.split_off(idx);
                if split_key > &rest[0].start_key {
                    rest[0].start_key = split_key.clone();
                }
                rest
            }

            let mut retried_empty_batch = false;
            let mut retried_gap_batch = false;
            let mut retried_leaderless_batch = false;
            let mut retried_error_batch = false;
            loop {
                let pd_ranges: Vec<pdpb::KeyRange> = remaining_ranges
                    .iter()
                    .map(|range| pdpb::KeyRange {
                        start_key: range.start_key.clone().into(),
                        end_key: range.end_key.clone().into(),
                    })
                    .collect();

                let result = self
                    .pd_region_meta_circuit_breaker
                    .execute(|| {
                        let inner = self.inner_client.clone();
                        let pd_ranges = pd_ranges;
                        async move {
                            inner
                                .batch_scan_regions(
                                    pd_ranges,
                                    DEFAULT_REGIONS_PER_BATCH,
                                    options.need_buckets,
                                )
                                .await
                        }
                    })
                    .await;

                match result {
                    Ok(regions) => {
                        retried_error_batch = false;
                        if regions.is_empty() {
                            stats::inc_stale_region_from_pd_counter();
                            if !retried_empty_batch {
                                retried_empty_batch = true;
                                continue;
                            }
                            break;
                        }
                        retried_empty_batch = false;

                        if regions_have_gap_in_ranges(
                            &remaining_ranges,
                            &regions,
                            DEFAULT_REGIONS_PER_BATCH as usize,
                        ) {
                            stats::inc_stale_region_from_pd_counter();
                            if !retried_gap_batch {
                                retried_gap_batch = true;
                                continue;
                            }
                            break;
                        }
                        retried_gap_batch = false;

                        let some_regions_missing_leader = options.need_region_has_leader_peer
                            && regions.iter().any(|(region, _)| region.leader.is_none());
                        if some_regions_missing_leader {
                            stats::inc_stale_region_from_pd_counter();
                            if !retried_leaderless_batch {
                                retried_leaderless_batch = true;
                                continue;
                            }
                            break;
                        }
                        retried_leaderless_batch = false;

                        let split_key = regions
                            .last()
                            .map(|(region, _)| region.end_key())
                            .unwrap_or_default();

                        for (region, buckets) in regions {
                            self.add_region_with_buckets(region, buckets).await;
                        }

                        // `end_key == ""` means the last region extends to +inf.
                        if split_key.is_empty() {
                            break;
                        }
                        // Defensive guard against non-progressing PD responses.
                        if split_key <= remaining_ranges[0].start_key {
                            break;
                        }

                        remaining_ranges = ranges_after_key(remaining_ranges, &split_key);
                        if remaining_ranges.is_empty() {
                            break;
                        }
                    }
                    Err(Error::Unimplemented) => break,
                    Err(_) => {
                        // Mirror the stale-response retries above: give the same PD batch one more
                        // chance before abandoning preheat and falling back to per-range locate.
                        if !retried_error_batch {
                            retried_error_batch = true;
                            continue;
                        }
                        break;
                    }
                }
            }
        }

        let mut locations = Vec::new();

        for (range, cached_prefix) in ranges.into_iter().zip(cached_prefix_regions) {
            let mut start_key = range.start_key;
            let end_key = range.end_key;

            for cached_region in cached_prefix {
                let cached_region = self.upgrade_region_from_cache_if_newer(cached_region).await;
                let (cached_region, location) = self
                    .key_location_for_region_with_opts(&start_key, cached_region, options)
                    .await?;
                let location_end = location.end_key.clone();
                let covers_end = location.covers_end_key(&end_key);
                if (!options.need_region_has_leader_peer || cached_region.leader.is_some())
                    && locations.last() != Some(&location)
                {
                    locations.push(location);
                }
                if covers_end || location_end == start_key {
                    start_key = location_end;
                    break;
                }
                start_key = location_end;
            }

            if start_key.is_empty() || (!end_key.is_empty() && start_key >= end_key) {
                continue;
            }

            for location in self
                .locate_key_range_with_opts(start_key, end_key, opts.clone())
                .await?
            {
                if locations.last() == Some(&location) {
                    continue;
                }
                locations.push(location);
            }
        }

        Ok(locations)
    }

    /// Return the store metadata for `id`, loading it from PD on cache miss.
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

    pub(crate) fn store_meta_by_id_cached(&self, id: StoreId) -> Option<Store> {
        self.store_cache
            .try_read()
            .ok()
            .and_then(|guard| guard.get(&id).cloned())
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
        let (region, buckets) = match self
            .pd_region_meta_circuit_breaker
            .execute(|| {
                self.inner_client
                    .clone()
                    .get_region_with_buckets(key.into())
            })
            .await
        {
            Ok((region, buckets)) => {
                stats::region_cache_operation("get_region_when_miss", true);
                stats::observe_load_region_cache("get_region_when_miss", started_at.elapsed());
                (region, buckets)
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
        self.add_region_with_buckets(region.clone(), buckets).await;
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
                .pd_region_meta_circuit_breaker
                .execute(|| {
                    self.inner_client.clone().scan_regions(
                        start_key.clone().into(),
                        end_key_bytes.clone(),
                        limit,
                    )
                })
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

        let result = match self
            .pd_region_meta_circuit_breaker
            .execute(|| self.inner_client.clone().get_region_by_id_with_buckets(id))
            .await
        {
            Ok((region, buckets)) => {
                if trace_enabled {
                    trace::trace(
                        Category::RegionCache,
                        "region_cache.read_through_region_by_id.pd_response",
                        &[TraceField::u64("regionID", region.id())],
                    );
                }
                self.add_region_with_buckets(region.clone(), buckets).await;
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

    /// Insert or refresh one region entry in the local cache.
    ///
    /// Existing overlapping entries are replaced according to the normal region-version rules.
    pub async fn add_region(&self, region: RegionWithLeader) {
        let _ = self.add_region_internal(region, None).await;
    }

    pub(crate) async fn add_region_with_buckets(
        &self,
        region: RegionWithLeader,
        buckets: Option<metapb::Buckets>,
    ) {
        let _ = self.add_region_internal(region, buckets).await;
    }

    async fn add_region_internal(
        &self,
        region: RegionWithLeader,
        buckets: Option<metapb::Buckets>,
    ) -> bool {
        // This takes a write lock because we update multiple indices consistently. This runs on
        // region-cache update paths (e.g. cache miss), not the hot-path request routing.
        let region_start_key = region.region.start_key.clone();
        let region_end_key = region.region.end_key.clone();

        let mut cache = self.region_cache.write().await;
        let new_ver_id = region.ver_id();
        if let Some(old_ver_id) = cache.id_to_ver_id.get(&new_ver_id.id) {
            // Like client-go, ignore stale region info that arrives out-of-order or from a PD
            // follower that lags behind. These stale entries can otherwise overwrite newer cached
            // regions.
            if old_ver_id.ver > new_ver_id.ver || old_ver_id.conf_ver > new_ver_id.conf_ver {
                stats::inc_stale_region_from_pd_counter();
                return false;
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
                            return false;
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
            if ver_id != new_ver_id || buckets.is_some() {
                cache.ver_id_to_buckets.remove(&ver_id);
            }
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
            .insert(ver_id.clone(), AtomicU64::new(ttl_deadline_ms));

        if let Some(mut buckets) = buckets {
            let should_replace = cache
                .ver_id_to_buckets
                .get(&ver_id)
                .map_or(true, |cached| buckets.version >= cached.version);
            if should_replace {
                let (keys, clamped) =
                    clamp_bucket_keys_to_region(&region_start_key, &region_end_key, buckets.keys);
                if clamped {
                    stats::inc_bucket_clamped_counter();
                }
                buckets.keys = keys;

                cache.ver_id_to_buckets.insert(ver_id, Arc::new(buckets));
            }
        }

        true
    }

    /// Update the cached leader peer for a known region version.
    ///
    /// If the region is no longer cached, this is a no-op.
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

    /// Remove a single cached region entry and any associated bucket metadata.
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
        cache.ver_id_to_buckets.remove(&ver_id);
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

    /// Drop one cached store entry so the next lookup reloads it from PD.
    pub async fn invalidate_store_cache(&self, store_id: StoreId) {
        stats::region_cache_operation("invalidate_store_regions", true);
        let mut cache = self.store_cache.write().await;
        cache.remove(&store_id);
    }

    /// Reload all valid TiKV stores from PD and refresh the local store cache.
    ///
    /// TiFlash and tombstone stores are filtered out to match the regular TiKV store view used by
    /// request dispatch.
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

    /// Reload all stores relevant to safe-ts observation from PD.
    ///
    /// This keeps TiFlash stores, but still filters out tombstone and TiFlash compute-only stores.
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

const ENGINE_LABEL_KEY: &str = crate::tikvrpc::ENGINE_LABEL_KEY;
const ENGINE_LABEL_TIFLASH: &str = crate::tikvrpc::ENGINE_LABEL_TIFLASH;
const ENGINE_LABEL_TIFLASH_COMPUTE: &str = crate::tikvrpc::ENGINE_LABEL_TIFLASH_COMPUTE;
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
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use serial_test::serial;
    use tokio::sync::Mutex;

    use super::with_need_buckets;
    use super::with_need_region_has_leader_peer;
    use super::BucketLocation;
    use super::KeyLocation;
    use super::RegionCache;
    use crate::common::Error;
    use crate::pd::RetryClientTrait;
    use crate::pd_region_meta_circuit_breaker::PdRegionMetaCircuitBreaker;
    use crate::proto::keyspacepb;
    use crate::proto::metapb::RegionEpoch;
    use crate::proto::metapb::{self};
    use crate::proto::pdpb;
    use crate::region::RegionId;
    use crate::region::RegionVerId;
    use crate::region::RegionWithLeader;
    use crate::region_cache::is_tiflash_compute_store;
    use crate::region_cache::is_tiflash_related_store;
    use crate::region_cache::is_tiflash_store;
    use crate::region_cache::is_valid_tikv_store;
    use crate::Key;
    use crate::Result;

    type ScanRegionsCall = (Vec<u8>, Vec<u8>, i32);
    type BatchScanRegionsCall = (Vec<pdpb::KeyRange>, i32, bool);

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
            .find(|family| family.get_name() == "tikv_client_stale_region_from_pd")
            .and_then(|family| {
                family
                    .get_metric()
                    .first()
                    .map(|metric| metric.get_counter().get_value())
            })
            .unwrap_or(0.0)
    }

    fn bucket_clamped_counter_value() -> f64 {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_bucket_clamped")
            .and_then(|family| {
                family
                    .get_metric()
                    .first()
                    .map(|metric| metric.get_counter().get_value())
            })
            .unwrap_or(0.0)
    }

    fn load_region_total_counter_value(tag: &str, reason: &str) -> f64 {
        prometheus::gather()
            .iter()
            .find(|family| family.get_name() == "tikv_client_load_region_total")
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
        pub buckets: Mutex<HashMap<RegionVerId, metapb::Buckets>>,
        pub stores: Mutex<Vec<metapb::Store>>,
        pub get_region_count: AtomicU64,
        pub get_store_count: AtomicU64,
        pub scan_regions_count: AtomicU64,
        pub scan_regions_calls: Mutex<Vec<ScanRegionsCall>>,
        pub batch_scan_regions_count: AtomicU64,
        pub batch_scan_regions_calls: Mutex<Vec<BatchScanRegionsCall>>,
        pub batch_scan_regions_failures: Mutex<VecDeque<Error>>,
        pub batch_scan_regions_results:
            Mutex<VecDeque<Vec<(RegionWithLeader, Option<metapb::Buckets>)>>>,
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

        async fn get_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<(crate::region::RegionWithLeader, Option<metapb::Buckets>)> {
            let region = self.clone().get_region(key).await?;
            let ver_id = region.ver_id();
            let buckets = self.buckets.lock().await.get(&ver_id).cloned();
            Ok((region, buckets))
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

        async fn get_region_by_id_with_buckets(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<(crate::region::RegionWithLeader, Option<metapb::Buckets>)> {
            let region = self.clone().get_region_by_id(region_id).await?;
            let ver_id = region.ver_id();
            let buckets = self.buckets.lock().await.get(&ver_id).cloned();
            Ok((region, buckets))
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

        async fn batch_scan_regions(
            self: Arc<Self>,
            ranges: Vec<pdpb::KeyRange>,
            limit: i32,
            need_buckets: bool,
        ) -> Result<Vec<(RegionWithLeader, Option<metapb::Buckets>)>> {
            self.batch_scan_regions_count.fetch_add(1, SeqCst);
            self.batch_scan_regions_calls
                .lock()
                .await
                .push((ranges.clone(), limit, need_buckets));

            if let Some(err) = self.batch_scan_regions_failures.lock().await.pop_front() {
                return Err(err);
            }
            if let Some(result) = self.batch_scan_regions_results.lock().await.pop_front() {
                return Ok(result);
            }

            if limit <= 0 {
                return Ok(Vec::new());
            }

            let mut regions: Vec<RegionWithLeader> =
                self.regions.lock().await.values().cloned().collect();
            regions.sort_by(|left, right| left.region.start_key.cmp(&right.region.start_key));

            let buckets = self.buckets.lock().await.clone();

            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for range in ranges {
                let start_key = Key::from(range.start_key);
                let end_key = Key::from(range.end_key);

                let Some(start_idx) = regions
                    .iter()
                    .position(|region| region.contains(&start_key))
                else {
                    continue;
                };

                for region in regions.iter().skip(start_idx) {
                    if !end_key.is_empty() && region.start_key() >= end_key {
                        break;
                    }
                    let ver_id = region.ver_id();
                    if !seen.insert(ver_id.clone()) {
                        continue;
                    }
                    let buckets = if need_buckets {
                        buckets.get(&ver_id).cloned()
                    } else {
                        None
                    };
                    out.push((region.clone(), buckets));
                    if out.len() >= limit as usize {
                        return Ok(out);
                    }
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
    async fn test_region_cache_caches_and_invalidates_buckets() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region = RegionWithLeader {
            region: metapb::Region {
                id: 42,
                start_key: vec![0],
                end_key: vec![10],
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };
        let ver_id = region.ver_id();

        let buckets = metapb::Buckets {
            region_id: region.id(),
            version: 7,
            keys: vec![vec![0], vec![5], vec![10]],
            ..Default::default()
        };

        retry_client
            .regions
            .lock()
            .await
            .insert(region.id(), region);
        retry_client
            .buckets
            .lock()
            .await
            .insert(ver_id.clone(), buckets.clone());

        let loaded = cache.read_through_region_by_key(Key::from(vec![1])).await?;
        assert_eq!(loaded.ver_id(), ver_id);

        let cached = cache
            .get_buckets_by_ver_id(&ver_id)
            .await
            .expect("expected cached buckets");
        assert_eq!(cached.version, buckets.version);

        // Refreshing the region info without buckets should keep the bucket meta.
        cache.add_region(loaded).await;
        assert!(cache.get_buckets_by_ver_id(&ver_id).await.is_some());

        cache.invalidate_region_cache(ver_id.clone()).await;
        assert!(cache.get_buckets_by_ver_id(&ver_id).await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_removes_buckets_on_region_replacement() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client, Duration::ZERO, Duration::ZERO);

        let region_v1 = RegionWithLeader {
            region: metapb::Region {
                id: 7,
                start_key: vec![],
                end_key: vec![10],
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };
        let ver_id_v1 = region_v1.ver_id();

        cache
            .add_region_with_buckets(
                region_v1,
                Some(metapb::Buckets {
                    region_id: 7,
                    version: 1,
                    keys: vec![vec![], vec![10]],
                    ..Default::default()
                }),
            )
            .await;
        assert!(cache.get_buckets_by_ver_id(&ver_id_v1).await.is_some());

        let region_v2 = RegionWithLeader {
            region: metapb::Region {
                id: 7,
                start_key: vec![],
                end_key: vec![20],
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 2,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 2,
                ..Default::default()
            }),
        };
        let ver_id_v2 = region_v2.ver_id();

        cache
            .add_region_with_buckets(
                region_v2,
                Some(metapb::Buckets {
                    region_id: 7,
                    version: 2,
                    keys: vec![vec![], vec![20]],
                    ..Default::default()
                }),
            )
            .await;

        assert!(cache.get_buckets_by_ver_id(&ver_id_v1).await.is_none());
        assert_eq!(
            cache
                .get_buckets_by_ver_id(&ver_id_v2)
                .await
                .expect("expected cached buckets")
                .version,
            2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_on_bucket_version_not_match_updates_cached_buckets() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client, Duration::ZERO, Duration::ZERO);

        let region = RegionWithLeader {
            region: metapb::Region {
                id: 9,
                start_key: vec![],
                end_key: vec![10],
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };
        let ver_id = region.ver_id();
        cache.add_region(region).await;

        cache
            .on_bucket_version_not_match(ver_id.clone(), 10, vec![vec![], vec![5], vec![10]])
            .await;
        let buckets = cache
            .get_buckets_by_ver_id(&ver_id)
            .await
            .expect("expected buckets");
        assert_eq!(buckets.version, 10);

        // Older versions should not overwrite the cached value.
        cache
            .on_bucket_version_not_match(ver_id.clone(), 5, vec![vec![1]])
            .await;
        let buckets = cache
            .get_buckets_by_ver_id(&ver_id)
            .await
            .expect("expected buckets");
        assert_eq!(buckets.version, 10);

        Ok(())
    }

    #[tokio::test]
    #[serial(metrics)]
    async fn test_region_cache_clamps_bucket_keys_and_records_metrics() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client, Duration::ZERO, Duration::ZERO);

        let region = RegionWithLeader {
            region: metapb::Region {
                id: 123,
                start_key: vec![10],
                end_key: vec![20],
                region_epoch: Some(RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };
        let ver_id = region.ver_id();

        let before = bucket_clamped_counter_value();
        cache
            .add_region_with_buckets(
                region,
                Some(metapb::Buckets {
                    region_id: 123,
                    version: 1,
                    keys: vec![vec![0], vec![15], vec![30]],
                    ..Default::default()
                }),
            )
            .await;

        let after = bucket_clamped_counter_value();
        assert!(
            after >= before + 1.0,
            "expected bucket_clamped counter to increase"
        );

        let buckets = cache
            .get_buckets_by_ver_id(&ver_id)
            .await
            .expect("expected buckets");
        assert_eq!(buckets.keys, vec![vec![10], vec![15], vec![20]]);

        Ok(())
    }

    #[test]
    fn test_key_location_locates_bucket_and_clamps_region_edges() {
        let location = KeyLocation {
            region: RegionVerId {
                id: 1,
                conf_ver: 1,
                ver: 1,
            },
            start_key: vec![10].into(),
            end_key: vec![20].into(),
            buckets: Some(Arc::new(metapb::Buckets {
                region_id: 1,
                version: 7,
                keys: vec![vec![12], vec![15]],
                ..Default::default()
            })),
        };

        assert_eq!(
            location.locate_bucket(&Key::from(vec![11])),
            Some(BucketLocation {
                start_key: vec![10].into(),
                end_key: vec![12].into(),
            })
        );
        assert_eq!(
            location.locate_bucket(&Key::from(vec![12])),
            Some(BucketLocation {
                start_key: vec![12].into(),
                end_key: vec![15].into(),
            })
        );
        assert_eq!(
            location.locate_bucket(&Key::from(vec![18])),
            Some(BucketLocation {
                start_key: vec![15].into(),
                end_key: vec![20].into(),
            })
        );
        assert_eq!(location.locate_bucket(&Key::from(vec![20])), None);
    }

    #[tokio::test]
    async fn test_region_cache_locate_key_range_returns_locations_with_bucket_versions(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let ver_id1 = region1.ver_id();
        let ver_id2 = region2.ver_id();

        retry_client.regions.lock().await.insert(1, region1);
        retry_client.regions.lock().await.insert(2, region2);
        retry_client.buckets.lock().await.insert(
            ver_id1,
            metapb::Buckets {
                region_id: 1,
                version: 3,
                keys: vec![vec![], vec![4], vec![10]],
                ..Default::default()
            },
        );
        retry_client.buckets.lock().await.insert(
            ver_id2,
            metapb::Buckets {
                region_id: 2,
                version: 5,
                keys: vec![vec![10], vec![15], vec![20]],
                ..Default::default()
            },
        );

        let locations = cache
            .locate_key_range(Key::from(vec![2]), Key::from(vec![18]))
            .await?;

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].bucket_version(), 3);
        assert_eq!(
            locations[0].locate_bucket(&Key::from(vec![2])),
            Some(BucketLocation {
                start_key: vec![].into(),
                end_key: vec![4].into(),
            })
        );
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(locations[1].bucket_version(), 5);
        assert_eq!(
            locations[1].locate_bucket(&Key::from(vec![17])),
            Some(BucketLocation {
                start_key: vec![15].into(),
                end_key: vec![20].into(),
            })
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_merges_duplicate_regions() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client, Duration::ZERO, Duration::ZERO);

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;
        cache.add_region(region(3, vec![20], vec![])).await;

        let locations = cache
            .batch_locate_key_ranges(vec![
                crate::kv::KeyRange::new(vec![1], vec![3]),
                crate::kv::KeyRange::new(vec![3], vec![8]),
                crate::kv::KeyRange::new(vec![12], vec![18]),
            ])
            .await?;

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[1].region.id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_uses_pd_batch_scan_when_cache_misses(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![20], vec![]);

        let ver_id1 = region1.ver_id();
        let ver_id2 = region2.ver_id();
        let ver_id3 = region3.ver_id();

        retry_client.regions.lock().await.insert(1, region1);
        retry_client.regions.lock().await.insert(2, region2);
        retry_client.regions.lock().await.insert(3, region3);

        retry_client.buckets.lock().await.insert(
            ver_id1,
            metapb::Buckets {
                region_id: 1,
                version: 3,
                keys: vec![vec![], vec![10]],
                ..Default::default()
            },
        );
        retry_client.buckets.lock().await.insert(
            ver_id2,
            metapb::Buckets {
                region_id: 2,
                version: 5,
                keys: vec![vec![10], vec![20]],
                ..Default::default()
            },
        );
        retry_client.buckets.lock().await.insert(
            ver_id3,
            metapb::Buckets {
                region_id: 3,
                version: 7,
                keys: vec![vec![20], vec![]],
                ..Default::default()
            },
        );

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![
                    crate::kv::KeyRange::new(vec![1], vec![3]),
                    crate::kv::KeyRange::new(vec![3], vec![8]),
                    crate::kv::KeyRange::new(vec![12], vec![18]),
                ],
                [with_need_buckets()],
            )
            .await?;

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].bucket_version(), 3);
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(locations[1].bucket_version(), 5);

        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 1);
        let calls = retry_client.batch_scan_regions_calls.lock().await.clone();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].2, "expected need_buckets=true");

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_retries_pd_batch_scan_when_all_regions_have_no_leader(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let leaderless_region = region(1, vec![], vec![10]);
        let leaderful_region = region_with_leader(1, vec![], vec![10], 42);
        retry_client
            .regions
            .lock()
            .await
            .insert(1, leaderless_region.clone());
        retry_client
            .batch_scan_regions_results
            .lock()
            .await
            .extend([
                vec![(leaderless_region, None)],
                vec![(leaderful_region, None)],
            ]);

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![8])],
                [with_need_region_has_leader_peer()],
            )
            .await?;

        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].start_key, Key::from(Vec::<u8>::new()));
        assert_eq!(locations[0].end_key, Key::from(vec![10]));
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_retries_pd_batch_scan_when_some_regions_have_no_leader(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region_with_leader(1, vec![], vec![10], 42);
        let leaderless_region2 = region(2, vec![10], vec![20]);
        let leaderful_region2 = region_with_leader(2, vec![10], vec![20], 42);
        retry_client.regions.lock().await.insert(1, region1.clone());
        retry_client
            .regions
            .lock()
            .await
            .insert(2, leaderful_region2.clone());
        retry_client
            .batch_scan_regions_results
            .lock()
            .await
            .extend([
                vec![(region1.clone(), None), (leaderless_region2, None)],
                vec![(region1, None), (leaderful_region2, None)],
            ]);

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![18])],
                [with_need_region_has_leader_peer()],
            )
            .await?;

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_retries_pd_batch_scan_when_pd_returns_empty(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region = region_with_leader(1, vec![], vec![10], 42);
        retry_client.regions.lock().await.insert(1, region.clone());
        retry_client
            .batch_scan_regions_results
            .lock()
            .await
            .extend([Vec::new(), vec![(region, None)]]);

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![8])],
                std::iter::empty(),
            )
            .await?;

        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].start_key, Key::from(Vec::<u8>::new()));
        assert_eq!(locations[0].end_key, Key::from(vec![10]));
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_retries_pd_batch_scan_when_pd_response_has_gap(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region_with_leader(1, vec![], vec![10], 42);
        let region2 = region_with_leader(2, vec![10], vec![20], 42);
        let region3 = region_with_leader(3, vec![20], vec![], 42);

        retry_client.regions.lock().await.insert(1, region1.clone());
        retry_client.regions.lock().await.insert(2, region2.clone());
        retry_client.regions.lock().await.insert(3, region3.clone());
        retry_client
            .batch_scan_regions_results
            .lock()
            .await
            .extend([
                vec![(region1.clone(), None), (region3.clone(), None)],
                vec![(region1, None), (region2, None), (region3, None)],
            ]);

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![25])],
                std::iter::empty(),
            )
            .await?;

        assert_eq!(locations.len(), 3);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(locations[2].region.id, 3);
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_retries_pd_batch_scan_after_transient_error(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region_with_leader(1, vec![], vec![10], 42);
        let region2 = region_with_leader(2, vec![10], vec![], 42);

        retry_client.regions.lock().await.insert(1, region1.clone());
        retry_client.regions.lock().await.insert(2, region2.clone());
        retry_client
            .batch_scan_regions_failures
            .lock()
            .await
            .push_back(Error::StringError("transient pd failure".to_owned()));
        retry_client
            .batch_scan_regions_results
            .lock()
            .await
            .push_back(vec![(region1, None), (region2, None)]);

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![25])],
                std::iter::empty(),
            )
            .await?;

        assert_eq!(locations.len(), 2);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[1].region.id, 2);
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_caps_pd_batch_scan_ranges(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        // Ensure the post-preload locate path can succeed without issuing per-range PD requests.
        retry_client
            .regions
            .lock()
            .await
            .insert(1, region(1, vec![], vec![]));

        let total = 2048 + 10;
        let mut ranges = Vec::with_capacity(total);
        for i in 0..total {
            let start = (i as u32).to_be_bytes().to_vec();
            let end = ((i + 1) as u32).to_be_bytes().to_vec();
            ranges.push(crate::kv::KeyRange::new(start, end));
        }

        let _ = cache
            .batch_locate_key_ranges_with_opts(ranges, std::iter::empty())
            .await?;

        let calls = retry_client.batch_scan_regions_calls.lock().await.clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.len(), 2048);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_repeats_pd_batch_scan_until_complete(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        // Prepare more regions than DEFAULT_REGIONS_PER_BATCH so a single PD BatchScanRegions call
        // will not cover the entire range.
        //
        // We use 200 contiguous regions: [-inf, 1), [1, 2), ..., [199, +inf).
        for i in 0u8..200 {
            let id = u64::from(i) + 1;
            let start_key = if i == 0 { vec![] } else { vec![i] };
            let end_key = if i == 199 { vec![] } else { vec![i + 1] };
            retry_client
                .regions
                .lock()
                .await
                .insert(id, region_with_leader(id, start_key, end_key, 42));
        }

        let _ = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![0], vec![200])],
                std::iter::empty(),
            )
            .await?;

        // Preheat should keep scanning until the requested ranges are covered so the main locate
        // path doesn't fall back to per-key PD lookups.
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 2);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_preheats_on_mid_range_cache_miss(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        // PD side knows about the full keyspace.
        retry_client
            .regions
            .lock()
            .await
            .insert(1, region_with_leader(1, vec![], vec![10], 42));
        retry_client
            .regions
            .lock()
            .await
            .insert(2, region_with_leader(2, vec![10], vec![20], 42));
        retry_client
            .regions
            .lock()
            .await
            .insert(3, region_with_leader(3, vec![20], vec![], 42));

        // Warm the local cache partially: the range start hits cache, but the tail will miss.
        cache
            .add_region(region_with_leader(1, vec![], vec![10], 42))
            .await;
        cache
            .add_region(region_with_leader(2, vec![10], vec![20], 42))
            .await;

        let _ = cache
            .batch_locate_key_ranges_with_opts(
                vec![crate::kv::KeyRange::new(vec![1], vec![25])],
                std::iter::empty(),
            )
            .await?;

        // Even if the range start hits cache, preheat should still batch-load the missing tail so
        // the main locate path doesn't fall back to per-key PD lookups.
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        let calls = retry_client.batch_scan_regions_calls.lock().await.clone();
        assert_eq!(calls.len(), 1);
        let sent = &calls[0].0;
        assert!(
            sent.windows(2)
                .all(|pair| pair[0].start_key <= pair[1].start_key),
            "expected preheat ranges sent to PD to be sorted"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_preheats_when_ranges_unsorted(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let region1 = region_with_leader(1, vec![], vec![10], 42);
        let region2 = region_with_leader(2, vec![10], vec![20], 42);
        let region3 = region_with_leader(3, vec![20], vec![], 42);

        retry_client.regions.lock().await.insert(1, region1);
        retry_client.regions.lock().await.insert(2, region2);
        retry_client.regions.lock().await.insert(3, region3);

        // Caller-provided ranges may not be sorted. Preheating should still send sorted ranges to
        // PD, while keeping the final locate output order unchanged.
        let _ = cache
            .batch_locate_key_ranges_with_opts(
                vec![
                    crate::kv::KeyRange::new(vec![12], vec![18]),
                    crate::kv::KeyRange::new(vec![1], vec![3]),
                    crate::kv::KeyRange::new(vec![3], vec![8]),
                ],
                std::iter::empty(),
            )
            .await?;

        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_merges_partial_cache_with_pd_results(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        for region in [
            region_with_leader_version(1, b"", b"a", 1),
            region_with_leader_version(2, b"a", b"b", 1),
            region_with_leader_version(3, b"b", b"c", 1),
            region_with_leader_version(4, b"c", b"d", 1),
            region_with_leader_version(5, b"d", b"e", 1),
            region_with_leader_version(6, b"e", b"f", 1),
            region_with_leader_version(7, b"f", b"g", 1),
            region_with_leader_version(8, b"g", b"", 1),
        ] {
            retry_client
                .regions
                .lock()
                .await
                .insert(region.id(), region);
        }

        cache
            .add_region(region_with_leader_version(2, b"a", b"b", 1))
            .await;
        cache
            .add_region(region_with_leader_version(6, b"e", b"f", 1))
            .await;

        let locations = cache
            .batch_locate_key_ranges(vec![
                crate::kv::KeyRange::new(b"a".to_vec(), b"d".to_vec()),
                crate::kv::KeyRange::new(b"e".to_vec(), b"g".to_vec()),
            ])
            .await?;

        assert_eq!(
            locations
                .iter()
                .map(|location| location.region.id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 6, 7]
        );
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_matches_client_go_basic_table_cases(
    ) -> Result<()> {
        fn key_range(start_key: &str, end_key: &str) -> crate::kv::KeyRange {
            crate::kv::KeyRange::new(start_key.as_bytes().to_vec(), end_key.as_bytes().to_vec())
        }

        #[derive(Clone)]
        struct Case {
            name: &'static str,
            ranges: Vec<crate::kv::KeyRange>,
            cached_regions: Vec<(RegionId, &'static [u8], &'static [u8])>,
            expected_ids: Vec<RegionId>,
        }

        let cases = vec![
            Case {
                name: "cached prefix before first split covers all requested ranges",
                ranges: vec![
                    key_range("A", "B"),
                    key_range("C", "D"),
                    key_range("E", "F"),
                ],
                cached_regions: vec![(1, b"", b"a")],
                expected_ids: vec![1],
            },
            Case {
                name: "cold cache walks every region in requested range",
                ranges: vec![key_range("a", "g")],
                cached_regions: Vec::new(),
                expected_ids: vec![2, 3, 4, 5, 6, 7],
            },
            Case {
                name: "disjoint ranges merge cached and pd-loaded regions in order",
                ranges: vec![key_range("a", "d"), key_range("e", "g")],
                cached_regions: vec![(2, b"a", b"b"), (6, b"e", b"f")],
                expected_ids: vec![2, 3, 4, 6, 7],
            },
        ];

        for case in cases {
            let retry_client = Arc::new(MockRetryClient::default());
            let cache =
                RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

            for region in [
                region_with_leader_version(1, b"", b"a", 1),
                region_with_leader_version(2, b"a", b"b", 1),
                region_with_leader_version(3, b"b", b"c", 1),
                region_with_leader_version(4, b"c", b"d", 1),
                region_with_leader_version(5, b"d", b"e", 1),
                region_with_leader_version(6, b"e", b"f", 1),
                region_with_leader_version(7, b"f", b"g", 1),
                region_with_leader_version(8, b"g", b"", 1),
            ] {
                retry_client
                    .regions
                    .lock()
                    .await
                    .insert(region.id(), region);
            }

            for (id, start_key, end_key) in case.cached_regions {
                cache
                    .add_region(region_with_leader_version(id, start_key, end_key, 1))
                    .await;
            }

            let locations = cache.batch_locate_key_ranges(case.ranges).await?;
            assert_eq!(
                locations
                    .iter()
                    .map(|location| location.region.id)
                    .collect::<Vec<_>>(),
                case.expected_ids,
                "{}",
                case.name
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_prefers_newer_pd_merge_over_stale_cache(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        cache
            .add_region(region_with_leader_version(2, b"a", b"b", 1))
            .await;
        cache
            .add_region(region_with_leader_version(6, b"e", b"f", 1))
            .await;

        for region in [
            region_with_leader_version(1, b"", b"a", 1),
            region_with_leader_version(2, b"a", b"c", 2),
            region_with_leader_version(4, b"c", b"d", 1),
            region_with_leader_version(5, b"d", b"e", 1),
            region_with_leader_version(6, b"e", b"f", 1),
            region_with_leader_version(7, b"f", b"g", 1),
            region_with_leader_version(8, b"g", b"", 1),
        ] {
            retry_client
                .regions
                .lock()
                .await
                .insert(region.id(), region);
        }

        let locations = cache
            .batch_locate_key_ranges(vec![
                crate::kv::KeyRange::new(b"a".to_vec(), b"d".to_vec()),
                crate::kv::KeyRange::new(b"e".to_vec(), b"g".to_vec()),
            ])
            .await?;

        assert_eq!(
            locations
                .iter()
                .map(|location| location.region.id)
                .collect::<Vec<_>>(),
            vec![2, 4, 6, 7]
        );
        assert_eq!(locations[0].start_key, Key::from(b"a".to_vec()));
        assert_eq!(locations[0].end_key, Key::from(b"c".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_keeps_cached_coverage_when_pd_split_only_covers_remaining_ranges(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        cache
            .add_region(region_with_leader_version(2, b"a", b"c", 2))
            .await;
        cache
            .add_region(region_with_leader_version(6, b"e", b"f", 1))
            .await;

        for region in [
            region_with_leader_version(1, b"", b"a", 1),
            region_with_leader_version(2, b"a", b"c", 2),
            region_with_leader_version(3, b"c", b"d", 1),
            region_with_leader_version(4, b"d", b"d2", 2),
            region_with_leader_version(9, b"d2", b"e1", 1),
            region_with_leader_version(10, b"e1", b"f", 1),
            region_with_leader_version(7, b"f", b"g", 1),
            region_with_leader_version(8, b"g", b"", 1),
        ] {
            retry_client
                .regions
                .lock()
                .await
                .insert(region.id(), region);
        }

        let locations = cache
            .batch_locate_key_ranges(vec![
                crate::kv::KeyRange::new(b"a".to_vec(), b"d3".to_vec()),
                crate::kv::KeyRange::new(b"e".to_vec(), b"g".to_vec()),
            ])
            .await?;

        assert_eq!(
            locations
                .iter()
                .map(|location| location.region.id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4, 9, 6, 7]
        );
        assert_eq!(locations[4].start_key, Key::from(b"e".to_vec()));
        assert_eq!(locations[4].end_key, Key::from(b"f".to_vec()));
        assert_eq!(retry_client.batch_scan_regions_count.load(SeqCst), 1);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_locate_key_range_with_opts_reloads_buckets() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let pd_region = region_with_leader(1, vec![], vec![10], 42);
        let ver_id = pd_region.ver_id();
        retry_client.regions.lock().await.insert(1, pd_region);
        retry_client.buckets.lock().await.insert(
            ver_id.clone(),
            metapb::Buckets {
                region_id: 1,
                version: 7,
                keys: vec![vec![], vec![4], vec![10]],
                ..Default::default()
            },
        );

        cache.add_region(region(1, vec![], vec![10])).await;

        let locations = cache
            .locate_key_range_with_opts(
                Key::from(vec![2]),
                Key::from(vec![8]),
                [with_need_buckets(), with_need_region_has_leader_peer()],
            )
            .await?;

        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].bucket_version(), 7);
        assert_eq!(
            locations[0].locate_bucket(&Key::from(vec![2])),
            Some(BucketLocation {
                start_key: vec![].into(),
                end_key: vec![4].into(),
            })
        );
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);
        assert_eq!(
            cache
                .get_buckets_by_ver_id(&ver_id)
                .await
                .map(|b| b.version),
            Some(7)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_region_cache_batch_locate_key_ranges_with_opts_skips_regions_without_leader(
    ) -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new_with_ttl(retry_client.clone(), Duration::ZERO, Duration::ZERO);

        let leaderful_region = region_with_leader(1, vec![], vec![10], 42);
        let leaderful_ver_id = leaderful_region.ver_id();
        retry_client
            .regions
            .lock()
            .await
            .insert(1, leaderful_region);
        retry_client
            .regions
            .lock()
            .await
            .insert(2, region(2, vec![10], vec![20]));
        retry_client.buckets.lock().await.insert(
            leaderful_ver_id,
            metapb::Buckets {
                region_id: 1,
                version: 11,
                keys: vec![vec![], vec![5], vec![10]],
                ..Default::default()
            },
        );

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;

        let locations = cache
            .batch_locate_key_ranges_with_opts(
                vec![
                    crate::kv::KeyRange::new(vec![1], vec![3]),
                    crate::kv::KeyRange::new(vec![3], vec![8]),
                    crate::kv::KeyRange::new(vec![12], vec![18]),
                ],
                [with_need_buckets(), with_need_region_has_leader_peer()],
            )
            .await?;

        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].region.id, 1);
        assert_eq!(locations[0].bucket_version(), 11);
        assert_eq!(
            locations[0].locate_bucket(&Key::from(vec![2])),
            Some(BucketLocation {
                start_key: vec![].into(),
                end_key: vec![5].into(),
            })
        );
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

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

    fn region_with_leader(
        id: RegionId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        store_id: u64,
    ) -> RegionWithLeader {
        let mut region = region(id, start_key, end_key);
        region.leader = Some(metapb::Peer {
            store_id,
            ..Default::default()
        });
        region
    }

    fn region_with_leader_version(
        id: RegionId,
        start_key: &[u8],
        end_key: &[u8],
        version: u64,
    ) -> RegionWithLeader {
        let mut region = region_with_leader(id, start_key.to_vec(), end_key.to_vec(), id + 1000);
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version,
        });
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

    #[tokio::test]
    async fn test_pd_region_meta_circuit_breaker_short_circuits_region_loads() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());

        let mut cache = RegionCache::new_with_ttl(
            retry_client.clone(),
            Duration::from_secs(1),
            Duration::from_secs(0),
        );
        cache.pd_region_meta_circuit_breaker = Arc::new(PdRegionMetaCircuitBreaker::new(
            crate::PdRegionMetaCircuitBreakerSettings {
                error_rate_window: Duration::from_millis(50),
                min_qps_for_open: 1,
                cool_down_interval: Duration::from_secs(3600),
                half_open_success_count: 1,
            },
        ));

        let _ = cache
            .read_through_region_by_key(Key::from(vec![1]))
            .await
            .expect_err("expected mock PD failure");
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        let err = cache
            .read_through_region_by_key(Key::from(vec![2]))
            .await
            .expect_err("expected circuit breaker to reject call");
        assert!(matches!(err, Error::InternalError { .. }));
        assert_eq!(
            retry_client.get_region_count.load(SeqCst),
            1,
            "circuit breaker must short-circuit PD get_region calls"
        );

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
