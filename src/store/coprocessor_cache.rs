use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::config::CoprocessorCacheConfig;
use crate::proto::coprocessor;
use crate::proto::kvrpcpb;
use crate::Error;
use crate::Result;

const BYTES_PER_MIB: u128 = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct CoprocessorCacheValue {
    key: Vec<u8>,
    data: Vec<u8>,
    timestamp: u64,
    region_id: u64,
    region_data_version: u64,
    page_start: Option<Vec<u8>>,
    page_end: Option<Vec<u8>>,
    estimated_cost: usize,
}

impl CoprocessorCacheValue {
    fn new(
        key: Vec<u8>,
        data: Vec<u8>,
        timestamp: u64,
        region_id: u64,
        region_data_version: u64,
        page_start: Option<Vec<u8>>,
        page_end: Option<Vec<u8>>,
    ) -> Self {
        let estimated_cost = std::mem::size_of::<CoprocessorCacheValue>()
            + key.len()
            + data.len()
            + page_start.as_ref().map_or(0, Vec::len)
            + page_end.as_ref().map_or(0, Vec::len);
        CoprocessorCacheValue {
            key,
            data,
            timestamp,
            region_id,
            region_data_version,
            page_start,
            page_end,
            estimated_cost,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CoprocessorCacheKeyHash(u64);

#[derive(Clone, Debug)]
pub(crate) struct CoprocessorCacheLookup {
    pub(crate) key: Vec<u8>,
    pub(crate) hash: CoprocessorCacheKeyHash,
    pub(crate) value: Option<Arc<CoprocessorCacheValue>>,
}

#[derive(Debug)]
struct CoprocessorCacheEntry {
    value: Arc<CoprocessorCacheValue>,
    access_id: u64,
}

#[derive(Debug, Default)]
struct CoprocessorCacheInner {
    entries: HashMap<u64, CoprocessorCacheEntry>,
    lru: VecDeque<(u64, u64)>,
    next_access_id: u64,
    total_estimated_cost: usize,
}

impl CoprocessorCacheInner {
    fn touch(&mut self, hash: CoprocessorCacheKeyHash, entry: &mut CoprocessorCacheEntry) {
        self.next_access_id = self.next_access_id.wrapping_add(1);
        entry.access_id = self.next_access_id;
        self.lru.push_back((hash.0, entry.access_id));
    }

    fn evict_if_needed(&mut self, max_cost: usize) -> usize {
        let mut evicted = 0;
        while self.total_estimated_cost > max_cost {
            let Some((hash, access_id)) = self.lru.pop_front() else {
                break;
            };
            let Some(entry) = self.entries.get(&hash) else {
                continue;
            };
            if entry.access_id != access_id {
                continue;
            }
            let removed = self.entries.remove(&hash).expect("entry must exist");
            self.total_estimated_cost = self
                .total_estimated_cost
                .saturating_sub(removed.value.estimated_cost);
            evicted += 1;
        }

        if self.lru.len() > self.entries.len().saturating_mul(4).max(1024) {
            self.rebuild_lru();
        }

        evicted
    }

    fn rebuild_lru(&mut self) {
        self.lru.clear();
        for (hash, entry) in self.entries.iter() {
            self.lru.push_back((*hash, entry.access_id));
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CoprocessorCache {
    max_cost: usize,
    admission_max_ranges: u64,
    admission_max_result_bytes: usize,
    admission_min_process_time: Duration,
    inner: Arc<Mutex<CoprocessorCacheInner>>,
}

impl CoprocessorCache {
    pub(crate) fn from_config(config: &CoprocessorCacheConfig) -> Option<CoprocessorCache> {
        if config.capacity_mb == 0 {
            return None;
        }

        if config.admission_max_result_mb == 0 {
            warn_invalid_copr_cache_config(
                "admission_max_result_mb must be > 0 to enable coprocessor cache",
            );
            return None;
        }

        let max_cost = mib_to_usize_bytes(config.capacity_mb);
        let admission_max_result_bytes = mib_to_usize_bytes(config.admission_max_result_mb);
        Some(CoprocessorCache {
            max_cost,
            admission_max_ranges: config.admission_max_ranges,
            admission_max_result_bytes,
            admission_min_process_time: Duration::from_millis(config.admission_min_process_ms),
            inner: Arc::new(Mutex::new(CoprocessorCacheInner::default())),
        })
    }

    pub(crate) fn check_request_admission(&self, ranges: usize) -> bool {
        if self.admission_max_ranges == 0 {
            return true;
        }
        (ranges as u64) <= self.admission_max_ranges
    }

    pub(crate) fn check_response_admission(
        &self,
        data_size: usize,
        process_time: Duration,
        paging_task_idx: u32,
    ) -> bool {
        if data_size == 0 || data_size > self.admission_max_result_bytes {
            return false;
        }
        if paging_task_idx > 50 {
            return false;
        }

        let mut min_process_time = self.admission_min_process_time;
        if paging_task_idx > 0 {
            min_process_time /= 3;
        }
        process_time >= min_process_time
    }

    pub(crate) fn build_key(cop_req: &coprocessor::Request) -> Result<Vec<u8>> {
        copr_cache_build_key(cop_req)
    }

    pub(crate) fn lookup(
        &self,
        key: &[u8],
    ) -> (CoprocessorCacheKeyHash, Option<Arc<CoprocessorCacheValue>>) {
        let hash = CoprocessorCacheKeyHash(hash_bytes(key));
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let is_match = inner
            .entries
            .get(&hash.0)
            .is_some_and(|entry| entry.value.key == key);
        if !is_match {
            return (hash, None);
        }

        inner.next_access_id = inner.next_access_id.wrapping_add(1);
        let access_id = inner.next_access_id;

        let value = {
            let entry = inner.entries.get_mut(&hash.0).expect("entry must exist");
            entry.access_id = access_id;
            Arc::clone(&entry.value)
        };
        inner.lru.push_back((hash.0, access_id));
        (hash, Some(value))
    }

    pub(crate) fn insert(
        &self,
        hash: CoprocessorCacheKeyHash,
        value: CoprocessorCacheValue,
    ) -> bool {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(existing) = inner.entries.remove(&hash.0) {
            inner.total_estimated_cost = inner
                .total_estimated_cost
                .saturating_sub(existing.value.estimated_cost);
        }

        inner.total_estimated_cost = inner
            .total_estimated_cost
            .saturating_add(value.estimated_cost);

        let value = Arc::new(value);
        let mut entry = CoprocessorCacheEntry {
            value,
            access_id: 0,
        };
        inner.touch(CoprocessorCacheKeyHash(hash.0), &mut entry);
        inner.entries.insert(hash.0, entry);

        inner.evict_if_needed(self.max_cost);
        true
    }

    pub(crate) fn prepare_request(
        &self,
        req: &mut coprocessor::Request,
        region_id: u64,
    ) -> Option<CoprocessorCacheLookup> {
        if !self.check_request_admission(req.ranges.len()) {
            return None;
        }

        let key = Self::build_key(req).ok()?;
        let (hash, cached) = self.lookup(&key);
        req.is_cache_enabled = true;

        let mut selected = None;
        if let Some(value) = cached {
            if value.region_id == region_id && value.timestamp <= req.start_ts {
                req.cache_if_match_version = value.region_data_version;
                selected = Some(value);
            } else {
                req.cache_if_match_version = 0;
            }
        } else {
            req.cache_if_match_version = 0;
        }

        Some(CoprocessorCacheLookup {
            key,
            hash,
            value: selected,
        })
    }

    pub(crate) fn handle_response(
        &self,
        req: &coprocessor::Request,
        region_id: u64,
        lookup: Option<CoprocessorCacheLookup>,
        resp: &mut coprocessor::Response,
    ) -> Result<()> {
        if resp.is_cache_hit {
            let Some(value) = lookup.and_then(|lookup| lookup.value) else {
                return Err(Error::InternalError {
                    message: "received illegal TiKV response: cache_hit without local cache value"
                        .to_owned(),
                });
            };
            resp.data = value.data.clone();
            if req.paging_size > 0 {
                let start = value.page_start.clone();
                let end = value.page_end.clone();
                resp.range = match (start, end) {
                    (None, None) => None,
                    (start, end) => Some(coprocessor::KeyRange {
                        start: start.unwrap_or_default(),
                        end: end.unwrap_or_default(),
                    }),
                };
            }
            return Ok(());
        }

        let Some(lookup) = lookup else {
            return Ok(());
        };

        if !resp.can_be_cached || resp.cache_last_version == 0 {
            return Ok(());
        }

        let Some(process_time) = extract_process_time(resp) else {
            return Ok(());
        };

        if !self.check_response_admission(resp.data.len(), process_time, 0) {
            return Ok(());
        }

        let (page_start, page_end) = resp
            .range
            .as_ref()
            .map(|r| (Some(r.start.clone()), Some(r.end.clone())))
            .unwrap_or((None, None));

        let value = CoprocessorCacheValue::new(
            lookup.key,
            resp.data.clone(),
            req.start_ts,
            region_id,
            resp.cache_last_version,
            page_start,
            page_end,
        );
        self.insert(lookup.hash, value);
        Ok(())
    }
}

fn copr_cache_build_key(cop_req: &coprocessor::Request) -> Result<Vec<u8>> {
    if !(0..=u8::MAX as i64).contains(&cop_req.tp) {
        return Err(Error::StringError(
            "coprocessor request tp too big".to_owned(),
        ));
    }

    let data_len = cop_req.data.len();
    if data_len > u32::MAX as usize {
        return Err(Error::StringError(
            "coprocessor cache data too big".to_owned(),
        ));
    }

    let mut total_len = 1usize
        .checked_add(4)
        .and_then(|len| len.checked_add(data_len))
        .ok_or_else(|| Error::StringError("coprocessor cache key too big".to_owned()))?;

    for r in &cop_req.ranges {
        if r.start.len() > u16::MAX as usize {
            return Err(Error::StringError(
                "coprocessor cache start key too big".to_owned(),
            ));
        }
        if r.end.len() > u16::MAX as usize {
            return Err(Error::StringError(
                "coprocessor cache end key too big".to_owned(),
            ));
        }
        total_len = total_len
            .checked_add(2 + r.start.len() + 2 + r.end.len())
            .ok_or_else(|| Error::StringError("coprocessor cache key too big".to_owned()))?;
    }

    if cop_req.paging_size > 0 {
        total_len = total_len
            .checked_add(1)
            .ok_or_else(|| Error::StringError("coprocessor cache key too big".to_owned()))?;
    }

    let mut key = vec![0u8; total_len];
    key[0] = cop_req.tp as u8;
    let mut dest = 1usize;

    key[dest..dest + 4].copy_from_slice(&(data_len as u32).to_le_bytes());
    dest += 4;

    key[dest..dest + data_len].copy_from_slice(&cop_req.data);
    dest += data_len;

    for r in &cop_req.ranges {
        key[dest..dest + 2].copy_from_slice(&(r.start.len() as u16).to_le_bytes());
        dest += 2;
        key[dest..dest + r.start.len()].copy_from_slice(&r.start);
        dest += r.start.len();

        key[dest..dest + 2].copy_from_slice(&(r.end.len() as u16).to_le_bytes());
        dest += 2;
        key[dest..dest + r.end.len()].copy_from_slice(&r.end);
        dest += r.end.len();
    }

    if cop_req.paging_size > 0 {
        key[dest] = 1;
    }
    Ok(key)
}

fn extract_process_time(resp: &coprocessor::Response) -> Option<Duration> {
    let v2 = resp.exec_details_v2.as_ref();
    if let Some(time_detail_v2) = v2.and_then(|d| d.time_detail_v2.as_ref()) {
        return Some(Duration::from_nanos(time_detail_v2.process_wall_time_ns));
    }
    if let Some(time_detail) = v2.and_then(|d| d.time_detail.as_ref()) {
        return Some(Duration::from_millis(time_detail.process_wall_time_ms));
    }

    let v1 = resp.exec_details.as_ref();
    v1.and_then(|d| d.time_detail.as_ref())
        .map(|d| Duration::from_millis(d.process_wall_time_ms))
}

fn mib_to_usize_bytes(mib: u64) -> usize {
    let bytes = (mib as u128).saturating_mul(BYTES_PER_MIB);
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

fn warn_invalid_copr_cache_config(message: &str) {
    let _ = message;
    // Intentionally no logging dependency here; callers decide whether to surface the warning.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_key_matches_tidb() {
        let request = coprocessor::Request {
            tp: 0xAB,
            start_ts: 0xAABBCC,
            data: vec![0x18, 0x0, 0x20, 0x0, 0x40, 0x0, 0x5a, 0x0],
            ranges: vec![
                coprocessor::KeyRange {
                    start: vec![0x01],
                    end: vec![0x01, 0x02],
                },
                coprocessor::KeyRange {
                    start: vec![0x01, 0x01, 0x02],
                    end: vec![0x01, 0x01, 0x03],
                },
            ],
            ..Default::default()
        };

        let key = CoprocessorCache::build_key(&request).expect("expected cache key");
        let expected: &[u8] = &[
            0xAB, // 1 byte Tp
            0x08, 0x00, 0x00, 0x00, // 4 bytes Data len (little endian)
            0x18, 0x00, 0x20, 0x00, 0x40, 0x00, 0x5a, 0x00, // Data
            0x01, 0x00, // 2 bytes StartKey len
            0x01, // StartKey
            0x02, 0x00, // 2 bytes EndKey len
            0x01, 0x02, // EndKey
            0x03, 0x00, // 2 bytes StartKey len
            0x01, 0x01, 0x02, // StartKey
            0x03, 0x00, // 2 bytes EndKey len
            0x01, 0x01, 0x03, // EndKey
        ];
        assert_eq!(key, expected);
    }

    #[test]
    fn test_build_key_rejects_large_tp() {
        let request = coprocessor::Request {
            tp: 0xABCC,
            data: vec![0x18],
            ..Default::default()
        };
        assert!(CoprocessorCache::build_key(&request).is_err());
    }

    #[test]
    fn test_admission_rules_match_tidb() {
        let config = CoprocessorCacheConfig {
            capacity_mb: 1,
            admission_max_ranges: 0,
            admission_max_result_mb: 1,
            admission_min_process_ms: 5,
        };
        let cache = CoprocessorCache::from_config(&config).expect("expected enabled cache");

        assert!(cache.check_request_admission(0));
        assert!(cache.check_request_admission(1000));

        assert!(!cache.check_response_admission(0, Duration::from_millis(0), 0));
        assert!(!cache.check_response_admission(0, Duration::from_millis(4), 0));
        assert!(!cache.check_response_admission(0, Duration::from_millis(5), 0));

        assert!(!cache.check_response_admission(1, Duration::from_millis(0), 0));
        assert!(!cache.check_response_admission(1, Duration::from_millis(4), 0));
        assert!(cache.check_response_admission(1, Duration::from_millis(5), 0));

        assert!(cache.check_response_admission(1024, Duration::from_millis(5), 0));
        assert!(cache.check_response_admission(1024 * 1024, Duration::from_millis(5), 0));
        assert!(!cache.check_response_admission(1024 * 1024 + 1, Duration::from_millis(5), 0));
        assert!(!cache.check_response_admission(1024 * 1024 + 1, Duration::from_millis(4), 0));

        assert!(cache.check_response_admission(1024, Duration::from_millis(4), 1));
        assert!(!cache.check_response_admission(1024, Duration::from_millis(4), 51));

        let config = CoprocessorCacheConfig {
            admission_max_ranges: 5,
            ..config
        };
        let cache = CoprocessorCache::from_config(&config).expect("expected enabled cache");
        assert!(cache.check_request_admission(0));
        assert!(cache.check_request_admission(5));
        assert!(!cache.check_request_admission(6));
    }

    #[test]
    fn test_from_config_disables_cache_for_invalid_config() {
        let config = CoprocessorCacheConfig {
            capacity_mb: 1,
            admission_max_ranges: 0,
            admission_max_result_mb: 0,
            admission_min_process_ms: 5,
        };
        assert!(CoprocessorCache::from_config(&config).is_none());

        let config = CoprocessorCacheConfig {
            capacity_mb: 0,
            admission_max_ranges: 0,
            admission_max_result_mb: 1,
            admission_min_process_ms: 5,
        };
        assert!(CoprocessorCache::from_config(&config).is_none());
    }

    #[test]
    fn test_prepare_and_handle_response_round_trip() -> Result<()> {
        let config = CoprocessorCacheConfig {
            capacity_mb: 1,
            admission_max_ranges: 500,
            admission_max_result_mb: 1,
            admission_min_process_ms: 5,
        };
        let cache = CoprocessorCache::from_config(&config).expect("expected enabled cache");

        let mut req = coprocessor::Request {
            tp: 1,
            start_ts: 10,
            data: vec![1, 2, 3],
            ranges: vec![coprocessor::KeyRange {
                start: vec![1],
                end: vec![2],
            }],
            ..Default::default()
        };

        let region_id = 7;
        let lookup = cache.prepare_request(&mut req, region_id);
        assert!(req.is_cache_enabled);
        assert_eq!(req.cache_if_match_version, 0);

        let mut resp = coprocessor::Response {
            data: vec![9],
            can_be_cached: true,
            cache_last_version: 123,
            exec_details: Some(kvrpcpb::ExecDetails {
                time_detail: Some(kvrpcpb::TimeDetail {
                    process_wall_time_ms: 5,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        cache.handle_response(&req, region_id, lookup, &mut resp)?;

        let mut req2 = coprocessor::Request {
            tp: 1,
            start_ts: 11,
            data: vec![1, 2, 3],
            ranges: vec![coprocessor::KeyRange {
                start: vec![1],
                end: vec![2],
            }],
            ..Default::default()
        };
        let lookup2 = cache
            .prepare_request(&mut req2, region_id)
            .expect("expected lookup");
        assert_eq!(req2.cache_if_match_version, 123);
        assert!(lookup2.value.is_some());

        let mut resp2 = coprocessor::Response {
            is_cache_hit: true,
            ..Default::default()
        };
        cache.handle_response(&req2, region_id, Some(lookup2), &mut resp2)?;
        assert_eq!(resp2.data, vec![9]);
        Ok(())
    }
}
