// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use derive_new::new;
use log::{debug, trace};

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::transaction::ResolveLockDetail;
use crate::transaction::Variables;
use crate::BoundRange;
use crate::CommandPriority;
use crate::IsolationLevel;
use crate::Key;
use crate::KvPair;
use crate::ReplicaReadType;
use crate::Result;
use crate::StoreLabel;
use crate::Timestamp;
use crate::Transaction;
use crate::Value;
use std::collections::{HashMap, HashSet};

use std::time::Duration;

/// A cached snapshot entry for point/batch reads.
///
/// This mirrors client-go `kv.ValueEntry` as used by `KVSnapshot`'s snapshot cache.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotCacheEntry {
    /// Cached value, or `None` if the key was not found.
    pub value: Option<Value>,
    /// Cached commit timestamp for MVCC reads. `0` means "not cached".
    pub commit_ts: u64,
}

const SNAPSHOT_CACHE_SIZE_LIMIT_BYTES: usize = 10 << 30;

#[derive(Debug, Default)]
struct SnapshotCache {
    hit_count: u64,
    cached_size_bytes: usize,
    cached: HashMap<Key, SnapshotCacheEntry>,
}

impl SnapshotCacheEntry {
    fn size(&self) -> usize {
        self.value.as_ref().map(Vec::len).unwrap_or(0)
            + if self.commit_ts > 0 {
                std::mem::size_of::<u64>()
            } else {
                0
            }
    }
}

impl SnapshotCache {
    fn hit_count(&self) -> u64 {
        self.hit_count
    }

    fn len(&self) -> usize {
        self.cached.len()
    }

    fn snapshot(&self) -> HashMap<Key, SnapshotCacheEntry> {
        self.cached.clone()
    }

    fn clear(&mut self) {
        self.cached.clear();
        self.cached_size_bytes = 0;
    }

    fn get(&mut self, key: &Key, require_commit_ts: bool) -> Option<SnapshotCacheEntry> {
        let mut entry = self.cached.get(key)?.clone();

        if require_commit_ts && entry.value.is_some() && entry.commit_ts == 0 {
            return None;
        }

        if !require_commit_ts {
            entry.commit_ts = 0;
        }

        self.hit_count += 1;
        Some(entry)
    }

    fn update<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = (Key, SnapshotCacheEntry)>,
    {
        let mut entries: Vec<(Key, SnapshotCacheEntry)> = entries.into_iter().collect();
        if entries.is_empty() {
            return;
        }

        let keep_keys: HashSet<Key> = entries.iter().map(|(key, _)| key.clone()).collect();

        for (key, entry) in entries.drain(..) {
            let entry_size = key.len() + entry.size();
            if let Some(old) = self.cached.insert(key.clone(), entry) {
                self.cached_size_bytes = self
                    .cached_size_bytes
                    .saturating_sub(key.len() + old.size());
            }
            self.cached_size_bytes = self.cached_size_bytes.saturating_add(entry_size);
        }

        if self.cached_size_bytes < SNAPSHOT_CACHE_SIZE_LIMIT_BYTES {
            return;
        }

        let mut evict_candidates: Vec<Key> = self
            .cached
            .keys()
            .filter(|key| !keep_keys.contains(*key))
            .cloned()
            .collect();

        for key in evict_candidates.drain(..) {
            if self.cached_size_bytes < SNAPSHOT_CACHE_SIZE_LIMIT_BYTES {
                break;
            }
            let Some(old) = self.cached.remove(&key) else {
                continue;
            };
            self.cached_size_bytes = self
                .cached_size_bytes
                .saturating_sub(key.len() + old.size());
        }
    }

    fn clean<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = Key>,
    {
        for key in keys {
            let Some(old) = self.cached.remove(&key) else {
                continue;
            };
            self.cached_size_bytes = self
                .cached_size_bytes
                .saturating_sub(key.len() + old.size());
        }
    }
}

/// A read-only transaction which reads at the given timestamp.
///
/// It behaves as if the snapshot was taken at the given timestamp,
/// i.e. it can read operations happened before the timestamp,
/// but ignores operations after the timestamp.
///
/// See the [Transaction](struct@crate::Transaction) docs for more information on the methods.
#[derive(new)]
pub struct Snapshot<PdC: PdClient = PdRpcClient> {
    transaction: Transaction<PdC>,
    #[new(default)]
    cache: SnapshotCache,
}

impl<PdC: PdClient> Snapshot<PdC> {
    fn cache_enabled(&self) -> bool {
        // Match client-go: `math.MaxUint64` means "read latest"; avoid caching to prevent anomaly.
        self.transaction.start_ts() != u64::MAX
    }

    /// Set the KV variables used by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetVars`.
    pub fn set_vars(&mut self, vars: Variables) {
        self.transaction.set_vars(vars);
    }

    /// Get the KV variables used by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.GetVars`.
    #[must_use]
    pub fn vars(&self) -> &Variables {
        self.transaction.vars()
    }

    /// Get lock-resolution runtime stats accumulated by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.GetResolveLockDetail`.
    #[must_use]
    pub fn resolve_lock_detail(&self) -> ResolveLockDetail {
        self.transaction.resolve_lock_detail()
    }

    /// Set the snapshot timestamp.
    ///
    /// This maps to client-go `KVSnapshot.SetSnapshotTS`.
    ///
    /// This resets snapshot-local caches, including:
    /// - the snapshot cache (`SnapCache*`), and
    /// - the resolved-lock tracking used by resolve-locks-for-read.
    pub fn set_snapshot_ts(&mut self, timestamp: Timestamp) -> Result<()> {
        self.transaction.set_snapshot_ts(timestamp)?;
        self.cache.clear();
        Ok(())
    }

    /// Get the snapshot cache hit count.
    ///
    /// This maps to client-go `KVSnapshot.SnapCacheHitCount` (primarily for testing/debugging).
    #[must_use]
    pub fn snap_cache_hit_count(&self) -> u64 {
        self.cache.hit_count()
    }

    /// Get the number of entries currently stored in the snapshot cache.
    ///
    /// This maps to client-go `KVSnapshot.SnapCacheSize` (primarily for testing/debugging).
    #[must_use]
    pub fn snap_cache_size(&self) -> usize {
        self.cache.len()
    }

    /// Get a copy of the snapshot cache.
    ///
    /// This maps to client-go `KVSnapshot.SnapCache` (primarily for testing/debugging).
    #[must_use]
    pub fn snap_cache(&self) -> HashMap<Key, SnapshotCacheEntry> {
        self.cache.snapshot()
    }

    /// Update snapshot cache entries for further fast reads with the same keys.
    ///
    /// This maps to client-go `KVSnapshot.UpdateSnapshotCache`.
    pub fn update_snapshot_cache(
        &mut self,
        entries: impl IntoIterator<Item = (impl Into<Key>, SnapshotCacheEntry)>,
    ) {
        if !self.cache_enabled() {
            return;
        }

        self.cache
            .update(entries.into_iter().map(|(key, entry)| (key.into(), entry)));
    }

    /// Remove the snapshot cache entries for the given keys.
    ///
    /// This maps to client-go `KVSnapshot.CleanCache` (primarily for testing/debugging).
    pub fn clean_cache(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) {
        self.cache.clean(keys.into_iter().map(Into::into));
    }

    /// Set an RPC interceptor for this snapshot.
    ///
    /// The interceptor is applied to all TiKV RPC requests initiated by this snapshot,
    /// including lock resolution and read RPCs.
    ///
    /// This maps to client-go `KVSnapshot.SetRPCInterceptor`.
    pub fn set_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.transaction.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor.
    ///
    /// Interceptors are executed in an "onion model": interceptors added earlier execute earlier,
    /// but return later. If multiple interceptors with the same name are added, only the last one
    /// is kept.
    ///
    /// This maps to client-go `KVSnapshot.AddRPCInterceptor`.
    pub fn add_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.transaction.add_rpc_interceptor(interceptor);
    }

    /// Clear all configured RPC interceptors.
    pub fn clear_rpc_interceptors(&mut self) {
        self.transaction.clear_rpc_interceptors();
    }

    /// Set replica read behavior.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_replica_read(&mut self, read_type: ReplicaReadType) {
        self.transaction.set_replica_read(read_type);
    }

    /// Set a replica read adjuster for point/batch gets.
    ///
    /// This option is only effective when `TransactionOptions::replica_read` is
    /// configured to a follower-read type.
    pub fn set_replica_read_adjuster<F>(&mut self, adjuster: F)
    where
        F: Fn(usize) -> ReplicaReadType + Send + Sync + 'static,
    {
        self.transaction.set_replica_read_adjuster(adjuster);
    }

    /// Set the busy threshold for read requests.
    ///
    /// This maps to client-go `KVSnapshot.SetLoadBasedReplicaReadThreshold` and writes to
    /// `kvrpcpb::Context.busy_threshold_ms`.
    pub fn set_load_based_replica_read_threshold(&mut self, threshold: Duration) {
        self.transaction
            .set_load_based_replica_read_threshold(threshold);
    }

    /// Set labels to filter target stores for replica reads.
    ///
    /// This maps to client-go `KVSnapshot.SetMatchStoreLabels`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_labels(&mut self, labels: impl IntoIterator<Item = StoreLabel>) {
        self.transaction.set_match_store_labels(labels);
    }

    /// Set store ids to filter target stores for replica reads.
    ///
    /// This maps to client-go `tikv.WithMatchStores` / `locate.WithMatchStores`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_ids(&mut self, store_ids: impl IntoIterator<Item = u64>) {
        self.transaction.set_match_store_ids(store_ids);
    }

    /// Enable or disable stale reads for this snapshot.
    ///
    /// When enabled, read requests will set `kvrpcpb::Context.stale_read = true`.
    ///
    /// This maps to client-go `KVSnapshot.SetIsStalenessReadOnly`.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.transaction.set_stale_read(stale_read);
    }

    /// Set whether read requests should fill TiKV block cache.
    ///
    /// This maps to client-go `KVSnapshot.SetNotFillCache`.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.transaction.set_not_fill_cache(not_fill_cache);
    }

    /// Set task ID hint for TiKV.
    ///
    /// This maps to client-go `KVSnapshot.SetTaskID`.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.transaction.set_task_id(task_id);
    }

    /// Set server-side maximum execution duration for read requests.
    ///
    /// This option writes to `kvrpcpb::Context.max_execution_duration_ms`.
    pub fn set_max_execution_duration(&mut self, duration: Duration) {
        self.transaction.set_max_execution_duration(duration);
    }

    /// Set timeout for individual KV read operations under this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetKVReadTimeout`.
    pub fn set_kv_read_timeout(&mut self, read_timeout: Duration) {
        self.transaction.set_kv_read_timeout(read_timeout);
    }

    /// Get the configured per-snapshot KV read timeout.
    ///
    /// Returns [`Duration::ZERO`] if unset.
    ///
    /// This maps to client-go `KVSnapshot.GetKVReadTimeout`.
    #[must_use]
    pub fn kv_read_timeout(&self) -> Duration {
        self.transaction.kv_read_timeout()
    }

    /// Set the priority for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetPriority`.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.transaction.set_priority(priority);
    }

    /// Set the isolation level for read requests.
    ///
    /// This maps to client-go `KVSnapshot.SetIsolationLevel`.
    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.transaction.set_isolation_level(isolation_level);
    }

    /// Set resource group tag for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTag`.
    pub fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.transaction.set_resource_group_tag(tag);
    }

    /// Set a resource group tagger used to fill `kvrpcpb::Context.resource_group_tag`.
    ///
    /// The tagger is invoked only when no explicit resource group tag is configured via
    /// [`Snapshot::set_resource_group_tag`] / [`crate::TransactionOptions::resource_group_tag`],
    /// matching client-go behavior.
    ///
    /// The tagger input is the request label (for example, `"kv_get"` or `"kv_commit"`).
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTagger`.
    pub fn set_resource_group_tagger<F>(&mut self, tagger: F)
    where
        F: Fn(&str) -> Vec<u8> + Send + Sync + 'static,
    {
        self.transaction.set_resource_group_tagger(tagger);
    }

    /// Clear the configured resource group tagger.
    pub fn clear_resource_group_tagger(&mut self) {
        self.transaction.clear_resource_group_tagger();
    }

    /// Set resource group name for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupName`.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.transaction.set_resource_group_name(name);
    }

    /// Set request source for requests.
    ///
    /// This option writes to `kvrpcpb::Context.request_source`.
    ///
    /// For client-go compatible formatting (internal/external prefixes and optional explicit type),
    /// use [`RequestSource`](crate::RequestSource).
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.transaction.set_request_source(source);
    }

    /// Returns true if this snapshot is used by internal executions.
    ///
    /// This maps to client-go `KVSnapshot.IsInternal`.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.transaction
            .request_source()
            .is_some_and(crate::request_context::is_internal_request_source)
    }

    /// Get the value associated with the given key.
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking get request on snapshot");
        let key = key.into();
        if self.cache_enabled() {
            if let Some(entry) = self.cache.get(&key, false) {
                return Ok(entry.value);
            }
        }

        let value = self.transaction.get(key.clone()).await?;
        if self.cache_enabled() {
            self.cache.update([(
                key,
                SnapshotCacheEntry {
                    value: value.clone(),
                    commit_ts: 0,
                },
            )]);
        }
        Ok(value)
    }

    /// Get the value associated with the given key and its commit timestamp.
    ///
    /// Returns [`crate::Error::CommitTsRequiredButNotReturned`] if TiKV does not return a commit
    /// timestamp for an existing key.
    pub async fn get_with_commit_ts(
        &mut self,
        key: impl Into<Key>,
    ) -> Result<Option<(Value, u64)>> {
        trace!("invoking get_with_commit_ts request on snapshot");
        let key = key.into();
        if self.cache_enabled() {
            if let Some(entry) = self.cache.get(&key, true) {
                return Ok(entry.value.map(|value| (value, entry.commit_ts)));
            }
        }

        let value = self.transaction.get_with_commit_ts(key.clone()).await?;
        if self.cache_enabled() {
            let entry = match &value {
                Some((value, commit_ts)) => SnapshotCacheEntry {
                    value: Some(value.clone()),
                    commit_ts: *commit_ts,
                },
                None => SnapshotCacheEntry {
                    value: None,
                    commit_ts: 0,
                },
            };
            self.cache.update([(key, entry)]);
        }
        Ok(value)
    }

    /// Check whether the key exists.
    pub async fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        debug!("invoking key_exists request on snapshot");
        self.transaction.key_exists(key).await
    }

    /// Get the values associated with the given keys.
    pub async fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking batch_get request on snapshot");
        let keys: Vec<Key> = keys.into_iter().map(Into::into).collect();
        if keys.is_empty() {
            return Ok(Vec::<KvPair>::new().into_iter());
        }

        let mut out = Vec::new();
        let mut missing = Vec::new();
        if self.cache_enabled() {
            for key in &keys {
                if let Some(entry) = self.cache.get(key, false) {
                    if let Some(value) = entry.value {
                        out.push(KvPair(key.clone(), value));
                    }
                } else {
                    missing.push(key.clone());
                }
            }
        } else {
            missing = keys.clone();
        }

        if missing.is_empty() {
            return Ok(out.into_iter());
        }

        let fetched: Vec<KvPair> = self.transaction.batch_get(missing.clone()).await?.collect();

        if self.cache_enabled() {
            let mut returned = HashSet::new();
            let mut cache_updates = Vec::with_capacity(missing.len());
            for pair in &fetched {
                returned.insert(pair.0.clone());
                cache_updates.push((
                    pair.0.clone(),
                    SnapshotCacheEntry {
                        value: Some(pair.1.clone()),
                        commit_ts: 0,
                    },
                ));
            }
            for key in missing {
                if !returned.contains(&key) {
                    cache_updates.push((
                        key,
                        SnapshotCacheEntry {
                            value: None,
                            commit_ts: 0,
                        },
                    ));
                }
            }
            self.cache.update(cache_updates);
        }

        out.extend(fetched);
        Ok(out.into_iter())
    }

    /// Get the values associated with the given keys and their commit timestamps.
    ///
    /// Returns [`crate::Error::CommitTsRequiredButNotReturned`] if TiKV does not return a commit
    /// timestamp for an existing key.
    pub async fn batch_get_with_commit_ts(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = (KvPair, u64)>> {
        debug!("invoking batch_get_with_commit_ts request on snapshot");
        let keys: Vec<Key> = keys.into_iter().map(Into::into).collect();
        if keys.is_empty() {
            return Ok(Vec::<(KvPair, u64)>::new().into_iter());
        }

        let mut out = Vec::new();
        let mut missing = Vec::new();
        if self.cache_enabled() {
            for key in &keys {
                if let Some(entry) = self.cache.get(key, true) {
                    if let Some(value) = entry.value {
                        out.push((KvPair(key.clone(), value), entry.commit_ts));
                    }
                } else {
                    missing.push(key.clone());
                }
            }
        } else {
            missing = keys.clone();
        }

        if missing.is_empty() {
            return Ok(out.into_iter());
        }

        let fetched: Vec<(KvPair, u64)> = self
            .transaction
            .batch_get_with_commit_ts(missing.clone())
            .await?
            .collect();

        if self.cache_enabled() {
            let mut returned = HashSet::new();
            let mut cache_updates = Vec::with_capacity(missing.len());
            for (pair, commit_ts) in &fetched {
                returned.insert(pair.0.clone());
                cache_updates.push((
                    pair.0.clone(),
                    SnapshotCacheEntry {
                        value: Some(pair.1.clone()),
                        commit_ts: *commit_ts,
                    },
                ));
            }
            for key in missing {
                if !returned.contains(&key) {
                    cache_updates.push((
                        key,
                        SnapshotCacheEntry {
                            value: None,
                            commit_ts: 0,
                        },
                    ));
                }
            }
            self.cache.update(cache_updates);
        }

        out.extend(fetched);
        Ok(out.into_iter())
    }

    /// Scan a range, return at most `limit` key-value pairs that lying in the range.
    pub async fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan request on snapshot");
        self.transaction.scan(range, limit).await
    }

    /// Scan a range, return at most `limit` keys that lying in the range.
    pub async fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys request on snapshot");
        self.transaction.scan_keys(range, limit).await
    }

    /// Similar to scan, but in the reverse direction.
    pub async fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan_reverse request on snapshot");
        self.transaction.scan_reverse(range, limit).await
    }

    /// Similar to scan_keys, but in the reverse direction.
    pub async fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys_reverse request on snapshot");
        self.transaction.scan_keys_reverse(range, limit).await
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::request::Keyspace;
    use crate::timestamp::TimestampExt;
    use crate::Timestamp;
    use crate::{CheckLevel, Error, TransactionOptions};

    use super::*;

    #[tokio::test]
    async fn test_snapshot_get_uses_snapshot_cache() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::Unimplemented);
                };

                get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert!(!req.need_commit_ts);

                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = false;
                resp.value = b"v".to_vec();
                resp.commit_ts = 0;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        assert_eq!(
            snapshot.get(b"k".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(
            snapshot.get(b"k".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.snap_cache_hit_count(), 1);
        assert_eq!(snapshot.snap_cache_size(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_get_with_commit_ts_populates_cache() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::Unimplemented);
                };

                get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = false;
                resp.value = b"v".to_vec();
                resp.commit_ts = if req.need_commit_ts { 123 } else { 0 };
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        snapshot.get(b"k".to_vec()).await.unwrap();
        assert_eq!(
            snapshot.get_with_commit_ts(b"k".to_vec()).await.unwrap(),
            Some((b"v".to_vec(), 123))
        );
        assert_eq!(
            snapshot.get_with_commit_ts(b"k".to_vec()).await.unwrap(),
            Some((b"v".to_vec(), 123))
        );

        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
        assert_eq!(snapshot.snap_cache_hit_count(), 1);

        let entry = snapshot
            .snap_cache()
            .get(&Key::from(b"k".to_vec()))
            .cloned()
            .expect("expected cache entry");
        assert_eq!(entry.commit_ts, 123);
    }

    #[tokio::test]
    async fn test_snapshot_batch_get_uses_snapshot_cache_for_hits_and_misses() {
        let batch_get_calls = Arc::new(AtomicUsize::new(0));
        let batch_get_calls_cloned = batch_get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() else {
                    return Err(Error::Unimplemented);
                };

                batch_get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert!(!req.need_commit_ts);

                let mut resp = kvrpcpb::BatchGetResponse::default();
                for key in &req.keys {
                    if key.as_slice() == b"k2" {
                        continue;
                    }
                    resp.pairs.push(kvrpcpb::KvPair {
                        key: key.clone(),
                        value: b"v".to_vec(),
                        error: None,
                        commit_ts: 0,
                    });
                }
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        let keys = vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()];
        let first: Vec<KvPair> = snapshot.batch_get(keys.clone()).await.unwrap().collect();
        assert_eq!(first.len(), 2);
        assert_eq!(batch_get_calls.load(Ordering::SeqCst), 1);

        let second: Vec<KvPair> = snapshot.batch_get(keys).await.unwrap().collect();
        assert_eq!(second.len(), 2);
        assert_eq!(batch_get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.snap_cache_hit_count(), 3);
        assert_eq!(snapshot.snap_cache_size(), 3);
    }

    #[tokio::test]
    async fn test_snapshot_set_snapshot_ts_clears_snapshot_cache() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::Unimplemented);
                };

                get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert!(!req.need_commit_ts);

                let mut resp = kvrpcpb::GetResponse::default();
                resp.not_found = false;
                resp.value = b"v".to_vec();
                resp.commit_ts = 0;
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        assert_eq!(
            snapshot.get(b"k".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(
            snapshot.get(b"k".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.snap_cache_hit_count(), 1);
        assert_eq!(snapshot.snap_cache_size(), 1);

        snapshot
            .set_snapshot_ts(Timestamp::from_version(11))
            .expect("set_snapshot_ts");
        assert_eq!(snapshot.snap_cache_hit_count(), 1);
        assert_eq!(snapshot.snap_cache_size(), 0);

        assert_eq!(
            snapshot.get(b"k".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(get_calls.load(Ordering::SeqCst), 2);
        assert_eq!(snapshot.snap_cache_hit_count(), 1);
        assert_eq!(snapshot.snap_cache_size(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_set_kv_read_timeout_sets_max_execution_duration_by_default() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::Unimplemented);
                };
                let ctx = req.context.as_ref().expect("context should be populated");
                assert_eq!(ctx.max_execution_duration_ms, 321);

                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        snapshot.set_kv_read_timeout(Duration::from_millis(321));
        assert_eq!(snapshot.kv_read_timeout(), Duration::from_millis(321));

        let _ = snapshot.get(b"k".to_vec()).await.unwrap();
    }

    #[tokio::test]
    async fn test_snapshot_set_kv_read_timeout_does_not_override_explicit_max_execution_duration() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() else {
                    return Err(Error::Unimplemented);
                };
                let ctx = req.context.as_ref().expect("context should be populated");
                assert_eq!(ctx.max_execution_duration_ms, 999);

                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        snapshot.set_max_execution_duration(Duration::from_millis(999));
        snapshot.set_kv_read_timeout(Duration::from_millis(321));

        let _ = snapshot.get(b"k".to_vec()).await.unwrap();
    }

    #[test]
    fn test_snapshot_is_internal_matches_request_source_prefix() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |_req: &dyn Any| Err(Error::Unimplemented),
        )));

        let mut snapshot = Snapshot::new(Transaction::new(
            Timestamp::from_version(10),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        ));

        assert!(!snapshot.is_internal());

        snapshot.set_request_source("internal_gc");
        assert!(snapshot.is_internal());

        snapshot.set_request_source("external_gc");
        assert!(!snapshot.is_internal());
    }
}
