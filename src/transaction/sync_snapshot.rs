use crate::transaction::sync_client::safe_block_on;
use crate::{
    BoundRange, CommandPriority, IsolationLevel, Key, KvPair, ReplicaReadType, Result, Snapshot,
    StoreLabel, Value,
};
use std::sync::Arc;
use std::time::Duration;

/// A synchronous read-only snapshot.
///
/// This is a wrapper around the async [`Snapshot`] that provides blocking methods.
/// All operations block the current thread until completed.
pub struct SyncSnapshot {
    inner: Snapshot,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncSnapshot {
    pub(crate) fn new(inner: Snapshot, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, runtime }
    }

    /// Get the value associated with the given key.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get(key))
    }

    /// Get the value associated with the given key and its commit timestamp.
    pub fn get_with_commit_ts(&mut self, key: impl Into<Key>) -> Result<Option<(Value, u64)>> {
        safe_block_on(&self.runtime, self.inner.get_with_commit_ts(key))
    }

    /// Check whether the key exists.
    pub fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        safe_block_on(&self.runtime, self.inner.key_exists(key))
    }

    /// Get the values associated with the given keys.
    pub fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get(keys))
    }

    /// Get the values associated with the given keys and their commit timestamps.
    pub fn batch_get_with_commit_ts(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = (KvPair, u64)>> {
        safe_block_on(&self.runtime, self.inner.batch_get_with_commit_ts(keys))
    }

    /// Scan a range, return at most `limit` key-value pairs that lie in the range.
    pub fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan(range, limit))
    }

    /// Scan a range, return at most `limit` keys that lie in the range.
    pub fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys(range, limit))
    }

    /// Similar to scan, but in the reverse direction.
    pub fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan_reverse(range, limit))
    }

    /// Similar to scan_keys, but in the reverse direction.
    pub fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys_reverse(range, limit))
    }

    /// Set replica read behavior.
    pub fn set_replica_read(&mut self, read_type: ReplicaReadType) {
        self.inner.set_replica_read(read_type);
    }

    /// Set a replica read adjuster for point/batch gets.
    pub fn set_replica_read_adjuster<F>(&mut self, adjuster: F)
    where
        F: Fn(usize) -> ReplicaReadType + Send + Sync + 'static,
    {
        self.inner.set_replica_read_adjuster(adjuster);
    }

    /// Set the busy threshold for read requests.
    pub fn set_load_based_replica_read_threshold(&mut self, threshold: Duration) {
        self.inner.set_load_based_replica_read_threshold(threshold);
    }

    /// Set labels to filter target stores for replica reads.
    ///
    /// This maps to client-go `KVSnapshot.SetMatchStoreLabels`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_labels(&mut self, labels: impl IntoIterator<Item = StoreLabel>) {
        self.inner.set_match_store_labels(labels);
    }

    /// Set store ids to filter target stores for replica reads.
    ///
    /// This maps to client-go `tikv.WithMatchStores` / `locate.WithMatchStores`.
    ///
    /// This option is only effective for read-only snapshots.
    pub fn set_match_store_ids(&mut self, store_ids: impl IntoIterator<Item = u64>) {
        self.inner.set_match_store_ids(store_ids);
    }

    /// Enable or disable stale reads for this snapshot.
    ///
    /// When enabled, read requests will set `kvrpcpb::Context.stale_read = true`.
    ///
    /// This maps to client-go `KVSnapshot.SetIsStalenessReadOnly`.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.inner.set_stale_read(stale_read);
    }

    /// Set whether read requests should fill TiKV block cache.
    ///
    /// This maps to client-go `KVSnapshot.SetNotFillCache`.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.inner.set_not_fill_cache(not_fill_cache);
    }

    /// Set task ID hint for TiKV.
    ///
    /// This maps to client-go `KVSnapshot.SetTaskID`.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.inner.set_task_id(task_id);
    }

    /// Set server-side maximum execution duration for read requests.
    ///
    /// This option writes to `kvrpcpb::Context.max_execution_duration_ms`.
    pub fn set_max_execution_duration(&mut self, duration: Duration) {
        self.inner.set_max_execution_duration(duration);
    }

    /// Set the priority for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetPriority`.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.inner.set_priority(priority);
    }

    /// Set the isolation level for read requests.
    ///
    /// This maps to client-go `KVSnapshot.SetIsolationLevel`.
    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.inner.set_isolation_level(isolation_level);
    }

    /// Set resource group tag for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTag`.
    pub fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.inner.set_resource_group_tag(tag);
    }

    /// Set a resource group tagger used to fill `kvrpcpb::Context.resource_group_tag`.
    ///
    /// The tagger is invoked only when no explicit resource group tag is configured via
    /// [`SyncSnapshot::set_resource_group_tag`] / [`crate::TransactionOptions::resource_group_tag`],
    /// matching client-go behavior.
    ///
    /// The tagger input is the request label (for example, `"kv_get"` or `"kv_commit"`).
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupTagger`.
    pub fn set_resource_group_tagger<F>(&mut self, tagger: F)
    where
        F: Fn(&str) -> Vec<u8> + Send + Sync + 'static,
    {
        self.inner.set_resource_group_tagger(tagger);
    }

    /// Clear the configured resource group tagger.
    pub fn clear_resource_group_tagger(&mut self) {
        self.inner.clear_resource_group_tagger();
    }

    /// Set resource group name for requests.
    ///
    /// This maps to client-go `KVSnapshot.SetResourceGroupName`.
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.inner.set_resource_group_name(name);
    }

    /// Set request source for requests.
    ///
    /// This option writes to `kvrpcpb::Context.request_source`.
    ///
    /// For client-go compatible formatting (internal/external prefixes and optional explicit type),
    /// use [`RequestSource`](crate::RequestSource).
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.inner.set_request_source(source);
    }
}
