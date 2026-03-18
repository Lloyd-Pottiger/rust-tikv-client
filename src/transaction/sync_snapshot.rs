use crate::transaction::sync_client::safe_block_on;
use crate::{
    BoundRange, CommandPriority, IsolationLevel, Key, KvPair, ReplicaReadType, ResolveLockDetail,
    Result, Snapshot, SnapshotCacheEntry, SnapshotRuntimeStats, StoreLabel, Timestamp, Value,
    Variables,
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

enum SyncSnapshotScanDirection {
    Forward { next_start: Key, upper_bound: Key },
    Reverse { lower_bound: Key, next_end: Key },
}

struct SyncSnapshotScanIterator<'a> {
    snapshot: &'a mut SyncSnapshot,
    direction: SyncSnapshotScanDirection,
    pending: std::vec::IntoIter<KvPair>,
    finished: bool,
    batch_size: u32,
}

impl<'a> SyncSnapshotScanIterator<'a> {
    fn forward(snapshot: &'a mut SyncSnapshot, start_key: Key, upper_bound: Key) -> Self {
        Self {
            batch_size: snapshot.scan_batch_size(),
            snapshot,
            direction: SyncSnapshotScanDirection::Forward {
                next_start: start_key,
                upper_bound,
            },
            pending: Vec::<KvPair>::new().into_iter(),
            finished: false,
        }
    }

    fn reverse(snapshot: &'a mut SyncSnapshot, start_key: Key, lower_bound: Key) -> Self {
        Self {
            batch_size: snapshot.scan_batch_size(),
            snapshot,
            direction: SyncSnapshotScanDirection::Reverse {
                lower_bound,
                next_end: start_key,
            },
            pending: Vec::<KvPair>::new().into_iter(),
            finished: false,
        }
    }
}

impl Iterator for SyncSnapshotScanIterator<'_> {
    type Item = Result<KvPair>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(pair) = self.pending.next() {
                return Some(Ok(pair));
            }
            if self.finished {
                return None;
            }

            let (batch, last_key) = match &mut self.direction {
                SyncSnapshotScanDirection::Forward {
                    next_start,
                    upper_bound,
                } => match self
                    .snapshot
                    .scan(next_start.clone()..upper_bound.clone(), self.batch_size)
                {
                    Ok(iter) => {
                        let batch: Vec<KvPair> = iter.collect();
                        let last_key = batch.last().map(|pair| pair.key().clone());
                        (batch, last_key)
                    }
                    Err(err) => {
                        self.finished = true;
                        return Some(Err(err));
                    }
                },
                SyncSnapshotScanDirection::Reverse {
                    lower_bound,
                    next_end,
                } => match self
                    .snapshot
                    .scan_reverse(lower_bound.clone()..next_end.clone(), self.batch_size)
                {
                    Ok(iter) => {
                        let batch: Vec<KvPair> = iter.collect();
                        let last_key = batch.last().map(|pair| pair.key().clone());
                        (batch, last_key)
                    }
                    Err(err) => {
                        self.finished = true;
                        return Some(Err(err));
                    }
                },
            };

            let Some(last_key) = last_key else {
                self.finished = true;
                return None;
            };

            match &mut self.direction {
                SyncSnapshotScanDirection::Forward { next_start, .. } => {
                    *next_start = last_key.next_key();
                }
                SyncSnapshotScanDirection::Reverse { next_end, .. } => {
                    *next_end = last_key;
                }
            }

            self.pending = batch.into_iter();
        }
    }
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

    /// Create a streaming iterator over key-value pairs starting at `start_key`.
    ///
    /// The returned iterator yields all pairs whose keys are in the range `[start_key, upper_bound)`.
    /// When `upper_bound` is empty, it means upper unbounded.
    ///
    /// This maps to client-go `KVSnapshot.Iter`.
    pub fn iter(
        &mut self,
        start_key: impl Into<Key>,
        upper_bound: impl Into<Key>,
    ) -> impl Iterator<Item = Result<KvPair>> + '_ {
        SyncSnapshotScanIterator::forward(self, start_key.into(), upper_bound.into())
    }

    /// Create a reversed streaming iterator positioned on the first entry with key < `start_key`.
    ///
    /// The returned iterator yields all pairs whose keys are in the range `[lower_bound, start_key)`,
    /// in descending order. When `lower_bound` is empty, it means lower unbounded.
    ///
    /// This maps to client-go `KVSnapshot.IterReverse`.
    pub fn iter_reverse(
        &mut self,
        start_key: impl Into<Key>,
        lower_bound: impl Into<Key>,
    ) -> impl Iterator<Item = Result<KvPair>> + '_ {
        SyncSnapshotScanIterator::reverse(self, start_key.into(), lower_bound.into())
    }

    /// Set the KV variables used by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetVars`.
    pub fn set_vars(&mut self, vars: Variables) {
        self.inner.set_vars(vars);
    }

    /// Get the KV variables used by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.GetVars`.
    #[must_use]
    pub fn vars(&self) -> &Variables {
        self.inner.vars()
    }

    /// Get lock-resolution runtime stats accumulated by this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.GetResolveLockDetail`.
    #[must_use]
    pub fn resolve_lock_detail(&self) -> ResolveLockDetail {
        self.inner.resolve_lock_detail()
    }

    /// Attach or clear snapshot runtime stats collection.
    ///
    /// This maps to client-go `KVSnapshot.SetRuntimeStats`.
    pub fn set_runtime_stats(&mut self, stats: Option<Arc<SnapshotRuntimeStats>>) {
        self.inner.set_runtime_stats(stats);
    }

    /// Get the currently attached snapshot runtime stats container.
    #[must_use]
    pub fn runtime_stats(&self) -> Option<Arc<SnapshotRuntimeStats>> {
        self.inner.runtime_stats()
    }

    /// Set the snapshot timestamp.
    ///
    /// This maps to client-go `KVSnapshot.SetSnapshotTS`.
    pub fn set_snapshot_ts(&mut self, timestamp: Timestamp) -> Result<()> {
        self.inner.set_snapshot_ts(timestamp)
    }

    /// Mark this snapshot as reading through a pipelined transaction's start timestamp.
    ///
    /// This maps to client-go `KVSnapshot.SetPipelined`.
    pub fn set_pipelined(&mut self, start_ts: u64) {
        self.inner.set_pipelined(start_ts);
    }

    /// Set the geographical scope of this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetTxnScope` / `KVSnapshot.SetReadReplicaScope`.
    pub fn set_txn_scope(&mut self, txn_scope: impl AsRef<str>) {
        self.inner.set_txn_scope(txn_scope);
    }

    /// Get the geographical scope of this snapshot.
    ///
    /// Returns `"global"` if global scope is used.
    ///
    /// This maps to client-go `KVSnapshot.SetTxnScope` / `KVSnapshot.SetReadReplicaScope`.
    #[must_use]
    pub fn txn_scope(&self) -> &str {
        self.inner.txn_scope()
    }

    /// Set the read replica scope of this snapshot.
    ///
    /// In client-go, `KVSnapshot.SetReadReplicaScope` is an alias of `KVSnapshot.SetTxnScope`.
    /// This method is provided for parity and forwards to [`SyncSnapshot::set_txn_scope`].
    pub fn set_read_replica_scope(&mut self, scope: impl AsRef<str>) {
        self.inner.set_read_replica_scope(scope);
    }

    /// Returns true if this snapshot is used by internal executions.
    ///
    /// This maps to client-go `KVSnapshot.IsInternal`.
    #[must_use]
    pub fn is_internal(&self) -> bool {
        self.inner.is_internal()
    }

    /// Get the snapshot cache hit count.
    ///
    /// This maps to client-go `KVSnapshot.SnapCacheHitCount`.
    #[must_use]
    pub fn snap_cache_hit_count(&self) -> u64 {
        self.inner.snap_cache_hit_count()
    }

    /// Get the number of entries currently stored in the snapshot cache.
    ///
    /// This maps to client-go `KVSnapshot.SnapCacheSize`.
    #[must_use]
    pub fn snap_cache_size(&self) -> usize {
        self.inner.snap_cache_size()
    }

    /// Get a copy of the snapshot cache.
    ///
    /// This maps to client-go `KVSnapshot.SnapCache`.
    #[must_use]
    pub fn snap_cache(&self) -> std::collections::HashMap<Key, SnapshotCacheEntry> {
        self.inner.snap_cache()
    }

    /// Update snapshot cache entries for further fast reads with the same keys.
    ///
    /// This maps to client-go `KVSnapshot.UpdateSnapshotCache`.
    pub fn update_snapshot_cache(
        &mut self,
        entries: impl IntoIterator<Item = (impl Into<Key>, SnapshotCacheEntry)>,
    ) {
        self.inner.update_snapshot_cache(entries);
    }

    /// Remove the snapshot cache entries for the given keys.
    ///
    /// This maps to client-go `KVSnapshot.CleanCache`.
    pub fn clean_cache(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) {
        self.inner.clean_cache(keys);
    }

    /// Set an RPC interceptor for this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetRPCInterceptor`.
    pub fn set_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.inner.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor.
    ///
    /// This maps to client-go `KVSnapshot.AddRPCInterceptor`.
    pub fn add_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.inner.add_rpc_interceptor(interceptor);
    }

    /// Clear all configured RPC interceptors.
    pub fn clear_rpc_interceptors(&mut self) {
        self.inner.clear_rpc_interceptors();
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

    /// Set whether scan requests should return only keys (no values).
    ///
    /// When enabled, `scan`/`scan_reverse` and the streaming iterators (`iter`/`iter_reverse`)
    /// set `kvrpcpb::ScanRequest.key_only = true`.
    ///
    /// This maps to client-go `KVSnapshot.SetKeyOnly`.
    pub fn set_key_only(&mut self, key_only: bool) {
        self.inner.set_key_only(key_only);
    }

    /// Get whether key-only scan mode is enabled for this snapshot.
    #[must_use]
    pub fn key_only(&self) -> bool {
        self.inner.key_only()
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

    /// Set timeout for individual KV read operations under this snapshot.
    ///
    /// This maps to client-go `KVSnapshot.SetKVReadTimeout`.
    pub fn set_kv_read_timeout(&mut self, read_timeout: Duration) {
        self.inner.set_kv_read_timeout(read_timeout);
    }

    /// Get the configured per-snapshot KV read timeout.
    ///
    /// Returns [`Duration::ZERO`] if unset.
    ///
    /// This maps to client-go `KVSnapshot.GetKVReadTimeout`.
    #[must_use]
    pub fn kv_read_timeout(&self) -> Duration {
        self.inner.kv_read_timeout()
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

    /// Set scan sampling step for TiKV scan requests.
    ///
    /// If `step > 0`, TiKV skips `step - 1` keys after each returned key.
    ///
    /// This maps to client-go `KVSnapshot.SetSampleStep`.
    pub fn set_sample_step(&mut self, step: u32) {
        self.inner.set_sample_step(step);
    }

    /// Set the scan batch size used by [`SyncSnapshot::iter`] and [`SyncSnapshot::iter_reverse`].
    ///
    /// When set to `0` or `1`, the default batch size (`256`) is used.
    ///
    /// This maps to client-go `KVSnapshot.SetScanBatchSize`.
    pub fn set_scan_batch_size(&mut self, batch_size: u32) {
        self.inner.set_scan_batch_size(batch_size);
    }

    /// Get the scan batch size used by [`SyncSnapshot::iter`] and [`SyncSnapshot::iter_reverse`].
    ///
    /// This maps to client-go `KVSnapshot.SetScanBatchSize`.
    #[must_use]
    pub fn scan_batch_size(&self) -> u32 {
        self.inner.scan_batch_size()
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

    /// Set trace id for requests.
    ///
    /// This option writes to `kvrpcpb::Context.trace_id`.
    pub fn set_trace_id(&mut self, trace_id: Vec<u8>) {
        self.inner.set_trace_id(trace_id);
    }

    /// Clear the configured trace id.
    pub fn clear_trace_id(&mut self) {
        self.inner.clear_trace_id();
    }

    /// Set trace control flags for requests.
    ///
    /// This option writes to `kvrpcpb::Context.trace_control_flags`.
    pub fn set_trace_control_flags(&mut self, flags: crate::TraceControlFlags) {
        self.inner.set_trace_control_flags(flags);
    }
}
