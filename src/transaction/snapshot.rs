// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use derive_new::new;
use log::{debug, trace};

use crate::transaction::Variables;
use crate::BoundRange;
use crate::CommandPriority;
use crate::IsolationLevel;
use crate::Key;
use crate::KvPair;
use crate::ReplicaReadType;
use crate::Result;
use crate::StoreLabel;
use crate::Transaction;
use crate::Value;

use std::time::Duration;

/// A read-only transaction which reads at the given timestamp.
///
/// It behaves as if the snapshot was taken at the given timestamp,
/// i.e. it can read operations happened before the timestamp,
/// but ignores operations after the timestamp.
///
/// See the [Transaction](struct@crate::Transaction) docs for more information on the methods.
#[derive(new)]
pub struct Snapshot {
    transaction: Transaction,
}

impl Snapshot {
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

    /// Get the value associated with the given key.
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking get request on snapshot");
        self.transaction.get(key).await
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
        self.transaction.get_with_commit_ts(key).await
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
        self.transaction.batch_get(keys).await
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
        self.transaction.batch_get_with_commit_ts(keys).await
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
