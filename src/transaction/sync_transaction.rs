use crate::transaction::sync_client::safe_block_on;
use crate::{
    transaction::Mutation, BoundRange, DiskFullOpt, Key, KvPair, Result, Timestamp, Transaction,
    Value,
};
use std::sync::Arc;
use std::time::Duration;

/// A synchronous transaction.
///
/// This is a wrapper around the async [`Transaction`] that provides blocking methods.
/// All operations block the current thread until completed.
pub struct SyncTransaction {
    inner: Transaction,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncTransaction {
    pub(crate) fn new(inner: Transaction, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, runtime }
    }

    /// Set the geographical scope of the transaction.
    pub fn set_txn_scope(&mut self, txn_scope: impl AsRef<str>) {
        self.inner.set_txn_scope(txn_scope);
    }

    /// Returns the geographical scope of the transaction.
    #[must_use]
    pub fn txn_scope(&self) -> &str {
        self.inner.txn_scope()
    }

    /// Enable or disable async commit.
    ///
    /// This maps to client-go `KVTxn.SetEnableAsyncCommit`.
    pub fn set_enable_async_commit(&mut self, enabled: bool) {
        self.inner.set_enable_async_commit(enabled);
    }

    /// Enable or disable 1PC.
    ///
    /// This maps to client-go `KVTxn.SetEnable1PC`.
    pub fn set_enable_one_pc(&mut self, enabled: bool) {
        self.inner.set_enable_one_pc(enabled);
    }

    /// Set whether the transaction uses causal consistency instead of linearizability.
    ///
    /// When enabled, async-commit/1PC does not fetch a fresh PD TSO to seed `min_commit_ts`.
    ///
    /// This maps to client-go `KVTxn.SetCausalConsistency`.
    pub fn set_causal_consistency(&mut self, enabled: bool) {
        self.inner.set_causal_consistency(enabled);
    }

    /// Set the minimum commit timestamp constraint for the transaction.
    pub fn set_commit_wait_until_tso(&mut self, commit_wait_until_tso: u64) {
        self.inner.set_commit_wait_until_tso(commit_wait_until_tso);
    }

    /// Returns the commit-wait constraint configured by [`SyncTransaction::set_commit_wait_until_tso`].
    #[must_use]
    pub fn commit_wait_until_tso(&self) -> u64 {
        self.inner.commit_wait_until_tso()
    }

    /// Set the maximum time allowed for PD TSO to catch up to the commit-wait target timestamp.
    pub fn set_commit_wait_until_tso_timeout(&mut self, timeout: Duration) {
        self.inner.set_commit_wait_until_tso_timeout(timeout);
    }

    /// Returns the commit-wait timeout configured by
    /// [`SyncTransaction::set_commit_wait_until_tso_timeout`].
    #[must_use]
    pub fn commit_wait_until_tso_timeout(&self) -> Duration {
        self.inner.commit_wait_until_tso_timeout()
    }

    /// Set a commit-ts upper bound checker for this transaction.
    pub fn set_commit_ts_upper_bound_check<F>(&mut self, checker: F)
    where
        F: Fn(u64) -> bool + Send + Sync + 'static,
    {
        self.inner.set_commit_ts_upper_bound_check(checker);
    }

    /// Clear the configured commit-ts upper bound checker.
    pub fn clear_commit_ts_upper_bound_check(&mut self) {
        self.inner.clear_commit_ts_upper_bound_check();
    }

    /// Set whether current operation is allowed in each TiKV disk usage level.
    pub fn set_disk_full_opt(&mut self, opt: DiskFullOpt) {
        self.inner.set_disk_full_opt(opt);
    }

    /// Set the source of the transaction.
    pub fn set_txn_source(&mut self, txn_source: u64) {
        self.inner.set_txn_source(txn_source);
    }

    /// Enable forcing TiKV to always sync logs for transactional write requests.
    pub fn enable_force_sync_log(&mut self) {
        self.inner.enable_force_sync_log();
    }

    /// Set whether TiKV should sync logs for transactional write requests.
    pub fn set_sync_log(&mut self, enabled: bool) {
        self.inner.set_sync_log(enabled);
    }

    /// Set the server-side maximum execution duration for transactional write requests.
    pub fn set_max_write_execution_duration(&mut self, duration: Duration) {
        self.inner.set_max_write_execution_duration(duration);
    }

    /// Get the value associated with the given key.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get(key))
    }

    /// Get the value associated with the given key, and lock the key.
    pub fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get_for_update(key))
    }

    /// Check if the given key exists.
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

    /// Get the values associated with the given keys, and lock the keys.
    pub fn batch_get_for_update(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get_for_update(keys))
    }

    /// Scan a range and return the key-value pairs.
    pub fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan(range, limit))
    }

    /// Scan a range and return only the keys.
    pub fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys(range, limit))
    }

    /// Scan a range in reverse order.
    pub fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan_reverse(range, limit))
    }

    /// Scan keys in a range in reverse order.
    pub fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys_reverse(range, limit))
    }

    /// Set the value associated with the given key.
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.put(key, value))
    }

    /// Insert the key-value pair. Returns an error if the key already exists.
    pub fn insert(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.insert(key, value))
    }

    /// Delete the given key.
    pub fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.delete(key))
    }

    /// Apply multiple mutations atomically.
    pub fn batch_mutate(&mut self, mutations: impl IntoIterator<Item = Mutation>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.batch_mutate(mutations))
    }

    /// Lock the given keys without associating any values.
    pub fn lock_keys(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.lock_keys(keys))
    }

    /// Commit the transaction.
    pub fn commit(&mut self) -> Result<Option<Timestamp>> {
        safe_block_on(&self.runtime, self.inner.commit())
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.rollback())
    }

    /// Send a heart beat message to keep the transaction alive.
    pub fn send_heart_beat(&mut self) -> Result<u64> {
        safe_block_on(&self.runtime, self.inner.send_heart_beat())
    }
}
