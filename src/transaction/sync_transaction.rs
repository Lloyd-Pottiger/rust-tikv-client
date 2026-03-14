use crate::transaction::sync_client::safe_block_on;
use crate::{
    transaction::Mutation, AssertionLevel, BoundRange, CommandPriority, DiskFullOpt, Error, Key,
    KvPair, LockWaitTimeout, PrewriteEncounterLockPolicy, ResolveLockDetail, Result,
    SchemaLeaseChecker, Timestamp, Transaction, Value, Variables,
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

    /// Set the KV variables used by this transaction.
    ///
    /// This maps to client-go `KVTxn.SetVars`.
    pub fn set_vars(&mut self, vars: Variables) {
        self.inner.set_vars(vars);
    }

    /// Get the KV variables used by this transaction.
    ///
    /// This maps to client-go `KVTxn.GetVars`.
    #[must_use]
    pub fn vars(&self) -> &Variables {
        self.inner.vars()
    }

    /// Get lock-resolution runtime stats accumulated by this transaction.
    ///
    /// This mirrors client-go `KVTxn.GetResolveLockDetail`.
    #[must_use]
    pub fn resolve_lock_detail(&self) -> ResolveLockDetail {
        self.inner.resolve_lock_detail()
    }

    /// Set an RPC interceptor for this transaction.
    ///
    /// This maps to client-go `KVTxn.SetRPCInterceptor`.
    pub fn set_rpc_interceptor<I>(&mut self, interceptor: I)
    where
        I: crate::RpcInterceptor,
    {
        self.inner.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor.
    ///
    /// This maps to client-go `KVTxn.AddRPCInterceptor`.
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

    /// Set a callback invoked when [`SyncTransaction::commit`] finishes.
    ///
    /// The callback receives:
    /// - `info`: a JSON string containing the commit info (client-go `TxnInfo` compatible); and
    /// - `err`: the commit error (if any).
    ///
    /// This maps to client-go `KVTxn.SetCommitCallback`.
    pub fn set_commit_callback<F>(&mut self, callback: F)
    where
        F: Fn(&str, Option<&Error>) + Send + Sync + 'static,
    {
        self.inner.set_commit_callback(callback);
    }

    /// Clear the commit callback.
    pub fn clear_commit_callback(&mut self) {
        self.inner.clear_commit_callback();
    }

    /// Set a hook that is triggered when the local mutation buffer memory footprint changes.
    ///
    /// This maps to client-go `KVTxn.SetMemoryFootprintChangeHook`.
    pub fn set_memory_footprint_change_hook<F>(&mut self, hook: F)
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        self.inner.set_memory_footprint_change_hook(hook);
    }

    /// Clear the memory footprint change hook.
    pub fn clear_memory_footprint_change_hook(&mut self) {
        self.inner.clear_memory_footprint_change_hook();
    }

    /// Returns whether the memory footprint change hook is set.
    ///
    /// This maps to client-go `KVTxn.MemHookSet`.
    #[must_use]
    pub fn mem_hook_set(&self) -> bool {
        self.inner.mem_hook_set()
    }

    /// Returns the current memory footprint of the local mutation buffer.
    ///
    /// This maps to client-go `KVTxn.Mem`.
    #[must_use]
    pub fn mem(&self) -> u64 {
        self.inner.mem()
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

    /// Set how strict to enforce mutation assertions during prewrite/flush.
    ///
    /// This maps to client-go `KVTxn.SetAssertionLevel`.
    pub fn set_assertion_level(&mut self, assertion_level: AssertionLevel) {
        self.inner.set_assertion_level(assertion_level);
    }

    /// Set the policy for handling locks encountered during prewrite.
    ///
    /// When set to [`PrewriteEncounterLockPolicy::NoResolve`], prewrite returns lock errors directly
    /// without attempting lock resolution.
    ///
    /// This maps to client-go `KVTxn.SetPrewriteEncounterLockPolicy`.
    pub fn set_prewrite_encounter_lock_policy(&mut self, policy: PrewriteEncounterLockPolicy) {
        self.inner.set_prewrite_encounter_lock_policy(policy);
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

    /// Get a timestamp version that can be used as the commit timestamp for this transaction.
    ///
    /// This maps to client-go `KVTxn.GetTimestampForCommit`.
    pub fn get_timestamp_for_commit(&mut self) -> Result<u64> {
        safe_block_on(&self.runtime, self.inner.get_timestamp_for_commit())
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

    /// Set the schema version used for schema validity checks during commit.
    pub fn set_schema_ver(&mut self, schema_ver: i64) {
        self.inner.set_schema_ver(schema_ver);
    }

    /// Clear the configured schema version.
    pub fn clear_schema_ver(&mut self) {
        self.inner.clear_schema_ver();
    }

    /// Set a schema lease checker used to validate schema changes during commit.
    pub fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        self.inner.set_schema_lease_checker(checker);
    }

    /// Clear the configured schema lease checker.
    pub fn clear_schema_lease_checker(&mut self) {
        self.inner.clear_schema_lease_checker();
    }

    /// Set the priority for requests.
    pub fn set_priority(&mut self, priority: CommandPriority) {
        self.inner.set_priority(priority);
    }

    /// Set resource group tag for requests.
    pub fn set_resource_group_tag(&mut self, tag: Vec<u8>) {
        self.inner.set_resource_group_tag(tag);
    }

    /// Set a resource group tagger used to fill `kvrpcpb::Context.resource_group_tag`.
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
    pub fn set_resource_group_name(&mut self, name: impl Into<String>) {
        self.inner.set_resource_group_name(name);
    }

    /// Set request source for requests.
    pub fn set_request_source(&mut self, source: impl Into<String>) {
        self.inner.set_request_source(source);
    }

    /// Get the value associated with the given key.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get(key))
    }

    /// Get the value associated with the given key, and lock the key.
    pub fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get_for_update(key))
    }

    /// Get the value associated with the given key, and lock the key in share mode.
    pub fn get_for_share(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get_for_share(key))
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

    /// Get the values associated with the given keys, and lock the keys in share mode.
    pub fn batch_get_for_share(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get_for_share(keys))
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

    /// Lock the given keys without associating any values, using the provided lock wait timeout.
    ///
    /// This maps to client-go `KVTxn.LockKeysWithWaitTime`.
    pub fn lock_keys_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<()> {
        safe_block_on(
            &self.runtime,
            self.inner
                .lock_keys_with_wait_timeout(keys, lock_wait_timeout),
        )
    }

    /// Lock the given keys in share mode without associating any values.
    pub fn lock_keys_in_share_mode(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.lock_keys_in_share_mode(keys))
    }

    /// Lock the given keys in share mode without associating any values, using the provided lock
    /// wait timeout.
    pub fn lock_keys_in_share_mode_with_wait_timeout(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        lock_wait_timeout: LockWaitTimeout,
    ) -> Result<()> {
        safe_block_on(
            &self.runtime,
            self.inner
                .lock_keys_in_share_mode_with_wait_timeout(keys, lock_wait_timeout),
        )
    }

    /// Get the start timestamp version of this transaction.
    ///
    /// This maps to client-go `KVTxn.StartTS`.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.inner.start_ts()
    }

    /// Get the commit timestamp of this transaction (if committed).
    #[must_use]
    pub fn commit_timestamp(&self) -> Option<Timestamp> {
        self.inner.commit_timestamp()
    }

    /// Get the commit timestamp version of this transaction.
    ///
    /// Returns 0 when the transaction is not committed.
    ///
    /// This maps to client-go `KVTxn.CommitTS`.
    #[must_use]
    pub fn commit_ts(&self) -> u64 {
        self.inner.commit_ts()
    }

    /// Returns whether this transaction has only performed read operations so far.
    ///
    /// This maps to client-go `KVTxn.IsReadOnly`.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    /// Returns whether the transaction is valid.
    ///
    /// A transaction becomes invalid after commit or rollback. This maps to client-go `KVTxn.Valid`.
    #[must_use]
    pub fn valid(&self) -> bool {
        self.inner.valid()
    }

    /// Returns the number of buffered entries in this transaction.
    ///
    /// This maps to client-go `KVTxn.Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the transaction has no buffered entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the buffered size (sum of key and value lengths) of this transaction.
    ///
    /// This maps to client-go `KVTxn.Size`.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.inner.size()
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
