use crate::{
    request::plan::CleanupLocksResult,
    transaction::{
        client::Client, sync_snapshot::SyncSnapshot, sync_transaction::SyncTransaction,
        ResolveLocksForReadResult, ResolveLocksOptions, ResolveLocksResult,
    },
    BoundRange, Config, Error, Result, Timestamp, TransactionOptions,
};
use std::sync::Arc;

/// Detects whether a Tokio async runtime is already running on the current thread.
///
/// When the synchronous transaction client is used from within an existing async
/// runtime, blocking operations (such as `block_on`) can cause deadlocks or other
/// unexpected blocking behavior. This helper checks for a currently active Tokio
/// runtime and returns `Error::NestedRuntimeError` if one is found, allowing callers
/// to detect and handle incorrect use of the synchronous client from within an
/// existing async runtime instead of risking deadlocks or unexpected blocking.
///
/// Note: checks only for Tokio runtimes, not other async runtimes.
///
/// # Error Handling
///
/// If this function returns `Error::NestedRuntimeError`, callers should:
/// - Use the async [`TransactionClient`](crate::TransactionClient) instead of `SyncTransactionClient`
/// - Move the `SyncTransactionClient` creation and usage outside of the async context
/// - Consider restructuring the code to avoid mixing sync and async execution contexts
pub(crate) fn check_nested_runtime() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::NestedRuntimeError(String::new()));
    }
    Ok(())
}

/// Run a `Result`-returning future on the given Tokio runtime with nested-runtime detection.
///
/// This is a thin wrapper around [`tokio::runtime::Runtime::block_on`] that first checks
/// whether a Tokio runtime is already active in the current context. If a nested runtime
/// is detected, it returns [`Error::NestedRuntimeError`] instead of attempting to block,
/// which helps prevent potential deadlocks and provides clearer error messages when
/// `block_on` is misused from within an existing async runtime.
///
/// # Returns
///
/// - `Ok(T)` with the successful result produced by the provided future when no nested
///   runtime is detected and the future completes successfully.
/// - `Err(Error::NestedRuntimeError)` if a Tokio runtime is already active on the current
///   thread when this function is called.
/// - `Err(e)` for any other [`Error`] produced either by the future itself or by
///   `runtime.block_on`.
pub(crate) fn safe_block_on<F, T>(runtime: &tokio::runtime::Runtime, future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    check_nested_runtime()?;
    runtime.block_on(future)
}

/// Synchronous TiKV transactional client.
///
/// This is a synchronous wrapper around the async [`TransactionClient`](crate::TransactionClient).
/// All methods block the current thread until completion.
///
/// For async operations, use [`TransactionClient`](crate::TransactionClient) instead.
pub struct SyncTransactionClient {
    client: Client,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncTransactionClient {
    /// Create a synchronous transactional [`SyncTransactionClient`] and connect to the TiKV cluster.
    ///
    /// See usage example in the documentation of [`TransactionClient::new`](crate::TransactionClient::new).
    pub fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default())
    }

    /// Create a synchronous transactional [`SyncTransactionClient`] with a custom configuration.
    ///
    /// See usage example in the documentation of [`TransactionClient::new_with_config`](crate::TransactionClient::new_with_config).
    pub fn new_with_config<S: Into<String>>(pd_endpoints: Vec<S>, config: Config) -> Result<Self> {
        check_nested_runtime()?;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let client = runtime.block_on(Client::new_with_config(pd_endpoints, config))?;
        Ok(Self { client, runtime })
    }

    /// Create a synchronous transactional [`SyncTransactionClient`] that uses API V2 without adding
    /// or removing any API V2 keyspace/key-mode prefix, with a custom configuration.
    ///
    /// This is intended for **server-side embedding** use cases. `config.keyspace` must be unset.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::new_with_config_api_v2_no_prefix`](crate::TransactionClient::new_with_config_api_v2_no_prefix).
    pub fn new_with_config_api_v2_no_prefix<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Self> {
        check_nested_runtime()?;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let client = runtime.block_on(Client::new_with_config_api_v2_no_prefix(
            pd_endpoints,
            config,
        ))?;
        Ok(Self { client, runtime })
    }

    /// Returns the TiKV cluster ID.
    ///
    /// This is a synchronous version of [`TransactionClient::cluster_id`](crate::TransactionClient::cluster_id).
    pub fn cluster_id(&self) -> u64 {
        self.client.cluster_id()
    }

    /// Closes this client and releases cached resources.
    ///
    /// This is a synchronous version of [`TransactionClient::close`](crate::TransactionClient::close).
    pub fn close(&self) -> Result<()> {
        safe_block_on(&self.runtime, async {
            self.client.close().await;
            Ok(())
        })
    }

    /// Returns a handle to the underlying PD RPC client.
    ///
    /// This is a synchronous version of [`TransactionClient::pd_client`](crate::TransactionClient::pd_client).
    #[must_use]
    pub fn pd_client(&self) -> Arc<crate::PdRpcClient> {
        self.client.pd_client()
    }

    /// Returns whether this client has been explicitly closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Returns a [`LockResolver`](crate::transaction::LockResolver) handle associated with this client.
    #[must_use]
    pub fn lock_resolver(&self) -> crate::transaction::LockResolver {
        self.client.lock_resolver()
    }

    /// Returns a [`BoundLockResolver`](crate::transaction::BoundLockResolver) handle associated with this client.
    ///
    /// This is a synchronous version of [`TransactionClient::bound_lock_resolver`](crate::TransactionClient::bound_lock_resolver).
    #[must_use]
    pub fn bound_lock_resolver(&self) -> crate::transaction::BoundLockResolver<crate::PdRpcClient> {
        self.client.bound_lock_resolver()
    }

    /// Creates a new optimistic [`SyncTransaction`].
    ///
    /// Use the transaction to issue requests like [`get`](SyncTransaction::get) or
    /// [`put`](SyncTransaction::put).
    ///
    /// This is a synchronous version of [`TransactionClient::begin_optimistic`](crate::TransactionClient::begin_optimistic).
    pub fn begin_optimistic(&self) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_optimistic())?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Creates a new pessimistic [`SyncTransaction`].
    ///
    /// This is a synchronous version of [`TransactionClient::begin_pessimistic`](crate::TransactionClient::begin_pessimistic).
    pub fn begin_pessimistic(&self) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_pessimistic())?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Create a new customized [`SyncTransaction`].
    ///
    /// This is a synchronous version of [`TransactionClient::begin_with_options`](crate::TransactionClient::begin_with_options).
    pub fn begin_with_options(&self, options: TransactionOptions) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_with_options(options))?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Create a new customized [`SyncTransaction`] in the given transaction scope.
    ///
    /// This is a synchronous version of [`TransactionClient::begin_with_txn_scope`](crate::TransactionClient::begin_with_txn_scope).
    pub fn begin_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        options: TransactionOptions,
    ) -> Result<SyncTransaction> {
        let inner = safe_block_on(
            &self.runtime,
            self.client.begin_with_txn_scope(txn_scope, options),
        )?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Create a new customized [`SyncTransaction`] with an explicit start timestamp.
    ///
    /// This does not contact PD to fetch a timestamp. The provided `timestamp` is used as the
    /// transaction's start timestamp (`start_ts`).
    #[must_use]
    pub fn begin_with_start_timestamp(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> SyncTransaction {
        let inner = self.client.begin_with_start_timestamp(timestamp, options);
        SyncTransaction::new(inner, Arc::clone(&self.runtime))
    }

    /// Create a new read-only [`SyncSnapshot`] at the given [`Timestamp`].
    ///
    /// This is a synchronous version of [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> SyncSnapshot {
        let inner = self.client.snapshot(timestamp, options);
        SyncSnapshot::new(inner, Arc::clone(&self.runtime))
    }

    /// Retrieve the current [`Timestamp`].
    ///
    /// This is a synchronous version of [`TransactionClient::current_timestamp`](crate::TransactionClient::current_timestamp).
    pub fn current_timestamp(&self) -> Result<Timestamp> {
        safe_block_on(&self.runtime, self.client.current_timestamp())
    }

    /// Retrieve the current [`Timestamp`] for the given transaction scope.
    ///
    /// This is a synchronous version of [`TransactionClient::current_timestamp_with_txn_scope`](crate::TransactionClient::current_timestamp_with_txn_scope).
    pub fn current_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
    ) -> Result<Timestamp> {
        safe_block_on(
            &self.runtime,
            self.client.current_timestamp_with_txn_scope(txn_scope),
        )
    }

    /// Retrieve a minimum [`Timestamp`] from all TSO keyspace groups.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::current_all_tso_keyspace_group_min_ts`](crate::TransactionClient::current_all_tso_keyspace_group_min_ts).
    pub fn current_all_tso_keyspace_group_min_ts(&self) -> Result<Timestamp> {
        safe_block_on(
            &self.runtime,
            self.client.current_all_tso_keyspace_group_min_ts(),
        )
    }

    /// Retrieve the PD external timestamp.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::external_timestamp`](crate::TransactionClient::external_timestamp).
    pub fn external_timestamp(&self) -> Result<u64> {
        safe_block_on(&self.runtime, self.client.external_timestamp())
    }

    /// Set the PD external timestamp.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::set_external_timestamp`](crate::TransactionClient::set_external_timestamp).
    pub fn set_external_timestamp(&self, timestamp: u64) -> Result<()> {
        safe_block_on(&self.runtime, self.client.set_external_timestamp(timestamp))
    }

    /// Validate that `read_ts` is safe to use for reads (i.e. it is not in the future).
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::validate_read_ts`](crate::TransactionClient::validate_read_ts).
    pub fn validate_read_ts(&self, read_ts: u64, is_stale_read: bool) -> Result<()> {
        safe_block_on(
            &self.runtime,
            self.client.validate_read_ts(read_ts, is_stale_read),
        )
    }

    /// Validate that `read_ts` is safe to use for reads for the given transaction scope.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::validate_read_ts_with_txn_scope`](crate::TransactionClient::validate_read_ts_with_txn_scope).
    pub fn validate_read_ts_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        read_ts: u64,
        is_stale_read: bool,
    ) -> Result<()> {
        safe_block_on(
            &self.runtime,
            self.client
                .validate_read_ts_with_txn_scope(txn_scope, read_ts, is_stale_read),
        )
    }

    /// Set the refresh interval for low resolution timestamps.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::set_low_resolution_timestamp_update_interval`](crate::TransactionClient::set_low_resolution_timestamp_update_interval).
    pub fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: std::time::Duration,
    ) -> Result<()> {
        self.client
            .set_low_resolution_timestamp_update_interval(update_interval)
    }

    /// Retrieve a low resolution timestamp for the global txn scope.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::low_resolution_timestamp`](crate::TransactionClient::low_resolution_timestamp).
    pub fn low_resolution_timestamp(&self) -> Result<Timestamp> {
        safe_block_on(&self.runtime, self.client.low_resolution_timestamp())
    }

    /// Retrieve a low resolution timestamp for the given transaction scope.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::low_resolution_timestamp_with_txn_scope`](crate::TransactionClient::low_resolution_timestamp_with_txn_scope).
    pub fn low_resolution_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
    ) -> Result<Timestamp> {
        safe_block_on(
            &self.runtime,
            self.client
                .low_resolution_timestamp_with_txn_scope(txn_scope),
        )
    }

    /// Generate a timestamp representing the time `prev_seconds` seconds ago.
    ///
    /// This is a synchronous version of [`TransactionClient::stale_timestamp`](crate::TransactionClient::stale_timestamp).
    pub fn stale_timestamp(&self, prev_seconds: u64) -> Result<Timestamp> {
        safe_block_on(&self.runtime, self.client.stale_timestamp(prev_seconds))
    }

    /// Generate a timestamp representing the time `prev_seconds` seconds ago for the given
    /// transaction scope.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::stale_timestamp_with_txn_scope`](crate::TransactionClient::stale_timestamp_with_txn_scope).
    pub fn stale_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        prev_seconds: u64,
    ) -> Result<Timestamp> {
        safe_block_on(
            &self.runtime,
            self.client
                .stale_timestamp_with_txn_scope(txn_scope, prev_seconds),
        )
    }

    /// Get the cluster-wide minimum `safe_ts` across all TiKV stores.
    ///
    /// This is a synchronous version of [`TransactionClient::min_safe_ts`](crate::TransactionClient::min_safe_ts).
    pub fn min_safe_ts(&self) -> Result<u64> {
        safe_block_on(&self.runtime, self.client.min_safe_ts())
    }

    /// Get the minimum `safe_ts` for a transaction scope.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::min_safe_ts_with_txn_scope`](crate::TransactionClient::min_safe_ts_with_txn_scope).
    pub fn min_safe_ts_with_txn_scope(&self, txn_scope: impl AsRef<str>) -> Result<u64> {
        let txn_scope = txn_scope.as_ref().to_owned();
        safe_block_on(
            &self.runtime,
            self.client.min_safe_ts_with_txn_scope(txn_scope),
        )
    }

    /// Update the PD "service GC safe point" for the given service.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::update_service_gc_safe_point`](crate::TransactionClient::update_service_gc_safe_point).
    pub fn update_service_gc_safe_point(
        &self,
        service_id: impl Into<String>,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        safe_block_on(
            &self.runtime,
            self.client
                .update_service_gc_safe_point(service_id, ttl, safe_point),
        )
    }

    /// Update the PD "service safe point" (V2) for the given keyspace and service.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::update_service_safe_point_v2`](crate::TransactionClient::update_service_safe_point_v2).
    pub fn update_service_safe_point_v2(
        &self,
        keyspace_id: u32,
        service_id: impl Into<String>,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        safe_block_on(
            &self.runtime,
            self.client
                .update_service_safe_point_v2(keyspace_id, service_id, ttl, safe_point),
        )
    }

    /// Update the PD GC safe point (V2) for the given keyspace.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::update_gc_safe_point_v2`](crate::TransactionClient::update_gc_safe_point_v2).
    pub fn update_gc_safe_point_v2(&self, keyspace_id: u32, safe_point: u64) -> Result<u64> {
        safe_block_on(
            &self.runtime,
            self.client.update_gc_safe_point_v2(keyspace_id, safe_point),
        )
    }

    /// Get the PD GC safe point.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::get_gc_safe_point`](crate::TransactionClient::get_gc_safe_point).
    pub fn get_gc_safe_point(&self) -> Result<u64> {
        safe_block_on(&self.runtime, self.client.get_gc_safe_point())
    }

    /// Get the PD GC safe point (V2) for a given keyspace.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::get_gc_safe_point_v2`](crate::TransactionClient::get_gc_safe_point_v2).
    pub fn get_gc_safe_point_v2(&self, keyspace_id: u32) -> Result<u64> {
        safe_block_on(&self.runtime, self.client.get_gc_safe_point_v2(keyspace_id))
    }

    /// Check if it is safe to read using the given `start_ts`.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::check_visibility`](crate::TransactionClient::check_visibility).
    pub fn check_visibility(&self, start_ts: u64) -> Result<()> {
        safe_block_on(&self.runtime, self.client.check_visibility(start_ts))
    }

    /// Request garbage collection (GC) of the TiKV cluster.
    ///
    /// This is a synchronous version of [`TransactionClient::gc`](crate::TransactionClient::gc).
    pub fn gc(&self, safepoint: Timestamp) -> Result<bool> {
        safe_block_on(&self.runtime, self.client.gc(safepoint))
    }

    /// Request garbage collection (GC) of the TiKV cluster and return the effective safepoint.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::gc_safepoint`](crate::TransactionClient::gc_safepoint).
    pub fn gc_safepoint(&self, safepoint: Timestamp) -> Result<u64> {
        safe_block_on(&self.runtime, self.client.gc_safepoint(safepoint))
    }

    /// Clean up all locks in the specified range.
    ///
    /// This is a synchronous version of [`TransactionClient::cleanup_locks`](crate::TransactionClient::cleanup_locks).
    pub fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        safe_block_on(
            &self.runtime,
            self.client.cleanup_locks(range, safepoint, options),
        )
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This is a synchronous version of [`TransactionClient::resolve_locks`](crate::TransactionClient::resolve_locks).
    pub fn resolve_locks(
        &self,
        locks: Vec<crate::transaction::ProtoLockInfo>,
        timestamp: Timestamp,
        backoff: crate::Backoff,
    ) -> Result<Vec<crate::transaction::ProtoLockInfo>> {
        safe_block_on(
            &self.runtime,
            self.client.resolve_locks(locks, timestamp, backoff),
        )
    }

    /// Performs a one-shot lock resolve attempt and returns the outcome.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::resolve_locks_once`](crate::TransactionClient::resolve_locks_once).
    pub fn resolve_locks_once(
        &self,
        locks: Vec<crate::transaction::ProtoLockInfo>,
        timestamp: Timestamp,
        pessimistic_region_resolve: bool,
    ) -> Result<ResolveLocksResult> {
        safe_block_on(
            &self.runtime,
            self.client
                .resolve_locks_once(locks, timestamp, pessimistic_region_resolve),
        )
    }

    /// Resolves locks for read and returns any that remain live.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::resolve_locks_for_read`](crate::TransactionClient::resolve_locks_for_read).
    pub fn resolve_locks_for_read(
        &self,
        locks: Vec<crate::transaction::ProtoLockInfo>,
        timestamp: Timestamp,
        force_resolve_lock_lite: bool,
    ) -> Result<ResolveLocksForReadResult> {
        safe_block_on(
            &self.runtime,
            self.client
                .resolve_locks_for_read(locks, timestamp, force_resolve_lock_lite),
        )
    }

    /// Cleans up all keys in a range and quickly reclaim disk space.
    ///
    /// The range can span over multiple regions.
    ///
    /// Note that the request will directly delete data from RocksDB, and all MVCC will be erased.
    ///
    /// This interface is intended for special scenarios that resemble operations like "drop table" or "drop database" in TiDB.
    ///
    /// This is a synchronous version of [`TransactionClient::unsafe_destroy_range`](crate::TransactionClient::unsafe_destroy_range).
    pub fn unsafe_destroy_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        safe_block_on(&self.runtime, self.client.unsafe_destroy_range(range))
    }

    /// Delete all versions of all keys in the given key range.
    ///
    /// This is a synchronous version of [`TransactionClient::delete_range`](crate::TransactionClient::delete_range).
    pub fn delete_range(&self, range: impl Into<BoundRange>, concurrency: usize) -> Result<usize> {
        safe_block_on(&self.runtime, self.client.delete_range(range, concurrency))
    }

    /// Notify regions in the given key range of an upcoming unsafe-destroy-range operation.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::notify_delete_range`](crate::TransactionClient::notify_delete_range).
    pub fn notify_delete_range(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Result<usize> {
        safe_block_on(
            &self.runtime,
            self.client.notify_delete_range(range, concurrency),
        )
    }

    /// Split regions by the provided split keys.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::split_regions`](crate::TransactionClient::split_regions).
    pub fn split_regions(
        &self,
        split_keys: impl IntoIterator<Item = impl Into<crate::Key>>,
        scatter: bool,
        table_id: Option<i64>,
    ) -> Result<Vec<u64>> {
        safe_block_on(
            &self.runtime,
            self.client.split_regions(split_keys, scatter, table_id),
        )
    }

    /// Wait for a scatter-region PD operator to finish for the given region.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::wait_scatter_region_finish`](crate::TransactionClient::wait_scatter_region_finish).
    pub fn wait_scatter_region_finish(
        &self,
        region_id: u64,
        backoff: crate::Backoff,
    ) -> Result<()> {
        safe_block_on(
            &self.runtime,
            self.client.wait_scatter_region_finish(region_id, backoff),
        )
    }

    /// Check whether a region is currently in scattering (`scatter-region` operator is running).
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::check_region_in_scattering`](crate::TransactionClient::check_region_in_scattering).
    pub fn check_region_in_scattering(&self, region_id: u64) -> Result<bool> {
        safe_block_on(
            &self.runtime,
            self.client.check_region_in_scattering(region_id),
        )
    }

    /// Compact a specified key range on TiFlash stores.
    ///
    /// This is a synchronous version of [`TransactionClient::compact`](crate::TransactionClient::compact).
    pub fn compact(
        &self,
        physical_table_id: i64,
        logical_table_id: i64,
        start_key: impl Into<crate::Key>,
    ) -> Result<Vec<crate::transaction::ProtoCompactResponse>> {
        safe_block_on(
            &self.runtime,
            self.client
                .compact(physical_table_id, logical_table_id, start_key),
        )
    }

    /// Get system table data from TiFlash stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::tiflash_system_table`](crate::TransactionClient::tiflash_system_table).
    pub fn tiflash_system_table(
        &self,
        sql: impl Into<String>,
    ) -> Result<Vec<crate::transaction::ProtoTiFlashSystemTableResponse>> {
        safe_block_on(&self.runtime, self.client.tiflash_system_table(sql))
    }

    /// Register a lock observer on all TiKV stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::register_lock_observer`](crate::TransactionClient::register_lock_observer).
    pub fn register_lock_observer(&self, max_ts: u64) -> Result<()> {
        safe_block_on(&self.runtime, self.client.register_lock_observer(max_ts))
    }

    /// Check a lock observer on all TiKV stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::check_lock_observer`](crate::TransactionClient::check_lock_observer).
    pub fn check_lock_observer(
        &self,
        max_ts: u64,
    ) -> Result<(bool, Vec<crate::transaction::ProtoLockInfo>)> {
        safe_block_on(&self.runtime, self.client.check_lock_observer(max_ts))
    }

    /// Remove a lock observer on all TiKV stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::remove_lock_observer`](crate::TransactionClient::remove_lock_observer).
    pub fn remove_lock_observer(&self, max_ts: u64) -> Result<()> {
        safe_block_on(&self.runtime, self.client.remove_lock_observer(max_ts))
    }

    /// Physical scan locks from all TiKV stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::physical_scan_lock`](crate::TransactionClient::physical_scan_lock).
    pub fn physical_scan_lock(
        &self,
        max_ts: u64,
        start_key: impl Into<crate::Key>,
        limit: u32,
    ) -> Result<Vec<crate::transaction::ProtoLockInfo>> {
        safe_block_on(
            &self.runtime,
            self.client.physical_scan_lock(max_ts, start_key, limit),
        )
    }

    /// Get current lock waiting status from all TiKV stores.
    ///
    /// This is a synchronous version of [`TransactionClient::lock_wait_info`](crate::TransactionClient::lock_wait_info).
    pub fn lock_wait_info(&self) -> Result<Vec<crate::transaction::ProtoWaitForEntry>> {
        safe_block_on(&self.runtime, self.client.lock_wait_info())
    }

    /// Get lock waiting history from all TiKV stores.
    ///
    /// This is a synchronous version of
    /// [`TransactionClient::lock_wait_history`](crate::TransactionClient::lock_wait_history).
    pub fn lock_wait_history(&self) -> Result<Vec<crate::transaction::ProtoWaitForEntry>> {
        safe_block_on(&self.runtime, self.client.lock_wait_history())
    }

    /// Scan all locks in the specified range.
    ///
    /// This is only available for integration tests.
    ///
    /// Note: `batch_size` must be >= expected number of locks.
    ///
    /// This is a synchronous version of [`TransactionClient::scan_locks`](crate::TransactionClient::scan_locks).
    #[cfg(feature = "integration-tests")]
    pub fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<crate::proto::kvrpcpb::LockInfo>> {
        safe_block_on(
            &self.runtime,
            self.client.scan_locks(safepoint, range, batch_size),
        )
    }
}

impl Clone for SyncTransactionClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_nested_runtime_outside_async() {
        // When called outside an async context, should return Ok(())
        let result = check_nested_runtime();
        assert!(
            result.is_ok(),
            "check_nested_runtime should succeed outside async context"
        );
    }

    #[test]
    fn test_check_nested_runtime_inside_async() {
        // When called inside an async context, should return Err
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = check_nested_runtime();
            assert!(
                result.is_err(),
                "check_nested_runtime should fail inside async context"
            );

            // Verify the error type is correct
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_safe_block_on_outside_async() {
        // safe_block_on should work when called outside an async context
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = safe_block_on(&rt, async { Ok::<_, Error>(42) });
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_safe_block_on_inside_async() {
        // safe_block_on should fail when called inside an async context
        let outer_rt = tokio::runtime::Runtime::new().unwrap();

        // Create the inner runtime OUTSIDE the async context
        let inner_rt = tokio::runtime::Runtime::new().unwrap();

        outer_rt.block_on(async {
            let result = safe_block_on(&inner_rt, async { Ok::<_, Error>(42) });

            assert!(
                result.is_err(),
                "safe_block_on should fail inside async context"
            );

            // Verify the error type is correct
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_nested_runtime_error_matching() {
        // Verify that NestedRuntimeError can be matched on programmatically
        let outer_rt = tokio::runtime::Runtime::new().unwrap();

        outer_rt.block_on(async {
            let result = check_nested_runtime();

            assert!(result.is_err());

            // Demonstrate type-safe error matching
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }
}
