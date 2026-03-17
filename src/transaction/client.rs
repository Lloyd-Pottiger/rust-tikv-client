// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use log::info;
use tokio::sync::RwLock;

use super::latches::TxnLocalLatches;
use crate::backoff::{DEFAULT_REGION_BACKOFF, DEFAULT_STORE_BACKOFF};
use crate::config::Config;
use crate::gc_safe_point::GcSafePointCache;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::pdpb::Timestamp;
use crate::request::plan::CleanupLocksResult;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::Plan;
use crate::safe_ts::SafeTsCache;
use crate::timestamp::TimestampExt;
use crate::transaction::lock::ResolveLocksOptions;
use crate::transaction::lowering::new_check_lock_observer_request;
use crate::transaction::lowering::new_compact_request;
use crate::transaction::lowering::new_delete_range_request;
use crate::transaction::lowering::new_flashback_to_version_request;
use crate::transaction::lowering::new_get_lock_wait_history_request;
use crate::transaction::lowering::new_get_lock_wait_info_request;
use crate::transaction::lowering::new_get_ti_flash_system_table_request;
use crate::transaction::lowering::new_physical_scan_lock_request;
use crate::transaction::lowering::new_prepare_flashback_to_version_request;
use crate::transaction::lowering::new_register_lock_observer_request;
use crate::transaction::lowering::new_remove_lock_observer_request;
use crate::transaction::lowering::new_scan_lock_request;
use crate::transaction::lowering::new_split_region_request;
use crate::transaction::lowering::new_unsafe_destroy_range_request;
use crate::transaction::BoundLockResolver;
use crate::transaction::LockResolver;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksForReadResult;
use crate::transaction::ResolveLocksResult;
use crate::transaction::Snapshot;
use crate::transaction::Transaction;
use crate::transaction::TransactionOptions;
use crate::Backoff;
use crate::BoundRange;
use crate::Key;
use crate::Result;

/// Protobuf-generated lock information returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::LockInfo as ProtoLockInfo;

/// Protobuf-generated lock waiting entry returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::deadlock::WaitForEntry as ProtoWaitForEntry;

/// Protobuf-generated compact response returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::CompactResponse as ProtoCompactResponse;

/// Protobuf-generated TiFlash system table response returned by TiKV.
///
/// This type is generated from TiKV's protobuf definitions and may change in a
/// future release even if the wire format is compatible.
#[doc(inline)]
pub use crate::proto::kvrpcpb::TiFlashSystemTableResponse as ProtoTiFlashSystemTableResponse;

/// The TiKV transactional `Client` is used to interact with TiKV using transactional requests.
///
/// Transactions support optimistic and pessimistic modes. For more details see the SIG-transaction
/// [docs](https://github.com/tikv/sig-transaction/tree/master/doc/tikv#optimistic-and-pessimistic-transactions).
///
/// Begin a [`Transaction`] by calling [`begin_optimistic`](Client::begin_optimistic) or
/// [`begin_pessimistic`](Client::begin_pessimistic). A transaction must be rolled back or committed.
///
/// Besides transactions, the client provides some further functionality:
/// - `gc`: trigger a GC process which clears stale data in the cluster.
/// - `current_timestamp`: get the current `Timestamp` from PD.
/// - `snapshot`: get a [`Snapshot`] of the database at a specified timestamp.
///   A `Snapshot` is a read-only transaction.
///
/// The returned results of transactional requests are [`Future`](std::future::Future)s that must be
/// awaited to execute.
pub struct Client<PdC: PdClient = PdRpcClient> {
    pd: Arc<PdC>,
    keyspace: Keyspace,
    resolve_locks_ctx: ResolveLocksContext,
    safe_ts: SafeTsCache<PdC>,
    gc_safe_point: GcSafePointCache<PdC>,
    last_tsos: Arc<RwLock<HashMap<String, LastTso>>>,
    low_resolution_ts_update_interval_ms: Arc<AtomicU64>,
    txn_latches: Option<Arc<TxnLocalLatches>>,
}

#[derive(Clone, Debug)]
struct LastTso {
    tso: Timestamp,
    arrival: Instant,
}

const DEFAULT_LOW_RESOLUTION_TS_UPDATE_INTERVAL_MS: u64 = 2_000;

fn default_low_resolution_ts_update_interval_ms() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(DEFAULT_LOW_RESOLUTION_TS_UPDATE_INTERVAL_MS))
}

impl<PdC: PdClient> Clone for Client<PdC> {
    fn clone(&self) -> Self {
        Self {
            pd: self.pd.clone(),
            keyspace: self.keyspace,
            resolve_locks_ctx: self.resolve_locks_ctx.clone(),
            safe_ts: self.safe_ts.clone(),
            gc_safe_point: self.gc_safe_point.clone(),
            last_tsos: self.last_tsos.clone(),
            low_resolution_ts_update_interval_ms: self.low_resolution_ts_update_interval_ms.clone(),
            txn_latches: self.txn_latches.clone(),
        }
    }
}

impl Client {
    /// Create a transactional [`Client`] and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// # });
    /// ```
    pub async fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Client> {
        // debug!("creating transactional client");
        Self::new_with_config(pd_endpoints, Config::default()).await
    }

    /// Create a transactional [`Client`] with a custom configuration, and connect to the TiKV cluster.
    ///
    /// Because TiKV is managed by a [PD](https://github.com/pingcap/pd/) cluster, the endpoints for
    /// PD must be provided, not the TiKV nodes. It's important to include more than one PD endpoint
    /// (include all endpoints, if possible), this helps avoid having a single point of failure.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::time::Duration;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new_with_config(
    ///     vec!["192.168.0.100"],
    ///     Config::default().with_timeout(Duration::from_secs(60)),
    /// )
    /// .await
    /// .unwrap();
    /// # });
    /// ```
    pub async fn new_with_config<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Client> {
        debug!("creating new transactional client");
        let enable_region_cache_preload = config.enable_region_cache_preload;
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let health_feedback_update_interval = config.health_feedback_update_interval;
        let txn_local_latches_capacity = config.txn_local_latches_capacity;
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        pd.install_health_feedback_observer();
        crate::pd::spawn_health_feedback_updater(pd.clone(), health_feedback_update_interval);
        let keyspace = match config.keyspace {
            Some(name) => {
                let keyspace = pd.load_keyspace(&name).await?;
                Keyspace::Enable {
                    keyspace_id: keyspace.id,
                }
            }
            None => Keyspace::Disable,
        };
        if enable_region_cache_preload {
            let (start_key, end_key) = keyspace.prefix_range(KeyMode::Txn);
            pd.clone().spawn_region_cache_preload(start_key, end_key);
        }
        let txn_latches = (txn_local_latches_capacity > 0)
            .then(|| Arc::new(TxnLocalLatches::new(txn_local_latches_capacity)));
        Ok(Client {
            safe_ts: SafeTsCache::new(pd.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd.clone(), keyspace),
            pd,
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms: default_low_resolution_ts_update_interval_ms(),
            txn_latches,
        })
    }

    /// Create a transactional [`Client`] that uses API V2 without adding or removing any API V2
    /// keyspace/key-mode prefix, with a custom configuration.
    ///
    /// This is intended for **server-side embedding** use cases. `config.keyspace` must be unset.
    pub async fn new_with_config_api_v2_no_prefix<S: Into<String>>(
        pd_endpoints: Vec<S>,
        config: Config,
    ) -> Result<Client> {
        if config.keyspace.is_some() {
            return Err(crate::Error::StringError(
                "config.keyspace must be unset when using api-v2-no-prefix mode".to_owned(),
            ));
        }

        debug!("creating new transactional client (api-v2-no-prefix)");
        let enable_region_cache_preload = config.enable_region_cache_preload;
        let pd_endpoints: Vec<String> = pd_endpoints.into_iter().map(Into::into).collect();
        let health_feedback_update_interval = config.health_feedback_update_interval;
        let txn_local_latches_capacity = config.txn_local_latches_capacity;
        let pd = Arc::new(PdRpcClient::connect(&pd_endpoints, config.clone(), true).await?);
        pd.install_health_feedback_observer();
        crate::pd::spawn_health_feedback_updater(pd.clone(), health_feedback_update_interval);
        if enable_region_cache_preload {
            let (start_key, end_key) = Keyspace::ApiV2NoPrefix.prefix_range(KeyMode::Txn);
            pd.clone().spawn_region_cache_preload(start_key, end_key);
        }
        let txn_latches = (txn_local_latches_capacity > 0)
            .then(|| Arc::new(TxnLocalLatches::new(txn_local_latches_capacity)));
        Ok(Client {
            safe_ts: SafeTsCache::new(pd.clone(), Keyspace::ApiV2NoPrefix),
            gc_safe_point: GcSafePointCache::new(pd.clone(), Keyspace::ApiV2NoPrefix),
            pd,
            keyspace: Keyspace::ApiV2NoPrefix,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms: default_low_resolution_ts_update_interval_ms(),
            txn_latches,
        })
    }

    /// Returns the TiKV cluster ID.
    pub fn cluster_id(&self) -> u64 {
        self.pd.cluster_id()
    }

    /// Close cached gRPC connections to a TiKV store address.
    ///
    /// The client will reconnect the next time that address is used.
    pub async fn close_addr(&self, address: &str) -> bool {
        self.pd.close_addr(address).await
    }
}

impl<PdC: PdClient> Client<PdC> {
    /// Returns a handle to the underlying PD client.
    #[must_use]
    pub fn pd_client(&self) -> Arc<PdC> {
        self.pd.clone()
    }

    fn canonicalize_txn_scope(txn_scope: &str) -> String {
        if txn_scope.is_empty() || txn_scope == "global" {
            String::new()
        } else {
            txn_scope.to_owned()
        }
    }

    async fn record_last_tso(&self, dc_location: String, tso: Timestamp) {
        let mut last_tsos = self.last_tsos.write().await;
        last_tsos.insert(
            dc_location,
            LastTso {
                tso,
                arrival: Instant::now(),
            },
        );
    }

    fn stale_timestamp_from_last_tso(last_tso: &LastTso, prev_seconds: u64) -> Result<Timestamp> {
        let physical_seconds = last_tso.tso.physical / 1000;
        if physical_seconds <= i64::try_from(prev_seconds).unwrap_or(i64::MAX) {
            return Err(crate::Error::StringError(format!(
                "invalid prev_seconds {prev_seconds}"
            )));
        }

        let elapsed_ms = last_tso.arrival.elapsed().as_millis();
        let elapsed_ms = i128::try_from(elapsed_ms).unwrap_or(i128::MAX);
        let prev_ms = u128::from(prev_seconds).saturating_mul(1000);
        let prev_ms = i128::try_from(prev_ms).unwrap_or(i128::MAX);

        let physical_ms = i128::from(last_tso.tso.physical);
        let stale_physical_ms = physical_ms + elapsed_ms - prev_ms;
        if stale_physical_ms < 0 {
            return Err(crate::Error::StringError(format!(
                "invalid prev_seconds {prev_seconds}"
            )));
        }

        Ok(Timestamp {
            physical: i64::try_from(stale_physical_ms).unwrap_or(i64::MAX),
            logical: 0,
            suffix_bits: last_tso.tso.suffix_bits,
        })
    }

    async fn stale_timestamp_with_dc_location(
        &self,
        dc_location: String,
        prev_seconds: u64,
    ) -> Result<Timestamp> {
        if let Some(last_tso) = self.last_tsos.read().await.get(&dc_location).cloned() {
            return Self::stale_timestamp_from_last_tso(&last_tso, prev_seconds);
        }

        let tso = if dc_location.is_empty() {
            self.pd.clone().get_timestamp().await?
        } else {
            PdClient::get_timestamp_with_dc_location(self.pd.clone(), dc_location.clone()).await?
        };
        let last_tso = LastTso {
            tso: tso.clone(),
            arrival: Instant::now(),
        };
        self.last_tsos
            .write()
            .await
            .insert(dc_location, last_tso.clone());
        Self::stale_timestamp_from_last_tso(&last_tso, prev_seconds)
    }

    /// Returns a [`LockResolver`] handle associated with this client.
    ///
    /// The returned resolver shares the resolve-lock caches with this client.
    #[must_use]
    pub fn lock_resolver(&self) -> LockResolver {
        LockResolver::new(self.resolve_locks_ctx.clone())
    }

    /// Returns a [`BoundLockResolver`] handle associated with this client.
    ///
    /// The returned resolver binds this client's PD client and keyspace. It also shares the
    /// resolve-lock caches with this client.
    #[must_use]
    pub fn bound_lock_resolver(&self) -> BoundLockResolver<PdC> {
        BoundLockResolver::new(
            self.pd.clone(),
            self.keyspace,
            self.resolve_locks_ctx.clone(),
        )
    }

    /// Creates a new optimistic [`Transaction`].
    ///
    /// Use the transaction to issue requests like [`get`](Transaction::get) or
    /// [`put`](Transaction::put).
    ///
    /// Write operations do not lock data in TiKV, thus the commit request may fail due to a write
    /// conflict.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_optimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_optimistic(&self) -> Result<Transaction<PdC>> {
        debug!("creating new optimistic transaction");
        let timestamp = self.current_timestamp().await?;
        Ok(self.new_transaction(timestamp, TransactionOptions::new_optimistic()))
    }

    /// Creates a new pessimistic [`Transaction`].
    ///
    /// Write operations will lock the data until committed, thus commit requests should not suffer
    /// from write conflicts.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client.begin_pessimistic().await.unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_pessimistic(&self) -> Result<Transaction<PdC>> {
        debug!("creating new pessimistic transaction");
        let timestamp = self.current_timestamp().await?;
        Ok(self.new_transaction(timestamp, TransactionOptions::new_pessimistic()))
    }

    /// Create a new customized [`Transaction`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient, TransactionOptions};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut transaction = client
    ///     .begin_with_options(TransactionOptions::default().use_async_commit())
    ///     .await
    ///     .unwrap();
    /// // ... Issue some commands.
    /// transaction.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction<PdC>> {
        debug!("creating new customized transaction");
        options.validate()?;
        let txn_scope = options
            .txn_scope_as_deref()
            .map(|txn_scope| txn_scope.to_owned());
        let timestamp = match txn_scope.as_deref() {
            None => self.current_timestamp().await?,
            Some(scope) => self.current_timestamp_with_txn_scope(scope).await?,
        };
        Ok(self.new_transaction(timestamp, options))
    }

    /// Create a new customized [`Transaction`] in the given transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator
    /// (`dc_location=""`). Otherwise `txn_scope` is passed through as PD `dc_location` to request
    /// a local TSO.
    pub async fn begin_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        options: TransactionOptions,
    ) -> Result<Transaction<PdC>> {
        let txn_scope = txn_scope.as_ref();
        debug!(
            "creating new customized transaction with txn_scope={}",
            txn_scope
        );
        options.validate()?;
        let timestamp = self.current_timestamp_with_txn_scope(txn_scope).await?;
        Ok(self.new_transaction(timestamp, options.txn_scope(txn_scope)))
    }

    /// Create a new customized [`Transaction`] with an explicit start timestamp.
    ///
    /// This does not contact PD to fetch a timestamp. The provided `timestamp` is used as the
    /// transaction's start timestamp (`start_ts`).
    #[must_use]
    pub fn begin_with_start_timestamp(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Transaction<PdC> {
        debug!("creating new customized transaction with explicit start timestamp");
        self.new_transaction(timestamp, options)
    }

    /// Retrieve the current [`Timestamp`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let timestamp = client.current_timestamp().await.unwrap();
    /// # });
    /// ```
    pub async fn current_timestamp(&self) -> Result<Timestamp> {
        let timestamp = self.pd.clone().get_timestamp().await?;
        self.record_last_tso(String::new(), timestamp.clone()).await;
        Ok(timestamp)
    }

    /// Retrieve the current [`Timestamp`] for the given transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this uses the global TSO allocator
    /// (`dc_location=""`). Otherwise `txn_scope` is passed through as PD `dc_location` to request
    /// a local TSO, matching client-go `CurrentTimestamp(txnScope)` behavior.
    pub async fn current_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
    ) -> Result<Timestamp> {
        let dc_location = Self::canonicalize_txn_scope(txn_scope.as_ref());
        let timestamp =
            PdClient::get_timestamp_with_dc_location(self.pd.clone(), dc_location.clone()).await?;
        self.record_last_tso(dc_location, timestamp.clone()).await;
        Ok(timestamp)
    }

    /// Retrieve a minimum [`Timestamp`] from all TSO keyspace groups.
    ///
    /// This maps to client-go `KVStore.CurrentAllTSOKeyspaceGroupMinTs`.
    pub async fn current_all_tso_keyspace_group_min_ts(&self) -> Result<Timestamp> {
        PdClient::get_min_ts(self.pd.clone()).await
    }

    /// Retrieve the PD external timestamp.
    ///
    /// This maps to client-go `Oracle.GetExternalTimestamp`.
    pub async fn external_timestamp(&self) -> Result<u64> {
        PdClient::get_external_timestamp(self.pd.clone()).await
    }

    /// Set the PD external timestamp.
    ///
    /// This maps to client-go `Oracle.SetExternalTimestamp`.
    pub async fn set_external_timestamp(&self, timestamp: u64) -> Result<()> {
        PdClient::set_external_timestamp(self.pd.clone(), timestamp).await
    }

    /// Validate that `read_ts` is safe to use for reads (i.e. it is not in the future).
    ///
    /// This maps to client-go `Oracle.ValidateReadTS` for the global txn scope.
    pub async fn validate_read_ts(&self, read_ts: u64, is_stale_read: bool) -> Result<()> {
        self.validate_read_ts_with_txn_scope("", read_ts, is_stale_read)
            .await
    }

    /// Validate that `read_ts` is safe to use for reads for the given transaction scope.
    ///
    /// This maps to client-go `Oracle.ValidateReadTS(ctx, readTS, isStaleRead, opt)` where
    /// `opt.TxnScope` is `txn_scope`.
    pub async fn validate_read_ts_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        read_ts: u64,
        is_stale_read: bool,
    ) -> Result<()> {
        // Guard against a common misuse we have seen in client-go.
        if read_ts >= i64::MAX as u64 && read_ts < u64::MAX {
            return Err(crate::Error::StringError(format!(
                "MaxInt64 <= readTS < MaxUint64, readTS={read_ts}"
            )));
        }

        // `u64::MAX` is used as "latest" in some code paths, but it must not be used for stale reads.
        if read_ts == u64::MAX {
            if is_stale_read {
                return Err(crate::Error::StringError(
                    "cannot set read ts to max uint64 for stale read".to_owned(),
                ));
            }
            return Ok(());
        }

        let dc_location = Self::canonicalize_txn_scope(txn_scope.as_ref());
        if let Some(last_tso) = self.last_tsos.read().await.get(&dc_location) {
            if read_ts <= last_tso.tso.version() {
                return Ok(());
            }
        }

        let current_ts = self
            .current_timestamp_with_txn_scope(txn_scope.as_ref())
            .await?;
        let current_version = current_ts.version();

        if read_ts <= current_version {
            Ok(())
        } else {
            Err(crate::Error::StringError(format!(
                "cannot set read timestamp to a future time, readTS: {read_ts}, currentTS: {current_version}"
            )))
        }
    }

    /// Set the refresh interval for low resolution timestamps.
    ///
    /// This maps to client-go `Oracle.SetLowResolutionTimestampUpdateInterval`.
    pub fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: Duration,
    ) -> Result<()> {
        if update_interval.is_zero() {
            return Err(crate::Error::StringError(
                "updateInterval must be > 0".to_owned(),
            ));
        }

        let interval_ms = update_interval.as_millis().max(1);
        let interval_ms = u64::try_from(interval_ms)
            .map_err(|_| crate::Error::StringError("updateInterval is too large".to_owned()))?;
        self.low_resolution_ts_update_interval_ms
            .store(interval_ms, Ordering::Relaxed);
        Ok(())
    }

    /// Retrieve a low resolution timestamp for the global txn scope.
    ///
    /// This maps to client-go `Oracle.GetLowResolutionTimestamp`.
    pub async fn low_resolution_timestamp(&self) -> Result<Timestamp> {
        self.low_resolution_timestamp_with_txn_scope("").await
    }

    /// Retrieve a low resolution timestamp for the given transaction scope.
    ///
    /// This maps to client-go `Oracle.GetLowResolutionTimestamp`.
    pub async fn low_resolution_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
    ) -> Result<Timestamp> {
        let dc_location = Self::canonicalize_txn_scope(txn_scope.as_ref());
        let interval_ms = self
            .low_resolution_ts_update_interval_ms
            .load(Ordering::Relaxed)
            .max(1);
        let update_interval = Duration::from_millis(interval_ms);

        if let Some(last_tso) = self.last_tsos.read().await.get(&dc_location) {
            if last_tso.arrival.elapsed() <= update_interval {
                return Ok(last_tso.tso.clone());
            }
        }

        self.current_timestamp_with_txn_scope(txn_scope.as_ref())
            .await
    }

    /// Generate a timestamp representing the time `prev_seconds` seconds ago.
    ///
    /// This is intended for staleness reads: when combined with
    /// [`Snapshot::set_stale_read`](crate::Snapshot::set_stale_read), reads at the returned
    /// timestamp can be served from replicas whose `safe_ts >= start_ts`.
    ///
    /// This maps to client-go `Oracle.GetStaleTimestamp` for the global txn scope.
    ///
    /// This method uses the most recently observed PD timestamp to avoid an extra PD call. When no
    /// timestamp has been observed yet, it fetches one from PD.
    pub async fn stale_timestamp(&self, prev_seconds: u64) -> Result<Timestamp> {
        self.stale_timestamp_with_dc_location(String::new(), prev_seconds)
            .await
    }

    /// Generate a timestamp representing the time `prev_seconds` seconds ago for the given
    /// transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this behaves the same as
    /// [`Client::stale_timestamp`].
    ///
    /// This maps to client-go `Oracle.GetStaleTimestamp(txnScope, prevSecond)`.
    pub async fn stale_timestamp_with_txn_scope(
        &self,
        txn_scope: impl AsRef<str>,
        prev_seconds: u64,
    ) -> Result<Timestamp> {
        let dc_location = Self::canonicalize_txn_scope(txn_scope.as_ref());
        self.stale_timestamp_with_dc_location(dc_location, prev_seconds)
            .await
    }

    /// Get the cluster-wide minimum `safe_ts` across all TiKV stores (and TiFlash stores, if
    /// present).
    ///
    /// This value is a best-effort signal used by stale reads: if it is non-zero, reads at
    /// timestamps less than or equal to the returned `safe_ts` can be served from replicas (subject
    /// to per-region `safe_ts`).
    ///
    /// Returns `0` when the minimum safe-ts cannot be determined (for example, if it has not been
    /// successfully refreshed yet).
    ///
    /// The returned value is cached and best-effort: once a non-zero safe-ts has been observed for
    /// all stores, transient store errors will not cause the returned minimum safe-ts to drop back
    /// to `0`.
    pub async fn min_safe_ts(&self) -> Result<u64> {
        self.safe_ts.min_safe_ts().await
    }

    /// Get the minimum `safe_ts` for a transaction scope.
    ///
    /// When `txn_scope` is `"global"` (or empty), this behaves the same as [`Client::min_safe_ts`].
    /// Otherwise, only stores whose `zone` label matches `txn_scope` are considered, matching
    /// client-go `GetMinSafeTS(txnScope)` behavior.
    ///
    /// Returns `0` when the minimum safe-ts cannot be determined, or when no stores match the
    /// provided scope.
    ///
    /// This method uses the same cached best-effort semantics as [`Client::min_safe_ts`].
    pub async fn min_safe_ts_with_txn_scope(&self, txn_scope: impl AsRef<str>) -> Result<u64> {
        self.safe_ts
            .min_safe_ts_with_txn_scope(txn_scope.as_ref())
            .await
    }

    /// Update the PD "service GC safe point" for the given service.
    ///
    /// This maps to client-go `pd.Client.UpdateServiceGCSafePoint`.
    ///
    /// On success, returns PD's `min_safe_point` (the effective service GC safe point).
    pub async fn update_service_gc_safe_point(
        &self,
        service_id: impl Into<String>,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.pd
            .clone()
            .update_service_gc_safe_point(service_id.into(), ttl, safe_point)
            .await
    }

    /// Update the PD "service safe point" (V2) for the given keyspace and service.
    ///
    /// This maps to client-go `pd.Client.UpdateServiceSafePointV2`.
    ///
    /// On success, returns PD's `min_safe_point` (the effective service safe point).
    pub async fn update_service_safe_point_v2(
        &self,
        keyspace_id: u32,
        service_id: impl Into<String>,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        self.pd
            .clone()
            .update_service_safe_point_v2(keyspace_id, service_id.into(), ttl, safe_point)
            .await
    }

    /// Update the PD GC safe point (V2) for the given keyspace.
    ///
    /// This maps to client-go `pd.Client.UpdateGCSafePointV2`.
    ///
    /// On success, returns PD's `new_safe_point` (the effective GC safe point).
    pub async fn update_gc_safe_point_v2(&self, keyspace_id: u32, safe_point: u64) -> Result<u64> {
        self.pd
            .clone()
            .update_gc_safe_point_v2(keyspace_id, safe_point)
            .await
    }

    /// Get the PD GC safe point.
    ///
    /// This maps to client-go `pd.Client.GetGCSafePoint`.
    ///
    /// On success, returns PD's `safe_point`.
    pub async fn get_gc_safe_point(&self) -> Result<u64> {
        self.pd.clone().get_gc_safe_point().await
    }

    /// Get the PD GC safe point (V2) for a given keyspace.
    ///
    /// This maps to client-go `pd.Client.GetGCSafePointV2`.
    ///
    /// On success, returns PD's `safe_point`.
    pub async fn get_gc_safe_point_v2(&self, keyspace_id: u32) -> Result<u64> {
        self.pd.clone().get_gc_safe_point_v2(keyspace_id).await
    }

    /// Check if it is safe to read using the given `start_ts`.
    ///
    /// This maps to client-go `KVStore.CheckVisibility`.
    ///
    /// When the provided `start_ts` falls behind the current GC safe point, this returns
    /// [`Error::TxnAbortedByGc`].
    pub async fn check_visibility(&self, start_ts: u64) -> Result<()> {
        self.gc_safe_point.check_visibility(start_ts).await
    }

    /// Request garbage collection (GC) of the TiKV cluster.
    ///
    /// GC deletes MVCC records whose timestamp is lower than the given `safepoint`. We must guarantee
    ///  that all transactions started before this timestamp had committed. We can keep an active
    /// transaction list in application to decide which is the minimal start timestamp of them.
    ///
    /// For each key, the last mutation record (unless it's a deletion) before `safepoint` is retained.
    ///
    /// GC is performed by:
    /// 1. resolving all locks with timestamp <= `safepoint`
    /// 2. updating PD's known safepoint
    ///
    /// This is a simplified version of [GC in TiDB](https://docs.pingcap.com/tidb/stable/garbage-collection-overview).
    /// We skip the second step "delete ranges" which is an optimization for TiDB.
    pub async fn gc(&self, safepoint: Timestamp) -> Result<bool> {
        let requested = safepoint.version();
        let new_safe_point = self.gc_safepoint(safepoint).await?;
        Ok(new_safe_point == requested)
    }

    /// Request garbage collection (GC) of the TiKV cluster and return the effective safepoint.
    ///
    /// This is identical to [`Client::gc`] except it returns PD's `new_safe_point` (mirroring
    /// client-go GC behavior, which may return a safepoint lower than requested).
    pub async fn gc_safepoint(&self, safepoint: Timestamp) -> Result<u64> {
        debug!("invoking transactional gc request");

        let options = ResolveLocksOptions::default();
        self.cleanup_locks(.., &safepoint, options).await?;

        // update safepoint to PD
        let requested = safepoint.version();
        let new_safe_point = match self.keyspace {
            Keyspace::Enable { keyspace_id } => match self
                .pd
                .clone()
                .update_gc_safe_point_v2(keyspace_id, requested)
                .await
            {
                Ok(new_safe_point) => new_safe_point,
                Err(crate::Error::Unimplemented) => {
                    self.pd.clone().update_safepoint(requested).await?
                }
                Err(err) => return Err(err),
            },
            _ => self.pd.clone().update_safepoint(requested).await?,
        };
        self.gc_safe_point.observe_safe_point(new_safe_point).await;
        if new_safe_point != safepoint.version() {
            info!(
                "new safepoint {} != user-specified safepoint {}",
                new_safe_point,
                safepoint.version()
            );
        }
        Ok(new_safe_point)
    }

    /// Clean up locks in the given key range up to the provided `safepoint`.
    ///
    /// This is primarily intended for GC-like workflows. When `options.async_commit_only` is set,
    /// only async-commit locks are processed (other lock types are ignored).
    pub async fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        debug!(
            "invoking cleanup locks (async_commit_only={})",
            options.async_commit_only
        );
        // scan all locks with ts <= safepoint
        let ctx = self.resolve_locks_ctx.clone();
        let backoff = Backoff::equal_jitter_backoff(100, 10000, 50);
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, options.batch_size);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .preserve_shard()
            .cleanup_locks(ctx.clone(), options, self.keyspace)
            .retry_multi_region(backoff)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    // Note: `batch_size` must be >= expected number of locks.
    pub async fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_scan_lock_request(range, safepoint, batch_size);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Resolves the given locks and returns any that remain live.
    ///
    /// This method retries until either all locks are resolved or the provided
    /// `backoff` is exhausted. The `timestamp` is used as the caller start
    /// timestamp when checking transaction status.
    pub async fn resolve_locks(
        &self,
        locks: Vec<ProtoLockInfo>,
        timestamp: Timestamp,
        backoff: Backoff,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let lock_resolver = self.bound_lock_resolver();
        let live_locks = lock_resolver
            .resolve_locks(
                locks.encode_keyspace(self.keyspace, KeyMode::Txn),
                timestamp,
                backoff,
                false,
            )
            .await?;
        Ok(live_locks.truncate_keyspace(self.keyspace))
    }

    /// Performs a one-shot lock resolve attempt and returns the outcome.
    ///
    /// Unlike [`Client::resolve_locks`], this method does not perform the caller-side sleep loop.
    /// The returned [`ResolveLocksResult::ms_before_txn_expired`] can be used to decide how long
    /// to backoff/sleep before retrying.
    pub async fn resolve_locks_once(
        &self,
        locks: Vec<ProtoLockInfo>,
        timestamp: Timestamp,
        pessimistic_region_resolve: bool,
    ) -> Result<ResolveLocksResult> {
        use crate::request::TruncateKeyspace;

        let lock_resolver = self.bound_lock_resolver();
        let mut resolve_result = lock_resolver
            .resolve_locks_once(
                locks.encode_keyspace(self.keyspace, KeyMode::Txn),
                timestamp,
                pessimistic_region_resolve,
            )
            .await?;
        resolve_result.live_locks = resolve_result.live_locks.truncate_keyspace(self.keyspace);
        Ok(resolve_result)
    }

    /// Resolves locks for read and returns any that remain live.
    ///
    /// This method mirrors client-go `LockResolver.ResolveLocksForRead` and uses a read-optimized
    /// lock-resolve strategy. Non-pessimistic lock cleanup is performed asynchronously in a
    /// background task.
    pub async fn resolve_locks_for_read(
        &self,
        locks: Vec<ProtoLockInfo>,
        timestamp: Timestamp,
        force_resolve_lock_lite: bool,
    ) -> Result<ResolveLocksForReadResult> {
        use crate::request::TruncateKeyspace;

        let lock_resolver = self.bound_lock_resolver();
        let mut resolve_result = lock_resolver
            .resolve_locks_for_read(
                locks.encode_keyspace(self.keyspace, KeyMode::Txn),
                timestamp,
                force_resolve_lock_lite,
            )
            .await?;
        resolve_result.live_locks = resolve_result.live_locks.truncate_keyspace(self.keyspace);
        Ok(resolve_result)
    }

    async fn delete_range_inner(
        &self,
        range: BoundRange,
        notify_only: bool,
        concurrency: usize,
    ) -> Result<usize> {
        if concurrency == 0 {
            return Err(crate::Error::StringError(
                "delete_range concurrency must be greater than 0".to_owned(),
            ));
        }

        let range = range.encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_delete_range_request(range, notify_only);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region_with_concurrency(DEFAULT_REGION_BACKOFF, concurrency)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Delete all versions of all keys in the given key range.
    ///
    /// This is a region-based request (the range can span multiple regions).
    ///
    /// Returns the number of regions affected when successful.
    ///
    /// This mirrors client-go `KVStore.DeleteRange`.
    pub async fn delete_range(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Result<usize> {
        self.delete_range_inner(range.into(), false, concurrency)
            .await
    }

    /// Notify regions in the given key range of an upcoming unsafe-destroy-range operation.
    ///
    /// This sends `DeleteRangeRequest` with `notify_only=true`, which replicates the operation
    /// through Raft without actually deleting the data.
    ///
    /// Returns the number of regions affected when successful.
    pub async fn notify_delete_range(
        &self,
        range: impl Into<BoundRange>,
        concurrency: usize,
    ) -> Result<usize> {
        self.delete_range_inner(range.into(), true, concurrency)
            .await
    }

    /// Prepares flashback for a key range.
    ///
    /// This is a region-based request (the range can span multiple regions). TiKV requires that
    /// each region is prepared (locked) before calling [`Client::flashback_to_version`].
    ///
    /// Returns the number of regions affected when successful.
    pub async fn prepare_flashback_to_version(
        &self,
        range: impl Into<BoundRange>,
        start_ts: u64,
        version: u64,
        concurrency: usize,
    ) -> Result<usize> {
        if concurrency == 0 {
            return Err(crate::Error::StringError(
                "prepare_flashback_to_version concurrency must be greater than 0".to_owned(),
            ));
        }

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_prepare_flashback_to_version_request(range, start_ts, version);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region_with_concurrency(DEFAULT_REGION_BACKOFF, concurrency)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Flashbacks a key range to a specific version.
    ///
    /// This is a region-based request (the range can span multiple regions). TiKV requires that
    /// each region is prepared (locked) via [`Client::prepare_flashback_to_version`] first.
    ///
    /// Returns the number of regions affected when successful.
    pub async fn flashback_to_version(
        &self,
        range: impl Into<BoundRange>,
        version: u64,
        start_ts: u64,
        commit_ts: u64,
        concurrency: usize,
    ) -> Result<usize> {
        if concurrency == 0 {
            return Err(crate::Error::StringError(
                "flashback_to_version concurrency must be greater than 0".to_owned(),
            ));
        }

        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_flashback_to_version_request(range, version, start_ts, commit_ts);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region_with_concurrency(DEFAULT_REGION_BACKOFF, concurrency)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Split regions by the provided split keys.
    ///
    /// When `scatter` is true, this will also ask PD to scatter the resulting regions. When
    /// `table_id` is provided, it is passed as the PD scatter `group` (matching client-go
    /// `WithGroup(fmt.Sprintf("%v", tableID))` behavior).
    ///
    /// Returns the region IDs reported by TiKV, which corresponds to `SplitRegionResponse.regions`
    /// excluding the last region (matching client-go `KVStore.SplitRegions` behavior).
    pub async fn split_regions(
        &self,
        split_keys: impl IntoIterator<Item = impl Into<Key>>,
        scatter: bool,
        table_id: Option<i64>,
    ) -> Result<Vec<u64>> {
        let mut split_keys = split_keys.into_iter().map(Into::into).collect::<Vec<_>>();
        if split_keys.is_empty() {
            return Ok(Vec::new());
        }

        split_keys = split_keys
            .into_iter()
            .map(|key| key.encode_keyspace(self.keyspace, KeyMode::Txn))
            .collect();

        let req = new_split_region_request(split_keys.into_iter(), false);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .retry_multi_region(DEFAULT_REGION_BACKOFF)
            .extract_error()
            .merge(crate::request::Collect)
            .plan();
        let region_ids = plan.execute().await?;

        if scatter && !region_ids.is_empty() {
            let group = table_id.map(|id| id.to_string());
            let _resp = self
                .pd
                .clone()
                .scatter_regions(region_ids.clone(), group)
                .await?;
        }

        Ok(region_ids)
    }

    /// Wait for a scatter-region PD operator to finish for the given region.
    ///
    /// This mirrors client-go `KVStore.WaitScatterRegionFinish`.
    ///
    /// The provided `backoff` controls how long to wait before giving up. When the backoff is
    /// exhausted and the scatter operator is still running, this returns an error.
    pub async fn wait_scatter_region_finish(
        &self,
        region_id: u64,
        mut backoff: Backoff,
    ) -> Result<()> {
        let mut last_err = None;

        loop {
            match self.pd.clone().get_operator(region_id).await {
                Ok(resp) => {
                    let is_scatter_region = resp.desc == b"scatter-region";
                    let is_running =
                        resp.status == crate::proto::pdpb::OperatorStatus::Running as i32;
                    if !is_scatter_region || !is_running {
                        return Ok(());
                    }
                }
                Err(err) => {
                    last_err = Some(err);
                }
            }

            let Some(delay) = backoff.next_delay_duration() else {
                return Err(last_err.unwrap_or_else(|| {
                    crate::Error::StringError("wait scatter region timeout".to_owned())
                }));
            };

            tokio::time::sleep(delay).await;
        }
    }

    /// Check whether a region is currently in scattering (`scatter-region` operator is running).
    ///
    /// This mirrors client-go `KVStore.CheckRegionInScattering`.
    pub async fn check_region_in_scattering(&self, region_id: u64) -> Result<bool> {
        let resp = self.pd.clone().get_operator(region_id).await?;
        Ok(resp.desc == b"scatter-region"
            && resp.status == crate::proto::pdpb::OperatorStatus::Running as i32)
    }

    /// Cleans up all keys in a range and quickly reclaim disk space.
    ///
    /// The range can span over multiple regions.
    ///
    /// Note that the request will directly delete data from RocksDB, and all MVCC will be erased.
    ///
    /// This interface is intended for special scenarios that resemble operations like "drop table" or "drop database" in TiDB.
    pub async fn unsafe_destroy_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_unsafe_destroy_range_request(range);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Compact a specified key range on TiFlash stores.
    ///
    /// This is a store-level request (not tied to a specific region). Each store compacts its
    /// local data only. The request is only sent to TiFlash stores (filtered from PD store
    /// metadata).
    ///
    /// `start_key` controls where compaction starts. Pass [`Key::EMPTY`] to start from the
    /// beginning. For incremental compaction, call again using the `compacted_end_key` returned in
    /// each [`ProtoCompactResponse`].
    ///
    /// Returns one [`ProtoCompactResponse`] per TiFlash store.
    ///
    /// Returns an error if any store request fails.
    pub async fn compact(
        &self,
        physical_table_id: i64,
        logical_table_id: i64,
        start_key: impl Into<Key>,
    ) -> Result<Vec<ProtoCompactResponse>> {
        use crate::request::TruncateKeyspace;

        let start_key = start_key.into();
        let keyspace_id = match self.keyspace {
            Keyspace::Enable { keyspace_id } => keyspace_id,
            _ => 0,
        };

        let req = new_compact_request(start_key, physical_table_id, logical_table_id, keyspace_id)
            .encode_keyspace(self.keyspace, KeyMode::Txn);

        let stores = self.pd.all_stores_for_safe_ts().await?;
        let stores = stores
            .into_iter()
            .filter(|store| crate::region_cache::is_tiflash_store(&store.meta))
            .collect::<Vec<_>>();

        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .stores(stores, DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();

        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Get system table data from TiFlash stores.
    ///
    /// This is a store-level request (not tied to a specific region). The request is only sent to
    /// TiFlash stores (filtered from PD store metadata).
    ///
    /// Returns one [`ProtoTiFlashSystemTableResponse`] per TiFlash store.
    pub async fn tiflash_system_table(
        &self,
        sql: impl Into<String>,
    ) -> Result<Vec<ProtoTiFlashSystemTableResponse>> {
        let stores = self.pd.all_stores_for_safe_ts().await?;
        let stores = stores
            .into_iter()
            .filter(|store| crate::region_cache::is_tiflash_store(&store.meta))
            .collect::<Vec<_>>();

        let req = new_get_ti_flash_system_table_request(sql.into());
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .stores(stores, DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Register a lock observer on all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region).
    ///
    /// Returns an error if any store request fails.
    pub async fn register_lock_observer(&self, max_ts: u64) -> Result<()> {
        let req = new_register_lock_observer_request(max_ts);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Check a lock observer on all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region).
    ///
    /// Returns `(is_clean, locks)` where `is_clean` is true only if all stores report clean.
    ///
    /// Returns an error if any store request fails.
    pub async fn check_lock_observer(&self, max_ts: u64) -> Result<(bool, Vec<ProtoLockInfo>)> {
        use crate::request::TruncateKeyspace;

        let req = new_check_lock_observer_request(max_ts);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        let (is_clean, locks) = plan.execute().await?;
        Ok((is_clean, locks.truncate_keyspace(self.keyspace)))
    }

    /// Remove a lock observer on all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region).
    ///
    /// Returns an error if any store request fails.
    pub async fn remove_lock_observer(&self, max_ts: u64) -> Result<()> {
        let req = new_remove_lock_observer_request(max_ts);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        plan.execute().await
    }

    /// Physical scan locks from all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region). The returned locks are
    /// concatenated across all stores.
    ///
    /// Returns an error if any store request fails.
    pub async fn physical_scan_lock(
        &self,
        max_ts: u64,
        start_key: impl Into<Key>,
        limit: u32,
    ) -> Result<Vec<ProtoLockInfo>> {
        use crate::request::TruncateKeyspace;

        let start_key = start_key
            .into()
            .encode_keyspace(self.keyspace, KeyMode::Txn);
        let req = new_physical_scan_lock_request(max_ts, start_key, limit);
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Get current lock waiting status from all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region). The returned entries are a
    /// snapshot assembled by querying all stores and concatenating their results.
    ///
    /// Returns an error if any store request fails.
    pub async fn lock_wait_info(&self) -> Result<Vec<ProtoWaitForEntry>> {
        use crate::request::TruncateKeyspace;

        let req = new_get_lock_wait_info_request();
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    /// Get lock waiting history from all TiKV stores.
    ///
    /// This is a store-level request (not tied to a specific region). The returned entries are a
    /// snapshot assembled by querying all stores and concatenating their results.
    ///
    /// Returns an error if any store request fails.
    pub async fn lock_wait_history(&self) -> Result<Vec<ProtoWaitForEntry>> {
        use crate::request::TruncateKeyspace;

        let req = new_get_lock_wait_history_request();
        let plan = crate::request::PlanBuilder::new(self.pd.clone(), self.keyspace, req)
            .all_stores(DEFAULT_STORE_BACKOFF)
            .merge(crate::request::Collect)
            .plan();
        Ok(plan.execute().await?.truncate_keyspace(self.keyspace))
    }

    fn new_transaction(
        &self,
        timestamp: Timestamp,
        options: TransactionOptions,
    ) -> Transaction<PdC> {
        Transaction::new_with_resolve_locks_ctx(
            timestamp,
            self.pd.clone(),
            options,
            self.keyspace,
            self.resolve_locks_ctx.clone(),
            self.gc_safe_point.clone(),
            self.txn_latches.clone(),
        )
    }
}

impl Client {
    /// Create a new [`Snapshot`](Snapshot) at the given [`Timestamp`](Timestamp).
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> Snapshot {
        debug!("creating new snapshot");
        Snapshot::new(self.new_transaction(timestamp, options.read_only()))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::Instant;

    use crate::gc_safe_point::GcSafePointCache;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::errorpb;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::request::Keyspace;
    use crate::safe_ts::SafeTsCache;
    use crate::timestamp::TimestampExt;
    use crate::transaction::lock::ResolveLocksOptions;
    use crate::transaction::DeleteRangeTask;
    use crate::transaction::HeartbeatOption;
    use crate::transaction::ResolveLocksContext;
    use crate::Backoff;
    use crate::Error;
    use crate::Timestamp;
    use crate::TransactionOptions;

    use super::Client;

    #[test]
    fn test_pd_client_accessor_returns_inner_pd_handle() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert!(Arc::ptr_eq(&client.pd_client(), &pd_client));
    }

    #[tokio::test]
    async fn test_begin_with_start_timestamp_uses_provided_start_ts_without_pd_tso() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let start_ts = Timestamp {
            physical: 0,
            logical: 42,
            ..Default::default()
        };
        let start_version = start_ts.version();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, start_version);

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut transaction = client.begin_with_start_timestamp(
            start_ts,
            TransactionOptions::new_optimistic().drop_check(crate::CheckLevel::None),
        );
        assert_eq!(pd_client.get_timestamp_call_count(), 0);

        let value = transaction.get("k".to_owned()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        assert_eq!(pd_client.get_timestamp_call_count(), 0);
        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_lock_wait_info_and_history_collect_entries_from_all_stores() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_key_raw = b"wait-key".to_vec();
        let expected_key_encoded: Vec<u8> = crate::Key::from(expected_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let info_calls = Arc::new(AtomicUsize::new(0));
        let history_calls = Arc::new(AtomicUsize::new(0));
        let info_calls_cloned = info_calls.clone();
        let history_calls_cloned = history_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::GetLockWaitInfoRequest>()
                    .is_some()
                {
                    let call_index = info_calls_cloned.fetch_add(1, Ordering::SeqCst) as u64;
                    let entry = crate::proto::deadlock::WaitForEntry {
                        txn: call_index + 1,
                        wait_for_txn: 42,
                        key_hash: 0,
                        key: expected_key_encoded.clone(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    };
                    return Ok(Box::new(kvrpcpb::GetLockWaitInfoResponse {
                        region_error: None,
                        error: "".to_owned(),
                        entries: vec![entry],
                    }) as Box<dyn Any>);
                }

                if req
                    .downcast_ref::<kvrpcpb::GetLockWaitHistoryRequest>()
                    .is_some()
                {
                    let call_index = history_calls_cloned.fetch_add(1, Ordering::SeqCst) as u64;
                    let entry = crate::proto::deadlock::WaitForEntry {
                        txn: call_index + 101,
                        wait_for_txn: 42,
                        key_hash: 0,
                        key: expected_key_encoded.clone(),
                        resource_group_tag: Vec::new(),
                        wait_time: 0,
                    };
                    return Ok(Box::new(kvrpcpb::GetLockWaitHistoryResponse {
                        region_error: None,
                        error: "".to_owned(),
                        entries: vec![entry],
                    }) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut info = client.lock_wait_info().await.unwrap();
        info.sort_by_key(|e| e.txn);
        assert_eq!(info.len(), 2);
        assert_eq!(info[0].txn, 1);
        assert_eq!(info[1].txn, 2);
        for entry in &info {
            assert_eq!(entry.key, expected_key_raw);
        }
        assert_eq!(info_calls.load(Ordering::SeqCst), 2);

        let mut history = client.lock_wait_history().await.unwrap();
        history.sort_by_key(|e| e.txn);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].txn, 101);
        assert_eq!(history[1].txn, 102);
        for entry in &history {
            assert_eq!(entry.key, expected_key_raw);
        }
        assert_eq!(history_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_lock_observer_and_physical_scan_lock_apis_truncate_keyspace() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };

        let start_key = crate::Key::from(b"start".to_vec());
        let expected_start_key: Vec<u8> = start_key
            .clone()
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let expected_observer_lock_key_raw = b"lock-key".to_vec();
        let expected_observer_lock_key_encoded: Vec<u8> =
            crate::Key::from(expected_observer_lock_key_raw.clone())
                .encode_keyspace(keyspace, KeyMode::Txn)
                .into();

        let register_calls = Arc::new(AtomicUsize::new(0));
        let check_calls = Arc::new(AtomicUsize::new(0));
        let remove_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let register_calls_cloned = register_calls.clone();
        let check_calls_cloned = check_calls.clone();
        let remove_calls_cloned = remove_calls.clone();
        let scan_calls_cloned = scan_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::RegisterLockObserverRequest>() {
                    assert_eq!(req.max_ts, 1);
                    register_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RegisterLockObserverResponse {
                        error: "".to_owned(),
                    }) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckLockObserverRequest>() {
                    assert_eq!(req.max_ts, 2);
                    let call_index = check_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    let is_clean = call_index == 0;
                    let locks = if is_clean {
                        vec![]
                    } else {
                        vec![kvrpcpb::LockInfo {
                            key: expected_observer_lock_key_encoded.clone(),
                            primary_lock: expected_observer_lock_key_encoded.clone(),
                            ..Default::default()
                        }]
                    };
                    return Ok(Box::new(kvrpcpb::CheckLockObserverResponse {
                        error: "".to_owned(),
                        is_clean,
                        locks,
                    }) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::RemoveLockObserverRequest>() {
                    assert_eq!(req.max_ts, 3);
                    remove_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::RemoveLockObserverResponse {
                        error: "".to_owned(),
                    }) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::PhysicalScanLockRequest>() {
                    assert_eq!(req.max_ts, 4);
                    assert_eq!(req.start_key, expected_start_key);
                    assert_eq!(req.limit, 123);

                    let call_index = scan_calls_cloned.fetch_add(1, Ordering::SeqCst) as u64;
                    let key_raw = format!("scan-{call_index}").into_bytes();
                    let key_encoded: Vec<u8> = crate::Key::from(key_raw.clone())
                        .encode_keyspace(keyspace, KeyMode::Txn)
                        .into();
                    let lock = kvrpcpb::LockInfo {
                        key: key_encoded.clone(),
                        primary_lock: key_encoded,
                        ..Default::default()
                    };

                    return Ok(Box::new(kvrpcpb::PhysicalScanLockResponse {
                        error: "".to_owned(),
                        locks: vec![lock],
                    }) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.register_lock_observer(1).await.unwrap();
        assert_eq!(register_calls.load(Ordering::SeqCst), 2);

        let (is_clean, locks) = client.check_lock_observer(2).await.unwrap();
        assert!(!is_clean);
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].key, expected_observer_lock_key_raw);
        assert_eq!(locks[0].primary_lock, expected_observer_lock_key_raw);
        assert_eq!(check_calls.load(Ordering::SeqCst), 2);

        client.remove_lock_observer(3).await.unwrap();
        assert_eq!(remove_calls.load(Ordering::SeqCst), 2);

        let mut locks = client.physical_scan_lock(4, start_key, 123).await.unwrap();
        locks.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(locks.len(), 2);
        assert!(locks.iter().all(|lock| lock.key.starts_with(b"scan-")));
        assert_eq!(scan_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_delete_range_apis_encode_keyspace_and_set_notify_only() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let start_key_raw = b"begin".to_vec();
        let end_key_raw = b"end".to_vec();
        let expected_start_key_encoded: Vec<u8> = crate::Key::from(start_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();
        let expected_end_key_encoded: Vec<u8> = crate::Key::from(end_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let delete_range_calls = Arc::new(AtomicUsize::new(0));
        let delete_range_calls_cloned = delete_range_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::DeleteRangeRequest = req.downcast_ref().unwrap();
                let call_index = delete_range_calls_cloned.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_key, expected_start_key_encoded);
                assert_eq!(req.end_key, expected_end_key_encoded);
                assert_eq!(req.notify_only, call_index == 1);

                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.api_version, expected_api_version);
                assert_eq!(ctx.keyspace_id, 0xCAFE);

                Ok(Box::new(kvrpcpb::DeleteRangeResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert_eq!(
            client
                .delete_range(start_key_raw.clone()..end_key_raw.clone(), 4)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            client
                .notify_delete_range(start_key_raw..end_key_raw, 4)
                .await
                .unwrap(),
            1
        );
        assert_eq!(delete_range_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_delete_range_splits_range_across_regions_and_returns_completed_count() {
        let keyspace = Keyspace::Disable;

        let seen_ranges = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
        let seen_ranges_cloned = seen_ranges.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::DeleteRangeRequest = req.downcast_ref().unwrap();
                assert!(!req.notify_only);
                seen_ranges_cloned
                    .lock()
                    .unwrap()
                    .push((req.start_key.clone(), req.end_key.clone()));

                Ok(Box::new(kvrpcpb::DeleteRangeResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let completed = client.delete_range(vec![1]..vec![11], 2).await.unwrap();
        assert_eq!(completed, 2);

        let mut seen_ranges = seen_ranges.lock().unwrap().clone();
        seen_ranges.sort();
        assert_eq!(seen_ranges, vec![(vec![1], vec![10]), (vec![10], vec![11])]);
    }

    #[tokio::test]
    async fn test_delete_range_rejects_zero_concurrency() {
        let keyspace = Keyspace::Disable;

        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_cloned = dispatch_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_cloned.fetch_add(1, Ordering::SeqCst);
                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client.delete_range(vec![1]..vec![2], 0).await.unwrap_err();
        assert!(matches!(
            err,
            crate::Error::StringError(msg) if msg.contains("concurrency must be greater than 0")
        ));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_delete_range_task_executes_delete_range_and_reports_completed_regions() {
        let keyspace = Keyspace::Disable;

        let seen_ranges = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
        let seen_ranges_cloned = seen_ranges.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::DeleteRangeRequest = req.downcast_ref().unwrap();
                assert!(!req.notify_only);
                seen_ranges_cloned
                    .lock()
                    .unwrap()
                    .push((req.start_key.clone(), req.end_key.clone()));

                Ok(Box::new(kvrpcpb::DeleteRangeResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut task = DeleteRangeTask::new(client, vec![1]..vec![11], 2);
        task.execute().await.unwrap();
        assert_eq!(task.completed_regions(), 2);

        let mut seen_ranges = seen_ranges.lock().unwrap().clone();
        seen_ranges.sort();
        assert_eq!(seen_ranges, vec![(vec![1], vec![10]), (vec![10], vec![11])]);
    }

    #[tokio::test]
    async fn test_delete_range_task_notify_only_sends_notify_requests() {
        let keyspace = Keyspace::Disable;

        let seen_notify_only = Arc::new(AtomicUsize::new(0));
        let seen_notify_only_cloned = seen_notify_only.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::DeleteRangeRequest = req.downcast_ref().unwrap();
                if req.notify_only {
                    seen_notify_only_cloned.fetch_add(1, Ordering::SeqCst);
                }
                Ok(Box::new(kvrpcpb::DeleteRangeResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut task = DeleteRangeTask::new_notify(client, vec![1]..vec![11], 2);
        task.execute().await.unwrap();
        assert_eq!(task.completed_regions(), 2);
        assert_eq!(seen_notify_only.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_delete_range_task_reports_completed_regions_on_error() {
        let keyspace = Keyspace::Disable;

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::DeleteRangeRequest = req.downcast_ref().unwrap();
                if req.start_key == vec![10] {
                    return Err(Error::StringError("injected delete range error".to_owned()));
                }
                Ok(Box::new(kvrpcpb::DeleteRangeResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut task = DeleteRangeTask::new(client, vec![1]..vec![11], 1);
        let err = task.execute().await.unwrap_err();
        assert_eq!(task.completed_regions(), 1);
        match err {
            Error::StringError(message) => {
                assert!(message.contains("range task runner delete-range failed"));
                assert!(message.contains("injected delete range error"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_prepare_flashback_to_version_encodes_keyspace_and_sets_fields() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let start_ts = 42;
        let version = 123;

        let start_key_raw = b"begin".to_vec();
        let end_key_raw = b"end".to_vec();
        let expected_start_key_encoded: Vec<u8> = crate::Key::from(start_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();
        let expected_end_key_encoded: Vec<u8> = crate::Key::from(end_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::PrepareFlashbackToVersionRequest = req.downcast_ref().unwrap();
                assert_eq!(req.start_key, expected_start_key_encoded);
                assert_eq!(req.end_key, expected_end_key_encoded);
                assert_eq!(req.start_ts, start_ts);
                assert_eq!(req.version, version);

                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.api_version, expected_api_version);
                assert_eq!(ctx.keyspace_id, 0xCAFE);

                Ok(Box::new(kvrpcpb::PrepareFlashbackToVersionResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert_eq!(
            client
                .prepare_flashback_to_version(start_key_raw..end_key_raw, start_ts, version, 4)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_flashback_to_version_encodes_keyspace_and_sets_fields() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let start_ts = 42;
        let commit_ts = 87;
        let version = 123;

        let start_key_raw = b"begin".to_vec();
        let end_key_raw = b"end".to_vec();
        let expected_start_key_encoded: Vec<u8> = crate::Key::from(start_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();
        let expected_end_key_encoded: Vec<u8> = crate::Key::from(end_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::FlashbackToVersionRequest = req.downcast_ref().unwrap();
                assert_eq!(req.start_key, expected_start_key_encoded);
                assert_eq!(req.end_key, expected_end_key_encoded);
                assert_eq!(req.start_ts, start_ts);
                assert_eq!(req.commit_ts, commit_ts);
                assert_eq!(req.version, version);

                let ctx = req.context.as_ref().expect("context should be set");
                assert_eq!(ctx.api_version, expected_api_version);
                assert_eq!(ctx.keyspace_id, 0xCAFE);

                Ok(Box::new(kvrpcpb::FlashbackToVersionResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert_eq!(
            client
                .flashback_to_version(start_key_raw..end_key_raw, version, start_ts, commit_ts, 4)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_flashback_to_version_splits_range_across_regions_and_returns_completed_count() {
        let keyspace = Keyspace::Disable;

        let seen_ranges = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
        let seen_ranges_cloned = seen_ranges.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req: &kvrpcpb::FlashbackToVersionRequest = req.downcast_ref().unwrap();
                seen_ranges_cloned
                    .lock()
                    .unwrap()
                    .push((req.start_key.clone(), req.end_key.clone()));

                Ok(Box::new(kvrpcpb::FlashbackToVersionResponse {
                    region_error: None,
                    error: "".to_owned(),
                }) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let completed = client
            .flashback_to_version(vec![1]..vec![11], 123, 42, 87, 2)
            .await
            .unwrap();
        assert_eq!(completed, 2);

        let mut seen_ranges = seen_ranges.lock().unwrap().clone();
        seen_ranges.sort();
        assert_eq!(seen_ranges, vec![(vec![1], vec![10]), (vec![10], vec![11])]);
    }

    #[tokio::test]
    async fn test_flashback_to_version_rejects_zero_concurrency() {
        let keyspace = Keyspace::Disable;

        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let dispatch_calls_cloned = dispatch_calls.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_req: &dyn Any| {
                dispatch_calls_cloned.fetch_add(1, Ordering::SeqCst);
                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .flashback_to_version(vec![1]..vec![2], 123, 42, 87, 0)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::StringError(msg) if msg.contains("concurrency must be greater than 0")
        ));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_compact_api_encodes_start_key_and_truncates_response_keys() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;
        use crate::request::Keyspace;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let start_key_raw = b"start".to_vec();
        let expected_start_key_encoded: Vec<u8> = crate::Key::from(start_key_raw.clone())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let expected_compacted_start_key_raw = b"compacted-start".to_vec();
        let expected_compacted_end_key_raw = b"compacted-end".to_vec();
        let expected_compacted_start_key_encoded: Vec<u8> =
            crate::Key::from(expected_compacted_start_key_raw.clone())
                .encode_keyspace(keyspace, KeyMode::Txn)
                .into();
        let expected_compacted_end_key_encoded: Vec<u8> =
            crate::Key::from(expected_compacted_end_key_raw.clone())
                .encode_keyspace(keyspace, KeyMode::Txn)
                .into();

        let compact_calls = Arc::new(AtomicUsize::new(0));
        let compact_calls_cloned = compact_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::CompactRequest>() else {
                    return Err(crate::Error::Unimplemented);
                };

                assert_eq!(req.physical_table_id, 101);
                assert_eq!(req.logical_table_id, 202);
                assert_eq!(req.api_version, expected_api_version);
                assert_eq!(req.keyspace_id, 0xCAFE);
                assert_eq!(req.start_key, expected_start_key_encoded);

                compact_calls_cloned.fetch_add(1, Ordering::SeqCst);

                Ok(Box::new(kvrpcpb::CompactResponse {
                    error: None,
                    has_remaining: true,
                    compacted_start_key: expected_compacted_start_key_encoded.clone(),
                    compacted_end_key: expected_compacted_end_key_encoded.clone(),
                }) as Box<dyn Any>)
            },
        )));

        let tiflash_label = metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        };

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                labels: vec![tiflash_label.clone()],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                labels: vec![tiflash_label],
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let res = client.compact(101, 202, start_key_raw).await.unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(compact_calls.load(Ordering::SeqCst), 2);

        for resp in &res {
            assert!(resp.error.is_none());
            assert!(resp.has_remaining);
            assert_eq!(resp.compacted_start_key, expected_compacted_start_key_raw);
            assert_eq!(resp.compacted_end_key, expected_compacted_end_key_raw);
        }
    }

    #[tokio::test]
    async fn test_compact_api_keeps_empty_start_key_unencoded() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;
        use crate::request::Keyspace;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let expected_compacted_start_key_raw = b"compacted-start".to_vec();
        let expected_compacted_end_key_raw = b"compacted-end".to_vec();
        let expected_compacted_start_key_encoded: Vec<u8> =
            crate::Key::from(expected_compacted_start_key_raw.clone())
                .encode_keyspace(keyspace, KeyMode::Txn)
                .into();
        let expected_compacted_end_key_encoded: Vec<u8> =
            crate::Key::from(expected_compacted_end_key_raw.clone())
                .encode_keyspace(keyspace, KeyMode::Txn)
                .into();

        let compact_calls = Arc::new(AtomicUsize::new(0));
        let compact_calls_cloned = compact_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::CompactRequest>() else {
                    return Err(crate::Error::Unimplemented);
                };

                assert_eq!(req.physical_table_id, 303);
                assert_eq!(req.logical_table_id, 404);
                assert_eq!(req.api_version, expected_api_version);
                assert_eq!(req.keyspace_id, 0xCAFE);
                assert!(req.start_key.is_empty());

                compact_calls_cloned.fetch_add(1, Ordering::SeqCst);

                Ok(Box::new(kvrpcpb::CompactResponse {
                    error: None,
                    has_remaining: false,
                    compacted_start_key: expected_compacted_start_key_encoded.clone(),
                    compacted_end_key: expected_compacted_end_key_encoded.clone(),
                }) as Box<dyn Any>)
            },
        )));

        let tiflash_label = metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        };

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                labels: vec![tiflash_label.clone()],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                labels: vec![tiflash_label],
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let res = client.compact(303, 404, crate::Key::EMPTY).await.unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(compact_calls.load(Ordering::SeqCst), 2);

        for resp in &res {
            assert!(resp.error.is_none());
            assert!(!resp.has_remaining);
            assert_eq!(resp.compacted_start_key, expected_compacted_start_key_raw);
            assert_eq!(resp.compacted_end_key, expected_compacted_end_key_raw);
        }
    }

    #[tokio::test]
    async fn test_compact_returns_empty_when_no_tiflash_stores() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cloned = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::CompactRequest>().is_some() {
                    calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CompactResponse::default()) as Box<dyn Any>);
                }
                Err(crate::Error::Unimplemented)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let res = client.compact(1, 2, crate::Key::EMPTY).await.unwrap();
        assert!(res.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_tiflash_system_table_returns_empty_when_no_tiflash_stores() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cloned = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req
                    .downcast_ref::<kvrpcpb::TiFlashSystemTableRequest>()
                    .is_some()
                {
                    calls_cloned.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::TiFlashSystemTableResponse {
                        data: b"unexpected".to_vec(),
                    }) as Box<dyn Any>);
                }
                Err(crate::Error::Unimplemented)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let res = client.tiflash_system_table("select 1").await.unwrap();
        assert!(res.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_tiflash_system_table_queries_all_tiflash_stores() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_cloned = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let Some(req) = req.downcast_ref::<kvrpcpb::TiFlashSystemTableRequest>() else {
                    return Err(crate::Error::Unimplemented);
                };
                assert_eq!(req.sql, "select 42");

                let idx = calls_cloned.fetch_add(1, Ordering::SeqCst) as u8;
                Ok(
                    Box::new(kvrpcpb::TiFlashSystemTableResponse { data: vec![idx] })
                        as Box<dyn Any>,
                )
            },
        )));

        let tiflash_label = metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        };

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                address: "mock://1".to_owned(),
                labels: vec![tiflash_label.clone()],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                address: "mock://2".to_owned(),
                labels: vec![tiflash_label],
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let res = client.tiflash_system_table("select 42").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(res.len(), 2);

        let mut results = res.into_iter().map(|r| r.data).collect::<Vec<_>>();
        results.sort();
        assert_eq!(results, vec![vec![0], vec![1]]);
    }

    #[tokio::test]
    async fn test_current_timestamp_with_txn_scope_maps_global_and_empty_to_global_dc_location() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let _ts = client.current_timestamp_with_txn_scope("").await.unwrap();
        let _ts = client
            .current_timestamp_with_txn_scope("global")
            .await
            .unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["".to_owned(), "".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_current_all_tso_keyspace_group_min_ts_delegates_to_pd_get_min_ts() {
        let min_version = 123u64 << 18;
        let pd_client = Arc::new(MockPdClient::default().with_min_ts_version(min_version));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let ts = client
            .current_all_tso_keyspace_group_min_ts()
            .await
            .unwrap();
        assert_eq!(pd_client.get_min_ts_call_count(), 1);
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
        assert_eq!(ts.version(), min_version);
    }

    #[tokio::test]
    async fn test_current_all_tso_keyspace_group_min_ts_does_not_record_last_tso() {
        let start_version = 10_000u64 << 18;
        let min_version = 123u64 << 18;
        let pd_client = Arc::new(
            MockPdClient::default()
                .with_tso_sequence(start_version)
                .with_min_ts_version(min_version),
        );
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let _ = client
            .current_all_tso_keyspace_group_min_ts()
            .await
            .unwrap();
        let _ = client.stale_timestamp(5).await.unwrap();

        assert_eq!(pd_client.get_min_ts_call_count(), 1);
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
    }

    #[tokio::test]
    async fn test_external_timestamp_delegates_to_pd_client() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.set_external_timestamp(42).await.unwrap();
        let ts = client.external_timestamp().await.unwrap();
        assert_eq!(ts, 42);

        assert_eq!(pd_client.set_external_timestamp_call_count(), 1);
        assert_eq!(pd_client.get_external_timestamp_call_count(), 1);
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_validate_read_ts_allows_latest_for_non_stale_read() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.validate_read_ts(u64::MAX, false).await.unwrap();
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_validate_read_ts_rejects_latest_for_stale_read() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .validate_read_ts(u64::MAX, true)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("max uint64"));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_validate_read_ts_uses_cached_last_tso_without_fetching_pd_timestamp() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let cached = Timestamp {
            physical: 10_000,
            logical: 7,
            ..Default::default()
        };
        let cached_version = cached.version();
        {
            let mut last_tsos = client.last_tsos.write().await;
            last_tsos.insert(
                String::new(),
                super::LastTso {
                    tso: cached,
                    arrival: Instant::now(),
                },
            );
        }

        client
            .validate_read_ts(cached_version, false)
            .await
            .unwrap();
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_validate_read_ts_rejects_future_ts() {
        let start_version = 10_000u64 << 18;
        let pd_client = Arc::new(MockPdClient::default().with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .validate_read_ts(start_version + 1, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("future time"));
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
    }

    #[test]
    fn test_set_low_resolution_timestamp_update_interval_rejects_zero() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .set_low_resolution_timestamp_update_interval(Duration::ZERO)
            .unwrap_err()
            .to_string();
        assert!(err.contains("updateInterval must be > 0"));
    }

    #[tokio::test]
    async fn test_low_resolution_timestamp_uses_cached_last_tso_without_fetching_pd_timestamp() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };
        client
            .set_low_resolution_timestamp_update_interval(Duration::from_secs(10))
            .unwrap();

        let cached = Timestamp {
            physical: 10_000,
            logical: 7,
            ..Default::default()
        };
        let cached_version = cached.version();
        {
            let mut last_tsos = client.last_tsos.write().await;
            last_tsos.insert(
                String::new(),
                super::LastTso {
                    tso: cached,
                    arrival: Instant::now(),
                },
            );
        }

        let ts = client.low_resolution_timestamp().await.unwrap();
        assert_eq!(ts.version(), cached_version);
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_low_resolution_timestamp_refreshes_when_cache_stale() {
        let start_version = 10_000u64 << 18;
        let pd_client = Arc::new(MockPdClient::default().with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };
        client
            .set_low_resolution_timestamp_update_interval(Duration::from_secs(1))
            .unwrap();

        {
            let mut last_tsos = client.last_tsos.write().await;
            last_tsos.insert(
                String::new(),
                super::LastTso {
                    tso: Timestamp {
                        physical: 10_000,
                        logical: 7,
                        ..Default::default()
                    },
                    arrival: Instant::now() - Duration::from_secs(10),
                },
            );
        }

        let ts = client.low_resolution_timestamp().await.unwrap();
        assert_eq!(ts.version(), start_version);
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
    }

    #[tokio::test]
    async fn test_stale_timestamp_uses_cached_last_tso_without_fetching_pd_timestamp() {
        let pd_client = Arc::new(MockPdClient::default());
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        {
            let mut last_tsos = client.last_tsos.write().await;
            last_tsos.insert(
                String::new(),
                super::LastTso {
                    tso: Timestamp {
                        physical: 10_000,
                        logical: 7,
                        ..Default::default()
                    },
                    arrival: Instant::now() - Duration::from_secs(10),
                },
            );
        }

        let stale = client.stale_timestamp(5).await.unwrap();
        assert_eq!(stale.logical, 0);
        assert!(
            (14_500..=15_500).contains(&stale.physical),
            "unexpected stale physical {}",
            stale.physical
        );
        assert_eq!(
            pd_client.get_timestamp_call_count(),
            0,
            "stale_timestamp should not fetch PD TSO when cached"
        );
    }

    #[tokio::test]
    async fn test_stale_timestamp_fetches_current_timestamp_when_cache_empty() {
        let start_version = 10_000u64 << 18;
        let pd_client = Arc::new(MockPdClient::default().with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let stale = client.stale_timestamp(5).await.unwrap();
        assert_eq!(stale.logical, 0);
        assert!(
            (4_500..=5_500).contains(&stale.physical),
            "unexpected stale physical {}",
            stale.physical
        );
        assert_eq!(pd_client.get_timestamp_call_count(), 1);

        let _stale2 = client.stale_timestamp(5).await.unwrap();
        assert_eq!(
            pd_client.get_timestamp_call_count(),
            1,
            "stale_timestamp should reuse cached last tso"
        );
    }

    #[tokio::test]
    async fn test_begin_with_txn_scope_uses_pd_dc_location() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let get_calls_cloned = get_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    get_calls_cloned.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.version, 0);

                    let mut resp = kvrpcpb::GetResponse::default();
                    resp.value = b"v".to_vec();
                    resp.not_found = false;
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut transaction = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic().drop_check(crate::CheckLevel::None),
            )
            .await
            .unwrap();
        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned()]
        );

        let value = transaction.get("k".to_owned()).await.unwrap();
        assert_eq!(value, Some(b"v".to_vec()));

        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_begin_with_options_respects_txn_scope() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let _txn = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .txn_scope("dc1")
                    .drop_check(crate::CheckLevel::None),
            )
            .await
            .unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 1);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_begin_with_options_rejects_pipelined_with_async_commit_or_one_pc() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .pipelined()
                    .use_async_commit()
                    .drop_check(crate::CheckLevel::None),
            )
            .await
            .err()
            .expect("pipelined txn should reject async-commit");
        assert!(matches!(
            err,
            Error::StringError(msg)
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);

        let err = client
            .begin_with_options(
                TransactionOptions::new_optimistic()
                    .pipelined()
                    .try_one_pc()
                    .drop_check(crate::CheckLevel::None),
            )
            .await
            .err()
            .expect("pipelined txn should reject 1pc");
        assert!(matches!(
            err,
            Error::StringError(msg)
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_begin_with_options_rejects_pipelined_pessimistic() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .begin_with_options(
                TransactionOptions::new_pessimistic()
                    .pipelined()
                    .drop_check(crate::CheckLevel::None),
            )
            .await
            .err()
            .expect("pipelined txn should reject pessimistic mode");
        assert!(matches!(
            err,
            Error::StringError(msg) if msg == "pipelined txn does not support pessimistic mode"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_begin_with_start_timestamp_rejects_pipelined_with_async_commit_or_one_pc() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut txn = client.begin_with_start_timestamp(
            Timestamp::from_version(5),
            TransactionOptions::new_optimistic()
                .pipelined()
                .use_async_commit()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
        );
        assert!(matches!(
            txn.commit().await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);

        let mut txn = client.begin_with_start_timestamp(
            Timestamp::from_version(5),
            TransactionOptions::new_optimistic()
                .pipelined()
                .try_one_pc()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
        );
        assert!(matches!(
            txn.commit().await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support async-commit or 1pc"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_begin_with_start_timestamp_rejects_pipelined_pessimistic() {
        let pd_client = Arc::new(MockPdClient::default());

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut txn = client.begin_with_start_timestamp(
            Timestamp::from_version(5),
            TransactionOptions::new_pessimistic()
                .pipelined()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
        );
        assert!(matches!(
            txn.commit().await,
            Err(Error::StringError(msg))
                if msg == "pipelined txn does not support pessimistic mode"
        ));
        assert_eq!(pd_client.get_timestamp_call_count(), 0);
    }

    #[tokio::test]
    async fn test_begin_with_txn_scope_propagates_to_commit_tso_requests() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_min_safe_ts_with_txn_scope_filters_stores_by_zone_label() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_captured = calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let req = req
                    .downcast_ref::<kvrpcpb::StoreSafeTsRequest>()
                    .expect("expected store safe-ts request");
                let range = req.key_range.as_ref().expect("expected key_range");
                assert!(range.start_key.is_empty());
                assert!(range.end_key.is_empty());

                calls_captured.fetch_add(1, Ordering::SeqCst);
                let resp = kvrpcpb::StoreSafeTsResponse { safe_ts: 42 };
                Ok(Box::new(resp) as Box<dyn Any>)
            },
        )));

        pd_client
            .insert_store_meta(metapb::Store {
                id: 1,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "dc1".to_owned(),
                }],
                ..Default::default()
            })
            .await;
        pd_client
            .insert_store_meta(metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "dc2".to_owned(),
                }],
                ..Default::default()
            })
            .await;

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client,
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert_eq!(client.min_safe_ts_with_txn_scope("dc1").await.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(client.min_safe_ts_with_txn_scope("dc2").await.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(client.min_safe_ts_with_txn_scope("dc3").await.unwrap(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        assert_eq!(
            client.min_safe_ts_with_txn_scope("global").await.unwrap(),
            42
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_local_txn_scope_disables_async_commit_and_one_pc() {
        let prewrite_count = Arc::new(AtomicUsize::new(0));
        let commit_count = Arc::new(AtomicUsize::new(0));

        let prewrite_count_captured = prewrite_count.clone();
        let commit_count_captured = commit_count.clone();

        let start_version = 7;
        let commit_version = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                prewrite_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert!(
                    !req.use_async_commit,
                    "local scope must not use async-commit"
                );
                assert!(!req.try_one_pc, "local scope must not use 1pc");
                assert_eq!(
                    req.min_commit_ts,
                    start_version.saturating_add(1),
                    "local scope should set min_commit_ts from start_ts"
                );
                assert_eq!(
                    req.max_commit_ts, 0,
                    "local scope must not set max_commit_ts"
                );
                return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
            }

            if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                commit_count_captured.fetch_add(1, Ordering::SeqCst);
                assert_eq!(req.start_version, start_version);
                assert_eq!(req.commit_version, commit_version);
                return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_optimistic()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.set_enable_async_commit(true);
        txn.set_enable_one_pc(true);
        txn.put("k".to_owned(), "v".to_owned()).await.unwrap();

        let commit_ts = txn.commit().await.unwrap().expect("expected commit ts");
        assert_eq!(commit_ts.version(), commit_version);

        assert_eq!(
            pd_client.get_timestamp_call_count(),
            2,
            "local scope must not fetch extra PD TSO during prewrite"
        );
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
        assert_eq!(prewrite_count.load(Ordering::SeqCst), 1);
        assert_eq!(commit_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_local_txn_scope_uses_dc_location_for_pessimistic_for_update_ts() {
        let start_version = 7;
        let expected_for_update_ts = 8;

        let client = MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
            if let Some(req) = req.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                assert_eq!(req.for_update_ts, expected_for_update_ts);
                return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
            }

            panic!("unexpected request type: {:?}", req.type_id());
        });

        let pd_client = Arc::new(MockPdClient::new(client).with_tso_sequence(start_version));
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut txn = client
            .begin_with_txn_scope(
                "dc1",
                TransactionOptions::new_pessimistic()
                    .drop_check(crate::CheckLevel::None)
                    .heartbeat_option(HeartbeatOption::NoHeartbeat),
            )
            .await
            .unwrap();
        txn.lock_keys(vec!["k".to_owned()]).await.unwrap();

        assert_eq!(pd_client.get_timestamp_call_count(), 2);
        assert_eq!(
            pd_client.get_timestamp_dc_locations(),
            vec!["dc1".to_owned(), "dc1".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_resolve_locks_once_delegates_to_bound_lock_resolver() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 50,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 50);
                    assert!(
                        req.keys.is_empty(),
                        "non-lite resolve should not send key list"
                    );
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let resolve_result = client
            .resolve_locks_once(vec![lock], Timestamp::from_version(42), false)
            .await
            .unwrap();

        assert!(resolve_result.live_locks.is_empty());
        assert_eq!(resolve_result.ms_before_txn_expired, 0);
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_resolve_locks_once_returns_ttl_for_live_lock() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);

                    let mut lock_info = kvrpcpb::LockInfo::default();
                    lock_info.key = vec![1];
                    lock_info.primary_lock = vec![2];
                    lock_info.lock_version = 7;
                    lock_info.lock_ttl = 100;
                    lock_info.txn_size = 20;
                    lock_info.lock_type = kvrpcpb::Op::Put as i32;

                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        action: kvrpcpb::Action::NoAction as i32,
                        lock_info: Some(lock_info),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    panic!(
                        "resolve_locks_once should not issue resolve-lock cleanup for live locks"
                    );
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let resolve_result = client
            .resolve_locks_once(vec![lock], Timestamp::from_version(42), false)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_result.live_locks.len(), 1);
        assert_eq!(resolve_result.ms_before_txn_expired, 100);
    }

    #[tokio::test]
    async fn test_resolve_locks_for_read_wrapper_encodes_and_truncates_lock_keys() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let check_txn_status_count_captured = check_txn_status_count.clone();

        let keyspace_id = 0x010203;
        let mut expected_encoded_key = vec![b'x', 0x01, 0x02, 0x03];
        expected_encoded_key.extend_from_slice(b"k1");

        let pd_client = Arc::new(
            MockPdClient::new(MockKvClient::with_dispatch_hook(move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);

                    let ctx = req
                        .context
                        .as_ref()
                        .expect("check txn status request must have context");
                    assert_eq!(ctx.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(ctx.keyspace_id, keyspace_id);
                    assert_eq!(req.primary_key, expected_encoded_key);
                    assert_eq!(req.caller_start_ts, 42);
                    assert_eq!(req.current_ts, 100);
                    assert!(
                        !req.rollback_if_not_exist,
                        "resolve locks for read should not request rollback for non-existing locks"
                    );

                    let mut lock_info = kvrpcpb::LockInfo::default();
                    lock_info.key = expected_encoded_key.clone();
                    lock_info.primary_lock = expected_encoded_key.clone();
                    lock_info.lock_version = 7;
                    lock_info.lock_ttl = 100;
                    lock_info.txn_size = 20;
                    lock_info.lock_type = kvrpcpb::Op::Put as i32;

                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 100,
                        action: kvrpcpb::Action::NoAction as i32,
                        lock_info: Some(lock_info),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            }))
            .with_tso_sequence(100),
        );

        let keyspace = Keyspace::Enable { keyspace_id };
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = b"k1".to_vec();
        lock.primary_lock = b"k1".to_vec();
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let resolve_result = client
            .resolve_locks_for_read(vec![lock], Timestamp::from_version(42), false)
            .await
            .unwrap();

        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_result.live_locks.len(), 1);
        assert_eq!(resolve_result.live_locks[0].key, b"k1".to_vec());
        assert_eq!(resolve_result.live_locks[0].primary_lock, b"k1".to_vec());
        assert_eq!(resolve_result.ms_before_txn_expired, 100);
        assert!(resolve_result.resolved_locks.is_empty());
        assert!(resolve_result.committed_locks.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_locks_delegates_to_bound_lock_resolver() {
        let check_txn_status_count = Arc::new(AtomicUsize::new(0));
        let resolve_lock_count = Arc::new(AtomicUsize::new(0));

        let check_txn_status_count_captured = check_txn_status_count.clone();
        let resolve_lock_count_captured = resolve_lock_count.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(_req) = req.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_txn_status_count_captured.fetch_add(1, Ordering::SeqCst);
                    let resp = kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 50,
                        action: kvrpcpb::Action::NoAction as i32,
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                if let Some(req) = req.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
                    resolve_lock_count_captured.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(req.start_version, 7);
                    assert_eq!(req.commit_version, 50);
                    assert!(
                        req.keys.is_empty(),
                        "non-lite resolve should not send key list"
                    );
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any>);
                }

                panic!("unexpected request type: {:?}", req.type_id());
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let mut lock = kvrpcpb::LockInfo::default();
        lock.key = vec![1];
        lock.primary_lock = vec![2];
        lock.lock_version = 7;
        lock.lock_ttl = 100;
        lock.txn_size = 20;
        lock.lock_type = kvrpcpb::Op::Put as i32;

        let live_locks = client
            .resolve_locks(
                vec![lock],
                Timestamp::from_version(42),
                Backoff::no_backoff(),
            )
            .await
            .unwrap();

        assert!(live_locks.is_empty());
        assert_eq!(check_txn_status_count.load(Ordering::SeqCst), 1);
        assert_eq!(resolve_lock_count.load(Ordering::SeqCst), 1);

        let lock_resolver = client.lock_resolver();
        assert!(lock_resolver.resolving().await.is_empty());
    }

    #[tokio::test]
    async fn test_split_regions_encodes_keyspace_and_scatter_calls_pd() {
        use crate::request::EncodeKeyspace;
        use crate::request::KeyMode;

        let keyspace = Keyspace::Enable {
            keyspace_id: 0xCAFE,
        };
        let expected_api_version = keyspace.api_version() as i32;

        let split_key_raw = crate::Key::from(b"k1".to_vec());
        let expected_split_key_encoded: Vec<u8> = split_key_raw
            .clone()
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into();

        let split_calls = Arc::new(AtomicUsize::new(0));
        let split_calls_cloned = split_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::SplitRegionRequest>() {
                    split_calls_cloned.fetch_add(1, Ordering::SeqCst);

                    let ctx = req.context.as_ref().expect("context");
                    assert_eq!(ctx.api_version, expected_api_version);
                    assert_eq!(req.split_keys, vec![expected_split_key_encoded.clone()]);
                    assert!(!req.is_raw_kv);

                    let resp = kvrpcpb::SplitRegionResponse {
                        regions: vec![
                            metapb::Region {
                                id: 100,
                                ..Default::default()
                            },
                            metapb::Region {
                                id: 200,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Err(crate::Error::Unimplemented)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let region_ids = client
            .split_regions(vec![split_key_raw], true, Some(7))
            .await
            .unwrap();
        assert_eq!(region_ids, vec![100]);
        assert_eq!(split_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            pd_client.scatter_regions_calls(),
            vec![(vec![100], Some("7".to_owned()))]
        );
    }

    #[tokio::test]
    async fn test_wait_scatter_region_finish_polls_until_operator_finishes() {
        use crate::proto::pdpb;

        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Running as i32,
            ..Default::default()
        });
        pd_client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Success as i32,
            ..Default::default()
        });

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client
            .wait_scatter_region_finish(42, Backoff::no_jitter_backoff(0, 0, 2))
            .await
            .unwrap();

        assert_eq!(pd_client.get_operator_calls(), vec![42, 42]);
    }

    #[tokio::test]
    async fn test_wait_scatter_region_finish_returns_timeout_when_backoff_exhausted() {
        use crate::proto::pdpb;

        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Running as i32,
            ..Default::default()
        });

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let err = client
            .wait_scatter_region_finish(42, Backoff::no_backoff())
            .await
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::StringError(msg) if msg == "wait scatter region timeout")
        );
        assert_eq!(pd_client.get_operator_calls(), vec![42]);
    }

    #[tokio::test]
    async fn test_check_region_in_scattering_returns_true_only_for_running_scatter_operator() {
        use crate::proto::pdpb;

        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Running as i32,
            ..Default::default()
        });
        pd_client.push_get_operator_response(pdpb::GetOperatorResponse {
            desc: b"scatter-region".to_vec(),
            status: pdpb::OperatorStatus::Success as i32,
            ..Default::default()
        });

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        assert!(client.check_region_in_scattering(42).await.unwrap());
        assert!(!client.check_region_in_scattering(42).await.unwrap());
        assert_eq!(pd_client.get_operator_calls(), vec![42, 42]);
    }

    #[tokio::test]
    async fn test_gc_safepoint_uses_update_gc_safe_point_v2_when_keyspace_enabled() {
        let scan_lock_calls = Arc::new(AtomicUsize::new(0));
        let scan_lock_calls_captured = scan_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    scan_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::ScanLockResponse::default()) as Box<dyn Any>);
                }

                unreachable!("unexpected request type")
            },
        )));
        pd_client.push_update_gc_safe_point_v2_response(100);

        let keyspace = Keyspace::Enable { keyspace_id: 7 };
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let new_safe_point = client
            .gc_safepoint(Timestamp::from_version(9))
            .await
            .unwrap();
        assert_eq!(new_safe_point, 100);
        assert_eq!(pd_client.update_gc_safe_point_v2_calls(), vec![(7, 9)]);
        assert!(pd_client.update_safepoint_calls().is_empty());
        assert!(scan_lock_calls.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn test_gc_safepoint_uses_update_safepoint_when_keyspace_disabled() {
        let scan_lock_calls = Arc::new(AtomicUsize::new(0));
        let scan_lock_calls_captured = scan_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    scan_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::ScanLockResponse::default()) as Box<dyn Any>);
                }

                unreachable!("unexpected request type")
            },
        )));
        pd_client.push_update_safepoint_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let new_safe_point = client
            .gc_safepoint(Timestamp::from_version(9))
            .await
            .unwrap();
        assert_eq!(new_safe_point, 100);
        assert_eq!(pd_client.update_safepoint_calls(), vec![9]);
        assert!(pd_client.update_gc_safe_point_v2_calls().is_empty());
        assert!(scan_lock_calls.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn test_cleanup_locks_uses_gc_backoff_for_region_retries() {
        let scan_lock_calls = Arc::new(AtomicUsize::new(0));
        let scan_lock_calls_captured = scan_lock_calls.clone();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let _req: &kvrpcpb::ScanLockRequest = req.downcast_ref().unwrap();
                let call_index = scan_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                if call_index < 11 {
                    let resp = kvrpcpb::ScanLockResponse {
                        region_error: Some(errorpb::Error {
                            message: "not leader".to_owned(),
                            not_leader: Some(errorpb::NotLeader {
                                leader: Some(metapb::Peer {
                                    store_id: 41,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    return Ok(Box::new(resp) as Box<dyn Any>);
                }

                Ok(Box::new(kvrpcpb::ScanLockResponse::default()) as Box<dyn Any>)
            },
        )));

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let options = ResolveLocksOptions::default();
        let result = client
            .cleanup_locks(vec![1]..vec![2], &Timestamp::from_version(1), options)
            .await
            .unwrap();

        // DEFAULT_REGION_BACKOFF caps multi-region retries at 10. This should continue retrying
        // past that limit (GC-style backoff uses 50 attempts).
        assert_eq!(scan_lock_calls.load(Ordering::SeqCst), 12);
        assert_eq!(result.resolved_locks, 0);
    }

    #[tokio::test]
    async fn test_update_service_gc_safe_point_calls_pd() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_update_service_gc_safe_point_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let min_safe_point = client
            .update_service_gc_safe_point("service".to_owned(), 42, 7)
            .await
            .unwrap();
        assert_eq!(min_safe_point, 100);
        assert_eq!(
            pd_client.update_service_gc_safe_point_calls(),
            vec![("service".to_owned(), 42, 7)]
        );
    }

    #[tokio::test]
    async fn test_update_service_safe_point_v2_calls_pd() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_update_service_safe_point_v2_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let min_safe_point = client
            .update_service_safe_point_v2(7, "service".to_owned(), 42, 9)
            .await
            .unwrap();
        assert_eq!(min_safe_point, 100);
        assert_eq!(
            pd_client.update_service_safe_point_v2_calls(),
            vec![(7, "service".to_owned(), 42, 9)]
        );
    }

    #[tokio::test]
    async fn test_update_gc_safe_point_v2_calls_pd() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_update_gc_safe_point_v2_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let new_safe_point = client.update_gc_safe_point_v2(7, 9).await.unwrap();
        assert_eq!(new_safe_point, 100);
        assert_eq!(pd_client.update_gc_safe_point_v2_calls(), vec![(7, 9)]);
    }

    #[tokio::test]
    async fn test_get_gc_safe_point_calls_pd() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_gc_safe_point_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let safe_point = client.get_gc_safe_point().await.unwrap();
        assert_eq!(safe_point, 100);
        assert_eq!(pd_client.get_gc_safe_point_call_count(), 1);
    }

    #[tokio::test]
    async fn test_get_gc_safe_point_v2_calls_pd() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_gc_safe_point_v2_response(100);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let safe_point = client.get_gc_safe_point_v2(7).await.unwrap();
        assert_eq!(safe_point, 100);
        assert_eq!(pd_client.get_gc_safe_point_v2_calls(), vec![7]);
    }

    #[tokio::test]
    async fn test_check_visibility_returns_txn_aborted_by_gc_when_start_ts_is_too_old() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_gc_safe_point_response(50);

        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), Keyspace::Disable),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), Keyspace::Disable),
            pd: pd_client.clone(),
            keyspace: Keyspace::Disable,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.check_visibility(100).await.unwrap();

        let err = client.check_visibility(40).await.unwrap_err();
        assert!(matches!(
            err,
            crate::Error::TxnAbortedByGc {
                start_ts: 40,
                safe_point: 50
            }
        ));
        assert_eq!(pd_client.get_gc_safe_point_call_count(), 1);
    }

    #[tokio::test]
    async fn test_check_visibility_uses_gc_safe_point_v2_when_keyspace_enabled() {
        let pd_client = Arc::new(MockPdClient::default());
        pd_client.push_get_gc_safe_point_v2_response(50);

        let keyspace = Keyspace::Enable { keyspace_id: 7 };
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client.clone(),
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.check_visibility(100).await.unwrap();
        assert_eq!(pd_client.get_gc_safe_point_v2_calls(), vec![7]);
        assert_eq!(pd_client.get_gc_safe_point_call_count(), 0);
    }

    #[derive(Clone)]
    struct UnimplementedGcSafePointV2PdClient {
        inner: Arc<MockPdClient>,
    }

    #[async_trait::async_trait]
    impl crate::pd::PdClient for UnimplementedGcSafePointV2PdClient {
        type KvClient = MockKvClient;

        async fn map_region_to_store(
            self: Arc<Self>,
            region: crate::region::RegionWithLeader,
        ) -> crate::Result<crate::store::RegionStore> {
            self.inner.clone().map_region_to_store(region).await
        }

        async fn region_for_key(
            &self,
            key: &crate::Key,
        ) -> crate::Result<crate::region::RegionWithLeader> {
            self.inner.region_for_key(key).await
        }

        async fn region_for_id(
            &self,
            id: crate::region::RegionId,
        ) -> crate::Result<crate::region::RegionWithLeader> {
            self.inner.region_for_id(id).await
        }

        async fn all_stores(&self) -> crate::Result<Vec<crate::store::Store>> {
            self.inner.all_stores().await
        }

        async fn get_timestamp(self: Arc<Self>) -> crate::Result<crate::Timestamp> {
            self.inner.clone().get_timestamp().await
        }

        async fn get_gc_safe_point(self: Arc<Self>) -> crate::Result<u64> {
            self.inner.clone().get_gc_safe_point().await
        }

        async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> crate::Result<u64> {
            self.inner.clone().update_safepoint(safepoint).await
        }

        async fn load_keyspace(
            &self,
            keyspace: &str,
        ) -> crate::Result<crate::proto::keyspacepb::KeyspaceMeta> {
            self.inner.load_keyspace(keyspace).await
        }

        async fn update_leader(
            &self,
            ver_id: crate::region::RegionVerId,
            leader: crate::proto::metapb::Peer,
        ) -> crate::Result<()> {
            self.inner.update_leader(ver_id, leader).await
        }

        async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
            self.inner.invalidate_region_cache(ver_id).await;
        }

        async fn invalidate_store_cache(&self, store_id: crate::region::StoreId) {
            self.inner.invalidate_store_cache(store_id).await;
        }
    }

    #[tokio::test]
    async fn test_gc_safepoint_falls_back_to_update_safepoint_when_gc_safe_point_v2_unimplemented()
    {
        let scan_lock_calls = Arc::new(AtomicUsize::new(0));
        let scan_lock_calls_captured = scan_lock_calls.clone();

        let inner = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::ScanLockRequest>().is_some() {
                    scan_lock_calls_captured.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::ScanLockResponse::default()) as Box<dyn Any>);
                }

                unreachable!("unexpected request type")
            },
        )));
        inner.push_update_safepoint_response(100);

        let pd_client = Arc::new(UnimplementedGcSafePointV2PdClient {
            inner: inner.clone(),
        });

        let keyspace = Keyspace::Enable { keyspace_id: 7 };
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client,
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        let new_safe_point = client
            .gc_safepoint(Timestamp::from_version(9))
            .await
            .unwrap();
        assert_eq!(new_safe_point, 100);
        assert_eq!(inner.update_safepoint_calls(), vec![9]);
        assert!(inner.update_gc_safe_point_v2_calls().is_empty());
        assert!(scan_lock_calls.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn test_check_visibility_falls_back_to_gc_safe_point_when_gc_safe_point_v2_unimplemented()
    {
        let inner = Arc::new(MockPdClient::default());
        inner.push_get_gc_safe_point_response(50);

        let pd_client = Arc::new(UnimplementedGcSafePointV2PdClient {
            inner: inner.clone(),
        });

        let keyspace = Keyspace::Enable { keyspace_id: 7 };
        let client = Client {
            safe_ts: SafeTsCache::new(pd_client.clone(), keyspace),
            gc_safe_point: GcSafePointCache::new(pd_client.clone(), keyspace),
            pd: pd_client,
            keyspace,
            resolve_locks_ctx: ResolveLocksContext::default(),
            last_tsos: Default::default(),
            low_resolution_ts_update_interval_ms:
                super::default_low_resolution_ts_update_interval_ms(),
            txn_latches: None,
        };

        client.check_visibility(100).await.unwrap();
        assert_eq!(inner.get_gc_safe_point_call_count(), 1);
        assert!(inner.get_gc_safe_point_v2_calls().is_empty());
    }
}
