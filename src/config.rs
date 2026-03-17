// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;
use serde_derive::Serialize;

/// gRPC compression type for TiKV channels.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GrpcCompressionType {
    #[default]
    None,
    Gzip,
}

/// The configuration for either a [`RawClient`](crate::RawClient) or a
/// [`TransactionClient`](crate::TransactionClient).
///
/// See also [`TransactionOptions`](crate::TransactionOptions) which provides more ways to configure
/// requests.
///
/// This struct is marked `#[non_exhaustive]` to allow adding new configuration options in the
/// future without breaking downstream code. Construct it via [`Config::default`] and then use the
/// `with_*` methods (or field assignment) to customize it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub struct Config {
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub timeout: Duration,
    /// Maximum concurrency for 2PC committer multi-region requests.
    ///
    /// This limits the number of region shards executed concurrently for the prewrite, secondary
    /// commit, and rollback phases.
    ///
    /// This maps to client-go `CommitterConcurrency`.
    pub committer_concurrency: usize,
    /// Maximum lifetime for transactions using managed lock TTL heartbeats.
    ///
    /// When a transaction has been alive longer than this duration, the automatic transaction
    /// heartbeat loop stops so locks won't be kept alive forever.
    ///
    /// This maps to client-go `MaxTxnTTL` (default: 1 hour).
    pub max_txn_ttl: Duration,
    /// The maximum number of pending batched TSO requests buffered in the timestamp oracle.
    ///
    /// When exhausted, new timestamp requests will apply backpressure until pending requests are
    /// drained.
    pub tso_max_pending_count: usize,
    pub grpc_max_decoding_message_size: usize,
    /// The maximum number of gRPC connections established with each TiKV server (client-go
    /// `GrpcConnectionCount`).
    pub grpc_connection_count: usize,
    /// gRPC compression type for TiKV channels (client-go `GrpcCompressionType`).
    pub grpc_compression_type: GrpcCompressionType,
    /// Enable batch RPC (`BatchCommands`) for supported KV requests.
    ///
    /// When enabled, the client attempts to dispatch supported requests over a persistent
    /// `BatchCommands` stream and falls back to unary RPCs when batch RPC is unavailable.
    pub enable_batch_rpc: bool,
    /// Maximum number of requests coalesced into a single outbound `BatchCommandsRequest`.
    ///
    /// When batch RPC is enabled, the client merges immediately-available queued requests into a
    /// single `BatchCommandsRequest` up to this limit. This improves throughput under high
    /// concurrency without adding extra wait time.
    ///
    /// This maps to client-go `TiKVClient.MaxBatchSize` (default: 128).
    pub batch_rpc_max_batch_size: usize,
    /// Timeout for establishing gRPC connections (client-go `dialTimeout`).
    ///
    /// Set to `Duration::ZERO` to disable the connect timeout (use the system default).
    pub grpc_connect_timeout: Duration,
    /// After a duration of this time without RPC activity, the client pings the server to see if
    /// the transport is still alive (client-go `GrpcKeepAliveTime`).
    ///
    /// Set to `Duration::ZERO` to disable gRPC HTTP2 keepalive pings.
    pub grpc_keepalive_time: Duration,
    /// After having pinged for keepalive check, the client waits for this timeout for a response
    /// and if no activity is seen even after that, the connection is closed (client-go
    /// `GrpcKeepAliveTimeout`).
    ///
    /// The minimum value is 50ms (matching client-go).
    pub grpc_keepalive_timeout: Duration,
    /// gRPC initial HTTP2 stream window size, in bytes (client-go `GrpcInitialWindowSize`).
    ///
    /// Set to `0` to use `tonic`/`hyper`'s defaults.
    pub grpc_initial_window_size: u32,
    /// gRPC initial HTTP2 connection window size, in bytes (client-go `GrpcInitialConnWindowSize`).
    ///
    /// Set to `0` to use `tonic`/`hyper`'s defaults.
    pub grpc_initial_conn_window_size: u32,
    pub keyspace: Option<String>,
    pub resolve_lock_lite_threshold: u64,
    /// Transaction size threshold for starting the automatic transaction heartbeat loop.
    ///
    /// This maps to client-go `TTLRefreshedTxnSize`.
    ///
    /// - For **optimistic, non-pipelined** transactions, auto-heartbeat is started only when the
    ///   transaction's total write size exceeds this threshold.
    /// - For **pessimistic** and **pipelined** transactions, auto-heartbeat may still be started
    ///   once a primary key is established (client-go parity).
    ///
    /// Set to `0` to always start auto-heartbeat for optimistic transactions.
    pub ttl_refreshed_txn_size: u64,
    /// Time-to-live for cached region metadata (client-go `RegionCacheTTL`).
    ///
    /// When non-zero, cached region entries expire after roughly this duration of inactivity
    /// (plus jitter) and will be refreshed from PD on next use.
    ///
    /// Set to `Duration::ZERO` to disable region cache TTL expiry.
    pub region_cache_ttl: Duration,
    /// Jitter added to region cache TTL expiry to avoid stampedes.
    ///
    /// When non-zero, the effective TTL is in
    /// `[region_cache_ttl, region_cache_ttl + region_cache_ttl_jitter)`.
    ///
    /// Set to `Duration::ZERO` to disable jitter.
    pub region_cache_ttl_jitter: Duration,
    /// How often to refresh TiKV store health feedback (slow score).
    ///
    /// When non-zero, the client periodically issues `GetHealthFeedback` to all stores and uses
    /// the responses to update best-effort slow-store heuristics for replica read selection.
    ///
    /// Defaults to disabled (`0s`).
    pub health_feedback_update_interval: Duration,
    /// Enable transaction local latches (client-side commit latches).
    ///
    /// When non-zero, the transactional client serializes optimistic transaction commits on
    /// overlapping keys to reduce write conflicts (client-go `TxnLocalLatches.Capacity`).
    ///
    /// Defaults to disabled (`0`).
    pub txn_local_latches_capacity: usize,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_COMMITTER_CONCURRENCY: usize = 128;
pub(crate) const DEFAULT_MAX_TXN_TTL: Duration = Duration::from_secs(60 * 60);
pub(crate) const DEFAULT_TSO_MAX_PENDING_COUNT: usize = 1 << 16;
pub(crate) const DEFAULT_BATCH_RPC_MAX_BATCH_SIZE: usize = 128;
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4MB
const DEFAULT_GRPC_CONNECTION_COUNT: usize = 4;
const DEFAULT_GRPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_GRPC_KEEPALIVE_TIME: Duration = Duration::from_secs(10);
const DEFAULT_GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_GRPC_INITIAL_WINDOW_SIZE: u32 = 1 << 27; // 128MiB
const DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE: u32 = 1 << 27; // 128MiB
const MIN_GRPC_KEEPALIVE_TIMEOUT: Duration = Duration::from_millis(50);
pub(crate) const DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD: u64 = 16;
pub(crate) const DEFAULT_TTL_REFRESHED_TXN_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_REGION_CACHE_TTL: Duration = Duration::from_secs(600);
const DEFAULT_REGION_CACHE_TTL_JITTER: Duration = Duration::from_secs(60);

impl Default for Config {
    fn default() -> Self {
        Config {
            ca_path: None,
            cert_path: None,
            key_path: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            committer_concurrency: DEFAULT_COMMITTER_CONCURRENCY,
            max_txn_ttl: DEFAULT_MAX_TXN_TTL,
            tso_max_pending_count: DEFAULT_TSO_MAX_PENDING_COUNT,
            grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            grpc_connection_count: DEFAULT_GRPC_CONNECTION_COUNT,
            grpc_compression_type: GrpcCompressionType::None,
            enable_batch_rpc: false,
            batch_rpc_max_batch_size: DEFAULT_BATCH_RPC_MAX_BATCH_SIZE,
            grpc_connect_timeout: DEFAULT_GRPC_CONNECT_TIMEOUT,
            grpc_keepalive_time: DEFAULT_GRPC_KEEPALIVE_TIME,
            grpc_keepalive_timeout: DEFAULT_GRPC_KEEPALIVE_TIMEOUT,
            grpc_initial_window_size: DEFAULT_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE,
            keyspace: None,
            resolve_lock_lite_threshold: DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD,
            ttl_refreshed_txn_size: DEFAULT_TTL_REFRESHED_TXN_SIZE,
            region_cache_ttl: DEFAULT_REGION_CACHE_TTL,
            region_cache_ttl_jitter: DEFAULT_REGION_CACHE_TTL_JITTER,
            health_feedback_update_interval: Duration::ZERO,
            txn_local_latches_capacity: 0,
        }
    }
}

impl Config {
    /// Validate the configuration.
    ///
    /// This is called internally by client constructors, and can also be called by users to catch
    /// invalid configurations early.
    pub fn validate(&self) -> crate::Result<()> {
        if self.committer_concurrency == 0 {
            return Err(crate::Error::StringError(
                "committer-concurrency should be greater than 0".to_owned(),
            ));
        }
        if self.max_txn_ttl.is_zero() {
            return Err(crate::Error::StringError(
                "max-txn-ttl should be greater than 0".to_owned(),
            ));
        }
        if self.grpc_connection_count == 0 {
            return Err(crate::Error::StringError(
                "grpc-connection-count should be greater than 0".to_owned(),
            ));
        }
        if self.batch_rpc_max_batch_size == 0 {
            return Err(crate::Error::StringError(
                "batch-rpc-max-batch-size should be greater than 0".to_owned(),
            ));
        }
        if self.grpc_keepalive_timeout < MIN_GRPC_KEEPALIVE_TIMEOUT {
            return Err(crate::Error::StringError(format!(
                "grpc-keepalive-timeout should be at least {MIN_GRPC_KEEPALIVE_TIMEOUT:?}, but got {:?}",
                self.grpc_keepalive_timeout
            )));
        }
        Ok(())
    }

    /// Set the certificate authority, certificate, and key locations for clients.
    ///
    /// By default, this client will use an insecure connection over instead of one protected by
    /// Transport Layer Security (TLS). Your deployment may have chosen to rely on security measures
    /// such as a private network, or a VPN layer to provide secure transmission.
    ///
    /// To use a TLS secured connection, use the `with_security` function to set the required
    /// parameters.
    ///
    /// TiKV does not currently offer encrypted storage (or encryption-at-rest).
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// let config = Config::default().with_security("root.ca", "internal.cert", "internal.key");
    /// ```
    #[must_use]
    pub fn with_security(
        mut self,
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.ca_path = Some(ca_path.into());
        self.cert_path = Some(cert_path.into());
        self.key_path = Some(key_path.into());
        self
    }

    /// Set the timeout for clients.
    ///
    /// The timeout is used for all requests when using or connecting to a TiKV cluster (including
    /// PD nodes). If the request does not complete within timeout, the request is cancelled and
    /// an error returned to the user.
    ///
    /// The default timeout is two seconds.
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// # use std::time::Duration;
    /// let config = Config::default().with_timeout(Duration::from_secs(10));
    /// ```
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum concurrency for 2PC committer multi-region requests.
    ///
    /// This maps to client-go `CommitterConcurrency`.
    #[must_use]
    pub fn with_committer_concurrency(mut self, concurrency: usize) -> Self {
        self.committer_concurrency = concurrency;
        self
    }

    /// Set the maximum transaction lifetime for auto-heartbeat.
    ///
    /// This maps to client-go `MaxTxnTTL`.
    #[must_use]
    pub fn with_max_txn_ttl(mut self, ttl: Duration) -> Self {
        self.max_txn_ttl = ttl;
        self
    }

    /// Set the maximum number of pending batched TSO requests buffered in the timestamp oracle.
    ///
    /// Values less than 1 are treated as 1.
    #[must_use]
    pub fn with_tso_max_pending_count(mut self, max_pending_count: usize) -> Self {
        self.tso_max_pending_count = max_pending_count.max(1);
        self
    }

    /// Set the maximum decoding message size for gRPC.
    #[must_use]
    pub fn with_grpc_max_decoding_message_size(mut self, size: usize) -> Self {
        self.grpc_max_decoding_message_size = size;
        self
    }

    /// Set the maximum number of gRPC connections established with each TiKV server.
    ///
    /// Values less than 1 are treated as 1.
    #[must_use]
    pub fn with_grpc_connection_count(mut self, count: usize) -> Self {
        self.grpc_connection_count = count.max(1);
        self
    }

    /// Set the gRPC compression type for TiKV channels.
    #[must_use]
    pub fn with_grpc_compression_type(mut self, compression_type: GrpcCompressionType) -> Self {
        self.grpc_compression_type = compression_type;
        self
    }

    /// Enable or disable batch RPC (`BatchCommands`) for supported KV requests.
    #[must_use]
    pub fn with_enable_batch_rpc(mut self, enable: bool) -> Self {
        self.enable_batch_rpc = enable;
        self
    }

    /// Set the maximum number of requests coalesced into a single outbound `BatchCommandsRequest`.
    ///
    /// This only affects the `BatchCommands` stream when `enable_batch_rpc` is set.
    ///
    /// Values less than 1 are treated as 1.
    #[must_use]
    pub fn with_batch_rpc_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.batch_rpc_max_batch_size = max_batch_size.max(1);
        self
    }

    /// Set the timeout for establishing gRPC connections.
    ///
    /// Set to `Duration::ZERO` to disable the connect timeout (use the system default).
    #[must_use]
    pub fn with_grpc_connect_timeout(mut self, timeout: Duration) -> Self {
        self.grpc_connect_timeout = timeout;
        self
    }

    /// Set the gRPC HTTP2 keepalive ping interval.
    ///
    /// Set to `Duration::ZERO` to disable keepalive pings.
    #[must_use]
    pub fn with_grpc_keepalive_time(mut self, interval: Duration) -> Self {
        self.grpc_keepalive_time = interval;
        self
    }

    /// Set the gRPC HTTP2 keepalive ping timeout.
    ///
    /// The minimum supported value is 50ms (matching client-go). Use [`Config::validate`] to
    /// verify a configuration before constructing a client.
    #[must_use]
    pub fn with_grpc_keepalive_timeout(mut self, timeout: Duration) -> Self {
        self.grpc_keepalive_timeout = timeout;
        self
    }

    /// Set the gRPC initial HTTP2 stream window size (in bytes).
    ///
    /// Set to `0` to use `tonic`/`hyper`'s defaults.
    #[must_use]
    pub fn with_grpc_initial_window_size(mut self, size: u32) -> Self {
        self.grpc_initial_window_size = size;
        self
    }

    /// Set the gRPC initial HTTP2 connection window size (in bytes).
    ///
    /// Set to `0` to use `tonic`/`hyper`'s defaults.
    #[must_use]
    pub fn with_grpc_initial_conn_window_size(mut self, size: u32) -> Self {
        self.grpc_initial_conn_window_size = size;
        self
    }

    /// Set to use default keyspace.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_default_keyspace(self) -> Self {
        self.with_keyspace("DEFAULT")
    }

    /// Set the use keyspace for the client.
    ///
    /// Server should enable `storage.api-version = 2` to use this feature.
    #[must_use]
    pub fn with_keyspace(mut self, keyspace: &str) -> Self {
        self.keyspace = Some(keyspace.to_owned());
        self
    }

    /// Set the `txn_size` threshold for ResolveLock "lite" mode.
    ///
    /// When resolving locks, transactions with `txn_size < threshold` will use ResolveLock lite
    /// (populate `kvrpcpb::ResolveLockRequest.keys`) to resolve only the conflicting key, avoiding
    /// scanning the whole region for `start_ts`.
    ///
    /// The default is `16` (matching client-go).
    #[must_use]
    pub fn with_resolve_lock_lite_threshold(mut self, threshold: u64) -> Self {
        self.resolve_lock_lite_threshold = threshold;
        self
    }

    /// Set the transaction size threshold for starting optimistic auto-heartbeat.
    ///
    /// This maps to client-go `TTLRefreshedTxnSize`.
    ///
    /// Set to `0` to always start optimistic auto-heartbeat.
    #[must_use]
    pub fn with_ttl_refreshed_txn_size(mut self, size: u64) -> Self {
        self.ttl_refreshed_txn_size = size;
        self
    }

    /// Set the region cache TTL for cached region metadata (client-go `RegionCacheTTL`).
    ///
    /// Set to `Duration::ZERO` to disable region cache TTL expiry.
    #[must_use]
    pub fn with_region_cache_ttl(mut self, ttl: Duration) -> Self {
        self.region_cache_ttl = ttl;
        self
    }

    /// Set the jitter added to region cache TTL expiry.
    ///
    /// Set to `Duration::ZERO` to disable jitter.
    #[must_use]
    pub fn with_region_cache_ttl_jitter(mut self, jitter: Duration) -> Self {
        self.region_cache_ttl_jitter = jitter;
        self
    }

    /// Set how often to refresh TiKV store health feedback (slow score).
    ///
    /// Set to `Duration::ZERO` to disable the background refresher (default).
    #[must_use]
    pub fn with_health_feedback_update_interval(mut self, interval: Duration) -> Self {
        self.health_feedback_update_interval = interval;
        self
    }

    /// Enable transaction local latches (client-side commit latches).
    ///
    /// Set to `0` to disable (default).
    #[must_use]
    pub fn with_txn_local_latches_capacity(mut self, capacity: usize) -> Self {
        self.txn_local_latches_capacity = capacity;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_grpc_keepalive_and_window_sizes() {
        let config = Config::default();
        assert_eq!(config.grpc_keepalive_time, Duration::from_secs(10));
        assert_eq!(config.grpc_keepalive_timeout, Duration::from_secs(3));
        assert_eq!(config.grpc_initial_window_size, 1 << 27);
        assert_eq!(config.grpc_initial_conn_window_size, 1 << 27);
        assert_eq!(config.grpc_connection_count, 4);
        assert_eq!(config.grpc_compression_type, GrpcCompressionType::None);
        assert!(!config.enable_batch_rpc);
        assert_eq!(
            config.batch_rpc_max_batch_size,
            DEFAULT_BATCH_RPC_MAX_BATCH_SIZE
        );
        assert_eq!(config.grpc_connect_timeout, Duration::from_secs(5));
        assert_eq!(config.committer_concurrency, DEFAULT_COMMITTER_CONCURRENCY);
        assert_eq!(config.max_txn_ttl, DEFAULT_MAX_TXN_TTL);
        assert_eq!(
            config.ttl_refreshed_txn_size,
            DEFAULT_TTL_REFRESHED_TXN_SIZE
        );
        assert_eq!(config.region_cache_ttl, Duration::from_secs(600));
        assert_eq!(config.region_cache_ttl_jitter, Duration::from_secs(60));
        config.validate().unwrap();
    }

    #[test]
    fn test_with_batch_rpc_max_batch_size_clamps_to_one() {
        let config = Config::default().with_batch_rpc_max_batch_size(0);
        assert_eq!(config.batch_rpc_max_batch_size, 1);
    }

    #[test]
    fn test_config_validate_grpc_keepalive_timeout_min() {
        let config = Config::default().with_grpc_keepalive_timeout(Duration::from_millis(49));
        let err = config.validate().unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_config_validate_committer_concurrency_min() {
        let mut config = Config::default();
        config.committer_concurrency = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_config_validate_max_txn_ttl_min() {
        let mut config = Config::default();
        config.max_txn_ttl = Duration::ZERO;
        let err = config.validate().unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_config_validate_grpc_connection_count_min() {
        let mut config = Config::default();
        config.grpc_connection_count = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_config_validate_batch_rpc_max_batch_size_min() {
        let mut config = Config::default();
        config.batch_rpc_max_batch_size = 0;
        let err = config.validate().unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }
}
