// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

use lazy_static::lazy_static;
use serde_derive::Deserialize;
use serde_derive::Serialize;

/// Parsed result of [`parse_path`].
///
/// This mirrors the output of client-go `config.ParsePath`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedTikvPath {
    /// Comma-separated PD endpoints from the URI host part.
    pub pd_addrs: Vec<String>,
    /// Whether GC is disabled (parsed from `disableGC` query parameter).
    pub disable_gc: bool,
    /// Keyspace name parsed from `keyspaceName` query parameter.
    ///
    /// `None` means the parameter is absent or empty.
    pub keyspace_name: Option<String>,
}

/// Parse a `tikv://...` connection URI.
///
/// This is a compatibility helper mirroring client-go `config.ParsePath`.
///
/// # Accepted format
///
/// - `tikv://<pd-addr>[,<pd-addr>...]?disableGC=<true|false>&keyspaceName=<name>`
///
/// The host part is treated as a comma-separated list of PD endpoints. The parser is intentionally
/// permissive and does not validate each endpoint beyond being non-empty.
pub fn parse_path(path: &str) -> crate::Result<ParsedTikvPath> {
    let (scheme, rest) = path.split_once("://").ok_or_else(|| {
        crate::Error::StringError("invalid tikv path: missing scheme delimiter".to_owned())
    })?;

    if !scheme.eq_ignore_ascii_case("tikv") {
        return Err(crate::Error::StringError(format!(
            "invalid tikv path scheme: {scheme}"
        )));
    }

    // Split off query and fragment first; then ignore any path component and keep only authority.
    let (authority_and_path, query_and_fragment) = rest.split_once('?').unwrap_or((rest, ""));
    let authority = authority_and_path.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return Err(crate::Error::StringError(
            "invalid tikv path: missing pd endpoints".to_owned(),
        ));
    }

    let pd_addrs: Vec<String> = authority
        .split(',')
        .map(str::trim)
        .map(|s| s.to_owned())
        .collect();
    if pd_addrs.iter().any(|s| s.is_empty()) {
        return Err(crate::Error::StringError(
            "invalid tikv path: empty pd endpoint".to_owned(),
        ));
    }

    let mut disable_gc = false;
    let mut disable_gc_seen = false;
    let mut keyspace_name = None;
    let mut keyspace_name_seen = false;

    let query = query_and_fragment.split('#').next().unwrap_or_default();
    if !query.is_empty() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_query_component(raw_key)?;
            let value = decode_query_component(raw_value)?;

            match key.as_str() {
                // Match client-go ParsePath parameter names.
                "disableGC" => {
                    // url.Values.Get returns first value; keep first seen.
                    if disable_gc_seen {
                        continue;
                    }
                    disable_gc_seen = true;
                    match value.to_ascii_lowercase().as_str() {
                        "true" => disable_gc = true,
                        "false" | "" => disable_gc = false,
                        _ => {
                            return Err(crate::Error::StringError(
                                "invalid tikv path: disableGC must be true/false".to_owned(),
                            ))
                        }
                    }
                }
                "keyspaceName" => {
                    if keyspace_name_seen {
                        continue;
                    }
                    keyspace_name_seen = true;
                    if !value.is_empty() {
                        keyspace_name = Some(value);
                    }
                }
                _ => {}
            }
        }
    }

    Ok(ParsedTikvPath {
        pd_addrs,
        disable_gc,
        keyspace_name,
    })
}

pub(crate) fn normalize_txn_scope(txn_scope: &str) -> Option<String> {
    if txn_scope.is_empty() || txn_scope == "global" {
        None
    } else {
        Some(txn_scope.to_owned())
    }
}

fn decode_query_component(component: &str) -> crate::Result<String> {
    let mut bytes = Vec::with_capacity(component.len());
    let mut iter = component.as_bytes().iter().copied();
    while let Some(b) = iter.next() {
        match b {
            b'+' => bytes.push(b' '),
            b'%' => {
                let hi = iter.next().ok_or_else(|| {
                    crate::Error::StringError(
                        "invalid tikv path query: incomplete percent encoding".to_owned(),
                    )
                })?;
                let lo = iter.next().ok_or_else(|| {
                    crate::Error::StringError(
                        "invalid tikv path query: incomplete percent encoding".to_owned(),
                    )
                })?;
                let hi = from_hex_digit(hi)?;
                let lo = from_hex_digit(lo)?;
                bytes.push((hi << 4) | lo);
            }
            _ => bytes.push(b),
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| crate::Error::StringError("invalid tikv path query: invalid utf-8".to_owned()))
}

fn from_hex_digit(digit: u8) -> crate::Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(crate::Error::StringError(
            "invalid tikv path query: invalid hex digit".to_owned(),
        )),
    }
}

/// gRPC compression type for TiKV channels.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GrpcCompressionType {
    #[default]
    None,
    Gzip,
}

/// Standalone TLS/security settings.
///
/// This mirrors client-go `config.Security`, but integrates with the Rust client through
/// [`Security::apply_to_config`] and [`Security::security_manager`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Security {
    /// CA certificate path used to verify TiKV / PD servers.
    pub ca_path: Option<PathBuf>,
    /// Client certificate path presented to TiKV / PD.
    pub cert_path: Option<PathBuf>,
    /// Client private key path presented to TiKV / PD.
    pub key_path: Option<PathBuf>,
    /// Expected server certificate common names.
    ///
    /// This field is preserved for API parity. The current transport path does not yet enforce it.
    pub verify_cn: Vec<String>,
}

impl Security {
    /// Create a new standalone security config.
    #[must_use]
    pub fn new(
        ca_path: impl Into<PathBuf>,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        verify_cn: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            ca_path: normalize_optional_path(ca_path),
            cert_path: normalize_optional_path(cert_path),
            key_path: normalize_optional_path(key_path),
            verify_cn: verify_cn.into_iter().map(Into::into).collect(),
        }
    }

    /// Apply these security settings to an existing [`Config`].
    #[must_use]
    pub fn apply_to_config(&self, mut config: Config) -> Config {
        config.ca_path = self.ca_path.clone();
        config.cert_path = self.cert_path.clone();
        config.key_path = self.key_path.clone();
        config
    }

    /// Convert these security settings into a standalone [`Config`].
    #[must_use]
    pub fn into_config(self) -> Config {
        self.apply_to_config(Config::default())
    }

    /// Build a [`crate::SecurityManager`] from these security settings.
    pub fn security_manager(&self) -> crate::Result<crate::SecurityManager> {
        self.apply_to_config(Config::default()).security_manager()
    }
}

fn normalize_optional_path(path: impl Into<PathBuf>) -> Option<PathBuf> {
    let path = path.into();
    (!path.as_os_str().is_empty()).then_some(path)
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
    /// Maximum number of in-flight requests allowed per TiKV store.
    ///
    /// When set to a value greater than 0, the client applies a best-effort per-store token limit
    /// and returns an error immediately once the limit is exceeded.
    ///
    /// This maps to client-go `kv.StoreLimit`.
    ///
    /// Defaults to disabled (`0`).
    pub store_limit: i64,
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
    /// Enable TiKV request forwarding through a proxy store (client-go `EnableForwarding`).
    ///
    /// When enabled, the client may send a request to a proxy TiKV store and ask it to forward the
    /// request to the intended store. This is mainly useful when the target store is reachable
    /// from other TiKV nodes but unreachable from the client (for example due to network
    /// partition).
    ///
    /// Defaults to disabled (`false`).
    pub enable_forwarding: bool,
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
    /// Optional custom DNS server used for resolving gRPC endpoints.
    ///
    /// This mirrors client-go `util.GetCustomDNSDialer` and is mainly useful for connecting TiKV
    /// components in Kubernetes environments.
    ///
    /// When set, the client resolves gRPC endpoint hostnames via this DNS server (UDP/TCP) rather
    /// than the system resolver.
    pub grpc_custom_dns_server: Option<SocketAddr>,
    /// Optional DNS domain suffix appended to all gRPC endpoints before resolving.
    ///
    /// For example, when set to `"cluster.local"`, the endpoint `"pd0.pd:2379"` becomes
    /// `"pd0.pd.cluster.local:2379"` when dialing.
    ///
    /// This mirrors client-go `util.GetCustomDNSDialer`.
    pub grpc_custom_dns_domain: Option<String>,
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
    /// Whether to preload region metadata into the local region cache.
    ///
    /// When enabled, the client will asynchronously scan regions from PD on startup and warm the
    /// local region cache. Errors are logged and ignored.
    ///
    /// This maps to client-go `EnablePreload`.
    ///
    /// Defaults to disabled (`false`).
    pub enable_region_cache_preload: bool,
    /// The local "zone" label used to classify cross-zone traffic.
    ///
    /// When set, the client compares it with the target store "zone" label (from PD store
    /// metadata) and records cross-zone traffic counters in [`crate::util::TrafficDetails`].
    ///
    /// When unset (or empty), cross-zone classification is disabled and traffic is recorded only
    /// in the total counters.
    ///
    /// This maps to client-go global config `ZoneLabel`.
    pub zone_label: Option<String>,
    /// Default transaction scope used when starting transactions without an explicit scope.
    ///
    /// When set to `"global"` (or empty), the client uses the global TSO allocator. Otherwise the
    /// value is passed through as PD `dc_location`, matching client-go global config `TxnScope`.
    ///
    /// This affects transaction creation helpers such as
    /// [`crate::TransactionClient::begin_optimistic`] and
    /// [`crate::TransactionClient::begin_pessimistic`] when the caller does not set a scope via
    /// [`crate::TransactionOptions::txn_scope`].
    pub txn_scope: Option<String>,
    /// How often to refresh TiKV store health feedback (slow score).
    ///
    /// When non-zero, the client periodically issues `GetHealthFeedback` to all stores and uses
    /// the responses to update best-effort slow-store heuristics for replica read selection.
    ///
    /// Defaults to disabled (`0s`).
    pub health_feedback_update_interval: Duration,
    /// How often to refresh TiKV store liveness via the KV status API (gRPC health check).
    ///
    /// When non-zero, the client periodically issues `grpc.health.v1.Health/Check` to all stores
    /// and records best-effort liveness state metrics.
    ///
    /// Defaults to disabled (`0s`).
    pub store_liveness_update_interval: Duration,
    /// Timeout for each KV status API call (gRPC health check).
    ///
    /// This maps to client-go `storeLivenessTimeout`.
    pub store_liveness_timeout: Duration,
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
const DEFAULT_STORE_LIVENESS_TIMEOUT: Duration = Duration::from_secs(1);
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
            store_limit: 0,
            committer_concurrency: DEFAULT_COMMITTER_CONCURRENCY,
            max_txn_ttl: DEFAULT_MAX_TXN_TTL,
            tso_max_pending_count: DEFAULT_TSO_MAX_PENDING_COUNT,
            grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            grpc_connection_count: DEFAULT_GRPC_CONNECTION_COUNT,
            grpc_compression_type: GrpcCompressionType::None,
            enable_batch_rpc: false,
            enable_forwarding: false,
            batch_rpc_max_batch_size: DEFAULT_BATCH_RPC_MAX_BATCH_SIZE,
            grpc_connect_timeout: DEFAULT_GRPC_CONNECT_TIMEOUT,
            grpc_custom_dns_server: None,
            grpc_custom_dns_domain: None,
            grpc_keepalive_time: DEFAULT_GRPC_KEEPALIVE_TIME,
            grpc_keepalive_timeout: DEFAULT_GRPC_KEEPALIVE_TIMEOUT,
            grpc_initial_window_size: DEFAULT_GRPC_INITIAL_WINDOW_SIZE,
            grpc_initial_conn_window_size: DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE,
            keyspace: None,
            resolve_lock_lite_threshold: DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD,
            ttl_refreshed_txn_size: DEFAULT_TTL_REFRESHED_TXN_SIZE,
            region_cache_ttl: DEFAULT_REGION_CACHE_TTL,
            region_cache_ttl_jitter: DEFAULT_REGION_CACHE_TTL_JITTER,
            enable_region_cache_preload: false,
            zone_label: None,
            txn_scope: None,
            health_feedback_update_interval: Duration::ZERO,
            store_liveness_update_interval: Duration::ZERO,
            store_liveness_timeout: DEFAULT_STORE_LIVENESS_TIMEOUT,
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
        if self.store_limit < 0 {
            return Err(crate::Error::StringError(
                "store-limit should be greater than or equal to 0".to_owned(),
            ));
        }
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
        if let Some(server) = self.grpc_custom_dns_server {
            if server.port() == 0 {
                return Err(crate::Error::StringError(
                    "grpc-custom-dns-server port should be greater than 0".to_owned(),
                ));
            }
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

    /// Build a [`crate::SecurityManager`] from this config.
    ///
    /// This is the Rust equivalent of materializing client-go `config.Security`
    /// into a TLS-enabled client configuration. When the config does not contain
    /// a complete CA/cert/key triple, the returned manager uses insecure
    /// defaults, matching the client’s current connection behavior.
    pub fn security_manager(&self) -> crate::Result<crate::SecurityManager> {
        let manager = if let (Some(ca_path), Some(cert_path), Some(key_path)) =
            (&self.ca_path, &self.cert_path, &self.key_path)
        {
            crate::SecurityManager::load(ca_path, cert_path, key_path)?
        } else {
            crate::SecurityManager::default()
        };

        Ok(manager
            .with_grpc_keepalive(self.grpc_keepalive_time, self.grpc_keepalive_timeout)
            .with_grpc_initial_window_sizes(
                self.grpc_initial_window_size,
                self.grpc_initial_conn_window_size,
            )
            .with_grpc_connect_timeout(self.grpc_connect_timeout)
            .with_grpc_custom_dns(
                self.grpc_custom_dns_server,
                self.grpc_custom_dns_domain.clone(),
            ))
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

    /// Set the maximum number of in-flight requests allowed per TiKV store (client-go `kv.StoreLimit`).
    ///
    /// Set to `0` to disable (default).
    #[must_use]
    pub fn with_store_limit(mut self, limit: i64) -> Self {
        self.store_limit = limit;
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

    /// Enable forwarding/proxy request routing (client-go `EnableForwarding`).
    ///
    /// Defaults to disabled.
    #[must_use]
    pub fn with_enable_forwarding(mut self, enable: bool) -> Self {
        self.enable_forwarding = enable;
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

    /// Set a custom DNS server used for resolving gRPC endpoints.
    ///
    /// When set, the client resolves gRPC endpoint hostnames via this DNS server rather than the
    /// system resolver.
    ///
    /// This mirrors client-go `util.GetCustomDNSDialer`.
    #[must_use]
    pub fn with_grpc_custom_dns_server(mut self, dns_server: SocketAddr) -> Self {
        self.grpc_custom_dns_server = Some(dns_server);
        self
    }

    /// Set an optional DNS domain suffix appended to all gRPC endpoints before resolving.
    ///
    /// When set to `"cluster.local"`, the endpoint `"pd0.pd:2379"` becomes
    /// `"pd0.pd.cluster.local:2379"` when dialing.
    ///
    /// This mirrors client-go `util.GetCustomDNSDialer`.
    #[must_use]
    pub fn with_grpc_custom_dns_domain(mut self, dns_domain: impl Into<String>) -> Self {
        let dns_domain = dns_domain.into();
        self.grpc_custom_dns_domain = (!dns_domain.is_empty()).then_some(dns_domain);
        self
    }

    /// Convenience method: configure both custom DNS server and domain suffix.
    #[must_use]
    pub fn with_grpc_custom_dns(
        mut self,
        dns_server: SocketAddr,
        dns_domain: impl Into<String>,
    ) -> Self {
        self.grpc_custom_dns_server = Some(dns_server);
        self.with_grpc_custom_dns_domain(dns_domain)
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

    /// Enable or disable region cache preload on client startup (client-go `EnablePreload`).
    #[must_use]
    pub fn with_enable_region_cache_preload(mut self, enable: bool) -> Self {
        self.enable_region_cache_preload = enable;
        self
    }

    /// Set the local "zone" label used to classify cross-zone traffic.
    ///
    /// This is a compatibility knob for client-go `config.GetGlobalConfig().ZoneLabel`.
    #[must_use]
    pub fn with_zone_label(mut self, zone_label: impl Into<String>) -> Self {
        let zone_label = zone_label.into();
        self.zone_label = (!zone_label.is_empty()).then_some(zone_label);
        self
    }

    /// Set the default transaction scope for new transactions created by a client with this config.
    ///
    /// When `txn_scope` is `"global"` (or empty), the default is cleared and transactions use the
    /// global TSO allocator unless an explicit scope is provided.
    #[must_use]
    pub fn with_txn_scope(mut self, txn_scope: impl AsRef<str>) -> Self {
        self.txn_scope = normalize_txn_scope(txn_scope.as_ref());
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

    /// Set how often to refresh TiKV store liveness via the KV status API (gRPC health check).
    ///
    /// Set to `Duration::ZERO` to disable the background refresher (default).
    #[must_use]
    pub fn with_store_liveness_update_interval(mut self, interval: Duration) -> Self {
        self.store_liveness_update_interval = interval;
        self
    }

    /// Set the timeout for each KV status API call (gRPC health check).
    ///
    /// This maps to client-go `storeLivenessTimeout`.
    #[must_use]
    pub fn with_store_liveness_timeout(mut self, timeout: Duration) -> Self {
        self.store_liveness_timeout = timeout;
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

lazy_static! {
    static ref GLOBAL_CONFIG: RwLock<Config> = RwLock::new(Config::default());
}

#[cfg(test)]
pub(crate) static GLOBAL_CONFIG_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// Get the global client configuration.
///
/// This is a compatibility layer for client-go `config.GetGlobalConfig`. Most Rust code should
/// prefer passing a per-client [`Config`] explicitly.
pub fn get_global_config() -> Config {
    GLOBAL_CONFIG
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// Get the effective default transaction scope from the global config.
///
/// This mirrors client-go `config.GetTxnScopeFromConfig`.
///
/// Returns `"global"` when the global config does not specify a local transaction scope.
pub fn get_txn_scope_from_global_config() -> String {
    get_global_config()
        .txn_scope
        .as_deref()
        .and_then(normalize_txn_scope)
        .unwrap_or_else(|| "global".to_owned())
}

/// Set the global client configuration.
///
/// This is a compatibility layer for client-go `config.StoreGlobalConfig`. Most Rust code should
/// prefer passing a per-client [`Config`] explicitly.
pub fn set_global_config(config: Config) {
    *GLOBAL_CONFIG.write().unwrap_or_else(|e| e.into_inner()) = config;
}

/// Update the global client configuration in-place.
pub fn update_global_config(update: impl FnOnce(&mut Config)) {
    let mut config = GLOBAL_CONFIG.write().unwrap_or_else(|e| e.into_inner());
    update(&mut config);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct GlobalConfigGuard {
        prev: Config,
    }

    impl Drop for GlobalConfigGuard {
        fn drop(&mut self) {
            set_global_config(self.prev.clone());
        }
    }

    fn set_global_config_scoped(config: Config) -> GlobalConfigGuard {
        let prev = get_global_config();
        set_global_config(config);
        GlobalConfigGuard { prev }
    }

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
        assert!(!config.enable_forwarding);
        assert_eq!(
            config.batch_rpc_max_batch_size,
            DEFAULT_BATCH_RPC_MAX_BATCH_SIZE
        );
        assert_eq!(config.grpc_connect_timeout, Duration::from_secs(5));
        assert!(config.zone_label.is_none());
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

    #[test]
    fn test_global_config_set_get_update() {
        let _lock = super::GLOBAL_CONFIG_TEST_LOCK.blocking_lock();

        let mut config = Config::default();
        config.enable_batch_rpc = true;
        let _guard = set_global_config_scoped(config.clone());

        assert_eq!(get_global_config(), config);
        update_global_config(|cfg| cfg.enable_batch_rpc = false);
        assert!(!get_global_config().enable_batch_rpc);
    }

    #[test]
    fn test_config_with_txn_scope_and_global_helper() {
        let _lock = super::GLOBAL_CONFIG_TEST_LOCK.blocking_lock();

        assert_eq!(get_txn_scope_from_global_config(), "global");

        let _guard = set_global_config_scoped(Config::default().with_txn_scope("dc1"));
        assert_eq!(get_global_config().txn_scope.as_deref(), Some("dc1"));
        assert_eq!(get_txn_scope_from_global_config(), "dc1");
    }

    #[test]
    fn test_parse_path_basic() {
        let parsed = parse_path("tikv://127.0.0.1:2379").unwrap();
        assert_eq!(parsed.pd_addrs, vec!["127.0.0.1:2379".to_owned()]);
        assert!(!parsed.disable_gc);
        assert_eq!(parsed.keyspace_name, None);
    }

    #[test]
    fn test_parse_path_multi_endpoints_and_params() {
        let parsed = parse_path("tikv://pd1:2379,pd2:2379?disableGC=true&keyspaceName=ks").unwrap();
        assert_eq!(
            parsed.pd_addrs,
            vec!["pd1:2379".to_owned(), "pd2:2379".to_owned()]
        );
        assert!(parsed.disable_gc);
        assert_eq!(parsed.keyspace_name.as_deref(), Some("ks"));
    }

    #[test]
    fn test_parse_path_disable_gc_invalid_value() {
        let err = parse_path("tikv://pd:2379?disableGC=maybe").unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_parse_path_invalid_scheme() {
        let err = parse_path("http://pd:2379").unwrap_err();
        assert!(matches!(err, crate::Error::StringError(_)));
    }

    #[test]
    fn test_parse_path_decodes_keyspace_name() {
        // '+' decodes to space, '%2B' decodes to '+' (x-www-form-urlencoded style, matching Go).
        let parsed =
            parse_path("tikv://pd:2379?keyspaceName=hello%2Bworld+space&disableGC=false").unwrap();
        assert_eq!(parsed.keyspace_name.as_deref(), Some("hello+world space"));
        assert!(!parsed.disable_gc);
    }

    #[test]
    fn test_config_security_manager_defaults_without_tls_material() {
        let manager = Config::default().security_manager().unwrap();

        assert_eq!(
            manager.grpc_keepalive_config(),
            (DEFAULT_GRPC_KEEPALIVE_TIME, DEFAULT_GRPC_KEEPALIVE_TIMEOUT)
        );
        assert_eq!(
            manager.grpc_window_sizes(),
            (
                DEFAULT_GRPC_INITIAL_WINDOW_SIZE,
                DEFAULT_GRPC_INITIAL_CONN_WINDOW_SIZE
            )
        );
        assert_eq!(manager.grpc_connect_timeout(), DEFAULT_GRPC_CONNECT_TIMEOUT);
    }

    #[test]
    fn test_config_security_manager_loads_tls_and_applies_overrides() {
        let temp = tempfile::tempdir().unwrap();
        let ca_path = temp.path().join("ca.pem");
        let cert_path = temp.path().join("cert.pem");
        let key_path = temp.path().join("key.pem");
        for (id, path) in [&ca_path, &cert_path, &key_path].iter().enumerate() {
            std::fs::write(path, [id as u8]).unwrap();
        }

        let manager = Config::default()
            .with_security(&ca_path, &cert_path, &key_path)
            .with_grpc_keepalive_time(Duration::from_secs(11))
            .with_grpc_keepalive_timeout(Duration::from_secs(12))
            .with_grpc_initial_window_size(123)
            .with_grpc_initial_conn_window_size(456)
            .with_grpc_connect_timeout(Duration::from_secs(7))
            .security_manager()
            .unwrap();

        assert_eq!(
            manager.grpc_keepalive_config(),
            (Duration::from_secs(11), Duration::from_secs(12))
        );
        assert_eq!(manager.grpc_window_sizes(), (123, 456));
        assert_eq!(manager.grpc_connect_timeout(), Duration::from_secs(7));
    }

    #[test]
    fn test_security_new_and_apply_to_config() {
        let security = Security::new(
            "ca.pem",
            "cert.pem",
            "key.pem",
            ["tikv-server", "pd-server"],
        );

        assert_eq!(
            security.ca_path.as_deref(),
            Some(std::path::Path::new("ca.pem"))
        );
        assert_eq!(
            security.cert_path.as_deref(),
            Some(std::path::Path::new("cert.pem"))
        );
        assert_eq!(
            security.key_path.as_deref(),
            Some(std::path::Path::new("key.pem"))
        );
        assert_eq!(security.verify_cn, vec!["tikv-server", "pd-server"]);

        let config =
            security.apply_to_config(Config::default().with_timeout(Duration::from_secs(9)));
        assert_eq!(
            config.ca_path.as_deref(),
            Some(std::path::Path::new("ca.pem"))
        );
        assert_eq!(
            config.cert_path.as_deref(),
            Some(std::path::Path::new("cert.pem"))
        );
        assert_eq!(
            config.key_path.as_deref(),
            Some(std::path::Path::new("key.pem"))
        );
        assert_eq!(config.timeout, Duration::from_secs(9));
    }

    #[test]
    fn test_security_manager_matches_config_security_manager() {
        let temp = tempfile::tempdir().unwrap();
        let ca_path = temp.path().join("ca.pem");
        let cert_path = temp.path().join("cert.pem");
        let key_path = temp.path().join("key.pem");
        for (id, path) in [&ca_path, &cert_path, &key_path].iter().enumerate() {
            std::fs::write(path, [id as u8]).unwrap();
        }

        let security = Security::new(
            &ca_path,
            &cert_path,
            &key_path,
            std::iter::empty::<String>(),
        );
        let from_security = security.security_manager().unwrap();
        let from_config = security
            .clone()
            .apply_to_config(Config::default())
            .security_manager()
            .unwrap();

        assert_eq!(
            from_security.grpc_keepalive_config(),
            from_config.grpc_keepalive_config()
        );
        assert_eq!(
            from_security.grpc_window_sizes(),
            from_config.grpc_window_sizes()
        );
        assert_eq!(
            from_security.grpc_connect_timeout(),
            from_config.grpc_connect_timeout()
        );
    }
}
