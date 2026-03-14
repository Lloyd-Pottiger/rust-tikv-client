// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;
use serde_derive::Serialize;

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
    /// The maximum number of pending batched TSO requests buffered in the timestamp oracle.
    ///
    /// When exhausted, new timestamp requests will apply backpressure until pending requests are
    /// drained.
    pub tso_max_pending_count: usize,
    pub grpc_max_decoding_message_size: usize,
    pub keyspace: Option<String>,
    pub resolve_lock_lite_threshold: u64,
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
pub(crate) const DEFAULT_TSO_MAX_PENDING_COUNT: usize = 1 << 16;
const DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4MB
pub(crate) const DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD: u64 = 16;

impl Default for Config {
    fn default() -> Self {
        Config {
            ca_path: None,
            cert_path: None,
            key_path: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            tso_max_pending_count: DEFAULT_TSO_MAX_PENDING_COUNT,
            grpc_max_decoding_message_size: DEFAULT_GRPC_MAX_DECODING_MESSAGE_SIZE,
            keyspace: None,
            resolve_lock_lite_threshold: DEFAULT_RESOLVE_LOCK_LITE_THRESHOLD,
            health_feedback_update_interval: Duration::ZERO,
            txn_local_latches_capacity: 0,
        }
    }
}

impl Config {
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
