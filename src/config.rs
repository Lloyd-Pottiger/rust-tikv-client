// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::time::Duration;

use serde_derive::Deserialize;
use serde_derive::Serialize;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PdRetryConfig {
    pub reconnect_interval: Duration,
    pub max_reconnect_attempts: usize,
    pub leader_change_retry: usize,
}

impl Default for PdRetryConfig {
    fn default() -> Self {
        Self {
            reconnect_interval: Duration::from_secs(1),
            max_reconnect_attempts: 5,
            leader_change_retry: 10,
        }
    }
}

/// The configuration for either a [`RawClient`](crate::RawClient) or a
/// [`TransactionClient`](crate::TransactionClient).
///
/// See also [`TransactionOptions`](crate::TransactionOptions) which provides more ways to configure
/// requests.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub timeout: Duration,
    pub keyspace: Option<String>,
    pub pd_retry: PdRetryConfig,
    /// Region cache TTL base (see `region_cache_ttl_jitter`).
    pub region_cache_ttl: Duration,
    /// Adds jitter to region cache TTL to avoid thundering herds.
    ///
    /// The real TTL is in range `[region_cache_ttl, region_cache_ttl + region_cache_ttl_jitter)`.
    pub region_cache_ttl_jitter: Duration,
}

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REGION_CACHE_TTL: Duration = Duration::from_secs(600);
const DEFAULT_REGION_CACHE_TTL_JITTER: Duration = Duration::from_secs(60);

impl Default for Config {
    fn default() -> Self {
        Config {
            ca_path: None,
            cert_path: None,
            key_path: None,
            timeout: DEFAULT_REQUEST_TIMEOUT,
            keyspace: None,
            pd_retry: PdRetryConfig::default(),
            region_cache_ttl: DEFAULT_REGION_CACHE_TTL,
            region_cache_ttl_jitter: DEFAULT_REGION_CACHE_TTL_JITTER,
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

    #[must_use]
    pub fn with_pd_retry_config(mut self, pd_retry: PdRetryConfig) -> Self {
        self.pd_retry = pd_retry;
        self
    }

    /// Configure the region cache TTL base and jitter.
    ///
    /// The cache is best-effort. A shorter TTL reduces staleness but increases PD load.
    ///
    /// # Examples
    /// ```rust
    /// # use tikv_client::Config;
    /// # use std::time::Duration;
    /// let config = Config::default()
    ///     .with_region_cache_ttl(Duration::from_secs(300), Duration::from_secs(30));
    /// ```
    #[must_use]
    pub fn with_region_cache_ttl(mut self, base: Duration, jitter: Duration) -> Self {
        self.region_cache_ttl = base;
        self.region_cache_ttl_jitter = jitter;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_stable() {
        let cfg = Config::default();
        assert_eq!(cfg.timeout, Duration::from_secs(2));
        assert_eq!(cfg.region_cache_ttl, Duration::from_secs(600));
        assert_eq!(cfg.region_cache_ttl_jitter, Duration::from_secs(60));
        assert_eq!(cfg.keyspace, None);
        assert_eq!(cfg.pd_retry, PdRetryConfig::default());

        let pd = PdRetryConfig::default();
        assert_eq!(pd.reconnect_interval, Duration::from_secs(1));
        assert_eq!(pd.max_reconnect_attempts, 5);
        assert_eq!(pd.leader_change_retry, 10);
    }

    #[test]
    fn builder_methods_update_fields() {
        let cfg = Config::default()
            .with_security("ca", "cert", "key")
            .with_timeout(Duration::from_secs(10))
            .with_keyspace("KS")
            .with_region_cache_ttl(Duration::from_secs(3), Duration::from_secs(1));

        assert_eq!(cfg.ca_path, Some(PathBuf::from("ca")));
        assert_eq!(cfg.cert_path, Some(PathBuf::from("cert")));
        assert_eq!(cfg.key_path, Some(PathBuf::from("key")));
        assert_eq!(cfg.timeout, Duration::from_secs(10));
        assert_eq!(cfg.keyspace.as_deref(), Some("KS"));
        assert_eq!(cfg.region_cache_ttl, Duration::from_secs(3));
        assert_eq!(cfg.region_cache_ttl_jitter, Duration::from_secs(1));
    }

    #[test]
    fn default_keyspace_is_default() {
        let cfg = Config::default().with_default_keyspace();
        assert_eq!(cfg.keyspace.as_deref(), Some("DEFAULT"));
    }

    #[test]
    fn pd_retry_config_is_settable() {
        let pd_retry = PdRetryConfig {
            reconnect_interval: Duration::from_secs(9),
            max_reconnect_attempts: 99,
            leader_change_retry: 123,
        };
        let cfg = Config::default().with_pd_retry_config(pd_retry);
        assert_eq!(cfg.pd_retry, pd_retry);
    }
}
