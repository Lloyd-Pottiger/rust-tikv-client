//! Oracle implementations mirroring client-go `oracle/oracles`.
//!
//! The Rust client uses PD as its primary oracle (exposed as [`crate::TransactionClient`], which
//! implements [`crate::oracle::Oracle`]). This module provides additional oracle implementations
//! that are useful for tests or environments where PD is not available.

use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use async_trait::async_trait;

use super::Oracle;
use super::OracleOption;

/// PD-backed oracle implementation.
///
/// In the Rust client, this is [`crate::TransactionClient`], which implements [`crate::oracle::Oracle`].
#[doc(inline)]
pub use crate::TransactionClient as PdOracle;

#[derive(Debug, Default)]
struct LocalOracleState {
    last_timestamp_ts: u64,
    last_timestamp_seq: u64,
    external_timestamp: u64,
}

/// An oracle implementation backed by local wall clock time.
///
/// This mirrors client-go `oracle/oracles.NewLocalOracle`.
#[derive(Debug, Default)]
pub struct LocalOracle {
    state: Mutex<LocalOracleState>,
}

impl LocalOracle {
    /// Create a new local oracle.
    #[must_use]
    pub fn new() -> LocalOracle {
        LocalOracle::default()
    }

    fn timestamp_from_now(&self, now: SystemTime) -> crate::Result<u64> {
        let ts = super::ts_from_system_time(now)
            .map_err(|e| crate::Error::StringError(e.to_string()))?;

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if ts > state.last_timestamp_ts {
            state.last_timestamp_ts = ts;
            state.last_timestamp_seq = 0;
            return Ok(ts);
        }

        // Ensure strict monotonicity even when time is unchanged (or moves backwards).
        state.last_timestamp_seq = state.last_timestamp_seq.saturating_add(1);
        Ok(state
            .last_timestamp_ts
            .saturating_add(state.last_timestamp_seq))
    }

    fn stale_timestamp_from_now(&self, now: SystemTime, prev_seconds: u64) -> crate::Result<u64> {
        let target = now
            .checked_sub(Duration::from_secs(prev_seconds))
            .ok_or_else(|| {
                crate::Error::StringError(
                    "time overflow while computing stale timestamp".to_owned(),
                )
            })?;

        super::ts_from_system_time(target).map_err(|e| crate::Error::StringError(e.to_string()))
    }
}

/// Create an Oracle that uses local time as its data source.
///
/// This mirrors client-go `oracle/oracles.NewLocalOracle`.
#[doc(alias = "NewLocalOracle")]
#[must_use]
pub fn new_local_oracle() -> LocalOracle {
    LocalOracle::new()
}

/// Create a PD-backed oracle client by connecting to the given PD endpoints.
///
/// This mirrors client-go `oracle/oracles.NewPdOracle` as a convenience constructor.
#[doc(alias = "NewPdOracle")]
pub async fn new_pd_oracle<S: Into<String>>(pd_endpoints: Vec<S>) -> crate::Result<PdOracle> {
    crate::TransactionClient::new(pd_endpoints).await
}

/// Create a PD-backed oracle client by connecting to the given PD endpoints with the provided config.
///
/// This mirrors client-go `oracle/oracles.NewPdOracle` options as a convenience constructor.
#[doc(alias = "NewPdOracle")]
pub async fn new_pd_oracle_with_config<S: Into<String>>(
    pd_endpoints: Vec<S>,
    config: crate::Config,
) -> crate::Result<PdOracle> {
    crate::TransactionClient::new_with_config(pd_endpoints, config).await
}

#[async_trait]
impl Oracle for LocalOracle {
    async fn get_timestamp(&self, _opt: &OracleOption) -> crate::Result<u64> {
        self.timestamp_from_now(SystemTime::now())
    }

    async fn get_low_resolution_timestamp(&self, opt: &OracleOption) -> crate::Result<u64> {
        self.get_timestamp(opt).await
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        _update_interval: Duration,
    ) -> crate::Result<()> {
        Ok(())
    }

    async fn get_stale_timestamp(&self, _txn_scope: &str, prev_seconds: u64) -> crate::Result<u64> {
        self.stale_timestamp_from_now(SystemTime::now(), prev_seconds)
    }

    async fn get_external_timestamp(&self) -> crate::Result<u64> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(state.external_timestamp)
    }

    async fn set_external_timestamp(&self, ts: u64) -> crate::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.external_timestamp = ts;
        Ok(())
    }

    async fn get_all_tso_keyspace_group_min_ts(&self) -> crate::Result<u64> {
        super::ts_from_system_time(SystemTime::now())
            .map_err(|e| crate::Error::StringError(e.to_string()))
    }

    async fn validate_read_ts(
        &self,
        _read_ts: u64,
        _is_stale_read: bool,
        _opt: &OracleOption,
    ) -> crate::Result<()> {
        Ok(())
    }
}
