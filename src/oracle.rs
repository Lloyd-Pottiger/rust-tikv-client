//! Timestamp oracle helper types and functions.
//!
//! This module provides small utilities that mirror client-go `oracle` timestamp helpers (compose
//! and extract physical/logical parts, and convert between wall clock time and a TSO timestamp).

use async_trait::async_trait;
use std::time::Duration;
use std::time::SystemTime;
use std::time::SystemTimeError;
use std::time::UNIX_EPOCH;

use crate::TimestampExt;

/// Oracle implementations mirroring client-go `oracle/oracles`.
pub mod oracles;

/// The shift bit width used by PD timestamps (physical ms + logical part).
///
/// This mirrors client-go `oracle.physicalShiftBits`.
pub const PHYSICAL_SHIFT_BITS: u32 = 18;

const LOGICAL_MASK: u64 = (1u64 << PHYSICAL_SHIFT_BITS) - 1;

/// The default transaction scope for an Oracle service.
///
/// This mirrors client-go `oracle.GlobalTxnScope`.
pub const GLOBAL_TXN_SCOPE: &str = "global";

/// Options passed to the oracle APIs.
///
/// This mirrors client-go `oracle.Option`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OracleOption {
    /// The transaction scope to send when requesting timestamps from PD.
    pub txn_scope: String,
}

impl OracleOption {
    /// Create default oracle options using [`GLOBAL_TXN_SCOPE`].
    pub fn new() -> OracleOption {
        OracleOption::default()
    }
}

/// A dummy read-ts validator that always lets the validation pass.
///
/// This mirrors client-go `oracle.NoopReadTSValidator` and is useful in tests or in code paths that
/// do not require timestamp validation (for example, rawkv operations).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(alias = "NoopReadTSValidator")]
pub struct NoopReadTsValidator;

impl NoopReadTsValidator {
    /// Validate that `read_ts` is safe to use for reads.
    ///
    /// This always returns `Ok(())`.
    pub async fn validate_read_ts(
        &self,
        _read_ts: u64,
        _is_stale_read: bool,
        _opt: &OracleOption,
    ) -> crate::Result<()> {
        Ok(())
    }
}

/// Timestamp oracle interface.
///
/// This is a Rust mapping of client-go `oracle.Oracle`. It is intentionally async and focuses on
/// the parts that are surfaced as public APIs in this crate.
#[async_trait]
pub trait Oracle: Send + Sync {
    /// Get a new monotonic timestamp.
    async fn get_timestamp(&self, opt: &OracleOption) -> crate::Result<u64>;

    /// Get a low-resolution timestamp.
    async fn get_low_resolution_timestamp(&self, opt: &OracleOption) -> crate::Result<u64>;

    /// Set the refresh interval for low-resolution timestamps.
    fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: Duration,
    ) -> crate::Result<()>;

    /// Get a stale timestamp for the given `txn_scope`.
    async fn get_stale_timestamp(&self, txn_scope: &str, prev_seconds: u64) -> crate::Result<u64>;

    /// Get the external timestamp from PD.
    async fn get_external_timestamp(&self) -> crate::Result<u64>;

    /// Set the external timestamp in PD.
    async fn set_external_timestamp(&self, ts: u64) -> crate::Result<()>;

    /// Get a minimum timestamp from all TSO keyspace groups.
    async fn get_all_tso_keyspace_group_min_ts(&self) -> crate::Result<u64>;

    /// Validate that `read_ts` is safe to use for reads.
    async fn validate_read_ts(
        &self,
        read_ts: u64,
        is_stale_read: bool,
        opt: &OracleOption,
    ) -> crate::Result<()>;
}

#[async_trait]
impl Oracle for crate::TransactionClient {
    async fn get_timestamp(&self, opt: &OracleOption) -> crate::Result<u64> {
        Ok(self
            .current_timestamp_with_txn_scope(&opt.txn_scope)
            .await?
            .version())
    }

    async fn get_low_resolution_timestamp(&self, opt: &OracleOption) -> crate::Result<u64> {
        Ok(self
            .low_resolution_timestamp_with_txn_scope(&opt.txn_scope)
            .await?
            .version())
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: Duration,
    ) -> crate::Result<()> {
        crate::TransactionClient::set_low_resolution_timestamp_update_interval(
            self,
            update_interval,
        )
    }

    async fn get_stale_timestamp(&self, txn_scope: &str, prev_seconds: u64) -> crate::Result<u64> {
        Ok(self
            .stale_timestamp_with_txn_scope(txn_scope, prev_seconds)
            .await?
            .version())
    }

    async fn get_external_timestamp(&self) -> crate::Result<u64> {
        self.external_timestamp().await
    }

    async fn set_external_timestamp(&self, ts: u64) -> crate::Result<()> {
        crate::TransactionClient::set_external_timestamp(self, ts).await
    }

    async fn get_all_tso_keyspace_group_min_ts(&self) -> crate::Result<u64> {
        Ok(self
            .current_all_tso_keyspace_group_min_ts()
            .await?
            .version())
    }

    async fn validate_read_ts(
        &self,
        read_ts: u64,
        is_stale_read: bool,
        opt: &OracleOption,
    ) -> crate::Result<()> {
        self.validate_read_ts_with_txn_scope(&opt.txn_scope, read_ts, is_stale_read)
            .await
    }
}

/// Compose a TSO timestamp from physical and logical parts.
///
/// This mirrors client-go `oracle.ComposeTS`:
/// `uint64((physical << physicalShiftBits) + logical)`.
pub fn compose_ts(physical: i64, logical: i64) -> u64 {
    physical
        .wrapping_shl(PHYSICAL_SHIFT_BITS)
        .wrapping_add(logical) as u64
}

/// Extract the physical part (in milliseconds) from a TSO timestamp.
///
/// This mirrors client-go `oracle.ExtractPhysical`.
pub fn extract_physical(ts: u64) -> i64 {
    (ts >> PHYSICAL_SHIFT_BITS) as i64
}

/// Extract the logical part from a TSO timestamp.
///
/// This mirrors client-go `oracle.ExtractLogical`.
pub fn extract_logical(ts: u64) -> i64 {
    (ts & LOGICAL_MASK) as i64
}

/// Errors returned by the time conversion helpers in this module.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TimeConversionError {
    #[error("duration overflow")]
    /// The elapsed duration cannot be represented in the target integer range.
    DurationOverflow,
    #[error("time overflow")]
    /// The converted timestamp or wall-clock time would overflow the target type.
    TimeOverflow,
}

fn millis_from_duration(duration: Duration) -> Result<i64, TimeConversionError> {
    i64::try_from(duration.as_millis()).map_err(|_| TimeConversionError::DurationOverflow)
}

/// Extract the physical part (in milliseconds) from a wall clock time.
///
/// This mirrors client-go `oracle.GetPhysical` (`t.UnixNano()/time.Millisecond`).
pub fn physical_ms_from_system_time(t: SystemTime) -> Result<i64, TimeConversionError> {
    match t.duration_since(UNIX_EPOCH) {
        Ok(duration) => millis_from_duration(duration),
        Err(err) => {
            let duration = system_time_error_duration(err);
            millis_from_duration(duration).map(|ms| -ms)
        }
    }
}

fn system_time_error_duration(err: SystemTimeError) -> Duration {
    err.duration()
}

/// Convert a TSO timestamp to a wall clock time.
///
/// This mirrors client-go `oracle.GetTimeFromTS`. The logical part is ignored.
pub fn system_time_from_ts(ts: u64) -> Result<SystemTime, TimeConversionError> {
    system_time_from_physical_ms(extract_physical(ts))
}

fn system_time_from_physical_ms(ms: i64) -> Result<SystemTime, TimeConversionError> {
    if ms >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_millis(ms as u64))
            .ok_or(TimeConversionError::TimeOverflow)
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_millis(ms.unsigned_abs()))
            .ok_or(TimeConversionError::TimeOverflow)
    }
}

/// Convert a wall clock time to a TSO timestamp.
///
/// This mirrors client-go `oracle.GoTimeToTS`. The logical part is always 0.
pub fn ts_from_system_time(t: SystemTime) -> Result<u64, TimeConversionError> {
    Ok(compose_ts(physical_ms_from_system_time(t)?, 0))
}

/// Return the min start_ts of an uncommitted transaction.
///
/// This mirrors client-go `oracle.GoTimeToLowerLimitStartTS`.
pub fn lower_limit_start_ts(
    now: SystemTime,
    max_txn_time_use_ms: i64,
) -> Result<u64, TimeConversionError> {
    let target = if max_txn_time_use_ms >= 0 {
        now.checked_sub(Duration::from_millis(max_txn_time_use_ms as u64))
    } else {
        now.checked_add(Duration::from_millis(max_txn_time_use_ms.unsigned_abs()))
    }
    .ok_or(TimeConversionError::TimeOverflow)?;
    ts_from_system_time(target)
}

/// Return whether `lock_ts + ttl_ms` is expired, compared to `current_ts`.
///
/// This mirrors client-go `Oracle.IsExpired`, but is implemented as a pure helper.
pub fn is_expired(lock_ts: u64, ttl_ms: u64, current_ts: u64) -> bool {
    let ttl_ms = i64::try_from(ttl_ms).unwrap_or(i64::MAX);
    let expires_at = extract_physical(lock_ts).saturating_add(ttl_ms);
    extract_physical(current_ts) >= expires_at
}

/// Return the remaining milliseconds until `lock_ts + ttl_ms` expires.
///
/// This mirrors client-go `Oracle.UntilExpired`, but is implemented as a pure helper.
pub fn until_expired_ms(lock_ts: u64, ttl_ms: u64, current_ts: u64) -> i64 {
    let ttl_ms = i64::try_from(ttl_ms).unwrap_or(i64::MAX);
    let expires_at = extract_physical(lock_ts).saturating_add(ttl_ms);
    let current = extract_physical(current_ts);

    let remaining = i128::from(expires_at) - i128::from(current);
    remaining.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// An error indicating a read timestamp is in the future.
///
/// This mirrors client-go `oracle.ErrFutureTSRead`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("cannot set read timestamp to a future time, readTS: {read_ts}, currentTS: {current_ts}")]
pub struct ErrFutureTsRead {
    /// The requested read timestamp that lies in the future.
    pub read_ts: u64,
    /// The current timestamp observed when validating the request.
    pub current_ts: u64,
}

/// An error indicating a stale read cannot use `u64::MAX` as a timestamp.
///
/// This mirrors client-go `oracle.ErrLatestStaleRead`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("cannot set read ts to max uint64 for stale read")]
pub struct ErrLatestStaleRead;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compose_and_extract_ts_round_trip() {
        let physical = 123i64;
        let logical = 456i64;
        let ts = compose_ts(physical, logical);
        assert_eq!(extract_physical(ts), physical);
        assert_eq!(extract_logical(ts), logical);
    }

    #[test]
    fn test_ts_system_time_round_trip_epoch() {
        let ts = ts_from_system_time(UNIX_EPOCH).unwrap();
        assert_eq!(ts, 0);
        assert_eq!(system_time_from_ts(ts).unwrap(), UNIX_EPOCH);
    }

    #[test]
    fn test_physical_ms_supports_times_before_epoch() {
        let before = UNIX_EPOCH.checked_sub(Duration::from_millis(42)).unwrap();
        assert_eq!(physical_ms_from_system_time(before).unwrap(), -42);
    }

    #[test]
    fn test_system_time_from_ts_ignores_logical_part() {
        let ts = compose_ts(10, 123);
        assert_eq!(
            system_time_from_ts(ts).unwrap(),
            UNIX_EPOCH.checked_add(Duration::from_millis(10)).unwrap()
        );
    }

    #[test]
    fn test_is_expired_and_until_expired_ms() {
        let lock_ts = compose_ts(1000, 0);
        let ttl_ms = 500;

        let current_ts = compose_ts(1499, 0);
        assert!(!is_expired(lock_ts, ttl_ms, current_ts));
        assert_eq!(until_expired_ms(lock_ts, ttl_ms, current_ts), 1);

        let current_ts = compose_ts(1500, 0);
        assert!(is_expired(lock_ts, ttl_ms, current_ts));
        assert_eq!(until_expired_ms(lock_ts, ttl_ms, current_ts), 0);

        let current_ts = compose_ts(1501, 0);
        assert!(is_expired(lock_ts, ttl_ms, current_ts));
        assert_eq!(until_expired_ms(lock_ts, ttl_ms, current_ts), -1);
    }

    #[test]
    fn test_transaction_client_implements_oracle_trait() {
        fn assert_oracle<T: Oracle>() {}
        assert_oracle::<crate::TransactionClient>();
    }
}
