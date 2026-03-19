//! Timestamp oracle helper types and functions.
//!
//! This module provides small utilities that mirror client-go `oracle` timestamp helpers (compose
//! and extract physical/logical parts, and convert between wall clock time and a TSO timestamp).

use std::time::Duration;
use std::time::SystemTime;
use std::time::SystemTimeError;
use std::time::UNIX_EPOCH;

/// The shift bit width used by PD timestamps (physical ms + logical part).
///
/// This mirrors client-go `oracle.physicalShiftBits`.
pub const PHYSICAL_SHIFT_BITS: u32 = 18;

const LOGICAL_MASK: u64 = (1u64 << PHYSICAL_SHIFT_BITS) - 1;

/// The default transaction scope for an Oracle service.
///
/// This mirrors client-go `oracle.GlobalTxnScope`.
pub const GLOBAL_TXN_SCOPE: &str = "global";

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
    DurationOverflow,
    #[error("time overflow")]
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

/// An error indicating a read timestamp is in the future.
///
/// This mirrors client-go `oracle.ErrFutureTSRead`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("cannot set read timestamp to a future time, read_ts: {read_ts}, current_ts: {current_ts}")]
pub struct ErrFutureTsRead {
    pub read_ts: u64,
    pub current_ts: u64,
}

/// An error indicating a stale read cannot use `u64::MAX` as a timestamp.
///
/// This mirrors client-go `oracle.ErrLatestStaleRead`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("cannot set read ts to max u64 for stale read")]
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
}
