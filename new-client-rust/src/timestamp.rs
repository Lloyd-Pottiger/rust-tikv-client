//! A timestamp returned from the timestamp oracle.
//!
//! The version used in transactions can be converted from a timestamp.
//! The lower 18 (PHYSICAL_SHIFT_BITS) bits are the logical part of the timestamp.
//! The higher bits of the version are the physical part of the timestamp.

use std::convert::TryInto;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::internal_err;
use crate::Result;

pub use crate::proto::pdpb::Timestamp;

const PHYSICAL_SHIFT_BITS: u32 = 18;
const LOGICAL_MASK: i64 = (1_i64 << PHYSICAL_SHIFT_BITS) - 1;

/// The default transaction scope used by TiKV's timestamp oracle.
pub const GLOBAL_TXN_SCOPE: &str = "global";

/// Create a transaction timestamp from its physical and logical parts.
///
/// This matches TiKV's timestamp encoding: `(physical_ms << 18) + logical`.
pub fn compose_ts(physical_ms: i64, logical: i64) -> Result<u64> {
    // Use a wider type to avoid debug-overflow panics, and validate the final range.
    let ts: i128 = ((physical_ms as i128) << PHYSICAL_SHIFT_BITS) + (logical as i128);
    ts.try_into().map_err(|_| {
        internal_err!("invalid ts parts (physical_ms={physical_ms}, logical={logical})")
    })
}

/// Extract the physical part (milliseconds since Unix epoch) from a TiKV timestamp.
pub fn extract_physical(ts: u64) -> i64 {
    (ts >> PHYSICAL_SHIFT_BITS) as i64
}

/// Extract the logical part from a TiKV timestamp.
pub fn extract_logical(ts: u64) -> i64 {
    (ts as i64) & LOGICAL_MASK
}

/// Return the physical part (milliseconds since Unix epoch) of the given time.
///
/// This is the same physical component used by TiKV's timestamps.
pub fn get_physical(t: SystemTime) -> Result<i64> {
    let (is_before_epoch, duration) = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (false, d),
        Err(e) => (true, e.duration()),
    };

    let millis: i64 = duration
        .as_millis()
        .try_into()
        .map_err(|_| internal_err!("overflow converting SystemTime to milliseconds"))?;
    Ok(if is_before_epoch { -millis } else { millis })
}

/// Convert a TiKV timestamp to a `SystemTime`.
///
/// The logical part of the timestamp is ignored (millisecond precision only).
pub fn get_time_from_ts(ts: u64) -> SystemTime {
    let ms = extract_physical(ts);
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis((-ms) as u64)
    }
}

/// Convert a `SystemTime` to a TiKV timestamp.
///
/// The logical part of the resulting timestamp is `0` (millisecond precision only).
pub fn time_to_ts(t: SystemTime) -> Result<u64> {
    compose_ts(get_physical(t)?, 0)
}

/// Return the minimum start-ts for uncommitted transactions at `now`.
///
/// This is equivalent to `time_to_ts(now - max_txn_time_use)`.
pub fn lower_limit_start_ts(now: SystemTime, max_txn_time_use: Duration) -> Result<u64> {
    time_to_ts(now - max_txn_time_use)
}

/// A helper trait to convert a Timestamp to and from an u64.
///
/// Currently the only implementation of this trait is [`Timestamp`] in TiKV.
/// It contains a physical part (first 46 bits) and a logical part (last 18 bits).
pub trait TimestampExt: Sized {
    /// Convert the timestamp to u64.
    fn version(&self) -> u64;
    /// Convert u64 to a timestamp.
    fn from_version(version: u64) -> Self;
    /// Convert u64 to an optional timestamp, where `0` represents no timestamp.
    fn try_from_version(version: u64) -> Option<Self>;
}

impl TimestampExt for Timestamp {
    fn version(&self) -> u64 {
        let ts: i128 = ((self.physical as i128) << PHYSICAL_SHIFT_BITS) + (self.logical as i128);
        ts.try_into().unwrap_or(0)
    }

    fn from_version(version: u64) -> Self {
        let version = version as i64;
        Self {
            physical: version >> PHYSICAL_SHIFT_BITS,
            logical: version & LOGICAL_MASK,
            // Now we only support global transactions: suffix_bits: 0,
            ..Default::default()
        }
    }

    fn try_from_version(version: u64) -> Option<Self> {
        if version == 0 {
            None
        } else {
            Some(Self::from_version(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_parts_round_trip() {
        let physical_ms = 1_700_000_123_456_i64;
        let logical = 42_i64;
        let ts = compose_ts(physical_ms, logical).unwrap();
        assert_eq!(extract_physical(ts), physical_ms);
        assert_eq!(extract_logical(ts), logical);
    }

    #[test]
    fn system_time_round_trip_is_ms_precision() {
        let t = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let ts = time_to_ts(t).unwrap();
        assert_eq!(extract_logical(ts), 0);
        assert_eq!(get_time_from_ts(ts), t);
    }

    #[test]
    fn lower_limit_matches_subtracted_time() {
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let max_txn_time_use = Duration::from_secs(2);
        assert_eq!(
            lower_limit_start_ts(now, max_txn_time_use).unwrap(),
            time_to_ts(now - max_txn_time_use).unwrap()
        );
    }

    #[test]
    fn timestamp_ext_matches_helpers() {
        let physical_ms = 1_700_000_123_456_i64;
        let logical = 42_i64;
        let ts = compose_ts(physical_ms, logical).unwrap();

        let ts_struct = Timestamp {
            physical: physical_ms,
            logical,
            ..Default::default()
        };
        assert_eq!(ts_struct.version(), ts);
        assert_eq!(Timestamp::from_version(ts), ts_struct);
    }

    #[test]
    fn compose_ts_rejects_negative_timestamps() {
        assert!(compose_ts(-1, 0).is_err());
    }
}
