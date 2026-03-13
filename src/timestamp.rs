//! A timestamp returned from the timestamp oracle.
//!
//! The version used in transactions can be converted from a timestamp.
//! The lower 18 (PHYSICAL_SHIFT_BITS) bits are the logical part of the timestamp.
//! The higher bits of the version are the physical part of the timestamp.

pub use crate::proto::pdpb::Timestamp;

const PHYSICAL_SHIFT_BITS: u32 = 18;
const LOGICAL_MASK: u64 = (1u64 << PHYSICAL_SHIFT_BITS) - 1;

/// A helper trait to convert a Timestamp to and from an u64.
///
/// Currently the only implmentation of this trait is [`Timestamp`](Timestamp) in TiKV.
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
        let physical = u64::try_from(self.physical).unwrap_or(0);
        let logical = u64::try_from(self.logical).unwrap_or(0) & LOGICAL_MASK;
        let physical_multiplier = 1u64 << PHYSICAL_SHIFT_BITS;
        physical
            .checked_mul(physical_multiplier)
            .and_then(|physical| physical.checked_add(logical))
            .unwrap_or(u64::MAX)
    }

    fn from_version(version: u64) -> Self {
        Self {
            physical: (version >> PHYSICAL_SHIFT_BITS) as i64,
            logical: (version & LOGICAL_MASK) as i64,
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
    fn test_timestamp_round_trips_large_versions() {
        let versions = [1u64, (1u64 << 32) + 123, (1u64 << 63) + 12345, u64::MAX];

        for version in versions {
            let ts = <Timestamp as TimestampExt>::from_version(version);
            assert_eq!(ts.version(), version);
        }
    }

    #[test]
    fn test_timestamp_try_from_version_zero_returns_none() {
        assert!(<Timestamp as TimestampExt>::try_from_version(0).is_none());
    }

    #[test]
    fn test_timestamp_version_does_not_panic_on_negative_parts() {
        let ts = Timestamp {
            physical: -1,
            logical: -1,
            ..Default::default()
        };
        assert_eq!(ts.version(), 0);

        let ts = Timestamp {
            physical: -1,
            logical: 5,
            ..Default::default()
        };
        assert_eq!(ts.version(), 5);
    }

    #[test]
    fn test_timestamp_version_saturates_on_overflowing_physical() {
        let ts = Timestamp {
            physical: i64::MAX,
            logical: 1,
            ..Default::default()
        };
        assert_eq!(ts.version(), u64::MAX);
    }
}
