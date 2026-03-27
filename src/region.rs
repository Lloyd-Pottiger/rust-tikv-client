// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use derive_new::new;

use crate::proto::metapb;
use crate::Error;
use crate::Key;
use crate::Result;

/// The numeric ID of a TiKV region.
pub type RegionId = u64;
/// The numeric ID of a TiKV store.
pub type StoreId = u64;

/// The ID and version information of a region.
#[derive(Eq, PartialEq, Hash, Clone, Default, Debug)]
pub struct RegionVerId {
    /// The stable region ID.
    pub id: RegionId,
    /// The configuration version, incremented when peers are added or removed.
    pub conf_ver: u64,
    /// The data version, incremented when the region is split or merged.
    pub ver: u64,
}

/// Information about a TiKV region and its leader.
///
/// In TiKV all data is partitioned by range. Each partition is called a region.
#[derive(new, Clone, Default, Debug, PartialEq)]
pub struct RegionWithLeader {
    /// Region metadata, including key range and epoch information.
    pub region: metapb::Region,
    /// The currently known leader peer, if PD returned one.
    pub leader: Option<metapb::Peer>,
}

impl Eq for RegionWithLeader {}

impl RegionWithLeader {
    /// Returns whether `key` falls inside this region's half-open key range.
    ///
    /// An empty end key means the region is open-ended on the right.
    pub fn contains(&self, key: &Key) -> bool {
        let key: &[u8] = key.into();
        let start_key = &self.region.start_key;
        let end_key = &self.region.end_key;
        key >= start_key.as_slice() && (key < end_key.as_slice() || end_key.is_empty())
    }

    /// Returns the inclusive start key of this region.
    pub fn start_key(&self) -> Key {
        self.region.start_key.to_vec().into()
    }

    /// Returns the exclusive end key of this region.
    ///
    /// An empty key means the region extends to the end of the keyspace.
    pub fn end_key(&self) -> Key {
        self.region.end_key.to_vec().into()
    }

    /// Returns this region's `(start_key, end_key)` pair.
    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }

    /// Returns the versioned region identity derived from the protobuf metadata.
    ///
    /// Missing epoch information is treated as all-zero version fields.
    pub fn ver_id(&self) -> RegionVerId {
        let region = &self.region;
        let default_epoch = metapb::RegionEpoch::default();
        let epoch = region.region_epoch.as_ref().unwrap_or(&default_epoch);
        RegionVerId {
            id: region.id,
            conf_ver: epoch.conf_ver,
            ver: epoch.version,
        }
    }

    /// Returns this region's stable numeric ID.
    pub fn id(&self) -> RegionId {
        self.region.id
    }

    /// Returns the leader's store ID, or an error if no leader is currently known.
    pub fn get_store_id(&self) -> Result<StoreId> {
        self.leader
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::LeaderNotFound {
                region: self.ver_id(),
            })
            .map(|s| s.store_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_with_leader_ver_id_handles_missing_epoch() {
        let region = RegionWithLeader::default();
        assert_eq!(
            region.ver_id(),
            RegionVerId {
                id: 0,
                conf_ver: 0,
                ver: 0,
            }
        );
    }

    #[test]
    fn test_region_with_leader_ver_id_uses_epoch_when_present() {
        let mut region = metapb::Region::default();
        region.id = 42;
        region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 7,
            version: 9,
        });

        let region = RegionWithLeader {
            region,
            leader: None,
        };
        assert_eq!(
            region.ver_id(),
            RegionVerId {
                id: 42,
                conf_ver: 7,
                ver: 9,
            }
        );
    }
}
