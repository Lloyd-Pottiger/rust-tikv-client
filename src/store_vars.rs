//! Store-scoped global variables and helpers mirroring client-go `kv/store_vars.go`.

use crate::StoreLabel;

/// Classifies whether a request targets the local zone or crosses zones.
///
/// This mirrors client-go `kv.AccessLocationType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum AccessLocationType {
    /// The access location cannot be determined (missing/empty zone label configuration).
    Unknown = 0,
    /// The request targets a store in the same zone as the local client.
    LocalZone = 1,
    /// The request targets a store in a different zone.
    CrossZone = 2,
}

const ZONE_LABEL_KEY: &str = "zone";

fn store_zone_label(store_labels: &[StoreLabel]) -> Option<&str> {
    store_labels
        .iter()
        .find(|label| label.key == ZONE_LABEL_KEY)
        .map(|label| label.value.as_str())
}

/// Compute the [`AccessLocationType`] for a store based on its labels.
///
/// Returns [`AccessLocationType::Unknown`] when either:
/// - the local zone label is not configured, or
/// - the target store does not report a `zone` label.
#[must_use]
pub fn access_location_type(
    self_zone_label: Option<&str>,
    store_labels: &[StoreLabel],
) -> AccessLocationType {
    let Some(self_zone_label) = self_zone_label else {
        return AccessLocationType::Unknown;
    };
    if self_zone_label.is_empty() {
        return AccessLocationType::Unknown;
    }
    let Some(target_zone_label) = store_zone_label(store_labels) else {
        return AccessLocationType::Unknown;
    };
    if target_zone_label.is_empty() {
        return AccessLocationType::Unknown;
    }

    if self_zone_label == target_zone_label {
        AccessLocationType::LocalZone
    } else {
        AccessLocationType::CrossZone
    }
}
