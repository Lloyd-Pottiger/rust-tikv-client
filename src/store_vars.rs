//! Store-scoped global variables and helpers mirroring client-go `kv/store_vars.go`.

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use crate::StoreLabel;

static GLOBAL_STORE_LIMIT: AtomicI64 = AtomicI64::new(0);

tokio::task_local! {
    static TASK_STORE_LIMIT: i64;
}

fn normalize_store_limit(limit: i64) -> i64 {
    if limit <= 0 {
        0
    } else {
        limit
    }
}

/// Override the global store request concurrency limit.
///
/// This mirrors client-go `kv.StoreLimit` (a global atomic).
pub fn set_store_limit(limit: i64) {
    GLOBAL_STORE_LIMIT.store(normalize_store_limit(limit), Ordering::Relaxed);
}

/// Get the global store request concurrency limit.
///
/// This mirrors client-go `kv.StoreLimit.Load()`.
#[must_use]
pub fn global_store_limit() -> i64 {
    GLOBAL_STORE_LIMIT.load(Ordering::Relaxed)
}

/// Get the effective store request concurrency limit.
///
/// Prefer [`with_store_limit`] for scoped overrides (for example, in tests).
#[must_use]
pub fn store_limit() -> i64 {
    TASK_STORE_LIMIT
        .try_with(|value| *value)
        .unwrap_or_else(|_| global_store_limit())
}

/// Runs `future` with a task-local store request concurrency limit.
///
/// The task-local value takes precedence over the global default used by [`store_limit`].
pub fn with_store_limit<T, F>(limit: i64, future: F) -> impl std::future::Future<Output = T>
where
    F: std::future::Future<Output = T>,
{
    TASK_STORE_LIMIT.scope(normalize_store_limit(limit), future)
}

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
