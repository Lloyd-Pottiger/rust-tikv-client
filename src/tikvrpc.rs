//! Lightweight TiKV RPC typing helpers mirroring client-go `tikvrpc`.
//!
//! This module intentionally exposes only small stable enums and label mappings used by the Rust
//! client's existing typed request pipeline. It does not attempt to recreate client-go's full
//! `tikvrpc::Request` / `Response` wrappers.

use std::fmt;

macro_rules! cmd_type_table {
    ($(($variant:ident, $label:literal, $name:literal),)*) => {
        /// Stable command type identifiers for TiKV RPCs.
        ///
        /// Variants primarily mirror client-go `tikvrpc::CmdType`, with a few Rust-specific
        /// additions for commands that exist in this client but do not have a direct upstream
        /// `CmdType` counterpart.
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[non_exhaustive]
        pub enum CmdType {
            $($variant,)*
            /// Command label not known to this compatibility layer.
            Unknown,
        }

        impl CmdType {
            /// Convert an internal request label into a [`CmdType`].
            #[must_use]
            pub fn from_label(label: &str) -> Self {
                match label {
                    $($label => Self::$variant,)*
                    _ => Self::Unknown,
                }
            }

            /// Return the canonical command name.
            ///
            /// For upstream-compatible commands this matches client-go `CmdType.String()`.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)*
                    Self::Unknown => "Unknown",
                }
            }
        }
    };
}

cmd_type_table! {
    (Get, "kv_get", "Get"),
    (Scan, "kv_scan", "Scan"),
    (Prewrite, "kv_prewrite", "Prewrite"),
    (Commit, "kv_commit", "Commit"),
    (Cleanup, "kv_cleanup", "Cleanup"),
    (BatchGet, "kv_batch_get", "BatchGet"),
    (BatchRollback, "kv_batch_rollback", "BatchRollback"),
    (ScanLock, "kv_scan_lock", "ScanLock"),
    (ResolveLock, "kv_resolve_lock", "ResolveLock"),
    (Gc, "kv_gc", "GC"),
    (DeleteRange, "kv_delete_range", "DeleteRange"),
    (PessimisticLock, "kv_pessimistic_lock", "PessimisticLock"),
    (PessimisticRollback, "kv_pessimistic_rollback", "PessimisticRollback"),
    (TxnHeartBeat, "kv_txn_heart_beat", "TxnHeartBeat"),
    (CheckTxnStatus, "kv_check_txn_status", "CheckTxnStatus"),
    (CheckSecondaryLocks, "kv_check_secondary_locks_request", "CheckSecondaryLocks"),
    (FlashbackToVersion, "kv_flashback_to_version", "FlashbackToVersion"),
    (PrepareFlashbackToVersion, "kv_prepare_flashback_to_version", "PrepareFlashbackToVersion"),
    (Flush, "kv_flush", "Flush"),
    (BufferBatchGet, "kv_buffer_batch_get", "BufferBatchGet"),
    (RawGet, "raw_get", "RawGet"),
    (RawBatchGet, "raw_batch_get", "RawBatchGet"),
    (RawPut, "raw_put", "RawPut"),
    (RawBatchPut, "raw_batch_put", "RawBatchPut"),
    (RawDelete, "raw_delete", "RawDelete"),
    (RawBatchDelete, "raw_batch_delete", "RawBatchDelete"),
    (RawDeleteRange, "raw_delete_range", "RawDeleteRange"),
    (RawScan, "raw_scan", "RawScan"),
    (RawBatchScan, "raw_batch_scan", "RawBatchScan"),
    (RawGetKeyTtl, "raw_get_key_ttl", "RawGetKeyTTL"),
    (RawCompareAndSwap, "raw_compare_and_swap", "RawCompareAndSwap"),
    (RawChecksum, "raw_checksum", "RawChecksum"),
    (RawCoprocessor, "raw_coprocessor", "RawCoprocessor"),
    (UnsafeDestroyRange, "unsafe_destroy_range", "UnsafeDestroyRange"),
    (RegisterLockObserver, "register_lock_observer", "RegisterLockObserver"),
    (CheckLockObserver, "check_lock_observer", "CheckLockObserver"),
    (RemoveLockObserver, "remove_lock_observer", "RemoveLockObserver"),
    (PhysicalScanLock, "physical_scan_lock", "PhysicalScanLock"),
    (StoreSafeTs, "get_store_safe_ts", "StoreSafeTS"),
    (LockWaitInfo, "get_lock_wait_info", "LockWaitInfo"),
    (GetLockWaitHistory, "get_lock_wait_history", "GetLockWaitHistory"),
    (GetHealthFeedback, "get_health_feedback", "GetHealthFeedback"),
    (BroadcastTxnStatus, "broadcast_txn_status", "BroadcastTxnStatus"),
    (Cop, "coprocessor", "Cop"),
    (BatchCop, "batch_coprocessor", "BatchCop"),
    (SplitRegion, "split_region", "SplitRegion"),
    (Compact, "compact", "Compact"),
    (GetTiFlashSystemTable, "get_ti_flash_system_table", "GetTiFlashSystemTable"),
}

impl fmt::Display for CmdType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Types of remote endpoints.
///
/// This mirrors client-go `tikvrpc::EndpointType`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum EndpointType {
    /// A TiKV server.
    TiKv,
    /// A TiFlash server.
    TiFlash,
    /// A TiDB server.
    TiDb,
    /// A TiFlash compute node.
    TiFlashCompute,
}

impl EndpointType {
    /// Returns the name of this endpoint type.
    ///
    /// This mirrors client-go `EndpointType.Name()`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            EndpointType::TiKv => "tikv",
            EndpointType::TiFlash => "tiflash",
            EndpointType::TiDb => "tidb",
            EndpointType::TiFlashCompute => "tiflash_compute",
        }
    }

    /// Returns true if this endpoint is TiFlash-related.
    ///
    /// This mirrors client-go `EndpointType.IsTiFlashRelatedType()`.
    #[must_use]
    pub const fn is_tiflash_related_type(self) -> bool {
        matches!(self, EndpointType::TiFlash | EndpointType::TiFlashCompute)
    }

    /// Determine the endpoint type from store labels.
    ///
    /// This maps to client-go `GetStoreTypeByMeta`, but operates on the store's label list to
    /// avoid exposing the full `metapb::Store` protobuf message.
    #[must_use]
    pub fn from_store_labels(labels: &[crate::StoreLabel]) -> EndpointType {
        for label in labels {
            if label.key == ENGINE_LABEL_KEY && label.value == ENGINE_LABEL_TIFLASH {
                return EndpointType::TiFlash;
            }
            if label.key == ENGINE_LABEL_KEY && label.value == ENGINE_LABEL_TIFLASH_COMPUTE {
                return EndpointType::TiFlashCompute;
            }
        }
        EndpointType::TiKv
    }
}

impl fmt::Display for EndpointType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Constants to determine engine type.
///
/// They should be kept in sync with PD and client-go.
pub const ENGINE_LABEL_KEY: &str = "engine";
pub const ENGINE_LABEL_TIFLASH: &str = "tiflash";
pub const ENGINE_LABEL_TIFLASH_COMPUTE: &str = "tiflash_compute";

/// Constants to determine engine role.
///
/// They should be kept in sync with PD and client-go.
pub const ENGINE_ROLE_LABEL_KEY: &str = "engine_role";
pub const ENGINE_ROLE_WRITE: &str = "write";

#[cfg(test)]
mod tests {
    use super::*;

    fn label(key: &str, value: &str) -> crate::StoreLabel {
        crate::StoreLabel {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn test_endpoint_type_name_and_tiflash_related() {
        assert_eq!(EndpointType::TiKv.name(), "tikv");
        assert_eq!(EndpointType::TiFlash.name(), "tiflash");
        assert_eq!(EndpointType::TiDb.name(), "tidb");
        assert_eq!(EndpointType::TiFlashCompute.name(), "tiflash_compute");

        assert!(!EndpointType::TiKv.is_tiflash_related_type());
        assert!(EndpointType::TiFlash.is_tiflash_related_type());
        assert!(!EndpointType::TiDb.is_tiflash_related_type());
        assert!(EndpointType::TiFlashCompute.is_tiflash_related_type());
    }

    #[test]
    fn test_endpoint_type_from_store_labels() {
        assert_eq!(EndpointType::from_store_labels(&[]), EndpointType::TiKv);

        let tiflash_labels = [label(ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH)];
        assert_eq!(
            EndpointType::from_store_labels(&tiflash_labels),
            EndpointType::TiFlash
        );

        let compute_labels = [label(ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH_COMPUTE)];
        assert_eq!(
            EndpointType::from_store_labels(&compute_labels),
            EndpointType::TiFlashCompute
        );

        let unrelated = [
            label("zone", "z1"),
            label(ENGINE_LABEL_KEY, "not-a-store-type"),
        ];
        assert_eq!(
            EndpointType::from_store_labels(&unrelated),
            EndpointType::TiKv
        );
    }
}
