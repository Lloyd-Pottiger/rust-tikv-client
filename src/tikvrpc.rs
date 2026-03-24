//! Lightweight command typing helpers mirroring client-go `tikvrpc::CmdType`.
//!
//! This module intentionally exposes only the command enum and label mapping used by the Rust
//! client's existing typed request pipeline. It does not attempt to recreate client-go's
//! full `tikvrpc::Request` / `Response` wrappers.

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
