//! Lightweight TiKV RPC helpers mirroring client-go `tikvrpc`.
//!
//! The Rust client uses a typed protobuf request pipeline internally, but still needs a small set
//! of stable identifiers and views for metrics, tracing, and optional hooks.
//!
//! This module provides:
//! - [`CmdType`] and [`EndpointType`] for upstream-compatible identifiers and label mappings.
//! - [`Request`] and [`Response`], lightweight wrappers that expose a stable `label` / [`CmdType`]
//!   plus common context accessors.

use std::any::Any;
use std::fmt;

/// A lightweight view of a TiKV RPC request.
///
/// This is a compatibility alias for [`crate::RpcRequest`], re-exported from this module so users
/// familiar with client-go can reach it via `tikv_client::tikvrpc::Request`.
pub use crate::RpcRequest as Request;

/// A lightweight view of a TiKV RPC response.
///
/// Unlike client-go, the Rust client does not currently expose a single unified request/response
/// send API. This wrapper exists primarily for hooks that need access to a stable label (and thus
/// [`CmdType`]) plus a way to downcast into the concrete protobuf response type.
#[derive(Clone, Copy, Debug)]
pub struct Response<'a> {
    label: &'static str,
    response: &'a dyn Any,
}

impl<'a> Response<'a> {
    /// Create a new response wrapper.
    #[must_use]
    pub fn new(label: &'static str, response: &'a dyn Any) -> Self {
        Self { label, response }
    }

    /// A stable response label (for example, `"kv_get"` or `"kv_commit"`).
    #[must_use]
    pub fn label(self) -> &'static str {
        self.label
    }

    /// The stable command type derived from [`Self::label`].
    #[must_use]
    pub fn cmd_type(self) -> CmdType {
        CmdType::from_label(self.label)
    }

    /// Returns the underlying response as an [`Any`] trait object.
    #[must_use]
    pub fn as_any(self) -> &'a dyn Any {
        self.response
    }

    /// Downcast the response to the expected concrete type.
    #[must_use]
    pub fn downcast_ref<T: Any>(self) -> Option<&'a T> {
        self.response.downcast_ref::<T>()
    }
}

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

    #[test]
    fn test_request_alias_has_cmd_type_and_accessors() {
        let mut ctx = crate::proto::kvrpcpb::Context::default();
        let mut req = Request::new("mock://tikv", "raw_get", Some(&mut ctx));

        assert_eq!(req.label(), "raw_get");
        assert_eq!(req.cmd_type(), CmdType::RawGet);

        assert_eq!(req.request_source(), None);
        req.set_request_source("tests");
        assert_eq!(req.request_source(), Some("tests"));
    }

    #[test]
    fn test_response_wrapper_cmd_type_and_downcast() {
        #[derive(Debug)]
        struct MockResponse {
            value: u64,
        }

        let response = MockResponse { value: 42 };
        let wrapper = Response::new("raw_get", &response);

        assert_eq!(wrapper.label(), "raw_get");
        assert_eq!(wrapper.cmd_type(), CmdType::RawGet);
        assert_eq!(wrapper.downcast_ref::<MockResponse>().unwrap().value, 42);
        assert!(wrapper.downcast_ref::<u32>().is_none());
    }
}
