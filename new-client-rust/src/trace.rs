//! Request tracing controls (TiKV-side trace flags).
//!
//! TiKV supports attaching a trace id and a set of control flags to the per-request
//! `kvrpcpb::Context`. This module provides a small, Rust-idiomatic representation of those flags.

/// A trace category (as used by TiKV-side tracing).
pub type Category = u32;

pub const CATEGORY_KV_REQUEST: Category = 1;
pub const CATEGORY_REGION_CACHE: Category = 2;
pub const CATEGORY_TXN_2PC: Category = 3;
pub const CATEGORY_TXN_LOCK_RESOLVE: Category = 4;

/// Trace control flags carried by `kvrpcpb::Context.trace_control_flags`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TraceControlFlags(pub u64);

impl TraceControlFlags {
    #[inline]
    pub const fn has(self, flag: TraceControlFlags) -> bool {
        (self.0 & flag.0) != 0
    }

    #[inline]
    pub const fn with(self, flag: TraceControlFlags) -> TraceControlFlags {
        TraceControlFlags(self.0 | flag.0)
    }
}

pub const FLAG_IMMEDIATE_LOG: TraceControlFlags = TraceControlFlags(1 << 0);
pub const FLAG_TIKV_CATEGORY_REQUEST: TraceControlFlags = TraceControlFlags(1 << 1);
pub const FLAG_TIKV_CATEGORY_WRITE_DETAILS: TraceControlFlags = TraceControlFlags(1 << 2);
pub const FLAG_TIKV_CATEGORY_READ_DETAILS: TraceControlFlags = TraceControlFlags(1 << 3);
