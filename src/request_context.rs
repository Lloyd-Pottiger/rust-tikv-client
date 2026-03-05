// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Stable wrapper types for common `kvrpcpb::Context` fields.
//!
//! TiKV RPCs attach extra metadata via `kvrpcpb::Context` (e.g. priority,
//! isolation level). We expose a small, stable set of enums so applications
//! don't need to depend on the generated protobuf types directly.

/// The priority of commands executed by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum CommandPriority {
    /// Normal is the default value.
    #[default]
    Normal = 0,
    Low = 1,
    High = 2,
}

/// Transaction isolation level for reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum IsolationLevel {
    /// Snapshot isolation (default).
    #[default]
    Si = 0,
    /// Read committed.
    Rc = 1,
    /// Read committed + extra check for more recent versions.
    RcCheckTs = 2,
}

/// Used to tell TiKV whether operations are allowed or not on different disk usages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DiskFullOpt {
    /// Disallow operations on both almost-full and already-full disks.
    #[default]
    NotAllowedOnFull = 0,
    /// Allow operations when disk is almost full.
    AllowedOnAlmostFull = 1,
    /// Allow operations when disk is already full.
    AllowedOnAlreadyFull = 2,
}
