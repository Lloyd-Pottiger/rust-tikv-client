// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Race-detector build helpers.
//!
//! This mirrors the client-go `util/israce` package layout.

/// Whether the crate is built with a race detector enabled.
///
/// Rust client builds do not currently toggle a dedicated race-detector cfg, so this stays `false`
/// unless the crate grows an explicit equivalent in the future.
#[doc(alias = "RaceEnabled")]
pub const RACE_ENABLED: bool = false;
