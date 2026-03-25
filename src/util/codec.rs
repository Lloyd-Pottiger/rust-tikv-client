//! Codec helpers.
//!
//! This module mirrors client-go's public `util/codec` package layout.
//!
//! The Rust client hosts these helpers under [`crate::kv::codec`]. This module exists mainly as a
//! stable, discoverable namespace for users familiar with client-go.

pub use crate::kv::codec::*;
