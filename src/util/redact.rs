//! Redaction helpers.
//!
//! This module mirrors client-go's public `util/redact` package layout.
//!
//! The Rust client exposes the implementation at [`crate::redact`]. This module exists mainly as a
//! stable, discoverable namespace for users familiar with client-go.

pub use crate::redact::*;
