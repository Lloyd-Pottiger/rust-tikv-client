// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Re-exported protobuf and gRPC bindings used by TiKV and PD.
//!
//! The generated code itself lives under `src/generated/`, but this module provides the stable
//! crate-public namespace for downstream users that need direct access to protobuf messages or
//! service clients.

#![allow(clippy::large_enum_variant)]
#![allow(clippy::enum_variant_names)]

pub use protos::*;

#[allow(clippy::doc_lazy_continuation)]
mod protos {
    include!("generated/mod.rs");
}
