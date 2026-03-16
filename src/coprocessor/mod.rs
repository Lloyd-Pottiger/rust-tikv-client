mod requests;

pub mod lowering;

pub use requests::CoprocessorStreamRequest;

// Re-export protobuf-generated coprocessor types under `tikv_client::coprocessor::*` so downstream
// code can build/dispatch requests without importing `tikv_client::proto::*` directly.
pub use crate::proto::coprocessor::*;
