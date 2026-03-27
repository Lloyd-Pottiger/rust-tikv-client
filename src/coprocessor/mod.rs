//! Coprocessor request helpers and protobuf re-exports.
//!
//! This namespace mirrors the user-facing parts of client-go's coprocessor support. It exposes
//! request-lowering helpers, stream request wrappers, and the generated protobuf request/response
//! types under a stable module path.

use std::any::Any;

use tonic::codec::Streaming;

use crate::Error;
use crate::Result;

mod requests;

pub mod lowering;

pub use requests::CoprocessorStreamRequest;

// Re-export protobuf-generated coprocessor types under `tikv_client::coprocessor::*` so downstream
// code can build/dispatch requests without importing `tikv_client::proto::*` directly.
pub use crate::proto::coprocessor::*;

pub type CoprocessorResponseStream = Streaming<Response>;
pub type BatchCoprocessorResponseStream = Streaming<BatchResponse>;

pub fn downcast_coprocessor_response_stream(
    response: Box<dyn Any>,
) -> Result<CoprocessorResponseStream> {
    response
        .downcast::<CoprocessorResponseStream>()
        .map(|stream| *stream)
        .map_err(|_| Error::InternalError {
            message: "expected coprocessor stream response".to_owned(),
        })
}

pub fn downcast_batch_coprocessor_response_stream(
    response: Box<dyn Any>,
) -> Result<BatchCoprocessorResponseStream> {
    response
        .downcast::<BatchCoprocessorResponseStream>()
        .map(|stream| *stream)
        .map_err(|_| Error::InternalError {
            message: "expected batch coprocessor stream response".to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downcast_helpers_reject_unexpected_types() {
        let err = downcast_coprocessor_response_stream(Box::new(()) as Box<dyn Any>)
            .expect_err("expected downcast error");
        assert!(matches!(err, Error::InternalError { .. }));

        let err = downcast_batch_coprocessor_response_stream(Box::new(()) as Box<dyn Any>)
            .expect_err("expected downcast error");
        assert!(matches!(err, Error::InternalError { .. }));
    }
}
