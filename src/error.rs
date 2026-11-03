//! Client error types.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),

    #[error("Internal error: {0}")]
    Internal(String),

    /// The segment evaluator rejected a signal or segment config call.
    #[error("segment config API {status}: {message}")]
    SegmentConfig { status: u16, message: String },
}
