use thiserror::Error;

/// Core error types for the AeroStream client SDK.
#[derive(Error, Debug)]
pub enum AeroError {
    /// Authentication handshake failed with broker.
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    /// Protocol framing or decoding error.
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Underlying I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Out of order sequence number encountered during produce (status 45).
    #[error("Out of order sequence number")]
    OutOfOrder,

    /// Connection to the broker was closed or disconnected.
    #[error("Broker connection disconnected")]
    Disconnected,

    /// Operation timed out.
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Configuration parameters are invalid.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}
