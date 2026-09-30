//! Production-grade Rust client SDK for the AeroStream native binary protocol.
//! Provides low-latency async publishing, consuming, and connection pooling.
//! Operates over TCP framing with zero-copy buffer parsing and automatic retries.

pub mod client;
pub mod connection;
pub mod consumer;
pub mod error;
pub mod producer;
pub mod protocol;

// Re-export core primitives for ergonomic top-level crate usage.
pub use client::{AeroClient, ClientConfig, ClientConfigBuilder};
pub use connection::{AeroConnection, TransportStream};
pub use consumer::{AeroConsumer, ConsumerConfig, ConsumerConfigBuilder, RecordStream};
pub use error::AeroError;
pub use producer::{AeroProducer, ProducerConfig, ProducerConfigBuilder};
pub use protocol::{Command, Record, MAGIC, STATUS_AUTH_FAILED, STATUS_DATA, STATUS_EMPTY, STATUS_OK, STATUS_OUT_OF_ORDER};
