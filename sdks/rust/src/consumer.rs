use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use tokio::sync::{mpsc, RwLock};
use tracing::warn;

use crate::connection::AeroConnection;
use crate::error::AeroError;
use crate::protocol::Record;

/// Configuration options for an AeroStream consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Broker endpoint addresses.
    pub bootstrap_servers: Vec<String>,
    /// Target topic to subscribe to.
    pub topic: String,
    /// Target partition to consume from.
    pub partition: u32,
    /// Starting offset for consumption.
    pub initial_offset: u64,
    /// Maximum bytes per fetch request.
    pub max_bytes: u32,
    /// Maximum long-poll wait time on broker in milliseconds.
    pub max_wait_ms: u32,
    /// Optional authentication bearer token.
    pub token: Option<String>,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Poll retry backoff duration when empty or broker throttles.
    pub backoff_on_empty: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9091".to_string()],
            topic: String::new(),
            partition: 0,
            initial_offset: 0,
            max_bytes: 1024 * 1024,
            max_wait_ms: 500,
            token: None,
            connect_timeout: Duration::from_secs(5),
            backoff_on_empty: Duration::from_millis(50),
        }
    }
}

impl ConsumerConfig {
    /// Create a fluent builder for ConsumerConfig.
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }
}

/// Fluent builder for constructing `ConsumerConfig` instances.
#[derive(Default)]
pub struct ConsumerConfigBuilder {
    config: ConsumerConfig,
}

impl ConsumerConfigBuilder {
    /// Set list of broker addresses.
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.config.bootstrap_servers = servers;
        self
    }

    /// Add a single bootstrap broker address.
    pub fn bootstrap_server(mut self, server: impl Into<String>) -> Self {
        self.config.bootstrap_servers = vec![server.into()];
        self
    }

    /// Set topic to consume from.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.config.topic = topic.into();
        self
    }

    /// Set partition index to consume from.
    pub fn partition(mut self, partition: u32) -> Self {
        self.config.partition = partition;
        self
    }

    /// Set initial starting offset.
    pub fn initial_offset(mut self, offset: u64) -> Self {
        self.config.initial_offset = offset;
        self
    }

    /// Set maximum bytes to retrieve per fetch request.
    pub fn max_bytes(mut self, max_bytes: u32) -> Self {
        self.config.max_bytes = max_bytes;
        self
    }

    /// Set broker long-poll wait time in milliseconds.
    pub fn max_wait_ms(mut self, max_wait_ms: u32) -> Self {
        self.config.max_wait_ms = max_wait_ms;
        self
    }

    /// Configure authentication token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// Set connection timeout duration.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Validate and build the `ConsumerConfig`.
    pub fn build(self) -> Result<ConsumerConfig, AeroError> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(AeroError::InvalidConfiguration(
                "bootstrap_servers cannot be empty".into(),
            ));
        }
        if self.config.topic.is_empty() {
            return Err(AeroError::InvalidConfiguration("topic cannot be empty".into()));
        }
        Ok(self.config)
    }
}

/// Continuous stream of records yielding `Result<Record, AeroError>`.
pub struct RecordStream {
    receiver: mpsc::Receiver<Result<Record, AeroError>>,
    worker_handle: tokio::task::JoinHandle<()>,
}

impl Drop for RecordStream {
    fn drop(&mut self) {
        self.worker_handle.abort();
    }
}

impl Stream for RecordStream {
    type Item = Result<Record, AeroError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Asynchronous consumer client for fetching messages from AeroStream.
#[derive(Clone)]
pub struct AeroConsumer {
    config: Arc<ConsumerConfig>,
    connection: Arc<RwLock<Option<AeroConnection>>>,
    current_offset: Arc<AtomicU64>,
}

impl AeroConsumer {
    /// Create a new `AeroConsumer` with the specified configuration.
    pub fn new(config: ConsumerConfig) -> Self {
        let initial_offset = config.initial_offset;
        Self {
            config: Arc::new(config),
            connection: Arc::new(RwLock::new(None)),
            current_offset: Arc::new(AtomicU64::new(initial_offset)),
        }
    }

    /// Get current consumption offset.
    pub fn offset(&self) -> u64 {
        self.current_offset.load(Ordering::Acquire)
    }

    /// Set consumption offset.
    pub fn seek(&self, offset: u64) {
        self.current_offset.store(offset, Ordering::Release);
    }

    /// Obtain an active connection or establish a new one.
    async fn get_or_connect(&self) -> Result<AeroConnection, AeroError> {
        {
            let read = self.connection.read().await;
            if let Some(conn) = &*read {
                if conn.is_alive() {
                    return Ok(conn.clone());
                }
            }
        }

        let mut write = self.connection.write().await;
        if let Some(conn) = &*write {
            if conn.is_alive() {
                return Ok(conn.clone());
            }
        }

        let mut last_err = AeroError::Disconnected;
        for server in &self.config.bootstrap_servers {
            match AeroConnection::connect(
                server,
                self.config.token.as_deref(),
                self.config.connect_timeout,
            )
            .await
            {
                Ok(conn) => {
                    *write = Some(conn.clone());
                    return Ok(conn);
                }
                Err(e) => {
                    warn!(broker = %server, error = %e, "Failed connecting to broker");
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }

    /// Fetch a batch of records using Command 4 (Multi-Entry Long-Polling Fetch).
    pub async fn fetch_multi(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<Vec<Record>, AeroError> {
        let conn = self.get_or_connect().await?;
        match conn
            .fetch_multi(topic, partition, start_offset, max_bytes, max_wait_ms)
            .await
        {
            Ok(records) => Ok(records),
            Err(e) => {
                conn.mark_broken();
                Err(e)
            }
        }
    }

    /// Poll for next available records and automatically advance internal offset.
    pub async fn poll_next_batch(&self) -> Result<Vec<Record>, AeroError> {
        let offset = self.offset();
        let records = self
            .fetch_multi(
                &self.config.topic,
                self.config.partition,
                offset,
                self.config.max_bytes,
                self.config.max_wait_ms,
            )
            .await?;

        if let Some(last) = records.last() {
            self.seek(last.offset + 1);
        }
        Ok(records)
    }

    /// Create a continuous asynchronous stream of records.
    pub fn stream(&self) -> RecordStream {
        let (tx, rx) = mpsc::channel(256);
        let consumer = self.clone();

        let worker_handle = tokio::spawn(async move {
            loop {
                match consumer.poll_next_batch().await {
                    Ok(records) => {
                        if records.is_empty() {
                            tokio::time::sleep(consumer.config.backoff_on_empty).await;
                            continue;
                        }

                        for record in records {
                            if tx.send(Ok(record)).await.is_err() {
                                return; // Downstream receiver dropped
                            }
                        }
                    }
                    Err(err) => {
                        let is_fatal = matches!(&err, AeroError::AuthFailed(_));
                        if tx.send(Err(err)).await.is_err() || is_fatal {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });

        RecordStream {
            receiver: rx,
            worker_handle,
        }
    }
}
