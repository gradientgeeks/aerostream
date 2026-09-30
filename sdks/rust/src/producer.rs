use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::connection::AeroConnection;
use crate::error::AeroError;

/// Configuration settings for an AeroStream producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Broker endpoint addresses (e.g., `["127.0.0.1:9091"]`).
    pub bootstrap_servers: Vec<String>,
    /// Optional authentication bearer token.
    pub token: Option<String>,
    /// Maximum retry attempts for transient failures.
    pub max_retries: usize,
    /// Initial backoff duration before first retry.
    pub initial_backoff: Duration,
    /// Upper bound on backoff sleep duration.
    pub max_backoff: Duration,
    /// Multiplier applied to backoff for each subsequent retry.
    pub backoff_multiplier: f64,
    /// Socket connection timeout.
    pub connect_timeout: Duration,
    /// Produce request round-trip timeout.
    pub request_timeout: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9091".to_string()],
            token: None,
            max_retries: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(2000),
            backoff_multiplier: 2.0,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl ProducerConfig {
    /// Create a fluent builder for ProducerConfig.
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }
}

/// Fluent builder for constructing `ProducerConfig` instances.
#[derive(Default)]
pub struct ProducerConfigBuilder {
    config: ProducerConfig,
}

impl ProducerConfigBuilder {
    /// Set the list of broker endpoints.
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.config.bootstrap_servers = servers;
        self
    }

    /// Add a single broker endpoint address.
    pub fn bootstrap_server(mut self, server: impl Into<String>) -> Self {
        self.config.bootstrap_servers = vec![server.into()];
        self
    }

    /// Configure authentication token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// Set maximum retry attempts.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set initial retry backoff duration.
    pub fn initial_backoff(mut self, backoff: Duration) -> Self {
        self.config.initial_backoff = backoff;
        self
    }

    /// Set maximum retry backoff duration.
    pub fn max_backoff(mut self, backoff: Duration) -> Self {
        self.config.max_backoff = backoff;
        self
    }

    /// Set retry backoff multiplier.
    pub fn backoff_multiplier(mut self, mult: f64) -> Self {
        self.config.backoff_multiplier = mult;
        self
    }

    /// Set connection timeout duration.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set request timeout duration.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Validate and build the `ProducerConfig`.
    pub fn build(self) -> Result<ProducerConfig, AeroError> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(AeroError::InvalidConfiguration(
                "bootstrap_servers cannot be empty".into(),
            ));
        }
        Ok(self.config)
    }
}

/// Calculate backoff duration with full jitter.
fn compute_backoff(
    attempt: usize,
    initial: Duration,
    max: Duration,
    multiplier: f64,
) -> Duration {
    let exponent = (attempt.saturating_sub(1)) as i32;
    let base_ms = (initial.as_millis() as f64) * multiplier.powi(exponent);
    let capped_ms = base_ms.min(max.as_millis() as f64);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    // Jitter range [0.75, 1.25]
    let jitter = 0.75 + ((nanos % 500) as f64 / 1000.0);
    Duration::from_millis((capped_ms * jitter).max(1.0) as u64)
}

/// Asynchronous producer client for publishing messages to AeroStream.
#[derive(Clone)]
pub struct AeroProducer {
    config: Arc<ProducerConfig>,
    connection: Arc<RwLock<Option<AeroConnection>>>,
}

impl AeroProducer {
    /// Create a new `AeroProducer` with the specified configuration.
    pub fn new(config: ProducerConfig) -> Self {
        Self {
            config: Arc::new(config),
            connection: Arc::new(RwLock::new(None)),
        }
    }

    /// Create an `AeroProducer` using default settings for a broker address.
    pub fn connect(addr: impl Into<String>) -> Self {
        let config = ProducerConfig::builder()
            .bootstrap_server(addr)
            .build()
            .expect("default config valid");
        Self::new(config)
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
        // Re-check after acquiring write lock
        if let Some(conn) = &*write {
            if conn.is_alive() {
                return Ok(conn.clone());
            }
        }

        // Attempt connection to bootstrap servers
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

    /// Publish a message to a topic and partition with automatic retries.
    pub async fn send(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
    ) -> Result<u64, AeroError> {
        let mut attempts = 0;

        loop {
            attempts += 1;
            let conn = match self.get_or_connect().await {
                Ok(c) => c,
                Err(e) => {
                    if attempts > self.config.max_retries {
                        return Err(e);
                    }
                    let backoff = compute_backoff(
                        attempts,
                        self.config.initial_backoff,
                        self.config.max_backoff,
                        self.config.backoff_multiplier,
                    );
                    debug!(attempt = attempts, backoff = ?backoff, "Connection retry backoff");
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };

            let send_fut = conn.produce(topic, partition, payload);
            let send_res = tokio::time::timeout(self.config.request_timeout, send_fut).await;

            match send_res {
                Ok(Ok(offset)) => return Ok(offset),
                Ok(Err(err)) => {
                    // Non-retryable errors abort immediately
                    match &err {
                        AeroError::AuthFailed(_) | AeroError::OutOfOrder => return Err(err),
                        _ => {}
                    }

                    conn.mark_broken();
                    if attempts > self.config.max_retries {
                        return Err(err);
                    }
                    let backoff = compute_backoff(
                        attempts,
                        self.config.initial_backoff,
                        self.config.max_backoff,
                        self.config.backoff_multiplier,
                    );
                    debug!(attempt = attempts, error = %err, backoff = ?backoff, "Produce retry");
                    tokio::time::sleep(backoff).await;
                }
                Err(_) => {
                    conn.mark_broken();
                    if attempts > self.config.max_retries {
                        return Err(AeroError::Timeout("Produce request timed out".into()));
                    }
                    let backoff = compute_backoff(
                        attempts,
                        self.config.initial_backoff,
                        self.config.max_backoff,
                        self.config.backoff_multiplier,
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}
