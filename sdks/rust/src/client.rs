use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::debug;

use crate::connection::AeroConnection;
use crate::consumer::{AeroConsumer, ConsumerConfig};
use crate::error::AeroError;
use crate::producer::{AeroProducer, ProducerConfig};

/// Global configuration options for the AeroStream client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Broker endpoint addresses.
    pub bootstrap_servers: Vec<String>,
    /// Optional authentication token.
    pub token: Option<String>,
    /// Connection timeout.
    pub connect_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["127.0.0.1:9091".to_string()],
            token: None,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

impl ClientConfig {
    /// Create a fluent builder for ClientConfig.
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }
}

/// Fluent builder for constructing `ClientConfig` instances.
#[derive(Default)]
pub struct ClientConfigBuilder {
    config: ClientConfig,
}

impl ClientConfigBuilder {
    /// Set the list of bootstrap broker endpoints.
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.config.bootstrap_servers = servers;
        self
    }

    /// Add a single bootstrap broker endpoint.
    pub fn bootstrap_server(mut self, server: impl Into<String>) -> Self {
        self.config.bootstrap_servers = vec![server.into()];
        self
    }

    /// Configure authentication bearer token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// Set connection timeout duration.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Validate and construct ClientConfig.
    pub fn build(self) -> Result<ClientConfig, AeroError> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(AeroError::InvalidConfiguration(
                "bootstrap_servers cannot be empty".into(),
            ));
        }
        Ok(self.config)
    }
}

/// High-level client managing connection pools and orchestrating producers and consumers.
#[derive(Clone)]
pub struct AeroClient {
    config: Arc<ClientConfig>,
    pool: Arc<RwLock<HashMap<String, AeroConnection>>>,
    default_producer: Arc<RwLock<Option<AeroProducer>>>,
}

impl AeroClient {
    /// Initialize a new client with the provided configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config: Arc::new(config),
            pool: Arc::new(RwLock::new(HashMap::new())),
            default_producer: Arc::new(RwLock::new(None)),
        }
    }

    /// Connect using default configuration for a single broker endpoint.
    pub fn connect(addr: impl Into<String>) -> Result<Self, AeroError> {
        let config = ClientConfig::builder().bootstrap_server(addr).build()?;
        Ok(Self::new(config))
    }

    /// Retrieve an existing pooled connection or establish a new connection.
    pub async fn get_connection(&self, broker_addr: &str) -> Result<AeroConnection, AeroError> {
        {
            let read = self.pool.read().await;
            if let Some(conn) = read.get(broker_addr) {
                if conn.is_alive() {
                    return Ok(conn.clone());
                }
            }
        }

        let mut write = self.pool.write().await;
        if let Some(conn) = write.get(broker_addr) {
            if conn.is_alive() {
                return Ok(conn.clone());
            }
        }

        debug!(addr = %broker_addr, "Establishing new pooled connection");
        let conn = AeroConnection::connect(
            broker_addr,
            self.config.token.as_deref(),
            self.config.connect_timeout,
        )
        .await?;

        write.insert(broker_addr.to_string(), conn.clone());
        Ok(conn)
    }

    /// Create an `AeroProducer` inheriting the client's bootstrap servers and auth token.
    pub fn producer(&self) -> AeroProducer {
        let config = ProducerConfig::builder()
            .bootstrap_servers(self.config.bootstrap_servers.clone())
            .token(self.config.token.clone().unwrap_or_default())
            .connect_timeout(self.config.connect_timeout)
            .build()
            .expect("valid producer configuration");
        AeroProducer::new(config)
    }

    /// Create an `AeroProducer` with customized configuration.
    pub fn producer_with_config(&self, config: ProducerConfig) -> AeroProducer {
        AeroProducer::new(config)
    }

    /// Create an `AeroConsumer` for a topic and partition.
    pub fn consumer(&self, topic: impl Into<String>, partition: u32) -> AeroConsumer {
        let config = ConsumerConfig::builder()
            .bootstrap_servers(self.config.bootstrap_servers.clone())
            .token(self.config.token.clone().unwrap_or_default())
            .topic(topic)
            .partition(partition)
            .connect_timeout(self.config.connect_timeout)
            .build()
            .expect("valid consumer configuration");
        AeroConsumer::new(config)
    }

    /// Create an `AeroConsumer` with customized configuration.
    pub fn consumer_with_config(&self, config: ConsumerConfig) -> AeroConsumer {
        AeroConsumer::new(config)
    }

    /// Publish a message directly through the client's shared producer.
    pub async fn publish(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
    ) -> Result<u64, AeroError> {
        let producer = {
            let read = self.default_producer.read().await;
            if let Some(p) = &*read {
                p.clone()
            } else {
                drop(read);
                let mut write = self.default_producer.write().await;
                if let Some(p) = &*write {
                    p.clone()
                } else {
                    let p = self.producer();
                    *write = Some(p.clone());
                    p
                }
            }
        };

        producer.send(topic, partition, payload).await
    }
}
