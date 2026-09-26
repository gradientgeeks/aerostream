use std::str::FromStr;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::azure::{AzureBlobConfig, AzureBlobStorageProvider};
use super::gcs::{GcsConfig, GcsStorageProvider};
use super::local::{LocalStorageConfig, LocalStorageProvider};
use super::provider::{StorageError, TieredStorageProvider};
use super::s3::{S3Config, S3StorageProvider};

/// Supported tiered storage provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderType {
    S3,
    Gcs,
    Azure,
    Local,
    Disabled,
}

impl Default for ProviderType {
    fn default() -> Self {
        ProviderType::Disabled
    }
}

impl FromStr for ProviderType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "s3" | "aws" | "minio" => Ok(ProviderType::S3),
            "gcs" | "google" => Ok(ProviderType::Gcs),
            "azure" | "blob" => Ok(ProviderType::Azure),
            "local" | "fs" | "filesystem" | "nfs" => Ok(ProviderType::Local),
            "disabled" | "none" | "null" | "off" => Ok(ProviderType::Disabled),
            other => Err(format!("Unknown tiered storage provider type: '{}'", other)),
        }
    }
}

/// Comprehensive tiered storage configuration for AeroMQ
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TieredStorageConfig {
    /// Global toggle for tiered storage offloading
    pub enabled: bool,
    /// Storage provider type: s3, gcs, azure, local, disabled
    pub provider: ProviderType,
    /// Local filesystem / NFS configuration
    pub local: LocalStorageConfig,
    /// AWS S3 / MinIO configuration
    pub s3: S3Config,
    /// Google Cloud Storage configuration
    pub gcs: GcsConfig,
    /// Azure Blob Storage configuration
    pub azure: AzureBlobConfig,
}

impl Default for TieredStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: ProviderType::Disabled,
            local: LocalStorageConfig::default(),
            s3: S3Config::default(),
            gcs: GcsConfig::default(),
            azure: AzureBlobConfig::default(),
        }
    }
}

/// NullStorageProvider acts as a no-op / disabled provider when tiered storage is disabled.
#[derive(Debug, Default)]
pub struct NullStorageProvider;

impl NullStorageProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TieredStorageProvider for NullStorageProvider {
    async fn put_segment(&self, _key: &str, _data: &[u8]) -> Result<(), StorageError> {
        Err(StorageError::Config(
            "Tiered storage is disabled (NullStorageProvider)".to_string(),
        ))
    }

    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::NotFound(format!(
            "Tiered storage is disabled; cannot fetch key '{}'",
            key
        )))
    }

    async fn delete_segment(&self, _key: &str) -> Result<(), StorageError> {
        Ok(())
    }

    async fn exists(&self, _key: &str) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn list_segments(&self, _prefix: &str) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }

    fn provider_name(&self) -> &'static str {
        "null"
    }
}

/// StorageProviderFactory instantiates the configured TieredStorageProvider.
pub struct StorageProviderFactory;

impl StorageProviderFactory {
    /// Create a shared `TieredStorageProvider` instance from the given configuration.
    pub fn create(config: &TieredStorageConfig) -> Result<Arc<dyn TieredStorageProvider>, StorageError> {
        if !config.enabled || config.provider == ProviderType::Disabled {
            return Ok(Arc::new(NullStorageProvider::new()));
        }

        match config.provider {
            ProviderType::Disabled => Ok(Arc::new(NullStorageProvider::new())),
            ProviderType::Local => {
                let provider = LocalStorageProvider::from_config(&config.local);
                Ok(Arc::new(provider))
            }
            ProviderType::S3 => {
                let provider = S3StorageProvider::new(&config.s3)?;
                Ok(Arc::new(provider))
            }
            ProviderType::Gcs => {
                let provider = GcsStorageProvider::new(config.gcs.clone());
                Ok(Arc::new(provider))
            }
            ProviderType::Azure => {
                let provider = AzureBlobStorageProvider::new(config.azure.clone());
                Ok(Arc::new(provider))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTempDir(std::path::PathBuf);

    impl TestTempDir {
        fn new() -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("aeromq_factory_test_{}_{}", std::process::id(), nanos));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn test_provider_type_parsing() {
        assert_eq!("s3".parse::<ProviderType>().unwrap(), ProviderType::S3);
        assert_eq!("minio".parse::<ProviderType>().unwrap(), ProviderType::S3);
        assert_eq!("gcs".parse::<ProviderType>().unwrap(), ProviderType::Gcs);
        assert_eq!("azure".parse::<ProviderType>().unwrap(), ProviderType::Azure);
        assert_eq!("local".parse::<ProviderType>().unwrap(), ProviderType::Local);
        assert_eq!("fs".parse::<ProviderType>().unwrap(), ProviderType::Local);
        assert_eq!("disabled".parse::<ProviderType>().unwrap(), ProviderType::Disabled);
        assert_eq!("none".parse::<ProviderType>().unwrap(), ProviderType::Disabled);
        assert!("invalid".parse::<ProviderType>().is_err());
    }

    #[test]
    fn test_factory_create_disabled() {
        let mut cfg = TieredStorageConfig::default();
        cfg.enabled = false;
        let provider = StorageProviderFactory::create(&cfg).unwrap();
        assert_eq!(provider.provider_name(), "null");

        cfg.enabled = true;
        cfg.provider = ProviderType::Disabled;
        let provider = StorageProviderFactory::create(&cfg).unwrap();
        assert_eq!(provider.provider_name(), "null");
    }

    #[test]
    fn test_factory_create_local() {
        let dir = TestTempDir::new();
        let mut cfg = TieredStorageConfig::default();
        cfg.enabled = true;
        cfg.provider = ProviderType::Local;
        cfg.local.root_path = dir.path().to_path_buf();

        let provider = StorageProviderFactory::create(&cfg).unwrap();
        assert_eq!(provider.provider_name(), "local");
    }

    #[test]
    fn test_factory_create_gcs() {
        let mut cfg = TieredStorageConfig::default();
        cfg.enabled = true;
        cfg.provider = ProviderType::Gcs;
        cfg.gcs.bucket = "test-gcs-bucket".to_string();

        let provider = StorageProviderFactory::create(&cfg).unwrap();
        assert_eq!(provider.provider_name(), "gcs");
    }

    #[test]
    fn test_factory_create_azure() {
        let mut cfg = TieredStorageConfig::default();
        cfg.enabled = true;
        cfg.provider = ProviderType::Azure;
        cfg.azure.container_name = "test-azure-container".to_string();

        let provider = StorageProviderFactory::create(&cfg).unwrap();
        assert_eq!(provider.provider_name(), "azure");
    }

    #[tokio::test]
    async fn test_null_provider_behaviors() {
        let null_provider = NullStorageProvider::new();
        assert_eq!(null_provider.provider_name(), "null");
        assert!(!null_provider.exists("any-key").await.unwrap());
        assert!(null_provider.list_segments("").await.unwrap().is_empty());
        assert!(null_provider.delete_segment("any-key").await.is_ok());
        assert!(null_provider.put_segment("key", b"data").await.is_err());
        assert!(null_provider.get_segment("key").await.is_err());
    }

    #[test]
    fn test_tiered_storage_config_toml_deserialization() {
        let toml_str = r#"
            enabled = true
            provider = "local"

            [local]
            root_path = "/var/aeromq/tiered"

            [s3]
            bucket = "my-s3-bucket"
            region = "us-west-2"
        "#;

        let cfg: TieredStorageConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.provider, ProviderType::Local);
        assert_eq!(cfg.local.root_path.to_str().unwrap(), "/var/aeromq/tiered");
        assert_eq!(cfg.s3.bucket, "my-s3-bucket");
        assert_eq!(cfg.s3.region.as_deref(), Some("us-west-2"));
    }
}
