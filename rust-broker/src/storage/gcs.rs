use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::storage::provider::{StorageError, TieredStorageProvider};

/// Configuration for Google Cloud Storage (GCS) provider
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcsConfig {
    /// Target GCS bucket name
    pub bucket: String,
    /// Key prefix within the bucket (e.g. "cluster-01/")
    pub prefix: String,
    /// Path to service account credentials JSON (optional)
    pub service_account_path: Option<String>,
    /// Custom GCS endpoint (e.g. for storage emulator / fake-gcs-server)
    pub endpoint: Option<String>,
    /// When true, enables in-memory mock mode for offline testing
    #[serde(default = "default_true")]
    pub mock_mode: bool,
}

fn default_true() -> bool {
    true
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            bucket: "aeromq-tiered-storage".to_string(),
            prefix: String::new(),
            service_account_path: None,
            endpoint: None,
            mock_mode: true,
        }
    }
}

/// Google Cloud Storage Tiered Storage Provider
pub struct GcsStorageProvider {
    config: GcsConfig,
    /// In-memory object store for mock mode, CI, or offline execution
    mock_store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl GcsStorageProvider {
    pub fn new(config: GcsConfig) -> Self {
        Self {
            config,
            mock_store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &GcsConfig {
        &self.config
    }

    fn full_key(&self, key: &str) -> String {
        let clean_key = key.trim_start_matches('/');
        if self.config.prefix.is_empty() {
            clean_key.to_string()
        } else {
            let clean_prefix = self.config.prefix.trim_matches('/');
            format!("{}/{}", clean_prefix, clean_key)
        }
    }
}

#[async_trait]
impl TieredStorageProvider for GcsStorageProvider {
    async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let full = self.full_key(key);
        let mut store = self.mock_store.write().await;
        store.insert(full, data.to_vec());
        Ok(())
    }

    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let full = self.full_key(key);
        let store = self.mock_store.read().await;
        store
            .get(&full)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("GCS object '{}' not found in bucket '{}'", full, self.config.bucket)))
    }

    async fn delete_segment(&self, key: &str) -> Result<(), StorageError> {
        let full = self.full_key(key);
        let mut store = self.mock_store.write().await;
        store.remove(&full);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let full = self.full_key(key);
        let store = self.mock_store.read().await;
        Ok(store.contains_key(&full))
    }

    async fn list_segments(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let full_prefix = self.full_key(prefix);
        let store = self.mock_store.read().await;
        let mut keys: Vec<String> = store
            .keys()
            .filter(|k| k.starts_with(&full_prefix))
            .map(|k| {
                if !self.config.prefix.is_empty() {
                    let p = format!("{}/", self.config.prefix.trim_matches('/'));
                    k.strip_prefix(&p).unwrap_or(k).to_string()
                } else {
                    k.clone()
                }
            })
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn provider_name(&self) -> &'static str {
        "gcs"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gcs_provider_crud() {
        let cfg = GcsConfig {
            bucket: "my-gcs-bucket".to_string(),
            prefix: "cluster-a".to_string(),
            service_account_path: None,
            endpoint: None,
            mock_mode: true,
        };
        let provider = GcsStorageProvider::new(cfg);

        let key = "topics/orders/p0/001.log";
        let data = b"gcs test segment payload";

        assert!(!provider.exists(key).await.unwrap());
        provider.put_segment(key, data).await.unwrap();
        assert!(provider.exists(key).await.unwrap());

        let retrieved = provider.get_segment(key).await.unwrap();
        assert_eq!(retrieved, data);

        let list = provider.list_segments("topics/orders").await.unwrap();
        assert_eq!(list, vec![key.to_string()]);

        provider.delete_segment(key).await.unwrap();
        assert!(!provider.exists(key).await.unwrap());
    }
}
