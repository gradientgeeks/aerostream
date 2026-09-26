use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::storage::provider::{StorageError, TieredStorageProvider};

/// Configuration for Azure Blob Storage provider
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AzureBlobConfig {
    /// Azure Blob container name
    pub container_name: String,
    /// Storage account name
    pub account_name: String,
    /// Prefix or virtual folder inside the container
    pub prefix: String,
    /// Account access key or SAS token (optional)
    pub access_key: Option<String>,
    /// Custom Azure Blob endpoint (e.g. Azurite emulator `http://127.0.0.1:10000/devstoreaccount1`)
    pub endpoint: Option<String>,
    /// When true, enables in-memory mock mode for offline testing
    #[serde(default = "default_true")]
    pub mock_mode: bool,
}

fn default_true() -> bool {
    true
}

impl Default for AzureBlobConfig {
    fn default() -> Self {
        Self {
            container_name: "aeromq-segments".to_string(),
            account_name: "aeromqstorage".to_string(),
            prefix: String::new(),
            access_key: None,
            endpoint: None,
            mock_mode: true,
        }
    }
}

/// Azure Blob Storage Tiered Storage Provider
pub struct AzureBlobStorageProvider {
    config: AzureBlobConfig,
    /// In-memory object store for mock mode, CI, or offline execution
    mock_store: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl AzureBlobStorageProvider {
    pub fn new(config: AzureBlobConfig) -> Self {
        Self {
            config,
            mock_store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn config(&self) -> &AzureBlobConfig {
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
impl TieredStorageProvider for AzureBlobStorageProvider {
    async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let full = self.full_key(key);
        let mut store = self.mock_store.write().await;
        store.insert(full, data.to_vec());
        Ok(())
    }

    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let full = self.full_key(key);
        let store = self.mock_store.read().await;
        store.get(&full).cloned().ok_or_else(|| {
            StorageError::NotFound(format!(
                "Azure blob '{}' not found in container '{}'",
                full, self.config.container_name
            ))
        })
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
        "azure"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_azure_provider_crud() {
        let cfg = AzureBlobConfig {
            container_name: "test-container".to_string(),
            account_name: "testacct".to_string(),
            prefix: "backups".to_string(),
            access_key: None,
            endpoint: None,
            mock_mode: true,
        };
        let provider = AzureBlobStorageProvider::new(cfg);

        let key = "topics/telemetry/p0/000.log";
        let data = b"azure blob test data";

        assert!(!provider.exists(key).await.unwrap());
        provider.put_segment(key, data).await.unwrap();
        assert!(provider.exists(key).await.unwrap());

        let retrieved = provider.get_segment(key).await.unwrap();
        assert_eq!(retrieved, data);

        let list = provider.list_segments("topics/telemetry").await.unwrap();
        assert_eq!(list, vec![key.to_string()]);

        provider.delete_segment(key).await.unwrap();
        assert!(!provider.exists(key).await.unwrap());
    }
}
