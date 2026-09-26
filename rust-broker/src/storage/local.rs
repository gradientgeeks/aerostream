use std::path::{Path, PathBuf};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::storage::provider::{StorageError, TieredStorageProvider};

/// Configuration for local filesystem / NFS tiered storage provider
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalStorageConfig {
    pub root_path: PathBuf,
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            root_path: PathBuf::from("./data/tiered_storage"),
        }
    }
}

/// LocalStorageProvider stores segments on local filesystem or mounted NFS volume.
/// It provides full tiered storage capabilities for local testing, offline setups,
/// edge nodes, or file-tier fallback.
pub struct LocalStorageProvider {
    root_path: PathBuf,
}

impl LocalStorageProvider {
    pub fn new(root_path: impl Into<PathBuf>) -> Self {
        Self {
            root_path: root_path.into(),
        }
    }

    pub fn from_config(config: &LocalStorageConfig) -> Self {
        Self::new(config.root_path.clone())
    }

    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    fn resolve_path(&self, key: &str) -> PathBuf {
        let clean_key = key.trim_start_matches('/');
        self.root_path.join(clean_key)
    }
}

#[async_trait]
impl TieredStorageProvider for LocalStorageProvider {
    async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let path = self.resolve_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(StorageError::Io)?;
        }

        // Write atomically using a temporary file
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let tmp_path = format!("{}.{}.tmp", path.display(), nanos);
        fs::write(&tmp_path, data).await.map_err(StorageError::Io)?;
        fs::rename(&tmp_path, &path).await.map_err(StorageError::Io)?;

        Ok(())
    }

    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.resolve_path(key);
        match fs::read(&path).await {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(format!("Key '{}' not found at {:?}", key, path)))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn delete_segment(&self, key: &str) -> Result<(), StorageError> {
        let path = self.resolve_path(key);
        match fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Idempotent deletion
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let path = self.resolve_path(key);
        Ok(fs::metadata(&path).await.is_ok())
    }

    async fn list_segments(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let mut results = Vec::new();
        if fs::metadata(&self.root_path).await.is_err() {
            return Ok(results);
        }

        let clean_prefix = prefix.trim_start_matches('/');
        let mut dirs = vec![self.root_path.clone()];

        while let Some(current_dir) = dirs.pop() {
            let mut entries = match fs::read_dir(&current_dir).await {
                Ok(e) => e,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(StorageError::Io(err)),
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    dirs.push(entry_path);
                } else if entry_path.is_file() {
                    // Do not list temporary write files
                    if entry_path.to_string_lossy().ends_with(".tmp") {
                        continue;
                    }
                    if let Ok(rel) = entry_path.strip_prefix(&self.root_path) {
                        let rel_str = rel.to_string_lossy().replace('\\', "/");
                        if rel_str.starts_with(clean_prefix) {
                            results.push(rel_str);
                        }
                    }
                }
            }
        }

        results.sort();
        Ok(results)
    }

    fn provider_name(&self) -> &'static str {
        "local"
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
            let path = std::env::temp_dir().join(format!("aeromq_local_test_{}_{}", std::process::id(), nanos));
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

    #[tokio::test]
    async fn test_local_storage_put_get_exists_delete() {
        let dir = TestTempDir::new();
        let provider = LocalStorageProvider::new(dir.path());

        let key = "topics/test-topic/partition-0/00000000000000000000.log";
        let payload = b"hello world tiered storage on local filesystem";

        // Initially does not exist
        assert!(!provider.exists(key).await.unwrap());
        let get_res = provider.get_segment(key).await;
        assert!(matches!(get_res, Err(StorageError::NotFound(_))));

        // Put segment
        provider.put_segment(key, payload).await.unwrap();
        assert!(provider.exists(key).await.unwrap());

        // Get segment
        let retrieved = provider.get_segment(key).await.unwrap();
        assert_eq!(retrieved, payload);

        // Delete segment
        provider.delete_segment(key).await.unwrap();
        assert!(!provider.exists(key).await.unwrap());

        // Idempotent delete
        provider.delete_segment(key).await.unwrap();
    }

    #[tokio::test]
    async fn test_local_storage_list_segments() {
        let dir = TestTempDir::new();
        let provider = LocalStorageProvider::new(dir.path());

        let k1 = "topics/t1/p0/000.log";
        let k2 = "topics/t1/p0/000.idx";
        let k3 = "topics/t1/p1/000.log";
        let k4 = "topics/t2/p0/000.log";

        provider.put_segment(k1, b"1").await.unwrap();
        provider.put_segment(k2, b"2").await.unwrap();
        provider.put_segment(k3, b"3").await.unwrap();
        provider.put_segment(k4, b"4").await.unwrap();

        let list_all = provider.list_segments("").await.unwrap();
        assert_eq!(list_all.len(), 4);

        let list_t1_p0 = provider.list_segments("topics/t1/p0").await.unwrap();
        assert_eq!(list_t1_p0, vec![k2.to_string(), k1.to_string()]);

        let list_t2 = provider.list_segments("topics/t2").await.unwrap();
        assert_eq!(list_t2, vec![k4.to_string()]);

        let list_empty = provider.list_segments("topics/nonexistent").await.unwrap();
        assert!(list_empty.is_empty());
    }
}
