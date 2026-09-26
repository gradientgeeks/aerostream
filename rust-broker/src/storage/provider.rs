use std::fmt;
use std::io;

/// Custom error type for tiered storage operations.
#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    S3(String),
    Config(String),
    NotFound(String),
    Other(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "IO error: {}", e),
            StorageError::S3(e) => write!(f, "S3 storage error: {}", e),
            StorageError::Config(e) => write!(f, "Storage config error: {}", e),
            StorageError::NotFound(e) => write!(f, "Storage object not found: {}", e),
            StorageError::Other(e) => write!(f, "Storage error: {}", e),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        StorageError::Io(err)
    }
}

impl From<String> for StorageError {
    fn from(err: String) -> Self {
        StorageError::Other(err)
    }
}

impl From<&str> for StorageError {
    fn from(err: &str) -> Self {
        StorageError::Other(err.to_string())
    }
}

impl<E: std::fmt::Display> From<aws_sdk_s3::error::SdkError<E>> for StorageError {
    fn from(err: aws_sdk_s3::error::SdkError<E>) -> Self {
        StorageError::S3(err.to_string())
    }
}

/// Abstract provider interface for offloading log segments to tiered cold storage.
#[async_trait::async_trait]
pub trait TieredStorageProvider: Send + Sync {
    /// Uploads segment bytes to cold storage identified by `key`.
    async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError>;

    /// Retrieves segment bytes from cold storage identified by `key`.
    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    /// Deletes a segment in cold storage identified by `key`.
    async fn delete_segment(&self, key: &str) -> Result<(), StorageError>;

    /// Checks if a segment exists in cold storage.
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;

    /// Lists segment keys with the given key prefix.
    async fn list_segments(&self, prefix: &str) -> Result<Vec<String>, StorageError>;

    /// Human-readable identifier of the storage provider.
    fn provider_name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    /// In-memory implementation of `TieredStorageProvider` for testing trait dispatch.
    struct InMemoryStorageProvider {
        data: RwLock<HashMap<String, Vec<u8>>>,
    }

    impl InMemoryStorageProvider {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl TieredStorageProvider for InMemoryStorageProvider {
        async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
            self.data.write().unwrap().insert(key.to_string(), data.to_vec());
            Ok(())
        }

        async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            self.data
                .read()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(key.to_string()))
        }

        async fn delete_segment(&self, key: &str) -> Result<(), StorageError> {
            self.data.write().unwrap().remove(key);
            Ok(())
        }

        async fn exists(&self, key: &str) -> Result<bool, StorageError> {
            Ok(self.data.read().unwrap().contains_key(key))
        }

        async fn list_segments(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            let guard = self.data.read().unwrap();
            let mut keys: Vec<String> = guard
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        fn provider_name(&self) -> &'static str {
            "in-memory"
        }
    }

    #[tokio::test]
    async fn test_tiered_storage_provider_in_memory_crud() {
        let provider: Box<dyn TieredStorageProvider> = Box::new(InMemoryStorageProvider::new());
        assert_eq!(provider.provider_name(), "in-memory");

        // Key initially doesn't exist
        assert!(!provider.exists("seg-0").await.unwrap());
        assert!(matches!(
            provider.get_segment("seg-0").await,
            Err(StorageError::NotFound(_))
        ));

        // Put segment
        let payload = b"cold-storage-log-segment-bytes";
        provider.put_segment("seg-0", payload).await.unwrap();
        provider.put_segment("seg-1", b"another-segment").await.unwrap();
        provider.put_segment("other/seg-2", b"other-segment").await.unwrap();

        // Exists & Get
        assert!(provider.exists("seg-0").await.unwrap());
        let fetched = provider.get_segment("seg-0").await.unwrap();
        assert_eq!(fetched, payload);

        // List segments
        let list_all = provider.list_segments("").await.unwrap();
        assert_eq!(list_all, vec!["other/seg-2", "seg-0", "seg-1"]);

        let list_seg = provider.list_segments("seg").await.unwrap();
        assert_eq!(list_seg, vec!["seg-0", "seg-1"]);

        // Delete segment
        provider.delete_segment("seg-0").await.unwrap();
        assert!(!provider.exists("seg-0").await.unwrap());
        assert_eq!(provider.list_segments("seg").await.unwrap(), vec!["seg-1"]);
    }

    #[test]
    fn test_storage_error_source_and_display() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let storage_err = StorageError::from(io_err);
        use std::error::Error;
        assert!(storage_err.source().is_some());
        assert_eq!(
            storage_err.source().unwrap().to_string(),
            "access denied"
        );

        let other_err = StorageError::Other("custom".into());
        assert!(other_err.source().is_none());
        assert_eq!(other_err.to_string(), "Storage error: custom");
    }
}
