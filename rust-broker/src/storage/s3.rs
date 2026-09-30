use super::provider::{StorageError, TieredStorageProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};

/// Configuration options for connecting to AWS S3 or S3-compatible cold object storage
/// (e.g., MinIO, LocalStack, Ceph).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional object key prefix (namespace) within the bucket.
    pub prefix: Option<String>,
    /// AWS Region (defaults to "us-east-1").
    pub region: Option<String>,
    /// Custom S3 endpoint URL (e.g. "http://localhost:9000" for MinIO).
    pub endpoint_url: Option<String>,
    /// Static AWS Access Key ID.
    pub access_key_id: Option<String>,
    /// Static AWS Secret Access Key.
    pub secret_access_key: Option<String>,
    /// Whether to force path-style addressing (`bucket/key` instead of `bucket.s3...`).
    /// Automatically defaults to `true` if `endpoint_url` is specified.
    pub force_path_style: Option<bool>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            force_path_style: None,
        }
    }
}

/// S3 / MinIO implementation of `TieredStorageProvider`.
#[derive(Clone, Debug)]
pub struct S3StorageProvider {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3StorageProvider {
    /// Creates a new `S3StorageProvider` from the given `S3Config`.
    pub fn new(config: &S3Config) -> Result<Self, StorageError> {
        if config.bucket.trim().is_empty() {
            return Err(StorageError::Config(
                "S3 bucket name cannot be empty".to_string(),
            ));
        }

        let region_str = config.region.as_deref().unwrap_or("us-east-1");
        let region = aws_config::Region::new(region_str.to_string());

        let mut sdk_builder = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(region);

        if let Some(endpoint) = &config.endpoint_url {
            sdk_builder = sdk_builder.endpoint_url(endpoint);
        }

        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let creds = aws_sdk_s3::config::Credentials::new(
                access_key.clone(),
                secret_key.clone(),
                None,
                None,
                "aeromq-s3-config",
            );
            sdk_builder = sdk_builder.credentials_provider(
                aws_sdk_s3::config::SharedCredentialsProvider::new(creds),
            );
        }

        let sdk_config = sdk_builder.build();
        let mut config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        let force_path_style = config.force_path_style.unwrap_or_else(|| {
            // Default to path-style if custom endpoint is specified (e.g. MinIO/LocalStack/Ceph)
            config.endpoint_url.is_some()
        });
        config_builder = config_builder.force_path_style(force_path_style);

        let s3_conf = config_builder.build();
        let client = Client::from_conf(s3_conf);

        let prefix = config
            .prefix
            .as_deref()
            .unwrap_or("")
            .trim_matches('/')
            .to_string();

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix,
        })
    }

    /// Creates an `S3StorageProvider` with an existing `aws_sdk_s3::Client`.
    pub fn with_client(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        let clean_prefix = prefix.into().trim_matches('/').to_string();
        Self {
            client,
            bucket: bucket.into(),
            prefix: clean_prefix,
        }
    }

    /// Returns a reference to the inner AWS S3 client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns the target bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the root key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Formats a segment key by prepending the provider's configured prefix.
    pub fn format_key(&self, key: &str) -> String {
        Self::build_key(&self.prefix, key)
    }

    /// Formats a key given a base prefix and relative key.
    pub fn build_key(prefix: &str, key: &str) -> String {
        let clean_prefix = prefix.trim_matches('/');
        let clean_key = key.trim_start_matches('/');
        if clean_prefix.is_empty() {
            clean_key.to_string()
        } else if clean_key.is_empty() {
            clean_prefix.to_string()
        } else {
            format!("{}/{}", clean_prefix, clean_key)
        }
    }

    /// Strips the provider's configured prefix from an S3 object key.
    pub fn strip_prefix<'a>(&self, key: &'a str) -> &'a str {
        Self::strip_prefix_from_key(&self.prefix, key)
    }

    /// Strips the specified prefix from an S3 object key.
    pub fn strip_prefix_from_key<'a>(prefix: &str, key: &'a str) -> &'a str {
        let clean_prefix = prefix.trim_matches('/');
        if clean_prefix.is_empty() {
            return key.trim_start_matches('/');
        }
        let stripped = key.strip_prefix(clean_prefix).unwrap_or(key);
        stripped.trim_start_matches('/')
    }

    /// Formats a prefix query string for list operations.
    pub fn format_list_prefix(&self, sub_prefix: &str) -> String {
        let clean_base = self.prefix.trim_matches('/');
        let clean_sub = sub_prefix.trim_matches('/');
        if clean_base.is_empty() {
            if clean_sub.is_empty() {
                String::new()
            } else {
                clean_sub.to_string()
            }
        } else if clean_sub.is_empty() {
            format!("{}/", clean_base)
        } else {
            format!("{}/{}", clean_base, clean_sub)
        }
    }
}

#[async_trait::async_trait]
impl TieredStorageProvider for S3StorageProvider {
    async fn put_segment(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let full_key = self.format_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }

    async fn get_segment(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let full_key = self.format_key(key);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|err| {
                let is_not_found = match &err {
                    aws_sdk_s3::error::SdkError::ServiceError(s_err) => {
                        s_err.err().is_no_such_key() || s_err.raw().status().as_u16() == 404
                    }
                    _ => false,
                };
                if is_not_found {
                    StorageError::NotFound(format!(
                        "Segment '{}' not found in bucket '{}'",
                        full_key, self.bucket
                    ))
                } else {
                    StorageError::S3(err.to_string())
                }
            })?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .to_vec();

        Ok(bytes)
    }

    async fn delete_segment(&self, key: &str) -> Result<(), StorageError> {
        let full_key = self.format_key(key);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| StorageError::S3(e.to_string()))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let full_key = self.format_key(key);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                let is_not_found = match &err {
                    aws_sdk_s3::error::SdkError::ServiceError(s_err) => {
                        s_err.err().is_not_found() || s_err.raw().status().as_u16() == 404
                    }
                    _ => false,
                };
                if is_not_found {
                    Ok(false)
                } else {
                    Err(StorageError::S3(err.to_string()))
                }
            }
        }
    }

    async fn list_segments(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let full_prefix = self.format_list_prefix(prefix);
        let mut continuation_token = None;
        let mut results = Vec::new();

        loop {
            let mut req = self.client.list_objects_v2().bucket(&self.bucket);
            if !full_prefix.is_empty() {
                req = req.prefix(&full_prefix);
            }
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.map_err(|e| StorageError::S3(e.to_string()))?;

            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    let relative = self.strip_prefix(k);
                    results.push(relative.to_string());
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                if let Some(token) = resp.next_continuation_token() {
                    continuation_token = Some(token.to_string());
                    continue;
                }
            }
            break;
        }

        Ok(results)
    }

    fn provider_name(&self) -> &'static str {
        "aws-s3"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_default_and_serialization() {
        let cfg = S3Config::default();
        assert_eq!(cfg.bucket, "");
        assert_eq!(cfg.region, Some("us-east-1".to_string()));
        assert_eq!(cfg.prefix, None);
        assert_eq!(cfg.endpoint_url, None);
        assert_eq!(cfg.force_path_style, None);

        let toml_str = r#"
            bucket = "my-cold-bucket"
            prefix = "aeromq/cold"
            region = "us-west-2"
            endpoint_url = "http://minio.local:9000"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"
            force_path_style = true
        "#;
        let parsed: S3Config = toml::from_str(toml_str).expect("parse toml");
        assert_eq!(parsed.bucket, "my-cold-bucket");
        assert_eq!(parsed.prefix, Some("aeromq/cold".to_string()));
        assert_eq!(parsed.region, Some("us-west-2".to_string()));
        assert_eq!(parsed.endpoint_url, Some("http://minio.local:9000".to_string()));
        assert_eq!(parsed.access_key_id, Some("minioadmin".to_string()));
        assert_eq!(parsed.secret_access_key, Some("minioadmin".to_string()));
        assert_eq!(parsed.force_path_style, Some(true));
    }

    #[test]
    fn test_s3_storage_provider_empty_bucket_validation() {
        let cfg = S3Config {
            bucket: "   ".to_string(),
            ..Default::default()
        };
        let res = S3StorageProvider::new(&cfg);
        match res {
            Err(StorageError::Config(msg)) => {
                assert!(msg.contains("bucket name cannot be empty"));
            }
            _ => panic!("Expected StorageError::Config for empty bucket"),
        }
    }

    #[test]
    fn test_s3_storage_provider_new_with_valid_config() {
        let cfg = S3Config {
            bucket: "test-bucket".to_string(),
            prefix: Some("/cold/storage/".to_string()),
            region: Some("eu-central-1".to_string()),
            endpoint_url: Some("http://127.0.0.1:9000".to_string()),
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            force_path_style: Some(true),
        };

        let provider = S3StorageProvider::new(&cfg).expect("failed to create S3 provider");
        assert_eq!(provider.bucket(), "test-bucket");
        assert_eq!(provider.prefix(), "cold/storage");
        assert_eq!(provider.provider_name(), "aws-s3");
    }

    #[test]
    fn test_key_formatting_and_prefix_handling() {
        // Provider with empty prefix
        let provider_no_prefix = S3StorageProvider {
            client: S3StorageProvider::new(&S3Config {
                bucket: "b".to_string(),
                ..Default::default()
            })
            .unwrap()
            .client,
            bucket: "b".to_string(),
            prefix: "".to_string(),
        };

        assert_eq!(provider_no_prefix.format_key("segment-001.log"), "segment-001.log");
        assert_eq!(provider_no_prefix.format_key("/segment-001.log"), "segment-001.log");
        assert_eq!(provider_no_prefix.strip_prefix("segment-001.log"), "segment-001.log");
        assert_eq!(provider_no_prefix.strip_prefix("/segment-001.log"), "segment-001.log");
        assert_eq!(provider_no_prefix.format_list_prefix(""), "");
        assert_eq!(provider_no_prefix.format_list_prefix("topic-1"), "topic-1");

        // Provider with configured prefix
        let provider_with_prefix = S3StorageProvider {
            client: provider_no_prefix.client.clone(),
            bucket: "b".to_string(),
            prefix: "aeromq/cold".to_string(),
        };

        assert_eq!(
            provider_with_prefix.format_key("topicA/part0/000000.log"),
            "aeromq/cold/topicA/part0/000000.log"
        );
        assert_eq!(
            provider_with_prefix.format_key("/topicA/part0/000000.log"),
            "aeromq/cold/topicA/part0/000000.log"
        );
        assert_eq!(
            provider_with_prefix.strip_prefix("aeromq/cold/topicA/part0/000000.log"),
            "topicA/part0/000000.log"
        );
        assert_eq!(
            provider_with_prefix.strip_prefix("unrelated/key.log"),
            "unrelated/key.log"
        );
        assert_eq!(
            provider_with_prefix.format_list_prefix(""),
            "aeromq/cold/"
        );
        assert_eq!(
            provider_with_prefix.format_list_prefix("topicA"),
            "aeromq/cold/topicA"
        );
    }

    #[test]
    fn test_error_conversions_and_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let storage_err = StorageError::from(io_err);
        match &storage_err {
            StorageError::Io(_) => (),
            _ => panic!("Expected StorageError::Io"),
        }
        assert!(storage_err.to_string().contains("IO error: file missing"));

        let str_err: StorageError = "custom error".into();
        match &str_err {
            StorageError::Other(msg) => assert_eq!(msg, "custom error"),
            _ => panic!("Expected StorageError::Other"),
        }

        let string_err: StorageError = String::from("custom string").into();
        match &string_err {
            StorageError::Other(msg) => assert_eq!(msg, "custom string"),
            _ => panic!("Expected StorageError::Other"),
        }

        let config_err = StorageError::Config("invalid setting".to_string());
        assert_eq!(
            config_err.to_string(),
            "Storage config error: invalid setting"
        );

        let s3_err = StorageError::S3("NoSuchBucket".to_string());
        assert_eq!(s3_err.to_string(), "S3 storage error: NoSuchBucket");

        let not_found_err = StorageError::NotFound("seg-1".to_string());
        assert_eq!(
            not_found_err.to_string(),
            "Storage object not found: seg-1"
        );
    }
}
