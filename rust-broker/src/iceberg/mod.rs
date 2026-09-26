//! Iceberg topics: tails topic partitions, converts records to Parquet and
//! commits Iceberg v2 snapshots to a warehouse. Zero external dependencies
//! (hand-written Parquet/Avro/JSON); gated by cargo feature `iceberg`.
//!
//! Warehouse layout (Hadoop-style catalog, readable via `metadata/version-hint.text`
//! or by pointing any engine at `metadata/v<N>.metadata.json`):
//!   <warehouse>/<namespace>/<topic>/data/<uuid>.parquet
//!   <warehouse>/<namespace>/<topic>/metadata/{v<N>.metadata.json, snap-*.avro, *-m0.avro}

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IcebergTopicConfig {
    pub name: String,
    /// `key_value` (default) or `json` (unpack top-level fields of a JSON value
    /// into typed columns listed in `json_fields`).
    pub mode: String,
    /// For `json` mode: entries like `user_id:long`, `name:string`, `amount:double`, `ok:boolean`.
    pub json_fields: Vec<String>,
}

impl Default for IcebergTopicConfig {
    fn default() -> Self {
        Self { name: String::new(), mode: "key_value".into(), json_fields: vec![] }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IcebergConfig {
    pub enabled: bool,
    /// `file:///abs/dir`, a bare directory path, or `s3://bucket/prefix`
    /// (S3 uses the broker's `[tiered_storage.s3]` credentials/endpoint).
    pub warehouse: String,
    pub namespace: String,
    pub commit_interval_secs: u64,
    pub max_rows_per_commit: usize,
    pub topics: Vec<IcebergTopicConfig>,
}

impl Default for IcebergConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            warehouse: "./data/warehouse".into(),
            namespace: "aerostream".into(),
            commit_interval_secs: 60,
            max_rows_per_commit: 100_000,
            topics: vec![],
        }
    }
}

#[cfg(feature = "iceberg")]
pub mod avro;
#[cfg(feature = "iceberg")]
pub mod jsonlite;
#[cfg(feature = "iceberg")]
pub mod parquet;
#[cfg(feature = "iceberg")]
pub mod table;

#[cfg(feature = "iceberg")]
mod manager;
#[cfg(feature = "iceberg")]
#[allow(unused_imports)]
pub use manager::{build_columns, IcebergManager, TopicStatus};
