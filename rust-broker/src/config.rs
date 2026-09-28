use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

/// Top-level broker configuration. Loaded from a TOML file (`--config`),
/// then selectively overridden by explicit CLI flags in `main.rs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    /// Unique broker ID.
    pub id: u32,
    /// Bind IP for the TCP data plane.
    pub host: String,
    /// TCP port for client data operations.
    pub data_port: i32,
    /// TCP port for Kafka wire protocol operations.
    pub kafka_port: i32,
    /// gRPC endpoint of the Go control plane.
    pub controller: String,
    /// Path to store physical partition log files (defaults to ./data/broker_{id}).
    pub storage_dir: Option<PathBuf>,
    /// Number of shard threads for thread-per-core mode.
    /// 0 = auto-detect (one per CPU core). Set to 1 to disable sharding.
    pub shard_threads: usize,
    /// `broker.rack`: rack / availability-zone label reported to the controller and in Metadata.
    pub rack: Option<String>,
    /// KIP-392 replica selector: "rack_aware" (default) or "leader".
    pub replica_selector: String,
    /// Consumer-group coordinator: delay before the first rebalance of an empty group completes.
    pub group_initial_rebalance_delay_ms: u64,

    pub storage: StorageConfig,
    pub tiered_storage: crate::storage::TieredStorageConfig,
    pub tls: TlsConfig,
    pub auth: AuthConfig,
    /// Default topic `compression.type`: producer | uncompressed | gzip | snappy | lz4 | zstd.
    pub compression_type: String,
    /// Client quotas (`[[quotas]]` tables); the controller can override them via heartbeat.
    pub quotas: Vec<crate::kafka::quota::QuotaEntry>,
    /// Iceberg topics (`[iceberg]` section).
    pub iceberg: crate::iceberg::IcebergConfig,
    /// Transaction coordinator settings (`[txn]`).
    pub txn: crate::txn::TxnConfig,
    /// Share group (KIP-932) settings (`[share]`).
    pub share: crate::share::ShareConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Max size of an active log segment before it rolls over, in bytes.
    pub max_segment_size: u64,
    /// Max total on-disk size of a partition before old segments are evicted.
    pub max_retention_size: Option<u64>,
    /// Max age of a segment before it is eligible for deletion, in seconds.
    pub max_retention_age_secs: Option<u64>,
    /// Enable log compaction for closed segments.
    pub compaction_enabled: bool,
    /// Dirty ratio threshold to trigger compaction (default 0.5).
    pub dirty_ratio_threshold: f64,
    /// Duration in seconds to retain tombstones before deleting them (default 86400 = 24h).
    pub tombstone_retention_secs: u64,
    /// Start page-cache writeback of each partition's active segment every this many bytes (0 disables).
    /// Prevents dirty pages from piling up into multi-second write stalls, notably under a container memory limit.
    pub writeback_bytes: u64,
    /// After a range has been written back, drop it from the page cache. Caps page-cache use and paces the writer
    /// to the disk, at the cost of serving very recent reads from disk.
    pub drop_cache_after_writeback: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS on the TCP data plane. NOTE: enabling this disables the
    /// `sendfile` zero-copy fast path (encryption must happen in userspace).
    pub enabled: bool,
    /// Server certificate (PEM) for the data plane.
    pub cert_file: Option<PathBuf>,
    /// Server private key (PEM) for the data plane.
    pub key_file: Option<PathBuf>,
    /// CA certificate (PEM) used to verify the controller's TLS cert when the
    /// controller endpoint is `https://`.
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Shared bearer token required by the control plane (gRPC metadata) and
    /// the data-plane AUTH handshake. `None` disables authentication.
    pub token: Option<String>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            id: 1,
            host: "127.0.0.1".to_string(),
            data_port: 9091,
            kafka_port: 9093,
            controller: "http://127.0.0.1:8001".to_string(),
            storage_dir: None,
            shard_threads: 0,
            rack: None,
            replica_selector: "rack_aware".to_string(),
            group_initial_rebalance_delay_ms: 3000,
            storage: StorageConfig::default(),
            tiered_storage: crate::storage::TieredStorageConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
            compression_type: "producer".to_string(),
            quotas: Vec::new(),
            iceberg: crate::iceberg::IcebergConfig::default(),
            txn: crate::txn::TxnConfig::default(),
            share: crate::share::ShareConfig::default(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            // Production-sane default: 128 MiB segments (was 210 bytes for testing).
            max_segment_size: 128 * 1024 * 1024,
            // 1 GiB per-partition retention.
            max_retention_size: Some(1024 * 1024 * 1024),
            // 7 days.
            max_retention_age_secs: Some(7 * 24 * 3600),
            // Log compaction opt-in per topic (default false, enabled for cleanup.policy=compact)
            compaction_enabled: false,
            dirty_ratio_threshold: 0.5,
            tombstone_retention_secs: 86400,
            writeback_bytes: 8 * 1024 * 1024,
            drop_cache_after_writeback: false,
        }
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { token: None }
    }
}

impl BrokerConfig {
    /// Load configuration from an optional TOML file. When `path` is `None`,
    /// returns built-in defaults.
    pub fn load(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        match path {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| format!("failed to read config file {:?}: {}", p, e))?;
                let cfg: BrokerConfig = toml::from_str(&contents)
                    .map_err(|e| format!("failed to parse config file {:?}: {}", p, e))?;
                Ok(cfg)
            }
            None => Ok(BrokerConfig::default()),
        }
    }

    /// Convenience: retention age as a `Duration`.
    pub fn max_retention_age(&self) -> Option<Duration> {
        self.storage.max_retention_age_secs.map(Duration::from_secs)
    }

    /// Resolve the storage directory, defaulting to `./data/broker_{id}`.
    pub fn resolved_storage_dir(&self) -> PathBuf {
        self.storage_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("./data/broker_{}", self.id)))
    }
}

/// Returns (used_bytes, free_bytes) for the filesystem containing `path`,
/// via `statvfs(2)`. Returns (0, 0) on error.
pub fn disk_stats(path: &Path) -> (u64, u64) {
    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return (0, 0),
    };
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let frsize = stat.f_frsize as u64;
            let total = stat.f_blocks as u64 * frsize;
            let free = stat.f_bavail as u64 * frsize;
            let used = total.saturating_sub(free);
            (used, free)
        } else {
            (0, 0)
        }
    }
}
