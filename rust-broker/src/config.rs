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
    /// gRPC endpoint of the Go control plane.
    pub controller: String,
    /// Path to store physical partition log files (defaults to ./data/broker_{id}).
    pub storage_dir: Option<PathBuf>,

    pub storage: StorageConfig,
    pub tls: TlsConfig,
    pub auth: AuthConfig,
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
            controller: "http://127.0.0.1:8001".to_string(),
            storage_dir: None,
            storage: StorageConfig::default(),
            tls: TlsConfig::default(),
            auth: AuthConfig::default(),
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
