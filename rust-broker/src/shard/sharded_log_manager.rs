use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::log::manager::{LogManager, PartitionStore};
use crate::shard::{ShardHandle, ShardConfig, spawn_shards};

pub struct ShardedLogManager {
    pub handle: ShardHandle,
    pub inner: Arc<LogManager>,
}

impl ShardedLogManager {
    pub fn new(inner: Arc<LogManager>, handle: ShardHandle) -> Self {
        Self { inner, handle }
    }

    pub fn inner(&self) -> &Arc<LogManager> {
        &self.inner
    }

    pub fn base_dir(&self) -> &Path {
        self.inner.base_dir()
    }

    pub async fn get_partition_next_offset(&self, topic: &str, partition: u32) -> Result<u64, io::Error> {
        self.handle.get_next_offset(topic, partition).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn spawn_cleaner_loop(&self, interval: std::time::Duration) -> tokio::task::JoinHandle<()> {
        self.inner.clone().spawn_cleaner_loop(interval)
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<usize, io::Error> {
        // Assume handle has delete_topic, if not, we use inner.
        // The prompt says "delegates to handle.delete_topic()"
        // We will implement it on handle if it's missing in engine.rs
        self.handle.delete_topic(topic).await
    }
}

#[async_trait::async_trait]
impl PartitionStore for ShardedLogManager {
    async fn get_all_offsets(&self) -> Vec<(String, u32, i64)> {
        self.handle.get_all_offsets().await
            .into_iter()
            .map(|(t, p, o)| (t, p, o as i64))
            .collect()
    }

    async fn partitions_for_topic(&self, topic: &str) -> Vec<u32> {
        self.inner.partitions_for_topic(topic).await
    }

    async fn delete_topic(&self, topic: &str) -> std::io::Result<usize> {
        self.handle.delete_topic(topic).await
    }

    fn base_dir(&self) -> &std::path::Path {
        self.inner.base_dir()
    }

    fn broker_id(&self) -> u32 {
        self.inner.broker_id
    }
}

pub fn create_sharded(log_manager: Arc<LogManager>, num_shards: usize) -> ShardedLogManager {
    let config = ShardConfig {
        base_dir: log_manager.base_dir().to_path_buf(),
        broker_id: log_manager.broker_id,
        max_segment_size: log_manager.max_segment_size,
        max_retention_size: log_manager.max_retention_size,
        max_retention_age: log_manager.max_retention_age,
        compaction_enabled: log_manager.compaction_enabled,
        dirty_ratio_threshold: log_manager.dirty_ratio_threshold,
        tombstone_retention: log_manager.tombstone_retention,
        writeback_bytes: log_manager.writeback_bytes,
        drop_cache_after_writeback: log_manager.drop_cache_after_writeback,
        offload_tx: None,
        tiered_provider: None,
    };
    let handle = spawn_shards(num_shards, config);
    ShardedLogManager::new(log_manager, handle)
}
