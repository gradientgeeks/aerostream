use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{error, info};

use super::provider::{StorageError, TieredStorageProvider};

/// Task describing a segment to be offloaded to tiered object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadTask {
    pub topic: String,
    pub partition: u32,
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub idx_path: PathBuf,
}

impl OffloadTask {
    pub fn new(
        topic: impl Into<String>,
        partition: u32,
        base_offset: u64,
        log_path: impl Into<PathBuf>,
        idx_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            base_offset,
            log_path: log_path.into(),
            idx_path: idx_path.into(),
        }
    }

    /// Generates the standard object key for a segment file in tiered storage:
    /// `tiered/{topic}/partition_{partition}/{base_offset:020}.{extension}`
    pub fn segment_key(topic: &str, partition: u32, base_offset: u64, extension: &str) -> String {
        format!(
            "tiered/{}/partition_{}/{:020}.{}",
            topic, partition, base_offset, extension
        )
    }

    pub fn log_key(&self) -> String {
        Self::segment_key(&self.topic, self.partition, self.base_offset, "log")
    }

    pub fn idx_key(&self) -> String {
        Self::segment_key(&self.topic, self.partition, self.base_offset, "idx")
    }
}

/// TieredStorageOffloader worker pipeline.
/// Consumes `OffloadTask`s from an async channel and persists the `.log` and `.idx` segment files
/// to the remote object storage provider (`S3`, `GCS`, `Azure`, or `Local`).
pub struct TieredStorageOffloader {
    provider: Arc<dyn TieredStorageProvider>,
    rx: mpsc::Receiver<OffloadTask>,
}

impl TieredStorageOffloader {
    pub fn new(
        provider: Arc<dyn TieredStorageProvider>,
        rx: mpsc::Receiver<OffloadTask>,
    ) -> Self {
        Self { provider, rx }
    }

    /// Spawns the offloader pipeline as a background Tokio task.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Runs the offloader worker loop until the task sender is dropped.
    pub async fn run(&mut self) {
        info!(
            "[AeroMQ Tiered Storage] Offloader worker loop running with provider '{}'",
            self.provider.provider_name()
        );

        while let Some(task) = self.rx.recv().await {
            if let Err(e) = self.process_task(&task).await {
                error!(
                    "[AeroMQ Tiered Storage] Failed to offload segment {} for topic {} partition {}: {}",
                    task.base_offset, task.topic, task.partition, e
                );
            }
        }

        info!("[AeroMQ Tiered Storage] Offloader task channel closed; worker terminating.");
    }

    /// Processes a single offload task by reading `.log` and `.idx` from disk and uploading them.
    pub async fn process_task(&self, task: &OffloadTask) -> Result<(), StorageError> {
        let log_key = task.log_key();
        let idx_key = task.idx_key();

        info!(
            "[AeroMQ Tiered Storage] Starting offload for {}/partition_{} base_offset={}",
            task.topic, task.partition, task.base_offset
        );

        // Read log segment bytes
        let log_bytes = fs::read(&task.log_path).await.map_err(|e| {
            error!(
                "[AeroMQ Tiered Storage] Could not read log segment file {:?}: {}",
                task.log_path, e
            );
            StorageError::Io(e)
        })?;

        // Read index segment bytes
        let idx_bytes = fs::read(&task.idx_path).await.map_err(|e| {
            error!(
                "[AeroMQ Tiered Storage] Could not read index segment file {:?}: {}",
                task.idx_path, e
            );
            StorageError::Io(e)
        })?;

        // Upload log file to provider
        self.provider.put_segment(&log_key, &log_bytes).await?;

        // Upload idx file to provider
        self.provider.put_segment(&idx_key, &idx_bytes).await?;

        info!(
            "[AeroMQ Tiered Storage] Offload completed for {}/partition_{} base_offset={} (log={} bytes, idx={} bytes) -> provider '{}'",
            task.topic,
            task.partition,
            task.base_offset,
            log_bytes.len(),
            idx_bytes.len(),
            self.provider.provider_name()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::LocalStorageProvider;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_offload_task_key_generation() {
        let task = OffloadTask::new(
            "orders",
            3,
            42,
            PathBuf::from("/tmp/00000000000000000042.log"),
            PathBuf::from("/tmp/00000000000000000042.idx"),
        );

        assert_eq!(task.log_key(), "tiered/orders/partition_3/00000000000000000042.log");
        assert_eq!(task.idx_key(), "tiered/orders/partition_3/00000000000000000042.idx");
    }

    #[tokio::test]
    async fn test_offloader_pipeline_with_local_provider() {
        let temp = tempdir().unwrap();
        let src_dir = temp.path().join("source");
        let remote_dir = temp.path().join("remote_tiered");
        fs::create_dir_all(&src_dir).await.unwrap();

        let log_file = src_dir.join("00000000000000000100.log");
        let idx_file = src_dir.join("00000000000000000100.idx");
        fs::write(&log_file, b"sample log segment bytes").await.unwrap();
        fs::write(&idx_file, b"sample idx segment bytes").await.unwrap();

        let provider: Arc<dyn TieredStorageProvider> = Arc::new(LocalStorageProvider::new(&remote_dir));
        let (tx, rx) = mpsc::channel(10);
        let offloader = TieredStorageOffloader::new(provider.clone(), rx);

        let handle = offloader.spawn();

        let task = OffloadTask::new("payments", 1, 100, &log_file, &idx_file);
        tx.send(task.clone()).await.unwrap();

        // Drop sender to allow worker loop to exit
        drop(tx);
        handle.await.unwrap();

        // Verify segments were uploaded to remote provider
        assert!(provider.exists(&task.log_key()).await.unwrap());
        assert!(provider.exists(&task.idx_key()).await.unwrap());

        let fetched_log = provider.get_segment(&task.log_key()).await.unwrap();
        let fetched_idx = provider.get_segment(&task.idx_key()).await.unwrap();
        assert_eq!(fetched_log, b"sample log segment bytes");
        assert_eq!(fetched_idx, b"sample idx segment bytes");
    }
}
