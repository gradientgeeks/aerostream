use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use crate::log::manager::{PartitionKey, PartitionLog};
use crate::shard::router::ShardRouter;

#[derive(Clone)]
pub struct ShardConfig {
    pub base_dir: PathBuf,
    pub broker_id: u32,
    pub max_segment_size: u64,
    pub max_retention_size: Option<u64>,
    pub max_retention_age: Option<std::time::Duration>,
    pub compaction_enabled: bool,
    pub dirty_ratio_threshold: f64,
    pub tombstone_retention: std::time::Duration,
    pub writeback_bytes: u64,
    pub drop_cache_after_writeback: bool,
    pub offload_tx: Option<tokio::sync::mpsc::Sender<crate::storage::offloader::OffloadTask>>,
    pub tiered_provider: Option<Arc<dyn crate::storage::TieredStorageProvider>>,
}

/// A request sent to a shard thread via a flume channel.
pub enum ShardRequest {
    /// Append raw record bytes to a partition.
    Append {
        topic: String,
        partition: u32,
        data: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<u64, io::Error>>,
    },
    /// Append a pre-encoded Kafka batch slice (used by handle_produce for Kafka wire protocol).
    AppendBatchSlice {
        topic: String,
        partition: u32,
        base_offset: i64,
        batch: Vec<u8>,
        record_count: u64,
        reply: tokio::sync::oneshot::Sender<Result<u64, io::Error>>,
    },
    /// Read from a partition at an offset.
    ReadFromOffset {
        topic: String,
        partition: u32,
        start_offset: u64,
        max_bytes: u32,
        reply: tokio::sync::oneshot::Sender<Result<Option<(Vec<u8>, u64)>, io::Error>>,
    },
    /// Get the next_offset for a partition.
    GetNextOffset {
        topic: String,
        partition: u32,
        reply: tokio::sync::oneshot::Sender<Result<u64, io::Error>>,
    },
    /// Get all offsets across all partitions owned by this shard.
    GetAllOffsets {
        reply: tokio::sync::oneshot::Sender<Vec<(String, u32, i64)>>,
    },
    /// Ensure a partition exists on this shard (create if needed).
    EnsurePartition {
        topic: String,
        partition: u32,
        reply: tokio::sync::oneshot::Sender<Result<(), io::Error>>,
    },
    /// Delete all partitions of a topic on this shard.
    DeleteTopic {
        topic: String,
        reply: tokio::sync::oneshot::Sender<Result<usize, io::Error>>,
    },
    /// Shutdown this shard.
    Shutdown,
}

pub struct ShardEngine {
    shard_id: usize,
    partitions: HashMap<PartitionKey, PartitionLog>,
    rx: flume::Receiver<ShardRequest>,
    config: ShardConfig,
}

impl ShardEngine {
    pub fn new(shard_id: usize, rx: flume::Receiver<ShardRequest>, config: ShardConfig) -> Self {
        Self {
            shard_id,
            partitions: HashMap::new(),
            rx,
            config,
        }
    }

    pub fn run(&mut self) {
        while let Ok(req) = self.rx.recv() {
            match req {
                ShardRequest::Append { topic, partition, data, reply } => {
                    let mut log = self.get_or_create_partition(&topic, partition);
                    let result = log.as_mut().map_err(|e| io::Error::new(e.kind(), e.to_string())).and_then(|l| l.append(&data));
                    let _ = reply.send(result);
                }
                ShardRequest::AppendBatchSlice { topic, partition, base_offset, batch, record_count, reply } => {
                    let mut log = self.get_or_create_partition(&topic, partition);
                    let result = log.as_mut().map_err(|e| io::Error::new(e.kind(), e.to_string())).and_then(|l| l.append_batch_slice(base_offset, &batch, record_count));
                    let _ = reply.send(result);
                }
                ShardRequest::ReadFromOffset { topic, partition, start_offset, max_bytes, reply } => {
                    let mut log = self.get_or_create_partition(&topic, partition);
                    let result = match log {
                        Ok(l) => {
                            match l.read_from_offset(start_offset, max_bytes) {
                                Ok(Some((mut file, pos, len))) => {
                                    let mut buf = vec![0; len as usize];
                                    if let Err(e) = file.seek(SeekFrom::Start(pos)) {
                                        Err(e)
                                    } else if let Err(e) = file.read_exact(&mut buf) {
                                        Err(e)
                                    } else {
                                        Ok(Some((buf, pos))) // Wait, should it be start_offset or what? We will just return start_offset back to the user since pos is physical file position
                                    }
                                }
                                Ok(None) => Ok(None),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                    };
                    
                    // Let's modify the above to properly return start_offset instead of file position
                    let result = result.map(|opt| opt.map(|(buf, _pos)| (buf, start_offset)));
                    let _ = reply.send(result);
                }
                ShardRequest::GetNextOffset { topic, partition, reply } => {
                    let mut log = self.get_or_create_partition(&topic, partition);
                    let result = log.map(|l| l.next_offset).map_err(|e| io::Error::new(e.kind(), e.to_string()));
                    let _ = reply.send(result);
                }
                ShardRequest::GetAllOffsets { reply } => {
                    let mut offsets = Vec::new();
                    for (key, log) in &self.partitions {
                        offsets.push((key.topic.clone(), key.partition, log.next_offset as i64));
                    }
                    let _ = reply.send(offsets);
                }
                ShardRequest::EnsurePartition { topic, partition, reply } => {
                    let result = self.get_or_create_partition(&topic, partition).map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string()));
                    let _ = reply.send(result);
                }
                ShardRequest::DeleteTopic { topic, reply } => {
                    let mut count = 0;
                    let keys: Vec<PartitionKey> = self.partitions.keys().cloned().filter(|k| k.topic == topic).collect();
                    for key in keys {
                        if let Some(_log) = self.partitions.remove(&key) {
                            let partition_dir = self.config.base_dir.join(&topic).join(format!("partition_{}", key.partition));
                            let _ = fs::remove_dir_all(partition_dir);
                            count += 1;
                        }
                    }
                    let _ = reply.send(Ok(count));
                }
                ShardRequest::Shutdown => {
                    break;
                }
            }
        }
    }

    fn get_or_create_partition(&mut self, topic: &str, partition: u32) -> Result<&mut PartitionLog, io::Error> {
        let key = PartitionKey::new(topic, partition);
        if !self.partitions.contains_key(&key) {
            let mut log = PartitionLog::new(
                &self.config.base_dir,
                topic,
                partition,
                self.config.broker_id,
                self.config.max_segment_size,
                self.config.max_retention_size,
                self.config.max_retention_age,
            )?;
            log.compaction_enabled = self.config.compaction_enabled;
            log.dirty_ratio_threshold = self.config.dirty_ratio_threshold;
            log.tombstone_retention = self.config.tombstone_retention;
            log.offload_tx = self.config.offload_tx.clone();
            log.tiered_provider = self.config.tiered_provider.clone();
            log.writeback_bytes = self.config.writeback_bytes;
            log.drop_cache_after_writeback = self.config.drop_cache_after_writeback;
            log.append_notify = Some(Arc::new(tokio::sync::Notify::new()));
            self.partitions.insert(key.clone(), log);
        }
        Ok(self.partitions.get_mut(&key).unwrap())
    }
}

#[derive(Clone)]
pub struct ShardHandle {
    senders: Vec<flume::Sender<ShardRequest>>,
    router: ShardRouter,
}

impl ShardHandle {
    pub async fn append(&self, topic: &str, partition: u32, data: Vec<u8>) -> Result<u64, io::Error> {
        let shard_id = self.router.shard_for(topic, partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.senders[shard_id].send_async(ShardRequest::Append {
            topic: topic.to_string(),
            partition,
            data,
            reply: tx,
        }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard channel closed"))?;
        rx.await.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::BrokenPipe, "reply channel closed")))
    }

    pub async fn append_batch_slice(&self, topic: &str, partition: u32, base_offset: i64, batch: Vec<u8>, record_count: u64) -> Result<u64, io::Error> {
        let shard_id = self.router.shard_for(topic, partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.senders[shard_id].send_async(ShardRequest::AppendBatchSlice {
            topic: topic.to_string(),
            partition,
            base_offset,
            batch,
            record_count,
            reply: tx,
        }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard channel closed"))?;
        rx.await.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::BrokenPipe, "reply channel closed")))
    }

    pub async fn read_from_offset(&self, topic: &str, partition: u32, start_offset: u64, max_bytes: u32) -> Result<Option<(Vec<u8>, u64)>, io::Error> {
        let shard_id = self.router.shard_for(topic, partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.senders[shard_id].send_async(ShardRequest::ReadFromOffset {
            topic: topic.to_string(),
            partition,
            start_offset,
            max_bytes,
            reply: tx,
        }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard channel closed"))?;
        rx.await.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::BrokenPipe, "reply channel closed")))
    }

    pub async fn get_next_offset(&self, topic: &str, partition: u32) -> Result<u64, io::Error> {
        let shard_id = self.router.shard_for(topic, partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.senders[shard_id].send_async(ShardRequest::GetNextOffset {
            topic: topic.to_string(),
            partition,
            reply: tx,
        }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard channel closed"))?;
        rx.await.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::BrokenPipe, "reply channel closed")))
    }

    pub async fn get_all_offsets(&self) -> Vec<(String, u32, i64)> {
        let mut futures = Vec::new();
        for sender in &self.senders {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender.send_async(ShardRequest::GetAllOffsets { reply: tx }).await.is_ok() {
                futures.push(rx);
            }
        }
        
        let mut results = Vec::new();
        for rx in futures {
            if let Ok(offsets) = rx.await {
                results.extend(offsets);
            }
        }
        results
    }

    pub async fn ensure_partition(&self, topic: &str, partition: u32) -> Result<(), io::Error> {
        let shard_id = self.router.shard_for(topic, partition);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.senders[shard_id].send_async(ShardRequest::EnsurePartition {
            topic: topic.to_string(),
            partition,
            reply: tx,
        }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "shard channel closed"))?;
        rx.await.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::BrokenPipe, "reply channel closed")))
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<usize, io::Error> {
        let mut futures = Vec::new();
        for sender in &self.senders {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if sender.send_async(ShardRequest::DeleteTopic { topic: topic.to_string(), reply: tx }).await.is_ok() {
                futures.push(rx);
            }
        }
        
        let mut total_deleted = 0;
        for rx in futures {
            if let Ok(Ok(count)) = rx.await {
                total_deleted += count;
            }
        }
        Ok(total_deleted)
    }

    pub fn shutdown(&self) {
        for sender in &self.senders {
            let _ = sender.send(ShardRequest::Shutdown);
        }
    }
}

pub fn spawn_shards(num_shards: usize, config: ShardConfig) -> ShardHandle {
    let router = ShardRouter::new(num_shards);
    let mut senders = Vec::with_capacity(num_shards);
    
    for shard_id in 0..num_shards {
        let (tx, rx) = flume::unbounded();
        let mut engine = ShardEngine::new(shard_id, rx, config.clone());
        
        // Spawn on dedicated OS thread, pinned to CPU core
        std::thread::Builder::new()
            .name(format!("shard-{}", shard_id))
            .spawn(move || {
                // Pin to CPU core
                unsafe {
                    let num_cpus = std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1);
                    let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_SET(shard_id % num_cpus, &mut cpuset);
                    libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
                }
                tracing::info!("[AeroStream Shard] Shard {} started on dedicated OS thread", shard_id);
                engine.run();
            })
            .expect("failed to spawn shard thread");
        
        senders.push(tx);
    }
    
    ShardHandle { senders, router }
}
