use hashbrown::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info, warn};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionKey {
    pub topic: String,
    pub partition: u32,
}

impl PartitionKey {
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl std::hash::Hash for PartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.partition.hash(state);
    }
}

impl hashbrown::Equivalent<PartitionKey> for (&str, u32) {
    fn equivalent(&self, key: &PartitionKey) -> bool {
        self.0 == key.topic.as_str() && self.1 == key.partition
    }
}

#[derive(Clone, Debug)]
pub struct LogSegment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub idx_path: PathBuf,
}

use crate::log::producer_state::{ProducerStateTracker, SequenceCheckResult};

// Configurations
pub struct PartitionLog {
    pub topic: String,
    pub partition: u32,
    pub partition_dir: PathBuf,
    pub segments: Vec<LogSegment>,
    pub active_log_file: File,
    pub active_idx_file: File,
    pub next_offset: u64,
    // Tracked sizes of the active segment files (avoid a metadata()/lseek syscall per append).
    active_len: u64,
    active_idx_len: u64,
    last_retention_check: std::time::Instant,
    /// Start of the active-segment byte range whose writeback has not been started yet.
    writeback_start: u64,
    /// Start of the range handed to writeback on the previous step (dropped from the page cache next time).
    writeback_prev_start: u64,
    /// Start writeback every this many bytes (0 disables). See `write_back_progress`.
    pub writeback_bytes: u64,
    /// After a range has been written back, drop it from the page cache (POSIX_FADV_DONTNEED).
    pub drop_cache_after_writeback: bool,

    // Configurations
    pub max_segment_size: u64,
    pub broker_id: u32,
    // Root of the broker's configured storage directory (i.e. the `base_dir`
    // passed into `PartitionLog::new`). Cold-storage segments are nested
    // under this same root, rather than a hardcoded path relative to the
    // process's current working directory.
    storage_base_dir: PathBuf,
    pub max_retention_size: Option<u64>,
    pub max_retention_age: Option<std::time::Duration>,

    // Compaction configuration
    pub compaction_enabled: bool,
    pub dirty_ratio_threshold: f64,
    pub tombstone_retention: std::time::Duration,

    // Tiered storage offloader and provider
    pub offload_tx: Option<tokio::sync::mpsc::Sender<crate::storage::offloader::OffloadTask>>,
    pub tiered_provider: Option<Arc<dyn crate::storage::TieredStorageProvider>>,

    // High-Watermark and replica tracking
    pub high_watermark: u64,
    pub replica_offsets: HashMap<u32, u64>,
    pub replica_ids: Vec<u32>,

    // Idempotent producer sequence tracking
    pub producer_tracker: ProducerStateTracker,
    pub producer_states: HashMap<i64, ProducerState>,

    // Transaction index (ongoing txns for LSO, aborted txns for read_committed fetch)
    pub txn_index: crate::txn::PartitionTxnIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerState {
    pub last_sequence: i32,
    pub last_offset: i64,
}

impl PartitionLog {
    pub fn new(
        base_dir: &Path,
        topic: &str,
        partition: u32,
        broker_id: u32,
        max_segment_size: u64,
        max_retention_size: Option<u64>,
        max_retention_age: Option<std::time::Duration>,
    ) -> io::Result<Self> {
        let storage_base_dir = base_dir.to_path_buf();
        let partition_dir = base_dir
            .join(topic)
            .join(format!("partition_{}", partition));

        fs::create_dir_all(&partition_dir)?;

        // Migration step: rename old style files if they exist
        let old_log = partition_dir.join("partition.log");
        let old_idx = partition_dir.join("partition.idx");
        if old_log.exists() {
            let new_log = partition_dir.join(format!("{:020}.log", 0));
            let new_idx = partition_dir.join(format!("{:020}.idx", 0));
            fs::rename(&old_log, &new_log)?;
            if old_idx.exists() {
                fs::rename(&old_idx, &new_idx)?;
            }
        }

        // Discover segments
        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&partition_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "log" {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name_str) = stem.to_str() {
                                if name_str.len() == 20 {
                                    if let Ok(offset) = name_str.parse::<u64>() {
                                        base_offsets.push(offset);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        base_offsets.sort_unstable();

        if base_offsets.is_empty() {
            base_offsets.push(0);
        }

        let mut segments = Vec::new();
        for &offset in &base_offsets {
            let log_path = partition_dir.join(format!("{:020}.log", offset));
            let idx_path = partition_dir.join(format!("{:020}.idx", offset));
            segments.push(LogSegment {
                base_offset: offset,
                log_path,
                idx_path,
            });
        }

        let active_seg = segments.last().unwrap();
        let active_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&active_seg.log_path)?;

        let mut active_idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&active_seg.idx_path)?;

        // Determine next offset from the last index entry of active segment
        let index_len = active_idx_file.metadata()?.len();
        let next_offset = if index_len >= 16 {
            active_idx_file.seek(SeekFrom::Start(index_len - 16))?;
            let mut buf = [0u8; 16];
            active_idx_file.read_exact(&mut buf)?;
            u64::from_be_bytes(buf[0..8].try_into().unwrap()) + 1
        } else {
            active_seg.base_offset
        };

        // Appends use positioned writes at the tracked lengths
        let active_len = active_log_file.metadata()?.len();
        let active_idx_len = index_len;

        let partition_dir_for_txn = partition_dir.clone();
        let mut log = Self {
            topic: topic.to_string(),
            partition,
            partition_dir,
            segments,
            active_log_file,
            active_idx_file,
            next_offset,
            active_len,
            active_idx_len,
            last_retention_check: std::time::Instant::now(),
            writeback_start: active_len,
            writeback_prev_start: active_len,
            writeback_bytes: 0,
            drop_cache_after_writeback: false,
            max_segment_size,
            broker_id,
            storage_base_dir,
            max_retention_size,
            max_retention_age,
            compaction_enabled: false,
            dirty_ratio_threshold: 0.5,
            tombstone_retention: std::time::Duration::from_secs(86400),
            offload_tx: None,
            tiered_provider: None,
            high_watermark: 0,
            replica_offsets: HashMap::new(),
            replica_ids: Vec::new(),
            producer_tracker: ProducerStateTracker::new(),
            producer_states: HashMap::new(),
            txn_index: crate::txn::PartitionTxnIndex::open(&partition_dir_for_txn),
        };
        log.recompute_high_watermark();
        Ok(log)
    }

    /// Directory where cold-tier segments for this partition are stored,
    /// nested under the broker's configured storage directory:
    /// `<storage_base_dir>/cold_storage/broker_{id}/<topic>/partition_{n}`.
    fn cold_dir(&self) -> PathBuf {
        self.storage_base_dir
            .join("cold_storage")
            .join(format!("broker_{}", self.broker_id))
            .join(&self.topic)
            .join(format!("partition_{}", self.partition))
    }

    pub fn update_follower_offset(&mut self, replica_id: u32, offset: u64) {
        self.replica_offsets.insert(replica_id, offset);
        self.recompute_high_watermark();
    }

    pub fn recompute_high_watermark(&mut self) {
        let has_other_replicas = self.replica_ids.iter().any(|&id| id != self.broker_id);
        if !has_other_replicas {
            self.high_watermark = self.next_offset;
            return;
        }

        let mut min_offset = self.next_offset;
        for &rep_id in &self.replica_ids {
            if rep_id != self.broker_id {
                let rep_offset = self.replica_offsets.get(&rep_id).copied().unwrap_or(0);
                if rep_offset < min_offset {
                    min_offset = rep_offset;
                }
            }
        }
        self.high_watermark = min_offset;
    }

    /// Checks and updates producer sequence using `ProducerStateTracker`.
    pub fn check_and_update_producer(
        &mut self,
        producer_id: i64,
        epoch: i16,
        base_sequence: i32,
        record_count: i32,
    ) -> SequenceCheckResult {
        let next_off = self.next_offset;
        self.producer_tracker.check_and_update_sequence(
            producer_id,
            epoch,
            base_sequence,
            record_count,
            next_off,
        )
    }

    /// Validates sequence number for idempotent producer.
    /// Returns Ok(None) if sequence is valid and should be appended.
    /// Returns Ok(Some(last_offset)) if duplicate sequence (return duplicate ACK without appending).
    /// Returns Err(45) if sequence gap detected (OutOfOrderSequenceNumber).
    pub fn validate_idempotent_produce(&self, producer_id: i64, base_sequence: i32) -> Result<Option<i64>, i16> {
        if producer_id < 0 {
            return Ok(None);
        }
        if let Some(state) = self.producer_tracker.get_producer_state(producer_id) {
            let last_seq = state.last_sequence;
            if base_sequence <= last_seq {
                // Duplicate message sequence -> return duplicate ACK without writing
                let cached = self
                    .producer_tracker
                    .sequence_offsets
                    .get(&(producer_id, base_sequence))
                    .copied()
                    .unwrap_or(state.last_offset);
                Ok(Some(cached as i64))
            } else if base_sequence == last_seq + 1 {
                // Monotonically ordered next sequence
                Ok(None)
            } else {
                // Sequence gap! e.g., seq 5 when last seq was 0
                Err(45) // OutOfOrderSequenceNumber
            }
        } else {
            if base_sequence == 0 {
                // First sequence from new producer must be 0
                Ok(None)
            } else {
                // Sequence gap from uninitialized producer
                Err(45) // OutOfOrderSequenceNumber
            }
        }
    }

    /// Records sequence number and offset for an idempotent producer.
    pub fn update_producer_state(&mut self, producer_id: i64, base_sequence: i32, records_count: i32, offset: i64) {
        if producer_id >= 0 {
            let last_seq = base_sequence + records_count.max(1) - 1;
            let _ = self.producer_tracker.check_and_update_sequence(
                producer_id,
                0,
                base_sequence,
                records_count,
                offset.max(0) as u64,
            );
            self.producer_states.insert(producer_id, ProducerState {
                last_sequence: last_seq,
                last_offset: offset,
            });
        }
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;

        let mut rolled = false;
        if self.active_len + data.len() as u64 > self.max_segment_size && self.active_len > 0 {
            self.roll_over()?;
            rolled = true;
        }

        // Positioned writes at the tracked lengths: no metadata(), lseek or split index writes per record.
        let pos = self.active_len;
        self.active_log_file.write_all_at(data, pos)?;
        self.active_len += data.len() as u64;
        self.write_back_progress();

        let offset = self.next_offset;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..].copy_from_slice(&pos.to_be_bytes());
        self.active_idx_file.write_all_at(&entry, self.active_idx_len)?;
        self.active_idx_len += 16;

        self.next_offset += 1;

        // Retention is enforced on a timer (like Kafka's log cleaner) and after a roll, not after every record:
        // it stats every segment file.
        if rolled || self.last_retention_check.elapsed() >= std::time::Duration::from_secs(1) {
            self.clean_retention()?;
            self.last_retention_check = std::time::Instant::now();
        }

        self.recompute_high_watermark();
        Ok(offset)
    }

    /// Appends a raw record batch slice with in-place base-offset patching directly on disk.
    /// This bypasses heap cloning of large multi-megabyte record batches.
    pub fn append_batch_slice(&mut self, base_offset: i64, batch: &[u8]) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;

        let mut rolled = false;
        if self.active_len + batch.len() as u64 > self.max_segment_size && self.active_len > 0 {
            self.roll_over()?;
            rolled = true;
        }

        let pos = self.active_len;
        // In-place base offset patching directly at disk position without cloning the batch:
        let off_bytes = base_offset.to_be_bytes();
        self.active_log_file.write_all_at(&off_bytes, pos)?;
        if batch.len() > 8 {
            self.active_log_file.write_all_at(&batch[8..], pos + 8)?;
        }
        self.active_len += batch.len() as u64;
        self.write_back_progress();

        let offset = self.next_offset;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..].copy_from_slice(&pos.to_be_bytes());
        self.active_idx_file.write_all_at(&entry, self.active_idx_len)?;
        self.active_idx_len += 16;

        self.next_offset += 1;

        if rolled || self.last_retention_check.elapsed() >= std::time::Duration::from_secs(1) {
            self.clean_retention()?;
            self.last_retention_check = std::time::Instant::now();
        }

        self.recompute_high_watermark();
        Ok(offset)
    }

    /// Compacts closed segments using key-offset deduplication and tombstone eviction.
    pub fn compact_partition_with_stats(&mut self) -> io::Result<crate::log::compactor::CompactionStats> {
        if self.segments.len() <= 1 {
            return Ok(crate::log::compactor::CompactionStats::default());
        }
        let active_seg = self.segments.pop().unwrap();
        let mut closed = std::mem::take(&mut self.segments);
        let res = crate::log::compactor::compact_segments(&self.partition_dir, &mut closed, self.tombstone_retention);
        closed.push(active_seg);
        self.segments = closed;
        res
    }

    /// Compacts closed segments if compaction is enabled.
    pub fn compact_partition(&mut self) -> io::Result<()> {
        let _ = self.compact_partition_with_stats()?;
        Ok(())
    }

    /// Computes the ratio of dirty (redundant / expired) bytes in closed segments.
    pub fn compute_dirty_ratio(&self) -> io::Result<f64> {
        if self.segments.len() <= 1 {
            return Ok(0.0);
        }
        let closed = &self.segments[..self.segments.len() - 1];
        crate::log::compactor::compute_dirty_ratio(closed, self.tombstone_retention)
    }

    /// Paced writeback of the active segment (like RocksDB's `bytes_per_sync`).
    ///
    /// Appends only dirty the page cache. Left alone, dirty pages pile up until the kernel (or, in a container, the
    /// memory cgroup limit) forces the writer to wait for a large flush, which shows up as multi-second stalls. Every
    /// `writeback_bytes` we ask the kernel to start writing the newly appended range (`SYNC_FILE_RANGE_WRITE`, does not
    /// wait). With `drop_cache_after_writeback`, the range started on the previous step is waited for and dropped from
    /// the page cache, which caps the broker's page-cache footprint and throttles the writer to the disk's speed.
    fn write_back_progress(&mut self) {
        if self.writeback_bytes == 0 || self.active_len - self.writeback_start < self.writeback_bytes {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.active_log_file.as_raw_fd();
            let (start, len) = (self.writeback_start, self.active_len - self.writeback_start);
            // SAFETY: plain syscalls on a valid, open file descriptor; failures are only advisory.
            unsafe {
                libc::sync_file_range(fd, start as libc::off64_t, len as libc::off64_t, libc::SYNC_FILE_RANGE_WRITE);
                if self.drop_cache_after_writeback && self.writeback_prev_start < start {
                    let (ps, pl) = (self.writeback_prev_start, start - self.writeback_prev_start);
                    libc::sync_file_range(
                        fd,
                        ps as libc::off64_t,
                        pl as libc::off64_t,
                        libc::SYNC_FILE_RANGE_WAIT_BEFORE | libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER,
                    );
                    libc::posix_fadvise(fd, ps as libc::off_t, pl as libc::off_t, libc::POSIX_FADV_DONTNEED);
                }
            }
            self.writeback_prev_start = start;
        }
        self.writeback_start = self.active_len;
    }

    /// Archives a sealed segment into `cold_dir`. A hard link shares the file's data blocks, so this costs no copy and
    /// no extra page cache, and the archived copy survives retention deleting the local name (compaction replaces
    /// segment files by rename, so the link keeps the segment exactly as it was sealed). If linking is not possible
    /// (different filesystem), the segment is copied on a background thread instead of on the append path.
    fn archive_sealed_segment(&self, seg: &LogSegment) -> io::Result<()> {
        let cold_dir = self.cold_dir();
        fs::create_dir_all(&cold_dir)?;
        let cold_log_path = cold_dir.join(format!("{:020}.log", seg.base_offset));
        let cold_idx_path = cold_dir.join(format!("{:020}.idx", seg.base_offset));
        let _ = fs::remove_file(&cold_log_path);
        let _ = fs::remove_file(&cold_idx_path);

        let offload = self.offload_tx.clone().map(|tx| {
            (tx, crate::storage::offloader::OffloadTask::new(
                &self.topic, self.partition, seg.base_offset, &cold_log_path, &cold_idx_path,
            ))
        });

        if fs::hard_link(&seg.log_path, &cold_log_path).is_ok() && fs::hard_link(&seg.idx_path, &cold_idx_path).is_ok() {
            debug!("[AeroMQ Broker] Segment {} sealed; linked into cold storage {:?}", seg.base_offset, cold_log_path);
            if let Some((tx, task)) = offload {
                let _ = tx.try_send(task);
            }
            return Ok(());
        }
        let _ = fs::remove_file(&cold_log_path);
        // Open the sources now: retention may unlink them before the copy runs; open handles keep the data readable.
        let mut src_log = File::open(&seg.log_path)?;
        let mut src_idx = File::open(&seg.idx_path)?;
        let base = seg.base_offset;
        std::thread::spawn(move || {
            let mut copy = || -> io::Result<()> {
                io::copy(&mut src_log, &mut File::create(&cold_log_path)?)?;
                io::copy(&mut src_idx, &mut File::create(&cold_idx_path)?)?;
                Ok(())
            };
            match copy() {
                Ok(()) => {
                    info!("[AeroMQ Broker] Segment {} copied to cold storage {:?} (background)", base, cold_log_path);
                    if let Some((tx, task)) = offload {
                        let _ = tx.try_send(task);
                    }
                }
                Err(e) => warn!("[AeroMQ Broker] Background cold-storage copy of segment {} failed: {}", base, e),
            }
        });
        Ok(())
    }

    fn roll_over(&mut self) -> io::Result<()> {
        self.active_log_file.flush()?;
        self.active_idx_file.flush()?;

        let old_active_seg = self.segments.last().unwrap().clone();

        // Start writeback of the unflushed tail of the sealed segment, then archive it (link, no data copy).
        if self.writeback_bytes > 0 && self.active_len > self.writeback_start {
            #[cfg(target_os = "linux")]
            unsafe {
                use std::os::unix::io::AsRawFd;
                libc::sync_file_range(
                    self.active_log_file.as_raw_fd(),
                    self.writeback_start as libc::off64_t,
                    (self.active_len - self.writeback_start) as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                );
            }
        }
        self.archive_sealed_segment(&old_active_seg)?;

        // Create new active segment
        let new_base_offset = self.next_offset;
        let new_log_path = self.partition_dir.join(format!("{:020}.log", new_base_offset));
        let new_idx_path = self.partition_dir.join(format!("{:020}.idx", new_base_offset));

        let new_log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_log_path)?;

        let new_idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_idx_path)?;

        self.active_log_file = new_log_file;
        self.active_idx_file = new_idx_file;
        self.active_len = 0;
        self.active_idx_len = 0;
        self.writeback_start = 0;
        self.writeback_prev_start = 0;

        self.segments.push(LogSegment {
            base_offset: new_base_offset,
            log_path: new_log_path,
            idx_path: new_idx_path,
        });

        Ok(())
    }

    fn clean_retention(&mut self) -> io::Result<()> {
        if let Some(max_size) = self.max_retention_size {
            while self.segments.len() > 1 {
                let mut total_size = 0;
                for seg in &self.segments {
                    if let Ok(meta) = fs::metadata(&seg.log_path) {
                        total_size += meta.len();
                    }
                    if let Ok(meta) = fs::metadata(&seg.idx_path) {
                        total_size += meta.len();
                    }
                }

                if total_size <= max_size {
                    break;
                }

                let oldest = self.segments.remove(0);
                let _ = fs::remove_file(&oldest.log_path);
                let _ = fs::remove_file(&oldest.idx_path);
                info!(
                    "[AeroMQ Broker] Retention cleaner: total directory size ({}) exceeded limit ({}). Removed local segment {}",
                    total_size, max_size, oldest.base_offset
                );
            }
        }

        if let Some(max_age) = self.max_retention_age {
            let now = std::time::SystemTime::now();
            while self.segments.len() > 1 {
                let oldest_log = &self.segments[0].log_path;
                let should_remove = if let Ok(meta) = fs::metadata(oldest_log) {
                    if let Ok(modified) = meta.modified() {
                        if let Ok(duration) = now.duration_since(modified) {
                            duration > max_age
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !should_remove {
                    break;
                }

                let oldest = self.segments.remove(0);
                let _ = fs::remove_file(&oldest.log_path);
                let _ = fs::remove_file(&oldest.idx_path);
                info!(
                    "[AeroMQ Broker] Retention cleaner: age threshold exceeded. Removed local segment {}",
                    oldest.base_offset
                );
            }
        }

        Ok(())
    }

    fn find_cold_segment(&self, start_offset: u64) -> io::Result<Option<LogSegment>> {
        let cold_dir = self.cold_dir();

        if !cold_dir.exists() {
            return Ok(None);
        }

        let mut base_offsets = Vec::new();
        for entry in fs::read_dir(&cold_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "log" {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name_str) = stem.to_str() {
                                if name_str.len() == 20 {
                                    if let Ok(offset) = name_str.parse::<u64>() {
                                        base_offsets.push(offset);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if base_offsets.is_empty() {
            return Ok(None);
        }

        base_offsets.sort_unstable();

        let mut low = 0;
        let mut high = base_offsets.len() - 1;
        let mut found_offset = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_base = base_offsets[mid];
            if mid_base <= start_offset {
                found_offset = Some(mid_base);
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }

        if let Some(offset) = found_offset {
            let log_path = cold_dir.join(format!("{:020}.log", offset));
            let idx_path = cold_dir.join(format!("{:020}.idx", offset));
            Ok(Some(LogSegment {
                base_offset: offset,
                log_path,
                idx_path,
            }))
        } else {
            Ok(None)
        }
    }

    /// Read data starting from a specific offset.
    /// Returns the file handle, the starting position in the file, and the number of bytes to read.
    pub fn read_from_offset(&mut self, start_offset: u64, max_bytes: u32) -> io::Result<Option<(File, u64, u32)>> {
        let mut seg = None;

        if !self.segments.is_empty() && start_offset >= self.segments[0].base_offset {
            // Binary search local segments
            let mut low = 0;
            let mut high = self.segments.len() - 1;
            let mut found_seg_idx = None;

            while low <= high {
                let mid = low + (high - low) / 2;
                let mid_base = self.segments[mid].base_offset;
                if mid_base <= start_offset {
                    found_seg_idx = Some(mid);
                    low = mid + 1;
                } else {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }

            if let Some(idx) = found_seg_idx {
                seg = Some(self.segments[idx].clone());
            }
        }

        if seg.is_none() {
            // Try cold storage
            seg = self.find_cold_segment(start_offset)?;
        }

        let seg = match seg {
            Some(s) => s,
            None => return Ok(None),
        };

        // Binary search on the segment's index file
        let mut idx_file = File::open(&seg.idx_path)?;
        let index_len = idx_file.metadata()?.len();
        let num_entries = index_len / 16;
        if num_entries == 0 {
            return Ok(None);
        }

        let mut low = 0;
        let mut high = num_entries - 1;
        let mut found_idx = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            idx_file.seek(SeekFrom::Start(mid * 16))?;
            let mut buf = [0u8; 16];
            idx_file.read_exact(&mut buf)?;

            let offset = u64::from_be_bytes(buf[0..8].try_into().unwrap());
            
            if offset >= start_offset {
                found_idx = Some(mid);
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        let entry_idx = match found_idx {
            Some(idx) => idx,
            None => return Ok(None),
        };

        idx_file.seek(SeekFrom::Start(entry_idx * 16))?;
        let mut buf = [0u8; 16];
        idx_file.read_exact(&mut buf)?;
        let position = u64::from_be_bytes(buf[8..16].try_into().unwrap());

        let log_file = File::open(&seg.log_path)?;
        let log_len = log_file.metadata()?.len();
        let end_pos = if entry_idx + 1 < num_entries {
            idx_file.seek(SeekFrom::Start((entry_idx + 1) * 16))?;
            let mut next_buf = [0u8; 16];
            idx_file.read_exact(&mut next_buf)?;
            u64::from_be_bytes(next_buf[8..16].try_into().unwrap())
        } else {
            log_len
        };

        let bytes_to_read = std::cmp::min((end_pos - position) as u32, max_bytes);
        if bytes_to_read == 0 {
            return Ok(None);
        }

        Ok(Some((log_file, position, bytes_to_read)))
    }
}

pub struct LogManager {
    base_dir: PathBuf,
    pub broker_id: u32,
    pub max_segment_size: u64,
    pub max_retention_size: Option<u64>,
    pub max_retention_age: Option<std::time::Duration>,

    // Compaction configuration
    pub compaction_enabled: bool,
    pub dirty_ratio_threshold: f64,
    pub tombstone_retention: std::time::Duration,

    // Tiered storage offloader and provider
    pub offload_tx: Option<tokio::sync::mpsc::Sender<crate::storage::offloader::OffloadTask>>,
    pub tiered_provider: Option<Arc<dyn crate::storage::TieredStorageProvider>>,

    // Paced writeback (see PartitionLog::write_back_progress)
    pub writeback_bytes: u64,
    pub drop_cache_after_writeback: bool,

    partitions: RwLock<HashMap<PartitionKey, Arc<Mutex<PartitionLog>>>>,

    /// Lazily-created transaction coordinator (see `crate::txn`).
    pub txn_coord: std::sync::OnceLock<Arc<crate::txn::TxnCoordinator>>,
    /// Lazily-created share-group coordinator (see `crate::share`).
    pub share_coord: std::sync::OnceLock<Arc<crate::share::ShareCoordinator>>,
}

impl LogManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, broker_id: u32) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            broker_id,
            max_segment_size: 210, // 210 bytes default for testing/prototype segment rolling
            max_retention_size: Some(256 * 1024), // 256KB default
            max_retention_age: Some(std::time::Duration::from_secs(3600)), // 1 hour default
            compaction_enabled: false,
            dirty_ratio_threshold: 0.5,
            tombstone_retention: std::time::Duration::from_secs(86400),
            offload_tx: None,
            tiered_provider: None,
            writeback_bytes: 0,
            drop_cache_after_writeback: false,
            partitions: RwLock::new(HashMap::new()),
            txn_coord: std::sync::OnceLock::new(),
            share_coord: std::sync::OnceLock::new(),
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn with_limits(
        mut self,
        max_segment_size: u64,
        max_retention_size: Option<u64>,
        max_retention_age: Option<std::time::Duration>,
    ) -> Self {
        self.max_segment_size = max_segment_size;
        self.max_retention_size = max_retention_size;
        self.max_retention_age = max_retention_age;
        self
    }

    pub fn with_compaction(
        mut self,
        enabled: bool,
        dirty_ratio_threshold: f64,
        tombstone_retention: std::time::Duration,
    ) -> Self {
        self.compaction_enabled = enabled;
        self.dirty_ratio_threshold = dirty_ratio_threshold;
        self.tombstone_retention = tombstone_retention;
        self
    }

    pub fn with_writeback(mut self, writeback_bytes: u64, drop_cache_after_writeback: bool) -> Self {
        self.writeback_bytes = writeback_bytes;
        self.drop_cache_after_writeback = drop_cache_after_writeback;
        self
    }

    pub fn with_tiered_storage(
        mut self,
        provider: Arc<dyn crate::storage::TieredStorageProvider>,
        offload_tx: tokio::sync::mpsc::Sender<crate::storage::offloader::OffloadTask>,
    ) -> Self {
        self.tiered_provider = Some(provider);
        self.offload_tx = Some(offload_tx);
        self
    }

    pub async fn get_all_offsets(&self) -> Vec<(String, u32, i64)> {
        // Snapshot Arc references under shared read lock, then drop the map lock
        // to prevent lock convoy / inversion with partition locks.
        let snapshot: Vec<(String, u32, Arc<Mutex<PartitionLog>>)> = {
            let parts = self.partitions.read().await;
            parts
                .iter()
                .map(|(k, v)| (k.topic.clone(), k.partition, Arc::clone(v)))
                .collect()
        }; // <-- Read lock dropped here!

        let mut offsets = Vec::with_capacity(snapshot.len());
        for (topic, partition, part_log_arc) in snapshot {
            let part_log = part_log_arc.lock().await;
            offsets.push((topic, partition, part_log.next_offset as i64));
        }
        offsets
    }

    /// Lists partition ids of `topic` present on local disk (used by the Iceberg tailer).
    pub async fn partitions_for_topic(&self, topic: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = Vec::new();
        if let Ok(rd) = fs::read_dir(self.base_dir.join(topic)) {
            for e in rd.flatten() {
                if let Some(n) = e.file_name().to_str().and_then(|s| s.strip_prefix("partition_")) {
                    if let Ok(id) = n.parse::<u32>() {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids
    }

    pub async fn compact_eligible_partitions(&self) -> io::Result<usize> {
        // Snapshot Arcs under shared read lock, then drop map lock so compaction
        // disk I/O does not block incoming partition requests.
        let eligible: Vec<Arc<Mutex<PartitionLog>>> = {
            let parts = self.partitions.read().await;
            parts.values().cloned().collect()
        }; // <-- Read lock dropped here!

        let mut compacted = 0;
        for part_arc in eligible {
            let mut part = part_arc.lock().await;
            if !part.compaction_enabled {
                continue;
            }
            let dirty_ratio = part.compute_dirty_ratio().unwrap_or(0.0);
            if dirty_ratio >= part.dirty_ratio_threshold {
                part.compact_partition()?;
                compacted += 1;
            }
        }
        Ok(compacted)
    }

    pub fn spawn_cleaner_loop(self: Arc<Self>, interval: std::time::Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = self.compact_eligible_partitions().await {
                    tracing::error!("[AeroMQ Broker] Background compaction cleaner error: {:?}", e);
                }
            }
        })
    }

    /// Drop all local partitions of `topic` and remove their on-disk directories (used by DeleteTopics).
    /// Returns the number of partitions removed.
    pub async fn delete_topic(&self, topic: &str) -> io::Result<usize> {
        // Extract matching partitions under exclusive write lock, then release map lock
        // before executing disk deletion and awaiting partition locks.
        let removed_partitions: Vec<Arc<Mutex<PartitionLog>>> = {
            let mut parts = self.partitions.write().await;
            let keys_to_remove: Vec<PartitionKey> = parts
                .keys()
                .filter(|k| k.topic == topic)
                .cloned()
                .collect();
            let mut logs = Vec::with_capacity(keys_to_remove.len());
            for k in keys_to_remove {
                if let Some(log) = parts.remove(&k) {
                    logs.push(log);
                }
            }
            logs
        }; // <-- Write lock dropped here!

        let mut removed = 0;
        for log in removed_partitions {
            let dir = log.lock().await.partition_dir.clone();
            if dir.exists() {
                std::fs::remove_dir_all(&dir)?;
            }
            removed += 1;
        }
        Ok(removed)
    }

    pub async fn get_partition(&self, topic: &str, partition: u32) -> io::Result<Arc<Mutex<PartitionLog>>> {
        // Fast path: shared read lock with zero-allocation lookup
        {
            let parts = self.partitions.read().await;
            if let Some(log) = parts.get(&(topic, partition)) {
                return Ok(log.clone());
            }
        }

        // Slow path: acquire exclusive write lock to instantiate and insert new partition
        let mut parts = self.partitions.write().await;
        // Double-check under write lock
        if let Some(log) = parts.get(&(topic, partition)) {
            return Ok(log.clone());
        }

        let mut log = PartitionLog::new(
            &self.base_dir,
            topic,
            partition,
            self.broker_id,
            self.max_segment_size,
            self.max_retention_size,
            self.max_retention_age,
        )?;
        log.compaction_enabled = self.compaction_enabled;
        log.dirty_ratio_threshold = self.dirty_ratio_threshold;
        log.tombstone_retention = self.tombstone_retention;
        log.offload_tx = self.offload_tx.clone();
        log.tiered_provider = self.tiered_provider.clone();
        log.writeback_bytes = self.writeback_bytes;
        log.drop_cache_after_writeback = self.drop_cache_after_writeback;

        let shared = Arc::new(Mutex::new(log));
        parts.insert(PartitionKey::new(topic, partition), shared.clone());
        Ok(shared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_segmented_log_rollover_and_cold_storage() {
        let test_dir = PathBuf::from("./data/test_temp_segmented_log");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        // Cold storage for broker 99 is now derived from the configured
        // storage dir (test_dir), not a hardcoded "./data/cold_storage" path.
        let cold_dir = test_dir.join("cold_storage").join("broker_99");
        let _ = fs::remove_dir_all(&cold_dir);

        let manager = LogManager::new(&test_dir, 99)
            .with_limits(100, Some(250), None); // small limits for testing

        let part_log_arc = manager.get_partition("test-topic", 0).await.unwrap();
        
        let mut log = part_log_arc.lock().await;
        
        // Append 40 bytes. Max segment size is 100 bytes.
        let off0 = log.append(&[1; 40]).unwrap();
        let off1 = log.append(&[2; 40]).unwrap();
        assert_eq!(off0, 0);
        assert_eq!(off1, 1);
        assert_eq!(log.segments.len(), 1);

        // This append should trigger rollover because 80 + 40 = 120 > 100
        let off2 = log.append(&[3; 40]).unwrap();
        assert_eq!(off2, 2);
        
        // We should have 2 segments now
        assert_eq!(log.segments.len(), 2);
        assert_eq!(log.segments[0].base_offset, 0);
        assert_eq!(log.segments[1].base_offset, 2);

        // Check that segment 0 exists in cold storage
        let cold_log_path = cold_dir
            .join("test-topic")
            .join("partition_0")
            .join(format!("{:020}.log", 0));
        assert!(cold_log_path.exists());

        // Write more to trigger retention deletion of segment 0 locally
        // Total retention limit is 250 bytes.
        let _off3 = log.append(&[4; 40]).unwrap(); // segment 1 size = 80b
        let _off4 = log.append(&[5; 40]).unwrap(); // rollover! segment 2 starts at offset 4
        let _off5 = log.append(&[6; 40]).unwrap(); // segment 2 size = 80b

        // Local segments:
        // Segment 0 (offset 0): size is 80b log + 32b idx = 112b
        // Segment 2 (offset 2): size is 80b log + 32b idx = 112b
        // Segment 4 (offset 4): size is 80b log + 32b idx = 112b
        // Total = 336b > 250b limit. So segment 0 should be deleted locally.
        
        assert!(!log.segments.iter().any(|s| s.base_offset == 0));
        
        // But we should still be able to read offset 0 and 1 seamlessly from cold storage!
        let read_res = log.read_from_offset(0, 40).unwrap();
        assert!(read_res.is_some());
        let (mut file, pos, bytes) = read_res.unwrap();
        assert_eq!(bytes, 40);
        let mut buf = vec![0; bytes as usize];
        file.seek(SeekFrom::Start(pos)).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![1; 40]);

        // Clean up
        let _ = fs::remove_dir_all(&test_dir);
        let _ = fs::remove_dir_all(&cold_dir);
    }

    #[tokio::test]
    async fn writeback_and_cache_drop_keep_data_intact_across_rolls() {
        let dir = tempfile::tempdir().unwrap();
        let manager = LogManager::new(dir.path(), 7)
            .with_limits(64 * 1024, None, None)
            .with_writeback(8 * 1024, true);
        let part = manager.get_partition("wb", 0).await.unwrap();
        let mut log = part.lock().await;
        let payload = |i: u64| vec![(i % 251) as u8; 3000];
        for i in 0..100u64 {
            assert_eq!(log.append(&payload(i)).unwrap(), i);
        }
        assert!(log.segments.len() > 1, "expected segment rolls");
        for i in [0u64, 21, 22, 57, 99] {
            let (file, pos, len) = log.read_from_offset(i, 3000).unwrap().unwrap();
            let mut buf = vec![0u8; len as usize];
            std::os::unix::fs::FileExt::read_exact_at(&file, &mut buf, pos).unwrap();
            assert_eq!(buf, payload(i), "offset {}", i);
        }
    }

    #[tokio::test]
    async fn sealed_segment_is_hard_linked_into_cold_storage() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let manager = LogManager::new(dir.path(), 8).with_limits(100, None, None);
        let part = manager.get_partition("cold", 0).await.unwrap();
        let mut log = part.lock().await;
        log.append(&[1; 60]).unwrap();
        log.append(&[2; 60]).unwrap(); // rolls segment 0
        let local = log.segments[0].log_path.clone();
        let cold = dir.path().join("cold_storage").join("broker_8").join("cold").join("partition_0").join(format!("{:020}.log", 0));
        assert!(cold.exists());
        assert_eq!(fs::metadata(&local).unwrap().ino(), fs::metadata(&cold).unwrap().ino(), "archived by hard link, not copy");
        fs::remove_file(&local).unwrap(); // as retention would
        assert_eq!(fs::read(&cold).unwrap(), vec![1u8; 60]);
    }
}
