use hashbrown::HashMap;
use std::fs::{self, File};
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

use crate::log::fd_pool::{FdPool, PooledFile};
use crate::log::producer_state::{ProducerStateTracker, SequenceCheckResult};
use crate::log::sparse_index::SparseIndex;

/// Partitions idle this long are made dormant (0 disables). Set from `storage.partition_idle_secs`.
static IDLE_EVICT_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(300);

pub fn set_idle_evict_secs(secs: u64) {
    IDLE_EVICT_SECS.store(secs, std::sync::atomic::Ordering::Relaxed);
}

// Configurations
pub struct PartitionLog {
    pub topic: String,
    pub partition: u32,
    pub partition_dir: PathBuf,
    pub segments: Vec<LogSegment>,
    /// Active segment handles live in the shared LRU `FdPool` (Phase 10): no fd is pinned per partition.
    active_log: PooledFile,
    active_idx: PooledFile,
    /// L1 sparse indexes keyed by idx path, built lazily from the on-disk L2 index (see `sparse_index`).
    l1: HashMap<PathBuf, SparseIndex>,
    /// Last append/fetch; drives dormant-partition eviction.
    last_access: std::time::Instant,
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
    /// Woken after every append so long-polling Fetch requests (min_bytes / max_wait_ms) can re-check.
    pub append_notify: Option<Arc<tokio::sync::Notify>>,

    // Configurations
    pub max_segment_size: u64,
    pub broker_id: u32,
    // Root of broker storage directory; cold-storage segments are nested under this root.
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
        let pool = FdPool::global().clone();
        let active_log = PooledFile::new(pool.clone(), active_seg.log_path.clone());
        let active_idx = PooledFile::new(pool, active_seg.idx_path.clone());

        // Determine next offset from the last index entry of active segment. The handles are opened through the
        // pool, so recovering thousands of partitions at startup never holds more than the pool cap.
        let idx_file = active_idx.get()?;
        let index_len = idx_file.metadata()?.len();
        let next_offset = if index_len >= 16 {
            use std::os::unix::fs::FileExt;
            let mut buf = [0u8; 16];
            idx_file.read_exact_at(&mut buf, index_len - 16)?;
            u64::from_be_bytes(buf[0..8].try_into().unwrap()) + 1
        } else {
            active_seg.base_offset
        };
        drop(idx_file);

        // Appends use positioned writes at the tracked lengths
        let active_len = active_log.get()?.metadata()?.len();
        let active_idx_len = index_len;

        let partition_dir_for_txn = partition_dir.clone();
        let mut log = Self {
            topic: topic.to_string(),
            partition,
            partition_dir,
            segments,
            active_log,
            active_idx,
            l1: HashMap::new(),
            last_access: std::time::Instant::now(),
            next_offset,
            active_len,
            active_idx_len,
            last_retention_check: std::time::Instant::now(),
            writeback_start: active_len,
            writeback_prev_start: active_len,
            writeback_bytes: 0,
            drop_cache_after_writeback: false,
            append_notify: None,
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

    /// Validates idempotent producer sequence: Ok(None) to append, Ok(Some(last_offset))
    /// for duplicates, or Err(45) for OutOfOrderSequenceNumber gaps.
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
        self.last_access = std::time::Instant::now();
        self.active_log.get()?.write_all_at(data, pos)?;
        self.active_len += data.len() as u64;
        self.write_back_progress();

        let offset = self.next_offset;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..].copy_from_slice(&pos.to_be_bytes());
        self.active_idx.get()?.write_all_at(&entry, self.active_idx_len)?;
        self.active_idx_len += 16;

        self.next_offset += 1;

        // Retention is enforced on a timer (like Kafka's log cleaner) and after a roll, not after every record:
        // it stats every segment file.
        if rolled || self.last_retention_check.elapsed() >= std::time::Duration::from_secs(1) {
            self.clean_retention()?;
            self.last_retention_check = std::time::Instant::now();
        }

        self.recompute_high_watermark();
        if let Some(n) = &self.append_notify {
            n.notify_waiters();
        }
        Ok(offset)
    }

    /// Appends a raw batch slice with in-place base-offset patching, advancing
    /// `next_offset` by `record_count` to maintain accurate offset boundaries.
    pub fn append_batch_slice(&mut self, base_offset: i64, batch: &[u8], record_count: u64) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;

        let mut rolled = false;
        if self.active_len + batch.len() as u64 > self.max_segment_size && self.active_len > 0 {
            self.roll_over()?;
            rolled = true;
        }

        let pos = self.active_len;
        // In-place base offset patching directly at disk position without cloning the batch:
        let off_bytes = base_offset.to_be_bytes();
        self.last_access = std::time::Instant::now();
        let log_file = self.active_log.get()?;
        log_file.write_all_at(&off_bytes, pos)?;
        if batch.len() > 8 {
            log_file.write_all_at(&batch[8..], pos + 8)?;
        }
        self.active_len += batch.len() as u64;
        self.write_back_progress();

        let offset = self.next_offset;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..].copy_from_slice(&pos.to_be_bytes());
        self.active_idx.get()?.write_all_at(&entry, self.active_idx_len)?;
        self.active_idx_len += 16;

        self.next_offset += record_count.max(1);

        if rolled || self.last_retention_check.elapsed() >= std::time::Duration::from_secs(1) {
            self.clean_retention()?;
            self.last_retention_check = std::time::Instant::now();
        }

        self.recompute_high_watermark();
        if let Some(n) = &self.append_notify {
            n.notify_waiters();
        }
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
        // Compaction rewrites closed idx files: cached L1 samples may no longer match.
        self.l1.retain(|p, _| Some(p) == self.segments.last().map(|s| &s.idx_path));
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

    /// Initiates paced writeback on the active segment using `sync_file_range`
    /// to prevent dirty page accumulation and writeback stalls.
    fn write_back_progress(&mut self) {
        if self.writeback_bytes == 0 || self.active_len - self.writeback_start < self.writeback_bytes {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let Ok(log_file) = self.active_log.get() else { return };
            let fd = log_file.as_raw_fd();
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

    /// Archives a sealed segment into `cold_dir` asynchronously.
    /// Attempts a zero-copy hard link first, falling back to background file copy.
    fn archive_sealed_segment(&self, seg: &LogSegment) -> io::Result<()> {
        let src_log = File::open(&seg.log_path)?;
        let src_idx = File::open(&seg.idx_path)?;

        let cold_dir = self.cold_dir();
        let offload_tx = self.offload_tx.clone();
        let topic = self.topic.clone();
        let partition = self.partition;
        let seg = seg.clone();

        std::thread::spawn(move || {
            if let Err(e) = fs::create_dir_all(&cold_dir) {
                warn!("[AeroMQ Broker] Failed to create cold storage dir {:?}: {}", cold_dir, e);
                return;
            }
            let cold_log_path = cold_dir.join(format!("{:020}.log", seg.base_offset));
            let cold_idx_path = cold_dir.join(format!("{:020}.idx", seg.base_offset));
            let _ = fs::remove_file(&cold_log_path);
            let _ = fs::remove_file(&cold_idx_path);

            let offload = offload_tx.map(|tx| {
                (tx, crate::storage::offloader::OffloadTask::new(
                    &topic, partition, seg.base_offset, &cold_log_path, &cold_idx_path,
                ))
            });

            if fs::hard_link(&seg.log_path, &cold_log_path).is_ok() && fs::hard_link(&seg.idx_path, &cold_idx_path).is_ok() {
                debug!("[AeroMQ Broker] Segment {} sealed; linked into cold storage {:?}", seg.base_offset, cold_log_path);
                if let Some((tx, task)) = offload {
                    let _ = tx.try_send(task);
                }
                return;
            }
            let _ = fs::remove_file(&cold_log_path);
            let copy = || -> io::Result<()> {
                io::copy(&mut &src_log, &mut File::create(&cold_log_path)?)?;
                io::copy(&mut &src_idx, &mut File::create(&cold_idx_path)?)?;
                Ok(())
            };
            match copy() {
                Ok(()) => {
                    info!("[AeroMQ Broker] Segment {} copied to cold storage {:?} (background)", seg.base_offset, cold_log_path);
                    if let Some((tx, task)) = offload {
                        let _ = tx.try_send(task);
                    }
                }
                Err(e) => warn!("[AeroMQ Broker] Background cold-storage copy of segment {} failed: {}", seg.base_offset, e),
            }
        });
        Ok(())
    }

    /// Time since the last append or fetch.
    pub fn idle_for(&self) -> std::time::Duration {
        self.last_access.elapsed()
    }

    /// Puts an idle partition into its dormant state: closes the active segment's fds and drops the derived L1
    /// indexes. Everything is reopened/rebuilt transparently by the next append or fetch, so only the small
    /// bookkeeping struct stays resident. Returns true if anything was released.
    pub fn make_dormant(&mut self) -> bool {
        let had_state = !self.l1.is_empty();
        self.active_log.close();
        self.active_idx.close();
        self.l1 = HashMap::new();
        self.txn_index.close_journal();
        had_state
    }

    /// Dormant-eviction hook: call periodically; releases resources of partitions idle past the configured timeout.
    pub fn evict_if_idle(&mut self) -> bool {
        let secs = IDLE_EVICT_SECS.load(std::sync::atomic::Ordering::Relaxed);
        secs > 0 && self.idle_for() >= std::time::Duration::from_secs(secs) && self.make_dormant()
    }

    fn roll_over(&mut self) -> io::Result<()> {
        let old_active_seg = self.segments.last().unwrap().clone();

        // Start writeback of the unflushed tail of the sealed segment, then archive it (link, no data copy).
        if self.writeback_bytes > 0 && self.active_len > self.writeback_start {
            #[cfg(target_os = "linux")]
            if let Ok(log_file) = self.active_log.get() {
                use std::os::unix::io::AsRawFd;
                // SAFETY: plain syscall on a valid, open file descriptor; failure is only advisory.
                unsafe { libc::sync_file_range(
                    log_file.as_raw_fd(),
                    self.writeback_start as libc::off64_t,
                    (self.active_len - self.writeback_start) as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                ); }
            }
        }
        self.archive_sealed_segment(&old_active_seg)?;

        // Create new active segment
        let new_base_offset = self.next_offset;
        let new_log_path = self.partition_dir.join(format!("{:020}.log", new_base_offset));
        let new_idx_path = self.partition_dir.join(format!("{:020}.idx", new_base_offset));

        // Retarget the pooled handles at the new segment (drops the sealed segment's fds) and create the files now
        // so a failure surfaces at roll time rather than on the next append.
        self.active_log.retarget(new_log_path.clone());
        self.active_idx.retarget(new_idx_path.clone());
        self.active_log.get()?;
        self.active_idx.get()?;
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
                self.l1.remove(&oldest.idx_path);
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
                self.l1.remove(&oldest.idx_path);
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

    /// Segment that holds `offset` (local first, then cold storage).
    fn segment_for_offset(&self, offset: u64) -> io::Result<Option<LogSegment>> {
        if !self.segments.is_empty() && offset >= self.segments[0].base_offset {
            let i = self.segments.partition_point(|s| s.base_offset <= offset);
            if i > 0 {
                return Ok(Some(self.segments[i - 1].clone()));
            }
        }
        self.find_cold_segment(offset)
    }

    /// Reads a contiguous byte range of log entries in `[start_offset, end_offset)`,
    /// bounded by `max_bytes` while guaranteeing at least the full first entry.
    pub fn read_range(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u32,
    ) -> io::Result<Option<(File, u64, u32, u32)>> {
        let r = match self.range_bounds(start_offset, end_offset, max_bytes)? {
            Some(r) => r,
            None => return Ok(None),
        };
        let first_len = r.end_pos_of(r.first + 1)? - r.pos;
        Ok(Some((r.log, r.pos, (r.end - r.pos) as u32, first_len as u32)))
    }

    /// Like `read_range`, but also returns `(offset, length)` of every entry in the region, read from the index
    /// with one positioned read. The native multi-entry Fetch sends this table ahead of the (sendfile) data.
    pub fn read_range_entries(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        max_bytes: u32,
    ) -> io::Result<Option<(File, u64, u32, Vec<(u64, u32)>)>> {
        use std::os::unix::fs::FileExt;
        let r = match self.range_bounds(start_offset, end_offset, max_bytes)? {
            Some(r) => r,
            None => return Ok(None),
        };
        let mut raw = vec![0u8; ((r.k - r.first) * 16) as usize];
        r.idx.read_exact_at(&mut raw, r.first * 16)?;
        let mut entries = Vec::with_capacity(raw.len() / 16);
        for (i, e) in raw.chunks_exact(16).enumerate() {
            let off = u64::from_be_bytes(e[0..8].try_into().unwrap());
            let pos = u64::from_be_bytes(e[8..16].try_into().unwrap());
            let next = if i + 1 < raw.len() / 16 {
                u64::from_be_bytes(raw[(i + 1) * 16 + 8..(i + 2) * 16].try_into().unwrap())
            } else {
                r.end
            };
            entries.push((off, (next - pos) as u32));
        }
        Ok(Some((r.log, r.pos, (r.end - r.pos) as u32, entries)))
    }

    /// Locates the contiguous index entries `[first, k)` of one segment for `read_range`: offsets in
    /// `[start_offset, end_offset)`, total size within `max_bytes` (at least one entry).
    fn range_bounds(&mut self, start_offset: u64, end_offset: u64, max_bytes: u32) -> io::Result<Option<RangeBounds>> {
        use std::os::unix::fs::FileExt;
        // If start_offset exceeds end_offset or next_offset, no data is available yet.
        if start_offset >= end_offset || start_offset >= self.next_offset {
            return Ok(None);
        }
        self.last_access = std::time::Instant::now();
        let seg = match self.segment_for_offset(start_offset)? {
            Some(s) => s,
            None => return Ok(None),
        };
        let idx = File::open(&seg.idx_path)?;
        let n = idx.metadata()?.len() / 16;
        if n == 0 {
            return Ok(None);
        }
        // L1 sparse index for this segment (built lazily, extended as the index grows). Bounded so a scan across
        // thousands of cold segments cannot accumulate unbounded metadata.
        if !self.l1.contains_key(&seg.idx_path) {
            if self.l1.len() >= 256 {
                self.l1.clear();
            }
            self.l1.insert(seg.idx_path.clone(), SparseIndex::new(seg.base_offset));
        }
        let l1 = self.l1.get_mut(&seg.idx_path).unwrap();
        let entry = |i: u64| -> io::Result<(u64, u64)> {
            let mut b = [0u8; 16];
            idx.read_exact_at(&mut b, i * 16)?;
            Ok((u64::from_be_bytes(b[0..8].try_into().unwrap()), u64::from_be_bytes(b[8..16].try_into().unwrap())))
        };
        // Locate index entry containing start_offset using L1 window and L2 search.
        let mut lo = l1.first_at_or_after(&idx, n, start_offset.saturating_add(1))?;
        if lo == 0 {
            // start_offset is before the earliest entry in this segment: nothing to clamp to, fall back to the
            // first entry (matches the previous behavior for an out-of-range-low start_offset).
            lo = 1;
        }
        let first = lo - 1;
        if first >= n {
            return Ok(None);
        }
        let (_, pos) = entry(first)?;
        let log = File::open(&seg.log_path)?;
        let log_len = log.metadata()?.len();
        let end_pos_of = |k: u64| -> io::Result<u64> { if k < n { Ok(entry(k)?.1) } else { Ok(log_len) } };

        // Upper bound index search: entries in [first, k_off) are strictly before end_offset.
        let k_off = l1.first_at_or_after(&idx, n, end_offset)?.max(first + 1);
        // largest k in (first, k_off] whose end position fits in max_bytes; at least first + 1
        let limit = pos + max_bytes as u64;
        let (mut lo, mut hi) = (first + 1, k_off);
        while lo < hi {
            let mid = hi - (hi - lo) / 2;
            if end_pos_of(mid)? <= limit { lo = mid; } else { hi = mid - 1; }
        }
        let k = lo;
        let end = end_pos_of(k)?;
        if end <= pos {
            return Ok(None);
        }
        Ok(Some(RangeBounds { idx, log, log_len, n, first, k, pos, end }))
    }
}

/// Result of `PartitionLog::range_bounds`: index entries `[first, k)` of one segment, data bytes `[pos, end)`.
struct RangeBounds {
    idx: File,
    log: File,
    log_len: u64,
    n: u64,
    first: u64,
    k: u64,
    pos: u64,
    end: u64,
}

impl RangeBounds {
    /// Start position of index entry `i`, or the log length past the last entry.
    fn end_pos_of(&self, i: u64) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;
        if i >= self.n {
            return Ok(self.log_len);
        }
        let mut b = [0u8; 8];
        self.idx.read_exact_at(&mut b, i * 16 + 8)?;
        Ok(u64::from_be_bytes(b))
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
    /// Shared notifier for appends on any partition (see PartitionLog::append_notify).
    pub append_notify: Arc<tokio::sync::Notify>,

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
            append_notify: Arc::new(tokio::sync::Notify::new()),
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
            part.evict_if_idle();
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
        // Each partition gets its own Notify. A single shared Notify would mean every append to any
        // partition on the broker wakes every long-polling Fetch on every other partition too (an
        // O(total waiters) wakeup storm per append).
        log.append_notify = Some(Arc::new(tokio::sync::Notify::new()));

        let shared = Arc::new(Mutex::new(log));
        parts.insert(PartitionKey::new(topic, partition), shared.clone());
        Ok(shared)
    }
}

#[async_trait::async_trait]
pub trait PartitionStore: Send + Sync {
    async fn get_all_offsets(&self) -> Vec<(String, u32, i64)>;
    async fn partitions_for_topic(&self, topic: &str) -> Vec<u32>;
    async fn delete_topic(&self, topic: &str) -> std::io::Result<usize>;
    fn base_dir(&self) -> &std::path::Path;
    fn broker_id(&self) -> u32;
}

#[async_trait::async_trait]
impl PartitionStore for LogManager {
    async fn get_all_offsets(&self) -> Vec<(String, u32, i64)> {
        self.get_all_offsets().await
    }
    async fn partitions_for_topic(&self, topic: &str) -> Vec<u32> {
        self.partitions_for_topic(topic).await
    }
    async fn delete_topic(&self, topic: &str) -> std::io::Result<usize> {
        self.delete_topic(topic).await
    }
    fn base_dir(&self) -> &std::path::Path {
        self.base_dir()
    }
    fn broker_id(&self) -> u32 {
        self.broker_id
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Manual density probe: `cargo test --release density_probe -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn density_probe() {
        let rss_kb = || -> u64 {
            std::fs::read_to_string("/proc/self/status").unwrap().lines()
                .find(|l| l.starts_with("VmRSS")).unwrap()
                .split_whitespace().nth(1).unwrap().parse().unwrap()
        };
        let fds = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let dir = tempfile::tempdir().unwrap();
        let (n, r0) = (10_000u32, rss_kb());
        let t = std::time::Instant::now();
        let mut logs: Vec<PartitionLog> = (0..n)
            .map(|p| PartitionLog::new(dir.path(), "probe", p, 1, 1 << 20, None, None).unwrap())
            .collect();
        println!("create {n} partitions: {:?}, fds={}, rssΔ={} MB", t.elapsed(), fds(), (rss_kb() - r0) / 1024);
        let t = std::time::Instant::now();
        for round in 0..5 {
            for l in logs.iter_mut() { l.append(&[round; 100]).unwrap(); }
        }
        println!("{} appends round-robin: {:?} ({:.0}/s), fds={}, pool={:?}", n * 5, t.elapsed(),
            (n * 5) as f64 / t.elapsed().as_secs_f64(), fds(), FdPool::global().stats());
        let t = std::time::Instant::now();
        for l in logs.iter_mut() { l.read_range(2, 5, 1 << 20).unwrap().unwrap(); }
        println!("{n} cold fetches: {:?}", t.elapsed());
        for l in logs.iter_mut() { l.make_dormant(); }
        println!("all dormant: fds={}, rssΔ={} MB", fds(), (rss_kb() - r0) / 1024);
    }

    /// Phase 10: many partitions with a tiny fd pool still append and fetch correctly, dormant eviction is
    /// transparent, and fetches through the L1 sparse index return the right ranges across several windows.
    #[test]
    fn dense_partitions_share_bounded_fd_pool() {
        let dir = tempfile::tempdir().unwrap();
        FdPool::global().set_capacity(32);
        let mut logs: Vec<PartitionLog> = (0..200)
            .map(|p| PartitionLog::new(dir.path(), "dense", p, 1, 1 << 20, None, None).unwrap())
            .collect();
        for round in 0..3u8 {
            for l in logs.iter_mut() {
                l.append(&[round; 10]).unwrap();
            }
        }
        assert!(FdPool::global().stats().open <= 32 + 16, "pool exceeded cap");
        // Dormant everything, then keep using it.
        for l in logs.iter_mut() {
            l.make_dormant();
        }
        for l in logs.iter_mut() {
            l.append(&[9; 10]).unwrap();
        }
        for l in logs.iter_mut() {
            assert_eq!(l.next_offset, 4);
            let (f, pos, len, first) = l.read_range(2, 4, 1 << 20).unwrap().unwrap();
            assert_eq!((pos, len, first), (20, 20, 10));
            let mut b = [0u8; 20];
            std::os::unix::fs::FileExt::read_exact_at(&f, &mut b, pos).unwrap();
            assert_eq!(&b[..10], &[2u8; 10]);
            assert_eq!(&b[10..], &[9u8; 10]);
        }
        // > SAMPLE entries so lookups cross L1 windows and the index grows between fetches.
        let l = &mut logs[0];
        for i in 0..1000u32 {
            l.append(&i.to_be_bytes()).unwrap();
            if i % 97 == 0 {
                let want = l.next_offset - 1;
                let (f, pos, len, _) = l.read_range(want, want + 1, 1 << 20).unwrap().unwrap();
                assert_eq!(len, 4);
                let mut b = [0u8; 4];
                std::os::unix::fs::FileExt::read_exact_at(&f, &mut b, pos).unwrap();
                assert_eq!(u32::from_be_bytes(b), i);
            }
        }
        FdPool::global().set_capacity(crate::log::fd_pool::DEFAULT_CAPACITY);
    }

    /// Archiving into cold storage now happens on a spawned background thread (see `archive_sealed_segment`), not
    /// inline with `roll_over`, so tests that check for it must poll instead of asserting immediately.
    async fn wait_until_exists(path: &Path) {
        for _ in 0..500 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("{:?} did not appear within 5s of being archived", path);
    }

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
        wait_until_exists(&cold_log_path).await;

        // Write more to trigger retention deletion of segment 0 locally
        // Total retention limit is 250 bytes.
        let _off3 = log.append(&[4; 40]).unwrap(); // segment 1 size = 80b
        let _off4 = log.append(&[5; 40]).unwrap(); // rollover! segment 2 starts at offset 4
        let _off5 = log.append(&[6; 40]).unwrap(); // segment 2 size = 80b

        // Total segment size (336B) exceeds 250B limit; segment 0 is pruned locally.
        
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
        wait_until_exists(&cold).await;
        assert_eq!(fs::metadata(&local).unwrap().ino(), fs::metadata(&cold).unwrap().ino(), "archived by hard link, not copy");
        fs::remove_file(&local).unwrap(); // as retention would
        assert_eq!(fs::read(&cold).unwrap(), vec![1u8; 60]);
    }

    #[tokio::test]
    async fn read_range_spans_entries_within_bounds() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let manager = LogManager::new(dir.path(), 9).with_limits(1 << 20, None, None);
        let part = manager.get_partition("rr", 0).await.unwrap();
        let mut log = part.lock().await;
        for i in 0..20u8 {
            log.append(&[i; 100]).unwrap();
        }
        // offsets 5..12 (end bound), plenty of bytes
        let (f, pos, len, first) = log.read_range(5, 12, 1 << 20).unwrap().unwrap();
        assert_eq!((pos, len, first), (500, 700, 100));
        let mut b = vec![0u8; len as usize];
        f.read_exact_at(&mut b, pos).unwrap();
        assert_eq!(b[0], 5);
        assert_eq!(b[699], 11);
        // byte limit 350 -> 3 whole entries
        let (_, _, len, _) = log.read_range(5, 20, 350).unwrap().unwrap();
        assert_eq!(len, 300);
        // limit below one entry still returns the whole first entry
        let (_, _, len, _) = log.read_range(5, 20, 10).unwrap().unwrap();
        assert_eq!(len, 100);
        // at / past the end bound: nothing
        assert!(log.read_range(12, 12, 1 << 20).unwrap().is_none());
        assert!(log.read_range(20, 25, 1 << 20).unwrap().is_none());
    }

    #[tokio::test]
    async fn read_range_entries_reports_each_entry() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let manager = LogManager::new(dir.path(), 9).with_limits(1 << 20, None, None);
        let part = manager.get_partition("rre", 0).await.unwrap();
        let mut log = part.lock().await;
        for i in 0..10u8 {
            log.append(&vec![i; 10 + i as usize]).unwrap(); // entry i is 10 + i bytes
        }
        let (f, pos, len, entries) = log.read_range_entries(3, 7, 1 << 20).unwrap().unwrap();
        assert_eq!(entries, vec![(3, 13), (4, 14), (5, 15), (6, 16)]);
        assert_eq!(len, 13 + 14 + 15 + 16);
        let mut b = vec![0u8; len as usize];
        f.read_exact_at(&mut b, pos).unwrap();
        assert_eq!((b[0], b[12], b[13], b[len as usize - 1]), (3, 3, 4, 6));
        // up to the end of the log, byte limit cuts at whole entries
        let (_, _, len, entries) = log.read_range_entries(8, 10, 1 << 20).unwrap().unwrap();
        assert_eq!((len, entries), (18 + 19, vec![(8, 18), (9, 19)]));
        let (_, _, _, entries) = log.read_range_entries(3, 10, 30).unwrap().unwrap();
        assert_eq!(entries, vec![(3, 13), (4, 14)]);
        assert!(log.read_range_entries(10, 10, 1 << 20).unwrap().is_none());
    }
}
