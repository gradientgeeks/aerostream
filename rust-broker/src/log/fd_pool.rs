//! Striped LRU pool for active-segment file descriptors.
//! Partitions acquire cached file handles on demand with O(log n) eviction.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const STRIPES: usize = 16;
/// Default pool capacity: comfortably below typical `ulimit -n` (1 Ki .. 1 Mi) while leaving room for sockets.
pub const DEFAULT_CAPACITY: usize = 2048;

/// Identifies one file (log or idx of one partition's active segment) in the pool.
pub type SlotId = u64;

#[derive(Default)]
struct Stripe {
    /// slot -> (handle, last-use tick)
    map: HashMap<SlotId, (Arc<File>, u64)>,
    /// last-use tick -> slot, oldest first
    lru: BTreeMap<u64, SlotId>,
    tick: u64,
}

pub struct FdPool {
    stripes: Vec<Mutex<Stripe>>,
    per_stripe_cap: AtomicUsize,
    next_slot: AtomicU64,
    opens: AtomicU64,
    evictions: AtomicU64,
}

/// Pool statistics (for metrics/tests).
#[derive(Debug, Clone, Copy, Default)]
pub struct FdPoolStats {
    pub open: usize,
    pub opens: u64,
    pub evictions: u64,
}

impl FdPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Mutex::new(Stripe::default())).collect(),
            per_stripe_cap: AtomicUsize::new(Self::stripe_cap(capacity)),
            next_slot: AtomicU64::new(1),
            opens: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn stripe_cap(capacity: usize) -> usize {
        (capacity / STRIPES).max(1)
    }

    /// Process-wide pool used by every `PartitionLog`.
    pub fn global() -> &'static Arc<FdPool> {
        static POOL: OnceLock<Arc<FdPool>> = OnceLock::new();
        POOL.get_or_init(|| Arc::new(FdPool::new(DEFAULT_CAPACITY)))
    }

    /// Resizes the pool (takes effect on the next insert; shrinking evicts lazily).
    pub fn set_capacity(&self, capacity: usize) {
        self.per_stripe_cap.store(Self::stripe_cap(capacity.max(1)), Ordering::Relaxed);
    }

    pub fn alloc_slot(&self) -> SlotId {
        self.next_slot.fetch_add(1, Ordering::Relaxed)
    }

    fn stripe(&self, slot: SlotId) -> &Mutex<Stripe> {
        &self.stripes[(slot as usize) % STRIPES]
    }

    /// Returns the open handle for `slot`, opening `path` (create + read/write) if absent or evicted.
    /// The file open itself happens outside the stripe lock.
    pub fn get(&self, slot: SlotId, path: &Path) -> io::Result<Arc<File>> {
        {
            let mut s = self.stripe(slot).lock().unwrap();
            if let Some(f) = Self::touch(&mut s, slot) {
                return Ok(f);
            }
        }
        let file = Arc::new(OpenOptions::new().create(true).read(true).write(true).open(path)?);
        self.opens.fetch_add(1, Ordering::Relaxed);

        let mut evicted: Vec<Arc<File>> = Vec::new();
        let result = {
            let mut s = self.stripe(slot).lock().unwrap();
            // Lost a race with another opener of the same slot: keep theirs.
            if let Some(f) = Self::touch(&mut s, slot) {
                f
            } else {
                s.tick += 1;
                let t = s.tick;
                s.map.insert(slot, (file.clone(), t));
                s.lru.insert(t, slot);
                let cap = self.per_stripe_cap.load(Ordering::Relaxed);
                while s.map.len() > cap {
                    let Some((&old_t, &old_slot)) = s.lru.iter().next() else { break };
                    s.lru.remove(&old_t);
                    if let Some((f, _)) = s.map.remove(&old_slot) {
                        evicted.push(f);
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                file
            }
        };
        drop(evicted); // close(2) outside the lock
        Ok(result)
    }

    fn touch(s: &mut Stripe, slot: SlotId) -> Option<Arc<File>> {
        let old_t = s.map.get(&slot)?.1;
        s.tick += 1;
        let t = s.tick;
        s.lru.remove(&old_t);
        s.lru.insert(t, slot);
        let entry = s.map.get_mut(&slot)?;
        entry.1 = t;
        Some(entry.0.clone())
    }

    /// Closes the handle for `slot` (segment rolled, partition dormant or deleted).
    pub fn release(&self, slot: SlotId) {
        let removed = {
            let mut s = self.stripe(slot).lock().unwrap();
            s.map.remove(&slot).map(|(f, t)| {
                s.lru.remove(&t);
                f
            })
        };
        drop(removed);
    }

    pub fn stats(&self) -> FdPoolStats {
        let open = self.stripes.iter().map(|s| s.lock().unwrap().map.len()).sum();
        FdPoolStats {
            open,
            opens: self.opens.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

/// One pooled file owned by a partition: path + slot. Cheap (no fd) while evicted.
pub struct PooledFile {
    pool: Arc<FdPool>,
    slot: SlotId,
    path: std::path::PathBuf,
}

impl PooledFile {
    pub fn new(pool: Arc<FdPool>, path: std::path::PathBuf) -> Self {
        let slot = pool.alloc_slot();
        Self { pool, slot, path }
    }

    pub fn get(&self) -> io::Result<Arc<File>> {
        self.pool.get(self.slot, &self.path)
    }

    /// Points at a different file (segment roll) and drops the old handle.
    pub fn retarget(&mut self, path: std::path::PathBuf) {
        self.pool.release(self.slot);
        self.path = path;
    }

    /// Closes the fd now; it reopens on next `get`.
    pub fn close(&self) {
        self.pool.release(self.slot);
    }
}

impl Drop for PooledFile {
    fn drop(&mut self) {
        self.pool.release(self.slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;

    #[test]
    fn evicts_beyond_capacity_and_reopens_transparently() {
        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(FdPool::new(STRIPES * 2));
        let files: Vec<PooledFile> = (0..500)
            .map(|i| PooledFile::new(pool.clone(), dir.path().join(format!("{i}.log"))))
            .collect();
        for (i, f) in files.iter().enumerate() {
            f.get().unwrap().write_all_at(&[i as u8; 4], 0).unwrap();
        }
        let st = pool.stats();
        assert!(st.open <= STRIPES * 2, "open fds {} exceed cap", st.open);
        assert!(st.evictions >= 500 - (STRIPES * 2) as u64);
        // Evicted files still hold their data and reopen on demand.
        for (i, f) in files.iter().enumerate() {
            let mut b = [0u8; 4];
            f.get().unwrap().read_exact_at(&mut b, 0).unwrap();
            assert_eq!(b, [i as u8; 4]);
        }
    }

    #[test]
    fn drop_releases_slot() {
        let dir = tempfile::tempdir().unwrap();
        let pool = Arc::new(FdPool::new(64));
        {
            let f = PooledFile::new(pool.clone(), dir.path().join("a.log"));
            f.get().unwrap();
            assert_eq!(pool.stats().open, 1);
        }
        assert_eq!(pool.stats().open, 0);
    }
}
