//! Two-level sparse offset index: L1 is an in-memory sample table (4 bytes per sample),
//! and L2 is the on-disk 16-byte entry index (.idx file).

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

/// One L1 sample per this many index entries.
pub const SAMPLE: u64 = 128;
const ENTRY: u64 = 16;

#[derive(Debug, Default)]
pub struct SparseIndex {
    base_offset: u64,
    /// `samples[i]` = offset of entry `i * SAMPLE`, relative to `base_offset`.
    samples: Vec<u32>,
    /// Number of L2 entries covered so far (samples exist for all multiples of SAMPLE below this).
    covered: u64,
    /// A relative offset overflowed u32: L1 is unusable for this segment, fall back to a full L2 search.
    disabled: bool,
}

fn read_entry_offset(idx: &File, i: u64) -> io::Result<u64> {
    let mut b = [0u8; 8];
    idx.read_exact_at(&mut b, i * ENTRY)?;
    Ok(u64::from_be_bytes(b))
}

impl SparseIndex {
    pub fn new(base_offset: u64) -> Self {
        Self { base_offset, ..Default::default() }
    }

    pub fn heap_bytes(&self) -> usize {
        self.samples.capacity() * 4
    }

    /// Brings L1 up to date with an index of `n` entries (extends, or rebuilds if the file shrank).
    fn sync(&mut self, idx: &File, n: u64) -> io::Result<()> {
        if n < self.covered {
            let base = self.base_offset;
            *self = Self::new(base);
        }
        if self.disabled {
            return Ok(());
        }
        let mut i = self.samples.len() as u64 * SAMPLE;
        while i < n {
            let off = read_entry_offset(idx, i)?;
            match off.checked_sub(self.base_offset).and_then(|r| u32::try_from(r).ok()) {
                Some(r) => self.samples.push(r),
                None => {
                    self.disabled = true;
                    self.samples = Vec::new();
                    break;
                }
            }
            i += SAMPLE;
        }
        self.covered = n;
        Ok(())
    }

    /// Index of the first entry whose offset is `>= target` (`n` if none), for an index file of `n` entries.
    pub fn first_at_or_after(&mut self, idx: &File, n: u64, target: u64) -> io::Result<u64> {
        if n == 0 {
            return Ok(0);
        }
        self.sync(idx, n)?;
        // Window of entries [lo, hi) that must contain the answer's predecessor boundary.
        let (lo, hi) = if self.disabled {
            (0, n)
        } else {
            // First sample whose offset >= target; a target below the segment base matches sample 0.
            let s = match target.checked_sub(self.base_offset) {
                None => 0,
                Some(rel) if rel > u32::MAX as u64 => self.samples.len(),
                Some(rel) => self.samples.partition_point(|&r| (r as u64) < rel),
            } as u64;
            if s == 0 {
                return Ok(0);
            }
            let lo = (s - 1) * SAMPLE + 1;
            let hi = if s < self.samples.len() as u64 { s * SAMPLE + 1 } else { n };
            (lo, hi.min(n))
        };
        if lo >= hi {
            return Ok(hi);
        }
        // One positioned read of the window, then an in-memory binary search.
        let mut buf = vec![0u8; ((hi - lo) * ENTRY) as usize];
        idx.read_exact_at(&mut buf, lo * ENTRY)?;
        let pp = partition_point_entries(&buf, target);
        Ok(lo + pp as u64)
    }
}

/// Number of leading entries in `buf` (16-byte records) whose offset is `< target`.
fn partition_point_entries(buf: &[u8], target: u64) -> usize {
    let (mut lo, mut hi) = (0usize, buf.len() / ENTRY as usize);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = u64::from_be_bytes(buf[mid * 16..mid * 16 + 8].try_into().unwrap());
        if off < target { lo = mid + 1 } else { hi = mid }
    }
    lo
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_idx(offsets: &[u64]) -> (tempfile::NamedTempFile, File) {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        for (i, o) in offsets.iter().enumerate() {
            tmp.write_all(&o.to_be_bytes()).unwrap();
            tmp.write_all(&(i as u64 * 100).to_be_bytes()).unwrap();
        }
        tmp.flush().unwrap();
        let f = tmp.reopen().unwrap();
        (tmp, f)
    }

    fn reference(offsets: &[u64], t: u64) -> u64 {
        offsets.partition_point(|&o| o < t) as u64
    }

    #[test]
    fn matches_full_binary_search() {
        // Gappy offsets (multi-record batches) across several sample windows.
        let base = 1_000u64;
        let offsets: Vec<u64> = (0..1000u64).map(|i| base + i * 3).collect();
        let (_t, f) = make_idx(&offsets);
        let mut si = SparseIndex::new(base);
        let n = offsets.len() as u64;
        for t in (base - 5)..(base + 3100) {
            assert_eq!(si.first_at_or_after(&f, n, t).unwrap(), reference(&offsets, t), "target {t}");
        }
        assert_eq!(si.samples.len() as u64, n.div_ceil(SAMPLE));
    }

    #[test]
    fn extends_as_index_grows_and_handles_tiny() {
        let offsets: Vec<u64> = (0..300u64).collect();
        let (_t, f) = make_idx(&offsets);
        let mut si = SparseIndex::new(0);
        for n in [0u64, 1, 5, 127, 128, 129, 256, 300] {
            for t in [0u64, 1, 4, 127, 128, 200, 299, 300, 1000] {
                let want = offsets[..n as usize].partition_point(|&o| o < t) as u64;
                assert_eq!(si.first_at_or_after(&f, n, t).unwrap(), want, "n {n} t {t}");
            }
        }
    }

    #[test]
    fn overflowing_relative_offset_falls_back() {
        let base = 0u64;
        let offsets = vec![0, 1, 2, u32::MAX as u64 + 10];
        let mut padded: Vec<u64> = (0..SAMPLE).collect();
        padded.push(u32::MAX as u64 + 10); // sample #1 overflows
        padded.extend(offsets.iter().skip(3).map(|o| o + 1));
        let (_t, f) = make_idx(&padded);
        let mut si = SparseIndex::new(base);
        let n = padded.len() as u64;
        assert_eq!(si.first_at_or_after(&f, n, 50).unwrap(), reference(&padded, 50));
        assert_eq!(si.first_at_or_after(&f, n, u32::MAX as u64 + 5).unwrap(), reference(&padded, u32::MAX as u64 + 5));
        assert!(si.disabled);
    }
}
