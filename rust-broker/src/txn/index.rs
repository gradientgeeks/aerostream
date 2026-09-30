//! Per-partition transaction index tracking ongoing transactions for LSO and aborted
//! transactions for Fetch. Persisted as an append-only journal (`txn.index`).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OngoingTxn {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbortedTxn {
    pub producer_id: i64,
    pub first_offset: u64,
    /// Offset of the abort control marker.
    pub last_offset: u64,
}

#[derive(Debug, Default)]
pub struct PartitionTxnIndex {
    ongoing: BTreeMap<i64, OngoingTxn>,
    aborted: Vec<AbortedTxn>,
    journal_path: Option<PathBuf>,
    journal: Option<File>,
}

impl PartitionTxnIndex {
    /// In-memory only index (tests).
    pub fn in_memory() -> Self {
        Self::default()
    }

    /// Opens (and replays) the journal in `partition_dir`.
    pub fn open(partition_dir: &Path) -> Self {
        let path = partition_dir.join("txn.index");
        let mut idx = Self::default();
        if let Ok(f) = File::open(&path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                let p: Vec<&str> = line.split(' ').collect();
                match p.as_slice() {
                    ["B", pid, epoch, first] => {
                        if let (Ok(pid), Ok(epoch), Ok(first)) = (pid.parse(), epoch.parse(), first.parse()) {
                            idx.ongoing.insert(
                                pid,
                                OngoingTxn { producer_id: pid, producer_epoch: epoch, first_offset: first },
                            );
                        }
                    }
                    ["C", pid, _marker] => {
                        if let Ok(pid) = pid.parse::<i64>() {
                            idx.ongoing.remove(&pid);
                        }
                    }
                    ["A", pid, marker] => {
                        if let (Ok(pid), Ok(marker)) = (pid.parse::<i64>(), marker.parse::<u64>()) {
                            if let Some(o) = idx.ongoing.remove(&pid) {
                                idx.aborted.push(AbortedTxn {
                                    producer_id: pid,
                                    first_offset: o.first_offset,
                                    last_offset: marker,
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        // The journal fd is opened lazily on the first transactional write (most partitions never see one) and
        // released by `close_journal` when the partition goes dormant: no fd per partition (Phase 10).
        idx.journal_path = Some(path);
        idx
    }

    /// Releases the journal fd; it reopens on the next write.
    pub fn close_journal(&mut self) {
        self.journal = None;
    }

    fn log(&mut self, line: String) {
        if self.journal.is_none() {
            if let Some(path) = &self.journal_path {
                self.journal = OpenOptions::new().create(true).append(true).open(path).ok();
            }
        }
        if let Some(f) = self.journal.as_mut() {
            let _ = writeln!(f, "{}", line);
        }
    }

    pub fn is_ongoing(&self, pid: i64) -> bool {
        self.ongoing.contains_key(&pid)
    }

    pub fn ongoing(&self) -> impl Iterator<Item = &OngoingTxn> {
        self.ongoing.values()
    }

    /// Records a transactional data entry at `offset`; starts tracking the
    /// transaction if this is its first entry on the partition.
    pub fn note_data(&mut self, pid: i64, epoch: i16, offset: u64) {
        if let Some(o) = self.ongoing.get(&pid) {
            if o.producer_epoch == epoch {
                return;
            }
        }
        self.ongoing.insert(
            pid,
            OngoingTxn { producer_id: pid, producer_epoch: epoch, first_offset: offset },
        );
        self.log(format!("B {} {} {}", pid, epoch, offset));
    }

    /// Records the end marker for `pid` written at `marker_offset`.
    /// Returns true if a transaction was actually ongoing.
    pub fn complete(&mut self, pid: i64, marker_offset: u64, commit: bool) -> bool {
        match self.ongoing.remove(&pid) {
            Some(o) => {
                if commit {
                    self.log(format!("C {} {}", pid, marker_offset));
                } else {
                    self.log(format!("A {} {}", pid, marker_offset));
                    self.aborted.push(AbortedTxn {
                        producer_id: pid,
                        first_offset: o.first_offset,
                        last_offset: marker_offset,
                    });
                }
                true
            }
            None => false,
        }
    }

    /// Last Stable Offset: smallest first-offset of any ongoing transaction,
    /// else the high watermark.
    pub fn lso(&self, high_watermark: u64) -> u64 {
        self.ongoing
            .values()
            .map(|o| o.first_offset)
            .min()
            .map_or(high_watermark, |m| m.min(high_watermark))
    }

    /// Aborted transactions overlapping `[from, to]` (inclusive offsets).
    pub fn aborted_in_range(&self, from: u64, to: u64) -> Vec<AbortedTxn> {
        self.aborted
            .iter()
            .filter(|a| a.first_offset <= to && a.last_offset >= from)
            .cloned()
            .collect()
    }

    /// True if `offset` written by `pid` belongs to an aborted transaction.
    pub fn is_aborted(&self, pid: i64, offset: u64) -> bool {
        self.aborted
            .iter()
            .any(|a| a.producer_id == pid && a.first_offset <= offset && offset < a.last_offset)
    }

    /// Drops aborted-transaction entries fully below `log_start` (retention).
    pub fn prune(&mut self, log_start: u64) {
        self.aborted.retain(|a| a.last_offset >= log_start);
    }

    pub fn aborted_len(&self) -> usize {
        self.aborted.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lso_and_aborted() {
        let mut i = PartitionTxnIndex::in_memory();
        assert_eq!(i.lso(10), 10);
        i.note_data(5, 0, 3);
        i.note_data(5, 0, 4);
        i.note_data(6, 0, 6);
        assert_eq!(i.lso(10), 3);
        assert!(i.complete(5, 8, false));
        assert_eq!(i.lso(10), 6);
        assert_eq!(i.aborted_in_range(0, 10), vec![AbortedTxn { producer_id: 5, first_offset: 3, last_offset: 8 }]);
        assert!(i.is_aborted(5, 4));
        assert!(!i.is_aborted(5, 9));
        assert!(i.complete(6, 9, true));
        assert_eq!(i.lso(10), 10);
        assert!(!i.complete(6, 9, true));
        assert!(i.aborted_in_range(9, 20).is_empty());
    }

    #[test]
    fn journal_replay() {
        let dir = std::env::temp_dir().join(format!("aero_txnidx_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::remove_file(dir.join("txn.index"));
        {
            let mut i = PartitionTxnIndex::open(&dir);
            i.note_data(1, 0, 0);
            i.note_data(2, 0, 1);
            i.complete(1, 5, false);
        }
        let i = PartitionTxnIndex::open(&dir);
        assert!(i.is_ongoing(2));
        assert!(!i.is_ongoing(1));
        assert!(i.is_aborted(1, 0));
        assert_eq!(i.lso(9), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
