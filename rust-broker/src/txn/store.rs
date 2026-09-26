//! Durable transaction-coordinator state (the moral equivalent of Kafka's
//! `__transaction_state` topic): an append-only, last-writer-wins journal of
//! per-transactional-id metadata records plus the producer-id block high-water
//! mark. Compacted on load.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
}

impl TxnState {
    pub fn name(self) -> &'static str {
        match self {
            TxnState::Empty => "Empty",
            TxnState::Ongoing => "Ongoing",
            TxnState::PrepareCommit => "PrepareCommit",
            TxnState::PrepareAbort => "PrepareAbort",
            TxnState::CompleteCommit => "CompleteCommit",
            TxnState::CompleteAbort => "CompleteAbort",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "Empty" => TxnState::Empty,
            "Ongoing" => TxnState::Ongoing,
            "PrepareCommit" => TxnState::PrepareCommit,
            "PrepareAbort" => TxnState::PrepareAbort,
            "CompleteCommit" => TxnState::CompleteCommit,
            "CompleteAbort" => TxnState::CompleteAbort,
            _ => return None,
        })
    }
    pub fn is_prepare(self) -> bool {
        matches!(self, TxnState::PrepareCommit | TxnState::PrepareAbort)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOffset {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnMetadata {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// Previous epoch (for fencing diagnostics); -1 if none.
    pub last_producer_epoch: i16,
    pub timeout_ms: i32,
    pub state: TxnState,
    pub partitions: BTreeSet<(String, i32)>,
    pub groups: BTreeSet<String>,
    pub pending_offsets: Vec<PendingOffset>,
    pub start_ms: i64,
    pub update_ms: i64,
}

fn hex(s: &str) -> String {
    if s.is_empty() {
        return "~".to_string();
    }
    s.bytes().map(|b| format!("{:02x}", b)).collect()
}

fn unhex(s: &str) -> Option<String> {
    if s == "~" {
        return Some(String::new());
    }
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).ok()?);
    }
    String::from_utf8(out).ok()
}

fn join_or_dash(v: Vec<String>) -> String {
    if v.is_empty() {
        "-".into()
    } else {
        v.join(",")
    }
}

impl TxnMetadata {
    fn encode(&self) -> String {
        let parts = join_or_dash(self.partitions.iter().map(|(t, p)| format!("{}:{}", hex(t), p)).collect());
        let groups = join_or_dash(self.groups.iter().map(|g| hex(g)).collect());
        let offs = join_or_dash(
            self.pending_offsets
                .iter()
                .map(|o| {
                    format!(
                        "{}:{}:{}:{}:{}:{}",
                        hex(&o.group),
                        hex(&o.topic),
                        o.partition,
                        o.offset,
                        o.leader_epoch,
                        o.metadata.as_deref().map(hex).unwrap_or_else(|| "-".into())
                    )
                })
                .collect(),
        );
        format!(
            "T\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            hex(&self.transactional_id),
            self.producer_id,
            self.producer_epoch,
            self.last_producer_epoch,
            self.timeout_ms,
            self.state.name(),
            self.start_ms,
            self.update_ms,
            parts,
            groups,
            offs
        )
    }

    fn decode(f: &[&str]) -> Option<Self> {
        if f.len() != 12 || f[0] != "T" {
            return None;
        }
        let mut partitions = BTreeSet::new();
        if f[9] != "-" {
            for p in f[9].split(',') {
                let (t, n) = p.rsplit_once(':')?;
                partitions.insert((unhex(t)?, n.parse().ok()?));
            }
        }
        let mut groups = BTreeSet::new();
        if f[10] != "-" {
            for g in f[10].split(',') {
                groups.insert(unhex(g)?);
            }
        }
        let mut pending_offsets = Vec::new();
        if f[11] != "-" {
            for o in f[11].split(',') {
                let x: Vec<&str> = o.split(':').collect();
                if x.len() != 6 {
                    return None;
                }
                pending_offsets.push(PendingOffset {
                    group: unhex(x[0])?,
                    topic: unhex(x[1])?,
                    partition: x[2].parse().ok()?,
                    offset: x[3].parse().ok()?,
                    leader_epoch: x[4].parse().ok()?,
                    metadata: if x[5] == "-" { None } else { Some(unhex(x[5])?) },
                });
            }
        }
        Some(TxnMetadata {
            transactional_id: unhex(f[1])?,
            producer_id: f[2].parse().ok()?,
            producer_epoch: f[3].parse().ok()?,
            last_producer_epoch: f[4].parse().ok()?,
            timeout_ms: f[5].parse().ok()?,
            state: TxnState::parse(f[6])?,
            start_ms: f[7].parse().ok()?,
            update_ms: f[8].parse().ok()?,
            partitions,
            groups,
            pending_offsets,
        })
    }
}

pub struct TxnStore {
    path: Option<PathBuf>,
    file: Option<File>,
}

impl TxnStore {
    pub fn in_memory() -> Self {
        Self { path: None, file: None }
    }

    /// Opens the journal in `dir`, returning the store plus recovered state.
    pub fn open(dir: &Path) -> (Self, HashMap<String, TxnMetadata>, Option<i64>) {
        let _ = fs::create_dir_all(dir);
        let path = dir.join("txn.journal");
        let mut txns: HashMap<String, TxnMetadata> = HashMap::new();
        let mut block_end: Option<i64> = None;
        if let Ok(f) = File::open(&path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                let f: Vec<&str> = line.split('\t').collect();
                match f.first().copied() {
                    Some("T") => {
                        if let Some(m) = TxnMetadata::decode(&f) {
                            txns.insert(m.transactional_id.clone(), m);
                        }
                    }
                    Some("D") if f.len() == 2 => {
                        if let Some(t) = unhex(f[1]) {
                            txns.remove(&t);
                        }
                    }
                    Some("P") if f.len() == 2 => {
                        if let Ok(v) = f[1].parse::<i64>() {
                            block_end = Some(block_end.map_or(v, |b| b.max(v)));
                        }
                    }
                    _ => {}
                }
            }
        }
        // Compact: rewrite with only live records.
        let tmp = dir.join("txn.journal.tmp");
        if let Ok(mut t) = File::create(&tmp) {
            if let Some(b) = block_end {
                let _ = writeln!(t, "P\t{}", b);
            }
            for m in txns.values() {
                let _ = writeln!(t, "{}", m.encode());
            }
            let _ = t.sync_all();
            let _ = fs::rename(&tmp, &path);
        }
        let file = OpenOptions::new().create(true).append(true).open(&path).ok();
        (Self { path: Some(path), file }, txns, block_end)
    }

    fn write(&mut self, line: String) {
        if let Some(f) = self.file.as_mut() {
            if writeln!(f, "{}", line).and_then(|_| f.flush()).is_err() {
                tracing::error!("[AeroMQ Txn] failed writing txn journal {:?}", self.path);
            }
        }
    }

    pub fn put(&mut self, m: &TxnMetadata) {
        self.write(m.encode());
    }

    pub fn delete(&mut self, tid: &str) {
        self.write(format!("D\t{}", hex(tid)));
    }

    pub fn put_block_end(&mut self, end: i64) {
        self.write(format!("P\t{}", end));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_encode_decode() {
        let mut m = TxnMetadata {
            transactional_id: "tx\t1,:".into(),
            producer_id: 1_099_511_628_000,
            producer_epoch: 3,
            last_producer_epoch: 2,
            timeout_ms: 60000,
            state: TxnState::Ongoing,
            partitions: BTreeSet::new(),
            groups: BTreeSet::new(),
            pending_offsets: vec![],
            start_ms: 5,
            update_ms: 6,
        };
        m.partitions.insert(("topic:a".into(), 2));
        m.groups.insert("g".into());
        m.pending_offsets.push(PendingOffset {
            group: "g".into(),
            topic: "t".into(),
            partition: 1,
            offset: 42,
            leader_epoch: -1,
            metadata: Some("md".into()),
        });
        let line = m.encode();
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(TxnMetadata::decode(&f).unwrap(), m);
    }
}
