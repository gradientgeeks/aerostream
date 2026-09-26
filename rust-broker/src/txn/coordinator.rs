//! Transaction coordinator (KIP-98 / KIP-890 TV1 semantics).
//!
//! Lives in the broker: it owns the partition logs, so it can append commit /
//! abort control markers directly. State is journaled to
//! `<storage_dir>/__txn_state/txn.journal` and recovered on start; transactions
//! caught in `Prepare*` at crash time are re-driven by `sweep`.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use tracing::{error, info, warn};

use super::batch::encode_control_batch;
use super::err;
use super::sink::{InMemorySink, OffsetSink};
use super::store::{PendingOffset, TxnMetadata, TxnState, TxnStore};
use super::TxnConfig;
use crate::log::LogManager;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

const PID_BLOCK: i64 = 1000;

struct Inner {
    txns: HashMap<String, TxnMetadata>,
    store: TxnStore,
    next_pid: i64,
    pid_block_end: i64,
}

pub struct TxnCoordinator {
    inner: Mutex<Inner>,
    log: Weak<LogManager>,
    sink: Arc<dyn OffsetSink>,
    pub cfg: TxnConfig,
    coordinator_epoch: i32,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct InitResult {
    pub error: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl TxnCoordinator {
    /// Opens a coordinator persisting into `dir` (`None` = memory only).
    pub fn open(
        dir: Option<&Path>,
        broker_id: u32,
        log: Weak<LogManager>,
        sink: Arc<dyn OffsetSink>,
        cfg: TxnConfig,
    ) -> Self {
        let base = ((broker_id as i64) << 40) + PID_BLOCK;
        let (store, txns, block_end) = match dir {
            Some(d) => TxnStore::open(d),
            None => (TxnStore::in_memory(), HashMap::new(), None),
        };
        let start = block_end.unwrap_or(base).max(base);
        Self {
            inner: Mutex::new(Inner { txns, store, next_pid: start, pid_block_end: start }),
            log,
            sink,
            cfg,
            coordinator_epoch: 0,
        }
    }

    pub fn in_memory(log: Weak<LogManager>) -> Self {
        Self::open(None, 1, log, Arc::new(InMemorySink::default()), TxnConfig::default())
    }

    fn alloc_pid(inner: &mut Inner) -> i64 {
        if inner.next_pid >= inner.pid_block_end {
            inner.pid_block_end = inner.next_pid + PID_BLOCK;
            let end = inner.pid_block_end;
            inner.store.put_block_end(end);
        }
        let p = inner.next_pid;
        inner.next_pid += 1;
        p
    }

    /// Allocates a cluster-unique (broker-prefixed), restart-safe producer id.
    pub fn allocate_producer_id(&self) -> i64 {
        Self::alloc_pid(&mut self.inner.lock().unwrap())
    }

    pub fn get(&self, tid: &str) -> Option<TxnMetadata> {
        self.inner.lock().unwrap().txns.get(tid).cloned()
    }

    #[cfg(test)]
    pub fn force_state(&self, tid: &str, state: TxnState) {
        let mut g = self.inner.lock().unwrap();
        if let Some(m) = g.txns.get_mut(tid) {
            m.state = state;
        }
        Self::persist(&mut g, tid);
    }

    pub fn list(&self) -> Vec<TxnMetadata> {
        let mut v: Vec<_> = self.inner.lock().unwrap().txns.values().cloned().collect();
        v.sort_by(|a, b| a.transactional_id.cmp(&b.transactional_id));
        v
    }

    fn persist(inner: &mut Inner, tid: &str) {
        if let Some(m) = inner.txns.get(tid).cloned() {
            inner.store.put(&m);
        }
    }

    // ------------------------------------------------------------------
    // InitProducerId (22)
    // ------------------------------------------------------------------
    pub async fn init_producer_id(
        &self,
        tid: Option<&str>,
        timeout_ms: i32,
        req_pid: i64,
        req_epoch: i16,
    ) -> InitResult {
        let fail = |e| InitResult { error: e, producer_id: -1, producer_epoch: -1 };
        let tid = match tid {
            None => {
                // Idempotent (non-transactional) producer.
                return InitResult { error: 0, producer_id: self.allocate_producer_id(), producer_epoch: 0 };
            }
            Some("") => return fail(err::INVALID_REQUEST),
            Some(t) => t,
        };
        if timeout_ms <= 0 || timeout_ms > self.cfg.max_timeout_ms {
            return fail(err::INVALID_TRANSACTION_TIMEOUT);
        }

        // Phase 1: validate, and possibly move an ongoing txn to PrepareAbort.
        let abort_parts = {
            let mut g = self.inner.lock().unwrap();
            let now = now_ms();
            if !g.txns.contains_key(tid) {
                let pid = Self::alloc_pid(&mut g);
                let m = TxnMetadata {
                    transactional_id: tid.to_string(),
                    producer_id: pid,
                    producer_epoch: 0,
                    last_producer_epoch: -1,
                    timeout_ms,
                    state: TxnState::Empty,
                    partitions: BTreeSet::new(),
                    groups: BTreeSet::new(),
                    pending_offsets: vec![],
                    start_ms: -1,
                    update_ms: now,
                };
                g.txns.insert(tid.to_string(), m);
                Self::persist(&mut g, tid);
                return InitResult { error: 0, producer_id: pid, producer_epoch: 0 };
            }
            let m = g.txns.get_mut(tid).unwrap();
            if m.state.is_prepare() {
                return fail(err::CONCURRENT_TRANSACTIONS);
            }
            if req_pid != -1 && (req_pid != m.producer_id || req_epoch != m.producer_epoch) {
                return fail(err::PRODUCER_FENCED);
            }
            m.timeout_ms = timeout_ms;
            if m.state == TxnState::Ongoing {
                m.state = TxnState::PrepareAbort;
                m.update_ms = now;
                let parts = m.partitions.iter().cloned().collect::<Vec<_>>();
                Self::persist(&mut g, tid);
                Some(parts)
            } else {
                None
            }
        };

        if let Some(parts) = abort_parts {
            // Fence the previous incarnation: abort its open transaction first.
            let (pid, epoch) = {
                let g = self.inner.lock().unwrap();
                let m = &g.txns[tid];
                (m.producer_id, m.producer_epoch)
            };
            if let Err(e) = self.write_markers(pid, epoch, &parts, false).await {
                warn!("[AeroMQ Txn] abort markers failed during InitProducerId for {}: {}", tid, e);
                return fail(err::COORDINATOR_NOT_AVAILABLE);
            }
        }

        // Phase 2: bump epoch.
        let mut g = self.inner.lock().unwrap();
        let now = now_ms();
        let (mut pid, epoch) = {
            let m = &g.txns[tid];
            (m.producer_id, m.producer_epoch)
        };
        let (new_pid, new_epoch) = if epoch >= i16::MAX - 2 {
            pid = Self::alloc_pid(&mut g);
            (pid, 0)
        } else {
            (pid, epoch + 1)
        };
        let m = g.txns.get_mut(tid).unwrap();
        m.last_producer_epoch = if new_pid == m.producer_id { m.producer_epoch } else { -1 };
        m.producer_id = new_pid;
        m.producer_epoch = new_epoch;
        m.state = TxnState::Empty;
        m.partitions.clear();
        m.groups.clear();
        m.pending_offsets.clear();
        m.start_ms = -1;
        m.update_ms = now;
        Self::persist(&mut g, tid);
        InitResult { error: 0, producer_id: new_pid, producer_epoch: new_epoch }
    }

    fn check_producer(m: &TxnMetadata, pid: i64, epoch: i16) -> i16 {
        if m.producer_id != pid {
            return err::INVALID_PRODUCER_ID_MAPPING;
        }
        if m.producer_epoch != epoch {
            return err::PRODUCER_FENCED;
        }
        0
    }

    // ------------------------------------------------------------------
    // AddPartitionsToTxn (24)
    // ------------------------------------------------------------------
    pub fn add_partitions(&self, tid: &str, pid: i64, epoch: i16, parts: &[(String, i32)]) -> i16 {
        let mut g = self.inner.lock().unwrap();
        let now = now_ms();
        let m = match g.txns.get_mut(tid) {
            Some(m) => m,
            None => return err::TRANSACTIONAL_ID_NOT_FOUND,
        };
        let e = Self::check_producer(m, pid, epoch);
        if e != 0 {
            return e;
        }
        if m.state.is_prepare() {
            return err::CONCURRENT_TRANSACTIONS;
        }
        if m.state != TxnState::Ongoing {
            m.state = TxnState::Ongoing;
            m.start_ms = now;
            m.partitions.clear();
            m.groups.clear();
            m.pending_offsets.clear();
        }
        m.update_ms = now;
        for p in parts {
            m.partitions.insert(p.clone());
        }
        Self::persist(&mut g, tid);
        0
    }

    // ------------------------------------------------------------------
    // AddOffsetsToTxn (25)
    // ------------------------------------------------------------------
    pub fn add_offsets(&self, tid: &str, pid: i64, epoch: i16, group: &str) -> i16 {
        let mut g = self.inner.lock().unwrap();
        let now = now_ms();
        let m = match g.txns.get_mut(tid) {
            Some(m) => m,
            None => return err::TRANSACTIONAL_ID_NOT_FOUND,
        };
        let e = Self::check_producer(m, pid, epoch);
        if e != 0 {
            return e;
        }
        if m.state.is_prepare() {
            return err::CONCURRENT_TRANSACTIONS;
        }
        if m.state != TxnState::Ongoing {
            m.state = TxnState::Ongoing;
            m.start_ms = now;
            m.partitions.clear();
            m.groups.clear();
            m.pending_offsets.clear();
        }
        m.update_ms = now;
        m.groups.insert(group.to_string());
        Self::persist(&mut g, tid);
        0
    }

    // ------------------------------------------------------------------
    // TxnOffsetCommit (28)
    // ------------------------------------------------------------------
    /// Stages offsets; they become visible in the group only on commit.
    pub fn txn_offset_commit(
        &self,
        tid: &str,
        group: &str,
        pid: i64,
        epoch: i16,
        offsets: Vec<PendingOffset>,
    ) -> i16 {
        let mut g = self.inner.lock().unwrap();
        let m = match g.txns.get_mut(tid) {
            Some(m) => m,
            None => return err::TRANSACTIONAL_ID_NOT_FOUND,
        };
        let e = Self::check_producer(m, pid, epoch);
        if e != 0 {
            return e;
        }
        if m.state.is_prepare() {
            return err::CONCURRENT_TRANSACTIONS;
        }
        if m.state != TxnState::Ongoing || !m.groups.contains(group) {
            return err::INVALID_TXN_STATE;
        }
        for o in offsets {
            m.pending_offsets
                .retain(|x| !(x.group == o.group && x.topic == o.topic && x.partition == o.partition));
            m.pending_offsets.push(o);
        }
        m.update_ms = now_ms();
        Self::persist(&mut g, tid);
        0
    }

    // ------------------------------------------------------------------
    // Produce-side verification
    // ------------------------------------------------------------------
    /// Verifies a transactional produce to `(topic, partition)` is legal. Returns an error code (0 = ok).
    pub fn verify_produce(&self, tid: Option<&str>, pid: i64, epoch: i16, topic: &str, partition: i32) -> i16 {
        let tid = match tid {
            Some(t) if !t.is_empty() => t,
            _ => return err::INVALID_TXN_STATE,
        };
        let g = self.inner.lock().unwrap();
        let m = match g.txns.get(tid) {
            Some(m) => m,
            None => return err::INVALID_PRODUCER_ID_MAPPING,
        };
        if m.producer_id != pid {
            return err::INVALID_PRODUCER_ID_MAPPING;
        }
        if m.producer_epoch != epoch {
            return err::INVALID_PRODUCER_EPOCH;
        }
        if m.state != TxnState::Ongoing || !m.partitions.contains(&(topic.to_string(), partition)) {
            return err::INVALID_TXN_STATE;
        }
        0
    }

    // ------------------------------------------------------------------
    // EndTxn (26)
    // ------------------------------------------------------------------
    pub async fn end_txn(&self, tid: &str, pid: i64, epoch: i16, commit: bool) -> i16 {
        let (parts, epoch_for_marker) = {
            let mut g = self.inner.lock().unwrap();
            let now = now_ms();
            let m = match g.txns.get_mut(tid) {
                Some(m) => m,
                None => return err::TRANSACTIONAL_ID_NOT_FOUND,
            };
            let e = Self::check_producer(m, pid, epoch);
            if e != 0 {
                return e;
            }
            match m.state {
                TxnState::Ongoing => {}
                TxnState::Empty => return err::INVALID_TXN_STATE,
                TxnState::CompleteCommit => return if commit { 0 } else { err::INVALID_TXN_STATE },
                TxnState::CompleteAbort => return if commit { err::INVALID_TXN_STATE } else { 0 },
                TxnState::PrepareCommit => {
                    return if commit { err::CONCURRENT_TRANSACTIONS } else { err::INVALID_TXN_STATE }
                }
                TxnState::PrepareAbort => {
                    return if commit { err::INVALID_TXN_STATE } else { err::CONCURRENT_TRANSACTIONS }
                }
            }
            m.state = if commit { TxnState::PrepareCommit } else { TxnState::PrepareAbort };
            m.update_ms = now;
            let parts: Vec<(String, i32)> = m.partitions.iter().cloned().collect();
            let ep = m.producer_epoch;
            Self::persist(&mut g, tid);
            (parts, ep)
        };
        let _ = epoch_for_marker;
        match self.complete_txn(tid, commit, parts).await {
            Ok(()) => 0,
            Err(e) => {
                error!("[AeroMQ Txn] EndTxn({}, commit={}) incomplete: {}", tid, commit, e);
                err::COORDINATOR_NOT_AVAILABLE
            }
        }
    }

    /// Writes markers, applies staged offsets (commit) and moves to Complete*.
    async fn complete_txn(&self, tid: &str, commit: bool, parts: Vec<(String, i32)>) -> Result<(), String> {
        let (pid, epoch, offsets) = {
            let g = self.inner.lock().unwrap();
            let m = &g.txns[tid];
            (m.producer_id, m.producer_epoch, m.pending_offsets.clone())
        };
        self.write_markers(pid, epoch, &parts, commit).await?;
        if commit && !offsets.is_empty() {
            let mut by_group: HashMap<String, Vec<(String, i32, i64)>> = HashMap::new();
            for o in &offsets {
                by_group.entry(o.group.clone()).or_default().push((o.topic.clone(), o.partition, o.offset));
            }
            for (group, offs) in by_group {
                self.sink.commit(&group, &offs).await?;
            }
        }
        let mut g = self.inner.lock().unwrap();
        let now = now_ms();
        if let Some(m) = g.txns.get_mut(tid) {
            m.state = if commit { TxnState::CompleteCommit } else { TxnState::CompleteAbort };
            m.partitions.clear();
            m.groups.clear();
            m.pending_offsets.clear();
            m.update_ms = now;
        }
        Self::persist(&mut g, tid);
        Ok(())
    }

    /// Appends a control marker to every listed partition that has the producer's
    /// transaction open, updating the partition transaction index (LSO / aborted list).
    async fn write_markers(&self, pid: i64, epoch: i16, parts: &[(String, i32)], commit: bool) -> Result<(), String> {
        let lm = self.log.upgrade().ok_or_else(|| "log manager dropped".to_string())?;
        for (topic, partition) in parts {
            let pl = lm
                .get_partition(topic, *partition as u32)
                .await
                .map_err(|e| format!("open {}/{}: {}", topic, partition, e))?;
            let mut guard = pl.lock().await;
            if !guard.txn_index.is_ongoing(pid) {
                continue; // nothing written by this txn here, or marker already written
            }
            let off = guard.next_offset;
            let entry = encode_control_batch(off as i64, pid, epoch, commit, self.coordinator_epoch, now_ms());
            let assigned = guard.append(&entry).map_err(|e| format!("append marker {}/{}: {}", topic, partition, e))?;
            guard.txn_index.complete(pid, assigned, commit);
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Background maintenance
    // ------------------------------------------------------------------
    /// Aborts timed-out transactions (fencing the zombie by bumping its epoch),
    /// re-drives transactions stuck in Prepare*, and expires idle transactional ids.
    pub async fn sweep(&self, now: i64) {
        enum Action {
            Redrive(String, bool, Vec<(String, i32)>),
            Timeout(String, Vec<(String, i32)>),
            Expire(String),
        }
        let actions: Vec<Action> = {
            let mut g = self.inner.lock().unwrap();
            let mut acts = Vec::new();
            for m in g.txns.values() {
                match m.state {
                    TxnState::PrepareCommit | TxnState::PrepareAbort => acts.push(Action::Redrive(
                        m.transactional_id.clone(),
                        m.state == TxnState::PrepareCommit,
                        m.partitions.iter().cloned().collect(),
                    )),
                    TxnState::Ongoing if m.start_ms >= 0 && now - m.start_ms >= m.timeout_ms as i64 => {
                        acts.push(Action::Timeout(m.transactional_id.clone(), m.partitions.iter().cloned().collect()))
                    }
                    TxnState::Empty | TxnState::CompleteCommit | TxnState::CompleteAbort
                        if now - m.update_ms >= self.cfg.transactional_id_expiration_ms =>
                    {
                        acts.push(Action::Expire(m.transactional_id.clone()))
                    }
                    _ => {}
                }
            }
            for a in &acts {
                if let Action::Timeout(t, _) = a {
                    if let Some(m) = g.txns.get_mut(t) {
                        m.state = TxnState::PrepareAbort;
                        m.update_ms = now;
                    }
                    Self::persist(&mut g, t);
                }
                if let Action::Expire(t) = a {
                    g.txns.remove(t);
                    g.store.delete(t);
                }
            }
            acts
        };
        for a in actions {
            match a {
                Action::Redrive(tid, commit, parts) => {
                    if let Err(e) = self.complete_txn(&tid, commit, parts).await {
                        warn!("[AeroMQ Txn] re-drive of {} failed: {}", tid, e);
                    }
                }
                Action::Timeout(tid, parts) => {
                    info!("[AeroMQ Txn] transaction {} timed out; aborting and fencing producer", tid);
                    if self.complete_txn(&tid, false, parts).await.is_ok() {
                        // Bump epoch so the zombie producer is fenced.
                        let mut g = self.inner.lock().unwrap();
                        let mut new_pid = None;
                        if let Some(m) = g.txns.get(&tid) {
                            if m.producer_epoch >= i16::MAX - 2 {
                                new_pid = Some(());
                            }
                        }
                        let np = if new_pid.is_some() { Some(Self::alloc_pid(&mut g)) } else { None };
                        if let Some(m) = g.txns.get_mut(&tid) {
                            m.last_producer_epoch = m.producer_epoch;
                            match np {
                                Some(p) => {
                                    m.producer_id = p;
                                    m.producer_epoch = 0;
                                    m.last_producer_epoch = -1;
                                }
                                None => m.producer_epoch += 1,
                            }
                        }
                        Self::persist(&mut g, &tid);
                    }
                }
                Action::Expire(_) => {}
            }
        }
    }
}

/// Spawns the periodic sweeper (holds only a weak reference).
pub fn spawn_sweeper(coord: &Arc<TxnCoordinator>) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let weak = Arc::downgrade(coord);
    let interval = std::time::Duration::from_millis(coord.cfg.sweep_interval_ms.max(50));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match weak.upgrade() {
                Some(c) => c.sweep(now_ms()).await,
                None => break,
            }
        }
    });
}
