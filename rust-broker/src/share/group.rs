//! Share group coordinator + share-partition manager (KIP-932).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tracing::{debug, warn};

use super::state::{ArchiveReason, SharePartition};
use super::{topic_id, ShareConfig};
use crate::kafka::handlers::{encode_idempotent_records_batch, encode_single_record_batch, parse_records, KafkaRecord};
use crate::log::LogManager;
use crate::txn::batch;
use crate::txn::coordinator::now_ms;
use crate::txn::err;

/// Effective per-group settings (broker defaults, overridable per group).
#[derive(Debug, Clone, PartialEq)]
pub struct GroupConfig {
    pub earliest: bool,
    pub read_committed: bool,
    pub lock_timeout_ms: i64,
    pub max_delivery_attempts: i16,
    pub max_in_flight: usize,
    pub dlq_topic: Option<String>,
}

impl GroupConfig {
    fn from_broker(c: &ShareConfig) -> Self {
        Self {
            earliest: c.auto_offset_reset.eq_ignore_ascii_case("earliest"),
            read_committed: c.isolation_level.eq_ignore_ascii_case("read_committed"),
            lock_timeout_ms: c.lock_timeout_ms,
            max_delivery_attempts: c.max_delivery_attempts,
            max_in_flight: c.max_in_flight,
            dlq_topic: c.dlq_topic.clone(),
        }
    }
}

type PartKey = (String, String, i32);

#[derive(Debug, Clone)]
struct Member {
    id: String,
    epoch: i32,
    subscribed: Vec<String>,
    rack: Option<String>,
    last_hb: i64,
    assignment: BTreeSet<(String, i32)>,
}

#[derive(Debug)]
struct Group {
    epoch: i32,
    members: BTreeMap<String, Member>,
}

#[derive(Debug, Clone)]
struct Session {
    /// The epoch the next request must carry.
    epoch: i32,
    partitions: BTreeSet<(String, i32)>,
}

struct Inner {
    groups: HashMap<String, Group>,
    sessions: HashMap<(String, String), Session>,
    parts: HashMap<PartKey, Arc<Mutex<SharePartition>>>,
    topic_ids: HashMap<[u8; 16], String>,
    overrides: HashMap<String, GroupConfig>,
}

pub struct ShareCoordinator {
    inner: Mutex<Inner>,
    log: Weak<LogManager>,
    pub cfg: ShareConfig,
    dir: Option<PathBuf>,
}

// ----------------------------- request / result types -----------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckBatch {
    pub first: i64,
    pub last: i64,
    pub types: Vec<i8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HbResult {
    pub error: i16,
    pub message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    /// `None` when the assignment did not change.
    pub assignment: Option<Vec<(String, Vec<i32>)>>,
}

#[derive(Debug, Clone, Default)]
pub struct ShareFetchArgs {
    pub group_id: String,
    pub member_id: String,
    pub epoch: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_records: i32,
    /// (topic id, [(partition, acks)])
    pub topics: Vec<([u8; 16], Vec<(i32, Vec<AckBatch>)>)>,
    pub forgotten: Vec<([u8; 16], Vec<i32>)>,
}

#[derive(Debug, Clone, Default)]
pub struct PartResult {
    pub topic: String,
    pub topic_id: [u8; 16],
    pub partition: i32,
    pub error: i16,
    pub ack_error: i16,
    /// Concatenated record batches (one per acquired offset).
    pub records: Vec<u8>,
    /// (first_offset, last_offset, delivery_count)
    pub acquired: Vec<(i64, i64, i16)>,
}

#[derive(Debug, Clone, Default)]
pub struct ShareFetchOutcome {
    pub error: i16,
    pub message: Option<String>,
    pub lock_timeout_ms: i32,
    pub partitions: Vec<PartResult>,
}

#[derive(Debug, Clone)]
pub struct MemberDescription {
    pub member_id: String,
    pub rack: Option<String>,
    pub epoch: i32,
    pub subscribed: Vec<String>,
    pub assignment: Vec<(String, Vec<i32>)>,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub error: i16,
    pub state: &'static str,
    pub epoch: i32,
    pub members: Vec<MemberDescription>,
}

impl ShareCoordinator {
    pub fn open(dir: Option<&Path>, log: Weak<LogManager>, cfg: ShareConfig) -> Self {
        if let Some(d) = dir {
            let _ = std::fs::create_dir_all(d);
        }
        Self {
            inner: Mutex::new(Inner {
                groups: HashMap::new(),
                sessions: HashMap::new(),
                parts: HashMap::new(),
                topic_ids: HashMap::new(),
                overrides: HashMap::new(),
            }),
            log,
            cfg,
            dir: dir.map(|d| d.to_path_buf()),
        }
    }

    pub fn set_group_config(&self, group: &str, cfg: GroupConfig) {
        self.inner.lock().unwrap().overrides.insert(group.to_string(), cfg);
    }

    pub fn group_config(&self, group: &str) -> GroupConfig {
        self.inner
            .lock()
            .unwrap()
            .overrides
            .get(group)
            .cloned()
            .unwrap_or_else(|| GroupConfig::from_broker(&self.cfg))
    }

    pub fn topic_name(&self, id: &[u8; 16]) -> Option<String> {
        self.inner.lock().unwrap().topic_ids.get(id).cloned()
    }

    fn register_topic(inner: &mut Inner, name: &str) -> [u8; 16] {
        let id = topic_id(name);
        inner.topic_ids.insert(id, name.to_string());
        id
    }

    /// Snapshot of `topic -> sorted partitions` materialised on this broker.
    async fn local_partitions(&self) -> HashMap<String, Vec<i32>> {
        let mut m: HashMap<String, Vec<i32>> = HashMap::new();
        if let Some(lm) = self.log.upgrade() {
            for (t, p, _) in lm.get_all_offsets().await {
                m.entry(t).or_default().push(p as i32);
            }
        }
        for v in m.values_mut() {
            v.sort_unstable();
            v.dedup();
        }
        m
    }

    /// Deterministic assignment: with M <= P members partitions are dealt
    /// round-robin; with M > P every partition is shared by several members
    /// (share groups may assign a partition to multiple members).
    fn compute_assignment(group: &Group, topic_parts: &HashMap<String, Vec<i32>>) -> HashMap<String, BTreeSet<(String, i32)>> {
        let mut out: HashMap<String, BTreeSet<(String, i32)>> =
            group.members.keys().map(|k| (k.clone(), BTreeSet::new())).collect();
        let topics: BTreeSet<&String> = group.members.values().flat_map(|m| m.subscribed.iter()).collect();
        for t in topics {
            let parts = match topic_parts.get(t) {
                Some(p) if !p.is_empty() => p,
                _ => continue,
            };
            let subs: Vec<&Member> = group.members.values().filter(|m| m.subscribed.contains(t)).collect();
            let (m, p) = (subs.len(), parts.len());
            if m == 0 {
                continue;
            }
            if m <= p {
                for (i, part) in parts.iter().enumerate() {
                    out.get_mut(&subs[i % m].id).unwrap().insert((t.clone(), *part));
                }
            } else {
                for (i, mem) in subs.iter().enumerate() {
                    out.get_mut(&mem.id).unwrap().insert((t.clone(), parts[i % p]));
                }
            }
        }
        out
    }

    fn group_assignment_view(a: &BTreeSet<(String, i32)>) -> Vec<(String, Vec<i32>)> {
        let mut m: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (t, p) in a {
            m.entry(t.clone()).or_default().push(*p);
        }
        m.into_iter().collect()
    }

    // --------------------------------------------------------------------
    // ShareGroupHeartbeat (76)
    // --------------------------------------------------------------------
    pub async fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
        rack: Option<String>,
        subscribed: Option<Vec<String>>,
    ) -> HbResult {
        let fail = |e: i16, msg: &str| HbResult {
            error: e,
            message: Some(msg.to_string()),
            member_id: None,
            member_epoch: 0,
            heartbeat_interval_ms: self.cfg.heartbeat_interval_ms,
            assignment: None,
        };
        if group_id.is_empty() {
            return fail(err::INVALID_REQUEST, "GroupId can't be empty");
        }
        if member_epoch < -1 {
            return fail(err::INVALID_REQUEST, "MemberEpoch is invalid");
        }
        let topic_parts = self.local_partitions().await;
        let now = now_ms();
        let mut released: Vec<String> = Vec::new();
        let result = {
            let mut g = self.inner.lock().unwrap();
            if let Some(subs) = &subscribed {
                for t in subs {
                    Self::register_topic(&mut g, t);
                }
            }
            // Leave.
            if member_epoch == -1 {
                let grp = match g.groups.get_mut(group_id) {
                    Some(x) => x,
                    None => return fail(err::GROUP_ID_NOT_FOUND, "Group not found"),
                };
                if grp.members.remove(member_id).is_none() {
                    return fail(err::UNKNOWN_MEMBER_ID, "Member not found");
                }
                grp.epoch += 1;
                g.sessions.remove(&(group_id.to_string(), member_id.to_string()));
                released.push(member_id.to_string());
                Some(HbResult {
                    error: 0,
                    message: None,
                    member_id: Some(member_id.to_string()),
                    member_epoch: -1,
                    heartbeat_interval_ms: self.cfg.heartbeat_interval_ms,
                    assignment: None,
                })
            } else {
                let is_join = member_epoch == 0;
                let mut mid = member_id.to_string();
                if is_join {
                    if mid.is_empty() {
                        mid = format!("member-{:x}-{:x}", now, g.groups.len());
                    }
                    if subscribed.is_none() {
                        return fail(err::INVALID_REQUEST, "SubscribedTopicNames must be set on join");
                    }
                    let grp = g.groups.entry(group_id.to_string()).or_insert(Group { epoch: 0, members: BTreeMap::new() });
                    let existed = grp.members.contains_key(&mid);
                    if !existed {
                        grp.epoch += 1;
                    }
                    grp.members.insert(
                        mid.clone(),
                        Member {
                            id: mid.clone(),
                            epoch: 0,
                            subscribed: subscribed.clone().unwrap(),
                            rack: rack.clone(),
                            last_hb: now,
                            assignment: BTreeSet::new(),
                        },
                    );
                } else {
                    let grp = match g.groups.get_mut(group_id) {
                        Some(x) => x,
                        None => return fail(err::UNKNOWN_MEMBER_ID, "Group not found"),
                    };
                    let m = match grp.members.get_mut(&mid) {
                        Some(m) => m,
                        None => return fail(err::UNKNOWN_MEMBER_ID, "Member not found"),
                    };
                    if m.epoch != member_epoch {
                        return fail(err::FENCED_MEMBER_EPOCH, "Member epoch is stale");
                    }
                    m.last_hb = now;
                    if rack.is_some() {
                        m.rack = rack.clone();
                    }
                    if let Some(subs) = &subscribed {
                        if &m.subscribed != subs {
                            m.subscribed = subs.clone();
                            grp.epoch += 1;
                        }
                    }
                }
                // Recompute assignment for the whole group and diff for this member.
                let grp = g.groups.get_mut(group_id).unwrap();
                let desired = Self::compute_assignment(grp, &topic_parts);
                let mine = desired.get(&mid).cloned().unwrap_or_default();
                let changed = grp.members[&mid].assignment != mine;
                if changed {
                    grp.epoch += 1;
                }
                let group_epoch = grp.epoch;
                let m = grp.members.get_mut(&mid).unwrap();
                let send = is_join || changed;
                if send {
                    m.assignment = mine.clone();
                    m.epoch = group_epoch;
                }
                let epoch_out = m.epoch;
                let assignment = if send { Some(Self::group_assignment_view(&mine)) } else { None };
                if let Some(a) = &assignment {
                    let names: Vec<String> = a.iter().map(|(t, _)| t.clone()).collect();
                    for t in names {
                        Self::register_topic(&mut g, &t);
                    }
                }
                Some(HbResult {
                    error: 0,
                    message: None,
                    member_id: Some(mid),
                    member_epoch: epoch_out,
                    heartbeat_interval_ms: self.cfg.heartbeat_interval_ms,
                    assignment,
                })
            }
        };
        for m in released {
            self.release_member_everywhere(group_id, &m).await;
        }
        result.unwrap()
    }

    // --------------------------------------------------------------------
    // Session handling
    // --------------------------------------------------------------------
    /// Validates the share session for `(group, member)`; returns whether the session was closed.
    fn check_session(inner: &mut Inner, group: &str, member: &str, epoch: i32) -> Result<bool, i16> {
        let grp = inner.groups.get(group).ok_or(err::GROUP_ID_NOT_FOUND)?;
        if !grp.members.contains_key(member) {
            return Err(err::UNKNOWN_MEMBER_ID);
        }
        let key = (group.to_string(), member.to_string());
        match epoch {
            0 => {
                inner.sessions.insert(key, Session { epoch: 1, partitions: BTreeSet::new() });
                Ok(false)
            }
            -1 => {
                if inner.sessions.remove(&key).is_none() {
                    return Err(err::SHARE_SESSION_NOT_FOUND);
                }
                Ok(true)
            }
            e if e > 0 => {
                let s = inner.sessions.get_mut(&key).ok_or(err::SHARE_SESSION_NOT_FOUND)?;
                if s.epoch != e {
                    return Err(err::INVALID_SHARE_SESSION_EPOCH);
                }
                s.epoch = if e == i32::MAX { 1 } else { e + 1 };
                Ok(false)
            }
            _ => Err(err::INVALID_SHARE_SESSION_EPOCH),
        }
    }

    // --------------------------------------------------------------------
    // ShareFetch (78)
    // --------------------------------------------------------------------
    pub async fn share_fetch(&self, a: ShareFetchArgs) -> ShareFetchOutcome {
        let now = now_ms();
        let gcfg = self.group_config(&a.group_id);
        let mut out = ShareFetchOutcome { lock_timeout_ms: gcfg.lock_timeout_ms as i32, ..Default::default() };

        // 1. session validation + partition set updates.
        let mut named_topics: Vec<(String, [u8; 16], Vec<(i32, Vec<AckBatch>)>)> = Vec::new();
        let mut unknown: Vec<PartResult> = Vec::new();
        let (closed, session_parts) = {
            let mut g = self.inner.lock().unwrap();
            let closed = match Self::check_session(&mut g, &a.group_id, &a.member_id, a.epoch) {
                Ok(c) => c,
                Err(e) => {
                    out.error = e;
                    return out;
                }
            };
            for (tid, parts) in &a.topics {
                match g.topic_ids.get(tid).cloned() {
                    Some(name) => named_topics.push((name, *tid, parts.clone())),
                    None => {
                        for (p, _) in parts {
                            unknown.push(PartResult {
                                topic_id: *tid,
                                partition: *p,
                                error: err::UNKNOWN_TOPIC_ID,
                                ..Default::default()
                            });
                        }
                    }
                }
            }
            if !closed {
                let key = (a.group_id.clone(), a.member_id.clone());
                let forgotten: Vec<(String, i32)> = a
                    .forgotten
                    .iter()
                    .filter_map(|(tid, ps)| g.topic_ids.get(tid).map(|n| ps.iter().map(|p| (n.clone(), *p)).collect::<Vec<_>>()))
                    .flatten()
                    .collect();
                let s = g.sessions.get_mut(&key).unwrap();
                for (name, _, parts) in &named_topics {
                    for (p, _) in parts {
                        s.partitions.insert((name.clone(), *p));
                    }
                }
                for f in forgotten {
                    s.partitions.remove(&f);
                }
                (false, s.partitions.iter().cloned().collect::<Vec<_>>())
            } else {
                (true, Vec::new())
            }
        };

        // 2. acknowledgements piggybacked on the fetch.
        let mut results: BTreeMap<(String, i32), PartResult> = BTreeMap::new();
        for r in unknown {
            results.insert((format!("?{:?}", r.topic_id), r.partition), r);
        }
        for (name, tid, parts) in &named_topics {
            for (p, acks) in parts {
                if acks.is_empty() {
                    continue;
                }
                let e = self.apply_acks(&a.group_id, &a.member_id, name, *p, acks, &gcfg).await;
                let r = results.entry((name.clone(), *p)).or_insert_with(|| PartResult {
                    topic: name.clone(),
                    topic_id: *tid,
                    partition: *p,
                    ..Default::default()
                });
                r.ack_error = e;
            }
        }

        if closed {
            self.release_member_everywhere(&a.group_id, &a.member_id).await;
            out.partitions = results.into_values().collect();
            return out;
        }

        // 3. acquire records (bounded wait for data).
        let max_records = if a.max_records > 0 { a.max_records as usize } else { self.cfg.default_max_records as usize };
        let max_bytes = if a.max_bytes > 0 { a.max_bytes as usize } else { usize::MAX };
        let deadline = now + a.max_wait_ms.max(0) as i64;
        let mut budget = max_records;
        let mut bytes_left = max_bytes;
        loop {
            let mut got = false;
            for (topic, part) in &session_parts {
                if budget == 0 || bytes_left == 0 {
                    break;
                }
                match self
                    .acquire_partition(&a.group_id, &a.member_id, topic, *part, &gcfg, budget, bytes_left, now_ms())
                    .await
                {
                    Ok((records, acquired)) => {
                        if acquired.is_empty() {
                            continue;
                        }
                        got = true;
                        let n: i64 = acquired.iter().map(|(f, l, _)| l - f + 1).sum();
                        budget = budget.saturating_sub(n as usize);
                        bytes_left = bytes_left.saturating_sub(records.len());
                        let r = results.entry((topic.clone(), *part)).or_insert_with(|| PartResult {
                            topic: topic.clone(),
                            topic_id: topic_id(topic),
                            partition: *part,
                            ..Default::default()
                        });
                        r.records.extend_from_slice(&records);
                        r.acquired.extend(acquired);
                    }
                    Err(e) => {
                        let r = results.entry((topic.clone(), *part)).or_insert_with(|| PartResult {
                            topic: topic.clone(),
                            topic_id: topic_id(topic),
                            partition: *part,
                            ..Default::default()
                        });
                        r.error = e;
                    }
                }
            }
            if got || now_ms() >= deadline || a.max_wait_ms <= 0 {
                break;
            }
            let remaining = (deadline - now_ms()).max(1) as u64;
            tokio::time::sleep(Duration::from_millis(remaining.min(20))).await;
        }
        out.partitions = results.into_values().collect();
        out
    }

    // --------------------------------------------------------------------
    // ShareAcknowledge (79)
    // --------------------------------------------------------------------
    /// Returns (top-level error, per-partition results with `ack_error`).
    pub async fn share_acknowledge(
        &self,
        group_id: &str,
        member_id: &str,
        epoch: i32,
        topics: Vec<([u8; 16], Vec<(i32, Vec<AckBatch>)>)>,
    ) -> (i16, Vec<PartResult>) {
        let gcfg = self.group_config(group_id);
        let mut named = Vec::new();
        let mut results = Vec::new();
        let closed = {
            let mut g = self.inner.lock().unwrap();
            let closed = match Self::check_session(&mut g, group_id, member_id, epoch) {
                Ok(c) => c,
                Err(e) => return (e, vec![]),
            };
            for (tid, parts) in topics {
                match g.topic_ids.get(&tid).cloned() {
                    Some(n) => named.push((n, tid, parts)),
                    None => {
                        for (p, _) in parts {
                            results.push(PartResult { topic_id: tid, partition: p, ack_error: err::UNKNOWN_TOPIC_ID, ..Default::default() });
                        }
                    }
                }
            }
            closed
        };
        for (name, tid, parts) in named {
            for (p, acks) in parts {
                let e = self.apply_acks(group_id, member_id, &name, p, &acks, &gcfg).await;
                results.push(PartResult { topic: name.clone(), topic_id: tid, partition: p, ack_error: e, ..Default::default() });
            }
        }
        if closed {
            self.release_member_everywhere(group_id, member_id).await;
        }
        (0, results)
    }

    /// Applies acknowledgement batches; returns the first error code (0 = ok).
    async fn apply_acks(&self, group: &str, member: &str, topic: &str, part: i32, acks: &[AckBatch], gcfg: &GroupConfig) -> i16 {
        let sp = match self.share_partition_if_exists(group, topic, part) {
            Some(sp) => sp,
            None => return err::INVALID_RECORD_STATE,
        };
        let now = now_ms();
        let mut first_err = 0;
        let mut archived: Vec<(u64, ArchiveReason)> = Vec::new();
        {
            let mut s = sp.lock().unwrap();
            for b in acks {
                if b.first < 0 || b.last < b.first {
                    first_err = first_err.max(err::INVALID_REQUEST);
                    continue;
                }
                let (r, arch) = s.acknowledge(member, b.first as u64, b.last as u64, &b.types, now);
                archived.extend(arch);
                if let Err(e) = r {
                    if first_err == 0 {
                        first_err = e;
                    }
                }
            }
        }
        self.dlq_forward(group, topic, part, &archived, gcfg).await;
        first_err
    }

    fn share_partition_if_exists(&self, group: &str, topic: &str, part: i32) -> Option<Arc<Mutex<SharePartition>>> {
        self.inner.lock().unwrap().parts.get(&(group.to_string(), topic.to_string(), part)).cloned()
    }

    fn state_path(&self, key: &PartKey) -> Option<PathBuf> {
        let d = self.dir.as_ref()?;
        let hex = |s: &str| s.bytes().map(|b| format!("{:02x}", b)).collect::<String>();
        Some(d.join(hex(&key.0)).join(format!("{}_{}.state", hex(&key.1), key.2)))
    }

    /// Gets or creates (loading persisted state) the share partition.
    fn share_partition(&self, group: &str, topic: &str, part: i32, initial_start: u64, gcfg: &GroupConfig) -> Arc<Mutex<SharePartition>> {
        let key: PartKey = (group.to_string(), topic.to_string(), part);
        let mut g = self.inner.lock().unwrap();
        if let Some(sp) = g.parts.get(&key) {
            return sp.clone();
        }
        let loaded = self
            .state_path(&key)
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|t| SharePartition::decode(&t, gcfg.max_delivery_attempts, gcfg.lock_timeout_ms, gcfg.max_in_flight));
        let sp = Arc::new(Mutex::new(loaded.unwrap_or_else(|| {
            SharePartition::new(initial_start, gcfg.max_delivery_attempts, gcfg.lock_timeout_ms, gcfg.max_in_flight)
        })));
        g.parts.insert(key, sp.clone());
        sp
    }

    /// Reads the log entry stored at `offset`. Returns the raw entry bytes.
    fn read_entry(log: &mut crate::log::PartitionLog, offset: u64) -> Option<Vec<u8>> {
        match log.read_from_offset(offset, u32::MAX) {
            Ok(Some((mut file, pos, len))) => {
                file.seek(SeekFrom::Start(pos)).ok()?;
                let mut buf = vec![0u8; len as usize];
                file.read_exact(&mut buf).ok()?;
                Some(buf)
            }
            _ => None,
        }
    }

    /// Converts a stored entry into a single-record batch whose base offset is `offset`.
    fn deliverable(entry: &[u8], offset: u64) -> Vec<u8> {
        if batch::is_magic2(entry) {
            let mut e = entry.to_vec();
            batch::patch_base_offset(&mut e, offset as i64);
            e
        } else {
            let rec = parse_records(entry)
                .ok()
                .and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) })
                .unwrap_or_else(|| KafkaRecord::new(None, Some(entry.to_vec())));
            encode_single_record_batch(offset as i64, &rec)
        }
    }

    /// Acquires up to `max_records` records for `member` from one partition.
    #[allow(clippy::too_many_arguments)]
    async fn acquire_partition(
        &self,
        group: &str,
        member: &str,
        topic: &str,
        part: i32,
        gcfg: &GroupConfig,
        max_records: usize,
        max_bytes: usize,
        now: i64,
    ) -> Result<(Vec<u8>, Vec<(i64, i64, i16)>), i16> {
        let lm = self.log.upgrade().ok_or(err::KAFKA_STORAGE_ERROR)?;
        let pl = lm.get_partition(topic, part as u32).await.map_err(|_| err::UNKNOWN_TOPIC_OR_PARTITION)?;
        let mut log = pl.lock().await;
        let initial = if gcfg.earliest {
            log.segments.first().map(|s| s.base_offset).unwrap_or(0)
        } else {
            log.high_watermark
        };
        let sp_arc = self.share_partition(group, topic, part, initial, gcfg);
        let bound = if gcfg.read_committed { log.txn_index.lso(log.high_watermark) } else { log.high_watermark };

        let (records, acquired, expired) = {
        let mut sp = sp_arc.lock().unwrap();
        let expired = sp.expire_locks(now);
        let mut cands = sp.available(max_records);
        let capacity = sp.max_in_flight.saturating_sub(sp.in_flight());
        let mut next = sp.end_offset;
        let mut new_count = 0;
        while cands.len() < max_records && new_count < capacity && next < bound {
            cands.push(next);
            next += 1;
            new_count += 1;
        }

        let mut records = Vec::new();
        let mut acquired: Vec<(i64, i64, i16)> = Vec::new();
        let mut skip_below = 0u64;
        for off in cands {
            if off < skip_below {
                continue;
            }
            if !records.is_empty() && records.len() >= max_bytes {
                break;
            }
            let entry = match Self::read_entry(&mut log, off) {
                Some(e) => e,
                None => {
                    if off >= log.next_offset {
                        break;
                    }
                    sp.archive_skipped(off);
                    continue;
                }
            };
            let mut actual = off;
            if batch::is_magic2(&entry) {
                let base = i64::from_be_bytes(entry[0..8].try_into().unwrap());
                if base >= off as i64 && (base as u64) < log.next_offset {
                    actual = base as u64;
                }
            }
            if actual > off {
                for o in off..actual {
                    sp.archive_skipped(o);
                }
                if actual >= bound {
                    break;
                }
                skip_below = actual + 1;
            }
            // Skip control batches and (read_committed) aborted transactional data.
            if batch::is_control(&entry) {
                sp.archive_skipped(actual);
                continue;
            }
            if gcfg.read_committed && batch::is_transactional(&entry) {
                if let Some((pid, ..)) = batch::producer_info(&entry) {
                    if log.txn_index.is_aborted(pid, actual) {
                        sp.archive_skipped(actual);
                        continue;
                    }
                }
            }
            let bytes = Self::deliverable(&entry, actual);
            let count = sp.acquire(actual, member, now);
            records.extend_from_slice(&bytes);
            match acquired.last_mut() {
                Some(l) if l.1 + 1 == actual as i64 && l.2 == count => l.1 = actual as i64,
                _ => acquired.push((actual as i64, actual as i64, count)),
            }
        }
        sp.trim();
        (records, acquired, expired)
        };
        drop(log);
        self.dlq_forward(group, topic, part, &expired, gcfg).await;
        Ok((records, acquired))
    }

    // --------------------------------------------------------------------
    // Release / expiry / DLQ
    // --------------------------------------------------------------------
    async fn release_member_everywhere(&self, group: &str, member: &str) {
        let gcfg = self.group_config(group);
        let parts: Vec<(PartKey, Arc<Mutex<SharePartition>>)> = {
            let g = self.inner.lock().unwrap();
            g.parts.iter().filter(|(k, _)| k.0 == group).map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        for (k, sp) in parts {
            let archived = sp.lock().unwrap().release_member(member);
            self.dlq_forward(group, &k.1, k.2, &archived, &gcfg).await;
        }
    }

    /// Copies archived (rejected / delivery-limit) records into the group's DLQ topic.
    async fn dlq_forward(&self, group: &str, topic: &str, part: i32, archived: &[(u64, ArchiveReason)], gcfg: &GroupConfig) {
        let tmpl = match &gcfg.dlq_topic {
            Some(t) if !archived.is_empty() => t.clone(),
            _ => return,
        };
        let lm = match self.log.upgrade() {
            Some(l) => l,
            None => return,
        };
        let dlq_topic = tmpl.replace("{topic}", topic).replace("{group}", group);
        if dlq_topic == topic {
            return;
        }
        // Read originals.
        let mut recs: Vec<(u64, ArchiveReason, KafkaRecord)> = Vec::new();
        if let Ok(pl) = lm.get_partition(topic, part as u32).await {
            let mut log = pl.lock().await;
            for (off, reason) in archived {
                if let Some(entry) = Self::read_entry(&mut log, *off) {
                    let rec = if batch::is_magic2(&entry) {
                        parse_records(&entry).ok().and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) })
                    } else {
                        parse_records(&entry).ok().and_then(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) })
                    }
                    .unwrap_or_else(|| KafkaRecord::new(None, Some(entry.clone())));
                    recs.push((*off, *reason, rec));
                }
            }
        }
        if recs.is_empty() {
            return;
        }
        match lm.get_partition(&dlq_topic, part as u32).await {
            Ok(dpl) => {
                let mut dlog = dpl.lock().await;
                for (off, reason, mut rec) in recs {
                    rec.headers.push(("x-original-topic".into(), topic.as_bytes().to_vec()));
                    rec.headers.push(("x-original-partition".into(), part.to_string().into_bytes()));
                    rec.headers.push(("x-original-offset".into(), off.to_string().into_bytes()));
                    rec.headers.push(("x-share-group".into(), group.as_bytes().to_vec()));
                    rec.headers.push((
                        "x-dlq-reason".into(),
                        match reason {
                            ArchiveReason::Rejected => b"rejected".to_vec(),
                            ArchiveReason::DeliveryLimit => b"delivery-limit-exceeded".to_vec(),
                        },
                    ));
                    let next = dlog.next_offset as i64;
                    let b = encode_idempotent_records_batch(next, -1, -1, -1, std::slice::from_ref(&rec));
                    if let Err(e) = dlog.append(&b) {
                        warn!("[AeroMQ Share] DLQ append failed: {}", e);
                    } else {
                        debug!("[AeroMQ Share] {}/{}@{} -> {}", topic, part, off, dlq_topic);
                    }
                }
            }
            Err(e) => warn!("[AeroMQ Share] cannot open DLQ topic {}: {}", dlq_topic, e),
        }
    }

    // --------------------------------------------------------------------
    // ShareGroupDescribe (77)
    // --------------------------------------------------------------------
    pub fn describe(&self, group_ids: &[String]) -> Vec<GroupDescription> {
        let g = self.inner.lock().unwrap();
        group_ids
            .iter()
            .map(|id| match g.groups.get(id) {
                None => GroupDescription { group_id: id.clone(), error: err::GROUP_ID_NOT_FOUND, state: "Dead", epoch: 0, members: vec![] },
                Some(grp) => GroupDescription {
                    group_id: id.clone(),
                    error: 0,
                    state: if grp.members.is_empty() { "Empty" } else { "Stable" },
                    epoch: grp.epoch,
                    members: grp
                        .members
                        .values()
                        .map(|m| MemberDescription {
                            member_id: m.id.clone(),
                            rack: m.rack.clone(),
                            epoch: m.epoch,
                            subscribed: m.subscribed.clone(),
                            assignment: Self::group_assignment_view(&m.assignment),
                        })
                        .collect(),
                },
            })
            .collect()
    }

    /// Inspection helper: (spso, speo, available, acquired, acknowledged, archived).
    pub fn partition_stats(&self, group: &str, topic: &str, part: i32) -> Option<(u64, u64, usize, usize, usize, usize)> {
        let sp = self.share_partition_if_exists(group, topic, part)?;
        let s = sp.lock().unwrap();
        let (a, q, k, r) = s.state_counts();
        Some((s.start_offset, s.end_offset, a, q, k, r))
    }

    // --------------------------------------------------------------------
    // Maintenance
    // --------------------------------------------------------------------
    /// Expires members past the session timeout, expires acquisition locks and flushes state.
    pub async fn sweep(&self, now: i64) {
        // 1. members
        let mut dead: Vec<(String, String)> = Vec::new();
        {
            let mut g = self.inner.lock().unwrap();
            let timeout = self.cfg.session_timeout_ms;
            let mut gone = Vec::new();
            for (gid, grp) in g.groups.iter_mut() {
                let expired: Vec<String> =
                    grp.members.values().filter(|m| now - m.last_hb > timeout).map(|m| m.id.clone()).collect();
                for m in expired {
                    grp.members.remove(&m);
                    grp.epoch += 1;
                    gone.push((gid.clone(), m));
                }
            }
            for (gid, m) in &gone {
                g.sessions.remove(&(gid.clone(), m.clone()));
            }
            dead.extend(gone);
        }
        for (gid, m) in dead {
            self.release_member_everywhere(&gid, &m).await;
        }
        // 2. acquisition locks
        let parts: Vec<(PartKey, Arc<Mutex<SharePartition>>)> =
            self.inner.lock().unwrap().parts.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (k, sp) in &parts {
            let archived = sp.lock().unwrap().expire_locks(now);
            if !archived.is_empty() {
                let gcfg = self.group_config(&k.0);
                self.dlq_forward(&k.0, &k.1, k.2, &archived, &gcfg).await;
            }
        }
        // 3. persist
        self.flush();
    }

    /// Writes dirty share-partition snapshots to disk.
    pub fn flush(&self) {
        let parts: Vec<(PartKey, Arc<Mutex<SharePartition>>)> =
            self.inner.lock().unwrap().parts.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (k, sp) in parts {
            let text = {
                let mut s = sp.lock().unwrap();
                if !s.dirty {
                    continue;
                }
                s.dirty = false;
                s.encode()
            };
            if let Some(path) = self.state_path(&k) {
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let tmp = path.with_extension("tmp");
                if std::fs::write(&tmp, text).and_then(|_| std::fs::rename(&tmp, &path)).is_err() {
                    warn!("[AeroMQ Share] failed to persist {:?}", path);
                    sp.lock().unwrap().dirty = true;
                }
            }
        }
    }
}

/// Spawns the periodic sweeper (weak reference only).
pub fn spawn_sweeper(c: &Arc<ShareCoordinator>) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    let weak = Arc::downgrade(c);
    let interval = Duration::from_millis(c.cfg.sweep_interval_ms.max(20));
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
