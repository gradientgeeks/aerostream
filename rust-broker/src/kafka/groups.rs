//! Kafka classic consumer-group coordinator (JoinGroup / SyncGroup / Heartbeat / LeaveGroup state machine).
//!
//! Group membership state lives in memory on the coordinator broker (chosen by hashing the group id over
//! the live brokers, see `TopologyCache::coordinator_for`). Committed offsets are kept here as a cache and
//! written through to the controller (Raft-replicated) by the wire layer (`group_api.rs`) so they survive
//! coordinator moves and broker restarts.
//!
//! States follow Kafka: Empty -> PreparingRebalance -> CompletingRebalance -> Stable.
//! JoinGroup and SyncGroup responses are delivered asynchronously through oneshot channels.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

// Kafka error codes used by the coordinator.
pub const NONE: i16 = 0;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const INVALID_GROUP_ID: i16 = 24;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const INVALID_SESSION_TIMEOUT: i16 = 26;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const GROUP_ID_NOT_FOUND: i16 = 69;
pub const NON_EMPTY_GROUP: i16 = 68;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl GroupState {
    pub fn name(self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
            GroupState::Dead => "Dead",
        }
    }
}

#[derive(Clone, Debug)]
pub struct JoinRequest {
    pub group_id: String,
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
}

#[derive(Clone, Debug, Default)]
pub struct JoinOutcome {
    pub error: i16,
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol: Option<String>,
    pub leader: String,
    pub member_id: String,
    /// (member id, instance id, metadata) - only populated for the leader.
    pub members: Vec<(String, Option<String>, Vec<u8>)>,
}

impl JoinOutcome {
    fn err(code: i16, member_id: &str) -> Self {
        JoinOutcome { error: code, generation: -1, member_id: member_id.to_string(), ..Default::default() }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SyncOutcome {
    pub error: i16,
    pub protocol_type: Option<String>,
    pub protocol: Option<String>,
    pub assignment: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct OffsetEntry {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_ms: i64,
}

struct Member {
    id: String,
    instance_id: Option<String>,
    client_id: String,
    client_host: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocols: Vec<(String, Vec<u8>)>,
    assignment: Vec<u8>,
    last_hb: Instant,
    join_tx: Option<oneshot::Sender<JoinOutcome>>,
    sync_tx: Option<oneshot::Sender<SyncOutcome>>,
}

struct Group {
    id: String,
    state: GroupState,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: Option<String>,
    members: BTreeMap<String, Member>,
    epoch: u64,
    delay_active: bool,
    offsets: HashMap<(String, i32), OffsetEntry>,
}

impl Group {
    fn new(id: &str) -> Self {
        Group {
            id: id.to_string(),
            state: GroupState::Empty,
            generation: 0,
            protocol_type: None,
            protocol: None,
            leader: None,
            members: BTreeMap::new(),
            epoch: 0,
            delay_active: false,
            offsets: HashMap::new(),
        }
    }

    fn candidate_protocols(&self) -> Vec<String> {
        let mut iter = self.members.values();
        let first = match iter.next() {
            Some(m) => m,
            None => return vec![],
        };
        let mut cands: Vec<String> = first.protocols.iter().map(|p| p.0.clone()).collect();
        for m in iter {
            cands.retain(|c| m.protocols.iter().any(|p| &p.0 == c));
        }
        cands
    }

    fn select_protocol(&self) -> Option<String> {
        let cands = self.candidate_protocols();
        if cands.is_empty() {
            return None;
        }
        let mut votes: BTreeMap<&str, usize> = BTreeMap::new();
        for m in self.members.values() {
            if let Some(p) = m.protocols.iter().find(|p| cands.contains(&p.0)) {
                *votes.entry(p.0.as_str()).or_default() += 1;
            }
        }
        // highest votes, ties resolved by the first member's preference order
        cands
            .iter()
            .max_by_key(|c| (votes.get(c.as_str()).copied().unwrap_or(0), std::cmp::Reverse(cands.iter().position(|x| x == *c).unwrap())))
            .cloned()
    }

    fn all_joined(&self) -> bool {
        !self.members.is_empty() && self.members.values().all(|m| m.join_tx.is_some())
    }

    fn fail_sync_waiters(&mut self, code: i16) {
        for m in self.members.values_mut() {
            if let Some(tx) = m.sync_tx.take() {
                let _ = tx.send(SyncOutcome { error: code, ..Default::default() });
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemberDescription {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct GroupDescription {
    pub group_id: String,
    pub state: GroupState,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<MemberDescription>,
}

pub struct GroupCoordinator {
    groups: Mutex<HashMap<String, Group>>,
    initial_delay: Duration,
    seq: std::sync::atomic::AtomicU64,
}

impl GroupCoordinator {
    pub fn new(initial_delay: Duration) -> Arc<Self> {
        let c = Arc::new(GroupCoordinator {
            groups: Mutex::new(HashMap::new()),
            initial_delay,
            seq: std::sync::atomic::AtomicU64::new(1),
        });
        // Session-timeout reaper (weak so the loop stops when the coordinator is dropped).
        if tokio::runtime::Handle::try_current().is_ok() {
            let weak = Arc::downgrade(&c);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    match weak.upgrade() {
                        Some(c) => c.reap_expired(),
                        None => break,
                    }
                }
            });
        }
        c
    }

    fn new_member_id(&self, client_id: &str) -> String {
        let n = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        format!("{}-{:08x}{:08x}", if client_id.is_empty() { "member" } else { client_id }, nanos, n)
    }

    // ---------------- timers & rebalance transitions ----------------

    fn schedule(self: &Arc<Self>, gid: &str, epoch: u64, delay: Duration, force: bool) {
        let this = self.clone();
        let gid = gid.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            this.on_timer(&gid, epoch, force);
        });
    }

    fn on_timer(self: &Arc<Self>, gid: &str, epoch: u64, force: bool) {
        let mut groups = self.groups.lock().unwrap();
        let g = match groups.get_mut(gid) {
            Some(g) => g,
            None => return,
        };
        if g.epoch != epoch || g.state != GroupState::PreparingRebalance {
            return;
        }
        g.delay_active = false;
        if force || g.all_joined() {
            self.complete_join(g);
        } else {
            // initial delay elapsed but known members have not all rejoined: wait up to the rebalance timeout
            let wait = g.members.values().map(|m| m.rebalance_timeout_ms).max().unwrap_or(60_000).max(1) as u64;
            let (gid, ep) = (g.id.clone(), g.epoch);
            self.schedule(&gid, ep, Duration::from_millis(wait), true);
        }
    }

    fn prepare_rebalance(self: &Arc<Self>, g: &mut Group) {
        if g.state == GroupState::CompletingRebalance {
            g.fail_sync_waiters(REBALANCE_IN_PROGRESS);
        }
        let initial = g.state == GroupState::Empty;
        g.state = GroupState::PreparingRebalance;
        g.epoch += 1;
        for m in g.members.values_mut() {
            m.assignment.clear();
        }
        let (delay, force) = if initial && !self.initial_delay.is_zero() {
            g.delay_active = true;
            (self.initial_delay, false)
        } else {
            g.delay_active = false;
            let max_rt = g.members.values().map(|m| m.rebalance_timeout_ms).max().unwrap_or(60_000).max(1) as u64;
            (Duration::from_millis(max_rt), true)
        };
        let (gid, ep) = (g.id.clone(), g.epoch);
        self.schedule(&gid, ep, delay, force);
    }

    fn try_complete(self: &Arc<Self>, g: &mut Group) {
        if g.state == GroupState::PreparingRebalance && !g.delay_active && g.all_joined() {
            self.complete_join(g);
        }
    }

    fn complete_join(self: &Arc<Self>, g: &mut Group) {
        // Members that did not rejoin in time are dropped.
        let stale: Vec<String> = g.members.iter().filter(|(_, m)| m.join_tx.is_none()).map(|(k, _)| k.clone()).collect();
        for id in stale {
            g.members.remove(&id);
        }
        g.generation += 1;
        if g.members.is_empty() {
            g.state = GroupState::Empty;
            g.protocol = None;
            g.protocol_type = None;
            g.leader = None;
            return;
        }
        g.protocol = g.select_protocol();
        let leader_ok = g.leader.as_ref().map_or(false, |l| g.members.contains_key(l));
        if !leader_ok {
            g.leader = g.members.keys().next().cloned();
        }
        g.state = GroupState::CompletingRebalance;
        let proto = g.protocol.clone().unwrap_or_default();
        let leader = g.leader.clone().unwrap_or_default();
        let member_list: Vec<(String, Option<String>, Vec<u8>)> = g
            .members
            .values()
            .map(|m| {
                let md = m.protocols.iter().find(|p| p.0 == proto).map(|p| p.1.clone()).unwrap_or_default();
                (m.id.clone(), m.instance_id.clone(), md)
            })
            .collect();
        let (generation, ptype, protocol) = (g.generation, g.protocol_type.clone(), g.protocol.clone());
        let now = Instant::now();
        for m in g.members.values_mut() {
            m.last_hb = now;
            if let Some(tx) = m.join_tx.take() {
                let is_leader = m.id == leader;
                let _ = tx.send(JoinOutcome {
                    error: NONE,
                    generation,
                    protocol_type: ptype.clone(),
                    protocol: protocol.clone(),
                    leader: leader.clone(),
                    member_id: m.id.clone(),
                    members: if is_leader { member_list.clone() } else { vec![] },
                });
            }
        }
    }

    fn remove_member(self: &Arc<Self>, g: &mut Group, member_id: &str) {
        if g.members.remove(member_id).is_none() {
            return;
        }
        if g.leader.as_deref() == Some(member_id) {
            g.leader = None;
        }
        match g.state {
            GroupState::Stable | GroupState::CompletingRebalance => {
                if g.members.is_empty() {
                    g.fail_sync_waiters(UNKNOWN_MEMBER_ID);
                    g.state = GroupState::Empty;
                    g.generation += 1;
                    g.protocol = None;
                    g.protocol_type = None;
                } else {
                    self.prepare_rebalance(g);
                }
            }
            GroupState::PreparingRebalance => {
                if g.members.is_empty() {
                    g.state = GroupState::Empty;
                    g.epoch += 1;
                    g.generation += 1;
                    g.protocol = None;
                    g.protocol_type = None;
                } else {
                    self.try_complete(g);
                }
            }
            _ => {}
        }
    }

    fn reap_expired(self: &Arc<Self>) {
        let mut groups = self.groups.lock().unwrap();
        let now = Instant::now();
        for g in groups.values_mut() {
            if !matches!(g.state, GroupState::Stable | GroupState::CompletingRebalance) {
                continue;
            }
            let expired: Vec<String> = g
                .members
                .values()
                .filter(|m| m.join_tx.is_none() && now.duration_since(m.last_hb) > Duration::from_millis(m.session_timeout_ms.max(1) as u64))
                .map(|m| m.id.clone())
                .collect();
            for id in expired {
                self.remove_member(g, &id);
            }
        }
    }

    // ---------------- JoinGroup ----------------

    pub async fn join(self: &Arc<Self>, req: JoinRequest) -> JoinOutcome {
        let rx = {
            let mut groups = self.groups.lock().unwrap();
            self.join_locked(&mut groups, req)
        };
        match rx {
            Ok(rx) => rx.await.unwrap_or_else(|_| JoinOutcome::err(UNKNOWN_MEMBER_ID, "")),
            Err(o) => o,
        }
    }

    fn join_locked(
        self: &Arc<Self>,
        groups: &mut HashMap<String, Group>,
        req: JoinRequest,
    ) -> Result<oneshot::Receiver<JoinOutcome>, JoinOutcome> {
        if req.group_id.is_empty() {
            return Err(JoinOutcome::err(INVALID_GROUP_ID, &req.member_id));
        }
        if req.protocols.is_empty() || req.protocol_type.is_empty() {
            return Err(JoinOutcome::err(INCONSISTENT_GROUP_PROTOCOL, &req.member_id));
        }
        let g = groups.entry(req.group_id.clone()).or_insert_with(|| Group::new(&req.group_id));
        if g.state == GroupState::Dead {
            return Err(JoinOutcome::err(COORDINATOR_NOT_AVAILABLE, &req.member_id));
        }
        if !g.members.is_empty() {
            if g.protocol_type.as_deref() != Some(req.protocol_type.as_str()) {
                return Err(JoinOutcome::err(INCONSISTENT_GROUP_PROTOCOL, &req.member_id));
            }
            // At least one protocol must be supported by every member (other than this one).
            let ok = g
                .members
                .values()
                .filter(|m| m.id != req.member_id)
                .all(|m| m.protocols.iter().any(|p| req.protocols.iter().any(|q| q.0 == p.0)))
                || g.members.len() == 1 && g.members.contains_key(&req.member_id);
            if !ok {
                return Err(JoinOutcome::err(INCONSISTENT_GROUP_PROTOCOL, &req.member_id));
            }
        }

        // Resolve member id (static membership maps a known instance id back to its member).
        let mut member_id = req.member_id.clone();
        if member_id.is_empty() {
            if let Some(inst) = &req.instance_id {
                if let Some(m) = g.members.values().find(|m| m.instance_id.as_ref() == Some(inst)) {
                    member_id = m.id.clone();
                }
            }
        }
        let is_new = member_id.is_empty();
        if is_new {
            member_id = self.new_member_id(&req.client_id);
        } else if !g.members.contains_key(&member_id) {
            return Err(JoinOutcome::err(UNKNOWN_MEMBER_ID, &member_id));
        }

        let (tx, rx) = oneshot::channel();
        let protocols_changed = g.members.get(&member_id).map_or(true, |m| m.protocols != req.protocols);
        if g.members.is_empty() {
            g.protocol_type = Some(req.protocol_type.clone());
        }
        let member = g.members.entry(member_id.clone()).or_insert_with(|| Member {
            id: member_id.clone(),
            instance_id: req.instance_id.clone(),
            client_id: req.client_id.clone(),
            client_host: req.client_host.clone(),
            session_timeout_ms: req.session_timeout_ms,
            rebalance_timeout_ms: req.rebalance_timeout_ms,
            protocols: req.protocols.clone(),
            assignment: vec![],
            last_hb: Instant::now(),
            join_tx: None,
            sync_tx: None,
        });
        member.client_id = req.client_id.clone();
        member.client_host = req.client_host.clone();
        member.session_timeout_ms = req.session_timeout_ms;
        member.rebalance_timeout_ms = req.rebalance_timeout_ms;
        member.protocols = req.protocols.clone();
        member.last_hb = Instant::now();
        // a previous pending join from the same member is superseded
        if let Some(old) = member.join_tx.take() {
            let _ = old.send(JoinOutcome::err(REBALANCE_IN_PROGRESS, &member_id));
        }

        match g.state {
            GroupState::Stable if !is_new && !protocols_changed && g.leader.as_deref() != Some(member_id.as_str()) => {
                // Non-leader rejoining with unchanged metadata: answer with the current generation, no rebalance.
                let proto = g.protocol.clone();
                let out = JoinOutcome {
                    error: NONE,
                    generation: g.generation,
                    protocol_type: g.protocol_type.clone(),
                    protocol: proto,
                    leader: g.leader.clone().unwrap_or_default(),
                    member_id: member_id.clone(),
                    members: vec![],
                };
                let _ = tx.send(out);
                return Ok(rx);
            }
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance => {
                g.members.get_mut(&member_id).unwrap().join_tx = Some(tx);
                self.prepare_rebalance(g);
            }
            GroupState::PreparingRebalance => {
                g.members.get_mut(&member_id).unwrap().join_tx = Some(tx);
            }
            GroupState::Dead => unreachable!(),
        }
        self.try_complete(g);
        Ok(rx)
    }

    // ---------------- SyncGroup ----------------

    pub async fn sync(
        self: &Arc<Self>,
        group_id: &str,
        member_id: &str,
        generation: i32,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncOutcome {
        let rx = {
            let mut groups = self.groups.lock().unwrap();
            let g = match groups.get_mut(group_id) {
                Some(g) => g,
                None => return SyncOutcome { error: UNKNOWN_MEMBER_ID, ..Default::default() },
            };
            if !g.members.contains_key(member_id) {
                return SyncOutcome { error: UNKNOWN_MEMBER_ID, ..Default::default() };
            }
            if generation != g.generation {
                return SyncOutcome { error: ILLEGAL_GENERATION, ..Default::default() };
            }
            if protocol_type.map_or(false, |t| Some(t) != g.protocol_type.as_deref())
                || protocol_name.map_or(false, |n| Some(n) != g.protocol.as_deref())
            {
                return SyncOutcome { error: INCONSISTENT_GROUP_PROTOCOL, ..Default::default() };
            }
            match g.state {
                GroupState::Empty | GroupState::Dead => return SyncOutcome { error: UNKNOWN_MEMBER_ID, ..Default::default() },
                GroupState::PreparingRebalance => return SyncOutcome { error: REBALANCE_IN_PROGRESS, ..Default::default() },
                GroupState::Stable => {
                    let m = g.members.get_mut(member_id).unwrap();
                    m.last_hb = Instant::now();
                    return SyncOutcome {
                        error: NONE,
                        protocol_type: g.protocol_type.clone(),
                        protocol: g.protocol.clone(),
                        assignment: m.assignment.clone(),
                    };
                }
                GroupState::CompletingRebalance => {}
            }
            let (tx, rx) = oneshot::channel();
            g.members.get_mut(member_id).unwrap().sync_tx = Some(tx);
            if g.leader.as_deref() == Some(member_id) {
                for (mid, data) in assignments {
                    if let Some(m) = g.members.get_mut(&mid) {
                        m.assignment = data;
                    }
                }
                g.state = GroupState::Stable;
                let (pt, pn) = (g.protocol_type.clone(), g.protocol.clone());
                let now = Instant::now();
                for m in g.members.values_mut() {
                    m.last_hb = now;
                    if let Some(tx) = m.sync_tx.take() {
                        let _ = tx.send(SyncOutcome {
                            error: NONE,
                            protocol_type: pt.clone(),
                            protocol: pn.clone(),
                            assignment: m.assignment.clone(),
                        });
                    }
                }
            }
            rx
        };
        rx.await.unwrap_or(SyncOutcome { error: UNKNOWN_MEMBER_ID, ..Default::default() })
    }

    // ---------------- Heartbeat / Leave ----------------

    pub fn heartbeat(self: &Arc<Self>, group_id: &str, member_id: &str, generation: i32) -> i16 {
        let mut groups = self.groups.lock().unwrap();
        let g = match groups.get_mut(group_id) {
            Some(g) => g,
            None => return UNKNOWN_MEMBER_ID,
        };
        if g.state == GroupState::Dead {
            return COORDINATOR_NOT_AVAILABLE;
        }
        if !g.members.contains_key(member_id) {
            return UNKNOWN_MEMBER_ID;
        }
        if generation != g.generation {
            return ILLEGAL_GENERATION;
        }
        match g.state {
            GroupState::Empty => UNKNOWN_MEMBER_ID,
            GroupState::PreparingRebalance => {
                g.members.get_mut(member_id).unwrap().last_hb = Instant::now();
                REBALANCE_IN_PROGRESS
            }
            _ => {
                g.members.get_mut(member_id).unwrap().last_hb = Instant::now();
                NONE
            }
        }
    }

    /// Remove a member (by member id, or static instance id when `member_id` is empty). Returns an error code.
    pub fn leave(self: &Arc<Self>, group_id: &str, member_id: &str, instance_id: Option<&str>) -> i16 {
        let mut groups = self.groups.lock().unwrap();
        let g = match groups.get_mut(group_id) {
            Some(g) => g,
            None => return UNKNOWN_MEMBER_ID,
        };
        let id = if member_id.is_empty() {
            match instance_id.and_then(|i| g.members.values().find(|m| m.instance_id.as_deref() == Some(i))) {
                Some(m) => m.id.clone(),
                None => return UNKNOWN_MEMBER_ID,
            }
        } else {
            member_id.to_string()
        };
        if !g.members.contains_key(&id) {
            return UNKNOWN_MEMBER_ID;
        }
        self.remove_member(g, &id);
        NONE
    }

    // ---------------- Offsets ----------------

    /// Validate an OffsetCommit against group membership and record the offsets.
    pub fn commit_offsets(
        self: &Arc<Self>,
        group_id: &str,
        member_id: &str,
        generation: i32,
        offsets: &[(String, i32, OffsetEntry)],
    ) -> i16 {
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(group_id.to_string()).or_insert_with(|| Group::new(group_id));
        if g.state == GroupState::Dead {
            return COORDINATOR_NOT_AVAILABLE;
        }
        if generation < 0 && member_id.is_empty() {
            // Standalone / admin commit: only allowed while the group has no active members.
            if !g.members.is_empty() {
                return UNKNOWN_MEMBER_ID;
            }
        } else {
            if !g.members.contains_key(member_id) {
                return UNKNOWN_MEMBER_ID;
            }
            if generation != g.generation {
                return ILLEGAL_GENERATION;
            }
            if matches!(g.state, GroupState::PreparingRebalance | GroupState::CompletingRebalance) {
                return REBALANCE_IN_PROGRESS;
            }
            if let Some(m) = g.members.get_mut(member_id) {
                m.last_hb = Instant::now();
            }
        }
        for (t, p, e) in offsets {
            g.offsets.insert((t.clone(), *p), e.clone());
        }
        NONE
    }

    /// Cache offsets learned from the controller (does not override newer local commits).
    pub fn cache_offsets(&self, group_id: &str, offsets: Vec<(String, i32, i64)>) {
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry(group_id.to_string()).or_insert_with(|| Group::new(group_id));
        for (t, p, o) in offsets {
            if o >= 0 {
                g.offsets.entry((t, p)).or_insert(OffsetEntry { offset: o, leader_epoch: -1, metadata: None, commit_ms: 0 });
            }
        }
    }

    pub fn get_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<OffsetEntry> {
        let groups = self.groups.lock().unwrap();
        groups.get(group_id)?.offsets.get(&(topic.to_string(), partition)).cloned()
    }

    /// All committed offsets of a group as (topic, partition, entry), sorted.
    pub fn all_offsets(&self, group_id: &str) -> Vec<(String, i32, OffsetEntry)> {
        let groups = self.groups.lock().unwrap();
        let mut v: Vec<_> = groups
            .get(group_id)
            .map(|g| g.offsets.iter().map(|((t, p), e)| (t.clone(), *p, e.clone())).collect())
            .unwrap_or_default();
        v.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        v
    }

    pub fn group_state(&self, group_id: &str) -> Option<GroupState> {
        self.groups.lock().unwrap().get(group_id).map(|g| g.state)
    }

    // ---------------- Describe / List / Delete ----------------

    pub fn describe(&self, group_id: &str) -> GroupDescription {
        let groups = self.groups.lock().unwrap();
        match groups.get(group_id) {
            None => GroupDescription {
                group_id: group_id.to_string(),
                state: GroupState::Dead,
                protocol_type: String::new(),
                protocol: String::new(),
                members: vec![],
            },
            Some(g) => {
                let proto = g.protocol.clone().unwrap_or_default();
                GroupDescription {
                    group_id: g.id.clone(),
                    state: g.state,
                    protocol_type: g.protocol_type.clone().unwrap_or_default(),
                    protocol: proto.clone(),
                    members: g
                        .members
                        .values()
                        .map(|m| MemberDescription {
                            member_id: m.id.clone(),
                            instance_id: m.instance_id.clone(),
                            client_id: m.client_id.clone(),
                            client_host: m.client_host.clone(),
                            metadata: m.protocols.iter().find(|p| p.0 == proto).map(|p| p.1.clone()).unwrap_or_default(),
                            assignment: m.assignment.clone(),
                        })
                        .collect(),
                }
            }
        }
    }

    /// (group id, protocol type, state) for every known group, sorted by id.
    pub fn list(&self) -> Vec<(String, String, GroupState)> {
        let groups = self.groups.lock().unwrap();
        let mut v: Vec<_> = groups
            .values()
            .filter(|g| !(g.state == GroupState::Empty && g.offsets.is_empty() && g.protocol_type.is_none() && g.generation == 0))
            .map(|g| (g.id.clone(), g.protocol_type.clone().unwrap_or_default(), g.state))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// Delete an empty group (and its committed offsets).
    pub fn delete_group(&self, group_id: &str) -> i16 {
        let mut groups = self.groups.lock().unwrap();
        match groups.get(group_id) {
            None => GROUP_ID_NOT_FOUND,
            Some(g) if !g.members.is_empty() || g.state != GroupState::Empty => NON_EMPTY_GROUP,
            Some(_) => {
                groups.remove(group_id);
                NONE
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jr(group: &str, member: &str, protos: &[(&str, &[u8])]) -> JoinRequest {
        JoinRequest {
            group_id: group.into(),
            member_id: member.into(),
            instance_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 2_000,
            protocol_type: "consumer".into(),
            protocols: protos.iter().map(|(n, m)| (n.to_string(), m.to_vec())).collect(),
        }
    }

    #[tokio::test]
    async fn single_member_join_sync_heartbeat_leave() {
        let c = GroupCoordinator::new(Duration::ZERO);
        let j = c.join(jr("g", "", &[("range", b"m1")])).await;
        assert_eq!(j.error, NONE);
        assert_eq!(j.generation, 1);
        assert_eq!(j.leader, j.member_id);
        assert_eq!(j.members.len(), 1);
        assert_eq!(j.protocol.as_deref(), Some("range"));
        assert_eq!(c.group_state("g"), Some(GroupState::CompletingRebalance));

        let s = c.sync("g", &j.member_id, 1, None, None, vec![(j.member_id.clone(), b"assign".to_vec())]).await;
        assert_eq!(s.error, NONE);
        assert_eq!(s.assignment, b"assign");
        assert_eq!(c.group_state("g"), Some(GroupState::Stable));

        assert_eq!(c.heartbeat("g", &j.member_id, 1), NONE);
        assert_eq!(c.heartbeat("g", &j.member_id, 7), ILLEGAL_GENERATION);
        assert_eq!(c.heartbeat("g", "nobody", 1), UNKNOWN_MEMBER_ID);

        assert_eq!(c.leave("g", &j.member_id, None), NONE);
        assert_eq!(c.group_state("g"), Some(GroupState::Empty));
    }

    #[tokio::test]
    async fn two_members_rebalance_and_sync_via_leader() {
        let c = GroupCoordinator::new(Duration::from_millis(50));
        let c1 = c.clone();
        let c2 = c.clone();
        let t1 = tokio::spawn(async move { c1.join(jr("g2", "", &[("range", b"a"), ("roundrobin", b"a")])).await });
        let t2 = tokio::spawn(async move { c2.join(jr("g2", "", &[("roundrobin", b"b")])).await });
        let (j1, j2) = (t1.await.unwrap(), t2.await.unwrap());
        assert_eq!((j1.error, j2.error), (NONE, NONE));
        assert_eq!(j1.generation, 1);
        assert_eq!(j1.leader, j2.leader);
        // only protocol common to both
        assert_eq!(j1.protocol.as_deref(), Some("roundrobin"));
        let (leader, follower) = if j1.member_id == j1.leader { (&j1, &j2) } else { (&j2, &j1) };
        assert_eq!(leader.members.len(), 2);
        assert!(follower.members.is_empty());

        // follower syncs first (blocks), leader then delivers assignments
        let cf = c.clone();
        let fid = follower.member_id.clone();
        let ft = tokio::spawn(async move { cf.sync("g2", &fid, 1, None, None, vec![]).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let ls = c
            .sync(
                "g2",
                &leader.member_id,
                1,
                Some("consumer"),
                Some("roundrobin"),
                vec![(leader.member_id.clone(), b"L".to_vec()), (follower.member_id.clone(), b"F".to_vec())],
            )
            .await;
        assert_eq!(ls.assignment, b"L");
        let fs = ft.await.unwrap();
        assert_eq!(fs.error, NONE);
        assert_eq!(fs.assignment, b"F");

        // follower leaves -> group must rebalance; leader's heartbeat gets REBALANCE_IN_PROGRESS
        assert_eq!(c.leave("g2", &follower.member_id, None), NONE);
        assert_eq!(c.heartbeat("g2", &leader.member_id, 1), REBALANCE_IN_PROGRESS);
        // leader rejoins -> new generation with sole member
        let j = c.join(jr("g2", &leader.member_id, &[("roundrobin", b"a")])).await;
        assert_eq!(j.error, NONE);
        assert_eq!(j.generation, 2);
        assert_eq!(j.members.len(), 1);
    }

    #[tokio::test]
    async fn late_joiner_triggers_rebalance_of_stable_group() {
        let c = GroupCoordinator::new(Duration::ZERO);
        let j1 = c.join(jr("g3", "", &[("range", b"a")])).await;
        c.sync("g3", &j1.member_id, 1, None, None, vec![(j1.member_id.clone(), b"x".to_vec())]).await;
        // new member joins: existing leader must rejoin for the round to complete
        let cn = c.clone();
        let newcomer = tokio::spawn(async move { cn.join(jr("g3", "", &[("range", b"b")])).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(c.group_state("g3"), Some(GroupState::PreparingRebalance));
        assert_eq!(c.heartbeat("g3", &j1.member_id, 1), REBALANCE_IN_PROGRESS);
        let j1b = c.join(jr("g3", &j1.member_id, &[("range", b"a")])).await;
        let jn = newcomer.await.unwrap();
        assert_eq!(j1b.generation, 2);
        assert_eq!(jn.generation, 2);
        assert_eq!(j1b.leader, jn.leader);
    }

    #[tokio::test]
    async fn rebalance_timeout_evicts_silent_member() {
        let c = GroupCoordinator::new(Duration::ZERO);
        let mut r = jr("g4", "", &[("range", b"a")]);
        r.rebalance_timeout_ms = 150;
        let j1 = c.join(r.clone()).await;
        c.sync("g4", &j1.member_id, 1, None, None, vec![(j1.member_id.clone(), vec![])]).await;
        let mut r2 = r.clone();
        r2.protocols = vec![("range".into(), b"b".to_vec())];
        let cn = c.clone();
        // second member joins, first never rejoins -> evicted after rebalance timeout
        let jn = cn.join(r2).await;
        assert_eq!(jn.error, NONE);
        assert_eq!(jn.generation, 2);
        let d = c.describe("g4");
        assert_eq!(d.members.len(), 1);
        assert_eq!(d.members[0].member_id, jn.member_id);
    }

    #[tokio::test]
    async fn session_timeout_expires_member() {
        let c = GroupCoordinator::new(Duration::ZERO);
        let mut r = jr("g5", "", &[("range", b"a")]);
        r.session_timeout_ms = 300;
        let j = c.join(r).await;
        c.sync("g5", &j.member_id, 1, None, None, vec![(j.member_id.clone(), vec![])]).await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert_eq!(c.group_state("g5"), Some(GroupState::Empty));
        assert_eq!(c.heartbeat("g5", &j.member_id, 1), UNKNOWN_MEMBER_ID);
    }

    #[tokio::test]
    async fn error_paths() {
        let c = GroupCoordinator::new(Duration::ZERO);
        assert_eq!(c.join(jr("", "", &[("r", b"")])).await.error, INVALID_GROUP_ID);
        assert_eq!(c.join(jr("g6", "ghost", &[("r", b"")])).await.error, UNKNOWN_MEMBER_ID);
        let j = c.join(jr("g6", "", &[("r", b"")])).await;
        let mut other = jr("g6", "", &[("r", b"")]);
        other.protocol_type = "connect".into();
        assert_eq!(c.join(other).await.error, INCONSISTENT_GROUP_PROTOCOL);
        assert_eq!(c.sync("g6", &j.member_id, 99, None, None, vec![]).await.error, ILLEGAL_GENERATION);
        assert_eq!(c.sync("nogroup", "x", 1, None, None, vec![]).await.error, UNKNOWN_MEMBER_ID);
        assert_eq!(c.leave("g6", "ghost", None), UNKNOWN_MEMBER_ID);
    }

    #[tokio::test]
    async fn offsets_commit_validation_and_delete() {
        let c = GroupCoordinator::new(Duration::ZERO);
        let e = |o| OffsetEntry { offset: o, leader_epoch: -1, metadata: None, commit_ms: 1 };
        // standalone commit on unknown group is allowed
        assert_eq!(c.commit_offsets("og", "", -1, &[("t".into(), 0, e(5))]), NONE);
        assert_eq!(c.get_offset("og", "t", 0).unwrap().offset, 5);
        assert_eq!(c.get_offset("og", "t", 1).map(|x| x.offset), None);
        // member commit needs valid membership
        assert_eq!(c.commit_offsets("og", "m", 1, &[("t".into(), 0, e(6))]), UNKNOWN_MEMBER_ID);
        let j = c.join(jr("og", "", &[("r", b"")])).await;
        assert_eq!(c.commit_offsets("og", &j.member_id, 1, &[("t".into(), 0, e(7))]), REBALANCE_IN_PROGRESS);
        c.sync("og", &j.member_id, 1, None, None, vec![(j.member_id.clone(), vec![])]).await;
        assert_eq!(c.commit_offsets("og", &j.member_id, 1, &[("t".into(), 0, e(7))]), NONE);
        assert_eq!(c.commit_offsets("og", &j.member_id, 5, &[("t".into(), 0, e(8))]), ILLEGAL_GENERATION);
        assert_eq!(c.get_offset("og", "t", 0).unwrap().offset, 7);
        assert_eq!(c.delete_group("og"), NON_EMPTY_GROUP);
        c.leave("og", &j.member_id, None);
        assert_eq!(c.delete_group("og"), NONE);
        assert_eq!(c.delete_group("og"), GROUP_ID_NOT_FOUND);
    }
}
