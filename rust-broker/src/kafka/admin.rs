//! Kafka admin API breadth (stream C / #7).
//!
//! Implements, with proper classic *and* flexible (KIP-482) encodings:
//!   CreateTopics(19) DeleteTopics(20) DescribeConfigs(32) AlterConfigs(33) IncrementalAlterConfigs(44)
//!   CreatePartitions(37) ListOffsets(2) ElectLeaders(43) DescribeCluster(60)
//! and, via `group_api`, the consumer-group APIs (FindCoordinator, JoinGroup, Heartbeat, LeaveGroup,
//! SyncGroup, OffsetCommit, OffsetFetch, DescribeGroups, ListGroups, DeleteGroups).
//!
//! Topic/cluster mutations are mapped onto the controller's gRPC (`Controller` trait). Metadata reads use
//! the broker's `TopologyCache`.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tracing::warn;

use super::codec::{first_flexible_version, CodecResult, Rd, Wr};
use super::group_api;
use super::groups::GroupCoordinator;
use crate::config::BrokerConfig;
use crate::grpc::aeromq;
use crate::log::LogManager;
use crate::topology::{self, Controller, Snapshot, TopologyCache};

// Kafka error codes
pub const E_NONE: i16 = 0;
pub const E_OFFSET_OUT_OF_RANGE: i16 = 1;
pub const E_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const E_INVALID_TOPIC: i16 = 17;
pub const E_INVALID_PARTITIONS: i16 = 37;
pub const E_INVALID_REPLICATION_FACTOR: i16 = 38;
pub const E_INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const E_INVALID_CONFIG: i16 = 40;
pub const E_NOT_CONTROLLER: i16 = 41;
pub const E_INVALID_REQUEST: i16 = 42;
pub const E_TOPIC_ALREADY_EXISTS: i16 = 36;
pub const E_UNKNOWN_SERVER_ERROR: i16 = -1;
pub const E_UNKNOWN_TOPIC_ID: i16 = 100;

/// (api_key, min_version, max_version) for every API implemented in this module tree.
/// Registered in ApiVersions by `net::kafka_server`.
pub const ADMIN_APIS: &[(i16, i16, i16)] = &[
    (2, 0, 7),   // ListOffsets
    (8, 0, 8),   // OffsetCommit
    (9, 0, 7),   // OffsetFetch
    (10, 0, 4),  // FindCoordinator
    (11, 0, 9),  // JoinGroup
    (12, 0, 4),  // Heartbeat
    (13, 0, 5),  // LeaveGroup
    (14, 0, 5),  // SyncGroup
    (15, 0, 5),  // DescribeGroups
    (16, 0, 4),  // ListGroups
    (19, 0, 7),  // CreateTopics
    (20, 0, 6),  // DeleteTopics
    (32, 0, 4),  // DescribeConfigs
    (33, 0, 2),  // AlterConfigs
    (37, 0, 3),  // CreatePartitions
    (42, 0, 2),  // DeleteGroups
    (43, 0, 2),  // ElectLeaders
    (44, 0, 1),  // IncrementalAlterConfigs
    (60, 0, 1),  // DescribeCluster
];

pub fn supported_range(api_key: i16) -> Option<(i16, i16)> {
    ADMIN_APIS.iter().find(|(k, _, _)| *k == api_key).map(|(_, lo, hi)| (*lo, *hi))
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

pub struct AdminState {
    pub cfg: Arc<BrokerConfig>,
    pub log_manager: Arc<LogManager>,
    pub topo: Arc<TopologyCache>,
    pub ctl: Option<Arc<dyn Controller>>,
    pub groups: Arc<GroupCoordinator>,
}

static STATE: OnceLock<Arc<AdminState>> = OnceLock::new();

/// Initialise (once) the shared admin state and start the topology refresh loop.
pub fn init(cfg: &Arc<BrokerConfig>, log_manager: &Arc<LogManager>) -> Arc<AdminState> {
    STATE
        .get_or_init(|| {
            let ctl = topology::grpc_controller(cfg);
            let topo = TopologyCache::global();
            if let Some(c) = &ctl {
                topo.clone().spawn_refresh_loop(c.clone(), Duration::from_secs(2));
            }
            Arc::new(AdminState {
                cfg: cfg.clone(),
                log_manager: log_manager.clone(),
                topo,
                ctl,
                groups: GroupCoordinator::new(Duration::from_millis(cfg.group_initial_rebalance_delay_ms)),
            })
        })
        .clone()
}

impl AdminState {
    pub fn new(
        cfg: Arc<BrokerConfig>,
        log_manager: Arc<LogManager>,
        topo: Arc<TopologyCache>,
        ctl: Option<Arc<dyn Controller>>,
        groups: Arc<GroupCoordinator>,
    ) -> Arc<Self> {
        Arc::new(Self { cfg, log_manager, topo, ctl, groups })
    }

    pub fn my_id(&self) -> i32 {
        self.cfg.id as i32
    }

    /// Refresh the topology cache if older than ~2s (failed attempts are rate limited).
    pub async fn ensure_fresh(&self) {
        if let Some(c) = &self.ctl {
            self.topo.refresh_if_stale(c.as_ref(), Duration::from_secs(2)).await;
        }
    }

    /// Force a refresh (after topology mutations).
    pub async fn refresh_now(&self) {
        if let Some(c) = &self.ctl {
            let _ = self.topo.refresh(c.as_ref()).await;
        }
    }

    /// (node id, host, kafka port) of the coordinator for `key`.
    pub fn coordinator_node(&self, key: &str) -> (i32, String, i32) {
        match self.topo.coordinator_for(key) {
            Some(b) => (b.id, b.host, b.kafka_port),
            None => (self.my_id(), self.cfg.host.clone(), self.cfg.kafka_port),
        }
    }

    pub fn is_coordinator(&self, key: &str) -> bool {
        self.coordinator_node(key).0 == self.my_id()
    }

    pub async fn persist_offsets(&self, group: &str, offsets: Vec<(String, i32, i64)>) {
        if let Some(c) = &self.ctl {
            if let Err(e) = c.commit_offsets(group, offsets).await {
                warn!("[AeroStream Kafka] offset write-through to controller failed for group {}: {}", group, e);
            }
        }
    }

    /// Load committed offsets of `topics` from the controller into the coordinator cache when unknown locally.
    pub async fn hydrate_offsets(&self, group: &str, topics: Vec<String>) {
        let ctl = match &self.ctl {
            Some(c) => c,
            None => return,
        };
        let known: std::collections::HashSet<String> = self.groups.all_offsets(group).into_iter().map(|(t, _, _)| t).collect();
        let missing: Vec<String> = topics.into_iter().filter(|t| !known.contains(t)).collect();
        if missing.is_empty() {
            return;
        }
        if let Ok(offs) = ctl.fetch_offsets(group, missing).await {
            self.groups.cache_offsets(group, offs);
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

fn frame(corr: i32, flex: bool, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.extend_from_slice(&corr.to_be_bytes());
    if flex {
        out.push(0); // response header v1 tagged fields
    }
    out.extend_from_slice(&body);
    out
}

/// Handle one request for an API in `ADMIN_APIS`. `body` starts right after the (classic) client id, i.e.
/// at the header's tagged fields for flexible versions. Returns the full response payload
/// (correlation id + header + body), or `None` when `api_key` is not handled here.
pub async fn dispatch(
    st: &Arc<AdminState>,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    client_id: &str,
    body: &[u8],
) -> Option<Result<Vec<u8>, String>> {
    let (lo, hi) = supported_range(api_key)?;
    if version < lo || version > hi {
        return Some(Err(format!("unsupported version {version} for api {api_key}")));
    }
    let flex = first_flexible_version(api_key).map_or(false, |f| version >= f);
    let mut rd = Rd::new(body, flex);
    if let Err(e) = rd.tagged() {
        return Some(Err(e));
    }
    let res = match api_key {
        2 => list_offsets(st, version, &mut rd).await,
        19 => create_topics(st, version, &mut rd).await,
        20 => delete_topics(st, version, &mut rd).await,
        32 => describe_configs(st, version, &mut rd).await,
        33 => alter_configs(st, version, &mut rd, false).await,
        44 => alter_configs(st, version, &mut rd, true).await,
        37 => create_partitions(st, version, &mut rd).await,
        43 => elect_leaders(st, version, &mut rd).await,
        60 => describe_cluster(st, version, &mut rd).await,
        8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 42 => group_api::handle(st, api_key, version, &mut rd, client_id).await,
        _ => return None,
    };
    Some(res.map(|b| frame(correlation_id, flex, b)))
}

// ---------------------------------------------------------------------------
// Topic config catalogue
// ---------------------------------------------------------------------------

// Kafka ConfigDef types
const T_BOOLEAN: i8 = 1;
const T_STRING: i8 = 2;
const T_INT: i8 = 3;
const T_LONG: i8 = 5;
const T_DOUBLE: i8 = 6;
const T_LIST: i8 = 7;

// Kafka ConfigSource
const SRC_DYNAMIC_TOPIC: i8 = 1;
const SRC_STATIC_BROKER: i8 = 4;
const SRC_DEFAULT: i8 = 5;

/// (name, default value, type) for topic configs, derived from the broker's storage configuration.
pub fn topic_config_defaults(cfg: &BrokerConfig) -> Vec<(&'static str, String, i8)> {
    let long_max = i64::MAX.to_string();
    vec![
        ("cleanup.policy", if cfg.storage.compaction_enabled { "delete".into() } else { "delete".into() }, T_LIST),
        ("compression.type", "producer".into(), T_STRING),
        ("delete.retention.ms", (cfg.storage.tombstone_retention_secs * 1000).to_string(), T_LONG),
        ("file.delete.delay.ms", "60000".into(), T_LONG),
        ("flush.messages", long_max.clone(), T_LONG),
        ("flush.ms", long_max.clone(), T_LONG),
        ("index.interval.bytes", "4096".into(), T_INT),
        ("max.compaction.lag.ms", long_max, T_LONG),
        ("max.message.bytes", "1048588".into(), T_INT),
        ("message.timestamp.type", "CreateTime".into(), T_STRING),
        ("min.cleanable.dirty.ratio", cfg.storage.dirty_ratio_threshold.to_string(), T_DOUBLE),
        ("min.compaction.lag.ms", "0".into(), T_LONG),
        ("min.insync.replicas", "1".into(), T_INT),
        ("preallocate", "false".into(), T_BOOLEAN),
        ("retention.bytes", cfg.storage.max_retention_size.map_or(-1, |v| v as i64).to_string(), T_LONG),
        ("retention.ms", cfg.storage.max_retention_age_secs.map_or(-1, |v| (v * 1000) as i64).to_string(), T_LONG),
        ("segment.bytes", cfg.storage.max_segment_size.to_string(), T_INT),
        ("segment.index.bytes", "10485760".into(), T_INT),
        ("segment.jitter.ms", "0".into(), T_LONG),
        ("segment.ms", "604800000".into(), T_LONG),
        ("unclean.leader.election.enable", "false".into(), T_BOOLEAN),
    ]
}

/// Extra accepted topic config keys (valid in Kafka but without a meaningful default here).
const EXTRA_TOPIC_CONFIG_KEYS: &[&str] = &[
    "message.format.version",
    "message.timestamp.difference.max.ms",
    "message.downconversion.enable",
    "follower.replication.throttled.replicas",
    "leader.replication.throttled.replicas",
    "remote.storage.enable",
    "local.retention.ms",
    "local.retention.bytes",
    "confluent.value.schema.validation",
    "iceberg.enabled",
    "iceberg.catalog",
    "iceberg.table",
];

fn validate_topic_configs(cfg: &BrokerConfig, kv: &[(String, Option<String>)]) -> Option<String> {
    let defs = topic_config_defaults(cfg);
    for (k, v) in kv {
        let known = defs.iter().find(|d| d.0 == k);
        if known.is_none() && !EXTRA_TOPIC_CONFIG_KEYS.contains(&k.as_str()) && !k.starts_with("aerostream.") {
            return Some(format!("Unknown topic config name: {k}"));
        }
        if let (Some(d), Some(v)) = (known, v) {
            let ok = match d.2 {
                T_LONG => v.parse::<i64>().is_ok(),
                T_INT => v.parse::<i32>().is_ok(),
                T_DOUBLE => v.parse::<f64>().map_or(false, |x| (0.0..=1.0).contains(&x) || !k.contains("ratio")),
                T_BOOLEAN => v == "true" || v == "false",
                _ => true,
            };
            if !ok {
                return Some(format!("Invalid value {v} for configuration {k}"));
            }
        }
    }
    None
}

fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

fn topic_uuid(name: &str) -> [u8; 16] {
    fn h(seed: u64, s: &str) -> u64 {
        let mut x = seed;
        for b in s.bytes() {
            x ^= b as u64;
            x = x.wrapping_mul(0x100000001b3);
        }
        x
    }
    let a = h(0xcbf29ce484222325, name).to_be_bytes();
    let b = h(0x84222325cbf29ce4, name).to_be_bytes();
    let mut u = [0u8; 16];
    u[..8].copy_from_slice(&a);
    u[8..].copy_from_slice(&b);
    u
}

/// Map a controller AdminResponse to (error_code, message).
fn admin_result(r: Result<aeromq::AdminResponse, String>) -> (i16, Option<String>) {
    match r {
        Ok(r) if r.success => (E_NONE, None),
        Ok(r) => (if r.error_code != 0 { r.error_code as i16 } else { E_UNKNOWN_SERVER_ERROR }, Some(r.message)),
        Err(e) => (E_NOT_CONTROLLER, Some(format!("controller unavailable: {e}"))),
    }
}

// ---------------------------------------------------------------------------
// CreateTopics (19)
// ---------------------------------------------------------------------------

struct NewTopic {
    name: String,
    partitions: i32,
    rf: i16,
    assignments: Vec<(i32, Vec<i32>)>,
    configs: Vec<(String, Option<String>)>,
}

async fn create_topics(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut topics = Vec::new();
    for _ in 0..rd.arr()? {
        let name = rd.str()?;
        let partitions = rd.i32()?;
        let rf = rd.i16()?;
        let mut assignments = Vec::new();
        for _ in 0..rd.arr()? {
            let p = rd.i32()?;
            let mut ids = Vec::new();
            for _ in 0..rd.arr()? {
                ids.push(rd.i32()?);
            }
            rd.tagged()?;
            assignments.push((p, ids));
        }
        let mut configs = Vec::new();
        for _ in 0..rd.arr()? {
            let k = rd.str()?;
            let val = rd.nstr()?;
            rd.tagged()?;
            configs.push((k, val));
        }
        rd.tagged()?;
        topics.push(NewTopic { name, partitions, rf, assignments, configs });
    }
    let _timeout = rd.i32()?;
    let validate_only = if v >= 1 { rd.bool()? } else { false };
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let mut results: Vec<(String, i16, Option<String>, i32, i16, Vec<(String, Option<String>)>)> = Vec::new();
    let mut mutated = false;
    let mut seen = std::collections::HashSet::new();

    for t in topics {
        let (code, msg) = validate_new_topic(st, &snap, &t, &mut seen);
        let (mut code, mut msg) = (code, msg);
        let (mut np, mut rf) = (t.partitions, t.rf);
        if !t.assignments.is_empty() {
            np = t.assignments.len() as i32;
            rf = t.assignments[0].1.len() as i16;
        } else {
            if np == -1 {
                np = 1;
            }
            if rf == -1 {
                rf = 1;
            }
        }
        if code == E_NONE && !validate_only {
            match &st.ctl {
                None => {
                    code = E_NOT_CONTROLLER;
                    msg = Some("no controller connection".into());
                }
                Some(c) => {
                    let req = aeromq::CreateTopicRequest {
                        topic: t.name.clone(),
                        partitions: np as u32,
                        replication_factor: rf as u32,
                        configs: t.configs.iter().filter_map(|(k, v)| v.clone().map(|v| (k.clone(), v))).collect(),
                        manual_assignments: t
                            .assignments
                            .iter()
                            .map(|(p, ids)| aeromq::ReplicaAssignment {
                                partition: *p as u32,
                                broker_ids: ids.iter().map(|&i| i as u32).collect(),
                            })
                            .collect(),
                        validate_only: false,
                        fail_if_exists: true,
                    };
                    match c.create_topic(req).await {
                        Ok(r) if r.success => mutated = true,
                        Ok(r) => {
                            code = if r.error_code != 0 { r.error_code as i16 } else { E_UNKNOWN_SERVER_ERROR };
                            msg = Some(r.message);
                        }
                        Err(e) => {
                            code = E_NOT_CONTROLLER;
                            msg = Some(format!("controller unavailable: {e}"));
                        }
                    }
                }
            }
        }
        if code != E_NONE {
            np = -1;
            rf = -1;
        }
        results.push((t.name, code, msg, np, rf, if code == E_NONE { t.configs } else { vec![] }));
    }
    if mutated {
        st.refresh_now().await;
    }

    let mut w = Wr::new(rd.flex);
    if v >= 2 {
        w.i32(0);
    }
    w.arr(results.len());
    for (name, code, msg, np, rf, configs) in &results {
        w.str(name);
        if v >= 7 {
            w.uuid(&topic_uuid(name));
        }
        w.i16(*code);
        if v >= 1 {
            w.nstr(msg.as_deref());
        }
        if v >= 5 {
            w.i32(*np).i16(*rf);
            if *code == E_NONE {
                w.arr(configs.len());
                for (k, val) in configs {
                    w.str(k).nstr(val.as_deref()).bool(false).i8(SRC_DYNAMIC_TOPIC).bool(false).tagged();
                }
            } else {
                w.null_arr();
            }
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

fn validate_new_topic(
    st: &AdminState,
    snap: &Snapshot,
    t: &NewTopic,
    seen: &mut std::collections::HashSet<String>,
) -> (i16, Option<String>) {
    if !valid_topic_name(&t.name) {
        return (E_INVALID_TOPIC, Some(format!("Topic name is invalid: '{}'", t.name)));
    }
    if !seen.insert(t.name.clone()) {
        return (E_INVALID_REQUEST, Some(format!("Duplicate topic name: {}", t.name)));
    }
    if !t.assignments.is_empty() {
        if t.partitions != -1 || t.rf != -1 {
            return (
                E_INVALID_REQUEST,
                Some("Both numPartitions or replicationFactor and replicasAssignments were set. Both cannot be used at the same time.".into()),
            );
        }
        let mut idx: Vec<i32> = t.assignments.iter().map(|a| a.0).collect();
        idx.sort();
        if idx.iter().enumerate().any(|(i, p)| *p != i as i32) {
            return (E_INVALID_REPLICA_ASSIGNMENT, Some("Partitions in replica assignments must be contiguous starting at 0".into()));
        }
        let rf0 = t.assignments[0].1.len();
        for (_, ids) in &t.assignments {
            let mut u = ids.clone();
            u.sort();
            u.dedup();
            if ids.is_empty() || ids.len() != rf0 || u.len() != ids.len() {
                return (E_INVALID_REPLICA_ASSIGNMENT, Some("Invalid replica assignment (empty, duplicate or non-uniform replicas)".into()));
            }
            if !snap.brokers.is_empty() && ids.iter().any(|i| !snap.brokers.contains_key(i)) {
                return (E_INVALID_REPLICA_ASSIGNMENT, Some("Replica assignment references unknown broker".into()));
            }
        }
    } else {
        if t.partitions == 0 || t.partitions < -1 {
            return (E_INVALID_PARTITIONS, Some("Number of partitions must be larger than 0.".into()));
        }
        if t.rf == 0 || t.rf < -1 {
            return (E_INVALID_REPLICATION_FACTOR, Some("Replication factor must be larger than 0.".into()));
        }
        if t.rf > 0 && !snap.brokers.is_empty() && t.rf as usize > snap.brokers.len() {
            return (
                E_INVALID_REPLICATION_FACTOR,
                Some(format!("Replication factor: {} larger than available brokers: {}.", t.rf, snap.brokers.len())),
            );
        }
    }
    if let Some(m) = validate_topic_configs(&st.cfg, &t.configs) {
        return (E_INVALID_CONFIG, Some(m));
    }
    if snap.topics.contains_key(&t.name) {
        return (E_TOPIC_ALREADY_EXISTS, Some(format!("Topic '{}' already exists.", t.name)));
    }
    (E_NONE, None)
}

// ---------------------------------------------------------------------------
// DeleteTopics (20)
// ---------------------------------------------------------------------------

async fn delete_topics(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut targets: Vec<(Option<String>, [u8; 16])> = Vec::new();
    if v >= 6 {
        for _ in 0..rd.arr()? {
            let name = rd.nstr()?;
            let id = rd.uuid()?;
            rd.tagged()?;
            targets.push((name, id));
        }
    } else {
        for _ in 0..rd.arr()? {
            targets.push((Some(rd.str()?), [0; 16]));
        }
    }
    let _timeout = rd.i32()?;
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let mut results = Vec::new();
    let mut mutated = false;
    for (name, id) in targets {
        let resolved = match &name {
            Some(n) => Some(n.clone()),
            None => snap.topics.keys().find(|k| topic_uuid(k) == id).cloned(),
        };
        let (code, msg) = match &resolved {
            None => (E_UNKNOWN_TOPIC_ID, Some("This server does not host this topic ID.".to_string())),
            Some(n) if !valid_topic_name(n) => (E_INVALID_TOPIC, Some("Invalid topic name".to_string())),
            Some(n) => match &st.ctl {
                None => (E_NOT_CONTROLLER, Some("no controller connection".to_string())),
                Some(c) => {
                    let r = admin_result(c.delete_topic(n.clone()).await);
                    if r.0 == E_NONE {
                        mutated = true;
                        // Remove this broker's local logs for the topic (other brokers keep theirs until purged).
                        if let Err(e) = st.log_manager.delete_topic(n).await {
                            warn!("[AeroStream Kafka] failed to remove local logs of deleted topic {}: {}", n, e);
                        }
                    }
                    r
                }
            },
        };
        let out_id = resolved.as_deref().map(topic_uuid).unwrap_or(id);
        results.push((name.or(resolved), out_id, code, msg));
    }
    if mutated {
        st.refresh_now().await;
    }

    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    w.arr(results.len());
    for (name, id, code, msg) in &results {
        w.nstr(name.as_deref());
        if v >= 6 {
            w.uuid(id);
        }
        w.i16(*code);
        if v >= 5 {
            w.nstr(msg.as_deref());
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// DescribeConfigs (32)
// ---------------------------------------------------------------------------

const RES_TOPIC: i8 = 2;
const RES_BROKER: i8 = 4;
const RES_BROKER_LOGGER: i8 = 8;

struct ConfigEntry {
    name: String,
    value: Option<String>,
    read_only: bool,
    is_default: bool,
    source: i8,
    ty: i8,
}

fn broker_configs(st: &AdminState) -> Vec<ConfigEntry> {
    let c = &st.cfg;
    let mk = |n: &str, v: String, ty: i8| ConfigEntry { name: n.into(), value: Some(v), read_only: true, is_default: false, source: SRC_STATIC_BROKER, ty };
    let mut v = vec![
        mk("broker.id", c.id.to_string(), T_INT),
        mk("advertised.listeners", format!("PLAINTEXT://{}:{}", c.host, c.kafka_port), T_LIST),
        mk("log.segment.bytes", c.storage.max_segment_size.to_string(), T_INT),
        mk("log.retention.bytes", c.storage.max_retention_size.map_or(-1, |x| x as i64).to_string(), T_LONG),
        mk("log.retention.ms", c.storage.max_retention_age_secs.map_or(-1, |x| (x * 1000) as i64).to_string(), T_LONG),
        mk("log.cleaner.enable", c.storage.compaction_enabled.to_string(), T_BOOLEAN),
        mk("log.cleaner.min.cleanable.ratio", c.storage.dirty_ratio_threshold.to_string(), T_DOUBLE),
        mk("group.initial.rebalance.delay.ms", c.group_initial_rebalance_delay_ms.to_string(), T_INT),
        mk("replica.selector.class", c.replica_selector.clone(), T_STRING),
    ];
    if let Some(r) = &c.rack {
        v.push(mk("broker.rack", r.clone(), T_STRING));
    }
    v
}

async fn describe_configs(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    struct Res {
        ty: i8,
        name: String,
        keys: Option<Vec<String>>,
    }
    let mut resources = Vec::new();
    for _ in 0..rd.arr()? {
        let ty = rd.i8()?;
        let name = rd.str()?;
        let keys = match rd.narr()? {
            None => None,
            Some(n) => {
                let mut k = Vec::new();
                for _ in 0..n {
                    k.push(rd.str()?);
                }
                Some(k)
            }
        };
        rd.tagged()?;
        resources.push(Res { ty, name, keys });
    }
    let include_synonyms = if v >= 1 { rd.bool()? } else { false };
    let _include_doc = if v >= 3 { rd.bool()? } else { false };
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();

    let mut w = Wr::new(rd.flex);
    w.i32(0);
    w.arr(resources.len());
    for r in resources {
        let (code, msg, entries): (i16, Option<String>, Vec<ConfigEntry>) = match r.ty {
            RES_TOPIC => match snap.topics.get(&r.name) {
                None => (E_UNKNOWN_TOPIC_OR_PARTITION, Some(format!("Topic '{}' not found.", r.name)), vec![]),
                Some(ti) => {
                    let mut entries: Vec<ConfigEntry> = topic_config_defaults(&st.cfg)
                        .into_iter()
                        .map(|(k, dv, ty)| match ti.configs.get(k) {
                            Some(ov) => ConfigEntry { name: k.into(), value: Some(ov.clone()), read_only: false, is_default: false, source: SRC_DYNAMIC_TOPIC, ty },
                            None => ConfigEntry { name: k.into(), value: Some(dv), read_only: false, is_default: true, source: SRC_DEFAULT, ty },
                        })
                        .collect();
                    for (k, ov) in &ti.configs {
                        if !entries.iter().any(|e| &e.name == k) {
                            entries.push(ConfigEntry { name: k.clone(), value: Some(ov.clone()), read_only: false, is_default: false, source: SRC_DYNAMIC_TOPIC, ty: T_STRING });
                        }
                    }
                    (E_NONE, None, entries)
                }
            },
            RES_BROKER => {
                if r.name.is_empty() || r.name == st.my_id().to_string() {
                    (E_NONE, None, broker_configs(st))
                } else {
                    (E_INVALID_REQUEST, Some(format!("Broker {} is not this broker; send the request to that broker.", r.name)), vec![])
                }
            }
            RES_BROKER_LOGGER => (E_NONE, None, vec![]),
            other => (E_INVALID_REQUEST, Some(format!("Unsupported resource type {other}")), vec![]),
        };
        let entries: Vec<ConfigEntry> = match &r.keys {
            Some(keys) => entries.into_iter().filter(|e| keys.contains(&e.name)).collect(),
            None => entries,
        };
        w.i16(code).nstr(msg.as_deref()).i8(r.ty).str(&r.name).arr(entries.len());
        for e in &entries {
            w.str(&e.name).nstr(e.value.as_deref()).bool(e.read_only);
            if v == 0 {
                w.bool(e.is_default);
            } else {
                w.i8(e.source);
            }
            w.bool(false); // is_sensitive
            if v >= 1 {
                if include_synonyms {
                    // one synonym describing where the effective value comes from
                    w.arr(1).str(&e.name).nstr(e.value.as_deref()).i8(e.source).tagged();
                } else {
                    w.arr(0);
                }
            }
            if v >= 3 {
                w.i8(e.ty).nstr(None);
            }
            w.tagged();
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// AlterConfigs (33) / IncrementalAlterConfigs (44)
// ---------------------------------------------------------------------------

async fn alter_configs(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>, incremental: bool) -> CodecResult<Vec<u8>> {
    struct Op {
        name: String,
        value: Option<String>,
        op: i8, // 0 SET, 1 DELETE, 2 APPEND, 3 SUBTRACT
    }
    struct Res {
        ty: i8,
        name: String,
        ops: Vec<Op>,
    }
    let mut resources = Vec::new();
    for _ in 0..rd.arr()? {
        let ty = rd.i8()?;
        let name = rd.str()?;
        let mut ops = Vec::new();
        for _ in 0..rd.arr()? {
            let k = rd.str()?;
            let op = if incremental { rd.i8()? } else { 0 };
            let val = rd.nstr()?;
            rd.tagged()?;
            ops.push(Op { name: k, value: val, op });
        }
        rd.tagged()?;
        resources.push(Res { ty, name, ops });
    }
    let validate_only = rd.bool()?;
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let mut out = Vec::new();
    let mut mutated = false;
    for r in resources {
        let (code, msg) = match r.ty {
            RES_TOPIC => {
                let kv: Vec<(String, Option<String>)> = r.ops.iter().map(|o| (o.name.clone(), o.value.clone())).collect();
                if !valid_topic_name(&r.name) {
                    (E_INVALID_TOPIC, Some("Invalid topic name".to_string()))
                } else if let Some(m) = validate_topic_configs(&st.cfg, &kv) {
                    (E_INVALID_CONFIG, Some(m))
                } else if r.ops.iter().any(|o| o.op > 1) {
                    (E_INVALID_REQUEST, Some("APPEND/SUBTRACT operations are not supported for these configs".to_string()))
                } else if !snap.topics.contains_key(&r.name) && !snap.topics.is_empty() {
                    (E_UNKNOWN_TOPIC_OR_PARTITION, Some(format!("Topic '{}' not found.", r.name)))
                } else {
                    match &st.ctl {
                        None => (E_NOT_CONTROLLER, Some("no controller connection".to_string())),
                        Some(c) => {
                            let mut set = std::collections::HashMap::new();
                            let mut del = Vec::new();
                            for o in &r.ops {
                                match (o.op, &o.value) {
                                    (1, _) | (0, None) => del.push(o.name.clone()),
                                    (_, Some(val)) => {
                                        set.insert(o.name.clone(), val.clone());
                                    }
                                    _ => {}
                                }
                            }
                            let req = aeromq::AlterTopicConfigsRequest {
                                topic: r.name.clone(),
                                set,
                                delete: del,
                                replace_all: !incremental,
                                validate_only,
                            };
                            let res = admin_result(c.alter_topic_configs(req).await);
                            if res.0 == E_NONE && !validate_only {
                                mutated = true;
                            }
                            res
                        }
                    }
                }
            }
            RES_BROKER | RES_BROKER_LOGGER => (
                E_INVALID_REQUEST,
                Some("Dynamic broker configuration is not supported; use static broker config".to_string()),
            ),
            other => (E_INVALID_REQUEST, Some(format!("Unsupported resource type {other}"))),
        };
        out.push((code, msg, r.ty, r.name));
    }
    if mutated {
        st.refresh_now().await;
    }
    let _ = v;
    let mut w = Wr::new(rd.flex);
    w.i32(0).arr(out.len());
    for (code, msg, ty, name) in &out {
        w.i16(*code).nstr(msg.as_deref()).i8(*ty).str(name).tagged();
    }
    w.tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// CreatePartitions (37)
// ---------------------------------------------------------------------------

async fn create_partitions(st: &Arc<AdminState>, _v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    struct Req {
        name: String,
        count: i32,
        assignments: Option<Vec<Vec<i32>>>,
    }
    let mut reqs = Vec::new();
    for _ in 0..rd.arr()? {
        let name = rd.str()?;
        let count = rd.i32()?;
        let assignments = match rd.narr()? {
            None => None,
            Some(n) => {
                let mut a = Vec::new();
                for _ in 0..n {
                    let mut ids = Vec::new();
                    for _ in 0..rd.arr()? {
                        ids.push(rd.i32()?);
                    }
                    rd.tagged()?;
                    a.push(ids);
                }
                Some(a)
            }
        };
        rd.tagged()?;
        reqs.push(Req { name, count, assignments });
    }
    let _timeout = rd.i32()?;
    let validate_only = rd.bool()?;
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let mut out = Vec::new();
    let mut mutated = false;
    for r in reqs {
        let (code, msg) = match snap.topics.get(&r.name) {
            None => (E_UNKNOWN_TOPIC_OR_PARTITION, Some("This server does not host this topic-partition.".to_string())),
            Some(ti) => {
                let cur = ti.partitions.len() as i32;
                if r.count <= cur {
                    (E_INVALID_PARTITIONS, Some(format!("Topic currently has {cur} partitions, which is higher than the requested {}.", r.count)))
                } else if let Some(a) = r.assignments.as_ref().filter(|a| a.len() as i32 != r.count - cur) {
                    (
                        E_INVALID_REPLICA_ASSIGNMENT,
                        Some(format!("Increasing the number of partitions by {} but {} assignments provided.", r.count - cur, a.len())),
                    )
                } else {
                    match &st.ctl {
                        None => (E_NOT_CONTROLLER, Some("no controller connection".to_string())),
                        Some(c) => {
                            let req = aeromq::CreatePartitionsRequest {
                                topic: r.name.clone(),
                                new_total: r.count as u32,
                                assignments: r
                                    .assignments
                                    .iter()
                                    .flatten()
                                    .enumerate()
                                    .map(|(i, ids)| aeromq::ReplicaAssignment {
                                        partition: i as u32,
                                        broker_ids: ids.iter().map(|&x| x as u32).collect(),
                                    })
                                    .collect(),
                                validate_only,
                            };
                            let res = admin_result(c.create_partitions(req).await);
                            if res.0 == E_NONE && !validate_only {
                                mutated = true;
                            }
                            res
                        }
                    }
                }
            }
        };
        out.push((r.name, code, msg));
    }
    if mutated {
        st.refresh_now().await;
    }
    let mut w = Wr::new(rd.flex);
    w.i32(0).arr(out.len());
    for (name, code, msg) in &out {
        w.str(name).i16(*code).nstr(msg.as_deref()).tagged();
    }
    w.tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// ElectLeaders (43)
// ---------------------------------------------------------------------------

async fn elect_leaders(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let election_type = if v >= 1 { rd.i8()? } else { 0 };
    let requested: Option<Vec<(String, Vec<i32>)>> = match rd.narr()? {
        None => None,
        Some(n) => {
            let mut a = Vec::new();
            for _ in 0..n {
                let t = rd.str()?;
                let mut ps = Vec::new();
                for _ in 0..rd.arr()? {
                    ps.push(rd.i32()?);
                }
                rd.tagged()?;
                a.push((t, ps));
            }
            Some(a)
        }
    };
    let _timeout = rd.i32()?;
    rd.tagged()?;

    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let targets: Vec<(String, Vec<i32>)> = match requested {
        Some(r) => r,
        None => snap
            .topics
            .iter()
            .map(|(t, ti)| (t.clone(), ti.partitions.keys().copied().collect()))
            .collect(),
    };
    let mut results: Vec<(String, Vec<(i32, i16, Option<String>)>)> = Vec::new();
    let mut top_err = E_NONE;
    let mut mutated = false;
    for (topic, parts) in targets {
        let mut pr = Vec::new();
        for p in parts {
            let (code, msg) = if election_type != 0 && election_type != 1 {
                top_err = E_INVALID_REQUEST;
                (E_INVALID_REQUEST, Some("Unknown election type".to_string()))
            } else {
                match &st.ctl {
                    None => (E_NOT_CONTROLLER, Some("no controller connection".to_string())),
                    Some(c) => {
                        let r = admin_result(
                            c.elect_leaders(aeromq::ElectLeadersRequest {
                                topic: topic.clone(),
                                partition: p as u32,
                                election_type: election_type as i32,
                            })
                            .await,
                        );
                        if r.0 == E_NONE {
                            mutated = true;
                        }
                        r
                    }
                }
            };
            pr.push((p, code, msg));
        }
        results.push((topic, pr));
    }
    if mutated {
        st.refresh_now().await;
    }
    let mut w = Wr::new(rd.flex);
    w.i32(0);
    if v >= 1 {
        w.i16(top_err);
    }
    w.arr(results.len());
    for (t, pr) in &results {
        w.str(t).arr(pr.len());
        for (p, code, msg) in pr {
            w.i32(*p).i16(*code).nstr(msg.as_deref()).tagged();
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// DescribeCluster (60)
// ---------------------------------------------------------------------------

async fn describe_cluster(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let _include_authz = rd.bool()?;
    let endpoint_type = if v >= 1 { rd.i8()? } else { 1 };
    rd.tagged()?;
    st.ensure_fresh().await;
    let snap = st.topo.snapshot();
    let mut brokers: Vec<(i32, String, i32, Option<String>)> = snap
        .brokers
        .values()
        .map(|b| (b.id, b.host.clone(), b.kafka_port, b.rack.clone()))
        .collect();
    if brokers.is_empty() {
        brokers.push((st.my_id(), st.cfg.host.clone(), st.cfg.kafka_port, st.cfg.rack.clone()));
    }
    let controller = brokers.iter().map(|b| b.0).min().unwrap_or(st.my_id());
    let (code, msg) = if endpoint_type == 1 { (E_NONE, None) } else { (E_INVALID_REQUEST, Some("Unsupported endpoint type".to_string())) };
    let mut w = Wr::new(rd.flex);
    w.i32(0).i16(code).nstr(msg.as_deref());
    if v >= 1 {
        w.i8(endpoint_type);
    }
    w.str("aerostream-cluster").i32(controller).arr(brokers.len());
    for (id, host, port, rack) in &brokers {
        w.i32(*id).str(host).i32(*port).nstr(rack.as_deref()).tagged();
    }
    w.i32(i32::MIN).tagged();
    Ok(w.finish())
}

// ---------------------------------------------------------------------------
// ListOffsets (2), v0-v7
// ---------------------------------------------------------------------------

/// Smallest offset whose batch max timestamp is >= `ts`, scanning stored batches.
async fn offset_for_timestamp(log: &Arc<tokio::sync::Mutex<crate::log::PartitionLog>>, ts: i64, latest: u64, earliest: u64) -> Option<(i64, i64)> {
    use std::io::{Read, Seek, SeekFrom};
    let mut g = log.lock().await;
    let mut off = earliest;
    while off < latest {
        if let Ok(Some((mut file, pos, len))) = g.read_from_offset(off, 43) {
            if len >= 43 && file.seek(SeekFrom::Start(pos)).is_ok() {
                let mut hdr = [0u8; 43];
                if file.read_exact(&mut hdr).is_ok() && hdr[16] == 2 {
                    let max_ts = i64::from_be_bytes(hdr[35..43].try_into().unwrap());
                    if max_ts >= ts {
                        let first_ts = i64::from_be_bytes(hdr[27..35].try_into().unwrap());
                        return Some((first_ts.max(ts).min(max_ts), off as i64));
                    }
                }
            }
        }
        off += 1;
    }
    None
}

async fn list_offsets(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let _replica_id = rd.i32()?;
    let _isolation = if v >= 2 { rd.i8()? } else { 0 };
    let mut topics = Vec::new();
    for _ in 0..rd.arr()? {
        let name = rd.str()?;
        let mut parts = Vec::new();
        for _ in 0..rd.arr()? {
            let p = rd.i32()?;
            if v >= 4 {
                let _epoch = rd.i32()?;
            }
            let ts = rd.i64()?;
            if v == 0 {
                let _max = rd.i32()?;
            }
            rd.tagged()?;
            parts.push((p, ts));
        }
        rd.tagged()?;
        topics.push((name, parts));
    }
    rd.tagged()?;

    st.ensure_fresh().await;
    let local: std::collections::HashSet<(String, u32)> =
        st.log_manager.get_all_offsets().await.into_iter().map(|(t, p, _)| (t, p)).collect();

    let mut w = Wr::new(rd.flex);
    if v >= 2 {
        w.i32(0);
    }
    w.arr(topics.len());
    for (name, parts) in &topics {
        w.str(name).arr(parts.len());
        for (p, ts) in parts {
            let info = st.topo.partition(name, *p);
            let known = info.is_some() || local.contains(&(name.clone(), *p as u32));
            let (mut code, mut ts_out, mut off, mut epoch) = (E_NONE, -1i64, -1i64, -1i32);
            if !known || *p < 0 {
                code = E_UNKNOWN_TOPIC_OR_PARTITION;
            } else if let Some(i) = &info {
                if i.leader != 0 && i.leader != st.my_id() && !i.replicas.contains(&st.my_id()) {
                    code = 6; // NOT_LEADER_OR_FOLLOWER
                }
            }
            if code == E_NONE {
                match st.log_manager.get_partition(name, *p as u32).await {
                    Err(_) => code = E_UNKNOWN_TOPIC_OR_PARTITION,
                    Ok(log) => {
                        let (earliest, mut latest) = {
                            let g = log.lock().await;
                            (g.segments.first().map_or(0, |s| s.base_offset), g.high_watermark)
                        };
                        if let Some(i) = &info {
                            if i.leader != st.my_id() && i.high_watermark >= 0 {
                                latest = latest.min(i.high_watermark as u64);
                            }
                            epoch = 0;
                        }
                        match *ts {
                            -2 => off = earliest as i64,
                            -1 | -3 => off = latest as i64,
                            t if t >= 0 => match offset_for_timestamp(&log, t, latest, earliest).await {
                                Some((found_ts, found_off)) => {
                                    ts_out = found_ts;
                                    off = found_off;
                                }
                                None => {
                                    ts_out = -1;
                                    off = -1;
                                }
                            },
                            _ => code = E_INVALID_REQUEST,
                        }
                        if v == 0 && off < 0 && code == E_NONE {
                            // v0 returns an empty offsets array when nothing matches
                        }
                    }
                }
            }
            if code != E_NONE {
                ts_out = -1;
                off = -1;
                epoch = -1;
            }
            w.i32(*p).i16(code);
            if v == 0 {
                if off >= 0 {
                    w.arr(1).i64(off);
                } else {
                    w.arr(0);
                }
            } else {
                w.i64(ts_out).i64(off);
                if v >= 4 {
                    w.i32(epoch);
                }
            }
            w.tagged();
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

/// Used by tests and Metadata: description of a topic map keyed by name.
#[allow(dead_code)]
pub fn topic_names(snap: &Snapshot) -> BTreeMap<String, usize> {
    snap.topics.iter().map(|(k, v)| (k.clone(), v.partitions.len())).collect()
}

#[cfg(test)]
mod tests;
