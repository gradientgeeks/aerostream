//! Cluster topology view for the Kafka shim: brokers (with racks), partition leaders/replicas/ISR,
//! topic configs, plus KIP-392 "fetch from closest replica" selection and group-coordinator hashing.
//!
//! The controller (Go, Raft) stays the source of truth. Brokers keep a small cached `Snapshot`
//! refreshed periodically (and on demand) through the `Controller` abstraction, which is also the
//! seam used by unit tests.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{metadata::MetadataValue, Request};
use tracing::{debug, warn};

use crate::config::BrokerConfig;
use crate::grpc::aeromq::{self, admin_service_client::AdminServiceClient, discovery_service_client::DiscoveryServiceClient};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerNode {
    pub id: i32,
    pub host: String,
    /// Kafka wire-protocol port (falls back to the data port for brokers that did not report one).
    pub kafka_port: i32,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionInfo {
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub high_watermark: i64,
    /// Log end offset (as last reported by heartbeat) per replica.
    pub replica_offsets: HashMap<i32, i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopicInfo {
    pub partitions: BTreeMap<i32, PartitionInfo>,
    pub configs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub brokers: BTreeMap<i32, BrokerNode>,
    pub topics: BTreeMap<String, TopicInfo>,
}

impl Snapshot {
    pub fn from_proto(resp: aeromq::MetadataResponse) -> Self {
        let mut s = Snapshot::default();
        for b in resp.brokers {
            let rack = if b.rack.is_empty() { None } else { Some(b.rack) };
            s.brokers.insert(
                b.broker_id as i32,
                BrokerNode {
                    id: b.broker_id as i32,
                    host: b.host,
                    kafka_port: if b.kafka_port > 0 { b.kafka_port } else { b.port },
                    rack,
                },
            );
        }
        for t in resp.topics {
            let mut ti = TopicInfo::default();
            for (k, v) in t.configs {
                ti.configs.insert(k, v);
            }
            for p in t.partitions {
                ti.partitions.insert(
                    p.partition_id as i32,
                    PartitionInfo {
                        leader: p.leader_id as i32,
                        replicas: p.replica_ids.iter().map(|&x| x as i32).collect(),
                        isr: p.isr.iter().map(|&x| x as i32).collect(),
                        high_watermark: p.high_watermark,
                        replica_offsets: p.replica_offsets.iter().map(|(k, v)| (*k as i32, *v)).collect(),
                    },
                );
            }
            s.topics.insert(t.topic, ti);
        }
        s
    }
}

/// KIP-392 replica selector policy (`replica.selector.class`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaSelector {
    /// Always fetch from the leader (Kafka default).
    Leader,
    /// Prefer an in-sync replica in the client's rack (RackAwareReplicaSelector).
    RackAware,
}

impl ReplicaSelector {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().replace('-', "_").as_str() {
            "leader" | "leader_selector" => ReplicaSelector::Leader,
            _ => ReplicaSelector::RackAware,
        }
    }
}

/// Pick the broker a consumer in `client_rack` should read `p` from. Returns `p.leader` when the
/// selector is `Leader`, the client rack is unknown/empty, or no suitable same-rack replica exists.
///
/// Rules (mirrors Kafka's RackAwareReplicaSelector): candidates are replicas in the client's rack
/// that are in the ISR and have already replicated `fetch_offset`. If the leader is a candidate it
/// wins; otherwise the candidate with the highest log end offset (ties: lowest id).
pub fn select_replica(
    selector: ReplicaSelector,
    client_rack: Option<&str>,
    p: &PartitionInfo,
    brokers: &BTreeMap<i32, BrokerNode>,
    fetch_offset: i64,
) -> i32 {
    if selector == ReplicaSelector::Leader {
        return p.leader;
    }
    let rack = match client_rack {
        Some(r) if !r.is_empty() => r,
        _ => return p.leader,
    };
    let mut best: Option<(i64, i32)> = None;
    for &id in &p.replicas {
        let same_rack = brokers.get(&id).and_then(|b| b.rack.as_deref()) == Some(rack);
        if !same_rack {
            continue;
        }
        if id == p.leader {
            return id;
        }
        if !p.isr.contains(&id) {
            continue;
        }
        let leo = p.replica_offsets.get(&id).copied().unwrap_or(0);
        if leo < fetch_offset {
            continue;
        }
        match best {
            Some((bl, bid)) if bl > leo || (bl == leo && bid < id) => {}
            _ => best = Some((leo, id)),
        }
    }
    best.map(|(_, id)| id).unwrap_or(p.leader)
}

/// Stable FNV-1a hash used for group-coordinator placement.
fn fnv1a(key: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub struct TopologyCache {
    inner: RwLock<(Snapshot, Option<Instant>)>,
}

impl Default for TopologyCache {
    fn default() -> Self {
        Self { inner: RwLock::new((Snapshot::default(), None)) }
    }
}

static GLOBAL: OnceLock<Arc<TopologyCache>> = OnceLock::new();

impl TopologyCache {
    pub fn global() -> Arc<TopologyCache> {
        GLOBAL.get_or_init(|| Arc::new(TopologyCache::default())).clone()
    }

    pub fn update(&self, s: Snapshot) {
        *self.inner.write().unwrap() = (s, Some(Instant::now()));
    }

    pub fn snapshot(&self) -> Snapshot {
        self.inner.read().unwrap().0.clone()
    }

    pub fn age(&self) -> Option<Duration> {
        self.inner.read().unwrap().1.map(|t| t.elapsed())
    }

    pub fn partition(&self, topic: &str, partition: i32) -> Option<PartitionInfo> {
        self.inner.read().unwrap().0.topics.get(topic)?.partitions.get(&partition).cloned()
    }

    pub fn broker(&self, id: i32) -> Option<BrokerNode> {
        self.inner.read().unwrap().0.brokers.get(&id).cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().0.brokers.is_empty()
    }

    /// Broker that coordinates `key` (a group id or transactional id): hash over sorted broker ids.
    pub fn coordinator_for(&self, key: &str) -> Option<BrokerNode> {
        let g = self.inner.read().unwrap();
        let n = g.0.brokers.len();
        if n == 0 {
            return None;
        }
        let idx = (fnv1a(key) % n as u64) as usize;
        g.0.brokers.values().nth(idx).cloned()
    }

    pub async fn refresh(&self, ctl: &dyn Controller) -> Result<(), String> {
        let s = ctl.metadata(vec![]).await?;
        self.update(s);
        Ok(())
    }

    /// Refresh when the cache is older than `max_age` (errors are logged, stale data is kept).
    pub async fn refresh_if_stale(&self, ctl: &dyn Controller, max_age: Duration) {
        if self.age().map_or(true, |a| a > max_age) {
            if let Err(e) = self.refresh(ctl).await {
                debug!("[AeroStream Topology] refresh failed: {}", e);
            }
        }
    }

    pub fn spawn_refresh_loop(self: Arc<Self>, ctl: Arc<dyn Controller>, every: Duration) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.refresh(ctl.as_ref()).await {
                    debug!("[AeroStream Topology] periodic refresh failed: {}", e);
                }
                tokio::time::sleep(every).await;
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Controller abstraction (gRPC in production, mock in tests)
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Controller: Send + Sync {
    async fn metadata(&self, topics: Vec<String>) -> Result<Snapshot, String>;
    async fn create_topic(&self, req: aeromq::CreateTopicRequest) -> Result<aeromq::CreateTopicResponse, String>;
    async fn delete_topic(&self, topic: String) -> Result<aeromq::AdminResponse, String>;
    async fn create_partitions(&self, req: aeromq::CreatePartitionsRequest) -> Result<aeromq::AdminResponse, String>;
    async fn alter_topic_configs(&self, req: aeromq::AlterTopicConfigsRequest) -> Result<aeromq::AdminResponse, String>;
    async fn elect_leaders(&self, req: aeromq::ElectLeadersRequest) -> Result<aeromq::AdminResponse, String>;
    async fn commit_offsets(&self, group: &str, offsets: Vec<(String, i32, i64)>) -> Result<(), String>;
    async fn fetch_offsets(&self, group: &str, topics: Vec<String>) -> Result<Vec<(String, i32, i64)>, String>;
}

pub struct GrpcController {
    channel: Channel,
    token: Option<String>,
}

impl GrpcController {
    pub fn new(cfg: &BrokerConfig) -> Result<Self, String> {
        let uri = if cfg.controller.starts_with("http://") || cfg.controller.starts_with("https://") {
            cfg.controller.clone()
        } else {
            format!("http://{}", cfg.controller)
        };
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| e.to_string())?
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10));
        if uri.starts_with("https://") {
            let mut tls = ClientTlsConfig::new();
            if let Some(ca) = &cfg.tls.ca_file {
                let pem = std::fs::read(ca).map_err(|e| e.to_string())?;
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            ep = ep.tls_config(tls).map_err(|e| e.to_string())?;
        }
        Ok(Self { channel: ep.connect_lazy(), token: cfg.auth.token.clone() })
    }

    fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        if let Some(t) = &self.token {
            if let Ok(v) = MetadataValue::try_from(format!("Bearer {}", t)) {
                r.metadata_mut().insert("authorization", v);
            }
        }
        r
    }
}

fn st(e: tonic::Status) -> String {
    e.message().to_string()
}

#[async_trait]
impl Controller for GrpcController {
    async fn metadata(&self, topics: Vec<String>) -> Result<Snapshot, String> {
        let mut c = DiscoveryServiceClient::new(self.channel.clone());
        let r = c.get_metadata(self.req(aeromq::MetadataRequest { topics })).await.map_err(st)?;
        Ok(Snapshot::from_proto(r.into_inner()))
    }
    async fn create_topic(&self, req: aeromq::CreateTopicRequest) -> Result<aeromq::CreateTopicResponse, String> {
        let mut c = DiscoveryServiceClient::new(self.channel.clone());
        Ok(c.create_topic(self.req(req)).await.map_err(st)?.into_inner())
    }
    async fn delete_topic(&self, topic: String) -> Result<aeromq::AdminResponse, String> {
        let mut c = AdminServiceClient::new(self.channel.clone());
        Ok(c.delete_topic(self.req(aeromq::DeleteTopicRequest { topic })).await.map_err(st)?.into_inner())
    }
    async fn create_partitions(&self, req: aeromq::CreatePartitionsRequest) -> Result<aeromq::AdminResponse, String> {
        let mut c = AdminServiceClient::new(self.channel.clone());
        Ok(c.create_partitions(self.req(req)).await.map_err(st)?.into_inner())
    }
    async fn alter_topic_configs(&self, req: aeromq::AlterTopicConfigsRequest) -> Result<aeromq::AdminResponse, String> {
        let mut c = AdminServiceClient::new(self.channel.clone());
        Ok(c.alter_topic_configs(self.req(req)).await.map_err(st)?.into_inner())
    }
    async fn elect_leaders(&self, req: aeromq::ElectLeadersRequest) -> Result<aeromq::AdminResponse, String> {
        let mut c = AdminServiceClient::new(self.channel.clone());
        Ok(c.elect_leaders(self.req(req)).await.map_err(st)?.into_inner())
    }
    async fn commit_offsets(&self, group: &str, offsets: Vec<(String, i32, i64)>) -> Result<(), String> {
        let mut c = DiscoveryServiceClient::new(self.channel.clone());
        let req = aeromq::CommitOffsetsRequest {
            group_id: group.to_string(),
            member_id: String::new(),
            generation_id: 0,
            offsets: offsets
                .into_iter()
                .map(|(t, p, o)| aeromq::TopicPartitionOffset { topic: t, partition: p as u32, offset: o })
                .collect(),
        };
        let r = c.commit_offsets(self.req(req)).await.map_err(st)?.into_inner();
        if r.success { Ok(()) } else { Err("controller rejected offset commit".into()) }
    }
    async fn fetch_offsets(&self, group: &str, topics: Vec<String>) -> Result<Vec<(String, i32, i64)>, String> {
        let mut c = DiscoveryServiceClient::new(self.channel.clone());
        let r = c
            .fetch_offsets(self.req(aeromq::FetchOffsetsRequest { group_id: group.to_string(), topics }))
            .await
            .map_err(st)?
            .into_inner();
        Ok(r.offsets.into_iter().map(|o| (o.topic, o.partition as i32, o.offset)).collect())
    }
}

/// Build the production controller client, logging (not failing) on a bad URI.
pub fn grpc_controller(cfg: &BrokerConfig) -> Option<Arc<dyn Controller>> {
    match GrpcController::new(cfg) {
        Ok(c) => Some(Arc::new(c)),
        Err(e) => {
            warn!("[AeroStream Topology] cannot build controller client: {}", e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brokers() -> BTreeMap<i32, BrokerNode> {
        let mut m = BTreeMap::new();
        for (id, rack) in [(1, Some("a")), (2, Some("b")), (3, Some("b")), (4, None)] {
            m.insert(id, BrokerNode { id, host: format!("h{id}"), kafka_port: 9092, rack: rack.map(String::from) });
        }
        m
    }

    fn part() -> PartitionInfo {
        PartitionInfo {
            leader: 1,
            replicas: vec![1, 2, 3, 4],
            isr: vec![1, 2, 3, 4],
            high_watermark: 100,
            replica_offsets: [(1, 100), (2, 90), (3, 100), (4, 100)].into_iter().collect(),
        }
    }

    #[test]
    fn leader_selector_always_leader() {
        assert_eq!(select_replica(ReplicaSelector::Leader, Some("b"), &part(), &brokers(), 0), 1);
    }

    #[test]
    fn rack_aware_prefers_most_caught_up_in_rack() {
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some("b"), &part(), &brokers(), 0), 3);
    }

    #[test]
    fn leader_in_rack_wins() {
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some("a"), &part(), &brokers(), 0), 1);
    }

    #[test]
    fn falls_back_to_leader() {
        let b = brokers();
        let p = part();
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some("zzz"), &p, &b, 0), 1);
        assert_eq!(select_replica(ReplicaSelector::RackAware, None, &p, &b, 0), 1);
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some(""), &p, &b, 0), 1);
    }

    #[test]
    fn skips_out_of_sync_and_behind_replicas() {
        let mut p = part();
        p.isr = vec![1, 2];
        // broker 3 out of ISR -> broker 2, but 2 is behind fetch offset 95 -> leader
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some("b"), &p, &brokers(), 50), 2);
        assert_eq!(select_replica(ReplicaSelector::RackAware, Some("b"), &p, &brokers(), 95), 1);
    }

    #[test]
    fn selector_parse() {
        assert_eq!(ReplicaSelector::parse("leader"), ReplicaSelector::Leader);
        assert_eq!(ReplicaSelector::parse("rack_aware"), ReplicaSelector::RackAware);
        assert_eq!(ReplicaSelector::parse("Rack-Aware"), ReplicaSelector::RackAware);
    }

    #[test]
    fn coordinator_is_stable_and_spread() {
        let c = TopologyCache::default();
        assert!(c.coordinator_for("g").is_none());
        c.update(Snapshot { brokers: brokers(), topics: BTreeMap::new() });
        let a = c.coordinator_for("group-1").unwrap().id;
        assert_eq!(a, c.coordinator_for("group-1").unwrap().id);
        let mut seen = std::collections::HashSet::new();
        for i in 0..64 {
            seen.insert(c.coordinator_for(&format!("g{i}")).unwrap().id);
        }
        assert!(seen.len() >= 3, "groups should spread over brokers: {seen:?}");
    }

    #[test]
    fn snapshot_from_proto_maps_rack_and_ports() {
        let resp = aeromq::MetadataResponse {
            brokers: vec![
                aeromq::BrokerInfo { broker_id: 1, host: "x".into(), port: 9091, rack: "r1".into(), kafka_port: 9093 },
                aeromq::BrokerInfo { broker_id: 2, host: "y".into(), port: 9092, rack: "".into(), kafka_port: 0 },
            ],
            topics: vec![aeromq::TopicMetadata {
                topic: "t".into(),
                partitions: vec![aeromq::PartitionMetadata {
                    partition_id: 0,
                    leader_id: 1,
                    replica_ids: vec![1, 2],
                    isr: vec![1],
                    high_watermark: 5,
                    replica_offsets: [(1u32, 5i64)].into_iter().collect(),
                }],
                configs: [("retention.ms".to_string(), "5".to_string())].into_iter().collect(),
            }],
        };
        let s = Snapshot::from_proto(resp);
        assert_eq!(s.brokers[&1].rack.as_deref(), Some("r1"));
        assert_eq!(s.brokers[&1].kafka_port, 9093);
        assert_eq!(s.brokers[&2].rack, None);
        assert_eq!(s.brokers[&2].kafka_port, 9092);
        assert_eq!(s.topics["t"].partitions[&0].isr, vec![1]);
        assert_eq!(s.topics["t"].configs["retention.ms"], "5");
    }
}
