use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;

use super::*;
use crate::kafka::codec::is_flexible;
use crate::topology::{BrokerNode, PartitionInfo, TopicInfo};

/// In-memory controller emulating the FSM semantics the shim relies on.
struct Mock {
    snap: Mutex<Snapshot>,
    offsets: Mutex<HashMap<(String, String, i32), i64>>,
    commits: Mutex<usize>,
}

impl Mock {
    fn new(brokers: &[(i32, Option<&str>)]) -> Arc<Self> {
        let mut s = Snapshot::default();
        for (id, rack) in brokers {
            s.brokers.insert(
                *id,
                BrokerNode { id: *id, host: format!("host{id}"), kafka_port: 9000 + *id, rack: rack.map(String::from) },
            );
        }
        Arc::new(Mock { snap: Mutex::new(s), offsets: Mutex::new(HashMap::new()), commits: Mutex::new(0) })
    }

    fn assign(&self, start: i32, count: i32, rf: i32) -> Vec<Vec<i32>> {
        let ids: Vec<i32> = self.snap.lock().unwrap().brokers.keys().copied().collect();
        (0..count)
            .map(|i| (0..rf.min(ids.len() as i32)).map(|r| ids[((start + i + r) as usize) % ids.len()]).collect())
            .collect()
    }
}

fn resp(ok: bool, code: i32, msg: &str) -> aeromq::AdminResponse {
    aeromq::AdminResponse { success: ok, message: msg.into(), error_code: code }
}

#[async_trait]
impl Controller for Mock {
    async fn metadata(&self, _t: Vec<String>) -> Result<Snapshot, String> {
        Ok(self.snap.lock().unwrap().clone())
    }
    async fn create_topic(&self, r: aeromq::CreateTopicRequest) -> Result<aeromq::CreateTopicResponse, String> {
        if self.snap.lock().unwrap().topics.contains_key(&r.topic) && r.fail_if_exists {
            return Ok(aeromq::CreateTopicResponse { success: false, message: "exists".into(), error_code: 36 });
        }
        let layout: Vec<Vec<i32>> = if r.manual_assignments.is_empty() {
            self.assign(0, r.partitions as i32, r.replication_factor as i32)
        } else {
            r.manual_assignments.iter().map(|a| a.broker_ids.iter().map(|&x| x as i32).collect()).collect()
        };
        let mut ti = TopicInfo::default();
        for (i, reps) in layout.into_iter().enumerate() {
            ti.partitions.insert(
                i as i32,
                PartitionInfo { leader: reps[0], isr: reps.clone(), replicas: reps, high_watermark: 0, replica_offsets: HashMap::new() },
            );
        }
        ti.configs = r.configs.into_iter().collect();
        self.snap.lock().unwrap().topics.insert(r.topic, ti);
        Ok(aeromq::CreateTopicResponse { success: true, message: String::new(), error_code: 0 })
    }
    async fn delete_topic(&self, topic: String) -> Result<aeromq::AdminResponse, String> {
        Ok(if self.snap.lock().unwrap().topics.remove(&topic).is_some() { resp(true, 0, "") } else { resp(false, 3, "unknown") })
    }
    async fn create_partitions(&self, r: aeromq::CreatePartitionsRequest) -> Result<aeromq::AdminResponse, String> {
        let cur = match self.snap.lock().unwrap().topics.get(&r.topic) {
            Some(t) => t.partitions.len() as i32,
            None => return Ok(resp(false, 3, "unknown")),
        };
        let add = r.new_total as i32 - cur;
        let layout = self.assign(cur, add, 1);
        let mut g = self.snap.lock().unwrap();
        let t = g.topics.get_mut(&r.topic).unwrap();
        for (i, reps) in layout.into_iter().enumerate() {
            t.partitions.insert(
                cur + i as i32,
                PartitionInfo { leader: reps[0], isr: reps.clone(), replicas: reps, ..Default::default() },
            );
        }
        Ok(resp(true, 0, ""))
    }
    async fn alter_topic_configs(&self, r: aeromq::AlterTopicConfigsRequest) -> Result<aeromq::AdminResponse, String> {
        let mut g = self.snap.lock().unwrap();
        match g.topics.get_mut(&r.topic) {
            None => Ok(resp(false, 3, "unknown")),
            Some(t) => {
                if r.validate_only {
                    return Ok(resp(true, 0, ""));
                }
                if r.replace_all {
                    t.configs.clear();
                }
                for k in &r.delete {
                    t.configs.remove(k);
                }
                for (k, v) in r.set {
                    t.configs.insert(k, v);
                }
                Ok(resp(true, 0, ""))
            }
        }
    }
    async fn elect_leaders(&self, r: aeromq::ElectLeadersRequest) -> Result<aeromq::AdminResponse, String> {
        let mut g = self.snap.lock().unwrap();
        let p = match g.topics.get_mut(&r.topic).and_then(|t| t.partitions.get_mut(&(r.partition as i32))) {
            Some(p) => p,
            None => return Ok(resp(false, 3, "unknown")),
        };
        if p.leader == p.replicas[0] {
            return Ok(resp(false, 84, "not needed"));
        }
        p.leader = p.replicas[0];
        Ok(resp(true, 0, ""))
    }
    async fn commit_offsets(&self, group: &str, offsets: Vec<(String, i32, i64)>) -> Result<(), String> {
        *self.commits.lock().unwrap() += 1;
        for (t, p, o) in offsets {
            self.offsets.lock().unwrap().insert((group.to_string(), t, p), o);
        }
        Ok(())
    }
    async fn fetch_offsets(&self, group: &str, topics: Vec<String>) -> Result<Vec<(String, i32, i64)>, String> {
        let o = self.offsets.lock().unwrap();
        Ok(o.iter().filter(|((g, t, _), _)| g == group && topics.contains(t)).map(|((_, t, p), v)| (t.clone(), *p, *v)).collect())
    }
}

struct Env {
    st: Arc<AdminState>,
    mock: Arc<Mock>,
    _dir: tempfile::TempDir,
}

fn env_with(brokers: &[(i32, Option<&str>)], my_id: u32) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Arc::new(BrokerConfig { id: my_id, host: "127.0.0.1".into(), kafka_port: 9092, ..Default::default() });
    let mock = Mock::new(brokers);
    let st = AdminState::new(
        cfg,
        Arc::new(LogManager::new(dir.path(), my_id)),
        Arc::new(TopologyCache::default()),
        Some(mock.clone() as Arc<dyn Controller>),
        GroupCoordinator::new(Duration::ZERO),
    );
    Env { st, mock, _dir: dir }
}

fn env() -> Env {
    env_with(&[(1, Some("a")), (2, Some("b")), (3, Some("c"))], 1)
}

fn build<F: FnOnce(&mut Wr)>(api: i16, v: i16, f: F) -> Vec<u8> {
    let mut w = Wr::new(is_flexible(api, v));
    w.tagged(); // request header tagged fields (flexible only)
    f(&mut w);
    w.finish()
}

async fn call<F: FnOnce(&mut Wr)>(st: &Arc<AdminState>, api: i16, v: i16, f: F) -> Vec<u8> {
    let body = build(api, v, f);
    let out = dispatch(st, api, v, 4242, "test-client", &body).await.expect("handled").expect("ok");
    let flex = is_flexible(api, v);
    assert_eq!(i32::from_be_bytes(out[..4].try_into().unwrap()), 4242);
    let skip = if flex { 5 } else { 4 };
    if flex {
        assert_eq!(out[4], 0, "response header v1 tagged fields");
    }
    out[skip..].to_vec()
}

// ---------------------------------------------------------------------------

#[test]
fn api_table_is_consistent() {
    let mut keys = std::collections::HashSet::new();
    for (k, lo, hi) in ADMIN_APIS {
        assert!(keys.insert(*k), "duplicate api {k}");
        assert!(lo <= hi);
        assert!(first_flexible_version(*k).is_some(), "api {k} needs a flexible-version entry");
    }
    for k in [2, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 32, 33, 37, 43, 44] {
        assert!(supported_range(k).is_some(), "api {k} must be advertised");
    }
}

#[tokio::test]
async fn create_topics_all_encodings() {
    let e = env();
    for (i, v) in [0i16, 1, 2, 4, 5, 6, 7].into_iter().enumerate() {
        let name = format!("topic-v{v}");
        let out = call(&e.st, 19, v, |w| {
            w.arr(1).str(&name).i32(3).i16(2);
            w.arr(0); // assignments
            w.arr(1).str("retention.ms").nstr(Some("60000")).tagged(); // configs
            w.tagged(); // topic struct tagged
            w.i32(1000);
            if v >= 1 {
                w.bool(false);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(19, v));
        if v >= 2 {
            r.i32().unwrap();
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), name);
        if v >= 7 {
            r.uuid().unwrap();
        }
        assert_eq!(r.i16().unwrap(), 0, "v{v}");
        if v >= 1 {
            assert_eq!(r.nstr().unwrap(), None);
        }
        if v >= 5 {
            assert_eq!(r.i32().unwrap(), 3);
            assert_eq!(r.i16().unwrap(), 2);
            assert_eq!(r.arr().unwrap(), 1);
            assert_eq!(r.str().unwrap(), "retention.ms");
        }
        let _ = i;
    }
    let snap = e.st.topo.snapshot();
    let t = &snap.topics["topic-v7"];
    assert_eq!(t.partitions.len(), 3);
    assert_eq!(t.partitions[&0].replicas.len(), 2);
    assert_eq!(t.configs["retention.ms"], "60000");
}

#[tokio::test]
async fn create_topics_errors() {
    let e = env();
    // exists / invalid name / bad rf / bad partitions / bad config / duplicate in request
    let mk = |w: &mut Wr, name: &str, np: i32, rf: i16, cfg: Option<(&str, &str)>| {
        w.str(name).i32(np).i16(rf).arr(0);
        match cfg {
            Some((k, v)) => {
                w.arr(1).str(k).nstr(Some(v)).tagged();
            }
            None => {
                w.arr(0);
            }
        }
        w.tagged();
    };
    let out = call(&e.st, 19, 5, |w| {
        w.arr(1);
        mk(w, "good", 1, 1, None);
        w.i32(1000).bool(false).tagged();
    })
    .await;
    assert_eq!(Rd::new(&out, true).i32().unwrap(), 0);

    let out = call(&e.st, 19, 5, |w| {
        w.arr(6);
        mk(w, "good", 1, 1, None); // TOPIC_ALREADY_EXISTS
        mk(w, "bad name!", 1, 1, None); // INVALID_TOPIC
        mk(w, "rf", 1, 9, None); // INVALID_REPLICATION_FACTOR (3 brokers)
        mk(w, "zero", 0, 1, None); // INVALID_PARTITIONS
        mk(w, "cfg", 1, 1, Some(("retention.ms", "abc"))); // INVALID_CONFIG
        mk(w, "cfg2", 1, 1, Some(("no.such.config", "1"))); // INVALID_CONFIG
        w.i32(1000).bool(false).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.arr().unwrap(), 6);
    let mut codes = Vec::new();
    for _ in 0..6 {
        r.str().unwrap();
        codes.push(r.i16().unwrap());
        r.nstr().unwrap(); // message
        r.i32().unwrap();
        r.i16().unwrap();
        assert_eq!(r.narr().unwrap(), None); // configs null on error
        r.tagged().unwrap();
    }
    assert_eq!(codes, vec![36, 17, 38, 37, 40, 40]);
    assert!(!e.st.topo.snapshot().topics.contains_key("rf"));
}

#[tokio::test]
async fn create_topics_validate_only_and_manual_assignment() {
    let e = env();
    let out = call(&e.st, 19, 4, |w| {
        w.arr(1).str("dry").i32(1).i16(1).arr(0).arr(0).i32(1000).bool(true);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.i32().unwrap();
    r.arr().unwrap();
    r.str().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    assert!(!e.st.topo.snapshot().topics.contains_key("dry"), "validate_only must not create");

    let out = call(&e.st, 19, 2, |w| {
        w.arr(1).str("manual").i32(-1).i16(-1);
        w.arr(2);
        w.i32(0).arr(2).i32(3).i32(1);
        w.i32(1).arr(2).i32(2).i32(3);
        w.arr(0).i32(1000).bool(false);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.i32().unwrap();
    r.arr().unwrap();
    r.str().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    let t = &e.st.topo.snapshot().topics["manual"];
    assert_eq!(t.partitions[&0].replicas, vec![3, 1]);
    assert_eq!(t.partitions[&0].leader, 3);
    assert_eq!(t.partitions[&1].replicas, vec![2, 3]);
}

#[tokio::test]
async fn delete_topics_by_name_and_by_id() {
    let e = env();
    for n in ["d1", "d2"] {
        call(&e.st, 19, 0, |w| {
            w.arr(1).str(n).i32(1).i16(1).arr(0).arr(0).i32(1000);
        })
        .await;
    }
    // v1: by name
    let out = call(&e.st, 20, 1, |w| {
        w.arr(2).str("d1").str("missing").i32(1000);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.i32().unwrap();
    assert_eq!(r.arr().unwrap(), 2);
    assert_eq!(r.str().unwrap(), "d1");
    assert_eq!(r.i16().unwrap(), 0);
    assert_eq!(r.str().unwrap(), "missing");
    assert_eq!(r.i16().unwrap(), 3);
    assert!(!e.st.topo.snapshot().topics.contains_key("d1"));

    // v6: by topic id, name null
    let id = topic_uuid("d2");
    let out = call(&e.st, 20, 6, |w| {
        w.arr(1).nstr(None).uuid(&id).tagged().i32(1000).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.arr().unwrap(), 1);
    assert_eq!(r.nstr().unwrap().as_deref(), Some("d2"));
    assert_eq!(r.uuid().unwrap(), id);
    assert_eq!(r.i16().unwrap(), 0);
    assert!(e.st.topo.snapshot().topics.is_empty());

    // unknown id
    let out = call(&e.st, 20, 6, |w| {
        w.arr(1).nstr(None).uuid(&[7; 16]).tagged().i32(1000).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    r.arr().unwrap();
    r.nstr().unwrap();
    r.uuid().unwrap();
    assert_eq!(r.i16().unwrap(), E_UNKNOWN_TOPIC_ID);
}

async fn make_topic(e: &Env, name: &str, parts: i32, rf: i16, cfg: &[(&str, &str)]) {
    call(&e.st, 19, 0, |w| {
        w.arr(1).str(name).i32(parts).i16(rf).arr(0);
        w.arr(cfg.len());
        for (k, v) in cfg {
            w.str(k).nstr(Some(v));
        }
        w.i32(1000);
    })
    .await;
}

#[tokio::test]
async fn describe_configs_topic_and_broker() {
    let e = env();
    make_topic(&e, "cfgt", 1, 1, &[("retention.ms", "12345")]).await;
    for v in [0i16, 1, 2, 3, 4] {
        let out = call(&e.st, 32, v, |w| {
            w.arr(2);
            w.i8(2).str("cfgt").null_arr().tagged();
            w.i8(4).str("1").arr(1).str("broker.id").tagged();
            if v >= 1 {
                w.bool(true);
            }
            if v >= 3 {
                w.bool(false);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(32, v));
        r.i32().unwrap();
        assert_eq!(r.arr().unwrap(), 2);
        // topic resource
        assert_eq!(r.i16().unwrap(), 0);
        r.nstr().unwrap();
        assert_eq!(r.i8().unwrap(), 2);
        assert_eq!(r.str().unwrap(), "cfgt");
        let n = r.arr().unwrap();
        assert!(n >= 20);
        let mut found = false;
        for _ in 0..n {
            let name = r.str().unwrap();
            let val = r.nstr().unwrap();
            let _ro = r.bool().unwrap();
            let (is_default, source) = if v == 0 { (r.bool().unwrap(), 0) } else { (false, r.i8().unwrap()) };
            let _sens = r.bool().unwrap();
            if v >= 1 {
                for _ in 0..r.arr().unwrap() {
                    r.str().unwrap();
                    r.nstr().unwrap();
                    r.i8().unwrap();
                    r.tagged().unwrap();
                }
            }
            if v >= 3 {
                r.i8().unwrap();
                r.nstr().unwrap();
            }
            r.tagged().unwrap();
            if name == "retention.ms" {
                found = true;
                assert_eq!(val.as_deref(), Some("12345"));
                if v == 0 {
                    assert!(!is_default);
                } else {
                    assert_eq!(source, 1);
                }
            }
            if name == "segment.ms" && v >= 1 {
                assert_eq!(source, 5);
            }
        }
        assert!(found);
        r.tagged().unwrap();
        // broker resource, filtered to broker.id
        assert_eq!(r.i16().unwrap(), 0);
        r.nstr().unwrap();
        assert_eq!(r.i8().unwrap(), 4);
        assert_eq!(r.str().unwrap(), "1");
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), "broker.id");
        assert_eq!(r.nstr().unwrap().as_deref(), Some("1"));
    }
}

#[tokio::test]
async fn describe_configs_unknown_topic_and_other_broker() {
    let e = env();
    let out = call(&e.st, 32, 2, |w| {
        w.arr(2).i8(2).str("nope").null_arr().i8(4).str("2").null_arr().bool(false);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.i32().unwrap();
    r.arr().unwrap();
    assert_eq!(r.i16().unwrap(), 3);
    r.nstr().unwrap();
    r.i8().unwrap();
    r.str().unwrap();
    assert_eq!(r.arr().unwrap(), 0);
    assert_eq!(r.i16().unwrap(), 42);
}

#[tokio::test]
async fn alter_and_incremental_alter_configs() {
    let e = env();
    make_topic(&e, "alt", 1, 1, &[("retention.ms", "1"), ("segment.ms", "5")]).await;

    // Incremental (v1, flexible): SET max.message.bytes, DELETE segment.ms
    let out = call(&e.st, 44, 1, |w| {
        w.arr(1).i8(2).str("alt").arr(2);
        w.str("max.message.bytes").i8(0).nstr(Some("2048")).tagged();
        w.str("segment.ms").i8(1).nstr(None).tagged();
        w.tagged().bool(false).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.arr().unwrap(), 1);
    assert_eq!(r.i16().unwrap(), 0);
    let cfg = e.st.topo.snapshot().topics["alt"].configs.clone();
    assert_eq!(cfg.get("max.message.bytes").map(String::as_str), Some("2048"));
    assert_eq!(cfg.get("retention.ms").map(String::as_str), Some("1"), "incremental keeps others");
    assert!(!cfg.contains_key("segment.ms"));

    // Legacy AlterConfigs (v0) replaces all
    let out = call(&e.st, 33, 0, |w| {
        w.arr(1).i8(2).str("alt").arr(1).str("cleanup.policy").nstr(Some("compact")).bool(false);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.i32().unwrap();
    r.arr().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    let cfg = e.st.topo.snapshot().topics["alt"].configs.clone();
    assert_eq!(cfg.len(), 1);
    assert_eq!(cfg["cleanup.policy"], "compact");

    // validate_only, invalid value, unknown topic, broker resource
    let out = call(&e.st, 33, 2, |w| {
        w.arr(4);
        w.i8(2).str("alt").arr(1).str("retention.ms").nstr(Some("5")).tagged().tagged();
        w.i8(2).str("alt").arr(1).str("retention.ms").nstr(Some("xx")).tagged().tagged();
        w.i8(2).str("ghost").arr(1).str("retention.ms").nstr(Some("5")).tagged().tagged();
        w.i8(4).str("1").arr(0).tagged();
        w.bool(true).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.arr().unwrap(), 4);
    let mut codes = vec![];
    for _ in 0..4 {
        codes.push(r.i16().unwrap());
        r.nstr().unwrap();
        r.i8().unwrap();
        r.str().unwrap();
        r.tagged().unwrap();
    }
    assert_eq!(codes, vec![0, 40, 3, 42]);
    assert_eq!(e.st.topo.snapshot().topics["alt"].configs["cleanup.policy"], "compact", "validate_only left state alone");
}

#[tokio::test]
async fn create_partitions_flow() {
    let e = env();
    make_topic(&e, "grow", 2, 1, &[]).await;
    for (v, total) in [(0i16, 4i32), (2, 6)] {
        let out = call(&e.st, 37, v, |w| {
            w.arr(3);
            w.str("grow").i32(total).null_arr().tagged();
            w.str("grow").i32(1).null_arr().tagged(); // shrink -> INVALID_PARTITIONS
            w.str("ghost").i32(9).null_arr().tagged();
            w.i32(1000).bool(false).tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(37, v));
        r.i32().unwrap();
        assert_eq!(r.arr().unwrap(), 3);
        let mut codes = vec![];
        for _ in 0..3 {
            r.str().unwrap();
            codes.push(r.i16().unwrap());
            r.nstr().unwrap();
            r.tagged().unwrap();
        }
        assert_eq!(codes, vec![0, 37, 3], "v{v}");
        assert_eq!(e.st.topo.snapshot().topics["grow"].partitions.len() as i32, total);
    }
    // assignment count mismatch
    let out = call(&e.st, 37, 3, |w| {
        w.arr(1).str("grow").i32(8).arr(1).arr(1).i32(1).tagged().tagged().i32(1000).bool(false).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    r.arr().unwrap();
    r.str().unwrap();
    assert_eq!(r.i16().unwrap(), 39);
}

#[tokio::test]
async fn elect_leaders_flow() {
    let e = env();
    make_topic(&e, "el", 1, 2, &[]).await;
    // move leader away from preferred replica in the mock
    {
        let mut g = e.mock.snap.lock().unwrap();
        let p = g.topics.get_mut("el").unwrap().partitions.get_mut(&0).unwrap();
        let pref = p.replicas[0];
        p.leader = p.replicas[1];
        assert_ne!(pref, p.leader);
    }
    for v in [0i16, 1, 2] {
        let out = call(&e.st, 43, v, |w| {
            if v >= 1 {
                w.i8(0);
            }
            w.arr(1).str("el").arr(1).i32(0).tagged().i32(1000).tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(43, v));
        r.i32().unwrap();
        if v >= 1 {
            assert_eq!(r.i16().unwrap(), 0);
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), "el");
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.i32().unwrap(), 0);
        let code = r.i16().unwrap();
        // first call elects (0), later ones are ELECTION_NOT_NEEDED (84)
        assert_eq!(code, if v == 0 { 0 } else { 84 });
    }
    // null topic_partitions = all partitions
    let out = call(&e.st, 43, 2, |w| {
        w.i8(0).null_arr().i32(1000).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    assert_eq!(r.arr().unwrap(), 1);
}

#[tokio::test]
async fn describe_cluster_reports_racks() {
    let e = env();
    let out = call(&e.st, 60, 1, |w| {
        w.bool(false).i8(1).tagged();
    })
    .await;
    let mut r = Rd::new(&out, true);
    r.i32().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    r.nstr().unwrap();
    assert_eq!(r.i8().unwrap(), 1);
    assert_eq!(r.str().unwrap(), "aerostream-cluster");
    assert_eq!(r.i32().unwrap(), 1);
    assert_eq!(r.arr().unwrap(), 3);
    assert_eq!(r.i32().unwrap(), 1);
    assert_eq!(r.str().unwrap(), "host1");
    assert_eq!(r.i32().unwrap(), 9001);
    assert_eq!(r.nstr().unwrap().as_deref(), Some("a"));
}

#[tokio::test]
async fn list_offsets_earliest_latest_and_errors() {
    let e = env();
    // Produce three entries directly into the partition log.
    let log = e.st.log_manager.get_partition("lo", 0).await.unwrap();
    {
        let mut g = log.lock().await;
        for i in 0..3u8 {
            g.append(&[i; 8]).unwrap();
        }
    }
    for v in [0i16, 1, 2, 4, 6, 7] {
        let out = call(&e.st, 2, v, |w| {
            w.i32(-1);
            if v >= 2 {
                w.i8(0);
            }
            w.arr(1).str("lo").arr(3);
            for (p, ts) in [(0, -2i64), (0, -1), (5, -1)] {
                w.i32(p);
                if v >= 4 {
                    w.i32(-1);
                }
                w.i64(ts);
                if v == 0 {
                    w.i32(1);
                }
                w.tagged();
            }
            w.tagged().tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(2, v));
        if v >= 2 {
            r.i32().unwrap();
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), "lo");
        assert_eq!(r.arr().unwrap(), 3);
        let mut got = vec![];
        for _ in 0..3 {
            r.i32().unwrap();
            let code = r.i16().unwrap();
            let off = if v == 0 {
                let n = r.arr().unwrap();
                if n > 0 { r.i64().unwrap() } else { -1 }
            } else {
                r.i64().unwrap();
                r.i64().unwrap()
            };
            if v >= 4 {
                r.i32().unwrap();
            }
            r.tagged().unwrap();
            got.push((code, off));
        }
        assert_eq!(got, vec![(0, 0), (0, 3), (3, if v == 0 { -1 } else { -1 })], "v{v}");
    }
}

#[tokio::test]
async fn list_offsets_by_timestamp() {
    let e = env();
    let log = e.st.log_manager.get_partition("ts", 0).await.unwrap();
    // three RecordBatch v2 stubs (61-byte header) with max timestamps 100, 200, 300
    {
        let mut g = log.lock().await;
        for (i, ts) in [100i64, 200, 300].into_iter().enumerate() {
            let mut b = vec![0u8; 61];
            b[8..12].copy_from_slice(&49i32.to_be_bytes());
            b[16] = 2;
            b[27..35].copy_from_slice(&ts.to_be_bytes());
            b[35..43].copy_from_slice(&ts.to_be_bytes());
            b[61 - 4..].copy_from_slice(&1i32.to_be_bytes());
            let _ = i;
            g.append(&b).unwrap();
        }
    }
    let out = call(&e.st, 2, 1, |w| {
        w.i32(-1).arr(1).str("ts").arr(2);
        w.i32(0).i64(150);
        w.i32(0).i64(999);
    })
    .await;
    let mut r = Rd::new(&out, false);
    assert_eq!(r.arr().unwrap(), 1);
    r.str().unwrap();
    assert_eq!(r.arr().unwrap(), 2);
    r.i32().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    assert_eq!(r.i64().unwrap(), 200); // timestamp of first batch with maxTs >= 150
    assert_eq!(r.i64().unwrap(), 1); // its offset
    r.i32().unwrap();
    assert_eq!(r.i16().unwrap(), 0);
    assert_eq!(r.i64().unwrap(), -1);
    assert_eq!(r.i64().unwrap(), -1);
}

// ---------------------------------------------------------------------------
// Consumer group APIs on the wire
// ---------------------------------------------------------------------------

async fn find_coord(e: &Env, v: i16, key: &str) -> (i16, i32, String, i32) {
    let out = call(&e.st, 10, v, |w| {
        if v >= 4 {
            w.i8(0).arr(1).str(key).tagged();
        } else {
            w.str(key);
            if v >= 1 {
                w.i8(0);
            }
            w.tagged();
        }
    })
    .await;
    let mut r = Rd::new(&out, is_flexible(10, v));
    if v >= 1 {
        r.i32().unwrap();
    }
    if v >= 4 {
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), key);
        let id = r.i32().unwrap();
        let host = r.str().unwrap();
        let port = r.i32().unwrap();
        let code = r.i16().unwrap();
        (code, id, host, port)
    } else {
        let code = r.i16().unwrap();
        if v >= 1 {
            r.nstr().unwrap();
        }
        (code, r.i32().unwrap(), r.str().unwrap(), r.i32().unwrap())
    }
}

async fn join(e: &Env, v: i16, group: &str, member: &str) -> (i16, i32, String, String, Vec<String>) {
    let out = call(&e.st, 11, v, |w| {
        w.str(group).i32(10_000);
        if v >= 1 {
            w.i32(5_000);
        }
        w.str(member);
        if v >= 5 {
            w.nstr(None);
        }
        w.str("consumer").arr(1).str("range").bytes(b"meta").tagged();
        if v >= 8 {
            w.nstr(Some("test"));
        }
        w.tagged();
    })
    .await;
    let mut r = Rd::new(&out, is_flexible(11, v));
    if v >= 2 {
        r.i32().unwrap();
    }
    let code = r.i16().unwrap();
    let generation = r.i32().unwrap();
    if v >= 7 {
        assert_eq!(r.nstr().unwrap().as_deref(), Some("consumer"));
        assert_eq!(r.nstr().unwrap().as_deref(), Some("range"));
    } else {
        assert_eq!(r.str().unwrap(), "range");
    }
    let leader = r.str().unwrap();
    if v >= 9 {
        assert!(!r.bool().unwrap());
    }
    let member_id = r.str().unwrap();
    let mut members = vec![];
    for _ in 0..r.arr().unwrap() {
        members.push(r.str().unwrap());
        if v >= 5 {
            r.nstr().unwrap();
        }
        assert_eq!(r.bytes().unwrap(), b"meta");
        r.tagged().unwrap();
    }
    (code, generation, leader, member_id, members)
}

#[tokio::test]
async fn find_coordinator_all_versions() {
    let e = env();
    for v in 0..=4i16 {
        let (code, id, host, port) = find_coord(&e, v, "some-group").await;
        assert_eq!(code, 0, "v{v}");
        assert!((1..=3).contains(&id));
        assert_eq!(host, format!("host{id}"));
        assert_eq!(port, 9000 + id);
    }
    e.st.ensure_fresh().await; // populated by find_coord already
    // stable
    assert_eq!(find_coord(&e, 3, "g").await, find_coord(&e, 3, "g").await);
    // empty key
    let (code, id, _, _) = find_coord(&e, 3, "").await;
    assert_eq!((code, id), (24, -1));
}

#[tokio::test]
async fn full_group_lifecycle_on_the_wire() {
    for (i, v) in [(0, 0i16), (1, 2), (2, 5), (3, 6), (4, 9)] {
        let e = env_with(&[(1, None)], 1);
        let group = format!("wire-group-{i}");
        let (code, generation, leader, member, members) = join(&e, v, &group, "").await;
        assert_eq!((code, generation), (0, 1), "join v{v}");
        assert_eq!(leader, member);
        assert_eq!(members, vec![member.clone()]);

        // SyncGroup
        let sv = [0i16, 1, 3, 4, 5][i];
        let out = call(&e.st, 14, sv, |w| {
            w.str(&group).i32(generation).str(&member);
            if sv >= 3 {
                w.nstr(None);
            }
            if sv >= 5 {
                w.nstr(Some("consumer")).nstr(Some("range"));
            }
            w.arr(1).str(&member).bytes(b"my-assignment").tagged();
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(14, sv));
        if sv >= 1 {
            r.i32().unwrap();
        }
        assert_eq!(r.i16().unwrap(), 0);
        if sv >= 5 {
            assert_eq!(r.nstr().unwrap().as_deref(), Some("consumer"));
            assert_eq!(r.nstr().unwrap().as_deref(), Some("range"));
        }
        assert_eq!(r.bytes().unwrap(), b"my-assignment");

        // Heartbeat ok + bad generation
        for (g_, expect) in [(generation, 0i16), (generation + 5, 22)] {
            let hv = [0i16, 1, 3, 4, 4][i];
            let out = call(&e.st, 12, hv, |w| {
                w.str(&group).i32(g_).str(&member);
                if hv >= 3 {
                    w.nstr(None);
                }
                w.tagged();
            })
            .await;
            let mut r = Rd::new(&out, is_flexible(12, hv));
            if hv >= 1 {
                r.i32().unwrap();
            }
            assert_eq!(r.i16().unwrap(), expect);
        }

        // OffsetCommit (v0/v2/v5/v7/v8 choices per iteration)
        let ov = [2i16, 2, 5, 7, 8][i];
        let out = call(&e.st, 8, ov, |w| {
            w.str(&group);
            if ov >= 1 {
                w.i32(generation).str(&member);
            }
            if ov >= 7 {
                w.nstr(None);
            }
            if (2..=4).contains(&ov) {
                w.i64(-1);
            }
            w.arr(1).str("orders").arr(2);
            for (p, off) in [(0, 42i64), (1, 7)] {
                w.i32(p).i64(off);
                if ov >= 6 {
                    w.i32(-1);
                }
                w.nstr(Some("md")).tagged();
            }
            w.tagged().tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(8, ov));
        if ov >= 3 {
            r.i32().unwrap();
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), "orders");
        assert_eq!(r.arr().unwrap(), 2);
        r.i32().unwrap();
        assert_eq!(r.i16().unwrap(), 0, "commit v{ov}");
        assert_eq!(*e.mock.commits.lock().unwrap(), 1, "write-through to controller");
        assert_eq!(e.mock.offsets.lock().unwrap()[&(group.clone(), "orders".to_string(), 0)], 42);

        // OffsetFetch for committed and unknown partitions
        let fv = [0i16, 2, 5, 6, 7][i];
        let out = call(&e.st, 9, fv, |w| {
            w.str(&group).arr(1).str("orders").arr(3).i32(0).i32(1).i32(9).tagged();
            if fv >= 7 {
                w.bool(false);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(9, fv));
        if fv >= 3 {
            r.i32().unwrap();
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), "orders");
        assert_eq!(r.arr().unwrap(), 3);
        let mut got = vec![];
        for _ in 0..3 {
            let p = r.i32().unwrap();
            let off = r.i64().unwrap();
            if fv >= 5 {
                r.i32().unwrap();
            }
            let md = r.nstr().unwrap();
            let err = r.i16().unwrap();
            r.tagged().unwrap();
            got.push((p, off, md, err));
        }
        assert_eq!(got[0], (0, 42, Some("md".to_string()), 0));
        assert_eq!(got[1].1, 7);
        assert_eq!((got[2].1, got[2].3), (-1, 0));

        // ListGroups + DescribeGroups
        let lv = [0i16, 1, 3, 4, 4][i];
        let out = call(&e.st, 16, lv, |w| {
            if lv >= 4 {
                w.arr(0);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(16, lv));
        if lv >= 1 {
            r.i32().unwrap();
        }
        assert_eq!(r.i16().unwrap(), 0);
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), group);
        assert_eq!(r.str().unwrap(), "consumer");
        if lv >= 4 {
            assert_eq!(r.str().unwrap(), "Stable");
        }

        let dv = [0i16, 1, 3, 4, 5][i];
        let out = call(&e.st, 15, dv, |w| {
            w.arr(1).str(&group);
            if dv >= 3 {
                w.bool(false);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(15, dv));
        if dv >= 1 {
            r.i32().unwrap();
        }
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.i16().unwrap(), 0);
        assert_eq!(r.str().unwrap(), group);
        assert_eq!(r.str().unwrap(), "Stable");
        assert_eq!(r.str().unwrap(), "consumer");
        assert_eq!(r.str().unwrap(), "range");
        assert_eq!(r.arr().unwrap(), 1);
        assert_eq!(r.str().unwrap(), member);
        if dv >= 4 {
            r.nstr().unwrap();
        }
        assert_eq!(r.str().unwrap(), "test-client");
        r.str().unwrap();
        assert_eq!(r.bytes().unwrap(), b"meta");
        assert_eq!(r.bytes().unwrap(), b"my-assignment");

        // LeaveGroup
        let xv = [0i16, 1, 3, 4, 5][i];
        let out = call(&e.st, 13, xv, |w| {
            w.str(&group);
            if xv >= 3 {
                w.arr(1).str(&member).nstr(None);
                if xv >= 5 {
                    w.nstr(Some("bye"));
                }
                w.tagged();
            } else {
                w.str(&member);
            }
            w.tagged();
        })
        .await;
        let mut r = Rd::new(&out, is_flexible(13, xv));
        if xv >= 1 {
            r.i32().unwrap();
        }
        assert_eq!(r.i16().unwrap(), 0);
        if xv >= 3 {
            assert_eq!(r.arr().unwrap(), 1);
            r.str().unwrap();
            r.nstr().unwrap();
            assert_eq!(r.i16().unwrap(), 0);
        }
        assert_eq!(e.st.groups.group_state(&group), Some(crate::kafka::groups::GroupState::Empty));
    }
}

#[tokio::test]
async fn offsets_survive_coordinator_move_via_controller() {
    let e = env_with(&[(1, None)], 1);
    // controller already knows an offset (as after a coordinator failover)
    e.mock.offsets.lock().unwrap().insert(("moved".into(), "t".into(), 0), 99);
    let out = call(&e.st, 9, 1, |w| {
        w.str("moved").arr(1).str("t").arr(1).i32(0);
    })
    .await;
    let mut r = Rd::new(&out, false);
    r.arr().unwrap();
    r.str().unwrap();
    r.arr().unwrap();
    r.i32().unwrap();
    assert_eq!(r.i64().unwrap(), 99);
}

#[tokio::test]
async fn non_coordinator_broker_rejects_group_requests() {
    // Find a group whose coordinator is NOT broker 1 in a 3-broker cluster.
    let e = env();
    e.st.ensure_fresh().await;
    let group = (0..50).map(|i| format!("g{i}")).find(|g| e.st.coordinator_node(g).0 != 1).unwrap();
    let out = call(&e.st, 11, 1, |w| {
        w.str(&group).i32(10_000).i32(5_000).str("").str("consumer").arr(1).str("range").bytes(b"m");
    })
    .await;
    let mut r = Rd::new(&out, false);
    assert_eq!(r.i16().unwrap(), 16, "NOT_COORDINATOR");
    // heartbeat too
    let out = call(&e.st, 12, 0, |w| {
        w.str(&group).i32(1).str("m");
    })
    .await;
    assert_eq!(Rd::new(&out, false).i16().unwrap(), 16);
}

#[tokio::test]
async fn unsupported_version_and_unknown_api() {
    let e = env();
    assert!(dispatch(&e.st, 19, 99, 1, "c", &[]).await.unwrap().is_err());
    assert!(dispatch(&e.st, 999, 0, 1, "c", &[]).await.is_none());
    // truncated body must error, not panic
    assert!(dispatch(&e.st, 19, 0, 1, "c", &[0, 0]).await.unwrap().is_err());
}

#[test]
fn topic_config_validation() {
    let cfg = BrokerConfig::default();
    let kv = |k: &str, v: &str| vec![(k.to_string(), Some(v.to_string()))];
    assert!(validate_topic_configs(&cfg, &kv("retention.ms", "1000")).is_none());
    assert!(validate_topic_configs(&cfg, &kv("retention.ms", "x")).is_some());
    assert!(validate_topic_configs(&cfg, &kv("preallocate", "maybe")).is_some());
    assert!(validate_topic_configs(&cfg, &kv("min.cleanable.dirty.ratio", "2.0")).is_some());
    assert!(validate_topic_configs(&cfg, &kv("bogus", "1")).is_some());
    assert!(validate_topic_configs(&cfg, &[("retention.ms".into(), None)]).is_none());
    assert!(valid_topic_name("a.b_c-1"));
    assert!(!valid_topic_name("") && !valid_topic_name("..") && !valid_topic_name("a b"));
}

#[test]
fn topic_names_helper() {
    let mut s = Snapshot::default();
    s.topics.insert("x".into(), TopicInfo { partitions: BTreeMap::new(), configs: BTreeMap::new() });
    assert_eq!(topic_names(&s)["x"], 0);
}
