use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{error, info};

use super::jsonlite::Json;
use super::parquet::{ColType, Column, Val};
use super::table::{IcebergTable, Warehouse};
use super::{IcebergConfig, IcebergTopicConfig};
use crate::kafka::handlers::parse_records;
use crate::log::LogManager;

#[derive(Debug, Clone)]
pub struct TopicStatus {
    pub topic: String,
    pub mode: String,
    pub snapshots: usize,
    pub total_records: i64,
    pub location: String,
    pub offsets: BTreeMap<u32, i64>,
}

/// Builds the Iceberg/Parquet column list for a topic config.
/// Fixed columns: partition(1) offset(2) timestamp(3) key(4) value(5) headers(6);
/// json-mode columns get ids 10+.
pub fn build_columns(tc: &IcebergTopicConfig) -> Result<Vec<Column>, String> {
    let mut cols = vec![
        Column { id: 1, name: "partition".into(), ty: ColType::Int },
        Column { id: 2, name: "offset".into(), ty: ColType::Long },
        Column { id: 3, name: "timestamp".into(), ty: ColType::TimestampTz },
        Column { id: 4, name: "key".into(), ty: ColType::Binary },
        Column { id: 5, name: "value".into(), ty: ColType::Binary },
        Column { id: 6, name: "headers".into(), ty: ColType::String },
    ];
    match tc.mode.as_str() {
        "key_value" => {}
        "json" => {
            for (i, f) in tc.json_fields.iter().enumerate() {
                let (n, t) = f.split_once(':').ok_or_else(|| format!("bad json field spec {:?}", f))?;
                let ty = match t {
                    "string" => ColType::String,
                    "long" => ColType::Long,
                    "int" => ColType::Int,
                    "double" => ColType::Double,
                    "boolean" => ColType::Boolean,
                    o => return Err(format!("unsupported json field type {:?}", o)),
                };
                cols.push(Column { id: 10 + i as i32, name: n.to_string(), ty });
            }
        }
        m => return Err(format!("unsupported iceberg mode {:?}", m)),
    }
    Ok(cols)
}

fn json_to_val(ty: ColType, j: Option<&Json>) -> Val {
    match (ty, j) {
        (_, None) | (_, Some(Json::Null)) => Val::Null,
        (ColType::String, Some(Json::Str(s))) => Val::Bytes(s.clone().into_bytes()),
        (ColType::String, Some(o)) => Val::Bytes(o.to_json_string().into_bytes()),
        (ColType::Long, Some(v)) => v.as_i64().map(Val::Long).unwrap_or(Val::Null),
        (ColType::Int, Some(v)) => v.as_i64().map(|i| Val::Int(i as i32)).unwrap_or(Val::Null),
        (ColType::Double, Some(Json::Int(i))) => Val::Double(*i as f64),
        (ColType::Double, Some(Json::Float(f))) => Val::Double(*f),
        (ColType::Boolean, Some(Json::Bool(b))) => Val::Bool(*b),
        _ => Val::Null,
    }
}

pub struct IcebergManager {
    cfg: IcebergConfig,
    wh: Arc<Warehouse>,
    log: Arc<LogManager>,
    topics: Mutex<HashMap<String, IcebergTopicConfig>>,
    tables: Mutex<HashMap<String, IcebergTable>>,
}

impl IcebergManager {
    pub fn new(cfg: IcebergConfig, wh: Arc<Warehouse>, log: Arc<LogManager>) -> Arc<Self> {
        let topics = cfg.topics.iter().map(|t| (t.name.clone(), t.clone())).collect();
        Arc::new(Self { cfg, wh, log, topics: Mutex::new(topics), tables: Mutex::new(HashMap::new()) })
    }

    /// Runtime enable (e.g. from a topic config `iceberg.enabled=true`).
    pub async fn enable_topic(&self, tc: IcebergTopicConfig) -> Result<(), String> {
        build_columns(&tc)?;
        self.topics.lock().await.insert(tc.name.clone(), tc);
        Ok(())
    }

    pub async fn disable_topic(&self, topic: &str) {
        self.topics.lock().await.remove(topic);
        self.tables.lock().await.remove(topic);
    }

    pub async fn status(&self) -> Vec<TopicStatus> {
        let topics = self.topics.lock().await.clone();
        let tables = self.tables.lock().await;
        let mut out: Vec<TopicStatus> = topics
            .values()
            .map(|tc| match tables.get(&tc.name) {
                Some(t) => TopicStatus {
                    topic: tc.name.clone(),
                    mode: tc.mode.clone(),
                    snapshots: t.state.snapshots.len(),
                    total_records: t.total_records(),
                    location: t.location(),
                    offsets: t.state.offsets.clone(),
                },
                None => TopicStatus {
                    topic: tc.name.clone(),
                    mode: tc.mode.clone(),
                    snapshots: 0,
                    total_records: 0,
                    location: String::new(),
                    offsets: BTreeMap::new(),
                },
            })
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }

    async fn read_entry(&self, topic: &str, partition: u32, offset: u64) -> Option<Vec<u8>> {
        let part = self.log.get_partition(topic, partition).await.ok()?;
        let mut guard = part.lock().await;
        let (file, pos, len) = guard.read_from_offset(offset, 8 * 1024 * 1024).ok()??;
        drop(guard);
        let mut buf = vec![0u8; len as usize];
        file.read_exact_at(&mut buf, pos).ok()?;
        Some(buf)
    }

    /// Tails all partitions of `topic` from the last committed offsets and
    /// commits one snapshot. Returns Some(snapshot_id) if anything was committed.
    pub async fn commit_topic(&self, topic: &str) -> Result<Option<i64>, String> {
        let tc = self.topics.lock().await.get(topic).cloned().ok_or("topic not iceberg-enabled")?;
        let cols = build_columns(&tc)?;
        let mut tables = self.tables.lock().await;
        if !tables.contains_key(topic) {
            let t = IcebergTable::open(self.wh.clone(), &self.cfg.namespace, topic, cols.clone()).await?;
            tables.insert(topic.to_string(), t);
        }
        let table = tables.get_mut(topic).unwrap();

        let mut rows: Vec<Vec<Val>> = Vec::new();
        let mut new_offsets: BTreeMap<u32, i64> = BTreeMap::new();
        'outer: for p in self.log.partitions_for_topic(topic).await {
            let hw = match self.log.get_partition(topic, p).await {
                Ok(pl) => pl.lock().await.high_watermark,
                Err(_) => continue,
            };
            let mut next = table.state.offsets.get(&p).copied().unwrap_or(0) as u64;
            while next < hw {
                if rows.len() >= self.cfg.max_rows_per_commit {
                    new_offsets.insert(p, next as i64);
                    break 'outer;
                }
                let bytes = match self.read_entry(topic, p, next).await {
                    Some(b) => b,
                    None => break,
                };
                let recs = parse_records(&bytes).map_err(|e| format!("decode {}[{}]@{}: {:?}", topic, p, next, e))?;
                for r in recs {
                    let mut row = vec![
                        Val::Int(p as i32),
                        Val::Long(next as i64),
                        Val::Long(r.timestamp * 1000),
                        r.key.clone().map(Val::Bytes).unwrap_or(Val::Null),
                        r.value.clone().map(Val::Bytes).unwrap_or(Val::Null),
                        Val::Bytes(
                            Json::Arr(
                                r.headers
                                    .iter()
                                    .map(|(k, v)| {
                                        Json::obj(vec![("key", Json::s(k)), ("value", Json::Str(String::from_utf8_lossy(v).into()))])
                                    })
                                    .collect(),
                            )
                            .to_json_string()
                            .into_bytes(),
                        ),
                    ];
                    if tc.mode == "json" {
                        let parsed = r
                            .value
                            .as_ref()
                            .and_then(|v| Json::parse(&String::from_utf8_lossy(v)).ok());
                        for c in &cols[6..] {
                            row.push(json_to_val(c.ty, parsed.as_ref().and_then(|j| j.get(&c.name))));
                        }
                    }
                    rows.push(row);
                }
                next += 1;
            }
            if next > table.state.offsets.get(&p).copied().unwrap_or(0) as u64 {
                new_offsets.insert(p, next as i64);
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        table.commit(rows, new_offsets).await.map(Some)
    }

    pub async fn commit_all(&self) {
        let names: Vec<String> = self.topics.lock().await.keys().cloned().collect();
        for n in names {
            match self.commit_topic(&n).await {
                Ok(Some(id)) => info!("[AeroStream Iceberg] committed snapshot {} for topic {}", id, n),
                Ok(None) => {}
                Err(e) => error!("[AeroStream Iceberg] commit failed for topic {}: {}", n, e),
            }
        }
    }

    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let every = Duration::from_secs(self.cfg.commit_interval_secs.max(1));
        tokio::spawn(async move {
            let mut t = tokio::time::interval(every);
            loop {
                t.tick().await;
                self.commit_all().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::handlers::{encode_single_record_batch, KafkaRecord};

    #[tokio::test]
    async fn tail_and_commit_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        // ICEBERG_KEEP_DIR: write into a fixed dir so pyiceberg can validate it afterwards.
        let keep = std::env::var("ICEBERG_KEEP_DIR").ok();
        let wh_tmp = tempfile::tempdir().unwrap();
        let wh_path = keep.map(std::path::PathBuf::from).unwrap_or_else(|| wh_tmp.path().to_path_buf());
        let _ = std::fs::remove_dir_all(&wh_path);
        struct P(std::path::PathBuf);
        impl P { fn path(&self) -> &std::path::Path { &self.0 } }
        let wh_dir = P(wh_path);
        let lm = Arc::new(LogManager::new(dir.path(), 1).with_limits(1 << 20, None, None));
        {
            let p = lm.get_partition("events", 0).await.unwrap();
            let mut g = p.lock().await;
            for i in 0..5 {
                let rec = KafkaRecord::new(
                    Some(format!("k{}", i).into_bytes()),
                    Some(format!(r#"{{"user":"u{}","n":{},"ok":true}}"#, i, i).into_bytes()),
                );
                g.append(&encode_single_record_batch(0, &rec)).unwrap();
            }
        }
        let cfg = IcebergConfig::default();
        let wh = Arc::new(Warehouse::local(wh_dir.path().to_str().unwrap()));
        let mgr = IcebergManager::new(cfg, wh.clone(), lm.clone());
        mgr.enable_topic(IcebergTopicConfig {
            name: "events".into(),
            mode: "json".into(),
            json_fields: vec!["user:string".into(), "n:long".into(), "ok:boolean".into()],
        })
        .await
        .unwrap();
        let s1 = mgr.commit_topic("events").await.unwrap().unwrap();
        assert!(mgr.commit_topic("events").await.unwrap().is_none());
        // more data -> second snapshot, resumes from stored offsets (even after restart)
        {
            let p = lm.get_partition("events", 0).await.unwrap();
            let rec = KafkaRecord::new(None, Some(br#"{"user":"z","n":9}"#.to_vec()));
            p.lock().await.append(&encode_single_record_batch(0, &rec)).unwrap();
        }
        let mgr2 = IcebergManager::new(IcebergConfig::default(), wh, lm);
        mgr2.enable_topic(IcebergTopicConfig {
            name: "events".into(),
            mode: "json".into(),
            json_fields: vec!["user:string".into(), "n:long".into(), "ok:boolean".into()],
        })
        .await
        .unwrap();
        let s2 = mgr2.commit_topic("events").await.unwrap().unwrap();
        assert_ne!(s1, s2);
        let st = mgr2.status().await;
        assert_eq!(st[0].total_records, 6);
        assert_eq!(st[0].snapshots, 2);
        let base = wh_dir.path().join("aerostream/events/metadata");
        assert!(base.join("v2.metadata.json").exists());
        assert_eq!(std::fs::read_to_string(base.join("version-hint.text")).unwrap(), "2");
    }
}
