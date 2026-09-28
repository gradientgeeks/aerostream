//! Iceberg v2 table writer: data file + manifest + manifest list + metadata.json
//! commit to a warehouse (local filesystem or an object store provider).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::avro::{self, DataFile, ManifestFile};
use super::jsonlite::Json;
use super::parquet::{write_parquet, Column, Val};
use crate::storage::TieredStorageProvider;

// ---------------- ids ----------------

static COUNTER: AtomicU64 = AtomicU64::new(1);

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn rand_u64() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    splitmix(nanos ^ splitmix(COUNTER.fetch_add(1, Ordering::Relaxed)) ^ (std::process::id() as u64) << 32)
}

pub fn new_uuid() -> String {
    let a = rand_u64();
    let b = rand_u64();
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&a.to_be_bytes());
    bytes[8..].copy_from_slice(&b.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let h: Vec<String> = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    format!(
        "{}-{}-{}-{}-{}",
        h[0..4].concat(),
        h[4..6].concat(),
        h[6..8].concat(),
        h[8..10].concat(),
        h[10..16].concat()
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------- warehouse ----------------

pub enum Warehouse {
    Local { root: PathBuf },
    Remote { provider: Arc<dyn TieredStorageProvider>, bucket: String, prefix: String },
}

impl Warehouse {
    /// Parse a warehouse URI: `file:///abs/dir`, a bare path, or `s3://bucket/prefix`
    /// (the caller supplies the provider for remote schemes).
    pub fn local(path: &str) -> Warehouse {
        let p = path.strip_prefix("file://").unwrap_or(path);
        Warehouse::Local { root: PathBuf::from(p) }
    }

    pub fn base_uri(&self) -> String {
        match self {
            Warehouse::Local { root } => {
                let abs = std::fs::canonicalize(root).unwrap_or_else(|_| {
                    if root.is_absolute() { root.clone() } else { std::env::current_dir().unwrap_or_default().join(root) }
                });
                format!("file://{}", abs.display())
            }
            Warehouse::Remote { bucket, prefix, .. } => {
                if prefix.is_empty() { format!("s3://{}", bucket) } else { format!("s3://{}/{}", bucket, prefix) }
            }
        }
    }

    pub async fn put(&self, rel: &str, data: &[u8]) -> Result<(), String> {
        match self {
            Warehouse::Local { root } => {
                let p = root.join(rel);
                if let Some(d) = p.parent() {
                    tokio::fs::create_dir_all(d).await.map_err(|e| e.to_string())?;
                }
                let tmp = p.with_extension(format!("tmp{}", rand_u64() % 100000));
                tokio::fs::write(&tmp, data).await.map_err(|e| e.to_string())?;
                tokio::fs::rename(&tmp, &p).await.map_err(|e| e.to_string())
            }
            Warehouse::Remote { provider, prefix, .. } => {
                let key = if prefix.is_empty() { rel.to_string() } else { format!("{}/{}", prefix, rel) };
                provider.put_segment(&key, data).await.map_err(|e| e.to_string())
            }
        }
    }

    pub async fn get(&self, rel: &str) -> Result<Option<Vec<u8>>, String> {
        match self {
            Warehouse::Local { root } => match tokio::fs::read(root.join(rel)).await {
                Ok(d) => Ok(Some(d)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.to_string()),
            },
            Warehouse::Remote { provider, prefix, .. } => {
                let key = if prefix.is_empty() { rel.to_string() } else { format!("{}/{}", prefix, rel) };
                match provider.exists(&key).await {
                    Ok(true) => provider.get_segment(&key).await.map(Some).map_err(|e| e.to_string()),
                    Ok(false) => Ok(None),
                    Err(e) => Err(e.to_string()),
                }
            }
        }
    }
}

// ---------------- table ----------------

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub id: i64,
    pub parent: Option<i64>,
    pub seq: i64,
    pub ts_ms: i64,
    pub manifest_list: String,
    pub added_records: i64,
    pub added_files: i64,
    pub total_records: i64,
}

#[derive(Debug, Clone)]
pub struct TableState {
    pub uuid: String,
    pub version: i64,
    pub last_seq: i64,
    pub last_updated_ms: i64,
    pub manifests: Vec<ManifestFile>,
    pub snapshots: Vec<SnapshotInfo>,
    pub offsets: BTreeMap<u32, i64>,
    pub metadata_log: Vec<(i64, String)>,
}

pub struct IcebergTable {
    wh: Arc<Warehouse>,
    pub namespace: String,
    pub name: String,
    pub cols: Vec<Column>,
    pub state: TableState,
}

impl IcebergTable {
    fn rel(&self, p: &str) -> String {
        format!("{}/{}/{}", self.namespace, self.name, p)
    }
    pub fn location(&self) -> String {
        format!("{}/{}/{}", self.wh.base_uri(), self.namespace, self.name)
    }

    /// Opens an existing table (from its state sidecar) or prepares a new one.
    pub async fn open(wh: Arc<Warehouse>, namespace: &str, name: &str, cols: Vec<Column>) -> Result<Self, String> {
        let mut t = IcebergTable {
            wh,
            namespace: namespace.to_string(),
            name: name.to_string(),
            cols,
            state: TableState {
                uuid: new_uuid(),
                version: 0,
                last_seq: 0,
                last_updated_ms: now_ms(),
                manifests: vec![],
                snapshots: vec![],
                offsets: BTreeMap::new(),
                metadata_log: vec![],
            },
        };
        if let Some(bytes) = t.wh.get(&t.rel("metadata/aerostream-state.json")).await? {
            let j = Json::parse(&String::from_utf8_lossy(&bytes))?;
            t.state = parse_state(&j).ok_or("corrupt state sidecar")?;
        }
        Ok(t)
    }

    pub fn total_records(&self) -> i64 {
        self.state.snapshots.last().map(|s| s.total_records).unwrap_or(0)
    }

    /// Commit `rows` as one new append snapshot and record the consumed offsets.
    /// Returns the new snapshot id.
    pub async fn commit(&mut self, rows: Vec<Vec<Val>>, offsets: BTreeMap<u32, i64>) -> Result<i64, String> {
        if rows.is_empty() {
            return Err("nothing to commit".into());
        }
        let base = self.wh.base_uri();
        let uid = new_uuid();
        let snapshot_id = (rand_u64() >> 1) as i64 | 1;
        let seq = self.state.last_seq + 1;
        let ts = now_ms();

        // 1. data file
        let (pq, stats) = write_parquet(&self.cols, &rows);
        let data_rel = self.rel(&format!("data/{}.parquet", uid));
        self.wh.put(&data_rel, &pq).await?;
        let df = DataFile { path: format!("{}/{}", base, data_rel), size: pq.len() as i64, stats };

        // 2. manifest
        let mut sync = [0u8; 16];
        sync[..8].copy_from_slice(&rand_u64().to_be_bytes());
        sync[8..].copy_from_slice(&rand_u64().to_be_bytes());
        let schema_json = self.schema_json().to_json_string();
        let entry = avro::encode_entry(&df, snapshot_id, seq);
        let manifest_bytes = avro::container(
            avro::MANIFEST_ENTRY_SCHEMA,
            &[
                ("schema", schema_json),
                ("partition-spec", "[]".to_string()),
                ("partition-spec-id", "0".to_string()),
                ("format-version", "2".to_string()),
                ("content", "data".to_string()),
            ],
            sync,
            &[entry],
        );
        let manifest_rel = self.rel(&format!("metadata/{}-m0.avro", uid));
        self.wh.put(&manifest_rel, &manifest_bytes).await?;
        let mf = ManifestFile {
            path: format!("{}/{}", base, manifest_rel),
            length: manifest_bytes.len() as i64,
            seq,
            min_seq: seq,
            snapshot_id,
            added_files: 1,
            added_rows: df.stats.record_count,
        };

        // 3. manifest list (previous manifests + new one)
        let mut all = self.state.manifests.clone();
        all.push(mf.clone());
        let recs: Vec<Vec<u8>> = all.iter().map(avro::encode_manifest_file).collect();
        let parent = self.state.snapshots.last().map(|s| s.id);
        let ml_bytes = avro::container(
            avro::MANIFEST_FILE_SCHEMA,
            &[
                ("snapshot-id", snapshot_id.to_string()),
                ("parent-snapshot-id", parent.map(|p| p.to_string()).unwrap_or_else(|| "null".into())),
                ("sequence-number", seq.to_string()),
                ("format-version", "2".to_string()),
            ],
            sync,
            &recs,
        );
        let ml_rel = self.rel(&format!("metadata/snap-{}-1-{}.avro", snapshot_id, uid));
        self.wh.put(&ml_rel, &ml_bytes).await?;

        // 4. new state + metadata.json
        let added = df.stats.record_count;
        let mut st = self.state.clone();
        st.last_seq = seq;
        st.last_updated_ms = ts;
        st.manifests = all;
        st.snapshots.push(SnapshotInfo {
            id: snapshot_id,
            parent,
            seq,
            ts_ms: ts,
            manifest_list: format!("{}/{}", base, ml_rel),
            added_records: added,
            added_files: 1,
            total_records: self.total_records() + added,
        });
        for (p, o) in offsets {
            st.offsets.insert(p, o);
        }
        st.version += 1;
        let meta_rel = self.rel(&format!("metadata/v{}.metadata.json", st.version));
        if st.version > 1 {
            let prev = format!("{}/{}", base, self.rel(&format!("metadata/v{}.metadata.json", st.version - 1)));
            st.metadata_log.push((self.state.last_updated_ms, prev));
        }
        let meta = self.metadata_json(&st);
        self.wh.put(&meta_rel, meta.to_json_string().as_bytes()).await?;
        self.wh.put(&self.rel("metadata/version-hint.text"), st.version.to_string().as_bytes()).await?;
        self.wh
            .put(&self.rel("metadata/aerostream-state.json"), state_json(&st).to_json_string().as_bytes())
            .await?;
        self.state = st;
        Ok(snapshot_id)
    }

    fn schema_json(&self) -> Json {
        Json::obj(vec![
            ("type", Json::s("struct")),
            ("schema-id", Json::Int(0)),
            (
                "fields",
                Json::Arr(
                    self.cols
                        .iter()
                        .map(|c| {
                            Json::obj(vec![
                                ("id", Json::Int(c.id as i64)),
                                ("name", Json::s(&c.name)),
                                ("required", Json::Bool(false)),
                                ("type", Json::s(c.ty.iceberg_name())),
                            ])
                        })
                        .collect(),
                ),
            ),
        ])
    }

    pub fn metadata_json(&self, st: &TableState) -> Json {
        let last_col = self.cols.iter().map(|c| c.id).max().unwrap_or(0);
        let snaps: Vec<Json> = st
            .snapshots
            .iter()
            .map(|s| {
                let mut f = vec![
                    ("snapshot-id", Json::Int(s.id)),
                    ("sequence-number", Json::Int(s.seq)),
                    ("timestamp-ms", Json::Int(s.ts_ms)),
                    ("manifest-list", Json::s(&s.manifest_list)),
                    (
                        "summary",
                        Json::obj(vec![
                            ("operation", Json::s("append")),
                            ("added-data-files", Json::Str(s.added_files.to_string())),
                            ("added-records", Json::Str(s.added_records.to_string())),
                            ("total-records", Json::Str(s.total_records.to_string())),
                        ]),
                    ),
                    ("schema-id", Json::Int(0)),
                ];
                if let Some(p) = s.parent {
                    f.insert(1, ("parent-snapshot-id", Json::Int(p)));
                }
                Json::obj(f)
            })
            .collect();
        let offsets = Json::Obj(st.offsets.iter().map(|(p, o)| (p.to_string(), Json::Int(*o))).collect());
        Json::obj(vec![
            ("format-version", Json::Int(2)),
            ("table-uuid", Json::s(&st.uuid)),
            ("location", Json::Str(self.location())),
            ("last-sequence-number", Json::Int(st.last_seq)),
            ("last-updated-ms", Json::Int(st.last_updated_ms)),
            ("last-column-id", Json::Int(last_col as i64)),
            ("current-schema-id", Json::Int(0)),
            ("schemas", Json::Arr(vec![self.schema_json()])),
            ("default-spec-id", Json::Int(0)),
            (
                "partition-specs",
                Json::Arr(vec![Json::obj(vec![("spec-id", Json::Int(0)), ("fields", Json::Arr(vec![]))])]),
            ),
            ("last-partition-id", Json::Int(999)),
            ("default-sort-order-id", Json::Int(0)),
            (
                "sort-orders",
                Json::Arr(vec![Json::obj(vec![("order-id", Json::Int(0)), ("fields", Json::Arr(vec![]))])]),
            ),
            (
                "properties",
                Json::obj(vec![
                    ("write.format.default", Json::s("parquet")),
                    ("aerostream.source-offsets", Json::Str(offsets.to_json_string())),
                ]),
            ),
            (
                "current-snapshot-id",
                Json::Int(st.snapshots.last().map(|s| s.id).unwrap_or(-1)),
            ),
            ("snapshots", Json::Arr(snaps)),
            (
                "snapshot-log",
                Json::Arr(
                    st.snapshots
                        .iter()
                        .map(|s| Json::obj(vec![("timestamp-ms", Json::Int(s.ts_ms)), ("snapshot-id", Json::Int(s.id))]))
                        .collect(),
                ),
            ),
            (
                "metadata-log",
                Json::Arr(
                    st.metadata_log
                        .iter()
                        .map(|(t, f)| Json::obj(vec![("timestamp-ms", Json::Int(*t)), ("metadata-file", Json::s(f))]))
                        .collect(),
                ),
            ),
        ])
    }
}

fn state_json(st: &TableState) -> Json {
    Json::obj(vec![
        ("uuid", Json::s(&st.uuid)),
        ("version", Json::Int(st.version)),
        ("last_seq", Json::Int(st.last_seq)),
        ("last_updated_ms", Json::Int(st.last_updated_ms)),
        (
            "manifests",
            Json::Arr(
                st.manifests
                    .iter()
                    .map(|m| {
                        Json::obj(vec![
                            ("path", Json::s(&m.path)),
                            ("length", Json::Int(m.length)),
                            ("seq", Json::Int(m.seq)),
                            ("min_seq", Json::Int(m.min_seq)),
                            ("snapshot_id", Json::Int(m.snapshot_id)),
                            ("added_files", Json::Int(m.added_files as i64)),
                            ("added_rows", Json::Int(m.added_rows)),
                        ])
                    })
                    .collect(),
            ),
        ),
        (
            "snapshots",
            Json::Arr(
                st.snapshots
                    .iter()
                    .map(|s| {
                        Json::obj(vec![
                            ("id", Json::Int(s.id)),
                            ("parent", s.parent.map(Json::Int).unwrap_or(Json::Null)),
                            ("seq", Json::Int(s.seq)),
                            ("ts_ms", Json::Int(s.ts_ms)),
                            ("manifest_list", Json::s(&s.manifest_list)),
                            ("added_records", Json::Int(s.added_records)),
                            ("added_files", Json::Int(s.added_files)),
                            ("total_records", Json::Int(s.total_records)),
                        ])
                    })
                    .collect(),
            ),
        ),
        (
            "offsets",
            Json::Obj(st.offsets.iter().map(|(p, o)| (p.to_string(), Json::Int(*o))).collect()),
        ),
        (
            "metadata_log",
            Json::Arr(
                st.metadata_log
                    .iter()
                    .map(|(t, f)| Json::obj(vec![("ts", Json::Int(*t)), ("file", Json::s(f))]))
                    .collect(),
            ),
        ),
    ])
}

fn parse_state(j: &Json) -> Option<TableState> {
    let i = |o: &Json, k: &str| o.get(k).and_then(|v| v.as_i64());
    let manifests = j
        .get("manifests")?
        .as_arr()?
        .iter()
        .map(|m| {
            Some(ManifestFile {
                path: m.get("path")?.as_str()?.to_string(),
                length: i(m, "length")?,
                seq: i(m, "seq")?,
                min_seq: i(m, "min_seq")?,
                snapshot_id: i(m, "snapshot_id")?,
                added_files: i(m, "added_files")? as i32,
                added_rows: i(m, "added_rows")?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let snapshots = j
        .get("snapshots")?
        .as_arr()?
        .iter()
        .map(|s| {
            Some(SnapshotInfo {
                id: i(s, "id")?,
                parent: s.get("parent").and_then(|p| p.as_i64()),
                seq: i(s, "seq")?,
                ts_ms: i(s, "ts_ms")?,
                manifest_list: s.get("manifest_list")?.as_str()?.to_string(),
                added_records: i(s, "added_records")?,
                added_files: i(s, "added_files")?,
                total_records: i(s, "total_records")?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let mut offsets = BTreeMap::new();
    if let Some(Json::Obj(f)) = j.get("offsets") {
        for (k, v) in f {
            offsets.insert(k.parse().ok()?, v.as_i64()?);
        }
    }
    let metadata_log = j
        .get("metadata_log")?
        .as_arr()?
        .iter()
        .map(|m| Some((i(m, "ts")?, m.get("file")?.as_str()?.to_string())))
        .collect::<Option<Vec<_>>>()?;
    Some(TableState {
        uuid: j.get("uuid")?.as_str()?.to_string(),
        version: i(j, "version")?,
        last_seq: i(j, "last_seq")?,
        last_updated_ms: i(j, "last_updated_ms")?,
        manifests,
        snapshots,
        offsets,
        metadata_log,
    })
}
