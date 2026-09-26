//! Minimal Avro object-container-file writer plus the Iceberg v2 manifest and
//! manifest-list encoders (null codec, no partition fields).

use super::parquet::FileStats;

pub fn write_long(buf: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        buf.push((z as u8 & 0x7f) | 0x80);
        z >>= 7;
    }
    buf.push(z as u8);
}

pub fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_long(buf, s.len() as i64);
    buf.extend_from_slice(s.as_bytes());
}

/// Builds an Avro object container file with the given schema, metadata and
/// already-encoded records.
pub fn container(schema: &str, extra_meta: &[(&str, String)], sync: [u8; 16], records: &[Vec<u8>]) -> Vec<u8> {
    let mut out = b"Obj\x01".to_vec();
    let mut meta: Vec<(&str, Vec<u8>)> = vec![
        ("avro.schema", schema.as_bytes().to_vec()),
        ("avro.codec", b"null".to_vec()),
    ];
    for (k, v) in extra_meta {
        meta.push((k, v.as_bytes().to_vec()));
    }
    write_long(&mut out, meta.len() as i64);
    for (k, v) in &meta {
        write_str(&mut out, k);
        write_long(&mut out, v.len() as i64);
        out.extend_from_slice(v);
    }
    write_long(&mut out, 0);
    out.extend_from_slice(&sync);
    if !records.is_empty() {
        let body: Vec<u8> = records.concat();
        write_long(&mut out, records.len() as i64);
        write_long(&mut out, body.len() as i64);
        out.extend_from_slice(&body);
        out.extend_from_slice(&sync);
    }
    out
}

pub const MANIFEST_ENTRY_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
{"name":"status","type":"int","field-id":0},
{"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},
{"name":"data_sequence_number","type":["null","long"],"default":null,"field-id":3},
{"name":"file_sequence_number","type":["null","long"],"default":null,"field-id":4},
{"name":"data_file","type":{"type":"record","name":"r2","fields":[
{"name":"content","type":"int","field-id":134},
{"name":"file_path","type":"string","field-id":100},
{"name":"file_format","type":"string","field-id":101},
{"name":"partition","type":{"type":"record","name":"r102","fields":[]},"field-id":102},
{"name":"record_count","type":"long","field-id":103},
{"name":"file_size_in_bytes","type":"long","field-id":104},
{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]},"logicalType":"map"}],"default":null,"field-id":108},
{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"default":null,"field-id":109},
{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"default":null,"field-id":110}
]},"field-id":2}
]}"#;

pub const MANIFEST_FILE_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
{"name":"manifest_path","type":"string","field-id":500},
{"name":"manifest_length","type":"long","field-id":501},
{"name":"partition_spec_id","type":"int","field-id":502},
{"name":"content","type":"int","field-id":517},
{"name":"sequence_number","type":"long","field-id":515},
{"name":"min_sequence_number","type":"long","field-id":516},
{"name":"added_snapshot_id","type":"long","field-id":503},
{"name":"added_files_count","type":"int","field-id":504},
{"name":"existing_files_count","type":"int","field-id":505},
{"name":"deleted_files_count","type":"int","field-id":506},
{"name":"added_rows_count","type":"long","field-id":512},
{"name":"existing_rows_count","type":"long","field-id":513},
{"name":"deleted_rows_count","type":"long","field-id":514},
{"name":"partitions","type":["null",{"type":"array","items":{"type":"record","name":"r508","fields":[
{"name":"contains_null","type":"boolean","field-id":509},
{"name":"contains_nan","type":["null","boolean"],"default":null,"field-id":518},
{"name":"lower_bound","type":["null","bytes"],"default":null,"field-id":510},
{"name":"upper_bound","type":["null","bytes"],"default":null,"field-id":511}]},"element-id":508}],"default":null,"field-id":507},
{"name":"key_metadata","type":["null","bytes"],"default":null,"field-id":519}
]}"#;

#[derive(Debug, Clone)]
pub struct DataFile {
    pub path: String,
    pub size: i64,
    pub stats: FileStats,
}

fn write_map(buf: &mut Vec<u8>, m: &[(i32, i64)]) {
    write_long(buf, 1); // union index: array
    if !m.is_empty() {
        write_long(buf, m.len() as i64);
        for (k, v) in m {
            write_long(buf, *k as i64);
            write_long(buf, *v);
        }
    }
    write_long(buf, 0);
}

fn opt_long(buf: &mut Vec<u8>, v: i64) {
    write_long(buf, 1);
    write_long(buf, v);
}

/// Encode one ADDED manifest entry (status=1).
pub fn encode_entry(f: &DataFile, snapshot_id: i64, seq: i64) -> Vec<u8> {
    let mut b = Vec::new();
    write_long(&mut b, 1); // status ADDED
    opt_long(&mut b, snapshot_id);
    opt_long(&mut b, seq);
    opt_long(&mut b, seq);
    write_long(&mut b, 0); // content DATA
    write_str(&mut b, &f.path);
    write_str(&mut b, "PARQUET");
    // empty partition record: no bytes
    write_long(&mut b, f.stats.record_count);
    write_long(&mut b, f.size);
    write_map(&mut b, &f.stats.column_sizes);
    write_map(&mut b, &f.stats.value_counts);
    write_map(&mut b, &f.stats.null_counts);
    b
}

#[derive(Debug, Clone)]
pub struct ManifestFile {
    pub path: String,
    pub length: i64,
    pub seq: i64,
    pub min_seq: i64,
    pub snapshot_id: i64,
    pub added_files: i32,
    pub added_rows: i64,
}

pub fn encode_manifest_file(m: &ManifestFile) -> Vec<u8> {
    let mut b = Vec::new();
    write_str(&mut b, &m.path);
    write_long(&mut b, m.length);
    write_long(&mut b, 0); // spec id
    write_long(&mut b, 0); // content: data
    write_long(&mut b, m.seq);
    write_long(&mut b, m.min_seq);
    write_long(&mut b, m.snapshot_id);
    write_long(&mut b, m.added_files as i64);
    write_long(&mut b, 0);
    write_long(&mut b, 0);
    write_long(&mut b, m.added_rows);
    write_long(&mut b, 0);
    write_long(&mut b, 0);
    write_long(&mut b, 0); // partitions: null
    write_long(&mut b, 0); // key_metadata: null
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schemas_are_valid_json() {
        use super::super::jsonlite::Json;
        Json::parse(MANIFEST_ENTRY_SCHEMA).unwrap();
        Json::parse(MANIFEST_FILE_SCHEMA).unwrap();
    }
    #[test]
    fn zigzag() {
        let mut b = Vec::new();
        write_long(&mut b, -1);
        write_long(&mut b, 64);
        assert_eq!(b, vec![1, 0x80, 1]);
    }
}
