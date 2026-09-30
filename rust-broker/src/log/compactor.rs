use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::log::manager::LogSegment;

// ---------------------------------------------------------------------------
// Extracted Record Info & Parsers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedKey {
    pub key: Option<Vec<u8>>,
    pub is_tombstone: bool,
    pub timestamp_ms: Option<i64>,
}

/// Helper to encode a generic key-value record:
/// `[key_len: u32 BE][key bytes][val_len: u32 BE][val bytes]`
#[allow(dead_code)]
pub fn encode_kv_record(key: &[u8], value: Option<&[u8]>) -> Vec<u8> {
    let key_len = key.len() as u32;
    let val_len = value.map_or(0, |v| v.len()) as u32;
    let mut buf = Vec::with_capacity(8 + key.len() + val_len as usize);
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&val_len.to_be_bytes());
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
    buf
}

/// Helper to encode a generic key-value record with a timestamp (epoch ms):
/// `[timestamp: i64 BE][key_len: u32 BE][key bytes][val_len: u32 BE][val bytes]`
#[allow(dead_code)]
pub fn encode_kv_record_with_timestamp(key: &[u8], value: Option<&[u8]>, timestamp_ms: i64) -> Vec<u8> {
    let key_len = key.len() as u32;
    let val_len = value.map_or(0, |v| v.len()) as u32;
    let mut buf = Vec::with_capacity(16 + key.len() + val_len as usize);
    buf.extend_from_slice(&timestamp_ms.to_be_bytes());
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&val_len.to_be_bytes());
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
    buf
}

/// Extracts key, tombstone flag, and timestamp from raw record payload.
/// Supports Kafka RecordBatch / MessageSet records as well as generic/native KV formats.
pub fn extract_key(data: &[u8]) -> ExtractedKey {
    // Transaction markers (control batches) are never compacted away: they carry no user key.
    if crate::txn::batch::is_control(data) {
        return ExtractedKey { key: None, is_tombstone: false, timestamp_ms: None };
    }
    // 1. Try parsing as Kafka RecordBatch (magic 2) or MessageSet (magic 0/1)
    if let Ok(records) = crate::kafka::handlers::parse_records(data) {
        if let Some(first) = records.into_iter().next() {
            let is_tombstone = first.value.as_ref().map_or(true, |v| v.is_empty());
            return ExtractedKey {
                key: first.key,
                is_tombstone,
                timestamp_ms: Some(first.timestamp),
            };
        }
    }

    // 2. Try generic timestamped KV format:
    // [ts: 8 bytes i64 BE][key_len: 4 bytes u32 BE][key][val_len: 4 bytes u32 BE][val]
    if data.len() >= 16 {
        let ts = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let key_len = u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize;
        if 12 + key_len + 4 <= data.len() {
            let val_len = u32::from_be_bytes(data[12 + key_len..16 + key_len].try_into().unwrap()) as usize;
            if 16 + key_len + val_len == data.len() {
                let key = data[12..12 + key_len].to_vec();
                let is_tombstone = val_len == 0;
                return ExtractedKey {
                    key: Some(key),
                    is_tombstone,
                    timestamp_ms: Some(ts),
                };
            }
        }
    }

    // 3. Try generic binary KV format:
    // [key_len: 4 bytes u32 BE][key][val_len: 4 bytes u32 BE][val]
    if data.len() >= 8 {
        let key_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        if 4 + key_len + 4 <= data.len() {
            let val_len = u32::from_be_bytes(data[4 + key_len..8 + key_len].try_into().unwrap()) as usize;
            if 8 + key_len + val_len == data.len() {
                let key = data[4..4 + key_len].to_vec();
                let is_tombstone = val_len == 0;
                return ExtractedKey {
                    key: Some(key),
                    is_tombstone,
                    timestamp_ms: None,
                };
            }
        }
    }

    // 4. Try magic-tagged KV format:
    // b"AEKV" [key_len: 4 bytes u32 BE][key][val_len: 4 bytes u32 BE][val]
    if data.len() >= 12 && &data[0..4] == b"AEKV" {
        let key_len = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        if 8 + key_len + 4 <= data.len() {
            let val_len = u32::from_be_bytes(data[8 + key_len..12 + key_len].try_into().unwrap()) as usize;
            if 12 + key_len + val_len == data.len() {
                let key = data[8..8 + key_len].to_vec();
                let is_tombstone = val_len == 0;
                return ExtractedKey {
                    key: Some(key),
                    is_tombstone,
                    timestamp_ms: None,
                };
            }
        }
    }

    // Fallback: unkeyed raw record
    ExtractedKey {
        key: None,
        is_tombstone: false,
        timestamp_ms: None,
    }
}

// ---------------------------------------------------------------------------
// Index Entries & Compaction Statistics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: u64,
    pub position: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub segments_scanned: usize,
    pub segments_cleaned: usize,
    pub segments_deleted: usize,
    pub records_scanned: usize,
    pub records_retained: usize,
    pub records_discarded: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

pub fn read_index_entries(idx_path: &Path) -> io::Result<Vec<IndexEntry>> {
    let mut file = File::open(idx_path)?;
    let len = file.metadata()?.len();
    let num_entries = (len / 16) as usize;
    let mut entries = Vec::with_capacity(num_entries);
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    for chunk in buf.chunks_exact(16) {
        let offset = u64::from_be_bytes(chunk[0..8].try_into().unwrap());
        let position = u64::from_be_bytes(chunk[8..16].try_into().unwrap());
        entries.push(IndexEntry { offset, position });
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Key Index & Dirty Ratio
// ---------------------------------------------------------------------------

/// Builds an in-memory key-to-latest-offset index for the given closed segments.
/// Returns `HashMap<Vec<u8>, (u64, bool)>` mapping `key` -> `(latest_offset, is_tombstone)`.
pub fn build_key_offset_map(
    segments: &[LogSegment],
) -> io::Result<HashMap<Vec<u8>, (u64, bool)>> {
    let mut map: HashMap<Vec<u8>, (u64, bool)> = HashMap::new();

    for seg in segments {
        if !seg.log_path.exists() || !seg.idx_path.exists() {
            continue;
        }
        let entries = read_index_entries(&seg.idx_path)?;
        if entries.is_empty() {
            continue;
        }
        let mut log_file = File::open(&seg.log_path)?;
        let log_len = log_file.metadata()?.len();
        let mut log_bytes = vec![0u8; log_len as usize];
        log_file.read_exact(&mut log_bytes)?;

        for (i, entry) in entries.iter().enumerate() {
            let start = entry.position as usize;
            let end = if i + 1 < entries.len() {
                entries[i + 1].position as usize
            } else {
                log_len as usize
            };

            if start >= log_bytes.len() || end > log_bytes.len() || start >= end {
                continue;
            }

            let slice = &log_bytes[start..end];
            let extracted = extract_key(slice);
            if let Some(key) = extracted.key {
                map.entry(key)
                    .and_modify(|(latest_offset, is_tombstone)| {
                        if entry.offset >= *latest_offset {
                            *latest_offset = entry.offset;
                            *is_tombstone = extracted.is_tombstone;
                        }
                    })
                    .or_insert((entry.offset, extracted.is_tombstone));
            }
        }
    }

    Ok(map)
}

/// Computes the ratio of dirty (superseded or expired) bytes to total bytes across closed segments.
pub fn compute_dirty_ratio(
    segments: &[LogSegment],
    tombstone_retention: Duration,
) -> io::Result<f64> {
    if segments.is_empty() {
        return Ok(0.0);
    }

    let key_map = build_key_offset_map(segments)?;
    if key_map.is_empty() {
        return Ok(0.0);
    }

    let mut total_bytes = 0u64;
    let mut dirty_bytes = 0u64;

    for seg in segments {
        if !seg.log_path.exists() || !seg.idx_path.exists() {
            continue;
        }
        let entries = read_index_entries(&seg.idx_path)?;
        let mut log_file = File::open(&seg.log_path)?;
        let log_len = log_file.metadata()?.len();
        let mut log_bytes = vec![0u8; log_len as usize];
        log_file.read_exact(&mut log_bytes)?;

        let seg_mtime = fs::metadata(&seg.log_path)
            .and_then(|m| m.modified())
            .unwrap_or_else(|_| SystemTime::now());

        for (i, entry) in entries.iter().enumerate() {
            let start = entry.position as usize;
            let end = if i + 1 < entries.len() {
                entries[i + 1].position as usize
            } else {
                log_len as usize
            };

            if start >= log_bytes.len() || end > log_bytes.len() || start >= end {
                continue;
            }

            let slice = &log_bytes[start..end];
            let rec_len = (end - start) as u64;
            total_bytes += rec_len;

            let extracted = extract_key(slice);
            if let Some(key) = &extracted.key {
                if let Some(&(latest_offset, _)) = key_map.get(key) {
                    if entry.offset < latest_offset {
                        // Older duplicate offset for this key is dirty
                        dirty_bytes += rec_len;
                    } else if extracted.is_tombstone {
                        // Tombstone at latest offset: check if expired
                        let record_time = extracted.timestamp_ms
                            .map(|ts| {
                                if ts >= 0 {
                                    SystemTime::UNIX_EPOCH + Duration::from_millis(ts as u64)
                                } else {
                                    SystemTime::UNIX_EPOCH
                                }
                            })
                            .unwrap_or(seg_mtime);

                        let is_expired = match SystemTime::now().duration_since(record_time) {
                            Ok(age) => age > tombstone_retention,
                            Err(_) => false,
                        };

                        if is_expired {
                            dirty_bytes += rec_len;
                        }
                    }
                }
            }
        }
    }

    if total_bytes == 0 {
        Ok(0.0)
    } else {
        Ok(dirty_bytes as f64 / total_bytes as f64)
    }
}

// ---------------------------------------------------------------------------
// Compactor Engine
// ---------------------------------------------------------------------------

/// Compacts closed segments by retaining only latest offset per key and non-expired tombstones.
/// Surviving records are written to `.clean.log` and `.clean.idx` and atomically swapped in place.
pub fn compact_segments(
    partition_dir: &Path,
    closed_segments: &mut Vec<LogSegment>,
    tombstone_retention: Duration,
) -> io::Result<CompactionStats> {
    let mut stats = CompactionStats::default();
    if closed_segments.is_empty() {
        return Ok(stats);
    }

    // 1. Build in-memory key-to-latest-offset index
    let key_map = build_key_offset_map(closed_segments)?;

    let mut surviving_segments = Vec::new();

    for seg in closed_segments.iter() {
        stats.segments_scanned += 1;
        if !seg.log_path.exists() || !seg.idx_path.exists() {
            continue;
        }

        let entries = read_index_entries(&seg.idx_path)?;
        let mut log_file = File::open(&seg.log_path)?;
        let log_len = log_file.metadata()?.len();
        stats.bytes_before += log_len;

        let mut log_bytes = vec![0u8; log_len as usize];
        log_file.read_exact(&mut log_bytes)?;
        drop(log_file);

        let seg_mtime = fs::metadata(&seg.log_path)
            .and_then(|m| m.modified())
            .unwrap_or_else(|_| SystemTime::now());

        // Temporary clean files
        let clean_log_path = partition_dir.join(format!("{:020}.clean.log", seg.base_offset));
        let clean_idx_path = partition_dir.join(format!("{:020}.clean.idx", seg.base_offset));

        let mut clean_log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&clean_log_path)?;

        let mut clean_idx_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&clean_idx_path)?;

        let mut clean_pos = 0u64;
        let mut segment_records_scanned = 0;
        let mut segment_records_retained = 0;
        let mut segment_records_discarded = 0;

        for (i, entry) in entries.iter().enumerate() {
            let start = entry.position as usize;
            let end = if i + 1 < entries.len() {
                entries[i + 1].position as usize
            } else {
                log_len as usize
            };

            if start >= log_bytes.len() || end > log_bytes.len() || start >= end {
                continue;
            }

            segment_records_scanned += 1;
            let slice = &log_bytes[start..end];
            let extracted = extract_key(slice);

            let retain = if let Some(key) = &extracted.key {
                if let Some(&(latest_offset, _)) = key_map.get(key) {
                    if entry.offset != latest_offset {
                        // Older record for this key: discard
                        false
                    } else if extracted.is_tombstone {
                        // Tombstone at latest offset: retain if within retention, discard if expired
                        let record_time = extracted.timestamp_ms
                            .map(|ts| {
                                if ts >= 0 {
                                    SystemTime::UNIX_EPOCH + Duration::from_millis(ts as u64)
                                } else {
                                    SystemTime::UNIX_EPOCH
                                }
                            })
                            .unwrap_or(seg_mtime);

                        let is_expired = match SystemTime::now().duration_since(record_time) {
                            Ok(age) => age > tombstone_retention,
                            Err(_) => false,
                        };
                        !is_expired
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                // Keyless records are retained
                true
            };

            if retain {
                segment_records_retained += 1;
                clean_idx_file.write_all(&entry.offset.to_be_bytes())?;
                clean_idx_file.write_all(&clean_pos.to_be_bytes())?;
                clean_log_file.write_all(slice)?;
                clean_pos += slice.len() as u64;
            } else {
                segment_records_discarded += 1;
            }
        }

        clean_log_file.flush()?;
        clean_idx_file.flush()?;
        drop(clean_log_file);
        drop(clean_idx_file);

        stats.records_scanned += segment_records_scanned;
        stats.records_retained += segment_records_retained;
        stats.records_discarded += segment_records_discarded;

        if segment_records_retained == 0 {
            // Entire segment was compacted away
            let _ = fs::remove_file(&clean_log_path);
            let _ = fs::remove_file(&clean_idx_path);
            let _ = fs::remove_file(&seg.log_path);
            let _ = fs::remove_file(&seg.idx_path);
            stats.segments_deleted += 1;
            stats.segments_cleaned += 1;
        } else if segment_records_discarded > 0 {
            // Atomic swap: rename .clean.log and .clean.idx over original .log and .idx
            fs::rename(&clean_log_path, &seg.log_path)?;
            fs::rename(&clean_idx_path, &seg.idx_path)?;
            stats.segments_cleaned += 1;
            stats.bytes_after += clean_pos;
            surviving_segments.push(seg.clone());
        } else {
            // No records discarded in this segment, keep original files
            let _ = fs::remove_file(&clean_log_path);
            let _ = fs::remove_file(&clean_idx_path);
            stats.bytes_after += log_len;
            surviving_segments.push(seg.clone());
        }
    }

    *closed_segments = surviving_segments;
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Unit Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::handlers::{encode_single_record_batch, parse_records, KafkaRecord};
    use crate::log::manager::PartitionLog;
    use std::io::Seek;
    use std::path::PathBuf;

    #[test]
    fn test_record_key_extraction_kafka() {
        let rec = KafkaRecord::new(Some(b"my-key".to_vec()), Some(b"my-val".to_vec()));
        let batch = encode_single_record_batch(42, &rec);
        let extracted = extract_key(&batch);
        assert_eq!(extracted.key.as_deref(), Some(&b"my-key"[..]));
        assert!(!extracted.is_tombstone);
        assert!(extracted.timestamp_ms.is_some());

        // Tombstone (null value)
        let tombstone_rec = KafkaRecord::new(Some(b"tomb-key".to_vec()), None);
        let tomb_batch = encode_single_record_batch(43, &tombstone_rec);
        let extracted_tomb = extract_key(&tomb_batch);
        assert_eq!(extracted_tomb.key.as_deref(), Some(&b"tomb-key"[..]));
        assert!(extracted_tomb.is_tombstone);

        // Tombstone (empty value)
        let empty_val_rec = KafkaRecord::new(Some(b"empty-key".to_vec()), Some(Vec::new()));
        let empty_batch = encode_single_record_batch(44, &empty_val_rec);
        let extracted_empty = extract_key(&empty_batch);
        assert_eq!(extracted_empty.key.as_deref(), Some(&b"empty-key"[..]));
        assert!(extracted_empty.is_tombstone);
    }

    #[test]
    fn test_record_key_extraction_generic() {
        let data = encode_kv_record(b"user-100", Some(b"profile-data"));
        let extracted = extract_key(&data);
        assert_eq!(extracted.key.as_deref(), Some(&b"user-100"[..]));
        assert!(!extracted.is_tombstone);

        // Tombstone
        let tomb_data = encode_kv_record(b"user-100", None);
        let extracted_tomb = extract_key(&tomb_data);
        assert_eq!(extracted_tomb.key.as_deref(), Some(&b"user-100"[..]));
        assert!(extracted_tomb.is_tombstone);

        // Timestamped KV
        let ts_data = encode_kv_record_with_timestamp(b"sensor-1", Some(b"25.4C"), 1672531199000);
        let extracted_ts = extract_key(&ts_data);
        assert_eq!(extracted_ts.key.as_deref(), Some(&b"sensor-1"[..]));
        assert_eq!(extracted_ts.timestamp_ms, Some(1672531199000));
        assert!(!extracted_ts.is_tombstone);
    }

    #[tokio::test]
    async fn test_deduplication_same_key_latest_offset_kept() {
        let test_dir = PathBuf::from("./data/test_compaction_dedup");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let mut log = PartitionLog::new(
            &test_dir,
            "orders",
            0,
            1,
            280, // Segment limit sized to fit offsets 0, 1, 2 and roll over at offset 3
            None,
            None,
        )
        .unwrap();

        // Write order-1 at offset 0 (initial state: pending)
        let rec0 = KafkaRecord::new(Some(b"order-1".to_vec()), Some(b"pending".to_vec()));
        let off0 = log.append(&encode_single_record_batch(0, &rec0)).unwrap();
        assert_eq!(off0, 0);

        // Write order-1 at offset 1 (updated: processing)
        let rec1 = KafkaRecord::new(Some(b"order-1".to_vec()), Some(b"processing".to_vec()));
        let off1 = log.append(&encode_single_record_batch(1, &rec1)).unwrap();
        assert_eq!(off1, 1);

        // Write order-1 at offset 2 (updated: completed)
        let rec2 = KafkaRecord::new(Some(b"order-1".to_vec()), Some(b"completed".to_vec()));
        let off2 = log.append(&encode_single_record_batch(2, &rec2)).unwrap();
        assert_eq!(off2, 2);

        // Write order-2 at offset 3 - this will trigger rollover since 3 records exceed 150 bytes
        let rec3 = KafkaRecord::new(Some(b"order-2".to_vec()), Some(b"shipped".to_vec()));
        let off3 = log.append(&encode_single_record_batch(3, &rec3)).unwrap();
        assert_eq!(off3, 3);

        // Rollover occurred, segment 0 is now closed and contains offsets 0, 1, 2!
        assert!(log.segments.len() >= 2);

        // Compact the closed segment(s)
        let stats = log.compact_partition_with_stats().unwrap();
        assert_eq!(stats.records_scanned, 3); // offsets 0, 1, 2
        assert_eq!(stats.records_retained, 1); // only offset 2
        assert_eq!(stats.records_discarded, 2); // offsets 0 and 1 discarded

        // Reading from offset 0 should jump directly to surviving offset 2
        let read_res = log.read_from_offset(0, 4096).unwrap();
        assert!(read_res.is_some());
        let (mut file, pos, bytes) = read_res.unwrap();
        let mut buf = vec![0u8; bytes as usize];
        file.seek(std::io::SeekFrom::Start(pos)).unwrap();
        file.read_exact(&mut buf).unwrap();

        let parsed = parse_records(&buf).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].offset, 2);
        assert_eq!(parsed[0].key.as_deref(), Some(&b"order-1"[..]));
        assert_eq!(parsed[0].value.as_deref(), Some(&b"completed"[..]));

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_multiple_keys_preserved() {
        let test_dir = PathBuf::from("./data/test_compaction_multi_keys");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let mut log = PartitionLog::new(
            &test_dir,
            "multi-keys",
            0,
            1,
            100, // Small limit: records 0-4 (95 bytes) fit, record 5 triggers rollover
            None,
            None,
        )
        .unwrap();

        // Key A @ 0
        let r0 = encode_kv_record(b"key-A", Some(b"val-A1"));
        let off0 = log.append(&r0).unwrap();
        assert_eq!(off0, 0);

        // Key B @ 1
        let r1 = encode_kv_record(b"key-B", Some(b"val-B1"));
        let off1 = log.append(&r1).unwrap();
        assert_eq!(off1, 1);

        // Key C @ 2 (will never be updated)
        let r2 = encode_kv_record(b"key-C", Some(b"val-C1"));
        let off2 = log.append(&r2).unwrap();
        assert_eq!(off2, 2);

        // Key A @ 3 (updated)
        let r3 = encode_kv_record(b"key-A", Some(b"val-A2"));
        let off3 = log.append(&r3).unwrap();
        assert_eq!(off3, 3);

        // Key B @ 4 (updated) -> triggers rollover
        let r4 = encode_kv_record(b"key-B", Some(b"val-B2"));
        let off4 = log.append(&r4).unwrap();
        assert_eq!(off4, 4);

        // Active segment write
        let r5 = encode_kv_record(b"key-D", Some(b"val-D1"));
        let _off5 = log.append(&r5).unwrap();

        assert!(log.segments.len() >= 2);

        let stats = log.compact_partition_with_stats().unwrap();
        assert_eq!(stats.records_discarded, 2); // A1 and B1 discarded
        assert_eq!(stats.records_retained, 3); // C1 (off 2), A2 (off 3), B2 (off 4) retained

        // Verify reading from offset 0 finds C1 at offset 2
        let read0 = log.read_from_offset(0, 4096).unwrap().unwrap();
        let mut file = read0.0;
        let mut buf = vec![0u8; read0.2 as usize];
        file.seek(std::io::SeekFrom::Start(read0.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        let ext = extract_key(&buf);
        assert_eq!(ext.key.as_deref(), Some(&b"key-C"[..]));

        // Reading from offset 3 finds A2
        let read3 = log.read_from_offset(3, 4096).unwrap().unwrap();
        let mut file = read3.0;
        let mut buf = vec![0u8; read3.2 as usize];
        file.seek(std::io::SeekFrom::Start(read3.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        let ext = extract_key(&buf);
        assert_eq!(ext.key.as_deref(), Some(&b"key-A"[..]));

        // Reading from offset 4 finds B2
        let read4 = log.read_from_offset(4, 4096).unwrap().unwrap();
        let mut file = read4.0;
        let mut buf = vec![0u8; read4.2 as usize];
        file.seek(std::io::SeekFrom::Start(read4.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        let ext = extract_key(&buf);
        assert_eq!(ext.key.as_deref(), Some(&b"key-B"[..]));

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_tombstone_deletion_after_retention_expiry() {
        let test_dir = PathBuf::from("./data/test_compaction_tombstones");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let mut log = PartitionLog::new(
            &test_dir,
            "tombstones",
            0,
            1,
            100,
            None,
            None,
        )
        .unwrap();

        // 1. Write an initial record
        let r0 = encode_kv_record(b"user-to-delete", Some(b"active-user"));
        log.append(&r0).unwrap();

        // 2. Write an expired tombstone: timestamp was 1 hour ago
        let one_hour_ago = (SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64)
            - 3600 * 1000;
        let r1 = encode_kv_record_with_timestamp(b"user-to-delete", None, one_hour_ago);
        log.append(&r1).unwrap();

        // 3. Write another key to trigger rollover
        let r2 = encode_kv_record(b"persistent-user", Some(b"kept"));
        log.append(&r2).unwrap();

        // Rollover to active segment
        let r3 = encode_kv_record(b"active-seg-user", Some(b"active"));
        log.append(&r3).unwrap();

        assert!(log.segments.len() >= 2);

        // Case A: Tombstone retention is 24 hours -> 1 hour old tombstone is NOT expired yet
        log.tombstone_retention = Duration::from_secs(24 * 3600);
        let stats_retained = log.compact_partition_with_stats().unwrap();
        // r0 was discarded (superseded by r1 tombstone), r1 tombstone retained because not expired!
        assert_eq!(stats_retained.records_discarded, 1);
        assert_eq!(stats_retained.records_retained, 2); // r1 (tombstone) + r2

        // Case B: Tombstone retention is 10 minutes -> 1 hour old tombstone IS expired!
        log.tombstone_retention = Duration::from_secs(600);
        let stats_expired = log.compact_partition_with_stats().unwrap();
        // The tombstone r1 is now discarded! Only r2 survives.
        assert_eq!(stats_expired.records_discarded, 1);
        assert_eq!(stats_expired.records_retained, 1); // only r2

        // Verify that user-to-delete is completely gone
        let read0 = log.read_from_offset(0, 4096).unwrap().unwrap();
        let mut file = read0.0;
        let mut buf = vec![0u8; read0.2 as usize];
        file.seek(std::io::SeekFrom::Start(read0.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        let ext = extract_key(&buf);
        assert_eq!(ext.key.as_deref(), Some(&b"persistent-user"[..]));

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_binary_search_accuracy_on_compacted_index() {
        let test_dir = PathBuf::from("./data/test_compaction_binary_search");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let mut log = PartitionLog::new(
            &test_dir,
            "bsearch",
            0,
            1,
            65, // Small limit: records 0-4 (60 bytes) fit, record 5 triggers rollover
            None,
            None,
        )
        .unwrap();

        // Seed 6 records across keys K1-K4 to test deduplication and rollover.
        log.append(&encode_kv_record(b"K1", Some(b"v0"))).unwrap();
        log.append(&encode_kv_record(b"K2", Some(b"v1"))).unwrap();
        log.append(&encode_kv_record(b"K1", Some(b"v2"))).unwrap();
        log.append(&encode_kv_record(b"K3", Some(b"v3"))).unwrap();
        log.append(&encode_kv_record(b"K2", Some(b"v4"))).unwrap();
        // Trigger rollover
        log.append(&encode_kv_record(b"K4", Some(b"v5"))).unwrap();

        assert!(log.segments.len() >= 2);

        // Compact closed segment
        let stats = log.compact_partition_with_stats().unwrap();
        assert_eq!(stats.records_discarded, 2); // offsets 0 and 1
        assert_eq!(stats.records_retained, 3); // offsets 2, 3, 4

        // Index in compacted segment should now have entries: [2, 3, 4]
        let compacted_seg = &log.segments[0];
        let entries = read_index_entries(&compacted_seg.idx_path).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].offset, 2);
        assert_eq!(entries[1].offset, 3);
        assert_eq!(entries[2].offset, 4);

        // Test binary search via read_from_offset:
        // Seeking offset 0 -> finds offset 2
        let r0 = log.read_from_offset(0, 4096).unwrap().unwrap();
        let mut file = r0.0;
        let mut buf = vec![0u8; r0.2 as usize];
        file.seek(std::io::SeekFrom::Start(r0.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(extract_key(&buf).key.as_deref(), Some(&b"K1"[..]));

        // Seeking offset 1 -> finds offset 2
        let r1 = log.read_from_offset(1, 4096).unwrap().unwrap();
        assert_eq!(r1.1, r0.1); // same position

        // Seeking offset 2 -> finds offset 2
        let r2 = log.read_from_offset(2, 4096).unwrap().unwrap();
        assert_eq!(r2.1, r0.1);

        // Seeking offset 3 -> finds offset 3
        let r3 = log.read_from_offset(3, 4096).unwrap().unwrap();
        let mut file = r3.0;
        let mut buf = vec![0u8; r3.2 as usize];
        file.seek(std::io::SeekFrom::Start(r3.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(extract_key(&buf).key.as_deref(), Some(&b"K3"[..]));

        // Seeking offset 4 -> finds offset 4
        let r4 = log.read_from_offset(4, 4096).unwrap().unwrap();
        let mut file = r4.0;
        let mut buf = vec![0u8; r4.2 as usize];
        file.seek(std::io::SeekFrom::Start(r4.1)).unwrap();
        file.read_exact(&mut buf).unwrap();
        assert_eq!(extract_key(&buf).key.as_deref(), Some(&b"K2"[..]));

        let _ = fs::remove_dir_all(&test_dir);
    }
}
