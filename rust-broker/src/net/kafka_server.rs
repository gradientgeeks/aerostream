use std::io::{self, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::config::BrokerConfig;
use crate::log::LogManager;

pub struct KafkaServer {
    addr: SocketAddr,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
}

impl KafkaServer {
    pub fn new(addr: SocketAddr, log_manager: Arc<LogManager>, cfg: Arc<BrokerConfig>) -> Self {
        Self {
            addr,
            log_manager,
            cfg,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.addr).await?;
        info!(
            "[AeroMQ Kafka] Kafka Wire Protocol TCP listener active on {}",
            self.addr
        );

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let log_manager = self.log_manager.clone();
            let cfg = self.cfg.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_kafka_connection(stream, log_manager, cfg).await {
                    debug!(
                        "[AeroMQ Kafka] Connection ended for {}: {:?}",
                        peer_addr, e
                    );
                }
            });
        }
    }
}

async fn handle_kafka_connection(
    mut stream: TcpStream,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut len_buf = [0u8; 4];

    loop {
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let frame_len = i32::from_be_bytes(len_buf);
        if frame_len <= 0 || frame_len > 64 * 1024 * 1024 {
            warn!("[AeroMQ Kafka] Invalid frame length: {}", frame_len);
            break;
        }

        let mut frame_buf = vec![0u8; frame_len as usize];
        stream.read_exact(&mut frame_buf).await?;

        if let Some(resp_bytes) = handle_kafka_frame(&frame_buf, &log_manager, &cfg).await? {
            let resp_len = (resp_bytes.len() as i32).to_be_bytes();
            stream.write_all(&resp_len).await?;
            stream.write_all(&resp_bytes).await?;
            stream.flush().await?;
        }
    }

    Ok(())
}

pub async fn handle_kafka_frame(
    frame: &[u8],
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut cursor = io::Cursor::new(frame);
    if cursor.remaining() < 8 {
        return Err("Kafka frame too short for header".into());
    }

    let api_key = cursor.get_i16();
    let api_version = cursor.get_i16();
    let correlation_id = cursor.get_i32();
    let _client_id = read_kafka_string(&mut cursor)?;

    match api_key {
        18 => {
            // ApiVersions
            let resp = handle_api_versions(correlation_id, api_version)?;
            Ok(Some(resp))
        }
        3 => {
            // Metadata
            let resp = handle_metadata(correlation_id, api_version, &mut cursor, log_manager, cfg).await?;
            Ok(Some(resp))
        }
        0 => {
            // Produce
            let resp_opt = handle_produce(correlation_id, api_version, &mut cursor, log_manager).await?;
            Ok(resp_opt)
        }
        1 => {
            // Fetch
            let resp = handle_fetch(correlation_id, api_version, &mut cursor, log_manager).await?;
            Ok(Some(resp))
        }
        2 => {
            // ListOffsets
            let resp = handle_list_offsets(correlation_id, api_version, &mut cursor, log_manager).await?;
            Ok(Some(resp))
        }
        22 => {
            // InitProducerId (ApiKey 22)
            let resp = handle_init_producer_id(correlation_id, api_version, &mut cursor)?;
            Ok(Some(resp))
        }
        _ => {
            warn!("[AeroMQ Kafka] Unsupported API key: {}", api_key);
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(35); // UNSUPPORTED_VERSION error code
            Ok(Some(resp.to_vec()))
        }
    }
}

// ============================================================================
// API Handlers
// ============================================================================

/// Handler for ApiVersions (API Key 18)
fn handle_api_versions(
    correlation_id: i32,
    api_version: i16,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    buf.put_i16(0); // ErrorCode: 0 (NONE)

    // Supported API keys list
    let api_keys: [(i16, i16, i16); 6] = [
        (0, 0, 7),  // Produce: v0 - v7
        (1, 0, 7),  // Fetch: v0 - v7
        (2, 0, 2),  // ListOffsets: v0 - v2
        (3, 0, 5),  // Metadata: v0 - v5
        (18, 0, 3), // ApiVersions: v0 - v3
        (22, 0, 4), // InitProducerId: v0 - v4
    ];

    buf.put_i32(api_keys.len() as i32);
    for (key, min_v, max_v) in api_keys {
        buf.put_i16(key);
        buf.put_i16(min_v);
        buf.put_i16(max_v);
    }

    if api_version >= 1 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    if api_version >= 3 {
        buf.put_u8(0); // Empty tagged fields buffer
    }

    Ok(buf.to_vec())
}

/// Handler for InitProducerId (API Key 22)
fn handle_init_producer_id(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let _transactional_id = read_kafka_string(cursor)?;
    let _transaction_timeout_ms = if cursor.remaining() >= 4 {
        cursor.get_i32()
    } else {
        60000
    };

    let producer_id = crate::kafka::handlers::allocate_producer_id();
    let producer_epoch = 0i16;

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    buf.put_i32(0); // ThrottleTimeMs
    buf.put_i16(0); // ErrorCode: 0 (NONE)
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);

    if api_version >= 2 {
        buf.put_u8(0); // empty tagged fields
    }

    Ok(buf.to_vec())
}

/// Handler for Metadata (API Key 3)
async fn handle_metadata(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut requested_topics = Vec::new();
    if cursor.remaining() >= 4 {
        let topics_count = cursor.get_i32();
        if topics_count > 0 {
            for _ in 0..topics_count {
                if let Some(topic) = read_kafka_string(cursor)? {
                    requested_topics.push(topic);
                }
            }
        }
    }

    // If no specific topics requested (empty or null array), list known topics from log manager
    let topics_to_report = if requested_topics.is_empty() {
        let existing = log_manager.get_all_offsets().await;
        let mut set = std::collections::HashSet::new();
        for (topic, _, _) in existing {
            set.insert(topic);
        }
        if set.is_empty() {
            vec!["default".to_string()]
        } else {
            set.into_iter().collect()
        }
    } else {
        requested_topics
    };

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    if api_version >= 1 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    // Brokers array
    buf.put_i32(1); // 1 broker
    buf.put_i32(cfg.id as i32); // NodeId
    put_kafka_string(&mut buf, Some(&cfg.host));
    buf.put_i32(cfg.kafka_port);
    if api_version >= 1 {
        put_kafka_string(&mut buf, None); // Rack: null
    }

    if api_version >= 1 {
        buf.put_i32(cfg.id as i32); // ControllerId
    }

    // TopicMetadata array
    buf.put_i32(topics_to_report.len() as i32);
    for topic_name in topics_to_report {
        buf.put_i16(0); // ErrorCode: 0 (NONE)
        put_kafka_string(&mut buf, Some(&topic_name));

        if api_version >= 1 {
            buf.put_u8(0); // IsInternal: false
        }

        // Partitions array (Partition 0)
        buf.put_i32(1); // 1 partition
        buf.put_i16(0); // Partition ErrorCode: 0
        buf.put_i32(0); // PartitionIndex: 0
        buf.put_i32(cfg.id as i32); // LeaderId

        if api_version >= 7 {
            buf.put_i32(0); // LeaderEpoch: 0
        }

        // Replicas: [broker_id]
        buf.put_i32(1);
        buf.put_i32(cfg.id as i32);

        // Isr: [broker_id]
        buf.put_i32(1);
        buf.put_i32(cfg.id as i32);

        if api_version >= 5 {
            buf.put_i32(0); // OfflineReplicas count: 0
        }
    }

    Ok(buf.to_vec())
}

/// Handler for Produce (API Key 0)
async fn handle_produce(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    if api_version >= 3 {
        let _transactional_id = read_kafka_string(cursor)?;
    }

    if cursor.remaining() < 6 {
        return Err("Produce request truncated".into());
    }

    let acks = cursor.get_i16();
    let _timeout_ms = cursor.get_i32();

    let topics_count = cursor.get_i32();
    let mut topic_results = Vec::new();

    for _ in 0..topics_count {
        let topic_name = read_kafka_string(cursor)?.unwrap_or_default();
        let partitions_count = cursor.get_i32();
        let mut part_results = Vec::new();

        for _ in 0..partitions_count {
            let partition_index = cursor.get_i32();
            let records_size = cursor.get_i32();

            let mut appended_offset = 0i64;
            let mut error_code = 0i16;

            if records_size > 0 && cursor.remaining() >= records_size as usize {
                let mut records_data = vec![0u8; records_size as usize];
                cursor.copy_to_slice(&mut records_data);

                // Detect modern RecordBatch (magic byte 2 at index 16) with PID and sequence
                let (producer_id, base_sequence, records_count) = if records_data.len() >= 61 && records_data[16] == 2 {
                    let pid = i64::from_be_bytes(records_data[43..51].try_into().unwrap());
                    let seq = i32::from_be_bytes(records_data[53..57].try_into().unwrap());
                    let count = i32::from_be_bytes(records_data[57..61].try_into().unwrap());
                    (pid, seq, count)
                } else {
                    (-1i64, -1i32, 1i32)
                };

                match log_manager.get_partition(&topic_name, partition_index as u32).await {
                    Ok(part_log) => {
                        let mut guard = part_log.lock().await;
                        match guard.validate_idempotent_produce(producer_id, base_sequence) {
                            Ok(Some(cached_offset)) => {
                                // Duplicate batch! Return duplicate ACK with cached offset without writing to disk
                                appended_offset = cached_offset;
                                error_code = 0;
                            }
                            Ok(None) => {
                                match guard.append(&records_data) {
                                    Ok(off) => {
                                        appended_offset = off as i64;
                                        guard.update_producer_state(producer_id, base_sequence, records_count, appended_offset);
                                    }
                                    Err(e) => {
                                        error!("[AeroMQ Kafka] Append error for {}-{}: {:?}", topic_name, partition_index, e);
                                        error_code = 1; // OFFSET_OUT_OF_RANGE or general error
                                    }
                                }
                            }
                            Err(err) => {
                                warn!("[AeroMQ Kafka] Idempotent produce sequence error for PID {} seq {}: err {}", producer_id, base_sequence, err);
                                error_code = err; // 45: OutOfOrderSequenceNumber
                            }
                        }
                    }
                    Err(e) => {
                        error!("[AeroMQ Kafka] Failed to get partition {}-{}: {:?}", topic_name, partition_index, e);
                        error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                }
            }

            part_results.push((partition_index, error_code, appended_offset));
        }

        topic_results.push((topic_name, part_results));
    }

    // If acks == 0, Kafka specification dictates NO response frame is returned to the client
    if acks == 0 {
        return Ok(None);
    }

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    buf.put_i32(topic_results.len() as i32);

    for (topic, parts) in topic_results {
        put_kafka_string(&mut buf, Some(&topic));
        buf.put_i32(parts.len() as i32);
        for (part_idx, err_code, base_off) in parts {
            buf.put_i32(part_idx);
            buf.put_i16(err_code);
            buf.put_i64(base_off);
            if api_version >= 2 {
                buf.put_i64(-1); // LogAppendTimeMs
            }
            if api_version >= 5 {
                buf.put_i64(0); // LogStartOffset
            }
        }
    }

    if api_version >= 1 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    Ok(Some(buf.to_vec()))
}

/// Handler for Fetch (API Key 1)
async fn handle_fetch(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if cursor.remaining() < 12 {
        return Err("Fetch request truncated".into());
    }

    let _replica_id = cursor.get_i32();
    let _max_wait_ms = cursor.get_i32();
    let _min_bytes = cursor.get_i32();

    if api_version >= 3 && cursor.remaining() >= 4 {
        let _max_bytes = cursor.get_i32();
    }
    if api_version >= 4 && cursor.remaining() >= 1 {
        let _isolation_level = cursor.get_i8();
    }
    if api_version >= 7 && cursor.remaining() >= 8 {
        let _session_id = cursor.get_i32();
        let _session_epoch = cursor.get_i32();
    }

    let topics_count = cursor.get_i32();
    let mut topic_results = Vec::new();

    for _ in 0..topics_count {
        let topic_name = read_kafka_string(cursor)?.unwrap_or_default();
        let partitions_count = cursor.get_i32();
        let mut part_results = Vec::new();

        for _ in 0..partitions_count {
            let partition_index = cursor.get_i32();
            if api_version >= 5 && cursor.remaining() >= 4 {
                let _current_leader_epoch = cursor.get_i32();
            }
            let fetch_offset = cursor.get_i64();
            if api_version >= 5 && cursor.remaining() >= 8 {
                let _log_start_offset = cursor.get_i64();
            }
            let partition_max_bytes = cursor.get_i32();

            let mut high_watermark = 0i64;
            let mut record_set = Vec::new();
            let mut error_code = 0i16;

            match log_manager.get_partition(&topic_name, partition_index as u32).await {
                Ok(part_log) => {
                    let mut guard = part_log.lock().await;
                    high_watermark = guard.high_watermark as i64;

                    if (fetch_offset as u64) < guard.high_watermark {
                        let max_read = (partition_max_bytes as u32).min(32 * 1024 * 1024);
                        if let Ok(Some((mut file, position, bytes_to_read))) =
                            guard.read_from_offset(fetch_offset as u64, max_read)
                        {
                            if let Ok(_) = file.seek(SeekFrom::Start(position)) {
                                let mut raw_buf = vec![0u8; bytes_to_read as usize];
                                if let Ok(_) = file.read_exact(&mut raw_buf) {
                                    record_set = ensure_kafka_record_set(fetch_offset, &raw_buf);
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                }
            }

            part_results.push((partition_index, error_code, high_watermark, record_set));
        }

        topic_results.push((topic_name, part_results));
    }

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    if api_version >= 1 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    if api_version >= 7 {
        buf.put_i16(0); // ErrorCode
        buf.put_i32(0); // SessionId
    }

    buf.put_i32(topic_results.len() as i32);
    for (topic, parts) in topic_results {
        put_kafka_string(&mut buf, Some(&topic));
        buf.put_i32(parts.len() as i32);
        for (part_idx, err_code, hw, records) in parts {
            buf.put_i32(part_idx);
            buf.put_i16(err_code);
            buf.put_i64(hw);
            if api_version >= 4 {
                buf.put_i64(hw); // LastStableOffset
            }
            if api_version >= 5 {
                buf.put_i64(0); // LogStartOffset
            }
            if api_version >= 4 {
                buf.put_i32(0); // AbortedTransactions count: 0
            }
            buf.put_i32(records.len() as i32);
            buf.put_slice(&records);
        }
    }

    Ok(buf.to_vec())
}

/// Handler for ListOffsets (API Key 2)
async fn handle_list_offsets(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if cursor.remaining() < 8 {
        return Err("ListOffsets request truncated".into());
    }

    let _replica_id = cursor.get_i32();
    if api_version >= 2 && cursor.remaining() >= 1 {
        let _isolation_level = cursor.get_i8();
    }

    let topics_count = cursor.get_i32();
    let mut topic_results = Vec::new();

    for _ in 0..topics_count {
        let topic_name = read_kafka_string(cursor)?.unwrap_or_default();
        let partitions_count = cursor.get_i32();
        let mut part_results = Vec::new();

        for _ in 0..partitions_count {
            let partition_index = cursor.get_i32();
            let timestamp = cursor.get_i64();
            if api_version == 0 && cursor.remaining() >= 4 {
                let _max_num_offsets = cursor.get_i32();
            }

            let mut offset = 0i64;
            if let Ok(part_log) = log_manager.get_partition(&topic_name, partition_index as u32).await {
                let guard = part_log.lock().await;
                if timestamp == -2 {
                    // Earliest offset
                    offset = 0;
                } else {
                    // Latest offset (timestamp == -1 or current)
                    offset = guard.next_offset as i64;
                }
            }

            part_results.push((partition_index, 0i16, offset));
        }

        topic_results.push((topic_name, part_results));
    }

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    if api_version >= 2 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    buf.put_i32(topic_results.len() as i32);
    for (topic, parts) in topic_results {
        put_kafka_string(&mut buf, Some(&topic));
        buf.put_i32(parts.len() as i32);
        for (part_idx, err_code, off) in parts {
            buf.put_i32(part_idx);
            buf.put_i16(err_code);
            if api_version == 0 {
                buf.put_i32(1); // Offsets array len = 1
                buf.put_i64(off);
            } else {
                buf.put_i64(-1); // Timestamp
                buf.put_i64(off);
                if api_version >= 4 {
                    buf.put_i32(0); // LeaderEpoch: 0
                }
            }
        }
    }

    Ok(buf.to_vec())
}

// ============================================================================
// Encoding / Decoding Utilities
// ============================================================================

pub fn read_kafka_string(
    cursor: &mut io::Cursor<&[u8]>,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    if cursor.remaining() < 2 {
        return Ok(None);
    }
    let len = cursor.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let ulen = len as usize;
    if cursor.remaining() < ulen {
        return Err("Kafka string length exceeds available bytes".into());
    }
    let mut buf = vec![0u8; ulen];
    cursor.copy_to_slice(&mut buf);
    Ok(Some(String::from_utf8(buf)?))
}

pub fn put_kafka_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(val) => {
            buf.put_i16(val.len() as i16);
            buf.put_slice(val.as_bytes());
        }
        None => {
            buf.put_i16(-1);
        }
    }
}

#[allow(dead_code)]
pub fn put_kafka_bytes(buf: &mut BytesMut, b: Option<&[u8]>) {
    match b {
        Some(val) => {
            buf.put_i32(val.len() as i32);
            buf.put_slice(val);
        }
        None => {
            buf.put_i32(-1);
        }
    }
}

/// Ensures the data is in Kafka record format. If it is already formatted as a
/// RecordBatch (magic=2) or MessageSet (magic=0 or 1), returns it directly;
/// otherwise wraps the raw payload into a standard Kafka MessageSet v0 frame.
pub fn ensure_kafka_record_set(offset: i64, raw_buf: &[u8]) -> Vec<u8> {
    if raw_buf.len() >= 17 {
        // RecordBatch check: magic byte 2 at offset 16
        if raw_buf[16] == 2 {
            let batch_len = i32::from_be_bytes(raw_buf[8..12].try_into().unwrap());
            if batch_len > 0 && (batch_len as usize + 12) <= raw_buf.len() {
                return raw_buf.to_vec();
            }
        }
        // MessageSet check: magic byte 0 or 1 at offset 16
        if raw_buf[16] == 0 || raw_buf[16] == 1 {
            let msg_size = i32::from_be_bytes(raw_buf[8..12].try_into().unwrap());
            if msg_size > 0 && (msg_size as usize + 12) <= raw_buf.len() {
                return raw_buf.to_vec();
            }
        }
    }

    wrap_in_kafka_message_set(offset, raw_buf)
}

/// Wrap a raw payload into a Kafka v0 MessageSet entry.
pub fn wrap_in_kafka_message_set(offset: i64, payload: &[u8]) -> Vec<u8> {
    // Inner message: [magic(1)][attributes(1)][key_len(4)][val_len(4)][value]
    let inner_len = 1 + 1 + 4 + 4 + payload.len();
    let mut inner = Vec::with_capacity(inner_len);
    inner.push(0u8); // magic: 0
    inner.push(0u8); // attributes: 0
    inner.extend_from_slice(&(-1i32).to_be_bytes()); // null key
    inner.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    inner.extend_from_slice(payload);

    let crc = crc32_ieee(&inner);

    // Frame: [offset(8)][message_size(4)][crc(4)][inner...]
    let mut record = Vec::with_capacity(8 + 4 + 4 + inner.len());
    record.extend_from_slice(&offset.to_be_bytes());
    record.extend_from_slice(&((4 + inner.len()) as i32).to_be_bytes());
    record.extend_from_slice(&crc.to_be_bytes());
    record.extend_from_slice(&inner);
    record
}

/// Standard IEEE 802.3 CRC32 implementation.
pub fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = -((crc & 1) as i32) as u32;
            crc = (crc >> 1) ^ (0xEDB88320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    struct TestDir(std::path::PathBuf);
    impl TestDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("aeromq_test_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> std::io::Result<TestDir> {
        Ok(TestDir::new())
    }

    #[test]
    fn test_crc32_ieee() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn test_wrap_in_kafka_message_set() {
        let payload = b"hello kafka";
        let wrapped = wrap_in_kafka_message_set(42, payload);

        // Header: offset(8) + message_size(4) + crc(4) + magic(1) + attr(1) + key(-1: 4) + val_len(4) + val(11)
        assert_eq!(wrapped.len(), 8 + 4 + 4 + 1 + 1 + 4 + 4 + payload.len());
        let offset = i64::from_be_bytes(wrapped[0..8].try_into().unwrap());
        assert_eq!(offset, 42);
    }

    #[tokio::test]
    async fn test_handle_api_versions() {
        let resp = handle_api_versions(1234, 0).unwrap();
        let mut cursor = io::Cursor::new(resp.as_slice());
        assert_eq!(cursor.get_i32(), 1234); // correlation_id
        assert_eq!(cursor.get_i16(), 0);    // error_code: NONE
        let num_keys = cursor.get_i32();
        assert_eq!(num_keys, 6);
    }

    #[tokio::test]
    async fn test_handle_metadata() {
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // Request metadata for "test-topic"
        let mut req = BytesMut::new();
        req.put_i32(1); // 1 topic
        put_kafka_string(&mut req, Some("test-topic"));

        let mut cursor = io::Cursor::new(req.as_ref());
        let resp = handle_metadata(5678, 0, &mut cursor, &log_mgr, &cfg).await.unwrap();

        let mut resp_cursor = io::Cursor::new(resp.as_slice());
        assert_eq!(resp_cursor.get_i32(), 5678); // correlation_id
        let brokers_count = resp_cursor.get_i32();
        assert_eq!(brokers_count, 1);
        assert_eq!(resp_cursor.get_i32(), 1); // broker id = 1
        let host = read_kafka_string(&mut resp_cursor).unwrap().unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(resp_cursor.get_i32(), 9093); // kafka port
    }

    #[tokio::test]
    async fn test_produce_and_fetch_round_trip() {
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // 1. Build Produce request
        let payload = b"aero-kafka-record";
        let message_record = wrap_in_kafka_message_set(0, payload);

        let mut prod_frame = BytesMut::new();
        prod_frame.put_i16(0); // api_key = 0 (Produce)
        prod_frame.put_i16(0); // api_version = 0
        prod_frame.put_i32(999); // correlation_id = 999
        put_kafka_string(&mut prod_frame, Some("test-client"));
        prod_frame.put_i16(1); // acks = 1
        prod_frame.put_i32(1000); // timeout_ms
        prod_frame.put_i32(1); // 1 topic
        put_kafka_string(&mut prod_frame, Some("my-topic"));
        prod_frame.put_i32(1); // 1 partition
        prod_frame.put_i32(0); // partition 0
        prod_frame.put_i32(message_record.len() as i32);
        prod_frame.put_slice(&message_record);

        let prod_resp = handle_kafka_frame(&prod_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur = io::Cursor::new(prod_resp.as_slice());
        assert_eq!(cur.get_i32(), 999); // correlation_id
        assert_eq!(cur.get_i32(), 1);   // 1 topic
        let resp_topic = read_kafka_string(&mut cur).unwrap().unwrap();
        assert_eq!(resp_topic, "my-topic");
        assert_eq!(cur.get_i32(), 1);   // 1 partition
        assert_eq!(cur.get_i32(), 0);   // partition 0
        assert_eq!(cur.get_i16(), 0);   // error_code 0
        let base_offset = cur.get_i64();
        assert_eq!(base_offset, 0);

        // 2. Build Fetch request
        let mut fetch_frame = BytesMut::new();
        fetch_frame.put_i16(1); // api_key = 1 (Fetch)
        fetch_frame.put_i16(0); // api_version = 0
        fetch_frame.put_i32(1000); // correlation_id
        put_kafka_string(&mut fetch_frame, Some("test-consumer"));
        fetch_frame.put_i32(-1); // replica_id
        fetch_frame.put_i32(500); // max_wait_ms
        fetch_frame.put_i32(1); // min_bytes
        fetch_frame.put_i32(1); // 1 topic
        put_kafka_string(&mut fetch_frame, Some("my-topic"));
        fetch_frame.put_i32(1); // 1 partition
        fetch_frame.put_i32(0); // partition 0
        fetch_frame.put_i64(0); // fetch_offset = 0
        fetch_frame.put_i32(65536); // partition_max_bytes

        let fetch_resp = handle_kafka_frame(&fetch_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut fcur = io::Cursor::new(fetch_resp.as_slice());
        assert_eq!(fcur.get_i32(), 1000); // correlation_id
        assert_eq!(fcur.get_i32(), 1);    // 1 topic
        let ftopic = read_kafka_string(&mut fcur).unwrap().unwrap();
        assert_eq!(ftopic, "my-topic");
        assert_eq!(fcur.get_i32(), 1);    // 1 partition
        assert_eq!(fcur.get_i32(), 0);    // partition 0
        assert_eq!(fcur.get_i16(), 0);    // error_code 0
        let hw = fcur.get_i64();
        assert!(hw >= 1);
        let records_len = fcur.get_i32();
        assert!(records_len > 0);
    }

    #[tokio::test]
    async fn test_init_producer_id_and_idempotent_produce() {
        use crate::kafka::handlers::{encode_idempotent_records_batch, KafkaRecord};

        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // 1. Send InitProducerId (ApiKey 22)
        let mut init_frame = BytesMut::new();
        init_frame.put_i16(22); // ApiKey = 22 (InitProducerId)
        init_frame.put_i16(0);  // ApiVersion = 0
        init_frame.put_i32(701); // CorrelationId = 701
        put_kafka_string(&mut init_frame, Some("idempotent-client"));
        put_kafka_string(&mut init_frame, None); // transactional_id: null
        init_frame.put_i32(60000); // transaction_timeout_ms

        let init_resp = handle_kafka_frame(&init_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut icur = io::Cursor::new(init_resp.as_slice());
        assert_eq!(icur.get_i32(), 701); // correlation_id
        assert_eq!(icur.get_i32(), 0);   // throttle_time_ms
        assert_eq!(icur.get_i16(), 0);   // error_code: 0
        let producer_id = icur.get_i64();
        let producer_epoch = icur.get_i16();
        assert!(producer_id >= 1000);
        assert_eq!(producer_epoch, 0);

        // 2. First Idempotent Produce: sequence 0
        let record = KafkaRecord::new(Some(b"key-0".to_vec()), Some(b"val-seq-0".to_vec()));
        let batch_seq_0 = encode_idempotent_records_batch(0, producer_id, producer_epoch, 0, &[record]);

        let mut prod_frame_0 = BytesMut::new();
        prod_frame_0.put_i16(0); // Produce
        prod_frame_0.put_i16(0); // v0
        prod_frame_0.put_i32(702); // CorrelationId
        put_kafka_string(&mut prod_frame_0, Some("idempotent-client"));
        prod_frame_0.put_i16(1); // acks = 1
        prod_frame_0.put_i32(1000); // timeout
        prod_frame_0.put_i32(1); // 1 topic
        put_kafka_string(&mut prod_frame_0, Some("idemp-topic"));
        prod_frame_0.put_i32(1); // 1 partition
        prod_frame_0.put_i32(0); // partition 0
        prod_frame_0.put_i32(batch_seq_0.len() as i32);
        prod_frame_0.put_slice(&batch_seq_0);

        let resp_0 = handle_kafka_frame(&prod_frame_0, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur0 = io::Cursor::new(resp_0.as_slice());
        assert_eq!(cur0.get_i32(), 702);
        assert_eq!(cur0.get_i32(), 1);
        let _ = read_kafka_string(&mut cur0);
        assert_eq!(cur0.get_i32(), 1); // 1 part
        assert_eq!(cur0.get_i32(), 0); // part 0
        assert_eq!(cur0.get_i16(), 0); // error_code: 0
        let offset_0 = cur0.get_i64();
        assert_eq!(offset_0, 0);

        // Verify partition next_offset is 1
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }

        // 3. Duplicate Produce: duplicate sequence 0
        let mut prod_frame_dup = BytesMut::new();
        prod_frame_dup.put_i16(0);
        prod_frame_dup.put_i16(0);
        prod_frame_dup.put_i32(703);
        put_kafka_string(&mut prod_frame_dup, Some("idempotent-client"));
        prod_frame_dup.put_i16(1);
        prod_frame_dup.put_i32(1000);
        prod_frame_dup.put_i32(1);
        put_kafka_string(&mut prod_frame_dup, Some("idemp-topic"));
        prod_frame_dup.put_i32(1);
        prod_frame_dup.put_i32(0);
        prod_frame_dup.put_i32(batch_seq_0.len() as i32);
        prod_frame_dup.put_slice(&batch_seq_0);

        let resp_dup = handle_kafka_frame(&prod_frame_dup, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur_dup = io::Cursor::new(resp_dup.as_slice());
        assert_eq!(cur_dup.get_i32(), 703);
        assert_eq!(cur_dup.get_i32(), 1);
        let _ = read_kafka_string(&mut cur_dup);
        assert_eq!(cur_dup.get_i32(), 1);
        assert_eq!(cur_dup.get_i32(), 0);
        assert_eq!(cur_dup.get_i16(), 0); // duplicate ACK returned without error
        let dup_offset = cur_dup.get_i64();
        assert_eq!(dup_offset, 0); // returned cached offset 0

        // Assert log length only increased by 1 and broker did not write a second record!
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }

        // 4. Sequence gap: send sequence 5 when last sequence was 0 -> assert error code 45 (OutOfOrderSequenceNumber)
        let record_gap = KafkaRecord::new(Some(b"key-gap".to_vec()), Some(b"val-gap".to_vec()));
        let batch_gap = encode_idempotent_records_batch(1, producer_id, producer_epoch, 5, &[record_gap]);

        let mut prod_frame_gap = BytesMut::new();
        prod_frame_gap.put_i16(0);
        prod_frame_gap.put_i16(0);
        prod_frame_gap.put_i32(704);
        put_kafka_string(&mut prod_frame_gap, Some("idempotent-client"));
        prod_frame_gap.put_i16(1);
        prod_frame_gap.put_i32(1000);
        prod_frame_gap.put_i32(1);
        put_kafka_string(&mut prod_frame_gap, Some("idemp-topic"));
        prod_frame_gap.put_i32(1);
        prod_frame_gap.put_i32(0);
        prod_frame_gap.put_i32(batch_gap.len() as i32);
        prod_frame_gap.put_slice(&batch_gap);

        let resp_gap = handle_kafka_frame(&prod_frame_gap, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur_gap = io::Cursor::new(resp_gap.as_slice());
        assert_eq!(cur_gap.get_i32(), 704);
        assert_eq!(cur_gap.get_i32(), 1);
        let _ = read_kafka_string(&mut cur_gap);
        assert_eq!(cur_gap.get_i32(), 1);
        assert_eq!(cur_gap.get_i32(), 0);
        let gap_err_code = cur_gap.get_i16();
        assert_eq!(gap_err_code, 45); // OutOfOrderSequenceNumber!

        // Assert log still did not write the out-of-order record!
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }
    }
}
