use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::{debug, error, warn};

pub use crate::kafka::protocol::{
    read_nullable_string, read_string, write_nullable_string, write_string,
    read_unsigned_varint, write_unsigned_varint,
    encode_response_envelope, KafkaProtocolError, RequestHeader,
    InitProducerIdRequest, InitProducerIdResponse,
};
use crate::log::LogManager;
use crate::log::producer_state::SequenceCheckResult;

// ---------------------------------------------------------------------------
// CRC32 (IEEE 802.3) and CRC32C (Castagnoli) for Kafka Records
// ---------------------------------------------------------------------------

const fn make_crc_table(poly: u32) -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const CRC32_TABLE: [u32; 256] = make_crc_table(0xEDB8_8320); // IEEE 802.3 for MessageSet
#[cfg(test)]
#[cfg(test)]
const CRC32C_TABLE: [u32; 256] = make_crc_table(0x82F6_3B78); // Castagnoli for RecordBatch

/// Computes standard IEEE 802.3 CRC32 used in legacy Kafka MessageSet (magic 0 and 1).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[idx];
    }
    !crc
}

/// Computes Castagnoli CRC32C used in modern Kafka RecordBatch (magic 2).
/// Uses the hardware instruction (SSE4.2 / ARMv8, detected at runtime): the byte-at-a-time table version this
/// replaced ran at ~0.5 GB/s, 17-22x slower, and dominated broker CPU on small records.
pub fn crc32c(data: &[u8]) -> u32 {
    ::crc32c::crc32c(data)
}

/// Reference software implementation, kept to cross-check the hardware path in tests.
#[cfg(test)]
fn crc32c_software(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[idx];
    }
    !crc
}

// ---------------------------------------------------------------------------
// ZigZag Varint and Varlong Primitives
// ---------------------------------------------------------------------------

pub fn read_varint(buf: &mut impl Buf) -> Result<i32, KafkaProtocolError> {
    let raw = read_unsigned_varint(buf)?;
    Ok(((raw >> 1) as i32) ^ (-((raw & 1) as i32)))
}

pub fn write_varint(val: i32, buf: &mut impl BufMut) {
    let raw = ((val << 1) ^ (val >> 31)) as u32;
    write_unsigned_varint(raw, buf);
}

pub fn read_varlong(buf: &mut impl Buf) -> Result<i64, KafkaProtocolError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    while buf.has_remaining() {
        let b = buf.get_u8();
        value |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            let res = ((value >> 1) as i64) ^ (-((value & 1) as i64));
            return Ok(res);
        }
        shift += 7;
        if shift >= 70 {
            return Err(KafkaProtocolError::MalformedVarint);
        }
    }
    Err(KafkaProtocolError::UnexpectedEof)
}

pub fn write_varlong(val: i64, buf: &mut impl BufMut) {
    let mut raw = ((val << 1) ^ (val >> 63)) as u64;
    while raw >= 0x80 {
        buf.put_u8(((raw & 0x7F) | 0x80) as u8);
        raw >>= 7;
    }
    buf.put_u8(raw as u8);
}

// ---------------------------------------------------------------------------
// Unified Kafka Record Data Structure
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
    pub offset: i64,
}

impl KafkaRecord {
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            headers: Vec::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            offset: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// RecordSet Parsing: RecordBatch (Magic 2) & MessageSet (Magic 0 & 1)
// ---------------------------------------------------------------------------

/// Parses raw Kafka records bytes containing either modern RecordBatches (magic 2)
/// or legacy MessageSets (magic 0/1). Handles concatenated batches seamlessly.
pub fn parse_records(mut data: &[u8]) -> Result<Vec<KafkaRecord>, KafkaProtocolError> {
    let mut records = Vec::new();

    while !data.is_empty() {
        if data.len() < 17 {
            // Incomplete header or end of padding
            break;
        }

        let magic = data[16];
        match magic {
            2 => {
                // Modern RecordBatch
                let batch_records = parse_single_record_batch(&mut data)?;
                records.extend(batch_records);
            }
            0 | 1 => {
                // Legacy MessageSet
                let msg_records = parse_single_message_set(&mut data, magic)?;
                records.extend(msg_records);
            }
            other => {
                return Err(KafkaProtocolError::Custom(format!(
                    "Unsupported record magic byte: {}",
                    other
                )));
            }
        }
    }

    Ok(records)
}

fn parse_single_record_batch(data: &mut &[u8]) -> Result<Vec<KafkaRecord>, KafkaProtocolError> {
    if data.len() < 61 {
        return Err(KafkaProtocolError::UnexpectedEof);
    }

    let mut cursor = &data[..];
    let base_offset = cursor.get_i64();
    let batch_length = cursor.get_i32();
    if batch_length < 49 {
        return Err(KafkaProtocolError::Custom(format!(
            "Invalid RecordBatch length: {}",
            batch_length
        )));
    }

    let total_batch_size = 12 + batch_length as usize;
    if data.len() < total_batch_size {
        return Err(KafkaProtocolError::UnexpectedEof);
    }

    let batch_slice = &data[..total_batch_size];
    // Advance outer buffer to next batch
    *data = &data[total_batch_size..];

    // Validate CRC32C (from byte 21 attributes to end of batch)
    let stored_crc = u32::from_be_bytes(batch_slice[17..21].try_into().unwrap());
    let computed_crc = crc32c(&batch_slice[21..]);
    if stored_crc != computed_crc {
        warn!(
            "[AeroMQ Kafka] RecordBatch CRC32C mismatch: stored=0x{:08X}, computed=0x{:08X}",
            stored_crc, computed_crc
        );
    }

    let mut batch_cursor = &batch_slice[12..]; // starts at partition_leader_epoch
    let _leader_epoch = batch_cursor.get_i32();
    let _magic = batch_cursor.get_i8();
    let _crc = batch_cursor.get_u32();
    let attributes = batch_cursor.get_i16();
    let _last_offset_delta = batch_cursor.get_i32();
    let base_timestamp = batch_cursor.get_i64();
    let _max_timestamp = batch_cursor.get_i64();
    let _producer_id = batch_cursor.get_i64();
    let _producer_epoch = batch_cursor.get_i16();
    let _base_sequence = batch_cursor.get_i32();
    let records_count = batch_cursor.get_i32();

    // Batch-level compression (attribute bits 0-2): the records section is compressed as a whole.
    let decompressed_records;
    if attributes & 0x07 != 0 {
        let codec = crate::kafka::compression::Codec::from_id(attributes & 0x07)
            .map_err(|e| KafkaProtocolError::Custom(e.to_string()))?;
        decompressed_records = crate::kafka::compression::decompress(codec, batch_cursor)
            .map_err(|e| KafkaProtocolError::Custom(e.to_string()))?;
        batch_cursor = &decompressed_records[..];
    }

    let mut records = Vec::with_capacity(records_count.max(0) as usize);
    for _ in 0..records_count {
        if !batch_cursor.has_remaining() {
            break;
        }

        let record_size = read_varint(&mut batch_cursor)?;
        if record_size <= 0 || batch_cursor.remaining() < record_size as usize {
            return Err(KafkaProtocolError::UnexpectedEof);
        }

        let mut rec_buf = &batch_cursor[..record_size as usize];
        batch_cursor = &batch_cursor[record_size as usize..];

        let _rec_attributes = rec_buf.get_i8();
        let timestamp_delta = read_varlong(&mut rec_buf)?;
        let offset_delta = read_varint(&mut rec_buf)?;

        let key_len = read_varint(&mut rec_buf)?;
        let key = if key_len >= 0 {
            if rec_buf.remaining() < key_len as usize {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let mut k = vec![0u8; key_len as usize];
            rec_buf.copy_to_slice(&mut k);
            Some(k)
        } else {
            None
        };

        let val_len = read_varint(&mut rec_buf)?;
        let value = if val_len >= 0 {
            if rec_buf.remaining() < val_len as usize {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let mut v = vec![0u8; val_len as usize];
            rec_buf.copy_to_slice(&mut v);
            Some(v)
        } else {
            None
        };

        let headers_count = read_varint(&mut rec_buf).unwrap_or(0);
        let mut headers = Vec::new();
        for _ in 0..headers_count.max(0) {
            let h_key_len = read_varint(&mut rec_buf)?;
            if h_key_len < 0 || rec_buf.remaining() < h_key_len as usize {
                break;
            }
            let mut h_key_bytes = vec![0u8; h_key_len as usize];
            rec_buf.copy_to_slice(&mut h_key_bytes);
            let h_key = String::from_utf8_lossy(&h_key_bytes).to_string();

            let h_val_len = read_varint(&mut rec_buf)?;
            let h_val = if h_val_len >= 0 {
                if rec_buf.remaining() < h_val_len as usize {
                    break;
                }
                let mut v = vec![0u8; h_val_len as usize];
                rec_buf.copy_to_slice(&mut v);
                v
            } else {
                Vec::new()
            };
            headers.push((h_key, h_val));
        }

        records.push(KafkaRecord {
            key,
            value,
            headers,
            timestamp: base_timestamp + timestamp_delta,
            offset: base_offset + offset_delta as i64,
        });
    }

    Ok(records)
}

fn parse_single_message_set(data: &mut &[u8], magic: u8) -> Result<Vec<KafkaRecord>, KafkaProtocolError> {
    if data.len() < 12 {
        return Err(KafkaProtocolError::UnexpectedEof);
    }

    let mut cursor = &data[..];
    let offset = cursor.get_i64();
    let message_size = cursor.get_i32();
    if message_size <= 0 {
        return Err(KafkaProtocolError::Custom("Invalid message size".into()));
    }

    let total_size = 12 + message_size as usize;
    if data.len() < total_size {
        return Err(KafkaProtocolError::UnexpectedEof);
    }

    let msg_slice = &data[..total_size];
    *data = &data[total_size..];

    // Validate CRC32 (bytes 16 to end of message)
    let stored_crc = u32::from_be_bytes(msg_slice[12..16].try_into().unwrap());
    let computed_crc = crc32(&msg_slice[16..]);
    if stored_crc != computed_crc {
        warn!(
            "[AeroMQ Kafka] MessageSet CRC32 mismatch: stored=0x{:08X}, computed=0x{:08X}",
            stored_crc, computed_crc
        );
    }

    let mut body = &msg_slice[16..];
    let _magic = body.get_i8();
    let attributes = body.get_i8();

    let timestamp = if magic == 1 {
        body.get_i64()
    } else {
        0
    };

    let key_len = body.get_i32();
    let key = if key_len >= 0 {
        if body.remaining() < key_len as usize {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let mut k = vec![0u8; key_len as usize];
        body.copy_to_slice(&mut k);
        Some(k)
    } else {
        None
    };

    let val_len = body.get_i32();
    let value = if val_len >= 0 {
        if body.remaining() < val_len as usize {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let mut v = vec![0u8; val_len as usize];
        body.copy_to_slice(&mut v);
        Some(v)
    } else {
        None
    };

    // Compressed wrapper message: the value holds a compressed inner message set.
    if attributes & 0x07 != 0 {
        let codec = crate::kafka::compression::Codec::from_id((attributes & 0x07) as i16)
            .map_err(|e| KafkaProtocolError::Custom(e.to_string()))?;
        let inner_bytes = crate::kafka::compression::decompress(codec, value.as_deref().unwrap_or(&[]))
            .map_err(|e| KafkaProtocolError::Custom(e.to_string()))?;
        let mut inner = parse_records(&inner_bytes)?;
        if magic == 1 {
            // Magic 1 inner offsets are relative; the wrapper carries the last absolute offset.
            let last_rel = inner.last().map(|r| r.offset).unwrap_or(0);
            for r in inner.iter_mut() {
                r.offset = offset - last_rel + r.offset;
            }
        }
        return Ok(inner);
    }

    Ok(vec![KafkaRecord {
        key,
        value,
        headers: Vec::new(),
        timestamp,
        offset,
    }])
}

// ---------------------------------------------------------------------------
// RecordBatch Encoding (Magic 2)
// ---------------------------------------------------------------------------

/// Encodes a single `KafkaRecord` into a self-contained modern Kafka `RecordBatch` (magic 2).
/// When stored in `PartitionLog`, `base_offset` directly matches the index entry offset.
pub fn encode_single_record_batch(base_offset: i64, record: &KafkaRecord) -> Vec<u8> {
    encode_records_batch(base_offset, std::slice::from_ref(record))
}

pub fn encode_records_batch(base_offset: i64, records: &[KafkaRecord]) -> Vec<u8> {
    encode_idempotent_records_batch(base_offset, -1, -1, -1, records)
}

/// Encodes a list of `KafkaRecord`s into a standard Kafka `RecordBatch` (magic 2) with PID and sequence.
pub fn encode_idempotent_records_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: &[KafkaRecord],
) -> Vec<u8> {
    if records.is_empty() {
        return Vec::new();
    }

    let first_ts = records[0].timestamp;
    let max_ts = records.iter().map(|r| r.timestamp).max().unwrap_or(first_ts);
    let last_offset_delta = (records.len() - 1) as i32;

    // Encode records body into buffer first
    let mut recs_buf = Vec::new();
    for (i, rec) in records.iter().enumerate() {
        let mut rec_inner = Vec::new();
        rec_inner.push(0u8); // attributes
        write_varlong(rec.timestamp - first_ts, &mut rec_inner);
        write_varint(i as i32, &mut rec_inner); // offset_delta

        match &rec.key {
            Some(k) => {
                write_varint(k.len() as i32, &mut rec_inner);
                rec_inner.extend_from_slice(k);
            }
            None => {
                write_varint(-1, &mut rec_inner);
            }
        }

        match &rec.value {
            Some(v) => {
                write_varint(v.len() as i32, &mut rec_inner);
                rec_inner.extend_from_slice(v);
            }
            None => {
                write_varint(-1, &mut rec_inner);
            }
        }

        write_varint(rec.headers.len() as i32, &mut rec_inner);
        for (h_k, h_v) in &rec.headers {
            write_varint(h_k.len() as i32, &mut rec_inner);
            rec_inner.extend_from_slice(h_k.as_bytes());
            write_varint(h_v.len() as i32, &mut rec_inner);
            rec_inner.extend_from_slice(h_v);
        }

        // Prepend varint record length
        write_varint(rec_inner.len() as i32, &mut recs_buf);
        recs_buf.extend_from_slice(&rec_inner);
    }

    let batch_length = 49 + recs_buf.len() as i32;

    let mut batch = Vec::with_capacity(12 + batch_length as usize);
    batch.extend_from_slice(&base_offset.to_be_bytes()); // 0..8
    batch.extend_from_slice(&batch_length.to_be_bytes()); // 8..12
    batch.extend_from_slice(&0i32.to_be_bytes()); // 12..16: partition_leader_epoch = 0
    batch.push(2u8); // 16: magic = 2

    // Placeholder for CRC32C (bytes 17..21)
    let crc_offset = batch.len();
    batch.extend_from_slice(&[0u8; 4]);

    // Bytes to calculate CRC32C over (from attributes onwards)
    let crc_start = batch.len();
    batch.extend_from_slice(&0i16.to_be_bytes()); // 21..23: attributes (no compression)
    batch.extend_from_slice(&last_offset_delta.to_be_bytes()); // 23..27: last_offset_delta
    batch.extend_from_slice(&first_ts.to_be_bytes()); // 27..35: base_timestamp
    batch.extend_from_slice(&max_ts.to_be_bytes()); // 35..43: max_timestamp
    batch.extend_from_slice(&producer_id.to_be_bytes()); // 43..51: producer_id
    batch.extend_from_slice(&producer_epoch.to_be_bytes()); // 51..53: producer_epoch
    batch.extend_from_slice(&base_sequence.to_be_bytes()); // 53..57: base_sequence
    batch.extend_from_slice(&(records.len() as i32).to_be_bytes()); // 57..61: records_count
    batch.extend_from_slice(&recs_buf); // 61..end: records

    // Compute CRC32C and patch it
    let computed_crc = crc32c(&batch[crc_start..]);
    batch[crc_offset..crc_offset + 4].copy_from_slice(&computed_crc.to_be_bytes());

    batch
}

/// Encodes a single `KafkaRecord` into a self-contained modern Kafka `RecordBatch` (magic 2) with PID and sequence.
pub fn encode_single_idempotent_record_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    record: &KafkaRecord,
) -> Vec<u8> {
    encode_idempotent_records_batch(
        base_offset,
        producer_id,
        producer_epoch,
        base_sequence,
        std::slice::from_ref(record),
    )
}

/// Extracts producer ID, epoch, base sequence, and record count from a modern RecordBatch (magic 2).
pub fn extract_batch_producer_info(data: &[u8]) -> Option<(i64, i16, i32, i32)> {
    if data.len() < 61 || data[16] != 2 {
        return None;
    }
    let producer_id = i64::from_be_bytes(data[43..51].try_into().ok()?);
    let producer_epoch = i16::from_be_bytes(data[51..53].try_into().ok()?);
    let base_sequence = i32::from_be_bytes(data[53..57].try_into().ok()?);
    let records_count = i32::from_be_bytes(data[57..61].try_into().ok()?);
    Some((producer_id, producer_epoch, base_sequence, records_count))
}

// ---------------------------------------------------------------------------
// InitProducerId (ApiKey 22) Monotonic ID Allocation & Handlers
// ---------------------------------------------------------------------------

static NEXT_PRODUCER_ID: AtomicI64 = AtomicI64::new(1000);

/// Assign monotonically increasing producer_id (starting at 1000).
pub fn allocate_producer_id() -> i64 {
    NEXT_PRODUCER_ID.fetch_add(1, Ordering::SeqCst)
}

/// Synchronous handler for InitProducerId request.
pub fn handle_init_producer_id_sync(_req: &InitProducerIdRequest) -> InitProducerIdResponse {
    let producer_id = allocate_producer_id();
    InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code: 0,
        producer_id,
        producer_epoch: 0,
    }
}

/// Asynchronous handler for Kafka InitProducerId request frame.
pub async fn handle_init_producer_id(
    header: &RequestHeader,
    mut body: Bytes,
) -> Result<InitProducerIdResponse, Box<dyn std::error::Error + Send + Sync>> {
    let req = InitProducerIdRequest::decode(&mut body, header.api_version)?;
    Ok(handle_init_producer_id_sync(&req))
}

// ---------------------------------------------------------------------------
// Produce Request & Response Types (ApiKey 0, Versions 0 to 8)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<TopicProduceData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicProduceData {
    pub topic: String,
    pub partition_data: Vec<PartitionProduceData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionProduceData {
    pub partition: i32,
    pub records: Bytes,
}

impl ProduceRequest {
    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        let transactional_id = if version >= 3 {
            read_nullable_string(buf)?
        } else {
            None
        };

        if buf.remaining() < 6 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let acks = buf.get_i16();
        let timeout_ms = buf.get_i32();

        if buf.remaining() < 4 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let topic_count = buf.get_i32();
        let mut topic_data = Vec::with_capacity(topic_count.max(0) as usize);

        for _ in 0..topic_count {
            let topic = read_string(buf)?;
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let part_count = buf.get_i32();
            let mut partition_data = Vec::with_capacity(part_count.max(0) as usize);

            for _ in 0..part_count {
                if buf.remaining() < 8 {
                    return Err(KafkaProtocolError::UnexpectedEof);
                }
                let partition = buf.get_i32();
                let records_len = buf.get_i32();
                let records = if records_len > 0 {
                    if buf.remaining() < records_len as usize {
                        return Err(KafkaProtocolError::UnexpectedEof);
                    }
                    let mut b = vec![0u8; records_len as usize];
                    buf.copy_to_slice(&mut b);
                    Bytes::from(b)
                } else {
                    Bytes::new()
                };
                partition_data.push(PartitionProduceData { partition, records });
            }

            topic_data.push(TopicProduceData {
                topic,
                partition_data,
            });
        }

        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            write_nullable_string(self.transactional_id.as_deref(), buf);
        }
        buf.put_i16(self.acks);
        buf.put_i32(self.timeout_ms);

        buf.put_i32(self.topic_data.len() as i32);
        for t in &self.topic_data {
            write_string(&t.topic, buf);
            buf.put_i32(t.partition_data.len() as i32);
            for p in &t.partition_data {
                buf.put_i32(p.partition);
                buf.put_i32(p.records.len() as i32);
                buf.put_slice(&p.records);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    pub responses: Vec<TopicProduceResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicProduceResponse {
    pub topic: String,
    pub partition_responses: Vec<PartitionProduceResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time: i64,
    pub log_start_offset: i64,
    pub error_message: Option<String>,
}

impl ProduceResponse {
    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 4 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let topic_count = buf.get_i32();
        let mut responses = Vec::with_capacity(topic_count.max(0) as usize);

        for _ in 0..topic_count {
            let topic = read_string(buf)?;
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let part_count = buf.get_i32();
            let mut partition_responses = Vec::with_capacity(part_count.max(0) as usize);

            for _ in 0..part_count {
                if buf.remaining() < 14 {
                    return Err(KafkaProtocolError::UnexpectedEof);
                }
                let partition = buf.get_i32();
                let error_code = buf.get_i16();
                let base_offset = buf.get_i64();
                let log_append_time = if version >= 2 {
                    if buf.remaining() < 8 {
                        return Err(KafkaProtocolError::UnexpectedEof);
                    }
                    buf.get_i64()
                } else {
                    -1
                };
                let log_start_offset = if version >= 5 {
                    if buf.remaining() < 8 {
                        return Err(KafkaProtocolError::UnexpectedEof);
                    }
                    buf.get_i64()
                } else {
                    0
                };

                let error_message = if version >= 8 {
                    let err_count = buf.get_i32(); // record_errors array
                    for _ in 0..err_count {
                        let _batch_idx = buf.get_i32();
                        let _msg = read_nullable_string(buf)?;
                    }
                    read_nullable_string(buf)?
                } else {
                    None
                };

                partition_responses.push(PartitionProduceResponse {
                    partition,
                    error_code,
                    base_offset,
                    log_append_time,
                    log_start_offset,
                    error_message,
                });
            }

            responses.push(TopicProduceResponse {
                topic,
                partition_responses,
            });
        }

        let throttle_time_ms = if version >= 1 && buf.remaining() >= 4 {
            buf.get_i32()
        } else {
            0
        };

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i32(self.responses.len() as i32);
        for t in &self.responses {
            write_string(&t.topic, buf);
            buf.put_i32(t.partition_responses.len() as i32);
            for p in &t.partition_responses {
                buf.put_i32(p.partition);
                buf.put_i16(p.error_code);
                buf.put_i64(p.base_offset);
                if version >= 2 {
                    buf.put_i64(p.log_append_time);
                }
                if version >= 5 {
                    buf.put_i64(p.log_start_offset);
                }
                if version >= 8 {
                    buf.put_i32(0); // record_errors length = 0
                    write_nullable_string(p.error_message.as_deref(), buf);
                }
            }
        }
        if version >= 1 {
            buf.put_i32(self.throttle_time_ms);
        }
    }
}

// ---------------------------------------------------------------------------
// Fetch Request & Response Types (ApiKey 1, Versions 0 to 11)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopic>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

impl FetchRequest {
    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 12 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let replica_id = buf.get_i32();
        let max_wait_ms = buf.get_i32();
        let min_bytes = buf.get_i32();

        let max_bytes = if version >= 3 {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            buf.get_i32()
        } else {
            0
        };

        let isolation_level = if version >= 4 {
            if !buf.has_remaining() {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            buf.get_i8()
        } else {
            0
        };

        let (session_id, session_epoch) = if version >= 7 {
            if buf.remaining() < 8 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            (buf.get_i32(), buf.get_i32())
        } else {
            (0, -1)
        };

        if buf.remaining() < 4 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let topic_count = buf.get_i32();
        let mut topics = Vec::with_capacity(topic_count.max(0) as usize);

        for _ in 0..topic_count {
            let topic = read_string(buf)?;
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let part_count = buf.get_i32();
            let mut partitions = Vec::with_capacity(part_count.max(0) as usize);

            for _ in 0..part_count {
                let partition = buf.get_i32();
                let current_leader_epoch = if version >= 9 {
                    buf.get_i32()
                } else {
                    -1
                };
                let fetch_offset = buf.get_i64();
                let log_start_offset = if version >= 5 {
                    buf.get_i64()
                } else {
                    0
                };
                let p_max_bytes = buf.get_i32();

                partitions.push(FetchPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    log_start_offset,
                    max_bytes: p_max_bytes,
                });
            }

            topics.push(FetchTopic { topic, partitions });
        }

        if version >= 7 && buf.has_remaining() {
            // forgotten_topics_data
            let forgotten_count = buf.get_i32();
            for _ in 0..forgotten_count {
                let _f_topic = read_string(buf)?;
                let f_part_count = buf.get_i32();
                for _ in 0..f_part_count {
                    let _f_partition = buf.get_i32();
                }
            }
        }

        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i32(self.replica_id);
        buf.put_i32(self.max_wait_ms);
        buf.put_i32(self.min_bytes);

        if version >= 3 {
            buf.put_i32(self.max_bytes);
        }
        if version >= 4 {
            buf.put_i8(self.isolation_level);
        }
        if version >= 7 {
            buf.put_i32(self.session_id);
            buf.put_i32(self.session_epoch);
        }

        buf.put_i32(self.topics.len() as i32);
        for t in &self.topics {
            write_string(&t.topic, buf);
            buf.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                buf.put_i32(p.partition);
                if version >= 9 {
                    buf.put_i32(p.current_leader_epoch);
                }
                buf.put_i64(p.fetch_offset);
                if version >= 5 {
                    buf.put_i64(p.log_start_offset);
                }
                buf.put_i32(p.max_bytes);
            }
        }

        if version >= 7 {
            buf.put_i32(0); // empty forgotten_topics_data
        }
    }
}

/// Partition records payload for Fetch responses.
/// Can be empty, an in-memory buffer, or an open segment file region
/// for zero-copy kernel transfer (`sendfile`).
pub enum PartitionRecords {
    Empty,
    Buffer(Bytes),
    FileRegion {
        file: File,
        position: u64,
        length: u32,
    },
}

impl std::fmt::Debug for PartitionRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "PartitionRecords::Empty"),
            Self::Buffer(b) => write!(f, "PartitionRecords::Buffer({} bytes)", b.len()),
            Self::FileRegion { position, length, .. } => {
                write!(f, "PartitionRecords::FileRegion(pos: {}, len: {})", position, length)
            }
        }
    }
}

impl PartitionRecords {
    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Buffer(b) => b.len(),
            Self::FileRegion { length, .. } => *length as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reads this record source into an in-memory buffer if it was a file region.
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        match self {
            Self::Empty => Ok(Bytes::new()),
            Self::Buffer(b) => Ok(b.clone()),
            Self::FileRegion { file, position, length } => {
                let mut f = file.try_clone()?;
                f.seek(SeekFrom::Start(*position))?;
                let mut buf = vec![0u8; *length as usize];
                f.read_exact(&mut buf)?;
                Ok(Bytes::from(buf))
            }
        }
    }
}

#[derive(Debug)]
pub struct FetchResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub session_id: i32,
    pub responses: Vec<FetchTopicResponse>,
}

#[derive(Debug)]
pub struct FetchTopicResponse {
    pub topic: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub records: PartitionRecords,
}

impl FetchResponse {
    /// Serializes the entire Fetch response into a single `Bytes` envelope,
    /// reading any `FileRegion` segments into memory.
    pub fn encode_to_bytes(&self, version: i16, correlation_id: i32) -> Result<Bytes, KafkaProtocolError> {
        let mut body = BytesMut::new();

        if version >= 1 {
            body.put_i32(self.throttle_time_ms);
        }
        if version >= 7 {
            body.put_i16(self.error_code);
            body.put_i32(self.session_id);
        }

        body.put_i32(self.responses.len() as i32);
        for t in &self.responses {
            write_string(&t.topic, &mut body);
            body.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                body.put_i32(p.partition_index);
                body.put_i16(p.error_code);
                body.put_i64(p.high_watermark);
                if version >= 4 {
                    body.put_i64(p.last_stable_offset);
                }
                if version >= 5 {
                    body.put_i64(p.log_start_offset);
                }
                if version >= 4 {
                    body.put_i32(0); // empty aborted_transactions
                }
                if version >= 11 {
                    body.put_i32(-1); // preferred_read_replica
                }

                let rec_bytes = p.records.to_bytes().map_err(|e| {
                    KafkaProtocolError::Custom(format!("Failed reading segment file: {}", e))
                })?;
                body.put_i32(rec_bytes.len() as i32);
                body.put_slice(&rec_bytes);
            }
        }

        Ok(encode_response_envelope(correlation_id, &body))
    }

    /// Splits the Fetch response into:
    /// 1. Preamble bytes (up to and including records_len prefix)
    /// 2. Optional `FileRegion` (file handle, position, length) for zero-copy `sendfile`
    /// 3. Remaining response bytes
    /// Enables true zero-copy OS page-cache transfer directly to TCP socket.
    pub fn encode_split_for_zero_copy(
        &self,
        version: i16,
        correlation_id: i32,
    ) -> Result<ZeroCopyFetchEnvelope, KafkaProtocolError> {
        let total_file_regions: usize = self
            .responses
            .iter()
            .flat_map(|t| &t.partitions)
            .filter(|p| matches!(p.records, PartitionRecords::FileRegion { .. }))
            .count();

        // If there are no FileRegions or multiple topics, serialize directly to single buffer
        if total_file_regions != 1 || self.responses.len() != 1 || self.responses[0].partitions.len() != 1 {
            let full_bytes = self.encode_to_bytes(version, correlation_id)?;
            return Ok(ZeroCopyFetchEnvelope::Buffered(full_bytes));
        }

        // Single partition with FileRegion: we can emit preamble, file slice, and 0 postamble
        let topic_resp = &self.responses[0];
        let part_resp = &topic_resp.partitions[0];

        let (file_clone, pos, len) = match &part_resp.records {
            PartitionRecords::FileRegion { file, position, length } => {
                let f = file.try_clone().map_err(|e| {
                    KafkaProtocolError::Custom(format!("Failed cloning file: {}", e))
                })?;
                (f, *position, *length)
            }
            _ => {
                let full_bytes = self.encode_to_bytes(version, correlation_id)?;
                return Ok(ZeroCopyFetchEnvelope::Buffered(full_bytes));
            }
        };

        // Preamble body
        let mut preamble_body = BytesMut::new();
        if version >= 1 {
            preamble_body.put_i32(self.throttle_time_ms);
        }
        if version >= 7 {
            preamble_body.put_i16(self.error_code);
            preamble_body.put_i32(self.session_id);
        }

        preamble_body.put_i32(1); // 1 topic
        write_string(&topic_resp.topic, &mut preamble_body);
        preamble_body.put_i32(1); // 1 partition

        preamble_body.put_i32(part_resp.partition_index);
        preamble_body.put_i16(part_resp.error_code);
        preamble_body.put_i64(part_resp.high_watermark);
        if version >= 4 {
            preamble_body.put_i64(part_resp.last_stable_offset);
        }
        if version >= 5 {
            preamble_body.put_i64(part_resp.log_start_offset);
        }
        if version >= 4 {
            preamble_body.put_i32(0); // empty aborted_transactions
        }
        if version >= 11 {
            preamble_body.put_i32(-1); // preferred_read_replica
        }

        // records length prefix
        preamble_body.put_i32(len as i32);

        // Frame header: total message length = correlation_id(4) + preamble_body.len() + len
        let total_msg_len = 4 + preamble_body.len() + len as usize;
        let mut frame_header = BytesMut::with_capacity(8);
        frame_header.put_i32(total_msg_len as i32);
        frame_header.put_i32(correlation_id);

        let mut preamble = BytesMut::with_capacity(frame_header.len() + preamble_body.len());
        preamble.extend_from_slice(&frame_header);
        preamble.extend_from_slice(&preamble_body);

        Ok(ZeroCopyFetchEnvelope::ZeroCopy {
            preamble: preamble.freeze(),
            file: file_clone,
            position: pos,
            length: len,
        })
    }
}

pub enum ZeroCopyFetchEnvelope {
    Buffered(Bytes),
    ZeroCopy {
        preamble: Bytes,
        file: File,
        position: u64,
        length: u32,
    },
}

// ---------------------------------------------------------------------------
// Business Logic Handlers: Produce (ApiKey 0) & Fetch (ApiKey 1)
// ---------------------------------------------------------------------------

/// Handles an incoming Kafka Produce request (ApiKey 0, versions 0 to 8).
///
/// 1. Decodes and parses RecordBatches or legacy MessageSets across topics/partitions.
/// 2. Directly appends incoming records into AeroStream's `PartitionLog` via `append()`.
/// 3. Returns standard Kafka Produce response with the assigned base offset.
pub async fn handle_produce(
    header: &RequestHeader,
    mut body: Bytes,
    log_manager: &Arc<LogManager>,
) -> Result<ProduceResponse, Box<dyn std::error::Error + Send + Sync>> {
    let req = ProduceRequest::decode(&mut body, header.api_version)?;
    let mut responses = Vec::with_capacity(req.topic_data.len());

    for topic_entry in req.topic_data {
        let mut part_responses = Vec::with_capacity(topic_entry.partition_data.len());

        for part_entry in topic_entry.partition_data {
            let partition = part_entry.partition;

            // Validate codec/body up-front so unsupported codecs map to UNSUPPORTED_COMPRESSION_TYPE (76)
            if let Err(e) = crate::kafka::compression::normalize_produce_payload(
                &part_entry.records,
                crate::kafka::compression::CompressionType::Producer,
            ) {
                part_responses.push(PartitionProduceResponse {
                    partition,
                    error_code: e.error_code(),
                    base_offset: -1,
                    log_append_time: -1,
                    log_start_offset: 0,
                    error_message: Some(e.to_string()),
                });
                continue;
            }
            let topic_ctype = crate::kafka::compression::registry().for_topic(&topic_entry.topic);

            // Parse records from the incoming RecordBatch / MessageSet bytes
            let records = match parse_records(&part_entry.records) {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        "[AeroMQ Kafka] Failed parsing produce records for {}/{}: {:?}",
                        topic_entry.topic, partition, e
                    );
                    part_responses.push(PartitionProduceResponse {
                        partition,
                        error_code: 2, // CORRUPT_MESSAGE
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: 0,
                        error_message: Some(e.to_string()),
                    });
                    continue;
                }
            };

            // Acquire partition log
            let part_log = match log_manager.get_partition(&topic_entry.topic, partition as u32).await {
                Ok(l) => l,
                Err(e) => {
                    error!(
                        "[AeroMQ Kafka] Failed accessing partition log for {}/{}: {:?}",
                        topic_entry.topic, partition, e
                    );
                    part_responses.push(PartitionProduceResponse {
                        partition,
                        error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: 0,
                        error_message: Some(e.to_string()),
                    });
                    continue;
                }
            };

            let mut guard = part_log.lock().await;
            let mut base_offset = guard.next_offset as i64;
            let current_next_offset = guard.next_offset;

            let producer_info = extract_batch_producer_info(&part_entry.records);
            if let Some((producer_id, epoch, base_sequence, record_count)) = producer_info {
                if producer_id >= 0 && base_sequence >= 0 {
                    let seq_check = guard.producer_tracker.check_and_update_sequence(
                        producer_id,
                        epoch,
                        base_sequence,
                        record_count,
                        current_next_offset,
                    );
                    match seq_check {
                        SequenceCheckResult::Duplicate { last_offset } => {
                            debug!(
                                "[AeroMQ Kafka] Duplicate sequence {} for producer_id {}, returning cached offset {}",
                                base_sequence, producer_id, last_offset
                            );
                            part_responses.push(PartitionProduceResponse {
                                partition,
                                error_code: 0,
                                base_offset: last_offset as i64,
                                log_append_time: -1,
                                log_start_offset: 0,
                                error_message: None,
                            });
                            continue;
                        }
                        SequenceCheckResult::OutOfOrder { error_code } => {
                            warn!(
                                "[AeroMQ Kafka] Out of order sequence {} for producer_id {} (error {})",
                                base_sequence, producer_id, error_code
                            );
                            part_responses.push(PartitionProduceResponse {
                                partition,
                                error_code, // 45: OutOfOrderSequenceNumber
                                base_offset: -1,
                                log_append_time: -1,
                                log_start_offset: 0,
                                error_message: Some("OutOfOrderSequenceNumber".to_string()),
                            });
                            continue;
                        }
                        SequenceCheckResult::ValidNext => {
                            // Proceed to append!
                        }
                    }
                }
            }

            if records.is_empty() {
                // Empty produce / heartbeat record
                part_responses.push(PartitionProduceResponse {
                    partition,
                    error_code: 0,
                    base_offset,
                    log_append_time: -1,
                    log_start_offset: 0,
                    error_message: None,
                });
                continue;
            }

            let mut first_assigned = None;
            for (i, rec) in records.iter().enumerate() {
                let next_off = guard.next_offset as i64;
                // Encode record as a valid modern RecordBatch so that it can be
                // served directly with zero-copy on subsequent Kafka Fetch requests!
                let encoded_batch = if let Some((pid, epoch, base_seq, _)) = producer_info {
                    if pid >= 0 && base_seq >= 0 {
                        encode_single_idempotent_record_batch(
                            next_off,
                            pid,
                            epoch,
                            base_seq + i as i32,
                            rec,
                        )
                    } else {
                        encode_single_record_batch(next_off, rec)
                    }
                } else {
                    encode_single_record_batch(next_off, rec)
                };
                let encoded_batch = match crate::kafka::compression::normalize_produce_payload(&encoded_batch, topic_ctype) {
                    Ok(b) => b,
                    Err(_) => encoded_batch,
                };
                let assigned_offset = guard.append(&encoded_batch)?;
                if first_assigned.is_none() {
                    first_assigned = Some(assigned_offset as i64);
                }
            }

            base_offset = first_assigned.unwrap_or(base_offset);
            debug!(
                "[AeroMQ Kafka] Produced {} records to {}/{} at base_offset {}",
                records.len(),
                topic_entry.topic,
                partition,
                base_offset
            );

            part_responses.push(PartitionProduceResponse {
                partition,
                error_code: 0,
                base_offset,
                log_append_time: -1,
                log_start_offset: 0,
                error_message: None,
            });
        }

        responses.push(TopicProduceResponse {
            topic: topic_entry.topic,
            partition_responses: part_responses,
        });
    }

    Ok(ProduceResponse {
        responses,
        throttle_time_ms: 0,
    })
}

/// Handles an incoming Kafka Fetch request (ApiKey 1, versions 0 to 11).
///
/// 1. Reads segments from AeroStream's `PartitionLog` via `read_from_offset()`.
/// 2. Respects partition High-Watermark limits.
/// 3. Returns a `FetchResponse` configured for zero-copy file region or buffer transfer.
pub async fn handle_fetch(
    header: &RequestHeader,
    mut body: Bytes,
    log_manager: &Arc<LogManager>,
) -> Result<FetchResponse, Box<dyn std::error::Error + Send + Sync>> {
    let req = FetchRequest::decode(&mut body, header.api_version)?;
    let mut responses = Vec::with_capacity(req.topics.len());

    for topic_req in req.topics {
        let mut part_responses = Vec::with_capacity(topic_req.partitions.len());

        for part_req in topic_req.partitions {
            let partition = part_req.partition;
            let fetch_offset = part_req.fetch_offset;
            let max_bytes = part_req.max_bytes.max(1024) as u32;

            let part_log = match log_manager.get_partition(&topic_req.topic, partition as u32).await {
                Ok(l) => l,
                Err(_e) => {
                    part_responses.push(FetchPartitionResponse {
                        partition_index: partition,
                        error_code: 3, // UNKNOWN_TOPIC_OR_PARTITION
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        records: PartitionRecords::Empty,
                    });
                    continue;
                }
            };
            let (hw, next_off, read_opt) = {
                let mut guard = part_log.lock().await;
                let hw = guard.high_watermark as i64;
                let next_off = guard.next_offset as i64;
                let opt = if fetch_offset <= next_off && fetch_offset < hw {
                    guard.read_from_offset(fetch_offset as u64, max_bytes)?
                } else {
                    None
                };
                (hw, next_off, opt)
            }; // <-- Partition lock released!

            // Offset validation
            if fetch_offset > next_off {
                part_responses.push(FetchPartitionResponse {
                    partition_index: partition,
                    error_code: 1, // OFFSET_OUT_OF_RANGE
                    high_watermark: hw,
                    last_stable_offset: hw,
                    log_start_offset: 0,
                    records: PartitionRecords::Empty,
                });
                continue;
            }

            // High-Watermark guard: do not return uncommitted data
            if fetch_offset >= hw {
                part_responses.push(FetchPartitionResponse {
                    partition_index: partition,
                    error_code: 0,
                    high_watermark: hw,
                    last_stable_offset: hw,
                    log_start_offset: 0,
                    records: PartitionRecords::Empty,
                });
                continue;
            }

            // Read segment from log
            match read_opt {
                Some((mut file, position, bytes_to_read)) => {
                    // Check if segment is already a valid RecordBatch (magic = 2 at byte 16)
                    let is_valid_batch = if bytes_to_read >= 61 {
                        let mut header_check = [0u8; 17];
                        let _ = file.seek(SeekFrom::Start(position));
                        if file.read_exact(&mut header_check).is_ok() && header_check[16] == 2 {
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    let records = if is_valid_batch {
                        // Direct zero-copy file region!
                        PartitionRecords::FileRegion {
                            file,
                            position,
                            length: bytes_to_read,
                        }
                    } else {
                        // Raw payload (from native AeroStream produce): wrap in standard RecordBatch
                        let _ = file.seek(SeekFrom::Start(position));
                        let mut raw_buf = vec![0u8; bytes_to_read as usize];
                        file.read_exact(&mut raw_buf)?;

                        let rec = KafkaRecord::new(None, Some(raw_buf));
                        let batch = encode_single_record_batch(fetch_offset, &rec);
                        PartitionRecords::Buffer(Bytes::from(batch))
                    };

                    part_responses.push(FetchPartitionResponse {
                        partition_index: partition,
                        error_code: 0,
                        high_watermark: hw,
                        last_stable_offset: hw,
                        log_start_offset: 0,
                        records,
                    });
                }
                None => {
                    part_responses.push(FetchPartitionResponse {
                        partition_index: partition,
                        error_code: 0,
                        high_watermark: hw,
                        last_stable_offset: hw,
                        log_start_offset: 0,
                        records: PartitionRecords::Empty,
                    });
                }
            }
        }

        responses.push(FetchTopicResponse {
            topic: topic_req.topic,
            partitions: part_responses,
        });
    }

    Ok(FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses,
    })
}

// ---------------------------------------------------------------------------
// Unit and Integration Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_crc32_and_crc32c_calculations() {
        let data = b"123456789";
        // Standard check values for "123456789":
        // CRC32 (IEEE 802.3): 0xCBF43926
        // CRC32C (Castagnoli): 0xE3069283
        assert_eq!(crc32(data), 0xCBF43926);
        assert_eq!(crc32c(data), 0xE3069283);
    }

    #[test]
    fn crc32c_hardware_matches_software_for_all_small_lengths_and_large_buffers() {
        let data: Vec<u8> = (0..70_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        for len in (0..300).chain([1023, 1024, 1025, 4096, 65_536, 70_000]) {
            assert_eq!(crc32c(&data[..len]), crc32c_software(&data[..len]), "length {}", len);
        }
        // unaligned starts
        for start in 1..9 {
            assert_eq!(crc32c(&data[start..start + 5000]), crc32c_software(&data[start..start + 5000]));
        }
    }

    #[test]
    fn test_zigzag_varint_and_varlong() {
        let test_vals_32 = [0, -1, 1, -2, 2, 2147483647, -2147483648];
        for &val in &test_vals_32 {
            let mut buf = Vec::new();
            write_varint(val, &mut buf);
            let mut read_buf = &buf[..];
            let decoded = read_varint(&mut read_buf).unwrap();
            assert_eq!(decoded, val);
            assert!(read_buf.is_empty());
        }

        let test_vals_64 = [0, -1, 1, -2, 2, 9223372036854775807, -9223372036854775808];
        for &val in &test_vals_64 {
            let mut buf = Vec::new();
            write_varlong(val, &mut buf);
            let mut read_buf = &buf[..];
            let decoded = read_varlong(&mut read_buf).unwrap();
            assert_eq!(decoded, val);
            assert!(read_buf.is_empty());
        }
    }

    #[test]
    fn test_record_batch_roundtrip() {
        let mut rec1 = KafkaRecord::new(Some(b"order-key".to_vec()), Some(b"order-payload-1".to_vec()));
        rec1.headers.push(("source".to_string(), b"checkout".to_vec()));
        let rec2 = KafkaRecord::new(None, Some(b"order-payload-2".to_vec()));

        let records = vec![rec1, rec2];
        let base_offset = 100;
        let batch_bytes = encode_records_batch(base_offset, &records);

        // Check magic byte at offset 16
        assert_eq!(batch_bytes[16], 2);

        let parsed = parse_records(&batch_bytes).expect("should parse valid RecordBatch");
        assert_eq!(parsed.len(), 2);

        assert_eq!(parsed[0].offset, 100);
        assert_eq!(parsed[0].key.as_deref(), Some(&b"order-key"[..]));
        assert_eq!(parsed[0].value.as_deref(), Some(&b"order-payload-1"[..]));
        assert_eq!(parsed[0].headers.len(), 1);
        assert_eq!(parsed[0].headers[0].0, "source");
        assert_eq!(parsed[0].headers[0].1, b"checkout");

        assert_eq!(parsed[1].offset, 101);
        assert_eq!(parsed[1].key, None);
        assert_eq!(parsed[1].value.as_deref(), Some(&b"order-payload-2"[..]));
    }

    #[test]
    fn test_produce_request_and_response_roundtrip() {
        let req = ProduceRequest {
            transactional_id: Some("txn-app-1".to_string()),
            acks: 1,
            timeout_ms: 5000,
            topic_data: vec![TopicProduceData {
                topic: "orders".to_string(),
                partition_data: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from(encode_single_record_batch(
                        0,
                        &KafkaRecord::new(Some(b"k1".to_vec()), Some(b"v1".to_vec())),
                    )),
                }],
            }],
        };

        for version in [0, 1, 2, 3, 5, 7, 8] {
            let mut buf = BytesMut::new();
            req.encode(version, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = ProduceRequest::decode(&mut read_buf, version).unwrap();

            assert_eq!(decoded.acks, req.acks);
            assert_eq!(decoded.timeout_ms, req.timeout_ms);
            assert_eq!(decoded.topic_data.len(), 1);
            assert_eq!(decoded.topic_data[0].topic, "orders");
            assert_eq!(decoded.topic_data[0].partition_data[0].partition, 0);

            if version >= 3 {
                assert_eq!(decoded.transactional_id, req.transactional_id);
            }
        }

        let resp = ProduceResponse {
            responses: vec![TopicProduceResponse {
                topic: "orders".to_string(),
                partition_responses: vec![PartitionProduceResponse {
                    partition: 0,
                    error_code: 0,
                    base_offset: 42,
                    log_append_time: 123456789,
                    log_start_offset: 0,
                    error_message: None,
                }],
            }],
            throttle_time_ms: 15,
        };

        for version in [0, 1, 2, 5, 8] {
            let mut buf = BytesMut::new();
            resp.encode(version, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = ProduceResponse::decode(&mut read_buf, version).unwrap();

            assert_eq!(decoded.responses.len(), 1);
            assert_eq!(decoded.responses[0].topic, "orders");
            assert_eq!(decoded.responses[0].partition_responses[0].base_offset, 42);
            if version >= 1 {
                assert_eq!(decoded.throttle_time_ms, 15);
            }
            if version >= 2 {
                assert_eq!(decoded.responses[0].partition_responses[0].log_append_time, 123456789);
            }
        }
    }

    #[test]
    fn test_fetch_request_and_response_roundtrip() {
        let req = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 65536,
            isolation_level: 0,
            session_id: 10,
            session_epoch: 1,
            topics: vec![FetchTopic {
                topic: "clicks".to_string(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    fetch_offset: 100,
                    log_start_offset: 0,
                    max_bytes: 32768,
                }],
            }],
        };

        for version in [0, 1, 3, 4, 5, 7, 9, 11] {
            let mut buf = BytesMut::new();
            req.encode(version, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = FetchRequest::decode(&mut read_buf, version).unwrap();

            assert_eq!(decoded.replica_id, -1);
            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].topic, "clicks");
            assert_eq!(decoded.topics[0].partitions[0].fetch_offset, 100);
        }
    }

    #[tokio::test]
    async fn test_produce_and_fetch_end_to_end_with_partition_log() {
        let test_dir = PathBuf::from("./data/test_kafka_handlers_pipeline");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let log_manager = Arc::new(LogManager::new(&test_dir, 1).with_limits(1024, None, None));

        // 1. Build Produce Request
        let mut rec1 = KafkaRecord::new(Some(b"user-42".to_vec()), Some(b"purchase-item-99".to_vec()));
        rec1.headers.push(("currency".to_string(), b"USD".to_vec()));
        let rec2 = KafkaRecord::new(Some(b"user-43".to_vec()), Some(b"purchase-item-100".to_vec()));

        let records_batch = encode_records_batch(0, &[rec1, rec2]);

        let produce_req = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 1000,
            topic_data: vec![TopicProduceData {
                topic: "transactions".to_string(),
                partition_data: vec![PartitionProduceData {
                    partition: 0,
                    records: Bytes::from(records_batch),
                }],
            }],
        };

        let mut produce_buf = BytesMut::new();
        produce_req.encode(7, &mut produce_buf);

        let produce_header = RequestHeader::new(0, 7, 1001, Some("producer-client".to_string()));
        let produce_resp = handle_produce(&produce_header, produce_buf.freeze(), &log_manager)
            .await
            .unwrap();

        assert_eq!(produce_resp.responses.len(), 1);
        let part_res = &produce_resp.responses[0].partition_responses[0];
        assert_eq!(part_res.error_code, 0);
        assert_eq!(part_res.base_offset, 0);

        // 2. Fetch offset 0
        let fetch_req = FetchRequest {
            replica_id: -1,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 65536,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: "transactions".to_string(),
                partitions: vec![FetchPartition {
                    partition: 0,
                    current_leader_epoch: 0,
                    fetch_offset: 0,
                    log_start_offset: 0,
                    max_bytes: 65536,
                }],
            }],
        };

        let mut fetch_buf = BytesMut::new();
        fetch_req.encode(11, &mut fetch_buf);

        let fetch_header = RequestHeader::new(1, 11, 1002, Some("consumer-client".to_string()));
        let fetch_resp = handle_fetch(&fetch_header, fetch_buf.freeze(), &log_manager)
            .await
            .unwrap();

        assert_eq!(fetch_resp.responses.len(), 1);
        let f_part = &fetch_resp.responses[0].partitions[0];
        assert_eq!(f_part.error_code, 0);
        assert!(f_part.records.len() > 0);

        // Verify zero-copy splitting envelope
        let zero_copy_env = fetch_resp
            .encode_split_for_zero_copy(11, 1002)
            .expect("should encode zero-copy split");

        match zero_copy_env {
            ZeroCopyFetchEnvelope::ZeroCopy { preamble, file, position, length } => {
                assert!(!preamble.is_empty());
                assert!(length > 0);

                // Verify file content directly
                let mut f = file;
                f.seek(SeekFrom::Start(position)).unwrap();
                let mut read_bytes = vec![0u8; length as usize];
                f.read_exact(&mut read_bytes).unwrap();

                let fetched_records = parse_records(&read_bytes).unwrap();
                assert_eq!(fetched_records.len(), 1);
                assert_eq!(fetched_records[0].offset, 0);
                assert_eq!(fetched_records[0].key.as_deref(), Some(&b"user-42"[..]));
                assert_eq!(fetched_records[0].value.as_deref(), Some(&b"purchase-item-99"[..]));
            }
            ZeroCopyFetchEnvelope::Buffered(_) => panic!("Expected ZeroCopy split"),
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_dir);
    }

    #[tokio::test]
    async fn test_init_producer_id_handler_monotonic_pid() {
        let header = RequestHeader::new(22, 2, 9999, Some("test-client".to_string()));
        let req1 = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 10000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let mut buf1 = BytesMut::new();
        req1.encode(2, &mut buf1);

        let resp1 = handle_init_producer_id(&header, buf1.freeze()).await.unwrap();
        assert_eq!(resp1.error_code, 0);
        assert!(resp1.producer_id >= 1000);
        assert_eq!(resp1.producer_epoch, 0);

        let req2 = InitProducerIdRequest {
            transactional_id: None,
            transaction_timeout_ms: 10000,
            producer_id: -1,
            producer_epoch: -1,
        };
        let mut buf2 = BytesMut::new();
        req2.encode(2, &mut buf2);

        let resp2 = handle_init_producer_id(&header, buf2.freeze()).await.unwrap();
        assert_eq!(resp2.error_code, 0);
        assert_eq!(resp2.producer_id, resp1.producer_id + 1);
        assert_eq!(resp2.producer_epoch, 0);
    }

    #[tokio::test]
    async fn test_idempotent_produce_sequence_progression_duplicate_and_out_of_order() {
        let test_dir = PathBuf::from("./data/test_kafka_idempotent_produce");
        let _ = fs::remove_dir_all(&test_dir);
        fs::create_dir_all(&test_dir).unwrap();

        let log_manager = Arc::new(LogManager::new(&test_dir, 1).with_limits(1024 * 1024, None, None));
        let topic = "idempotent-topic";
        let partition = 0i32;

        let pid = allocate_producer_id();
        let epoch = 0i16;

        let produce_batch = |seq: i32, payload: &[u8]| -> ProduceRequest {
            let rec = KafkaRecord::new(None, Some(payload.to_vec()));
            let batch_bytes = encode_idempotent_records_batch(0, pid, epoch, seq, &[rec]);
            ProduceRequest {
                transactional_id: None,
                acks: 1,
                timeout_ms: 1000,
                topic_data: vec![TopicProduceData {
                    topic: topic.to_string(),
                    partition_data: vec![PartitionProduceData {
                        partition,
                        records: Bytes::from(batch_bytes),
                    }],
                }],
            }
        };

        // 1. Sequence Progression: 0, 1, 2 accepted
        for seq in 0..=2 {
            let req = produce_batch(seq, format!("msg-seq-{}", seq).as_bytes());
            let mut buf = BytesMut::new();
            req.encode(7, &mut buf);
            let header = RequestHeader::new(0, 7, 2000 + seq, Some("idempotent-producer".to_string()));
            let resp = handle_produce(&header, buf.freeze(), &log_manager).await.unwrap();

            assert_eq!(resp.responses.len(), 1);
            let part_resp = &resp.responses[0].partition_responses[0];
            assert_eq!(part_resp.error_code, 0, "Sequence {} should be accepted", seq);
            assert_eq!(part_resp.base_offset, seq as i64, "Sequence {} should have offset {}", seq, seq);
        }

        // Verify partition log contains exactly 3 records (next_offset == 3)
        let part_log = log_manager.get_partition(topic, partition as u32).await.unwrap();
        {
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 3);
        }

        // 2. Duplicate Sequence: retrying sequence 1 does NOT append duplicate record, returns cached offset
        let dup_req = produce_batch(1, b"msg-seq-1-retry");
        let mut dup_buf = BytesMut::new();
        dup_req.encode(7, &mut dup_buf);
        let dup_header = RequestHeader::new(0, 7, 3001, Some("idempotent-producer".to_string()));
        let dup_resp = handle_produce(&dup_header, dup_buf.freeze(), &log_manager).await.unwrap();

        assert_eq!(dup_resp.responses.len(), 1);
        let dup_part_resp = &dup_resp.responses[0].partition_responses[0];
        assert_eq!(dup_part_resp.error_code, 0, "Duplicate sequence should return success (error 0)");
        assert_eq!(dup_part_resp.base_offset, 1, "Duplicate sequence 1 must return cached offset 1");

        // Verify partition log was NOT appended (next_offset is STILL 3)
        {
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 3, "Duplicate record must NOT be appended to disk");
        }

        // 3. Out of Order Sequence: sequence 5 after sequence 2 (or 1) returns OutOfOrder error (45)
        let ooo_req = produce_batch(5, b"msg-seq-5-gap");
        let mut ooo_buf = BytesMut::new();
        ooo_req.encode(7, &mut ooo_buf);
        let ooo_header = RequestHeader::new(0, 7, 3005, Some("idempotent-producer".to_string()));
        let ooo_resp = handle_produce(&ooo_header, ooo_buf.freeze(), &log_manager).await.unwrap();

        assert_eq!(ooo_resp.responses.len(), 1);
        let ooo_part_resp = &ooo_resp.responses[0].partition_responses[0];
        assert_eq!(ooo_part_resp.error_code, 45, "Out of order sequence must return error code 45 (OutOfOrderSequenceNumber)");
        assert_eq!(ooo_part_resp.base_offset, -1);

        // Verify partition log still untouched
        {
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 3, "Out of order sequence must NOT be appended to disk");
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_dir);
    }
}
