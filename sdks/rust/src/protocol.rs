use bytes::{BufMut, Bytes, BytesMut};
use crate::error::AeroError;

/// AeroStream native protocol magic bytes: [0xAE, 0x01].
pub const MAGIC: [u8; 2] = [0xAE, 0x01];

/// Protocol header length in bytes: magic (2B) + command (1B) + body_len (4B).
pub const HEADER_LEN: usize = 7;

/// Protocol status code for successful operation.
pub const STATUS_OK: u8 = 0;

/// Protocol status code indicating no data available (empty response).
pub const STATUS_EMPTY: u8 = 1;

/// Protocol status code indicating data payload follows.
pub const STATUS_DATA: u8 = 2;

/// Protocol status code indicating authentication failure.
pub const STATUS_AUTH_FAILED: u8 = 3;

/// Protocol status code indicating out-of-order sequence number.
pub const STATUS_OUT_OF_ORDER: u8 = 45;

/// Native protocol command identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Command {
    /// Command 0: Authentication handshake.
    Auth = 0,
    /// Command 1: Produce/Append message.
    Produce = 1,
    /// Command 2: Consumer Fetch (single/range).
    Fetch = 2,
    /// Command 3: Replica Fetch/Sync.
    ReplicaFetch = 3,
    /// Command 4: Multi-Entry Long-Polling Fetch.
    FetchMulti = 4,
}

impl Command {
    /// Convert byte to Command enum variant.
    pub fn from_u8(b: u8) -> Result<Self, AeroError> {
        match b {
            0 => Ok(Command::Auth),
            1 => Ok(Command::Produce),
            2 => Ok(Command::Fetch),
            3 => Ok(Command::ReplicaFetch),
            4 => Ok(Command::FetchMulti),
            other => Err(AeroError::ProtocolError(format!("Unknown command ID: {other}"))),
        }
    }
}

/// A consumed record with its log offset and payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Commit log offset of the message.
    pub offset: u64,
    /// Raw message payload.
    pub payload: Bytes,
}

impl Record {
    /// Construct a new Record.
    pub fn new(offset: u64, payload: Bytes) -> Self {
        Self { offset, payload }
    }
}

/// Validate 2-byte protocol magic prefix.
#[inline]
pub fn is_valid_magic(b0: u8, b1: u8) -> bool {
    b0 == MAGIC[0] && b1 == MAGIC[1]
}

/// Encode a standard 7-byte request header into a pre-allocated buffer.
pub fn encode_header(cmd: Command, body_len: u32, buf: &mut BytesMut) {
    buf.put_u8(MAGIC[0]);
    buf.put_u8(MAGIC[1]);
    buf.put_u8(cmd as u8);
    buf.put_u32(body_len);
}

/// Encode Command 0 (Auth) request: `[token: UTF-8]`.
pub fn encode_auth_request(token: &str) -> Bytes {
    let token_bytes = token.as_bytes();
    let mut buf = BytesMut::with_capacity(HEADER_LEN + token_bytes.len());
    encode_header(Command::Auth, token_bytes.len() as u32, &mut buf);
    buf.put_slice(token_bytes);
    buf.freeze()
}

/// Encode Command 1 (Produce) request:
/// `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][payload_len: u32 BE][payload: raw bytes]`.
pub fn encode_produce_request(topic: &str, partition: u32, payload: &[u8]) -> Result<Bytes, AeroError> {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return Err(AeroError::ProtocolError(format!(
            "Topic length {} exceeds u16::MAX",
            topic_bytes.len()
        )));
    }
    let body_len = 2 + topic_bytes.len() + 4 + 4 + payload.len();
    if body_len > u32::MAX as usize {
        return Err(AeroError::ProtocolError("Produce body length exceeds u32::MAX".into()));
    }

    let mut buf = BytesMut::with_capacity(HEADER_LEN + body_len);
    encode_header(Command::Produce, body_len as u32, &mut buf);
    buf.put_u16(topic_bytes.len() as u16);
    buf.put_slice(topic_bytes);
    buf.put_u32(partition);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

/// Encode Command 2 (Fetch) request:
/// `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE]`.
pub fn encode_fetch_request(
    topic: &str,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
) -> Result<Bytes, AeroError> {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return Err(AeroError::ProtocolError(format!(
            "Topic length {} exceeds u16::MAX",
            topic_bytes.len()
        )));
    }
    let body_len = 2 + topic_bytes.len() + 4 + 8 + 4;
    let mut buf = BytesMut::with_capacity(HEADER_LEN + body_len);
    encode_header(Command::Fetch, body_len as u32, &mut buf);
    buf.put_u16(topic_bytes.len() as u16);
    buf.put_slice(topic_bytes);
    buf.put_u32(partition);
    buf.put_u64(start_offset);
    buf.put_u32(max_bytes);
    Ok(buf.freeze())
}

/// Encode Command 4 (FetchMulti) request:
/// `[topic_len: u16 BE][topic: UTF-8][partition: u32 BE][start_offset: u64 BE][max_bytes: u32 BE][max_wait_ms: u32 BE]`.
pub fn encode_fetch_multi_request(
    topic: &str,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
) -> Result<Bytes, AeroError> {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return Err(AeroError::ProtocolError(format!(
            "Topic length {} exceeds u16::MAX",
            topic_bytes.len()
        )));
    }
    let body_len = 2 + topic_bytes.len() + 4 + 8 + 4 + 4;
    let mut buf = BytesMut::with_capacity(HEADER_LEN + body_len);
    encode_header(Command::FetchMulti, body_len as u32, &mut buf);
    buf.put_u16(topic_bytes.len() as u16);
    buf.put_slice(topic_bytes);
    buf.put_u32(partition);
    buf.put_u64(start_offset);
    buf.put_u32(max_bytes);
    buf.put_u32(max_wait_ms);
    Ok(buf.freeze())
}

/// Decode response prefix `[0xAE, 0x01, status]`.
pub fn decode_status_prefix(bytes: &[u8; 3]) -> Result<u8, AeroError> {
    if !is_valid_magic(bytes[0], bytes[1]) {
        return Err(AeroError::ProtocolError(format!(
            "Invalid magic prefix: [0x{:02X}, 0x{:02X}], expected [0xAE, 0x01]",
            bytes[0], bytes[1]
        )));
    }
    Ok(bytes[2])
}

/// Decode produce response: parses status and the 8-byte offset if successful.
pub fn decode_produce_response(status: u8, offset_bytes: Option<&[u8; 8]>) -> Result<u64, AeroError> {
    match status {
        STATUS_OK => {
            let bytes = offset_bytes.ok_or_else(|| {
                AeroError::ProtocolError("Produce OK status missing 8-byte offset".into())
            })?;
            Ok(u64::from_be_bytes(*bytes))
        }
        STATUS_AUTH_FAILED => Err(AeroError::AuthFailed("Produce rejected: not authenticated".into())),
        STATUS_OUT_OF_ORDER => Err(AeroError::OutOfOrder),
        other => Err(AeroError::ProtocolError(format!("Produce rejected with status: {other}"))),
    }
}

/// Decode Multi-Entry Fetch response entries and slice payloads.
pub fn decode_multi_entries(
    entry_count: u32,
    index_data: &[u8],
    mut payload_bytes: Bytes,
) -> Result<Vec<Record>, AeroError> {
    let expected_index_len = (entry_count as usize) * 12;
    if index_data.len() < expected_index_len {
        return Err(AeroError::ProtocolError(format!(
            "Multi-fetch index data too short: got {} bytes, expected {}",
            index_data.len(),
            expected_index_len
        )));
    }

    let mut records = Vec::with_capacity(entry_count as usize);
    let mut offset_idx = 0;
    for _ in 0..entry_count {
        let off = u64::from_be_bytes(index_data[offset_idx..offset_idx + 8].try_into().unwrap());
        let len = u32::from_be_bytes(index_data[offset_idx + 8..offset_idx + 12].try_into().unwrap()) as usize;
        offset_idx += 12;

        if payload_bytes.len() < len {
            return Err(AeroError::ProtocolError(format!(
                "Multi-fetch payload truncated: needed {} bytes, remaining {}",
                len,
                payload_bytes.len()
            )));
        }
        let record_payload = payload_bytes.split_to(len);
        records.push(Record::new(off, record_payload));
    }

    Ok(records)
}
