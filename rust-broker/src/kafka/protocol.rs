use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Kafka API Keys as defined in the Apache Kafka protocol specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    InitProducerId = 22,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(val: i16) -> Self {
        match val {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            4 => ApiKey::LeaderAndIsr,
            5 => ApiKey::StopReplica,
            6 => ApiKey::UpdateMetadata,
            7 => ApiKey::ControlledShutdown,
            8 => ApiKey::OffsetCommit,
            9 => ApiKey::OffsetFetch,
            10 => ApiKey::FindCoordinator,
            11 => ApiKey::JoinGroup,
            12 => ApiKey::Heartbeat,
            13 => ApiKey::LeaveGroup,
            14 => ApiKey::SyncGroup,
            15 => ApiKey::DescribeGroups,
            16 => ApiKey::ListGroups,
            17 => ApiKey::SaslHandshake,
            18 => ApiKey::ApiVersions,
            19 => ApiKey::CreateTopics,
            20 => ApiKey::DeleteTopics,
            22 => ApiKey::InitProducerId,
            other => ApiKey::Unknown(other),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Metadata => 3,
            ApiKey::LeaderAndIsr => 4,
            ApiKey::StopReplica => 5,
            ApiKey::UpdateMetadata => 6,
            ApiKey::ControlledShutdown => 7,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::DescribeGroups => 15,
            ApiKey::ListGroups => 16,
            ApiKey::SaslHandshake => 17,
            ApiKey::ApiVersions => 18,
            ApiKey::CreateTopics => 19,
            ApiKey::DeleteTopics => 20,
            ApiKey::InitProducerId => 22,
            ApiKey::Unknown(v) => v,
        }
    }
}

impl ApiKey {
    pub fn as_i16(self) -> i16 {
        i16::from(self)
    }
}

/// Errors encountered when decoding or encoding Kafka wire protocol frames.
#[derive(Debug, PartialEq, Eq)]
pub enum KafkaProtocolError {
    UnexpectedEof,
    InvalidUtf8(String),
    MalformedVarint,
    UnexpectedNullString,
    UnsupportedApiKey(i16),
    UnsupportedVersion { api_key: i16, version: i16 },
    FrameTooLarge(usize),
    InvalidFrameSize(i32),
    Custom(String),
}

impl std::fmt::Display for KafkaProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "Unexpected EOF while parsing Kafka protocol frame"),
            Self::InvalidUtf8(e) => write!(f, "Invalid UTF-8 string: {}", e),
            Self::MalformedVarint => write!(f, "Malformed varint in Kafka packet"),
            Self::UnexpectedNullString => write!(f, "Unexpected null string in non-nullable position"),
            Self::UnsupportedApiKey(k) => write!(f, "Unsupported Kafka ApiKey: {}", k),
            Self::UnsupportedVersion { api_key, version } => {
                write!(f, "Unsupported version {} for ApiKey {}", version, api_key)
            }
            Self::FrameTooLarge(sz) => write!(f, "Kafka frame too large: {} bytes", sz),
            Self::InvalidFrameSize(sz) => write!(f, "Invalid Kafka frame size: {}", sz),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for KafkaProtocolError {}

// ---------------------------------------------------------------------------
// Low-level Kafka wire primitives
// ---------------------------------------------------------------------------

pub fn read_unsigned_varint(buf: &mut impl Buf) -> Result<u32, KafkaProtocolError> {
    let mut value: u32 = 0;
    let mut shift: u32 = 0;
    while buf.has_remaining() {
        let b = buf.get_u8();
        value |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 35 {
            return Err(KafkaProtocolError::MalformedVarint);
        }
    }
    Err(KafkaProtocolError::UnexpectedEof)
}

pub fn write_unsigned_varint(mut value: u32, buf: &mut impl BufMut) {
    while value >= 0x80 {
        buf.put_u8(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

pub fn skip_tagged_fields(buf: &mut impl Buf) -> Result<(), KafkaProtocolError> {
    let count = read_unsigned_varint(buf)?;
    for _ in 0..count {
        let _tag = read_unsigned_varint(buf)?;
        let size = read_unsigned_varint(buf)?;
        if buf.remaining() < size as usize {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        buf.advance(size as usize);
    }
    Ok(())
}

pub fn read_string(buf: &mut impl Buf) -> Result<String, KafkaProtocolError> {
    read_nullable_string(buf)?.ok_or(KafkaProtocolError::UnexpectedNullString)
}

pub fn read_nullable_string(buf: &mut impl Buf) -> Result<Option<String>, KafkaProtocolError> {
    if buf.remaining() < 2 {
        return Err(KafkaProtocolError::UnexpectedEof);
    }
    let len = buf.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(KafkaProtocolError::UnexpectedEof);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    let s = String::from_utf8(bytes).map_err(|e| KafkaProtocolError::InvalidUtf8(e.to_string()))?;
    Ok(Some(s))
}

pub fn write_string(s: &str, buf: &mut impl BufMut) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

pub fn write_nullable_string(s: Option<&str>, buf: &mut impl BufMut) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => {
            buf.put_i16(-1);
        }
    }
}

pub fn read_compact_string(buf: &mut impl Buf) -> Result<String, KafkaProtocolError> {
    read_nullable_compact_string(buf)?.ok_or(KafkaProtocolError::UnexpectedNullString)
}

pub fn read_nullable_compact_string(buf: &mut impl Buf) -> Result<Option<String>, KafkaProtocolError> {
    let len_plus_one = read_unsigned_varint(buf)?;
    if len_plus_one == 0 {
        return Ok(None);
    }
    let len = (len_plus_one - 1) as usize;
    if buf.remaining() < len {
        return Err(KafkaProtocolError::UnexpectedEof);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    let s = String::from_utf8(bytes).map_err(|e| KafkaProtocolError::InvalidUtf8(e.to_string()))?;
    Ok(Some(s))
}

pub fn write_compact_string(s: &str, buf: &mut impl BufMut) {
    write_unsigned_varint((s.len() + 1) as u32, buf);
    buf.put_slice(s.as_bytes());
}

pub fn write_nullable_compact_string(s: Option<&str>, buf: &mut impl BufMut) {
    match s {
        Some(s) => {
            write_unsigned_varint((s.len() + 1) as u32, buf);
            buf.put_slice(s.as_bytes());
        }
        None => {
            write_unsigned_varint(0, buf);
        }
    }
}

// ---------------------------------------------------------------------------
// Frame Level Helpers
// ---------------------------------------------------------------------------

/// Decode a length-prefixed frame from a buffer if a full frame is available.
/// Returns Ok(None) if fewer bytes than the required frame size are present.
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<Bytes>, KafkaProtocolError> {
    if src.len() < 4 {
        return Ok(None);
    }
    let frame_len = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    if frame_len < 0 {
        return Err(KafkaProtocolError::InvalidFrameSize(frame_len));
    }
    let frame_len = frame_len as usize;
    if frame_len > 64 * 1024 * 1024 {
        return Err(KafkaProtocolError::FrameTooLarge(frame_len));
    }
    if src.len() < 4 + frame_len {
        return Ok(None);
    }
    src.advance(4);
    let frame_bytes = src.split_to(frame_len).freeze();
    Ok(Some(frame_bytes))
}

/// Encode a frame with a 4-byte big-endian size prefix.
pub fn encode_frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_i32(payload.len() as i32);
    buf.put_slice(payload);
    buf.freeze()
}

/// Wraps correlation_id and payload into a framed Kafka response packet.
pub fn encode_response_envelope(correlation_id: i32, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(4 + 4 + body.len());
    buf.put_i32((4 + body.len()) as i32);
    buf.put_i32(correlation_id);
    buf.put_slice(body);
    buf.freeze()
}

// ---------------------------------------------------------------------------
// 1. Kafka Request Header & Response Header
// ---------------------------------------------------------------------------

/// Standard Kafka Request Header (ApiKey, ApiVersion, CorrelationId, ClientId).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: Option<String>) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 8 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id = if buf.has_remaining() {
            read_nullable_string(buf)?
        } else {
            None
        };
        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i16(self.api_key);
        buf.put_i16(self.api_version);
        buf.put_i32(self.correlation_id);
        write_nullable_string(self.client_id.as_deref(), buf);
    }
}

/// Standard Kafka Response Header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 4 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        Ok(Self {
            correlation_id: buf.get_i32(),
        })
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.correlation_id);
    }
}

// ---------------------------------------------------------------------------
// 2. ApiVersions request / response (ApiKey 18)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionKey {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: Option<String>,
    pub client_software_version: Option<String>,
}

impl ApiVersionsRequest {
    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if version >= 3 && buf.has_remaining() {
            let name = read_nullable_compact_string(buf)?;
            let ver = if buf.has_remaining() {
                read_nullable_compact_string(buf)?
            } else {
                None
            };
            if buf.has_remaining() {
                let _ = skip_tagged_fields(buf);
            }
            Ok(Self {
                client_software_name: name,
                client_software_version: ver,
            })
        } else {
            Ok(Self::default())
        }
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            write_nullable_compact_string(self.client_software_name.as_deref(), buf);
            write_nullable_compact_string(self.client_software_version.as_deref(), buf);
            write_unsigned_varint(0, buf); // empty tagged fields
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersionKey>,
    pub throttle_time_ms: i32,
}

impl ApiVersionsResponse {
    pub fn new(error_code: i16, api_keys: Vec<ApiVersionKey>, throttle_time_ms: i32) -> Self {
        Self {
            error_code,
            api_keys,
            throttle_time_ms,
        }
    }

    /// Report supported versions for AeroMQ:
    /// ApiKey 0 (Produce v0-v8), ApiKey 1 (Fetch v0-v11),
    /// ApiKey 3 (Metadata v0-v9), ApiKey 18 (ApiVersions v0-v3).
    pub fn default_supported() -> Self {
        Self {
            error_code: 0,
            api_keys: vec![
                ApiVersionKey {
                    api_key: ApiKey::Produce.as_i16(), // 0
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersionKey {
                    api_key: ApiKey::Fetch.as_i16(), // 1
                    min_version: 0,
                    max_version: 11,
                },
                ApiVersionKey {
                    api_key: ApiKey::Metadata.as_i16(), // 3
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionKey {
                    api_key: ApiKey::ApiVersions.as_i16(), // 18
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersionKey {
                    api_key: ApiKey::InitProducerId.as_i16(), // 22
                    min_version: 0,
                    max_version: 4,
                },
            ],
            throttle_time_ms: 0,
        }
    }

    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 2 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let error_code = buf.get_i16();
        let mut api_keys = Vec::new();

        if version >= 3 {
            let count_plus_one = read_unsigned_varint(buf)?;
            let count = if count_plus_one > 0 { count_plus_one - 1 } else { 0 };
            for _ in 0..count {
                if buf.remaining() < 6 {
                    return Err(KafkaProtocolError::UnexpectedEof);
                }
                let api_key = buf.get_i16();
                let min_version = buf.get_i16();
                let max_version = buf.get_i16();
                skip_tagged_fields(buf)?;
                api_keys.push(ApiVersionKey {
                    api_key,
                    min_version,
                    max_version,
                });
            }
            let throttle_time_ms = if buf.remaining() >= 4 {
                buf.get_i32()
            } else {
                0
            };
            if buf.has_remaining() {
                let _ = skip_tagged_fields(buf);
            }
            Ok(Self {
                error_code,
                api_keys,
                throttle_time_ms,
            })
        } else {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let count = buf.get_i32();
            if count > 0 {
                for _ in 0..count {
                    if buf.remaining() < 6 {
                        return Err(KafkaProtocolError::UnexpectedEof);
                    }
                    let api_key = buf.get_i16();
                    let min_version = buf.get_i16();
                    let max_version = buf.get_i16();
                    api_keys.push(ApiVersionKey {
                        api_key,
                        min_version,
                        max_version,
                    });
                }
            }
            let throttle_time_ms = if version >= 1 && buf.remaining() >= 4 {
                buf.get_i32()
            } else {
                0
            };
            Ok(Self {
                error_code,
                api_keys,
                throttle_time_ms,
            })
        }
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i16(self.error_code);
        if version >= 3 {
            write_unsigned_varint((self.api_keys.len() + 1) as u32, buf);
            for k in &self.api_keys {
                buf.put_i16(k.api_key);
                buf.put_i16(k.min_version);
                buf.put_i16(k.max_version);
                write_unsigned_varint(0, buf); // empty tagged fields per element
            }
            buf.put_i32(self.throttle_time_ms);
            write_unsigned_varint(0, buf); // empty tagged fields for response
        } else {
            buf.put_i32(self.api_keys.len() as i32);
            for k in &self.api_keys {
                buf.put_i16(k.api_key);
                buf.put_i16(k.min_version);
                buf.put_i16(k.max_version);
            }
            if version >= 1 {
                buf.put_i32(self.throttle_time_ms);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Metadata request / response (ApiKey 3)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequest {
    /// None means "all topics" (null in Kafka protocol).
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}

impl MetadataRequest {
    pub fn new(topics: Option<Vec<String>>, allow_auto_topic_creation: bool) -> Self {
        Self {
            topics,
            allow_auto_topic_creation,
        }
    }

    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        let (topics, allow_auto_topic_creation) = if version >= 9 {
            // Flexible version
            let count_plus_one = read_unsigned_varint(buf)?;
            let topics = if count_plus_one == 0 {
                None
            } else {
                let count = count_plus_one - 1;
                let mut v = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let s = read_compact_string(buf)?;
                    skip_tagged_fields(buf)?;
                    v.push(s);
                }
                Some(v)
            };
            let allow_auto = if buf.has_remaining() {
                buf.get_u8() != 0
            } else {
                false
            };
            if buf.has_remaining() {
                let _ = skip_tagged_fields(buf);
            }
            (topics, allow_auto)
        } else {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let count = buf.get_i32();
            let topics = if count < 0 {
                None
            } else {
                let mut v = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    v.push(read_string(buf)?);
                }
                Some(v)
            };
            let allow_auto = if version >= 4 && buf.has_remaining() {
                buf.get_u8() != 0
            } else {
                false
            };
            (topics, allow_auto)
        };

        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 9 {
            match &self.topics {
                Some(topics) => {
                    write_unsigned_varint((topics.len() + 1) as u32, buf);
                    for t in topics {
                        write_compact_string(t, buf);
                        write_unsigned_varint(0, buf); // tagged fields
                    }
                }
                None => {
                    write_unsigned_varint(0, buf);
                }
            }
            buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
            write_unsigned_varint(0, buf); // tagged fields
        } else {
            match &self.topics {
                Some(topics) => {
                    buf.put_i32(topics.len() as i32);
                    for t in topics {
                        write_string(t, buf);
                    }
                }
                None => {
                    buf.put_i32(-1);
                }
            }
            if version >= 4 {
                buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_id: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<BrokerMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn new(
        throttle_time_ms: i32,
        brokers: Vec<BrokerMetadata>,
        cluster_id: Option<String>,
        controller_id: i32,
        topics: Vec<TopicMetadata>,
    ) -> Self {
        Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        }
    }

    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        let throttle_time_ms = if version >= 3 {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            buf.get_i32()
        } else {
            0
        };

        let mut brokers = Vec::new();
        if version >= 9 {
            let count_plus_one = read_unsigned_varint(buf)?;
            let count = if count_plus_one > 0 { count_plus_one - 1 } else { 0 };
            for _ in 0..count {
                if buf.remaining() < 8 {
                    return Err(KafkaProtocolError::UnexpectedEof);
                }
                let node_id = buf.get_i32();
                let host = read_compact_string(buf)?;
                let port = buf.get_i32();
                let rack = read_nullable_compact_string(buf)?;
                skip_tagged_fields(buf)?;
                brokers.push(BrokerMetadata {
                    node_id,
                    host,
                    port,
                    rack,
                });
            }
        } else {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let count = buf.get_i32();
            if count > 0 {
                for _ in 0..count {
                    if buf.remaining() < 8 {
                        return Err(KafkaProtocolError::UnexpectedEof);
                    }
                    let node_id = buf.get_i32();
                    let host = read_string(buf)?;
                    let port = buf.get_i32();
                    let rack = if version >= 1 {
                        read_nullable_string(buf)?
                    } else {
                        None
                    };
                    brokers.push(BrokerMetadata {
                        node_id,
                        host,
                        port,
                        rack,
                    });
                }
            }
        }

        let cluster_id = if version >= 9 {
            let s = read_nullable_compact_string(buf)?;
            s
        } else if version >= 2 {
            read_nullable_string(buf)?
        } else {
            None
        };

        let controller_id = if version >= 1 {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            buf.get_i32()
        } else {
            -1
        };

        let mut topics = Vec::new();
        if version >= 9 {
            let count_plus_one = read_unsigned_varint(buf)?;
            let count = if count_plus_one > 0 { count_plus_one - 1 } else { 0 };
            for _ in 0..count {
                let error_code = buf.get_i16();
                let topic = read_compact_string(buf)?;
                let is_internal = buf.get_u8() != 0;
                let part_count_plus_one = read_unsigned_varint(buf)?;
                let part_count = if part_count_plus_one > 0 { part_count_plus_one - 1 } else { 0 };
                let mut partitions = Vec::with_capacity(part_count as usize);
                for _ in 0..part_count {
                    let p_err = buf.get_i16();
                    let p_id = buf.get_i32();
                    let leader_id = buf.get_i32();
                    let leader_epoch = buf.get_i32();

                    let rep_plus_one = read_unsigned_varint(buf)?;
                    let rep_count = if rep_plus_one > 0 { rep_plus_one - 1 } else { 0 };
                    let mut replica_nodes = Vec::with_capacity(rep_count as usize);
                    for _ in 0..rep_count {
                        replica_nodes.push(buf.get_i32());
                    }

                    let isr_plus_one = read_unsigned_varint(buf)?;
                    let isr_count = if isr_plus_one > 0 { isr_plus_one - 1 } else { 0 };
                    let mut isr_nodes = Vec::with_capacity(isr_count as usize);
                    for _ in 0..isr_count {
                        isr_nodes.push(buf.get_i32());
                    }

                    let off_plus_one = read_unsigned_varint(buf)?;
                    let off_count = if off_plus_one > 0 { off_plus_one - 1 } else { 0 };
                    let mut offline_replicas = Vec::with_capacity(off_count as usize);
                    for _ in 0..off_count {
                        offline_replicas.push(buf.get_i32());
                    }
                    skip_tagged_fields(buf)?;
                    partitions.push(PartitionMetadata {
                        error_code: p_err,
                        partition_id: p_id,
                        leader_id,
                        leader_epoch,
                        replica_nodes,
                        isr_nodes,
                        offline_replicas,
                    });
                }
                let _topic_auth = if buf.remaining() >= 4 { buf.get_i32() } else { 0 };
                skip_tagged_fields(buf)?;
                topics.push(TopicMetadata {
                    error_code,
                    topic,
                    is_internal,
                    partitions,
                });
            }
            if buf.has_remaining() {
                let _ = skip_tagged_fields(buf);
            }
        } else {
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let count = buf.get_i32();
            if count > 0 {
                for _ in 0..count {
                    let error_code = buf.get_i16();
                    let topic = read_string(buf)?;
                    let is_internal = if version >= 1 { buf.get_u8() != 0 } else { false };
                    let part_count = buf.get_i32();
                    let mut partitions = Vec::new();
                    if part_count > 0 {
                        for _ in 0..part_count {
                            let p_err = buf.get_i16();
                            let p_id = buf.get_i32();
                            let leader_id = buf.get_i32();
                            let leader_epoch = if version >= 7 { buf.get_i32() } else { 0 };

                            let rep_count = buf.get_i32();
                            let mut replica_nodes = Vec::new();
                            if rep_count > 0 {
                                for _ in 0..rep_count {
                                    replica_nodes.push(buf.get_i32());
                                }
                            }

                            let isr_count = buf.get_i32();
                            let mut isr_nodes = Vec::new();
                            if isr_count > 0 {
                                for _ in 0..isr_count {
                                    isr_nodes.push(buf.get_i32());
                                }
                            }

                            let mut offline_replicas = Vec::new();
                            if version >= 5 {
                                let off_count = buf.get_i32();
                                if off_count > 0 {
                                    for _ in 0..off_count {
                                        offline_replicas.push(buf.get_i32());
                                    }
                                }
                            }

                            partitions.push(PartitionMetadata {
                                error_code: p_err,
                                partition_id: p_id,
                                leader_id,
                                leader_epoch,
                                replica_nodes,
                                isr_nodes,
                                offline_replicas,
                            });
                        }
                    }
                    if version >= 8 && buf.remaining() >= 4 {
                        let _auth = buf.get_i32();
                    }
                    topics.push(TopicMetadata {
                        error_code,
                        topic,
                        is_internal,
                        partitions,
                    });
                }
            }
        }

        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 3 {
            buf.put_i32(self.throttle_time_ms);
        }

        if version >= 9 {
            write_unsigned_varint((self.brokers.len() + 1) as u32, buf);
            for b in &self.brokers {
                buf.put_i32(b.node_id);
                write_compact_string(&b.host, buf);
                buf.put_i32(b.port);
                write_nullable_compact_string(b.rack.as_deref(), buf);
                write_unsigned_varint(0, buf); // tagged fields
            }
            write_nullable_compact_string(self.cluster_id.as_deref(), buf);
            buf.put_i32(self.controller_id);

            write_unsigned_varint((self.topics.len() + 1) as u32, buf);
            for t in &self.topics {
                buf.put_i16(t.error_code);
                write_compact_string(&t.topic, buf);
                buf.put_u8(if t.is_internal { 1 } else { 0 });

                write_unsigned_varint((t.partitions.len() + 1) as u32, buf);
                for p in &t.partitions {
                    buf.put_i16(p.error_code);
                    buf.put_i32(p.partition_id);
                    buf.put_i32(p.leader_id);
                    buf.put_i32(p.leader_epoch);

                    write_unsigned_varint((p.replica_nodes.len() + 1) as u32, buf);
                    for r in &p.replica_nodes {
                        buf.put_i32(*r);
                    }

                    write_unsigned_varint((p.isr_nodes.len() + 1) as u32, buf);
                    for i in &p.isr_nodes {
                        buf.put_i32(*i);
                    }

                    write_unsigned_varint((p.offline_replicas.len() + 1) as u32, buf);
                    for o in &p.offline_replicas {
                        buf.put_i32(*o);
                    }
                    write_unsigned_varint(0, buf); // partition tagged fields
                }
                buf.put_i32(0); // topic_authorized_operations
                write_unsigned_varint(0, buf); // topic tagged fields
            }
            write_unsigned_varint(0, buf); // response tagged fields
        } else {
            buf.put_i32(self.brokers.len() as i32);
            for b in &self.brokers {
                buf.put_i32(b.node_id);
                write_string(&b.host, buf);
                buf.put_i32(b.port);
                if version >= 1 {
                    write_nullable_string(b.rack.as_deref(), buf);
                }
            }

            if version >= 2 {
                write_nullable_string(self.cluster_id.as_deref(), buf);
            }
            if version >= 1 {
                buf.put_i32(self.controller_id);
            }

            buf.put_i32(self.topics.len() as i32);
            for t in &self.topics {
                buf.put_i16(t.error_code);
                write_string(&t.topic, buf);
                if version >= 1 {
                    buf.put_u8(if t.is_internal { 1 } else { 0 });
                }

                buf.put_i32(t.partitions.len() as i32);
                for p in &t.partitions {
                    buf.put_i16(p.error_code);
                    buf.put_i32(p.partition_id);
                    buf.put_i32(p.leader_id);
                    if version >= 7 {
                        buf.put_i32(p.leader_epoch);
                    }

                    buf.put_i32(p.replica_nodes.len() as i32);
                    for r in &p.replica_nodes {
                        buf.put_i32(*r);
                    }

                    buf.put_i32(p.isr_nodes.len() as i32);
                    for i in &p.isr_nodes {
                        buf.put_i32(*i);
                    }

                    if version >= 5 {
                        buf.put_i32(p.offline_replicas.len() as i32);
                        for o in &p.offline_replicas {
                            buf.put_i32(*o);
                        }
                    }
                }
                if version >= 8 {
                    buf.put_i32(0); // topic_authorized_operations
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 4. InitProducerId request / response (ApiKey 22, Versions 0 to 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdRequest {
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    pub fn new(
        transactional_id: Option<String>,
        transaction_timeout_ms: i32,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
        }
    }

    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if version >= 2 {
            let transactional_id = read_nullable_compact_string(buf)?;
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let transaction_timeout_ms = buf.get_i32();
            let (producer_id, producer_epoch) = if version >= 3 {
                if buf.remaining() < 10 {
                    return Err(KafkaProtocolError::UnexpectedEof);
                }
                (buf.get_i64(), buf.get_i16())
            } else {
                (-1, -1)
            };
            if buf.has_remaining() {
                skip_tagged_fields(buf)?;
            }
            Ok(Self {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            })
        } else {
            let transactional_id = read_nullable_string(buf)?;
            if buf.remaining() < 4 {
                return Err(KafkaProtocolError::UnexpectedEof);
            }
            let transaction_timeout_ms = buf.get_i32();
            let (producer_id, producer_epoch) = if buf.remaining() >= 10 {
                (buf.get_i64(), buf.get_i16())
            } else {
                (-1, -1)
            };
            Ok(Self {
                transactional_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            })
        }
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        if version >= 2 {
            write_nullable_compact_string(self.transactional_id.as_deref(), buf);
            buf.put_i32(self.transaction_timeout_ms);
            if version >= 3 {
                buf.put_i64(self.producer_id);
                buf.put_i16(self.producer_epoch);
            }
            write_unsigned_varint(0, buf); // empty tagged fields
        } else {
            write_nullable_string(self.transactional_id.as_deref(), buf);
            buf.put_i32(self.transaction_timeout_ms);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponse {
    pub fn new(
        throttle_time_ms: i32,
        error_code: i16,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        }
    }

    pub fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, KafkaProtocolError> {
        if buf.remaining() < 16 {
            return Err(KafkaProtocolError::UnexpectedEof);
        }
        let throttle_time_ms = buf.get_i32();
        let error_code = buf.get_i16();
        let producer_id = buf.get_i64();
        let producer_epoch = buf.get_i16();
        if version >= 2 && buf.has_remaining() {
            skip_tagged_fields(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }

    pub fn encode(&self, version: i16, buf: &mut impl BufMut) {
        buf.put_i32(self.throttle_time_ms);
        buf.put_i16(self.error_code);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        if version >= 2 {
            write_unsigned_varint(0, buf); // empty tagged fields
        }
    }
}

// ---------------------------------------------------------------------------
// High-Level Envelope & Decoders / Encoders
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaRequestBody {
    ApiVersions(ApiVersionsRequest),
    Metadata(MetadataRequest),
    InitProducerId(InitProducerIdRequest),
    Unknown { api_key: i16, payload: Bytes },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub body: KafkaRequestBody,
}

impl KafkaRequest {
    pub fn decode(mut buf: Bytes) -> Result<Self, KafkaProtocolError> {
        let header = RequestHeader::decode(&mut buf)?;
        let body = match header.api_key {
            18 => {
                let req = ApiVersionsRequest::decode(&mut buf, header.api_version)?;
                KafkaRequestBody::ApiVersions(req)
            }
            3 => {
                let req = MetadataRequest::decode(&mut buf, header.api_version)?;
                KafkaRequestBody::Metadata(req)
            }
            22 => {
                let req = InitProducerIdRequest::decode(&mut buf, header.api_version)?;
                KafkaRequestBody::InitProducerId(req)
            }
            other => KafkaRequestBody::Unknown {
                api_key: other,
                payload: buf,
            },
        };
        Ok(Self { header, body })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaResponseBody {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
    InitProducerId(InitProducerIdResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub body: KafkaResponseBody,
}

impl KafkaResponse {
    pub fn new(correlation_id: i32, body: KafkaResponseBody) -> Self {
        Self {
            header: ResponseHeader::new(correlation_id),
            body,
        }
    }

    pub fn encode(&self, version: i16) -> Bytes {
        let mut body_buf = BytesMut::new();
        match &self.body {
            KafkaResponseBody::ApiVersions(resp) => {
                resp.encode(version, &mut body_buf);
            }
            KafkaResponseBody::Metadata(resp) => {
                resp.encode(version, &mut body_buf);
            }
            KafkaResponseBody::InitProducerId(resp) => {
                resp.encode(version, &mut body_buf);
            }
        }
        encode_response_envelope(self.header.correlation_id, &body_buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header_codec() {
        let header = RequestHeader {
            api_key: 18,
            api_version: 3,
            correlation_id: 12345,
            client_id: Some("client-test-id".to_string()),
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = RequestHeader::decode(&mut read_buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_request_header_null_client_id() {
        let header = RequestHeader {
            api_key: 3,
            api_version: 1,
            correlation_id: 999,
            client_id: None,
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let mut read_buf = buf.freeze();
        let decoded = RequestHeader::decode(&mut read_buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_api_versions_response_default_supported() {
        let resp = ApiVersionsResponse::default_supported();
        assert_eq!(resp.error_code, 0);

        // Verify the 4 mandatory supported API keys
        let produce = resp.api_keys.iter().find(|k| k.api_key == 0).unwrap();
        assert_eq!(produce.min_version, 0);
        assert_eq!(produce.max_version, 8);

        let fetch = resp.api_keys.iter().find(|k| k.api_key == 1).unwrap();
        assert_eq!(fetch.min_version, 0);
        assert_eq!(fetch.max_version, 11);

        let metadata = resp.api_keys.iter().find(|k| k.api_key == 3).unwrap();
        assert_eq!(metadata.min_version, 0);
        assert_eq!(metadata.max_version, 9);

        let api_versions = resp.api_keys.iter().find(|k| k.api_key == 18).unwrap();
        assert_eq!(api_versions.min_version, 0);
        assert_eq!(api_versions.max_version, 3);
    }

    #[test]
    fn test_api_versions_roundtrip_v0_to_v3() {
        let resp = ApiVersionsResponse::default_supported();
        for version in 0..=3 {
            let mut buf = BytesMut::new();
            resp.encode(version, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = ApiVersionsResponse::decode(&mut read_buf, version).unwrap();
            assert_eq!(decoded.error_code, resp.error_code);
            assert_eq!(decoded.api_keys, resp.api_keys);
        }
    }

    #[test]
    fn test_metadata_request_and_response_roundtrip() {
        let req = MetadataRequest {
            topics: Some(vec!["events".to_string(), "logs".to_string()]),
            allow_auto_topic_creation: true,
        };

        for v in [0, 1, 4, 8, 9] {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = MetadataRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.topics, req.topics);
            if v >= 4 {
                assert_eq!(decoded.allow_auto_topic_creation, req.allow_auto_topic_creation);
            }
        }

        let resp = MetadataResponse {
            throttle_time_ms: 10,
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "127.0.0.1".to_string(),
                port: 9092,
                rack: Some("rack-a".to_string()),
            }],
            cluster_id: Some("aeromq-cluster-1".to_string()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                topic: "events".to_string(),
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_id: 0,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    offline_replicas: vec![],
                }],
            }],
        };

        for v in [0, 1, 2, 3, 5, 7, 8, 9] {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = MetadataResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.brokers.len(), 1);
            assert_eq!(decoded.brokers[0].node_id, 1);
            assert_eq!(decoded.brokers[0].host, "127.0.0.1");
            assert_eq!(decoded.brokers[0].port, 9092);

            assert_eq!(decoded.topics.len(), 1);
            assert_eq!(decoded.topics[0].topic, "events");
            assert_eq!(decoded.topics[0].partitions.len(), 1);
            assert_eq!(decoded.topics[0].partitions[0].partition_id, 0);
            assert_eq!(decoded.topics[0].partitions[0].leader_id, 1);
        }
    }

    #[test]
    fn test_framing_codec() {
        let mut buf = BytesMut::new();
        let payload = b"kafka-wire-frame-test";
        let framed = encode_frame(payload);
        buf.extend_from_slice(&framed);

        let decoded = decode_frame(&mut buf).unwrap().expect("frame should decode");
        assert_eq!(&decoded[..], payload);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_init_producer_id_roundtrip() {
        let req = InitProducerIdRequest {
            transactional_id: Some("tx-producer-1".to_string()),
            transaction_timeout_ms: 30000,
            producer_id: 1005,
            producer_epoch: 2,
        };

        for v in 0..=4 {
            let mut buf = BytesMut::new();
            req.encode(v, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = InitProducerIdRequest::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.transactional_id, req.transactional_id);
            assert_eq!(decoded.transaction_timeout_ms, req.transaction_timeout_ms);
            if v >= 3 {
                assert_eq!(decoded.producer_id, req.producer_id);
                assert_eq!(decoded.producer_epoch, req.producer_epoch);
            }
        }

        let resp = InitProducerIdResponse {
            throttle_time_ms: 5,
            error_code: 0,
            producer_id: 1000,
            producer_epoch: 0,
        };

        for v in 0..=4 {
            let mut buf = BytesMut::new();
            resp.encode(v, &mut buf);
            let mut read_buf = buf.freeze();
            let decoded = InitProducerIdResponse::decode(&mut read_buf, v).unwrap();
            assert_eq!(decoded.throttle_time_ms, resp.throttle_time_ms);
            assert_eq!(decoded.error_code, resp.error_code);
            assert_eq!(decoded.producer_id, resp.producer_id);
            assert_eq!(decoded.producer_epoch, resp.producer_epoch);
        }
    }
}
