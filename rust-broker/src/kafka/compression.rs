//! Record-batch compression codecs (Kafka magic v2 attribute bits 0-2, and the
//! legacy magic 0/1 wrapper-message attribute bits 0-2).
//!
//! Codec ids (KIP-110 for zstd): 0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd.
//! Wire formats match the Java client:
//!   * gzip   - RFC 1952 gzip stream
//!   * snappy - xerial snappy-java framing (raw snappy is also accepted on read)
//!   * lz4    - LZ4 frame format
//!   * zstd   - standard zstd frame
//!
//! Batch-level compression covers only the `records` section (everything after the
//! 61-byte header), so a batch can be re-compressed by rewriting the attributes,
//! batch length and CRC32C without touching any per-record bytes.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{OnceLock, RwLock};

use crate::kafka::handlers::crc32c;

/// Upper bound for a decompressed records section (guards against decompression bombs).
pub const MAX_DECOMPRESSED_BYTES: u64 = 256 * 1024 * 1024;

pub const ERR_CORRUPT_MESSAGE: i16 = 2;
pub const ERR_UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;

const BATCH_HEADER_LEN: usize = 61;
const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];
const XERIAL_BLOCK: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionError {
    UnsupportedCodec(i16),
    Corrupt(String),
}

impl CompressionError {
    /// Kafka error code to return to the producer.
    pub fn error_code(&self) -> i16 {
        match self {
            CompressionError::UnsupportedCodec(_) => ERR_UNSUPPORTED_COMPRESSION_TYPE,
            CompressionError::Corrupt(_) => ERR_CORRUPT_MESSAGE,
        }
    }
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::UnsupportedCodec(c) => write!(f, "unsupported compression codec id {}", c),
            CompressionError::Corrupt(m) => write!(f, "corrupt compressed payload: {}", m),
        }
    }
}

impl std::error::Error for CompressionError {}

impl Codec {
    pub fn id(self) -> i16 {
        match self {
            Codec::None => 0,
            Codec::Gzip => 1,
            Codec::Snappy => 2,
            Codec::Lz4 => 3,
            Codec::Zstd => 4,
        }
    }

    pub fn from_id(id: i16) -> Result<Codec, CompressionError> {
        match id {
            0 => Ok(Codec::None),
            1 => Ok(Codec::Gzip),
            2 => Ok(Codec::Snappy),
            3 => Ok(Codec::Lz4),
            4 => Ok(Codec::Zstd),
            other => Err(CompressionError::UnsupportedCodec(other)),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Codec::None => "uncompressed",
            Codec::Gzip => "gzip",
            Codec::Snappy => "snappy",
            Codec::Lz4 => "lz4",
            Codec::Zstd => "zstd",
        }
    }
}

/// Topic/broker `compression.type` setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// Keep whatever codec the producer used (Kafka default).
    Producer,
    /// Re-encode every batch with this codec on the broker.
    Codec(Codec),
}

impl CompressionType {
    /// Parses a Kafka `compression.type` value ("producer", "uncompressed", "none", "gzip", ...).
    pub fn parse(s: &str) -> Option<CompressionType> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "producer" => Some(CompressionType::Producer),
            "uncompressed" | "none" => Some(CompressionType::Codec(Codec::None)),
            "gzip" => Some(CompressionType::Codec(Codec::Gzip)),
            "snappy" => Some(CompressionType::Codec(Codec::Snappy)),
            "lz4" => Some(CompressionType::Codec(Codec::Lz4)),
            "zstd" => Some(CompressionType::Codec(Codec::Zstd)),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            CompressionType::Producer => "producer",
            CompressionType::Codec(c) => c.name(),
        }
    }
}

fn corrupt<E: std::fmt::Display>(e: E) -> CompressionError {
    CompressionError::Corrupt(e.to_string())
}

fn read_limited<R: Read>(r: R) -> Result<Vec<u8>, CompressionError> {
    let mut out = Vec::new();
    r.take(MAX_DECOMPRESSED_BYTES + 1).read_to_end(&mut out).map_err(corrupt)?;
    if out.len() as u64 > MAX_DECOMPRESSED_BYTES {
        return Err(CompressionError::Corrupt("decompressed size exceeds limit".into()));
    }
    Ok(out)
}

pub fn compress(codec: Codec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        Codec::None => Ok(data.to_vec()),
        Codec::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).map_err(corrupt)?;
            enc.finish().map_err(corrupt)
        }
        Codec::Snappy => {
            let mut out = Vec::with_capacity(data.len() / 2 + 32);
            out.extend_from_slice(&XERIAL_MAGIC);
            out.extend_from_slice(&1i32.to_be_bytes()); // version
            out.extend_from_slice(&1i32.to_be_bytes()); // min compatible version
            let mut enc = snap::raw::Encoder::new();
            for chunk in data.chunks(XERIAL_BLOCK) {
                let c = enc.compress_vec(chunk).map_err(corrupt)?;
                out.extend_from_slice(&(c.len() as i32).to_be_bytes());
                out.extend_from_slice(&c);
            }
            Ok(out)
        }
        Codec::Lz4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(data).map_err(corrupt)?;
            enc.finish().map_err(corrupt)
        }
        Codec::Zstd => zstd::stream::encode_all(data, 3).map_err(corrupt),
    }
}

pub fn decompress(codec: Codec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        Codec::None => Ok(data.to_vec()),
        Codec::Gzip => read_limited(flate2::read::GzDecoder::new(data)),
        Codec::Snappy => {
            if data.len() >= 16 && data[..8] == XERIAL_MAGIC {
                let mut pos = 16;
                let mut out = Vec::new();
                let mut dec = snap::raw::Decoder::new();
                while pos < data.len() {
                    if pos + 4 > data.len() {
                        return Err(CompressionError::Corrupt("truncated xerial block header".into()));
                    }
                    let len = i32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    if len < 0 || pos + len as usize > data.len() {
                        return Err(CompressionError::Corrupt("bad xerial block length".into()));
                    }
                    let block = dec.decompress_vec(&data[pos..pos + len as usize]).map_err(corrupt)?;
                    pos += len as usize;
                    if (out.len() + block.len()) as u64 > MAX_DECOMPRESSED_BYTES {
                        return Err(CompressionError::Corrupt("decompressed size exceeds limit".into()));
                    }
                    out.extend_from_slice(&block);
                }
                Ok(out)
            } else {
                // Raw snappy block (e.g. librdkafka). Check the declared length first.
                let declared = snap::raw::decompress_len(data).map_err(corrupt)?;
                if declared as u64 > MAX_DECOMPRESSED_BYTES {
                    return Err(CompressionError::Corrupt("decompressed size exceeds limit".into()));
                }
                snap::raw::Decoder::new().decompress_vec(data).map_err(corrupt)
            }
        }
        Codec::Lz4 => read_limited(lz4_flex::frame::FrameDecoder::new(data)),
        Codec::Zstd => {
            let dec = zstd::stream::read::Decoder::new(data).map_err(corrupt)?;
            read_limited(dec)
        }
    }
}

// ---------------------------------------------------------------------------
// Record batch (magic 2) helpers
// ---------------------------------------------------------------------------

/// Codec of a magic-2 batch (`batch` must start at base_offset and hold >= 23 bytes).
pub fn batch_codec(batch: &[u8]) -> Result<Codec, CompressionError> {
    if batch.len() < 23 {
        return Err(CompressionError::Corrupt("batch too short".into()));
    }
    let attrs = i16::from_be_bytes([batch[21], batch[22]]);
    Codec::from_id(attrs & 0x07)
}

/// Returns the (decompressed) records section of a single magic-2 batch.
pub fn decompress_batch_records(batch: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if batch.len() < BATCH_HEADER_LEN {
        return Err(CompressionError::Corrupt("batch too short".into()));
    }
    let codec = batch_codec(batch)?;
    decompress(codec, &batch[BATCH_HEADER_LEN..])
}

/// Rewrites a single magic-2 batch with a different codec. Header fields (offsets,
/// producer id/epoch/sequence, timestamps, count) are preserved; only attributes,
/// length and CRC32C change. Returns the batch unchanged when the codec already matches.
pub fn recompress_batch(batch: &[u8], target: Codec) -> Result<Vec<u8>, CompressionError> {
    let current = batch_codec(batch)?;
    if current == target {
        return Ok(batch.to_vec());
    }
    let raw = decompress(current, &batch[BATCH_HEADER_LEN..])?;
    let body = compress(target, &raw)?;
    let mut out = Vec::with_capacity(BATCH_HEADER_LEN + body.len());
    out.extend_from_slice(&batch[..BATCH_HEADER_LEN]);
    let attrs = i16::from_be_bytes([batch[21], batch[22]]);
    let new_attrs = (attrs & !0x07) | target.id();
    out[21..23].copy_from_slice(&new_attrs.to_be_bytes());
    out[8..12].copy_from_slice(&((49 + body.len()) as i32).to_be_bytes());
    out.extend_from_slice(&body);
    let crc = crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    Ok(out)
}

/// Validates every batch in a produce payload (known codec, decodable body) and, when
/// `ctype` forces a codec, re-encodes batches to it. Legacy magic 0/1 data and any
/// trailing partial batch are passed through untouched. Control batches are never
/// re-compressed.
pub fn normalize_produce_payload<'a>(data: &'a [u8], ctype: CompressionType) -> Result<std::borrow::Cow<'a, [u8]>, CompressionError> {
    let needs_recompression = match ctype {
        CompressionType::Codec(target) => {
            let mut pos = 0usize;
            let mut needs = false;
            while pos < data.len() {
                let rest = &data[pos..];
                if rest.len() < 17 || rest[16] != 2 {
                    break;
                }
                let batch_len = i32::from_be_bytes(rest[8..12].try_into().unwrap_or([0; 4]));
                let total = 12usize.saturating_add(batch_len.max(0) as usize);
                if batch_len < 49 || total > rest.len() {
                    break;
                }
                let batch = &rest[..total];
                let codec = batch_codec(batch)?;
                let attrs = i16::from_be_bytes([batch[21], batch[22]]);
                let is_control = attrs & 0x20 != 0;
                if !is_control && target != codec {
                    needs = true;
                    break;
                }
                if codec != Codec::None {
                    decompress_batch_records(batch)?;
                }
                pos += total;
            }
            needs
        }
        _ => {
            let mut pos = 0usize;
            while pos < data.len() {
                let rest = &data[pos..];
                if rest.len() < 17 || rest[16] != 2 {
                    break;
                }
                let batch_len = i32::from_be_bytes(rest[8..12].try_into().unwrap_or([0; 4]));
                let total = 12usize.saturating_add(batch_len.max(0) as usize);
                if batch_len < 49 || total > rest.len() {
                    break;
                }
                let batch = &rest[..total];
                let codec = batch_codec(batch)?;
                if codec != Codec::None {
                    decompress_batch_records(batch)?;
                }
                pos += total;
            }
            false
        }
    };

    if !needs_recompression {
        return Ok(std::borrow::Cow::Borrowed(data));
    }

    let mut out = Vec::with_capacity(data.len());
    let mut pos = 0usize;
    while pos < data.len() {
        let rest = &data[pos..];
        if rest.len() < 17 || rest[16] != 2 {
            out.extend_from_slice(rest);
            break;
        }
        let batch_len = i32::from_be_bytes(rest[8..12].try_into().unwrap());
        let total = 12usize.saturating_add(batch_len.max(0) as usize);
        if batch_len < 49 || total > rest.len() {
            out.extend_from_slice(rest);
            break;
        }
        let batch = &rest[..total];
        let codec = batch_codec(batch)?;
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        let is_control = attrs & 0x20 != 0;
        match ctype {
            CompressionType::Codec(target) if !is_control && target != codec => {
                out.extend_from_slice(&recompress_batch(batch, target)?);
            }
            _ => {
                out.extend_from_slice(batch);
            }
        }
        pos += total;
    }
    Ok(std::borrow::Cow::Owned(out))
}

// ---------------------------------------------------------------------------
// Topic compression.type registry (populated from broker config / controller)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct CompressionRegistry {
    default: RwLock<Option<CompressionType>>,
    topics: RwLock<HashMap<String, CompressionType>>,
}

impl CompressionRegistry {
    pub fn set_default(&self, t: CompressionType) {
        *self.default.write().unwrap() = Some(t);
    }

    /// Replaces all per-topic overrides (controller pushes the full map).
    pub fn replace_topics(&self, topics: HashMap<String, CompressionType>) {
        *self.topics.write().unwrap() = topics;
    }

    pub fn set_topic(&self, topic: &str, t: CompressionType) {
        self.topics.write().unwrap().insert(topic.to_string(), t);
    }

    pub fn for_topic(&self, topic: &str) -> CompressionType {
        if let Some(t) = self.topics.read().unwrap().get(topic) {
            return *t;
        }
        self.default.read().unwrap().unwrap_or(CompressionType::Producer)
    }
}

pub fn registry() -> &'static CompressionRegistry {
    static REG: OnceLock<CompressionRegistry> = OnceLock::new();
    REG.get_or_init(CompressionRegistry::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::handlers::{encode_records_batch, parse_records, KafkaRecord};

    fn sample_records(n: usize) -> Vec<KafkaRecord> {
        (0..n)
            .map(|i| {
                let mut r = KafkaRecord::new(
                    Some(format!("key-{}", i % 3).into_bytes()),
                    Some(format!("value-{}-{}", i, "x".repeat(200)).into_bytes()),
                );
                r.timestamp = 1_700_000_000_000 + i as i64;
                r.headers.push(("h".into(), vec![1, 2, 3]));
                r
            })
            .collect()
    }

    const ALL: [Codec; 5] = [Codec::None, Codec::Gzip, Codec::Snappy, Codec::Lz4, Codec::Zstd];

    #[test]
    fn roundtrip_all_codecs() {
        let data: Vec<u8> = (0..100_000u32).flat_map(|i| (i % 251).to_be_bytes()).collect();
        for c in ALL {
            let z = compress(c, &data).unwrap();
            assert_eq!(decompress(c, &z).unwrap(), data, "codec {:?}", c);
        }
    }

    #[test]
    fn snappy_raw_block_accepted() {
        let raw = snap::raw::Encoder::new().compress_vec(b"hello hello hello hello").unwrap();
        assert_eq!(decompress(Codec::Snappy, &raw).unwrap(), b"hello hello hello hello");
    }

    #[test]
    fn corrupt_payload_rejected() {
        for c in [Codec::Gzip, Codec::Lz4, Codec::Zstd] {
            assert!(decompress(c, b"definitely not compressed").is_err());
        }
    }

    #[test]
    fn unknown_codec_id() {
        assert_eq!(Codec::from_id(7), Err(CompressionError::UnsupportedCodec(7)));
        assert_eq!(CompressionError::UnsupportedCodec(7).error_code(), 76);
    }

    #[test]
    fn recompress_preserves_records_and_header() {
        let recs = sample_records(20);
        let plain = crate::kafka::handlers::encode_idempotent_records_batch(5, 42, 3, 9, &recs);
        for c in ALL {
            let z = recompress_batch(&plain, c).unwrap();
            assert_eq!(batch_codec(&z).unwrap(), c);
            // crc valid
            assert_eq!(u32::from_be_bytes(z[17..21].try_into().unwrap()), crc32c(&z[21..]));
            // producer info preserved
            assert_eq!(
                crate::kafka::handlers::extract_batch_producer_info(&z),
                Some((42, 3, 9, 20))
            );
            let parsed = parse_records(&z).unwrap();
            assert_eq!(parsed.len(), 20);
            assert_eq!(parsed[7].value, recs[7].value);
            assert_eq!(parsed[7].offset, 5 + 7);
            assert_eq!(parsed[7].headers, recs[7].headers);
            if c != Codec::None {
                assert!(z.len() < plain.len(), "{:?} should shrink repetitive data", c);
            }
            // and back
            let back = recompress_batch(&z, Codec::None).unwrap();
            assert_eq!(back, plain);
        }
    }

    #[test]
    fn normalize_forces_topic_codec_and_validates() {
        let recs = sample_records(5);
        let plain = encode_records_batch(0, &recs);
        let mut two = plain.clone();
        two.extend_from_slice(&plain);
        let out = normalize_produce_payload(&two, CompressionType::Codec(Codec::Zstd)).unwrap();
        assert_eq!(parse_records(&out).unwrap().len(), 10);
        assert_eq!(batch_codec(&out).unwrap(), Codec::Zstd);
        // producer mode keeps as is
        let keep = normalize_produce_payload(&two, CompressionType::Producer).unwrap();
        assert_eq!(keep, two);
        // unsupported codec bits rejected
        let mut bad = plain.clone();
        bad[22] = 7;
        assert_eq!(
            normalize_produce_payload(&bad, CompressionType::Producer).unwrap_err().error_code(),
            76
        );
        // corrupt compressed body rejected
        let mut z = recompress_batch(&plain, Codec::Gzip).unwrap();
        let n = z.len();
        z[n - 10] ^= 0xFF;
        z[62] ^= 0xFF;
        assert!(normalize_produce_payload(&z, CompressionType::Producer).is_err());
    }

    #[test]
    fn parse_compression_type() {
        assert_eq!(CompressionType::parse("Producer"), Some(CompressionType::Producer));
        assert_eq!(CompressionType::parse("zstd"), Some(CompressionType::Codec(Codec::Zstd)));
        assert_eq!(CompressionType::parse("uncompressed"), Some(CompressionType::Codec(Codec::None)));
        assert_eq!(CompressionType::parse("bogus"), None);
    }
}
