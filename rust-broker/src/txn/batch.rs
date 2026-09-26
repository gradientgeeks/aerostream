//! Record batch (magic 2) helpers for transactions: attribute bits, control
//! batches (commit/abort markers), and splitting client batches into
//! single-record log entries so that one log offset == one record.

use crate::kafka::handlers::{crc32c, encode_idempotent_records_batch, parse_records, KafkaRecord};

/// Batch attribute bit: batch is part of a transaction.
pub const ATTR_TRANSACTIONAL: i16 = 0x10;
/// Batch attribute bit: batch is a control batch (transaction marker).
pub const ATTR_CONTROL: i16 = 0x20;
const ATTR_COMPRESSION_MASK: i16 = 0x07;

/// Control record types (key `type` field).
pub const CONTROL_ABORT: i16 = 0;
pub const CONTROL_COMMIT: i16 = 1;

/// Fixed record batch header size (base_offset .. records_count).
pub const BATCH_HEADER_LEN: usize = 61;

pub fn is_magic2(data: &[u8]) -> bool {
    data.len() >= BATCH_HEADER_LEN && data[16] == 2
}

pub fn attributes(data: &[u8]) -> i16 {
    i16::from_be_bytes([data[21], data[22]])
}

pub fn is_transactional(data: &[u8]) -> bool {
    is_magic2(data) && attributes(data) & ATTR_TRANSACTIONAL != 0
}

pub fn is_control(data: &[u8]) -> bool {
    is_magic2(data) && attributes(data) & ATTR_CONTROL != 0
}

pub fn is_compressed(data: &[u8]) -> bool {
    is_magic2(data) && attributes(data) & ATTR_COMPRESSION_MASK != 0
}

/// (producer_id, producer_epoch, base_sequence, record_count)
pub fn producer_info(data: &[u8]) -> Option<(i64, i16, i32, i32)> {
    if !is_magic2(data) {
        return None;
    }
    Some((
        i64::from_be_bytes(data[43..51].try_into().ok()?),
        i16::from_be_bytes(data[51..53].try_into().ok()?),
        i32::from_be_bytes(data[53..57].try_into().ok()?),
        i32::from_be_bytes(data[57..61].try_into().ok()?),
    ))
}

/// Overwrites the batch base offset (not covered by the CRC).
pub fn patch_base_offset(data: &mut [u8], offset: i64) {
    if data.len() >= 8 {
        data[0..8].copy_from_slice(&offset.to_be_bytes());
    }
}

pub fn set_attributes(batch: &mut [u8], attrs: i16) {
    batch[21..23].copy_from_slice(&attrs.to_be_bytes());
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
}

/// Encodes a control batch (transaction marker) holding a single control record.
pub fn encode_control_batch(
    base_offset: i64,
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    coordinator_epoch: i32,
    timestamp: i64,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes()); // version
    key.extend_from_slice(&(if commit { CONTROL_COMMIT } else { CONTROL_ABORT }).to_be_bytes());
    let mut val = Vec::with_capacity(6);
    val.extend_from_slice(&0i16.to_be_bytes()); // version
    val.extend_from_slice(&coordinator_epoch.to_be_bytes());
    let rec = KafkaRecord {
        key: Some(key),
        value: Some(val),
        headers: Vec::new(),
        timestamp,
        offset: 0,
    };
    // Control batches carry base_sequence = -1.
    let mut b = encode_idempotent_records_batch(base_offset, producer_id, producer_epoch, -1, &[rec]);
    set_attributes(&mut b, ATTR_TRANSACTIONAL | ATTR_CONTROL);
    b
}

/// If `entry` is a control batch, returns its control type (0 abort, 1 commit).
pub fn control_type(entry: &[u8]) -> Option<i16> {
    if !is_control(entry) {
        return None;
    }
    // First record starts at byte 61: length varint, attrs(1), ts varlong, offset varint, key len varint, key.
    let mut i = BATCH_HEADER_LEN;
    let (_len, n) = read_zigzag(&entry[i..])?;
    i += n;
    i += 1; // attributes
    let (_, n) = read_zigzag(&entry[i..])?; // ts delta
    i += n;
    let (_, n) = read_zigzag(&entry[i..])?; // offset delta
    i += n;
    let (klen, n) = read_zigzag(&entry[i..])?;
    i += n;
    if klen < 4 || entry.len() < i + 4 {
        return None;
    }
    Some(i16::from_be_bytes([entry[i + 2], entry[i + 3]]))
}

fn read_zigzag(b: &[u8]) -> Option<(i64, usize)> {
    let mut v: u64 = 0;
    for (i, byte) in b.iter().enumerate().take(10) {
        v |= ((byte & 0x7f) as u64) << (7 * i);
        if byte & 0x80 == 0 {
            let dec = ((v >> 1) as i64) ^ -((v & 1) as i64);
            return Some((dec, i + 1));
        }
    }
    None
}

/// Iterates over the concatenated record batches inside a produce payload.
/// Returns `None` if the payload is not a well-formed sequence of magic-2 batches.
pub fn split_batches(payload: &[u8]) -> Option<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut rest = payload;
    while !rest.is_empty() {
        if !is_magic2(rest) {
            return None;
        }
        let blen = i32::from_be_bytes(rest[8..12].try_into().ok()?);
        if blen < 49 {
            return None;
        }
        let total = 12 + blen as usize;
        if rest.len() < total {
            return None;
        }
        out.push(&rest[..total]);
        rest = &rest[total..];
    }
    Some(out)
}

/// Converts one client batch into log entries, one per record, each a valid
/// single-record batch whose base_offset equals its log offset.
/// Producer id / epoch / transactional flag are preserved; sequence is base+i.
/// Compressed batches are decompressed by `parse_records` and stored as uncompressed
/// single-record entries, so that every record keeps its own log offset (a compressed batch
/// stored whole would occupy one offset while its header claims `count` of them).
/// Anything unparsable is stored as one entry.
pub fn to_entries(batch: &[u8], first_offset: u64) -> Vec<Vec<u8>> {
    let (pid, epoch, base_seq, count) = match producer_info(batch) {
        Some(x) => x,
        None => return vec![batch.to_vec()],
    };
    let attrs = attributes(batch);
    if count <= 1 {
        let mut e = batch.to_vec();
        patch_base_offset(&mut e, first_offset as i64);
        return vec![e];
    }
    match parse_records(batch) {
        Ok(records) if records.len() == count as usize => records
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let seq = if base_seq >= 0 { base_seq + i as i32 } else { -1 };
                let mut e = encode_idempotent_records_batch(
                    (first_offset + i as u64) as i64,
                    pid,
                    epoch,
                    seq,
                    std::slice::from_ref(r),
                );
                let keep = attrs & (ATTR_TRANSACTIONAL | ATTR_CONTROL);
                if keep != 0 {
                    set_attributes(&mut e, keep);
                }
                e
            })
            .collect(),
        _ => {
            let mut e = batch.to_vec();
            patch_base_offset(&mut e, first_offset as i64);
            vec![e]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_batch_roundtrip() {
        let b = encode_control_batch(7, 1234, 2, true, 5, 1_700_000_000_000);
        assert!(is_control(&b));
        assert!(is_transactional(&b));
        assert_eq!(control_type(&b), Some(CONTROL_COMMIT));
        let (pid, epoch, seq, count) = producer_info(&b).unwrap();
        assert_eq!((pid, epoch, seq, count), (1234, 2, -1, 1));
        let stored = u32::from_be_bytes(b[17..21].try_into().unwrap());
        assert_eq!(stored, crc32c(&b[21..]));
        let a = encode_control_batch(8, 1234, 2, false, 5, 0);
        assert_eq!(control_type(&a), Some(CONTROL_ABORT));
        assert_eq!(i64::from_be_bytes(a[0..8].try_into().unwrap()), 8);
    }

    #[test]
    fn split_multi_record_transactional_batch() {
        let recs: Vec<KafkaRecord> = (0..3)
            .map(|i| KafkaRecord::new(None, Some(format!("v{}", i).into_bytes())))
            .collect();
        let mut batch = encode_idempotent_records_batch(0, 77, 1, 10, &recs);
        set_attributes(&mut batch, ATTR_TRANSACTIONAL);
        let entries = to_entries(&batch, 40);
        assert_eq!(entries.len(), 3);
        for (i, e) in entries.iter().enumerate() {
            assert!(is_transactional(e));
            assert!(!is_control(e));
            let (pid, epoch, seq, count) = producer_info(e).unwrap();
            assert_eq!((pid, epoch, seq, count), (77, 1, 10 + i as i32, 1));
            assert_eq!(i64::from_be_bytes(e[0..8].try_into().unwrap()), 40 + i as i64);
            assert_eq!(parse_records(e).unwrap()[0].value.as_deref(), Some(format!("v{}", i).as_bytes()));
        }
    }

    #[test]
    fn split_compressed_multi_record_batch_gives_one_offset_per_record() {
        use crate::kafka::compression::{recompress_batch, Codec};
        let recs: Vec<KafkaRecord> = (0..5)
            .map(|i| KafkaRecord::new(None, Some(format!("value-{}-{}", i, "z".repeat(100)).into_bytes()))
            )
            .collect();
        let plain = encode_idempotent_records_batch(0, 9, 0, 0, &recs);
        for codec in [Codec::Gzip, Codec::Zstd] {
            let compressed = recompress_batch(&plain, codec).unwrap();
            assert!(is_compressed(&compressed));
            let entries = to_entries(&compressed, 100);
            assert_eq!(entries.len(), 5, "{:?}", codec);
            for (i, e) in entries.iter().enumerate() {
                assert_eq!(i64::from_be_bytes(e[0..8].try_into().unwrap()), 100 + i as i64);
                let (_, _, seq, count) = producer_info(e).unwrap();
                assert_eq!((seq, count), (i as i32, 1));
                let r = parse_records(e).unwrap();
                assert!(r[0].value.as_deref().unwrap().starts_with(format!("value-{}-", i).as_bytes()));
            }
        }
    }

    #[test]
    fn split_batches_detects_concatenation() {
        let r = KafkaRecord::new(None, Some(b"x".to_vec()));
        let a = encode_idempotent_records_batch(0, 1, 0, 0, std::slice::from_ref(&r));
        let b = encode_idempotent_records_batch(0, 1, 0, 1, std::slice::from_ref(&r));
        let mut payload = a.clone();
        payload.extend_from_slice(&b);
        let parts = split_batches(&payload).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], &a[..]);
    }
}
