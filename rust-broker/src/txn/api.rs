//! Kafka wire handlers for the transaction APIs:
//! InitProducerId (22), AddPartitionsToTxn (24), AddOffsetsToTxn (25),
//! EndTxn (26), TxnOffsetCommit (28).

use std::sync::Arc;

use super::codec::{CResult, Rd, Wr};
use super::store::PendingOffset;
use super::{coordinator_for, err};
use crate::config::BrokerConfig;
use crate::log::LogManager;

/// (api_key, min_version, max_version) advertised by this module.
pub const TXN_API_VERSIONS: [(i16, i16, i16); 5] = [
    (22, 0, 4), // InitProducerId
    (24, 0, 3), // AddPartitionsToTxn (client-facing versions)
    (25, 0, 3), // AddOffsetsToTxn
    (26, 0, 4), // EndTxn (TV1)
    (28, 0, 3), // TxnOffsetCommit
];

/// Whether `(api_key, version)` uses flexible (compact + tagged) encoding.
pub fn flexible(api_key: i16, v: i16) -> bool {
    match api_key {
        22 => v >= 2,
        24 | 25 | 26 | 28 => v >= 3,
        76..=79 => true,
        _ => false,
    }
}

pub fn supports(api_key: i16, v: i16) -> bool {
    TXN_API_VERSIONS.iter().any(|(k, lo, hi)| *k == api_key && v >= *lo && v <= *hi)
}

// ------------------------------- InitProducerId -------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdReq {
    pub transactional_id: Option<String>,
    pub timeout_ms: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdReq {
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 2);
        let transactional_id = r.nstring()?;
        let timeout_ms = r.i32()?;
        let (producer_id, producer_epoch) = if v >= 3 { (r.i64()?, r.i16()?) } else { (-1, -1) };
        r.skip_tags()?;
        Ok(Self { transactional_id, timeout_ms, producer_id, producer_epoch })
    }
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 2);
        w.nstring(self.transactional_id.as_deref());
        w.i32(self.timeout_ms);
        if v >= 3 {
            w.i64(self.producer_id);
            w.i16(self.producer_epoch);
        }
        w.tags();
        w.buf.to_vec()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerIdResp {
    pub error: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResp {
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 2);
        w.i32(0);
        w.i16(self.error);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        w.tags();
        w.buf.to_vec()
    }
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 2);
        let _throttle = r.i32()?;
        let out = Self { error: r.i16()?, producer_id: r.i64()?, producer_epoch: r.i16()? };
        r.skip_tags()?;
        Ok(out)
    }
}

// ----------------------------- AddPartitionsToTxn -----------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsReq {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<(String, Vec<i32>)>,
}

impl AddPartitionsReq {
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 3);
        let transactional_id = r.string()?;
        let producer_id = r.i64()?;
        let producer_epoch = r.i16()?;
        let mut topics = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let name = r.string()?;
            let mut parts = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                parts.push(r.i32()?);
            }
            r.skip_tags()?;
            topics.push((name, parts));
        }
        r.skip_tags()?;
        Ok(Self { transactional_id, producer_id, producer_epoch, topics })
    }
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 3);
        w.string(&self.transactional_id);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        w.array_len(self.topics.len());
        for (n, ps) in &self.topics {
            w.string(n);
            w.array_len(ps.len());
            for p in ps {
                w.i32(*p);
            }
            w.tags();
        }
        w.tags();
        w.buf.to_vec()
    }
}

/// results: topic -> [(partition, error)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsResp {
    pub results: Vec<(String, Vec<(i32, i16)>)>,
}

impl AddPartitionsResp {
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 3);
        w.i32(0);
        w.array_len(self.results.len());
        for (n, ps) in &self.results {
            w.string(n);
            w.array_len(ps.len());
            for (p, e) in ps {
                w.i32(*p);
                w.i16(*e);
                w.tags();
            }
            w.tags();
        }
        w.tags();
        w.buf.to_vec()
    }
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 3);
        let _ = r.i32()?;
        let mut results = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let n = r.string()?;
            let mut ps = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                ps.push((r.i32()?, r.i16()?));
                r.skip_tags()?;
            }
            r.skip_tags()?;
            results.push((n, ps));
        }
        r.skip_tags()?;
        Ok(Self { results })
    }
}

// ------------------------------ AddOffsetsToTxn ------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsReq {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: String,
}

impl AddOffsetsReq {
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 3);
        let out = Self {
            transactional_id: r.string()?,
            producer_id: r.i64()?,
            producer_epoch: r.i16()?,
            group_id: r.string()?,
        };
        r.skip_tags()?;
        Ok(out)
    }
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 3);
        w.string(&self.transactional_id);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        w.string(&self.group_id);
        w.tags();
        w.buf.to_vec()
    }
}

/// Response shared by AddOffsetsToTxn and EndTxn: throttle + error code.
pub fn encode_error_only(v: i16, flex_from: i16, error: i16) -> Vec<u8> {
    let mut w = Wr::new(v >= flex_from);
    w.i32(0);
    w.i16(error);
    w.tags();
    w.buf.to_vec()
}

pub fn decode_error_only(b: &[u8], v: i16, flex_from: i16) -> CResult<i16> {
    let mut r = Rd::new(b, v >= flex_from);
    let _ = r.i32()?;
    let e = r.i16()?;
    r.skip_tags()?;
    Ok(e)
}

// --------------------------------- EndTxn ---------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnReq {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub committed: bool,
}

impl EndTxnReq {
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 3);
        let out = Self {
            transactional_id: r.string()?,
            producer_id: r.i64()?,
            producer_epoch: r.i16()?,
            committed: r.bool()?,
        };
        r.skip_tags()?;
        Ok(out)
    }
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 3);
        w.string(&self.transactional_id);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        w.bool(self.committed);
        w.tags();
        w.buf.to_vec()
    }
}

// ------------------------------ TxnOffsetCommit ------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnOffsetCommitReq {
    pub transactional_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    /// topic -> [(partition, offset, leader_epoch, metadata)]
    pub topics: Vec<(String, Vec<(i32, i64, i32, Option<String>)>)>,
}

impl TxnOffsetCommitReq {
    pub fn decode(b: &[u8], v: i16) -> CResult<Self> {
        let mut r = Rd::new(b, v >= 3);
        let transactional_id = r.string()?;
        let group_id = r.string()?;
        let producer_id = r.i64()?;
        let producer_epoch = r.i16()?;
        let (generation_id, member_id, group_instance_id) =
            if v >= 3 { (r.i32()?, r.string()?, r.nstring()?) } else { (-1, String::new(), None) };
        let mut topics = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let name = r.string()?;
            let mut ps = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                let p = r.i32()?;
                let off = r.i64()?;
                let le = if v >= 2 { r.i32()? } else { -1 };
                let md = r.nstring()?;
                r.skip_tags()?;
                ps.push((p, off, le, md));
            }
            r.skip_tags()?;
            topics.push((name, ps));
        }
        r.skip_tags()?;
        Ok(Self { transactional_id, group_id, producer_id, producer_epoch, generation_id, member_id, group_instance_id, topics })
    }
    pub fn encode(&self, v: i16) -> Vec<u8> {
        let mut w = Wr::new(v >= 3);
        w.string(&self.transactional_id);
        w.string(&self.group_id);
        w.i64(self.producer_id);
        w.i16(self.producer_epoch);
        if v >= 3 {
            w.i32(self.generation_id);
            w.string(&self.member_id);
            w.nstring(self.group_instance_id.as_deref());
        }
        w.array_len(self.topics.len());
        for (n, ps) in &self.topics {
            w.string(n);
            w.array_len(ps.len());
            for (p, off, le, md) in ps {
                w.i32(*p);
                w.i64(*off);
                if v >= 2 {
                    w.i32(*le);
                }
                w.nstring(md.as_deref());
                w.tags();
            }
            w.tags();
        }
        w.tags();
        w.buf.to_vec()
    }
}

pub fn encode_txn_offset_commit_resp(v: i16, topics: &[(String, Vec<i32>)], error: i16) -> Vec<u8> {
    let mut w = Wr::new(v >= 3);
    w.i32(0);
    w.array_len(topics.len());
    for (n, ps) in topics {
        w.string(n);
        w.array_len(ps.len());
        for p in ps {
            w.i32(*p);
            w.i16(error);
            w.tags();
        }
        w.tags();
    }
    w.tags();
    w.buf.to_vec()
}

pub fn decode_txn_offset_commit_resp(b: &[u8], v: i16) -> CResult<Vec<(String, Vec<(i32, i16)>)>> {
    let mut r = Rd::new(b, v >= 3);
    let _ = r.i32()?;
    let mut out = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let n = r.string()?;
        let mut ps = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            ps.push((r.i32()?, r.i16()?));
            r.skip_tags()?;
        }
        r.skip_tags()?;
        out.push((n, ps));
    }
    r.skip_tags()?;
    Ok(out)
}

// -------------------------------- dispatch --------------------------------

/// Handles a transaction API request. `body` is the request payload following the
/// request header (client id and, for flexible versions, header tagged fields
/// already consumed). Returns the response body (without header).
pub async fn handle_body(
    api_key: i16,
    v: i16,
    body: &[u8],
    lm: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Result<Vec<u8>, String> {
    let coord = coordinator_for(lm, cfg);
    let map_err = |e: super::codec::CodecError| e.to_string();
    match api_key {
        22 => {
            let req = InitProducerIdReq::decode(body, v).map_err(map_err)?;
            let res = coord
                .init_producer_id(req.transactional_id.as_deref(), req.timeout_ms, req.producer_id, req.producer_epoch)
                .await;
            Ok(InitProducerIdResp { error: res.error, producer_id: res.producer_id, producer_epoch: res.producer_epoch }
                .encode(v))
        }
        24 => {
            let req = AddPartitionsReq::decode(body, v).map_err(map_err)?;
            let parts: Vec<(String, i32)> =
                req.topics.iter().flat_map(|(t, ps)| ps.iter().map(move |p| (t.clone(), *p))).collect();
            let e = coord.add_partitions(&req.transactional_id, req.producer_id, req.producer_epoch, &parts);
            let results = req
                .topics
                .iter()
                .map(|(t, ps)| (t.clone(), ps.iter().map(|p| (*p, e)).collect()))
                .collect();
            Ok(AddPartitionsResp { results }.encode(v))
        }
        25 => {
            let req = AddOffsetsReq::decode(body, v).map_err(map_err)?;
            let e = coord.add_offsets(&req.transactional_id, req.producer_id, req.producer_epoch, &req.group_id);
            Ok(encode_error_only(v, 3, e))
        }
        26 => {
            let req = EndTxnReq::decode(body, v).map_err(map_err)?;
            let e = coord.end_txn(&req.transactional_id, req.producer_id, req.producer_epoch, req.committed).await;
            Ok(encode_error_only(v, 3, e))
        }
        28 => {
            let req = TxnOffsetCommitReq::decode(body, v).map_err(map_err)?;
            let offsets: Vec<PendingOffset> = req
                .topics
                .iter()
                .flat_map(|(t, ps)| {
                    let g = req.group_id.clone();
                    ps.iter().map(move |(p, off, le, md)| PendingOffset {
                        group: g.clone(),
                        topic: t.clone(),
                        partition: *p,
                        offset: *off,
                        leader_epoch: *le,
                        metadata: md.clone(),
                    })
                })
                .collect();
            let e = coord.txn_offset_commit(&req.transactional_id, &req.group_id, req.producer_id, req.producer_epoch, offsets);
            let topics: Vec<(String, Vec<i32>)> =
                req.topics.iter().map(|(t, ps)| (t.clone(), ps.iter().map(|p| p.0).collect())).collect();
            Ok(encode_txn_offset_commit_resp(v, &topics, e))
        }
        _ => Err(format!("not a transaction api: {}", api_key)),
    }
}

/// Full-frame dispatch. `rest` = bytes after the header's client_id string.
/// Returns the complete response payload (correlation id + header + body).
pub async fn handle_frame(
    api_key: i16,
    v: i16,
    correlation_id: i32,
    rest: &[u8],
    lm: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&correlation_id.to_be_bytes());
    if !supports(api_key, v) {
        // Response header for unsupported versions cannot be flexible-aware; emit error code only.
        out.extend_from_slice(&err::UNSUPPORTED_VERSION.to_be_bytes());
        return out;
    }
    let flex = flexible(api_key, v);
    let mut body = rest;
    if flex {
        // Request header v2 carries tagged fields after client_id.
        let mut r = Rd::new(rest, true);
        if r.skip_tags().is_err() {
            out.extend_from_slice(&err::INVALID_REQUEST.to_be_bytes());
            return out;
        }
        let consumed = rest.len() - r.remaining();
        body = &rest[consumed..];
        out.push(0); // response header v1: empty tagged fields
    }
    match handle_body(api_key, v, body, lm, cfg).await {
        Ok(b) => out.extend_from_slice(&b),
        Err(e) => {
            tracing::warn!("[AeroMQ Txn] bad request api={} v={}: {}", api_key, v, e);
            out.truncate(4);
            out.extend_from_slice(&err::INVALID_REQUEST.to_be_bytes());
        }
    }
    out
}
