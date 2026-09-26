//! Kafka wire codecs and dispatch for share groups:
//! ShareGroupHeartbeat (76), ShareGroupDescribe (77), ShareFetch (78), ShareAcknowledge (79).
//! All versions are flexible (compact encodings, tagged fields).

use std::sync::Arc;

use super::coordinator_for;
use super::group::{AckBatch, GroupDescription, HbResult, PartResult, ShareFetchArgs, ShareFetchOutcome};
use crate::config::BrokerConfig;
use crate::log::LogManager;
use crate::txn::codec::{CResult, CodecError, Rd, Wr};
use crate::txn::err;

/// (api_key, min_version, max_version) advertised by this module.
pub const SHARE_API_VERSIONS: [(i16, i16, i16); 4] = [(76, 1, 1), (77, 1, 1), (78, 1, 2), (79, 1, 2)];

pub fn supports(api_key: i16, v: i16) -> bool {
    SHARE_API_VERSIONS.iter().any(|(k, lo, hi)| *k == api_key && v >= *lo && v <= *hi)
}

// ------------------------------- heartbeat -------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatReq {
    pub group_id: String,
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub subscribed_topic_names: Option<Vec<String>>,
}

impl HeartbeatReq {
    pub fn decode(b: &[u8]) -> CResult<Self> {
        let mut r = Rd::new(b, true);
        let group_id = r.string()?;
        let member_id = r.string()?;
        let member_epoch = r.i32()?;
        let rack_id = r.nstring()?;
        let subscribed_topic_names = match r.array_len()? {
            None => None,
            Some(n) => {
                let mut v = Vec::new();
                for _ in 0..n {
                    v.push(r.string()?);
                }
                Some(v)
            }
        };
        r.skip_tags()?;
        Ok(Self { group_id, member_id, member_epoch, rack_id, subscribed_topic_names })
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Wr::new(true);
        w.string(&self.group_id);
        w.string(&self.member_id);
        w.i32(self.member_epoch);
        w.nstring(self.rack_id.as_deref());
        match &self.subscribed_topic_names {
            None => w.null_array(),
            Some(v) => {
                w.array_len(v.len());
                for t in v {
                    w.string(t);
                }
            }
        }
        w.tags();
        w.buf.to_vec()
    }
}

pub fn encode_heartbeat_resp(r: &HbResult) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.i32(0);
    w.i16(r.error);
    w.nstring(r.message.as_deref());
    w.nstring(r.member_id.as_deref());
    w.i32(r.member_epoch);
    w.i32(r.heartbeat_interval_ms);
    match &r.assignment {
        None => w.i8(-1),
        Some(a) => {
            w.i8(1);
            w.array_len(a.len());
            for (t, ps) in a {
                w.uuid(&super::topic_id(t));
                w.array_len(ps.len());
                for p in ps {
                    w.i32(*p);
                }
                w.tags();
            }
            w.tags();
        }
    }
    w.tags();
    w.buf.to_vec()
}

/// Decoded ShareGroupHeartbeat response (topic ids, not names).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResp {
    pub error: i16,
    pub message: Option<String>,
    pub member_id: Option<String>,
    pub member_epoch: i32,
    pub heartbeat_interval_ms: i32,
    pub assignment: Option<Vec<([u8; 16], Vec<i32>)>>,
}

impl HeartbeatResp {
    pub fn decode(b: &[u8]) -> CResult<Self> {
        let mut r = Rd::new(b, true);
        let _ = r.i32()?;
        let error = r.i16()?;
        let message = r.nstring()?;
        let member_id = r.nstring()?;
        let member_epoch = r.i32()?;
        let heartbeat_interval_ms = r.i32()?;
        let assignment = if r.i8()? < 0 {
            None
        } else {
            let mut tps = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                let id = r.uuid()?;
                let mut ps = Vec::new();
                for _ in 0..r.array_len()?.unwrap_or(0) {
                    ps.push(r.i32()?);
                }
                r.skip_tags()?;
                tps.push((id, ps));
            }
            r.skip_tags()?;
            Some(tps)
        };
        r.skip_tags()?;
        Ok(Self { error, message, member_id, member_epoch, heartbeat_interval_ms, assignment })
    }
}

// -------------------------------- describe --------------------------------

pub fn decode_describe_req(b: &[u8]) -> CResult<Vec<String>> {
    let mut r = Rd::new(b, true);
    let mut ids = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        ids.push(r.string()?);
    }
    let _include_auth = r.bool()?;
    r.skip_tags()?;
    Ok(ids)
}

pub fn encode_describe_req(ids: &[String]) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.array_len(ids.len());
    for i in ids {
        w.string(i);
    }
    w.bool(false);
    w.tags();
    w.buf.to_vec()
}

pub fn encode_describe_resp(groups: &[GroupDescription]) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.i32(0);
    w.array_len(groups.len());
    for g in groups {
        w.i16(g.error);
        w.nstring(if g.error != 0 { Some("group not found") } else { None });
        w.string(&g.group_id);
        w.string(g.state);
        w.i32(g.epoch);
        w.i32(g.epoch);
        w.string("simple");
        w.array_len(g.members.len());
        for m in &g.members {
            w.string(&m.member_id);
            w.nstring(m.rack.as_deref());
            w.i32(m.epoch);
            w.string("");
            w.string("");
            w.array_len(m.subscribed.len());
            for t in &m.subscribed {
                w.string(t);
            }
            w.array_len(m.assignment.len());
            for (t, ps) in &m.assignment {
                w.uuid(&super::topic_id(t));
                w.string(t);
                w.array_len(ps.len());
                for p in ps {
                    w.i32(*p);
                }
                w.tags();
            }
            w.tags(); // assignment struct tags
            w.tags(); // member tags
        }
        w.i32(i32::MIN);
        w.tags();
    }
    w.tags();
    w.buf.to_vec()
}

/// (group_id, error, state, epoch, members: (member_id, epoch, subscribed, [(topic name, partitions)]))
pub type DescribedGroup = (String, i16, String, i32, Vec<(String, i32, Vec<String>, Vec<(String, Vec<i32>)>)>);

pub fn decode_describe_resp(b: &[u8]) -> CResult<Vec<DescribedGroup>> {
    let mut r = Rd::new(b, true);
    let _ = r.i32()?;
    let mut out = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let error = r.i16()?;
        let _msg = r.nstring()?;
        let gid = r.string()?;
        let state = r.string()?;
        let epoch = r.i32()?;
        let _aepoch = r.i32()?;
        let _assignor = r.string()?;
        let mut members = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let mid = r.string()?;
            let _rack = r.nstring()?;
            let mepoch = r.i32()?;
            let _cid = r.string()?;
            let _host = r.string()?;
            let mut subs = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                subs.push(r.string()?);
            }
            let mut asg = Vec::new();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                let _id = r.uuid()?;
                let name = r.string()?;
                let mut ps = Vec::new();
                for _ in 0..r.array_len()?.unwrap_or(0) {
                    ps.push(r.i32()?);
                }
                r.skip_tags()?;
                asg.push((name, ps));
            }
            r.skip_tags()?;
            r.skip_tags()?;
            members.push((mid, mepoch, subs, asg));
        }
        let _auth = r.i32()?;
        r.skip_tags()?;
        out.push((gid, error, state, epoch, members));
    }
    r.skip_tags()?;
    Ok(out)
}

// ------------------------------- share fetch -------------------------------

fn decode_ack_batches(r: &mut Rd) -> CResult<Vec<AckBatch>> {
    let mut batches = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let first = r.i64()?;
        let last = r.i64()?;
        let mut types = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            types.push(r.i8()?);
        }
        r.skip_tags()?;
        batches.push(AckBatch { first, last, types });
    }
    Ok(batches)
}

fn encode_ack_batches(w: &mut Wr, batches: &[AckBatch]) {
    w.array_len(batches.len());
    for b in batches {
        w.i64(b.first);
        w.i64(b.last);
        w.array_len(b.types.len());
        for t in &b.types {
            w.i8(*t);
        }
        w.tags();
    }
}

pub fn decode_share_fetch_req(b: &[u8], v: i16) -> CResult<ShareFetchArgs> {
    let mut r = Rd::new(b, true);
    let group_id = r.nstring()?.unwrap_or_default();
    let member_id = r.nstring()?.unwrap_or_default();
    let epoch = r.i32()?;
    let max_wait_ms = r.i32()?;
    let min_bytes = r.i32()?;
    let max_bytes = r.i32()?;
    let (max_records, _batch_size) = if v >= 1 { (r.i32()?, r.i32()?) } else { (0, 0) };
    if v >= 2 {
        let _acquire_mode = r.i8()?;
        let _is_renew = r.bool()?;
    }
    let mut topics = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let tid = r.uuid()?;
        let mut parts = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let p = r.i32()?;
            if v == 0 {
                let _pmb = r.i32()?;
            }
            let acks = decode_ack_batches(&mut r)?;
            r.skip_tags()?;
            parts.push((p, acks));
        }
        r.skip_tags()?;
        topics.push((tid, parts));
    }
    let mut forgotten = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let tid = r.uuid()?;
        let mut ps = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            ps.push(r.i32()?);
        }
        r.skip_tags()?;
        forgotten.push((tid, ps));
    }
    r.skip_tags()?;
    Ok(ShareFetchArgs { group_id, member_id, epoch, max_wait_ms, min_bytes, max_bytes, max_records, topics, forgotten })
}

pub fn encode_share_fetch_req(a: &ShareFetchArgs, v: i16) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.nstring(Some(&a.group_id));
    w.nstring(Some(&a.member_id));
    w.i32(a.epoch);
    w.i32(a.max_wait_ms);
    w.i32(a.min_bytes);
    w.i32(a.max_bytes);
    if v >= 1 {
        w.i32(a.max_records);
        w.i32(a.max_records);
    }
    if v >= 2 {
        w.i8(0);
        w.bool(false);
    }
    w.array_len(a.topics.len());
    for (tid, parts) in &a.topics {
        w.uuid(tid);
        w.array_len(parts.len());
        for (p, acks) in parts {
            w.i32(*p);
            encode_ack_batches(&mut w, acks);
            w.tags();
        }
        w.tags();
    }
    w.array_len(a.forgotten.len());
    for (tid, ps) in &a.forgotten {
        w.uuid(tid);
        w.array_len(ps.len());
        for p in ps {
            w.i32(*p);
        }
        w.tags();
    }
    w.tags();
    w.buf.to_vec()
}

fn group_by_topic(parts: &[PartResult]) -> Vec<(Vec<u8>, Vec<&PartResult>)> {
    let mut out: Vec<(Vec<u8>, Vec<&PartResult>)> = Vec::new();
    for p in parts {
        match out.iter_mut().find(|(t, _)| t.as_slice() == p.topic_id.as_slice()) {
            Some((_, v)) => v.push(p),
            None => out.push((p.topic_id.to_vec(), vec![p])),
        }
    }
    out
}

pub fn encode_share_fetch_resp(o: &ShareFetchOutcome, v: i16, leader_id: i32) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.i32(0);
    w.i16(o.error);
    w.nstring(o.message.as_deref());
    if v >= 1 {
        w.i32(o.lock_timeout_ms);
    }
    let topics = group_by_topic(&o.partitions);
    w.array_len(topics.len());
    for (tid, parts) in topics {
        w.uuid(&tid.as_slice().try_into().unwrap());
        w.array_len(parts.len());
        for p in parts {
            w.i32(p.partition);
            w.i16(p.error);
            w.nstring(None);
            w.i16(p.ack_error);
            w.nstring(None);
            w.i32(leader_id);
            w.i32(0);
            w.tags(); // CurrentLeader tags
            if p.records.is_empty() {
                w.bytes(Some(&[]));
            } else {
                w.bytes(Some(&p.records));
            }
            w.array_len(p.acquired.len());
            for (f, l, c) in &p.acquired {
                w.i64(*f);
                w.i64(*l);
                w.i16(*c);
                w.tags();
            }
            w.tags();
        }
        w.tags();
    }
    w.array_len(0); // NodeEndpoints
    w.tags();
    w.buf.to_vec()
}

/// Client-side decode (tests / tooling): returns (error, lock_timeout, partitions).
pub fn decode_share_fetch_resp(b: &[u8], v: i16) -> CResult<ShareFetchOutcome> {
    let mut r = Rd::new(b, true);
    let _ = r.i32()?;
    let mut o = ShareFetchOutcome { error: r.i16()?, message: r.nstring()?, ..Default::default() };
    if v >= 1 {
        o.lock_timeout_ms = r.i32()?;
    }
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let tid = r.uuid()?;
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let mut p = PartResult { topic_id: tid, partition: r.i32()?, error: r.i16()?, ..Default::default() };
            let _m = r.nstring()?;
            p.ack_error = r.i16()?;
            let _am = r.nstring()?;
            let _leader = (r.i32()?, r.i32()?);
            r.skip_tags()?;
            p.records = r.bytes()?.unwrap_or(&[]).to_vec();
            for _ in 0..r.array_len()?.unwrap_or(0) {
                p.acquired.push((r.i64()?, r.i64()?, r.i16()?));
                r.skip_tags()?;
            }
            r.skip_tags()?;
            o.partitions.push(p);
        }
        r.skip_tags()?;
    }
    for _ in 0..r.array_len()?.unwrap_or(0) {
        return Err(CodecError("unexpected node endpoints".into()));
    }
    r.skip_tags()?;
    Ok(o)
}

// ---------------------------- share acknowledge ----------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareAckReq {
    pub group_id: String,
    pub member_id: String,
    pub epoch: i32,
    pub topics: Vec<([u8; 16], Vec<(i32, Vec<AckBatch>)>)>,
}

pub fn decode_share_ack_req(b: &[u8], v: i16) -> CResult<ShareAckReq> {
    let mut r = Rd::new(b, true);
    let group_id = r.nstring()?.unwrap_or_default();
    let member_id = r.nstring()?.unwrap_or_default();
    let epoch = r.i32()?;
    if v >= 2 {
        let _renew = r.bool()?;
    }
    let mut topics = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let tid = r.uuid()?;
        let mut parts = Vec::new();
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let p = r.i32()?;
            let acks = decode_ack_batches(&mut r)?;
            r.skip_tags()?;
            parts.push((p, acks));
        }
        r.skip_tags()?;
        topics.push((tid, parts));
    }
    r.skip_tags()?;
    Ok(ShareAckReq { group_id, member_id, epoch, topics })
}

pub fn encode_share_ack_req(q: &ShareAckReq, v: i16) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.nstring(Some(&q.group_id));
    w.nstring(Some(&q.member_id));
    w.i32(q.epoch);
    if v >= 2 {
        w.bool(false);
    }
    w.array_len(q.topics.len());
    for (tid, parts) in &q.topics {
        w.uuid(tid);
        w.array_len(parts.len());
        for (p, acks) in parts {
            w.i32(*p);
            encode_ack_batches(&mut w, acks);
            w.tags();
        }
        w.tags();
    }
    w.tags();
    w.buf.to_vec()
}

pub fn encode_share_ack_resp(error: i16, parts: &[PartResult], v: i16, lock_ms: i32, leader_id: i32) -> Vec<u8> {
    let mut w = Wr::new(true);
    w.i32(0);
    w.i16(error);
    w.nstring(None);
    if v >= 2 {
        w.i32(lock_ms);
    }
    let topics = group_by_topic(parts);
    w.array_len(topics.len());
    for (tid, ps) in topics {
        w.uuid(&tid.as_slice().try_into().unwrap());
        w.array_len(ps.len());
        for p in ps {
            w.i32(p.partition);
            w.i16(p.ack_error);
            w.nstring(None);
            w.i32(leader_id);
            w.i32(0);
            w.tags();
            w.tags();
        }
        w.tags();
    }
    w.array_len(0);
    w.tags();
    w.buf.to_vec()
}

/// Client-side decode: (top-level error, [(topic id, partition, error)]).
pub fn decode_share_ack_resp(b: &[u8], v: i16) -> CResult<(i16, Vec<([u8; 16], i32, i16)>)> {
    let mut r = Rd::new(b, true);
    let _ = r.i32()?;
    let error = r.i16()?;
    let _m = r.nstring()?;
    if v >= 2 {
        let _ = r.i32()?;
    }
    let mut out = Vec::new();
    for _ in 0..r.array_len()?.unwrap_or(0) {
        let tid = r.uuid()?;
        for _ in 0..r.array_len()?.unwrap_or(0) {
            let p = r.i32()?;
            let e = r.i16()?;
            let _em = r.nstring()?;
            let _l = (r.i32()?, r.i32()?);
            r.skip_tags()?;
            r.skip_tags()?;
            out.push((tid, p, e));
        }
        r.skip_tags()?;
    }
    for _ in 0..r.array_len()?.unwrap_or(0) {
        return Err(CodecError("unexpected node endpoints".into()));
    }
    r.skip_tags()?;
    Ok((error, out))
}

// -------------------------------- dispatch --------------------------------

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
        out.extend_from_slice(&err::UNSUPPORTED_VERSION.to_be_bytes());
        return out;
    }
    // Request header v2: tagged fields after client_id.
    let mut hr = Rd::new(rest, true);
    if hr.skip_tags().is_err() {
        out.extend_from_slice(&err::INVALID_REQUEST.to_be_bytes());
        return out;
    }
    let body = &rest[rest.len() - hr.remaining()..];
    out.push(0); // response header v1 tagged fields
    let coord = coordinator_for(lm, cfg);
    let leader = cfg.id as i32;
    let res: Result<Vec<u8>, CodecError> = async {
        Ok(match api_key {
            76 => {
                let q = HeartbeatReq::decode(body)?;
                let r = coord
                    .heartbeat(&q.group_id, &q.member_id, q.member_epoch, q.rack_id, q.subscribed_topic_names)
                    .await;
                encode_heartbeat_resp(&r)
            }
            77 => {
                let ids = decode_describe_req(body)?;
                encode_describe_resp(&coord.describe(&ids))
            }
            78 => {
                let a = decode_share_fetch_req(body, v)?;
                let o = coord.share_fetch(a).await;
                encode_share_fetch_resp(&o, v, leader)
            }
            79 => {
                let q = decode_share_ack_req(body, v)?;
                let lock = coord.group_config(&q.group_id).lock_timeout_ms as i32;
                let (e, parts) = coord.share_acknowledge(&q.group_id, &q.member_id, q.epoch, q.topics).await;
                encode_share_ack_resp(e, &parts, v, lock, leader)
            }
            _ => return Err(CodecError(format!("not a share api: {}", api_key))),
        })
    }
    .await;
    match res {
        Ok(b) => out.extend_from_slice(&b),
        Err(e) => {
            tracing::warn!("[AeroMQ Share] bad request api={} v={}: {}", api_key, v, e);
            out.truncate(4);
            out.extend_from_slice(&err::INVALID_REQUEST.to_be_bytes());
        }
    }
    out
}
