//! Wire-level handlers for the consumer-group APIs:
//! FindCoordinator(10), JoinGroup(11), Heartbeat(12), LeaveGroup(13), SyncGroup(14),
//! OffsetCommit(8), OffsetFetch(9), DescribeGroups(15), ListGroups(16), DeleteGroups(42).

use std::sync::Arc;

use super::admin::AdminState;
use super::codec::{CodecResult, Rd, Wr};
use super::groups::*;

const NOT_COORD: i16 = NOT_COORDINATOR;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn handle(st: &Arc<AdminState>, api: i16, v: i16, rd: &mut Rd<'_>, client_id: &str) -> CodecResult<Vec<u8>> {
    match api {
        10 => find_coordinator(st, v, rd).await,
        11 => join_group(st, v, rd, client_id).await,
        12 => heartbeat(st, v, rd),
        13 => leave_group(st, v, rd),
        14 => sync_group(st, v, rd).await,
        8 => offset_commit(st, v, rd).await,
        9 => offset_fetch(st, v, rd).await,
        15 => describe_groups(st, v, rd),
        16 => list_groups(st, v, rd),
        42 => delete_groups(st, v, rd),
        _ => Err(format!("group_api: unsupported api {api}")),
    }
}

async fn find_coordinator(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut keys: Vec<String> = Vec::new();
    let mut _key_type = 0i8;
    if v >= 4 {
        _key_type = rd.i8()?;
        for _ in 0..rd.arr()? {
            keys.push(rd.str()?);
        }
    } else {
        keys.push(rd.str()?);
        if v >= 1 {
            _key_type = rd.i8()?;
        }
    }
    rd.tagged()?;
    st.ensure_fresh().await;

    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0); // throttle
    }
    let resolve = |key: &str| -> (i16, i32, String, i32) {
        if key.is_empty() {
            return (INVALID_GROUP_ID, -1, String::new(), -1);
        }
        let node = st.coordinator_node(key);
        (NONE, node.0, node.1, node.2)
    };
    if v >= 4 {
        w.arr(keys.len());
        for k in &keys {
            let (e, id, host, port) = resolve(k);
            w.str(k).i32(id).str(&host).i32(port).i16(e).nstr(None).tagged();
        }
    } else {
        let (e, id, host, port) = resolve(&keys[0]);
        if v == 0 {
            w.i16(e);
        } else {
            w.i16(e).nstr(None);
        }
        w.i32(id).str(&host).i32(port);
    }
    w.tagged();
    Ok(w.finish())
}

async fn join_group(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>, client_id: &str) -> CodecResult<Vec<u8>> {
    let group_id = rd.str()?;
    let session_timeout_ms = rd.i32()?;
    let rebalance_timeout_ms = if v >= 1 { rd.i32()? } else { session_timeout_ms };
    let member_id = rd.str()?;
    let instance_id = if v >= 5 { rd.nstr()? } else { None };
    let protocol_type = rd.str()?;
    let mut protocols = Vec::new();
    for _ in 0..rd.arr()? {
        let name = rd.str()?;
        let md = rd.bytes()?;
        rd.tagged()?;
        protocols.push((name, md));
    }
    if v >= 8 {
        let _reason = rd.nstr()?;
    }
    rd.tagged()?;

    let outcome = if !st.is_coordinator(&group_id) {
        JoinOutcome { error: NOT_COORD, generation: -1, member_id: member_id.clone(), ..Default::default() }
    } else if session_timeout_ms < 0 || rebalance_timeout_ms < 0 {
        JoinOutcome { error: INVALID_SESSION_TIMEOUT, generation: -1, member_id: member_id.clone(), ..Default::default() }
    } else {
        st.groups
            .join(JoinRequest {
                group_id,
                member_id,
                instance_id,
                client_id: client_id.to_string(),
                client_host: "/unknown".into(),
                session_timeout_ms,
                rebalance_timeout_ms,
                protocol_type,
                protocols,
            })
            .await
    };

    let mut w = Wr::new(rd.flex);
    if v >= 2 {
        w.i32(0);
    }
    w.i16(outcome.error).i32(outcome.generation);
    if v >= 7 {
        w.nstr(outcome.protocol_type.as_deref()).nstr(outcome.protocol.as_deref());
    } else {
        w.str(outcome.protocol.as_deref().unwrap_or(""));
    }
    w.str(&outcome.leader);
    if v >= 9 {
        w.bool(false); // skip_assignment
    }
    w.str(&outcome.member_id);
    w.arr(outcome.members.len());
    for (id, inst, md) in &outcome.members {
        w.str(id);
        if v >= 5 {
            w.nstr(inst.as_deref());
        }
        w.bytes(md).tagged();
    }
    w.tagged();
    Ok(w.finish())
}

fn heartbeat(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let group = rd.str()?;
    let generation = rd.i32()?;
    let member = rd.str()?;
    if v >= 3 {
        let _inst = rd.nstr()?;
    }
    rd.tagged()?;
    let err = if !st.is_coordinator(&group) { NOT_COORD } else { st.groups.heartbeat(&group, &member, generation) };
    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    w.i16(err).tagged();
    Ok(w.finish())
}

fn leave_group(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let group = rd.str()?;
    let mut members: Vec<(String, Option<String>)> = Vec::new();
    if v >= 3 {
        for _ in 0..rd.arr()? {
            let id = rd.str()?;
            let inst = rd.nstr()?;
            if v >= 5 {
                let _reason = rd.nstr()?;
            }
            rd.tagged()?;
            members.push((id, inst));
        }
    } else {
        members.push((rd.str()?, None));
    }
    rd.tagged()?;
    let coord = st.is_coordinator(&group);
    let results: Vec<(String, Option<String>, i16)> = members
        .into_iter()
        .map(|(id, inst)| {
            let e = if coord { st.groups.leave(&group, &id, inst.as_deref()) } else { NOT_COORD };
            (id, inst, e)
        })
        .collect();
    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    if v >= 3 {
        // top-level error is NONE; per-member errors carry detail (Kafka semantics)
        let top = if coord { NONE } else { NOT_COORD };
        w.i16(top);
        w.arr(results.len());
        for (id, inst, e) in &results {
            w.str(id).nstr(inst.as_deref()).i16(*e).tagged();
        }
    } else {
        w.i16(results[0].2);
    }
    w.tagged();
    Ok(w.finish())
}

async fn sync_group(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let group = rd.str()?;
    let generation = rd.i32()?;
    let member = rd.str()?;
    if v >= 3 {
        let _inst = rd.nstr()?;
    }
    let (ptype, pname) = if v >= 5 { (rd.nstr()?, rd.nstr()?) } else { (None, None) };
    let mut assignments = Vec::new();
    for _ in 0..rd.arr()? {
        let id = rd.str()?;
        let data = rd.bytes()?;
        rd.tagged()?;
        assignments.push((id, data));
    }
    rd.tagged()?;
    let out = if !st.is_coordinator(&group) {
        SyncOutcome { error: NOT_COORD, ..Default::default() }
    } else {
        st.groups
            .sync(&group, &member, generation, ptype.as_deref(), pname.as_deref(), assignments)
            .await
    };
    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    w.i16(out.error);
    if v >= 5 {
        w.nstr(out.protocol_type.as_deref()).nstr(out.protocol.as_deref());
    }
    w.bytes(&out.assignment).tagged();
    Ok(w.finish())
}

async fn offset_commit(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let group = rd.str()?;
    let (generation, member) = if v >= 1 { (rd.i32()?, rd.str()?) } else { (-1, String::new()) };
    if v >= 7 {
        let _inst = rd.nstr()?;
    }
    if (2..=4).contains(&v) {
        let _retention = rd.i64()?;
    }
    let mut topics: Vec<(String, Vec<(i32, OffsetEntry)>)> = Vec::new();
    let ts = now_ms();
    for _ in 0..rd.arr()? {
        let name = rd.str()?;
        let mut parts = Vec::new();
        for _ in 0..rd.arr()? {
            let p = rd.i32()?;
            let offset = rd.i64()?;
            let epoch = if v >= 6 { rd.i32()? } else { -1 };
            let commit_ts = if v == 1 { rd.i64()? } else { ts };
            let md = rd.nstr()?;
            rd.tagged()?;
            parts.push((p, OffsetEntry { offset, leader_epoch: epoch, metadata: md, commit_ms: commit_ts }));
        }
        rd.tagged()?;
        topics.push((name, parts));
    }
    rd.tagged()?;

    let flat: Vec<(String, i32, OffsetEntry)> = topics
        .iter()
        .flat_map(|(t, ps)| ps.iter().map(move |(p, e)| (t.clone(), *p, e.clone())))
        .collect();
    let err = if !st.is_coordinator(&group) {
        NOT_COORD
    } else {
        st.groups.commit_offsets(&group, &member, generation, &flat)
    };
    if err == NONE {
        // write-through to the controller so offsets survive coordinator moves / restarts
        st.persist_offsets(&group, flat.iter().map(|(t, p, e)| (t.clone(), *p, e.offset)).collect()).await;
    }

    let mut w = Wr::new(rd.flex);
    if v >= 3 {
        w.i32(0);
    }
    w.arr(topics.len());
    for (t, ps) in &topics {
        w.str(t).arr(ps.len());
        for (p, _) in ps {
            w.i32(*p).i16(err).tagged();
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

async fn offset_fetch(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let group = rd.str()?;
    let req_topics: Option<Vec<(String, Vec<i32>)>> = match rd.narr()? {
        None => None,
        Some(n) => {
            let mut v_ = Vec::new();
            for _ in 0..n {
                let t = rd.str()?;
                let mut ps = Vec::new();
                for _ in 0..rd.arr()? {
                    ps.push(rd.i32()?);
                }
                rd.tagged()?;
                v_.push((t, ps));
            }
            Some(v_)
        }
    };
    if v >= 7 {
        let _require_stable = rd.bool()?;
    }
    rd.tagged()?;

    let coord = st.is_coordinator(&group);
    // rows: topic -> [(partition, offset, epoch, metadata, err)]
    let mut rows: Vec<(String, Vec<(i32, i64, i32, Option<String>, i16)>)> = Vec::new();
    let mut top_err = NONE;
    if !coord {
        top_err = NOT_COORD;
    } else {
        // Hydrate the local cache from the controller for topics we do not know about yet.
        if let Some(rt) = &req_topics {
            st.hydrate_offsets(&group, rt.iter().map(|(t, _)| t.clone()).collect()).await;
        }
        match &req_topics {
            Some(rt) => {
                for (t, ps) in rt {
                    let mut out = Vec::new();
                    for p in ps {
                        match st.groups.get_offset(&group, t, *p) {
                            Some(e) => out.push((*p, e.offset, e.leader_epoch, e.metadata.clone(), NONE)),
                            None => out.push((*p, -1, -1, None, NONE)),
                        }
                    }
                    rows.push((t.clone(), out));
                }
            }
            None => {
                let mut by_topic: std::collections::BTreeMap<String, Vec<(i32, i64, i32, Option<String>, i16)>> = Default::default();
                for (t, p, e) in st.groups.all_offsets(&group) {
                    by_topic.entry(t).or_default().push((p, e.offset, e.leader_epoch, e.metadata, NONE));
                }
                rows = by_topic.into_iter().collect();
            }
        }
    }

    let mut w = Wr::new(rd.flex);
    if v >= 3 {
        w.i32(0);
    }
    w.arr(rows.len());
    for (t, ps) in &rows {
        w.str(t).arr(ps.len());
        for (p, off, ep, md, e) in ps {
            w.i32(*p).i64(*off);
            if v >= 5 {
                w.i32(*ep);
            }
            w.nstr(md.as_deref()).i16(*e).tagged();
        }
        w.tagged();
    }
    if v >= 2 {
        w.i16(top_err);
    }
    w.tagged();
    Ok(w.finish())
}

fn describe_groups(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut ids = Vec::new();
    for _ in 0..rd.arr()? {
        ids.push(rd.str()?);
    }
    if v >= 3 {
        let _authz = rd.bool()?;
    }
    rd.tagged()?;
    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    w.arr(ids.len());
    for id in &ids {
        let coord = st.is_coordinator(id);
        let d = st.groups.describe(id);
        w.i16(if coord { NONE } else { NOT_COORD }).str(id);
        if !coord {
            w.str("").str("").str("").arr(0);
        } else {
            w.str(d.state.name()).str(&d.protocol_type).str(&d.protocol).arr(d.members.len());
            for m in &d.members {
                w.str(&m.member_id);
                if v >= 4 {
                    w.nstr(m.instance_id.as_deref());
                }
                w.str(&m.client_id).str(&m.client_host).bytes(&m.metadata).bytes(&m.assignment).tagged();
            }
        }
        if v >= 3 {
            w.i32(i32::MIN);
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

fn list_groups(st: &Arc<AdminState>, v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut filter = Vec::new();
    if v >= 4 {
        for _ in 0..rd.arr()? {
            filter.push(rd.str()?.to_ascii_lowercase());
        }
    }
    rd.tagged()?;
    let groups: Vec<_> = st
        .groups
        .list()
        .into_iter()
        .filter(|(id, _, s)| st.is_coordinator(id) && (filter.is_empty() || filter.contains(&s.name().to_ascii_lowercase())))
        .collect();
    let mut w = Wr::new(rd.flex);
    if v >= 1 {
        w.i32(0);
    }
    w.i16(NONE).arr(groups.len());
    for (id, pt, s) in &groups {
        w.str(id).str(pt);
        if v >= 4 {
            w.str(s.name());
        }
        w.tagged();
    }
    w.tagged();
    Ok(w.finish())
}

fn delete_groups(st: &Arc<AdminState>, _v: i16, rd: &mut Rd<'_>) -> CodecResult<Vec<u8>> {
    let mut ids = Vec::new();
    for _ in 0..rd.arr()? {
        ids.push(rd.str()?);
    }
    rd.tagged()?;
    let mut w = Wr::new(rd.flex);
    w.i32(0).arr(ids.len());
    for id in &ids {
        let e = if st.is_coordinator(id) { st.groups.delete_group(id) } else { NOT_COORD };
        w.str(id).i16(e).tagged();
    }
    w.tagged();
    Ok(w.finish())
}
