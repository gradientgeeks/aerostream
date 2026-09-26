//! Share group (KIP-932) tests driven through the Kafka wire entry point.

use std::sync::Arc;
use std::time::Duration;

use super::api::*;
use super::group::{AckBatch, ShareFetchArgs};
use super::state::{ACK_ACCEPT, ACK_REJECT, ACK_RELEASE};
use super::{topic_id, ShareConfig, ShareCoordinator};
use crate::kafka::handlers::{encode_idempotent_records_batch, parse_records, KafkaRecord};
use crate::txn::batch::{self, ATTR_TRANSACTIONAL};
use crate::txn::coordinator::now_ms;
use crate::txn::err;
use crate::txn::testutil::*;

async fn produce_plain(e: &Env, topic: &str, part: i32, n: usize) {
    let recs: Vec<KafkaRecord> = (0..n).map(|i| KafkaRecord::new(None, Some(format!("m{}", i).into_bytes()))).collect();
    let b = encode_idempotent_records_batch(0, -1, -1, -1, &recs);
    let (er, _) = parse_produce_resp_v3(&call(e, 0, 3, false, &produce_body_v3(None, topic, part, &b)).await);
    assert_eq!(er, 0);
}

async fn hb(e: &Env, group: &str, member: &str, epoch: i32, subs: Option<Vec<&str>>) -> HeartbeatResp {
    let req = HeartbeatReq {
        group_id: group.into(),
        member_id: member.into(),
        member_epoch: epoch,
        rack_id: None,
        subscribed_topic_names: subs.map(|v| v.into_iter().map(String::from).collect()),
    }
    .encode();
    HeartbeatResp::decode(&call(e, 76, 1, true, &req).await).unwrap()
}

fn fetch_args(group: &str, member: &str, epoch: i32, tid: [u8; 16], part: i32, acks: Vec<AckBatch>) -> ShareFetchArgs {
    ShareFetchArgs {
        group_id: group.into(),
        member_id: member.into(),
        epoch,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        max_records: 100,
        topics: vec![(tid, vec![(part, acks)])],
        forgotten: vec![],
    }
}

async fn share_fetch(e: &Env, a: &ShareFetchArgs) -> crate::share::group::ShareFetchOutcome {
    decode_share_fetch_resp(&call(e, 78, 1, true, &encode_share_fetch_req(a, 1)).await, 1).unwrap()
}

async fn share_ack(e: &Env, group: &str, member: &str, epoch: i32, tid: [u8; 16], part: i32, acks: Vec<AckBatch>) -> (i16, Vec<([u8; 16], i32, i16)>) {
    let q = ShareAckReq { group_id: group.into(), member_id: member.into(), epoch, topics: vec![(tid, vec![(part, acks)])] };
    decode_share_ack_resp(&call(e, 79, 1, true, &encode_share_ack_req(&q, 1)).await, 1).unwrap()
}

fn ack(first: i64, last: i64, t: i8) -> AckBatch {
    AckBatch { first, last, types: vec![t] }
}

fn earliest_cfg(sc: &ShareCoordinator, group: &str) {
    let mut g = sc.group_config(group);
    g.earliest = true;
    sc.set_group_config(group, g);
}

#[tokio::test]
async fn share_group_wire_end_to_end_with_dlq() {
    let e = env("share_e2e");
    produce_plain(&e, "q", 0, 5).await;
    let sc = super::coordinator_for(&e.lm, &e.cfg);
    let mut gc = sc.group_config("g");
    gc.earliest = true;
    gc.max_delivery_attempts = 3;
    gc.dlq_topic = Some("{topic}.dlq".into());
    sc.set_group_config("g", gc);
    let tid = topic_id("q");

    // Join: assignment delivered with the topic UUID.
    let r = hb(&e, "g", "m1", 0, Some(vec!["q"])).await;
    assert_eq!(r.error, 0);
    assert_eq!(r.member_id.as_deref(), Some("m1"));
    assert!(r.member_epoch > 0);
    assert_eq!(r.assignment, Some(vec![(tid, vec![0])]));
    assert_eq!(r.heartbeat_interval_ms, 5000);
    // Steady-state heartbeat: no assignment resent.
    let r2 = hb(&e, "g", "m1", r.member_epoch, None).await;
    assert_eq!((r2.error, r2.assignment), (0, None));
    // Stale epoch is fenced; unknown member rejected.
    assert_eq!(hb(&e, "g", "m1", r.member_epoch + 5, None).await.error, err::FENCED_MEMBER_EPOCH);
    assert_eq!(hb(&e, "g", "ghost", 3, None).await.error, err::UNKNOWN_MEMBER_ID);

    // Open share session: acquire all 5 records.
    let o = share_fetch(&e, &fetch_args("g", "m1", 0, tid, 0, vec![])).await;
    assert_eq!(o.error, 0);
    assert_eq!(o.lock_timeout_ms, 30_000);
    assert_eq!(o.partitions.len(), 1);
    let p = &o.partitions[0];
    assert_eq!(p.acquired, vec![(0, 4, 1)]);
    let recs = parse_records(&p.records).unwrap();
    assert_eq!(recs.len(), 5);
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(r.offset, i as i64);
        assert_eq!(r.value.as_deref(), Some(format!("m{}", i).as_bytes()));
    }
    // Nothing left to acquire.
    let o = share_fetch(&e, &fetch_args("g", "m1", 1, tid, 0, vec![])).await;
    assert!(o.partitions.iter().all(|p| p.acquired.is_empty()));

    // Acknowledge: accept 0-2, release 3, reject 4.
    let (top, res) = share_ack(&e, "g", "m1", 2, tid, 0, vec![ack(0, 2, ACK_ACCEPT), ack(3, 3, ACK_RELEASE), ack(4, 4, ACK_REJECT)]).await;
    assert_eq!(top, 0);
    assert_eq!(res, vec![(tid, 0, 0)]);
    let st = sc.partition_stats("g", "q", 0).unwrap();
    assert_eq!((st.0, st.1, st.2, st.3), (3, 5, 1, 0)); // SPSO=3, SPEO=5, 1 available, 0 acquired
    // Rejected record went to the DLQ with provenance headers.
    {
        let dlq = e.lm.get_partition("q.dlq", 0).await.unwrap();
        let g = dlq.lock().await;
        assert_eq!(g.next_offset, 1);
    }
    let f = parse_fetch_resp_v7(&call(&e, 1, 7, false, &fetch_body_v7("q.dlq", 0, 0, 0)).await);
    let dl = parse_records(&f.records).unwrap();
    assert_eq!(dl[0].value.as_deref(), Some(&b"m4"[..]));
    let h = |k: &str| dl[0].headers.iter().find(|(n, _)| n == k).map(|(_, v)| String::from_utf8(v.clone()).unwrap());
    assert_eq!(h("x-original-offset").as_deref(), Some("4"));
    assert_eq!(h("x-dlq-reason").as_deref(), Some("rejected"));
    assert_eq!(h("x-share-group").as_deref(), Some("g"));

    // Released record is redelivered with delivery_count 2.
    let o = share_fetch(&e, &fetch_args("g", "m1", 3, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(3, 3, 2)]);
    // Acknowledging a record we do not hold -> INVALID_RECORD_STATE (piggybacked on fetch).
    let o = share_fetch(&e, &fetch_args("g", "m1", 4, tid, 0, vec![ack(0, 0, ACK_ACCEPT)])).await;
    assert_eq!(o.partitions[0].ack_error, err::INVALID_RECORD_STATE);
    // Piggybacked accept on the next fetch.
    let o = share_fetch(&e, &fetch_args("g", "m1", 5, tid, 0, vec![ack(3, 3, ACK_ACCEPT)])).await;
    assert_eq!(o.partitions[0].ack_error, 0);
    assert_eq!(sc.partition_stats("g", "q", 0).unwrap().0, 5);

    // Session epoch validation.
    let o = share_fetch(&e, &fetch_args("g", "m1", 99, tid, 0, vec![])).await;
    assert_eq!(o.error, err::INVALID_SHARE_SESSION_EPOCH);
    let o = share_fetch(&e, &fetch_args("g", "ghost", 1, tid, 0, vec![])).await;
    assert_eq!(o.error, err::UNKNOWN_MEMBER_ID);
    let o = share_fetch(&e, &fetch_args("g", "m1", 6, [9u8; 16], 0, vec![])).await;
    assert_eq!(o.partitions[0].error, err::UNKNOWN_TOPIC_ID);

    // Describe.
    let d = decode_describe_resp(&call(&e, 77, 1, true, &encode_describe_req(&["g".to_string(), "nope".to_string()])).await).unwrap();
    assert_eq!(d[0].0, "g");
    assert_eq!(d[0].2, "Stable");
    assert_eq!(d[0].4[0].3, vec![("q".to_string(), vec![0])]);
    assert_eq!(d[1].1, err::GROUP_ID_NOT_FOUND);

    // Leave.
    let r = hb(&e, "g", "m1", -1, None).await;
    assert_eq!((r.error, r.member_epoch), (0, -1));
    let d = decode_describe_resp(&call(&e, 77, 1, true, &encode_describe_req(&["g".to_string()])).await).unwrap();
    assert_eq!(d[0].2, "Empty");
}

#[tokio::test]
async fn closing_session_releases_acquired_records() {
    let e = env("share_close");
    produce_plain(&e, "q", 0, 3).await;
    let sc = super::coordinator_for(&e.lm, &e.cfg);
    earliest_cfg(&sc, "g");
    let tid = topic_id("q");
    hb(&e, "g", "m1", 0, Some(vec!["q"])).await;
    let o = share_fetch(&e, &fetch_args("g", "m1", 0, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(0, 2, 1)]);
    let mut close = fetch_args("g", "m1", -1, tid, 0, vec![ack(0, 0, ACK_ACCEPT)]);
    close.topics = vec![(tid, vec![(0, vec![ack(0, 0, ACK_ACCEPT)])])];
    let o = share_fetch(&e, &close).await;
    assert_eq!(o.error, 0);
    assert_eq!(o.partitions[0].ack_error, 0);
    let st = sc.partition_stats("g", "q", 0).unwrap();
    assert_eq!((st.0, st.2, st.3), (1, 2, 0)); // 0 accepted; 1,2 released
    // A second member picks the released records up.
    hb(&e, "g", "m2", 0, Some(vec!["q"])).await;
    let o = share_fetch(&e, &fetch_args("g", "m2", 0, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(1, 2, 2)]);
}

#[tokio::test]
async fn lock_timeout_redelivery_and_delivery_limit_archive() {
    let e = env("share_lock");
    produce_plain(&e, "q", 0, 1).await;
    let cfg = ShareConfig { lock_timeout_ms: 30, max_delivery_attempts: 2, auto_offset_reset: "earliest".into(), dlq_topic: Some("dead".into()), ..Default::default() };
    let sc = Arc::new(ShareCoordinator::open(None, Arc::downgrade(&e.lm), cfg));
    assert!(e.lm.share_coord.set(sc.clone()).is_ok());
    let tid = topic_id("q");
    hb(&e, "g", "m1", 0, Some(vec!["q"])).await;
    hb(&e, "g", "m2", 0, Some(vec!["q"])).await;

    let o = share_fetch(&e, &fetch_args("g", "m1", 0, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(0, 0, 1)]);
    // m2 sees nothing while the lock is held.
    let o = share_fetch(&e, &fetch_args("g", "m2", 0, tid, 0, vec![])).await;
    assert!(o.partitions.iter().all(|p| p.acquired.is_empty()));

    tokio::time::sleep(Duration::from_millis(60)).await;
    sc.sweep(now_ms()).await; // lock expires -> Available
    let o = share_fetch(&e, &fetch_args("g", "m2", 1, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(0, 0, 2)]);
    // m1 lost the lock: its late ack is rejected.
    let (_, res) = share_ack(&e, "g", "m1", 1, tid, 0, vec![ack(0, 0, ACK_ACCEPT)]).await;
    assert_eq!(res[0].2, err::INVALID_RECORD_STATE);

    tokio::time::sleep(Duration::from_millis(60)).await;
    sc.sweep(now_ms()).await; // 2nd expiry hits the delivery limit -> archived + DLQ
    let st = sc.partition_stats("g", "q", 0).unwrap();
    assert_eq!((st.0, st.2, st.3), (1, 0, 0));
    let dlq = e.lm.get_partition("dead", 0).await.unwrap();
    assert_eq!(dlq.lock().await.next_offset, 1);
}

#[tokio::test]
async fn assignment_is_balanced_and_rebalances_on_membership_change() {
    let e = env("share_assign");
    for p in 0..4 {
        e.lm.get_partition("t4", p).await.unwrap();
    }
    let a1 = hb(&e, "g", "a", 0, Some(vec!["t4"])).await;
    assert_eq!(a1.assignment.as_ref().unwrap()[0].1, vec![0, 1, 2, 3]);
    let b1 = hb(&e, "g", "b", 0, Some(vec!["t4"])).await;
    let b_parts = b1.assignment.clone().unwrap()[0].1.clone();
    assert_eq!(b_parts, vec![1, 3]);
    // Existing member learns about its reduced assignment on its next heartbeat.
    let a2 = hb(&e, "g", "a", a1.member_epoch, None).await;
    assert_eq!(a2.assignment.clone().unwrap()[0].1, vec![0, 2]);
    assert!(a2.member_epoch > a1.member_epoch);
    // More members than partitions: partitions are shared.
    for m in ["c", "d", "f"] {
        let r = hb(&e, "g", m, 0, Some(vec!["t4"])).await;
        assert_eq!(r.assignment.unwrap()[0].1.len(), 1);
    }
    let a3 = hb(&e, "g", "a", a2.member_epoch, None).await;
    assert_eq!(a3.assignment.as_ref().unwrap()[0].1.len(), 1); // 5 members > 4 partitions
    // Leaving members give partitions back.
    assert_eq!(hb(&e, "g", "f", -1, None).await.error, 0);
    assert_eq!(hb(&e, "g", "d", -1, None).await.error, 0);
    assert_eq!(hb(&e, "g", "c", -1, None).await.error, 0);
    let a4 = hb(&e, "g", "a", a3.member_epoch, None).await;
    assert_eq!(a4.assignment.unwrap()[0].1, vec![0, 2]);
    // Joining without a subscription is invalid.
    assert_eq!(hb(&e, "g", "z", 0, None).await.error, err::INVALID_REQUEST);
    assert_eq!(hb(&e, "", "z", 0, Some(vec![])).await.error, err::INVALID_REQUEST);
}

#[tokio::test]
async fn member_session_timeout_releases_records() {
    let e = env("share_session");
    produce_plain(&e, "q", 0, 2).await;
    let cfg = ShareConfig { session_timeout_ms: 20, auto_offset_reset: "earliest".into(), ..Default::default() };
    let sc = Arc::new(ShareCoordinator::open(None, Arc::downgrade(&e.lm), cfg));
    assert!(e.lm.share_coord.set(sc.clone()).is_ok());
    let tid = topic_id("q");
    hb(&e, "g", "m1", 0, Some(vec!["q"])).await;
    let o = share_fetch(&e, &fetch_args("g", "m1", 0, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired.len(), 1);
    tokio::time::sleep(Duration::from_millis(40)).await;
    sc.sweep(now_ms()).await;
    assert_eq!(sc.describe(&["g".to_string()])[0].members.len(), 0);
    let st = sc.partition_stats("g", "q", 0).unwrap();
    assert_eq!((st.2, st.3), (2, 0));
}

#[tokio::test]
async fn share_state_persists_across_restart() {
    let e = env("share_persist");
    produce_plain(&e, "q", 0, 4).await;
    let dir = e.dir.path().join("__share_state");
    let cfg = ShareConfig { auto_offset_reset: "earliest".into(), ..Default::default() };
    let sc = Arc::new(ShareCoordinator::open(Some(&dir), Arc::downgrade(&e.lm), cfg.clone()));
    assert!(e.lm.share_coord.set(sc.clone()).is_ok());
    let tid = topic_id("q");
    hb(&e, "g", "m1", 0, Some(vec!["q"])).await;
    share_fetch(&e, &fetch_args("g", "m1", 0, tid, 0, vec![])).await; // acquires 0..3
    share_ack(&e, "g", "m1", 1, tid, 0, vec![ack(0, 1, ACK_ACCEPT)]).await;
    sc.flush();

    // New coordinator instance on the same directory: SPSO kept, unacked records redelivered (count 2).
    let sc2 = ShareCoordinator::open(Some(&dir), Arc::downgrade(&e.lm), cfg);
    let o = {
        let g = sc2.heartbeat("g", "m9", 0, None, Some(vec!["q".into()])).await;
        assert_eq!(g.error, 0);
        sc2.share_fetch(fetch_args("g", "m9", 0, tid, 0, vec![])).await
    };
    assert_eq!(o.partitions[0].acquired, vec![(2, 3, 2)]);
}

#[tokio::test]
async fn read_committed_share_group_skips_aborted_and_control_records() {
    use crate::txn::api::*;
    let e = env("share_rc");
    // Transactional writer: abort one txn, then a plain record follows.
    let req = InitProducerIdReq { transactional_id: Some("tx".into()), timeout_ms: 60000, producer_id: -1, producer_epoch: -1 }.encode(4);
    let init = InitProducerIdResp::decode(&call(&e, 22, 4, true, &req).await, 4).unwrap();
    let ap = AddPartitionsReq { transactional_id: "tx".into(), producer_id: init.producer_id, producer_epoch: 0, topics: vec![("q".into(), vec![0])] }.encode(3);
    call(&e, 24, 3, true, &ap).await;
    let recs: Vec<KafkaRecord> = (0..2).map(|i| KafkaRecord::new(None, Some(format!("t{}", i).into_bytes()))).collect();
    let mut b = encode_idempotent_records_batch(0, init.producer_id, 0, 0, &recs);
    batch::set_attributes(&mut b, ATTR_TRANSACTIONAL);
    assert_eq!(parse_produce_resp_v3(&call(&e, 0, 3, false, &produce_body_v3(Some("tx"), "q", 0, &b)).await).0, 0);
    // While the transaction is open a read_committed group sees nothing.
    let sc = super::coordinator_for(&e.lm, &e.cfg);
    let mut gc = sc.group_config("rc");
    gc.earliest = true;
    gc.read_committed = true;
    sc.set_group_config("rc", gc.clone());
    let mut gu = gc.clone();
    gu.read_committed = false;
    sc.set_group_config("ru", gu);
    let tid = topic_id("q");
    hb(&e, "rc", "m", 0, Some(vec!["q"])).await;
    hb(&e, "ru", "m", 0, Some(vec!["q"])).await;
    let o = share_fetch(&e, &fetch_args("rc", "m", 0, tid, 0, vec![])).await;
    assert!(o.partitions.iter().all(|p| p.acquired.is_empty()));
    // read_uncommitted sees the open transaction's records.
    let o = share_fetch(&e, &fetch_args("ru", "m", 0, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(0, 1, 1)]);

    let et = EndTxnReq { transactional_id: "tx".into(), producer_id: init.producer_id, producer_epoch: 0, committed: false }.encode(3);
    call(&e, 26, 3, true, &et).await;
    produce_plain(&e, "q", 0, 1).await; // offset 3 (offset 2 is the abort marker)
    let o = share_fetch(&e, &fetch_args("rc", "m", 1, tid, 0, vec![])).await;
    assert_eq!(o.partitions[0].acquired, vec![(3, 3, 1)]); // aborted data + marker skipped
    let st = sc.partition_stats("rc", "q", 0).unwrap();
    assert_eq!((st.0, st.1), (3, 4)); // SPSO stops at the acquired record, skipped ones archived
}
