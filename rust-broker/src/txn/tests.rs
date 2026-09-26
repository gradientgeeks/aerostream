//! End-to-end transaction tests driven through the Kafka wire entry point.

use std::sync::Arc;

use super::api::*;
use super::batch::{self, ATTR_TRANSACTIONAL};
use super::coordinator::{now_ms, TxnCoordinator};
use super::err;
use super::sink::{InMemorySink, OffsetSink};
use super::store::TxnState;
use super::testutil::*;
use super::TxnConfig;
use crate::kafka::handlers::{encode_idempotent_records_batch, parse_records, KafkaRecord};
use crate::log::LogManager;

const TID: &str = "tx1";

async fn init(e: &Env, tid: &str) -> InitProducerIdResp {
    let req = InitProducerIdReq { transactional_id: Some(tid.into()), timeout_ms: 60_000, producer_id: -1, producer_epoch: -1 }.encode(4);
    InitProducerIdResp::decode(&call(e, 22, 4, true, &req).await, 4).unwrap()
}

async fn add_parts(e: &Env, tid: &str, pid: i64, epoch: i16, topic: &str, parts: &[i32]) -> i16 {
    let req = AddPartitionsReq {
        transactional_id: tid.into(),
        producer_id: pid,
        producer_epoch: epoch,
        topics: vec![(topic.into(), parts.to_vec())],
    }
    .encode(3);
    let r = AddPartitionsResp::decode(&call(e, 24, 3, true, &req).await, 3).unwrap();
    r.results[0].1[0].1
}

async fn end_txn(e: &Env, tid: &str, pid: i64, epoch: i16, commit: bool) -> i16 {
    let req = EndTxnReq { transactional_id: tid.into(), producer_id: pid, producer_epoch: epoch, committed: commit }.encode(3);
    decode_error_only(&call(e, 26, 3, true, &req).await, 3, 3).unwrap()
}

fn txn_batch(pid: i64, epoch: i16, seq: i32, n: usize, tag: &str) -> Vec<u8> {
    let recs: Vec<KafkaRecord> = (0..n)
        .map(|i| KafkaRecord::new(Some(b"k".to_vec()), Some(format!("{}-{}", tag, i).into_bytes())))
        .collect();
    let mut b = encode_idempotent_records_batch(0, pid, epoch, seq, &recs);
    batch::set_attributes(&mut b, ATTR_TRANSACTIONAL);
    b
}

async fn produce(e: &Env, tid: Option<&str>, topic: &str, part: i32, records: &[u8]) -> (i16, i64) {
    parse_produce_resp_v3(&call(e, 0, 3, false, &produce_body_v3(tid, topic, part, records)).await)
}

async fn fetch(e: &Env, topic: &str, off: i64, iso: i8) -> FetchPart {
    parse_fetch_resp_v7(&call(e, 1, 7, false, &fetch_body_v7(topic, 0, off, iso)).await)
}

#[tokio::test]
async fn abort_then_commit_flow_with_lso_and_aborted_list() {
    let e = env("txn_flow");
    let init = init(&e, TID).await;
    assert_eq!(init.error, 0);
    assert!(init.producer_id >= 1000);
    assert_eq!(init.producer_epoch, 0);
    let (pid, ep) = (init.producer_id, init.producer_epoch);

    // Produce before AddPartitionsToTxn is rejected with INVALID_TXN_STATE.
    let (er, _) = produce(&e, Some(TID), "t", 0, &txn_batch(pid, ep, 0, 1, "x")).await;
    assert_eq!(er, err::INVALID_TXN_STATE);

    assert_eq!(add_parts(&e, TID, pid, ep, "t", &[0]).await, 0);
    let (er, base) = produce(&e, Some(TID), "t", 0, &txn_batch(pid, ep, 0, 3, "a")).await;
    assert_eq!((er, base), (0, 0));
    let (er, base) = produce(&e, Some(TID), "t", 0, &txn_batch(pid, ep, 3, 2, "b")).await;
    assert_eq!((er, base), (0, 3));

    // read_committed: nothing visible, LSO pinned at the first txn offset.
    let f = fetch(&e, "t", 0, 1).await;
    assert_eq!(f.error, 0);
    assert_eq!((f.hw, f.lso), (5, 0));
    assert!(f.records.is_empty());
    assert_eq!(f.aborted, Some(vec![]));
    // read_uncommitted: data visible, aborted list is null, per-record offsets are exact.
    for off in 0..5 {
        let f = fetch(&e, "t", off, 0).await;
        assert_eq!(f.aborted, None);
        assert_eq!(f.lso, 0);
        assert_eq!(i64::from_be_bytes(f.records[0..8].try_into().unwrap()), off);
        assert!(batch::is_transactional(&f.records));
    }

    // Abort.
    assert_eq!(end_txn(&e, TID, pid, ep, false).await, 0);
    // retry of same outcome is idempotent, opposite outcome is invalid
    assert_eq!(end_txn(&e, TID, pid, ep, false).await, 0);
    assert_eq!(end_txn(&e, TID, pid, ep, true).await, err::INVALID_TXN_STATE);

    let f = fetch(&e, "t", 0, 1).await;
    assert_eq!((f.hw, f.lso), (6, 6));
    assert_eq!(f.aborted, Some(vec![(pid, 0)]));
    let marker = fetch(&e, "t", 5, 1).await;
    assert_eq!(batch::control_type(&marker.records), Some(batch::CONTROL_ABORT));
    assert!(batch::is_control(&marker.records));

    // Second transaction: commit.
    assert_eq!(add_parts(&e, TID, pid, ep, "t", &[0]).await, 0);
    let (er, base) = produce(&e, Some(TID), "t", 0, &txn_batch(pid, ep, 5, 1, "c")).await;
    assert_eq!((er, base), (0, 6));
    let f = fetch(&e, "t", 6, 1).await;
    assert_eq!(f.lso, 6); // open txn again pins LSO
    assert!(f.records.is_empty());
    assert_eq!(end_txn(&e, TID, pid, ep, true).await, 0);
    let f = fetch(&e, "t", 6, 1).await;
    assert_eq!((f.hw, f.lso), (8, 8));
    assert_eq!(f.aborted, Some(vec![])); // only offsets >= 6 requested: aborted txn ended at 5
    assert_eq!(parse_records(&f.records).unwrap()[0].value.as_deref(), Some(&b"c-0"[..]));
    let marker = fetch(&e, "t", 7, 1).await;
    assert_eq!(batch::control_type(&marker.records), Some(batch::CONTROL_COMMIT));
    // Full read_committed view from 0 still reports the earlier aborted txn.
    assert_eq!(fetch(&e, "t", 0, 1).await.aborted, Some(vec![(pid, 0)]));

    // Coordinator ended in CompleteCommit.
    let coord = super::coordinator_for(&e.lm, &e.cfg);
    assert_eq!(coord.get(TID).unwrap().state, TxnState::CompleteCommit);
}

#[tokio::test]
async fn fencing_and_epoch_bump() {
    let e = env("txn_fence");
    let a = init(&e, TID).await;
    assert_eq!(add_parts(&e, TID, a.producer_id, a.producer_epoch, "t", &[0]).await, 0);
    let (er, _) = produce(&e, Some(TID), "t", 0, &txn_batch(a.producer_id, a.producer_epoch, 0, 2, "z")).await;
    assert_eq!(er, 0);
    // A new incarnation of the same transactional id aborts the zombie's open txn and bumps the epoch.
    let b = init(&e, TID).await;
    assert_eq!(b.producer_id, a.producer_id);
    assert_eq!(b.producer_epoch, a.producer_epoch + 1);
    let f = fetch(&e, "t", 0, 1).await;
    assert_eq!(f.aborted, Some(vec![(a.producer_id, 0)]));
    assert_eq!(f.lso, 3); // 2 records + abort marker
    // Zombie is fenced everywhere.
    assert_eq!(add_parts(&e, TID, a.producer_id, a.producer_epoch, "t", &[0]).await, err::PRODUCER_FENCED);
    assert_eq!(end_txn(&e, TID, a.producer_id, a.producer_epoch, true).await, err::PRODUCER_FENCED);
    // New epoch restarts sequences at 0 (must not be mistaken for a duplicate).
    assert_eq!(add_parts(&e, TID, b.producer_id, b.producer_epoch, "t", &[0]).await, 0);
    let (er, base) = produce(&e, Some(TID), "t", 0, &txn_batch(b.producer_id, b.producer_epoch, 0, 1, "n")).await;
    assert_eq!((er, base), (0, 3));
    // Old epoch data is rejected at produce time.
    let (er, _) = produce(&e, Some(TID), "t", 0, &txn_batch(a.producer_id, a.producer_epoch, 2, 1, "old")).await;
    assert_eq!(er, err::INVALID_PRODUCER_EPOCH);
    assert_eq!(end_txn(&e, TID, b.producer_id, b.producer_epoch, true).await, 0);

    // Bad transactional ids / timeouts.
    let req = InitProducerIdReq { transactional_id: Some(String::new()), timeout_ms: 1000, producer_id: -1, producer_epoch: -1 }.encode(4);
    assert_eq!(InitProducerIdResp::decode(&call(&e, 22, 4, true, &req).await, 4).unwrap().error, err::INVALID_REQUEST);
    let req = InitProducerIdReq { transactional_id: Some("x".into()), timeout_ms: i32::MAX, producer_id: -1, producer_epoch: -1 }.encode(4);
    assert_eq!(InitProducerIdResp::decode(&call(&e, 22, 4, true, &req).await, 4).unwrap().error, err::INVALID_TRANSACTION_TIMEOUT);
    // Unknown transaction
    assert_eq!(end_txn(&e, "nope", 1, 0, true).await, err::TRANSACTIONAL_ID_NOT_FOUND);
}

#[tokio::test]
async fn timeout_aborts_and_fences() {
    let e = env("txn_timeout");
    let a = init(&e, TID).await;
    assert_eq!(add_parts(&e, TID, a.producer_id, a.producer_epoch, "t", &[0]).await, 0);
    produce(&e, Some(TID), "t", 0, &txn_batch(a.producer_id, a.producer_epoch, 0, 1, "x")).await;
    let coord = super::coordinator_for(&e.lm, &e.cfg);
    coord.sweep(now_ms() + 30_000).await; // not yet expired (60s timeout)
    assert_eq!(coord.get(TID).unwrap().state, TxnState::Ongoing);
    coord.sweep(now_ms() + 61_000).await;
    let m = coord.get(TID).unwrap();
    assert_eq!(m.state, TxnState::CompleteAbort);
    assert_eq!(m.producer_epoch, a.producer_epoch + 1); // zombie fenced
    let f = fetch(&e, "t", 0, 1).await;
    assert_eq!(f.aborted, Some(vec![(a.producer_id, 0)]));
    assert_eq!(f.lso, 2);
    assert_eq!(add_parts(&e, TID, a.producer_id, a.producer_epoch, "t", &[0]).await, err::PRODUCER_FENCED);
}

#[tokio::test]
async fn txn_offset_commit_applied_only_on_commit() {
    let e = env("txn_offsets");
    let sink = Arc::new(InMemorySink::default());
    let coord = Arc::new(TxnCoordinator::open(
        Some(&e.dir.path().join("__txn_state")),
        1,
        Arc::downgrade(&e.lm),
        sink.clone() as Arc<dyn OffsetSink>,
        TxnConfig::default(),
    ));
    assert!(e.lm.txn_coord.set(coord).is_ok());

    let a = init(&e, TID).await;
    let (pid, ep) = (a.producer_id, a.producer_epoch);
    let commit_req = |off: i64| {
        TxnOffsetCommitReq {
            transactional_id: TID.into(),
            group_id: "g1".into(),
            producer_id: pid,
            producer_epoch: ep,
            generation_id: 1,
            member_id: "m".into(),
            group_instance_id: None,
            topics: vec![("in".into(), vec![(0, off, -1, Some("md".into()))])],
        }
        .encode(3)
    };
    // Before AddOffsetsToTxn (and without an ongoing txn) -> INVALID_TXN_STATE.
    let r = decode_txn_offset_commit_resp(&call(&e, 28, 3, true, &commit_req(5)).await, 3).unwrap();
    assert_eq!(r[0].1[0].1, err::INVALID_TXN_STATE);

    let add = AddOffsetsReq { transactional_id: TID.into(), producer_id: pid, producer_epoch: ep, group_id: "g1".into() }.encode(3);
    assert_eq!(decode_error_only(&call(&e, 25, 3, true, &add).await, 3, 3).unwrap(), 0);
    let r = decode_txn_offset_commit_resp(&call(&e, 28, 3, true, &commit_req(5)).await, 3).unwrap();
    assert_eq!(r[0].1[0].1, 0);
    // Aborted transaction: offsets discarded.
    assert_eq!(end_txn(&e, TID, pid, ep, false).await, 0);
    assert!(sink.committed.lock().unwrap().is_empty());

    // Committed transaction: offsets forwarded (last write wins per partition).
    let add = AddOffsetsReq { transactional_id: TID.into(), producer_id: pid, producer_epoch: ep, group_id: "g1".into() }.encode(3);
    assert_eq!(decode_error_only(&call(&e, 25, 3, true, &add).await, 3, 3).unwrap(), 0);
    call(&e, 28, 3, true, &commit_req(7)).await;
    call(&e, 28, 3, true, &commit_req(9)).await;
    assert_eq!(end_txn(&e, TID, pid, ep, true).await, 0);
    let c = sink.committed.lock().unwrap();
    assert_eq!(c.len(), 1);
    assert_eq!(c[0].0, "g1");
    assert_eq!(c[0].1, vec![("in".to_string(), 0, 9)]);
}

#[tokio::test]
async fn state_survives_restart_and_prepare_is_redriven() {
    let e = env("txn_restart");
    let a = init(&e, TID).await;
    let (pid, ep) = (a.producer_id, a.producer_epoch);
    assert_eq!(add_parts(&e, TID, pid, ep, "t", &[0]).await, 0);
    produce(&e, Some(TID), "t", 0, &txn_batch(pid, ep, 0, 2, "x")).await;
    // Simulate a crash after PrepareCommit was journaled but before markers were written.
    super::coordinator_for(&e.lm, &e.cfg).force_state(TID, TxnState::PrepareCommit);

    // "Restart": new LogManager + coordinator on the same directory.
    let lm2 = Arc::new(LogManager::new(e.dir.path(), 1).with_limits(1024 * 1024, None, None));
    let e2 = Env { dir: TestDir(e.dir.path().to_path_buf()), lm: lm2, cfg: e.cfg.clone() };
    let coord = super::coordinator_for(&e2.lm, &e2.cfg);
    let m = coord.get(TID).unwrap();
    assert_eq!((m.producer_id, m.producer_epoch, m.state), (pid, ep, TxnState::PrepareCommit));
    // Partition index was replayed from disk: txn still open, LSO pinned.
    let f = fetch(&e2, "t", 0, 1).await;
    assert_eq!(f.lso, 0);
    // Sweeper re-drives the commit.
    coord.sweep(now_ms()).await;
    assert_eq!(coord.get(TID).unwrap().state, TxnState::CompleteCommit);
    let f = fetch(&e2, "t", 0, 1).await;
    assert_eq!((f.hw, f.lso), (3, 3));
    assert_eq!(f.aborted, Some(vec![]));
    // New pids never collide with old ones after restart.
    let b = init(&e2, "other").await;
    assert!(b.producer_id > pid);
    std::mem::forget(e2.dir); // e.dir owns cleanup
}

#[tokio::test]
async fn multi_record_client_batches_get_one_offset_per_record() {
    let e = env("txn_split");
    let recs: Vec<KafkaRecord> = (0..4).map(|i| KafkaRecord::new(None, Some(format!("v{}", i).into_bytes()))).collect();
    let b = encode_idempotent_records_batch(0, -1, -1, -1, &recs);
    let (er, base) = produce(&e, None, "plain", 0, &b).await;
    assert_eq!((er, base), (0, 0));
    let f = fetch(&e, "plain", 2, 0).await;
    assert_eq!(i64::from_be_bytes(f.records[0..8].try_into().unwrap()), 2);
    assert_eq!(parse_records(&f.records).unwrap()[0].value.as_deref(), Some(&b"v2"[..]));
    assert_eq!(f.hw, 4);
}

#[tokio::test]
async fn idempotent_producer_epoch_bump_resets_sequences() {
    let e = env("txn_idem");
    let a = init(&e, TID).await;
    let plain = |pid, ep, seq| {
        let r = KafkaRecord::new(None, Some(b"x".to_vec()));
        encode_idempotent_records_batch(0, pid, ep, seq, &[r])
    };
    // Non-transactional idempotent writes by a transactional-id producer are allowed.
    assert_eq!(produce(&e, None, "i", 0, &plain(a.producer_id, a.producer_epoch, 0)).await, (0, 0));
    assert_eq!(produce(&e, None, "i", 0, &plain(a.producer_id, a.producer_epoch, 1)).await, (0, 1));
    let b = init(&e, TID).await;
    assert_eq!(produce(&e, None, "i", 0, &plain(b.producer_id, b.producer_epoch, 0)).await, (0, 2));
    // Stale epoch rejected.
    assert_eq!(produce(&e, None, "i", 0, &plain(a.producer_id, a.producer_epoch, 2)).await.0, err::INVALID_PRODUCER_EPOCH);
}

#[tokio::test]
async fn unsupported_version_and_pid_allocation() {
    let e = env("txn_misc");
    let coord = super::coordinator_for(&e.lm, &e.cfg);
    let p1 = coord.allocate_producer_id();
    let p2 = coord.allocate_producer_id();
    assert_eq!(p2, p1 + 1);
    assert_eq!(p1 >> 40, 1); // broker-id prefixed
    let f = frame(26, 9, 7, true, &[]);
    let r = crate::net::kafka_server::handle_kafka_frame(&f, &e.lm, &e.cfg).await.unwrap().unwrap();
    assert_eq!(i16::from_be_bytes(r[4..6].try_into().unwrap()), err::UNSUPPORTED_VERSION);
}
