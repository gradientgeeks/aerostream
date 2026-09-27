//! Produce-path and fetch-path helpers for idempotent/transactional batches.

use super::batch::{self, is_control, is_transactional, producer_info, split_batches, to_entries};
use super::coordinator::TxnCoordinator;
use super::err;
use crate::log::producer_state::SequenceCheckResult;
use crate::log::PartitionLog;

#[derive(Debug, PartialEq, Eq)]
pub struct ProduceOutcome {
    pub error: i16,
    pub base_offset: i64,
}

/// Appends a produce payload made of magic-2 record batches to `log`.
///
/// Returns `None` if `payload` is not a clean sequence of magic-2 batches (caller
/// falls back to the legacy raw path). Otherwise performs:
///  * transactional verification against the coordinator,
///  * epoch-aware idempotent sequence validation (duplicates return the cached offset),
///  * splitting into single-record entries (1 record == 1 offset) and base-offset patching,
///  * partition transaction index maintenance (ongoing txn tracking for LSO).
pub fn append_payload(
    log: &mut PartitionLog,
    payload: &[u8],
    txn: &TxnCoordinator,
    transactional_id: Option<&str>,
    topic: &str,
    partition: i32,
) -> Option<ProduceOutcome> {
    let batches = split_batches(payload)?;
    let mut first_base: Option<i64> = None;

    for b in batches {
        let (pid, epoch, base_seq, count) = producer_info(b)?;
        if is_control(b) {
            // Clients may never write control batches directly.
            return Some(ProduceOutcome { error: err::INVALID_REQUEST, base_offset: -1 });
        }
        let transactional = is_transactional(b);
        if transactional {
            if pid < 0 {
                return Some(ProduceOutcome { error: err::INVALID_REQUEST, base_offset: -1 });
            }
            let e = txn.verify_produce(transactional_id, pid, epoch, topic, partition);
            if e != 0 {
                return Some(ProduceOutcome { error: e, base_offset: -1 });
            }
        }
        if pid >= 0 && base_seq >= 0 {
            if let Some(st) = log.producer_tracker.get_producer_state(pid) {
                if epoch < st.epoch {
                    return Some(ProduceOutcome { error: err::INVALID_PRODUCER_EPOCH, base_offset: -1 });
                }
            }
            let next = log.next_offset;
            match log.producer_tracker.check_and_update_sequence(pid, epoch, base_seq, count, next) {
                SequenceCheckResult::Duplicate { last_offset } => {
                    first_base.get_or_insert(last_offset as i64);
                    continue;
                }
                SequenceCheckResult::OutOfOrder { error_code } => {
                    return Some(ProduceOutcome { error: error_code, base_offset: -1 });
                }
                SequenceCheckResult::ValidNext => {}
            }
        }
        let first = log.next_offset;
        if count <= 1 {
            match log.append_batch_slice(first as i64, b) {
                Ok(off) => {
                    if transactional {
                        log.txn_index.note_data(pid, epoch, off);
                    }
                }
                Err(e) => {
                    tracing::error!("[AeroMQ Txn] append failed: {}", e);
                    return Some(ProduceOutcome { error: err::KAFKA_STORAGE_ERROR, base_offset: -1 });
                }
            }
        } else {
            for e in to_entries(b, first) {
                match log.append(&e) {
                    Ok(off) => {
                        if transactional {
                            log.txn_index.note_data(pid, epoch, off);
                        }
                    }
                    Err(e) => {
                        tracing::error!("[AeroMQ Txn] append failed: {}", e);
                        return Some(ProduceOutcome { error: err::KAFKA_STORAGE_ERROR, base_offset: -1 });
                    }
                }
            }
        }
        first_base.get_or_insert(first as i64);
    }
    Some(ProduceOutcome { error: 0, base_offset: first_base.unwrap_or(log.next_offset as i64) })
}

/// Fetch visibility for a partition.
#[derive(Debug, PartialEq, Eq)]
pub struct FetchView {
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    /// Exclusive upper bound of readable offsets for the requested isolation level.
    pub upper_bound: i64,
    /// `None` for read_uncommitted (encoded as a null array), else (pid, first_offset).
    pub aborted: Option<Vec<(i64, i64)>>,
}

/// Computes LSO / read bound / aborted-transaction list for a fetch at `fetch_offset`.
pub fn fetch_view(log: &PartitionLog, fetch_offset: i64, isolation_level: i8) -> FetchView {
    let hw = log.high_watermark as i64;
    let lso = log.txn_index.lso(log.high_watermark) as i64;
    if isolation_level == 1 {
        let aborted = if lso > fetch_offset {
            log.txn_index
                .aborted_in_range(fetch_offset.max(0) as u64, (lso - 1) as u64)
                .into_iter()
                .map(|a| (a.producer_id, a.first_offset as i64))
                .collect()
        } else {
            Vec::new()
        };
        FetchView { high_watermark: hw, last_stable_offset: lso, upper_bound: lso, aborted: Some(aborted) }
    } else {
        FetchView { high_watermark: hw, last_stable_offset: lso, upper_bound: hw, aborted: None }
    }
}

#[allow(dead_code)]
pub fn is_txn_entry(entry: &[u8]) -> bool {
    batch::is_transactional(entry)
}
