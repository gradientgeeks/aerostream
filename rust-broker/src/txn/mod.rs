//! Cross-partition transactions (KIP-98, KIP-890 "TV1" semantics).
//!
//! Where state lives (decision): the *transaction coordinator* runs inside the
//! broker (`coordinator.rs`) because only the broker that owns a partition log
//! can append commit/abort control markers to it, and there is no
//! controller->broker command channel. Coordinator state is journaled to
//! `<storage_dir>/__txn_state/txn.journal`; per-partition transaction indexes
//! (ongoing txns for the LSO, aborted txns for Fetch) are journaled next to
//! each partition (`txn.index`). Committed transactional consumer offsets are
//! forwarded to the controller's Raft-backed offset store on commit.
//! Producer ids are `(broker_id << 40) + counter`, allocated in persisted blocks,
//! so they are cluster-unique and restart-safe.

#![allow(dead_code)]

pub mod api;
pub mod batch;
pub mod codec;
pub mod coordinator;
pub mod index;
pub mod produce;
pub mod sink;
pub mod store;

#[cfg(test)]
pub mod testutil;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde::Deserialize;

pub use coordinator::TxnCoordinator;
pub use index::PartitionTxnIndex;

use crate::config::BrokerConfig;
use crate::log::LogManager;

/// Kafka error codes used by the transaction and share-group APIs.
pub mod err {
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const INVALID_REQUEST: i16 = 42;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub const INVALID_TXN_STATE: i16 = 48;
    pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
    pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
    pub const CONCURRENT_TRANSACTIONS: i16 = 51;
    pub const KAFKA_STORAGE_ERROR: i16 = 56;
    pub const GROUP_ID_NOT_FOUND: i16 = 69;
    pub const UNKNOWN_TOPIC_ID: i16 = 100;
    pub const PRODUCER_FENCED: i16 = 90;
    pub const TRANSACTIONAL_ID_NOT_FOUND: i16 = 105;
    pub const FENCED_MEMBER_EPOCH: i16 = 110;
    pub const INVALID_RECORD_STATE: i16 = 121;
    pub const SHARE_SESSION_NOT_FOUND: i16 = 122;
    pub const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TxnConfig {
    /// Upper bound accepted for `transaction.timeout.ms` (default 15 min).
    pub max_timeout_ms: i32,
    /// Idle transactional ids are expired after this long (default 7 days).
    pub transactional_id_expiration_ms: i64,
    /// Interval of the timeout / recovery sweeper.
    pub sweep_interval_ms: u64,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            max_timeout_ms: 900_000,
            transactional_id_expiration_ms: 7 * 24 * 3600 * 1000,
            sweep_interval_ms: 10_000,
        }
    }
}

/// Returns (lazily creating) the broker's transaction coordinator.
pub fn coordinator_for(lm: &Arc<LogManager>, cfg: &BrokerConfig) -> Arc<TxnCoordinator> {
    lm.txn_coord
        .get_or_init(|| {
            let dir = lm.base_dir().join("__txn_state");
            let sink: Arc<dyn sink::OffsetSink> =
                Arc::new(sink::GrpcOffsetSink::new(&cfg.controller, cfg.auth.token.clone()));
            let c = Arc::new(TxnCoordinator::open(
                Some(&dir),
                lm.broker_id,
                Arc::downgrade(lm),
                sink,
                cfg.txn.clone(),
            ));
            coordinator::spawn_sweeper(&c);
            c
        })
        .clone()
}
