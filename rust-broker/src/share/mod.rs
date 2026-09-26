//! Share groups (KIP-932 "Queues for Kafka").
//!
//! Where state lives (decision): membership, sessions and per-partition
//! delivery state live in the *broker* (`group.rs`), next to the partition logs
//! they read from. Share-partition state (SPSO/SPEO, per-record state and
//! delivery count) is journaled to `<storage_dir>/__share_state/` (snapshots,
//! flushed every sweep) and reloaded on start; acquisition locks do not survive
//! a restart (records return to Available with their delivery count kept), i.e.
//! at-least-once delivery.
//!
//! Wire: ApiKeys 76 (ShareGroupHeartbeat), 77 (ShareGroupDescribe),
//! 78 (ShareFetch), 79 (ShareAcknowledge). Topics are addressed by UUID; use
//! [`topic_id`] to derive the id for a topic name (Metadata must expose the same id).

#![allow(dead_code)]

pub mod api;
pub mod group;
pub mod state;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde::Deserialize;

pub use group::ShareCoordinator;

use crate::config::BrokerConfig;
use crate::log::LogManager;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShareConfig {
    pub heartbeat_interval_ms: i32,
    pub session_timeout_ms: i64,
    /// Acquisition lock timeout (`group.share.record.lock.duration.ms`).
    pub lock_timeout_ms: i64,
    /// Deliveries before a record is archived (`group.share.delivery.count.limit`).
    pub max_delivery_attempts: i16,
    /// Max in-flight (available + acquired) records per share partition.
    pub max_in_flight: usize,
    /// "latest" (Kafka default) or "earliest" (`share.auto.offset.reset`).
    pub auto_offset_reset: String,
    /// "read_uncommitted" (default) or "read_committed" (`share.isolation.level`).
    pub isolation_level: String,
    pub default_max_records: i32,
    /// Dead-letter topic template for archived (rejected / delivery-limit) records.
    /// `{topic}` and `{group}` are substituted. `None` disables DLQ.
    pub dlq_topic: Option<String>,
    pub sweep_interval_ms: u64,
}

impl Default for ShareConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5000,
            session_timeout_ms: 45_000,
            lock_timeout_ms: 30_000,
            max_delivery_attempts: 5,
            max_in_flight: 2000,
            auto_offset_reset: "latest".into(),
            isolation_level: "read_uncommitted".into(),
            default_max_records: 500,
            dlq_topic: None,
            sweep_interval_ms: 1000,
        }
    }
}

/// Deterministic topic UUID derived from the topic name (two FNV-1a 64 hashes).
pub fn topic_id(name: &str) -> [u8; 16] {
    fn fnv(seed: u64, s: &str) -> u64 {
        let mut h = seed;
        for b in s.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }
    let a = fnv(0xcbf2_9ce4_8422_2325, name);
    let b = fnv(0x9e37_79b9_7f4a_7c15, name);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&a.to_be_bytes());
    out[8..].copy_from_slice(&b.to_be_bytes());
    out[6] = (out[6] & 0x0f) | 0x80; // version 8 (custom)
    out[8] = (out[8] & 0x3f) | 0x80; // RFC 4122 variant
    out
}

/// Returns (lazily creating) the broker's share coordinator.
pub fn coordinator_for(lm: &Arc<LogManager>, cfg: &BrokerConfig) -> Arc<ShareCoordinator> {
    lm.share_coord
        .get_or_init(|| {
            let dir = lm.base_dir().join("__share_state");
            let c = Arc::new(ShareCoordinator::open(Some(&dir), Arc::downgrade(lm), cfg.share.clone()));
            group::spawn_sweeper(&c);
            c
        })
        .clone()
}
