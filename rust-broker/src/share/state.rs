//! Share-partition state machine (KIP-932).
//! Manages in-flight record acquisition, acknowledgement, rejection, and SPSO advancement.

use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecState {
    Available,
    Acquired,
    Acknowledged,
    Archived,
}

impl RecState {
    fn code(self) -> u8 {
        match self {
            RecState::Available => 0,
            RecState::Acquired => 1,
            RecState::Acknowledged => 2,
            RecState::Archived => 3,
        }
    }
    fn from_code(c: u8) -> Option<Self> {
        Some(match c {
            0 => RecState::Available,
            1 => RecState::Acquired,
            2 => RecState::Acknowledged,
            3 => RecState::Archived,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlight {
    pub state: RecState,
    pub delivery_count: i16,
    pub member: Option<String>,
    pub lock_deadline_ms: i64,
}

/// Acknowledge types on the wire.
pub const ACK_GAP: i8 = 0;
pub const ACK_ACCEPT: i8 = 1;
pub const ACK_RELEASE: i8 = 2;
pub const ACK_REJECT: i8 = 3;
pub const ACK_RENEW: i8 = 4;

/// Why a record was archived without being accepted (for DLQ routing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveReason {
    Rejected,
    DeliveryLimit,
}

#[derive(Debug, Clone)]
pub struct SharePartition {
    /// SPSO: lowest offset that is not yet finalised.
    pub start_offset: u64,
    /// SPEO: next offset never yet considered for delivery.
    pub end_offset: u64,
    records: BTreeMap<u64, InFlight>,
    pub max_delivery_attempts: i16,
    pub lock_timeout_ms: i64,
    pub max_in_flight: usize,
    pub dirty: bool,
}

impl SharePartition {
    pub fn new(start: u64, max_delivery_attempts: i16, lock_timeout_ms: i64, max_in_flight: usize) -> Self {
        Self {
            start_offset: start,
            end_offset: start,
            records: BTreeMap::new(),
            max_delivery_attempts,
            lock_timeout_ms,
            max_in_flight,
            dirty: true,
        }
    }

    pub fn get(&self, offset: u64) -> Option<&InFlight> {
        self.records.get(&offset)
    }

    pub fn in_flight(&self) -> usize {
        self.records.values().filter(|r| matches!(r.state, RecState::Available | RecState::Acquired)).count()
    }

    pub fn has_capacity(&self) -> bool {
        self.in_flight() < self.max_in_flight
    }

    /// Available (redeliverable) offsets in ascending order.
    pub fn available(&self, limit: usize) -> Vec<u64> {
        self.records
            .iter()
            .filter(|(_, r)| r.state == RecState::Available)
            .map(|(o, _)| *o)
            .take(limit)
            .collect()
    }

    /// Acquires `offset` for `member`; returns the new delivery count.
    pub fn acquire(&mut self, offset: u64, member: &str, now: i64) -> i16 {
        let lock = now + self.lock_timeout_ms;
        let rec = self.records.entry(offset).or_insert(InFlight {
            state: RecState::Available,
            delivery_count: 0,
            member: None,
            lock_deadline_ms: 0,
        });
        rec.state = RecState::Acquired;
        rec.delivery_count += 1;
        rec.member = Some(member.to_string());
        rec.lock_deadline_ms = lock;
        if offset >= self.end_offset {
            self.end_offset = offset + 1;
        }
        self.dirty = true;
        rec.delivery_count
    }

    /// Marks `offset` as skipped (control batch, aborted transaction, compaction gap).
    pub fn archive_skipped(&mut self, offset: u64) {
        self.records.insert(
            offset,
            InFlight { state: RecState::Archived, delivery_count: 0, member: None, lock_deadline_ms: 0 },
        );
        if offset >= self.end_offset {
            self.end_offset = offset + 1;
        }
        self.dirty = true;
    }

    fn release_one(&mut self, offset: u64) -> Option<ArchiveReason> {
        let max = self.max_delivery_attempts;
        let r = self.records.get_mut(&offset)?;
        r.member = None;
        r.lock_deadline_ms = 0;
        if r.delivery_count >= max {
            r.state = RecState::Archived;
            Some(ArchiveReason::DeliveryLimit)
        } else {
            r.state = RecState::Available;
            None
        }
    }

    /// Applies an acknowledgement batch `[first, last]` from `member`.
    /// `types` has either one entry (applies to all) or one per offset.
    /// Returns per-offset errors (offset, error) for invalid state and archived offsets.
    pub fn acknowledge(
        &mut self,
        member: &str,
        first: u64,
        last: u64,
        types: &[i8],
        now: i64,
    ) -> (Result<(), i16>, Vec<(u64, ArchiveReason)>) {
        let mut archived = Vec::new();
        if last < first || types.is_empty() || (types.len() != 1 && types.len() as u64 != last - first + 1) {
            return (Err(super::super::txn::err::INVALID_REQUEST), archived);
        }
        // Validate first (atomic per batch).
        for off in first..=last {
            let owned = matches!(self.records.get(&off),
                Some(r) if r.state == RecState::Acquired && r.member.as_deref() == Some(member));
            if !owned {
                return (Err(super::super::txn::err::INVALID_RECORD_STATE), archived);
            }
        }
        for off in first..=last {
            let t = if types.len() == 1 { types[0] } else { types[(off - first) as usize] };
            match t {
                ACK_ACCEPT => {
                    let r = self.records.get_mut(&off).unwrap();
                    r.state = RecState::Acknowledged;
                    r.member = None;
                }
                ACK_GAP => {
                    let r = self.records.get_mut(&off).unwrap();
                    r.state = RecState::Archived;
                    r.member = None;
                }
                ACK_REJECT => {
                    let r = self.records.get_mut(&off).unwrap();
                    r.state = RecState::Archived;
                    r.member = None;
                    archived.push((off, ArchiveReason::Rejected));
                }
                ACK_RELEASE => {
                    if let Some(reason) = self.release_one(off) {
                        archived.push((off, reason));
                    }
                }
                ACK_RENEW => {
                    let lock = now + self.lock_timeout_ms;
                    self.records.get_mut(&off).unwrap().lock_deadline_ms = lock;
                }
                _ => return (Err(super::super::txn::err::INVALID_REQUEST), archived),
            }
        }
        self.trim();
        self.dirty = true;
        (Ok(()), archived)
    }

    /// Releases everything acquired by `member` (member left / session closed).
    pub fn release_member(&mut self, member: &str) -> Vec<(u64, ArchiveReason)> {
        let offs: Vec<u64> = self
            .records
            .iter()
            .filter(|(_, r)| r.state == RecState::Acquired && r.member.as_deref() == Some(member))
            .map(|(o, _)| *o)
            .collect();
        let mut archived = Vec::new();
        for o in offs {
            if let Some(reason) = self.release_one(o) {
                archived.push((o, reason));
            }
        }
        self.trim();
        self.dirty = true;
        archived
    }

    /// Expires acquisition locks whose deadline passed.
    pub fn expire_locks(&mut self, now: i64) -> Vec<(u64, ArchiveReason)> {
        let offs: Vec<u64> = self
            .records
            .iter()
            .filter(|(_, r)| r.state == RecState::Acquired && r.lock_deadline_ms <= now)
            .map(|(o, _)| *o)
            .collect();
        let mut archived = Vec::new();
        for o in &offs {
            if let Some(reason) = self.release_one(*o) {
                archived.push((*o, reason));
            }
        }
        if !offs.is_empty() {
            self.trim();
            self.dirty = true;
        }
        archived
    }

    /// Drops leading finalised records, advancing the SPSO.
    pub fn trim(&mut self) {
        loop {
            let next = match self.records.iter().next() {
                Some((o, r)) if matches!(r.state, RecState::Acknowledged | RecState::Archived) => *o,
                _ => break,
            };
            self.records.remove(&next);
            self.start_offset = next + 1;
        }
        match self.records.iter().next() {
            Some((first, _)) => self.start_offset = *first,
            None => self.start_offset = self.end_offset.max(self.start_offset),
        }
        if self.start_offset > self.end_offset {
            self.end_offset = self.start_offset;
        }
    }

    pub fn state_counts(&self) -> (usize, usize, usize, usize) {
        let mut c = (0, 0, 0, 0);
        for r in self.records.values() {
            match r.state {
                RecState::Available => c.0 += 1,
                RecState::Acquired => c.1 += 1,
                RecState::Acknowledged => c.2 += 1,
                RecState::Archived => c.3 += 1,
            }
        }
        c
    }

    // ---------------------------- persistence ----------------------------

    /// Text snapshot. Acquired records are persisted as Available (locks do not
    /// survive a broker restart; delivery counts do).
    pub fn encode(&self) -> String {
        let mut s = format!("S {} {}\n", self.start_offset, self.end_offset);
        for (o, r) in &self.records {
            let st = if r.state == RecState::Acquired { RecState::Available } else { r.state };
            s.push_str(&format!("R {} {} {}\n", o, st.code(), r.delivery_count));
        }
        s
    }

    pub fn decode(text: &str, max_delivery_attempts: i16, lock_timeout_ms: i64, max_in_flight: usize) -> Option<Self> {
        let mut sp = SharePartition::new(0, max_delivery_attempts, lock_timeout_ms, max_in_flight);
        let mut saw_header = false;
        for line in text.lines() {
            let p: Vec<&str> = line.split(' ').collect();
            match p.as_slice() {
                ["S", a, b] => {
                    sp.start_offset = a.parse().ok()?;
                    sp.end_offset = b.parse().ok()?;
                    saw_header = true;
                }
                ["R", o, st, c] => {
                    sp.records.insert(
                        o.parse().ok()?,
                        InFlight {
                            state: RecState::from_code(st.parse().ok()?)?,
                            delivery_count: c.parse().ok()?,
                            member: None,
                            lock_deadline_ms: 0,
                        },
                    );
                }
                _ => {}
            }
        }
        if !saw_header {
            return None;
        }
        sp.dirty = false;
        Some(sp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sp() -> SharePartition {
        SharePartition::new(0, 3, 1000, 100)
    }

    #[test]
    fn acquire_accept_advances_start() {
        let mut p = sp();
        assert_eq!(p.acquire(0, "m1", 0), 1);
        assert_eq!(p.acquire(1, "m1", 0), 1);
        assert_eq!(p.end_offset, 2);
        let (r, arch) = p.acknowledge("m1", 0, 1, &[ACK_ACCEPT], 0);
        assert!(r.is_ok() && arch.is_empty());
        assert_eq!(p.start_offset, 2);
        assert_eq!(p.in_flight(), 0);
    }

    #[test]
    fn release_redelivers_and_counts() {
        let mut p = sp();
        p.acquire(0, "m1", 0);
        let (r, _) = p.acknowledge("m1", 0, 0, &[ACK_RELEASE], 0);
        assert!(r.is_ok());
        assert_eq!(p.available(10), vec![0]);
        assert_eq!(p.acquire(0, "m2", 5), 2);
        assert_eq!(p.get(0).unwrap().member.as_deref(), Some("m2"));
    }

    #[test]
    fn delivery_limit_archives() {
        let mut p = sp();
        for i in 0..3 {
            assert_eq!(p.acquire(0, "m", 0), i + 1);
            let (_, arch) = p.acknowledge("m", 0, 0, &[ACK_RELEASE], 0);
            if i == 2 {
                assert_eq!(arch, vec![(0, ArchiveReason::DeliveryLimit)]);
            } else {
                assert!(arch.is_empty());
            }
        }
        assert_eq!(p.start_offset, 1);
        assert!(p.available(10).is_empty());
    }

    #[test]
    fn reject_archives_and_wrong_owner_errors() {
        let mut p = sp();
        p.acquire(0, "m1", 0);
        p.acquire(1, "m1", 0);
        let (r, _) = p.acknowledge("other", 0, 0, &[ACK_ACCEPT], 0);
        assert_eq!(r, Err(121));
        let (r, arch) = p.acknowledge("m1", 1, 1, &[ACK_REJECT], 0);
        assert!(r.is_ok());
        assert_eq!(arch, vec![(1, ArchiveReason::Rejected)]);
        // offset 0 still acquired so SPSO stays 0
        assert_eq!(p.start_offset, 0);
        let (r, _) = p.acknowledge("m1", 0, 0, &[ACK_ACCEPT], 0);
        assert!(r.is_ok());
        assert_eq!(p.start_offset, 2);
    }

    #[test]
    fn per_offset_types_and_lock_expiry_and_renew() {
        let mut p = sp();
        for o in 0..3 {
            p.acquire(o, "m", 0);
        }
        let (r, _) = p.acknowledge("m", 0, 2, &[ACK_ACCEPT, ACK_RELEASE, ACK_RENEW], 500);
        assert!(r.is_ok());
        assert_eq!(p.start_offset, 1);
        assert_eq!(p.get(2).unwrap().lock_deadline_ms, 1500);
        assert_eq!(p.expire_locks(999), vec![]);
        assert_eq!(p.get(2).unwrap().state, RecState::Acquired);
        p.expire_locks(1500);
        assert_eq!(p.get(2).unwrap().state, RecState::Available);
        assert_eq!(p.available(10), vec![1, 2]);
    }

    #[test]
    fn release_member_and_persistence() {
        let mut p = sp();
        p.acquire(0, "a", 0);
        p.acquire(1, "b", 0);
        p.release_member("a");
        assert_eq!(p.available(10), vec![0]);
        let text = p.encode();
        let q = SharePartition::decode(&text, 3, 1000, 100).unwrap();
        assert_eq!(q.start_offset, 0);
        assert_eq!(q.end_offset, 2);
        // acquired persisted as available, delivery counts kept
        assert_eq!(q.get(1).unwrap().state, RecState::Available);
        assert_eq!(q.get(1).unwrap().delivery_count, 1);
    }
}
