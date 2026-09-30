use std::collections::HashMap;

/// State for a specific producer ID and epoch on a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerEpochState {
    pub epoch: i16,
    pub last_sequence: i32,
    pub last_offset: u64,
    pub last_timestamp: i64,
}

impl ProducerEpochState {
    pub fn new(epoch: i16, last_sequence: i32, last_offset: u64, last_timestamp: i64) -> Self {
        Self {
            epoch,
            last_sequence,
            last_offset,
            last_timestamp,
        }
    }
}

/// Result of checking a sequence number for an idempotent producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceCheckResult {
    /// Valid next sequence. Tracker updated; proceed with write.
    ValidNext,
    /// Duplicate sequence (network retry). Bypass log append; return cached last_offset.
    Duplicate { last_offset: u64 },
    /// Out-of-order sequence. Reject with error code 45 (OutOfOrderSequenceNumber).
    OutOfOrder { error_code: i16 },
}

impl SequenceCheckResult {
    pub fn is_valid_next(&self) -> bool {
        matches!(self, Self::ValidNext)
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate { .. })
    }

    pub fn is_out_of_order(&self) -> bool {
        matches!(self, Self::OutOfOrder { .. })
    }

    pub fn duplicate_offset(&self) -> Option<u64> {
        match self {
            Self::Duplicate { last_offset } => Some(*last_offset),
            _ => None,
        }
    }

    pub fn error_code(&self) -> Option<i16> {
        match self {
            Self::OutOfOrder { error_code } => Some(*error_code),
            _ => None,
        }
    }
}

/// Tracks idempotent producer sequence numbers per active partition head.
#[derive(Debug, Clone, Default)]
pub struct ProducerStateTracker {
    pub producers: HashMap<i64, ProducerEpochState>,
    pub sequence_offsets: HashMap<(i64, i32), u64>,
}

impl ProducerStateTracker {
    pub fn new() -> Self {
        Self {
            producers: HashMap::new(),
            sequence_offsets: HashMap::new(),
        }
    }

    pub fn get_producer_state(&self, producer_id: i64) -> Option<&ProducerEpochState> {
        self.producers.get(&producer_id)
    }

    /// Validates sequence number against tracker state:
    /// returns ValidNext for expected sequence, Duplicate for retry, or OutOfOrder.
    pub fn check_and_update_sequence(
        &mut self,
        producer_id: i64,
        epoch: i16,
        base_sequence: i32,
        record_count: i32,
        offset: u64,
    ) -> SequenceCheckResult {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let count = record_count.max(1);
        let last_seq = base_sequence + count - 1;

        match self.producers.get_mut(&producer_id) {
            None => {
                // First sequence from a new PID.
                // Standard Kafka producer starts at sequence 0.
                if base_sequence == 0 {
                    self.producers.insert(
                        producer_id,
                        ProducerEpochState {
                            epoch,
                            last_sequence: last_seq,
                            last_offset: offset,
                            last_timestamp: now,
                        },
                    );
                    for s in base_sequence..=last_seq {
                        self.sequence_offsets.insert((producer_id, s), offset);
                    }
                    SequenceCheckResult::ValidNext
                } else {
                    SequenceCheckResult::OutOfOrder { error_code: 45 }
                }
            }
            Some(state) => {
                if epoch > state.epoch {
                    // Epoch bumped: sequences reset starting from 0.
                    if base_sequence == 0 {
                        state.epoch = epoch;
                        state.last_sequence = last_seq;
                        state.last_offset = offset;
                        state.last_timestamp = now;
                        self.sequence_offsets.retain(|&(p, _), _| p != producer_id);
                        for s in base_sequence..=last_seq {
                            self.sequence_offsets.insert((producer_id, s), offset);
                        }
                        SequenceCheckResult::ValidNext
                    } else {
                        SequenceCheckResult::OutOfOrder { error_code: 45 }
                    }
                } else if epoch < state.epoch {
                    // Stale/fenced epoch
                    let cached = self
                        .sequence_offsets
                        .get(&(producer_id, base_sequence))
                        .copied()
                        .unwrap_or(state.last_offset);
                    SequenceCheckResult::Duplicate { last_offset: cached }
                } else if base_sequence == state.last_sequence + 1 {
                    // Valid next sequential batch
                    state.last_sequence = last_seq;
                    state.last_offset = offset;
                    state.last_timestamp = now;
                    for s in base_sequence..=last_seq {
                        self.sequence_offsets.insert((producer_id, s), offset);
                    }
                    SequenceCheckResult::ValidNext
                } else if base_sequence <= state.last_sequence {
                    // Duplicate / network retry
                    let cached = self
                        .sequence_offsets
                        .get(&(producer_id, base_sequence))
                        .copied()
                        .unwrap_or(state.last_offset);
                    SequenceCheckResult::Duplicate { last_offset: cached }
                } else {
                    // base_sequence > state.last_sequence + 1
                    SequenceCheckResult::OutOfOrder { error_code: 45 }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_progression_0_1_2_accepted() {
        let mut tracker = ProducerStateTracker::new();
        let pid = 1000i64;
        let epoch = 0i16;

        // Sequence 0
        let res0 = tracker.check_and_update_sequence(pid, epoch, 0, 1, 0);
        assert_eq!(res0, SequenceCheckResult::ValidNext);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_sequence, 0);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_offset, 0);

        // Sequence 1
        let res1 = tracker.check_and_update_sequence(pid, epoch, 1, 1, 1);
        assert_eq!(res1, SequenceCheckResult::ValidNext);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_sequence, 1);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_offset, 1);

        // Sequence 2
        let res2 = tracker.check_and_update_sequence(pid, epoch, 2, 1, 2);
        assert_eq!(res2, SequenceCheckResult::ValidNext);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_sequence, 2);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_offset, 2);
    }

    #[test]
    fn test_duplicate_sequence_returns_cached_offset() {
        let mut tracker = ProducerStateTracker::new();
        let pid = 1000i64;
        let epoch = 0i16;

        // Produce 0 and 1
        assert_eq!(
            tracker.check_and_update_sequence(pid, epoch, 0, 1, 10),
            SequenceCheckResult::ValidNext
        );
        assert_eq!(
            tracker.check_and_update_sequence(pid, epoch, 1, 1, 11),
            SequenceCheckResult::ValidNext
        );

        // Retrying sequence 1 with a hypothetical new offset 12
        let retry_res = tracker.check_and_update_sequence(pid, epoch, 1, 1, 12);
        assert_eq!(
            retry_res,
            SequenceCheckResult::Duplicate { last_offset: 11 }
        );
        // Tracker state remains at sequence 1, offset 11
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_sequence, 1);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_offset, 11);
    }

    #[test]
    fn test_out_of_order_sequence_after_sequence_1() {
        let mut tracker = ProducerStateTracker::new();
        let pid = 1000i64;
        let epoch = 0i16;

        // Produce 0 and 1
        assert_eq!(
            tracker.check_and_update_sequence(pid, epoch, 0, 1, 0),
            SequenceCheckResult::ValidNext
        );
        assert_eq!(
            tracker.check_and_update_sequence(pid, epoch, 1, 1, 1),
            SequenceCheckResult::ValidNext
        );

        // Sequence 5 after sequence 1
        let ooo_res = tracker.check_and_update_sequence(pid, epoch, 5, 1, 2);
        assert_eq!(
            ooo_res,
            SequenceCheckResult::OutOfOrder { error_code: 45 }
        );
        // State remains at sequence 1
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_sequence, 1);
        assert_eq!(tracker.get_producer_state(pid).unwrap().last_offset, 1);
    }

    #[test]
    fn test_new_pid_out_of_order_start() {
        let mut tracker = ProducerStateTracker::new();
        let pid = 2000i64;
        let epoch = 0i16;

        // Starting with sequence > 0 on new PID
        let res = tracker.check_and_update_sequence(pid, epoch, 3, 1, 0);
        assert_eq!(res, SequenceCheckResult::OutOfOrder { error_code: 45 });
        assert!(tracker.get_producer_state(pid).is_none());
    }
}
