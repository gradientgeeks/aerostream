package consensus

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// applyCmd is a small helper that marshals op/payload into a raft.Log the
// same way RaftNode.Propose would, and feeds it to the FSM synchronously.
func applyCmd(t *testing.T, f *FSM, op string, payload interface{}) {
	t.Helper()

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload for %s: %v", op, err)
	}

	cmd := Command{Op: op, Payload: rawPayload}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command %s: %v", op, err)
	}

	if res := f.Apply(&raft.Log{Data: data}); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("apply %s returned error: %v", op, err)
		}
	}
}

func registerBroker(t *testing.T, f *FSM, id uint32, host string, port int32) {
	t.Helper()
	applyCmd(t, f, CmdRegisterBroker, struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{ID: id, Host: host, Port: port})
}

func createTopic(t *testing.T, f *FSM, name string, partitions, replicationFactor uint32) {
	t.Helper()
	applyCmd(t, f, CmdCreateTopic, struct {
		Name              string `json:"name"`
		Partitions        uint32 `json:"partitions"`
		ReplicationFactor uint32 `json:"replication_factor"`
	}{Name: name, Partitions: partitions, ReplicationFactor: replicationFactor})
}

type heartbeatReplicaOffset struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
	Offset    int64  `json:"offset"`
}

func heartbeat(t *testing.T, f *FSM, brokerID uint32, offsets ...heartbeatReplicaOffset) {
	t.Helper()
	applyCmd(t, f, CmdBrokerHeartbeat, struct {
		ID             uint32                   `json:"id"`
		ReplicaOffsets []heartbeatReplicaOffset `json:"replica_offsets"`
	}{ID: brokerID, ReplicaOffsets: offsets})
}

func joinConsumerGroup(t *testing.T, f *FSM, groupID, memberID string, topics []string) {
	t.Helper()
	applyCmd(t, f, CmdJoinConsumerGroup, struct {
		GroupID  string   `json:"group_id"`
		MemberID string   `json:"member_id"`
		Topics   []string `json:"topics"`
	}{GroupID: groupID, MemberID: memberID, Topics: topics})
}

func commitOffset(t *testing.T, f *FSM, groupID, topic string, partition uint32, offset int64) {
	t.Helper()
	applyCmd(t, f, CmdCommitOffset, struct {
		GroupID   string `json:"group_id"`
		Topic     string `json:"topic"`
		Partition uint32 `json:"partition"`
		Offset    int64  `json:"offset"`
	}{GroupID: groupID, Topic: topic, Partition: partition, Offset: offset})
}

func cleanInactive(t *testing.T, f *FSM) {
	t.Helper()
	applyCmd(t, f, CmdCleanInactive, struct{}{})
}

// ---------------------------------------------------------------------
// CreateTopic
// ---------------------------------------------------------------------

func TestCreateTopic_RoundRobinAssignment(t *testing.T) {
	f := NewFSM(time.Minute, 10)

	registerBroker(t, f, 1, "h1", 9001)
	registerBroker(t, f, 2, "h2", 9002)
	registerBroker(t, f, 3, "h3", 9003)

	const numPartitions = 4
	const replicationFactor = 2
	createTopic(t, f, "orders", numPartitions, replicationFactor)

	state := f.GetMetadata(nil)
	topic, ok := state.Topics["orders"]
	if !ok {
		t.Fatalf("expected topic 'orders' to exist")
	}
	if len(topic.Partitions) != numPartitions {
		t.Fatalf("expected %d partitions, got %d", numPartitions, len(topic.Partitions))
	}

	// The leader assignment cycles round-robin over the (map-order,
	// non-deterministic) list of active brokers. Since there are 3
	// brokers and 4 partitions, partition 0 and partition 3 must land
	// on the same leader (index 0 mod 3), and partitions 0,1,2 must
	// cover all three distinct brokers exactly once each.
	leaders := make(map[uint32]bool)
	for i := uint32(0); i < 3; i++ {
		p, ok := topic.Partitions[i]
		if !ok {
			t.Fatalf("missing partition %d", i)
		}
		if p.LeaderID == 0 {
			t.Fatalf("partition %d has no leader assigned", i)
		}
		leaders[p.LeaderID] = true
	}
	if len(leaders) != 3 {
		t.Fatalf("expected leaders to be spread across all 3 brokers, got %d distinct leaders: %v", len(leaders), leaders)
	}
	if topic.Partitions[0].LeaderID != topic.Partitions[3].LeaderID {
		t.Fatalf("expected partition 0 and 3 to share the same leader (round robin wraparound), got %d vs %d",
			topic.Partitions[0].LeaderID, topic.Partitions[3].LeaderID)
	}

	// Every partition should have exactly min(RF, numBrokers) replicas,
	// the first of which is always the leader itself.
	for i := uint32(0); i < numPartitions; i++ {
		p := topic.Partitions[i]
		if len(p.ReplicaIDs) != replicationFactor {
			t.Errorf("partition %d: expected %d replicas, got %d (%v)", i, replicationFactor, len(p.ReplicaIDs), p.ReplicaIDs)
		}
		if len(p.ReplicaIDs) == 0 || p.ReplicaIDs[0] != p.LeaderID {
			t.Errorf("partition %d: expected first replica to be the leader %d, got replicas %v", i, p.LeaderID, p.ReplicaIDs)
		}
		if len(p.ISR) != len(p.ReplicaIDs) {
			t.Errorf("partition %d: expected initial ISR to equal replica set, got ISR=%v replicas=%v", i, p.ISR, p.ReplicaIDs)
		}
	}

	// Creating the same topic again must be a no-op (idempotent guard).
	createTopic(t, f, "orders", 99, 99)
	state2 := f.GetMetadata(nil)
	if len(state2.Topics["orders"].Partitions) != numPartitions {
		t.Fatalf("expected CreateTopic on existing topic to be a no-op, partition count changed to %d", len(state2.Topics["orders"].Partitions))
	}
}

func TestCreateTopic_NoActiveBrokers(t *testing.T) {
	f := NewFSM(time.Minute, 10)
	createTopic(t, f, "orphan", 2, 2)

	state := f.GetMetadata(nil)
	topic, ok := state.Topics["orphan"]
	if !ok {
		t.Fatalf("expected topic to be created even with no brokers")
	}
	for i := uint32(0); i < 2; i++ {
		p := topic.Partitions[i]
		if p.LeaderID != 0 {
			t.Errorf("expected leaderID 0 with no active brokers, got %d", p.LeaderID)
		}
		if len(p.ReplicaIDs) != 0 {
			t.Errorf("expected no replicas with no active brokers, got %v", p.ReplicaIDs)
		}
	}
}

// ---------------------------------------------------------------------
// BrokerHeartbeat / ISR / High Watermark
// ---------------------------------------------------------------------

func TestBrokerHeartbeat_UpdatesISRAndHighWatermark(t *testing.T) {
	// lag tolerance of 20: a replica within 20 offsets of the leader stays in the ISR.
	f := NewFSM(time.Minute, 20)

	registerBroker(t, f, 1, "h1", 9001)
	registerBroker(t, f, 2, "h2", 9002)
	createTopic(t, f, "t1", 1, 2)

	state := f.GetMetadata(nil)
	p := state.Topics["t1"].Partitions[0]
	leaderID := p.LeaderID
	var followerID uint32
	for _, r := range p.ReplicaIDs {
		if r != leaderID {
			followerID = r
		}
	}
	if followerID == 0 {
		t.Fatalf("expected a follower replica distinct from leader %d, got replicas %v", leaderID, p.ReplicaIDs)
	}

	// Leader at 100, follower lagging by 15 (within tolerance of 20) -> both in ISR, HW = min(100,85) = 85.
	heartbeat(t, f, leaderID, heartbeatReplicaOffset{Topic: "t1", Partition: 0, Offset: 100})
	heartbeat(t, f, followerID, heartbeatReplicaOffset{Topic: "t1", Partition: 0, Offset: 85})

	state = f.GetMetadata(nil)
	p = state.Topics["t1"].Partitions[0]
	if len(p.ISR) != 2 {
		t.Fatalf("expected both replicas in ISR, got %v", p.ISR)
	}
	if p.HighWatermark != 85 {
		t.Fatalf("expected high watermark 85 (min offset among ISR), got %d", p.HighWatermark)
	}

	// Follower falls further behind (lag 30 > tolerance 20) -> dropped from ISR, HW tracks leader alone.
	heartbeat(t, f, followerID, heartbeatReplicaOffset{Topic: "t1", Partition: 0, Offset: 70})

	state = f.GetMetadata(nil)
	p = state.Topics["t1"].Partitions[0]
	if len(p.ISR) != 1 || p.ISR[0] != leaderID {
		t.Fatalf("expected ISR to contain only the leader %d, got %v", leaderID, p.ISR)
	}
	if p.HighWatermark != 100 {
		t.Fatalf("expected high watermark to track leader offset 100, got %d", p.HighWatermark)
	}

	// Sanity: replica offsets are recorded per broker.
	if got := p.ReplicaOffsets[leaderID]; got != 100 {
		t.Errorf("expected leader replica offset 100, got %d", got)
	}
	if got := p.ReplicaOffsets[followerID]; got != 70 {
		t.Errorf("expected follower replica offset 70, got %d", got)
	}
}

// ---------------------------------------------------------------------
// JoinConsumerGroup / rebalance
// ---------------------------------------------------------------------

func TestJoinConsumerGroup_RebalancesAssignments(t *testing.T) {
	f := NewFSM(time.Minute, 10)
	registerBroker(t, f, 1, "h1", 9001)
	createTopic(t, f, "orders", 4, 1)

	joinConsumerGroup(t, f, "g1", "m1", []string{"orders"})

	state := f.GetMetadata(nil)
	group, ok := state.ConsumerGroups["g1"]
	if !ok {
		t.Fatalf("expected consumer group 'g1' to exist")
	}
	if group.Generation != 1 {
		t.Fatalf("expected generation 1 after first join, got %d", group.Generation)
	}
	if len(group.Assignments["m1"]) != 4 {
		t.Fatalf("expected sole member to receive all 4 partitions, got %d: %v", len(group.Assignments["m1"]), group.Assignments["m1"])
	}

	// A second member joins and subscribes to the same topic -> rebalance splits partitions.
	joinConsumerGroup(t, f, "g1", "m2", []string{"orders"})

	state = f.GetMetadata(nil)
	group = state.ConsumerGroups["g1"]
	if group.Generation != 2 {
		t.Fatalf("expected generation 2 after second join (rebalance), got %d", group.Generation)
	}

	total := len(group.Assignments["m1"]) + len(group.Assignments["m2"])
	if total != 4 {
		t.Fatalf("expected all 4 partitions assigned across both members, got %d", total)
	}
	if len(group.Assignments["m1"]) == 0 || len(group.Assignments["m2"]) == 0 {
		t.Fatalf("expected both members to receive at least one partition, got m1=%d m2=%d",
			len(group.Assignments["m1"]), len(group.Assignments["m2"]))
	}

	// No duplicate/missing partitions across the assignment.
	seen := make(map[uint32]bool)
	for _, member := range []string{"m1", "m2"} {
		for _, pt := range group.Assignments[member] {
			if pt.Topic != "orders" {
				t.Errorf("unexpected topic in assignment: %s", pt.Topic)
			}
			if seen[pt.Partition] {
				t.Errorf("partition %d assigned to more than one member", pt.Partition)
			}
			seen[pt.Partition] = true
		}
	}
	for i := uint32(0); i < 4; i++ {
		if !seen[i] {
			t.Errorf("partition %d was never assigned", i)
		}
	}
}

// ---------------------------------------------------------------------
// CommitOffset / GetOffset
// ---------------------------------------------------------------------

func TestCommitAndGetOffset_RoundTrip(t *testing.T) {
	f := NewFSM(time.Minute, 10)

	if got := f.GetOffset("g1", "topicA", 2); got != -1 {
		t.Fatalf("expected -1 for unknown offset, got %d", got)
	}

	commitOffset(t, f, "g1", "topicA", 2, 12345)
	if got := f.GetOffset("g1", "topicA", 2); got != 12345 {
		t.Fatalf("expected committed offset 12345, got %d", got)
	}

	// Distinct group/topic/partition combos must not collide.
	commitOffset(t, f, "g2", "topicA", 2, 999)
	commitOffset(t, f, "g1", "topicB", 2, 111)
	commitOffset(t, f, "g1", "topicA", 3, 222)

	if got := f.GetOffset("g1", "topicA", 2); got != 12345 {
		t.Fatalf("expected original offset 12345 to remain unaffected, got %d", got)
	}
	if got := f.GetOffset("g2", "topicA", 2); got != 999 {
		t.Fatalf("expected g2/topicA/2 = 999, got %d", got)
	}
	if got := f.GetOffset("g1", "topicB", 2); got != 111 {
		t.Fatalf("expected g1/topicB/2 = 111, got %d", got)
	}
	if got := f.GetOffset("g1", "topicA", 3); got != 222 {
		t.Fatalf("expected g1/topicA/3 = 222, got %d", got)
	}

	// Re-committing updates the value in place.
	commitOffset(t, f, "g1", "topicA", 2, 55555)
	if got := f.GetOffset("g1", "topicA", 2); got != 55555 {
		t.Fatalf("expected updated offset 55555, got %d", got)
	}
}

// ---------------------------------------------------------------------
// CleanInactive / leader failover
// ---------------------------------------------------------------------

func TestCleanInactive_MarksBrokerInactiveAndFailsOverLeader(t *testing.T) {
	const inactiveTimeout = 100 * time.Millisecond
	f := NewFSM(inactiveTimeout, 10)

	registerBroker(t, f, 1, "h1", 9001)
	registerBroker(t, f, 2, "h2", 9002)
	createTopic(t, f, "t1", 1, 2)

	state := f.GetMetadata(nil)
	p := state.Topics["t1"].Partitions[0]
	leaderID := p.LeaderID
	var followerID uint32
	for _, r := range p.ReplicaIDs {
		if r != leaderID {
			followerID = r
		}
	}
	if followerID == 0 {
		t.Fatalf("expected a follower replica, got replicas %v", p.ReplicaIDs)
	}

	// Let both brokers age a bit, then refresh only the follower so that,
	// after another wait, the leader is stale (> inactiveTimeout) while the
	// follower remains fresh. This makes which broker goes inactive
	// deterministic (independent of Go's randomized map iteration order).
	time.Sleep(inactiveTimeout * 3 / 5)
	heartbeat(t, f, followerID)
	time.Sleep(inactiveTimeout * 3 / 5)

	cleanInactive(t, f)

	state = f.GetMetadata(nil)
	if state.Brokers[leaderID].Active {
		t.Fatalf("expected leader broker %d to be marked inactive", leaderID)
	}
	if !state.Brokers[followerID].Active {
		t.Fatalf("expected follower broker %d to remain active", followerID)
	}

	newP := state.Topics["t1"].Partitions[0]
	if newP.LeaderID != followerID {
		t.Fatalf("expected leader failover to live replica %d, got leader %d", followerID, newP.LeaderID)
	}
}

func TestCleanInactive_NoFailoverWhenNoActiveBrokersRemain(t *testing.T) {
	const inactiveTimeout = 30 * time.Millisecond
	f := NewFSM(inactiveTimeout, 10)

	registerBroker(t, f, 1, "h1", 9001)
	createTopic(t, f, "t1", 1, 1)

	state := f.GetMetadata(nil)
	leaderID := state.Topics["t1"].Partitions[0].LeaderID

	time.Sleep(inactiveTimeout * 2)
	cleanInactive(t, f)

	state = f.GetMetadata(nil)
	if state.Brokers[leaderID].Active {
		t.Fatalf("expected the sole broker to be marked inactive")
	}
	// No live broker to fail over to: leader assignment is left unchanged.
	if state.Topics["t1"].Partitions[0].LeaderID != leaderID {
		t.Fatalf("expected leader to remain %d when no active broker is available for failover, got %d",
			leaderID, state.Topics["t1"].Partitions[0].LeaderID)
	}
}

// ---------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------

// fakeSnapshotSink is a minimal in-memory implementation of raft.SnapshotSink
// sufficient for exercising FSMSnapshot.Persist in tests.
type fakeSnapshotSink struct {
	buf bytes.Buffer
}

func (s *fakeSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *fakeSnapshotSink) Close() error                { return nil }
func (s *fakeSnapshotSink) ID() string                  { return "test-snapshot" }
func (s *fakeSnapshotSink) Cancel() error               { return nil }

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	f := NewFSM(time.Minute, 10)

	registerBroker(t, f, 1, "h1", 9001)
	registerBroker(t, f, 2, "h2", 9002)
	createTopic(t, f, "t1", 2, 2)
	heartbeat(t, f, 1, heartbeatReplicaOffset{Topic: "t1", Partition: 0, Offset: 50})
	joinConsumerGroup(t, f, "g1", "m1", []string{"t1"})
	commitOffset(t, f, "g1", "t1", 0, 42)

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}

	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist() failed: %v", err)
	}

	f2 := NewFSM(time.Minute, 10)
	if err := f2.Restore(nopCloser{bytes.NewReader(sink.buf.Bytes())}); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	// Compare via JSON: encoding/json marshals map keys deterministically
	// (sorted) and normalizes time.Time to its wall-clock representation,
	// so this sidesteps map-iteration-order and monotonic-clock-reading
	// differences between the two in-memory states.
	origJSON, err := json.Marshal(f.GetMetadata(nil))
	if err != nil {
		t.Fatalf("marshal original state: %v", err)
	}
	restoredJSON, err := json.Marshal(f2.GetMetadata(nil))
	if err != nil {
		t.Fatalf("marshal restored state: %v", err)
	}
	if string(origJSON) != string(restoredJSON) {
		t.Fatalf("restored state does not match original.\noriginal: %s\nrestored: %s", origJSON, restoredJSON)
	}

	// Spot-check a couple of fields directly for good measure.
	if got := f2.GetOffset("g1", "t1", 0); got != 42 {
		t.Errorf("expected restored offset 42, got %d", got)
	}
	restoredState := f2.GetMetadata(nil)
	if len(restoredState.Topics["t1"].Partitions) != 2 {
		t.Errorf("expected restored topic to have 2 partitions, got %d", len(restoredState.Topics["t1"].Partitions))
	}
	if len(restoredState.ConsumerGroups["g1"].Members) != 1 {
		t.Errorf("expected restored consumer group to have 1 member")
	}
}

// nopCloser adapts an io.Reader to io.ReadCloser for Restore().
type nopCloser struct{ r *bytes.Reader }

func (n nopCloser) Read(p []byte) (int, error) { return n.r.Read(p) }
func (n nopCloser) Close() error               { return nil }
