package consensus

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func applyRaw(f *FSM, op string, payload interface{}) error {
	raw, _ := json.Marshal(payload)
	data, _ := json.Marshal(Command{Op: op, Payload: raw})
	if res := f.Apply(&raft.Log{Data: data}); res != nil {
		if err, ok := res.(error); ok {
			return err
		}
	}
	return nil
}

func regRack(t *testing.T, f *FSM, id uint32, rack string) {
	t.Helper()
	if err := applyRaw(f, CmdRegisterBroker, map[string]interface{}{
		"id": id, "host": "h", "port": 9000 + id, "rack": rack, "kafka_port": 9100 + id,
	}); err != nil {
		t.Fatal(err)
	}
}

func newRackFSM(t *testing.T) *FSM {
	f := NewFSM(time.Hour, 10)
	regRack(t, f, 1, "a")
	regRack(t, f, 2, "a")
	regRack(t, f, 3, "b")
	regRack(t, f, 4, "b")
	return f
}

func TestRackAwareTopicCreation(t *testing.T) {
	f := newRackFSM(t)
	if err := applyRaw(f, CmdCreateTopic, map[string]interface{}{"name": "t", "partitions": 6, "replication_factor": 2}); err != nil {
		t.Fatal(err)
	}
	md := f.GetMetadata(nil)
	if md.Brokers[3].Rack != "b" || md.Brokers[3].KafkaPort != 9103 {
		t.Fatalf("rack/kafka port not stored: %+v", md.Brokers[3])
	}
	for pid, p := range md.Topics["t"].Partitions {
		if len(p.ReplicaIDs) != 2 {
			t.Fatalf("p%d replicas %v", pid, p.ReplicaIDs)
		}
		r0, r1 := md.Brokers[p.ReplicaIDs[0]].Rack, md.Brokers[p.ReplicaIDs[1]].Rack
		if r0 == r1 {
			t.Fatalf("p%d replicas %v share rack %s", pid, p.ReplicaIDs, r0)
		}
		if p.LeaderID != p.ReplicaIDs[0] {
			t.Fatalf("leader must be first replica")
		}
	}
}

func TestCreateTopicDuplicateAndConfigs(t *testing.T) {
	f := newRackFSM(t)
	body := map[string]interface{}{"name": "t", "partitions": 1, "replication_factor": 1,
		"configs": map[string]string{"retention.ms": "1000"}, "fail_if_exists": true}
	if err := applyRaw(f, CmdCreateTopic, body); err != nil {
		t.Fatal(err)
	}
	err := applyRaw(f, CmdCreateTopic, body)
	var ae *AdminError
	if !errors.As(err, &ae) || ae.Code != ErrTopicAlreadyExists {
		t.Fatalf("want TOPIC_ALREADY_EXISTS, got %v", err)
	}
	if f.GetMetadata(nil).Topics["t"].Configs["retention.ms"] != "1000" {
		t.Fatal("config lost")
	}
}

func TestManualAssignment(t *testing.T) {
	f := newRackFSM(t)
	err := applyRaw(f, CmdCreateTopic, map[string]interface{}{"name": "m", "manual": map[string][]uint32{"0": {3, 1}, "1": {2}}})
	if err != nil {
		t.Fatal(err)
	}
	p := f.GetMetadata(nil).Topics["m"].Partitions
	if len(p) != 2 || p[0].LeaderID != 3 || len(p[1].ReplicaIDs) != 1 {
		t.Fatalf("%+v", p)
	}
	err = applyRaw(f, CmdCreateTopic, map[string]interface{}{"name": "bad", "manual": map[string][]uint32{"0": {99}}})
	var ae *AdminError
	if !errors.As(err, &ae) || ae.Code != ErrInvalidReplicaAssign {
		t.Fatalf("got %v", err)
	}
}

func TestCreatePartitionsDeleteAlterElect(t *testing.T) {
	f := newRackFSM(t)
	_ = applyRaw(f, CmdCreateTopic, map[string]interface{}{"name": "t", "partitions": 2, "replication_factor": 2})

	if err := applyRaw(f, CmdCreatePartitions, map[string]interface{}{"name": "t", "new_total": 5}); err != nil {
		t.Fatal(err)
	}
	if n := len(f.GetMetadata(nil).Topics["t"].Partitions); n != 5 {
		t.Fatalf("partitions=%d", n)
	}
	var ae *AdminError
	if err := applyRaw(f, CmdCreatePartitions, map[string]interface{}{"name": "t", "new_total": 3}); !errors.As(err, &ae) || ae.Code != ErrInvalidPartitions {
		t.Fatalf("shrink must fail: %v", err)
	}

	_ = applyRaw(f, CmdAlterTopicConfig, map[string]interface{}{"name": "t", "set": map[string]string{"a": "1", "b": "2"}})
	_ = applyRaw(f, CmdAlterTopicConfig, map[string]interface{}{"name": "t", "delete": []string{"a"}})
	cfg := f.GetMetadata(nil).Topics["t"].Configs
	if _, ok := cfg["a"]; ok || cfg["b"] != "2" {
		t.Fatalf("%v", cfg)
	}
	_ = applyRaw(f, CmdAlterTopicConfig, map[string]interface{}{"name": "t", "set": map[string]string{"c": "3"}, "replace_all": true})
	if cfg = f.GetMetadata(nil).Topics["t"].Configs; len(cfg) != 1 || cfg["c"] != "3" {
		t.Fatalf("%v", cfg)
	}

	// Elect: already preferred => ELECTION_NOT_NEEDED; move leader then elect preferred.
	if err := applyRaw(f, CmdElectLeader, map[string]interface{}{"topic": "t", "partition": 0}); !errors.As(err, &ae) || ae.Code != ErrElectionNotNeeded {
		t.Fatalf("got %v", err)
	}
	p0 := f.state.Topics["t"].Partitions[0]
	pref := p0.ReplicaIDs[0]
	p0.LeaderID = p0.ReplicaIDs[1]
	if err := applyRaw(f, CmdElectLeader, map[string]interface{}{"topic": "t", "partition": 0}); err != nil {
		t.Fatal(err)
	}
	if f.state.Topics["t"].Partitions[0].LeaderID != pref {
		t.Fatal("preferred leader not restored")
	}

	if err := applyRaw(f, CmdDeleteTopic, map[string]interface{}{"name": "t"}); err != nil {
		t.Fatal(err)
	}
	if err := applyRaw(f, CmdDeleteTopic, map[string]interface{}{"name": "t"}); !errors.As(err, &ae) || ae.Code != ErrUnknownTopicOrPartition {
		t.Fatalf("got %v", err)
	}
}
