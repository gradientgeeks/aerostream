package consensus

import (
	"net"
	"testing"
	"time"

	appconfig "github.com/gradientgeeks/aerostream/go-controller/pkg/config"
	"github.com/hashicorp/raft"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startNode(t *testing.T, dir, addr string, bootstrap bool) *RaftNode {
	t.Helper()
	cfg := appconfig.Default()
	cfg.NodeID = "node1"
	cfg.RaftAddr = addr
	cfg.DataDir = dir
	cfg.Bootstrap = bootstrap
	rn, err := NewRaftNode(cfg)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	return rn
}

func waitLeader(t *testing.T, rn *RaftNode) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if rn.Raft.State() == raft.Leader {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node never became leader (state=%s)", rn.Raft.State())
}

// A restarted node must recover its metadata from the durable Raft log + snapshots,
// even when it is (wrongly) started with bootstrap=true again.
func TestRaftStatePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)

	rn := startNode(t, dir, addr, true)
	waitLeader(t, rn)
	if err := rn.Propose(CmdRegisterBroker, struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{ID: 1, Host: "h1", Port: 9001}); err != nil {
		t.Fatalf("register broker: %v", err)
	}
	if err := rn.Propose(CmdCreateTopic, struct {
		Name              string `json:"name"`
		Partitions        uint32 `json:"partitions"`
		ReplicationFactor uint32 `json:"replication_factor"`
	}{Name: "orders", Partitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := rn.Shutdown(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	for i, bootstrap := range []bool{false, true} {
		rn = startNode(t, dir, addr, bootstrap)
		waitLeader(t, rn) // single voter recovered from its own persisted configuration
		topic, ok := rn.FSM.GetMetadata(nil).Topics["orders"]
		if !ok || len(topic.Partitions) != 3 {
			t.Fatalf("restart %d (bootstrap=%v): topic not recovered: %+v", i, bootstrap, topic)
		}
		if err := rn.Shutdown(); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}
}
