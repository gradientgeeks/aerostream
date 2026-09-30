package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	appconfig "github.com/gradientgeeks/aerostream/go-controller/pkg/config"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/consensus"
	pb "github.com/gradientgeeks/aerostream/go-controller/proto/aeromq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// freeTCPAddr grabs an OS-assigned free port and returns it as an address
// string, closing the listener immediately so raft's transport can bind it.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close probe listener: %v", err)
	}
	return addr
}

// newUnstartedRaftNode creates an unbootstrapped RaftNode to test non-leader rejection.
func newUnstartedRaftNode(t *testing.T) *consensus.RaftNode {
	t.Helper()
	cfg := appconfig.Default()
	cfg.NodeID = "test-node"
	cfg.RaftAddr = freeTCPAddr(t)
	cfg.DataDir = ""
	cfg.Bootstrap = false

	rn, err := consensus.NewRaftNode(cfg)
	if err != nil {
		t.Fatalf("failed to construct raft node: %v", err)
	}
	t.Cleanup(func() {
		_ = rn.Raft.Shutdown().Error()
	})
	return rn
}

func assertUnavailableNonLeaderError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error from a non-leader node, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got: %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v (message: %s)", st.Code(), st.Message())
	}
}

func TestCreateTopic_RejectsNonLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	_, err := srv.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Topic:             "t1",
		Partitions:        1,
		ReplicationFactor: 1,
	})
	assertUnavailableNonLeaderError(t, err)
}

func TestRegisterBroker_RejectsNonLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	_, err := srv.RegisterBroker(context.Background(), &pb.RegisterBrokerRequest{
		BrokerId: 1,
		Host:     "localhost",
		DataPort: 9000,
	})
	assertUnavailableNonLeaderError(t, err)
}

func TestJoinGroup_RejectsNonLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	_, err := srv.JoinGroup(context.Background(), &pb.JoinGroupRequest{
		GroupId: "g1",
		Topics:  []string{"t1"},
	})
	assertUnavailableNonLeaderError(t, err)
}

func TestCommitOffsets_RejectsNonLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	_, err := srv.CommitOffsets(context.Background(), &pb.CommitOffsetsRequest{
		GroupId: "g1",
		Offsets: []*pb.TopicPartitionOffset{{Topic: "t1", Partition: 0, Offset: 5}},
	})
	assertUnavailableNonLeaderError(t, err)
}

// FetchOffsets must not answer "no committed offset" before a leader exists: the FSM is simply not loaded yet
// (right after a restart), and an empty answer makes consumers reset and skip or reprocess data.
func TestFetchOffsets_UnavailableUntilLeaderKnown(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	_, err := srv.FetchOffsets(context.Background(), &pb.FetchOffsetsRequest{GroupId: "g1", Topics: []string{"t1"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while no leader is elected, got: %v", err)
	}
}

// GetMetadata deliberately serves from local FSM state without requiring leadership, so a non-leader node
// should still answer (with empty results, since nothing has been proposed yet).
func TestGetMetadata_DoesNotRequireLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)

	resp, err := srv.GetMetadata(context.Background(), &pb.MetadataRequest{})
	if err != nil {
		t.Fatalf("expected GetMetadata to succeed without leadership, got: %v", err)
	}
	if len(resp.Brokers) != 0 || len(resp.Topics) != 0 {
		t.Fatalf("expected empty metadata on a fresh cluster state, got brokers=%v topics=%v", resp.Brokers, resp.Topics)
	}
}
