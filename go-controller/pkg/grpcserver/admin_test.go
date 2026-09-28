package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/consensus"
	pb "github.com/gradientgeeks/aerostream/go-controller/proto/aeromq"
)

func TestAdminService_RejectsNonLeader(t *testing.T) {
	rn := newUnstartedRaftNode(t)
	srv := NewServer(rn, time.Hour)
	ctx := context.Background()

	_, err := srv.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: "t"})
	assertUnavailableNonLeaderError(t, err)
	_, err = srv.CreatePartitions(ctx, &pb.CreatePartitionsRequest{Topic: "t", NewTotal: 3})
	assertUnavailableNonLeaderError(t, err)
	_, err = srv.AlterTopicConfigs(ctx, &pb.AlterTopicConfigsRequest{Topic: "t"})
	assertUnavailableNonLeaderError(t, err)
	_, err = srv.ElectLeaders(ctx, &pb.ElectLeadersRequest{Topic: "t"})
	assertUnavailableNonLeaderError(t, err)
}

func TestAdminCodeExtraction(t *testing.T) {
	if got := adminCode(&consensus.AdminError{Code: consensus.ErrTopicAlreadyExists, Msg: "x"}); got != 36 {
		t.Fatalf("got %d", got)
	}
	if got := adminCode(errors.New("boom")); got != -1 {
		t.Fatalf("got %d", got)
	}
	r := adminResp(&consensus.AdminError{Code: consensus.ErrInvalidPartitions, Msg: "bad"})
	if r.Success || r.ErrorCode != 37 || r.Message != "bad" {
		t.Fatalf("%+v", r)
	}
	if !adminResp(nil).Success {
		t.Fatal("nil error must be success")
	}
}
