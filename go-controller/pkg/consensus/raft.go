package consensus

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	appconfig "github.com/gradientgeeks/aerostream/go-controller/pkg/config"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type RaftNode struct {
	Raft   *raft.Raft
	FSM    *FSM
	NodeID string

	closeStore func() error
}

// Shutdown stops Raft and closes the durable log store (if any).
func (rn *RaftNode) Shutdown() error {
	var err error
	if rn.Raft != nil {
		err = rn.Raft.Shutdown().Error()
	}
	if rn.closeStore != nil {
		if cerr := rn.closeStore(); err == nil {
			err = cerr
		}
	}
	return err
}

func NewRaftNode(cfg appconfig.ControllerConfig) (*RaftNode, error) {
	nodeID := cfg.NodeID
	raftAddr := cfg.RaftAddr
	dataDir := cfg.DataDir
	bootstrap := cfg.Bootstrap

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	// Apply configured timeouts.
	config.HeartbeatTimeout = cfg.Raft.HeartbeatTimeout()
	config.ElectionTimeout = cfg.Raft.ElectionTimeout()
	config.LeaderLeaseTimeout = cfg.Raft.LeaderLeaseTimeout()
	config.CommitTimeout = cfg.Raft.CommitTimeout()

	// Setup TCP transport
	bindAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft addr %s: %w", raftAddr, err)
	}
	advertiseAddr := bindAddr
	if bindAddr.IP == nil || bindAddr.IP.IsUnspecified() {
		advertiseAddr = &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: bindAddr.Port,
		}
	}

	transport, err := raft.NewTCPTransport(raftAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create tcp transport: %w", err)
	}

	fsm := NewFSM(cfg.Cluster.BrokerInactiveTimeout(), cfg.Cluster.ReplicaLagTolerance)

	// With a data dir the Raft log, current term and vote are durable (BoltDB) next to the snapshots,
	// so a restarted node keeps its state and a whole-quorum restart recovers (like KRaft's metadata log
	// on a PersistentVolume). Without one, everything is in memory.
	var (
		logStore      raft.LogStore
		stableStore   raft.StableStore
		snapshotStore raft.SnapshotStore
		closeStore    func() error
	)
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data dir: %w", err)
		}
		boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("failed to open raft log store: %w", err)
		}
		logStore, stableStore, closeStore = boltStore, boltStore, boltStore.Close
		snapshotStore, err = raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
		if err != nil {
			boltStore.Close()
			return nil, fmt.Errorf("failed to create file snapshot store: %w", err)
		}
	} else {
		mem := raft.NewInmemStore()
		logStore, stableStore = mem, mem
		snapshotStore = raft.NewDiscardSnapshotStore()
	}

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to construct raft node: %w", err)
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect raft state: %w", err)
	}
	if bootstrap && hasState {
		// Never re-bootstrap a node that already has durable state: it would fork a new cluster.
		fmt.Fprintf(os.Stderr, "[AeroMQ Controller] existing Raft state found in %s; skipping bootstrap\n", dataDir)
	}
	if bootstrap && !hasState {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return &RaftNode{
		Raft:       r,
		FSM:        fsm,
		NodeID:     nodeID,
		closeStore: closeStore,
	}, nil
}

// Propose applies a state change to the Raft consensus group.
// It will fail if this node is not currently the cluster leader.
func (rn *RaftNode) Propose(op string, payload interface{}) error {
	if rn.Raft == nil {
		if rn.FSM == nil {
			return fmt.Errorf("raft node not initialized")
		}
		rawPayload, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		cmd := Command{
			Op:      op,
			Payload: rawPayload,
		}
		data, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		if res := rn.FSM.Apply(&raft.Log{Data: data}); res != nil {
			if err, ok := res.(error); ok && err != nil {
				return err
			}
		}
		return nil
	}

	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader (current leader is %s)", rn.Raft.Leader())
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	cmd := Command{
		Op:      op,
		Payload: rawPayload,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rn.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply error: %w", err)
	}

	res := future.Response()
	if err, ok := res.(error); ok && err != nil {
		return err
	}

	return nil
}

// Join adds a new node to the cluster.
func (rn *RaftNode) Join(nodeID string, addr string) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("cannot join node: not the leader")
	}

	future := rn.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s (%s): %w", nodeID, addr, err)
	}

	return nil
}

// Leave removes a node from the cluster voter configuration.
func (rn *RaftNode) Leave(nodeID string) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("cannot leave node: not the leader")
	}

	future := rn.Raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove voter %s: %w", nodeID, err)
	}

	return nil
}
