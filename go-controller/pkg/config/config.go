package config

import (
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
)

// ControllerConfig holds all tunables for an AeroMQ controller node.
// Values are loaded from a TOML file and then selectively overridden by
// explicit CLI flags in cmd/controller/main.go.
type ControllerConfig struct {
	NodeID    string `toml:"node_id"`
	RaftAddr  string `toml:"raft_addr"`
	GRPCAddr  string `toml:"grpc_addr"`
	HTTPAddr  string `toml:"http_addr"`
	DataDir   string `toml:"data_dir"`
	Bootstrap bool   `toml:"bootstrap"`
	Join      string `toml:"join"`

	Raft    RaftConfig    `toml:"raft"`
	Cluster ClusterConfig `toml:"cluster"`
	TLS     TLSConfig     `toml:"tls"`
	Auth    AuthConfig    `toml:"auth"`
}

// RaftConfig holds the hashicorp/raft timing parameters (milliseconds).
type RaftConfig struct {
	HeartbeatTimeoutMs   int `toml:"heartbeat_timeout_ms"`
	ElectionTimeoutMs    int `toml:"election_timeout_ms"`
	LeaderLeaseTimeoutMs int `toml:"leader_lease_timeout_ms"`
	CommitTimeoutMs      int `toml:"commit_timeout_ms"`
}

// ClusterConfig holds broker failure-detection and ISR tunables.
type ClusterConfig struct {
	// How often the leader runs the inactive-broker cleanup sweep.
	FailureDetectionIntervalMs int `toml:"failure_detection_interval_ms"`
	// A broker is marked inactive if no heartbeat is seen within this window.
	BrokerInactiveTimeoutMs int `toml:"broker_inactive_timeout_ms"`
	// Max offset lag for a replica to remain in the ISR.
	ReplicaLagTolerance int64 `toml:"replica_lag_tolerance"`
}

// TLSConfig configures TLS for the gRPC control plane.
type TLSConfig struct {
	Enabled  bool   `toml:"enabled"`
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`
	// CAFile, when set, enables mTLS: clients must present a cert signed by this CA.
	CAFile string `toml:"ca_file"`
}

// AuthConfig configures the shared bearer token required on gRPC calls.
type AuthConfig struct {
	// Token, when non-empty, is required in the "authorization" metadata of
	// every gRPC request. Empty disables authentication.
	Token string `toml:"token"`
}

// Default returns a configuration with production-sane defaults.
func Default() ControllerConfig {
	return ControllerConfig{
		NodeID:   "node1",
		RaftAddr: "127.0.0.1:7001",
		GRPCAddr: "127.0.0.1:8001",
		HTTPAddr: "127.0.0.1:9001",
		Raft: RaftConfig{
			HeartbeatTimeoutMs:   1000,
			ElectionTimeoutMs:    1000,
			LeaderLeaseTimeoutMs: 500,
			CommitTimeoutMs:      50,
		},
		Cluster: ClusterConfig{
			FailureDetectionIntervalMs: 3000,
			BrokerInactiveTimeoutMs:    8000,
			ReplicaLagTolerance:        10,
		},
	}
}

// Load reads configuration from a TOML file, starting from Default() so any
// keys omitted in the file keep their default values. An empty path returns
// the defaults unchanged.
func Load(path string) (ControllerConfig, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, fmt.Errorf("failed to load config file %q: %w", path, err)
	}
	return cfg, nil
}

// Helper accessors converting millisecond fields to time.Duration.

func (r RaftConfig) HeartbeatTimeout() time.Duration {
	return time.Duration(r.HeartbeatTimeoutMs) * time.Millisecond
}
func (r RaftConfig) ElectionTimeout() time.Duration {
	return time.Duration(r.ElectionTimeoutMs) * time.Millisecond
}
func (r RaftConfig) LeaderLeaseTimeout() time.Duration {
	return time.Duration(r.LeaderLeaseTimeoutMs) * time.Millisecond
}
func (r RaftConfig) CommitTimeout() time.Duration {
	return time.Duration(r.CommitTimeoutMs) * time.Millisecond
}

func (c ClusterConfig) FailureDetectionInterval() time.Duration {
	return time.Duration(c.FailureDetectionIntervalMs) * time.Millisecond
}
func (c ClusterConfig) BrokerInactiveTimeout() time.Duration {
	return time.Duration(c.BrokerInactiveTimeoutMs) * time.Millisecond
}
