package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/placement"
	"github.com/hashicorp/raft"
)

// PartitionTopic represents a topic-partition pair
type PartitionTopic struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
}

// BrokerStatus tracks active brokers
type BrokerStatus struct {
	ID       uint32    `json:"id"`
	Host     string    `json:"host"`
	Port     int32     `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	Active   bool      `json:"active"`
	// Rack is the broker.rack label (empty = none). KafkaPort is the Kafka wire-protocol port.
	Rack      string `json:"rack,omitempty"`
	KafkaPort int32  `json:"kafka_port,omitempty"`
}

// PartitionState tracks partition leader and replicas
type PartitionState struct {
	PartitionID    uint32           `json:"partition_id"`
	LeaderID       uint32           `json:"leader_id"`
	ReplicaIDs     []uint32         `json:"replica_ids"`
	ISR            []uint32         `json:"isr"`
	HighWatermark  int64            `json:"high_watermark"`
	ReplicaOffsets map[uint32]int64 `json:"replica_offsets"` // key: broker_id -> offset
}

// TopicState tracks topic metadata
type TopicState struct {
	Name       string                     `json:"name"`
	Partitions map[uint32]*PartitionState `json:"partitions"`
	// Configs holds per-topic config overrides (Kafka DescribeConfigs/AlterConfigs).
	Configs map[string]string `json:"configs,omitempty"`
}

// GroupMember tracks a consumer in a consumer group
type GroupMember struct {
	ID                 string           `json:"id"`
	Topics             []string         `json:"topics"`
	LastSeen           time.Time        `json:"last_seen"`
	ClientHost         string           `json:"client_host,omitempty"`
	UserAgent          string           `json:"user_agent,omitempty"`
	AssignedPartitions []PartitionTopic `json:"assigned_partitions"`
	RevokingPartitions []PartitionTopic `json:"revoking_partitions"`
}

// ConsumerGroupState tracks consumer group state
type ConsumerGroupState struct {
	GroupID           string                      `json:"group_id"`
	Protocol          string                      `json:"protocol"`
	State             string                      `json:"state"`
	Generation        uint32                      `json:"generation"`
	LeaderID          string                      `json:"leader_id"`
	RebalanceCount    int64                       `json:"rebalance_count"`
	LastRebalanceTime time.Time                   `json:"last_rebalance_time"`
	Members           map[string]*GroupMember     `json:"members"`
	Assignments       map[string][]PartitionTopic `json:"assignments"` // key: member_id
}

// ClusterState is the state replicated by Raft
type ClusterState struct {
	Brokers        map[uint32]*BrokerStatus       `json:"brokers"`
	Topics         map[string]*TopicState         `json:"topics"`
	ConsumerGroups map[string]*ConsumerGroupState `json:"consumer_groups"`
	Offsets        map[string]int64               `json:"offsets"` // key: "group_id/topic/partition" (see offsetKey)
}

// FSM implements raft.FSM
type FSM struct {
	mu    sync.RWMutex
	state ClusterState

	// Cluster tunables (sourced from config).
	brokerInactiveTimeout time.Duration
	replicaLagTolerance   int64
}

// NewFSM constructs the cluster state machine. brokerInactiveTimeout is the
// window after which a silent broker is marked inactive; replicaLagTolerance
// is the max offset lag for a replica to stay in the ISR.
func NewFSM(brokerInactiveTimeout time.Duration, replicaLagTolerance int64) *FSM {
	return &FSM{
		state: ClusterState{
			Brokers:        make(map[uint32]*BrokerStatus),
			Topics:         make(map[string]*TopicState),
			ConsumerGroups: make(map[string]*ConsumerGroupState),
			Offsets:        make(map[string]int64),
		},
		brokerInactiveTimeout: brokerInactiveTimeout,
		replicaLagTolerance:   replicaLagTolerance,
	}
}

// Command types
const (
	CmdRegisterBroker    = "register_broker"
	CmdBrokerHeartbeat   = "broker_heartbeat"
	CmdCreateTopic       = "create_topic"
	CmdAssignPartitions  = "assign_partitions"
	CmdJoinConsumerGroup = "join_consumer_group"
	CmdRebalanceGroup    = "rebalance_group"
	CmdCommitOffset      = "commit_offset"
	CmdCleanInactive     = "clean_inactive"
	CmdDrainBroker       = "drain_broker"

	// Topology / admin (stream C)
	CmdDeleteTopic      = "delete_topic"
	CmdCreatePartitions = "create_partitions"
	CmdAlterTopicConfig = "alter_topic_config"
	CmdElectLeader      = "elect_leader"
)

// AdminError is returned from Apply for admin commands; Code is the Kafka protocol error code.
type AdminError struct {
	Code int32
	Msg  string
}

func (e *AdminError) Error() string { return e.Msg }

// Kafka error codes used by admin commands.
const (
	ErrUnknownTopicOrPartition int32 = 3
	ErrTopicAlreadyExists      int32 = 36
	ErrInvalidPartitions       int32 = 37
	ErrInvalidReplicaAssign    int32 = 39
	ErrInvalidConfig           int32 = 40
	ErrInvalidRequest          int32 = 42
	ErrElectionNotNeeded       int32 = 84
	ErrEligibleLeadersNA       int32 = 83
)

type Command struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

// offsetKey builds the composite key used to store/look up a consumer
// group's committed offset for a given topic-partition in f.state.Offsets.
// Both Apply (CmdCommitOffset) and GetOffset must use this helper so the
// write and read paths always agree on the key format.
func offsetKey(groupID, topic string, partition uint32) string {
	return fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case CmdRegisterBroker:
		var payload struct {
			ID        uint32 `json:"id"`
			Host      string `json:"host"`
			Port      int32  `json:"port"`
			Rack      string `json:"rack"`
			KafkaPort int32  `json:"kafka_port"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			f.state.Brokers[payload.ID] = &BrokerStatus{
				ID:        payload.ID,
				Host:      payload.Host,
				Port:      payload.Port,
				LastSeen:  time.Now(),
				Active:    true,
				Rack:      payload.Rack,
				KafkaPort: payload.KafkaPort,
			}
		}

	case CmdBrokerHeartbeat:
		var payload struct {
			ID             uint32 `json:"id"`
			ReplicaOffsets []struct {
				Topic     string `json:"topic"`
				Partition uint32 `json:"partition"`
				Offset    int64  `json:"offset"`
			} `json:"replica_offsets"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if broker, exists := f.state.Brokers[payload.ID]; exists {
				broker.LastSeen = time.Now()
				broker.Active = true
			}
			// Update replica offsets
			for _, ro := range payload.ReplicaOffsets {
				if topicState, exists := f.state.Topics[ro.Topic]; exists {
					if partitionState, exists := topicState.Partitions[ro.Partition]; exists {
						if partitionState.ReplicaOffsets == nil {
							partitionState.ReplicaOffsets = make(map[uint32]int64)
						}
						partitionState.ReplicaOffsets[payload.ID] = ro.Offset
					}
				}
			}
			f.updateISRAndHW()
		}

	case CmdDrainBroker:
		var payload struct {
			ID uint32 `json:"id"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if broker, exists := f.state.Brokers[payload.ID]; exists {
				broker.Active = false
			}
			activeBrokers := f.getActiveBrokerIDs()
			for _, topic := range f.state.Topics {
				for _, partition := range topic.Partitions {
					// 1. Leader migration if leader was the draining broker
					if partition.LeaderID == payload.ID {
						newLeader := uint32(0)
						for _, rID := range partition.ISR {
							if rID != payload.ID {
								newLeader = rID
								break
							}
						}
						if newLeader == 0 && len(activeBrokers) > 0 {
							newLeader = activeBrokers[0]
						}
						partition.LeaderID = newLeader
					}

					// 2. Remove from ReplicaIDs and replace if possible
					newReplicas := make([]uint32, 0, len(partition.ReplicaIDs))
					hadReplica := false
					for _, rID := range partition.ReplicaIDs {
						if rID != payload.ID {
							newReplicas = append(newReplicas, rID)
						} else {
							hadReplica = true
						}
					}
					if hadReplica {
						for _, abID := range activeBrokers {
							alreadyIn := false
							for _, rID := range newReplicas {
								if rID == abID {
									alreadyIn = true
									break
								}
							}
							if !alreadyIn {
								newReplicas = append(newReplicas, abID)
								break
							}
						}
					}
					partition.ReplicaIDs = newReplicas

					// 3. Remove from ISR and offsets
					newISR := make([]uint32, 0, len(partition.ISR))
					for _, isrID := range partition.ISR {
						if isrID != payload.ID {
							newISR = append(newISR, isrID)
						}
					}
					partition.ISR = newISR
					if partition.ReplicaOffsets != nil {
						delete(partition.ReplicaOffsets, payload.ID)
					}
				}
			}
			f.updateISRAndHW()
		}

	case CmdCreateTopic:
		var payload struct {
			Name              string              `json:"name"`
			Partitions        uint32              `json:"partitions"`
			ReplicationFactor uint32              `json:"replication_factor"`
			Configs           map[string]string   `json:"configs,omitempty"`
			Manual            map[uint32][]uint32 `json:"manual,omitempty"`
			FailIfExists      bool                `json:"fail_if_exists,omitempty"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if _, exists := f.state.Topics[payload.Name]; exists {
				if payload.FailIfExists {
					return &AdminError{Code: ErrTopicAlreadyExists, Msg: fmt.Sprintf("Topic '%s' already exists.", payload.Name)}
				}
				break
			}
			topic := &TopicState{
				Name:       payload.Name,
				Partitions: make(map[uint32]*PartitionState),
			}
			if len(payload.Configs) > 0 {
				topic.Configs = make(map[string]string, len(payload.Configs))
				for k, v := range payload.Configs {
					topic.Configs[k] = v
				}
			}

			var assignment [][]uint32
			if len(payload.Manual) > 0 {
				known := map[uint32]bool{}
				for _, id := range f.getActiveBrokerIDs() {
					known[id] = true
				}
				assignment = make([][]uint32, len(payload.Manual))
				for pid, reps := range payload.Manual {
					if int(pid) >= len(assignment) {
						return &AdminError{Code: ErrInvalidReplicaAssign, Msg: "manual assignment partitions must be contiguous from 0"}
					}
					if err := placement.ValidateManual(reps, known); err != nil {
						return &AdminError{Code: ErrInvalidReplicaAssign, Msg: err.Error()}
					}
					assignment[pid] = reps
				}
				payload.Partitions = uint32(len(assignment))
			} else {
				assignment, _ = placement.Assign(f.placementBrokers(), 0, int(payload.Partitions), int(payload.ReplicationFactor))
			}

			for i := uint32(0); i < payload.Partitions; i++ {
				replicas := assignment[i]
				leaderID := uint32(0)
				if len(replicas) > 0 {
					leaderID = replicas[0]
				}
				topic.Partitions[i] = &PartitionState{
					PartitionID:    i,
					LeaderID:       leaderID,
					ReplicaIDs:     replicas,
					ISR:            append([]uint32(nil), replicas...),
					HighWatermark:  0,
					ReplicaOffsets: make(map[uint32]int64),
				}
				for _, rID := range replicas {
					topic.Partitions[i].ReplicaOffsets[rID] = 0
				}
			}
			f.state.Topics[payload.Name] = topic
			f.updateISRAndHW()
		}

	case CmdAssignPartitions:
		var payload struct {
			Topic      string                     `json:"topic"`
			Partitions map[uint32]*PartitionState `json:"partitions"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if t, exists := f.state.Topics[payload.Topic]; exists {
				for pID, pState := range payload.Partitions {
					t.Partitions[pID] = pState
					if t.Partitions[pID].ReplicaOffsets == nil {
						t.Partitions[pID].ReplicaOffsets = make(map[uint32]int64)
					}
				}
				f.updateISRAndHW()
			}
		}

	case CmdJoinConsumerGroup:
		var payload struct {
			GroupID    string   `json:"group_id"`
			MemberID   string   `json:"member_id"`
			Topics     []string `json:"topics"`
			ClientHost string   `json:"client_host,omitempty"`
			UserAgent  string   `json:"user_agent,omitempty"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			g, exists := f.state.ConsumerGroups[payload.GroupID]
			if !exists {
				g = &ConsumerGroupState{
					GroupID:           payload.GroupID,
					Protocol:          "COOPERATIVE_STICKY",
					State:             "STABLE",
					Generation:        0,
					Members:           make(map[string]*GroupMember),
					Assignments:       make(map[string][]PartitionTopic),
					LastRebalanceTime: time.Now(),
				}
				f.state.ConsumerGroups[payload.GroupID] = g
			}

			m, mExists := g.Members[payload.MemberID]
			if !mExists {
				m = &GroupMember{
					ID:                 payload.MemberID,
					Topics:             payload.Topics,
					LastSeen:           time.Now(),
					ClientHost:         payload.ClientHost,
					UserAgent:          payload.UserAgent,
					AssignedPartitions: make([]PartitionTopic, 0),
					RevokingPartitions: make([]PartitionTopic, 0),
				}
				g.Members[payload.MemberID] = m
			} else {
				m.Topics = payload.Topics
				m.LastSeen = time.Now()
				if payload.ClientHost != "" {
					m.ClientHost = payload.ClientHost
				}
				if payload.UserAgent != "" {
					m.UserAgent = payload.UserAgent
				}
			}

			// Trigger a rebalance!
			f.rebalanceGroup(g)
		}

	case CmdRebalanceGroup:
		var payload struct {
			GroupID string `json:"group_id"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if g, exists := f.state.ConsumerGroups[payload.GroupID]; exists {
				f.rebalanceGroup(g)
			}
		}

	case CmdCommitOffset:
		var payload struct {
			GroupID   string `json:"group_id"`
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			Offset    int64  `json:"offset"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			key := offsetKey(payload.GroupID, payload.Topic, payload.Partition)
			f.state.Offsets[key] = payload.Offset
		}

	case CmdDeleteTopic:
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if _, ok := f.state.Topics[payload.Name]; !ok {
				return &AdminError{Code: ErrUnknownTopicOrPartition, Msg: fmt.Sprintf("This server does not host topic '%s'.", payload.Name)}
			}
			delete(f.state.Topics, payload.Name)
			mid := "/" + payload.Name + "/"
			for k := range f.state.Offsets {
				if strings.Contains(k, mid) {
					delete(f.state.Offsets, k)
				}
			}
			for _, g := range f.state.ConsumerGroups {
				for id, pts := range g.Assignments {
					g.Assignments[id] = filterTopic(pts, payload.Name)
				}
			}
		}

	case CmdCreatePartitions:
		var payload struct {
			Name     string              `json:"name"`
			NewTotal uint32              `json:"new_total"`
			Manual   map[uint32][]uint32 `json:"manual,omitempty"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			return f.applyCreatePartitions(payload.Name, payload.NewTotal, payload.Manual)
		}

	case CmdAlterTopicConfig:
		var payload struct {
			Name       string            `json:"name"`
			Set        map[string]string `json:"set"`
			Delete     []string          `json:"delete"`
			ReplaceAll bool              `json:"replace_all"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			t, ok := f.state.Topics[payload.Name]
			if !ok {
				return &AdminError{Code: ErrUnknownTopicOrPartition, Msg: fmt.Sprintf("This server does not host topic '%s'.", payload.Name)}
			}
			if payload.ReplaceAll || t.Configs == nil {
				t.Configs = map[string]string{}
			}
			for _, k := range payload.Delete {
				delete(t.Configs, k)
			}
			for k, v := range payload.Set {
				t.Configs[k] = v
			}
		}

	case CmdElectLeader:
		var payload struct {
			Topic     string `json:"topic"`
			Partition uint32 `json:"partition"`
			Unclean   bool   `json:"unclean"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			return f.applyElectLeader(payload.Topic, payload.Partition, payload.Unclean)
		}

	case CmdCleanInactive:
		// Clean inactive brokers and trigger failover if they were partition leaders
		now := time.Now()
		for _, broker := range f.state.Brokers {
			if broker.Active && now.Sub(broker.LastSeen) > f.brokerInactiveTimeout {
				broker.Active = false
				f.handleBrokerFailure(broker.ID)
			}
		}
		f.updateISRAndHW()
	}

	return nil
}

func (f *FSM) updateISRAndHW() {
	now := time.Now()
	for _, topic := range f.state.Topics {
		for _, partition := range topic.Partitions {
			leaderID := partition.LeaderID

			if partition.ReplicaOffsets == nil {
				partition.ReplicaOffsets = make(map[uint32]int64)
			}

			leaderOffset := int64(0)
			if val, ok := partition.ReplicaOffsets[leaderID]; ok {
				leaderOffset = val
			}

			newISR := []uint32{}
			leaderBroker, leaderExists := f.state.Brokers[leaderID]
			if leaderExists && leaderBroker.Active && now.Sub(leaderBroker.LastSeen) <= f.brokerInactiveTimeout {
				newISR = append(newISR, leaderID)
			}

			for _, rID := range partition.ReplicaIDs {
				if rID == leaderID {
					continue
				}
				broker, exists := f.state.Brokers[rID]
				if !exists || !broker.Active || now.Sub(broker.LastSeen) > f.brokerInactiveTimeout {
					continue
				}
				offset := partition.ReplicaOffsets[rID]
				lag := leaderOffset - offset
				if lag <= f.replicaLagTolerance {
					newISR = append(newISR, rID)
				}
			}

			partition.ISR = newISR

			if len(newISR) > 0 {
				var minOffset int64
				for i, rID := range newISR {
					off := partition.ReplicaOffsets[rID]
					if i == 0 || off < minOffset {
						minOffset = off
					}
				}
				partition.HighWatermark = minOffset
			} else {
				partition.HighWatermark = 0
			}
		}
	}
}

func (f *FSM) getActiveBrokerIDs() []uint32 {
	ids := []uint32{}
	for id, b := range f.state.Brokers {
		if b.Active {
			ids = append(ids, id)
		}
	}
	return ids
}

func (f *FSM) handleBrokerFailure(failedID uint32) {
	// Find all partitions where this broker was leader, and assign a new leader
	activeBrokers := f.getActiveBrokerIDs()
	if len(activeBrokers) == 0 {
		return // No active brokers to failover to!
	}

	for _, topic := range f.state.Topics {
		for _, partition := range topic.Partitions {
			if partition.LeaderID == failedID {
				// Pick a new leader from replicas if possible, else round robin active
				newLeader := uint32(0)
				for _, repID := range partition.ReplicaIDs {
					if repID != failedID {
						if b, exists := f.state.Brokers[repID]; exists && b.Active {
							newLeader = repID
							break
						}
					}
				}
				if newLeader == 0 {
					newLeader = activeBrokers[0] // Fallback
				}
				partition.LeaderID = newLeader
			}
		}
	}
}

func (f *FSM) rebalanceGroup(g *ConsumerGroupState) {
	if g == nil {
		return
	}
	g.Protocol = "COOPERATIVE_STICKY"
	g.State = "STABLE"
	g.Generation++
	g.RebalanceCount++
	g.LastRebalanceTime = time.Now()

	if g.Members == nil {
		g.Members = make(map[string]*GroupMember)
	}
	if g.Assignments == nil {
		g.Assignments = make(map[string][]PartitionTopic)
	}

	// Deterministic member IDs
	memberIDs := make([]string, 0, len(g.Members))
	for mID := range g.Members {
		memberIDs = append(memberIDs, mID)
	}
	sort.Strings(memberIDs)

	if len(memberIDs) == 0 {
		g.LeaderID = ""
		g.Assignments = make(map[string][]PartitionTopic)
		return
	}

	// Update leader ID
	if g.LeaderID == "" || g.Members[g.LeaderID] == nil {
		g.LeaderID = memberIDs[0]
	}

	// Reset revoking partitions for all active members
	for _, m := range g.Members {
		m.RevokingPartitions = make([]PartitionTopic, 0)
		if m.AssignedPartitions == nil {
			m.AssignedPartitions = make([]PartitionTopic, 0)
		}
	}

	// Collect unique topics subscribed across all members
	topicSet := make(map[string]bool)
	for _, mID := range memberIDs {
		m := g.Members[mID]
		for _, t := range m.Topics {
			topicSet[t] = true
		}
	}
	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	// Map to accumulate final assignments per member: memberID -> []PartitionTopic
	finalAssignments := make(map[string][]PartitionTopic)
	for _, mID := range memberIDs {
		finalAssignments[mID] = make([]PartitionTopic, 0)
	}

	// Perform cooperative sticky balancing topic by topic
	for _, topicName := range topics {
		topicState, exists := f.state.Topics[topicName]
		if !exists || len(topicState.Partitions) == 0 {
			continue
		}

		// Partitions for this topic, sorted
		partitionIDs := make([]uint32, 0, len(topicState.Partitions))
		for pID := range topicState.Partitions {
			partitionIDs = append(partitionIDs, pID)
		}
		sort.Slice(partitionIDs, func(i, j int) bool { return partitionIDs[i] < partitionIDs[j] })

		// Subscribed members for this topic, sorted
		subMembers := make([]string, 0)
		for _, mID := range memberIDs {
			m := g.Members[mID]
			for _, t := range m.Topics {
				if t == topicName {
					subMembers = append(subMembers, mID)
					break
				}
			}
		}
		if len(subMembers) == 0 {
			continue
		}

		numPartitions := len(partitionIDs)
		numMembers := len(subMembers)
		baseQuota := numPartitions / numMembers
		remainder := numPartitions % numMembers

		maxQuota := baseQuota
		if remainder > 0 {
			maxQuota = baseQuota + 1
		}

		// Find existing assignments for this topic
		currentHeld := make(map[string][]uint32)
		for _, mID := range subMembers {
			currentHeld[mID] = make([]uint32, 0)
		}

		claimed := make(map[uint32]string)
		for _, mID := range subMembers {
			var prevList []PartitionTopic
			if pts, ok := g.Assignments[mID]; ok {
				prevList = append(prevList, pts...)
			}
			if m, ok := g.Members[mID]; ok && m != nil {
				prevList = append(prevList, m.AssignedPartitions...)
			}

			seen := make(map[uint32]bool)
			for _, pt := range prevList {
				if pt.Topic == topicName {
					if _, valid := topicState.Partitions[pt.Partition]; valid {
						if !seen[pt.Partition] {
							seen[pt.Partition] = true
							if _, alreadyClaimed := claimed[pt.Partition]; !alreadyClaimed {
								claimed[pt.Partition] = mID
								currentHeld[mID] = append(currentHeld[mID], pt.Partition)
							}
						}
					}
				}
			}
			sort.Slice(currentHeld[mID], func(i, j int) bool {
				return currentHeld[mID][i] < currentHeld[mID][j]
			})
		}

		// Retained assignments per member
		retained := make(map[string][]uint32)
		unassigned := make([]uint32, 0)

		// 1. Cooperative preservation: keep partitions within maxQuota
		for _, mID := range subMembers {
			parts := currentHeld[mID]
			if len(parts) > maxQuota {
				retained[mID] = append([]uint32(nil), parts[:maxQuota]...)
				revoked := parts[maxQuota:]
				unassigned = append(unassigned, revoked...)
				for _, p := range revoked {
					g.Members[mID].RevokingPartitions = append(g.Members[mID].RevokingPartitions, PartitionTopic{
						Topic:     topicName,
						Partition: p,
					})
				}
			} else {
				retained[mID] = append([]uint32(nil), parts...)
			}
		}

		// 2. If count of members holding (baseQuota + 1) exceeds remainder, shed the excess from the end
		if remainder > 0 {
			countPlusOne := 0
			for _, mID := range subMembers {
				if len(retained[mID]) == baseQuota+1 {
					countPlusOne++
				}
			}
			for i := len(subMembers) - 1; i >= 0 && countPlusOne > remainder; i-- {
				mID := subMembers[i]
				if len(retained[mID]) == baseQuota+1 {
					revokedP := retained[mID][len(retained[mID])-1]
					retained[mID] = retained[mID][:len(retained[mID])-1]
					unassigned = append(unassigned, revokedP)
					g.Members[mID].RevokingPartitions = append(g.Members[mID].RevokingPartitions, PartitionTopic{
						Topic:     topicName,
						Partition: revokedP,
					})
					countPlusOne--
				}
			}
		}

		// 3. Find any partitions that were not held at all (orphaned from departed members or brand new)
		allHeld := make(map[uint32]bool)
		for _, mID := range subMembers {
			for _, p := range retained[mID] {
				allHeld[p] = true
			}
		}
		for _, p := range partitionIDs {
			if !allHeld[p] {
				found := false
				for _, u := range unassigned {
					if u == p {
						found = true
						break
					}
				}
				if !found {
					unassigned = append(unassigned, p)
				}
			}
		}
		sort.Slice(unassigned, func(i, j int) bool { return unassigned[i] < unassigned[j] })

		// 4. Reassign orphaned / new partitions to the least-loaded members
		for _, p := range unassigned {
			bestMember := ""
			bestCount := -1
			for _, mID := range subMembers {
				c := len(retained[mID])
				if bestCount == -1 || c < bestCount {
					bestCount = c
					bestMember = mID
				}
			}
			if bestMember != "" {
				retained[bestMember] = append(retained[bestMember], p)
				sort.Slice(retained[bestMember], func(i, j int) bool {
					return retained[bestMember][i] < retained[bestMember][j]
				})
			}
		}

		// Populate into finalAssignments
		for _, mID := range subMembers {
			for _, p := range retained[mID] {
				finalAssignments[mID] = append(finalAssignments[mID], PartitionTopic{
					Topic:     topicName,
					Partition: p,
				})
			}
		}
	}

	// Update g.Assignments and g.Members[mID].AssignedPartitions
	g.Assignments = finalAssignments
	for mID, m := range g.Members {
		m.AssignedPartitions = append([]PartitionTopic(nil), finalAssignments[mID]...)
	}
}

// GetMetadata returns current cluster snapshot metadata
func (f *FSM) GetMetadata(topics []string) ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Perform a deep copy of current state
	res := ClusterState{
		Brokers:        make(map[uint32]*BrokerStatus),
		Topics:         make(map[string]*TopicState),
		ConsumerGroups: make(map[string]*ConsumerGroupState),
		Offsets:        make(map[string]int64),
	}

	for k, v := range f.state.Brokers {
		res.Brokers[k] = &BrokerStatus{
			ID:        v.ID,
			Host:      v.Host,
			Port:      v.Port,
			LastSeen:  v.LastSeen,
			Active:    v.Active,
			Rack:      v.Rack,
			KafkaPort: v.KafkaPort,
		}
	}

	// Filter topics if requested
	filter := len(topics) > 0
	topicMap := make(map[string]bool)
	for _, t := range topics {
		topicMap[t] = true
	}

	for k, v := range f.state.Topics {
		if filter && !topicMap[k] {
			continue
		}
		tState := &TopicState{
			Name:       v.Name,
			Partitions: make(map[uint32]*PartitionState),
		}
		if len(v.Configs) > 0 {
			tState.Configs = make(map[string]string, len(v.Configs))
			for ck, cv := range v.Configs {
				tState.Configs[ck] = cv
			}
		}
		for pID, pVal := range v.Partitions {
			replicaOffsetsCopy := make(map[uint32]int64)
			for rk, rv := range pVal.ReplicaOffsets {
				replicaOffsetsCopy[rk] = rv
			}
			tState.Partitions[pID] = &PartitionState{
				PartitionID:    pVal.PartitionID,
				LeaderID:       pVal.LeaderID,
				ReplicaIDs:     append([]uint32(nil), pVal.ReplicaIDs...),
				ISR:            append([]uint32(nil), pVal.ISR...),
				HighWatermark:  pVal.HighWatermark,
				ReplicaOffsets: replicaOffsetsCopy,
			}
		}
		res.Topics[k] = tState
	}

	for k, v := range f.state.ConsumerGroups {
		cgState := &ConsumerGroupState{
			GroupID:           v.GroupID,
			Protocol:          v.Protocol,
			State:             v.State,
			Generation:        v.Generation,
			LeaderID:          v.LeaderID,
			RebalanceCount:    v.RebalanceCount,
			LastRebalanceTime: v.LastRebalanceTime,
			Members:           make(map[string]*GroupMember),
			Assignments:       make(map[string][]PartitionTopic),
		}
		for mk, mv := range v.Members {
			cgState.Members[mk] = &GroupMember{
				ID:                 mv.ID,
				Topics:             append([]string(nil), mv.Topics...),
				LastSeen:           mv.LastSeen,
				ClientHost:         mv.ClientHost,
				UserAgent:          mv.UserAgent,
				AssignedPartitions: append([]PartitionTopic(nil), mv.AssignedPartitions...),
				RevokingPartitions: append([]PartitionTopic(nil), mv.RevokingPartitions...),
			}
		}
		for ak, av := range v.Assignments {
			cgState.Assignments[ak] = append([]PartitionTopic(nil), av...)
		}
		res.ConsumerGroups[k] = cgState
	}

	for k, v := range f.state.Offsets {
		res.Offsets[k] = v
	}

	return res
}

func (f *FSM) GetOffset(groupID string, topic string, partition uint32) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	key := offsetKey(groupID, topic, partition)
	if val, exists := f.state.Offsets[key]; exists {
		return val
	}
	return -1
}

// Snapshot FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Create a backup of state
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{stateData: data}, nil
}

// Restore FSM state from snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct {
	stateData []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if _, err := sink.Write(s.stateData); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (s *fsmSnapshot) Release() {}

func filterTopic(pts []PartitionTopic, topic string) []PartitionTopic {
	out := make([]PartitionTopic, 0, len(pts))
	for _, p := range pts {
		if p.Topic != topic {
			out = append(out, p)
		}
	}
	return out
}

// placementBrokers returns the active brokers (deterministically ordered) with their racks.
func (f *FSM) placementBrokers() []placement.Broker {
	ids := f.getActiveBrokerIDs()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]placement.Broker, 0, len(ids))
	for _, id := range ids {
		out = append(out, placement.Broker{ID: id, Rack: f.state.Brokers[id].Rack})
	}
	return out
}

func (f *FSM) applyCreatePartitions(name string, newTotal uint32, manual map[uint32][]uint32) interface{} {
	t, ok := f.state.Topics[name]
	if !ok {
		return &AdminError{Code: ErrUnknownTopicOrPartition, Msg: fmt.Sprintf("This server does not host topic '%s'.", name)}
	}
	cur := uint32(len(t.Partitions))
	if newTotal <= cur {
		return &AdminError{Code: ErrInvalidPartitions, Msg: fmt.Sprintf("Topic already has %d partitions.", cur)}
	}
	add := int(newTotal - cur)
	rf := 1
	if p0, ok := t.Partitions[0]; ok && len(p0.ReplicaIDs) > 0 {
		rf = len(p0.ReplicaIDs)
	}
	var assignment [][]uint32
	if len(manual) > 0 {
		if len(manual) != add {
			return &AdminError{Code: ErrInvalidReplicaAssign, Msg: fmt.Sprintf("Increasing the number of partitions by %d but %d assignments provided.", add, len(manual))}
		}
		known := map[uint32]bool{}
		for _, id := range f.getActiveBrokerIDs() {
			known[id] = true
		}
		assignment = make([][]uint32, add)
		for i := 0; i < add; i++ {
			reps, ok := manual[uint32(i)]
			if !ok {
				return &AdminError{Code: ErrInvalidReplicaAssign, Msg: "manual assignments must be indexed 0..n-1 for the new partitions"}
			}
			if err := placement.ValidateManual(reps, known); err != nil {
				return &AdminError{Code: ErrInvalidReplicaAssign, Msg: err.Error()}
			}
			assignment[i] = reps
		}
	} else {
		assignment, _ = placement.Assign(f.placementBrokers(), int(cur), add, rf)
	}
	for i := 0; i < add; i++ {
		pid := cur + uint32(i)
		reps := assignment[i]
		leader := uint32(0)
		if len(reps) > 0 {
			leader = reps[0]
		}
		ps := &PartitionState{PartitionID: pid, LeaderID: leader, ReplicaIDs: reps,
			ISR: append([]uint32(nil), reps...), ReplicaOffsets: map[uint32]int64{}}
		for _, r := range reps {
			ps.ReplicaOffsets[r] = 0
		}
		t.Partitions[pid] = ps
	}
	f.updateISRAndHW()
	return nil
}

func (f *FSM) applyElectLeader(topic string, partition uint32, unclean bool) interface{} {
	t, ok := f.state.Topics[topic]
	if !ok {
		return &AdminError{Code: ErrUnknownTopicOrPartition, Msg: "unknown topic"}
	}
	p, ok := t.Partitions[partition]
	if !ok {
		return &AdminError{Code: ErrUnknownTopicOrPartition, Msg: "unknown partition"}
	}
	if len(p.ReplicaIDs) == 0 {
		return &AdminError{Code: ErrEligibleLeadersNA, Msg: "partition has no replicas"}
	}
	active := func(id uint32) bool { b, ok := f.state.Brokers[id]; return ok && b.Active }
	inISR := func(id uint32) bool {
		for _, i := range p.ISR {
			if i == id {
				return true
			}
		}
		return false
	}
	preferred := p.ReplicaIDs[0]
	if !unclean {
		if p.LeaderID == preferred {
			return &AdminError{Code: ErrElectionNotNeeded, Msg: "Leader is already the preferred replica."}
		}
		if !active(preferred) || !inISR(preferred) {
			return &AdminError{Code: ErrEligibleLeadersNA, Msg: "Preferred replica is not in sync or not alive."}
		}
		p.LeaderID = preferred
		f.updateISRAndHW()
		return nil
	}
	// Unclean: pick the most caught-up live replica.
	best := uint32(0)
	bestOff := int64(-1)
	for _, id := range p.ReplicaIDs {
		if active(id) && p.ReplicaOffsets[id] > bestOff {
			best, bestOff = id, p.ReplicaOffsets[id]
		}
	}
	if best == 0 {
		return &AdminError{Code: ErrEligibleLeadersNA, Msg: "No live replica available."}
	}
	if best == p.LeaderID {
		return &AdminError{Code: ErrElectionNotNeeded, Msg: "Leader is already the best available replica."}
	}
	p.LeaderID = best
	f.updateISRAndHW()
	return nil
}
