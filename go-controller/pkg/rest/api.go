package rest

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/auth"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/connect"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/consensus"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/schemaregistry"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/transform"
	"github.com/hashicorp/raft"
)

type Server struct {
	raftNode         *consensus.RaftNode
	httpAddr         string
	schemaRegistry   *schemaregistry.Registry
	aclManager       *auth.AclManager
	transformEngine  *transform.Engine
	connectorManager *connect.ConnectorManager
}

func NewServer(raftNode *consensus.RaftNode, httpAddr string, registry ...*schemaregistry.Registry) *Server {
	var reg *schemaregistry.Registry
	if len(registry) > 0 && registry[0] != nil {
		reg = registry[0]
	} else {
		reg = schemaregistry.NewRegistry()
	}
	return &Server{
		raftNode:         raftNode,
		httpAddr:         httpAddr,
		schemaRegistry:   reg,
		aclManager:       auth.NewAclManager(),
		transformEngine:  transform.NewEngine(),
		connectorManager: connect.NewManager(),
	}
}

func (s *Server) Registry() *schemaregistry.Registry {
	return s.schemaRegistry
}

func (s *Server) SetSchemaRegistry(r *schemaregistry.Registry) {
	s.schemaRegistry = r
}

func (s *Server) TransformEngine() *transform.Engine {
	return s.transformEngine
}

func (s *Server) SetTransformEngine(t *transform.Engine) {
	s.transformEngine = t
}

func (s *Server) AclManager() *auth.AclManager {
	return s.aclManager
}

func (s *Server) SetAclManager(m *auth.AclManager) {
	s.aclManager = m
}

func (s *Server) ConnectorManager() *connect.ConnectorManager {
	return s.connectorManager
}

func (s *Server) SetConnectorManager(m *connect.ConnectorManager) {
	s.connectorManager = m
}

func (s *Server) enableCORS(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return true
	}
	return false
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/cluster", s.handleCluster)
	mux.HandleFunc("/api/brokers", s.handleBrokers)
	mux.HandleFunc("/api/brokers/", s.handleBrokerItem)
	mux.HandleFunc("/api/topics", s.handleTopics)
	mux.HandleFunc("/api/groups", s.handleGroups)
	mux.HandleFunc("/api/groups/", s.handleGroups)
	mux.HandleFunc("/api/lag", s.handleLag)
	mux.HandleFunc("/api/produce", s.handleProduce)
	mux.HandleFunc("/api/messages", s.handleMessages)

	// ACLs and RBAC endpoints
	mux.HandleFunc("/api/acls", s.handleAcls)
	mux.HandleFunc("/api/acls/", s.handleAcls)
	mux.HandleFunc("/api/users", s.handleUsers)
	mux.HandleFunc("/api/users/", s.handleUsers)

	// Stream Transforms endpoints
	mux.HandleFunc("/api/transforms", s.handleTransforms)
	mux.HandleFunc("/api/transforms/", s.handleTransformItem)

	// Connectors Ecosystem endpoints (Kafka Connect-compatible & Native)
	mux.HandleFunc("/api/connectors", s.handleConnectors)
	mux.HandleFunc("/api/connectors/", s.handleConnectors)
	mux.HandleFunc("/api/connectors-detail", s.handleConnectorsDetail)
	mux.HandleFunc("/api/connector-plugins", s.handleConnectorPlugins)

	// Kafka Connect compatibility aliases
	mux.HandleFunc("/connectors", s.handleConnectors)
	mux.HandleFunc("/connectors/", s.handleConnectors)
	mux.HandleFunc("/connector-plugins", s.handleConnectorPlugins)

	// Schema Registry endpoints (Confluent-compatible)
	mux.HandleFunc("/subjects", s.handleSubjects)
	mux.HandleFunc("/subjects/", s.handleSubjects)
	mux.HandleFunc("/schemas/", s.handleSchemas)
	mux.HandleFunc("/compatibility/", s.handleCompatibility)
	mux.HandleFunc("/config", s.handleConfig)
	mux.HandleFunc("/config/", s.handleConfig)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	meta := s.raftNode.FSM.GetMetadata(nil)
	w.Header().Set("Content-Type", "application/json")

	brokersList := make([]interface{}, 0)
	if meta.Brokers != nil {
		for _, b := range meta.Brokers {
			brokersList = append(brokersList, map[string]interface{}{
				"id":        b.ID,
				"host":      b.Host,
				"port":      b.Port,
				"active":    b.Active,
				"last_seen": b.LastSeen,
			})
		}
	}

	resp := map[string]interface{}{
		"node_id":       s.raftNode.NodeID,
		"raft_state":    s.raftNode.Raft.State().String(),
		"raft_leader":   s.raftNode.Raft.Leader(),
		"brokers_count": len(meta.Brokers),
		"topics_count":  len(meta.Topics),
		"groups_count":  len(meta.ConsumerGroups),
		"brokers":       brokersList,
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleBrokers(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	meta := s.raftNode.FSM.GetMetadata(nil)
	w.Header().Set("Content-Type", "application/json")

	brokers := make([]interface{}, 0)
	if meta.Brokers != nil {
		for _, b := range meta.Brokers {
			brokers = append(brokers, b)
		}
	}
	json.NewEncoder(w).Encode(brokers)
}

func (s *Server) handleBrokerItem(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// Expected path: api/brokers/{id}/drain
	if len(parts) >= 4 && parts[3] == "drain" && r.Method == http.MethodPost {
		brokerID, err := strconv.ParseUint(parts[2], 10, 32)
		if err != nil {
			http.Error(w, "invalid broker id", http.StatusBadRequest)
			return
		}

		payload := struct {
			ID uint32 `json:"id"`
		}{ID: uint32(brokerID)}
		if err := s.raftNode.Propose(consensus.CmdDrainBroker, payload); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("broker %d drained and partitions reassigned", brokerID),
		})
		return
	}

	http.NotFound(w, r)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	if r.Method == http.MethodPost {
		var req struct {
			Name               string `json:"name"`
			Partitions         uint32 `json:"partitions"`
			ReplicationFactor  uint32 `json:"replication_factor"`
			CleanupPolicy      string `json:"cleanup_policy,omitempty"`
			RetentionPeriod    string `json:"retention_period,omitempty"`
			RetentionSize      string `json:"retention_size,omitempty"`
			SegmentSize        string `json:"segment_size,omitempty"`
			TombstoneRetention string `json:"tombstone_retention,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
		if req.Name == "" || req.Partitions == 0 {
			http.Error(w, "name and partitions are required", http.StatusBadRequest)
			return
		}
		if req.ReplicationFactor == 0 {
			req.ReplicationFactor = 1
		}

		err := s.raftNode.Propose(consensus.CmdCreateTopic, req)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to create topic: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("topic %s created successfully", req.Name),
		})
		return
	}

	meta := s.raftNode.FSM.GetMetadata(nil)
	w.Header().Set("Content-Type", "application/json")

	topics := make([]interface{}, 0)
	if meta.Topics != nil {
		for _, t := range meta.Topics {
			partitions := make([]interface{}, 0)
			if t.Partitions != nil {
				for _, p := range t.Partitions {
					replicaIDs := p.ReplicaIDs
					if replicaIDs == nil {
						replicaIDs = make([]uint32, 0)
					}
					isr := p.ISR
					if isr == nil {
						isr = make([]uint32, 0)
					}
					replicaOffsets := p.ReplicaOffsets
					if replicaOffsets == nil {
						replicaOffsets = make(map[uint32]int64)
					}
					partitions = append(partitions, map[string]interface{}{
						"partition_id":    p.PartitionID,
						"leader_id":       p.LeaderID,
						"replica_ids":     replicaIDs,
						"isr":             isr,
						"high_watermark":  p.HighWatermark,
						"replica_offsets": replicaOffsets,
					})
				}
			}
			topics = append(topics, map[string]interface{}{
				"name":       t.Name,
				"partitions": partitions,
				"configs":    t.Configs,
			})
		}
	}
	json.NewEncoder(w).Encode(topics)
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	path := strings.Trim(r.URL.Path, "/")

	// Handle POST /api/groups/{id}/rebalance
	if strings.HasPrefix(path, "api/groups/") && strings.HasSuffix(path, "/rebalance") {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rawGroup := strings.TrimPrefix(path, "api/groups/")
		rawGroup = strings.TrimSuffix(rawGroup, "/rebalance")
		rawGroup = strings.Trim(rawGroup, "/")
		groupID, err := url.PathUnescape(rawGroup)
		if err != nil || groupID == "" {
			http.Error(w, "invalid group id", http.StatusBadRequest)
			return
		}

		if s.raftNode == nil {
			http.Error(w, "raft node not initialized", http.StatusInternalServerError)
			return
		}

		meta := s.raftNode.FSM.GetMetadata(nil)
		if meta.ConsumerGroups == nil {
			http.Error(w, fmt.Sprintf("consumer group %s not found", groupID), http.StatusNotFound)
			return
		}
		cg, exists := meta.ConsumerGroups[groupID]
		if !exists {
			http.Error(w, fmt.Sprintf("consumer group %s not found", groupID), http.StatusNotFound)
			return
		}

		rebalancePayload := struct {
			GroupID string `json:"group_id"`
		}{
			GroupID: groupID,
		}

		if s.raftNode.Raft != nil {
			if err := s.raftNode.Propose(consensus.CmdRebalanceGroup, rebalancePayload); err != nil {
				http.Error(w, fmt.Sprintf("failed to trigger rebalance: %v", err), http.StatusInternalServerError)
				return
			}
		} else if s.raftNode.FSM != nil {
			raw, _ := json.Marshal(rebalancePayload)
			cmdData, _ := json.Marshal(consensus.Command{
				Op:      consensus.CmdRebalanceGroup,
				Payload: raw,
			})
			s.raftNode.FSM.Apply(&raft.Log{Data: cmdData})
		}

		// Re-fetch updated metadata
		updatedMeta := s.raftNode.FSM.GetMetadata(nil)
		var updatedGroup *consensus.ConsumerGroupState
		if updatedMeta.ConsumerGroups != nil {
			updatedGroup = updatedMeta.ConsumerGroups[groupID]
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		protocol := "COOPERATIVE_STICKY"
		state := "STABLE"
		gen := cg.Generation + 1
		leaderID := cg.LeaderID
		rebalanceCount := cg.RebalanceCount + 1

		if updatedGroup != nil {
			if updatedGroup.Protocol != "" {
				protocol = updatedGroup.Protocol
			}
			if updatedGroup.State != "" {
				state = updatedGroup.State
			}
			gen = updatedGroup.Generation
			leaderID = updatedGroup.LeaderID
			rebalanceCount = updatedGroup.RebalanceCount
		}

		resp := map[string]interface{}{
			"success":         true,
			"message":         fmt.Sprintf("cooperative rebalance triggered successfully for group %s", groupID),
			"group_id":        groupID,
			"protocol":        protocol,
			"state":           state,
			"generation":      gen,
			"leader_id":       leaderID,
			"rebalance_count": rebalanceCount,
		}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Handle GET /api/groups
	if path != "api/groups" && path != "api/groups/" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if s.raftNode == nil {
		json.NewEncoder(w).Encode([]interface{}{})
		return
	}

	meta := s.raftNode.FSM.GetMetadata(nil)
	groups := make([]interface{}, 0)
	if meta.ConsumerGroups != nil {
		for _, g := range meta.ConsumerGroups {
			members := make([]interface{}, 0)
			if g.Members != nil {
				for _, m := range g.Members {
					assigned := g.Assignments[m.ID]
					if assigned == nil {
						assigned = make([]consensus.PartitionTopic, 0)
					}
					topics := m.Topics
					if topics == nil {
						topics = make([]string, 0)
					}
					assignedParts := m.AssignedPartitions
					if assignedParts == nil {
						assignedParts = make([]consensus.PartitionTopic, 0)
					}
					revokingParts := m.RevokingPartitions
					if revokingParts == nil {
						revokingParts = make([]consensus.PartitionTopic, 0)
					}
					members = append(members, map[string]interface{}{
						"id":                  m.ID,
						"topics":              topics,
						"last_seen":           m.LastSeen,
						"client_host":         m.ClientHost,
						"user_agent":          m.UserAgent,
						"assigned_partitions": assignedParts,
						"revoking_partitions": revokingParts,
						"assignments":         assigned,
					})
				}
			}

			assignments := g.Assignments
			if assignments == nil {
				assignments = make(map[string][]consensus.PartitionTopic)
			}

			protocol := g.Protocol
			if protocol == "" {
				protocol = "COOPERATIVE_STICKY"
			}
			state := g.State
			if state == "" {
				state = "STABLE"
			}

			groups = append(groups, map[string]interface{}{
				"group_id":            g.GroupID,
				"protocol":            protocol,
				"state":               state,
				"rebalance_count":     g.RebalanceCount,
				"leader_id":           g.LeaderID,
				"generation":          g.Generation,
				"last_rebalance_time": g.LastRebalanceTime,
				"members":             members,
				"assignments":         assignments,
			})
		}
	}
	json.NewEncoder(w).Encode(groups)
}

func (s *Server) handleLag(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	meta := s.raftNode.FSM.GetMetadata(nil)
	w.Header().Set("Content-Type", "application/json")

	type PartitionLag struct {
		GroupID         string `json:"group_id"`
		Topic           string `json:"topic"`
		Partition       uint32 `json:"partition"`
		CommittedOffset int64  `json:"committed_offset"`
		HighWatermark   int64  `json:"high_watermark"`
		Lag             int64  `json:"lag"`
	}

	lagList := make([]PartitionLag, 0)
	if meta.Offsets != nil {
		for offsetKey, committedOffset := range meta.Offsets {
			parts := strings.Split(offsetKey, "/")
			if len(parts) != 3 {
				continue
			}
			groupID := parts[0]
			topicName := parts[1]
			pID, err := strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				continue
			}

			var hw int64 = 0
			if topic, exists := meta.Topics[topicName]; exists {
				if partition, ok := topic.Partitions[uint32(pID)]; ok {
					hw = partition.HighWatermark
				}
			}

			lag := hw - committedOffset
			if lag < 0 {
				lag = 0
			}

			lagList = append(lagList, PartitionLag{
				GroupID:         groupID,
				Topic:           topicName,
				Partition:       uint32(pID),
				CommittedOffset: committedOffset,
				HighWatermark:   hw,
				Lag:             lag,
			})
		}
	}

	json.NewEncoder(w).Encode(lagList)
}

func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic     string `json:"topic"`
		Partition uint32 `json:"partition"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
		return
	}

	meta := s.raftNode.FSM.GetMetadata([]string{req.Topic})
	topicState, exists := meta.Topics[req.Topic]
	if !exists {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	pState, exists := topicState.Partitions[req.Partition]
	if !exists {
		http.Error(w, "partition not found", http.StatusNotFound)
		return
	}

	broker, exists := meta.Brokers[pState.LeaderID]
	if !exists {
		http.Error(w, "leader broker not found", http.StatusBadGateway)
		return
	}

	addr := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to connect to leader broker: %v", err), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	topicBytes := []byte(req.Topic)
	msgBytes := []byte(req.Message)
	bodyLen := 2 + len(topicBytes) + 4 + 4 + len(msgBytes)

	header := make([]byte, 7)
	header[0] = 0xAE
	header[1] = 0x01
	header[2] = 1 // Cmd: Produce
	binary.BigEndian.PutUint32(header[3:7], uint32(bodyLen))

	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(topicBytes)))
	copy(body[2:2+len(topicBytes)], topicBytes)
	offset := 2 + len(topicBytes)
	binary.BigEndian.PutUint32(body[offset:offset+4], req.Partition)
	binary.BigEndian.PutUint32(body[offset+4:offset+8], uint32(len(msgBytes)))
	copy(body[offset+8:], msgBytes)

	if _, err := conn.Write(header); err != nil {
		http.Error(w, fmt.Sprintf("failed to send produce header: %v", err), http.StatusInternalServerError)
		return
	}
	if _, err := conn.Write(body); err != nil {
		http.Error(w, fmt.Sprintf("failed to send produce body: %v", err), http.StatusInternalServerError)
		return
	}

	respHeader := make([]byte, 11)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		http.Error(w, fmt.Sprintf("failed to read produce ack: %v", err), http.StatusInternalServerError)
		return
	}

	if respHeader[0] != 0xAE || respHeader[1] != 0x01 || respHeader[2] != 0 {
		http.Error(w, fmt.Sprintf("broker returned error status: %d", respHeader[2]), http.StatusInternalServerError)
		return
	}

	assignedOffset := binary.BigEndian.Uint64(respHeader[3:11])
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"topic":   req.Topic,
		"partition": req.Partition,
		"offset":  assignedOffset,
	})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	if topic == "" || partitionStr == "" {
		http.Error(w, "topic and partition query params are required", http.StatusBadRequest)
		return
	}

	pID, err := strconv.ParseUint(partitionStr, 10, 32)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	startOffset := uint64(0)
	if offsetStr != "" {
		if o, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			startOffset = o
		}
	}

	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
			limit = l
		}
	}

	meta := s.raftNode.FSM.GetMetadata([]string{topic})
	topicState, exists := meta.Topics[topic]
	if !exists {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	pState, exists := topicState.Partitions[uint32(pID)]
	if !exists {
		http.Error(w, "partition not found", http.StatusNotFound)
		return
	}

	broker, exists := meta.Brokers[pState.LeaderID]
	if !exists {
		http.Error(w, "leader broker not found", http.StatusBadGateway)
		return
	}

	addr := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to connect to leader broker: %v", err), http.StatusBadGateway)
		return
	}
	defer conn.Close()

	type MessageRecord struct {
		Offset  uint64 `json:"offset"`
		Payload string `json:"payload"`
		Length  int    `json:"length"`
	}

	messages := make([]MessageRecord, 0)
	currentOffset := startOffset

	for i := 0; i < limit; i++ {
		topicBytes := []byte(topic)
		bodyLen := 2 + len(topicBytes) + 4 + 8 + 4

		header := make([]byte, 7)
		header[0] = 0xAE
		header[1] = 0x01
		header[2] = 2 // Cmd: Fetch
		binary.BigEndian.PutUint32(header[3:7], uint32(bodyLen))

		body := make([]byte, bodyLen)
		binary.BigEndian.PutUint16(body[0:2], uint16(len(topicBytes)))
		copy(body[2:2+len(topicBytes)], topicBytes)
		off := 2 + len(topicBytes)
		binary.BigEndian.PutUint32(body[off:off+4], uint32(pID))
		binary.BigEndian.PutUint64(body[off+4:off+12], currentOffset)
		binary.BigEndian.PutUint32(body[off+12:off+16], 65536)

		if _, err := conn.Write(header); err != nil {
			break
		}
		if _, err := conn.Write(body); err != nil {
			break
		}

		respHeader := make([]byte, 3)
		if _, err := io.ReadFull(conn, respHeader); err != nil {
			break
		}

		if respHeader[0] != 0xAE || respHeader[1] != 0x01 || respHeader[2] != 2 {
			break
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(lenBuf)

		payload := make([]byte, dataLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			break
		}

		messages = append(messages, MessageRecord{
			Offset:  currentOffset,
			Payload: string(payload),
			Length:  len(payload),
		})
		currentOffset++
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"topic":     topic,
		"partition": pID,
		"messages":  messages,
		"count":     len(messages),
	})
}

type schemaRequestPayload struct {
	Schema     json.RawMessage `json:"schema"`
	SchemaType string          `json:"schemaType"`
	Type       string          `json:"type"`
}

func (p *schemaRequestPayload) getSchemaType() string {
	if p.SchemaType != "" {
		return p.SchemaType
	}
	if p.Type != "" {
		return p.Type
	}
	return schemaregistry.TypeAvro
}

func (p *schemaRequestPayload) getSchemaString() string {
	raw := strings.TrimSpace(string(p.Schema))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"") {
		var s string
		if err := json.Unmarshal(p.Schema, &s); err == nil {
			return s
		}
	}
	return raw
}

func (s *Server) handleSubjects(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.Trim(r.URL.Path, "/")
	if path == "subjects" {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
		subjects := s.schemaRegistry.ListSubjects()
		json.NewEncoder(w).Encode(subjects)
		return
	}

	if !strings.HasPrefix(path, "subjects/") {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	restPath := strings.TrimPrefix(path, "subjects/")
	idx := strings.Index(restPath, "/versions")
	if idx == -1 {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	rawSubject := restPath[:idx]
	subject, err := url.PathUnescape(rawSubject)
	if err != nil || subject == "" {
		writeError(w, http.StatusBadRequest, 400, "Invalid subject")
		return
	}

	after := restPath[idx+len("/versions"):]
	if after == "" || after == "/" {
		switch r.Method {
		case http.MethodGet:
			if !s.schemaRegistry.HasSubject(subject) {
				writeError(w, http.StatusNotFound, 40401, fmt.Sprintf("Subject '%s' not found.", subject))
				return
			}
			versions := s.schemaRegistry.ListVersions(subject)
			json.NewEncoder(w).Encode(versions)
			return

		case http.MethodPost:
			var req schemaRequestPayload
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid request payload: %v", err))
				return
			}
			schemaStr := req.getSchemaString()
			if schemaStr == "" {
				writeError(w, http.StatusUnprocessableEntity, 42201, "Schema cannot be empty")
				return
			}
			id, _, err := s.schemaRegistry.RegisterSchema(subject, req.getSchemaType(), schemaStr)
			if err != nil {
				if errors.Is(err, schemaregistry.ErrIncompatibleSchema) {
					writeError(w, http.StatusConflict, 409, fmt.Sprintf("Schema being registered is incompatible with an earlier schema: %v", err))
					return
				}
				if errors.Is(err, schemaregistry.ErrInvalidSchema) {
					writeError(w, http.StatusUnprocessableEntity, 42201, fmt.Sprintf("Invalid schema: %v", err))
					return
				}
				writeError(w, http.StatusInternalServerError, 500, fmt.Sprintf("Failed to register schema: %v", err))
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id": id,
			})
			return

		default:
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
	}

	versionStr := strings.TrimPrefix(after, "/")
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}

	var schema *schemaregistry.Schema
	if strings.ToLower(versionStr) == "latest" {
		schema, err = s.schemaRegistry.GetLatestSchema(subject)
	} else {
		v, parseErr := strconv.Atoi(versionStr)
		if parseErr != nil {
			writeError(w, http.StatusNotFound, 40402, fmt.Sprintf("Invalid version: %s", versionStr))
			return
		}
		schema, err = s.schemaRegistry.GetSchemaByVersion(subject, v)
	}

	if err != nil {
		if errors.Is(err, schemaregistry.ErrSubjectNotFound) {
			writeError(w, http.StatusNotFound, 40401, fmt.Sprintf("Subject '%s' not found.", subject))
			return
		}
		if errors.Is(err, schemaregistry.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, 40402, fmt.Sprintf("Version %s not found for subject '%s'.", versionStr, subject))
			return
		}
		writeError(w, http.StatusInternalServerError, 500, err.Error())
		return
	}

	json.NewEncoder(w).Encode(schema)
}

func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}

	path := strings.Trim(r.URL.Path, "/")
	if !strings.HasPrefix(path, "schemas/ids/") {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	idStr := strings.TrimPrefix(path, "schemas/ids/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, 400, "Invalid schema ID")
		return
	}

	sc, err := s.schemaRegistry.GetSchemaByID(id)
	if err != nil {
		writeError(w, http.StatusNotFound, 40403, fmt.Sprintf("Schema %d not found", id))
		return
	}

	resp := map[string]interface{}{
		"schema": sc.Schema,
		"id":     sc.ID,
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleCompatibility(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}

	path := strings.Trim(r.URL.Path, "/")
	if !strings.HasPrefix(path, "compatibility/subjects/") {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	restPath := strings.TrimPrefix(path, "compatibility/subjects/")
	idx := strings.Index(restPath, "/versions")
	if idx == -1 {
		writeError(w, http.StatusBadRequest, 400, "Invalid compatibility path")
		return
	}

	rawSubject := restPath[:idx]
	subject, err := url.PathUnescape(rawSubject)
	if err != nil || subject == "" {
		writeError(w, http.StatusBadRequest, 400, "Invalid subject")
		return
	}

	versionStr := "latest"
	if strings.HasPrefix(restPath[idx:], "/versions/") {
		versionStr = restPath[idx+len("/versions/"):]
	}
	var req schemaRequestPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid request payload: %v", err))
		return
	}
	schemaStr := req.getSchemaString()
	schemaType := req.getSchemaType()

	var versionNum int
	if strings.ToLower(versionStr) == "latest" {
		versionNum = 0
	} else {
		v, parseErr := strconv.Atoi(versionStr)
		if parseErr != nil {
			writeError(w, http.StatusNotFound, 40402, fmt.Sprintf("Invalid version: %s", versionStr))
			return
		}
		versionNum = v
	}

	if !s.schemaRegistry.HasSubject(subject) {
		writeError(w, http.StatusNotFound, 40401, fmt.Sprintf("Subject '%s' not found.", subject))
		return
	}

	isCompat, testErr := s.schemaRegistry.TestCompatibilityWithVersion(subject, versionNum, schemaType, schemaStr)
	if testErr != nil {
		if errors.Is(testErr, schemaregistry.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, 40402, testErr.Error())
			return
		}
		if errors.Is(testErr, schemaregistry.ErrInvalidSchema) {
			writeError(w, http.StatusUnprocessableEntity, 42201, testErr.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, 500, testErr.Error())
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"is_compatible": isCompat,
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.Trim(r.URL.Path, "/")
	var subject string
	if path != "config" && strings.HasPrefix(path, "config/") {
		rawSubject := strings.TrimPrefix(path, "config/")
		var err error
		subject, err = url.PathUnescape(rawSubject)
		if err != nil {
			writeError(w, http.StatusBadRequest, 400, "Invalid subject in config path")
			return
		}
	} else if path != "config" {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		compat := s.schemaRegistry.GetCompatibility(subject)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"compatibilityLevel": compat,
			"compatibility":      compat,
		})
		return

	case http.MethodPut:
		var req struct {
			Compatibility      string `json:"compatibility"`
			CompatibilityLevel string `json:"compatibilityLevel"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON body: %v", err))
			return
		}
		levelStr := req.Compatibility
		if levelStr == "" {
			levelStr = req.CompatibilityLevel
		}
		level := schemaregistry.CompatibilityLevel(strings.ToUpper(strings.TrimSpace(levelStr)))
		if err := s.schemaRegistry.SetCompatibility(subject, level); err != nil {
			writeError(w, http.StatusUnprocessableEntity, 42203, fmt.Sprintf("Invalid compatibility level: %v", err))
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"compatibility":      level,
			"compatibilityLevel": level,
		})
		return

	default:
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}
}

func (s *Server) handleAcls(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.Trim(r.URL.Path, "/")

	// POST /api/acls/test - Live authorization evaluation
	if path == "api/acls/test" {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
		var testReq struct {
			Principal    string            `json:"principal"`
			ResourceType auth.ResourceType `json:"resource_type"`
			ResourceName string            `json:"resource_name"`
			Operation    auth.Operation    `json:"operation"`
		}
		if err := json.NewDecoder(r.Body).Decode(&testReq); err != nil {
			writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
			return
		}
		if strings.TrimSpace(testReq.Principal) == "" || testReq.ResourceType == "" || strings.TrimSpace(testReq.ResourceName) == "" || testReq.Operation == "" {
			writeError(w, http.StatusBadRequest, 400, "principal, resource_type, resource_name, and operation are required")
			return
		}

		allowed, reason := s.aclManager.AuthorizeWithReason(testReq.Principal, testReq.ResourceType, testReq.ResourceName, testReq.Operation)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"allowed": allowed,
			"reason":  reason,
		})
		return
	}

	// /api/acls - List or Create
	if path == "api/acls" {
		switch r.Method {
		case http.MethodGet:
			rules := s.aclManager.ListRules()
			json.NewEncoder(w).Encode(rules)
			return

		case http.MethodPost:
			var rule auth.AclRule
			if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
				writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
				return
			}
			if err := s.aclManager.AddRule(&rule); err != nil {
				writeError(w, http.StatusBadRequest, 400, err.Error())
				return
			}
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(rule)
			return

		default:
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
	}

	// /api/acls/{id} - Delete rule
	if strings.HasPrefix(path, "api/acls/") {
		rawID := strings.TrimPrefix(path, "api/acls/")
		ruleID, err := url.PathUnescape(rawID)
		if err != nil || ruleID == "" {
			writeError(w, http.StatusBadRequest, 400, "Invalid ACL rule ID")
			return
		}

		switch r.Method {
		case http.MethodDelete:
			if err := s.aclManager.DeleteRule(ruleID); err != nil {
				writeError(w, http.StatusNotFound, 404, err.Error())
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"deleted": true,
				"id":      ruleID,
			})
			return

		default:
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
	}

	writeError(w, http.StatusNotFound, 404, "Not found")
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.Trim(r.URL.Path, "/")
	if path != "api/users" {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		users := s.aclManager.ListUsers()
		json.NewEncoder(w).Encode(users)
		return

	case http.MethodPost:
		var u auth.User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
			return
		}
		if err := s.aclManager.AddUser(&u); err != nil {
			writeError(w, http.StatusBadRequest, 400, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(u)
		return

	default:
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}
}

func writeError(w http.ResponseWriter, statusCode int, errorCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error_code": errorCode,
		"message":    message,
	})
}

func (s *Server) handleTransforms(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		transforms := s.transformEngine.ListTransforms()
		json.NewEncoder(w).Encode(transforms)

	case http.MethodPost:
		var t transform.Transform
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			http.Error(w, fmt.Sprintf("invalid transform payload: %v", err), http.StatusBadRequest)
			return
		}
		if err := s.transformEngine.RegisterTransform(&t); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(t)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTransformItem(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	subPath := strings.TrimPrefix(r.URL.Path, "/api/transforms/")
	subPath = strings.Trim(subPath, "/")

	if subPath == "" {
		s.handleTransforms(w, r)
		return
	}

	if subPath == "test" {
		s.handleTransformTest(w, r)
		return
	}

	parts := strings.Split(subPath, "/")
	name, err := url.PathUnescape(parts[0])
	if err != nil {
		name = parts[0]
	}

	w.Header().Set("Content-Type", "application/json")

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			t, err := s.transformEngine.GetTransform(name)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(t)

		case http.MethodDelete:
			if err := s.transformEngine.DeleteTransform(name); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "deleted",
				"name":    name,
				"message": fmt.Sprintf("Transform %s successfully removed", name),
			})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 2 {
		action := parts[1]
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		switch action {
		case "pause":
			if err := s.transformEngine.PauseTransform(name); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "PAUSED",
				"name":    name,
				"message": fmt.Sprintf("Transform %s paused", name),
			})

		case "resume":
			if err := s.transformEngine.ResumeTransform(name); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":  "RUNNING",
				"name":    name,
				"message": fmt.Sprintf("Transform %s resumed", name),
			})

		default:
			http.Error(w, "unknown action", http.StatusBadRequest)
		}
		return
	}

	http.Error(w, "invalid path", http.StatusNotFound)
}

func (s *Server) handleTransformTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TransformName string               `json:"transform_name,omitempty"`
		Transform     *transform.Transform `json:"transform,omitempty"`
		Payload       interface{}          `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid test request body: %v", err), http.StatusBadRequest)
		return
	}

	var targetTransform *transform.Transform
	if req.TransformName != "" {
		existing, err := s.transformEngine.GetTransform(req.TransformName)
		if err != nil {
			http.Error(w, fmt.Sprintf("transform %q not found: %v", req.TransformName, err), http.StatusNotFound)
			return
		}
		targetTransform = existing
	} else if req.Transform != nil {
		targetTransform = req.Transform
	} else {
		http.Error(w, "either transform_name or transform configuration must be provided", http.StatusBadRequest)
		return
	}

	var payloadBytes []byte
	switch p := req.Payload.(type) {
	case string:
		payloadBytes = []byte(p)
	case []byte:
		payloadBytes = p
	default:
		marshaled, err := json.Marshal(p)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to serialize test payload: %v", err), http.StatusBadRequest)
			return
		}
		payloadBytes = marshaled
	}

	start := time.Now()
	output, drop, err := s.transformEngine.ExecuteTransform(targetTransform, payloadBytes)
	elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0

	if err != nil {
		http.Error(w, fmt.Sprintf("execution failed: %v", err), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "success",
		"drop":       drop,
		"output":     string(output),
		"latency_ms": elapsedMs,
	})
}

func (s *Server) handleConnectors(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")

	path := strings.Trim(r.URL.Path, "/")
	if path == "api/connectors" || path == "connectors" {
		switch r.Method {
		case http.MethodGet:
			connectors := s.connectorManager.ListConnectors()
			names := make([]string, 0, len(connectors))
			for _, c := range connectors {
				names = append(names, c.Name)
			}
			json.NewEncoder(w).Encode(names)
			return

		case http.MethodPost:
			var req struct {
				Name       string            `json:"name"`
				Type       string            `json:"type"`
				Class      string            `json:"class"`
				Topic      string            `json:"topic"`
				Config     map[string]string `json:"config"`
				TasksCount int               `json:"tasks_count"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
				return
			}

			// Kafka Connect format fallback
			if req.Class == "" && req.Config != nil {
				if val, ok := req.Config["connector.class"]; ok {
					req.Class = val
				}
			}
			if req.Topic == "" && req.Config != nil {
				if val, ok := req.Config["topics"]; ok {
					req.Topic = val
				}
			}
			if req.Name == "" && req.Config != nil {
				if val, ok := req.Config["name"]; ok {
					req.Name = val
				}
			}
			if req.TasksCount <= 0 && req.Config != nil {
				if val, ok := req.Config["tasks.max"]; ok {
					if n, err := strconv.Atoi(val); err == nil && n > 0 {
						req.TasksCount = n
					}
				}
			}

			if req.Name == "" || req.Class == "" {
				writeError(w, http.StatusBadRequest, 400, "name and class are required")
				return
			}

			conn := &connect.Connector{
				Name:       req.Name,
				Type:       connect.ConnectorType(req.Type),
				Class:      req.Class,
				Topic:      req.Topic,
				Config:     req.Config,
				TasksCount: req.TasksCount,
			}

			if err := s.connectorManager.RegisterConnector(conn); err != nil {
				if errors.Is(err, connect.ErrConnectorExists) {
					writeError(w, http.StatusConflict, 409, fmt.Sprintf("Connector %s already exists", req.Name))
					return
				}
				writeError(w, http.StatusBadRequest, 400, err.Error())
				return
			}

			created, _ := s.connectorManager.GetConnector(req.Name)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(created)
			return

		default:
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
	}

	sub := ""
	if strings.HasPrefix(path, "api/connectors/") {
		sub = strings.TrimPrefix(path, "api/connectors/")
	} else if strings.HasPrefix(path, "connectors/") {
		sub = strings.TrimPrefix(path, "connectors/")
	} else {
		writeError(w, http.StatusNotFound, 404, "Not found")
		return
	}

	sub = strings.Trim(sub, "/")
	parts := strings.Split(sub, "/")
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		writeError(w, http.StatusBadRequest, 400, "Invalid connector name")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			c, err := s.connectorManager.GetConnector(name)
			if err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			json.NewEncoder(w).Encode(c)
			return

		case http.MethodDelete:
			if err := s.connectorManager.DeleteConnector(name); err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"deleted": true,
				"name":    name,
			})
			return

		default:
			writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
			return
		}
	}

	if len(parts) >= 2 {
		action := parts[1]
		switch action {
		case "status":
			if r.Method != http.MethodGet {
				writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
				return
			}
			c, err := s.connectorManager.GetConnector(name)
			if err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			workerID := "aeromq-controller-0"
			tasks := make([]map[string]interface{}, 0, c.TasksCount)
			for i := 0; i < c.TasksCount; i++ {
				tasks = append(tasks, map[string]interface{}{
					"id":        i,
					"state":     string(c.State),
					"worker_id": workerID,
				})
			}
			resp := map[string]interface{}{
				"name": name,
				"connector": map[string]interface{}{
					"state":     string(c.State),
					"worker_id": workerID,
				},
				"tasks":             tasks,
				"type":              strings.ToLower(string(c.Type)),
				"state":             string(c.State),
				"records_processed": c.RecordsProcessed,
				"bytes_transferred": c.BytesTransferred,
			}
			json.NewEncoder(w).Encode(resp)
			return

		case "pause":
			if r.Method != http.MethodPut && r.Method != http.MethodPost {
				writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
				return
			}
			if err := s.connectorManager.PauseConnector(name); err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":  name,
				"state": string(connect.StatePaused),
			})
			return

		case "resume":
			if r.Method != http.MethodPut && r.Method != http.MethodPost {
				writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
				return
			}
			if err := s.connectorManager.ResumeConnector(name); err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"name":  name,
				"state": string(connect.StateRunning),
			})
			return

		case "config":
			c, err := s.connectorManager.GetConnector(name)
			if err != nil {
				writeError(w, http.StatusNotFound, 404, fmt.Sprintf("Connector %s not found", name))
				return
			}
			json.NewEncoder(w).Encode(c.Config)
			return

		default:
			writeError(w, http.StatusNotFound, 404, "Not found")
			return
		}
	}

	writeError(w, http.StatusNotFound, 404, "Not found")
}

func (s *Server) handleConnectorsDetail(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}
	connectors := s.connectorManager.ListConnectors()
	json.NewEncoder(w).Encode(connectors)
}

func (s *Server) handleConnectorPlugins(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
		return
	}
	plugins := s.connectorManager.ListPlugins()
	json.NewEncoder(w).Encode(plugins)
}




