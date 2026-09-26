package rest

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gradientgeeks/aeromq/go-controller/pkg/consensus"
)

type Server struct {
	raftNode *consensus.RaftNode
	httpAddr string
}

func NewServer(raftNode *consensus.RaftNode, httpAddr string) *Server {
	return &Server{
		raftNode: raftNode,
		httpAddr: httpAddr,
	}
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
	mux.HandleFunc("/api/topics", s.handleTopics)
	mux.HandleFunc("/api/groups", s.handleGroups)
	mux.HandleFunc("/api/lag", s.handleLag)
	mux.HandleFunc("/api/produce", s.handleProduce)
	mux.HandleFunc("/api/messages", s.handleMessages)
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

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}

	if r.Method == http.MethodPost {
		var req struct {
			Name              string `json:"name"`
			Partitions        uint32 `json:"partitions"`
			ReplicationFactor uint32 `json:"replication_factor"`
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
			})
		}
	}
	json.NewEncoder(w).Encode(topics)
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	meta := s.raftNode.FSM.GetMetadata(nil)
	w.Header().Set("Content-Type", "application/json")

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
					members = append(members, map[string]interface{}{
						"id":          m.ID,
						"topics":      topics,
						"last_seen":   m.LastSeen,
						"assignments": assigned,
					})
				}
			}

			assignments := g.Assignments
			if assignments == nil {
				assignments = make(map[string][]consensus.PartitionTopic)
			}

			groups = append(groups, map[string]interface{}{
				"group_id":    g.GroupID,
				"generation":  g.Generation,
				"members":     members,
				"assignments": assignments,
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
