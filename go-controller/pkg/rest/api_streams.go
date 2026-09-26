package rest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/streams"
)

// StreamEngine exposes the stateful stream processing engine.
func (s *Server) StreamEngine() *streams.Engine { return s.streamEngine }

func (s *Server) initStreams() {
	e, err := streams.NewEngine(os.Getenv("AEROSTREAM_STREAMS_DIR"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[AeroStream Streams] state restore failed (%v); continuing in-memory\n", err)
		e, _ = streams.NewEngine("")
	}
	s.streamEngine = e
	e.SetSink(s.streamSink)
}

// StartStreamPolling tails the topics consumed by stream jobs until ctx ends.
func (s *Server) StartStreamPolling(ctx context.Context) {
	if s.raftNode == nil {
		return
	}
	go s.streamEngine.Run(ctx, s.streamFetch, s.streamPartitions, 500*time.Millisecond)
}

func (s *Server) streamPartitions(topic string) []uint32 {
	meta := s.raftNode.FSM.GetMetadata([]string{topic})
	ts, ok := meta.Topics[topic]
	if !ok {
		return nil
	}
	var out []uint32
	for p := range ts.Partitions {
		out = append(out, p)
	}
	return out
}

func (s *Server) dialLeader(topic string, partition uint32) (net.Conn, error) {
	meta := s.raftNode.FSM.GetMetadata([]string{topic})
	ts, ok := meta.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	ps, ok := ts.Partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d not found", partition)
	}
	b, ok := meta.Brokers[ps.LeaderID]
	if !ok {
		return nil, errors.New("leader broker not found")
	}
	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", b.Host, b.Port), 3*time.Second)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	return c, nil
}

// streamFetch reads one payload from the broker data plane.
func (s *Server) streamFetch(topic string, partition uint32, offset uint64) ([]byte, bool, error) {
	conn, err := s.dialLeader(topic, partition)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	tb := []byte(topic)
	bodyLen := 2 + len(tb) + 4 + 8 + 4
	hdr := []byte{0xAE, 0x01, 2, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(hdr[3:7], uint32(bodyLen))
	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(tb)))
	copy(body[2:], tb)
	o := 2 + len(tb)
	binary.BigEndian.PutUint32(body[o:], partition)
	binary.BigEndian.PutUint64(body[o+4:], offset)
	binary.BigEndian.PutUint32(body[o+12:], 65536)
	if _, err := conn.Write(append(hdr, body...)); err != nil {
		return nil, false, err
	}
	rh := make([]byte, 3)
	if _, err := io.ReadFull(conn, rh); err != nil || rh[0] != 0xAE || rh[1] != 0x01 || rh[2] != 2 {
		return nil, false, nil // nothing at this offset yet
	}
	lb := make([]byte, 4)
	if _, err := io.ReadFull(conn, lb); err != nil {
		return nil, false, nil
	}
	payload := make([]byte, binary.BigEndian.Uint32(lb))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, false, nil
	}
	return payload, true, nil
}

// streamSink produces a job result into its target topic (partition 0).
func (s *Server) streamSink(topic, key string, value []byte) {
	if s.raftNode == nil {
		return
	}
	conn, err := s.dialLeader(topic, 0)
	if err != nil {
		return
	}
	defer conn.Close()
	tb := []byte(topic)
	bodyLen := 2 + len(tb) + 4 + 4 + len(value)
	hdr := []byte{0xAE, 0x01, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(hdr[3:7], uint32(bodyLen))
	body := make([]byte, bodyLen)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(tb)))
	copy(body[2:], tb)
	o := 2 + len(tb)
	binary.BigEndian.PutUint32(body[o:], 0)
	binary.BigEndian.PutUint32(body[o+4:], uint32(len(value)))
	copy(body[o+8:], value)
	if _, err := conn.Write(append(hdr, body...)); err != nil {
		return
	}
	io.ReadFull(conn, make([]byte, 11))
}

type streamJobView struct {
	streams.JobConfig
	Status  string             `json:"status"`
	Metrics streams.JobMetrics `json:"metrics"`
}

func jobView(j *streams.Job) streamJobView {
	st := "RUNNING"
	if j.Paused {
		st = "PAUSED"
	}
	return streamJobView{JobConfig: j.Config, Status: st, Metrics: j.Metrics()}
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		out := []streamJobView{}
		for _, j := range s.streamEngine.List() {
			out = append(out, jobView(j))
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodPost:
		var cfg streams.JobConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.streamEngine.Register(cfg); err != nil {
			code := http.StatusBadRequest
			if errors.Is(err, streams.ErrJobExists) {
				code = http.StatusConflict
			}
			http.Error(w, err.Error(), code)
			return
		}
		j, _ := s.streamEngine.Get(strings.TrimSpace(cfg.Name))
		writeJSON(w, http.StatusCreated, jobView(j))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStreamItem serves /api/streams/{name}[/action[/arg]].
func (s *Server) handleStreamItem(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	sub := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/streams/"), "/")
	if sub == "" {
		s.handleStreams(w, r)
		return
	}
	parts := strings.SplitN(sub, "/", 3)
	name, err := url.PathUnescape(parts[0])
	if err != nil {
		name = parts[0]
	}
	j, err := s.streamEngine.Get(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, jobView(j))
		case http.MethodDelete:
			s.streamEngine.Delete(name)
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "name": name})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	switch parts[1] {
	case "pause", "resume":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.streamEngine.SetPaused(name, parts[1] == "pause")
		writeJSON(w, http.StatusOK, jobView(j))
	case "ingest":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Topic     string          `json:"topic"`
			Key       string          `json:"key"`
			Value     json.RawMessage `json:"value"`
			Timestamp int64           `json:"timestamp_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Topic == "" {
			req.Topic = j.Config.SourceTopic
		}
		ts := time.Now()
		if req.Timestamp > 0 {
			ts = time.UnixMilli(req.Timestamp)
		}
		j.Process(streams.Record{Topic: req.Topic, Partition: -1, Key: req.Key, Value: req.Value, Timestamp: ts})
		writeJSON(w, http.StatusOK, jobView(j))
	case "results":
		limit := 50
		if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
			limit = l
		}
		writeJSON(w, http.StatusOK, j.Recent(limit))
	case "state":
		// Interactive query: ?key=K[&from=ms&to=ms] -> windows; without key -> key list.
		q := r.URL.Query()
		key := q.Get("key")
		if key == "" {
			writeJSON(w, http.StatusOK, map[string]interface{}{"keys": j.Keys()})
			return
		}
		if j.Config.Type == streams.JobJoin {
			row, ok := j.TableRow(key)
			if !ok {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"key": key, "table": row})
			return
		}
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		ws := j.Windows(key, from, to)
		if ws == nil {
			ws = []streams.WindowResult{}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"key": key, "windows": ws})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
	}
}
