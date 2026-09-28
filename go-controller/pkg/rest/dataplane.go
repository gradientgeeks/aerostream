package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/dataplane"
)

// registerDataplaneRoutes wires client-quota and topic-compression admin endpoints.
func (s *Server) registerDataplaneRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/quotas", s.handleQuotas)
	mux.HandleFunc("/api/topic-compression", s.handleTopicCompression)
	mux.HandleFunc("/api/topic-compression/", s.handleTopicCompression)
}

// GET /api/quotas            list
// POST|PUT /api/quotas       upsert {user, client_id, producer_byte_rate, consumer_byte_rate, request_percentage}
// DELETE /api/quotas?user=&client_id=
func (s *Server) handleQuotas(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	store := dataplane.Default()
	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(store.ListQuotas())
	case http.MethodPost, http.MethodPut:
		var q dataplane.Quota
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
			return
		}
		if err := store.UpsertQuota(&q); err != nil {
			writeError(w, http.StatusBadRequest, 400, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(q)
	case http.MethodDelete:
		ok, err := store.DeleteQuota(r.URL.Query().Get("user"), r.URL.Query().Get("client_id"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, 404, "quota not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
	}
}

// GET /api/topic-compression                   map topic -> compression.type
// PUT|POST /api/topic-compression              {topic, compression_type}
// DELETE /api/topic-compression/{topic}
func (s *Server) handleTopicCompression(w http.ResponseWriter, r *http.Request) {
	if s.enableCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	store := dataplane.Default()
	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(store.TopicCompression())
	case http.MethodPost, http.MethodPut:
		var body struct {
			Topic           string `json:"topic"`
			CompressionType string `json:"compression_type"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, 400, fmt.Sprintf("Invalid JSON request body: %v", err))
			return
		}
		if err := store.SetTopicCompression(body.Topic, body.CompressionType); err != nil {
			writeError(w, http.StatusBadRequest, 400, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"topic": body.Topic, "compression_type": strings.ToLower(body.CompressionType)})
	case http.MethodDelete:
		topic := strings.TrimPrefix(r.URL.Path, "/api/topic-compression/")
		ok, err := store.DeleteTopicCompression(topic)
		if err != nil {
			writeError(w, http.StatusInternalServerError, 500, err.Error())
			return
		}
		if !ok {
			writeError(w, http.StatusNotFound, 404, "no compression override for topic")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, 405, "Method not allowed")
	}
}
