package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
)

func TestQuotaAndCompressionREST(t *testing.T) {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	do := func(method, url string, body interface{}) *httptest.ResponseRecorder {
		var rd *bytes.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		} else {
			rd = bytes.NewReader(nil)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, url, rd))
		return w
	}
	if w := do(http.MethodPost, "/api/quotas", map[string]interface{}{"client_id": "rest-test-c", "producer_byte_rate": 1024}); w.Code != http.StatusCreated {
		t.Fatalf("upsert: %d %s", w.Code, w.Body.String())
	}
	if w := do(http.MethodPost, "/api/quotas", map[string]interface{}{"client_id": "x"}); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", w.Code)
	}
	w := do(http.MethodGet, "/api/quotas", nil)
	var list []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &list)
	found := false
	for _, q := range list {
		if q["client_id"] == "rest-test-c" && q["producer_byte_rate"] == float64(1024) {
			found = true
		}
	}
	if !found {
		t.Fatalf("quota not listed: %s", w.Body.String())
	}
	if w := do(http.MethodDelete, "/api/quotas?client_id=rest-test-c", nil); w.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", w.Code)
	}
	if w := do(http.MethodPut, "/api/topic-compression", map[string]string{"topic": "rest-topic", "compression_type": "lz4"}); w.Code != http.StatusOK {
		t.Fatalf("set compression: %d %s", w.Code, w.Body.String())
	}
	if w := do(http.MethodPut, "/api/topic-compression", map[string]string{"topic": "rest-topic", "compression_type": "nope"}); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 got %d", w.Code)
	}
	if w := do(http.MethodDelete, "/api/topic-compression/rest-topic", nil); w.Code != http.StatusNoContent {
		t.Fatalf("delete compression: %d", w.Code)
	}
}
