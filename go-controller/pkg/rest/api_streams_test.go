package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
)

func TestStreamsREST(t *testing.T) {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	do := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		return rr
	}

	rr := do("POST", "/api/streams", `{"name":"spend","type":"AGGREGATE","source_topic":"orders","key_field":"user","value_field":"amt","agg":"sum","window":{"type":"tumbling","size_ms":60000}}`)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	if rr = do("POST", "/api/streams", `{"name":"spend","type":"AGGREGATE","source_topic":"orders"}`); rr.Code != http.StatusConflict && rr.Code != http.StatusBadRequest {
		t.Fatalf("dup: %d", rr.Code)
	}
	for _, v := range []string{`{"user":"ann","amt":10}`, `{"user":"ann","amt":5.5}`} {
		rr = do("POST", "/api/streams/spend/ingest", `{"value":`+v+`,"timestamp_ms":61000}`)
		if rr.Code != 200 {
			t.Fatalf("ingest: %d %s", rr.Code, rr.Body.String())
		}
	}
	rr = do("GET", "/api/streams/spend/state?key=ann", "")
	var out struct {
		Windows []struct {
			WindowStart int64   `json:"window_start"`
			Sum         float64 `json:"sum"`
			Count       int64   `json:"count"`
		} `json:"windows"`
	}
	json.Unmarshal(rr.Body.Bytes(), &out)
	if len(out.Windows) != 1 || out.Windows[0].Sum != 15.5 || out.Windows[0].Count != 2 || out.Windows[0].WindowStart != 60000 {
		t.Fatalf("state: %s", rr.Body.String())
	}
	if rr = do("GET", "/api/streams/spend/state", ""); rr.Code != 200 {
		t.Fatal("keys")
	}
	if rr = do("POST", "/api/streams/spend/pause", ""); rr.Code != 200 {
		t.Fatal("pause")
	}
	if rr = do("DELETE", "/api/streams/spend", ""); rr.Code != 200 {
		t.Fatal("delete")
	}
	if rr = do("GET", "/api/streams/spend", ""); rr.Code != 404 {
		t.Fatal("expected 404")
	}
}
