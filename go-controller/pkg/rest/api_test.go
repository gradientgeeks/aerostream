package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/schemaregistry"
)

const sampleAvro = `{"type": "record", "name": "Payment", "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}`
const sampleAvroWithDefault = `{"type": "record", "name": "Payment", "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}, {"name": "currency", "type": "string", "default": "USD"}]}`

func TestSchemaRegistryRESTEndpoints(t *testing.T) {
	reg := schemaregistry.NewRegistry()
	server := rest.NewServer(nil, "127.0.0.1:0", reg)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. POST /subjects/{subject}/versions
	regBody, _ := json.Marshal(map[string]interface{}{
		"schema":     sampleAvro,
		"schemaType": "AVRO",
	})
	req := httptest.NewRequest(http.MethodPost, "/subjects/payments-value/versions", bytes.NewReader(regBody))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || res["id"] == nil {
		t.Fatalf("unexpected response: %s", w.Body.String())
	}
	id := int64(res["id"].(float64))

	// 2. GET /subjects
	req = httptest.NewRequest(http.MethodGet, "/subjects", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var subjects []string
	if err := json.Unmarshal(w.Body.Bytes(), &subjects); err != nil || len(subjects) != 1 || subjects[0] != "payments-value" {
		t.Fatalf("unexpected subjects: %v", subjects)
	}

	// 3. GET /subjects/{subject}/versions
	req = httptest.NewRequest(http.MethodGet, "/subjects/payments-value/versions", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var versions []int
	if err := json.Unmarshal(w.Body.Bytes(), &versions); err != nil || len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("unexpected versions: %v", versions)
	}

	// 4. GET /subjects/{subject}/versions/{version}
	req = httptest.NewRequest(http.MethodGet, "/subjects/payments-value/versions/latest", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var schemaObj schemaregistry.Schema
	if err := json.Unmarshal(w.Body.Bytes(), &schemaObj); err != nil || schemaObj.ID != id || schemaObj.Version != 1 {
		t.Fatalf("unexpected schema response: %+v", schemaObj)
	}

	// 5. GET /schemas/ids/{id}
	req = httptest.NewRequest(http.MethodGet, "/schemas/ids/1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var idResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &idResp); err != nil || idResp["schema"] == "" {
		t.Fatalf("unexpected schema by id: %s", w.Body.String())
	}

	// 6. POST /compatibility/subjects/{subject}/versions/{version}
	compBody, _ := json.Marshal(map[string]interface{}{
		"schema": sampleAvroWithDefault,
	})
	req = httptest.NewRequest(http.MethodPost, "/compatibility/subjects/payments-value/versions/1", bytes.NewReader(compBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var compResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &compResp); err != nil || compResp["is_compatible"] != true {
		t.Fatalf("expected is_compatible: true, got %s", w.Body.String())
	}

	// 7. GET /config and PUT /config
	req = httptest.NewRequest(http.MethodGet, "/config", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	putCfg, _ := json.Marshal(map[string]interface{}{"compatibility": "FORWARD"})
	req = httptest.NewRequest(http.MethodPut, "/config", bytes.NewReader(putCfg))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// 8. GET /config/{subject} and PUT /config/{subject}
	putSubj, _ := json.Marshal(map[string]interface{}{"compatibility": "NONE"})
	req = httptest.NewRequest(http.MethodPut, "/config/payments-value", bytes.NewReader(putSubj))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/config/payments-value", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var getSubjResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &getSubjResp); err != nil || getSubjResp["compatibility"] != "NONE" {
		t.Fatalf("expected NONE, got %s", w.Body.String())
	}
}

func TestPhase5SchemaRegistryOrdersValueEndToEnd(t *testing.T) {
	reg := schemaregistry.NewRegistry()
	server := rest.NewServer(nil, "127.0.0.1:0", reg)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. Register an Avro schema via HTTP POST /subjects/orders-value/versions
	initialSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}`
	regBody, _ := json.Marshal(map[string]interface{}{
		"schema":     initialSchema,
		"schemaType": "AVRO",
	})
	req := httptest.NewRequest(http.MethodPost, "/subjects/orders-value/versions", bytes.NewReader(regBody))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /subjects/orders-value/versions failed (code %d): %s", w.Code, w.Body.String())
	}
	var regResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &regResp); err != nil || regResp["id"] == nil {
		t.Fatalf("invalid register response: %s", w.Body.String())
	}
	assignedID := int64(regResp["id"].(float64))
	if assignedID != 1 {
		t.Fatalf("expected assigned id 1, got %d", assignedID)
	}

	// 2. Retrieve schema by ID via GET /schemas/ids/1
	req = httptest.NewRequest(http.MethodGet, "/schemas/ids/1", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /schemas/ids/1 failed (code %d): %s", w.Code, w.Body.String())
	}
	var idResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &idResp); err != nil {
		t.Fatalf("invalid id response: %s", w.Body.String())
	}
	schemaStr, _ := idResp["schema"].(string)
	if !strings.Contains(schemaStr, "Order") || !strings.Contains(schemaStr, "amount") {
		t.Fatalf("unexpected schema content: %s", schemaStr)
	}

	// 3. Retrieve latest version via GET /subjects/orders-value/versions/latest
	req = httptest.NewRequest(http.MethodGet, "/subjects/orders-value/versions/latest", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /subjects/orders-value/versions/latest failed (code %d): %s", w.Code, w.Body.String())
	}
	var latestResp schemaregistry.Schema
	if err := json.Unmarshal(w.Body.Bytes(), &latestResp); err != nil {
		t.Fatalf("invalid latest response: %s", w.Body.String())
	}
	if latestResp.Subject != "orders-value" || latestResp.Version != 1 || latestResp.ID != 1 {
		t.Fatalf("unexpected latest version metadata: %+v", latestResp)
	}

	// 4. Test compatibility endpoint:
	// 4a. Verify backward compatible schema passes (adds 'status' field with default 'PENDING')
	compatSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"},{"name":"status","type":"string","default":"PENDING"}]}`
	compatReqBody, _ := json.Marshal(map[string]interface{}{
		"schema":     compatSchema,
		"schemaType": "AVRO",
	})
	req = httptest.NewRequest(http.MethodPost, "/compatibility/subjects/orders-value/versions/latest", bytes.NewReader(compatReqBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /compatibility/subjects/orders-value/versions/latest failed (code %d): %s", w.Code, w.Body.String())
	}
	var compatResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &compatResp); err != nil {
		t.Fatalf("invalid compatibility response: %s", w.Body.String())
	}
	if compatResp["is_compatible"] != true {
		t.Fatalf("expected compatible schema to return is_compatible: true, got %+v", compatResp)
	}

	// 4b. Verify incompatible schema returns false (adds new field 'priority' WITHOUT a default value)
	incompatSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"},{"name":"priority","type":"int"}]}`
	incompatReqBody, _ := json.Marshal(map[string]interface{}{
		"schema":     incompatSchema,
		"schemaType": "AVRO",
	})
	req = httptest.NewRequest(http.MethodPost, "/compatibility/subjects/orders-value/versions/latest", bytes.NewReader(incompatReqBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST incompatible compatibility failed (code %d): %s", w.Code, w.Body.String())
	}
	var incompatResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &incompatResp); err != nil {
		t.Fatalf("invalid incompatible response: %s", w.Body.String())
	}
	if incompatResp["is_compatible"] != false {
		t.Fatalf("expected incompatible schema to return is_compatible: false, got %+v", incompatResp)
	}
}
