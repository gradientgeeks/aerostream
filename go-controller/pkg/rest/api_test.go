package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/auth"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/consensus"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/schemaregistry"
	"github.com/hashicorp/raft"
	"time"
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

	// 9. DELETE /subjects/{subject}
	req = httptest.NewRequest(http.MethodDelete, "/subjects/payments-value", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 from DELETE /subjects/payments-value, got %d: %s", w.Code, w.Body.String())
	}
	var deletedVersions []int
	if err := json.Unmarshal(w.Body.Bytes(), &deletedVersions); err != nil || len(deletedVersions) != 1 || deletedVersions[0] != 1 {
		t.Fatalf("expected deleted versions [1], got %v", deletedVersions)
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

func TestAclRESTEndpoints(t *testing.T) {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. GET /api/users
	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/users failed (code %d): %s", w.Code, w.Body.String())
	}
	var users []*auth.User
	if err := json.Unmarshal(w.Body.Bytes(), &users); err != nil || len(users) < 4 {
		t.Fatalf("expected at least 4 seeded users, got %v", users)
	}

	// 2. POST /api/users
	newUserBody, _ := json.Marshal(map[string]interface{}{
		"username": "analytics_bot",
		"role":     "CONSUMER",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewReader(newUserBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST /api/users failed (code %d): %s", w.Code, w.Body.String())
	}

	// 3. GET /api/acls
	req = httptest.NewRequest(http.MethodGet, "/api/acls", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/acls failed (code %d): %s", w.Code, w.Body.String())
	}
	var rules []*auth.AclRule
	if err := json.Unmarshal(w.Body.Bytes(), &rules); err != nil || len(rules) < 6 {
		t.Fatalf("expected at least 6 seeded rules, got %v", rules)
	}

	// 4. POST /api/acls
	newRuleBody, _ := json.Marshal(map[string]interface{}{
		"principal":     "User:analytics_bot",
		"resource_type": "TOPIC",
		"resource_name": "analytics-*",
		"operation":     "READ",
		"permission":    "ALLOW",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/acls", bytes.NewReader(newRuleBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST /api/acls failed (code %d): %s", w.Code, w.Body.String())
	}
	var createdRule auth.AclRule
	if err := json.Unmarshal(w.Body.Bytes(), &createdRule); err != nil || createdRule.ID == "" {
		t.Fatalf("invalid created rule: %s", w.Body.String())
	}

	// 5. POST /api/acls/test -> Live authorization check
	testBody, _ := json.Marshal(map[string]interface{}{
		"principal":     "User:analytics_bot",
		"resource_type": "TOPIC",
		"resource_name": "analytics-pageviews",
		"operation":     "READ",
	})
	req = httptest.NewRequest(http.MethodPost, "/api/acls/test", bytes.NewReader(testBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /api/acls/test failed (code %d): %s", w.Code, w.Body.String())
	}
	var testResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &testResp); err != nil || testResp["allowed"] != true {
		t.Fatalf("expected allowed: true in test evaluation, got %+v", testResp)
	}

	// 6. DELETE /api/acls/{id}
	req = httptest.NewRequest(http.MethodDelete, "/api/acls/"+createdRule.ID, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("DELETE /api/acls/{id} failed (code %d): %s", w.Code, w.Body.String())
	}

	// 7. Verify test again after deletion -> should now be denied by zero trust
	req = httptest.NewRequest(http.MethodPost, "/api/acls/test", bytes.NewReader(testBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /api/acls/test after delete failed (code %d): %s", w.Code, w.Body.String())
	}
	var testRespDenied map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &testRespDenied); err != nil || testRespDenied["allowed"] != false {
		t.Fatalf("expected allowed: false after deleting rule, got %+v", testRespDenied)
	}
}

func TestTransformsRESTEndpoints(t *testing.T) {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. GET /api/transforms -> should contain seeded transforms
	req := httptest.NewRequest(http.MethodGet, "/api/transforms", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/transforms failed (code %d): %s", w.Code, w.Body.String())
	}
	var transforms []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &transforms); err != nil || len(transforms) < 2 {
		t.Fatalf("expected at least 2 seeded transforms, got %d (body: %s)", len(transforms), w.Body.String())
	}

	// 2. POST /api/transforms -> register new transform
	newXform := map[string]interface{}{
		"name":         "test-audit-pipeline",
		"source_topic": "audit-raw",
		"target_topic": "audit-clean",
		"type":         "JSON_MAP",
		"config": map[string]string{
			"set_environment": "staging",
		},
	}
	body, _ := json.Marshal(newXform)
	req = httptest.NewRequest(http.MethodPost, "/api/transforms", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST /api/transforms failed (code %d): %s", w.Code, w.Body.String())
	}

	// 3. POST /api/transforms/{name}/pause
	req = httptest.NewRequest(http.MethodPost, "/api/transforms/test-audit-pipeline/pause", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST pause failed (code %d): %s", w.Code, w.Body.String())
	}
	var pauseResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &pauseResp)
	if pauseResp["status"] != "PAUSED" {
		t.Errorf("expected status PAUSED, got %v", pauseResp["status"])
	}

	// 4. POST /api/transforms/{name}/resume
	req = httptest.NewRequest(http.MethodPost, "/api/transforms/test-audit-pipeline/resume", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST resume failed (code %d): %s", w.Code, w.Body.String())
	}
	var resumeResp map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resumeResp)
	if resumeResp["status"] != "RUNNING" {
		t.Errorf("expected status RUNNING, got %v", resumeResp["status"])
	}

	// 5. POST /api/transforms/test -> test PII Masking with sample payload
	testPIIBody, _ := json.Marshal(map[string]interface{}{
		"transform_name": "pii-masker-orders",
		"payload": map[string]interface{}{
			"order_id":    "ord-100",
			"credit_card": "4111-2222-3333-4444",
			"amount":      250.0,
		},
	})
	req = httptest.NewRequest(http.MethodPost, "/api/transforms/test", bytes.NewReader(testPIIBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /api/transforms/test failed (code %d): %s", w.Code, w.Body.String())
	}
	var piiResult map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &piiResult); err != nil {
		t.Fatalf("invalid json response from test: %v", err)
	}
	if piiResult["drop"] != false {
		t.Errorf("expected drop=false for PII test")
	}
	outputStr, ok := piiResult["output"].(string)
	if !ok || !strings.Contains(outputStr, "***") {
		t.Errorf("expected masked output to contain '***', got: %s", outputStr)
	}

	// 6. POST /api/transforms/test -> test Filter transform with dropping payload
	testFilterBody, _ := json.Marshal(map[string]interface{}{
		"transform_name": "telemetry-filter-critical",
		"payload":        `{"device_id": "d-99", "level": "DEBUG"}`,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/transforms/test", bytes.NewReader(testFilterBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /api/transforms/test (filter) failed: %d", w.Code)
	}
	var filterResult map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &filterResult)
	if filterResult["drop"] != true {
		t.Errorf("expected drop=true for DEBUG telemetry record, got %v", filterResult["drop"])
	}

	// 7. DELETE /api/transforms/{name}
	req = httptest.NewRequest(http.MethodDelete, "/api/transforms/test-audit-pipeline", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("DELETE /api/transforms failed (code %d): %s", w.Code, w.Body.String())
	}

	// 8. GET /api/transforms/test-audit-pipeline -> should now be 404
	req = httptest.NewRequest(http.MethodGet, "/api/transforms/test-audit-pipeline", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}
}

func TestConnectorsRESTEndpoints(t *testing.T) {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. GET /api/connectors -> list active connector names
	req := httptest.NewRequest(http.MethodGet, "/api/connectors", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/connectors failed (code %d): %s", w.Code, w.Body.String())
	}
	var names []string
	if err := json.Unmarshal(w.Body.Bytes(), &names); err != nil || len(names) != 2 {
		t.Fatalf("expected 2 seeded connectors, got %v", names)
	}

	// Also verify Kafka Connect alias /connectors
	req = httptest.NewRequest(http.MethodGet, "/connectors", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /connectors failed (code %d): %s", w.Code, w.Body.String())
	}

	// 2. GET /api/connectors-detail -> list connector objects with configs & metrics
	req = httptest.NewRequest(http.MethodGet, "/api/connectors-detail", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/connectors-detail failed (code %d): %s", w.Code, w.Body.String())
	}
	var detail []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil || len(detail) != 2 {
		t.Fatalf("expected 2 connector details, got %d", len(detail))
	}

	// 3. GET /api/connector-plugins -> list installed connector plugins
	req = httptest.NewRequest(http.MethodGet, "/api/connector-plugins", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/connector-plugins failed (code %d): %s", w.Code, w.Body.String())
	}
	var plugins []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &plugins); err != nil || len(plugins) != 4 {
		t.Fatalf("expected 4 plugins, got %d", len(plugins))
	}

	// 4. POST /api/connectors -> deploy new connector
	newConn := map[string]interface{}{
		"name":  "es-audit-sink",
		"class": "ElasticSearchSinkConnector",
		"topic": "audit-events",
		"config": map[string]string{
			"es.host":    "http://elasticsearch:9200",
			"es.index":   "audit-logs",
			"batch.size": "500",
		},
	}
	body, _ := json.Marshal(newConn)
	req = httptest.NewRequest(http.MethodPost, "/api/connectors", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST /api/connectors failed (code %d): %s", w.Code, w.Body.String())
	}

	// 5. GET /api/connectors/{name}/status -> status & tasks
	req = httptest.NewRequest(http.MethodGet, "/api/connectors/es-audit-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/connectors/es-audit-sink/status failed (code %d): %s", w.Code, w.Body.String())
	}
	var statusResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("invalid status response: %v", err)
	}
	if statusResp["state"] != "RUNNING" {
		t.Errorf("expected state RUNNING, got %v", statusResp["state"])
	}
	tasks, ok := statusResp["tasks"].([]interface{})
	if !ok || len(tasks) != 1 {
		t.Errorf("expected 1 task in status, got %v", statusResp["tasks"])
	}

	// 6. PUT /api/connectors/{name}/pause -> pause connector
	req = httptest.NewRequest(http.MethodPut, "/api/connectors/es-audit-sink/pause", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("PUT pause failed (code %d): %s", w.Code, w.Body.String())
	}

	// Verify state is PAUSED
	req = httptest.NewRequest(http.MethodGet, "/api/connectors/es-audit-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var pausedStatus map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &pausedStatus)
	if pausedStatus["state"] != "PAUSED" {
		t.Errorf("expected state PAUSED, got %v", pausedStatus["state"])
	}

	// 7. PUT /api/connectors/{name}/resume -> resume connector
	req = httptest.NewRequest(http.MethodPut, "/api/connectors/es-audit-sink/resume", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("PUT resume failed (code %d): %s", w.Code, w.Body.String())
	}

	// Verify state is RUNNING
	req = httptest.NewRequest(http.MethodGet, "/api/connectors/es-audit-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resumedStatus map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &resumedStatus)
	if resumedStatus["state"] != "RUNNING" {
		t.Errorf("expected state RUNNING, got %v", resumedStatus["state"])
	}

	// 8. DELETE /api/connectors/{name} -> delete connector
	req = httptest.NewRequest(http.MethodDelete, "/api/connectors/es-audit-sink", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("DELETE failed (code %d): %s", w.Code, w.Body.String())
	}

	// Verify connector is deleted
	req = httptest.NewRequest(http.MethodGet, "/api/connectors/es-audit-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}
}

func TestConsumerGroupsRESTEndpoints(t *testing.T) {
	fsm := consensus.NewFSM(time.Minute, 10)
	rn := &consensus.RaftNode{
		FSM:    fsm,
		NodeID: "test-node",
	}
	server := rest.NewServer(rn, "127.0.0.1:0")

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// 1. GET /api/groups initially returns empty list
	req := httptest.NewRequest(http.MethodGet, "/api/groups", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var emptyGroups []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &emptyGroups); err != nil || len(emptyGroups) != 0 {
		t.Fatalf("expected empty groups, got %s", w.Body.String())
	}

	// 2. Add topic and consumer group member into FSM
	rawBroker, _ := json.Marshal(struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{ID: 1, Host: "127.0.0.1", Port: 9001})
	fsm.Apply(&raft.Log{Data: mustMarshalCmd(t, consensus.CmdRegisterBroker, rawBroker)})

	rawTopic, _ := json.Marshal(struct {
		Name              string `json:"name"`
		Partitions        uint32 `json:"partitions"`
		ReplicationFactor uint32 `json:"replication_factor"`
	}{Name: "orders", Partitions: 4, ReplicationFactor: 1})
	fsm.Apply(&raft.Log{Data: mustMarshalCmd(t, consensus.CmdCreateTopic, rawTopic)})

	rawJoin, _ := json.Marshal(struct {
		GroupID    string   `json:"group_id"`
		MemberID   string   `json:"member_id"`
		Topics     []string `json:"topics"`
		ClientHost string   `json:"client_host"`
		UserAgent  string   `json:"user_agent"`
	}{
		GroupID:    "orders-processors",
		MemberID:   "worker-1",
		Topics:     []string{"orders"},
		ClientHost: "10.0.0.5",
		UserAgent:  "aeromq-go-worker/2.0",
	})
	fsm.Apply(&raft.Log{Data: mustMarshalCmd(t, consensus.CmdJoinConsumerGroup, rawJoin)})

	// 3. GET /api/groups should return group with cooperative sticky metadata
	req = httptest.NewRequest(http.MethodGet, "/api/groups", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var groups []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &groups); err != nil || len(groups) != 1 {
		t.Fatalf("expected 1 group, got %s", w.Body.String())
	}
	grp := groups[0]
	if grp["protocol"] != "COOPERATIVE_STICKY" {
		t.Errorf("expected protocol COOPERATIVE_STICKY, got %v", grp["protocol"])
	}
	if grp["state"] != "STABLE" {
		t.Errorf("expected state STABLE, got %v", grp["state"])
	}
	if grp["leader_id"] != "worker-1" {
		t.Errorf("expected leader worker-1, got %v", grp["leader_id"])
	}
	if grp["generation"].(float64) != 1 {
		t.Errorf("expected generation 1, got %v", grp["generation"])
	}
	if grp["rebalance_count"].(float64) != 1 {
		t.Errorf("expected rebalance_count 1, got %v", grp["rebalance_count"])
	}

	// 4. POST /api/groups/{id}/rebalance for nonexistent group -> 404
	req = httptest.NewRequest(http.MethodPost, "/api/groups/nonexistent-group/rebalance", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent group, got %d", w.Code)
	}

	// 5. POST /api/groups/{id}/rebalance for existing group -> 200 and increments generation
	req = httptest.NewRequest(http.MethodPost, "/api/groups/orders-processors/rebalance", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on rebalance, got %d: %s", w.Code, w.Body.String())
	}

	var rebResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &rebResp); err != nil {
		t.Fatalf("invalid rebalance response: %s", w.Body.String())
	}
	if rebResp["success"] != true {
		t.Errorf("expected success true, got %v", rebResp["success"])
	}
	if rebResp["protocol"] != "COOPERATIVE_STICKY" {
		t.Errorf("expected protocol COOPERATIVE_STICKY, got %v", rebResp["protocol"])
	}
	if rebResp["state"] != "STABLE" {
		t.Errorf("expected state STABLE, got %v", rebResp["state"])
	}
	if rebResp["generation"].(float64) != 2 {
		t.Errorf("expected generation 2, got %v", rebResp["generation"])
	}
	if rebResp["rebalance_count"].(float64) != 2 {
		t.Errorf("expected rebalance_count 2, got %v", rebResp["rebalance_count"])
	}
}

func mustMarshalCmd(t *testing.T, op string, payload []byte) []byte {
	t.Helper()
	cmd := consensus.Command{
		Op:      op,
		Payload: payload,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command failed: %v", err)
	}
	return data
}

func TestDrainBrokerRESTEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	fsm := consensus.NewFSM(time.Minute, 10)
	rn := &consensus.RaftNode{FSM: fsm}
	srv := rest.NewServer(rn, ":9001", schemaregistry.NewRegistry())
	srv.RegisterRoutes(mux)

	// Register broker 1 and 2
	b1, _ := json.Marshal(struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{ID: 1, Host: "127.0.0.1", Port: 9001})
	fsm.Apply(&raft.Log{Data: mustMarshalCmd(t, consensus.CmdRegisterBroker, b1)})

	b2, _ := json.Marshal(struct {
		ID   uint32 `json:"id"`
		Host string `json:"host"`
		Port int32  `json:"port"`
	}{ID: 2, Host: "127.0.0.1", Port: 9002})
	fsm.Apply(&raft.Log{Data: mustMarshalCmd(t, consensus.CmdRegisterBroker, b2)})

	// Call POST /api/brokers/1/drain
	req := httptest.NewRequest(http.MethodPost, "/api/brokers/1/drain", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp["success"] != true {
		t.Fatalf("expected success true, got %v", resp)
	}

	meta := fsm.GetMetadata(nil)
	if meta.Brokers[1].Active {
		t.Errorf("expected broker 1 to be marked inactive after drain")
	}
}
