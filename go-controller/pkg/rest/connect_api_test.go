package rest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/connect"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
)

func setupTestServer() *http.ServeMux {
	server := rest.NewServer(nil, "127.0.0.1:0")
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	return mux
}

func TestKafkaConnectREST_ListConnectors_Expand(t *testing.T) {
	mux := setupTestServer()

	// 1. GET /connectors without expand -> array of strings
	req := httptest.NewRequest(http.MethodGet, "/connectors", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var names []string
	if err := json.Unmarshal(w.Body.Bytes(), &names); err != nil {
		t.Fatalf("failed to decode list names: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 connectors, got %d", len(names))
	}

	// 2. GET /connectors?expand=status -> map with status
	req = httptest.NewRequest(http.MethodGet, "/connectors?expand=status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var statusMap map[string]map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &statusMap); err != nil {
		t.Fatalf("failed to decode expand=status response: %v", err)
	}
	if len(statusMap) != 2 {
		t.Fatalf("expected 2 connectors in expand map, got %d", len(statusMap))
	}
	s3Status, ok := statusMap["s3-cold-storage-sink"]["status"].(map[string]interface{})
	if !ok || s3Status["name"] != "s3-cold-storage-sink" {
		t.Fatalf("expected s3-cold-storage-sink status, got: %+v", statusMap["s3-cold-storage-sink"])
	}
	if s3Status["connector"].(map[string]interface{})["state"] != "RUNNING" {
		t.Fatalf("expected RUNNING state in connector status")
	}

	// 3. GET /connectors?expand=info -> map with info
	req = httptest.NewRequest(http.MethodGet, "/connectors?expand=info", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var infoMap map[string]map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &infoMap); err != nil {
		t.Fatalf("failed to decode expand=info response: %v", err)
	}
	s3Info, ok := infoMap["s3-cold-storage-sink"]["info"].(map[string]interface{})
	if !ok || s3Info["type"] != "sink" {
		t.Fatalf("expected s3-cold-storage-sink info, got: %+v", infoMap["s3-cold-storage-sink"])
	}

	// 4. GET /connectors?expand=status&expand=info -> both status and info
	req = httptest.NewRequest(http.MethodGet, "/connectors?expand=status&expand=info", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var bothMap map[string]map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &bothMap); err != nil {
		t.Fatalf("failed to decode both expand response: %v", err)
	}
	entry := bothMap["s3-cold-storage-sink"]
	if entry["status"] == nil || entry["info"] == nil {
		t.Fatalf("expected both status and info in response, got: %+v", entry)
	}
}

func TestKafkaConnectREST_ConnectorLifecycleAndTasks(t *testing.T) {
	mux := setupTestServer()

	// 1. POST /connectors -> create connector
	createPayload := map[string]interface{}{
		"name": "http-test-sink",
		"config": map[string]string{
			"connector.class": "HttpWebhookSinkConnector",
			"tasks.max":       "2",
			"topics":          "orders,notifications",
			"http.url":        "https://api.internal/webhook",
			"http.method":     "POST",
		},
	}
	body, _ := json.Marshal(createPayload)
	req := httptest.NewRequest(http.MethodPost, "/connectors", bytes.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", w.Code, w.Body.String())
	}
	var createdInfo map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &createdInfo); err != nil {
		t.Fatalf("failed to decode created info: %v", err)
	}
	if createdInfo["name"] != "http-test-sink" || createdInfo["type"] != "sink" {
		t.Fatalf("unexpected created info: %+v", createdInfo)
	}
	tasksList, ok := createdInfo["tasks"].([]interface{})
	if !ok || len(tasksList) != 2 {
		t.Fatalf("expected 2 tasks in info, got %v", createdInfo["tasks"])
	}

	// Duplicate POST should fail with 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/connectors", bytes.NewReader(body))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d", w.Code)
	}

	// 2. GET /connectors/{name} -> returns connector info
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var connInfo map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &connInfo); err != nil {
		t.Fatalf("failed to decode conn info: %v", err)
	}
	if connInfo["name"] != "http-test-sink" {
		t.Fatalf("expected connector name http-test-sink, got %v", connInfo["name"])
	}

	// 3. GET /connectors/{name}/config -> returns JSON config map
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/config", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var cfg map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if cfg["http.url"] != "https://api.internal/webhook" {
		t.Fatalf("expected http.url, got %v", cfg["http.url"])
	}

	// 4. PUT /connectors/{name}/config -> updates config, returns updated info
	updatedCfg := map[string]string{
		"connector.class": "HttpWebhookSinkConnector",
		"tasks.max":       "1",
		"topics":          "orders",
		"http.url":        "https://api.internal/v2/webhook",
	}
	upBody, _ := json.Marshal(updatedCfg)
	req = httptest.NewRequest(http.MethodPut, "/connectors/http-test-sink/config", bytes.NewReader(upBody))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var updatedInfo map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &updatedInfo)
	if updatedInfo["config"].(map[string]interface{})["http.url"] != "https://api.internal/v2/webhook" {
		t.Fatalf("expected updated http.url, got %v", updatedInfo["config"])
	}

	// 5. GET /connectors/{name}/status -> standard status
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var statusObj map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &statusObj); err != nil {
		t.Fatalf("failed to decode status: %v", err)
	}
	connState := statusObj["connector"].(map[string]interface{})["state"]
	if connState != "RUNNING" {
		t.Fatalf("expected RUNNING connector state, got %v", connState)
	}

	// 6. POST /connectors/{name}/restart -> returns 204
	req = httptest.NewRequest(http.MethodPost, "/connectors/http-test-sink/restart?includeTasks=true&onlyFailed=true", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content for restart, got %d", w.Code)
	}

	// 7. PUT /connectors/{name}/pause -> 202 Accepted
	req = httptest.NewRequest(http.MethodPut, "/connectors/http-test-sink/pause", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for pause, got %d", w.Code)
	}

	// 8. PUT /connectors/{name}/resume -> 202 Accepted
	req = httptest.NewRequest(http.MethodPut, "/connectors/http-test-sink/resume", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for resume, got %d", w.Code)
	}

	// 9. POST /connectors/{name}/stop (KIP-875) -> 202 Accepted
	req = httptest.NewRequest(http.MethodPost, "/connectors/http-test-sink/stop", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for stop, got %d", w.Code)
	}

	// Verify status shows STOPPED
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	_ = json.Unmarshal(w.Body.Bytes(), &statusObj)
	if statusObj["connector"].(map[string]interface{})["state"] != "STOPPED" {
		t.Fatalf("expected STOPPED state, got %v", statusObj["connector"])
	}

	// 10. Tasks endpoints
	// GET /connectors/{name}/tasks
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/tasks", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for tasks, got %d", w.Code)
	}
	var tasksArray []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &tasksArray); err != nil {
		t.Fatalf("failed to decode tasks: %v", err)
	}
	if len(tasksArray) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasksArray))
	}

	// GET /connectors/{name}/tasks/{taskId}/status
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/tasks/0/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for task status, got %d", w.Code)
	}
	var singleTaskStatus connect.TaskStatus
	if err := json.Unmarshal(w.Body.Bytes(), &singleTaskStatus); err != nil {
		t.Fatalf("failed to decode task status: %v", err)
	}
	if singleTaskStatus.ID != 0 {
		t.Fatalf("expected task id 0, got %d", singleTaskStatus.ID)
	}

	// POST /connectors/{name}/tasks/{taskId}/restart -> 204 No Content
	req = httptest.NewRequest(http.MethodPost, "/connectors/http-test-sink/tasks/0/restart", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for task restart, got %d", w.Code)
	}

	// 11. Topics endpoints
	// GET /connectors/{name}/topics -> {"http-test-sink": {"topics": ["orders"]}}
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink/topics", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for topics, got %d", w.Code)
	}
	var topicsMap map[string]map[string][]string
	if err := json.Unmarshal(w.Body.Bytes(), &topicsMap); err != nil {
		t.Fatalf("failed to decode topics response: %v", err)
	}
	if len(topicsMap["http-test-sink"]["topics"]) == 0 || topicsMap["http-test-sink"]["topics"][0] != "orders" {
		t.Fatalf("expected [orders], got %v", topicsMap["http-test-sink"]["topics"])
	}

	// PUT /connectors/{name}/topics/reset (KIP-558) -> 204 No Content
	req = httptest.NewRequest(http.MethodPut, "/connectors/http-test-sink/topics/reset", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for topics reset, got %d", w.Code)
	}

	// 12. DELETE /connectors/{name} -> 204 No Content
	req = httptest.NewRequest(http.MethodDelete, "/connectors/http-test-sink", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for delete, got %d", w.Code)
	}

	// Verify 404 after delete
	req = httptest.NewRequest(http.MethodGet, "/connectors/http-test-sink", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for deleted connector, got %d", w.Code)
	}
}

func TestKafkaConnectREST_PluginValidation(t *testing.T) {
	mux := setupTestServer()

	// 1. GET /connector-plugins -> list plugins
	req := httptest.NewRequest(http.MethodGet, "/connector-plugins", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var plugins []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &plugins); err != nil {
		t.Fatalf("failed to decode plugins list: %v", err)
	}
	if len(plugins) != 4 {
		t.Fatalf("expected 4 plugins, got %d", len(plugins))
	}
	if plugins[0]["type"] != "sink" && plugins[0]["type"] != "source" {
		t.Fatalf("expected lowercase plugin type, got %v", plugins[0]["type"])
	}

	// 2. PUT /connector-plugins/{pluginName}/config/validate with missing required configs
	invalidCfg := map[string]interface{}{
		"tasks.max": "1",
	}
	b, _ := json.Marshal(invalidCfg)
	req = httptest.NewRequest(http.MethodPut, "/connector-plugins/HttpWebhookSinkConnector/config/validate", bytes.NewReader(b))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for validation endpoint, got %d: %s", w.Code, w.Body.String())
	}
	var valResult connect.ConfigValidationResult
	if err := json.Unmarshal(w.Body.Bytes(), &valResult); err != nil {
		t.Fatalf("failed to decode validation result: %v", err)
	}
	if valResult.ErrorCount == 0 {
		t.Fatalf("expected error count > 0 for invalid config, got %d", valResult.ErrorCount)
	}
	if valResult.Name != "HttpWebhookSinkConnector" {
		t.Fatalf("expected plugin name in result, got %s", valResult.Name)
	}

	// 3. POST /connector-plugins/{pluginName}/config/validate with full valid config
	validCfg := map[string]interface{}{
		"name":            "prod-webhook",
		"connector.class": "HttpWebhookSinkConnector",
		"topics":          "events",
		"http.url":        "https://webhook.site/abc",
		"tasks.max":       "1",
	}
	b, _ = json.Marshal(validCfg)
	req = httptest.NewRequest(http.MethodPost, "/connector-plugins/HttpWebhookSinkConnector/config/validate", bytes.NewReader(b))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var validResult connect.ConfigValidationResult
	if err := json.Unmarshal(w.Body.Bytes(), &validResult); err != nil {
		t.Fatalf("failed to decode valid config result: %v", err)
	}
	if validResult.ErrorCount != 0 {
		t.Fatalf("expected 0 errors for valid config, got %d (configs: %+v)", validResult.ErrorCount, validResult.Configs)
	}

	// 4. Non-existent plugin -> 404 Not Found
	req = httptest.NewRequest(http.MethodPut, "/connector-plugins/NonExistentPlugin/config/validate", bytes.NewReader(b))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown plugin, got %d", w.Code)
	}
}

func TestKafkaConnectREST_ErrorHandling(t *testing.T) {
	mux := setupTestServer()

	// Missing connector name
	badPayload := map[string]interface{}{
		"config": map[string]string{
			"connector.class": "HttpWebhookSinkConnector",
		},
	}
	b, _ := json.Marshal(badPayload)
	req := httptest.NewRequest(http.MethodPost, "/connectors", bytes.NewReader(b))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing connector name, got %d", w.Code)
	}

	// Bad JSON body
	req = httptest.NewRequest(http.MethodPost, "/connectors", bytes.NewReader([]byte("{invalid-json")))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad JSON, got %d", w.Code)
	}

	// 404 for non-existent operations
	for _, path := range []string{
		"/connectors/non-existent-xyz",
		"/connectors/non-existent-xyz/status",
		"/connectors/non-existent-xyz/config",
		"/connectors/non-existent-xyz/tasks",
		"/connectors/non-existent-xyz/tasks/0/status",
		"/connectors/non-existent-xyz/topics",
	} {
		req = httptest.NewRequest(http.MethodGet, path, nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("path %s: expected 404, got %d", path, w.Code)
		}
	}
}
