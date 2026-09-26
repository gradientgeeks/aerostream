package schemaregistry_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/schemaregistry"
)

const (
	avroUserV1 = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	avroUserV2WithDefault = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string", "default": ""}
		]
	}`

	avroUserV2NoDefault = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"}
		]
	}`

	avroUserDeletedField = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "int"}
		]
	}`

	avroUserIncompatibleType = `{
		"type": "record",
		"name": "User",
		"namespace": "com.example",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "name", "type": "string"}
		]
	}`

	jsonSchemaV1 = `{
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"}
		},
		"required": ["id"]
	}`

	protoSchemaV1 = `
		syntax = "proto3";
		package example;
		message User {
			int32 id = 1;
			string name = 2;
		}
	`
)

// TestSchemaRegistration tests basic registration of Avro, JSON, and Protobuf schemas.
func TestSchemaRegistration(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	// 1. AVRO
	id1, v1, err := reg.RegisterSchema("user-avro", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("failed to register avro schema: %v", err)
	}
	if id1 <= 0 || v1 != 1 {
		t.Fatalf("expected positive ID and version 1, got id=%d, v=%d", id1, v1)
	}

	// 2. JSON
	id2, v2, err := reg.RegisterSchema("user-json", schemaregistry.TypeJSON, jsonSchemaV1)
	if err != nil {
		t.Fatalf("failed to register json schema: %v", err)
	}
	if id2 <= id1 || v2 != 1 {
		t.Fatalf("expected new ID > %d and version 1, got id=%d, v=%d", id1, id2, v2)
	}

	// 3. PROTOBUF
	id3, v3, err := reg.RegisterSchema("user-proto", schemaregistry.TypeProtobuf, protoSchemaV1)
	if err != nil {
		t.Fatalf("failed to register proto schema: %v", err)
	}
	if id3 <= id2 || v3 != 1 {
		t.Fatalf("expected new ID > %d and version 1, got id=%d, v=%d", id2, id3, v3)
	}
}

// TestSchemaVersioning tests that sequential valid schemas under a subject produce version 1, 2, etc.
func TestSchemaVersioning(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	id1, v1, err := reg.RegisterSchema("orders", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("expected version 1, got %d", v1)
	}

	id2, v2, err := reg.RegisterSchema("orders", schemaregistry.TypeAvro, avroUserV2WithDefault)
	if err != nil {
		t.Fatalf("failed to register v2: %v", err)
	}
	if v2 != 2 {
		t.Fatalf("expected version 2, got %d", v2)
	}
	if id1 == id2 {
		t.Fatalf("expected different IDs for distinct schemas, both got %d", id1)
	}

	versions := reg.ListVersions("orders")
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("unexpected versions: %v", versions)
	}
}

// TestSchemaDeduplication tests that identical schemas are not given new versions or redundant IDs.
func TestSchemaDeduplication(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	id1, v1, err := reg.RegisterSchema("dedup-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("initial registration failed: %v", err)
	}

	// Re-register identical schema under the same subject
	id2, v2, err := reg.RegisterSchema("dedup-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("duplicate registration failed: %v", err)
	}

	if id1 != id2 {
		t.Fatalf("expected identical schema ID (%d), got %d", id1, id2)
	}
	if v1 != v2 {
		t.Fatalf("expected version %d, got %d", v1, v2)
	}

	versions := reg.ListVersions("dedup-subject")
	if len(versions) != 1 {
		t.Fatalf("expected exactly 1 version after duplicate registration, got %d", len(versions))
	}

	// Register same schema under a different subject -> should reuse global ID, but get version 1
	id3, v3, err := reg.RegisterSchema("other-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("registration in other subject failed: %v", err)
	}
	if id3 != id1 {
		t.Fatalf("expected global schema ID reuse (%d), got %d", id1, id3)
	}
	if v3 != 1 {
		t.Fatalf("expected version 1 in new subject, got %d", v3)
	}
}

// TestGetSchemaByID tests retrieving registered schemas by global ID.
func TestGetSchemaByID(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	id, _, err := reg.RegisterSchema("lookup-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("registration failed: %v", err)
	}

	sc, err := reg.GetSchemaByID(id)
	if err != nil {
		t.Fatalf("failed to get schema by ID: %v", err)
	}
	if sc.ID != id || sc.Subject != "lookup-subject" || sc.Version != 1 {
		t.Fatalf("retrieved schema mismatch: %+v", sc)
	}

	// Non-existent ID
	_, err = reg.GetSchemaByID(99999)
	if err == nil {
		t.Fatalf("expected error for non-existent schema ID")
	}
}

// TestGetLatestAndByVersion tests GetLatestSchema and GetSchemaByVersion.
func TestGetLatestAndByVersion(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	_, _, _ = reg.RegisterSchema("versioned-subject", schemaregistry.TypeAvro, avroUserV1)
	_, _, _ = reg.RegisterSchema("versioned-subject", schemaregistry.TypeAvro, avroUserV2WithDefault)

	v1Schema, err := reg.GetSchemaByVersion("versioned-subject", 1)
	if err != nil {
		t.Fatalf("failed to get version 1: %v", err)
	}
	if v1Schema.Version != 1 {
		t.Fatalf("expected version 1, got %d", v1Schema.Version)
	}

	latest, err := reg.GetLatestSchema("versioned-subject")
	if err != nil {
		t.Fatalf("failed to get latest schema: %v", err)
	}
	if latest.Version != 2 {
		t.Fatalf("expected latest version 2, got %d", latest.Version)
	}

	// Missing subject or version
	_, err = reg.GetLatestSchema("nonexistent")
	if err == nil {
		t.Fatalf("expected error for non-existent subject")
	}
	_, err = reg.GetSchemaByVersion("versioned-subject", 99)
	if err == nil {
		t.Fatalf("expected error for non-existent version")
	}
}

// TestListSubjects tests listing all subject names in sorted order.
func TestListSubjects(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	_, _, _ = reg.RegisterSchema("zebra", schemaregistry.TypeAvro, avroUserV1)
	_, _, _ = reg.RegisterSchema("alpha", schemaregistry.TypeAvro, avroUserV1)
	_, _, _ = reg.RegisterSchema("beta", schemaregistry.TypeAvro, avroUserV1)

	subjects := reg.ListSubjects()
	if len(subjects) != 3 {
		t.Fatalf("expected 3 subjects, got %d", len(subjects))
	}
	if subjects[0] != "alpha" || subjects[1] != "beta" || subjects[2] != "zebra" {
		t.Fatalf("expected sorted subjects, got %v", subjects)
	}
}

// TestBackwardCompatibility checks enforcement of BACKWARD compatibility rules.
func TestBackwardCompatibility(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	// Default compatibility is BACKWARD
	if reg.GetCompatibility("compat-subject") != schemaregistry.BACKWARD {
		t.Fatalf("expected default compatibility to be BACKWARD")
	}

	// 1. Initial schema v1
	_, _, err := reg.RegisterSchema("compat-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// 2. Added field WITH default -> backward compatible
	_, _, err = reg.RegisterSchema("compat-subject", schemaregistry.TypeAvro, avroUserV2WithDefault)
	if err != nil {
		t.Fatalf("expected added field with default to be compatible: %v", err)
	}

	// 3. Added field WITHOUT default -> incompatible under BACKWARD
	_, _, err = reg.RegisterSchema("compat-subject", schemaregistry.TypeAvro, avroUserV2NoDefault)
	if err == nil {
		t.Fatalf("expected added field without default to fail backward compatibility")
	}

	// 4. Deleted field -> backward compatible (new consumer ignores missing old field)
	_, _, err = reg.RegisterSchema("compat-subject-2", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}
	_, _, err = reg.RegisterSchema("compat-subject-2", schemaregistry.TypeAvro, avroUserDeletedField)
	if err != nil {
		t.Fatalf("expected deleted field to be backward compatible: %v", err)
	}

	// 5. Incompatible field type change -> fails
	_, _, err = reg.RegisterSchema("compat-subject-2", schemaregistry.TypeAvro, avroUserIncompatibleType)
	if err == nil {
		t.Fatalf("expected field type change from int to string to fail compatibility")
	}
}

// TestCompatibilityLevels tests switching compatibility level (NONE, FULL, FORWARD).
func TestCompatibilityLevels(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	// Set subject compatibility to NONE
	err := reg.SetCompatibility("none-subject", schemaregistry.NONE)
	if err != nil {
		t.Fatalf("failed to set compatibility: %v", err)
	}
	if reg.GetCompatibility("none-subject") != schemaregistry.NONE {
		t.Fatalf("expected compatibility NONE")
	}

	// Register v1
	_, _, err = reg.RegisterSchema("none-subject", schemaregistry.TypeAvro, avroUserV1)
	if err != nil {
		t.Fatalf("failed to register v1: %v", err)
	}

	// Now an otherwise backward-incompatible schema succeeds under NONE
	_, v2, err := reg.RegisterSchema("none-subject", schemaregistry.TypeAvro, avroUserV2NoDefault)
	if err != nil {
		t.Fatalf("expected registration to succeed under compatibility NONE: %v", err)
	}
	if v2 != 2 {
		t.Fatalf("expected version 2, got %d", v2)
	}

	// Test invalid level
	err = reg.SetCompatibility("none-subject", "INVALID_LEVEL")
	if err == nil {
		t.Fatalf("expected error for invalid compatibility level")
	}
}

// TestTestCompatibilityMethod tests the TestCompatibility and TestCompatibilityWithVersion methods.
func TestTestCompatibilityMethod(t *testing.T) {
	reg := schemaregistry.NewRegistry()

	_, _, _ = reg.RegisterSchema("test-compat", schemaregistry.TypeAvro, avroUserV1)

	// Compatible candidate
	ok, err := reg.TestCompatibility("test-compat", avroUserV2WithDefault)
	if err != nil || !ok {
		t.Fatalf("expected true, nil, got ok=%v, err=%v", ok, err)
	}

	// Incompatible candidate
	ok, err = reg.TestCompatibility("test-compat", avroUserV2NoDefault)
	if err != nil || ok {
		t.Fatalf("expected false, nil, got ok=%v, err=%v", ok, err)
	}
}

// TestStateExportImport tests serialization and restoration of registry state (Raft-compatible).
func TestStateExportImport(t *testing.T) {
	reg1 := schemaregistry.NewRegistry()
	id1, _, _ := reg1.RegisterSchema("state-subject", schemaregistry.TypeAvro, avroUserV1)
	_ = reg1.SetCompatibility("state-subject", schemaregistry.FULL)

	data, err := reg1.ExportState()
	if err != nil {
		t.Fatalf("failed to export state: %v", err)
	}

	reg2 := schemaregistry.NewRegistry()
	err = reg2.ImportState(data)
	if err != nil {
		t.Fatalf("failed to import state: %v", err)
	}

	sc, err := reg2.GetSchemaByID(id1)
	if err != nil || sc.Subject != "state-subject" {
		t.Fatalf("restored schema mismatch: %v, %v", sc, err)
	}
	if reg2.GetCompatibility("state-subject") != schemaregistry.FULL {
		t.Fatalf("restored compatibility level mismatch")
	}
}

// TestRestAPIEndpoints tests the full set of Confluent Schema Registry HTTP endpoints using httptest.
func TestRestAPIEndpoints(t *testing.T) {
	reg := schemaregistry.NewRegistry()
	srv := rest.NewServer(nil, "127.0.0.1:0", reg)

	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// 1. POST /subjects/{subject}/versions (Register schema)
	body := map[string]interface{}{
		"schema":     avroUserV1,
		"schemaType": "AVRO",
	}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/subjects/user-value/versions", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from POST /subjects/user-value/versions, got %d: %s", w.Code, w.Body.String())
	}
	var postResp struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &postResp); err != nil || postResp.ID <= 0 {
		t.Fatalf("invalid register response: %s", w.Body.String())
	}
	firstID := postResp.ID

	// Deduplication via REST API
	reqDup := httptest.NewRequest(http.MethodPost, "/subjects/user-value/versions", bytes.NewReader(bodyBytes))
	wDup := httptest.NewRecorder()
	mux.ServeHTTP(wDup, reqDup)
	if wDup.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from duplicate registration, got %d", wDup.Code)
	}
	var dupResp struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(wDup.Body.Bytes(), &dupResp)
	if dupResp.ID != firstID {
		t.Fatalf("expected deduplicated ID %d, got %d", firstID, dupResp.ID)
	}

	// 2. GET /subjects
	reqSubj := httptest.NewRequest(http.MethodGet, "/subjects", nil)
	wSubj := httptest.NewRecorder()
	mux.ServeHTTP(wSubj, reqSubj)
	if wSubj.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /subjects, got %d", wSubj.Code)
	}
	var subjects []string
	if err := json.Unmarshal(wSubj.Body.Bytes(), &subjects); err != nil || len(subjects) != 1 || subjects[0] != "user-value" {
		t.Fatalf("unexpected subjects response: %s", wSubj.Body.String())
	}

	// 3. GET /subjects/{subject}/versions
	reqVers := httptest.NewRequest(http.MethodGet, "/subjects/user-value/versions", nil)
	wVers := httptest.NewRecorder()
	mux.ServeHTTP(wVers, reqVers)
	if wVers.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /subjects/user-value/versions, got %d", wVers.Code)
	}
	var versions []int
	if err := json.Unmarshal(wVers.Body.Bytes(), &versions); err != nil || len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("unexpected versions response: %s", wVers.Body.String())
	}

	// 4. GET /subjects/{subject}/versions/{version} (number and "latest")
	reqV1 := httptest.NewRequest(http.MethodGet, "/subjects/user-value/versions/1", nil)
	wV1 := httptest.NewRecorder()
	mux.ServeHTTP(wV1, reqV1)
	if wV1.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /subjects/user-value/versions/1, got %d", wV1.Code)
	}
	var schemaObj schemaregistry.Schema
	if err := json.Unmarshal(wV1.Body.Bytes(), &schemaObj); err != nil || schemaObj.Version != 1 {
		t.Fatalf("unexpected schema object response: %s", wV1.Body.String())
	}

	reqLatest := httptest.NewRequest(http.MethodGet, "/subjects/user-value/versions/latest", nil)
	wLatest := httptest.NewRecorder()
	mux.ServeHTTP(wLatest, reqLatest)
	if wLatest.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /subjects/user-value/versions/latest, got %d", wLatest.Code)
	}

	// 5. GET /schemas/ids/{id}
	reqID := httptest.NewRequest(http.MethodGet, "/schemas/ids/1", nil)
	wID := httptest.NewRecorder()
	mux.ServeHTTP(wID, reqID)
	if wID.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /schemas/ids/1, got %d: %s", wID.Code, wID.Body.String())
	}
	var idResp struct {
		Schema string `json:"schema"`
		ID     int64  `json:"id"`
	}
	if err := json.Unmarshal(wID.Body.Bytes(), &idResp); err != nil || idResp.ID != 1 || idResp.Schema == "" {
		t.Fatalf("unexpected schema by ID response: %s", wID.Body.String())
	}

	// 6. POST /compatibility/subjects/{subject}/versions/{version}
	compatBody := map[string]interface{}{
		"schema": avroUserV2WithDefault,
	}
	cBytes, _ := json.Marshal(compatBody)
	reqComp := httptest.NewRequest(http.MethodPost, "/compatibility/subjects/user-value/versions/latest", bytes.NewReader(cBytes))
	wComp := httptest.NewRecorder()
	mux.ServeHTTP(wComp, reqComp)
	if wComp.Code != http.StatusOK {
		t.Fatalf("expected 200 from compatibility test, got %d", wComp.Code)
	}
	var compResp struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.Unmarshal(wComp.Body.Bytes(), &compResp); err != nil || !compResp.IsCompatible {
		t.Fatalf("expected compatible=true, got %s", wComp.Body.String())
	}

	// 7. Incompatible registration -> HTTP 409 Conflict
	incompatBody := map[string]interface{}{
		"schema": avroUserV2NoDefault,
	}
	iBytes, _ := json.Marshal(incompatBody)
	reqIncomp := httptest.NewRequest(http.MethodPost, "/subjects/user-value/versions", bytes.NewReader(iBytes))
	wIncomp := httptest.NewRecorder()
	mux.ServeHTTP(wIncomp, reqIncomp)
	if wIncomp.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for incompatible schema, got %d: %s", wIncomp.Code, wIncomp.Body.String())
	}

	// 8. GET /config and PUT /config (Global compatibility)
	reqGetCfg := httptest.NewRequest(http.MethodGet, "/config", nil)
	wGetCfg := httptest.NewRecorder()
	mux.ServeHTTP(wGetCfg, reqGetCfg)
	if wGetCfg.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /config, got %d", wGetCfg.Code)
	}

	putCfgBody := map[string]interface{}{
		"compatibility": "NONE",
	}
	pBytes, _ := json.Marshal(putCfgBody)
	reqPutCfg := httptest.NewRequest(http.MethodPut, "/config", bytes.NewReader(pBytes))
	wPutCfg := httptest.NewRecorder()
	mux.ServeHTTP(wPutCfg, reqPutCfg)
	if wPutCfg.Code != http.StatusOK {
		t.Fatalf("expected 200 from PUT /config, got %d", wPutCfg.Code)
	}

	// 9. GET /config/{subject} and PUT /config/{subject}
	putSubjCfg := map[string]interface{}{
		"compatibility": "FULL",
	}
	psBytes, _ := json.Marshal(putSubjCfg)
	reqPutSubj := httptest.NewRequest(http.MethodPut, "/config/user-value", bytes.NewReader(psBytes))
	wPutSubj := httptest.NewRecorder()
	mux.ServeHTTP(wPutSubj, reqPutSubj)
	if wPutSubj.Code != http.StatusOK {
		t.Fatalf("expected 200 from PUT /config/user-value, got %d", wPutSubj.Code)
	}

	reqGetSubj := httptest.NewRequest(http.MethodGet, "/config/user-value", nil)
	wGetSubj := httptest.NewRecorder()
	mux.ServeHTTP(wGetSubj, reqGetSubj)
	if wGetSubj.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /config/user-value, got %d", wGetSubj.Code)
	}
	var subjCfgResp struct {
		Compatibility string `json:"compatibility"`
	}
	_ = json.Unmarshal(wGetSubj.Body.Bytes(), &subjCfgResp)
	if subjCfgResp.Compatibility != "FULL" {
		t.Fatalf("expected subject compatibility FULL, got %s", subjCfgResp.Compatibility)
	}

	// 10. Invalid schema format -> HTTP 422
	badReq := httptest.NewRequest(http.MethodPost, "/subjects/user-value/versions", bytes.NewReader([]byte(`{"schema": "{not valid json"}`)))
	wBad := httptest.NewRecorder()
	mux.ServeHTTP(wBad, badReq)
	if wBad.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for invalid schema, got %d: %s", wBad.Code, wBad.Body.String())
	}

	// 11. Missing subject / version -> HTTP 404
	missingReq := httptest.NewRequest(http.MethodGet, "/subjects/non-existent-subject/versions", nil)
	wMissing := httptest.NewRecorder()
	mux.ServeHTTP(wMissing, missingReq)
	if wMissing.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing subject, got %d", wMissing.Code)
	}
}
