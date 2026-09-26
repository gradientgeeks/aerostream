package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/rest"
	"github.com/gradientgeeks/aerostream/go-controller/pkg/schemaregistry"
)

func TestPhase5SchemaRegistryClientEndToEnd(t *testing.T) {
	reg := schemaregistry.NewRegistry()
	server := rest.NewServer(nil, "127.0.0.1:0", reg)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. Register an Avro schema via HTTP POST /subjects/orders-value/versions
	initialAvroSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"}]}`
	regPayload, _ := json.Marshal(map[string]interface{}{
		"schema":     initialAvroSchema,
		"schemaType": "AVRO",
	})

	resp, err := client.Post(ts.URL+"/subjects/orders-value/versions", "application/json", bytes.NewReader(regPayload))
	if err != nil {
		t.Fatalf("POST /subjects/orders-value/versions request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var regResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&regResult); err != nil {
		t.Fatalf("Failed to decode register response: %v", err)
	}
	schemaIDFloat, ok := regResult["id"].(float64)
	if !ok || int64(schemaIDFloat) != 1 {
		t.Fatalf("Expected assigned schema id 1, got %v", regResult["id"])
	}

	// 2. Retrieve schema by ID via GET /schemas/ids/1
	resp, err = client.Get(ts.URL + "/schemas/ids/1")
	if err != nil {
		t.Fatalf("GET /schemas/ids/1 request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var idResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&idResult); err != nil {
		t.Fatalf("Failed to decode id response: %v", err)
	}
	schemaStr, _ := idResult["schema"].(string)
	if !strings.Contains(schemaStr, "Order") || !strings.Contains(schemaStr, "amount") {
		t.Fatalf("Retrieved schema content mismatch: %s", schemaStr)
	}

	// 3. Retrieve latest version via GET /subjects/orders-value/versions/latest
	resp, err = client.Get(ts.URL + "/subjects/orders-value/versions/latest")
	if err != nil {
		t.Fatalf("GET /subjects/orders-value/versions/latest request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var latestResult schemaregistry.Schema
	if err := json.NewDecoder(resp.Body).Decode(&latestResult); err != nil {
		t.Fatalf("Failed to decode latest version response: %v", err)
	}
	if latestResult.Subject != "orders-value" || latestResult.Version != 1 || latestResult.ID != 1 {
		t.Fatalf("Unexpected latest version object: %+v", latestResult)
	}

	// 4. Test compatibility endpoint:
	// 4a. Verify backward compatible schema passes
	// Adding field 'status' with default value 'PENDING' is backward compatible
	compatAvroSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"},{"name":"status","type":"string","default":"PENDING"}]}`
	compatPayload, _ := json.Marshal(map[string]interface{}{
		"schema":     compatAvroSchema,
		"schemaType": "AVRO",
	})

	resp, err = client.Post(ts.URL+"/compatibility/subjects/orders-value/versions/latest", "application/json", bytes.NewReader(compatPayload))
	if err != nil {
		t.Fatalf("POST compatibility check request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var compatResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&compatResult); err != nil {
		t.Fatalf("Failed to decode compatibility response: %v", err)
	}
	if compatResult["is_compatible"] != true {
		t.Fatalf("Expected is_compatible: true for compatible schema, got %+v", compatResult)
	}

	// 4b. Verify incompatible schema returns false
	// Adding field 'tax_rate' WITHOUT a default value breaks backward compatibility
	incompatAvroSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double"},{"name":"tax_rate","type":"double"}]}`
	incompatPayload, _ := json.Marshal(map[string]interface{}{
		"schema":     incompatAvroSchema,
		"schemaType": "AVRO",
	})

	resp, err = client.Post(ts.URL+"/compatibility/subjects/orders-value/versions/latest", "application/json", bytes.NewReader(incompatPayload))
	if err != nil {
		t.Fatalf("POST incompatible check request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
	}

	var incompatResult map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&incompatResult); err != nil {
		t.Fatalf("Failed to decode incompatible response: %v", err)
	}
	if incompatResult["is_compatible"] != false {
		t.Fatalf("Expected is_compatible: false for incompatible schema, got %+v", incompatResult)
	}
}
