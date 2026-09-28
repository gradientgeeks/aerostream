package transform

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestEngine_SeedTransforms(t *testing.T) {
	eng := NewEngine().SeedDefaultTransforms()
	list := eng.ListTransforms()
	if len(list) < 2 {
		t.Fatalf("expected at least 2 seeded transforms, got %d", len(list))
	}

	pii, err := eng.GetTransform("pii-masker-orders")
	if err != nil {
		t.Fatalf("failed to get pii-masker-orders: %v", err)
	}
	if pii.Type != TypeMaskPII {
		t.Errorf("expected type MASK_PII, got %s", pii.Type)
	}
	if pii.SourceTopic != "orders" || pii.TargetTopic != "orders-sanitized" {
		t.Errorf("unexpected topics: %s -> %s", pii.SourceTopic, pii.TargetTopic)
	}

	telemetry, err := eng.GetTransform("telemetry-filter-critical")
	if err != nil {
		t.Fatalf("failed to get telemetry-filter-critical: %v", err)
	}
	if telemetry.Type != TypeFilter {
		t.Errorf("expected type FILTER, got %s", telemetry.Type)
	}
}

func TestEngine_RegisterAndLifecycle(t *testing.T) {
	eng := NewEngine()

	custom := &Transform{
		Name:        "custom-json-mapper",
		SourceTopic: "raw-clicks",
		TargetTopic: "enriched-clicks",
		Type:        TypeJSONMap,
		Config: map[string]string{
			"set_env": "production",
		},
	}

	err := eng.RegisterTransform(custom)
	if err != nil {
		t.Fatalf("failed to register custom transform: %v", err)
	}

	// Duplicate registration should fail
	err = eng.RegisterTransform(custom)
	if err == nil {
		t.Fatalf("expected error on duplicate registration, got nil")
	}

	// Verify it can be retrieved
	fetched, err := eng.GetTransform("custom-json-mapper")
	if err != nil {
		t.Fatalf("failed to retrieve registered transform: %v", err)
	}
	if fetched.Status != StatusRunning {
		t.Errorf("expected status RUNNING, got %s", fetched.Status)
	}

	// Pause
	err = eng.PauseTransform("custom-json-mapper")
	if err != nil {
		t.Fatalf("failed to pause: %v", err)
	}
	fetched, _ = eng.GetTransform("custom-json-mapper")
	if fetched.Status != StatusPaused {
		t.Errorf("expected status PAUSED, got %s", fetched.Status)
	}

	// Resume
	err = eng.ResumeTransform("custom-json-mapper")
	if err != nil {
		t.Fatalf("failed to resume: %v", err)
	}
	fetched, _ = eng.GetTransform("custom-json-mapper")
	if fetched.Status != StatusRunning {
		t.Errorf("expected status RUNNING, got %s", fetched.Status)
	}

	// Delete
	err = eng.DeleteTransform("custom-json-mapper")
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	_, err = eng.GetTransform("custom-json-mapper")
	if err != ErrTransformNotFound {
		t.Fatalf("expected ErrTransformNotFound, got %v", err)
	}
}

func TestEngine_ExecuteMaskPII(t *testing.T) {
	eng := NewEngine().SeedDefaultTransforms()
	piiTransform, err := eng.GetTransform("pii-masker-orders")
	if err != nil {
		t.Fatalf("failed to get pii transform: %v", err)
	}

	input := `{
		"order_id": "ORD-9912",
		"customer": {
			"name": "Jane Doe",
			"email": "jane@example.com",
			"credit_card": "4111-2222-3333-4444",
			"cvv": "123",
			"password": "supersecretpassword"
		},
		"amount": 149.99
	}`

	out, drop, err := eng.ExecuteTransform(piiTransform, []byte(input))
	if err != nil {
		t.Fatalf("execute transform failed: %v", err)
	}
	if drop {
		t.Fatalf("expected drop=false for PII transform, got true")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	cust, ok := parsed["customer"].(map[string]interface{})
	if !ok {
		t.Fatalf("customer object missing in output")
	}

	if cust["credit_card"] != "***" {
		t.Errorf("expected credit_card to be masked, got %v", cust["credit_card"])
	}
	if cust["cvv"] != "***" {
		t.Errorf("expected cvv to be masked, got %v", cust["cvv"])
	}
	if cust["password"] != "***" {
		t.Errorf("expected password to be masked, got %v", cust["password"])
	}
	if cust["email"] != "***" {
		t.Errorf("expected email to be masked, got %v", cust["email"])
	}
	if cust["name"] != "Jane Doe" {
		t.Errorf("expected name to be preserved, got %v", cust["name"])
	}
	if parsed["order_id"] != "ORD-9912" {
		t.Errorf("expected order_id to be preserved, got %v", parsed["order_id"])
	}
}

func TestEngine_ExecuteFilter(t *testing.T) {
	eng := NewEngine().SeedDefaultTransforms()
	filterTransform, err := eng.GetTransform("telemetry-filter-critical")
	if err != nil {
		t.Fatalf("failed to get filter transform: %v", err)
	}

	// Payload with level == CRITICAL -> Should PASS (drop == false)
	criticalPayload := `{"device_id": "sensor-01", "level": "CRITICAL", "temp": 105.4}`
	out, drop, err := eng.ExecuteTransform(filterTransform, []byte(criticalPayload))
	if err != nil {
		t.Fatalf("unexpected error on filter exec: %v", err)
	}
	if drop {
		t.Errorf("expected critical record to NOT be dropped")
	}
	if len(out) == 0 {
		t.Errorf("expected non-empty output for passed record")
	}

	// Payload with level == INFO -> Should DROP (drop == true)
	infoPayload := `{"device_id": "sensor-02", "level": "INFO", "temp": 72.1}`
	out, drop, err = eng.ExecuteTransform(filterTransform, []byte(infoPayload))
	if err != nil {
		t.Fatalf("unexpected error on filter exec: %v", err)
	}
	if !drop {
		t.Errorf("expected INFO record to be DROPPED")
	}
	if out != nil {
		t.Errorf("expected nil output for dropped record")
	}
}

func TestEngine_ExecuteJSONMap(t *testing.T) {
	eng := NewEngine()
	xform := &Transform{
		Name:        "enrich-test",
		SourceTopic: "src",
		TargetTopic: "dst",
		Type:        TypeJSONMap,
		Config: map[string]string{
			"set_region":    "us-east-1",
			"rename_fields": "old_name:new_name",
			"remove_fields": "debug_info",
		},
	}
	_ = eng.RegisterTransform(xform)

	payload := `{"old_name": "alpha", "debug_info": "trace-123", "value": 42}`
	out, drop, err := eng.ExecuteTransform(xform, []byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drop {
		t.Fatalf("expected drop == false")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if parsed["region"] != "us-east-1" {
		t.Errorf("expected set_region to be injected, got %v", parsed["region"])
	}
	if parsed["new_name"] != "alpha" {
		t.Errorf("expected renamed field new_name, got %v", parsed["new_name"])
	}
	if _, exists := parsed["old_name"]; exists {
		t.Errorf("old_name should have been removed")
	}
	if _, exists := parsed["debug_info"]; exists {
		t.Errorf("debug_info should have been removed")
	}
	if parsed["_engine"] != "aerostream-inline" {
		t.Errorf("expected _engine metadata")
	}
}

func TestEngine_ExecuteWASM(t *testing.T) {
	eng := NewEngine()
	wasmXform := &Transform{
		Name:        "wasm-runner",
		SourceTopic: "raw",
		TargetTopic: "processed",
		Type:        TypeWASM,
		Config: map[string]string{
			"action": "uppercase_keys",
		},
		Code: "(module (func $transform))",
	}
	_ = eng.RegisterTransform(wasmXform)

	payload := `{"greeting": "hello", "target": "world"}`
	out, drop, err := eng.ExecuteTransform(wasmXform, []byte(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if drop {
		t.Fatalf("expected drop == false")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to unmarshal wasm output: %v", err)
	}

	if parsed["GREETING"] != "hello" || parsed["TARGET"] != "world" {
		t.Errorf("expected uppercase keys in wasm output, got %v", parsed)
	}
	if parsed["_WASM"] == nil && parsed["_wasm_execution"] == nil {
		t.Errorf("expected wasm runtime telemetry metadata")
	}
}

func TestEngine_Concurrency(t *testing.T) {
	eng := NewEngine().SeedDefaultTransforms()
	t1, _ := eng.GetTransform("pii-masker-orders")

	payload := []byte(`{"credit_card": "1234-5678", "order_id": "X"}`)

	var wg sync.WaitGroup
	workers := 20
	iters := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_, _, _ = eng.ExecuteTransform(t1, payload)
				_ = eng.ListTransforms()
			}
		}()
	}

	wg.Wait()

	updated, err := eng.GetTransform("pii-masker-orders")
	if err != nil {
		t.Fatalf("failed to retrieve transform: %v", err)
	}
	if updated.MessagesProcessed < int64(workers*iters) {
		t.Errorf("expected MessagesProcessed >= %d, got %d", workers*iters, updated.MessagesProcessed)
	}
}

func TestEngine_FilterEdgeCases(t *testing.T) {
	eng := NewEngine()

	// Nested dot path filter
	nestedFilter := &Transform{
		Name:        "nested-filter",
		SourceTopic: "events",
		TargetTopic: "vip-events",
		Type:        TypeFilter,
		Config: map[string]string{
			"filter_expression": `user.tier == "VIP"`,
		},
	}
	_ = eng.RegisterTransform(nestedFilter)

	payloadPass := `{"user": {"tier": "VIP", "name": "Alice"}}`
	_, drop, err := eng.ExecuteTransform(nestedFilter, []byte(payloadPass))
	if err != nil || drop {
		t.Errorf("expected pass for VIP tier, drop=%v, err=%v", drop, err)
	}

	payloadFail := `{"user": {"tier": "STANDARD", "name": "Bob"}}`
	_, drop, err = eng.ExecuteTransform(nestedFilter, []byte(payloadFail))
	if err != nil || !drop {
		t.Errorf("expected drop for STANDARD tier, drop=%v, err=%v", drop, err)
	}

	// Numeric comparison filter (> 100)
	numFilter := &Transform{
		Name:        "amount-filter",
		SourceTopic: "transactions",
		TargetTopic: "large-tx",
		Type:        TypeFilter,
		Config: map[string]string{
			"filter_expression": `amount >= 100`,
		},
	}
	_ = eng.RegisterTransform(numFilter)

	payload100 := `{"tx_id": "tx-1", "amount": 100}`
	_, drop, _ = eng.ExecuteTransform(numFilter, []byte(payload100))
	if drop {
		t.Errorf("expected amount=100 to pass >= 100")
	}

	payload50 := `{"tx_id": "tx-2", "amount": 50}`
	_, drop, _ = eng.ExecuteTransform(numFilter, []byte(payload50))
	if !drop {
		t.Errorf("expected amount=50 to drop for >= 100")
	}

	// Contains filter
	containsFilter := &Transform{
		Name:        "msg-contains-filter",
		SourceTopic: "logs",
		TargetTopic: "fatal-logs",
		Type:        TypeFilter,
		Config: map[string]string{
			"filter_expression": `message contains "FATAL"`,
		},
	}
	_ = eng.RegisterTransform(containsFilter)

	logFatal := `{"message": "System crashed with FATAL exception"}`
	_, drop, _ = eng.ExecuteTransform(containsFilter, []byte(logFatal))
	if drop {
		t.Errorf("expected fatal log to pass")
	}

	logNormal := `{"message": "All systems operational"}`
	_, drop, _ = eng.ExecuteTransform(containsFilter, []byte(logNormal))
	if !drop {
		t.Errorf("expected normal log to drop")
	}
}

func TestEngine_ValidationErrors(t *testing.T) {
	eng := NewEngine()

	// Nil transform
	if err := eng.RegisterTransform(nil); err == nil {
		t.Errorf("expected error registering nil transform")
	}

	// Missing fields
	invalid := &Transform{Name: ""}
	if err := eng.RegisterTransform(invalid); err == nil {
		t.Errorf("expected error registering empty transform")
	}

	// Unknown type
	invalidType := &Transform{
		Name:        "test",
		SourceTopic: "src",
		TargetTopic: "dst",
		Type:        "UNKNOWN_TYPE",
	}
	if err := eng.RegisterTransform(invalidType); err == nil {
		t.Errorf("expected error registering unknown transform type")
	}

	// Non-existent operations
	if err := eng.PauseTransform("not-exists"); err != ErrTransformNotFound {
		t.Errorf("expected ErrTransformNotFound on pause")
	}
	if err := eng.ResumeTransform("not-exists"); err != ErrTransformNotFound {
		t.Errorf("expected ErrTransformNotFound on resume")
	}
	if err := eng.DeleteTransform("not-exists"); err != ErrTransformNotFound {
		t.Errorf("expected ErrTransformNotFound on delete")
	}
}
