package connect_test

import (
	"errors"
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/connect"
)

func TestConnectorManager_Defaults(t *testing.T) {
	cm := connect.NewManager().SeedDefaultConnectors()

	plugins := cm.ListPlugins()
	if len(plugins) != 4 {
		t.Fatalf("expected 4 seeded plugins, got %d", len(plugins))
	}

	connectors := cm.ListConnectors()
	if len(connectors) != 2 {
		t.Fatalf("expected 2 seeded connectors, got %d", len(connectors))
	}

	s3, err := cm.GetConnector("s3-cold-storage-sink")
	if err != nil {
		t.Fatalf("failed to get s3-cold-storage-sink: %v", err)
	}
	if s3.Class != "S3ArchivalSinkConnector" || s3.Type != connect.TypeSink {
		t.Fatalf("unexpected s3 connector: %+v", s3)
	}
	if s3.State != connect.StateRunning {
		t.Fatalf("expected s3 connector to be RUNNING, got %s", s3.State)
	}
	if len(s3.Tasks) != 1 || s3.Tasks[0].WorkerID == "" {
		t.Fatalf("expected initialized tasks, got %+v", s3.Tasks)
	}
	if len(s3.Topics) == 0 || s3.Topics[0] != "orders" {
		t.Fatalf("expected topics [orders], got %+v", s3.Topics)
	}

	webhook, err := cm.GetConnector("webhook-payment-notifier")
	if err != nil {
		t.Fatalf("failed to get webhook-payment-notifier: %v", err)
	}
	if webhook.Class != "HttpWebhookSinkConnector" || webhook.Topic != "payment-transactions" {
		t.Fatalf("unexpected webhook connector: %+v", webhook)
	}
}

func TestConnectorManager_RegisterAndLifecycle(t *testing.T) {
	cm := connect.NewManager()

	newConn := &connect.Connector{
		Name:  "cdc-postgres-source",
		Class: "DatabaseCdcSourceConnector",
		Topic: "postgres-changes",
		Config: map[string]string{
			"db.host": "localhost",
			"db.port": "5432",
			"db.user": "aeromq",
		},
	}

	if err := cm.RegisterConnector(newConn); err != nil {
		t.Fatalf("failed to register connector: %v", err)
	}

	// Verify auto-inferred fields
	got, err := cm.GetConnector("cdc-postgres-source")
	if err != nil {
		t.Fatalf("failed to retrieve connector: %v", err)
	}
	if got.Type != connect.TypeSource {
		t.Fatalf("expected inferred type SOURCE, got %s", got.Type)
	}
	if got.State != connect.StateRunning {
		t.Fatalf("expected state RUNNING, got %s", got.State)
	}
	if got.TasksCount != 1 {
		t.Fatalf("expected tasks_count 1, got %d", got.TasksCount)
	}
	if len(got.Tasks) != 1 || got.Tasks[0].ID != 0 {
		t.Fatalf("expected 1 task initialized, got %+v", got.Tasks)
	}
	if got.CreatedAt.IsZero() {
		t.Fatalf("expected non-zero created_at")
	}

	// Duplicate registration should fail
	if err := cm.RegisterConnector(newConn); err != connect.ErrConnectorExists {
		t.Fatalf("expected ErrConnectorExists, got %v", err)
	}

	// Pause
	if err := cm.PauseConnector("cdc-postgres-source"); err != nil {
		t.Fatalf("failed to pause connector: %v", err)
	}
	paused, _ := cm.GetConnector("cdc-postgres-source")
	if paused.State != connect.StatePaused {
		t.Fatalf("expected state PAUSED, got %s", paused.State)
	}
	if paused.Tasks[0].State != connect.StatePaused {
		t.Fatalf("expected task state PAUSED, got %s", paused.Tasks[0].State)
	}

	// Resume
	if err := cm.ResumeConnector("cdc-postgres-source"); err != nil {
		t.Fatalf("failed to resume connector: %v", err)
	}
	resumed, _ := cm.GetConnector("cdc-postgres-source")
	if resumed.State != connect.StateRunning {
		t.Fatalf("expected state RUNNING, got %s", resumed.State)
	}
	if resumed.Tasks[0].State != connect.StateRunning {
		t.Fatalf("expected task state RUNNING, got %s", resumed.Tasks[0].State)
	}

	// Delete
	if err := cm.DeleteConnector("cdc-postgres-source"); err != nil {
		t.Fatalf("failed to delete connector: %v", err)
	}
	_, err = cm.GetConnector("cdc-postgres-source")
	if err != connect.ErrConnectorNotFound {
		t.Fatalf("expected ErrConnectorNotFound, got %v", err)
	}

	// Delete non-existent
	if err := cm.DeleteConnector("non-existent"); err != connect.ErrConnectorNotFound {
		t.Fatalf("expected ErrConnectorNotFound for missing connector, got %v", err)
	}
}

func TestConnectorManager_StopAndRestart(t *testing.T) {
	cm := connect.NewManager().SeedDefaultConnectors()
	connName := "s3-cold-storage-sink"

	// 1. StopConnector (KIP-875)
	if err := cm.StopConnector(connName); err != nil {
		t.Fatalf("failed to stop connector: %v", err)
	}
	c, err := cm.GetConnector(connName)
	if err != nil {
		t.Fatalf("failed to get connector: %v", err)
	}
	if c.State != connect.StateStopped {
		t.Fatalf("expected STOPPED state, got %s", c.State)
	}
	for _, task := range c.Tasks {
		if task.State != connect.StateStopped {
			t.Fatalf("expected task state STOPPED, got %s", task.State)
		}
	}

	// Stop non-existent connector
	if err := cm.StopConnector("missing"); !errors.Is(err, connect.ErrConnectorNotFound) {
		t.Fatalf("expected ErrConnectorNotFound, got %v", err)
	}

	// 2. RestartConnector (all)
	if err := cm.RestartConnector(connName, true, false); err != nil {
		t.Fatalf("failed to restart connector: %v", err)
	}
	c, _ = cm.GetConnector(connName)
	if c.State != connect.StateRunning {
		t.Fatalf("expected RUNNING state after restart, got %s", c.State)
	}
	if c.Tasks[0].State != connect.StateRunning {
		t.Fatalf("expected task RUNNING state after restart, got %s", c.Tasks[0].State)
	}

	// 3. RestartConnector (onlyFailed)
	if err := cm.StopConnector(connName); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}
	// should not restart STOPPED if onlyFailed=true
	if err := cm.RestartConnector(connName, true, true); err != nil {
		t.Fatalf("failed restart onlyFailed: %v", err)
	}
	c, _ = cm.GetConnector(connName)
	if c.State != connect.StateStopped {
		t.Fatalf("expected still STOPPED when onlyFailed=true, got %s", c.State)
	}
}

func TestConnectorManager_TaskOperations(t *testing.T) {
	cm := connect.NewManager().SeedDefaultConnectors()
	connName := "s3-cold-storage-sink"

	// 1. GetTaskStatus
	ts, err := cm.GetTaskStatus(connName, 0)
	if err != nil {
		t.Fatalf("failed to get task status: %v", err)
	}
	if ts.ID != 0 || ts.State != connect.StateRunning {
		t.Fatalf("unexpected task status: %+v", ts)
	}

	// Non-existent task
	if _, err := cm.GetTaskStatus(connName, 999); !errors.Is(err, connect.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}

	// Non-existent connector
	if _, err := cm.GetTaskStatus("missing", 0); !errors.Is(err, connect.ErrConnectorNotFound) {
		t.Fatalf("expected ErrConnectorNotFound, got %v", err)
	}

	// 2. RestartTask
	if err := cm.RestartTask(connName, 0); err != nil {
		t.Fatalf("failed to restart task: %v", err)
	}

	// Restart non-existent task
	if err := cm.RestartTask(connName, 999); !errors.Is(err, connect.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestConnectorManager_Topics(t *testing.T) {
	cm := connect.NewManager().SeedDefaultConnectors()
	connName := "s3-cold-storage-sink"

	topics, err := cm.GetConnectorTopics(connName)
	if err != nil {
		t.Fatalf("failed to get topics: %v", err)
	}
	if len(topics) != 1 || topics[0] != "orders" {
		t.Fatalf("expected [orders], got %v", topics)
	}

	// Reset topics (KIP-558)
	if err := cm.ResetConnectorTopics(connName); err != nil {
		t.Fatalf("failed to reset topics: %v", err)
	}
	topics, err = cm.GetConnectorTopics(connName)
	if err != nil || len(topics) != 0 {
		t.Fatalf("expected empty topics after reset, got %v", topics)
	}

	// Missing connector
	if err := cm.ResetConnectorTopics("missing"); !errors.Is(err, connect.ErrConnectorNotFound) {
		t.Fatalf("expected ErrConnectorNotFound, got %v", err)
	}
}

func TestConnectorManager_UpdateConfig(t *testing.T) {
	cm := connect.NewManager().SeedDefaultConnectors()
	connName := "s3-cold-storage-sink"

	updated, err := cm.UpdateConnectorConfig(connName, map[string]string{
		"connector.class": "S3ArchivalSinkConnector",
		"topics":          "orders,invoices",
		"tasks.max":       "2",
		"s3.bucket":       "aeromq-backup",
	})
	if err != nil {
		t.Fatalf("failed to update connector config: %v", err)
	}
	if updated.TasksCount != 2 || len(updated.Tasks) != 2 {
		t.Fatalf("expected 2 tasks after config update, got %d tasks", len(updated.Tasks))
	}
	if len(updated.Topics) != 2 || updated.Topics[1] != "invoices" {
		t.Fatalf("expected updated topics, got %v", updated.Topics)
	}

	// Update creates connector if not present but class is specified
	created, err := cm.UpdateConnectorConfig("new-dynamo-sink", map[string]string{
		"connector.class": "HttpWebhookSinkConnector",
		"http.url":        "http://dynamo:8000",
		"tasks.max":       "1",
	})
	if err != nil {
		t.Fatalf("failed to create via update config: %v", err)
	}
	if created.Name != "new-dynamo-sink" || created.Class != "HttpWebhookSinkConnector" {
		t.Fatalf("unexpected created connector: %+v", created)
	}

	// Missing connector without class errors out
	_, err = cm.UpdateConnectorConfig("non-existent-without-class", map[string]string{})
	if !errors.Is(err, connect.ErrConnectorNotFound) {
		t.Fatalf("expected ErrConnectorNotFound, got %v", err)
	}
}

func TestConnectorManager_ValidatePluginConfig(t *testing.T) {
	cm := connect.NewManager()

	// 1. Validation with missing required properties
	res, err := cm.ValidatePluginConfig("HttpWebhookSinkConnector", map[string]string{})
	if err != nil {
		t.Fatalf("validation failed unexpectedly: %v", err)
	}
	if res.ErrorCount == 0 {
		t.Fatalf("expected errors for empty config, got 0")
	}

	// Check that connector.class and http.url have errors
	var hasClassError, hasUrlError bool
	for _, c := range res.Configs {
		if c.Definition.Name == "connector.class" && len(c.Value.Errors) > 0 {
			hasClassError = true
		}
		if c.Definition.Name == "http.url" && len(c.Value.Errors) > 0 {
			hasUrlError = true
		}
	}
	if !hasClassError || !hasUrlError {
		t.Fatalf("expected class and url errors, got class=%v, url=%v", hasClassError, hasUrlError)
	}

	// 2. Validation with valid config
	validRes, err := cm.ValidatePluginConfig("HttpWebhookSinkConnector", map[string]string{
		"name":            "valid-webhook",
		"connector.class": "HttpWebhookSinkConnector",
		"topics":          "events",
		"http.url":        "https://example.com/webhook",
		"tasks.max":       "2",
	})
	if err != nil {
		t.Fatalf("validation failed: %v", err)
	}
	if validRes.ErrorCount != 0 {
		t.Fatalf("expected 0 errors for valid config, got %d: %+v", validRes.ErrorCount, validRes.Configs)
	}

	// 3. Unknown plugin
	_, err = cm.ValidatePluginConfig("NonExistentConnector", map[string]string{})
	if !errors.Is(err, connect.ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got %v", err)
	}
}

func TestConnectorManager_Validation(t *testing.T) {
	cm := connect.NewManager()

	if err := cm.RegisterConnector(nil); err == nil {
		t.Fatalf("expected error for nil connector")
	}

	if err := cm.RegisterConnector(&connect.Connector{Name: ""}); err == nil {
		t.Fatalf("expected error for empty name")
	}

	if err := cm.RegisterConnector(&connect.Connector{Name: "c1", Class: ""}); err == nil {
		t.Fatalf("expected error for empty class")
	}
}

func TestConnectorManager_PluginDiscovery(t *testing.T) {
	cm := connect.NewManager()
	plugins := cm.ListPlugins()

	expectedClasses := map[string]connect.ConnectorType{
		"HttpWebhookSinkConnector":  connect.TypeSink,
		"S3ArchivalSinkConnector":   connect.TypeSink,
		"DatabaseCdcSourceConnector": connect.TypeSource,
		"ElasticSearchSinkConnector": connect.TypeSink,
	}

	for _, p := range plugins {
		expectedType, ok := expectedClasses[p.Class]
		if !ok {
			t.Errorf("unexpected plugin class: %s", p.Class)
			continue
		}
		if p.Type != expectedType {
			t.Errorf("plugin %s: expected type %s, got %s", p.Class, expectedType, p.Type)
		}
		if p.Version == "" || p.Description == "" {
			t.Errorf("plugin %s missing version or description", p.Class)
		}
	}
}
