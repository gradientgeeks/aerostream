package connect_test

import (
	"testing"

	"github.com/gradientgeeks/aerostream/go-controller/pkg/connect"
)

func TestConnectorManager_Defaults(t *testing.T) {
	cm := connect.NewManager()

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

	// Resume
	if err := cm.ResumeConnector("cdc-postgres-source"); err != nil {
		t.Fatalf("failed to resume connector: %v", err)
	}
	resumed, _ := cm.GetConnector("cdc-postgres-source")
	if resumed.State != connect.StateRunning {
		t.Fatalf("expected state RUNNING, got %s", resumed.State)
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
