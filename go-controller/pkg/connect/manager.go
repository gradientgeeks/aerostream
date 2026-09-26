package connect

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type ConnectorType string

const (
	TypeSource ConnectorType = "SOURCE"
	TypeSink   ConnectorType = "SINK"
)

type ConnectorState string

const (
	StateRunning ConnectorState = "RUNNING"
	StatePaused  ConnectorState = "PAUSED"
	StateFailed  ConnectorState = "FAILED"
)

var (
	ErrConnectorNotFound = errors.New("connector not found")
	ErrConnectorExists   = errors.New("connector already exists")
	ErrInvalidConnector  = errors.New("invalid connector specification")
)

type Connector struct {
	Name             string            `json:"name"`
	Type             ConnectorType     `json:"type"`
	Class            string            `json:"class"`
	Topic            string            `json:"topic"`
	Config           map[string]string `json:"config"`
	State            ConnectorState    `json:"state"`
	TasksCount       int               `json:"tasks_count"`
	RecordsProcessed int64             `json:"records_processed"`
	BytesTransferred int64             `json:"bytes_transferred"`
	LastError        string            `json:"last_error,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
}

type ConnectorPlugin struct {
	Class       string        `json:"class"`
	Type        ConnectorType `json:"type"`
	Version     string        `json:"version"`
	Description string        `json:"description"`
}

type ConnectorManager struct {
	mu         sync.RWMutex
	plugins    map[string]*ConnectorPlugin
	connectors map[string]*Connector
}

func NewManager() *ConnectorManager {
	cm := &ConnectorManager{
		plugins:    make(map[string]*ConnectorPlugin),
		connectors: make(map[string]*Connector),
	}

	// Seed built-in connector plugins
	plugins := []*ConnectorPlugin{
		{
			Class:       "HttpWebhookSinkConnector",
			Type:        TypeSink,
			Version:     "1.0.0",
			Description: "Dispatches topic records to external HTTP REST endpoints.",
		},
		{
			Class:       "S3ArchivalSinkConnector",
			Type:        TypeSink,
			Version:     "1.0.0",
			Description: "Streams records to AWS S3 / MinIO object storage.",
		},
		{
			Class:       "DatabaseCdcSourceConnector",
			Type:        TypeSource,
			Version:     "1.0.0",
			Description: "Ingests simulated Change Data Capture (CDC) events from PostgreSQL/MySQL.",
		},
		{
			Class:       "ElasticSearchSinkConnector",
			Type:        TypeSink,
			Version:     "1.0.0",
			Description: "Streams records to Elasticsearch / OpenSearch index.",
		},
	}
	for _, p := range plugins {
		cm.plugins[p.Class] = p
	}

	now := time.Now().UTC()

	// Seed default running connectors
	cm.connectors["s3-cold-storage-sink"] = &Connector{
		Name:  "s3-cold-storage-sink",
		Type:  TypeSink,
		Class: "S3ArchivalSinkConnector",
		Topic: "orders",
		Config: map[string]string{
			"connector.class": "S3ArchivalSinkConnector",
			"tasks.max":       "1",
			"topics":          "orders",
			"s3.bucket":       "aeromq-archives",
			"s3.region":       "us-east-1",
			"flush.size":      "1000",
		},
		State:            StateRunning,
		TasksCount:       1,
		RecordsProcessed: 142580,
		BytesTransferred: 52428800,
		CreatedAt:        now.Add(-24 * time.Hour),
	}

	cm.connectors["webhook-payment-notifier"] = &Connector{
		Name:  "webhook-payment-notifier",
		Type:  TypeSink,
		Class: "HttpWebhookSinkConnector",
		Topic: "payment-transactions",
		Config: map[string]string{
			"connector.class": "HttpWebhookSinkConnector",
			"tasks.max":       "1",
			"topics":          "payment-transactions",
			"http.url":        "https://api.internal/webhooks/payments",
			"http.method":     "POST",
		},
		State:            StateRunning,
		TasksCount:       1,
		RecordsProcessed: 89400,
		BytesTransferred: 18874368,
		CreatedAt:        now.Add(-12 * time.Hour),
	}

	return cm
}

func (cm *ConnectorManager) RegisterConnector(c *Connector) error {
	if c == nil {
		return ErrInvalidConnector
	}
	if c.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidConnector)
	}
	if c.Class == "" {
		return fmt.Errorf("%w: class is required", ErrInvalidConnector)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connectors[c.Name]; exists {
		return ErrConnectorExists
	}

	// Auto-infer type from plugin if empty
	targetType := c.Type
	if targetType == "" {
		if plugin, ok := cm.plugins[c.Class]; ok {
			targetType = plugin.Type
		} else {
			targetType = TypeSink
		}
	}

	state := c.State
	if state == "" {
		state = StateRunning
	}

	tasksCount := c.TasksCount
	if tasksCount <= 0 {
		tasksCount = 1
	}

	createdAt := c.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	cfgCopy := make(map[string]string)
	for k, v := range c.Config {
		cfgCopy[k] = v
	}

	cm.connectors[c.Name] = &Connector{
		Name:             c.Name,
		Type:             targetType,
		Class:            c.Class,
		Topic:            c.Topic,
		Config:           cfgCopy,
		State:            state,
		TasksCount:       tasksCount,
		RecordsProcessed: c.RecordsProcessed,
		BytesTransferred: c.BytesTransferred,
		LastError:        c.LastError,
		CreatedAt:        createdAt,
	}

	return nil
}

func (cm *ConnectorManager) ListConnectors() []*Connector {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make([]*Connector, 0, len(cm.connectors))
	for _, c := range cm.connectors {
		result = append(result, deepCopyConnector(c))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

func (cm *ConnectorManager) GetConnector(name string) (*Connector, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	c, exists := cm.connectors[name]
	if !exists {
		return nil, ErrConnectorNotFound
	}

	return deepCopyConnector(c), nil
}

func (cm *ConnectorManager) PauseConnector(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	c.State = StatePaused
	return nil
}

func (cm *ConnectorManager) ResumeConnector(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	c.State = StateRunning
	return nil
}

func (cm *ConnectorManager) DeleteConnector(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connectors[name]; !exists {
		return ErrConnectorNotFound
	}

	delete(cm.connectors, name)
	return nil
}

func (cm *ConnectorManager) ListPlugins() []*ConnectorPlugin {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make([]*ConnectorPlugin, 0, len(cm.plugins))
	for _, p := range cm.plugins {
		copyPlugin := *p
		result = append(result, &copyPlugin)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Class < result[j].Class
	})

	return result
}

func deepCopyConnector(c *Connector) *Connector {
	if c == nil {
		return nil
	}
	cfg := make(map[string]string, len(c.Config))
	for k, v := range c.Config {
		cfg[k] = v
	}
	return &Connector{
		Name:             c.Name,
		Type:             c.Type,
		Class:            c.Class,
		Topic:            c.Topic,
		Config:           cfg,
		State:            c.State,
		TasksCount:       c.TasksCount,
		RecordsProcessed: c.RecordsProcessed,
		BytesTransferred: c.BytesTransferred,
		LastError:        c.LastError,
		CreatedAt:        c.CreatedAt,
	}
}
