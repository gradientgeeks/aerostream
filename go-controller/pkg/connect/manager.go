package connect

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	StateRunning    ConnectorState = "RUNNING"
	StatePaused     ConnectorState = "PAUSED"
	StateStopped    ConnectorState = "STOPPED"
	StateFailed     ConnectorState = "FAILED"
	StateUnassigned ConnectorState = "UNASSIGNED"
)

var (
	ErrConnectorNotFound = errors.New("connector not found")
	ErrConnectorExists   = errors.New("connector already exists")
	ErrInvalidConnector  = errors.New("invalid connector specification")
	ErrPluginNotFound    = errors.New("plugin not found")
	ErrTaskNotFound      = errors.New("task not found")
)

type TaskStatus struct {
	ID       int            `json:"id"`
	State    ConnectorState `json:"state"`
	WorkerID string         `json:"worker_id"`
	Trace    string         `json:"trace,omitempty"`
}

type Connector struct {
	Name             string            `json:"name"`
	Type             ConnectorType     `json:"type"`
	Class            string            `json:"class"`
	Topic            string            `json:"topic"`
	Topics           []string          `json:"topics,omitempty"`
	Config           map[string]string `json:"config"`
	State            ConnectorState    `json:"state"`
	TasksCount       int               `json:"tasks_count"`
	Tasks            []TaskStatus      `json:"tasks,omitempty"`
	WorkerID         string            `json:"worker_id,omitempty"`
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

// ConfigDefinition describes a single configuration property for validation
type ConfigDefinition struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	Required      bool     `json:"required"`
	DefaultValue  *string  `json:"default_value"`
	Importance    string   `json:"importance"`
	Documentation string   `json:"documentation"`
	Group         string   `json:"group"`
	Width         string   `json:"width"`
	DisplayName   string   `json:"display_name"`
	Dependents    []string `json:"dependents"`
	Order         int      `json:"order"`
}

// ConfigValue represents the provided or computed value and any validation errors
type ConfigValue struct {
	Name              string   `json:"name"`
	Value             *string  `json:"value"`
	RecommendedValues []string `json:"recommended_values"`
	Errors            []string `json:"errors"`
	Visible           bool     `json:"visible"`
}

// ConfigValidationItem pairs a definition with its evaluated value
type ConfigValidationItem struct {
	Definition ConfigDefinition `json:"definition"`
	Value      ConfigValue      `json:"value"`
}

// ConfigValidationResult matches Kafka Connect validation response schema
type ConfigValidationResult struct {
	Name       string                 `json:"name"`
	ErrorCount int                    `json:"error_count"`
	Groups     []string               `json:"groups"`
	Configs    []ConfigValidationItem `json:"configs"`
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
	return cm
}

func (cm *ConnectorManager) SeedDefaultConnectors() *ConnectorManager {
	now := time.Now().UTC()
	defaultWorker := "aerostream-worker-1"

	// Seed default running connectors
	cm.connectors["s3-cold-storage-sink"] = &Connector{
		Name:   "s3-cold-storage-sink",
		Type:   TypeSink,
		Class:  "S3ArchivalSinkConnector",
		Topic:  "orders",
		Topics: []string{"orders"},
		Config: map[string]string{
			"connector.class": "S3ArchivalSinkConnector",
			"name":            "s3-cold-storage-sink",
			"tasks.max":       "1",
			"topics":          "orders",
			"s3.bucket":       "aeromq-archives",
			"s3.region":       "us-east-1",
			"flush.size":      "1000",
		},
		State:      StateRunning,
		TasksCount: 1,
		Tasks: []TaskStatus{
			{
				ID:       0,
				State:    StateRunning,
				WorkerID: defaultWorker,
			},
		},
		WorkerID:         defaultWorker,
		RecordsProcessed: 142580,
		BytesTransferred: 52428800,
		CreatedAt:        now.Add(-24 * time.Hour),
	}

	cm.connectors["webhook-payment-notifier"] = &Connector{
		Name:   "webhook-payment-notifier",
		Type:   TypeSink,
		Class:  "HttpWebhookSinkConnector",
		Topic:  "payment-transactions",
		Topics: []string{"payment-transactions"},
		Config: map[string]string{
			"connector.class": "HttpWebhookSinkConnector",
			"name":            "webhook-payment-notifier",
			"tasks.max":       "1",
			"topics":          "payment-transactions",
			"http.url":        "https://api.internal/webhooks/payments",
			"http.method":     "POST",
		},
		State:      StateRunning,
		TasksCount: 1,
		Tasks: []TaskStatus{
			{
				ID:       0,
				State:    StateRunning,
				WorkerID: defaultWorker,
			},
		},
		WorkerID:         defaultWorker,
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

	if c.Class == "" && c.Config != nil {
		c.Class = c.Config["connector.class"]
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
	if tasksCount <= 0 && c.Config != nil {
		if tMax, ok := c.Config["tasks.max"]; ok {
			if n, err := strconv.Atoi(tMax); err == nil && n > 0 {
				tasksCount = n
			}
		}
	}
	if tasksCount <= 0 {
		tasksCount = 1
	}

	createdAt := c.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	workerID := c.WorkerID
	if workerID == "" {
		workerID = "aerostream-worker-1"
	}

	topics := make([]string, 0)
	if len(c.Topics) > 0 {
		topics = append(topics, c.Topics...)
	} else if c.Topic != "" {
		topics = append(topics, c.Topic)
	} else if c.Config != nil && c.Config["topics"] != "" {
		for _, part := range strings.Split(c.Config["topics"], ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				topics = append(topics, trimmed)
			}
		}
	}

	topic := c.Topic
	if topic == "" && len(topics) > 0 {
		topic = topics[0]
	}

	cfgCopy := make(map[string]string)
	for k, v := range c.Config {
		cfgCopy[k] = v
	}
	if cfgCopy["connector.class"] == "" {
		cfgCopy["connector.class"] = c.Class
	}
	if cfgCopy["name"] == "" {
		cfgCopy["name"] = c.Name
	}
	if len(topics) > 0 && cfgCopy["topics"] == "" {
		cfgCopy["topics"] = strings.Join(topics, ",")
	}
	if cfgCopy["tasks.max"] == "" {
		cfgCopy["tasks.max"] = strconv.Itoa(tasksCount)
	}

	tasks := make([]TaskStatus, 0, tasksCount)
	if len(c.Tasks) > 0 {
		tasks = append(tasks, c.Tasks...)
	} else {
		for i := 0; i < tasksCount; i++ {
			tasks = append(tasks, TaskStatus{
				ID:       i,
				State:    state,
				WorkerID: workerID,
			})
		}
	}

	cm.connectors[c.Name] = &Connector{
		Name:             c.Name,
		Type:             targetType,
		Class:            c.Class,
		Topic:            topic,
		Topics:           topics,
		Config:           cfgCopy,
		State:            state,
		TasksCount:       tasksCount,
		Tasks:            tasks,
		WorkerID:         workerID,
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
	for i := range c.Tasks {
		c.Tasks[i].State = StatePaused
	}
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
	for i := range c.Tasks {
		c.Tasks[i].State = StateRunning
	}
	return nil
}

func (cm *ConnectorManager) StopConnector(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	c.State = StateStopped
	for i := range c.Tasks {
		c.Tasks[i].State = StateStopped
	}
	return nil
}

func (cm *ConnectorManager) RestartConnector(name string, includeTasks bool, onlyFailed bool) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	if onlyFailed {
		if c.State == StateFailed {
			c.State = StateRunning
			c.LastError = ""
		}
		if includeTasks {
			for i := range c.Tasks {
				if c.Tasks[i].State == StateFailed {
					c.Tasks[i].State = StateRunning
					c.Tasks[i].Trace = ""
				}
			}
		}
	} else {
		c.State = StateRunning
		c.LastError = ""
		if includeTasks {
			for i := range c.Tasks {
				c.Tasks[i].State = StateRunning
				c.Tasks[i].Trace = ""
			}
		}
	}

	return nil
}

func (cm *ConnectorManager) GetTaskStatus(name string, taskID int) (*TaskStatus, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	c, exists := cm.connectors[name]
	if !exists {
		return nil, ErrConnectorNotFound
	}

	for _, t := range c.Tasks {
		if t.ID == taskID {
			taskCopy := t
			return &taskCopy, nil
		}
	}

	return nil, ErrTaskNotFound
}

func (cm *ConnectorManager) RestartTask(name string, taskID int) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	for i := range c.Tasks {
		if c.Tasks[i].ID == taskID {
			c.Tasks[i].State = StateRunning
			c.Tasks[i].Trace = ""
			return nil
		}
	}

	return ErrTaskNotFound
}

func (cm *ConnectorManager) GetConnectorTopics(name string) ([]string, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	c, exists := cm.connectors[name]
	if !exists {
		return nil, ErrConnectorNotFound
	}

	result := make([]string, len(c.Topics))
	copy(result, c.Topics)
	return result, nil
}

func (cm *ConnectorManager) ResetConnectorTopics(name string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound
	}

	c.Topics = []string{}
	return nil
}

func (cm *ConnectorManager) UpdateConnectorConfig(name string, cfg map[string]string) (*Connector, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c, exists := cm.connectors[name]
	if !exists {
		class := cfg["connector.class"]
		if class == "" {
			return nil, ErrConnectorNotFound
		}

		// Create a new connector dynamically
		targetType := TypeSink
		if plugin, ok := cm.plugins[class]; ok {
			targetType = plugin.Type
		}

		tasksCount := 1
		if tMax, ok := cfg["tasks.max"]; ok {
			if n, err := strconv.Atoi(tMax); err == nil && n > 0 {
				tasksCount = n
			}
		}

		workerID := "aerostream-worker-1"
		topics := make([]string, 0)
		if topStr, ok := cfg["topics"]; ok && topStr != "" {
			for _, part := range strings.Split(topStr, ",") {
				if tr := strings.TrimSpace(part); tr != "" {
					topics = append(topics, tr)
				}
			}
		}

		topic := ""
		if len(topics) > 0 {
			topic = topics[0]
		}

		tasks := make([]TaskStatus, 0, tasksCount)
		for i := 0; i < tasksCount; i++ {
			tasks = append(tasks, TaskStatus{
				ID:       i,
				State:    StateRunning,
				WorkerID: workerID,
			})
		}

		cfgCopy := make(map[string]string)
		for k, v := range cfg {
			cfgCopy[k] = v
		}
		cfgCopy["name"] = name
		cfgCopy["connector.class"] = class
		if len(topics) > 0 && cfgCopy["topics"] == "" {
			cfgCopy["topics"] = strings.Join(topics, ",")
		}
		if cfgCopy["tasks.max"] == "" {
			cfgCopy["tasks.max"] = strconv.Itoa(tasksCount)
		}

		created := &Connector{
			Name:       name,
			Type:       targetType,
			Class:      class,
			Topic:      topic,
			Topics:     topics,
			Config:     cfgCopy,
			State:      StateRunning,
			TasksCount: tasksCount,
			Tasks:      tasks,
			WorkerID:   workerID,
			CreatedAt:  time.Now().UTC(),
		}
		cm.connectors[name] = created
		return deepCopyConnector(created), nil
	}

	// Update existing connector config
	cfgCopy := make(map[string]string)
	for k, v := range cfg {
		cfgCopy[k] = v
	}
	cfgCopy["name"] = name

	if class, ok := cfg["connector.class"]; ok && class != "" {
		c.Class = class
		if plugin, ok := cm.plugins[class]; ok {
			c.Type = plugin.Type
		}
	}

	if topicsStr, ok := cfg["topics"]; ok {
		topics := make([]string, 0)
		for _, part := range strings.Split(topicsStr, ",") {
			if tr := strings.TrimSpace(part); tr != "" {
				topics = append(topics, tr)
			}
		}
		c.Topics = topics
		if len(topics) > 0 {
			c.Topic = topics[0]
		}
	}

	if tMax, ok := cfg["tasks.max"]; ok {
		if n, err := strconv.Atoi(tMax); err == nil && n > 0 {
			c.TasksCount = n
			// Reconcile tasks
			if len(c.Tasks) < n {
				for i := len(c.Tasks); i < n; i++ {
					c.Tasks = append(c.Tasks, TaskStatus{
						ID:       i,
						State:    c.State,
						WorkerID: c.WorkerID,
					})
				}
			} else if len(c.Tasks) > n {
				c.Tasks = c.Tasks[:n]
			}
		}
	}

	c.Config = cfgCopy
	return deepCopyConnector(c), nil
}

func (cm *ConnectorManager) ValidatePluginConfig(pluginName string, config map[string]string) (*ConfigValidationResult, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var plugin *ConnectorPlugin
	for _, p := range cm.plugins {
		if p.Class == pluginName || strings.HasSuffix(pluginName, "."+p.Class) || strings.EqualFold(p.Class, pluginName) {
			plugin = p
			break
		}
	}
	if plugin == nil {
		return nil, fmt.Errorf("%w: %s", ErrPluginNotFound, pluginName)
	}

	groups := []string{"Common", "Transforms", "Predicates", "Error Handling"}
	configs := make([]ConfigValidationItem, 0)
	totalErrors := 0

	addConfig := func(name string, cfgType string, required bool, defVal *string, importance string, doc string, group string, dispName string, order int, validateFn func(val *string) []string) {
		var valPtr *string
		if v, ok := config[name]; ok {
			vCopy := v
			valPtr = &vCopy
		} else if defVal != nil {
			vCopy := *defVal
			valPtr = &vCopy
		}

		var errs []string
		if validateFn != nil {
			errs = validateFn(valPtr)
		} else if required && (valPtr == nil || *valPtr == "") {
			errs = []string{fmt.Sprintf("Missing required configuration \"%s\" which has no default value.", name)}
		}

		if len(errs) > 0 {
			totalErrors += len(errs)
		}

		configs = append(configs, ConfigValidationItem{
			Definition: ConfigDefinition{
				Name:          name,
				Type:          cfgType,
				Required:      required,
				DefaultValue:  defVal,
				Importance:    importance,
				Documentation: doc,
				Group:         group,
				Width:         "MEDIUM",
				DisplayName:   dispName,
				Dependents:    []string{},
				Order:         order,
			},
			Value: ConfigValue{
				Name:              name,
				Value:             valPtr,
				RecommendedValues: []string{},
				Errors:            errs,
				Visible:           true,
			},
		})
	}

	// 1. name
	addConfig("name", "STRING", true, nil, "HIGH", "Globally unique name to use for this connector.", "Common", "Connector Name", 1, func(v *string) []string {
		if v == nil || *v == "" {
			return []string{"Missing required configuration \"name\" which has no default value."}
		}
		return nil
	})

	// 2. connector.class
	addConfig("connector.class", "STRING", true, nil, "HIGH", "Connector class name.", "Common", "Connector Class", 2, func(v *string) []string {
		if v == nil || *v == "" {
			return []string{"Missing required configuration \"connector.class\" which has no default value."}
		}
		return nil
	})

	// 3. tasks.max
	defTasks := "1"
	addConfig("tasks.max", "INT", false, &defTasks, "HIGH", "Maximum number of tasks for this connector.", "Common", "Tasks Max", 3, func(v *string) []string {
		if v != nil && *v != "" {
			if n, err := strconv.Atoi(*v); err != nil || n <= 0 {
				return []string{fmt.Sprintf("Invalid value %s for configuration tasks.max: Not a positive integer", *v)}
			}
		}
		return nil
	})

	// 4. topics (for Sinks)
	order := 4
	if plugin.Type == TypeSink {
		addConfig("topics", "LIST", true, nil, "HIGH", "List of topics to consume from.", "Common", "Topics", order, func(v *string) []string {
			if (v == nil || *v == "") && config["topic"] == "" && config["topics.regex"] == "" {
				return []string{"Missing required configuration \"topics\" which has no default value."}
			}
			return nil
		})
		order++
	}

	// 5. Plugin-specific properties
	switch plugin.Class {
	case "HttpWebhookSinkConnector":
		addConfig("http.url", "STRING", true, nil, "HIGH", "Destination HTTP REST endpoint URL.", "Common", "HTTP URL", order, func(v *string) []string {
			if v == nil || *v == "" {
				return []string{"Missing required configuration \"http.url\" which has no default value."}
			}
			return nil
		})
		order++
		defMethod := "POST"
		addConfig("http.method", "STRING", false, &defMethod, "MEDIUM", "HTTP method (POST, PUT).", "Common", "HTTP Method", order, nil)
		order++

	case "S3ArchivalSinkConnector":
		addConfig("s3.bucket", "STRING", true, nil, "HIGH", "Destination AWS S3 bucket name.", "Common", "S3 Bucket", order, func(v *string) []string {
			if v == nil || *v == "" {
				return []string{"Missing required configuration \"s3.bucket\" which has no default value."}
			}
			return nil
		})
		order++
		defRegion := "us-east-1"
		addConfig("s3.region", "STRING", false, &defRegion, "MEDIUM", "AWS Region.", "Common", "S3 Region", order, nil)
		order++

	case "DatabaseCdcSourceConnector":
		addConfig("db.host", "STRING", true, nil, "HIGH", "Database hostname.", "Common", "DB Host", order, func(v *string) []string {
			if v == nil || *v == "" {
				return []string{"Missing required configuration \"db.host\" which has no default value."}
			}
			return nil
		})
		order++
		defPort := "5432"
		addConfig("db.port", "INT", false, &defPort, "MEDIUM", "Database port.", "Common", "DB Port", order, nil)
		order++

	case "ElasticSearchSinkConnector":
		addConfig("es.host", "STRING", true, nil, "HIGH", "Elasticsearch host URL.", "Common", "Elasticsearch Host", order, func(v *string) []string {
			if v == nil || *v == "" {
				return []string{"Missing required configuration \"es.host\" which has no default value."}
			}
			return nil
		})
		order++
		defIndex := "default-index"
		addConfig("es.index", "STRING", false, &defIndex, "MEDIUM", "Elasticsearch target index name.", "Common", "Elasticsearch Index", order, nil)
		order++
	}

	// 6. Include any remaining user configs not explicitly enumerated
	handled := make(map[string]bool)
	for _, cItem := range configs {
		handled[cItem.Definition.Name] = true
	}

	var extraKeys []string
	for k := range config {
		if !handled[k] {
			extraKeys = append(extraKeys, k)
		}
	}
	sort.Strings(extraKeys)
	for _, k := range extraKeys {
		val := config[k]
		valPtr := &val
		configs = append(configs, ConfigValidationItem{
			Definition: ConfigDefinition{
				Name:          k,
				Type:          "STRING",
				Required:      false,
				Importance:    "LOW",
				Documentation: "",
				Group:         "Common",
				Width:         "MEDIUM",
				DisplayName:   k,
				Dependents:    []string{},
				Order:         order,
			},
			Value: ConfigValue{
				Name:              k,
				Value:             valPtr,
				RecommendedValues: []string{},
				Errors:            []string{},
				Visible:           true,
			},
		})
		order++
	}

	return &ConfigValidationResult{
		Name:       plugin.Class,
		ErrorCount: totalErrors,
		Groups:     groups,
		Configs:    configs,
	}, nil
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

	tasks := make([]TaskStatus, len(c.Tasks))
	copy(tasks, c.Tasks)

	topics := make([]string, len(c.Topics))
	copy(topics, c.Topics)

	return &Connector{
		Name:             c.Name,
		Type:             c.Type,
		Class:            c.Class,
		Topic:            c.Topic,
		Topics:           topics,
		Config:           cfg,
		State:            c.State,
		TasksCount:       c.TasksCount,
		Tasks:            tasks,
		WorkerID:         c.WorkerID,
		RecordsProcessed: c.RecordsProcessed,
		BytesTransferred: c.BytesTransferred,
		LastError:        c.LastError,
		CreatedAt:        c.CreatedAt,
	}
}
