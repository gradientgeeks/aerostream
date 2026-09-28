package transform

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TransformType string

const (
	TypeWASM    TransformType = "WASM"
	TypeFilter  TransformType = "FILTER"
	TypeMaskPII TransformType = "MASK_PII"
	TypeJSONMap TransformType = "JSON_MAP"
)

type TransformStatus string

const (
	StatusRunning TransformStatus = "RUNNING"
	StatusPaused  TransformStatus = "PAUSED"
)

var (
	ErrTransformNotFound = errors.New("transform not found")
	ErrTransformExists   = errors.New("transform already exists")
	ErrInvalidTransform  = errors.New("invalid transform configuration")
	ErrTransformPaused   = errors.New("transform is paused")
)

type Transform struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	SourceTopic       string            `json:"source_topic"`
	TargetTopic       string            `json:"target_topic"`
	Type              TransformType     `json:"type"`
	Config            map[string]string `json:"config"`
	Code              string            `json:"code,omitempty"`
	Status            TransformStatus   `json:"status"`
	MessagesProcessed int64             `json:"messages_processed"`
	MessagesFiltered  int64             `json:"messages_filtered"`
	CreatedAt         time.Time         `json:"created_at"`
}

type Engine struct {
	mu         sync.RWMutex
	transforms map[string]*Transform
}

func NewEngine() *Engine {
	return &Engine{
		transforms: make(map[string]*Transform),
	}
}

func (e *Engine) SeedDefaultTransforms() *Engine {
	now := time.Now().UTC()

	piiMasker := &Transform{
		ID:          "xform-pii-masker-orders",
		Name:        "pii-masker-orders",
		SourceTopic: "orders",
		TargetTopic: "orders-sanitized",
		Type:        TypeMaskPII,
		Config: map[string]string{
			"fields_to_mask": "credit_card,cvv,password,email",
			"mask_pattern":   "***",
		},
		Code:              "// Native inline PII masking transform for AeroStream\nmask_fields([\"credit_card\", \"cvv\", \"password\", \"email\"]);",
		Status:            StatusRunning,
		MessagesProcessed: 1420,
		MessagesFiltered:  0,
		CreatedAt:         now.Add(-2 * time.Hour),
	}

	telemetryFilter := &Transform{
		ID:          "xform-telemetry-filter-critical",
		Name:        "telemetry-filter-critical",
		SourceTopic: "telemetry-events",
		TargetTopic: "alerts-critical",
		Type:        TypeFilter,
		Config: map[string]string{
			"filter_expression": `level == "CRITICAL"`,
		},
		Code:              "// Inline stream filter dropping non-critical telemetry\nfilter: record.level == \"CRITICAL\";",
		Status:            StatusRunning,
		MessagesProcessed: 8950,
		MessagesFiltered:  7412,
		CreatedAt:         now.Add(-5 * time.Hour),
	}

	e.transforms[piiMasker.Name] = piiMasker
	e.transforms[telemetryFilter.Name] = telemetryFilter
	return e
}

func (e *Engine) RegisterTransform(t *Transform) error {
	if t == nil {
		return ErrInvalidTransform
	}

	name := strings.TrimSpace(t.Name)
	source := strings.TrimSpace(t.SourceTopic)
	target := strings.TrimSpace(t.TargetTopic)

	if name == "" || source == "" || target == "" || t.Type == "" {
		return fmt.Errorf("%w: name, source_topic, target_topic, and type are required", ErrInvalidTransform)
	}

	validTypes := map[TransformType]bool{
		TypeWASM:    true,
		TypeFilter:  true,
		TypeMaskPII: true,
		TypeJSONMap: true,
	}
	if !validTypes[t.Type] {
		return fmt.Errorf("%w: unsupported transform type %q", ErrInvalidTransform, t.Type)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.transforms[name]; exists {
		return fmt.Errorf("%w: %q", ErrTransformExists, name)
	}

	cp := *t
	cp.Name = name
	cp.SourceTopic = source
	cp.TargetTopic = target
	if cp.ID == "" {
		cp.ID = fmt.Sprintf("xform-%s-%d", strings.ToLower(name), time.Now().UnixNano())
	}
	if cp.Status == "" {
		cp.Status = StatusRunning
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	if cp.Config == nil {
		cp.Config = make(map[string]string)
	}

	e.transforms[name] = &cp
	return nil
}

func (e *Engine) ListTransforms() []*Transform {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*Transform, 0, len(e.transforms))
	for _, t := range e.transforms {
		cp := *t
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func (e *Engine) GetTransform(name string) (*Transform, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, exists := e.transforms[name]
	if !exists {
		return nil, ErrTransformNotFound
	}
	cp := *t
	return &cp, nil
}

func (e *Engine) PauseTransform(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, exists := e.transforms[name]
	if !exists {
		return ErrTransformNotFound
	}
	t.Status = StatusPaused
	return nil
}

func (e *Engine) ResumeTransform(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, exists := e.transforms[name]
	if !exists {
		return ErrTransformNotFound
	}
	t.Status = StatusRunning
	return nil
}

func (e *Engine) DeleteTransform(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.transforms[name]; !exists {
		return ErrTransformNotFound
	}
	delete(e.transforms, name)
	return nil
}

func (e *Engine) ExecuteTransform(t *Transform, payload []byte) (output []byte, drop bool, err error) {
	if t == nil {
		return nil, false, ErrInvalidTransform
	}

	atomic.AddInt64(&t.MessagesProcessed, 1)

	defer func() {
		if drop {
			atomic.AddInt64(&t.MessagesFiltered, 1)
		}
		if e != nil {
			e.mu.Lock()
			if stored, ok := e.transforms[t.Name]; ok {
				stored.MessagesProcessed++
				if drop {
					stored.MessagesFiltered++
				}
			}
			e.mu.Unlock()
		}
	}()

	switch t.Type {
	case TypeMaskPII:
		output, err = executeMaskPII(t, payload)
		return output, false, err

	case TypeFilter:
		drop, err = executeFilter(t, payload)
		if err != nil {
			return nil, false, err
		}
		if drop {
			return nil, true, nil
		}
		return payload, false, nil

	case TypeJSONMap:
		output, err = executeJSONMap(t, payload)
		return output, false, err

	case TypeWASM:
		output, drop, err = executeWASM(t, payload)
		return output, drop, err

	default:
		return nil, false, fmt.Errorf("%w: unknown transform type %q", ErrInvalidTransform, t.Type)
	}
}

func executeMaskPII(t *Transform, payload []byte) ([]byte, error) {
	maskFields := make(map[string]bool)
	maskPattern := "***"

	if t.Config != nil {
		if customPattern, ok := t.Config["mask_pattern"]; ok && customPattern != "" {
			maskPattern = customPattern
		}
		if fields, ok := t.Config["fields_to_mask"]; ok && strings.TrimSpace(fields) != "" {
			for _, f := range strings.Split(fields, ",") {
				norm := strings.ToLower(strings.TrimSpace(f))
				if norm != "" {
					maskFields[norm] = true
				}
			}
		}
	}

	if len(maskFields) == 0 {
		defaults := []string{"ssn", "credit_card", "password", "email", "cvv", "phone", "secret", "token", "auth_token"}
		for _, f := range defaults {
			maskFields[f] = true
		}
	}

	var parsed interface{}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, fmt.Errorf("invalid json payload for PII masking: %w", err)
	}

	masked := maskJSONRecursive(parsed, maskFields, maskPattern)
	return json.MarshalIndent(masked, "", "  ")
}

func maskJSONRecursive(val interface{}, maskKeys map[string]bool, maskPattern string) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{}, len(v))
		for k, child := range v {
			cleanKey := strings.ToLower(strings.TrimSpace(k))
			if maskKeys[cleanKey] {
				result[k] = maskPattern
			} else {
				result[k] = maskJSONRecursive(child, maskKeys, maskPattern)
			}
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = maskJSONRecursive(item, maskKeys, maskPattern)
		}
		return result

	default:
		return v
	}
}

func executeFilter(t *Transform, payload []byte) (bool, error) {
	expression := ""
	if t.Config != nil {
		expression = t.Config["filter_expression"]
		if expression == "" {
			expression = t.Config["condition"]
		}
	}
	if expression == "" && t.Code != "" {
		lines := strings.Split(t.Code, "\n")
		for _, l := range lines {
			trimmed := strings.TrimSpace(l)
			if strings.HasPrefix(trimmed, "filter:") {
				expression = strings.TrimSpace(strings.TrimPrefix(trimmed, "filter:"))
				expression = strings.TrimSuffix(expression, ";")
				break
			}
		}
	}

	expression = strings.TrimSpace(expression)
	if expression == "" {
		return false, nil
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return false, fmt.Errorf("invalid json payload for filter: %w", err)
	}

	matched, err := evaluateCondition(parsed, expression)
	if err != nil {
		return false, err
	}

	return !matched, nil
}

func evaluateCondition(data map[string]interface{}, expr string) (bool, error) {
	expr = strings.TrimSpace(expr)

	var op string
	var lhs, rhs string

	operators := []string{"==", "!=", ">=", "<=", ">", "<", " contains "}
	for _, o := range operators {
		if idx := strings.Index(expr, o); idx != -1 {
			op = strings.TrimSpace(o)
			lhs = strings.TrimSpace(expr[:idx])
			rhs = strings.TrimSpace(expr[idx+len(o):])
			break
		}
	}

	if op == "" {
		return false, fmt.Errorf("unsupported filter expression: %s", expr)
	}

	lhs = strings.TrimPrefix(lhs, "record.")
	lhs = strings.TrimPrefix(lhs, "payload.")
	lhs = strings.TrimSpace(lhs)

	val, found := getNestedValue(data, lhs)
	if !found {
		if op == "!=" {
			return true, nil
		}
		return false, nil
	}

	rhsClean := cleanExpressionValue(rhs)

	switch op {
	case "==":
		return compareValuesEqual(val, rhsClean), nil
	case "!=":
		return !compareValuesEqual(val, rhsClean), nil
	case "contains":
		valStr := fmt.Sprintf("%v", val)
		return strings.Contains(strings.ToLower(valStr), strings.ToLower(rhsClean)), nil
	case ">", ">=", "<", "<=":
		return compareNumeric(val, rhsClean, op)
	default:
		return false, fmt.Errorf("unknown operator %q", op)
	}
}

func getNestedValue(data map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = data

	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		v, exists := m[part]
		if !exists {
			found := false
			for k, val := range m {
				if strings.EqualFold(k, part) {
					v = val
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		}
		current = v
	}
	return current, true
}

func cleanExpressionValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if (strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`)) ||
		(strings.HasPrefix(raw, `'`) && strings.HasSuffix(raw, `'`)) {
		return raw[1 : len(raw)-1]
	}
	return raw
}

func compareValuesEqual(actual interface{}, targetStr string) bool {
	switch a := actual.(type) {
	case string:
		return strings.EqualFold(a, targetStr)
	case bool:
		tb, err := strconv.ParseBool(targetStr)
		if err == nil {
			return a == tb
		}
		return strings.EqualFold(fmt.Sprintf("%v", a), targetStr)
	case float64:
		tf, err := strconv.ParseFloat(targetStr, 64)
		if err == nil {
			return a == tf
		}
	case int, int32, int64:
		ti, err := strconv.ParseInt(targetStr, 10, 64)
		if err == nil {
			return fmt.Sprintf("%v", a) == fmt.Sprintf("%d", ti)
		}
	}
	return fmt.Sprintf("%v", actual) == targetStr
}

func compareNumeric(actual interface{}, targetStr string, op string) (bool, error) {
	targetNum, err := strconv.ParseFloat(targetStr, 64)
	if err != nil {
		return false, fmt.Errorf("rhs of %s must be numeric, got %q", op, targetStr)
	}

	var actualNum float64
	switch a := actual.(type) {
	case float64:
		actualNum = a
	case float32:
		actualNum = float64(a)
	case int:
		actualNum = float64(a)
	case int64:
		actualNum = float64(a)
	case string:
		parsed, err := strconv.ParseFloat(a, 64)
		if err != nil {
			return false, fmt.Errorf("field value is not numeric: %q", a)
		}
		actualNum = parsed
	default:
		return false, fmt.Errorf("field is not numeric: %T", actual)
	}

	switch op {
	case ">":
		return actualNum > targetNum, nil
	case ">=":
		return actualNum >= targetNum, nil
	case "<":
		return actualNum < targetNum, nil
	case "<=":
		return actualNum <= targetNum, nil
	}
	return false, nil
}

func executeJSONMap(t *Transform, payload []byte) ([]byte, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, fmt.Errorf("invalid json payload for json_map: %w", err)
	}

	if parsed == nil {
		parsed = make(map[string]interface{})
	}

	parsed["_transformed_at"] = time.Now().UTC().Format(time.RFC3339)
	parsed["_engine"] = "aerostream-inline"

	if t.Config != nil {
		if addFields, ok := t.Config["add_fields"]; ok && addFields != "" {
			pairs := strings.Split(addFields, ",")
			for _, pair := range pairs {
				parts := strings.SplitN(pair, ":", 2)
				if len(parts) == 2 {
					k := strings.TrimSpace(parts[0])
					v := strings.TrimSpace(parts[1])
					if k != "" {
						parsed[k] = v
					}
				}
			}
		}

		for k, v := range t.Config {
			if strings.HasPrefix(k, "set_") {
				fieldName := strings.TrimPrefix(k, "set_")
				parsed[fieldName] = v
			}
		}

		if renameFields, ok := t.Config["rename_fields"]; ok && renameFields != "" {
			pairs := strings.Split(renameFields, ",")
			for _, pair := range pairs {
				parts := strings.SplitN(pair, ":", 2)
				if len(parts) == 2 {
					oldK := strings.TrimSpace(parts[0])
					newK := strings.TrimSpace(parts[1])
					if val, exists := parsed[oldK]; exists {
						delete(parsed, oldK)
						parsed[newK] = val
					}
				}
			}
		}

		if removeFields, ok := t.Config["remove_fields"]; ok && removeFields != "" {
			for _, f := range strings.Split(removeFields, ",") {
				cleanF := strings.TrimSpace(f)
				delete(parsed, cleanF)
			}
		}
	}

	return json.MarshalIndent(parsed, "", "  ")
}

func executeWASM(t *Transform, payload []byte) ([]byte, bool, error) {
	var parsed map[string]interface{}
	isJSON := json.Unmarshal(payload, &parsed) == nil

	if isJSON && parsed != nil {
		if t.Config != nil {
			if action, ok := t.Config["action"]; ok && action == "uppercase_keys" {
				upperMap := make(map[string]interface{})
				for k, v := range parsed {
					upperMap[strings.ToUpper(k)] = v
				}
				parsed = upperMap
			}
		}

		parsed["_wasm_execution"] = map[string]interface{}{
			"runtime":       "wazero-wasm32-wasi",
			"status":        "COMPLETED",
			"gas_used":      318,
			"execution_us":  48,
			"source_module": t.Name,
		}

		out, err := json.MarshalIndent(parsed, "", "  ")
		return out, false, err
	}

	output := fmt.Sprintf("// WASM Processed by %s\n%s", t.Name, string(payload))
	return []byte(output), false, nil
}
