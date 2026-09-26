package schemaregistry

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Supported schema types.
const (
	TypeAvro     = "AVRO"
	TypeJSON     = "JSON"
	TypeProtobuf = "PROTOBUF"
)

// CompatibilityLevel defines schema evolution compatibility rules.
type CompatibilityLevel string

const (
	BACKWARD CompatibilityLevel = "BACKWARD"
	FORWARD  CompatibilityLevel = "FORWARD"
	FULL     CompatibilityLevel = "FULL"
	NONE     CompatibilityLevel = "NONE"

	// Aliases
	CompatibilityBackward = BACKWARD
	CompatibilityForward  = FORWARD
	CompatibilityFull     = FULL
	CompatibilityNone     = NONE
)

var (
	ErrSchemaNotFound     = errors.New("schema not found")
	ErrSubjectNotFound    = errors.New("subject not found")
	ErrVersionNotFound    = errors.New("version not found")
	ErrIncompatibleSchema = errors.New("schema is incompatible")
	ErrInvalidSchema      = errors.New("invalid schema")
	ErrInvalidCompatLevel = errors.New("invalid compatibility level")
)

// Schema represents a registered schema record.
type Schema struct {
	ID      int64  `json:"id"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Type    string `json:"schemaType,omitempty"`
	Schema  string `json:"schema"`
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	type Alias Schema
	sType := s.Type
	if sType == "" {
		sType = TypeAvro
	}
	return json.Marshal(&struct {
		*Alias
		SchemaType string `json:"schemaType"`
		Type       string `json:"type,omitempty"`
	}{
		Alias:      (*Alias)(s),
		SchemaType: sType,
		Type:       sType,
	})
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	type Alias Schema
	aux := &struct {
		*Alias
		SchemaType string `json:"schemaType"`
		Type       string `json:"type"`
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if s.Type == "" {
		if aux.SchemaType != "" {
			s.Type = aux.SchemaType
		} else if aux.Type != "" {
			s.Type = aux.Type
		} else {
			s.Type = TypeAvro
		}
	}
	return nil
}

// RegistryState represents the full serializable state of the schema registry (Raft-compatible).
type RegistryState struct {
	SchemasByID     map[int64]*Schema             `json:"schemas_by_id"`
	SubjectVersions map[string][]*Schema          `json:"subject_versions"`
	SchemaHashToID  map[string]int64              `json:"schema_hash_to_id"`
	GlobalIDCounter int64                         `json:"global_id_counter"`
	GlobalCompat    CompatibilityLevel            `json:"global_compat"`
	SubjectCompat   map[string]CompatibilityLevel `json:"subject_compat"`
}

// Registry is a thread-safe, Raft-compatible schema registry.
type Registry struct {
	mu    sync.RWMutex
	state RegistryState
}

// NewRegistry constructs a new in-memory Schema Registry.
func NewRegistry() *Registry {
	return &Registry{
		state: RegistryState{
			SchemasByID:     make(map[int64]*Schema),
			SubjectVersions: make(map[string][]*Schema),
			SchemaHashToID:  make(map[string]int64),
			GlobalIDCounter: 0,
			GlobalCompat:    BACKWARD,
			SubjectCompat:   make(map[string]CompatibilityLevel),
		},
	}
}

// RegisterSchema validates schema format, dedupes identical schemas, enforces compatibility,
// and assigns global schema ID and subject version.
func (r *Registry) RegisterSchema(subject string, schemaType string, schemaStr string) (id int64, version int, err error) {
	if strings.TrimSpace(subject) == "" {
		return 0, 0, fmt.Errorf("subject cannot be empty")
	}

	normType := strings.ToUpper(strings.TrimSpace(schemaType))
	if normType == "" {
		normType = TypeAvro
	}

	canonicalStr, err := validateSchema(normType, schemaStr)
	if err != nil {
		return 0, 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Check if identical schema is already registered under this subject (deduplication)
	subjectSchemas := r.state.SubjectVersions[subject]
	for _, s := range subjectSchemas {
		if s.Type == normType && s.Schema == canonicalStr {
			return s.ID, s.Version, nil
		}
	}

	// 2. Compatibility check against latest version
	compatLevel := r.getCompatibilityLocked(subject)
	if len(subjectSchemas) > 0 {
		latest := subjectSchemas[len(subjectSchemas)-1]
		newCandidate := &Schema{
			Subject: subject,
			Type:    normType,
			Schema:  canonicalStr,
		}
		ok, err := checkCompatibility(compatLevel, latest, newCandidate)
		if err != nil {
			return 0, 0, fmt.Errorf("compatibility check error: %w", err)
		}
		if !ok {
			return 0, 0, fmt.Errorf("%w: schema is not %s compatible with version %d", ErrIncompatibleSchema, compatLevel, latest.Version)
		}
	}

	// 3. Assign global schema ID (reuse if canonical schema exists globally, else increment counter)
	hashKey := normType + ":" + canonicalStr
	schemaID, exists := r.state.SchemaHashToID[hashKey]
	if !exists {
		r.state.GlobalIDCounter++
		schemaID = r.state.GlobalIDCounter
		r.state.SchemaHashToID[hashKey] = schemaID
	}

	// 4. Assign subject version
	version = len(subjectSchemas) + 1

	newSchema := &Schema{
		ID:      schemaID,
		Subject: subject,
		Version: version,
		Type:    normType,
		Schema:  canonicalStr,
	}

	r.state.SubjectVersions[subject] = append(subjectSchemas, newSchema)
	r.state.SchemasByID[schemaID] = newSchema

	return schemaID, version, nil
}

// GetSchemaByID retrieves a schema by its global ID.
func (r *Registry) GetSchemaByID(id int64) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, exists := r.state.SchemasByID[id]
	if !exists {
		return nil, fmt.Errorf("%w: ID %d", ErrSchemaNotFound, id)
	}
	sc := *s
	return &sc, nil
}

// GetLatestSchema retrieves the latest version schema for a subject.
func (r *Registry) GetLatestSchema(subject string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas, exists := r.state.SubjectVersions[subject]
	if !exists || len(schemas) == 0 {
		return nil, fmt.Errorf("%w: subject %q", ErrSubjectNotFound, subject)
	}
	sc := *schemas[len(schemas)-1]
	return &sc, nil
}

// GetSchemaByVersion retrieves a schema for a specific subject and version number.
func (r *Registry) GetSchemaByVersion(subject string, version int) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas, exists := r.state.SubjectVersions[subject]
	if !exists || len(schemas) == 0 {
		return nil, fmt.Errorf("%w: subject %q", ErrSubjectNotFound, subject)
	}
	for _, s := range schemas {
		if s.Version == version {
			sc := *s
			return &sc, nil
		}
	}
	return nil, fmt.Errorf("%w: version %d for subject %q", ErrVersionNotFound, version, subject)
}

// ListSubjects returns all registered subject names in lexicographical order.
func (r *Registry) ListSubjects() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subjects := make([]string, 0, len(r.state.SubjectVersions))
	for s, versions := range r.state.SubjectVersions {
		if len(versions) > 0 {
			subjects = append(subjects, s)
		}
	}
	sort.Strings(subjects)
	return subjects
}

// ListVersions returns all registered versions for a subject.
func (r *Registry) ListVersions(subject string) []int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas, exists := r.state.SubjectVersions[subject]
	if !exists || len(schemas) == 0 {
		return []int{}
	}
	versions := make([]int, len(schemas))
	for i, s := range schemas {
		versions[i] = s.Version
	}
	return versions
}

// HasSubject checks if a subject exists and has at least one version.
func (r *Registry) HasSubject(subject string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas, exists := r.state.SubjectVersions[subject]
	return exists && len(schemas) > 0
}

// SetCompatibility sets compatibility level for a subject or globally if subject is empty.
func (r *Registry) SetCompatibility(subject string, level CompatibilityLevel) error {
	normLevel := CompatibilityLevel(strings.ToUpper(strings.TrimSpace(string(level))))
	switch normLevel {
	case BACKWARD, FORWARD, FULL, NONE:
	default:
		return fmt.Errorf("%w: %s", ErrInvalidCompatLevel, level)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if subject == "" {
		r.state.GlobalCompat = normLevel
	} else {
		r.state.SubjectCompat[subject] = normLevel
	}
	return nil
}

// GetCompatibility returns the effective compatibility level for a subject (or global).
func (r *Registry) GetCompatibility(subject string) CompatibilityLevel {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getCompatibilityLocked(subject)
}

func (r *Registry) getCompatibilityLocked(subject string) CompatibilityLevel {
	if subject != "" {
		if lvl, ok := r.state.SubjectCompat[subject]; ok {
			return lvl
		}
	}
	if r.state.GlobalCompat != "" {
		return r.state.GlobalCompat
	}
	return BACKWARD
}

// TestCompatibility tests if schemaStr is compatible with the latest schema version for subject.
func (r *Registry) TestCompatibility(subject string, schemaStr string) (bool, error) {
	return r.TestCompatibilityWithVersion(subject, 0, "", schemaStr)
}

// TestCompatibilityWithVersion tests if schemaStr is compatible with a given version (or latest if version <= 0).
func (r *Registry) TestCompatibilityWithVersion(subject string, version int, schemaType string, schemaStr string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	schemas, exists := r.state.SubjectVersions[subject]
	if !exists || len(schemas) == 0 {
		normType := strings.ToUpper(strings.TrimSpace(schemaType))
		if normType == "" {
			normType = TypeAvro
		}
		_, err := validateSchema(normType, schemaStr)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	var targetSchema *Schema
	if version <= 0 {
		targetSchema = schemas[len(schemas)-1]
	} else {
		for _, s := range schemas {
			if s.Version == version {
				targetSchema = s
				break
			}
		}
		if targetSchema == nil {
			return false, fmt.Errorf("%w: version %d for subject %q", ErrVersionNotFound, version, subject)
		}
	}

	normType := strings.ToUpper(strings.TrimSpace(schemaType))
	if normType == "" {
		normType = targetSchema.Type
	}

	canonicalStr, err := validateSchema(normType, schemaStr)
	if err != nil {
		return false, err
	}

	compatLevel := r.getCompatibilityLocked(subject)
	newCandidate := &Schema{
		Subject: subject,
		Type:    normType,
		Schema:  canonicalStr,
	}

	return checkCompatibility(compatLevel, targetSchema, newCandidate)
}

// ExportState dumps the registry state as JSON bytes (for Raft snapshots).
func (r *Registry) ExportState() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return json.Marshal(r.state)
}

// ImportState restores the registry state from JSON bytes (for Raft restore).
func (r *Registry) ImportState(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var state RegistryState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.SchemasByID == nil {
		state.SchemasByID = make(map[int64]*Schema)
	}
	if state.SubjectVersions == nil {
		state.SubjectVersions = make(map[string][]*Schema)
	}
	if state.SchemaHashToID == nil {
		state.SchemaHashToID = make(map[string]int64)
	}
	if state.SubjectCompat == nil {
		state.SubjectCompat = make(map[string]CompatibilityLevel)
	}
	if state.GlobalCompat == "" {
		state.GlobalCompat = BACKWARD
	}
	r.state = state
	return nil
}

// validateSchema checks schema format syntax and returns a canonical representation.
func validateSchema(schemaType, schemaStr string) (string, error) {
	if strings.TrimSpace(schemaStr) == "" {
		return "", fmt.Errorf("%w: schema string cannot be empty", ErrInvalidSchema)
	}

	normType := strings.ToUpper(strings.TrimSpace(schemaType))
	if normType == "" {
		normType = TypeAvro
	}

	switch normType {
	case TypeAvro, TypeJSON:
		var v interface{}
		if err := json.Unmarshal([]byte(schemaStr), &v); err != nil {
			return "", fmt.Errorf("%w: invalid JSON syntax: %v", ErrInvalidSchema, err)
		}
		if normType == TypeAvro {
			switch val := v.(type) {
			case map[string]interface{}:
				if _, hasType := val["type"]; !hasType {
					return "", fmt.Errorf("%w: avro schema object missing 'type' field", ErrInvalidSchema)
				}
			case string, []interface{}:
				// valid primitive or union
			default:
				return "", fmt.Errorf("%w: invalid avro schema root element", ErrInvalidSchema)
			}
		}
		canonicalBytes, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("%w: failed to canonicalize JSON: %v", ErrInvalidSchema, err)
		}
		return string(canonicalBytes), nil

	case TypeProtobuf:
		trimmed := strings.TrimSpace(schemaStr)
		braceCount := 0
		for _, ch := range trimmed {
			if ch == '{' {
				braceCount++
			} else if ch == '}' {
				braceCount--
				if braceCount < 0 {
					return "", fmt.Errorf("%w: protobuf unbalanced closing brace", ErrInvalidSchema)
				}
			}
		}
		if braceCount != 0 {
			return "", fmt.Errorf("%w: protobuf unbalanced braces", ErrInvalidSchema)
		}
		hasProtoKeyword := strings.Contains(trimmed, "syntax") ||
			strings.Contains(trimmed, "message") ||
			strings.Contains(trimmed, "enum") ||
			strings.Contains(trimmed, "service")
		if !hasProtoKeyword {
			return "", fmt.Errorf("%w: protobuf schema must contain proto definition (message, enum, etc.)", ErrInvalidSchema)
		}
		canonical := strings.ReplaceAll(trimmed, "\r\n", "\n")
		return canonical, nil

	default:
		return "", fmt.Errorf("%w: unsupported schema type %q", ErrInvalidSchema, schemaType)
	}
}
