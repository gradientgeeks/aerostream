// Package dataplane holds cluster-wide data-plane settings that the controller pushes to
// brokers on every heartbeat: client quotas (KIP-13 style) and per-topic compression.type.
package dataplane

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// DefaultEntity is the Kafka "<default>" entity name.
const DefaultEntity = "<default>"

// Quota is one quota entry. User/ClientID empty means the dimension is not part of the entity.
// Nil rates mean "unset" (no limit for that metric).
type Quota struct {
	User              string   `json:"user,omitempty"`
	ClientID          string   `json:"client_id,omitempty"`
	ProducerByteRate  *float64 `json:"producer_byte_rate,omitempty"`
	ConsumerByteRate  *float64 `json:"consumer_byte_rate,omitempty"`
	RequestPercentage *float64 `json:"request_percentage,omitempty"`
}

func (q *Quota) key() string { return q.User + "\x00" + q.ClientID }

// Validate checks the entity and rates.
func (q *Quota) Validate() error {
	if q.User == "" && q.ClientID == "" {
		return fmt.Errorf("quota entity requires user and/or client_id (use %q for defaults)", DefaultEntity)
	}
	if q.ProducerByteRate == nil && q.ConsumerByteRate == nil && q.RequestPercentage == nil {
		return fmt.Errorf("at least one of producer_byte_rate, consumer_byte_rate, request_percentage is required")
	}
	for name, v := range map[string]*float64{
		"producer_byte_rate": q.ProducerByteRate, "consumer_byte_rate": q.ConsumerByteRate, "request_percentage": q.RequestPercentage,
	} {
		if v != nil && *v < 0 {
			return fmt.Errorf("%s must be >= 0", name)
		}
	}
	return nil
}

var validCompression = map[string]bool{
	"producer": true, "uncompressed": true, "gzip": true, "snappy": true, "lz4": true, "zstd": true,
}

// ValidCompressionType reports whether s is a valid Kafka compression.type value.
func ValidCompressionType(s string) bool { return validCompression[strings.ToLower(s)] }

type persisted struct {
	Quotas           []*Quota          `json:"quotas"`
	TopicCompression map[string]string `json:"topic_compression"`
}

// Store is a thread-safe, optionally file-backed settings store.
type Store struct {
	mu          sync.RWMutex
	quotas      map[string]*Quota
	compression map[string]string
	path        string
}

// NewStore creates a store. If path is non-empty, state is loaded from and saved to it.
func NewStore(path string) *Store {
	s := &Store{quotas: map[string]*Quota{}, compression: map[string]string{}, path: path}
	if path != "" {
		if b, err := os.ReadFile(path); err == nil {
			var p persisted
			if json.Unmarshal(b, &p) == nil {
				for _, q := range p.Quotas {
					s.quotas[q.key()] = q
				}
				for k, v := range p.TopicCompression {
					s.compression[k] = v
				}
			}
		}
	}
	return s
}

var (
	defaultOnce  sync.Once
	defaultStore *Store
)

// Default returns the process-wide store (file from AEROSTREAM_DATAPLANE_FILE, if set).
func Default() *Store {
	defaultOnce.Do(func() { defaultStore = NewStore(os.Getenv("AEROSTREAM_DATAPLANE_FILE")) })
	return defaultStore
}

func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	p := persisted{TopicCompression: s.compression}
	for _, q := range s.quotas {
		p.Quotas = append(p.Quotas, q)
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o644)
}

// UpsertQuota validates and stores a quota entry.
func (s *Store) UpsertQuota(q *Quota) error {
	if err := q.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quotas[q.key()] = q
	return s.saveLocked()
}

// DeleteQuota removes an entry; returns false when it did not exist.
func (s *Store) DeleteQuota(user, clientID string) (bool, error) {
	k := (&Quota{User: user, ClientID: clientID}).key()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.quotas[k]; !ok {
		return false, nil
	}
	delete(s.quotas, k)
	return true, s.saveLocked()
}

// ListQuotas returns entries sorted by (user, client_id).
func (s *Store) ListQuotas() []*Quota {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Quota, 0, len(s.quotas))
	for _, q := range s.quotas {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// SetTopicCompression sets compression.type for a topic.
func (s *Store) SetTopicCompression(topic, ctype string) error {
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("topic is required")
	}
	if !ValidCompressionType(ctype) {
		return fmt.Errorf("invalid compression_type %q (want producer|uncompressed|gzip|snappy|lz4|zstd)", ctype)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compression[topic] = strings.ToLower(ctype)
	return s.saveLocked()
}

// DeleteTopicCompression removes a per-topic override.
func (s *Store) DeleteTopicCompression(topic string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.compression[topic]; !ok {
		return false, nil
	}
	delete(s.compression, topic)
	return true, s.saveLocked()
}

// TopicCompression returns a copy of all per-topic overrides.
func (s *Store) TopicCompression() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.compression))
	for k, v := range s.compression {
		out[k] = v
	}
	return out
}
