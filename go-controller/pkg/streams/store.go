package streams

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync"
)

// KV is a key/value pair returned from scans.
type KV struct {
	Key   string
	Value []byte
}

// Store is the embedded key-value state store used by stream jobs.
type Store interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Delete(key string)
	// Scan returns entries whose key has the prefix, sorted by key.
	Scan(prefix string) []KV
	Len() int
	Close() error
}

type changelogEntry struct {
	Op string `json:"op"`
	K  string `json:"k"`
	V  string `json:"v,omitempty"`
}

// MemStore is an in-memory store optionally backed by an append-only changelog
// file. On open the changelog is replayed (state restore); when it holds many
// superseded entries it is compacted to one put per live key.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	path string
	f    *os.File
	w    *bufio.Writer
	// Changelog, if set, is invoked for every mutation (e.g. to publish to a
	// changelog topic). op is "put" or "del".
	Changelog func(op, key string, value []byte)
	lines     int
}

// NewMemStore creates a purely in-memory store.
func NewMemStore() *MemStore { return &MemStore{data: map[string][]byte{}} }

// OpenStore opens (or creates) a changelog-backed store at path and restores state.
func OpenStore(path string) (*MemStore, error) {
	s := NewMemStore()
	s.path = path
	if err := s.restore(); err != nil {
		return nil, err
	}
	if s.lines > 2*len(s.data)+1000 {
		if err := s.compact(); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	s.w = bufio.NewWriter(f)
	return s, nil
}

func (s *MemStore) restore() error {
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var e changelogEntry
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			continue // torn tail write
		}
		s.lines++
		switch e.Op {
		case "put":
			v, err := base64.StdEncoding.DecodeString(e.V)
			if err == nil {
				s.data[e.K] = v
			}
		case "del":
			delete(s.data, e.K)
		}
	}
	return sc.Err()
}

func (s *MemStore) compact() error {
	tmp := s.path + ".compact"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for k, v := range s.data {
		b, _ := json.Marshal(changelogEntry{Op: "put", K: k, V: base64.StdEncoding.EncodeToString(v)})
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	s.lines = len(s.data)
	return os.Rename(tmp, s.path)
}

func (s *MemStore) log(op, key string, value []byte) {
	if s.Changelog != nil {
		s.Changelog(op, key, value)
	}
	if s.w == nil {
		return
	}
	e := changelogEntry{Op: op, K: key}
	if op == "put" {
		e.V = base64.StdEncoding.EncodeToString(value)
	}
	b, _ := json.Marshal(e)
	s.w.Write(b)
	s.w.WriteByte('\n')
	s.lines++
}

func (s *MemStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *MemStore) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), value...)
	s.data[key] = cp
	s.log("put", key, cp)
}

func (s *MemStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return
	}
	delete(s.data, key)
	s.log("del", key, nil)
}

func (s *MemStore) Scan(prefix string) []KV {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []KV
	for k, v := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, KV{k, v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Flush forces buffered changelog writes to disk.
func (s *MemStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *MemStore) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}
