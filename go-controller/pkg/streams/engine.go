package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Engine manages stateful stream jobs, their state stores and topic routing.
type Engine struct {
	mu   sync.RWMutex
	jobs map[string]*Job
	dir  string // "" = memory only
	sink Sink
}

// NewEngine creates an engine. stateDir, when non-empty, persists job
// definitions and per-job state changelogs there and restores them on start.
func NewEngine(stateDir string) (*Engine, error) {
	e := &Engine{jobs: map[string]*Job{}, dir: stateDir}
	if stateDir == "" {
		return e, nil
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(stateDir, "jobs.json"))
	if err == nil {
		var cfgs []JobConfig
		if json.Unmarshal(b, &cfgs) == nil {
			for _, c := range cfgs {
				if err := e.register(c, false); err != nil {
					return nil, fmt.Errorf("restore job %s: %w", c.Name, err)
				}
			}
		}
	}
	return e, nil
}

// SetSink sets the output sink (produces results into a job's target topic).
func (e *Engine) SetSink(s Sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sink = s
	for _, j := range e.jobs {
		j.mu.Lock()
		j.sink = s
		j.mu.Unlock()
	}
}

func (e *Engine) persistLocked() {
	if e.dir == "" {
		return
	}
	cfgs := make([]JobConfig, 0, len(e.jobs))
	for _, j := range e.jobs {
		cfgs = append(cfgs, j.Config)
	}
	sort.Slice(cfgs, func(i, k int) bool { return cfgs[i].Name < cfgs[k].Name })
	b, _ := json.MarshalIndent(cfgs, "", "  ")
	tmp := filepath.Join(e.dir, "jobs.json.tmp")
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, filepath.Join(e.dir, "jobs.json"))
	}
}

func (e *Engine) register(cfg JobConfig, persist bool) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.jobs[cfg.Name]; ok {
		return fmt.Errorf("%w: %q", ErrJobExists, cfg.Name)
	}
	var st Store
	if e.dir != "" {
		s, err := OpenStore(filepath.Join(e.dir, cfg.Name+".changelog"))
		if err != nil {
			return err
		}
		st = s
	} else {
		st = NewMemStore()
	}
	e.jobs[cfg.Name] = newJob(cfg, st, e.sink)
	if persist {
		e.persistLocked()
	}
	return nil
}

// Register validates and starts a new job.
func (e *Engine) Register(cfg JobConfig) error { return e.register(cfg, true) }

func (e *Engine) Get(name string) (*Job, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	j, ok := e.jobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrJobNotFound, name)
	}
	return j, nil
}

func (e *Engine) List() []*Job {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Job, 0, len(e.jobs))
	for _, j := range e.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Config.Name < out[k].Config.Name })
	return out
}

// Delete stops a job and removes its state.
func (e *Engine) Delete(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrJobNotFound, name)
	}
	j.mu.Lock()
	j.store.Close()
	j.mu.Unlock()
	delete(e.jobs, name)
	if e.dir != "" {
		os.Remove(filepath.Join(e.dir, name+".changelog"))
	}
	e.persistLocked()
	return nil
}

func (e *Engine) SetPaused(name string, paused bool) error {
	j, err := e.Get(name)
	if err != nil {
		return err
	}
	j.mu.Lock()
	j.Paused = paused
	j.mu.Unlock()
	return nil
}

// Ingest routes a record to every job subscribed to its topic.
func (e *Engine) Ingest(rec Record) int {
	e.mu.RLock()
	var targets []*Job
	for _, j := range e.jobs {
		if rec.Topic == j.Config.SourceTopic || (j.Config.Type == JobJoin && rec.Topic == j.Config.TableTopic) {
			targets = append(targets, j)
		}
	}
	e.mu.RUnlock()
	for _, j := range targets {
		j.Process(rec)
	}
	return len(targets)
}

// Topics returns the distinct topics any job consumes.
func (e *Engine) Topics() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	set := map[string]bool{}
	for _, j := range e.jobs {
		set[j.Config.SourceTopic] = true
		if j.Config.Type == JobJoin {
			set[j.Config.TableTopic] = true
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Flush syncs all changelogs to disk.
func (e *Engine) Flush() {
	for _, j := range e.List() {
		if m, ok := j.store.(*MemStore); ok {
			m.Flush()
		}
	}
}

// Close flushes and closes all stores.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, j := range e.jobs {
		j.store.Close()
	}
}

// ---------- topic polling ----------

// FetchFunc reads the payload stored at (topic, partition, offset).
// ok=false means no record is available yet (end of log).
type FetchFunc func(topic string, partition uint32, offset uint64) (payload []byte, ok bool, err error)

// PartitionsFunc lists partitions of a topic.
type PartitionsFunc func(topic string) []uint32

// Poll runs one polling pass for job-subscribed topics: each job resumes from
// the offset persisted in its own state store (consistent with the state).
// Returns the number of records processed.
func (e *Engine) Poll(fetch FetchFunc, parts PartitionsFunc, maxPerPartition int) int {
	n := 0
	for _, j := range e.List() {
		j.mu.Lock()
		paused := j.Paused
		j.mu.Unlock()
		if paused {
			continue
		}
		topics := []string{j.Config.SourceTopic}
		if j.Config.Type == JobJoin {
			topics = append([]string{j.Config.TableTopic}, topics...) // table first
		}
		for _, t := range topics {
			for _, p := range parts(t) {
				for i := 0; i < maxPerPartition; i++ {
					off := j.NextOffset(t, p)
					payload, ok, err := fetch(t, p, off)
					if err != nil || !ok {
						break
					}
					j.Process(Record{Topic: t, Partition: int32(p), Offset: off, Value: payload, Timestamp: time.Now()})
					n++
				}
			}
		}
	}
	return n
}

// Run polls until ctx is cancelled.
func (e *Engine) Run(ctx context.Context, fetch FetchFunc, parts PartitionsFunc, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			e.Flush()
			return
		case <-t.C:
			e.Poll(fetch, parts, 500)
			e.Flush()
		}
	}
}
