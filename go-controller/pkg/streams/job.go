package streams

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JobType string

const (
	JobAggregate JobType = "AGGREGATE"
	JobJoin      JobType = "JOIN"
)

type WindowType string

const (
	WindowTumbling WindowType = "tumbling"
	WindowHopping  WindowType = "hopping"
	WindowSliding  WindowType = "sliding"
	WindowSession  WindowType = "session"
)

var (
	ErrJobNotFound = errors.New("stream job not found")
	ErrJobExists   = errors.New("stream job already exists")
	ErrInvalidJob  = errors.New("invalid stream job configuration")
)

// WindowSpec configures the windowing of an aggregation.
type WindowSpec struct {
	Type      WindowType `json:"type"`
	SizeMs    int64      `json:"size_ms,omitempty"`
	AdvanceMs int64      `json:"advance_ms,omitempty"` // hopping
	GapMs     int64      `json:"gap_ms,omitempty"`     // session
}

// JobConfig is the user-facing definition of a stateful stream job.
type JobConfig struct {
	Name           string     `json:"name"`
	Type           JobType    `json:"type"`
	SourceTopic    string     `json:"source_topic"`
	TargetTopic    string     `json:"target_topic,omitempty"`
	TableTopic     string     `json:"table_topic,omitempty"` // JOIN: changelog/compacted topic materialized as a table
	KeyField       string     `json:"key_field,omitempty"`   // dotted JSON path; default: record key
	TableKeyField  string     `json:"table_key_field,omitempty"`
	ValueField     string     `json:"value_field,omitempty"`     // numeric field for AGGREGATE
	TimestampField string     `json:"timestamp_field,omitempty"` // event time (epoch ms/s or RFC3339); default: record time
	Agg            string     `json:"agg,omitempty"`             // count|sum|min|max|avg
	Window         WindowSpec `json:"window,omitempty"`
	GraceMs        int64      `json:"grace_ms,omitempty"`
	RetentionMs    int64      `json:"retention_ms,omitempty"` // how long closed windows stay queryable (0 = 1h)
	JoinType       string     `json:"join_type,omitempty"`    // inner|left
}

// Record is one input event.
type Record struct {
	Topic     string
	Partition int32 // <0 when injected via REST (no offset tracking)
	Offset    uint64
	Key       string
	Value     []byte
	Timestamp time.Time
}

// Agg is a mergeable aggregate.
type Agg struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

func (a *Agg) add(v float64) {
	if a.Count == 0 {
		a.Min, a.Max = v, v
	} else {
		a.Min = math.Min(a.Min, v)
		a.Max = math.Max(a.Max, v)
	}
	a.Count++
	a.Sum += v
}

func (a *Agg) merge(o Agg) {
	if o.Count == 0 {
		return
	}
	if a.Count == 0 {
		*a = o
		return
	}
	a.Min = math.Min(a.Min, o.Min)
	a.Max = math.Max(a.Max, o.Max)
	a.Count += o.Count
	a.Sum += o.Sum
}

func (a Agg) value(fn string) float64 {
	switch fn {
	case "count":
		return float64(a.Count)
	case "sum":
		return a.Sum
	case "min":
		return a.Min
	case "max":
		return a.Max
	case "avg":
		if a.Count == 0 {
			return 0
		}
		return a.Sum / float64(a.Count)
	}
	return 0
}

// WindowResult is an emitted / queryable window aggregate.
type WindowResult struct {
	Key         string  `json:"key"`
	WindowStart int64   `json:"window_start"` // ms, inclusive
	WindowEnd   int64   `json:"window_end"`   // ms, exclusive (session/sliding: last event ts)
	Count       int64   `json:"count"`
	Sum         float64 `json:"sum"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Avg         float64 `json:"avg"`
	Value       float64 `json:"value"`
}

// JoinResult is an emitted stream-table join row.
type JoinResult struct {
	Key    string          `json:"key"`
	Stream json.RawMessage `json:"stream"`
	Table  json.RawMessage `json:"table"`
}

// Sink receives emitted results (e.g. to produce into TargetTopic).
type Sink func(topic, key string, value []byte)

// JobMetrics are running counters.
type JobMetrics struct {
	Processed  int64 `json:"processed"`
	Late       int64 `json:"late_dropped"`
	Skipped    int64 `json:"skipped"`
	Emitted    int64 `json:"emitted"`
	Unmatched  int64 `json:"unmatched"`
	StateKeys  int   `json:"state_keys"`
	StreamTime int64 `json:"stream_time_ms"`
	TableRows  int   `json:"table_rows"`
}

// Job is a running stateful processor with its own state store.
type Job struct {
	Config JobConfig
	Paused bool

	mu      sync.Mutex
	store   Store
	sink    Sink
	metrics JobMetrics
	streamT int64
	recent  []json.RawMessage
	sincePr int
}

const recentCap = 200

// Validate normalizes and validates the config.
func (c *JobConfig) Validate() error {
	c.Name = strings.TrimSpace(c.Name)
	c.SourceTopic = strings.TrimSpace(c.SourceTopic)
	if c.Name == "" || c.SourceTopic == "" {
		return fmt.Errorf("%w: name and source_topic are required", ErrInvalidJob)
	}
	switch c.Type {
	case JobAggregate:
		c.Agg = strings.ToLower(c.Agg)
		if c.Agg == "" {
			c.Agg = "count"
		}
		switch c.Agg {
		case "count":
		case "sum", "min", "max", "avg":
			if c.ValueField == "" {
				return fmt.Errorf("%w: value_field required for agg %s", ErrInvalidJob, c.Agg)
			}
		default:
			return fmt.Errorf("%w: unknown agg %q", ErrInvalidJob, c.Agg)
		}
		w := &c.Window
		if w.Type == "" {
			w.Type = WindowTumbling
		}
		switch w.Type {
		case WindowTumbling, WindowSliding:
			if w.SizeMs <= 0 {
				return fmt.Errorf("%w: window.size_ms must be > 0", ErrInvalidJob)
			}
		case WindowHopping:
			if w.SizeMs <= 0 || w.AdvanceMs <= 0 || w.AdvanceMs > w.SizeMs {
				return fmt.Errorf("%w: hopping needs 0 < advance_ms <= size_ms", ErrInvalidJob)
			}
		case WindowSession:
			if w.GapMs <= 0 {
				return fmt.Errorf("%w: session window needs gap_ms > 0", ErrInvalidJob)
			}
		default:
			return fmt.Errorf("%w: unknown window type %q", ErrInvalidJob, w.Type)
		}
		if c.GraceMs < 0 {
			return fmt.Errorf("%w: grace_ms must be >= 0", ErrInvalidJob)
		}
	case JobJoin:
		if strings.TrimSpace(c.TableTopic) == "" {
			return fmt.Errorf("%w: table_topic required for JOIN", ErrInvalidJob)
		}
		c.JoinType = strings.ToLower(c.JoinType)
		if c.JoinType == "" {
			c.JoinType = "inner"
		}
		if c.JoinType != "inner" && c.JoinType != "left" {
			return fmt.Errorf("%w: join_type must be inner or left", ErrInvalidJob)
		}
	default:
		return fmt.Errorf("%w: type must be AGGREGATE or JOIN", ErrInvalidJob)
	}
	return nil
}

func newJob(cfg JobConfig, store Store, sink Sink) *Job {
	j := &Job{Config: cfg, store: store, sink: sink}
	if b, ok := store.Get(metaStreamTime); ok {
		j.streamT, _ = strconv.ParseInt(string(b), 10, 64)
	}
	return j
}

const (
	metaStreamTime = "m\x00streamtime"
	sepc           = "\x00"
)

// ---------- JSON helpers ----------

func parseJSON(b []byte) interface{} {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return nil
	}
	return v
}

func getPath(v interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}
	cur := v
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func toFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toKey(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func toMillis(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case float64:
		if t < 1e11 { // seconds
			return int64(t * 1000), true
		}
		return int64(t), true
	case string:
		if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return tm.UnixMilli(), true
		}
		if f, err := strconv.ParseFloat(t, 64); err == nil {
			return toMillis(f)
		}
	}
	return 0, false
}

// eventTime returns the event time in ms.
func (j *Job) eventTime(rec Record, parsed interface{}) int64 {
	if j.Config.TimestampField != "" {
		if v, ok := getPath(parsed, j.Config.TimestampField); ok {
			if ms, ok := toMillis(v); ok {
				return ms
			}
		}
	}
	if rec.Timestamp.IsZero() {
		return time.Now().UnixMilli()
	}
	return rec.Timestamp.UnixMilli()
}

func (j *Job) recordKey(rec Record, parsed interface{}, field string) string {
	if field != "" {
		if v, ok := getPath(parsed, field); ok {
			return toKey(v)
		}
		return ""
	}
	return rec.Key
}

// ---------- processing ----------

// Process handles one record. It is safe for concurrent use.
func (j *Job) Process(rec Record) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Paused {
		return
	}
	switch j.Config.Type {
	case JobAggregate:
		if rec.Topic == j.Config.SourceTopic {
			j.processAgg(rec)
		}
	case JobJoin:
		j.processJoin(rec)
	}
	if rec.Partition >= 0 {
		j.store.Put(offsetKey(rec.Topic, uint32(rec.Partition)), []byte(strconv.FormatUint(rec.Offset+1, 10)))
	}
	j.metrics.Processed++
}

func offsetKey(topic string, p uint32) string {
	return "o" + sepc + topic + sepc + strconv.FormatUint(uint64(p), 10)
}

// NextOffset is the next offset to consume for a topic partition (persisted with state).
func (j *Job) NextOffset(topic string, p uint32) uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	if b, ok := j.store.Get(offsetKey(topic, p)); ok {
		n, _ := strconv.ParseUint(string(b), 10, 64)
		return n
	}
	return 0
}

func (j *Job) emit(key string, v interface{}) {
	b, _ := json.Marshal(v)
	j.metrics.Emitted++
	j.recent = append(j.recent, b)
	if len(j.recent) > recentCap {
		j.recent = j.recent[len(j.recent)-recentCap:]
	}
	if j.sink != nil && j.Config.TargetTopic != "" {
		j.sink(j.Config.TargetTopic, key, b)
	}
}

func wkey(key string, start, end int64) string {
	return "w" + sepc + key + sepc + fmt.Sprintf("%020d", start) + sepc + fmt.Sprintf("%020d", end)
}

func (j *Job) result(key string, start, end int64, a Agg) WindowResult {
	avg := 0.0
	if a.Count > 0 {
		avg = a.Sum / float64(a.Count)
	}
	return WindowResult{Key: key, WindowStart: start, WindowEnd: end, Count: a.Count, Sum: a.Sum, Min: a.Min, Max: a.Max, Avg: avg, Value: a.value(j.Config.Agg)}
}

func (j *Job) advanceStreamTime(ts int64) {
	if ts > j.streamT {
		j.streamT = ts
		j.store.Put(metaStreamTime, []byte(strconv.FormatInt(ts, 10)))
	}
}

func (j *Job) processAgg(rec Record) {
	cfg := j.Config
	parsed := parseJSON(rec.Value)
	key := j.recordKey(rec, parsed, cfg.KeyField)
	ts := j.eventTime(rec, parsed)
	val := 1.0
	if cfg.Agg != "count" {
		f, ok := func() (float64, bool) {
			v, ok := getPath(parsed, cfg.ValueField)
			if !ok {
				return 0, false
			}
			return toFloat(v)
		}()
		if !ok {
			j.metrics.Skipped++
			return
		}
		val = f
	}
	j.advanceStreamTime(ts)
	w := cfg.Window
	switch w.Type {
	case WindowTumbling:
		start := floorDiv(ts, w.SizeMs) * w.SizeMs
		if !j.updateWindow(key, start, start+w.SizeMs, val) {
			j.metrics.Late++
		}
	case WindowHopping:
		// all windows [s, s+size) with s multiple of advance containing ts
		last := floorDiv(ts, w.AdvanceMs) * w.AdvanceMs
		any := false
		for s := last; s > ts-w.SizeMs; s -= w.AdvanceMs {
			if j.updateWindow(key, s, s+w.SizeMs, val) {
				any = true
			}
		}
		if !any {
			j.metrics.Late++
		}
	case WindowSliding:
		j.slide(key, ts, val)
	case WindowSession:
		j.session(key, ts, val)
	}
	j.sincePr++
	if j.sincePr >= 256 {
		j.sincePr = 0
		j.prune()
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// updateWindow adds val to a fixed window unless the window is closed
// (end + grace <= stream time). Returns false when dropped as late.
func (j *Job) updateWindow(key string, start, end int64, val float64) bool {
	if end+j.Config.GraceMs <= j.streamT {
		return false
	}
	k := wkey(key, start, end)
	var a Agg
	if b, ok := j.store.Get(k); ok {
		json.Unmarshal(b, &a)
	}
	a.add(val)
	b, _ := json.Marshal(a)
	j.store.Put(k, b)
	j.emit(key, j.result(key, start, end, a))
	return true
}

type slideEvent struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// slide implements sliding windows of size N ending at each event: the result
// for an event at t aggregates events in [t-size+1, t]. (Late events refresh
// only their own window; already-emitted later windows are not revised.)
func (j *Job) slide(key string, ts int64, val float64) {
	size := j.Config.Window.SizeMs
	if ts+j.Config.GraceMs < j.streamT-size {
		j.metrics.Late++
		return
	}
	k := "s" + sepc + key
	var evs []slideEvent
	if b, ok := j.store.Get(k); ok {
		json.Unmarshal(b, &evs)
	}
	evs = append(evs, slideEvent{ts, val})
	// keep sorted by time (insertion of near-sorted data)
	for i := len(evs) - 1; i > 0 && evs[i].T < evs[i-1].T; i-- {
		evs[i], evs[i-1] = evs[i-1], evs[i]
	}
	horizon := j.streamT - size - j.Config.GraceMs
	kept := evs[:0]
	for _, e := range evs {
		if e.T >= horizon {
			kept = append(kept, e)
		}
	}
	evs = kept
	b, _ := json.Marshal(evs)
	j.store.Put(k, b)
	var a Agg
	for _, e := range evs {
		if e.T > ts-size && e.T <= ts {
			a.add(e.V)
		}
	}
	r := j.result(key, ts-size+1, ts, a)
	rb, _ := json.Marshal(a)
	j.store.Put(wkey(key, ts-size+1, ts), rb)
	j.emit(key, r)
}

type sessionRec struct {
	S int64 `json:"s"`
	E int64 `json:"e"`
	A Agg   `json:"a"`
}

func (j *Job) session(key string, ts int64, val float64) {
	gap := j.Config.Window.GapMs
	k := "x" + sepc + key
	var ss []sessionRec
	if b, ok := j.store.Get(k); ok {
		json.Unmarshal(b, &ss)
	}
	cur := sessionRec{S: ts, E: ts}
	cur.A.add(val)
	var rest []sessionRec
	for _, s := range ss {
		if ts >= s.S-gap && ts <= s.E+gap {
			if s.S < cur.S {
				cur.S = s.S
			}
			if s.E > cur.E {
				cur.E = s.E
			}
			cur.A.merge(s.A)
		} else {
			rest = append(rest, s)
		}
	}
	rest = append(rest, cur)
	for i := len(rest) - 1; i > 0 && rest[i].S < rest[i-1].S; i-- {
		rest[i], rest[i-1] = rest[i-1], rest[i]
	}
	b, _ := json.Marshal(rest)
	j.store.Put(k, b)
	j.emit(key, j.result(key, cur.S, cur.E, cur.A))
}

func (j *Job) retention() int64 {
	if j.Config.RetentionMs > 0 {
		return j.Config.RetentionMs
	}
	return int64(time.Hour / time.Millisecond)
}

// prune drops windows/sessions/events that are closed and past retention.
func (j *Job) prune() {
	limit := j.streamT - j.Config.GraceMs - j.retention()
	for _, kv := range j.store.Scan("w" + sepc) {
		parts := strings.Split(kv.Key, sepc)
		if len(parts) != 4 {
			continue
		}
		end, _ := strconv.ParseInt(parts[3], 10, 64)
		if end < limit {
			j.store.Delete(kv.Key)
		}
	}
	for _, kv := range j.store.Scan("x" + sepc) {
		var ss []sessionRec
		json.Unmarshal(kv.Value, &ss)
		kept := ss[:0]
		for _, s := range ss {
			if s.E+j.Config.Window.GapMs >= limit {
				kept = append(kept, s)
			}
		}
		if len(kept) == 0 {
			j.store.Delete(kv.Key)
		} else if len(kept) != len(ss) {
			b, _ := json.Marshal(kept)
			j.store.Put(kv.Key, b)
		}
	}
}

func (j *Job) processJoin(rec Record) {
	cfg := j.Config
	if rec.Topic == cfg.TableTopic {
		parsed := parseJSON(rec.Value)
		key := j.recordKey(rec, parsed, cfg.TableKeyField)
		if key == "" {
			key = rec.Key // tombstones carry only the record key
		}
		if key == "" {
			j.metrics.Skipped++
			return
		}
		tk := "t" + sepc + key
		if len(rec.Value) == 0 || string(rec.Value) == "null" {
			j.store.Delete(tk) // tombstone
		} else {
			j.store.Put(tk, rec.Value)
		}
		return
	}
	if rec.Topic != cfg.SourceTopic {
		return
	}
	parsed := parseJSON(rec.Value)
	key := j.recordKey(rec, parsed, cfg.KeyField)
	tv, ok := j.store.Get("t" + sepc + key)
	if !ok {
		j.metrics.Unmatched++
		if cfg.JoinType != "left" {
			return
		}
		tv = []byte("null")
	}
	sv := rec.Value
	if !json.Valid(sv) {
		sv, _ = json.Marshal(string(sv))
	}
	j.emit(key, JoinResult{Key: key, Stream: sv, Table: tv})
}

// ---------- interactive queries ----------

// Windows returns window results for a key overlapping [from,to] (0 = unbounded), ordered by start.
func (j *Job) Windows(key string, from, to int64) []WindowResult {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []WindowResult
	in := func(s, e int64) bool {
		return (from == 0 || e >= from) && (to == 0 || s <= to)
	}
	if j.Config.Window.Type == WindowSession {
		if b, ok := j.store.Get("x" + sepc + key); ok {
			var ss []sessionRec
			json.Unmarshal(b, &ss)
			for _, s := range ss {
				if in(s.S, s.E) {
					out = append(out, j.result(key, s.S, s.E, s.A))
				}
			}
		}
		return out
	}
	for _, kv := range j.store.Scan("w" + sepc + key + sepc) {
		parts := strings.Split(kv.Key, sepc)
		if len(parts) != 4 {
			continue
		}
		s, _ := strconv.ParseInt(parts[2], 10, 64)
		e, _ := strconv.ParseInt(parts[3], 10, 64)
		if !in(s, e) {
			continue
		}
		var a Agg
		json.Unmarshal(kv.Value, &a)
		out = append(out, j.result(key, s, e, a))
	}
	return out
}

// Keys lists the distinct keys that have state.
func (j *Job) Keys() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	if j.Config.Type == JobJoin {
		for _, kv := range j.store.Scan("t" + sepc) {
			add(strings.TrimPrefix(kv.Key, "t"+sepc))
		}
		return out
	}
	pfx := "w"
	if j.Config.Window.Type == WindowSession {
		pfx = "x"
	}
	for _, kv := range j.store.Scan(pfx + sepc) {
		add(strings.Split(kv.Key, sepc)[1])
	}
	return out
}

// TableRow returns the materialized table value for a key (JOIN jobs).
func (j *Job) TableRow(key string) (json.RawMessage, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, ok := j.store.Get("t" + sepc + key)
	return json.RawMessage(b), ok
}

// Recent returns the last emitted results (newest last).
func (j *Job) Recent(limit int) []json.RawMessage {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.recent
	if limit > 0 && len(r) > limit {
		r = r[len(r)-limit:]
	}
	return append([]json.RawMessage(nil), r...)
}

// Metrics returns a snapshot of counters.
func (j *Job) Metrics() JobMetrics {
	j.mu.Lock()
	defer j.mu.Unlock()
	m := j.metrics
	m.StateKeys = j.store.Len()
	m.StreamTime = j.streamT
	if j.Config.Type == JobJoin {
		m.TableRows = len(j.store.Scan("t" + sepc))
	}
	return m
}
