package streams

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func rec(topic, key, val string, ts int64) Record {
	return Record{Topic: topic, Partition: -1, Key: key, Value: []byte(val), Timestamp: time.UnixMilli(ts)}
}

func newAgg(t *testing.T, w WindowSpec, agg string) (*Engine, *Job) {
	t.Helper()
	e, _ := NewEngine("")
	if err := e.Register(JobConfig{Name: "j", Type: JobAggregate, SourceTopic: "in", KeyField: "user", ValueField: "amt", Agg: agg, Window: w}); err != nil {
		t.Fatal(err)
	}
	j, _ := e.Get("j")
	return e, j
}

func TestTumbling(t *testing.T) {
	e, j := newAgg(t, WindowSpec{Type: WindowTumbling, SizeMs: 1000}, "sum")
	e.Ingest(rec("in", "", `{"user":"a","amt":5}`, 100))
	e.Ingest(rec("in", "", `{"user":"a","amt":7}`, 900))
	e.Ingest(rec("in", "", `{"user":"a","amt":1}`, 1000))
	e.Ingest(rec("in", "", `{"user":"b","amt":3}`, 1050))
	w := j.Windows("a", 0, 0)
	if len(w) != 2 || w[0].Sum != 12 || w[0].Count != 2 || w[0].WindowStart != 0 || w[0].WindowEnd != 1000 || w[1].Sum != 1 {
		t.Fatalf("unexpected %+v", w)
	}
	if len(j.Keys()) != 2 {
		t.Fatalf("keys %v", j.Keys())
	}
}

func TestHopping(t *testing.T) {
	_, j := newAgg(t, WindowSpec{Type: WindowHopping, SizeMs: 1000, AdvanceMs: 500}, "count")
	j.Process(rec("in", "", `{"user":"a","amt":1}`, 700))
	w := j.Windows("a", 0, 0)
	// windows [0,1000) and [500,1500)
	if len(w) != 2 || w[0].WindowStart != 0 || w[1].WindowStart != 500 || w[0].Count != 1 {
		t.Fatalf("unexpected %+v", w)
	}
}

func TestLateAndGrace(t *testing.T) {
	e, _ := NewEngine("")
	e.Register(JobConfig{Name: "j", Type: JobAggregate, SourceTopic: "in", KeyField: "user", Window: WindowSpec{Type: WindowTumbling, SizeMs: 1000}, GraceMs: 500})
	j, _ := e.Get("j")
	j.Process(rec("in", "", `{"user":"a"}`, 100))
	j.Process(rec("in", "", `{"user":"a"}`, 1400)) // window0 end 1000+500 > 1400 -> still open
	j.Process(rec("in", "", `{"user":"a"}`, 200))  // accepted
	j.Process(rec("in", "", `{"user":"a"}`, 2600)) // closes window0
	j.Process(rec("in", "", `{"user":"a"}`, 300))  // late
	if m := j.Metrics(); m.Late != 1 {
		t.Fatalf("late=%d", m.Late)
	}
	w := j.Windows("a", 0, 999)
	if len(w) != 1 || w[0].Count != 2 {
		t.Fatalf("unexpected %+v", w)
	}
}

func TestSession(t *testing.T) {
	_, j := newAgg(t, WindowSpec{Type: WindowSession, GapMs: 100}, "avg")
	for _, ts := range []int64{0, 50, 400} {
		j.Process(rec("in", "", `{"user":"a","amt":10}`, ts))
	}
	j.Process(rec("in", "", `{"user":"a","amt":30}`, 500)) // joins the 400 session
	j.Process(rec("in", "", `{"user":"a","amt":20}`, 220)) // own session
	w := j.Windows("a", 0, 0)
	if len(w) != 3 {
		t.Fatalf("want 3 sessions got %+v", w)
	}
	if w[0].WindowStart != 0 || w[0].WindowEnd != 50 || w[2].Count != 2 || w[2].Avg != 20 {
		t.Fatalf("unexpected %+v", w)
	}
	j.Process(rec("in", "", `{"user":"a","amt":0}`, 130)) // bridges sessions at 50 and 220
	w = j.Windows("a", 0, 0)
	if len(w) != 2 || w[0].WindowEnd != 220 || w[0].Count != 4 {
		t.Fatalf("merge failed %+v", w)
	}
}

func TestSliding(t *testing.T) {
	_, j := newAgg(t, WindowSpec{Type: WindowSliding, SizeMs: 1000}, "count")
	j.Process(rec("in", "", `{"user":"a"}`, 100))
	j.Process(rec("in", "", `{"user":"a"}`, 600))
	j.Process(rec("in", "", `{"user":"a"}`, 1300))
	w := j.Windows("a", 0, 0)
	last := w[len(w)-1]
	if last.Count != 2 { // events in (300,1300]: 600,1300
		t.Fatalf("last %+v", last)
	}
}

func TestJoin(t *testing.T) {
	e, _ := NewEngine("")
	var out []string
	e.SetSink(func(topic, key string, v []byte) { out = append(out, topic+":"+key+":"+string(v)) })
	if err := e.Register(JobConfig{Name: "enrich", Type: JobJoin, SourceTopic: "orders", TableTopic: "customers", KeyField: "cid", TableKeyField: "id", TargetTopic: "enriched", JoinType: "left"}); err != nil {
		t.Fatal(err)
	}
	e.Ingest(rec("customers", "", `{"id":"c1","name":"Ann"}`, 1))
	e.Ingest(rec("orders", "", `{"cid":"c1","amt":9}`, 2))
	e.Ingest(rec("orders", "", `{"cid":"zz","amt":1}`, 3))
	j, _ := e.Get("enrich")
	if len(out) != 2 || j.Metrics().Unmatched != 1 {
		t.Fatalf("out=%v m=%+v", out, j.Metrics())
	}
	row, ok := j.TableRow("c1")
	if !ok || string(row) != `{"id":"c1","name":"Ann"}` {
		t.Fatalf("row %s", row)
	}
	// tombstone (record key, empty value) removes the row
	e.Ingest(rec("customers", "c1", ``, 4))
	if _, ok := j.TableRow("c1"); ok {
		t.Fatal("tombstone should delete")
	}
	// inner join drops unmatched
	e.Register(JobConfig{Name: "inner", Type: JobJoin, SourceTopic: "orders", TableTopic: "customers", KeyField: "cid", TableKeyField: "id"})
	ij, _ := e.Get("inner")
	ij.Process(rec("orders", "", `{"cid":"nobody"}`, 5))
	if len(ij.Recent(0)) != 0 {
		t.Fatal("inner should drop")
	}
}

func TestValidation(t *testing.T) {
	e, _ := NewEngine("")
	bad := []JobConfig{
		{Name: "a", Type: JobAggregate, SourceTopic: "t", Window: WindowSpec{Type: WindowTumbling}},
		{Name: "a", Type: JobAggregate, SourceTopic: "t", Agg: "sum", Window: WindowSpec{Type: WindowTumbling, SizeMs: 1}},
		{Name: "a", Type: JobAggregate, SourceTopic: "t", Window: WindowSpec{Type: WindowHopping, SizeMs: 10, AdvanceMs: 20}},
		{Name: "a", Type: JobJoin, SourceTopic: "t"},
	}
	for i, c := range bad {
		if err := e.Register(c); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}

func TestStateRestoreAndOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := JobConfig{Name: "j", Type: JobAggregate, SourceTopic: "in", KeyField: "user", Window: WindowSpec{Type: WindowTumbling, SizeMs: 1000}}
	e, err := NewEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Register(cfg); err != nil {
		t.Fatal(err)
	}
	log := [][]byte{[]byte(`{"user":"a"}`), []byte(`{"user":"a"}`), []byte(`{"user":"b"}`)}
	fetch := func(topic string, p uint32, off uint64) ([]byte, bool, error) {
		if off >= uint64(len(log)) {
			return nil, false, nil
		}
		return log[off], true, nil
	}
	parts := func(string) []uint32 { return []uint32{0} }
	if n := e.Poll(fetch, parts, 100); n != 3 {
		t.Fatalf("polled %d", n)
	}
	e.Close()

	// restart: state and offsets restored; nothing reprocessed
	e2, err := NewEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if n := e2.Poll(fetch, parts, 100); n != 0 {
		t.Fatalf("reprocessed %d", n)
	}
	j, _ := e2.Get("j")
	var total int64
	for _, k := range j.Keys() {
		for _, w := range j.Windows(k, 0, 0) {
			total += w.Count
		}
	}
	if total != 3 {
		t.Fatalf("restored total %d", total)
	}
	log = append(log, []byte(`{"user":"a"}`))
	if n := e2.Poll(fetch, parts, 100); n != 1 {
		t.Fatalf("new polled %d", n)
	}
}

func TestStoreCompaction(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.log")
	s, _ := OpenStore(p)
	for i := 0; i < 3000; i++ {
		s.Put("k", []byte(fmt.Sprint(i)))
	}
	s.Put("gone", []byte("x"))
	s.Delete("gone")
	s.Close()
	s2, _ := OpenStore(p)
	defer s2.Close()
	if v, _ := s2.Get("k"); string(v) != "2999" {
		t.Fatalf("got %s", v)
	}
	if _, ok := s2.Get("gone"); ok {
		t.Fatal("deleted key present")
	}
	if s2.lines != 1 {
		t.Fatalf("expected compaction, lines=%d", s2.lines)
	}
}
