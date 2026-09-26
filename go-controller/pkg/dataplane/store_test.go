package dataplane

import (
	"path/filepath"
	"testing"
)

func f(v float64) *float64 { return &v }

func TestQuotaCRUDAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dp.json")
	s := NewStore(path)
	if err := s.UpsertQuota(&Quota{}); err == nil {
		t.Fatal("expected error for empty entity")
	}
	if err := s.UpsertQuota(&Quota{ClientID: "c"}); err == nil {
		t.Fatal("expected error for no rates")
	}
	if err := s.UpsertQuota(&Quota{ClientID: "c", ProducerByteRate: f(-1)}); err == nil {
		t.Fatal("expected error for negative rate")
	}
	if err := s.UpsertQuota(&Quota{ClientID: DefaultEntity, ProducerByteRate: f(1000)}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertQuota(&Quota{User: "alice", ClientID: "c", ConsumerByteRate: f(5), RequestPercentage: f(50)}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTopicCompression("t", "ZSTD"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTopicCompression("t", "bogus"); err == nil {
		t.Fatal("expected invalid compression error")
	}

	s2 := NewStore(path)
	if len(s2.ListQuotas()) != 2 || s2.TopicCompression()["t"] != "zstd" {
		t.Fatalf("persistence failed: %+v %v", s2.ListQuotas(), s2.TopicCompression())
	}
	ok, _ := s2.DeleteQuota("", DefaultEntity)
	if !ok || len(s2.ListQuotas()) != 1 {
		t.Fatal("delete failed")
	}
	if ok, _ := s2.DeleteTopicCompression("t"); !ok {
		t.Fatal("delete compression failed")
	}
}
