package aerostream_test

import (
	"errors"
	"testing"
	"time"

	"github.com/gradientgeeks/aerostream-sdk/go"
)

func TestRecordCreation(t *testing.T) {
	before := time.Now().UTC()
	rec := aerostream.NewRecord("orders", 1, 42, []byte("order-payload"))
	after := time.Now().UTC()

	if rec.Topic != "orders" {
		t.Fatalf("expected topic 'orders', got '%s'", rec.Topic)
	}
	if rec.Partition != 1 {
		t.Fatalf("expected partition 1, got %d", rec.Partition)
	}
	if rec.Offset != 42 {
		t.Fatalf("expected offset 42, got %d", rec.Offset)
	}
	if string(rec.Payload) != "order-payload" {
		t.Fatalf("payload mismatch")
	}
	if rec.Timestamp.Before(before) || rec.Timestamp.After(after) {
		t.Fatalf("timestamp out of expected range")
	}
}

func TestSentinelErrors(t *testing.T) {
	errs := []error{
		aerostream.ErrAuthFailed,
		aerostream.ErrOutOfOrderSequence,
		aerostream.ErrClosed,
		aerostream.ErrTimeout,
		aerostream.ErrInvalidMagic,
		aerostream.ErrEmpty,
		aerostream.ErrTopicTooLong,
		aerostream.ErrPayloadTooLarge,
	}

	for _, err := range errs {
		if err == nil || len(err.Error()) == 0 {
			t.Fatal("expected non-empty error")
		}
		if !errors.Is(err, err) {
			t.Fatal("error should equal itself")
		}
	}
}
