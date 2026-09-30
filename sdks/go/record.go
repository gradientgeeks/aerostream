package aerostream

import "time"

// Record represents a consumed message from an AeroStream partition.
type Record struct {
	Topic     string
	Partition uint32
	Offset    int64
	Payload   []byte
	Timestamp time.Time
}

// NewRecord creates a Record with the current UTC timestamp.
func NewRecord(topic string, partition uint32, offset int64, payload []byte) Record {
	return Record{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}
}
