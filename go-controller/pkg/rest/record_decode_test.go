package rest

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

func encodeZigzag(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

func putVarint(buf *bytes.Buffer, v int64) {
	uv := encodeZigzag(v)
	for uv >= 0x80 {
		buf.WriteByte(byte(uv) | 0x80)
		uv >>= 7
	}
	buf.WriteByte(byte(uv))
}

func buildRecordBatch(key, val string, headers map[string]string, codec uint16) []byte {
	// Build uncompressed record
	var rec bytes.Buffer
	var body bytes.Buffer

	// Attributes (1 byte)
	body.WriteByte(0)
	// Timestamp delta (0)
	putVarint(&body, 0)
	// Offset delta (0)
	putVarint(&body, 0)

	// Key
	if key == "" {
		putVarint(&body, -1)
	} else {
		putVarint(&body, int64(len(key)))
		body.WriteString(key)
	}

	// Value
	if val == "" {
		putVarint(&body, -1)
	} else {
		putVarint(&body, int64(len(val)))
		body.WriteString(val)
	}

	// Headers
	putVarint(&body, int64(len(headers)))
	for k, v := range headers {
		putVarint(&body, int64(len(k)))
		body.WriteString(k)
		putVarint(&body, int64(len(v)))
		body.WriteString(v)
	}

	// Length of body
	putVarint(&rec, int64(body.Len()))
	rec.Write(body.Bytes())

	rawRecords := rec.Bytes()
	var finalRecords []byte

	switch codec {
	case 1: // GZIP
		var gzBuf bytes.Buffer
		gw := gzip.NewWriter(&gzBuf)
		gw.Write(rawRecords)
		gw.Close()
		finalRecords = gzBuf.Bytes()
	case 2: // Snappy
		finalRecords = snappy.Encode(nil, rawRecords)
	case 3: // LZ4
		var lzBuf bytes.Buffer
		lw := lz4.NewWriter(&lzBuf)
		lw.Write(rawRecords)
		lw.Close()
		finalRecords = lzBuf.Bytes()
	default:
		finalRecords = rawRecords
	}

	// Full RecordBatch (61 bytes header + records)
	batch := make([]byte, 61+len(finalRecords))
	// Base offset: 0 (0..8)
	binary.BigEndian.PutUint64(batch[0:8], 0)
	// Batch length
	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-12))
	// Partition leader epoch: 0 (12..16)
	// Magic: 2 (16)
	batch[16] = 2
	// Attributes (21..23): codec in lowest 3 bits
	binary.BigEndian.PutUint16(batch[21:23], codec&0x07)
	// Base timestamp (27..35)
	binary.BigEndian.PutUint64(batch[27:35], 1700000000000)
	// Records count: 1 (57..61)
	binary.BigEndian.PutUint32(batch[57:61], 1)

	copy(batch[61:], finalRecords)
	return batch
}

func TestDecodeKafkaRecordPayload_Codecs(t *testing.T) {
	testHeaders := map[string]string{
		"X-Trace-Id": "trace-12345",
		"X-Source":   "unit-test",
	}

	codecs := []struct {
		name  string
		codec uint16
	}{
		{"Uncompressed", 0},
		{"GZIP", 1},
		{"Snappy", 2},
		{"LZ4", 3},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			rawBatch := buildRecordBatch("test-key", "test-payload-value-123", testHeaders, tc.codec)
			decoded := decodeKafkaRecordPayload(rawBatch)
			if decoded == nil {
				t.Fatalf("decodeKafkaRecordPayload returned nil for %s", tc.name)
			}
			if decoded.Key != "test-key" {
				t.Errorf("Expected key 'test-key', got '%s'", decoded.Key)
			}
			if decoded.Value != "test-payload-value-123" {
				t.Errorf("Expected value 'test-payload-value-123', got '%s'", decoded.Value)
			}
			if decoded.Headers["X-Trace-Id"] != "trace-12345" {
				t.Errorf("Expected header X-Trace-Id 'trace-12345', got '%s'", decoded.Headers["X-Trace-Id"])
			}
		})
	}
}
