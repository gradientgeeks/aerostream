package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func writeKafkaString(buf *bytes.Buffer, s string) {
	binary.Write(buf, binary.BigEndian, int16(len(s)))
	buf.WriteString(s)
}

func readKafkaString(r io.Reader) (string, error) {
	var strLen int16
	if err := binary.Read(r, binary.BigEndian, &strLen); err != nil {
		return "", err
	}
	if strLen < 0 {
		return "", nil
	}
	buf := make([]byte, strLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func wrapMessageSet(offset int64, payload []byte) []byte {
	inner := new(bytes.Buffer)
	inner.WriteByte(0) // magic
	inner.WriteByte(0) // attributes
	binary.Write(inner, binary.BigEndian, int32(-1)) // null key
	binary.Write(inner, binary.BigEndian, int32(len(payload)))
	inner.Write(payload)

	crc := crc32.ChecksumIEEE(inner.Bytes())
	msgSize := int32(4 + inner.Len())

	record := new(bytes.Buffer)
	binary.Write(record, binary.BigEndian, offset)
	binary.Write(record, binary.BigEndian, msgSize)
	binary.Write(record, binary.BigEndian, crc)
	record.Write(inner.Bytes())

	return record.Bytes()
}

func sendAndRecvKafkaFrame(conn net.Conn, reqPayload []byte) ([]byte, error) {
	frameLen := int32(len(reqPayload))
	if err := binary.Write(conn, binary.BigEndian, frameLen); err != nil {
		return nil, err
	}
	if _, err := conn.Write(reqPayload); err != nil {
		return nil, err
	}

	var respLen int32
	if err := binary.Read(conn, binary.BigEndian, &respLen); err != nil {
		return nil, err
	}
	if respLen <= 0 || respLen > 64*1024*1024 {
		return nil, fmt.Errorf("invalid response length: %d", respLen)
	}

	respBuf := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		return nil, err
	}
	return respBuf, nil
}

func TestKafkaWireProtocolLive(t *testing.T) {
	// Try connecting to default Kafka port 9093
	conn, err := net.DialTimeout("tcp", "127.0.0.1:9093", 1*time.Second)
	if err != nil {
		t.Skipf("AeroMQ broker Kafka port 9093 not currently running, skipping live connection test: %v", err)
		return
	}
	defer conn.Close()

	// 1. ApiVersions (ApiKey 18)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(18)) // ApiKey
		binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion
		binary.Write(req, binary.BigEndian, int32(101)) // CorrelationId
		writeKafkaString(req, "go-client-test")

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("ApiVersions frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		var errCode int16
		var numKeys int32
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &numKeys)

		if corrID != 101 {
			t.Errorf("Correlation ID mismatch: got %d, expected 101", corrID)
		}
		if errCode != 0 {
			t.Errorf("Expected errCode 0, got %d", errCode)
		}
		if numKeys < 4 {
			t.Errorf("Expected >= 4 supported API keys, got %d", numKeys)
		}
	}

	// 2. Metadata (ApiKey 3)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(3)) // ApiKey
		binary.Write(req, binary.BigEndian, int16(0)) // ApiVersion
		binary.Write(req, binary.BigEndian, int32(102)) // CorrelationId
		writeKafkaString(req, "go-client-test")
		binary.Write(req, binary.BigEndian, int32(1)) // 1 topic
		writeKafkaString(req, "go-test-topic")

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Metadata frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 102 {
			t.Errorf("Correlation ID mismatch: got %d, expected 102", corrID)
		}
	}

	// 3. Produce (ApiKey 0)
	testPayload := []byte("go-kafka-produce-test")
	recordSet := wrapMessageSet(0, testPayload)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion
		binary.Write(req, binary.BigEndian, int32(103)) // CorrelationId
		writeKafkaString(req, "go-client-test")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(1000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, "go-test-topic")
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordSet)))
		req.Write(recordSet)

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Produce frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 103 {
			t.Errorf("Correlation ID mismatch: got %d, expected 103", corrID)
		}
	}

	// 4. Fetch (ApiKey 1)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(1))   // ApiKey Fetch
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion
		binary.Write(req, binary.BigEndian, int32(104)) // CorrelationId
		writeKafkaString(req, "go-client-test")
		binary.Write(req, binary.BigEndian, int32(-1))  // replicaId
		binary.Write(req, binary.BigEndian, int32(500)) // maxWait
		binary.Write(req, binary.BigEndian, int32(1))   // minBytes
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, "go-test-topic")
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int64(0))   // fetch_offset 0
		binary.Write(req, binary.BigEndian, int32(65536)) // max_bytes

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Fetch frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 104 {
			t.Errorf("Correlation ID mismatch: got %d, expected 104", corrID)
		}
		if !bytes.Contains(respBytes, testPayload) {
			t.Errorf("Expected fetched payload to contain %q", testPayload)
		}
	}
}

func TestKafkaWireProtocolWithBroker(t *testing.T) {
	brokerBin := "../rust-broker/target/release/rust-broker"
	if _, err := os.Stat(brokerBin); err != nil {
		brokerBin = "../rust-broker/target/debug/rust-broker"
		if _, err := os.Stat(brokerBin); err != nil {
			t.Skipf("Broker binary not found at %s: %v", brokerBin, err)
			return
		}
	}

	tmpDir, err := os.MkdirTemp("", "aeromq-kafka-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cmd := exec.Command(brokerBin,
		"--host", "127.0.0.1",
		"--data-port", "19091",
		"--kafka-port", "19093",
		"--storage-dir", tmpDir,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait for port 19093 to be ready
	var conn net.Conn
	for i := 0; i < 30; i++ {
		c, err := net.Dial("tcp", "127.0.0.1:19093")
		if err == nil {
			conn = c
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("Timed out waiting for broker on port 19093")
	}
	defer conn.Close()

	// 1. ApiVersions test
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(18)) // ApiKey
		binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion
		binary.Write(req, binary.BigEndian, int32(501)) // CorrelationId
		writeKafkaString(req, "go-integration-test")

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("ApiVersions failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		var errCode int16
		var numKeys int32
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &numKeys)

		if corrID != 501 {
			t.Errorf("Correlation ID mismatch: got %d, expected 501", corrID)
		}
		if errCode != 0 {
			t.Errorf("Expected errCode 0, got %d", errCode)
		}
		if numKeys < 4 {
			t.Errorf("Expected >= 4 API keys, got %d", numKeys)
		}
	}

	// 2. Metadata test
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(3)) // ApiKey
		binary.Write(req, binary.BigEndian, int16(0)) // ApiVersion
		binary.Write(req, binary.BigEndian, int32(502)) // CorrelationId
		writeKafkaString(req, "go-integration-test")
		binary.Write(req, binary.BigEndian, int32(1)) // 1 topic
		writeKafkaString(req, "integ-topic")

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Metadata failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 502 {
			t.Errorf("Correlation ID mismatch: got %d, expected 502", corrID)
		}
	}

	// 3. Produce test
	testPayload := []byte("integration-kafka-produce-message")
	recordSet := wrapMessageSet(0, testPayload)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion
		binary.Write(req, binary.BigEndian, int32(503)) // CorrelationId
		writeKafkaString(req, "go-integration-test")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(1000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, "integ-topic")
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordSet)))
		req.Write(recordSet)

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Produce failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 503 {
			t.Errorf("Correlation ID mismatch: got %d, expected 503", corrID)
		}
	}

	// 4. Fetch test
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(1))   // ApiKey Fetch
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion
		binary.Write(req, binary.BigEndian, int32(504)) // CorrelationId
		writeKafkaString(req, "go-integration-test")
		binary.Write(req, binary.BigEndian, int32(-1))  // replicaId
		binary.Write(req, binary.BigEndian, int32(500)) // maxWait
		binary.Write(req, binary.BigEndian, int32(1))   // minBytes
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, "integ-topic")
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int64(0))   // fetch_offset 0
		binary.Write(req, binary.BigEndian, int32(65536)) // max_bytes

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		binary.Read(reader, binary.BigEndian, &corrID)
		if corrID != 504 {
			t.Errorf("Correlation ID mismatch: got %d, expected 504", corrID)
		}
		if !bytes.Contains(respBytes, testPayload) {
			t.Errorf("Expected fetched payload to contain %q", testPayload)
		}
	}
}

