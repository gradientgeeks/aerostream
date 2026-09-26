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

func writeVarint(buf *bytes.Buffer, val int32) {
	raw := uint32((val << 1) ^ (val >> 31))
	for raw >= 0x80 {
		buf.WriteByte(byte(raw&0x7F | 0x80))
		raw >>= 7
	}
	buf.WriteByte(byte(raw))
}

func writeVarlong(buf *bytes.Buffer, val int64) {
	raw := uint64((val << 1) ^ (val >> 63))
	for raw >= 0x80 {
		buf.WriteByte(byte(raw&0x7F | 0x80))
		raw >>= 7
	}
	buf.WriteByte(byte(raw))
}

func wrapIdempotentRecordBatch(baseOffset int64, producerID int64, producerEpoch int16, baseSequence int32, payload []byte) []byte {
	recInner := new(bytes.Buffer)
	recInner.WriteByte(0)     // attributes
	writeVarlong(recInner, 0) // timestamp delta
	writeVarint(recInner, 0)  // offset delta
	writeVarint(recInner, -1) // null key
	writeVarint(recInner, int32(len(payload)))
	recInner.Write(payload)
	writeVarint(recInner, 0) // headers count

	recsBuf := new(bytes.Buffer)
	writeVarint(recsBuf, int32(recInner.Len()))
	recsBuf.Write(recInner.Bytes())

	batchLen := int32(49 + recsBuf.Len())

	batch := new(bytes.Buffer)
	binary.Write(batch, binary.BigEndian, baseOffset) // 0..8
	binary.Write(batch, binary.BigEndian, batchLen)   // 8..12
	binary.Write(batch, binary.BigEndian, int32(0))   // 12..16 leader epoch
	batch.WriteByte(2)                                // 16 magic

	crcPos := batch.Len()
	binary.Write(batch, binary.BigEndian, uint32(0)) // 17..21

	crcStart := batch.Len()
	binary.Write(batch, binary.BigEndian, int16(0))      // 21..23 attributes
	binary.Write(batch, binary.BigEndian, int32(0))      // 23..27 last offset delta
	nowMs := time.Now().UnixMilli()
	binary.Write(batch, binary.BigEndian, nowMs)         // 27..35 first ts
	binary.Write(batch, binary.BigEndian, nowMs)         // 35..43 max ts
	binary.Write(batch, binary.BigEndian, producerID)    // 43..51 producer_id
	binary.Write(batch, binary.BigEndian, producerEpoch) // 51..53 producer_epoch
	binary.Write(batch, binary.BigEndian, baseSequence)  // 53..57 base_sequence
	binary.Write(batch, binary.BigEndian, int32(1))      // 57..61 records count
	batch.Write(recsBuf.Bytes())

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	batchBytes := batch.Bytes()
	c := crc32.Checksum(batchBytes[crcStart:], crcTable)
	binary.BigEndian.PutUint32(batchBytes[crcPos:crcPos+4], c)

	return batchBytes
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

func getLatestBrokerBin(t *testing.T) string {
	debugBin := "../rust-broker/target/debug/rust-broker"
	releaseBin := "../rust-broker/target/release/rust-broker"
	dStat, dErr := os.Stat(debugBin)
	rStat, rErr := os.Stat(releaseBin)
	if dErr == nil && rErr == nil {
		if dStat.ModTime().After(rStat.ModTime()) {
			return debugBin
		}
		return releaseBin
	}
	if dErr == nil {
		return debugBin
	}
	if rErr == nil {
		return releaseBin
	}
	t.Skipf("No broker binary found at %s or %s", debugBin, releaseBin)
	return ""
}

func TestKafkaWireProtocolWithBroker(t *testing.T) {
	brokerBin := getLatestBrokerBin(t)

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

func TestPhase4IdempotentProducerWithBroker(t *testing.T) {
	brokerBin := getLatestBrokerBin(t)

	tmpDir, err := os.MkdirTemp("", "aeromq-phase4-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cmd := exec.Command(brokerBin,
		"--host", "127.0.0.1",
		"--data-port", "19096",
		"--kafka-port", "19097",
		"--storage-dir", tmpDir,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start broker: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait for port 19097 to be ready
	var conn net.Conn
	for i := 0; i < 40; i++ {
		c, err := net.Dial("tcp", "127.0.0.1:19097")
		if err == nil {
			conn = c
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("Timed out waiting for broker on port 19097")
	}
	defer conn.Close()

	// 1. Test InitProducerId via Kafka TCP port (ApiKey 22)
	var producerID int64
	var producerEpoch int16
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(22)) // ApiKey 22 (InitProducerId)
		binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion 0
		binary.Write(req, binary.BigEndian, int32(801)) // CorrelationId 801
		writeKafkaString(req, "phase4-idemp-client")
		binary.Write(req, binary.BigEndian, int16(-1)) // transactional_id: null
		binary.Write(req, binary.BigEndian, int32(60000)) // transaction_timeout_ms

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("InitProducerId frame failed: %v", err)
		}

		reader := bytes.NewReader(respBytes)
		var corrID int32
		var throttleTime int32
		var errCode int16
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &throttleTime)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &producerID)
		binary.Read(reader, binary.BigEndian, &producerEpoch)

		if corrID != 801 {
			t.Errorf("InitProducerId correlation ID mismatch: got %d, expected 801", corrID)
		}
		if errCode != 0 {
			t.Errorf("InitProducerId expected errCode 0, got %d", errCode)
		}
		if producerID < 1000 {
			t.Errorf("Expected assigned producer_id >= 1000, got %d", producerID)
		}
	}

	topic := "idemp-orders-topic"
	testPayload := []byte("order-record-sequence-0")

	// 2. Test Idempotent Produce: produce with PID and sequence 0
	recordBatch0 := wrapIdempotentRecordBatch(0, producerID, producerEpoch, 0, testPayload)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion 0
		binary.Write(req, binary.BigEndian, int32(802)) // CorrelationId 802
		writeKafkaString(req, "phase4-idemp-client")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(1000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, topic)
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordBatch0)))
		req.Write(recordBatch0)

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("First produce frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		var numTopics int32
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &numTopics)
		readKafkaString(reader) // topic name
		var numParts int32
		binary.Read(reader, binary.BigEndian, &numParts)
		var partID int32
		var errCode int16
		var baseOffset int64
		binary.Read(reader, binary.BigEndian, &partID)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &baseOffset)

		if corrID != 802 {
			t.Errorf("Correlation ID mismatch: got %d, expected 802", corrID)
		}
		if errCode != 0 {
			t.Errorf("Expected errCode 0, got %d", errCode)
		}
		if baseOffset != 0 {
			t.Errorf("Expected baseOffset 0, got %d", baseOffset)
		}
	}

	// 3. Test duplicate sequence 0: send duplicate sequence 0 -> assert duplicate ACK without second record
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion 0
		binary.Write(req, binary.BigEndian, int32(803)) // CorrelationId 803
		writeKafkaString(req, "phase4-idemp-client")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(1000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, topic)
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordBatch0)))
		req.Write(recordBatch0)

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Duplicate produce frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		var numTopics int32
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &numTopics)
		readKafkaString(reader) // topic name
		var numParts int32
		binary.Read(reader, binary.BigEndian, &numParts)
		var partID int32
		var errCode int16
		var baseOffset int64
		binary.Read(reader, binary.BigEndian, &partID)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &baseOffset)

		if corrID != 803 {
			t.Errorf("Correlation ID mismatch: got %d, expected 803", corrID)
		}
		if errCode != 0 {
			t.Errorf("Expected duplicate produce to return errCode 0 (duplicate ACK), got %d", errCode)
		}
		if baseOffset != 0 {
			t.Errorf("Expected duplicate produce to return original baseOffset 0, got %d", baseOffset)
		}
	}

	// Verify log length only increased by 1 via Fetch offset 0 vs Fetch offset 1
	{
		// Fetch offset 0 -> must contain record
		req0 := new(bytes.Buffer)
		binary.Write(req0, binary.BigEndian, int16(1))   // Fetch
		binary.Write(req0, binary.BigEndian, int16(0))   // v0
		binary.Write(req0, binary.BigEndian, int32(804)) // CorrelationId 804
		writeKafkaString(req0, "phase4-idemp-client")
		binary.Write(req0, binary.BigEndian, int32(-1))  // replicaId
		binary.Write(req0, binary.BigEndian, int32(500)) // maxWait
		binary.Write(req0, binary.BigEndian, int32(1))   // minBytes
		binary.Write(req0, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req0, topic)
		binary.Write(req0, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req0, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req0, binary.BigEndian, int64(0))   // fetch_offset 0
		binary.Write(req0, binary.BigEndian, int32(65536)) // max_bytes

		respBytes0, err := sendAndRecvKafkaFrame(conn, req0.Bytes())
		if err != nil {
			t.Fatalf("Fetch offset 0 failed: %v", err)
		}
		if !bytes.Contains(respBytes0, testPayload) {
			t.Errorf("Expected fetched payload at offset 0 to contain %q", testPayload)
		}

		// Fetch offset 1 -> must NOT contain any records (log length is exactly 1)
		req1 := new(bytes.Buffer)
		binary.Write(req1, binary.BigEndian, int16(1))   // Fetch
		binary.Write(req1, binary.BigEndian, int16(0))   // v0
		binary.Write(req1, binary.BigEndian, int32(805)) // CorrelationId 805
		writeKafkaString(req1, "phase4-idemp-client")
		binary.Write(req1, binary.BigEndian, int32(-1))  // replicaId
		binary.Write(req1, binary.BigEndian, int32(500)) // maxWait
		binary.Write(req1, binary.BigEndian, int32(1))   // minBytes
		binary.Write(req1, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req1, topic)
		binary.Write(req1, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req1, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req1, binary.BigEndian, int64(1))   // fetch_offset 1
		binary.Write(req1, binary.BigEndian, int32(65536)) // max_bytes

		respBytes1, err := sendAndRecvKafkaFrame(conn, req1.Bytes())
		if err != nil {
			t.Fatalf("Fetch offset 1 failed: %v", err)
		}
		reader1 := bytes.NewReader(respBytes1)
		var corrID int32
		var numTopics int32
		binary.Read(reader1, binary.BigEndian, &corrID)
		binary.Read(reader1, binary.BigEndian, &numTopics)
		readKafkaString(reader1) // topic
		var numParts int32
		binary.Read(reader1, binary.BigEndian, &numParts)
		var pID int32
		var pErr int16
		var hw int64
		var recordsLen int32
		binary.Read(reader1, binary.BigEndian, &pID)
		binary.Read(reader1, binary.BigEndian, &pErr)
		binary.Read(reader1, binary.BigEndian, &hw)
		binary.Read(reader1, binary.BigEndian, &recordsLen)

		if recordsLen != 0 {
			t.Errorf("Expected 0 records at offset 1 (log length should be only 1), got recordsLen %d", recordsLen)
		}
		if hw != 1 {
			t.Errorf("Expected high watermark 1, got %d", hw)
		}
	}

	// 4. Test sequence gap: send sequence 5 when last sequence was 0 -> assert error code 45 (OutOfOrderSequenceNumber)
	gapPayload := []byte("gap-record-sequence-5")
	recordBatchGap := wrapIdempotentRecordBatch(1, producerID, producerEpoch, 5, gapPayload)
	{
		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion 0
		binary.Write(req, binary.BigEndian, int32(806)) // CorrelationId 806
		writeKafkaString(req, "phase4-idemp-client")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(1000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, topic)
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordBatchGap)))
		req.Write(recordBatchGap)

		respBytes, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			t.Fatalf("Gap produce frame failed: %v", err)
		}
		reader := bytes.NewReader(respBytes)
		var corrID int32
		var numTopics int32
		binary.Read(reader, binary.BigEndian, &corrID)
		binary.Read(reader, binary.BigEndian, &numTopics)
		readKafkaString(reader) // topic name
		var numParts int32
		binary.Read(reader, binary.BigEndian, &numParts)
		var partID int32
		var errCode int16
		var baseOffset int64
		binary.Read(reader, binary.BigEndian, &partID)
		binary.Read(reader, binary.BigEndian, &errCode)
		binary.Read(reader, binary.BigEndian, &baseOffset)

		if corrID != 806 {
			t.Errorf("Correlation ID mismatch: got %d, expected 806", corrID)
		}
		// In Kafka specification, OutOfOrderSequenceNumber is error code 45
		if errCode != 45 {
			t.Errorf("Expected error code 45 (OutOfOrderSequenceNumber), got %d", errCode)
		}
	}
}

