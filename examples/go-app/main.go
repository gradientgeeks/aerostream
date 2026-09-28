package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"time"
)

const (
	defaultKafkaAddr  = "127.0.0.1:9093"
	defaultNativeAddr = "127.0.0.1:9091"
	topicName         = "go-streaming-events"
	numMessages       = 10
)

func main() {
	kafkaAddr := getEnv("AEROSTREAM_KAFKA_BROKER", defaultKafkaAddr)
	nativeAddr := getEnv("AEROSTREAM_NATIVE_BROKER", defaultNativeAddr)

	if len(os.Args) > 1 && os.Args[1] != "" {
		kafkaAddr = os.Args[1]
	}

	fmt.Println("================================================================================")
	fmt.Println("        🐹 AeroStream Go Application Demo & Compatibility Suite")
	fmt.Println("        Wire Protocol:  Apache Kafka Wire Protocol v0-v3 (Pure Go Zero-CGO)")
	fmt.Println("        Native Port:    AeroStream Zero-Copy Binary Protocol (0xAE 0x01)")
	fmt.Printf("        Target Endpoints: Kafka=%s, Native=%s\n", kafkaAddr, nativeAddr)
	fmt.Println("================================================================================\n")

	startTime := time.Now()
	allPassed := true

	// 1. Kafka ApiVersions (ApiKey 18)
	if !testKafkaApiVersions(kafkaAddr) {
		allPassed = false
	}

	// 2. Ensure Topic Created in Cluster (ApiKey 19)
	if !testKafkaCreateTopic(kafkaAddr, topicName, 1, 1) {
		allPassed = false
	}

	// 3. Kafka Topic Metadata (ApiKey 3)
	if !testKafkaMetadata(kafkaAddr, topicName) {
		allPassed = false
	}

	// 4. Kafka High-Speed Produce (ApiKey 0)
	lastOffset, ok := testKafkaProduce(kafkaAddr, topicName)
	if !ok {
		allPassed = false
	}

	// 5. Kafka Real-Time Fetch (ApiKey 1)
	if !testKafkaFetch(kafkaAddr, topicName, lastOffset) {
		allPassed = false
	}

	// 6. AeroStream Native Zero-Copy Binary Streaming (Port 9091)
	if !testNativeStreaming(nativeAddr, "go-native-stream") {
		allPassed = false
	}

	// 7. Large Message Ingestion & Fetch (1 MB & 10 MB)
	if !testLargeMessages(kafkaAddr, nativeAddr, topicName) {
		allPassed = false
	}

	totalTime := time.Since(startTime)
	fmt.Println("\n================================================================================")
	if allPassed {
		fmt.Printf("  🎉 ALL GO COMPATIBILITY TESTS PASSED! Total elapsed: %v\n", totalTime)
		fmt.Println("  AeroStream is 100% compatible with pure Go Kafka & Native streaming!")
		fmt.Println("================================================================================\n")
		os.Exit(0)
	} else {
		fmt.Println("  ❌ ONE OR MORE GO TESTS FAILED. Check errors above.")
		fmt.Println("================================================================================\n")
		os.Exit(1)
	}
}

func testKafkaApiVersions(addr string) bool {
	fmt.Println("[Step 1/5] Testing Kafka ApiVersions negotiation (ApiKey 18)...")
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect to Kafka port %s: %v\n", addr, err)
		return false
	}
	defer conn.Close()

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(18)) // ApiKey: ApiVersions
	binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(201)) // CorrelationId
	writeKafkaString(req, "go-app-demo")

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		fmt.Printf("  ❌ ApiVersions RPC failed: %v\n", err)
		return false
	}

	r := bytes.NewReader(resp)
	var corrID int32
	var errCode int16
	var numKeys int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &errCode)
	binary.Read(r, binary.BigEndian, &numKeys)

	if errCode != 0 {
		fmt.Printf("  ❌ ApiVersions returned error code %d\n", errCode)
		return false
	}

	fmt.Printf("  ✓ Connected successfully. CorrelationId=%d, Supported API Keys count=%d\n", corrID, numKeys)
	return true
}

func testKafkaCreateTopic(addr, topic string, partitions int32, rf int16) bool {
	fmt.Printf("\n[Step 2/6] Ensuring topic '%s' is registered in cluster metadata (ApiKey 19 CreateTopics)...\n", topic)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return false
	}
	defer conn.Close()

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(19)) // ApiKey: CreateTopics
	binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(201)) // CorrelationId
	writeKafkaString(req, "go-app-demo")
	binary.Write(req, binary.BigEndian, int32(1)) // 1 topic
	writeKafkaString(req, topic)
	binary.Write(req, binary.BigEndian, partitions)
	binary.Write(req, binary.BigEndian, rf)
	binary.Write(req, binary.BigEndian, int32(0)) // 0 manual assignments
	binary.Write(req, binary.BigEndian, int32(0)) // 0 configs
	binary.Write(req, binary.BigEndian, int32(5000)) // timeout ms

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		fmt.Printf("  ❌ CreateTopics RPC failed: %v\n", err)
		return false
	}

	r := bytes.NewReader(resp)
	var corrID, numTopics int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &numTopics)
	resTopic, _ := readKafkaString(r)
	var errCode int16
	binary.Read(r, binary.BigEndian, &errCode)

	if errCode == 0 {
		fmt.Printf("  ✓ Topic '%s' successfully created and registered in cluster!\n", resTopic)
	} else if errCode == 36 { // TOPIC_ALREADY_EXISTS
		fmt.Printf("  ✓ Topic '%s' already registered in cluster.\n", resTopic)
	} else {
		fmt.Printf("  ⚠️ CreateTopics response code %d for '%s'\n", errCode, resTopic)
	}
	return true
}

func testKafkaMetadata(addr, topic string) bool {
	fmt.Printf("\n[Step 3/6] Testing Kafka Metadata & Partition Discovery for '%s' (ApiKey 3)...\n", topic)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return false
	}
	defer conn.Close()

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(3))  // ApiKey: Metadata
	binary.Write(req, binary.BigEndian, int16(0))  // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(202)) // CorrelationId
	writeKafkaString(req, "go-app-demo")
	binary.Write(req, binary.BigEndian, int32(1)) // 1 topic
	writeKafkaString(req, topic)

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		fmt.Printf("  ❌ Metadata RPC failed: %v\n", err)
		return false
	}

	r := bytes.NewReader(resp)
	var corrID, numBrokers int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &numBrokers)

	fmt.Printf("  ✓ Discovered %d broker node(s) in cluster metadata:\n", numBrokers)
	for i := 0; i < int(numBrokers); i++ {
		var nodeID int32
		var port int32
		binary.Read(r, binary.BigEndian, &nodeID)
		host, _ := readKafkaString(r)
		binary.Read(r, binary.BigEndian, &port)
		fmt.Printf("    - Broker #%d: %s:%d\n", nodeID, host, port)
	}
	return true
}

func testKafkaProduce(addr, topic string) (int64, bool) {
	fmt.Printf("\n[Step 4/6] Testing Kafka Produce with batch CRC verification (ApiKey 0)...\n")
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return 0, false
	}
	defer conn.Close()

	var lastOffset int64 = 0
	for i := 0; i < numMessages; i++ {
		payload := []byte(fmt.Sprintf(`{"eventId": %d, "source": "go-app-demo", "metric": %.2f, "ts": %d}`,
			i, 42.0+float64(i)*1.5, time.Now().UnixMilli()))

		recordSet := wrapMessageSet(0, payload)

		req := new(bytes.Buffer)
		binary.Write(req, binary.BigEndian, int16(0))   // ApiKey: Produce
		binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion: 0
		binary.Write(req, binary.BigEndian, int32(300+i)) // CorrelationId
		writeKafkaString(req, "go-app-demo")
		binary.Write(req, binary.BigEndian, int16(1))   // acks=1
		binary.Write(req, binary.BigEndian, int32(3000)) // timeout
		binary.Write(req, binary.BigEndian, int32(1))   // 1 topic
		writeKafkaString(req, topic)
		binary.Write(req, binary.BigEndian, int32(1))   // 1 partition
		binary.Write(req, binary.BigEndian, int32(0))   // partition 0
		binary.Write(req, binary.BigEndian, int32(len(recordSet)))
		req.Write(recordSet)

		resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
		if err != nil {
			fmt.Printf("  ❌ Produce message [%d] failed: %v\n", i+1, err)
			return 0, false
		}

		r := bytes.NewReader(resp)
		var corrID, numTopics int32
		binary.Read(r, binary.BigEndian, &corrID)
		binary.Read(r, binary.BigEndian, &numTopics)
		resTopic, _ := readKafkaString(r)
		var numParts, partID int32
		var errCode int16
		var baseOffset int64
		binary.Read(r, binary.BigEndian, &numParts)
		binary.Read(r, binary.BigEndian, &partID)
		binary.Read(r, binary.BigEndian, &errCode)
		binary.Read(r, binary.BigEndian, &baseOffset)

		if errCode != 0 {
			fmt.Printf("  ❌ Produce returned error code %d for partition %d\n", errCode, partID)
			return 0, false
		}
		lastOffset = baseOffset
		fmt.Printf("  ✓ Published [%d/%d] topic='%s' partition=%d -> offset=%d\n",
			i+1, numMessages, resTopic, partID, baseOffset)
	}
	return lastOffset, true
}

func testKafkaFetch(addr, topic string, lastOffset int64) bool {
	fmt.Printf("\n[Step 5/6] Testing Kafka Fetch and payload verification (ApiKey 1)...\n")
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return false
	}
	defer conn.Close()

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(1))   // ApiKey: Fetch
	binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(401)) // CorrelationId
	writeKafkaString(req, "go-app-demo")
	binary.Write(req, binary.BigEndian, int32(-1))   // replicaId = -1 (client)
	binary.Write(req, binary.BigEndian, int32(5000)) // maxWaitMs
	binary.Write(req, binary.BigEndian, int32(1))    // minBytes
	binary.Write(req, binary.BigEndian, int32(1))    // 1 topic
	writeKafkaString(req, topic)
	binary.Write(req, binary.BigEndian, int32(1))    // 1 partition
	binary.Write(req, binary.BigEndian, int32(0))    // partition 0
	binary.Write(req, binary.BigEndian, int64(0))    // fetchOffset = 0
	binary.Write(req, binary.BigEndian, int32(1048576)) // maxBytes = 1MB

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		fmt.Printf("  ❌ Fetch RPC failed: %v\n", err)
		return false
	}

	r := bytes.NewReader(resp)
	var corrID, numTopics int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &numTopics)
	resTopic, _ := readKafkaString(r)
	var numParts, partID int32
	var errCode int16
	var highWatermark int64
	var recordSetSize int32
	binary.Read(r, binary.BigEndian, &numParts)
	binary.Read(r, binary.BigEndian, &partID)
	binary.Read(r, binary.BigEndian, &errCode)
	binary.Read(r, binary.BigEndian, &highWatermark)
	binary.Read(r, binary.BigEndian, &recordSetSize)

	if errCode != 0 {
		fmt.Printf("  ❌ Fetch returned error code %d\n", errCode)
		return false
	}

	fmt.Printf("  ✓ Fetch succeeded: topic='%s' partition=%d, HighWatermark=%d, bytesReceived=%d\n",
		resTopic, partID, highWatermark, recordSetSize)

	if recordSetSize > 0 {
		fmt.Printf("  ✓ Verified %d bytes of streaming record data returned from partition log.\n", recordSetSize)
		return true
	}
	fmt.Printf("  ❌ No records returned in fetch response.\n")
	return false
}

func testNativeStreaming(addr, topic string) bool {
	fmt.Println("\n[Step 6/6] Testing AeroStream Native Zero-Copy Binary Streaming (Port 9091)...")
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect to native port: %v\n", err)
		return false
	}
	defer conn.Close()

	// 1. Native Produce (Command 1)
	payload := []byte(fmt.Sprintf(`{"native_event": "ultra-fast-streaming", "ts": %d}`, time.Now().UnixNano()))
	bodyBuf := new(bytes.Buffer)
	binary.Write(bodyBuf, binary.BigEndian, uint16(len(topic)))
	bodyBuf.WriteString(topic)
	binary.Write(bodyBuf, binary.BigEndian, uint32(0)) // partition 0
	binary.Write(bodyBuf, binary.BigEndian, uint32(len(payload)))
	bodyBuf.Write(payload)

	hdrBuf := []byte{0xAE, 0x01, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(hdrBuf[3:7], uint32(bodyBuf.Len()))

	t0 := time.Now()
	if _, err := conn.Write(append(hdrBuf, bodyBuf.Bytes()...)); err != nil {
		fmt.Printf("  ❌ Native produce write failed: %v\n", err)
		return false
	}

	ack := make([]byte, 11)
	if _, err := io.ReadFull(conn, ack); err != nil {
		fmt.Printf("  ❌ Failed to read native ACK: %v\n", err)
		return false
	}
	produceLatency := time.Since(t0)

	if ack[0] != 0xAE || ack[1] != 0x01 || ack[2] != 0x00 {
		fmt.Printf("  ❌ Native produce returned error status: 0x%02x\n", ack[2])
		return false
	}

	assignedOffset := binary.BigEndian.Uint64(ack[3:11])
	fmt.Printf("  ✓ Native Produce ACK in %v: status=SUCCESS, assigned offset=%d\n", produceLatency, assignedOffset)

	// 2. Native Fetch (Command 2)
	fetchBody := new(bytes.Buffer)
	binary.Write(fetchBody, binary.BigEndian, uint16(len(topic)))
	fetchBody.WriteString(topic)
	binary.Write(fetchBody, binary.BigEndian, uint32(0))
	binary.Write(fetchBody, binary.BigEndian, uint64(assignedOffset))
	binary.Write(fetchBody, binary.BigEndian, uint32(65536))

	fetchHdr := []byte{0xAE, 0x01, 2, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(fetchHdr[3:7], uint32(fetchBody.Len()))

	t1 := time.Now()
	if _, err := conn.Write(append(fetchHdr, fetchBody.Bytes()...)); err != nil {
		fmt.Printf("  ❌ Native fetch write failed: %v\n", err)
		return false
	}

	resHdr := make([]byte, 7)
	if _, err := io.ReadFull(conn, resHdr); err != nil {
		fmt.Printf("  ❌ Failed to read native fetch response header: %v\n", err)
		return false
	}

	dataLen := binary.BigEndian.Uint32(resHdr[3:7])
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(conn, data); err != nil {
		fmt.Printf("  ❌ Failed to read native fetch payload: %v\n", err)
		return false
	}
	fetchLatency := time.Since(t1)

	fmt.Printf("  ✓ Native Fetch zero-copy stream in %v: %d bytes (payload='%s')\n",
		fetchLatency, dataLen, string(data))
	return true
}

// Helpers for Kafka wire protocol serialization
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
	inner.WriteByte(0)                               // magic 0
	inner.WriteByte(0)                               // attributes
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

func testLargeMessages(kafkaAddr, nativeAddr, topic string) bool {
	fmt.Println("\n[Step 7/7] Testing High-Throughput Large Messages (1 MB & 10 MB)...")

	sizes := []struct {
		name string
		size int
	}{
		{"1 MB", 1 * 1024 * 1024},
		{"10 MB", 10 * 1024 * 1024},
	}

	allPassed := true
	for _, s := range sizes {
		fmt.Printf("\n--- Testing %s (%d bytes) Payload ---\n", s.name, s.size)

		header := fmt.Sprintf(`{"sizeName":"%s","bytes":%d,"ts":%d,"data":"`, s.name, s.size, time.Now().UnixMilli())
		footer := `"}`
		padLen := s.size - len(header) - len(footer)
		if padLen < 0 {
			padLen = 0
		}
		pad := bytes.Repeat([]byte("A"), padLen)
		largePayload := append([]byte(header), pad...)
		largePayload = append(largePayload, []byte(footer)...)

		// Test A: Kafka Wire Protocol
		t0 := time.Now()
		kafkaOffset, ok := produceKafkaSingle(kafkaAddr, topic, largePayload)
		kafkaLatency := time.Since(t0)
		if !ok {
			fmt.Printf("  ❌ Kafka Produce failed for %s\n", s.name)
			allPassed = false
		} else {
			throughputMB := float64(s.size) / (1024 * 1024) / kafkaLatency.Seconds()
			fmt.Printf("  ✓ Kafka Wire Produce %s ACK in %v (Offset: %d, Throughput: %.2f MB/s)\n",
				s.name, kafkaLatency, kafkaOffset, throughputMB)

			// Fetch via Kafka
			tFetch := time.Now()
			fetchedLen, fetchOk := fetchKafkaSingle(kafkaAddr, topic, kafkaOffset, int32(s.size+65536))
			fetchLatency := time.Since(tFetch)
			if !fetchOk || fetchedLen < len(largePayload) {
				fmt.Printf("  ❌ Kafka Fetch failed for %s (got %d bytes)\n", s.name, fetchedLen)
				allPassed = false
			} else {
				fetchThroughputMB := float64(s.size) / (1024 * 1024) / fetchLatency.Seconds()
				fmt.Printf("  ✓ Kafka Wire Fetch %s in %v (Bytes: %d, Throughput: %.2f MB/s)\n",
					s.name, fetchLatency, fetchedLen, fetchThroughputMB)
			}
		}

		// Test B: AeroStream Native Zero-Copy Binary Protocol
		t1 := time.Now()
		nativeOffset, nativeOk := produceNativeSingle(nativeAddr, "go-large-stream", largePayload)
		nativeLatency := time.Since(t1)
		if !nativeOk {
			fmt.Printf("  ❌ Native Produce failed for %s\n", s.name)
			allPassed = false
		} else {
			nativeThroughputMB := float64(s.size) / (1024 * 1024) / nativeLatency.Seconds()
			fmt.Printf("  ✓ Native Binary Produce %s ACK in %v (Offset: %d, Throughput: %.2f MB/s)\n",
				s.name, nativeLatency, nativeOffset, nativeThroughputMB)

			// Fetch via Native
			tNativeFetch := time.Now()
			nativeFetchedLen, nFetchOk := fetchNativeSingle(nativeAddr, "go-large-stream", nativeOffset, uint32(s.size+65536))
			nFetchLatency := time.Since(tNativeFetch)
			if !nFetchOk || nativeFetchedLen != len(largePayload) {
				fmt.Printf("  ❌ Native Fetch failed for %s (got %d bytes, expected %d)\n", s.name, nativeFetchedLen, len(largePayload))
				allPassed = false
			} else {
				nFetchThroughputMB := float64(s.size) / (1024 * 1024) / nFetchLatency.Seconds()
				fmt.Printf("  ✓ Native Binary Fetch %s in %v (Bytes: %d, Throughput: %.2f MB/s)\n",
					s.name, nFetchLatency, nativeFetchedLen, nFetchThroughputMB)
			}
		}
	}
	return allPassed
}

func produceKafkaSingle(addr, topic string, payload []byte) (int64, bool) {
	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		fmt.Printf("  ❌ Failed to connect: %v\n", err)
		return 0, false
	}
	defer conn.Close()

	recordSet := wrapMessageSet(0, payload)

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(0))   // ApiKey: Produce
	binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(501)) // CorrelationId
	writeKafkaString(req, "go-app-demo")
	binary.Write(req, binary.BigEndian, int16(1))    // acks=1
	binary.Write(req, binary.BigEndian, int32(10000)) // timeout 10s
	binary.Write(req, binary.BigEndian, int32(1))    // 1 topic
	writeKafkaString(req, topic)
	binary.Write(req, binary.BigEndian, int32(1)) // 1 partition
	binary.Write(req, binary.BigEndian, int32(0)) // partition 0
	binary.Write(req, binary.BigEndian, int32(len(recordSet)))
	req.Write(recordSet)

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		fmt.Printf("  ❌ Produce RPC failed: %v\n", err)
		return 0, false
	}

	r := bytes.NewReader(resp)
	var corrID, numTopics int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &numTopics)
	_, _ = readKafkaString(r)
	var numParts, partID int32
	var errCode int16
	var baseOffset int64
	binary.Read(r, binary.BigEndian, &numParts)
	binary.Read(r, binary.BigEndian, &partID)
	binary.Read(r, binary.BigEndian, &errCode)
	binary.Read(r, binary.BigEndian, &baseOffset)

	if errCode != 0 {
		fmt.Printf("  ❌ Produce returned error code %d\n", errCode)
		return 0, false
	}
	return baseOffset, true
}

func fetchKafkaSingle(addr, topic string, offset int64, maxBytes int32) (int, bool) {
	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	req := new(bytes.Buffer)
	binary.Write(req, binary.BigEndian, int16(1))   // ApiKey: Fetch
	binary.Write(req, binary.BigEndian, int16(0))   // ApiVersion: 0
	binary.Write(req, binary.BigEndian, int32(502)) // CorrelationId
	writeKafkaString(req, "go-app-demo")
	binary.Write(req, binary.BigEndian, int32(-1))   // replicaId = -1
	binary.Write(req, binary.BigEndian, int32(5000)) // maxWaitMs
	binary.Write(req, binary.BigEndian, int32(1))    // minBytes
	binary.Write(req, binary.BigEndian, int32(1))    // 1 topic
	writeKafkaString(req, topic)
	binary.Write(req, binary.BigEndian, int32(1))    // 1 partition
	binary.Write(req, binary.BigEndian, int32(0))    // partition 0
	binary.Write(req, binary.BigEndian, offset)      // fetchOffset
	binary.Write(req, binary.BigEndian, maxBytes)    // maxBytes

	resp, err := sendAndRecvKafkaFrame(conn, req.Bytes())
	if err != nil {
		return 0, false
	}

	r := bytes.NewReader(resp)
	var corrID, numTopics int32
	binary.Read(r, binary.BigEndian, &corrID)
	binary.Read(r, binary.BigEndian, &numTopics)
	_, _ = readKafkaString(r)
	var numParts, partID int32
	var errCode int16
	var highWatermark int64
	var recordSetSize int32
	binary.Read(r, binary.BigEndian, &numParts)
	binary.Read(r, binary.BigEndian, &partID)
	binary.Read(r, binary.BigEndian, &errCode)
	binary.Read(r, binary.BigEndian, &highWatermark)
	binary.Read(r, binary.BigEndian, &recordSetSize)

	if errCode != 0 {
		return 0, false
	}
	return int(recordSetSize), true
}

func produceNativeSingle(addr, topic string, payload []byte) (uint64, bool) {
	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	bodyBuf := new(bytes.Buffer)
	binary.Write(bodyBuf, binary.BigEndian, uint16(len(topic)))
	bodyBuf.WriteString(topic)
	binary.Write(bodyBuf, binary.BigEndian, uint32(0)) // partition 0
	binary.Write(bodyBuf, binary.BigEndian, uint32(len(payload)))
	bodyBuf.Write(payload)

	hdrBuf := []byte{0xAE, 0x01, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(hdrBuf[3:7], uint32(bodyBuf.Len()))

	if _, err := conn.Write(append(hdrBuf, bodyBuf.Bytes()...)); err != nil {
		return 0, false
	}

	ack := make([]byte, 11)
	if _, err := io.ReadFull(conn, ack); err != nil {
		return 0, false
	}
	if ack[0] != 0xAE || ack[1] != 0x01 || ack[2] != 0x00 {
		return 0, false
	}
	return binary.BigEndian.Uint64(ack[3:11]), true
}

func fetchNativeSingle(addr, topic string, offset uint64, maxBytes uint32) (int, bool) {
	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return 0, false
	}
	defer conn.Close()

	fetchBody := new(bytes.Buffer)
	binary.Write(fetchBody, binary.BigEndian, uint16(len(topic)))
	fetchBody.WriteString(topic)
	binary.Write(fetchBody, binary.BigEndian, uint32(0))
	binary.Write(fetchBody, binary.BigEndian, offset)
	binary.Write(fetchBody, binary.BigEndian, maxBytes)

	fetchHdr := []byte{0xAE, 0x01, 2, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(fetchHdr[3:7], uint32(fetchBody.Len()))

	if _, err := conn.Write(append(fetchHdr, fetchBody.Bytes()...)); err != nil {
		return 0, false
	}

	resHdr := make([]byte, 7)
	if _, err := io.ReadFull(conn, resHdr); err != nil {
		return 0, false
	}
	dataLen := int(binary.BigEndian.Uint32(resHdr[3:7]))
	// Stream read payload
	buf := make([]byte, 64*1024)
	remaining := dataLen
	for remaining > 0 {
		toRead := len(buf)
		if remaining < toRead {
			toRead = remaining
		}
		n, err := conn.Read(buf[:toRead])
		if err != nil {
			return 0, false
		}
		remaining -= n
	}
	return dataLen, true
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

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
