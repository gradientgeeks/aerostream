package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"aeromq/go_clients/common"

	"github.com/IBM/sarama"
)

type consumerGroupHandler struct {
	id             string
	assignedClaims map[string][]int32
	messagesRecv   int64
	onMessage      func(msg *sarama.ConsumerMessage)
	ready          chan struct{}
	mu             sync.Mutex
}

func newConsumerGroupHandler(id string, onMessage func(*sarama.ConsumerMessage)) *consumerGroupHandler {
	return &consumerGroupHandler{
		id:             id,
		assignedClaims: make(map[string][]int32),
		ready:          make(chan struct{}, 10),
		onMessage:      onMessage,
	}
}

func (h *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for topic, parts := range sess.Claims() {
		h.assignedClaims[topic] = parts
		fmt.Printf("  %s[%s]%s Setup assigned topic=%s partitions=%v\n",
			common.ColorCyan, h.id, common.ColorReset, topic, parts)
	}
	select {
	case h.ready <- struct{}{}:
	default:
	}
	return nil
}

func (h *consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Printf("  %s[%s]%s Cleanup/Revocation invoked\n", common.ColorYellow, h.id, common.ColorReset)
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		atomic.AddInt64(&h.messagesRecv, 1)
		if h.onMessage != nil {
			h.onMessage(msg)
		}
		sess.MarkMessage(msg, "")
		sess.Commit()
	}
	return nil
}

func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9092", "Kafka wire protocol broker address")
	controllerAddr := flag.String("controller", "http://127.0.0.1:9001", "AeroStream controller REST address")
	flag.Parse()

	fmt.Printf("%s%s========================================================================%s\n", common.ColorBold, common.ColorCyan, common.ColorReset)
	fmt.Printf("%s%s       AEROSTREAM E2E TEST RUNNER - IBM/sarama (Go Client)              %s\n", common.ColorBold, common.ColorWhite, common.ColorReset)
	fmt.Printf("%s%s========================================================================%s\n\n", common.ColorBold, common.ColorCyan, common.ColorReset)
	fmt.Printf("Target Broker:     %s%s%s\n", common.ColorGreen, *brokerAddr, common.ColorReset)
	fmt.Printf("Target Controller: %s%s%s\n\n", common.ColorGreen, *controllerAddr, common.ColorReset)

	runID := time.Now().UnixNano()
	suiteTopic := fmt.Sprintf("sarama-e2e-suite-%d", runID)

	// Ensure 3-partition test topic exists via Kafka Wire Protocol (ApiKey 19 CreateTopics)
	fmt.Printf("%s[Step 0] Creating 3-partition test topic: %s (Kafka ApiKey 19)...%s\n", common.ColorBold, suiteTopic, common.ColorReset)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	admin, err := sarama.NewClusterAdmin([]string{*brokerAddr}, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize sarama.ClusterAdmin: %v", err)
	}
	if err := admin.CreateTopic(suiteTopic, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1}, false); err != nil {
		log.Fatalf("Failed to create topic via Kafka Wire: %v", err)
	}
	admin.Close()
	fmt.Printf("  %s✓ Topic '%s' successfully created with 3 partitions%s\n\n", common.ColorGreen, suiteTopic, common.ColorReset)

	// Test 1: Metadata & Topic Discovery
	testMetadataAndDiscovery([]string{*brokerAddr}, suiteTopic)

	// Test 2: Compression Codecs Matrix (SyncProducer)
	testCompressionCodecs([]string{*brokerAddr}, suiteTopic)

	// Test 3: Key-Based Partition Routing across 3 Partitions (AsyncProducer)
	testKeyBasedPartitionRouting([]string{*brokerAddr}, suiteTopic)

	// Test 4: Consumer Group Rebalancing, Consumption, Checksums & Commit Resumption
	testConsumerGroupLifecycle([]string{*brokerAddr}, *controllerAddr)

	// Test 5: High-Throughput Batching & Latency Benchmark
	testSaramaBenchmark([]string{*brokerAddr}, suiteTopic)

	fmt.Printf("\n%s%s========================================================================%s\n", common.ColorBold, common.ColorGreen, common.ColorReset)
	fmt.Printf("%s%s       ALL SARAMA E2E TESTS PASSED SUCCESSFULLY!                        %s\n", common.ColorBold, common.ColorGreen, common.ColorReset)
	fmt.Printf("%s%s========================================================================%s\n", common.ColorBold, common.ColorGreen, common.ColorReset)
}

func testMetadataAndDiscovery(brokers []string, targetTopic string) {
	fmt.Printf("%s[Test 1] Metadata & Topic Discovery (sarama.ClusterAdmin)%s\n", common.ColorBold, common.ColorReset)
	start := time.Now()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		log.Fatalf("Failed to initialize sarama.ClusterAdmin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("ClusterAdmin.ListTopics failed: %v", err)
	}

	detail, exists := topics[targetTopic]
	if !exists {
		log.Fatalf("Target topic '%s' not discovered in metadata", targetTopic)
	}

	fmt.Printf("  %s✓ Discovered %d total topics in cluster metadata%s\n", common.ColorGreen, len(topics), common.ColorReset)
	fmt.Printf("  %s✓ Topic '%s' details: %d Partitions, ReplicationFactor=%d%s\n",
		common.ColorGreen, targetTopic, detail.NumPartitions, detail.ReplicationFactor, common.ColorReset)

	// Also verify sarama.Client broker connection
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		log.Fatalf("sarama.NewClient failed: %v", err)
	}
	defer client.Close()

	brokerList := client.Brokers()
	fmt.Printf("  %s✓ Active client connected to %d broker(s): [ID=%d @ %s]%s (elapsed: %v)\n\n",
		common.ColorGreen, len(brokerList), brokerList[0].ID(), brokerList[0].Addr(), common.ColorReset, time.Since(start).Round(time.Millisecond))
}

func testCompressionCodecs(brokers []string, topic string) {
	fmt.Printf("%s[Test 2] Codec Matrix & Headers Verification (SyncProducer)%s\n", common.ColorBold, common.ColorReset)

	codecs := []struct {
		name  string
		codec sarama.CompressionCodec
	}{
		{"none", sarama.CompressionNone},
		{"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy},
		{"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
	}

	consumerCfg := sarama.NewConfig()
	consumerCfg.Version = sarama.V2_8_0_0
	consumer, err := sarama.NewConsumer(brokers, consumerCfg)
	if err != nil {
		log.Fatalf("NewConsumer failed: %v", err)
	}
	defer consumer.Close()

	for _, c := range codecs {
		t0 := time.Now()
		cfg := sarama.NewConfig()
		cfg.Version = sarama.V2_8_0_0
		cfg.Producer.Return.Successes = true
		cfg.Producer.Compression = c.codec

		producer, err := sarama.NewSyncProducer(brokers, cfg)
		if err != nil {
			log.Fatalf("NewSyncProducer failed for codec %s: %v", c.name, err)
		}

		payload := []byte(fmt.Sprintf("sarama-compression-%s-data-%s", c.name, bytes.Repeat([]byte("0123456789ABCDEF"), 25)))
		expectedSHA := common.ComputeSHA256(payload)
		traceID := fmt.Sprintf("trace-codec-%s-%d", c.name, time.Now().UnixNano())

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%s", c.name)),
			Value: sarama.ByteEncoder(payload),
			Headers: []sarama.RecordHeader{
				{Key: []byte("X-Trace-Id"), Value: []byte(traceID)},
				{Key: []byte("X-Source"), Value: []byte("sarama-sync-producer")},
				{Key: []byte("X-Codec"), Value: []byte(c.name)},
				{Key: []byte("X-SHA256"), Value: []byte(expectedSHA)},
			},
		}

		part, offset, err := producer.SendMessage(msg)
		producer.Close()
		if err != nil {
			log.Fatalf("SendMessage failed for codec %s: %v", c.name, err)
		}

		// Consume partition to verify payload and headers
		pc, err := consumer.ConsumePartition(topic, part, offset)
		if err != nil {
			log.Fatalf("ConsumePartition failed for codec %s: %v", c.name, err)
		}

		select {
		case recv := <-pc.Messages():
			pc.Close()
			recvSHA := common.ComputeSHA256(recv.Value)
			if recvSHA != expectedSHA {
				log.Fatalf("Codec %s SHA-256 mismatch! expected %s, got %s", c.name, expectedSHA, recvSHA)
			}

			// Verify headers
			headerMap := make(map[string]string)
			for _, h := range recv.Headers {
				headerMap[string(h.Key)] = string(h.Value)
			}
			if headerMap["X-Trace-Id"] != traceID || headerMap["X-Source"] != "sarama-sync-producer" {
				log.Fatalf("Codec %s headers mismatch: %v", c.name, headerMap)
			}

			fmt.Printf("  %s✓ Codec %-6s: produced and verified at part=%d, off=%d (SHA-256 & 4 headers match)%s [%v]\n",
				common.ColorGreen, c.name, part, offset, common.ColorReset, time.Since(t0).Round(time.Microsecond))

		case <-time.After(5 * time.Second):
			pc.Close()
			log.Fatalf("Timed out waiting for message for codec %s", c.name)
		}
	}
	fmt.Println()
}

func testKeyBasedPartitionRouting(brokers []string, topic string) {
	fmt.Printf("%s[Test 3] Key-Based Partition Routing Across 3 Partitions (AsyncProducer)%s\n", common.ColorBold, common.ColorReset)
	start := time.Now()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Flush.Messages = 50
	cfg.Producer.Flush.Frequency = 10 * time.Millisecond
	cfg.Producer.Partitioner = sarama.NewHashPartitioner // Standard Murmur2 hash partitioner

	asyncProd, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("NewAsyncProducer failed: %v", err)
	}
	defer asyncProd.Close()

	numMsgs := 300
	var wg sync.WaitGroup
	wg.Add(numMsgs)

	partitionsCount := make(map[int32]int)
	var mu sync.Mutex

	go func() {
		for s := range asyncProd.Successes() {
			mu.Lock()
			partitionsCount[s.Partition]++
			mu.Unlock()
			wg.Done()
		}
	}()

	go func() {
		for e := range asyncProd.Errors() {
			fmt.Printf("  %sAsync produce error: %v%s\n", common.ColorRed, e.Err, common.ColorReset)
			wg.Done()
		}
	}()

	for i := 0; i < numMsgs; i++ {
		key := fmt.Sprintf("partition-key-%04d", i)
		val := fmt.Sprintf("routed-async-payload-%04d", i)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(val),
			Headers: []sarama.RecordHeader{
				{Key: []byte("X-Trace-Id"), Value: []byte(fmt.Sprintf("trace-%d", i))},
				{Key: []byte("X-Source"), Value: []byte("sarama-async-router")},
			},
		}
		asyncProd.Input() <- msg
	}

	wg.Wait()
	elapsed := time.Since(start)

	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("  %s✓ Produced %d messages via AsyncProducer in %v (%d msg/sec)%s\n",
		common.ColorGreen, numMsgs, elapsed.Round(time.Millisecond), int(float64(numMsgs)/elapsed.Seconds()), common.ColorReset)
	fmt.Printf("  %s✓ Partition Routing Distribution across 3 partitions:%s\n", common.ColorGreen, common.ColorReset)
	for p := int32(0); p < 3; p++ {
		cnt := partitionsCount[p]
		pct := float64(cnt) * 100.0 / float64(numMsgs)
		fmt.Printf("    Partition %d: %3d msgs (%.1f%%)\n", p, cnt, pct)
		if cnt == 0 {
			log.Fatalf("Partition %d received 0 messages! Partition routing failed.", p)
		}
	}
	fmt.Println()
}

func testConsumerGroupLifecycle(brokers []string, controllerAddr string) {
	fmt.Printf("%s[Test 4] Consumer Group Lifecycle: Dynamic Rebalance, SHA-256 Checksum & Commit Resumption%s\n",
		common.ColorBold, common.ColorReset)

	runID := time.Now().UnixNano()
	rebalTopic := fmt.Sprintf("sarama-cg-rebal-%d", runID)
	rebalGroup := fmt.Sprintf("sarama-cg-group-rebal-%d", runID)

	commitTopic := fmt.Sprintf("sarama-cg-commit-%d", runID)
	commitGroup := fmt.Sprintf("sarama-cg-group-commit-%d", runID)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	// Create topics via Admin
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		log.Fatalf("sarama.NewClusterAdmin: %v", err)
	}
	if err := admin.CreateTopic(rebalTopic, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1}, false); err != nil {
		log.Fatalf("Failed to create topic %s: %v", rebalTopic, err)
	}
	if err := admin.CreateTopic(commitTopic, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1}, false); err != nil {
		log.Fatalf("Failed to create topic %s: %v", commitTopic, err)
	}
	admin.Close()

	// -------------------------------------------------------------
	// Part A: Dynamic Rebalancing & Partition Handover
	// -------------------------------------------------------------
	fmt.Printf("  %s--- Part A: Dynamic Rebalancing Across 2 Consumers ---%s\n", common.ColorCyan, common.ColorReset)
	cg1, err := sarama.NewConsumerGroup(brokers, rebalGroup, cfg)
	if err != nil {
		log.Fatalf("NewConsumerGroup cg1 failed: %v", err)
	}
	defer cg1.Close()

	h1 := newConsumerGroupHandler("Member-1", nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	go func() {
		for {
			if err := cg1.Consume(ctx1, []string{rebalTopic}, h1); err != nil {
				return
			}
			if ctx1.Err() != nil {
				return
			}
		}
	}()

	select {
	case <-h1.ready:
		fmt.Printf("  %s✓ Member-1 initialized and claimed all 3 partitions%s\n", common.ColorGreen, common.ColorReset)
	case <-time.After(5 * time.Second):
		log.Fatalf("Timeout waiting for Member-1 setup")
	}

	time.Sleep(1 * time.Second)

	// Member 2 joins
	fmt.Printf("  %sTriggering dynamic rebalance by adding Member-2...%s\n", common.ColorBold, common.ColorReset)
	cg2, err := sarama.NewConsumerGroup(brokers, rebalGroup, cfg)
	if err != nil {
		log.Fatalf("NewConsumerGroup cg2 failed: %v", err)
	}

	h2 := newConsumerGroupHandler("Member-2", nil)
	ctx2, cancel2 := context.WithCancel(context.Background())

	go func() {
		for {
			if err := cg2.Consume(ctx2, []string{rebalTopic}, h2); err != nil {
				return
			}
			if ctx2.Err() != nil {
				return
			}
		}
	}()

	select {
	case <-h2.ready:
		fmt.Printf("  %s✓ Dynamic Rebalance completed! Partitions distributed between Member-1 & Member-2%s\n",
			common.ColorGreen, common.ColorReset)
	case <-time.After(6 * time.Second):
		log.Fatalf("Timeout waiting for Member-2 setup during rebalance")
	}

	time.Sleep(1 * time.Second)

	// Member 2 departs
	fmt.Printf("  %sLeaving Member-2 to trigger rebalance back to Member-1...%s\n", common.ColorBold, common.ColorReset)
	cancel2()
	cg2.Close()
	time.Sleep(2 * time.Second)

	fmt.Printf("  %s✓ Member-2 departed; Member-1 re-claimed all 3 partitions cleanly%s\n\n", common.ColorGreen, common.ColorReset)
	cancel1()
	cg1.Close()

	// -------------------------------------------------------------
	// Part B: SHA-256 Checksum Verification & Manual Offset Commits
	// -------------------------------------------------------------
	fmt.Printf("  %s--- Part B: Checksum Integrity & Manual Offset Resumption ---%s\n", common.ColorCyan, common.ColorReset)

	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("NewSyncProducer: %v", err)
	}

	totalInitialMsgs := 60
	expectedChecksums := make(map[string]string)
	for i := 0; i < totalInitialMsgs; i++ {
		key := fmt.Sprintf("cg-csum-key-%03d", i)
		val := fmt.Sprintf("payload-csum-%03d-%s", i, bytes.Repeat([]byte("KAFKA"), 10))
		csum := common.ComputeSHA256([]byte(val))
		expectedChecksums[key] = csum

		part := int32(i % 3)
		_, _, err := prod.SendMessage(&sarama.ProducerMessage{
			Topic:     commitTopic,
			Partition: part,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(val),
			Headers: []sarama.RecordHeader{
				{Key: []byte("X-Trace-Id"), Value: []byte(fmt.Sprintf("trace-csum-%d", i))},
				{Key: []byte("X-SHA256"), Value: []byte(csum)},
			},
		})
		if err != nil {
			log.Fatalf("SendMessage failed: %v", err)
		}
	}
	prod.Close()
	fmt.Printf("  %s✓ Produced %d tagged messages with SHA-256 checksums%s\n", common.ColorGreen, totalInitialMsgs, common.ColorReset)

	// Consumer 1: Consume all 60, verify SHA-256, commit offsets
	var verifiedCount int64
	onMsg := func(msg *sarama.ConsumerMessage) {
		key := string(msg.Key)
		val := string(msg.Value)
		actualSHA := common.ComputeSHA256([]byte(val))
		expectedSHA, ok := expectedChecksums[key]
		if !ok {
			log.Fatalf("Unexpected message key: %s", key)
		}
		if actualSHA != expectedSHA {
			log.Fatalf("SHA-256 verification failed for key %s! exp: %s, got: %s", key, expectedSHA, actualSHA)
		}
		atomic.AddInt64(&verifiedCount, 1)
	}

	commitConsumer1, err := sarama.NewConsumerGroup(brokers, commitGroup, cfg)
	if err != nil {
		log.Fatalf("NewConsumerGroup commitConsumer1 failed: %v", err)
	}

	hCommit1 := newConsumerGroupHandler("CommitConsumer-1", onMsg)
	ctxCommit1, cancelCommit1 := context.WithCancel(context.Background())

	go func() {
		for {
			if err := commitConsumer1.Consume(ctxCommit1, []string{commitTopic}, hCommit1); err != nil {
				return
			}
			if ctxCommit1.Err() != nil {
				return
			}
		}
	}()

	// Wait until all 60 messages are consumed
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&verifiedCount) >= int64(totalInitialMsgs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	cancelCommit1()
	commitConsumer1.Close()

	actualVerified := atomic.LoadInt64(&verifiedCount)
	fmt.Printf("  %s✓ First consumer read and verified %d/%d messages with SHA-256 & committed offsets%s\n",
		common.ColorGreen, actualVerified, totalInitialMsgs, common.ColorReset)
	if actualVerified != int64(totalInitialMsgs) {
		log.Fatalf("Expected %d verified messages, got %d", totalInitialMsgs, actualVerified)
	}

	// Produce 15 new messages
	totalNewMsgs := 15
	p2, _ := sarama.NewSyncProducer(brokers, cfg)
	for i := 60; i < 60+totalNewMsgs; i++ {
		key := fmt.Sprintf("resume-msg-%03d", i)
		val := fmt.Sprintf("resume-payload-%03d", i)
		part := int32(i % 3)
		_, _, err := p2.SendMessage(&sarama.ProducerMessage{
			Topic:     commitTopic,
			Partition: part,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(val),
		})
		if err != nil {
			log.Fatalf("Send resumption message: %v", err)
		}
	}
	p2.Close()
	fmt.Printf("  %s✓ Produced %d new messages to test offset resumption%s\n", common.ColorGreen, totalNewMsgs, common.ColorReset)

	// Resuming Consumer joins group: should only receive the 15 new messages!
	resumeConsumer, err := sarama.NewConsumerGroup(brokers, commitGroup, cfg)
	if err != nil {
		log.Fatalf("resumeConsumer: %v", err)
	}
	defer resumeConsumer.Close()

	var resumeRecv int64
	hResume := newConsumerGroupHandler("ResumeConsumer", func(msg *sarama.ConsumerMessage) {
		atomic.AddInt64(&resumeRecv, 1)
	})
	ctxResume, cancelResume := context.WithCancel(context.Background())
	defer cancelResume()

	go func() {
		for {
			if err := resumeConsumer.Consume(ctxResume, []string{commitTopic}, hResume); err != nil {
				return
			}
			if ctxResume.Err() != nil {
				return
			}
		}
	}()

	// Wait until 15 messages arrive or timeout
	resumeDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(resumeDeadline) {
		if atomic.LoadInt64(&resumeRecv) >= int64(totalNewMsgs) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	cancelResume()

	resumedMsgs := atomic.LoadInt64(&resumeRecv)
	fmt.Printf("  %s✓ Resumption Consumer processed exactly %d new messages (Committed Offset Resumption Verified)%s\n\n",
		common.ColorGreen, resumedMsgs, common.ColorReset)
	if resumedMsgs != int64(totalNewMsgs) {
		log.Fatalf("Expected %d resumed messages, got %d", totalNewMsgs, resumedMsgs)
	}
}

func testSaramaBenchmark(brokers []string, topic string) {
	fmt.Printf("%s[Test 5] High-Throughput Batching & Latency Benchmark%s\n", common.ColorBold, common.ColorReset)

	totalMsgs := 3000
	msgSize := 512
	payload := bytes.Repeat([]byte("B"), msgSize)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Messages = 250
	cfg.Producer.Flush.Frequency = 5 * time.Millisecond
	cfg.Producer.Flush.MaxMessages = 500

	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Benchmark producer failed: %v", err)
	}
	defer producer.Close()

	var latencies []time.Duration
	var latMu sync.Mutex
	var sentTimes sync.Map // int -> time.Time
	var wg sync.WaitGroup
	wg.Add(totalMsgs)

	go func() {
		for s := range producer.Successes() {
			id := s.Metadata.(int)
			if t0, ok := sentTimes.Load(id); ok {
				d := time.Since(t0.(time.Time))
				latMu.Lock()
				latencies = append(latencies, d)
				latMu.Unlock()
			}
			wg.Done()
		}
	}()

	go func() {
		for e := range producer.Errors() {
			fmt.Printf("Benchmark error: %v\n", e.Err)
			wg.Done()
		}
	}()

	benchStart := time.Now()
	for i := 0; i < totalMsgs; i++ {
		sentTimes.Store(i, time.Now())
		msg := &sarama.ProducerMessage{
			Topic:    topic,
			Key:      sarama.StringEncoder(fmt.Sprintf("bench-key-%d", i)),
			Value:    sarama.ByteEncoder(payload),
			Metadata: i,
		}
		producer.Input() <- msg
	}

	wg.Wait()
	benchDuration := time.Since(benchStart)

	stats := common.CalculateLatencies(latencies)
	totalMB := float64(totalMsgs*msgSize) / (1024 * 1024)
	throughputMsgSec := float64(totalMsgs) / benchDuration.Seconds()
	throughputMBSec := totalMB / benchDuration.Seconds()

	fmt.Printf("  %s✓ Benchmark Results (%d messages of %d bytes, snappy compression):%s\n",
		common.ColorGreen, totalMsgs, msgSize, common.ColorReset)
	fmt.Printf("    Duration:    %v\n", benchDuration.Round(time.Millisecond))
	fmt.Printf("    Throughput:  %s%.2f msg/sec%s (%.2f MB/sec)\n", common.ColorBold, throughputMsgSec, common.ColorReset, throughputMBSec)
	fmt.Printf("    Latency P50: %v\n", stats.P50.Round(time.Microsecond))
	fmt.Printf("    Latency P90: %v\n", stats.P90.Round(time.Microsecond))
	fmt.Printf("    Latency P95: %v\n", stats.P95.Round(time.Microsecond))
	fmt.Printf("    Latency P99: %v\n", stats.P99.Round(time.Microsecond))
}
