package io.aerostream.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scenario 1: JavaProducerTest
 * Validates high-throughput batching, headers, snappy/gzip/lz4/zstd compression,
 * key-based partition routing, and acks=all guarantees.
 */
public class JavaProducerTest {
    private static final Logger log = LoggerFactory.getLogger(JavaProducerTest.class);

    public static List<TestResult> runAll(String bootstrapServers, String baseTopic) {
        List<TestResult> results = new ArrayList<>();
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-batch", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-headers", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-comp", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-routing", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-acks", 3);

        results.add(testHighThroughputBatching(bootstrapServers, baseTopic + "-batch"));
        results.add(testRecordHeaders(bootstrapServers, baseTopic + "-headers"));
        results.add(testCompressionCodecs(bootstrapServers, baseTopic + "-comp"));
        results.add(testPartitionRoutingKeys(bootstrapServers, baseTopic + "-routing"));
        results.add(testAcksAll(bootstrapServers, baseTopic + "-acks"));
        return results;
    }

    /**
     * PROD-01: High Throughput Batching with 1,000 messages and SHA-256 payload integrity check.
     */
    public static TestResult testHighThroughputBatching(String bootstrapServers, String topic) {
        String testId = "PROD-01";
        String name = "High-Throughput Batching & Checksum Integrity";
        long start = System.currentTimeMillis();
        int messageCount = 1000;

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        prodProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        prodProps.put(ProducerConfig.ACKS_CONFIG, "1");

        Map<Integer, String> sentChecksums = new ConcurrentHashMap<>();
        Map<Integer, String> receivedChecksums = new ConcurrentHashMap<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            CountDownLatch latch = new CountDownLatch(messageCount);
            AtomicInteger sendErrors = new AtomicInteger(0);

            long prodStart = System.currentTimeMillis();
            for (int i = 0; i < messageCount; i++) {
                final int idx = i;
                String payload = "payload-data-" + String.format("%06d", i) + "-pad-" + "x".repeat(128);
                String sha = sha256Hex(payload);
                sentChecksums.put(idx, sha);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "k-" + idx, payload);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        sendErrors.incrementAndGet();
                    }
                    latch.countDown();
                });
            }
            producer.flush();
            boolean completed = latch.await(15, TimeUnit.SECONDS);
            long prodDur = Math.max(1, System.currentTimeMillis() - prodStart);
            double throughputMsgSec = (messageCount * 1000.0) / prodDur;

            if (!completed || sendErrors.get() > 0) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Producer failed to send all records. Errors: " + sendErrors.get());
            }

            // Verify with consumer
            Properties consProps = new Properties();
            consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID());
            consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
                consumer.subscribe(Collections.singletonList(topic));
                long deadline = System.currentTimeMillis() + 15000;
                while (System.currentTimeMillis() < deadline && receivedChecksums.size() < messageCount) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for (ConsumerRecord<String, String> rec : records) {
                        String val = rec.value();
                        if (val != null && val.startsWith("payload-data-")) {
                            int id = Integer.parseInt(val.substring(13, 19));
                            receivedChecksums.put(id, sha256Hex(val));
                        }
                    }
                }
            }

            int mismatches = 0;
            for (int i = 0; i < messageCount; i++) {
                String sent = sentChecksums.get(i);
                String rec = receivedChecksums.get(i);
                if (rec == null || !sent.equals(rec)) {
                    mismatches++;
                }
            }

            if (mismatches > 0) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Checksum mismatches: " + mismatches + " / " + messageCount);
            }

            long totalLat = System.currentTimeMillis() - start;
            return TestResult.pass(testId, name, totalLat, throughputMsgSec,
                    String.format("Verified %d messages, 0 mismatches, Throughput: %.1f msg/sec", messageCount, throughputMsgSec));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * PROD-02: Record Headers Verification.
     */
    public static TestResult testRecordHeaders(String bootstrapServers, String topic) {
        String testId = "PROD-02";
        String name = "Record Headers Propagation";
        long start = System.currentTimeMillis();

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String traceId = "trace-uuid-" + UUID.randomUUID();
        String sourceApp = "aerostream-java-client";
        String schemaVer = "v2.1.0";

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, "hdr-key", "hdr-value");
            record.headers().add(new RecordHeader("X-Trace-Id", traceId.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Source", sourceApp.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Schema-Version", schemaVer.getBytes(StandardCharsets.UTF_8)));

            producer.send(record).get(5, TimeUnit.SECONDS);
            producer.flush();

            Properties consProps = new Properties();
            consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "hdr-group-" + UUID.randomUUID());
            consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            boolean verified = false;
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
                TopicPartition tp = new TopicPartition(topic, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                long deadline = System.currentTimeMillis() + 8000;
                while (System.currentTimeMillis() < deadline && !verified) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for (ConsumerRecord<String, String> rec : records) {
                        if ("hdr-key".equals(rec.key()) && "hdr-value".equals(rec.value())) {
                            Map<String, String> readHeaders = new HashMap<>();
                            for (Header h : rec.headers()) {
                                readHeaders.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                            }
                            if (traceId.equals(readHeaders.get("X-Trace-Id")) &&
                                sourceApp.equals(readHeaders.get("X-Source")) &&
                                schemaVer.equals(readHeaders.get("X-Schema-Version"))) {
                                verified = true;
                                break;
                            }
                        }
                    }
                }
            }

            if (!verified) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Header values not found or mismatched");
            }
            return TestResult.pass(testId, name, System.currentTimeMillis() - start, "Headers X-Trace-Id, X-Source, X-Schema-Version verified");
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * PROD-03: Compression Codecs (none, snappy, gzip, lz4, zstd).
     */
    public static TestResult testCompressionCodecs(String bootstrapServers, String topic) {
        String testId = "PROD-03";
        String name = "Compression Codecs (snappy, gzip, lz4, zstd)";
        long start = System.currentTimeMillis();
        String[] codecs = new String[]{"none", "snappy", "gzip", "lz4", "zstd"};
        Map<String, Long> codecLatencies = new HashMap<>();

        try {
            for (String codec : codecs) {
                long cStart = System.currentTimeMillis();
                Properties prodProps = new Properties();
                prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                prodProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
                prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                String payload = "compression-test-" + codec + "-data-" + "repeat-content-".repeat(30);
                String key = "comp-key-" + codec;

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
                    producer.send(new ProducerRecord<>(topic, 0, key, payload)).get(5, TimeUnit.SECONDS);
                    producer.flush();
                }

                // Verify consumption
                Properties consProps = new Properties();
                consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "comp-grp-" + codec + "-" + UUID.randomUUID());
                consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                boolean found = false;
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
                    TopicPartition tp = new TopicPartition(topic, 0);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));

                    long deadline = System.currentTimeMillis() + 6000;
                    while (System.currentTimeMillis() < deadline && !found) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                        for (ConsumerRecord<String, String> rec : records) {
                            if (key.equals(rec.key()) && payload.equals(rec.value())) {
                                found = true;
                                break;
                            }
                        }
                    }
                }

                if (!found) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Failed to read compressed message for: " + codec);
                }
                codecLatencies.put(codec, System.currentTimeMillis() - cStart);
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    "All 5 codecs verified: " + codecLatencies.toString());
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * PROD-04: Partition Routing Keys across multi-partition topic.
     */
    public static TestResult testPartitionRoutingKeys(String bootstrapServers, String topic) {
        String testId = "PROD-04";
        String name = "Partition Routing Keys Determinism";
        long start = System.currentTimeMillis();

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        int numKeys = 10;
        int msgsPerKey = 10;
        Map<String, Integer> keyToPartition = new HashMap<>();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int k = 0; k < numKeys; k++) {
                String key = "route-key-" + k;
                for (int m = 0; m < msgsPerKey; m++) {
                    RecordMetadata meta = producer.send(new ProducerRecord<>(topic, key, "val-" + m)).get(5, TimeUnit.SECONDS);
                    int part = meta.partition();
                    if (keyToPartition.containsKey(key)) {
                        int expected = keyToPartition.get(key);
                        if (expected != part) {
                            return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                                    "Inconsistent partition for key " + key + ": expected " + expected + ", got " + part);
                        }
                    } else {
                        keyToPartition.put(key, part);
                    }
                }
            }
            producer.flush();

            Set<Integer> uniquePartitions = new HashSet<>(keyToPartition.values());
            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    String.format("%d keys consistently routed across %d distinct partitions %s",
                            numKeys, uniquePartitions.size(), uniquePartitions));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * PROD-05: acks=all Strong Durability Guarantee.
     */
    public static TestResult testAcksAll(String bootstrapServers, String topic) {
        String testId = "PROD-05";
        String name = "Acks=All Strong Durability Guarantee";
        long start = System.currentTimeMillis();

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            int count = 50;
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic, "acks-key-" + i, "acks-val-" + i)));
            }
            producer.flush();

            for (Future<RecordMetadata> f : futures) {
                RecordMetadata rm = f.get(5, TimeUnit.SECONDS);
                if (rm.offset() < 0 || rm.partition() < 0) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Invalid metadata returned for acks=all");
                }
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    "Acknowledged " + count + " records with acks=all");
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
