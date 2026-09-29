package io.aerostream.client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scenario 2: JavaConsumerTest
 * Validates consumer groups, ConsumerRebalanceListener, dynamic partition assignment,
 * manual offset commits (commitSync / commitAsync), and seek operations (beginning, end, explicit).
 */
public class JavaConsumerTest {
    private static final Logger log = LoggerFactory.getLogger(JavaConsumerTest.class);

    public static List<TestResult> runAll(String bootstrapServers, String baseTopic) {
        List<TestResult> results = new ArrayList<>();
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-rebalance", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-commits", 3);
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-seeks", 3);

        results.add(testConsumerGroupDynamicRebalance(bootstrapServers, baseTopic + "-rebalance"));
        results.add(testManualOffsetCommitsSyncAndAsync(bootstrapServers, baseTopic + "-commits"));
        results.add(testSeekBeginningEndAndExplicitOffset(bootstrapServers, baseTopic + "-seeks"));
        return results;
    }

    /**
     * CONS-01: Consumer Group Dynamic Rebalance with ConsumerRebalanceListener across 2 consumers.
     */
    public static TestResult testConsumerGroupDynamicRebalance(String bootstrapServers, String topic) {
        String testId = "CONS-01";
        String name = "Consumer Group Dynamic Rebalance & Assignment";
        long start = System.currentTimeMillis();
        String groupId = "grp-rebal-" + UUID.randomUUID();

        // Seed messages across 3 partitions
        try {
            seedTopic(bootstrapServers, topic, 3, 30);
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Failed seeding topic: " + e.getMessage());
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<TopicPartition> c1Assigned = Collections.synchronizedList(new ArrayList<>());
        List<TopicPartition> c2Assigned = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean c1Revoked = new AtomicBoolean(false);

        try (KafkaConsumer<String, String> c1 = new KafkaConsumer<>(props);
             KafkaConsumer<String, String> c2 = new KafkaConsumer<>(props)) {

            c1.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    c1Revoked.set(true);
                    c1Assigned.removeAll(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    c1Assigned.clear();
                    c1Assigned.addAll(partitions);
                }
            });

            // Poll c1 until it gets partitions
            long deadline = System.currentTimeMillis() + 10000;
            while (System.currentTimeMillis() < deadline && c1Assigned.isEmpty()) {
                c1.poll(Duration.ofMillis(300));
            }

            if (c1Assigned.isEmpty()) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Consumer 1 was not assigned partitions");
            }

            int initialC1Count = c1Assigned.size();

            // Start Consumer 2 to trigger rebalance
            c2.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    c2Assigned.removeAll(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    c2Assigned.clear();
                    c2Assigned.addAll(partitions);
                }
            });

            deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && c2Assigned.isEmpty()) {
                c1.poll(Duration.ofMillis(200));
                c2.poll(Duration.ofMillis(200));
            }

            if (c2Assigned.isEmpty()) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Consumer 2 was not assigned partitions after rebalance");
            }

            // Both consumers must have distinct non-empty partition assignments
            Set<TopicPartition> allAssigned = new HashSet<>(c1Assigned);
            for (TopicPartition p : c2Assigned) {
                if (allAssigned.contains(p)) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Partition overlap detected: " + p);
                }
                allAssigned.add(p);
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    String.format("Rebalance successful. C1 partitions: %s, C2 partitions: %s (Total: %d)",
                            c1Assigned, c2Assigned, allAssigned.size()));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * CONS-02: Manual Offset Commits (commitSync & commitAsync) and Offset Resumption.
     */
    public static TestResult testManualOffsetCommitsSyncAndAsync(String bootstrapServers, String topic) {
        String testId = "CONS-02";
        String name = "Manual Offset Commits (commitSync & commitAsync)";
        long start = System.currentTimeMillis();
        String groupId = "grp-commit-" + UUID.randomUUID();

        try {
            seedTopic(bootstrapServers, topic, 1, 20);
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Failed seeding topic: " + e.getMessage());
        }

        TopicPartition tp = new TopicPartition(topic, 0);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try {
            // Part 1: commitSync to offset 5
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.assign(Collections.singletonList(tp));
                Map<TopicPartition, OffsetAndMetadata> syncMap = Collections.singletonMap(tp, new OffsetAndMetadata(5L, "sync-meta"));
                consumer.commitSync(syncMap);

                OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp)).get(tp);
                if (committed == null || committed.offset() != 5L) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                            "commitSync mismatch: expected offset 5, got " + (committed != null ? committed.offset() : "null"));
                }
            }

            // Part 2: commitAsync to offset 10 with callback verification
            CountDownLatch asyncLatch = new CountDownLatch(1);
            AtomicBoolean asyncSuccess = new AtomicBoolean(false);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.assign(Collections.singletonList(tp));
                Map<TopicPartition, OffsetAndMetadata> asyncMap = Collections.singletonMap(tp, new OffsetAndMetadata(10L, "async-meta"));
                consumer.commitAsync(asyncMap, (offsets, exception) -> {
                    if (exception == null && offsets.containsKey(tp) && offsets.get(tp).offset() == 10L) {
                        asyncSuccess.set(true);
                    }
                    asyncLatch.countDown();
                });

                // Poll to drive consumer network I/O for async commit
                long deadline = System.currentTimeMillis() + 8000;
                while (System.currentTimeMillis() < deadline && asyncLatch.getCount() > 0) {
                    consumer.poll(Duration.ofMillis(100));
                }
            }

            if (!asyncSuccess.get()) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start, "commitAsync failed or timed out");
            }

            // Part 3: Verify offset resumption from 10 with a new consumer instance
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                OffsetAndMetadata committed = consumer.committed(Collections.singleton(tp)).get(tp);
                if (committed == null || committed.offset() != 10L) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                            "Offset resumption mismatch: expected offset 10, got " + (committed != null ? committed.offset() : "null"));
                }
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    "commitSync(offset 5) and commitAsync(offset 10) verified with callback and resumption");
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    /**
     * CONS-03: Seek to Beginning, Seek to End, and Seek to Explicit Offset.
     */
    public static TestResult testSeekBeginningEndAndExplicitOffset(String bootstrapServers, String topic) {
        String testId = "CONS-03";
        String name = "Seek to Beginning, End, and Explicit Offset";
        long start = System.currentTimeMillis();
        int totalMessages = 25;

        try {
            seedTopic(bootstrapServers, topic, 1, totalMessages);
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, "Failed seeding topic: " + e.getMessage());
        }

        TopicPartition tp = new TopicPartition(topic, 0);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seek-grp-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(Collections.singletonList(tp));

            // 1. Seek to Beginning
            consumer.seekToBeginning(Collections.singletonList(tp));
            long posBeginning = consumer.position(tp);
            if (posBeginning != 0) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "seekToBeginning failed: position is " + posBeginning + ", expected 0");
            }

            ConsumerRecords<String, String> recsFrom0 = consumer.poll(Duration.ofSeconds(2));
            if (recsFrom0.isEmpty() || recsFrom0.iterator().next().offset() != 0) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "First record after seekToBeginning did not have offset 0");
            }

            // 2. Seek to End
            consumer.seekToEnd(Collections.singletonList(tp));
            long posEnd = consumer.position(tp);
            if (posEnd < totalMessages) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "seekToEnd failed: position is " + posEnd + ", expected >= " + totalMessages);
            }

            // 3. Seek to Explicit Offset (e.g. offset 15)
            long targetOffset = 15L;
            consumer.seek(tp, targetOffset);
            long posExplicit = consumer.position(tp);
            if (posExplicit != targetOffset) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "seek to explicit offset failed: position is " + posExplicit + ", expected " + targetOffset);
            }

            ConsumerRecords<String, String> recsFrom15 = consumer.poll(Duration.ofSeconds(2));
            if (recsFrom15.isEmpty() || recsFrom15.iterator().next().offset() != targetOffset) {
                long actualOffset = recsFrom15.isEmpty() ? -1 : recsFrom15.iterator().next().offset();
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "First record after seek(15) had offset " + actualOffset + ", expected 15");
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    String.format("seekToBeginning (pos=%d), seekToEnd (pos=%d), seekExplicit (pos=%d) verified",
                            posBeginning, posEnd, posExplicit));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }

    private static void seedTopic(String bootstrapServers, String topic, int partitions, int totalMessages) throws Exception {
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "1");
        prodProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < totalMessages; i++) {
                int part = i % partitions;
                producer.send(new ProducerRecord<>(topic, part, "seed-key-" + i, "seed-val-" + i));
            }
            producer.flush();
        }
    }
}
