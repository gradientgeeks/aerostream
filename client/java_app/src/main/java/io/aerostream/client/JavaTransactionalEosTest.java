package io.aerostream.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

/**
 * Scenario 3: JavaTransactionalEosTest
 * Validates KIP-98 transactional lifecycle:
 * initTransactions(), beginTransaction(), commitTransaction(), abortTransaction(),
 * and validates that isolation.level=read_committed skips aborted records while delivering committed records.
 */
public class JavaTransactionalEosTest {
    private static final Logger log = LoggerFactory.getLogger(JavaTransactionalEosTest.class);

    public static List<TestResult> runAll(String bootstrapServers, String baseTopic) {
        List<TestResult> results = new ArrayList<>();
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-txns", 3);
        results.add(testTransactionalLifecycleAndIsolation(bootstrapServers, baseTopic + "-txns"));
        return results;
    }

    /**
     * TXN-01: Full Transactional Lifecycle & Read Committed Isolation.
     */
    public static TestResult testTransactionalLifecycleAndIsolation(String bootstrapServers, String topic) {
        String testId = "TXN-01";
        String name = "KIP-98 Transactional EOS & Read Committed Isolation";
        long start = System.currentTimeMillis();
        String txnId = "txn-" + UUID.randomUUID();
        TopicPartition tp = new TopicPartition(topic, 0);

        String abortedKey = "k-aborted-" + UUID.randomUUID();
        String abortedVal = "aborted-payload-" + UUID.randomUUID();

        String committedKey = "k-committed-" + UUID.randomUUID();
        String committedVal = "committed-payload-" + UUID.randomUUID();

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            // 1. Initialize Transactions
            producer.initTransactions();

            // 2. Transaction 1: Produce and ABORT
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, abortedKey, abortedVal));
            producer.flush();
            producer.abortTransaction();
            log.info("Transaction 1 aborted successfully.");

            // 3. Transaction 2: Produce and COMMIT
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, committedKey, committedVal));
            producer.flush();
            producer.commitTransaction();
            log.info("Transaction 2 committed successfully.");

            // 4. Consumer with read_uncommitted -> can see uncommitted/aborted messages
            Properties uncommittedProps = new Properties();
            uncommittedProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            uncommittedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "uncommitted-grp-" + UUID.randomUUID());
            uncommittedProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            uncommittedProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
            uncommittedProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            uncommittedProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            List<String> uncommittedReceived = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(uncommittedProps)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));
                long deadline = System.currentTimeMillis() + 6000;
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for (ConsumerRecord<String, String> rec : records) {
                        if (rec.value() != null) {
                            uncommittedReceived.add(rec.value());
                        }
                    }
                    if (uncommittedReceived.contains(committedVal)) {
                        break;
                    }
                }
            }

            // 5. Consumer with read_committed -> MUST ONLY see committed, NEVER aborted
            Properties committedProps = new Properties();
            committedProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            committedProps.put(ConsumerConfig.GROUP_ID_CONFIG, "committed-grp-" + UUID.randomUUID());
            committedProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            committedProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            committedProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            committedProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            List<String> committedReceived = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(committedProps)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));
                long deadline = System.currentTimeMillis() + 6000;
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                    for (ConsumerRecord<String, String> rec : records) {
                        if (rec.value() != null) {
                            committedReceived.add(rec.value());
                        }
                    }
                    if (committedReceived.contains(committedVal)) {
                        break;
                    }
                }
            }

            // Verify isolation guarantees
            boolean foundCommittedInReadCommitted = committedReceived.contains(committedVal);
            boolean foundAbortedInReadCommitted = committedReceived.contains(abortedVal);

            if (!foundCommittedInReadCommitted) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Committed message not found by read_committed consumer");
            }

            if (foundAbortedInReadCommitted) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "CRITICAL: Aborted message leaked into read_committed consumer!");
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    String.format("EOS verified: init/begin/commit/abort passed; read_committed received committed message and filtered out aborted message. (Uncommitted total: %d, Committed total: %d)",
                            uncommittedReceived.size(), committedReceived.size()));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }
}
