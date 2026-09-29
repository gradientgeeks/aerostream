package io.aerostream.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scenario 4: JavaIdempotenceTest
 * Validates enable.idempotence=true, strict sequence numbers, retries, and deduplication guarantees.
 */
public class JavaIdempotenceTest {
    private static final Logger log = LoggerFactory.getLogger(JavaIdempotenceTest.class);

    public static List<TestResult> runAll(String bootstrapServers, String baseTopic) {
        List<TestResult> results = new ArrayList<>();
        TopicHelper.ensureTopic(bootstrapServers, baseTopic + "-idempotence", 3);
        results.add(testIdempotentProducerWithRetries(bootstrapServers, baseTopic + "-idempotence"));
        return results;
    }

    /**
     * IDEMP-01: Idempotent Producer with Retries & Sequence Monotonicity.
     */
    public static TestResult testIdempotentProducerWithRetries(String bootstrapServers, String topic) {
        String testId = "IDEMP-01";
        String name = "Idempotent Producer Retries & Monotonic Sequence";
        long start = System.currentTimeMillis();
        int messageCount = 200;
        TopicPartition tp = new TopicPartition(topic, 0);

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        prodProps.put(ProducerConfig.RETRIES_CONFIG, 10);
        prodProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        List<Long> producedOffsets = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger sendErrors = new AtomicInteger(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < messageCount; i++) {
                final int seq = i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0, "idemp-key", "idemp-val-" + seq);
                producer.send(record, (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending idempotent record {}", seq, exception);
                        sendErrors.incrementAndGet();
                    } else {
                        producedOffsets.add(meta.offset());
                    }
                    latch.countDown();
                });
            }
            producer.flush();

            boolean done = latch.await(10, TimeUnit.SECONDS);
            if (!done || sendErrors.get() > 0) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Failed producing idempotent records. Errors: " + sendErrors.get());
            }

            // Verify monotonically increasing offsets
            Collections.sort(producedOffsets);
            if (producedOffsets.size() != messageCount) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Expected " + messageCount + " offsets, recorded: " + producedOffsets.size());
            }

            for (int i = 1; i < producedOffsets.size(); i++) {
                if (producedOffsets.get(i) <= producedOffsets.get(i - 1)) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                            "Non-monotonic offset progression detected at index " + i + ": "
                                    + producedOffsets.get(i - 1) + " -> " + producedOffsets.get(i));
                }
            }

            // Verify with consumer that all sequence numbers are present in strict ascending order without duplicates
            Properties consProps = new Properties();
            consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "idemp-verifier-" + UUID.randomUUID());
            consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            List<Integer> receivedSeqs = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seek(tp, producedOffsets.get(0));

                long deadline = System.currentTimeMillis() + 8000;
                while (System.currentTimeMillis() < deadline && receivedSeqs.size() < messageCount) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, String> rec : records) {
                        String v = rec.value();
                        if (v != null && v.startsWith("idemp-val-")) {
                            receivedSeqs.add(Integer.parseInt(v.substring(10)));
                        }
                    }
                }
            }

            if (receivedSeqs.size() != messageCount) {
                return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                        "Consumer received " + receivedSeqs.size() + " messages, expected " + messageCount);
            }

            for (int i = 0; i < messageCount; i++) {
                if (receivedSeqs.get(i) != i) {
                    return TestResult.fail(testId, name, System.currentTimeMillis() - start,
                            "Sequence mismatch at index " + i + ": expected " + i + ", got " + receivedSeqs.get(i));
                }
            }

            return TestResult.pass(testId, name, System.currentTimeMillis() - start,
                    String.format("Strict sequence 0..%d verified without duplicates or out-of-order deliveries. Monotonic offsets: %d..%d",
                            messageCount - 1, producedOffsets.get(0), producedOffsets.get(producedOffsets.size() - 1)));
        } catch (Exception e) {
            return TestResult.fail(testId, name, System.currentTimeMillis() - start, e.getMessage());
        }
    }
}
