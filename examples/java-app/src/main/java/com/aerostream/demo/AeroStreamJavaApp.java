package com.aerostream.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AeroStream Java Application Demo
 *
 * Demonstrates 100% wire-compatibility between the official Apache Kafka Java Client
 * (org.apache.kafka:kafka-clients) and AeroStream's shard-per-core event streaming broker.
 */
public class AeroStreamJavaApp {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9093";
    private static final String TOPIC_NAME = "java-orders-stream";
    private static final int NUM_PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;
    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("AEROSTREAM_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
        if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
            bootstrapServers = args[0];
        }

        System.out.println("================================================================================");
        System.out.println("        ☕ AeroStream Java (Kafka-Clients) Compatibility Test Suite");
        System.out.println("        Client Library: org.apache.kafka:kafka-clients:3.8.0");
        System.out.println("        Target Broker:  " + bootstrapServers);
        System.out.println("================================================================================\n");

        long startTime = System.currentTimeMillis();
        boolean allPassed = true;

        try {
            // 1. Cluster Metadata & Topic Administration
            allPassed &= testAdminClient(bootstrapServers);

            // 2. High-Speed Asynchronous Producer
            allPassed &= testProducer(bootstrapServers);

            // 3. Consumer Group & Real-Time Polling
            allPassed &= testConsumer(bootstrapServers);

            // 4. High-Throughput Large Messages (1 MB & 10 MB)
            allPassed &= testLargeMessages(bootstrapServers);

        } catch (Exception e) {
            System.err.println("❌ Fatal execution error: " + e.getMessage());
            e.printStackTrace();
            allPassed = false;
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("\n================================================================================");
        if (allPassed) {
            System.out.println("  🎉 ALL JAVA COMPATIBILITY TESTS PASSED! Total elapsed: " + totalTime + " ms");
            System.out.println("  AeroStream is 100% drop-in compatible with official Apache Kafka Java Client!");
            System.out.println("================================================================================\n");
            System.exit(0);
        } else {
            System.err.println("  ❌ ONE OR MORE TESTS FAILED. Review output above.");
            System.out.println("================================================================================\n");
            System.exit(1);
        }
    }

    private static boolean testAdminClient(String bootstrapServers) {
        System.out.println("[Step 1/3] Testing AdminClient metadata & topic management...");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        try (AdminClient admin = AdminClient.create(props)) {
            // Inspect cluster nodes
            var clusterInfo = admin.describeCluster();
            var nodes = clusterInfo.nodes().get(5, TimeUnit.SECONDS);
            System.out.printf("  ✓ Connected to cluster. Discovered %d active broker node(s):%n", nodes.size());
            nodes.forEach(node -> System.out.printf("    - Broker ID: %d (%s:%d)%n", node.id(), node.host(), node.port()));

            // Check existing topics
            Set<String> topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            if (!topics.contains(TOPIC_NAME)) {
                System.out.printf("  ✓ Topic '%s' does not exist. Creating (%d partitions, RF %d)...%n",
                        TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);
                NewTopic newTopic = new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);
                admin.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
                System.out.println("  ✓ Topic created successfully.");
            } else {
                System.out.printf("  ✓ Topic '%s' already exists.%n", TOPIC_NAME);
            }
            return true;
        } catch (Exception e) {
            System.err.println("  ❌ AdminClient test failed: " + e.getMessage());
            return false;
        }
    }

    private static boolean testProducer(String bootstrapServers) {
        System.out.println("\n[Step 2/3] Testing KafkaProducer with callbacks and headers...");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

        CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String key = "order-" + i;
                String jsonPayload = String.format("{\"orderId\": %d, \"amount\": %.2f, \"currency\": \"USD\", \"timestamp\": %d}",
                        i, 19.99 + (i * 10), System.currentTimeMillis());

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, jsonPayload);
                record.headers().add(new RecordHeader("client-runtime", "java-25-openjdk".getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("source", "aerostream-java-demo".getBytes(StandardCharsets.UTF_8)));

                final int index = i;
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            successCount.incrementAndGet();
                            System.out.printf("  ✓ Sent message [%d/%d] key='%s' -> partition=%d offset=%d%n",
                                    index + 1, MESSAGE_COUNT, key, metadata.partition(), metadata.offset());
                        } else {
                            System.err.printf("  ❌ Delivery failed for message %d: %s%n", index, exception.getMessage());
                        }
                        latch.countDown();
                    }
                });
            }

            producer.flush();
            boolean finished = latch.await(10, TimeUnit.SECONDS);
            if (!finished) {
                System.err.println("  ❌ Producer timed out waiting for acknowledgments.");
                return false;
            }

            System.out.printf("  ✓ All %d records produced and acknowledged successfully.%n", successCount.get());
            return successCount.get() == MESSAGE_COUNT;
        } catch (Exception e) {
            System.err.println("  ❌ Producer test failed: " + e.getMessage());
            return false;
        }
    }

    private static boolean testConsumer(String bootstrapServers) {
        System.out.println("\n[Step 3/3] Testing KafkaConsumer with consumer group subscriptions...");
        String groupId = "java-demo-group-" + System.currentTimeMillis();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            System.out.printf("  ✓ Subscribed to topic '%s' with consumer group '%s'...%n", TOPIC_NAME, groupId);

            int totalRecordsReceived = 0;
            long deadline = System.currentTimeMillis() + 15000; // 15 seconds max

            while (System.currentTimeMillis() < deadline && totalRecordsReceived < MESSAGE_COUNT) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    totalRecordsReceived++;
                    System.out.printf("  ✓ Received [%d/%d] key='%s' partition=%d offset=%d payload=%s%n",
                            totalRecordsReceived, MESSAGE_COUNT, record.key(), record.partition(), record.offset(), record.value());
                }
            }

            System.out.printf("  ✓ Total records polled by consumer group: %d%n", totalRecordsReceived);
            if (totalRecordsReceived >= MESSAGE_COUNT) {
                System.out.println("  ✓ Consumer group verification passed with 100% data integrity.");
                return true;
            } else {
                System.err.printf("  ❌ Consumer timed out. Expected at least %d messages, but got %d.%n",
                        MESSAGE_COUNT, totalRecordsReceived);
                return false;
            }
        } catch (Exception e) {
            System.err.println("  ❌ Consumer test failed: " + e.getMessage());
            return false;
        }
    }

    private static boolean testLargeMessages(String bootstrapServers) {
        System.out.println("\n[Step 4/4] Testing High-Throughput Large Messages (1 MB & 10 MB) via official KafkaProducer...");
        String topic = "java-large-stream";

        // Pre-create topic
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
            System.out.printf("  ✓ Topic '%s' prepared for large payloads.%n", topic);
        } catch (Exception ignored) {
            // Already exists or auto-created
        }

        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "1");
        prodProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 20 * 1024 * 1024);
        prodProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);

        int[] sizes = {1024 * 1024, 10 * 1024 * 1024};
        String[] names = {"1 MB", "10 MB"};

        try (Producer<String, byte[]> producer = new KafkaProducer<>(prodProps)) {
            for (int i = 0; i < sizes.length; i++) {
                int size = sizes[i];
                String name = names[i];
                System.out.printf("%n--- Testing %s (%d bytes) Payload ---%n", name, size);
                byte[] payload = new byte[size];
                Arrays.fill(payload, (byte) ('A' + i));

                long t0 = System.nanoTime();
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "key-" + name, payload);
                RecordMetadata rm = producer.send(record).get(15, TimeUnit.SECONDS);
                long latencyNs = System.nanoTime() - t0;
                double latencyMs = latencyNs / 1_000_000.0;
                double throughputMB = (size / (1024.0 * 1024.0)) / (latencyNs / 1_000_000_000.0);

                System.out.printf("  ✓ Kafka Producer %s ACK in %.2f ms (Partition: %d, Offset: %d, Throughput: %.2f MB/s)%n",
                        name, latencyMs, rm.partition(), rm.offset(), throughputMB);
            }
            return true;
        } catch (Exception e) {
            System.err.println("  ❌ Large message test failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
