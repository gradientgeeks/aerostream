package io.aerostream.streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * AeroStream Kafka Streams E2E Topology Application & Test Suite.
 *
 * Topology:
 *   Read stream -> filter non-empty -> transform values (uppercase) ->
 *   groupByKey -> aggregate/count occurrences -> write to output topic.
 */
public class StreamsApp {
    private static final Logger log = LoggerFactory.getLogger(StreamsApp.class);

    public static final String DEFAULT_INPUT_TOPIC = "streams-input-topic";
    public static final String DEFAULT_OUTPUT_TOPIC = "streams-output-topic";
    public static final String STORE_NAME = "streams-count-store";

    /**
     * Builds the required Kafka Streams topology.
     */
    public static Topology buildTopology(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();

        // 1. Read stream from input topic
        KStream<String, String> sourceStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. Filter non-empty values
        // 3. Transform values (e.g. uppercase)
        // 4. groupByKey
        // 5. aggregate / count occurrences
        KTable<String, Long> countTable = sourceStream
                .filter((key, value) -> {
                    boolean keep = value != null && !value.trim().isEmpty();
                    if (!keep) {
                        log.info("[Topology Filter] Dropping empty/blank record with key='{}'", key);
                    }
                    return keep;
                })
                .mapValues((key, value) -> {
                    String upper = value.toUpperCase(Locale.ROOT);
                    log.info("[Topology Transform] Value '{}' -> '{}' for key='{}'", value, upper, key);
                    return upper;
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // 6. Write aggregated counts to output topic
        countTable.toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    /**
     * Creates StreamsConfig properties tuned for fast, deterministic E2E verification.
     */
    public static Properties getStreamsConfig(String bootstrapServers, String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // Push updates immediately
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Helper to parse count value whether serialized as 8-byte big-endian Long or UTF-8 String.
     */
    public static long parseCountValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return 0L;
        }
        if (raw.length == 8) {
            return ByteBuffer.wrap(raw).getLong();
        }
        String str = new String(raw, StandardCharsets.UTF_8).trim();
        return Long.parseLong(str);
    }

    /**
     * Ensure topics exist using REST API and AdminClient.
     */
    public static void ensureTopics(String bootstrapServers, String httpUrl, String... topics) {
        for (String topic : topics) {
            // First attempt REST API topic creation
            try {
                URL url = new URL(httpUrl + "/api/topics");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);
                conn.setConnectTimeout(3000);
                conn.setReadTimeout(3000);

                String json = String.format("{\"name\":\"%s\",\"partitions\":1,\"replication_factor\":1}", topic);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(json.getBytes(StandardCharsets.UTF_8));
                }
                int code = conn.getResponseCode();
                conn.disconnect();
            } catch (Exception e) {
                log.warn("REST topic creation fallback for {}: {}", topic, e.getMessage());
            }
        }

        // Secondary verification via AdminClient
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : topics) {
                if (!existing.contains(t)) {
                    toCreate.add(new NewTopic(t, 1, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.info("AdminClient topic provisioning note: {}", e.getMessage());
        }
    }

    /**
     * Resets topics by deleting old instances if present and recreating them fresh.
     */
    public static void resetTopics(String bootstrapServers, String httpUrl, String... topics) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            List<String> toDelete = new ArrayList<>();
            for (String t : topics) {
                if (existing.contains(t)) {
                    toDelete.add(t);
                }
            }
            if (!toDelete.isEmpty()) {
                log.info("Deleting existing topics for clean test run: {}", toDelete);
                try {
                    admin.deleteTopics(toDelete).all().get(5, TimeUnit.SECONDS);
                    Thread.sleep(1200);
                } catch (Exception e) {
                    log.info("Topic deletion note: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.info("AdminClient reset error: {}", e.getMessage());
        }

        ensureTopics(bootstrapServers, httpUrl, topics);
    }

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String restUrl = "http://127.0.0.1:9001";
        String inputTopic = DEFAULT_INPUT_TOPIC;
        String outputTopic = DEFAULT_OUTPUT_TOPIC;
        String appId = "aerostream-streams-" + System.currentTimeMillis();

        for (int i = 0; i < args.length; i++) {
            if ("--bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                bootstrapServers = args[++i];
            } else if ("--rest-url".equals(args[i]) && i + 1 < args.length) {
                restUrl = args[++i];
            } else if ("--input-topic".equals(args[i]) && i + 1 < args.length) {
                inputTopic = args[++i];
            } else if ("--output-topic".equals(args[i]) && i + 1 < args.length) {
                outputTopic = args[++i];
            } else if ("--app-id".equals(args[i]) && i + 1 < args.length) {
                appId = args[++i];
            }
        }

        System.out.println("==========================================================================================");
        System.out.println("         AEROSTREAM KAFKA STREAMS E2E VERIFICATION SUITE                                  ");
        System.out.println("   Bootstrap Servers : " + bootstrapServers);
        System.out.println("   REST API URL      : " + restUrl);
        System.out.println("   Application ID    : " + appId);
        System.out.println("   Input Topic       : " + inputTopic);
        System.out.println("   Output Topic      : " + outputTopic);
        System.out.println("==========================================================================================");

        String changelogTopic = appId + "-" + STORE_NAME + "-changelog";
        // Reset and pre-create topics cleanly
        resetTopics(bootstrapServers, restUrl, inputTopic, outputTopic, changelogTopic);

        // Build topology
        Topology topology = buildTopology(inputTopic, outputTopic);
        System.out.println("\n[TOPOLOGY DESCRIPTION]:\n" + topology.describe());

        Properties streamsConfig = getStreamsConfig(bootstrapServers, appId);
        KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

        CountDownLatch runningLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            System.out.println("[KafkaStreams State] " + oldState + " -> " + newState);
            if (newState == KafkaStreams.State.RUNNING) {
                runningLatch.countDown();
            }
        });

        long startMs = System.currentTimeMillis();
        try {
            System.out.println("\n>>> [1/4] Starting Kafka Streams instance...");
            streams.start();

            boolean isRunning = runningLatch.await(30, TimeUnit.SECONDS);
            if (!isRunning) {
                System.err.println("[WARN] KafkaStreams did not transition to RUNNING within 30s. Current state: " + streams.state());
            } else {
                System.out.println("[SUCCESS] Kafka Streams transitioned to RUNNING state.");
            }

            // Input test dataset
            // Key -> values to produce
            List<Map.Entry<String, String>> testRecords = Arrays.asList(
                    new AbstractMap.SimpleEntry<>("apple", "red apple"),
                    new AbstractMap.SimpleEntry<>("banana", "yellow banana"),
                    new AbstractMap.SimpleEntry<>("apple", "green apple"),
                    new AbstractMap.SimpleEntry<>("orange", "sweet orange"),
                    new AbstractMap.SimpleEntry<>("banana", "ripe banana"),
                    new AbstractMap.SimpleEntry<>("apple", "honeycrisp apple"),
                    new AbstractMap.SimpleEntry<>("empty", ""),            // Should be filtered out
                    new AbstractMap.SimpleEntry<>("whitespace", "   ")      // Should be filtered out
            );

            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("apple", 3L);
            expectedCounts.put("banana", 2L);
            expectedCounts.put("orange", 1L);

            System.out.println("\n>>> [2/4] Producing test records into input topic '" + inputTopic + "'...");
            Properties prodProps = new Properties();
            prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prodProps.put(ProducerConfig.ACKS_CONFIG, "all");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
                List<Future<RecordMetadata>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : testRecords) {
                    ProducerRecord<String, String> rec = new ProducerRecord<>(inputTopic, entry.getKey(), entry.getValue());
                    futures.add(producer.send(rec));
                    System.out.printf("  Produced -> Key: %-12s Value: '%s'%n", entry.getKey(), entry.getValue());
                }
                producer.flush();
                for (Future<RecordMetadata> f : futures) {
                    f.get(5, TimeUnit.SECONDS);
                }
                System.out.println("[SUCCESS] All input records successfully written to " + inputTopic);
            }

            System.out.println("\n>>> [3/4] Consuming and verifying aggregated counts from '" + outputTopic + "'...");
            Properties consProps = new Properties();
            consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-cg-" + System.currentTimeMillis());
            consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

            Map<String, Long> observedCounts = new HashMap<>();
            long deadline = System.currentTimeMillis() + 45000; // 45 seconds timeout

            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consProps)) {
                consumer.subscribe(Collections.singletonList(outputTopic));

                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        long countVal = parseCountValue(record.value());
                        observedCounts.put(record.key(), countVal);
                        System.out.printf("  Received Output -> Key: %-12s Aggregated Count: %d (offset: %d)%n",
                                record.key(), countVal, record.offset());
                    }

                    // Check if expected counts are satisfied
                    boolean complete = true;
                    for (Map.Entry<String, Long> exp : expectedCounts.entrySet()) {
                        Long obs = observedCounts.get(exp.getKey());
                        if (obs == null || obs < exp.getValue()) {
                            complete = false;
                            break;
                        }
                    }
                    if (complete) {
                        System.out.println("[INFO] All expected keys reached target counts!");
                        break;
                    }
                }
            }

            System.out.println("\n>>> [4/4] Evaluating Test Assertions...");
            boolean passed = true;
            for (Map.Entry<String, Long> exp : expectedCounts.entrySet()) {
                Long actual = observedCounts.get(exp.getKey());
                if (actual == null || !actual.equals(exp.getValue())) {
                    System.err.printf("[FAIL] Count mismatch for key '%s': expected %d, got %s%n",
                            exp.getKey(), exp.getValue(), actual);
                    passed = false;
                } else {
                    System.out.printf("[PASS] Key: %-10s Expected: %d | Actual: %d%n",
                            exp.getKey(), exp.getValue(), actual);
                }
            }

            // Verify filtered keys did NOT produce output
            if (observedCounts.containsKey("empty")) {
                System.err.println("[FAIL] Key 'empty' was not filtered out: " + observedCounts.get("empty"));
                passed = false;
            } else {
                System.out.println("[PASS] Filter verified: 'empty' dropped.");
            }

            if (observedCounts.containsKey("whitespace")) {
                System.err.println("[FAIL] Key 'whitespace' was not filtered out: " + observedCounts.get("whitespace"));
                passed = false;
            } else {
                System.out.println("[PASS] Filter verified: 'whitespace' dropped.");
            }

            long elapsed = System.currentTimeMillis() - startMs;
            System.out.println("------------------------------------------------------------------------------------------");
            if (passed) {
                System.out.printf("[TEST SUMMARY] PASS - Kafka Streams topology successfully verified against AeroStream in %d ms.%n", elapsed);
                System.out.println("------------------------------------------------------------------------------------------");
                System.exit(0);
            } else {
                System.err.printf("[TEST SUMMARY] FAIL - Topology assertions failed after %d ms.%n", elapsed);
                System.out.println("------------------------------------------------------------------------------------------");
                System.exit(1);
            }

        } catch (Exception e) {
            System.err.println("[ERROR] Kafka Streams execution failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            System.out.println("Closing Kafka Streams application...");
            streams.close(Duration.ofSeconds(5));
            streams.cleanUp();
        }
    }
}
