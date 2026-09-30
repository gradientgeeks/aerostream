package org.gradientgeeks.aerostream.example;

import org.gradientgeeks.aerostream.client.AeroClient;
import org.gradientgeeks.aerostream.client.AeroConsumer;
import org.gradientgeeks.aerostream.client.AeroProducer;
import org.gradientgeeks.aerostream.common.AeroRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * End-to-end example demonstrating AeroStream Java SDK producer and consumer workflows.
 */
public class ExampleApp {

    private static final Logger log = LoggerFactory.getLogger(ExampleApp.class);

    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : "127.0.0.1:9091";
        String token = args.length > 1 ? args[1] : null;

        log.info("Connecting to AeroStream broker at {}", broker);

        try (AeroClient client = AeroClient.builder()
                .bootstrapServer(broker)
                .token(token)
                .connectTimeout(Duration.ofSeconds(3))
                .socketTimeout(Duration.ofSeconds(10))
                .build()) {

            String topic = "iot-metrics";
            int partition = 0;

            // 1. Synchronous Produce
            try (AeroProducer producer = client.producer()) {
                byte[] payload1 = "{\"sensor_id\":\"temp-1\",\"celsius\":21.8}".getBytes(StandardCharsets.UTF_8);
                long offset1 = producer.send(topic, partition, payload1);
                log.info("Produced synchronously: offset={}", offset1);

                // 2. Asynchronous Produce
                byte[] payload2 = "{\"sensor_id\":\"temp-2\",\"celsius\":22.4}".getBytes(StandardCharsets.UTF_8);
                CompletableFuture<Long> asyncFuture = producer.sendAsync(topic, partition, payload2);
                long offset2 = asyncFuture.get();
                log.info("Produced asynchronously: offset={}", offset2);

                // 3. Batch Produce
                List<byte[]> batch = List.of(
                        "batch-event-1".getBytes(StandardCharsets.UTF_8),
                        "batch-event-2".getBytes(StandardCharsets.UTF_8)
                );
                long[] batchOffsets = producer.sendBatch(topic, partition, batch);
                log.info("Produced batch of {}: offsets={}", batchOffsets.length, batchOffsets);
            }

            // 4. Batch Fetch Consumer
            try (AeroConsumer consumer = client.consumerBuilder()
                    .topic(topic)
                    .partition(partition)
                    .initialOffset(0)
                    .build()) {

                List<AeroRecord> records = consumer.fetch();
                log.info("Fetched {} records:", records.size());
                for (AeroRecord record : records) {
                    log.info("  [offset={}] payload='{}'", record.offset(), record.payloadAsString());
                }

                // 5. Continuous Stream Consumption
                log.info("Streaming up to 5 records:");
                consumer.seek(0);
                consumer.stream()
                        .limit(5)
                        .forEach(r -> log.info("  Streamed: offset={} payload='{}'", r.offset(), r.payloadAsString()));
            }
        }
    }
}
