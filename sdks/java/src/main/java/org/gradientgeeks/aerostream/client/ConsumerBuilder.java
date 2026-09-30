package org.gradientgeeks.aerostream.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Fluent builder for {@link AeroConsumer}.
 */
public class ConsumerBuilder {

    private final AeroClient client;
    private String topic;
    private int partition = 0;
    private long initialOffset = 0;
    private int maxBytes = 1048576; // 1 MB
    private Duration maxWait = Duration.ofMillis(100);

    public ConsumerBuilder(AeroClient client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    public ConsumerBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerBuilder partition(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("Partition must not be negative");
        }
        this.partition = partition;
        return this;
    }

    public ConsumerBuilder initialOffset(long initialOffset) {
        if (initialOffset < 0) {
            throw new IllegalArgumentException("Initial offset must not be negative");
        }
        this.initialOffset = initialOffset;
        return this;
    }

    public ConsumerBuilder maxBytes(int maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        this.maxBytes = maxBytes;
        return this;
    }

    public ConsumerBuilder maxWait(Duration maxWait) {
        this.maxWait = Objects.requireNonNull(maxWait, "maxWait must not be null");
        return this;
    }

    public AeroClient client() {
        return client;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long initialOffset() {
        return initialOffset;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public Duration maxWait() {
        return maxWait;
    }

    public AeroConsumer build() {
        return new AeroConsumer(this);
    }
}
