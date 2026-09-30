package org.gradientgeeks.aerostream.protocol;

import java.util.Objects;

/**
 * Request data structure for Command 2 (FETCH) and Command 4 (FETCH_MULTI).
 */
public final class FetchRequest {

    private final String topic;
    private final int partition;
    private final long startOffset;
    private final int maxBytes;
    private final int maxWaitMs;

    public FetchRequest(String topic, int partition, long startOffset, int maxBytes, int maxWaitMs) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.startOffset = startOffset;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
    }

    public FetchRequest(String topic, int partition, long startOffset, int maxBytes) {
        this(topic, partition, startOffset, maxBytes, 0);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long startOffset() {
        return startOffset;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public int maxWaitMs() {
        return maxWaitMs;
    }

    @Override
    public String toString() {
        return "FetchRequest{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", startOffset=" + startOffset +
                ", maxBytes=" + maxBytes +
                ", maxWaitMs=" + maxWaitMs +
                '}';
    }
}
