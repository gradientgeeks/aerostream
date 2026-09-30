package org.gradientgeeks.aerostream.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Command 1 (PRODUCE) request data structure.
 */
public final class ProduceRequest {

    private final String topic;
    private final int partition;
    private final byte[] payload;

    public ProduceRequest(String topic, int partition, byte[] payload) {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partition = partition;
        this.payload = payload != null ? payload.clone() : new byte[0];
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProduceRequest that)) return false;
        return partition == that.partition
                && Objects.equals(topic, that.topic)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(topic, partition);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "ProduceRequest{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", payloadLen=" + payload.length +
                '}';
    }
}
