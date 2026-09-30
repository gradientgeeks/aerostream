package org.gradientgeeks.aerostream.common;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * An immutable message record received from or sent to AeroStream.
 */
public final class AeroRecord {

    private final long offset;
    private final byte[] payload;
    private final String topic;
    private final int partition;
    private final Instant timestamp;

    public AeroRecord(long offset, byte[] payload, String topic, int partition, Instant timestamp) {
        this.offset = offset;
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.topic = topic != null ? topic : "";
        this.partition = partition;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
    }

    public AeroRecord(long offset, byte[] payload, String topic, int partition) {
        this(offset, payload, topic, partition, Instant.now());
    }

    public AeroRecord(long offset, byte[] payload) {
        this(offset, payload, "", 0, Instant.now());
    }

    public long offset() {
        return offset;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public byte[] getPayload() {
        return payload.clone();
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    public String topic() {
        return topic;
    }

    public String getTopic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public int getPartition() {
        return partition;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AeroRecord that)) return false;
        return offset == that.offset
                && partition == that.partition
                && Arrays.equals(payload, that.payload)
                && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(offset, topic, partition);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "AeroRecord{" +
                "offset=" + offset +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", payloadLen=" + payload.length +
                ", timestamp=" + timestamp +
                '}';
    }
}
