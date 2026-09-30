package org.gradientgeeks.aerostream.protocol;

import org.gradientgeeks.aerostream.common.AeroException;
import org.gradientgeeks.aerostream.common.AeroRecord;
import org.gradientgeeks.aerostream.common.AuthenticationException;
import org.gradientgeeks.aerostream.common.OutOfOrderException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.gradientgeeks.aerostream.protocol.ProtocolConstants.*;

/**
 * High-performance frame encoder and decoder for AeroStream native protocol.
 */
public final class FrameCodec {

    private FrameCodec() {
    }

    /**
     * Encodes a 7-byte protocol frame header into a newly allocated ByteBuffer.
     */
    public static ByteBuffer encodeHeader(byte cmd, int bodyLen) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.BIG_ENDIAN);
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(cmd);
        buf.putInt(bodyLen);
        buf.flip();
        return buf;
    }

    /**
     * Encodes Command 0 (AUTH) request: [token: UTF-8 string].
     */
    public static ByteBuffer encodeAuthRequest(String token) {
        byte[] tokenBytes = token != null ? token.getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + tokenBytes.length).order(ByteOrder.BIG_ENDIAN);
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(CMD_AUTH);
        buf.putInt(tokenBytes.length);
        buf.put(tokenBytes);
        buf.flip();
        return buf;
    }

    /**
     * Encodes Command 1 (PRODUCE) request.
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] payload) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("Topic length " + topicBytes.length + " exceeds maximum allowed " + MAX_TOPIC_LENGTH);
        }
        byte[] rawPayload = payload != null ? payload : new byte[0];
        int bodyLen = 2 + topicBytes.length + 4 + 4 + rawPayload.length;

        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + bodyLen).order(ByteOrder.BIG_ENDIAN);
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(CMD_PRODUCE);
        buf.putInt(bodyLen);

        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putInt(rawPayload.length);
        buf.put(rawPayload);
        buf.flip();
        return buf;
    }

    /**
     * Encodes Command 2 (FETCH) request for single-range fetch.
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long startOffset, int maxBytes) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("Topic length " + topicBytes.length + " exceeds maximum allowed " + MAX_TOPIC_LENGTH);
        }
        int bodyLen = 2 + topicBytes.length + 4 + 8 + 4;

        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + bodyLen).order(ByteOrder.BIG_ENDIAN);
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(CMD_FETCH);
        buf.putInt(bodyLen);

        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(startOffset);
        buf.putInt(maxBytes);
        buf.flip();
        return buf;
    }

    /**
     * Encodes Command 4 (FETCH_MULTI) request for multi-entry long-polling fetch.
     */
    public static ByteBuffer encodeMultiFetchRequest(String topic, int partition, long startOffset, int maxBytes, int maxWaitMs) {
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null");
        }
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        if (topicBytes.length > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("Topic length " + topicBytes.length + " exceeds maximum allowed " + MAX_TOPIC_LENGTH);
        }
        int bodyLen = 2 + topicBytes.length + 4 + 8 + 4 + 4;

        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + bodyLen).order(ByteOrder.BIG_ENDIAN);
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(CMD_FETCH_MULTI);
        buf.putInt(bodyLen);

        buf.putShort((short) topicBytes.length);
        buf.put(topicBytes);
        buf.putInt(partition);
        buf.putLong(startOffset);
        buf.putInt(maxBytes);
        buf.putInt(maxWaitMs);
        buf.flip();
        return buf;
    }

    /**
     * Validates magic bytes and returns the status code from a 3-byte response prefix [0xAE, 0x01, status].
     */
    public static byte decodeStatusPrefix(byte[] prefix3Bytes) {
        if (prefix3Bytes == null || prefix3Bytes.length < 3) {
            throw new AeroException("Prefix buffer too small: expected 3 bytes");
        }
        if (!isValidMagic(prefix3Bytes[0], prefix3Bytes[1])) {
            throw new AeroException(String.format("Invalid magic prefix: [0x%02X, 0x%02X], expected [0xAE, 0x01]",
                    prefix3Bytes[0], prefix3Bytes[1]));
        }
        return prefix3Bytes[2];
    }

    /**
     * Validates magic bytes and returns the status code from ByteBuffer at its current position.
     */
    public static byte decodeStatusPrefix(ByteBuffer buf) {
        if (buf.remaining() < 3) {
            throw new AeroException("Buffer underflow reading status prefix: need 3 bytes, available " + buf.remaining());
        }
        byte b0 = buf.get();
        byte b1 = buf.get();
        if (!isValidMagic(b0, b1)) {
            throw new AeroException(String.format("Invalid magic prefix: [0x%02X, 0x%02X], expected [0xAE, 0x01]", b0, b1));
        }
        return buf.get();
    }

    /**
     * Decodes produce response offset from status and 8-byte offset buffer.
     */
    public static long decodeProduceResponse(byte status, ByteBuffer offsetBuf) {
        if (status == STATUS_OK) {
            if (offsetBuf == null || offsetBuf.remaining() < 8) {
                throw new AeroException("Produce OK response missing 8-byte offset");
            }
            return offsetBuf.order(ByteOrder.BIG_ENDIAN).getLong();
        } else if (status == STATUS_AUTH_FAILED) {
            throw new AuthenticationException("Produce rejected: not authenticated");
        } else if (status == STATUS_OUT_OF_ORDER) {
            throw new OutOfOrderException("Produce rejected: out of order sequence number");
        } else {
            throw new AeroException("Produce rejected with status: " + status);
        }
    }

    /**
     * Decodes multi-entry records from index headers and payload byte buffer.
     */
    public static List<AeroRecord> decodeMultiEntries(int entryCount, ByteBuffer indexData, ByteBuffer payloadBytes, String topic, int partition) {
        if (entryCount <= 0) {
            return Collections.emptyList();
        }
        int expectedIndexLen = entryCount * 12;
        if (indexData.remaining() < expectedIndexLen) {
            throw new AeroException(String.format("Multi-fetch index data too short: got %d bytes, expected %d",
                    indexData.remaining(), expectedIndexLen));
        }

        indexData.order(ByteOrder.BIG_ENDIAN);
        long[] offsets = new long[entryCount];
        int[] lengths = new int[entryCount];
        long totalPayloadLen = 0;

        for (int i = 0; i < entryCount; i++) {
            offsets[i] = indexData.getLong();
            int len = indexData.getInt();
            if (len < 0) {
                throw new AeroException("Negative record length in multi-fetch response: " + len);
            }
            lengths[i] = len;
            totalPayloadLen += len;
        }

        if (payloadBytes.remaining() < totalPayloadLen) {
            throw new AeroException(String.format("Multi-fetch payload truncated: needed %d bytes, remaining %d",
                    totalPayloadLen, payloadBytes.remaining()));
        }

        Instant now = Instant.now();
        List<AeroRecord> records = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            byte[] recordPayload = new byte[lengths[i]];
            payloadBytes.get(recordPayload);
            records.add(new AeroRecord(offsets[i], recordPayload, topic, partition, now));
        }

        return records;
    }
}
