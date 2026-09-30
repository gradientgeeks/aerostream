package org.gradientgeeks.aerostream.protocol;

import org.gradientgeeks.aerostream.common.AeroException;
import org.gradientgeeks.aerostream.common.AeroRecord;
import org.gradientgeeks.aerostream.common.AuthenticationException;
import org.gradientgeeks.aerostream.common.OutOfOrderException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class ProtocolTest {

    @Test
    @DisplayName("Protocol magic validation")
    void testMagicValidation() {
        assertThat(ProtocolConstants.isValidMagic((byte) 0xAE, (byte) 0x01)).isTrue();
        assertThat(ProtocolConstants.isValidMagic((byte) 0xAE, (byte) 0x02)).isFalse();
        assertThat(ProtocolConstants.isValidMagic((byte) 0x00, (byte) 0x01)).isFalse();
        assertThat(ProtocolConstants.isValidMagic((byte) 0xFF, (byte) 0xFF)).isFalse();
    }

    @Test
    @DisplayName("Encode 7-byte header")
    void testEncodeHeader() {
        ByteBuffer buf = FrameCodec.encodeHeader(ProtocolConstants.CMD_PRODUCE, 128);
        assertThat(buf.remaining()).isEqualTo(7);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_0);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_1);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.CMD_PRODUCE);
        assertThat(buf.order(ByteOrder.BIG_ENDIAN).getInt()).isEqualTo(128);
    }

    @Test
    @DisplayName("Encode Auth request (Command 0)")
    void testEncodeAuthRequest() {
        String token = "super-secret-token";
        ByteBuffer buf = FrameCodec.encodeAuthRequest(token);

        assertThat(buf.remaining()).isEqualTo(7 + token.getBytes(StandardCharsets.UTF_8).length);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_0);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_1);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.CMD_AUTH);

        int bodyLen = buf.order(ByteOrder.BIG_ENDIAN).getInt();
        assertThat(bodyLen).isEqualTo(token.length());

        byte[] tokenBytes = new byte[bodyLen];
        buf.get(tokenBytes);
        assertThat(new String(tokenBytes, StandardCharsets.UTF_8)).isEqualTo(token);
    }

    @Test
    @DisplayName("Encode Produce request (Command 1)")
    void testEncodeProduceRequest() {
        String topic = "sensors";
        int partition = 3;
        byte[] payload = "temp=22.5C".getBytes(StandardCharsets.UTF_8);

        ByteBuffer buf = FrameCodec.encodeProduceRequest(topic, partition, payload);

        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_0);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_1);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.CMD_PRODUCE);

        int bodyLen = buf.order(ByteOrder.BIG_ENDIAN).getInt();
        assertThat(bodyLen).isEqualTo(2 + topic.length() + 4 + 4 + payload.length);

        short topicLen = buf.getShort();
        assertThat(topicLen).isEqualTo((short) topic.length());

        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        assertThat(new String(topicBytes, StandardCharsets.UTF_8)).isEqualTo(topic);

        int part = buf.getInt();
        assertThat(part).isEqualTo(partition);

        int payloadLen = buf.getInt();
        assertThat(payloadLen).isEqualTo(payload.length);

        byte[] payloadBytes = new byte[payloadLen];
        buf.get(payloadBytes);
        assertThat(payloadBytes).isEqualTo(payload);
    }

    @Test
    @DisplayName("Produce request rejects topics longer than 65535 bytes")
    void testProduceTopicLengthBoundary() {
        String longTopic = "a".repeat(65536);
        assertThatThrownBy(() -> FrameCodec.encodeProduceRequest(longTopic, 0, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum allowed");
    }

    @Test
    @DisplayName("Encode Fetch request (Command 2)")
    void testEncodeFetchRequest() {
        String topic = "events";
        int partition = 0;
        long startOffset = 42L;
        int maxBytes = 65536;

        ByteBuffer buf = FrameCodec.encodeFetchRequest(topic, partition, startOffset, maxBytes);

        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_0);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_1);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.CMD_FETCH);

        int bodyLen = buf.order(ByteOrder.BIG_ENDIAN).getInt();
        assertThat(bodyLen).isEqualTo(2 + topic.length() + 4 + 8 + 4);

        short topicLen = buf.getShort();
        assertThat(topicLen).isEqualTo((short) topic.length());

        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        assertThat(new String(topicBytes, StandardCharsets.UTF_8)).isEqualTo(topic);

        assertThat(buf.getInt()).isEqualTo(partition);
        assertThat(buf.getLong()).isEqualTo(startOffset);
        assertThat(buf.getInt()).isEqualTo(maxBytes);
    }

    @Test
    @DisplayName("Encode MultiFetch request (Command 4)")
    void testEncodeMultiFetchRequest() {
        String topic = "logs";
        int partition = 1;
        long startOffset = 1000L;
        int maxBytes = 1048576;
        int maxWaitMs = 250;

        ByteBuffer buf = FrameCodec.encodeMultiFetchRequest(topic, partition, startOffset, maxBytes, maxWaitMs);

        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_0);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.MAGIC_1);
        assertThat(buf.get()).isEqualTo(ProtocolConstants.CMD_FETCH_MULTI);

        int bodyLen = buf.order(ByteOrder.BIG_ENDIAN).getInt();
        assertThat(bodyLen).isEqualTo(2 + topic.length() + 4 + 8 + 4 + 4);

        short topicLen = buf.getShort();
        assertThat(topicLen).isEqualTo((short) topic.length());

        byte[] topicBytes = new byte[topicLen];
        buf.get(topicBytes);
        assertThat(new String(topicBytes, StandardCharsets.UTF_8)).isEqualTo(topic);

        assertThat(buf.getInt()).isEqualTo(partition);
        assertThat(buf.getLong()).isEqualTo(startOffset);
        assertThat(buf.getInt()).isEqualTo(maxBytes);
        assertThat(buf.getInt()).isEqualTo(maxWaitMs);
    }

    @Test
    @DisplayName("Decode status prefix successfully")
    void testDecodeStatusPrefixSuccess() {
        byte[] validPrefix = new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_OK};
        assertThat(FrameCodec.decodeStatusPrefix(validPrefix)).isEqualTo(ProtocolConstants.STATUS_OK);

        ByteBuffer buf = ByteBuffer.wrap(new byte[]{ProtocolConstants.MAGIC_0, ProtocolConstants.MAGIC_1, ProtocolConstants.STATUS_DATA});
        assertThat(FrameCodec.decodeStatusPrefix(buf)).isEqualTo(ProtocolConstants.STATUS_DATA);
    }

    @Test
    @DisplayName("Decode status prefix throws on invalid magic")
    void testDecodeStatusPrefixInvalidMagic() {
        byte[] invalidPrefix = new byte[]{(byte) 0x00, (byte) 0x01, ProtocolConstants.STATUS_OK};
        assertThatThrownBy(() -> FrameCodec.decodeStatusPrefix(invalidPrefix))
                .isInstanceOf(AeroException.class)
                .hasMessageContaining("Invalid magic prefix");
    }

    @Test
    @DisplayName("Decode Produce response statuses")
    void testDecodeProduceResponse() {
        ByteBuffer offsetBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        offsetBuf.putLong(999L);
        offsetBuf.flip();

        long offset = FrameCodec.decodeProduceResponse(ProtocolConstants.STATUS_OK, offsetBuf);
        assertThat(offset).isEqualTo(999L);

        assertThatThrownBy(() -> FrameCodec.decodeProduceResponse(ProtocolConstants.STATUS_AUTH_FAILED, null))
                .isInstanceOf(AuthenticationException.class);

        assertThatThrownBy(() -> FrameCodec.decodeProduceResponse(ProtocolConstants.STATUS_OUT_OF_ORDER, null))
                .isInstanceOf(OutOfOrderException.class);

        assertThatThrownBy(() -> FrameCodec.decodeProduceResponse((byte) 99, null))
                .isInstanceOf(AeroException.class)
                .hasMessageContaining("Produce rejected with status: 99");
    }

    @Test
    @DisplayName("Decode MultiFetch entries")
    void testDecodeMultiEntries() {
        int count = 2;
        ByteBuffer indexBuf = ByteBuffer.allocate(count * 12).order(ByteOrder.BIG_ENDIAN);
        indexBuf.putLong(10L).putInt(5);
        indexBuf.putLong(11L).putInt(4);
        indexBuf.flip();

        byte[] payload1 = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] payload2 = "java".getBytes(StandardCharsets.UTF_8);
        ByteBuffer payloadBuf = ByteBuffer.allocate(9);
        payloadBuf.put(payload1).put(payload2).flip();

        List<AeroRecord> records = FrameCodec.decodeMultiEntries(count, indexBuf, payloadBuf, "orders", 0);

        assertThat(records).hasSize(2);
        assertThat(records.get(0).offset()).isEqualTo(10L);
        assertThat(records.get(0).payloadAsString()).isEqualTo("hello");
        assertThat(records.get(0).topic()).isEqualTo("orders");
        assertThat(records.get(0).partition()).isEqualTo(0);

        assertThat(records.get(1).offset()).isEqualTo(11L);
        assertThat(records.get(1).payloadAsString()).isEqualTo("java");
    }

    @Test
    @DisplayName("AeroRecord model equals and properties")
    void testAeroRecordModel() {
        Instant now = Instant.now();
        byte[] payload = "test-data".getBytes(StandardCharsets.UTF_8);
        AeroRecord r1 = new AeroRecord(5L, payload, "topicA", 0, now);
        AeroRecord r2 = new AeroRecord(5L, payload, "topicA", 0, now);

        assertThat(r1).isEqualTo(r2);
        assertThat(r1.hashCode()).isEqualTo(r2.hashCode());
        assertThat(r1.offset()).isEqualTo(5L);
        assertThat(r1.getOffset()).isEqualTo(5L);
        assertThat(r1.payloadAsString()).isEqualTo("test-data");
        assertThat(r1.topic()).isEqualTo("topicA");
        assertThat(r1.partition()).isEqualTo(0);
        assertThat(r1.timestamp()).isEqualTo(now);
        assertThat(r1.toString()).contains("offset=5", "topic='topicA'");
    }
}
