using System.Buffers.Binary;
using System.Text;
using GradientGeeks.AeroStream.Client.Exceptions;
using GradientGeeks.AeroStream.Client.Protocol;
using Xunit;

namespace GradientGeeks.AeroStream.Client.Tests;

public class ProtocolTests
{
    [Fact]
    public void WriteHeader_EncodesMagicAndCommandAndBodyLengthCorrectly()
    {
        Span<byte> header = stackalloc byte[7];
        FrameCodec.WriteHeader(header, ProtocolConstants.CmdProduce, 1024);

        Assert.Equal(ProtocolConstants.MagicByte0, header[0]);
        Assert.Equal(ProtocolConstants.MagicByte1, header[1]);
        Assert.Equal(ProtocolConstants.CmdProduce, header[2]);
        Assert.Equal(1024u, BinaryPrimitives.ReadUInt32BigEndian(header[3..7]));
    }

    [Fact]
    public void TryValidateHeader_SucceedsForValidHeader()
    {
        Span<byte> header = stackalloc byte[7];
        FrameCodec.WriteHeader(header, ProtocolConstants.CmdFetchMulti, 256);

        bool valid = FrameCodec.TryValidateHeader(header, out byte cmd, out uint bodyLen);
        Assert.True(valid);
        Assert.Equal(ProtocolConstants.CmdFetchMulti, cmd);
        Assert.Equal(256u, bodyLen);
    }

    [Fact]
    public void TryValidateHeader_FailsForInvalidMagic()
    {
        byte[] invalid = new byte[] { 0x00, 0x00, 1, 0, 0, 0, 10 };
        bool valid = FrameCodec.TryValidateHeader(invalid, out _, out _);
        Assert.False(valid);
    }

    [Fact]
    public void EncodeAuthRequest_EncodesHeaderAndToken()
    {
        string token = "super-secret-key";
        byte[] frame = FrameCodec.EncodeAuthRequest(token);

        Assert.Equal(ProtocolConstants.HeaderLength + token.Length, frame.Length);
        Assert.Equal(ProtocolConstants.MagicByte0, frame[0]);
        Assert.Equal(ProtocolConstants.MagicByte1, frame[1]);
        Assert.Equal(ProtocolConstants.CmdAuth, frame[2]);
        Assert.Equal((uint)token.Length, BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(3, 4)));

        string extracted = Encoding.UTF8.GetString(frame.AsSpan(ProtocolConstants.HeaderLength));
        Assert.Equal(token, extracted);
    }

    [Fact]
    public void DecodeStatusPrefix_ValidatesMagicAndReturnsStatus()
    {
        byte[] okPrefix = new byte[] { 0xAE, 0x01, 0x00 };
        Assert.Equal(ProtocolConstants.StatusOk, FrameCodec.DecodeStatusPrefix(okPrefix));

        byte[] emptyPrefix = new byte[] { 0xAE, 0x01, 0x01 };
        Assert.Equal(ProtocolConstants.StatusEmpty, FrameCodec.DecodeStatusPrefix(emptyPrefix));

        byte[] dataPrefix = new byte[] { 0xAE, 0x01, 0x02 };
        Assert.Equal(ProtocolConstants.StatusData, FrameCodec.DecodeStatusPrefix(dataPrefix));

        byte[] badPrefix = new byte[] { 0xFF, 0x01, 0x00 };
        Assert.Throws<AeroStreamException>(() => FrameCodec.DecodeStatusPrefix(badPrefix));
    }

    [Fact]
    public void EncodeProduceRequest_EncodesExpectedBinaryLayout()
    {
        string topic = "telemetry";
        uint partition = 3;
        byte[] payload = Encoding.UTF8.GetBytes("payload-data");

        byte[] frame = FrameCodec.EncodeProduceRequest(topic, partition, payload);

        // Header check
        Assert.Equal(0xAE, frame[0]);
        Assert.Equal(0x01, frame[1]);
        Assert.Equal(ProtocolConstants.CmdProduce, frame[2]);

        int bodyLen = (int)BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(3, 4));
        Assert.Equal(frame.Length - ProtocolConstants.HeaderLength, bodyLen);

        // Body fields check
        int pos = ProtocolConstants.HeaderLength;
        ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(pos, 2));
        pos += 2;
        Assert.Equal(topic.Length, topicLen);

        string topicRead = Encoding.UTF8.GetString(frame.AsSpan(pos, topicLen));
        pos += topicLen;
        Assert.Equal(topic, topicRead);

        uint partRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        pos += 4;
        Assert.Equal(partition, partRead);

        uint payloadLen = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        pos += 4;
        Assert.Equal((uint)payload.Length, payloadLen);

        byte[] payloadRead = frame.AsSpan(pos, (int)payloadLen).ToArray();
        Assert.Equal(payload, payloadRead);
    }

    [Fact]
    public void DecodeProduceResponse_ThrowsOnAuthFailedAndOutOfOrder()
    {
        byte[] offsetBytes = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(offsetBytes, 42);

        long offset = FrameCodec.DecodeProduceResponse(ProtocolConstants.StatusOk, offsetBytes);
        Assert.Equal(42L, offset);

        Assert.Throws<AuthenticationException>(() =>
            FrameCodec.DecodeProduceResponse(ProtocolConstants.StatusAuthFailed, offsetBytes));

        Assert.Throws<OutOfOrderSequenceException>(() =>
            FrameCodec.DecodeProduceResponse(ProtocolConstants.StatusOutOfOrderSequenceNumber, offsetBytes));
    }

    [Fact]
    public void EncodeFetchRequest_EncodesExpectedBinaryLayout()
    {
        string topic = "events";
        uint partition = 1;
        ulong startOffset = 100;
        uint maxBytes = 65536;

        byte[] frame = FrameCodec.EncodeFetchRequest(topic, partition, startOffset, maxBytes);

        Assert.Equal(0xAE, frame[0]);
        Assert.Equal(0x01, frame[1]);
        Assert.Equal(ProtocolConstants.CmdFetch, frame[2]);

        int pos = ProtocolConstants.HeaderLength;
        ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(pos, 2));
        pos += 2;
        Assert.Equal(topic.Length, topicLen);

        string topicRead = Encoding.UTF8.GetString(frame.AsSpan(pos, topicLen));
        pos += topicLen;
        Assert.Equal(topic, topicRead);

        uint partRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        pos += 4;
        Assert.Equal(partition, partRead);

        ulong offsetRead = BinaryPrimitives.ReadUInt64BigEndian(frame.AsSpan(pos, 8));
        pos += 8;
        Assert.Equal(startOffset, offsetRead);

        uint maxBytesRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        Assert.Equal(maxBytes, maxBytesRead);
    }

    [Fact]
    public void EncodeFetchMultiRequest_EncodesExpectedBinaryLayout()
    {
        string topic = "orders";
        uint partition = 2;
        ulong startOffset = 500;
        uint maxBytes = 1048576;
        uint maxWaitMs = 1500;

        byte[] frame = FrameCodec.EncodeFetchMultiRequest(topic, partition, startOffset, maxBytes, maxWaitMs);

        Assert.Equal(0xAE, frame[0]);
        Assert.Equal(0x01, frame[1]);
        Assert.Equal(ProtocolConstants.CmdFetchMulti, frame[2]);

        int pos = ProtocolConstants.HeaderLength;
        ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(pos, 2));
        pos += 2;
        pos += topicLen;

        uint partRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        pos += 4;
        Assert.Equal(partition, partRead);

        ulong offsetRead = BinaryPrimitives.ReadUInt64BigEndian(frame.AsSpan(pos, 8));
        pos += 8;
        Assert.Equal(startOffset, offsetRead);

        uint maxBytesRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        pos += 4;
        Assert.Equal(maxBytes, maxBytesRead);

        uint maxWaitRead = BinaryPrimitives.ReadUInt32BigEndian(frame.AsSpan(pos, 4));
        Assert.Equal(maxWaitMs, maxWaitRead);
    }
}
