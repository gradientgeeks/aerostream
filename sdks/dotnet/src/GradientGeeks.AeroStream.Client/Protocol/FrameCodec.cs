using System.Buffers.Binary;
using System.Text;
using GradientGeeks.AeroStream.Client.Exceptions;

namespace GradientGeeks.AeroStream.Client.Protocol;

/// <summary>
/// High-performance zero-allocation binary codec for AeroStream frames.
/// </summary>
public static class FrameCodec
{
    public static void WriteHeader(Span<byte> destination, byte command, uint bodyLength)
    {
        destination[0] = ProtocolConstants.MagicByte0;
        destination[1] = ProtocolConstants.MagicByte1;
        destination[2] = command;
        BinaryPrimitives.WriteUInt32BigEndian(destination[3..7], bodyLength);
    }

    public static bool TryValidateHeader(ReadOnlySpan<byte> header, out byte command, out uint bodyLength)
    {
        if (header.Length < ProtocolConstants.HeaderLength ||
            header[0] != ProtocolConstants.MagicByte0 ||
            header[1] != ProtocolConstants.MagicByte1)
        {
            command = 0;
            bodyLength = 0;
            return false;
        }

        command = header[2];
        bodyLength = BinaryPrimitives.ReadUInt32BigEndian(header[3..7]);
        return true;
    }

    public static byte DecodeStatusPrefix(ReadOnlySpan<byte> prefix)
    {
        if (prefix.Length < ProtocolConstants.StatusPrefixLength)
        {
            throw new AeroStreamException($"Status prefix too short: {prefix.Length} bytes.");
        }

        if (prefix[0] != ProtocolConstants.MagicByte0 || prefix[1] != ProtocolConstants.MagicByte1)
        {
            throw new AeroStreamException($"Invalid magic prefix: [0x{prefix[0]:X2}, 0x{prefix[1]:X2}], expected [0xAE, 0x01].");
        }

        return prefix[2];
    }

    public static int GetAuthRequestLength(ReadOnlySpan<char> token) =>
        ProtocolConstants.HeaderLength + Encoding.UTF8.GetByteCount(token);

    public static int EncodeAuthRequest(ReadOnlySpan<char> token, Span<byte> destination)
    {
        int tokenByteCount = Encoding.UTF8.GetByteCount(token);
        WriteHeader(destination, ProtocolConstants.CmdAuth, (uint)tokenByteCount);
        return ProtocolConstants.HeaderLength + Encoding.UTF8.GetBytes(token, destination[ProtocolConstants.HeaderLength..]);
    }

    public static byte[] EncodeAuthRequest(string token)
    {
        byte[] buffer = new byte[GetAuthRequestLength(token)];
        EncodeAuthRequest(token.AsSpan(), buffer);
        return buffer;
    }

    public static int GetProduceRequestLength(ReadOnlySpan<char> topic, int payloadLength) =>
        ProtocolConstants.HeaderLength + 2 + Encoding.UTF8.GetByteCount(topic) + 4 + 4 + payloadLength;

    public static int EncodeProduceRequest(ReadOnlySpan<char> topic, uint partition, ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        int topicBytesLen = Encoding.UTF8.GetByteCount(topic);
        if (topicBytesLen > ushort.MaxValue)
        {
            throw new AeroStreamException($"Topic length {topicBytesLen} exceeds maximum UInt16.");
        }

        uint bodyLength = (uint)(2 + topicBytesLen + 4 + 4 + payload.Length);
        WriteHeader(destination, ProtocolConstants.CmdProduce, bodyLength);

        int pos = ProtocolConstants.HeaderLength;
        BinaryPrimitives.WriteUInt16BigEndian(destination[pos..], (ushort)topicBytesLen);
        pos += 2;

        pos += Encoding.UTF8.GetBytes(topic, destination[pos..]);

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], partition);
        pos += 4;

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], (uint)payload.Length);
        pos += 4;

        payload.CopyTo(destination[pos..]);
        pos += payload.Length;

        return pos;
    }

    public static byte[] EncodeProduceRequest(string topic, uint partition, ReadOnlySpan<byte> payload)
    {
        byte[] buffer = new byte[GetProduceRequestLength(topic, payload.Length)];
        EncodeProduceRequest(topic.AsSpan(), partition, payload, buffer);
        return buffer;
    }

    public static int GetFetchRequestLength(ReadOnlySpan<char> topic) =>
        ProtocolConstants.HeaderLength + 2 + Encoding.UTF8.GetByteCount(topic) + 4 + 8 + 4;

    public static int EncodeFetchRequest(ReadOnlySpan<char> topic, uint partition, ulong startOffset, uint maxBytes, Span<byte> destination)
    {
        int topicBytesLen = Encoding.UTF8.GetByteCount(topic);
        if (topicBytesLen > ushort.MaxValue)
        {
            throw new AeroStreamException($"Topic length {topicBytesLen} exceeds maximum UInt16.");
        }

        uint bodyLength = (uint)(2 + topicBytesLen + 4 + 8 + 4);
        WriteHeader(destination, ProtocolConstants.CmdFetch, bodyLength);

        int pos = ProtocolConstants.HeaderLength;
        BinaryPrimitives.WriteUInt16BigEndian(destination[pos..], (ushort)topicBytesLen);
        pos += 2;

        pos += Encoding.UTF8.GetBytes(topic, destination[pos..]);

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], partition);
        pos += 4;

        BinaryPrimitives.WriteUInt64BigEndian(destination[pos..], startOffset);
        pos += 8;

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], maxBytes);
        pos += 4;

        return pos;
    }

    public static byte[] EncodeFetchRequest(string topic, uint partition, ulong startOffset, uint maxBytes)
    {
        byte[] buffer = new byte[GetFetchRequestLength(topic)];
        EncodeFetchRequest(topic.AsSpan(), partition, startOffset, maxBytes, buffer);
        return buffer;
    }

    public static int GetFetchMultiRequestLength(ReadOnlySpan<char> topic) =>
        ProtocolConstants.HeaderLength + 2 + Encoding.UTF8.GetByteCount(topic) + 4 + 8 + 4 + 4;

    public static int EncodeFetchMultiRequest(ReadOnlySpan<char> topic, uint partition, ulong startOffset, uint maxBytes, uint maxWaitMs, Span<byte> destination)
    {
        int topicBytesLen = Encoding.UTF8.GetByteCount(topic);
        if (topicBytesLen > ushort.MaxValue)
        {
            throw new AeroStreamException($"Topic length {topicBytesLen} exceeds maximum UInt16.");
        }

        uint bodyLength = (uint)(2 + topicBytesLen + 4 + 8 + 4 + 4);
        WriteHeader(destination, ProtocolConstants.CmdFetchMulti, bodyLength);

        int pos = ProtocolConstants.HeaderLength;
        BinaryPrimitives.WriteUInt16BigEndian(destination[pos..], (ushort)topicBytesLen);
        pos += 2;

        pos += Encoding.UTF8.GetBytes(topic, destination[pos..]);

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], partition);
        pos += 4;

        BinaryPrimitives.WriteUInt64BigEndian(destination[pos..], startOffset);
        pos += 8;

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], maxBytes);
        pos += 4;

        BinaryPrimitives.WriteUInt32BigEndian(destination[pos..], maxWaitMs);
        pos += 4;

        return pos;
    }

    public static byte[] EncodeFetchMultiRequest(string topic, uint partition, ulong startOffset, uint maxBytes, uint maxWaitMs)
    {
        byte[] buffer = new byte[GetFetchMultiRequestLength(topic)];
        EncodeFetchMultiRequest(topic.AsSpan(), partition, startOffset, maxBytes, maxWaitMs, buffer);
        return buffer;
    }

    public static long DecodeProduceResponse(byte status, ReadOnlySpan<byte> offsetBytes)
    {
        return status switch
        {
            ProtocolConstants.StatusOk => unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(offsetBytes)),
            ProtocolConstants.StatusAuthFailed => throw new AuthenticationException("Produce rejected: not authenticated."),
            ProtocolConstants.StatusOutOfOrderSequenceNumber => throw new OutOfOrderSequenceException("Produce rejected: out of order sequence number."),
            _ => throw new AeroStreamException($"Produce rejected with status code: {status}")
        };
    }
}
