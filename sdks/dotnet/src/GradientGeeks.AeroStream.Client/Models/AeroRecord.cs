using System.Text;

namespace GradientGeeks.AeroStream.Client.Models;

/// <summary>
/// Represents a message record retrieved from an AeroStream partition log.
/// </summary>
public sealed class AeroRecord
{
    public long Offset { get; }
    public ReadOnlyMemory<byte> Payload { get; }
    public string Topic { get; }
    public uint Partition { get; }
    public DateTimeOffset Timestamp { get; }

    public AeroRecord(string topic, uint partition, long offset, ReadOnlyMemory<byte> payload, DateTimeOffset? timestamp = null)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Payload = payload;
        Timestamp = timestamp ?? DateTimeOffset.UtcNow;
    }

    public string GetPayloadString(Encoding? encoding = null) =>
        (encoding ?? Encoding.UTF8).GetString(Payload.Span);

    public override string ToString() =>
        $"AeroRecord(Topic={Topic}, Partition={Partition}, Offset={Offset}, PayloadLength={Payload.Length})";
}
