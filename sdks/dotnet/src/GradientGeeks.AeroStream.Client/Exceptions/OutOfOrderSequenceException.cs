namespace GradientGeeks.AeroStream.Client.Exceptions;

/// <summary>
/// Thrown when the broker rejects an append with OutOfOrderSequenceNumber (status 45).
/// </summary>
public sealed class OutOfOrderSequenceException : AeroStreamException
{
    public OutOfOrderSequenceException(string message) : base(message) { }
    public OutOfOrderSequenceException(string message, Exception innerException) : base(message, innerException) { }
}
