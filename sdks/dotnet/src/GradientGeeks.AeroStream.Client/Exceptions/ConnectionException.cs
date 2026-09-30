namespace GradientGeeks.AeroStream.Client.Exceptions;

/// <summary>
/// Thrown when a network transport or connection failure occurs.
/// </summary>
public sealed class ConnectionException : AeroStreamException
{
    public ConnectionException(string message) : base(message) { }
    public ConnectionException(string message, Exception innerException) : base(message, innerException) { }
}
