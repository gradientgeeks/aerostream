namespace GradientGeeks.AeroStream.Client.Exceptions;

/// <summary>
/// Thrown when authentication handshake fails with status code 3.
/// </summary>
public sealed class AuthenticationException : AeroStreamException
{
    public AuthenticationException(string message) : base(message) { }
    public AuthenticationException(string message, Exception innerException) : base(message, innerException) { }
}
