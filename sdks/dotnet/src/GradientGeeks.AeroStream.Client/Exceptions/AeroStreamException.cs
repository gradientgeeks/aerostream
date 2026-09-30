namespace GradientGeeks.AeroStream.Client.Exceptions;

/// <summary>
/// Base exception for all AeroStream client errors.
/// </summary>
public class AeroStreamException : Exception
{
    public AeroStreamException(string message) : base(message) { }
    public AeroStreamException(string message, Exception innerException) : base(message, innerException) { }
}
