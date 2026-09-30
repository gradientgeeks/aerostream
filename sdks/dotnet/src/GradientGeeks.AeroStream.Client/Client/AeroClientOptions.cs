namespace GradientGeeks.AeroStream.Client.Client;

/// <summary>
/// Configuration options for the AeroStream client.
/// </summary>
public sealed class AeroClientOptions
{
    public IReadOnlyList<string> BootstrapServers { get; set; } = new[] { "127.0.0.1:9091" };
    public string? Token { get; set; }
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public int MaxRetries { get; set; } = 5;
    public TimeSpan InitialBackoff { get; set; } = TimeSpan.FromMilliseconds(50);
    public TimeSpan MaxBackoff { get; set; } = TimeSpan.FromMilliseconds(2000);
    public double BackoffMultiplier { get; set; } = 2.0;
    public int MaxInFlightRequests { get; set; } = 1024;
    public bool UseTls { get; set; } = false;
    public string? TlsTargetHost { get; set; }
    public bool ValidateServerCertificate { get; set; } = true;

    public AeroClientOptions Clone() => (AeroClientOptions)MemberwiseClone();
}
