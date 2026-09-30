using System.Text;
using GradientGeeks.AeroStream.Client.Exceptions;
using GradientGeeks.AeroStream.Client.Transport;

namespace GradientGeeks.AeroStream.Client.Client;

/// <summary>
/// High-throughput asynchronous producer with backpressure and automatic retry handling.
/// </summary>
public sealed class AeroProducer : IDisposable
{
    private readonly AeroClientOptions _options;
    private readonly AeroConnection _connection;
    private readonly SemaphoreSlim _inFlightSemaphore;
    private readonly bool _ownsConnection;
    private bool _isDisposed;

    public AeroProducer(AeroClientOptions options, AeroConnection? connection = null)
    {
        _options = options?.Clone() ?? throw new ArgumentNullException(nameof(options));
        _inFlightSemaphore = new SemaphoreSlim(Math.Max(1, _options.MaxInFlightRequests), Math.Max(1, _options.MaxInFlightRequests));

        if (connection != null)
        {
            _connection = connection;
            _ownsConnection = false;
        }
        else
        {
            _connection = new AeroConnection(_options);
            _ownsConnection = true;
        }
    }

    public ValueTask<long> SendAsync(string topic, uint partition, string payload, CancellationToken ct = default) =>
        SendAsync(topic, partition, Encoding.UTF8.GetBytes(payload), ct);

    public async ValueTask<long> SendAsync(string topic, uint partition, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);

        await _inFlightSemaphore.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            int attempts = 0;
            while (true)
            {
                attempts++;
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(_options.RequestTimeout);

                try
                {
                    return await _connection.ProduceAsync(topic, partition, payload, cts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (ex is AuthenticationException or OutOfOrderSequenceException)
                    {
                        throw;
                    }

                    if (attempts > _options.MaxRetries)
                    {
                        throw;
                    }

                    _connection.MarkBroken();

                    TimeSpan backoff = ComputeBackoff(attempts, _options.InitialBackoff, _options.MaxBackoff, _options.BackoffMultiplier);
                    await Task.Delay(backoff, ct).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _inFlightSemaphore.Release();
        }
    }

    private static TimeSpan ComputeBackoff(int attempt, TimeSpan initial, TimeSpan max, double multiplier)
    {
        double exponent = Math.Max(0, attempt - 1);
        double baseMs = initial.TotalMilliseconds * Math.Pow(multiplier, exponent);
        double cappedMs = Math.Min(baseMs, max.TotalMilliseconds);
        double jitter = 0.75 + (Random.Shared.NextDouble() * 0.50);
        return TimeSpan.FromMilliseconds(Math.Max(1.0, cappedMs * jitter));
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _inFlightSemaphore.Dispose();
        if (_ownsConnection)
        {
            _connection.Dispose();
        }
    }
}
