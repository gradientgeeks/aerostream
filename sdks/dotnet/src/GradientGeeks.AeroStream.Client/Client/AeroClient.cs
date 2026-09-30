using System.Collections.Concurrent;
using System.Text;
using GradientGeeks.AeroStream.Client.Transport;

namespace GradientGeeks.AeroStream.Client.Client;

/// <summary>
/// Main entry point for interacting with an AeroStream cluster.
/// </summary>
public sealed class AeroClient : IAsyncDisposable, IDisposable
{
    private readonly AeroClientOptions _options;
    private readonly ConcurrentDictionary<string, AeroConnection> _connections = new();
    private readonly Lazy<AeroProducer> _sharedProducer;
    private bool _isDisposed;

    public AeroClientOptions Options => _options.Clone();

    public AeroClient(AeroClientOptions options)
    {
        _options = options?.Clone() ?? throw new ArgumentNullException(nameof(options));
        _sharedProducer = new Lazy<AeroProducer>(() => CreateProducer());
    }

    public static AeroClient Create(string endpoint, string? token = null)
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { endpoint },
            Token = token
        };
        return new AeroClient(options);
    }

    public AeroProducer CreateProducer()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        var connection = GetOrCreateConnection(_options.BootstrapServers[0]);
        return new AeroProducer(_options, connection);
    }

    public AeroConsumer CreateConsumer()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        var connection = GetOrCreateConnection(_options.BootstrapServers[0]);
        return new AeroConsumer(_options, connection);
    }

    public ValueTask<long> PublishAsync(string topic, uint partition, string payload, CancellationToken ct = default) =>
        PublishAsync(topic, partition, Encoding.UTF8.GetBytes(payload), ct);

    public ValueTask<long> PublishAsync(string topic, uint partition, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        return _sharedProducer.Value.SendAsync(topic, partition, payload, ct);
    }

    private AeroConnection GetOrCreateConnection(string server)
    {
        return _connections.GetOrAdd(server, s =>
        {
            var serverOptions = _options.Clone();
            serverOptions.BootstrapServers = new[] { s };
            return new AeroConnection(serverOptions);
        });
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        if (_sharedProducer.IsValueCreated)
        {
            _sharedProducer.Value.Dispose();
        }

        foreach (var conn in _connections.Values)
        {
            conn.Dispose();
        }
        _connections.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        if (_sharedProducer.IsValueCreated)
        {
            _sharedProducer.Value.Dispose();
        }

        foreach (var conn in _connections.Values)
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
        _connections.Clear();
    }
}
