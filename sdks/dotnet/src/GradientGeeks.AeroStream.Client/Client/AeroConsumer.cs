using System.Runtime.CompilerServices;
using GradientGeeks.AeroStream.Client.Exceptions;
using GradientGeeks.AeroStream.Client.Models;
using GradientGeeks.AeroStream.Client.Transport;

namespace GradientGeeks.AeroStream.Client.Client;

/// <summary>
/// Asynchronous consumer client supporting single-fetch, long-polling multi-fetch, and continuous IAsyncEnumerable streaming.
/// </summary>
public sealed class AeroConsumer : IDisposable
{
    private readonly AeroClientOptions _options;
    private readonly AeroConnection _connection;
    private readonly bool _ownsConnection;
    private bool _isDisposed;

    public AeroConsumer(AeroClientOptions options, AeroConnection? connection = null)
    {
        _options = options?.Clone() ?? throw new ArgumentNullException(nameof(options));

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

    public async ValueTask<ReadOnlyMemory<byte>?> FetchAsync(
        string topic,
        uint partition,
        long offset,
        uint maxBytes = 1048576,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        return await _connection.FetchAsync(topic, partition, offset, maxBytes, ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<AeroRecord>> FetchMultiAsync(
        string topic,
        uint partition,
        long startOffset,
        uint maxBytes = 1048576,
        uint maxWaitMs = 500,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        return await _connection.FetchMultiAsync(topic, partition, startOffset, maxBytes, maxWaitMs, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<AeroRecord> StreamAsync(
        string topic,
        uint partition,
        long startOffset,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var record in StreamAsync(topic, partition, startOffset, 1048576, 500, TimeSpan.FromMilliseconds(50), ct).ConfigureAwait(false))
        {
            yield return record;
        }
    }

    public async IAsyncEnumerable<AeroRecord> StreamAsync(
        string topic,
        uint partition,
        long startOffset,
        uint maxBytes,
        uint maxWaitMs,
        TimeSpan backoffOnEmpty,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        long currentOffset = startOffset;

        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<AeroRecord> records;
            try
            {
                records = await _connection.FetchMultiAsync(topic, partition, currentOffset, maxBytes, maxWaitMs, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is AuthenticationException)
                {
                    throw;
                }

                _connection.MarkBroken();
                await Task.Delay(200, ct).ConfigureAwait(false);
                continue;
            }

            if (records.Count == 0)
            {
                if (backoffOnEmpty > TimeSpan.Zero)
                {
                    await Task.Delay(backoffOnEmpty, ct).ConfigureAwait(false);
                }
                continue;
            }

            foreach (var record in records)
            {
                yield return record;
                currentOffset = Math.Max(currentOffset, record.Offset + 1);
            }
        }
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        if (_ownsConnection)
        {
            _connection.Dispose();
        }
    }
}
