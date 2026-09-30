using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using GradientGeeks.AeroStream.Client.Client;
using GradientGeeks.AeroStream.Client.Exceptions;
using GradientGeeks.AeroStream.Client.Models;
using GradientGeeks.AeroStream.Client.Protocol;

namespace GradientGeeks.AeroStream.Client.Transport;

/// <summary>
/// Thread-safe asynchronous TCP/TLS transport connection to an AeroStream broker.
/// </summary>
public sealed class AeroConnection : IAsyncDisposable, IDisposable
{
    private readonly AeroClientOptions _options;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private Socket? _socket;
    private Stream? _stream;
    private volatile bool _isBroken = true;
    private volatile bool _isDisposed;
    private int _serverIndex;

    public bool IsAlive => !_isBroken && !_isDisposed && _socket is { Connected: true };

    public AeroConnection(AeroClientOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public void MarkBroken()
    {
        _isBroken = true;
    }

    public async ValueTask EnsureConnectedAsync(CancellationToken ct = default)
    {
        if (IsAlive) return;

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (IsAlive) return;
            await ReconnectInternalAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task ReconnectInternalAsync(CancellationToken ct)
    {
        CloseCurrentStream();

        int attempts = 0;
        Exception? lastException = null;

        while (attempts <= _options.MaxRetries)
        {
            attempts++;
            ct.ThrowIfCancellationRequested();

            var servers = _options.BootstrapServers;
            if (servers.Count == 0)
            {
                throw new ConnectionException("No bootstrap servers configured.");
            }

            string server = servers[_serverIndex % servers.Count];
            _serverIndex++;

            try
            {
                await ConnectToServerAsync(server, ct).ConfigureAwait(false);
                _isBroken = false;
                return;
            }
            catch (Exception ex) when (ex is not AuthenticationException && attempts <= _options.MaxRetries)
            {
                lastException = ex;
                CloseCurrentStream();

                TimeSpan backoff = ComputeBackoff(attempts, _options.InitialBackoff, _options.MaxBackoff, _options.BackoffMultiplier);
                await Task.Delay(backoff, ct).ConfigureAwait(false);
            }
        }

        _isBroken = true;
        throw new ConnectionException($"Failed to connect after {attempts} attempts. Last error: {lastException?.Message}", lastException!);
    }

    private async Task ConnectToServerAsync(string endpoint, CancellationToken ct)
    {
        string host = endpoint;
        int port = 9091;
        int colonIdx = endpoint.LastIndexOf(':');
        if (colonIdx > 0 && int.TryParse(endpoint[(colonIdx + 1)..], out int parsedPort))
        {
            host = endpoint[..colonIdx];
            port = parsedPort;
        }

        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
        {
            NoDelay = true
        };
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(_options.ConnectTimeout);

        try
        {
            await socket.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);

            Stream stream = new NetworkStream(socket, ownsSocket: true);

            if (_options.UseTls)
            {
                var sslStream = new SslStream(stream, false, ValidateCertificate);
                string targetHost = _options.TlsTargetHost ?? host;
                await sslStream.AuthenticateAsClientAsync(targetHost).WaitAsync(_options.ConnectTimeout, cts.Token).ConfigureAwait(false);
                stream = sslStream;
            }

            _socket = socket;
            _stream = stream;

            if (!string.IsNullOrEmpty(_options.Token))
            {
                await PerformAuthHandshakeAsync(_stream, _options.Token, cts.Token).ConfigureAwait(false);
            }
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private bool ValidateCertificate(object sender, X509Certificate? cert, X509Chain? chain, SslPolicyErrors errors)
    {
        if (!_options.ValidateServerCertificate) return true;
        return errors == SslPolicyErrors.None;
    }

    private static async Task PerformAuthHandshakeAsync(Stream stream, string token, CancellationToken ct)
    {
        byte[] authFrame = FrameCodec.EncodeAuthRequest(token);
        await stream.WriteAsync(authFrame, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);

        byte[] respHeader = new byte[ProtocolConstants.StatusPrefixLength];
        await stream.ReadExactlyAsync(respHeader, ct).ConfigureAwait(false);

        byte status = FrameCodec.DecodeStatusPrefix(respHeader);
        if (status != ProtocolConstants.StatusOk)
        {
            throw new AuthenticationException($"Authentication failed with status code {status}.");
        }
    }

    public async ValueTask<long> ProduceAsync(string topic, uint partition, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        await EnsureConnectedAsync(ct).ConfigureAwait(false);

        byte[] requestBuffer = FrameCodec.EncodeProduceRequest(topic, partition, payload.Span);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        bool completed = false;
        try
        {
            var stream = _stream ?? throw new ConnectionException("Not connected.");

            await stream.WriteAsync(requestBuffer, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);

            byte[] statusBuf = new byte[ProtocolConstants.StatusPrefixLength];
            await stream.ReadExactlyAsync(statusBuf, ct).ConfigureAwait(false);
            byte status = FrameCodec.DecodeStatusPrefix(statusBuf);

            if (status == ProtocolConstants.StatusOk)
            {
                byte[] offsetBuf = new byte[8];
                await stream.ReadExactlyAsync(offsetBuf, ct).ConfigureAwait(false);
                completed = true;
                return unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(offsetBuf));
            }

            completed = true;
            return FrameCodec.DecodeProduceResponse(status, ReadOnlySpan<byte>.Empty);
        }
        catch
        {
            if (!completed)
            {
                MarkBroken();
                CloseCurrentStream();
            }
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>?> FetchAsync(string topic, uint partition, long startOffset, uint maxBytes, CancellationToken ct = default)
    {
        await EnsureConnectedAsync(ct).ConfigureAwait(false);

        byte[] requestBuffer = FrameCodec.EncodeFetchRequest(topic, partition, unchecked((ulong)startOffset), maxBytes);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        bool completed = false;
        try
        {
            var stream = _stream ?? throw new ConnectionException("Not connected.");

            await stream.WriteAsync(requestBuffer, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);

            byte[] statusBuf = new byte[ProtocolConstants.StatusPrefixLength];
            await stream.ReadExactlyAsync(statusBuf, ct).ConfigureAwait(false);
            byte status = FrameCodec.DecodeStatusPrefix(statusBuf);

            if (status == ProtocolConstants.StatusEmpty || status == ProtocolConstants.StatusOk)
            {
                completed = true;
                return null;
            }

            if (status == ProtocolConstants.StatusData)
            {
                byte[] lenBuf = new byte[4];
                await stream.ReadExactlyAsync(lenBuf, ct).ConfigureAwait(false);
                uint bytesToRead = BinaryPrimitives.ReadUInt32BigEndian(lenBuf);

                byte[] dataBuf = new byte[bytesToRead];
                await stream.ReadExactlyAsync(dataBuf, ct).ConfigureAwait(false);
                completed = true;
                return dataBuf;
            }

            throw new AeroStreamException($"Fetch failed with unexpected status code: {status}");
        }
        catch
        {
            if (!completed)
            {
                MarkBroken();
                CloseCurrentStream();
            }
            throw;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<IReadOnlyList<AeroRecord>> FetchMultiAsync(
        string topic,
        uint partition,
        long startOffset,
        uint maxBytes,
        uint maxWaitMs,
        CancellationToken ct = default)
    {
        await EnsureConnectedAsync(ct).ConfigureAwait(false);

        byte[] requestBuffer = FrameCodec.EncodeFetchMultiRequest(topic, partition, unchecked((ulong)startOffset), maxBytes, maxWaitMs);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        bool completed = false;
        try
        {
            var stream = _stream ?? throw new ConnectionException("Not connected.");

            await stream.WriteAsync(requestBuffer, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);

            byte[] statusBuf = new byte[ProtocolConstants.StatusPrefixLength];
            await stream.ReadExactlyAsync(statusBuf, ct).ConfigureAwait(false);
            byte status = FrameCodec.DecodeStatusPrefix(statusBuf);

            if (status == ProtocolConstants.StatusEmpty)
            {
                completed = true;
                return Array.Empty<AeroRecord>();
            }

            if (status == ProtocolConstants.StatusData)
            {
                byte[] countBuf = new byte[4];
                await stream.ReadExactlyAsync(countBuf, ct).ConfigureAwait(false);
                uint entryCount = BinaryPrimitives.ReadUInt32BigEndian(countBuf);

                if (entryCount == 0)
                {
                    completed = true;
                    return Array.Empty<AeroRecord>();
                }

                int indexLen = (int)entryCount * 12;
                byte[] indexBuf = new byte[indexLen];
                await stream.ReadExactlyAsync(indexBuf, ct).ConfigureAwait(false);

                long totalPayloadLen = 0;
                for (int i = 0; i < entryCount; i++)
                {
                    int offsetPos = i * 12;
                    uint len = BinaryPrimitives.ReadUInt32BigEndian(indexBuf.AsSpan(offsetPos + 8, 4));
                    totalPayloadLen += len;
                }

                byte[] payloadBuf = new byte[totalPayloadLen];
                if (totalPayloadLen > 0)
                {
                    await stream.ReadExactlyAsync(payloadBuf, ct).ConfigureAwait(false);
                }

                var records = new List<AeroRecord>((int)entryCount);
                int payloadSliceOffset = 0;
                var now = DateTimeOffset.UtcNow;

                for (int i = 0; i < entryCount; i++)
                {
                    int offsetPos = i * 12;
                    ulong off = BinaryPrimitives.ReadUInt64BigEndian(indexBuf.AsSpan(offsetPos, 8));
                    uint len = BinaryPrimitives.ReadUInt32BigEndian(indexBuf.AsSpan(offsetPos + 8, 4));

                    var recordPayload = payloadBuf.AsMemory(payloadSliceOffset, (int)len);
                    payloadSliceOffset += (int)len;

                    records.Add(new AeroRecord(topic, partition, unchecked((long)off), recordPayload, now));
                }

                completed = true;
                return records;
            }

            throw new AeroStreamException($"FetchMulti failed with unexpected status code: {status}");
        }
        catch
        {
            if (!completed)
            {
                MarkBroken();
                CloseCurrentStream();
            }
            throw;
        }
        finally
        {
            _gate.Release();
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

    private void CloseCurrentStream()
    {
        try
        {
            _stream?.Dispose();
        }
        catch { }
        finally
        {
            _stream = null;
            _socket = null;
        }
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _isBroken = true;
        CloseCurrentStream();
        _gate.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;
        _isBroken = true;

        if (_stream is not null)
        {
            try
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
            catch { }
            finally
            {
                _stream = null;
                _socket = null;
            }
        }

        _gate.Dispose();
    }
}
