using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using GradientGeeks.AeroStream.Client.Client;
using GradientGeeks.AeroStream.Client.Exceptions;
using GradientGeeks.AeroStream.Client.Models;
using GradientGeeks.AeroStream.Client.Protocol;
using Xunit;

namespace GradientGeeks.AeroStream.Client.Tests;

public class MockBrokerTests : IAsyncLifetime
{
    private TcpListener? _listener;
    private int _port;
    private CancellationTokenSource? _cts;
    private Task? _serverTask;
    private long _currentOffset = 100;
    private readonly string _validToken = "secret-token-123";

    public async Task InitializeAsync()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _cts = new CancellationTokenSource();
        _serverTask = Task.Run(() => RunMockServerAsync(_listener, _cts.Token));
        await Task.Yield();
    }

    public async Task DisposeAsync()
    {
        if (_cts != null)
        {
            _cts.Cancel();
        }
        _listener?.Stop();
        if (_serverTask != null)
        {
            try
            {
                await _serverTask;
            }
            catch { }
        }
        _cts?.Dispose();
    }

    private async Task RunMockServerAsync(TcpListener listener, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await listener.AcceptSocketAsync(ct);
            }
            catch
            {
                break;
            }

            _ = Task.Run(async () =>
            {
                using var s = socket;
                using var stream = new NetworkStream(s, ownsSocket: false);

                try
                {
                    byte[] header = new byte[ProtocolConstants.HeaderLength];
                    while (!ct.IsCancellationRequested)
                    {
                        int read = await stream.ReadAsync(header.AsMemory(0, ProtocolConstants.HeaderLength), ct);
                        if (read == 0) break;
                        if (read < ProtocolConstants.HeaderLength)
                        {
                            await stream.ReadExactlyAsync(header.AsMemory(read, ProtocolConstants.HeaderLength - read), ct);
                        }

                        if (!FrameCodec.TryValidateHeader(header, out byte cmd, out uint bodyLen))
                        {
                            break;
                        }

                        byte[] body = new byte[bodyLen];
                        if (bodyLen > 0)
                        {
                            await stream.ReadExactlyAsync(body, ct);
                        }

                        if (cmd == ProtocolConstants.CmdAuth)
                        {
                            string token = Encoding.UTF8.GetString(body);
                            byte status = token == _validToken ? ProtocolConstants.StatusOk : ProtocolConstants.StatusAuthFailed;
                            byte[] resp = new byte[] { ProtocolConstants.MagicByte0, ProtocolConstants.MagicByte1, status };
                            await stream.WriteAsync(resp, ct);
                            await stream.FlushAsync(ct);
                        }
                        else if (cmd == ProtocolConstants.CmdProduce)
                        {
                            ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(body.AsSpan(0, 2));
                            string topic = Encoding.UTF8.GetString(body.AsSpan(2, topicLen));

                            if (topic == "fail-out-of-order")
                            {
                                byte[] resp = new byte[] { ProtocolConstants.MagicByte0, ProtocolConstants.MagicByte1, ProtocolConstants.StatusOutOfOrderSequenceNumber };
                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                            else
                            {
                                long assigned = Interlocked.Increment(ref _currentOffset);
                                byte[] resp = new byte[3 + 8];
                                resp[0] = ProtocolConstants.MagicByte0;
                                resp[1] = ProtocolConstants.MagicByte1;
                                resp[2] = ProtocolConstants.StatusOk;
                                BinaryPrimitives.WriteUInt64BigEndian(resp.AsSpan(3, 8), unchecked((ulong)assigned));
                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                        }
                        else if (cmd == ProtocolConstants.CmdFetch)
                        {
                            ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(body.AsSpan(0, 2));
                            string topic = Encoding.UTF8.GetString(body.AsSpan(2, topicLen));

                            if (topic == "empty-topic")
                            {
                                byte[] resp = new byte[] { ProtocolConstants.MagicByte0, ProtocolConstants.MagicByte1, ProtocolConstants.StatusEmpty };
                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                            else
                            {
                                byte[] payload = Encoding.UTF8.GetBytes("sample-fetch-payload");
                                byte[] resp = new byte[3 + 4 + payload.Length];
                                resp[0] = ProtocolConstants.MagicByte0;
                                resp[1] = ProtocolConstants.MagicByte1;
                                resp[2] = ProtocolConstants.StatusData;
                                BinaryPrimitives.WriteUInt32BigEndian(resp.AsSpan(3, 4), (uint)payload.Length);
                                payload.CopyTo(resp.AsSpan(7));
                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                        }
                        else if (cmd == ProtocolConstants.CmdFetchMulti)
                        {
                            ushort topicLen = BinaryPrimitives.ReadUInt16BigEndian(body.AsSpan(0, 2));
                            int pos = 2 + topicLen + 4; // topic_len + topic + partition
                            ulong startOffset = BinaryPrimitives.ReadUInt64BigEndian(body.AsSpan(pos, 8));

                            if (startOffset > 500)
                            {
                                byte[] resp = new byte[] { ProtocolConstants.MagicByte0, ProtocolConstants.MagicByte1, ProtocolConstants.StatusEmpty };
                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                            else
                            {
                                byte[] p1 = Encoding.UTF8.GetBytes("item-1");
                                byte[] p2 = Encoding.UTF8.GetBytes("item-2");

                                int totalPayload = p1.Length + p2.Length;
                                byte[] resp = new byte[3 + 4 + (2 * 12) + totalPayload];
                                resp[0] = ProtocolConstants.MagicByte0;
                                resp[1] = ProtocolConstants.MagicByte1;
                                resp[2] = ProtocolConstants.StatusData;

                                BinaryPrimitives.WriteUInt32BigEndian(resp.AsSpan(3, 4), 2); // 2 entries

                                // Entry 1
                                BinaryPrimitives.WriteUInt64BigEndian(resp.AsSpan(7, 8), startOffset);
                                BinaryPrimitives.WriteUInt32BigEndian(resp.AsSpan(15, 4), (uint)p1.Length);

                                // Entry 2
                                BinaryPrimitives.WriteUInt64BigEndian(resp.AsSpan(19, 8), startOffset + 1);
                                BinaryPrimitives.WriteUInt32BigEndian(resp.AsSpan(27, 4), (uint)p2.Length);

                                // Concatenated payloads
                                p1.CopyTo(resp.AsSpan(31));
                                p2.CopyTo(resp.AsSpan(31 + p1.Length));

                                await stream.WriteAsync(resp, ct);
                                await stream.FlushAsync(ct);
                            }
                        }
                    }
                }
                catch { }
            }, ct);
        }
    }

    [Fact]
    public async Task ProduceAsync_WithValidAuth_ReturnsAssignedOffset()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken
        };

        await using var client = new AeroClient(options);
        long offset = await client.PublishAsync("orders", 0, "order-payload-1");
        Assert.True(offset > 100);
    }

    [Fact]
    public async Task ProduceAsync_WithInvalidAuth_ThrowsAuthenticationException()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = "wrong-token"
        };

        await using var client = new AeroClient(options);
        await Assert.ThrowsAsync<AuthenticationException>(async () =>
        {
            await client.PublishAsync("orders", 0, "test-data");
        });
    }

    [Fact]
    public async Task ProduceAsync_OutOfOrder_ThrowsOutOfOrderSequenceException()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken
        };

        await using var client = new AeroClient(options);
        await Assert.ThrowsAsync<OutOfOrderSequenceException>(async () =>
        {
            await client.PublishAsync("fail-out-of-order", 0, "out-of-order-data");
        });
    }

    [Fact]
    public async Task FetchAsync_RetrievesDataAndEmptyResponses()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken
        };

        await using var client = new AeroClient(options);
        using var consumer = client.CreateConsumer();

        var data = await consumer.FetchAsync("events", 0, 100);
        Assert.NotNull(data);
        Assert.Equal("sample-fetch-payload", Encoding.UTF8.GetString(data.Value.Span));

        var empty = await consumer.FetchAsync("empty-topic", 0, 100);
        Assert.Null(empty);
    }

    [Fact]
    public async Task FetchMultiAsync_RetrievesBatchRecords()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken
        };

        await using var client = new AeroClient(options);
        using var consumer = client.CreateConsumer();

        var records = await consumer.FetchMultiAsync("telemetry", 0, startOffset: 200);
        Assert.Equal(2, records.Count);
        Assert.Equal(200L, records[0].Offset);
        Assert.Equal("item-1", records[0].GetPayloadString());
        Assert.Equal(201L, records[1].Offset);
        Assert.Equal("item-2", records[1].GetPayloadString());
    }

    [Fact]
    public async Task StreamAsync_ConsumesRecordsContinuously()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken
        };

        await using var client = new AeroClient(options);
        using var consumer = client.CreateConsumer();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var consumed = new List<AeroRecord>();

        await foreach (var record in consumer.StreamAsync("events", 0, startOffset: 499, cts.Token))
        {
            consumed.Add(record);
            if (consumed.Count >= 2) break;
        }

        Assert.True(consumed.Count >= 2);
        Assert.Equal("item-1", consumed[0].GetPayloadString());
    }

    [Fact]
    public async Task Producer_SemaphoreBackpressure_UnderConcurrentSends()
    {
        var options = new AeroClientOptions
        {
            BootstrapServers = new[] { $"127.0.0.1:{_port}" },
            Token = _validToken,
            MaxInFlightRequests = 4
        };

        await using var client = new AeroClient(options);
        using var producer = client.CreateProducer();

        var tasks = Enumerable.Range(0, 20)
            .Select(i => producer.SendAsync("concurrent-topic", 0, $"message-{i}").AsTask())
            .ToArray();

        long[] results = await Task.WhenAll(tasks);
        Assert.Equal(20, results.Length);
        Assert.All(results, offset => Assert.True(offset > 100));
    }
}
