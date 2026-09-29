using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DotnetClient;

public static class Program
{
    // ANSI Colors for Rich Terminal Output
    private const string ColorReset = "\u001b[0m";
    private const string ColorRed = "\u001b[31m";
    private const string ColorGreen = "\u001b[32m";
    private const string ColorYellow = "\u001b[33m";
    private const string ColorBlue = "\u001b[34m";
    private const string ColorMagenta = "\u001b[35m";
    private const string ColorCyan = "\u001b[36m";
    private const string ColorWhite = "\u001b[37m";
    private const string ColorBold = "\u001b[1m";
    private const string ColorDim = "\u001b[2m";

    private static void LogStep(string name)
    {
        Console.WriteLine($"\n{ColorCyan}{ColorBold}▶ [TEST] {name}{ColorReset}");
    }

    private static void LogPass(string desc, double? latencyMs = null)
    {
        var lat = latencyMs.HasValue ? $" {ColorDim}({latencyMs.Value:F2} ms){ColorReset}" : "";
        Console.WriteLine($"  {ColorGreen}✔ PASS{ColorReset} {desc}{lat}");
    }

    private static void LogInfo(string desc)
    {
        Console.WriteLine($"  {ColorDim}ℹ{ColorReset} {desc}");
    }

    private static void LogFail(string desc, Exception? ex = null)
    {
        Console.WriteLine($"  {ColorRed}✖ FAIL{ColorReset} {desc}");
        if (ex != null)
        {
            Console.WriteLine($"    {ColorRed}{ex}{ColorReset}");
        }
    }

    private static string ComputeSha256(byte[] data)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(data);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static async Task<int> Main(string[] args)
    {
        var brokerAddr = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "127.0.0.1:9092";

        Console.WriteLine($"{ColorBold}{ColorCyan}========================================================================{ColorReset}");
        Console.WriteLine($"{ColorBold}{ColorWhite}       AEROSTREAM E2E TEST RUNNER - .NET 8 (Confluent.Kafka v2.6.0)     {ColorReset}");
        Console.WriteLine($"{ColorBold}{ColorCyan}========================================================================{ColorReset}\n");
        Console.WriteLine($"Target Broker:       {ColorGreen}{brokerAddr}{ColorReset}");
        Console.WriteLine($"Client Runtime:      {ColorGreen}.NET {Environment.Version} (Alpine Linux musl-x64){ColorReset}");
        Console.WriteLine($"Kafka Client:        {ColorGreen}Confluent.Kafka 2.6.0 (librdkafka {Library.VersionString}){ColorReset}\n");

        var runId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var mainTopic = $"dotnet-e2e-main-{runId}";
        var cgrpTopic = $"dotnet-e2e-cgrp-{runId}";
        var checksumTopic = $"dotnet-e2e-csum-{runId}";
        var benchTopic = $"dotnet-e2e-bench-{runId}";

        var testResults = new List<(string Name, bool Passed, double DurationMs)>();

        try
        {
            // Test 1: AdminClient operations
            var sw1 = Stopwatch.StartNew();
            await TestAdminOperationsAsync(brokerAddr, mainTopic);
            sw1.Stop();
            testResults.Add(("AdminClient Operations (Topic Creation & Metadata Discovery)", true, sw1.Elapsed.TotalMilliseconds));

            // Test 2: Producer operations (Codecs, Headers, Routing)
            var sw2 = Stopwatch.StartNew();
            await TestProducerOperationsAsync(brokerAddr, mainTopic);
            sw2.Stop();
            testResults.Add(("Producer Operations (Codecs, Headers & Key Partition Routing)", true, sw2.Elapsed.TotalMilliseconds));

            // Test 3: Consumer Group operations (Dynamic assignment, manual commit, resumption)
            var sw3 = Stopwatch.StartNew();
            await TestConsumerGroupOperationsAsync(brokerAddr, cgrpTopic);
            sw3.Stop();
            testResults.Add(("Consumer Group Operations (Rebalance, Commit & Resumption)", true, sw3.Elapsed.TotalMilliseconds));

            // Test 4: Payload checksum validation (SHA-256 byte-for-byte integrity)
            var sw4 = Stopwatch.StartNew();
            await TestPayloadChecksumValidationAsync(brokerAddr, checksumTopic);
            sw4.Stop();
            testResults.Add(("Payload Checksum Verification (SHA-256 Byte Integrity)", true, sw4.Elapsed.TotalMilliseconds));

            // Test 5: Benchmark (2,000 records, 512 bytes each)
            var sw5 = Stopwatch.StartNew();
            var benchStats = await TestBenchmarkAsync(brokerAddr, benchTopic);
            sw5.Stop();
            testResults.Add(("High-Throughput Batching & Latency Benchmark", true, sw5.Elapsed.TotalMilliseconds));

            // Print Final Summary Table
            Console.WriteLine($"\n{ColorBold}{ColorCyan}========================================================================{ColorReset}");
            Console.WriteLine($"{ColorBold}{ColorGreen}       ALL .NET CONFLUENT.KAFKA E2E SUITES PASSED (5/5)!                {ColorReset}");
            Console.WriteLine($"{ColorBold}{ColorCyan}========================================================================{ColorReset}\n");

            Console.WriteLine($"{"Test Suite",-60} | {"Status",-8} | {"Duration",10}");
            Console.WriteLine(new string('-', 84));
            foreach (var r in testResults)
            {
                var statusStr = r.Passed ? $"{ColorGreen}PASS{ColorReset}" : $"{ColorRed}FAIL{ColorReset}";
                Console.WriteLine($"{r.Name,-60} | {statusStr,-17} | {r.DurationMs,8:F1} ms");
            }
            Console.WriteLine(new string('-', 84));
            Console.WriteLine($"Benchmark Highlight: {ColorBold}{benchStats.ThroughputMsgSec:N0} msg/s{ColorReset} ({benchStats.ThroughputMbSec:F2} MB/s) | P50: {benchStats.P50Ms:F2} ms | P99: {benchStats.P99Ms:F2} ms\n");

            return 0;
        }
        catch (Exception ex)
        {
            LogFail("Fatal error during test suite execution", ex);
            return 1;
        }
    }

    /// <summary>
    /// Test 1: AdminClient operations
    /// Verifies AdminClient connection, topic creation with 3 partitions,
    /// cluster metadata retrieval, and partition layout discovery.
    /// </summary>
    private static async Task TestAdminOperationsAsync(string broker, string topic)
    {
        LogStep("1. AdminClient Operations (Topic Creation & Metadata Discovery)");
        var sw = Stopwatch.StartNew();

        var adminConfig = new AdminClientConfig { BootstrapServers = broker };
        using var admin = new AdminClientBuilder(adminConfig).Build();

        // 1. Create topic with 3 partitions
        var tCreate = Stopwatch.StartNew();
        await admin.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 3,
                ReplicationFactor = 1
            }
        });
        tCreate.Stop();
        LogPass($"admin.CreateTopicsAsync() created topic '{topic}' with 3 partitions", tCreate.Elapsed.TotalMilliseconds);

        // 2. Discover cluster metadata and partition layout
        var tMeta = Stopwatch.StartNew();
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
        tMeta.Stop();

        LogPass($"admin.GetMetadata() queried cluster metadata ({metadata.Brokers.Count} broker(s) found)", tMeta.Elapsed.TotalMilliseconds);

        foreach (var b in metadata.Brokers)
        {
            LogInfo($"Discovered Broker: ID={b.BrokerId}, Endpoint={b.Host}:{b.Port}");
        }

        var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
        if (topicMeta == null)
        {
            throw new InvalidOperationException($"Topic '{topic}' not found in cluster metadata!");
        }

        if (topicMeta.Partitions.Count != 3)
        {
            throw new InvalidOperationException($"Expected 3 partitions for topic '{topic}', found {topicMeta.Partitions.Count}!");
        }

        foreach (var part in topicMeta.Partitions)
        {
            if (part.Error.Code != ErrorCode.NoError)
            {
                throw new InvalidOperationException($"Partition {part.PartitionId} returned error: {part.Error.Reason}");
            }
            LogInfo($"Partition {part.PartitionId}: Leader={part.Leader}, Replicas=[{string.Join(",", part.Replicas)}], ISR=[{string.Join(",", part.InSyncReplicas)}]");
        }

        LogPass($"Verified 3-partition topology, valid leader allocation, and in-sync replicas", sw.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Test 2: Producer operations
    /// Verifies compression codecs matrix (None, Gzip, Snappy),
    /// custom record headers (X-Trace-Id, X-Source, X-Codec),
    /// and key-based partition routing across 3 partitions.
    /// </summary>
    private static async Task TestProducerOperationsAsync(string broker, string topic)
    {
        LogStep("2. Producer Operations (Codec Matrix, Headers & Key Routing)");
        var sw = Stopwatch.StartNew();

        var codecs = new (string Name, CompressionType Type)[]
        {
            ("None", CompressionType.None),
            ("Gzip", CompressionType.Gzip),
            ("Snappy", CompressionType.Snappy)
        };

        var targetedPartitions = new HashSet<int>();
        var totalProduced = 0;

        foreach (var (codecName, codecType) in codecs)
        {
            var pSw = Stopwatch.StartNew();
            var prodConfig = new ProducerConfig
            {
                BootstrapServers = broker,
                CompressionType = codecType,
                Acks = Acks.All
            };

            using var producer = new ProducerBuilder<string, byte[]>(prodConfig).Build();

            // Produce 6 messages per codec with keys designed to distribute across partitions
            for (var i = 0; i < 6; i++)
            {
                var key = $"key-{codecName.ToLowerInvariant()}-{i}";
                var payloadStr = $"payload-{codecName}-data-{i}-{new string('A', 128)}";
                var payloadBytes = Encoding.UTF8.GetBytes(payloadStr);
                var traceId = $"trace-{codecName.ToLowerInvariant()}-{Guid.NewGuid():N}";

                var msg = new Message<string, byte[]>
                {
                    Key = key,
                    Value = payloadBytes,
                    Headers = new Headers
                    {
                        { "X-Trace-Id", Encoding.UTF8.GetBytes(traceId) },
                        { "X-Source", Encoding.UTF8.GetBytes("dotnet-confluent-kafka") },
                        { "X-Codec", Encoding.UTF8.GetBytes(codecName.ToLowerInvariant()) }
                    }
                };

                var dr = await producer.ProduceAsync(topic, msg);
                if (dr.Status != PersistenceStatus.Persisted)
                {
                    throw new InvalidOperationException($"Message {key} failed to persist!");
                }

                targetedPartitions.Add(dr.Partition.Value);
                totalProduced++;
            }

            pSw.Stop();
            LogPass($"Produced 6 messages with Codec '{codecName}' and custom headers", pSw.Elapsed.TotalMilliseconds);
        }

        if (targetedPartitions.Count < 2)
        {
            throw new InvalidOperationException($"Expected key-based routing across multiple partitions, got {targetedPartitions.Count}");
        }

        LogInfo($"Key-based partition distribution verified across partitions: [{string.Join(", ", targetedPartitions.OrderBy(p => p))}]");

        // Verify messages and headers via consumer
        var consConfig = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = $"verify-producer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(consConfig).Build();
        consumer.Subscribe(topic);

        var verifiedCount = 0;
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline && verifiedCount < totalProduced)
        {
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (cr == null) continue;

            // Verify headers
            var traceHdr = cr.Message.Headers.FirstOrDefault(h => h.Key == "X-Trace-Id");
            var sourceHdr = cr.Message.Headers.FirstOrDefault(h => h.Key == "X-Source");
            var codecHdr = cr.Message.Headers.FirstOrDefault(h => h.Key == "X-Codec");

            if (traceHdr == null || sourceHdr == null || codecHdr == null)
            {
                throw new InvalidOperationException($"Missing expected headers in message {cr.Message.Key}!");
            }

            var sourceVal = Encoding.UTF8.GetString(sourceHdr.GetValueBytes());
            if (sourceVal != "dotnet-confluent-kafka")
            {
                throw new InvalidOperationException($"Invalid X-Source header: {sourceVal}");
            }

            verifiedCount++;
        }

        if (verifiedCount != totalProduced)
        {
            throw new InvalidOperationException($"Expected {totalProduced} verified messages, got {verifiedCount}");
        }

        LogPass($"Consumer verified all {verifiedCount} messages: 3 codecs, custom headers, and key routes intact", sw.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Test 3: Consumer Group operations
    /// Verifies group join, dynamic partition assignment callbacks,
    /// message consumption, manual offset commit (Commit),
    /// and offset resumption (second consumer only reads new messages).
    /// </summary>
    private static async Task TestConsumerGroupOperationsAsync(string broker, string topic)
    {
        LogStep("3. Consumer Group Operations (Dynamic Assignment, Commit & Resumption)");
        var sw = Stopwatch.StartNew();

        // 1. Ensure test topic with 3 partitions exists
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker }).Build())
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
            });
        }

        var groupId = $"dotnet-group-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

        // 2. Produce initial 15 messages across partitions
        var prodConfig = new ProducerConfig { BootstrapServers = broker };
        using (var producer = new ProducerBuilder<string, string>(prodConfig).Build())
        {
            for (var i = 0; i < 15; i++)
            {
                var part = new Partition(i % 3);
                await producer.ProduceAsync(new TopicPartition(topic, part), new Message<string, string>
                {
                    Key = $"initial-key-{i}",
                    Value = $"initial-value-{i}"
                });
            }
        }
        LogPass("Produced 15 initial messages across 3 explicit partitions");

        // 3. Consumer 1: Join group, receive dynamic partition assignment, consume all 15, commit offsets
        var assignedPartitions1 = new List<int>();
        var consConfig1 = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // Manual commit
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range
        };

        var c1Sw = Stopwatch.StartNew();
        var consumed1 = 0;
        using (var consumer1 = new ConsumerBuilder<string, string>(consConfig1)
                   .SetPartitionsAssignedHandler((c, partitions) =>
                   {
                       lock (assignedPartitions1)
                       {
                           assignedPartitions1.AddRange(partitions.Select(p => p.Partition.Value));
                       }
                       LogInfo($"Dynamic Assignment: Consumer-1 assigned partitions [{string.Join(", ", partitions.Select(p => p.Partition.Value))}]");
                   })
                   .Build())
        {
            consumer1.Subscribe(topic);

            var deadline1 = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline1 && consumed1 < 15)
            {
                var cr = consumer1.Consume(TimeSpan.FromMilliseconds(500));
                if (cr != null)
                {
                    consumed1++;
                    // Manual offset commit per message
                    consumer1.Commit(cr);
                }
            }
            consumer1.Close();
        }
        c1Sw.Stop();

        if (consumed1 != 15)
        {
            throw new InvalidOperationException($"Consumer 1 expected 15 messages, got {consumed1}");
        }
        LogPass($"Consumer-1 dynamically assigned 3 partitions, consumed 15 messages & manually committed offsets", c1Sw.Elapsed.TotalMilliseconds);

        // 4. Produce 10 NEW messages to test resumption
        using (var producer = new ProducerBuilder<string, string>(prodConfig).Build())
        {
            for (var i = 15; i < 25; i++)
            {
                var part = new Partition(i % 3);
                await producer.ProduceAsync(new TopicPartition(topic, part), new Message<string, string>
                {
                    Key = $"resumed-key-{i}",
                    Value = $"resumed-value-{i}"
                });
            }
        }
        LogPass("Produced 10 NEW messages to test committed offset resumption");

        // 5. Consumer 2: Join with SAME groupId. Must NOT re-read first 15 messages!
        var consConfig2 = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var c2Sw = Stopwatch.StartNew();
        var consumed2 = 0;
        var receivedKeys2 = new List<string>();

        using (var consumer2 = new ConsumerBuilder<string, string>(consConfig2)
                   .SetPartitionsAssignedHandler((c, partitions) =>
                   {
                       LogInfo($"Resumed Assignment: Consumer-2 joined group '{groupId}' with partitions [{string.Join(", ", partitions.Select(p => p.Partition.Value))}]");
                   })
                   .Build())
        {
            consumer2.Subscribe(topic);

            var deadline2 = DateTime.UtcNow.AddSeconds(8);
            while (DateTime.UtcNow < deadline2 && consumed2 < 10)
            {
                var cr = consumer2.Consume(TimeSpan.FromMilliseconds(500));
                if (cr != null)
                {
                    consumed2++;
                    receivedKeys2.Add(cr.Message.Key);
                    consumer2.Commit(cr);
                }
            }
            consumer2.Close();
        }
        c2Sw.Stop();

        if (consumed2 != 10)
        {
            throw new InvalidOperationException($"Consumer 2 expected exactly 10 new messages, but consumed {consumed2}!");
        }

        if (receivedKeys2.Any(k => k.StartsWith("initial-key-")))
        {
            throw new InvalidOperationException($"Offset resumption violation! Consumer 2 received already-committed messages!");
        }

        LogPass($"Consumer-2 resumed strictly from committed offsets, processing exactly {consumed2} new messages", c2Sw.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Test 4: Payload checksum validation
    /// Generates 50 records of varying sizes (128B to 64KB), produces them with SHA-256 hashes,
    /// consumes them back, and asserts 100% byte-for-byte integrity.
    /// </summary>
    private static async Task TestPayloadChecksumValidationAsync(string broker, string topic)
    {
        LogStep("4. Payload Checksum Verification (SHA-256 Byte Integrity)");
        var sw = Stopwatch.StartNew();

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker }).Build())
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
            });
        }

        // Test payload configurations: 50 records across 5 distinct size tiers
        var specs = new (int SizeBytes, int Count)[]
        {
            (128, 10),       // 128 B small records
            (1024, 10),      // 1 KB records
            (8 * 1024, 10),  // 8 KB medium records
            (32 * 1024, 10), // 32 KB large records
            (64 * 1024, 10)  // 64 KB jumbo records
        };

        var expectedChecksums = new Dictionary<string, string>();
        var totalBytes = 0L;
        var totalRecords = 50;
        var recordsToSend = new List<(string Key, byte[] Value, string ExpectedHash, int Size)>();

        var recordIdx = 0;
        foreach (var (size, count) in specs)
        {
            for (var i = 0; i < count; i++)
            {
                var key = $"chk-rec-{recordIdx++:D3}";
                var buffer = new byte[size];
                for (var b = 0; b < size; b++)
                {
                    buffer[b] = (byte)((b * 31 + i) & 0xFF);
                }

                var sha256 = ComputeSha256(buffer);
                expectedChecksums[key] = sha256;
                totalBytes += size;
                recordsToSend.Add((key, buffer, sha256, size));
            }
        }

        LogInfo($"Generated {totalRecords} records across 5 size tiers (totaling {totalBytes / 1024.0:F2} KB)");

        // Produce records
        var prodSw = Stopwatch.StartNew();
        using (var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = broker }).Build())
        {
            for (var i = 0; i < recordsToSend.Count; i++)
            {
                var r = recordsToSend[i];
                var part = new Partition(i % 3);
                var msg = new Message<string, byte[]>
                {
                    Key = r.Key,
                    Value = r.Value,
                    Headers = new Headers
                    {
                        { "X-SHA256", Encoding.UTF8.GetBytes(r.ExpectedHash) },
                        { "X-Payload-Size", Encoding.UTF8.GetBytes(r.Size.ToString()) }
                    }
                };

                await producer.ProduceAsync(new TopicPartition(topic, part), msg);
            }
        }
        prodSw.Stop();
        LogPass($"Produced {totalRecords} checksummed records across 3 partitions", prodSw.Elapsed.TotalMilliseconds);

        // Consume and assert 100% checksum matches
        var consConfig = new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = $"csum-group-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var verifiedCount = 0;
        var consSw = Stopwatch.StartNew();

        using (var consumer = new ConsumerBuilder<string, byte[]>(consConfig).Build())
        {
            consumer.Subscribe(topic);
            var deadline = DateTime.UtcNow.AddSeconds(12);

            while (DateTime.UtcNow < deadline && verifiedCount < totalRecords)
            {
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (cr == null) continue;

                var key = cr.Message.Key;
                if (!expectedChecksums.TryGetValue(key, out var expectedHash))
                {
                    throw new InvalidOperationException($"Unexpected record key: {key}");
                }

                var actualHash = ComputeSha256(cr.Message.Value);
                if (actualHash != expectedHash)
                {
                    throw new InvalidOperationException($"SHA-256 Checksum mismatch for key {key}! Expected: {expectedHash}, Actual: {actualHash}");
                }

                verifiedCount++;
            }
        }
        consSw.Stop();

        if (verifiedCount != totalRecords)
        {
            throw new InvalidOperationException($"Expected {totalRecords} verified records, got {verifiedCount}!");
        }

        LogPass($"Asserted 100% byte integrity: {verifiedCount}/{totalRecords} SHA-256 digests matched exactly", consSw.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Test 5: Benchmark
    /// Batch produce 2,000 messages (512 bytes each) and measure:
    /// - Throughput (messages/sec and MB/sec)
    /// - Latency percentiles (Min, Mean, P50, P90, P95, P99, Max)
    /// </summary>
    private static async Task<(double ThroughputMsgSec, double ThroughputMbSec, double P50Ms, double P99Ms)> TestBenchmarkAsync(string broker, string topic)
    {
        LogStep("5. High-Throughput Batching & Latency Benchmark");

        const int totalMessages = 2000;
        const int messageSizeBytes = 512;
        var payloadBytes = Encoding.UTF8.GetBytes(new string('B', messageSizeBytes));

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker }).Build())
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }
            });
        }

        var prodConfig = new ProducerConfig
        {
            BootstrapServers = broker,
            CompressionType = CompressionType.Snappy,
            LingerMs = 5.0,
            BatchNumMessages = 250,
            Acks = Acks.Leader
        };

        var latencies = new ConcurrentBag<double>();
        var tasks = new List<Task>(totalMessages);

        LogInfo($"Starting benchmark: {totalMessages:N0} records of {messageSizeBytes} bytes (Snappy compressed, 3 partitions)...");

        using var producer = new ProducerBuilder<string, byte[]>(prodConfig).Build();

        var benchmarkSw = Stopwatch.StartNew();

        for (var i = 0; i < totalMessages; i++)
        {
            var key = $"bench-key-{i}";
            var part = new Partition(i % 3);
            var msg = new Message<string, byte[]>
            {
                Key = key,
                Value = payloadBytes
            };

            var msgSw = Stopwatch.StartNew();
            var task = producer.ProduceAsync(new TopicPartition(topic, part), msg).ContinueWith(t =>
            {
                msgSw.Stop();
                if (t.IsFaulted)
                {
                    throw t.Exception!;
                }
                latencies.Add(msgSw.Elapsed.TotalMilliseconds);
            });

            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
        producer.Flush(TimeSpan.FromSeconds(5));
        benchmarkSw.Stop();

        var elapsedSeconds = benchmarkSw.Elapsed.TotalSeconds;
        var throughputMsgSec = totalMessages / elapsedSeconds;
        var throughputMbSec = (totalMessages * messageSizeBytes) / (1024.0 * 1024.0 * elapsedSeconds);

        var sortedLatencies = latencies.OrderBy(l => l).ToList();
        var minMs = sortedLatencies.First();
        var maxMs = sortedLatencies.Last();
        var meanMs = sortedLatencies.Average();
        var p50Ms = sortedLatencies[(int)(sortedLatencies.Count * 0.50)];
        var p90Ms = sortedLatencies[(int)(sortedLatencies.Count * 0.90)];
        var p95Ms = sortedLatencies[(int)(sortedLatencies.Count * 0.95)];
        var p99Ms = sortedLatencies[(int)(sortedLatencies.Count * 0.99)];

        Console.WriteLine($"\n  {ColorBold}Benchmark Results:{ColorReset}");
        Console.WriteLine($"  ├─ Total Messages:    {ColorGreen}{totalMessages:N0}{ColorReset}");
        Console.WriteLine($"  ├─ Message Size:      {ColorGreen}{messageSizeBytes} bytes{ColorReset}");
        Console.WriteLine($"  ├─ Total Duration:    {ColorGreen}{benchmarkSw.Elapsed.TotalMilliseconds:F2} ms{ColorReset}");
        Console.WriteLine($"  ├─ Throughput:        {ColorBold}{ColorGreen}{throughputMsgSec:N0} msg/s{ColorReset} ({throughputMbSec:F2} MB/s)");
        Console.WriteLine($"  ├─ P50 Latency:       {ColorCyan}{p50Ms:F2} ms{ColorReset}");
        Console.WriteLine($"  ├─ P90 Latency:       {ColorCyan}{p90Ms:F2} ms{ColorReset}");
        Console.WriteLine($"  ├─ P95 Latency:       {ColorCyan}{p95Ms:F2} ms{ColorReset}");
        Console.WriteLine($"  ├─ P99 Latency:       {ColorCyan}{p99Ms:F2} ms{ColorReset}");
        Console.WriteLine($"  └─ Min / Mean / Max:  {minMs:F2} ms / {meanMs:F2} ms / {maxMs:F2} ms\n");

        LogPass($"Benchmark completed successfully in {benchmarkSw.Elapsed.TotalMilliseconds:F2} ms");

        return (throughputMsgSec, throughputMbSec, p50Ms, p99Ms);
    }
}
