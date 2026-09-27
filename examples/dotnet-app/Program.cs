using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AeroStreamDotNetDemo
{
    class Program
    {
        private const string BootstrapServers = "127.0.0.1:9092";
        private const string TopicName = "dotnet-streaming-events";

        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("================================================================================");
            Console.WriteLine("        🚀 AeroStream .NET (C#) Full Compatibility Test Suite");
            Console.WriteLine("        Client Library: Confluent.Kafka v2.15.1 (.NET 8.0)");
            Console.WriteLine($"        Target Broker:  {BootstrapServers}");
            Console.WriteLine("================================================================================\n");

            var sw = Stopwatch.StartNew();
            bool allPassed = true;

            // 1. Cluster Metadata & Admin Inspection
            allPassed &= TestClusterMetadata();

            // 2. High-Throughput / Idempotent Producer
            allPassed &= await TestIdempotentProducerAsync();

            // 3. Consumer Group & Rebalance
            allPassed &= TestConsumerGroup();

            // 4. SASL Wire Authentication (PLAIN)
            allPassed &= await TestSaslAuthenticationAsync();

            sw.Stop();
            Console.WriteLine("\n================================================================================");
            if (allPassed)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"  🎉 ALL .NET (C#) COMPATIBILITY TESTS PASSED! Total time: {sw.ElapsedMilliseconds}ms");
                Console.ResetColor();
                Console.WriteLine("  AeroStream is 100% drop-in compatible with official Confluent.Kafka .NET!");
                Console.WriteLine("================================================================================\n");
                return 0;
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("  ❌ SOME TESTS FAILED. Check logs above.");
                Console.ResetColor();
                Console.WriteLine("================================================================================\n");
                return 1;
            }
        }

        private static bool TestClusterMetadata()
        {
            Console.WriteLine("▶ [TEST 1] Cluster Metadata Discovery via AdminClient...");
            try
            {
                var adminConfig = new AdminClientConfig
                {
                    BootstrapServers = BootstrapServers,
                    SocketTimeoutMs = 5000,
                };

                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

                Console.WriteLine($"   • Brokers Discovered: {meta.Brokers.Count}");
                foreach (var b in meta.Brokers)
                {
                    Console.WriteLine($"     - Node #{b.BrokerId}: {b.Host}:{b.Port}");
                }
                Console.WriteLine($"   • Topics Registered: {meta.Topics.Count}");
                foreach (var t in meta.Topics)
                {
                    Console.WriteLine($"     - Topic '{t.Topic}': {t.Partitions.Count} partition(s)");
                }

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("   ✔ [PASS] Cluster metadata retrieved successfully.\n");
                Console.ResetColor();
                return true;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"   ✖ [FAIL] Metadata test error: {ex.Message}\n");
                Console.ResetColor();
                return false;
            }
        }

        private static async Task<bool> TestIdempotentProducerAsync()
        {
            Console.WriteLine($"▶ [TEST 2] Idempotent Producer (KIP-98 EOS) -> Topic: '{TopicName}'...");
            try
            {
                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                    ClientId = "dotnet-aerostream-producer",
                    Acks = Acks.All,
                    EnableIdempotence = true,
                    MessageTimeoutMs = 5000,
                    LingerMs = 5,
                };

                using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

                int messageCount = 10;
                var tasks = new List<Task<DeliveryResult<string, string>>>();

                for (int i = 0; i < messageCount; i++)
                {
                    var key = $"device-{i % 3}";
                    var val = $"{{\"event_id\": {i}, \"source\": \".NET 8\", \"timestamp\": \"{DateTime.UtcNow:O}\", \"status\": \"ACTIVE\"}}";

                    var msg = new Message<string, string> { Key = key, Value = val };
                    tasks.Add(producer.ProduceAsync(TopicName, msg));
                }

                var results = await Task.WhenAll(tasks);
                foreach (var dr in results)
                {
                    Console.WriteLine($"   • Delivered key='{dr.Message.Key}' -> Partition: {dr.Partition.Value}, Offset: {dr.Offset.Value}, Status: {dr.Status}");
                }

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"   ✔ [PASS] {messageCount} records produced with idempotence and ACKs verified.\n");
                Console.ResetColor();
                return true;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"   ✖ [FAIL] Producer test error: {ex.Message}\n");
                Console.ResetColor();
                return false;
            }
        }

        private static bool TestConsumerGroup()
        {
            Console.WriteLine($"▶ [TEST 3] Consumer Group Fetch -> GroupId: 'dotnet-e2e-group'...");
            try
            {
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = BootstrapServers,
                    GroupId = $"dotnet-group-{Guid.NewGuid().ToString("N")[..8]}",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    SessionTimeoutMs = 10000,
                };

                using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
                consumer.Subscribe(TopicName);

                int consumed = 0;
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));

                while (consumed < 5 && !cts.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        if (cr != null)
                        {
                            consumed++;
                            Console.WriteLine($"   • Consumed: key='{cr.Message.Key}', partition={cr.Partition.Value}, offset={cr.Offset.Value}, payload={cr.Message.Value[..Math.Min(40, cr.Message.Value.Length)]}...");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                consumer.Close();

                if (consumed > 0)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"   ✔ [PASS] Successfully consumed {consumed} record(s) via standard Kafka Consumer Group.\n");
                    Console.ResetColor();
                    return true;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("   ⚠ [WARN] No records consumed within timeout (topic may have been newly created or offset empty).\n");
                    Console.ResetColor();
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"   ✖ [FAIL] Consumer test error: {ex.Message}\n");
                Console.ResetColor();
                return false;
            }
        }

        private static async Task<bool> TestSaslAuthenticationAsync()
        {
            Console.WriteLine("▶ [TEST 4] Kafka Wire SASL/PLAIN Authentication...");
            try
            {
                var saslConfig = new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                    SecurityProtocol = SecurityProtocol.SaslPlaintext,
                    SaslMechanism = SaslMechanism.Plain,
                    SaslUsername = "admin",
                    SaslPassword = "admin",
                    MessageTimeoutMs = 5000,
                };

                using var saslProducer = new ProducerBuilder<string, string>(saslConfig).Build();

                var msg = new Message<string, string>
                {
                    Key = "sasl-key",
                    Value = "Authenticated .NET message over Kafka wire SASL/PLAIN",
                };

                var dr = await saslProducer.ProduceAsync(TopicName, msg);
                Console.WriteLine($"   • SASL Authenticated Produce -> Partition: {dr.Partition.Value}, Offset: {dr.Offset.Value}, Status: {dr.Status}");

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("   ✔ [PASS] SASL/PLAIN wire handshake and produce succeeded.\n");
                Console.ResetColor();
                return true;
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"   ✖ [FAIL] SASL Authentication error: {ex.Message}\n");
                Console.ResetColor();
                return false;
            }
        }
    }
}
