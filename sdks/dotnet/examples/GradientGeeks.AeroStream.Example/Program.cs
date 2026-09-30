using GradientGeeks.AeroStream.Client.Client;
using GradientGeeks.AeroStream.Client.Models;

Console.WriteLine("=== AeroStream .NET C# Client Example ===");

var options = new AeroClientOptions
{
    BootstrapServers = new[] { "127.0.0.1:9091" },
    Token = "secret-token",
    RequestTimeout = TimeSpan.FromSeconds(5)
};

await using var client = new AeroClient(options);

try
{
    // 1. Produce records
    Console.WriteLine("Producing records to 'iot-sensors', partition 0...");
    using var producer = client.CreateProducer();
    for (int i = 0; i < 5; i++)
    {
        string payload = $"{{\"sensor\": \"temp-{i}\", \"value\": {20.5 + i}, \"ts\": {DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}";
        long offset = await producer.SendAsync("iot-sensors", 0, payload);
        Console.WriteLine($"[Produced] Offset: {offset} -> {payload}");
    }

    // 2. Consume records via IAsyncEnumerable
    Console.WriteLine("\nStreaming records from 'iot-sensors', starting at offset 0...");
    using var consumer = client.CreateConsumer();
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

    try
    {
        await foreach (AeroRecord record in consumer.StreamAsync("iot-sensors", 0, startOffset: 0, cts.Token))
        {
            Console.WriteLine($"[Consumed] Offset: {record.Offset} | Payload: {record.GetPayloadString()}");
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Streaming demo timed out as expected.");
    }

    Console.WriteLine("Example completed successfully.");
}
catch (Exception ex)
{
    Console.WriteLine($"[Note] Broker at 127.0.0.1:9091 was unreachable: {ex.Message}");
    Console.WriteLine("Ensure AeroStream broker is running on port 9091 before running this example.");
}
