use std::time::Duration;
use tokio_stream::StreamExt;
use aerostream_client::{AeroClient, ClientConfig, ConsumerConfig, ProducerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize console tracing
    tracing_subscriber::fmt::init();

    let broker_addr = std::env::var("AEROSTREAM_BROKER").unwrap_or_else(|_| "127.0.0.1:9091".to_string());
    println!("Connecting to AeroStream broker at {broker_addr}...");

    // 1. Initialize client with optional authentication
    let client_config = ClientConfig::builder()
        .bootstrap_server(&broker_addr)
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let client = AeroClient::new(client_config);

    // 2. Publish messages using AeroProducer
    let producer_config = ProducerConfig::builder()
        .bootstrap_server(&broker_addr)
        .max_retries(3)
        .initial_backoff(Duration::from_millis(50))
        .build()?;
    let producer = client.producer_with_config(producer_config);

    let topic = "market-ticks";
    let partition = 0;

    println!("Publishing messages to topic '{topic}', partition {partition}...");
    for i in 1..=5 {
        let payload = format!(r#"{{"symbol":"AAPL","price":{}.50,"tick":{}}}"#, 180 + i, i);
        let offset = producer.send(topic, partition, payload.as_bytes()).await?;
        println!("  -> Sent tick #{i} successfully! Broker assigned offset: {offset}");
    }

    // 3. Consume messages using AeroConsumer with fetch_multi
    println!("\nConsuming batch using fetch_multi (Command 4)...");
    let consumer_config = ConsumerConfig::builder()
        .bootstrap_server(&broker_addr)
        .topic(topic)
        .partition(partition)
        .initial_offset(0)
        .max_bytes(1024 * 1024)
        .max_wait_ms(500)
        .build()?;
    let consumer = client.consumer_with_config(consumer_config);

    let records = consumer
        .fetch_multi(topic, partition, 0, 1024 * 1024, 500)
        .await?;

    println!("  -> Received {} records:", records.len());
    for record in &records {
        let body = String::from_utf8_lossy(&record.payload);
        println!("     [offset {}]: {}", record.offset, body);
    }

    // 4. Demonstrate continuous streaming
    println!("\nStarting continuous consumer stream (reading next 2 records)...");
    let mut stream = consumer.stream();

    // Spawn a background producer to feed the stream
    let prod_clone = producer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 6..=7 {
            let payload = format!(r#"{{"symbol":"AAPL","price":188.00,"tick":{}}}"#, i);
            let _ = prod_clone.send(topic, partition, payload.as_bytes()).await;
        }
    });

    let mut stream_count = 0;
    while let Some(item) = stream.next().await {
        let record = item?;
        let body = String::from_utf8_lossy(&record.payload);
        println!("  -> [Stream Record @ offset {}]: {}", record.offset, body);
        stream_count += 1;
        if stream_count >= 2 {
            break;
        }
    }

    println!("\nAeroStream SDK demonstration completed successfully!");
    Ok(())
}
