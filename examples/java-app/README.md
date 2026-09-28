# AeroStream Java Application Example

This project demonstrates 100% wire-protocol compatibility between the official **Apache Kafka Java Client** (`org.apache.kafka:kafka-clients`) and **AeroStream**.

## Features Demonstrated
1. **Cluster Administration (`AdminClient`)**: Cluster metadata discovery, broker identification, and dynamic topic creation.
2. **High-Speed Producer (`KafkaProducer`)**: Asynchronous batching with record headers, callbacks, and partitioned message routing.
3. **Consumer Group (`KafkaConsumer`)**: Dynamic consumer group coordination, partition assignment, auto-commit, and message polling.

## Prerequisites
- Java 17+ (or Java 21/25)
- Apache Maven 3.8+
- Running AeroStream Broker on `127.0.0.1:9093` (or `127.0.0.1:9092`)

## Building and Running

```bash
# 1. Compile the project
mvn clean compile

# 2. Execute the application test suite
mvn exec:java

# Optional: specify custom broker bootstrap servers
mvn exec:java -Dexec.args="localhost:9092"
```
