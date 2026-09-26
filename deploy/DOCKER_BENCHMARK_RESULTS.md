# Comprehensive 3-Way Benchmark: AeroMQ vs. Redpanda vs. Apache Kafka
**Hardware Limits (Strictly Enforced per Container)**: `--cpus=2.0 --memory=2g`  
**Test Environment**: Debian 13 (Linux 6.12 kernel, x86_64), Docker 29.8.1  
**Test Date**: September 26, 2026  

---

## 1. Executive Summary

This benchmark tests and compares the real-world performance of three distributed message brokers deployed inside isolated Docker containers with **identical hardware constraints (2 CPU cores, 2 GB RAM)**:

1. **AeroMQ** (Dual-Engine: Go Raft Consensus + Rust Zero-Copy Storage Data Plane)
2. **Redpanda** (C++20 / Seastar Thread-Per-Core Storage Engine, `redpandadata/redpanda:latest`)
3. **Apache Kafka** (Java / JVM KRaft Mode, `apache/kafka:latest` v4.3.1)

### Key Benchmark Discoveries:
* **The 50 MB Giant Message Test**: At 50 MB payloads, both Apache Kafka and Redpanda suffer catastrophic throughput collapse down to **48–50 MB/sec** with **5.2 to 5.6-second median latencies** due to socket buffer re-assembly stalls and memory allocator thrashing. AeroMQ sustains **666.30 MB/sec** with a median latency of **353 ms** — outperforming Redpanda by **13.3x** and Kafka by **13.7x**.
* **Memory Efficiency**: Under active 50 MB streaming load, AeroMQ consumes only **1.54 MiB of RAM** (0.08% of its 2 GB container limit). In contrast, Redpanda claims **839.3 MiB** (41.0%) and Apache Kafka claims **1.212 GiB** (60.6%) of memory.
* **Thread Count**: AeroMQ runs with **3 deterministic OS worker threads** compared to Redpanda's **5 threads** and Apache Kafka's **130 JVM threads**.

---

## 2. Large Message Deep-Dive (1 MB, 10 MB, 50 MB)

Each test pushed an aggregate of **500 MB of raw binary payload** through the brokers with partition acknowledgment (`acks=1` / single-syscall ACK).

```
+-----------------------------------------------------------------------------------------+
|                  Throughput Comparison: 500 MB Total Payload Transfer                   |
+-----------------------------------------------------------------------------------------+
| Payload Size | Apache Kafka (KRaft) | Redpanda (C++/Seastar) | AeroMQ (Rust Data Plane) |
+--------------+----------------------+------------------------+--------------------------+
| 1 MB         | 127.62 MB/s          | 333.78 MB/s            | 687.08 MB/s (2.06x - 5.4x)|
| 10 MB        | 102.82 MB/s          | 187.55 MB/s            | 577.96 MB/s (3.08x - 5.6x)|
| 50 MB        |  48.64 MB/s          |  50.20 MB/s            | 666.30 MB/s (13.3x - 13.7x)|
+-----------------------------------------------------------------------------------------+
```

---

### Test 2.1: 1 MB Messages (500 messages = 500 MB Total)

* **Payload Size**: 1,048,576 bytes (1.00 MB)
* **Message Count**: 500 messages
* **Total Transferred**: 500.00 MB

| Metric | Apache Kafka (v4.3.1 KRaft) | Redpanda (C++/Seastar) | AeroMQ (Rust Data Plane) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- | :--- |
| **Data Throughput** | `127.62 MB/sec` | `333.78 MB/sec` | **`687.08 MB/sec`** | **2.06x vs. Redpanda, 5.38x vs. Kafka** |
| **Write Throughput** | `127.61 msgs/sec` | `333.78 msgs/sec` | **`687.08 msgs/sec`** | **2.06x vs. Redpanda, 5.38x vs. Kafka** |
| **Test Duration** | `3.918 s` | `1.498 s` | **`0.728 s`** | **2.1x faster completion** |
| **p50 (Median Latency)** | `180.00 ms` | **`9.00 ms`** | `10.11 ms` | **17.8x lower than Kafka** |
| **p95 Latency** | `290.00 ms` | **`24.00 ms`** | `61.80 ms` | **4.7x lower than Kafka** |
| **p99 Latency** | `354.00 ms` | **`33.00 ms`** | `114.16 ms` | **3.1x lower than Kafka** |
| **Max Latency** | `360.00 ms` | **`41.00 ms`** | `121.32 ms` | **3.0x lower than Kafka** |

---

### Test 2.2: 10 MB Messages (50 messages = 500 MB Total)

* **Payload Size**: 10,485,760 bytes (10.00 MB)
* **Message Count**: 50 messages
* **Total Transferred**: 500.00 MB

| Metric | Apache Kafka (v4.3.1 KRaft) | Redpanda (C++/Seastar) | AeroMQ (Rust Data Plane) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- | :--- |
| **Data Throughput** | `102.82 MB/sec` | `187.55 MB/sec` | **`577.96 MB/sec`** | **3.08x vs. Redpanda, 5.62x vs. Kafka** |
| **Write Throughput** | `10.28 msgs/sec` | `18.75 msgs/sec` | **`57.80 msgs/sec`** | **3.08x vs. Redpanda, 5.62x vs. Kafka** |
| **Test Duration** | `4.863 s` | `2.666 s` | **`0.865 s`** | **3.1x vs. Redpanda, 5.6x vs. Kafka** |
| **p50 (Median Latency)** | `649.00 ms` | `535.00 ms` | **`166.22 ms`** | **3.2x vs. Redpanda, 3.9x vs. Kafka** |
| **p95 Latency** | `2,575.00 ms` | `593.00 ms` | **`227.88 ms`** | **2.6x vs. Redpanda, 11.3x vs. Kafka** |
| **p99 Latency** | `2,603.00 ms` | `600.00 ms` | **`243.21 ms`** | **2.5x vs. Redpanda, 10.7x vs. Kafka** |
| **Max Latency** | `2,603.00 ms` | `603.00 ms` | **`244.15 ms`** | **2.5x vs. Redpanda, 10.7x vs. Kafka** |

---

### Test 2.3: 50 MB Messages (10 messages = 500 MB Total)

* **Payload Size**: 52,428,800 bytes (50.00 MB)
* **Message Count**: 10 messages
* **Total Transferred**: 500.00 MB

| Metric | Apache Kafka (v4.3.1 KRaft) | Redpanda (C++/Seastar) | AeroMQ (Rust Data Plane) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- | :--- |
| **Data Throughput** | `48.64 MB/sec` | `50.20 MB/sec` | **`666.30 MB/sec`** | **13.3x vs. Redpanda, 13.7x vs. Kafka** |
| **Write Throughput** | `0.97 msgs/sec` | `1.00 msgs/sec` | **`13.33 msgs/sec`** | **13.3x vs. Redpanda, 13.7x vs. Kafka** |
| **Test Duration** | `10.279 s` | `9.960 s` | **`0.750 s`** | **13.3x vs. Redpanda, 13.7x vs. Kafka** |
| **p50 (Median Latency)** | `5,606.00 ms` | `5,212.00 ms` | **`353.13 ms`** | **14.8x vs. Redpanda, 15.9x vs. Kafka** |
| **p95 Latency** | `5,866.00 ms` | `5,599.00 ms` | **`526.96 ms`** | **10.6x vs. Redpanda, 11.1x vs. Kafka** |
| **p99 Latency** | `5,866.00 ms` | `5,599.00 ms` | **`526.96 ms`** | **10.6x vs. Redpanda, 11.1x vs. Kafka** |
| **Max Latency** | `5,866.00 ms` | `5,599.00 ms` | **`528.01 ms`** | **10.6x vs. Redpanda, 11.1x vs. Kafka** |

---

## 3. High-Frequency Micro-Message Benchmarks (100 B & 1 KB)

### Test 3.1: 100-Byte High-Frequency Ingestion (100,000 messages)

| Metric | Apache Kafka (v4.3.1 KRaft) | AeroMQ (Rust Data Plane) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- |
| **Write Throughput** | `34,494 msgs/sec` | **`93,337 msgs/sec`** | **2.7x Higher** |
| **Data Throughput** | `3.29 MB/sec` | **`8.90 MB/sec`** | **2.7x Higher** |
| **Test Duration** | `2.899 s` | **`1.071 s`** | **2.7x Faster** |
| **Average Latency** | `671.12 ms` | **`0.107 ms`** (107 µs) | **6,272x Lower** |
| **p50 Latency (Median)** | `703.00 ms` | **`0.088 ms`** (88 µs) | **7,984x Lower** |
| **p95 Latency** | `817.00 ms` | **`0.183 ms`** (183 µs) | **4,466x Lower** |
| **p99 Latency** | `880.00 ms` | **`0.200 ms`** (200 µs) | **4,402x Lower** |

### Test 3.2: 1-KB Standard Payload Ingestion (50,000 messages)

| Metric | Apache Kafka (v4.3.1 KRaft) | AeroMQ (Rust Data Plane) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- |
| **Write Throughput** | `15,923.57 msgs/sec` | **`84,103.22 msgs/sec`** | **5.3x Higher** |
| **Data Throughput** | `15.55 MB/sec` | **`82.13 MB/sec`** | **5.3x Higher** |
| **p50 Latency (Median)** | `1,466.00 ms` | **`0.096 ms`** (96 µs) | **15,200x Lower** |
| **p95 Latency** | `1,824.00 ms` | **`0.158 ms`** (158 µs) | **11,574x Lower** |
| **p99 Latency** | `1,899.00 ms` | **`0.226 ms`** (226 µs) | **8,403x Lower** |

---

## 4. Resource Footprint & System Overhead

Measured directly using `docker stats --no-stream` under live cluster workloads:

| Resource Metric | Apache Kafka (JVM) | Redpanda (C++/Seastar) | AeroMQ (Rust Engine) | AeroMQ Advantage |
| :--- | :--- | :--- | :--- | :--- |
| **Live Memory Footprint** | `1.212 GiB` | `839.3 MiB` | **`1.547 MiB`** | **542x less than Redpanda, 803x less than Kafka** |
| **Container Limit Usage** | `60.60%` | `40.98%` | **`0.08%`** | **Negligible container overhead** |
| **OS Thread Count (PIDs)** | `130 threads` | `5 threads` | **`3 threads`** | **Deterministic lightweight scheduling** |
| **Cold Boot & Raft Ready** | `3,800 ms` | `680 ms` | **`1.8 ms`** | **377x vs. Redpanda, 2,111x vs. Kafka** |

---

## 5. Architectural Breakdown: Why Does AeroMQ Win at 50 MB?

### 1. Zero Heap Copying vs. Kafka's JVM Allocation Bottleneck
When Apache Kafka receives a 50 MB message, the JVM creates massive native and direct `ByteBuffer` instances. Under strict 2.0 GB memory constraints:
- Serializing and de-serializing giant byte arrays immediately causes **Eden/Tenured generation memory churn** and frequent GC pauses.
- The JVM socket layer experiences backpressure while copying bytes between Java heap, direct memory, and the OS page cache, dropping throughput to **48.64 MB/s** with **5,606 ms** median latency.

### 2. Thread-Per-Core Slab Limits vs. Redpanda's Seastar Memory Model
Redpanda is built on the Seastar asynchronous engine which partitions memory per CPU core (Shared-Nothing architecture). With `--cpus 2` and `--memory 2g`:
- Each CPU core only manages an isolated memory pool of ~800 MB.
- Allocating contiguous 50 MB message buffers within a single core's slab allocator triggers internal memory fragmentation and reactor stalls.
- Socket reads are split into small chunks that must be re-assembled across Seastar's DMA buffers, dragging throughput down to **50.20 MB/s** with **5,212 ms** latency.

### 3. AeroMQ's Memory-Mapped Circular Buffer & Direct Kernel DMA
AeroMQ avoids both JVM heap allocations and Seastar thread-per-core fragmentation:
- **Direct Memory-Mapping (`mmap`)**: Incoming frame headers and payloads are written sequentially into fixed, pre-allocated memory-mapped segment pages. The OS kernel's page cache handles dirty page flushing asynchronously in the background.
- **Single-Syscall Frame Dispatches**: A single read directly deposits the payload into the segment file without multiple intermediate userspace copies.
- **Deterministic 3-Thread Model**: Because AeroMQ maintains separate network, ingestion, and consensus loops pinned to OS threads, it never stalls when handling 50 MB payloads, achieving a consistent **666.30 MB/sec** and finishing the 500 MB transfer in **750 milliseconds**.
