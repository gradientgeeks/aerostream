# AeroStream Kafka Producer Test Suite

A modular, production-grade Kafka Producer test application suite engineered to validate and stress-test all features supported by **AeroStream's Kafka wire protocol** (TCP port 9092 / 9093).

Built with Python leveraging **`confluent-kafka`** (librdkafka C bindings) and **`kafka-python-ng`**, executable effortlessly via **`uv run`**.

---

## Feature & Scenario Matrix

| # | Scenario | CLI Flag | Subtests & Coverage | Protocol Verification |
|---|---|---|---|---|
| **1** | **Standard & Batch Produce** | `--scenario basic` | • `acks=1` (leader acknowledgment)<br>• `acks=all` / `-1` (durability across ISR replicas)<br>• `acks=0` (fire-and-forget, zero response wait)<br>• High-throughput batching (`batch.size=65536`, `linger.ms=20`) | ApiKey 0 (Produce v0–v7), acks parsing, batch threshold rollover |
| **2** | **Compression Codecs** | `--scenario compression` | • `none` (uncompressed)<br>• `gzip` (RFC 1952)<br>• `snappy` (xerial framing)<br>• `lz4` (LZ4 frame format)<br>• `zstd` (standard zstd frame, KIP-110) | Magic 2 RecordBatch compression attributes (bits 0–2), decompressor validation |
| **3** | **Message Headers** | `--scenario headers` | • Distributed tracing headers: `trace-id` (UUID), `source=aerostream-producer`, `timestamp`, `correlation-id`, `env`<br>• Binary tokens (raw byte vectors)<br>• Multi-byte UTF-8 Unicode strings<br>• Empty zero-length header values | KIP-98 magic v2 record header array varint encoding & wire integrity |
| **4** | **Partition Key Routing** | `--scenario keys` | • Deterministic key routing: verifies identical keys ALWAYS route to the same partition<br>• Null-key distribution: verifies round-robin / sticky partitioner spreads messages evenly without starvation | Partition hash determinism, murmur2 key hashing |
| **5** | **Large Payloads** | `--scenario large` | • `100 B` (micro-messages)<br>• `1 KB` (standard telemetry)<br>• `64 KB` (medium batch/documents)<br>• `1 MB` (large documents)<br>• `5 MB` (stress payload) | `message.max.bytes`, multi-frame TCP buffering, memory fragmentation |
| **6** | **Idempotent Producer** | `--scenario idempotence` | • `enable.idempotence=true` with monotonic sequence numbers<br>• Aggressive retries (`retries=10`, `retry.backoff.ms=50`) under in-flight load (`max.in.flight=5`) | ApiKey 22 (InitProducerId), Producer ID (PID), Producer Epoch, sequence deduplication |
| **7** | **Transactional Producer** | `--scenario transactions` | • Full KIP-98 two-phase commit lifecycle:<br>  1. `init_transactions()`: coordinator discovery and PID allocation<br>  2. `begin_transaction()` → `produce()` → `commit_transaction()`: atomic commit<br>  3. `begin_transaction()` → `produce()` → `abort_transaction()`: rollback & isolation testing<br>  4. Post-abort recovery: subsequent transaction cycle after abort | ApiKey 10 (FindCoordinator), ApiKey 22 (InitProducerId), ApiKey 24 (AddPartitionsToTxn), ApiKey 26 (EndTxn) |
| **8** | **Security & Auth** | `--scenario security` | • SASL/PLAIN (username/password credentials)<br>• SASL/SCRAM-SHA-256 (RFC 5802 challenge-response)<br>• TLS/SSL server certificate verification<br>• Mutual TLS (mTLS) with client certificate and Subject CN principal mapping | ApiKey 17 (SaslHandshake v0–v1), ApiKey 36 (SaslAuthenticate v0–v2), TLS handshake |

---

## Directory Layout

```
client/producer_app/
├── cli.py                  # Main CLI entry point
├── main.py                 # Alternative entry point alias
├── config.py               # Argument parser and configuration validator
├── pyproject.toml          # PEP 621 packaging specifications
├── requirements.txt        # Pinned dependency definitions
├── README.md               # Architecture, scenario matrix, and usage guide
├── test_producer_app.py    # Comprehensive test suite (17 automated tests)
├── scenarios/
│   ├── __init__.py         # Scenario registry mapping
│   ├── base.py             # ScenarioBase interface, payload generators, metric trackers
│   ├── basic.py            # Scenario 1: Standard & Batch Produce
│   ├── compression.py      # Scenario 2: Compression Codecs (none, snappy, gzip, lz4, zstd)
│   ├── headers.py          # Scenario 3: Message Headers
│   ├── keys.py             # Scenario 4: Partition Key Routing
│   ├── large.py            # Scenario 5: Large Payloads (100B - 5MB)
│   ├── idempotence.py      # Scenario 6: Idempotent Producer
│   ├── transactions.py     # Scenario 7: Transactional Producer (KIP-98)
│   └── security.py         # Scenario 8: SASL/PLAIN, SCRAM-SHA-256, TLS, mTLS
└── utils/
    ├── __init__.py
    ├── cert_gen.py         # Ephemeral CA, server, and client certificate generator
    ├── mock_broker.py      # In-process mock Kafka wire protocol server for standalone CI/testing
    ├── producer_factory.py # Unified factory for confluent-kafka and kafka-python-ng
    └── reporter.py         # Console tables, latency percentiles, and JSON serialization
```

---

## Quick Start & Execution

### 1. Run with `uv` (Recommended - Zero Manual Environment Setup)

```bash
cd /home/uttam/projects/AeroMQ/client/producer_app

# Execute all scenarios against live AeroStream broker (default port 9092)
uv run --with confluent-kafka --with kafka-python-ng --with lz4 --with zstandard --with tabulate \
    python3 cli.py --bootstrap-server 127.0.0.1:9092 --scenario all

# Execute all scenarios using the embedded Mock Broker (ideal for CI/offline verification)
uv run --with confluent-kafka --with kafka-python-ng --with lz4 --with zstandard --with tabulate \
    python3 cli.py --mock-broker --scenario all
```

### 2. Run Self-Test Suite (Automated Verification)

```bash
uv run --with confluent-kafka --with kafka-python-ng --with lz4 --with zstandard --with tabulate \
    python3 test_producer_app.py
```

---

## CLI Usage Reference

```
usage: aerostream-producer [-h]
                           [--scenario {all,basic,compression,headers,keys,large,idempotence,transactions,security}]
                           [--bootstrap-server BOOTSTRAP_SERVER]
                           [--topic TOPIC] [--count COUNT] [--json-output]
                           [--backend {confluent,kafka-python}] [--acks ACKS]
                           [--batch-size BATCH_SIZE] [--linger-ms LINGER_MS]
                           [--compression {none,gzip,snappy,lz4,zstd}]
                           [--payload-size PAYLOAD_SIZE]
                           [--transactional-id TRANSACTIONAL_ID]
                           [--security-protocol {PLAINTEXT,SSL,SASL_PLAINTEXT,SASL_SSL}]
                           [--sasl-mechanism {PLAIN,SCRAM-SHA-256}]
                           [--sasl-username SASL_USERNAME]
                           [--sasl-password SASL_PASSWORD] [--ca-cert CA_CERT]
                           [--client-cert CLIENT_CERT]
                           [--client-key CLIENT_KEY] [--timeout TIMEOUT]
                           [--mock-broker] [--verbose]
```

### Scenario Execution Examples

#### 1. Basic Produce with Custom Acks & Batching
```bash
uv run python3 cli.py --scenario basic --bootstrap-server 127.0.0.1:9092 \
    --acks all --batch-size 65536 --linger-ms 15 --count 500
```

#### 2. Test Specific Compression Codec (e.g. `zstd`)
```bash
uv run python3 cli.py --scenario compression --bootstrap-server 127.0.0.1:9092 \
    --compression zstd --count 200
```

#### 3. Custom Headers Verification
```bash
uv run python3 cli.py --scenario headers --bootstrap-server 127.0.0.1:9092 --count 100
```

#### 4. Partition Key Determinism Test
```bash
uv run python3 cli.py --scenario keys --bootstrap-server 127.0.0.1:9092 --count 150
```

#### 5. Large Payload Stress Test (e.g. `5MB`)
```bash
uv run python3 cli.py --scenario large --bootstrap-server 127.0.0.1:9092 \
    --payload-size 5MB --count 10
```

#### 6. Idempotent Producer Test
```bash
uv run python3 cli.py --scenario idempotence --bootstrap-server 127.0.0.1:9092 --count 100
```

#### 7. Transactional Producer (KIP-98 2PC Commit & Abort)
```bash
uv run python3 cli.py --scenario transactions --bootstrap-server 127.0.0.1:9092 \
    --transactional-id my-aerostream-txn --count 50
```

#### 8. Authenticated SASL / TLS Connections
```bash
# SASL/PLAIN
uv run python3 cli.py --scenario security --bootstrap-server 127.0.0.1:9092 \
    --security-protocol SASL_PLAINTEXT --sasl-mechanism PLAIN \
    --sasl-username aerostream --sasl-password aerostream123

# SASL/SCRAM-SHA-256
uv run python3 cli.py --scenario security --bootstrap-server 127.0.0.1:9092 \
    --security-protocol SASL_PLAINTEXT --sasl-mechanism SCRAM-SHA-256 \
    --sasl-username aerostream --sasl-password aerostream123

# Mutual TLS (mTLS)
uv run python3 cli.py --scenario security --bootstrap-server 127.0.0.1:9093 \
    --security-protocol SSL \
    --ca-cert /path/to/ca.crt \
    --client-cert /path/to/client.crt \
    --client-key /path/to/client.key
```

#### 9. JSON Output for Automation & CI Pipelines
```bash
uv run python3 cli.py --scenario all --mock-broker --json-output | jq .
```
