# AeroStream Kafka Consumer Test Application Suite

A production-grade, modular Kafka Consumer test application suite that exercises and verifies all consumer features supported by **AeroStream's Kafka wire protocol** (port 9092/9093).

---

## Feature Matrix

| # | Scenario | CLI Flag | Key Features & Wire Protocols Verified |
|---|---|---|---|
| **1** | **Standalone / Simple Consumer** | `--scenario simple` | Direct partition assignment (`TopicPartition(topic, 0)`), manual seek to earliest (`-2` / `OFFSET_BEGINNING`), latest (`-1` / `OFFSET_END`), and explicit offset with monotonicity checks. |
| **2** | **Consumer Groups & Dynamic Rebalance** | `--scenario group` | Joining a `group.id`, coordinator discovery (`FindCoordinator`), rebalance lifecycle (`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`), and dynamic partition assignment callbacks (`on_assign`, `on_revoke`). |
| **3** | **Multi-Consumer Group Coordination** | `--scenario rebalance` | Spawning multiple concurrent consumer instances in the same consumer group, verifying dynamic partition distribution across members, and asserting rebalance triggers on consumer join and leave. |
| **4** | **Offset Management & Commit** | `--scenario offsets` | Testing automatic offset commits (`enable.auto.commit=true`), explicit synchronous manual commits (`commitSync`), asynchronous commits (`commitAsync`), and verifying committed offsets persist across consumer restarts via `OffsetFetch`. |
| **5** | **Long Polling Verification** | `--scenario longpoll` | Precise timing verification of `fetch.min.bytes` and `fetch.max.wait.ms` behavior: measuring purgatory wait duration on empty partition (~600ms) vs immediate return (< 1ms) when data is present, and prompt wakeup on concurrent append. |
| **6** | **Transaction Isolation Level** | `--scenario isolation` | Asserting `isolation.level=read_uncommitted` vs `isolation.level=read_committed`: verifying aborted transactional messages are hidden from `read_committed` consumers while visible to `read_uncommitted`. |
| **7** | **Header & Payload Validation** | `--scenario headers` | Inspecting received message headers, and byte-for-byte SHA-256 checksum verification of decompressed payloads against producer payloads across variable payload sizes. |
| **8** | **Security & Authentication** | `--scenario security` | Authenticating via SASL/PLAIN (ApiKey 17 + 36), SASL/SCRAM-SHA-256 (RFC 5802 challenge exchange), credential failure rejection (`ERR_SASL_AUTHENTICATION_FAILED` code 58), and TLS/mTLS with client certificate generation. |

---

## Directory Structure

```
client/consumer_app/
├── pyproject.toml              # Project metadata & dependencies
├── requirements.txt            # Pip requirements definition
├── README.md                   # Complete documentation and usage guide
├── cli.py                      # Production CLI entrypoint with argument parsing
├── main.py                     # Execution entrypoint
├── config.py                   # Configuration builder for confluent-kafka & kafka-python-ng
├── models.py                   # Dataclasses for ConsumedRecord, ScenarioResult, SuiteReport
├── run_tests.py                # Self-test runner (unit tests + live broker integration)
├── scenarios/
│   ├── __init__.py             # Scenario registry and map
│   ├── base.py                 # BaseScenario abstract class with metrics and seeding
│   ├── simple_consumer.py      # Scenario 1: Standalone / Simple Consumer
│   ├── group_rebalance.py      # Scenario 2: Consumer Groups & Dynamic Rebalance
│   ├── multi_consumer.py       # Scenario 3: Multi-Consumer Group Coordination
│   ├── offset_management.py    # Scenario 4: Offset Management & Commit
│   ├── long_polling.py         # Scenario 5: Long Polling Verification
│   ├── txn_isolation.py        # Scenario 6: Transaction Isolation Level
│   ├── header_payload.py       # Scenario 7: Header & Payload Validation
│   └── security_auth.py        # Scenario 8: Security & Authentication
├── utils/
│   ├── __init__.py
│   ├── wire_protocol.py        # Kafka wire protocol codec and raw socket client
│   ├── cert_gen.py             # X.509 PKI generator (CA, broker, client certs for mTLS)
│   └── broker_runner.py        # Ephemeral rust-broker process manager for self-tests
└── tests/
    ├── __init__.py
    ├── test_syntax_and_imports.py # Unit tests for models, configs, imports
    └── test_wire_codec.py         # Unit tests for varint, varlong, CRC32C, framing
```

---

## Installation & Requirements

Python **>= 3.10** with `uv`:

```bash
cd /home/uttam/projects/AeroMQ/client
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main --help
```

---

## CLI Usage

### Running All Scenarios

```bash
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario all \
  --bootstrap-server 127.0.0.1:9093 \
  --auto-start-broker
```

### Running a Specific Scenario

```bash
# Scenario 1: Simple Consumer
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario simple --bootstrap-server 127.0.0.1:9093 --auto-start-broker

# Scenario 4: Offset Management & Commits
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario offsets --bootstrap-server 127.0.0.1:9093 --auto-start-broker

# Scenario 5: Long Polling Verification
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario longpoll --bootstrap-server 127.0.0.1:9093 --auto-start-broker

# Scenario 6: Transaction Isolation Level
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario isolation --bootstrap-server 127.0.0.1:9093 --auto-start-broker

# Scenario 8: Security & Authentication
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario security --bootstrap-server 127.0.0.1:9093 --auto-start-broker
```

### Formatted JSON Output

```bash
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 -m consumer_app.main \
  --scenario simple --auto-start-broker --json-output
```

---

## CLI Flags Reference

| Flag | Default | Description |
|---|---|---|
| `--scenario` | `all` | Scenario to run (`all`, `simple`, `group`, `rebalance`, `offsets`, `longpoll`, `isolation`, `headers`, `security`) |
| `--bootstrap-server` | `127.0.0.1:9093` | Host and port of the AeroStream broker's Kafka wire listener |
| `--topic` | `aeromq-consumer-test-topic` | Topic name prefix used for consumer tests |
| `--group-id` | `None` (auto-generated) | Consumer group identifier |
| `--timeout` | `8.0` | Timeout in seconds for consumer polling loops |
| `--expected-count` | `3` | Minimum number of records expected to consume |
| `--json-output` | `False` | Emits output strictly formatted as JSON to stdout |
| `--auto-start-broker` | `False` | Automatically boots an ephemeral `rust-broker` if the target server is unreachable |
| `-v`, `--verbose` | `False` | Enables detailed debug logging |

---

## Running the Self-Test Runner

The suite includes an end-to-end self-test script that runs all unit tests followed by live E2E scenarios against an ephemeral broker:

```bash
# Run unit tests and live broker integration tests:
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 consumer_app/run_tests.py

# Run unit tests only:
uv run --with confluent-kafka --with kafka-python-ng --with cryptography python3 consumer_app/run_tests.py --unit-only
```
