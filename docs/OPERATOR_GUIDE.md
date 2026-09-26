# AeroStream Operator & Developer Guide

This guide provides comprehensive, production-ready operational instructions and development integration guides for **AeroStream** (formerly AeroMQ) — a high-throughput, low-latency distributed event streaming platform featuring a Go-based Raft consensus control plane, a Rust zero-copy storage broker, and dual protocol support (native HTTP/REST + Apache Kafka Wire Protocol).

---

## Table of Contents

1. [Production Deployment Architecture](#1-production-deployment-architecture)
   - [Bare-Metal & Virtual Machines](#11-bare-metal--virtual-machines)
   - [Docker Compose Multi-Node Deployment](#12-docker-compose-multi-node-deployment)
   - [Kubernetes StatefulSet Architecture](#13-kubernetes-statefulset-architecture)
2. [Configuration Reference](#2-configuration-reference)
   - [Go Controller Configuration](#21-go-controller-configuration)
   - [Rust Broker Configuration](#22-rust-broker-configuration)
3. [Cluster Lifecycle Management](#3-cluster-lifecycle-management)
   - [Bootstrapping a Multi-Node Cluster](#31-bootstrapping-a-multi-node-cluster)
   - [Automated Kubernetes Scale-Down & Raft Eviction](#32-automated-kubernetes-scale-down--raft-eviction)
   - [Graceful Broker Draining & Partition Rebalancing](#33-graceful-broker-draining--partition-rebalancing)
4. [Monitoring & Observability](#4-monitoring--observability)
   - [Health Probes & Heartbeat Monitoring](#41-health-probes--heartbeat-monitoring)
   - [Cluster State & Prometheus Metrics](#42-cluster-state--prometheus-metrics)
   - [Web UI Console Management](#43-web-ui-console-management)
5. [Client Integration Quickstart](#5-client-integration-quickstart)
   - [Python (FastAPI & standard Kafka Clients)](#51-python-integration)
   - [Go (CLI & Native Client)](#52-go-integration)
   - [Java / Spring Kafka Integration](#53-java--spring-kafka-integration)
   - [HTTP REST & cURL Integration](#54-http-rest--curl-integration)

---

## 1. Production Deployment Architecture

AeroStream separates metadata consensus from data log persistence:
- **Go Controller Quorum**: Runs HashiCorp Raft consensus to manage cluster metadata, topic configurations, ISR (In-Sync Replicas) calculation, consumer groups, schema registry, and streaming transforms.
- **Rust Storage Brokers**: Runs an event loop leveraging Linux `sendfile(2)` zero-copy file transfer, pinned Tokio worker threads, memory-mapped segment indexes, and a Kafka wire protocol gateway.

![AeroStream Dual-Engine Architecture](images/dual_engine_architecture.png)

```
                  ┌───────────────────────────────────────────────┐
                  │           AeroStream Control Quorum           │
                  │   [Controller 1] ── [Controller 2] (Leader)   │
                  │             \            /                    │
                  │             [Controller 3]                    │
                  │          Raft: 7001 | gRPC: 8001              │
                  │         HTTP / Console: 9001                  │
                  └──────────────────────┬────────────────────────┘
                                         │ gRPC Heartbeats & Metadata
                     ┌───────────────────┴───────────────────┐
                     ▼                                       ▼
        ┌─────────────────────────┐             ┌─────────────────────────┐
        │   AeroStream Broker 1   │             │   AeroStream Broker 2   │
        │  Zero-Copy TCP: 9091    │◄───────────►│  Zero-Copy TCP: 9091    │
        │  Kafka Protocol: 9093   │ Replication │  Kafka Protocol: 9093   │
        │  Storage: NVMe /data/1  │             │  Storage: NVMe /data/2  │
        └─────────────────────────┘             └─────────────────────────┘
```

---

### 1.1 Bare-Metal & Virtual Machines

For optimal throughput (sub-millisecond latency and millions of msgs/sec), run controllers and brokers on dedicated hardware or compute-optimized VMs.

#### OS Kernel & Storage Tuning (`sysctl.conf`)
Apply these parameters in `/etc/sysctl.d/99-aerostream.conf`:

```ini
# Increase network backlog queue and max connection sockets
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.core.netdev_max_backlog = 100000

# TCP socket buffer sizes (min, default, max in bytes)
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Virtual memory and dirty page flushing for high write throughput
vm.dirty_background_ratio = 5
vm.dirty_ratio = 10
vm.max_map_count = 1048576

# File descriptors and epoll limits
fs.file-max = 2097152
```

Reload settings:
```bash
sudo sysctl --system
```

#### NVMe Mount Tuning
Mount commit log directories with `noatime,nodiratime` to avoid metadata write overhead:
```bash
sudo mount -o defaults,noatime,nodiratime /dev/nvme0n1 /var/lib/aerostream/broker_1
```

#### Systemd Service Units

**Controller Unit** (`/etc/systemd/system/aerostream-controller.service`):
```ini
[Unit]
Description=AeroStream Go Raft Controller
After=network.target

[Service]
Type=simple
User=aerostream
Group=aerostream
LimitNOFILE=65536
ExecStart=/usr/local/bin/controller \
  --config /etc/aerostream/controller.toml \
  --data-dir /var/lib/aerostream/controller
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

**Broker Unit** (`/etc/systemd/system/aerostream-broker.service`):
```ini
[Unit]
Description=AeroStream Rust Storage Broker
After=network.target aerostream-controller.service

[Service]
Type=simple
User=aerostream
Group=aerostream
LimitNOFILE=1048576
LimitMEMLOCK=infinity
CPUAffinity=0-7
ExecStart=/usr/local/bin/rust-broker \
  --config /etc/aerostream/broker.toml \
  --storage-dir /var/lib/aerostream/broker_1
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

---

### 1.2 Docker Compose Multi-Node Deployment

A pre-packaged, multi-node configuration is defined in [`docker-compose.yml`](file:///home/uttam/projects/AeroMQ/docker-compose.yml). It starts:
- A 3-node Controller quorum (`controller-1`, `controller-2`, `controller-3`)
- Two Rust Storage Brokers (`broker-1`, `broker-2`)
- Persistent volumes and healthchecks

#### Launching the Cluster
```bash
docker compose up -d
```

#### Inspecting Health & Services
```bash
docker compose ps
```

All 3 controllers dynamically form a Raft quorum:
- `controller-1` starts with `-bootstrap`
- `controller-2` and `controller-3` join via `http://controller-1:9001/join`
- Rust brokers register with `http://controller-1:8001` via gRPC and publish both native TCP (port 9091) and Kafka Wire Protocol (port 9093).

---

### 1.3 Kubernetes StatefulSet Architecture

Production manifests reside in [`deploy/k8s/`](file:///home/uttam/projects/AeroMQ/deploy/k8s):
- [`namespace.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/namespace.yaml): Creates the dedicated `aerostream` namespace.
- [`services.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/services.yaml): Defines headless services (`controller-headless`, `broker-headless`) and the load-balanced UI service.
- [`controller-statefulset.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/controller-statefulset.yaml): Deploys 3 controller replicas with automated Raft bootstrap, join, and `preStop` removal hooks.
- [`broker-statefulset.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/broker-statefulset.yaml): Deploys 2+ Rust brokers backed by persistent volume claims (`volumeClaimTemplates`).

#### Deploying to Kubernetes
```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
kubectl apply -f deploy/k8s/controller-statefulset.yaml
kubectl apply -f deploy/k8s/broker-statefulset.yaml
```

#### Stateful DNS Resolution
Kubernetes creates deterministic DNS names for all pods:
- `controller-0.controller-headless.aerostream.svc.cluster.local` (Port 7001 Raft, 8001 gRPC, 9001 HTTP)
- `controller-1.controller-headless.aerostream.svc.cluster.local`
- `controller-2.controller-headless.aerostream.svc.cluster.local`
- `broker-0.broker-headless.aerostream.svc.cluster.local` (Port 9091 TCP Data, 9093 Kafka)
- `broker-1.broker-headless.aerostream.svc.cluster.local`

---

## 2. Configuration Reference

### 2.1 Go Controller Configuration

Defined in [`go-controller/pkg/config/config.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/config/config.go) and example file [`config/controller.example.toml`](file:///home/uttam/projects/AeroMQ/config/controller.example.toml).

```toml
# Top-level Controller Options
node_id   = "node1"
raft_addr = "127.0.0.1:7001"
grpc_addr = "127.0.0.1:8001"
http_addr = "127.0.0.1:9001"
data_dir  = "/var/lib/aerostream/controller"
bootstrap = true

[raft]
heartbeat_timeout_ms    = 1000
election_timeout_ms     = 1000
leader_lease_timeout_ms = 500
commit_timeout_ms       = 50

[cluster]
failure_detection_interval_ms = 3000
broker_inactive_timeout_ms    = 8000
replica_lag_tolerance         = 10

[tls]
enabled   = false
cert_file = "config/certs/controller.crt"
key_file  = "config/certs/controller.key"
ca_file   = "config/certs/ca.crt"

[auth]
token     = "change-me-super-secret-token"
```

#### Tunable Parameters Reference

| Field | CLI Flag | Default | Description |
|---|---|---|---|
| `node_id` | `-id` | `"node1"` | Unique identifier of this Raft controller node. |
| `raft_addr` | `-raft-addr` | `"127.0.0.1:7001"` | TCP address for peer-to-peer Raft consensus communication. |
| `grpc_addr` | `-grpc-addr` | `"127.0.0.1:8001"` | Address for gRPC control plane (broker registration, metadata). |
| `http_addr` | `-http-addr` | `"127.0.0.1:9001"` | HTTP address for REST API, Web Console, `/join`, `/leave`, `/status`. |
| `data_dir` | `-data-dir` | `""` (in-memory) | Directory for Raft log storage and snapshots. |
| `bootstrap` | `-bootstrap` | `false` | True only for the initial seed node initializing a brand-new cluster. |
| `join` | `-join` | `""` | URL of existing leader's HTTP endpoint to join (e.g. `http://node1:9001`). |
| `ui_dir` | `-ui-dir` | `""` | Directory containing built Web Console SPA assets (HTML/JS/CSS). |
| `raft.heartbeat_timeout_ms` | — | `1000` | Raft leader heartbeat interval in milliseconds. |
| `raft.election_timeout_ms` | — | `1000` | Raft candidate election timeout in milliseconds. |
| `raft.leader_lease_timeout_ms` | — | `500` | Raft leader lease validity duration. |
| `raft.commit_timeout_ms` | — | `50` | Maximum Raft commit batch delay. |
| `cluster.failure_detection_interval_ms` | — | `3000` | Interval between background sweeps checking broker active status. |
| `cluster.broker_inactive_timeout_ms` | — | `8000` | Time without heartbeat before marking broker inactive and failing over. |
| `cluster.replica_lag_tolerance` | — | `10` | Maximum message offset lag allowed before removing replica from ISR. |
| `tls.enabled` | — | `false` | Enable TLS on gRPC control plane. |
| `tls.ca_file` | — | `""` | CA certificate for client certificate validation (enforces mTLS). |
| `auth.token` | `-token` | `""` | Bearer token required in gRPC metadata and REST `Authorization` header. |

---

### 2.2 Rust Broker Configuration

Defined in [`rust-broker/src/config.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/config.rs) and [`config/broker.example.toml`](file:///home/uttam/projects/AeroMQ/config/broker.example.toml).

```toml
id          = 1
host        = "127.0.0.1"
data_port   = 9091
kafka_port  = 9093
controller  = "http://127.0.0.1:8001"
storage_dir = "./data/broker_1"

[storage]
max_segment_size         = 134217728    # 128 MiB
max_retention_size       = 1073741824   # 1 GiB per partition
max_retention_age_secs   = 604800       # 7 days
compaction_enabled       = true         # Enable log compaction
dirty_ratio_threshold    = 0.5          # Trigger compaction at 50% dirty
tombstone_retention_secs = 86400        # Retain delete tombstones 24h

[tiered_storage]
enabled  = false
provider = "s3" # Options: "s3", "gcs", "azure", "local", "disabled"

[tiered_storage.s3]
bucket            = "aerostream-cold-storage"
prefix            = "cluster-01/"
region            = "us-east-1"
endpoint_url      = "http://minio:9000"
access_key_id     = "minioadmin"
secret_access_key = "minioadmin"
force_path_style  = true

[tls]
enabled   = false
cert_file = "config/certs/broker.crt"
key_file  = "config/certs/broker.key"
ca_file   = "config/certs/ca.crt"

[auth]
token     = "change-me-super-secret-token"
```

#### Tunable Parameters Reference

| Section / Key | CLI Override Flag | Default | Description |
|---|---|---|---|
| `id` | `--id` | `1` | Unique broker ID within the AeroStream cluster. |
| `host` | `--host` | `"127.0.0.1"` | Bind address or advertise host for data connections. |
| `data_port` | `--data-port` | `9091` | High-performance zero-copy TCP protocol port. |
| `kafka_port` | `--kafka-port` | `9093` | Apache Kafka binary wire protocol compatibility port. |
| `controller` | `--controller` | `"http://127.0.0.1:8001"` | Control plane gRPC endpoint. |
| `storage_dir` | `--storage-dir` | `./data/broker_{id}` | Physical filesystem directory for partition commit logs. |
| `storage.max_segment_size` | — | `134217728` (128 MB) | Log segment roll-over size threshold. |
| `storage.max_retention_size` | — | `1073741824` (1 GB) | Maximum disk usage per partition before segment eviction. |
| `storage.max_retention_age_secs` | — | `604800` (7 days) | Age threshold for segment expiration and cleanup. |
| `storage.compaction_enabled` | — | `true` | Enables background cleaner thread running every 30s. |
| `storage.dirty_ratio_threshold` | — | `0.5` | Ratio of superseded records to trigger segment compaction. |
| `storage.tombstone_retention_secs` | — | `86400` (24h) | Time tombstones are preserved before physical deletion. |
| `tiered_storage.enabled` | — | `false` | Asynchronously offload inactive segments to object store. |
| `tiered_storage.provider` | `--tiered-storage-provider` | `"disabled"` | Backend: `s3`, `gcs`, `azure`, `local`, `disabled`. |
| `tiered_storage.s3.bucket` | `--s3-bucket` | `""` | Target S3/MinIO bucket. |
| `tiered_storage.s3.endpoint_url`| `--s3-endpoint` | `None` | Custom S3 endpoint URL (MinIO, LocalStack, Ceph). |
| `tiered_storage.s3.region` | `--s3-region` | `"us-east-1"` | AWS Region. |
| `tiered_storage.local.root_path`| `--tiered-storage-dir` | `./data/tiered_storage` | Local directory or mounted NFS storage path. |

---

## 3. Cluster Lifecycle Management

### 3.1 Bootstrapping a Multi-Node Cluster

To bootstrap a cluster of 3 controllers and N brokers:

#### Step 1: Start Seed Controller (Node 1)
```bash
./go-controller/bin/controller \
  -id node1 \
  -raft-addr 10.0.0.1:7001 \
  -grpc-addr 10.0.0.1:8001 \
  -http-addr 10.0.0.1:9001 \
  -data-dir /var/lib/aerostream/controller \
  -bootstrap
```

#### Step 2: Join Remaining Controllers
Node 2:
```bash
./go-controller/bin/controller \
  -id node2 \
  -raft-addr 10.0.0.2:7001 \
  -grpc-addr 10.0.0.2:8001 \
  -http-addr 10.0.0.2:9001 \
  -data-dir /var/lib/aerostream/controller \
  -join http://10.0.0.1:9001
```

Node 3:
```bash
./go-controller/bin/controller \
  -id node3 \
  -raft-addr 10.0.0.3:7001 \
  -grpc-addr 10.0.0.3:8001 \
  -http-addr 10.0.0.3:9001 \
  -data-dir /var/lib/aerostream/controller \
  -join http://10.0.0.1:9001
```

#### Step 3: Start Rust Storage Brokers
Start broker 1:
```bash
./rust-broker/target/release/rust-broker \
  --id 1 \
  --host 10.0.0.11 \
  --data-port 9091 \
  --kafka-port 9093 \
  --controller http://10.0.0.1:8001 \
  --storage-dir /var/lib/aerostream/broker_1
```

Start broker 2:
```bash
./rust-broker/target/release/rust-broker \
  --id 2 \
  --host 10.0.0.12 \
  --data-port 9091 \
  --kafka-port 9093 \
  --controller http://10.0.0.1:8001 \
  --storage-dir /var/lib/aerostream/broker_2
```

Brokers automatically register with the active Raft leader, establish heartbeat streams, and report disk capacity via `statvfs(2)`.

---

### 3.2 Automated Kubernetes Scale-Down & Raft Eviction

![AeroStream Cluster Topology & Zero-Downtime Scale-Down](images/cluster_topology_scale_down.png)

When Kubernetes scales down a StatefulSet (e.g. from 3 replicas to 2), the pod with the highest ordinal is terminated. In Raft consensus, if a node disappears without being removed from the voter configuration, quorum calculations remain weighted on the dead node.

AeroStream solves this cleanly with a `preStop` container lifecycle hook in [`deploy/k8s/controller-statefulset.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/controller-statefulset.yaml#L81-L95):

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - "/bin/sh"
        - "-c"
        - |
          ORDINAL="${HOSTNAME##*-}"
          NODE_ID="node$((ORDINAL + 1))"
          BOOTSTRAP_HOST="controller-0.controller-headless.aerostream.svc.cluster.local"
          if [ "${ORDINAL}" != "0" ]; then
            echo "Gracefully leaving Raft cluster before pod termination..."
            curl -s -X POST "http://${BOOTSTRAP_HOST}:9001/leave?id=${NODE_ID}" || true
            sleep 2
          fi
```

The controller `/leave` endpoint executes `raftNode.Leave(id)`:
1. Verifies the caller node exists in the Raft configuration.
2. Proposes `RemoveServer` to the Raft consensus group.
3. Once committed, the cluster adjusts its quorum size downward immediately without downtime.

---

### 3.3 Graceful Broker Draining & Partition Rebalancing

Before terminating, upgrading, or resizing a Rust storage broker, operators must evacuate its partition leadership and replica assignments.

#### Triggering Broker Drain
Invoke the drain endpoint via REST or the client CLI:

```bash
# Via REST
curl -X POST http://10.0.0.1:9001/api/brokers/2/drain

# Via AeroStream Client CLI
./client/bin/client drain-broker 2 http://10.0.0.1:9001
```

#### Under the Hood: What Happens
Implemented in [`go-controller/pkg/consensus/fsm.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L183-L250):
1. **Status Transition**: Broker 2 is marked `Active = false`.
2. **Leader Migration**: For every partition where Broker 2 was leader, the controller immediately reassigns leadership to the next available healthy broker in the partition's ISR list.
3. **Replica Reassignment**: Broker 2 is removed from `ReplicaIDs`. If another active broker exists in the cluster that is not yet hosting this partition, it is assigned as the replacement replica.
4. **ISR & High Watermark Update**: Broker 2 is removed from the partition's ISR and offset tracking table. High Watermark is re-evaluated.
5. Clients fetching metadata (`ApiKey 3` or `GET /api/topics`) receive the updated leader and replica assignments instantly.

---

## 4. Monitoring & Observability

### 4.1 Health Probes & Heartbeat Monitoring

AeroStream provides lightweight endpoints for load balancer and Kubernetes health checks:

- **`/status` (HTTP Port 9001)**:
  Used by Kubernetes `livenessProbe` and `readinessProbe`. Returns HTTP 200 with Raft state and basic metrics:
  ```json
  {
    "node_id": "node1",
    "state": "Leader",
    "leader": "10.0.0.1:7001",
    "brokers_count": 2,
    "topics_count": 5
  }
  ```

- **Broker TCP Liveness (Port 9091)**:
  Kubernetes verifies broker readiness using a `tcpSocket` check against port 9091.

- **Broker Heartbeats**:
  Brokers send periodic gRPC heartbeat pings to the active controller. If no heartbeat arrives within `cluster.broker_inactive_timeout_ms` (default 8000ms), the controller automatically initiates partition failover.

---

### 4.2 Cluster State & Prometheus Metrics

#### Cluster Topology API (`GET /api/cluster`)
Returns a snapshot of cluster health and connected brokers:
```json
{
  "node_id": "node1",
  "raft_state": "Leader",
  "raft_leader": "127.0.0.1:7001",
  "brokers_count": 2,
  "topics_count": 8,
  "groups_count": 3,
  "brokers": [
    {
      "id": 1,
      "host": "10.0.0.11",
      "port": 9091,
      "active": true,
      "last_seen": 1727394820
    }
  ]
}
```

#### Key Metrics to Monitor

| Metric / Attribute | Source | Description | Alert Threshold |
|---|---|---|---|
| `brokers_count` | `/api/cluster` | Count of registered active brokers | `< Min required brokers` |
| `raft_state` | `/status` | Controller node state (`Leader` / `Follower`) | If 0 nodes report `Leader` |
| `high_watermark` - `replica_offset` | `GET /api/topics` | Replication offset lag across followers | `> replica_lag_tolerance` |
| `records_processed` | `GET /api/connectors-detail` | Processed event count for CDC/S3 sinks | Flatline on active workload |
| `messages_filtered` | `GET /api/transforms` | Messages filtered by stream transforms | Sudden spikes |
| `free_bytes` | Broker `statvfs` | Free disk space on commit log volume | `< 15% disk space` |

---

### 4.3 Web UI Console Management

AeroStream includes a built-in single-page management console served directly from the Go Controller on port 9001 at:
```
http://<controller-host>:9001/aerostream/console
```
*(Backward-compatible alias: `/aeromq/console`)*

#### Capabilities
- **Overview Dashboard**: Cluster health status, Raft leader, registered broker topology, active consumer lag.
- **Topic Explorer**: Visual partition distribution, ISR indicators, watermark offsets, and retention policies.
- **Live Message Inspector**: Real-time browsing and filtering of ingested messages across any topic and partition.
- **Schema Registry**: View, test, and register Avro, JSON Schema, and Protobuf schemas with compatibility verification.
- **Stream Transforms**: Visual WASM, Filter, and PII masking pipeline editor with live payload testing.
- **Connectors Hub**: Manage Kafka Connect compatible sources and sinks, pause/resume tasks, and view throughput metrics.

---

## 5. Client Integration Quickstart

### 5.1 Python Integration

#### Option A: FastAPI Native Async Client (`examples/fastapi-app/`)
See [`examples/fastapi-app/aerostream_client.py`](file:///home/uttam/projects/AeroMQ/examples/fastapi-app/aerostream_client.py) and [`examples/fastapi-app/main.py`](file:///home/uttam/projects/AeroMQ/examples/fastapi-app/main.py).

```python
import asyncio
from aerostream_client import AeroStreamClient

client = AeroStreamClient(
    kafka_host="127.0.0.1",
    kafka_port=9093,
    http_url="http://127.0.0.1:9001"
)

# Publish via binary Kafka wire protocol (ApiKey 0)
offset = client.produce_kafka("orders", partition=0, message='{"order_id": "1001", "total": 49.99}')
print(f"Produced record at offset: {offset}")

# Fetch records via HTTP REST
records = asyncio.run(client.fetch_http("orders", partition=0, offset=0, limit=10))
for r in records:
    print(r["offset"], r["payload"])
```

#### Option B: Standard Kafka Clients (`confluent-kafka` or `kafka-python`)
AeroStream brokers implement Kafka Wire Protocol natively on port 9093. Existing standard Kafka producers and consumers work out of the box:

```python
from confluent_kafka import Producer, Consumer

# Producer
p = Producer({'bootstrap.servers': 'localhost:9093'})
p.produce('orders', key='cust_1', value='{"status": "CONFIRMED"}')
p.flush()

# Consumer
c = Consumer({
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'order-processor-group',
    'auto.offset.reset': 'earliest'
})
c.subscribe(['orders'])
msg = c.poll(1.0)
if msg and not msg.error():
    print(f"Received: {msg.value().decode('utf-8')}")
c.close()
```

---

### 5.2 Go Integration

#### Using the CLI Client (`client/main.go`)
Compile the client binary:
```bash
cd client && go build -o bin/client main.go
```

Inspect cluster metadata:
```bash
./bin/client --controller 127.0.0.1:8001 metadata
```

Create a topic:
```bash
./bin/client --controller 127.0.0.1:8001 create-topic telemetry 3 2
```

Publish messages:
```bash
./bin/client --controller 127.0.0.1:8001 produce telemetry 0 '{"temp": 24.5, "unit": "C"}'
```

Consume messages:
```bash
./bin/client --controller 127.0.0.1:8001 consume telemetry 0 0 --follow
```

Join a consumer group:
```bash
./bin/client --controller 127.0.0.1:8001 consume-group telemetry analytics-group --follow
```

---

### 5.3 Java / Spring Kafka Integration

Spring Boot applications connect using standard `spring-kafka`:

#### `application.yml`
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9093
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: 1
    consumer:
      group-id: inventory-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

#### Java Service Example
```java
@Service
public class OrderEventService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void publishOrder(String orderId, String jsonPayload) {
        kafkaTemplate.send("orders", orderId, jsonPayload);
    }

    @KafkaListener(topics = "orders", groupId = "inventory-service")
    public void handleOrderEvent(String message) {
        System.out.println("Processing order event from AeroStream: " + message);
    }
}
```

---

### 5.4 HTTP REST & cURL Integration

For edge microservices or serverless functions without Kafka driver dependencies, use AeroStream's HTTP API:

#### 1. Ingest Message
```bash
curl -X POST http://127.0.0.1:9001/api/produce \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "events",
    "partition": 0,
    "message": "{\"device_id\": \"sensor_42\", \"status\": \"ACTIVE\"}"
  }'
```
Response:
```json
{"offset": 12, "partition": 0, "success": true, "topic": "events"}
```

#### 2. Fetch Messages
```bash
curl "http://127.0.0.1:9001/api/messages?topic=events&partition=0&offset=0&limit=50"
```
Response:
```json
{
  "topic": "events",
  "partition": 0,
  "count": 1,
  "messages": [
    {
      "offset": 12,
      "length": 45,
      "payload": "{\"device_id\": \"sensor_42\", \"status\": \"ACTIVE\"}"
    }
  ]
}
```
