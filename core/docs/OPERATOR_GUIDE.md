# AeroStream Operator & Developer Guide

This guide provides comprehensive, production-ready operational instructions and development integration guides for **AeroStream** (formerly AeroMQ) — a high-throughput, low-latency distributed event streaming platform featuring a Go-based Raft consensus control plane, a Rust zero-copy storage broker, and dual protocol support (native HTTP/REST + Apache Kafka Wire Protocol).

---

## Table of Contents

1. [Production Deployment Architecture](#1-production-deployment-architecture)
   - [Bare-Metal & Virtual Machines](#11-bare-metal--virtual-machines)
   - [Docker Compose Multi-Node Deployment](#12-docker-compose-multi-node-deployment)
   - [Kubernetes StatefulSet Architecture](#13-kubernetes-statefulset-architecture)
2. [Configuration Reference & Performance Tuning](#2-configuration-reference--performance-tuning)
   - [Go Controller Configuration](#21-go-controller-configuration)
   - [Rust Broker Configuration](#22-rust-broker-configuration)
   - [Tuning Shard-per-Core & Paced Writeback](#23-tuning-shard-per-core--paced-writeback)
3. [Cluster Lifecycle Management](#3-cluster-lifecycle-management)
   - [Bootstrapping a Multi-Node Cluster](#31-bootstrapping-a-multi-node-cluster)
   - [Automated Kubernetes Scale-Down & Raft Eviction](#32-automated-kubernetes-scale-down--raft-eviction)
   - [Graceful Broker Draining & Maintenance Procedures](#33-graceful-broker-draining--maintenance-procedures)
4. [Monitoring & Observability](#4-monitoring--observability)
   - [Health Probes & Heartbeat Monitoring](#41-health-probes--heartbeat-monitoring)
   - [Cluster State & Prometheus Metrics](#42-cluster-state--prometheus-metrics)
   - [Monitoring Shard Queues, Dirty Writeback & Consumer Lag](#43-monitoring-shard-queues-dirty-writeback--consumer-lag)
   - [Web UI Console Management](#44-web-ui-console-management)
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

* **Control Quorum (Go 1.26)**: 3-node HashiCorp Raft cluster (`controller-1`, `controller-2`, `controller-3`) managing state machine consensus on port `7001` (TCP transport), gRPC metadata service on port `8001` (HTTP/2), and REST API / Web Console on port `9001` (HTTP).
* **Storage Layer (Rust 1.98.1 Edition 2024)**: High-performance storage brokers (`broker-1`, `broker-2`) providing dual-protocol ingress on port `9091` (native binary) and port `9092` (Apache Kafka wire protocol), mounting direct NVMe storage pools and replicating partitions over low-latency TCP channels.


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
| `shard_threads` | `--shard-threads` | `0` (auto-detect) | Number of pinned shard worker threads. `0` = auto-detect one per CPU core; `1` = single-thread mode; `N` = fixed thread count. |
| `storage.max_segment_size` | — | `134217728` (128 MB) | Log segment roll-over size threshold. |
| `storage.max_retention_size` | — | `1073741824` (1 GB) | Maximum disk usage per partition before segment eviction. |
| `storage.max_retention_age_secs` | — | `604800` (7 days) | Age threshold for segment expiration and cleanup. |
| `storage.compaction_enabled` | — | `true` | Enables background cleaner thread running every 30s. |
| `storage.dirty_ratio_threshold` | — | `0.5` | Ratio of superseded records to trigger segment compaction. |
| `storage.tombstone_retention_secs` | — | `86400` (24h) | Time tombstones are preserved before physical deletion. |
| `storage.writeback_bytes` | — | `8388608` (8 MB) | Paced page-cache writeback interval (`libc::sync_file_range(SYNC_FILE_RANGE_WRITE)`). `0` disables writeback pacing. |
| `storage.drop_cache_after_writeback` | — | `false` | Evicts written pages from Linux page cache via `posix_fadvise(POSIX_FADV_DONTNEED)`. Caps memory in memory-starved containers. |
| `tiered_storage.enabled` | — | `false` | Asynchronously offload inactive segments to object store. |
| `tiered_storage.provider` | `--tiered-storage-provider` | `"disabled"` | Backend: `s3`, `gcs`, `azure`, `local`, `disabled`. |
| `tiered_storage.s3.bucket` | `--s3-bucket` | `""` | Target S3/MinIO bucket. |
| `tiered_storage.s3.endpoint_url`| `--s3-endpoint` | `None` | Custom S3 endpoint URL (MinIO, LocalStack, Ceph). |
| `tiered_storage.s3.region` | `--s3-region` | `"us-east-1"` | AWS Region. |
| `tiered_storage.local.root_path`| `--tiered-storage-dir` | `./data/tiered_storage` | Local directory or mounted NFS storage path. |

---

### 2.3 Tuning Shard-per-Core & Paced Writeback

#### 1. Sizing Shard Threads (`--shard-threads`)
- **Default (`0`)**: Automatically calls `std::thread::available_parallelism()` to allocate one shard per physical CPU core. Each shard runs on an isolated OS thread pinned via `libc::sched_setaffinity`.
- **Dedicated Bare-Metal / High-Core Hosts (16 – 64+ Cores)**: Leave at `0`. Partitions are evenly hashed across all available cores.
- **Shared / Burst Containers (e.g. 2 vCPU or 4 vCPU Kubernetes pods)**: Explicitly match your CPU request: `--shard-threads 2` or `--shard-threads 4`. This prevents thread over-subscription and eliminates scheduler migration.
- **Single-Thread Debugging Mode**: Set `--shard-threads 1` to disable actor sharding and run all partition operations sequentially on a single thread.

#### 2. Tuning Page-Cache Writeback (`storage.writeback_bytes`)
- **Default (`8388608` / 8 MiB)**: Initiates asynchronous page cache writeback via Linux `sync_file_range(SYNC_FILE_RANGE_WRITE)` every 8 MiB written to an active log segment.
- **High-End NVMe SSDs (>2 GB/s write)**: Increase to `16777216` (16 MiB) or `33554432` (32 MiB) to batch kernel I/O requests more aggressively while keeping dirty queues bounded.
- **Network / Cloud Attached Storage (AWS gp3 / Azure Premium SSD)**: Keep at `4194304` (4 MiB) or `8388608` (8 MiB) to maintain a steady, non-bursty IOPS profile, avoiding queue saturation and EBS credit depletion.
- **Disabling (`0`)**: Setting `writeback_bytes = 0` relies entirely on the Linux kernel dirty page background flusher (`vm.dirty_background_ratio`). **Not recommended in memory-limited containers**, as dirty page accumulation triggers multi-second write stalls.

#### 3. Dropping Cache Under Strict Memory Constraints (`storage.drop_cache_after_writeback`)
- **Default (`false`)**: Linux retains written log pages in the page cache so that recent consumer fetches are served directly from RAM (zero disk I/O).
- **When to Enable (`true`)**: For containers with severe memory limits (e.g. $\le 2\text{ GiB}$) that ingest massive write volumes with few real-time readers. After ranges are synced to disk, AeroStream issues `posix_fadvise(POSIX_FADV_DONTNEED)` to drop the physical memory pages, strictly bounding broker resident memory.


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

### 3.3 Graceful Broker Draining & Maintenance Procedures

Before terminating, upgrading, rebooting, or scaling down a Rust storage broker node, operators must execute an orderly drain to evacuate partition leadership and rebalance replicas to surviving nodes without downtime or client disconnect errors.

#### Step 1: Pre-Drain Cluster Health Inspection
Ensure the cluster is healthy and all partitions have sufficient in-sync replicas before initiating a drain:
```bash
# Verify active broker count and Raft leader
curl -s http://10.0.0.1:9001/api/cluster | jq .

# Verify that all topic partitions have ISR count >= 2
curl -s http://10.0.0.1:9001/api/topics | jq '.[] | {topic: .name, partitions: [.partitions[] | {id: .partition_id, leader: .leader_id, isr: .isr}]}'

# Confirm consumer groups are healthy and not severely lagged
curl -s http://10.0.0.1:9001/api/lag | jq .
```

#### Step 2: Trigger Graceful Broker Drain
Submit the drain request to the active controller REST API:
```bash
# Drain Broker 2 via REST
curl -s -X POST http://10.0.0.1:9001/api/brokers/2/drain | jq .

# Or via the AeroStream CLI
./client/bin/client drain-broker 2 http://10.0.0.1:9001
```

**Expected Controller Response**:
```json
{
  "success": true,
  "message": "broker 2 drained and partitions reassigned"
}
```

#### Step 3: What Happens Internally During Drain
Implemented in [`go-controller/pkg/consensus/fsm.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L183):
1. **Raft Proposal**: The leader controller proposes a `CmdDrainBroker` log entry through Raft quorum.
2. **Leader Migration**: For every partition where Broker 2 was leader, leadership is immediately reassigned to the most up-to-date replica currently present in the partition's ISR set.
3. **Replica Reassignment**: Broker 2 is removed from `ReplicaIDs`. If spare active brokers are available, they are assigned to replace Broker 2.
4. **ISR Cleansing**: Broker 2 is removed from the partition's ISR list, and the High Watermark is recomputed.
5. **Metadata Invalidation**: All connected Kafka clients and native TCP clients receive updated metadata on their next metadata refresh (or connection retry), seamlessly routing traffic to the new partition leaders in under 100 milliseconds.

#### Step 4: Verification & Broker Shutdown
Confirm that Broker 2 is no longer leading any partitions:
```bash
# Verify that no partition has leader_id == 2
curl -s http://10.0.0.1:9001/api/topics | jq '[.[] | .partitions[] | select(.leader_id == 2)] | length'
# Output should be: 0
```

Now safely stop the broker daemon:
```bash
# Systemd
sudo systemctl stop aerostream-broker

# Or Docker / Kubernetes
docker stop aerostream-broker-2
```

#### Step 5: Post-Maintenance Recovery & Rejoin
When maintenance is complete (e.g. OS upgrade, NVMe replacement, binary update), start the broker:
```bash
sudo systemctl start aerostream-broker
```
Upon startup:
1. The broker sends `RegisterBroker` via gRPC to the controller, re-registering host and ports.
2. The controller marks the broker `Active = true`.
3. The broker establishes its 2-second heartbeat loop and resumes native Command 3 replica fetching from current partition leaders.
4. As its local LEO catches up within `cluster.replica_lag_tolerance`, the controller automatically re-admits the broker to the partition's ISR set.

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

### 4.3 Monitoring Shard Queues, Dirty Writeback & Consumer Lag

Operating a high-throughput Shard-per-Core deployment requires tracking three critical performance dimensions:

#### 1. Shard Actor Queue Depth & Core Saturation
Each CPU-pinned shard thread processes requests from an asynchronous `flume` channel mailbox.
- **Metric**: `aerostream_shard_queue_depth{shard="<id>"}`
- **Healthy Baseline**: 0 to 50 queued operations under high load.
- **Investigation / Alert Condition**: If queue depth climbs consistently above 1,000 operations, the physical core is saturated or disk I/O latency on that shard's partitions has degraded.
- **Operator Remedy**:
  * Check partition distribution across shards: ensure hot topics have partitions distributed evenly across all cores.
  * Increase shard thread allocation using `--shard-threads` or scale out by adding storage brokers to rebalance partition leadership.

#### 2. Linux Page-Cache Dirty Memory & Writeback Health
AeroStream's paced writeback (`storage.writeback_bytes`) is designed to keep dirty page accumulation flat, preventing Linux kernel flusher stalls.
- **OS Health Check**:
  ```bash
  cat /proc/meminfo | grep -E "(Dirty|Writeback):"
  ```
- **Healthy Profile**: `Dirty` memory should hover steadily under 16 – 32 MiB, even while ingesting hundreds of megabytes per second.
- **Warning Profile**: If `Dirty` memory continues to climb steadily towards `vm.dirty_ratio` limits (>100 MiB), disk hardware write throughput is saturated.
- **Operator Remedy**:
  * Verify that `storage.writeback_bytes` is active (default 8 MiB, non-zero).
  * Check NVMe disk write latency using `iostat -xz 1` (`%util` and `w_await`).
  * If running under strict cgroup limits (e.g. 2 GiB memory), enable `storage.drop_cache_after_writeback = true` to force immediate page reclamation.

#### 3. Real-Time Consumer Lag Monitoring (`GET /api/lag`)
AeroStream computes lag dynamically as the difference between the partition High Watermark and the consumer group's committed offset:
$$\text{Consumer Lag} = \text{HighWatermark} - \text{CommittedOffset}$$

Query the live lag endpoint:
```bash
curl -s http://10.0.0.1:9001/api/lag | jq .
```

**Response Format**:
```json
[
  {
    "group": "order_processing_workers",
    "topic": "orders",
    "partition": 0,
    "high_watermark": 104850,
    "committed_offset": 104840,
    "lag": 10
  },
  {
    "group": "order_processing_workers",
    "topic": "orders",
    "partition": 1,
    "high_watermark": 89200,
    "committed_offset": 89198,
    "lag": 2
  }
]
```

- **Prometheus Metric**: `aerostream_consumer_group_lag{group="<group>",topic="<topic>",partition="<id>"}`
- **Alert Condition**: Trigger alerts when `lag > 5000` or when lag exhibits monotonic increase over 5 consecutive evaluation windows (indicating downstream consumer stall or processing bottleneck).

---

### 4.4 Web UI Console Management

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
    kafka_port=9092,
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
AeroStream brokers implement Kafka Wire Protocol natively on port 9092. Existing standard Kafka producers and consumers work out of the box:

```python
from confluent_kafka import Producer, Consumer

# Producer
p = Producer({'bootstrap.servers': 'localhost:9092'})
p.produce('orders', key='cust_1', value='{"status": "CONFIRMED"}')
p.flush()

# Consumer
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
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
    bootstrap-servers: localhost:9092
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

---

### 5.5 .NET / C# Integration (`Confluent.Kafka`)

AeroStream is verified 100% compatible with .NET 8 / 9 using the official `Confluent.Kafka` client library.

#### Project Configuration (`.csproj`)
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Confluent.Kafka" Version="2.15.1" />
  </ItemGroup>
</Project>
```

#### Idempotent Producer & Consumer Example
```csharp
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            EnableIdempotence = true, // KIP-98 exactly-once semantics
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        var report = await producer.ProduceAsync("orders", new Message<string, string>
        {
            Key = "order-101",
            Value = "{\"status\": \"CONFIRMED\", \"amount\": 149.99}"
        });
        Console.WriteLine($"Delivered to {report.TopicPartitionOffset}");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:9092",
            GroupId = "order-processing-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe("orders");
        var cr = consumer.Consume(TimeSpan.FromSeconds(5));
        if (cr != null)
        {
            Console.WriteLine($"Consumed: {cr.Message.Key} -> {cr.Message.Value}");
        }
    }
}
```

*For the complete automated 4-test test suite, see [`examples/dotnet-app/`](../examples/dotnet-app/).*

---

## 6. Troubleshooting & Operational Diagnostics

### 6.1 `Error: "Invalid protocol magic bytes"` on Broker Connection

#### Symptoms & Log Signature
The broker logs show rapid repeated connection attempts from an internal or client IP address failing immediately:
```text
2026-09-27T07:41:48.646104Z ERROR rust_broker::net::server: [AeroMQ Broker] Error handling connection from 127.0.0.1:58198: "Invalid protocol magic bytes"
2026-09-27T07:41:48.694697Z INFO  rust_broker::net::server: [AeroMQ Broker] Accepted connection from 127.0.0.1:58208 on CPU core 11
2026-09-27T07:41:48.694841Z ERROR rust_broker::net::server: [AeroMQ Broker] Error handling connection from 127.0.0.1:58208: "Invalid protocol magic bytes"
```

#### Dual-Listener Architecture Context
AeroStream runs two distinct TCP socket listeners per broker instance:
1. **Port 9091 (`rust_broker::net::server`)**: The **Native AeroStream Protocol**. All frames sent to this port **must** start with the 2-byte magic header `0xAE 0x51` (`AE`, `RO`), followed by command and body length.
2. **Port 9092 (`rust_broker::net::kafka_server`)**: The **Kafka Wire Protocol Engine**. It parses standard Kafka framing: `[int32 message_length][int16 api_key][int16 api_version][int32 correlation_id]...`.

#### Root Cause Analysis
This error occurs when a **Kafka client** inadvertently connects to the **native data port (9091)** instead of the **Kafka wire port (9092)**:

1. **Bootstrap & Metadata Request**:
   The Kafka client (e.g. `librdkafka`, `Confluent.Kafka`, Java `kafka-clients`) connects to `bootstrap.servers = 127.0.0.1:9092` and issues a `MetadataRequest` (ApiKey 3) or `FindCoordinatorRequest` (ApiKey 10).
2. **Advertised Port Misconfiguration**:
   If the broker's advertised broker topology lists the node's internal data port (`9091`) instead of its Kafka port (`9092`), `librdkafka` and standard Kafka client drivers dynamically update their internal node routing table with `host:9091`.
3. **Protocol Collision**:
   The Kafka client disconnects from the bootstrap socket and reconnects to `127.0.0.1:9091`, sending standard Kafka binary frames (such as `00 00 00 23 00 03 ...`).
4. **Header Rejection**:
   The native server reads the first two bytes (`0x00 0x00`), fails the `is_valid_magic` validation (`0x00 0x00 != 0xAE 0x51`), and terminates the connection with `"Invalid protocol magic bytes"`. The client driver immediately retries, creating an error loop.

```
Kafka Client                                  AeroStream Broker
────────────                                  ─────────────────
   │                                                  │
   │─── 1. MetadataRequest (ApiKey 3) ───────────────>│ Port 9092 (Kafka Listener)
   │                                                  │
   │<── 2. MetadataResponse (brokers=[host:9091]) ────│ ⚠️ Advertised Data Port!
   │                                                  │
   │─── 3. Reconnect to 127.0.0.1:9091 ──────────────>│ Port 9091 (Native Listener)
   │                                                  │
   │─── 4. Send Kafka Frame (00 00 00 ...) ──────────>│ Expects [0xAE, 0x51]!
   │                                                  │
   │<── 5. TCP RST ("Invalid protocol magic bytes") ──│ ❌ Connection Terminated
```

#### Resolution & Prevention
1. **Dynamic Metadata Port Override**:
   [`rust-broker/src/net/kafka_server.rs`](../rust-broker/src/net/kafka_server.rs) and [`rust-broker/src/kafka/admin.rs`](../rust-broker/src/kafka/admin.rs) ensure that `MetadataResponse` (ApiKey 3) and `FindCoordinatorResponse` (ApiKey 10) strictly advertise `cfg.kafka_port` (default 9092) for Kafka clients.
2. **Client Configuration Verification**:
   Always configure Kafka client libraries to point to the designated Kafka port (`9092`):
   - `.NET / C#`: `BootstrapServers = "127.0.0.1:9092"`
   - `Python`: `bootstrap_servers=['127.0.0.1:9092']`
   - `Java / Spring`: `spring.kafka.bootstrap-servers=localhost:9092`
   - Only native Go and Rust client drivers should connect to port `9091`.
