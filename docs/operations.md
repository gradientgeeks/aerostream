# Production Operations & Kubernetes

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Operations</span>
<span class="md-tag">8 min read</span>
<span class="md-tag">Kubernetes & Sizing</span>
</div>

Operating AeroStream in production is straightforward due to its lean memory footprint, lack of JVM Garbage Collection tuning, and native container cgroup awareness.

This guide outlines hardware sizing, Kubernetes StatefulSet deployment patterns, graceful scale-down draining, and Prometheus observability.

---

## Production Hardware Sizing

Thanks to Rust's zero-copy architecture and Go's Green Tea GC, AeroStream achieves exceptional compute density compared to legacy JVM-based platforms:

| Scale Tier | Throughput Target | Recommended CPU | Recommended RAM | Storage Configuration |
|---|---|---|---|---|
| **Edge / Dev** | Up to 50 MB/s | 1 vCPU | 512 MiB | Standard SATA / Cloud SSD |
| **Standard Production** | Up to 500 MB/s | 2 – 4 vCPUs | 2 – 4 GiB | Single NVMe SSD + S3 Tiered Storage |
| **Extreme Scale** | 1,000+ MB/s | 8 vCPUs (Pinned) | 8 – 16 GiB | Dual NVMe RAID-0 + S3 Tiered Storage |

!!! note "RAM Comparison: AeroStream vs Apache Kafka"
    Apache Kafka typically recommends **32 GiB to 64 GiB of RAM** per broker node to accommodate JVM heap allocations, GC buffers, and OS page cache. AeroStream delivers higher sustained throughput with just **2 GiB to 4 GiB of RAM**, eliminating out-of-memory (OOM) killer risks under bursty load.

---

## Kubernetes StatefulSets & PreStop

In Kubernetes, AeroStream is deployed as a **StatefulSet** backed by persistent volume claims (PVCs) for local NVMe storage:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: aerostream-broker
  namespace: aerostream
spec:
  serviceName: aerostream-headless
  replicas: 3
  selector:
    matchLabels:
      app: aerostream-broker
  template:
    metadata:
      labels:
        app: aerostream-broker
    spec:
      containers:
        - name: broker
          image: quay.io/gradientgeeks/aerostream:latest
          ports:
            - containerPort: 9091
              name: native-tcp
            - containerPort: 9092
              name: kafka-wire
            - containerPort: 9001
              name: http-rest
            - containerPort: 8001
              name: grpc
            - containerPort: 7001
              name: raft
          resources:
            requests:
              cpu: "2"
              memory: "2Gi"
            limits:
              cpu: "4"
              memory: "4Gi"
          lifecycle:
            preStop:
              exec:
                command:
                  - "/bin/sh"
                  - "-c"
                  - "curl -s -X POST http://127.0.0.1:9001/api/brokers/${HOSTNAME##*-}/drain && sleep 10"
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: nvme-storage
        resources:
          requests:
            storage: 200Gi
```

---

## Graceful Broker Partition Draining

When scaling down a cluster or taking a node offline for kernel upgrades, simply killing the broker process triggers emergency leader elections and transient consumer disconnections.

![Cluster Topology Scale Down](images/cluster_topology_scale_down.png)

AeroStream provides **Automated Partition Draining**:

```mermaid
sequenceDiagram
    autonumber
    actor Admin as SRE / K8s PreStop
    participant Ctrl as Controller Raft Leader
    participant B1 as Broker 1 (Draining)
    participant B2 as Broker 2 (Replica)

    Admin->>Ctrl: POST /api/brokers/1/drain
    Ctrl->>B1: Mark Draining (Stop accepting new leader partitions)
    Ctrl->>B2: Promote In-Sync Replica (ISR) to Partition Leader
    Note over B2: B2 assumes leadership seamlessly
    Ctrl->>B1: Confirm All Partitions Reassigned
    Ctrl-->>Admin: HTTP 200 OK (Drain Complete)
```

Trigger draining manually via the REST API:

```bash
# Gracefully drain broker ID 1
curl -X POST http://localhost:9001/api/brokers/1/drain
```

---

## Prometheus Observability & Health Checks

AeroStream exposes standard Prometheus metrics at `http://localhost:9001/metrics`.

### Key Performance Indicators (KPIs)

* `aerostream_produce_messages_total`: Cumulative count of ingested messages by topic and partition.
* `aerostream_produce_bytes_total`: Total ingress volume in bytes.
* `aerostream_fetch_bytes_total`: Total egress volume served via `sendfile(2)`.
* `aerostream_active_connections`: Current active TCP client connections.
* `aerostream_raft_leader_status`: 1 if the current node is the elected Raft leader, 0 otherwise.
* `aerostream_segment_roll_duration_seconds`: Time taken to seal and roll segment files.

### Health Check Probes

Configure Kubernetes probes to verify node health:

```yaml
livenessProbe:
  httpGet:
    path: /status
    port: 9001
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 9001
  initialDelaySeconds: 3
  periodSeconds: 5
```

---

## Automated AI Documentation Synchronization

AeroStream incorporates an automated documentation synchronization pipeline powered by **Google Antigravity** and **Gemini** (`.github/workflows/ai-docs-writer.yml`).

### Workflow Mechanics

1. **Diff Extraction**: On every push to `main` (or PR), git diffs across the Rust Broker, Go Controller, Proto contracts, Client SDKs, and Web UI are extracted.
2. **AI Analysis**: Google Antigravity / Gemini inspects architectural shifts, configuration additions, and API key updates.
3. **Documentation Regeneration**: Updates are generated for both MkDocs Material docs (`docs/`) and core whitepapers (`core/docs/`).
4. **Pull Request Automation**: A Pull Request is automatically opened against `main` via `peter-evans/create-pull-request` with a detailed markdown summary of documentation changes.

### Configuration

Add the `GEMINI_API_KEY` secret to your GitHub Repository settings under **Settings > Secrets and variables > Actions**:

* `GEMINI_API_KEY`: Google AI Studio or Gemini Developer API Key (or Service Account JSON).

