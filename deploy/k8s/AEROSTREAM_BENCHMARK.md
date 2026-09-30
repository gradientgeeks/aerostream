# AeroStream Kubernetes Benchmarking Guide

This guide provides a reproducible methodology for benchmarking **AeroStream** inside a Kubernetes cluster with fixed, explicit resource boundaries (CPU, memory, and NVMe/SSD storage), and for recording the results in a consistent way.

For AeroStream's published results (AWS EC2, OpenMessaging Benchmark, Kafka wire port) see [`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md).

---

## 1. How AeroStream Is Built (What You Are Measuring)

```
┌────────────────────────────────────────────────────────────────────────┐
│                       AeroStream (Rust + Tokio + Go)                   │
│                                                                        │
│  Client TCP ──> Thread-Pinned Tokio ──> Zero-Copy DMA ──> Commit Log   │
│  (sched_setaffinity, sendfile(2) direct kernel transfer, no GC)        │
└────────────────────────────────────────────────────────────────────────┘
```

| Dimension | AeroStream |
| :--- | :--- |
| **Runtime engine** | Native compiled Rust data plane + Go control plane |
| **Garbage collection** | None in the data plane (deterministic RAII memory management) |
| **Data plane I/O** | Tokio asynchronous epoll + Linux `sendfile(2)` zero-copy |
| **CPU scheduling** | Worker threads pinned per CPU core (`sched_setaffinity`) |
| **Metadata consensus** | HashiCorp Raft FSM in Go (isolated control plane) |

---

## 2. Experimental Environment: Resource Specification

Fix the resource limits up front and record them with every result, so runs can be compared with each other.

### Node & Pod Resource Sizing

| Metric | AeroStream Broker Pod |
| :--- | :--- |
| **CPU Request / Limit** | `1000m` / `2000m` (1-2 vCPUs) |
| **Memory Request / Limit** | `1024Mi` / `4096Mi` |
| **Storage Class** | High-IOPS NVMe / `standard-ssd` |
| **Volume Size** | `50Gi` PVC |
| **Cluster Topology** | 3 Go Controllers + 2 Rust Storage Brokers |

---

## 3. Step-by-Step AeroStream Deployment in Kubernetes

1. **Apply all AeroStream manifests:**
   ```bash
   kubectl apply -f deploy/k8s/namespace.yaml
   kubectl apply -f deploy/k8s/configmap.yaml
   kubectl apply -f deploy/k8s/controller-statefulset.yaml
   kubectl apply -f deploy/k8s/broker-statefulset.yaml
   kubectl apply -f deploy/k8s/services.yaml
   ```

2. **Verify AeroStream cluster readiness:**
   ```bash
   kubectl wait --namespace aerostream --for=condition=ready pod -l app.kubernetes.io/name=aerostream --timeout=120s
   ```

3. **Verify cluster controller quorum:**
   ```bash
   kubectl exec -n aerostream controller-0 -- curl -s http://localhost:9001/status
   ```
   *Expected output:*
   ```json
   {
     "node_id": "node1",
     "state": "Leader",
     "brokers_count": 2,
     "topics_count": 0
   }
   ```

---

## 4. Benchmark Protocols & Workloads

The manifests in this directory expose AeroStream's native data port (`9091`) through the `aerostream-broker` service. The commands below use the native benchmark client. To benchmark the Kafka wire port (`9092`), add a `9092` port to the broker service first, then point the OpenMessaging Benchmark (see [`core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md`](../../core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md)) or Kafka's `kafka-producer-perf-test.sh` at it.

### Benchmark 1: High-Concurrency Throughput Burst

*Goal: Saturate the broker's ingestion pipeline with 20 concurrent producers pushing 1 KB records.*

Launch via the preconfigured Kubernetes Job:
```bash
kubectl apply -f deploy/k8s/benchmark-job.yaml
kubectl wait --namespace aerostream --for=condition=complete job/aerostream-benchmark --timeout=180s
kubectl logs -n aerostream job/aerostream-benchmark
```

Or execute directly from an interactive pod / CLI:
```bash
kubectl run aerostream-bench-runner --rm -it --namespace aerostream \
  --image=aerostream-client:latest --image-pull-policy=IfNotPresent -- \
  --controller aerostream-controller.aerostream.svc.cluster.local:8001 \
  bench \
  --producers 20 \
  --messages 50000 \
  --size 1024 \
  --topic bench-test \
  --partition 0
```

### Benchmark 2: Latency-Critical Ingestion (Tail SLA)

*Goal: Measure individual message end-to-end write ACK latency under moderate concurrency without client-side artificial batching.*

```bash
kubectl run aerostream-latency-test --rm -it --namespace aerostream \
  --image=aerostream-client:latest --image-pull-policy=IfNotPresent -- \
  --controller aerostream-controller.aerostream.svc.cluster.local:8001 \
  bench \
  --producers 10 \
  --messages 10000 \
  --size 512 \
  --topic latency-test \
  --partition 0
```

---

## 5. Key Metrics to Watch

Record these four dimensions for every run.

### 1. Tail Latency & Percentile Degradation (p50, p95, p99, p99.9)

- Record the average, median, p95, p99, p99.9 and maximum latency the client reports. Tail percentiles show pauses that averages hide.
- At saturation, latency reflects queueing rather than the broker's steady-state behaviour; for latency, compare runs at a fixed offered load.
- The append path takes no runtime locks across partitions: Tokio workers are non-blocking and each partition is owned by one shard.

### 2. CPU Utilization & Core Affinity

- **Context switches:** check with `pidstat -w 1`.
- **Core pinning:** each Tokio worker thread is pinned to a CPU core with `libc::sched_setaffinity(0, ...)`, which keeps cache lines warm in L1/L2 and avoids CPU migrations across physical cores.
- **Zero-copy transfer:** fetches use Linux `sendfile(2)` directly on the file descriptor. Data moves from the kernel page cache to the network socket buffers without being copied through userspace.

### 3. Memory Footprint

Monitor pod memory with `kubectl top`:

```bash
kubectl top pods -n aerostream
```

Record idle memory and peak memory during the run. Container memory includes the page cache, so it rises toward the limit under sustained write load; watch the broker process's RSS as well if you need resident memory.

### 4. Disk IOPS and Page Cache Writebacks

- Inspect disk write rates using `iostat -xz 1` on the underlying host node.
- **Sequential append:** AeroStream uses fixed 64-bit offsets and compact index entries, so per-frame disk metadata overhead is small.
- **Segment rollover:** closed segments move into tiered cold storage asynchronously without holding locks on the active segment, so producers are not stalled at segment boundaries.

---

## 6. Results Template

Fill in one row per run and keep the raw tool output alongside it.

| Metric | Result |
| :--- | :--- |
| **Date / commit / image digest** | |
| **Pod CPU and memory limits** | |
| **Storage class and volume size** | |
| **Workload (producers, messages, size, partitions)** | |
| **Ingestion throughput (msgs/sec, MB/s)** | |
| **Average latency** | |
| **p50 latency (median)** | |
| **p99 latency** | |
| **p99.9 latency** | |
| **Memory footprint (idle / peak)** | |
| **Process startup time** | |

---

## 7. Troubleshooting & Performance Tuning Tips

1. **Host Network & Port Forwarding:**
   When running benchmarks externally from outside Kubernetes, use the LoadBalancer or NodePort services defined in `deploy/k8s/services.yaml` (`aerostream-ui-external` and `aerostream-broker-external`).
2. **Storage Volume Selection:**
   Always test using a persistent volume with defined IOPS provisioning (`gp3`, `io2`, or local NVMe SSDs) rather than standard HDD storage classes, as mechanical seek latencies will obscure the broker's own behaviour.
3. **CPU Throttling Check:**
   Verify Kubernetes CFS quota throttling using:
   ```bash
   kubectl exec -n aerostream broker-0 -- cat /sys/fs/cgroup/cpu.stat
   ```
   Ensure `nr_throttled` is near 0 by allocating sufficient CPU limits during maximum throughput runs.
