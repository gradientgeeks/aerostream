# AeroStream: System Architecture & Engineering Deep-Dive

**AeroStream** is a distributed, high-throughput, cloud-native event streaming and messaging engine designed for microsecond-latency event distribution, petabyte-scale retention, and modern cloud deployment. AeroStream addresses fundamental architectural bottlenecks in traditional distributed streaming platforms (such as JVM memory churn, stop-the-world garbage collection pauses, heavy thread thrashing, and fragile external consensus dependencies) by implementing a **Dual-Engine Architecture**:

1. A distributed, resilient **Control Plane** authored in **Go 1.26**, utilizing HashiCorp Raft for cluster state consensus, metadata quorum, schema governance, RBAC enforcement, connectors management, stream transforms, and Green Tea GC with SIMD Swiss Tables.
2. An extreme-performance, zero-copy **Data Plane** authored in **Rust 1.98.1 (Edition 2024)**, utilizing Tokio 1.48 with CPU-pinned worker threads, kernel `sendfile(2)` zero-copy transfers, memory-mapped (`mmap`) indices, append-only segmented commit logs, Kafka wire-protocol compatibility, and multi-cloud tiered storage offloading.

---

## 1. Executive Architectural Overview & Dual-Engine Rationale

Traditional event streaming architectures often compromise between runtime simplicity, developer velocity, operational stability, and hardware utilization. Systems implemented entirely on the Java Virtual Machine require hundreds of megabytes or gigabytes of heap memory, extensive garbage collection tuning, and hundreds of threads even under minimal workload. Systems implemented entirely in lower-level C++ or Rust often struggle with the complexity of maintaining stateful control plane services, dynamic REST endpoints, schema registries, and third-party connector ecosystems.

AeroStream resolves this dichotomy through clean physical and architectural decoupling:

![AeroStream Dual-Engine Architecture](images/dual_engine_architecture.png)

The AeroStream Dual-Engine topology separates ingress and storage into distinct operational planes:

* **Client Ingress Layer**: Supports Kafka clients (`:9092`), native binary streaming clients (`:9091`), Angular 21 Web Console (`:9001`), and HTTP REST endpoints (`:9001`).
* **Rust Storage Data Plane (Ports 9091/9092)**: Pinned Tokio worker threads, zero-copy `sendfile(2)` kernel dispatch, Shard-per-Core partition logs, hardware CRC32C, idempotent producer tracking, and background log compaction.
* **Go Raft Control Plane (Ports 7001/8001/9001)**: HashiCorp Raft quorum consensus, dynamic metadata FSM, Confluent-compatible Schema Registry, Swiss Tables SIMD RBAC evaluation, stream transforms, and connector runtimes.
* **Tiered Storage Pipeline**: Seamless offload from local NVMe hot segments to AWS S3, MinIO, Google Cloud Storage, or Azure Blob Storage.

### Why Go for the Control Plane?

1. **Rich Distributed Systems Ecosystem**: The Go ecosystem is the gold standard for consensus protocols, metadata coordination, and cloud-native integration (HashiCorp Raft, etcd/Raft, Kubernetes client libraries, gRPC-Go).
2. **Rapid Extensibility & Memory Safety**: The control plane requires comprehensive JSON/REST APIs, Confluent-compatible Schema Registry endpoints, role-based authorization rules, dynamic stream transformations, and Kafka Connect management. Go provides high concurrency with lightweight goroutines without the manual memory management or compile-time lifetime mechanics that would slow down iteration in Rust.
3. **Deterministic Memory Footprint for Quorum**: Control plane metadata state resides strictly in structured memory maps (topics, partitions, consumer group generations, ACLs), replicated via Raft logs and periodically snapshotted to disk.

### Why Rust for the Data Plane?

1. **Predictable Latency & Zero Garbage Collection**: The commit log storage engine, socket read/write loops, and segment rollover pipelines must never suffer from garbage collection pauses. Rust's compile-time ownership model guarantees zero GC stalls.
2. **Zero-Copy Kernel Socket Transfers**: For fetch operations, AeroStream invokes Linux [`sendfile(2)`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/server.rs#L135), transferring log segment pages directly from the Linux Page Cache to the network socket descriptor without transitioning through userspace memory buffers.
3. **Strict Memory Mapping & Cache Management**: Index lookups leverage fixed-size 16-byte entries (`[offset: 8 bytes BE][position: 8 bytes BE]`), allowing sub-microsecond binary searches over memory-mapped files without heap allocations.
4. **Hardware Affinity**: Tokio worker threads are pinned to isolated CPU cores using [`libc::sched_setaffinity`](file:///home/uttam/projects/AeroMQ/rust-broker/src/main.rs#L144), eliminating thread context switching and cache bouncing.

### Measured results

Tested under strict container constraints (`--cpus=2.0 --memory=2g`), single node, median of 3 runs, host networking, image `quay.io/gradientgeeks/aerostream:latest`.
Full methodology, per-run ranges and durability caveats are in [`benchmarks/BENCHMARK.md`](../../benchmarks/BENCHMARK.md).

| Metric | AeroStream (native port) | AeroStream (Kafka port) |
| :--- | ---: | ---: |
| **100 B messages (msgs/s)** | **186,727** | **188,324** |
| **1 KB messages (msgs/s)** | **174,714** | 71,023 |
| **1 MB messages (MB/s)** | 347 (277-1,233) | 333 |
| **10 MB messages (MB/s)** | 276 (271-914) | 166 |
| **50 MB messages (MB/s)** | **280** | 81 |
| **Broker idle memory** | **1.3-5.4 MiB** | 1.3 MiB |
| **Peak memory under load** | 320 MiB | 143 MiB |
| **OS threads** | 3 | 3 |
| **Time until usable** | 1.8-3.4 s | 1.8-3.4 s |


### OpenMessaging Benchmark (OMB) Results on AWS EC2 (`c6id.2xlarge`)

AeroStream was evaluated using the official Linux Foundation OpenMessaging Benchmark (OMB) suite through its Kafka wire protocol port (`9092`) on an AWS `c6id.2xlarge` (8 vCPUs, 16 GiB RAM, local NVMe SSD, 32 partitions, 1,024-byte payloads):

| Workload Target | Actual Publish Rate | Publish $p_{50}$ | Publish $p_{99}$ | Publish $p_{99.9}$ | End-to-End $p_{99}$ | Broker Cores Busy | Errors |
|---|---|---|---|---|---|---|---|
| **100,000 msg/s** (fixed) | 100,082 msg/s (97.7 MB/s) | **0.7 ms** | **1.4 ms** | **2.3 ms** | 2.0 ms | 14% | 0 |
| **200,000 msg/s** (fixed) | 200,175 msg/s (195.5 MB/s) | **0.7 ms** | **1.7 ms** | **3.0 ms** | 2.0 ms | 22% | 0 |
| **Maximum Rate** (unthrottled) | **271,350 msg/s** (265.0 MB/s) | 105 ms | 1,104 ms | 1,376 ms | 1,119 ms | 55% | 0 |

---

## 2. Control Plane Deep-Dive (`go-controller/`)

The AeroStream Control Plane runs as the central coordination daemon (`controller`). Engineered in **Go 1.26**, it leverages Go 1.26's **Green Tea GC** (providing ultra-low sub-millisecond stop-the-world pause guarantees), compiler-intrinsic **Swiss Tables** (SIMD-accelerated hash map lookups with fast metadata control bytes), and optimized goroutine scheduling. It manages cluster membership, topic partitions, leader assignments, high-watermark computation, schema governance, stream transformations, connectors, and role-based access control.

![AeroStream Controller-Broker Orchestration](images/controller_broker_orchestration.png)

The controller architecture decouples ingress interfaces, state consensus, and domain subsystems:
* **Ingress Protocols**:
  * **REST Management API (`:9001`)**: Administers topics, partitions, broker draining, schemas, transforms, connectors, and metrics.
  * **gRPC Control Service (`:8001`)**: High-throughput multiplexed HTTP/2 channel for broker registrations, status heartbeats, metadata updates, and replica ISR synchronization.
  * **Raft Consensus Transport (`:7001`)**: Dedicated TCP transport for HashiCorp Raft log replication, leader heartbeats, and cluster state agreement.
* **State Subsystems & Core Engines**:
  * **Consensus Core ([`RaftNode`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L14) & [`FSM`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L78))**: Finite state machine executing Raft commands, maintaining [`ClusterState`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L70), and generating compact point-in-time state snapshots.
  * **Schema Registry Engine ([`Registry`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/registry.go#L105))**: Thread-safe schema evolution evaluator supporting Avro, JSON Schema, and Protobuf with backward, forward, and full compatibility validation.
  * **Stream Transform Engine ([`Engine`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L52))**: Real-time record mutator running PII redaction, field extraction, mathematical expressions, and sandboxed WebAssembly (WASI) modules.
  * **RBAC & ACL Manager ([`AclManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/auth/acls.go#L66))**: Role-based access control engine evaluating fine-grained principal permissions with wildcard matching.
  * **Connect Manager ([`ConnectorManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/connect/manager.go#L53))**: Kafka Connect-compatible connector lifecycle manager and task distributor.


### 2.1 Raft Consensus Engine

The consensus core is implemented in [`RaftNode`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L14) using the HashiCorp Raft implementation.

1. **Node Initialization & Transport**:
   * [`NewRaftNode`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L20) parses [`ControllerConfig`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/config/config.go).
   * Configures heartbeat timeouts, election timeouts (1000ms default), leader lease timeouts (500ms), and commit timeouts (50ms).
   * Binds a dedicated TCP transport on port `7001` using `raft.NewTCPTransport`.
2. **Log, Stable, and Snapshot Storage**:
   * In-memory or file-backed stable storage is allocated alongside a [`raft.FileSnapshotStore`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L63) keeping the 2 most recent snapshots on disk under `cfg.DataDir`.
   * When `bootstrap = true`, the initial node bootstraps the single-voter cluster configuration. Subsequent nodes execute [`RaftNode.Join`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L155) to be admitted as voters via `AddVoter`.
3. **Finite State Machine ([`FSM`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L78))**:
   * Implements `raft.FSM` with [`Apply`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L129), [`Snapshot`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L841), and [`Restore`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L854).
   * Operates over a unified state container [`ClusterState`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L70):
     ```go
     type ClusterState struct {
         Brokers        map[uint32]*BrokerStatus        `json:"brokers"`
         Topics         map[string]*TopicState          `json:"topics"`
         ConsumerGroups map[string]*ConsumerGroupState  `json:"consumer_groups"`
         Offsets        map[string]int64                `json:"offsets"`
     }
     ```
   * State modifications are submitted through [`RaftNode.Propose(op, payload)`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L97). If the local node is not the leader, `Propose` fails immediately and redirects the caller to the active leader address returned by `Raft.Leader()`.

### 2.2 Topic & Partition Metadata State Machine

The FSM processes discrete state transition commands:

* **`CmdRegisterBroker`**: Adds or reactivates a broker record ([`BrokerStatus`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L21)), setting `Active: true` and updating `LastSeen`.
* **`CmdCreateTopic`**: Distributes partition replicas deterministically across active brokers using round-robin distribution:
  ```go
  leaderID = activeBrokers[int(i)%len(activeBrokers)]
  for r := uint32(0); r < payload.ReplicationFactor && r < uint32(len(activeBrokers)); r++ {
      replicaIdx := (int(i) + int(r)) % len(activeBrokers)
      replicas = append(replicas, activeBrokers[replicaIdx])
  }
  ```
* **`CmdBrokerHeartbeat`**: Updates replica log end offsets reported by storage brokers.
* **In-Sync Replicas (ISR) & High-Watermark (HW) Evaluation**:
  [`updateISRAndHW()`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L404) re-evaluates all partitions:
  1. The leader is included in the ISR if it is active and its heartbeat is within `brokerInactiveTimeout` (default 6 seconds).
  2. Follower replicas are validated: if `(leaderOffset - followerOffset) <= replicaLagTolerance` (default 1000 offsets), the follower is retained in `ISR`.
  3. The **High-Watermark** is strictly computed as the minimum offset among all replicas currently in the ISR:
     $$\text{HighWatermark} = \min_{r \in \text{ISR}} (\text{ReplicaOffsets}[r])$$
     Consumers are never permitted to read past the High-Watermark.

### 2.3 Cooperative Sticky Consumer Group Rebalance Protocol

AeroStream implements a non-blocking cooperative sticky rebalance protocol ([`rebalanceGroup`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L497)), modeled after Kafka's KIP-848:

1. **State Preservation**: When members join or leave, active members retain their currently assigned partitions (`g.Assignments[mID]`).
2. **Quota Calculation**: For each topic, partitions ($N$) and subscribed members ($M$) define the baseline quota $\lfloor N/M \rfloor$ and remainder $N \pmod M$.
3. **Cooperative Shedding**:
   * Members holding more than their allowed max quota shed excess partitions into `m.RevokingPartitions`.
   * Partitions not held by any member (or orphaned from departing members) are gathered into `unassigned`.
4. **Least-Loaded Migration**: Unassigned partitions are allocated exclusively to the least-loaded subscribed members, minimizing partition movement and eliminating stop-the-world consumer stalls.

### 2.4 Built-In Confluent-Compatible Schema Registry

Implemented in [`schemaregistry.Registry`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/registry.go#L105), AeroStream provides a drop-in replacement for the Confluent Schema Registry on `/subjects`, `/schemas`, and `/compatibility`:

* **Supported Formats**:
  * **Avro**: Validated as strict JSON; fields parsed for types, defaults, and promotions.
  * **JSON Schema**: Draft-07 compatible property parsing with `required` and `default` validation.
  * **Protobuf**: Proto3 specification parsing extracting field tags and types.
* **Global Deduplication & Versioning**:
  * When a schema is registered via [`RegisterSchema`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/registry.go#L126), AeroStream computes a canonical hash `type:canonicalStr`. If already present globally, the existing `id` is reused; otherwise, `GlobalIDCounter` is incremented.
  * Per-subject versions increment sequentially (`1, 2, 3...`).
* **Compatibility Rules Engine ([`compatibility.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/compatibility.go#L13))**:
  * `BACKWARD` (Default): New schema can read data written with old schema. Added fields must have default values; deleted fields are permitted.
  * `FORWARD`: Old schema can read data written with new schema. Deleted fields must have had default values in the old schema; added fields are permitted.
  * `FULL`: Enforces both `BACKWARD` and `FORWARD` compatibility simultaneously.
  * `NONE`: Compatibility validation disabled.
  * **Avro Type Promotion**: Supports automatic numeric promotions: `int` $\rightarrow$ `long` $\rightarrow$ `float` $\rightarrow$ `double`, and `string` $\leftrightarrow$ `bytes`.

### 2.5 In-Broker Stream Transforms Engine

The stream transform engine ([`transform.Engine`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L52)) executes real-time data transformations directly on record streams between a `source_topic` and a `target_topic`:

```
Incoming Record ──► [ Transform Engine ] ──┬──► (Match/Pass) ──► Target Topic
                                           └──► (Drop/Mask)  ──► Dropped / Sanitized
```

* **Transform Types**:
  1. [`TypeMaskPII`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L20): Recursively traverses JSON objects and masks sensitive keys (e.g. `credit_card`, `ssn`, `password`, `email`, `cvv`, `auth_token`) with configurable patterns (e.g. `***`).
  2. [`TypeFilter`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L19): Evaluates boolean and numeric expressions over JSON payloads (`==`, `!=`, `>`, `<`, `>=`, `<=`, `contains`). Records failing the condition are discarded without allocation.
  3. [`TypeJSONMap`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L21): Real-time field manipulation supporting `add_fields`, `rename_fields`, `remove_fields`, and `set_*` operators, stamping `_transformed_at` and `_engine: aerostream-inline`.
  4. [`TypeWASM`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L18): Runs WebAssembly WASI binaries sandboxed in the broker pipeline, measuring gas usage and execution microseconds (`wazero` compatible runtime).

### 2.6 Role-Based Access Control & Granular ACLs

Enterprise security is enforced by [`auth.AclManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/auth/acls.go#L66):

* **Standard Roles**: `SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`.
* **Zero-Trust Default Deny**: Every resource request is rejected unless an explicit `ALLOW` rule or `SUPER_ADMIN` identity is established.
* **Explicit Precedence**:
  1. **Explicit DENY**: An explicit `DENY` rule overrides all other rules, including `SUPER_ADMIN`.
  2. **SUPER_ADMIN**: Grants unrestricted cluster, topic, and consumer group privileges.
  3. **Explicit ALLOW**: Granted if rule criteria match.
  4. **Zero-Trust Fallback**: Rejected.
* **Wildcard & Pattern Matching**:
  * Principal match: Exact (`User:alice`), wildcard (`User:*` or `*`), or glob (`service-*`).
  * Resource match: Exact (`orders`), prefix (`orders-*`), or universal wildcard (`*`).
  * Operation match: `READ`, `WRITE`, `DESCRIBE`, `ALTER`, or `ALL`.

### 2.7 Connectors Ecosystem & Worker Task Model

AeroStream provides a Confluent Kafka Connect REST-compatible connector subsystem ([`connect.ConnectorManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/connect/manager.go#L53)):

* **REST API Endpoints**: `/api/connectors`, `/connectors`, `/connector-plugins`, `/api/connectors/{name}/pause`, `/api/connectors/{name}/resume`.
* **Built-in Connector Plugins**:
  * `S3ArchivalSinkConnector`: Archives real-time partition records into AWS S3/MinIO bucket hierarchies.
  * `HttpWebhookSinkConnector`: Dispatches records to external HTTP REST endpoints with configurable retry policies.
  * `DatabaseCdcSourceConnector`: Ingests simulated Change Data Capture (CDC) events from relational databases.
  * `ElasticSearchSinkConnector`: Streams structured JSON records to OpenSearch/Elasticsearch indexes.

### 2.8 Controller-Broker Control Plane Orchestration & Lifecycle

The orchestration between the Go Control Plane and the Rust Data Plane is driven by high-performance gRPC over HTTP/2, defined in [`proto/control.proto`](file:///home/uttam/projects/AeroMQ/proto/control.proto):

![AeroStream Controller-Broker Orchestration](images/controller_broker_orchestration.png)

The orchestration lifecycle operates across four tightly-coupled control channels:
* **Registration Channel**: Invoked once at storage daemon bootstrap to publish endpoint addresses, dual-protocol ports (`9091` Native, `9092` Kafka), hardware topology, and rack awareness.
* **Telemetry & Heartbeat Channel**: Periodic 2-second bidirectional keepalive exchanging storage metrics, Log End Offsets (LEO) for all local partitions, and receiving leader/follower assignments.
* **State Synchronization Channel**: Piggybacks client bandwidth quotas, dynamic topic configurations, and compression codecs (`zstd`, `lz4`, `snappy`) directly in heartbeat replies.
* **Failure Detection & Reassignment Channel**: A 3-second evaluation ticker that evicts unresponsive brokers after an 8-second timeout, immediately reassigning partition leadership to remaining in-sync replicas (ISR).


1. **Broker Registration**:
   Upon startup, each Rust broker calls [`ControlService.RegisterBroker`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/grpcserver/server.go#L62) carrying `broker_id`, `host`, `data_port` (`9091`), `kafka_port` (`9092`), and `rack` identifier. The controller commits a `CmdRegisterBroker` entry into the Raft log, recording the broker in [`ClusterState.Brokers`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L71).

2. **Bidirectional 2-Second Heartbeat & LEO Reporting**:
   Every 2 seconds, each broker invokes [`ControlService.Heartbeat`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/grpcserver/server.go#L88):
   * **Payload**: Carries `disk_usage_bytes`, `disk_free_bytes`, and `repeated ReplicaOffset replica_offsets` ($[\text{topic}, \text{partition}, \text{offset}]$). Each offset represents the local broker's Log End Offset (LEO) for partitions it replicates.
   * **Piggybacked Dynamic Configuration Push**: The controller's `HeartbeatResponse` carries cluster partition assignments (`assigned_leaders`, `assigned_followers`), and piggybacks live dynamic configurations without requiring separate polling loops:
     * `client_quotas`: Dynamic producer byte rates, consumer byte rates, and request percentage quotas.
     * `topic_compression`: Live per-topic forced compression codecs (`zstd`, `lz4`, `snappy`, `gzip`, `uncompressed`).

3. **In-Sync Replicas (ISR) & High-Watermark Dynamics**:
   The controller evaluates replica health via [`updateISRAndHW`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L404):
   * A replica is retained in the ISR if $(\text{leader\_LEO} - \text{follower\_LEO}) \le \text{replicaLagTolerance}$ (default 10 offsets).
   * If a follower lags beyond this tolerance, the controller evicts it from the ISR and broadcasts updated metadata.
   * The **High-Watermark (HW)** is computed as the minimum offset across all in-sync replicas:
     $$\text{HighWatermark} = \min_{r \in \text{ISR}} (\text{LEO}_r)$$
   * Consumers can only read up to the High-Watermark, guaranteeing that committed messages survive broker failovers.

4. **Failure Detection & Automatic Partition Failover (8s Timeout)**:
   [`Server.startFailureDetection`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/grpcserver/server.go#L37) sweeps registered brokers every 3 seconds. If any broker fails to heartbeat within `broker_inactive_timeout_ms` (default 8,000 ms), the controller immediately:
   * Proposes `CmdCleanInactive` through Raft consensus.
   * Marks the unresponsive broker `Active = false`.
   * Reassigns leadership of all orphaned partitions to the most caught-up replica currently in the ISR in under 100ms.

5. **Graceful Broker Draining Workflow**:
   Before performing maintenance or decommissioning a node, operators issue `POST /api/brokers/{id}/drain`. The controller proposes [`CmdDrainBroker`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L183):
   * Reassigns all partition leadership away from the draining broker to surviving ISR members.
   * Substitutes the draining broker in `ReplicaIDs` with healthy active brokers.
   * Cleanses the broker from all ISRs, enabling safe node retirement without client-facing errors or partition unavailability.

6. **Native Command 3 Follower Replication**:
   Follower storage brokers replicate partition data from leaders using native TCP Command 3 (Replica Fetch). The leader reads data past the High-Watermark (up to its active LEO), updates the follower's replica offset via [`update_follower_offset`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L540), and streams records via zero-copy `sendfile(2)`. As follower offsets advance, the leader recalculates its local High-Watermark and notifies waiting consumer fetch loops.

---

## 3. Data Plane Deep-Dive (`rust-broker/`)

The AeroStream Data Plane runs as the native storage daemon (`rust-broker`). It is engineered in pure Rust for non-blocking asynchronous I/O, physical log layout optimization, and kernel-level zero-copy data pipelines.

![AeroStream Shard-per-Core Architecture](images/shard_per_core_architecture.png)

The shard architecture orchestrates three fundamental layers:
* **Ingress & Dispatch Layer**: Dual Tokio listeners on port `9091` (native binary) and port `9092` (Kafka protocol) parse message batch envelopes and hash `(topic, partition)` through [`ShardRouter`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/router.rs#L5).
* **Shard Execution Layer**: Dedicated OS threads (`shard-0` through `shard-N`), each pinned to a dedicated hardware CPU core via `libc::sched_setaffinity`. Each thread possesses absolute, lock-free ownership of its assigned partition log instances.
* **Storage & Tiered Layer**: NVMe append-only commit logs, in-place base offset patching, dirty memory writeback pacing, and background offloading via asynchronous task queues to cloud object stores.


### 3.1 Shard-per-Core Architecture & Thread-per-Core Engine

Traditional multi-threaded streaming brokers protect partition commit logs with coarse read-write locks or mutexes across thread pools. Under high core counts, this model suffers from CPU cache-line bouncing, memory bus contention, and lock serialization stalls.

AeroStream eliminates these bottlenecks by implementing a **Shard-per-Core (Thread-per-Core)** execution model ([`rust-broker/src/shard/`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/)):

1. **Hardware Core Pinning (`libc::sched_setaffinity`)**:
   In [`spawn_shards`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/engine.rs#L305), the broker detects available CPU cores (`std::thread::available_parallelism()`) and spawns dedicated OS threads named `shard-0`, `shard-1`, ..., `shard-N`. Each thread is explicitly pinned to its designated core:
   ```rust
   let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
   libc::CPU_SET(shard_id % num_cpus, &mut cpuset);
   libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
   ```
   This isolates shard execution to specific CPU execution units, guaranteeing maximum L1/L2 data and instruction cache locality while preventing Linux thread scheduler migration penalties.

2. **Deterministic Partition Routing (`ShardRouter`)**:
   Network ingress handlers in Tokio route incoming requests using the deterministic [`ShardRouter`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/router.rs#L5):
   ```rust
   pub fn shard_for(&self, topic: &str, partition: u32) -> usize {
       let mut hasher = DefaultHasher::new();
       topic.hash(&mut hasher);
       partition.hash(&mut hasher);
       (hasher.finish() as usize) % self.num_shards
   }
   ```
   All requests for a given `(topic, partition)` pair are guaranteed to map to the exact same shard thread throughout cluster execution.

3. **Lock-Free Actor Model via Flume Channels**:
   Communication between Tokio network ingress tasks and shard threads uses cross-thread lock-free MPMC channels provided by `flume::unbounded()`:
   * **Requests**: Wrapped in the [`ShardRequest`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/engine.rs#L27) enum (`Append`, `AppendBatchSlice`, `ReadFromOffset`, `GetNextOffset`, `GetAllOffsets`, `EnsurePartition`, `DeleteTopic`, `Shutdown`).
   * **Responses**: Each request contains a `tokio::sync::oneshot::Sender<Result<T, io::Error>>`. The shard executes the request and transmits the result back without blocking the network worker.

4. **Zero-Contention `PartitionLog` Design**:
   Each `ShardEngine` maintains its own `HashMap<PartitionKey, PartitionLog>`. Because all operations on a partition execute on the partition's dedicated shard thread, partition mutation proceeds sequentially without mutex locks, atomic spinlocks, or cross-core synchronization.

---

### 3.2 Memory & Log I/O Optimizations

High-throughput streaming brokers frequently saturate the operating system page cache or spend excessive CPU time copying data in memory. AeroStream implements targeted kernel-level optimizations to maintain flat latency profiles under sustained multi-gigabyte workloads:

1. **In-Place Base Offset Patching Directly on Disk**:
   In the Kafka wire protocol, producers submit `RecordBatch` payloads with an unassigned or tentative base offset in the first 8 bytes. Traditional implementations clone the entire record batch on the heap to modify the base offset before writing to disk. For multi-megabyte produce frames, this causes severe memory allocation overhead and page faults.
   
   AeroStream implements **in-place base offset patching** ([`append_batch_slice`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L367)):
   ```rust
   let pos = self.active_len;
   let off_bytes = base_offset.to_be_bytes();
   self.active_log_file.write_all_at(&off_bytes, pos)?;
   if batch.len() > 8 {
       self.active_log_file.write_all_at(&batch[8..], pos + 8)?;
   }
   ```
   The 8-byte base offset is written directly to the target file offset using Linux `pwrite(2)` (`FileExt::write_all_at`), followed by the remaining batch slice. Zero heap memory is cloned, and zero memory allocations occur on the produce fast path.

2. **Paced Page-Cache Writeback (`libc::sync_file_range`)**:
   Under heavy write workloads, a storage broker can dirty Linux page cache pages faster than physical storage hardware can commit them to disk. In memory-constrained container environments (e.g. Docker/Kubernetes cgroups with 2 GiB memory limits), the cgroup dirty page limit triggers synchronous kernel writeback on the producer write path, causing severe latency spikes of 500ms to 5,000ms.
   
   AeroStream resolves this through **paced page-cache writeback** ([`PartitionLog::writeback_bytes`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L476)):
   ```rust
   // Periodically initiate asynchronous writeback for written ranges (default 8 MiB)
   libc::sync_file_range(fd, start as libc::off64_t, len as libc::off64_t, libc::SYNC_FILE_RANGE_WRITE);
   ```
   * `SYNC_FILE_RANGE_WRITE` initiates non-blocking background disk flushes without waiting for I/O completion.
   * Dirty pages are continuously fed to storage hardware at steady rates, preventing dirty page buildup and completely eliminating kernel write stalls.
   * Benchmark sweep results: 100 KB writes improved from **164 MB/s to 1,011 MB/s**, and worst-case latency dropped from **seconds down to ~10 milliseconds**.

3. **Page-Cache Drop for Constrained Runtimes (`posix_fadvise`)**:
   For ultra-low memory footprints, `storage.drop_cache_after_writeback = true` instructs the broker to wait for completed ranges and issue:
   ```rust
   libc::posix_fadvise(fd, offset, len, libc::POSIX_FADV_DONTNEED);
   ```
   This releases committed pages from kernel cache immediately, capping resident memory usage under strict container limits.

4. **Socket Buffer Tuning & Buffer Reuse**:
   Network connections configure 4 MiB socket buffers (`SO_RCVBUF`, `SO_SNDBUF`) and enable `TCP_QUICKACK`. Additionally, connection-level frame buffers are preserved across frame cycles via `frame_buf.resize(frame_len, 0)` rather than re-allocating new vectors, keeping physical memory pages resident and avoiding kernel page-fault penalties.

---

### 3.3 Kafka Fetch Long-Polling Engine

To maximize consumer throughput and eliminate busy-polling CPU waste, AeroStream features an asynchronous long-polling fetch engine ([`handle_fetch_with_topo`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs#L686)):

1. **`max_wait_ms` / `min_bytes` Purgatory**:
   When consumers issue Fetch requests specifying `min_bytes` and `max_wait_ms`, AeroStream evaluates the total bytes currently available across all requested partitions. If the available data is below `min_bytes` and the deadline has not expired, the request enters the long-polling wait state instead of returning an empty response.

2. **Lazy Per-Partition `tokio::sync::Notify` Registration**:
   Eagerly registering notification handles on every Fetch request introduces locking overhead. AeroStream uses **lazy registration**:
   * The first pass attempts to satisfy the fetch immediately from cache and disk. Under steady workload, the vast majority of fetches are satisfied on the first pass.
   * Only if the returned data is below `min_bytes` does the broker extract `append_notify` (`Arc<tokio::sync::Notify>`) from each requested partition.
   * The worker suspends via `futures_util::future::select_all(waits)` with a timeout bounded by `deadline - now`.
   * As soon as an append occurs on *any* of the requested partitions, `append_notify.notify_waiters()` immediately awakens the fetch loop to package the response.

3. **Out-of-Lock Disk I/O Reads**:
   In [`handle_fetch_with_topo`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs#L821), partition lock acquisition is strictly decoupled from physical disk I/O:
   ```rust
   // Partition lock covers ONLY in-memory state and the index lookup:
   let (hw, lso, aborted_txns, read_spec) = {
       let mut guard = part_log.lock().await;
       // ... compute upper_bound, HW, and query index ...
       (hw, lso, view.aborted, guard.read_range(fetch_offset, upper_bound, max_read))
   };
   // Physical disk read occurs AFTER releasing the partition lock:
   if let Some((file, position, len, _)) = read_spec {
       file.read_exact_at(&mut raw, position)?;
   }
   ```
   Holding the partition lock only long enough to look up the physical file position and length ensures that concurrent disk I/O from slow drives or heavy consumers never blocks incoming high-speed producer appends.

---

### 3.4 Cold Storage Archival Pipeline & Fast Hard-Linking

When active commit log segments reach `storage.max_segment_size` (default 128 MiB), the segment is sealed and transitioned into cold storage:

1. **Fast-Path Asynchronous Hard-Linking (`fs::hard_link`)**:
   In [`archive_sealed_segment`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L519), AeroStream avoids copying hundreds of megabytes across disk folders:
   * The sealed `.log` and `.idx` files are linked into `cold_storage/{topic}/partition_{p}/` via [`fs::hard_link`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L535).
   * Creating a hard link updates directory metadata and increments the file's inode link count in sub-millisecond time, requiring **zero physical data duplication**.
   * Directory operations and link syscalls run on a dedicated background OS thread (`std::thread::spawn`), preventing disk filesystem stalls from delaying the segment rollover.

2. **POSIX Unlink-After-Open Durability Semantics**:
   The source files are opened synchronously before spawning the archival thread. Under POSIX semantics, an open file descriptor keeps the underlying filesystem blocks intact and readable even if partition retention subsequently unlinks the active segment path. If cross-filesystem boundaries prevent hard-linking, the thread seamlessly falls back to streaming copy from the open file descriptors.

3. **Multi-Cloud Tiered Offloader Integration**:
   Once archived locally, an [`OffloadTask`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/offloader.rs#L11) is queued to the tiered storage worker via `tokio::sync::mpsc`. The offloader uploads segment chunks asynchronously to configured object storage providers (AWS S3, Google Cloud Storage, Azure Blob Storage, or MinIO).

---

### 3.5 Append-Only Log Storage Kernel ([`PartitionLog`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L19))

Physical partition state is maintained inside structured filesystem directories:
`<storage_dir>/<topic>/partition_<id>/`

1. **Segment Rolling Mechanism ([`roll_over`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L571))**:
   When active segment size exceeds `max_segment_size`:
   * The current active segment files are flushed and closed.
   * `sync_file_range` flushes the unflushed tail of the sealed segment.
   * `archive_sealed_segment` archives the sealed segment into cold storage.
   * A new active segment is initialized with base offset equal to `self.next_offset`.
2. **Segment File Naming**:
   Segments are named using 20-digit zero-padded base offsets:
   * Data log: `00000000000000000000.log`
   * Index file: `00000000000000000000.idx`

---

### 3.6 Physical Binary Layout & Index Architecture

```
Physical Segment Layout:

.log File (Append-Only Sequential Binary Data):
+───────────────────────────+───────────────────────────+───────────────────────────+
| Record at Offset 0        | Record at Offset 1        | Record at Offset 2        |
| [Length][Payload Bytes]   | [Length][Payload Bytes]   | [Length][Payload Bytes]   |
+───────────────────────────+───────────────────────────+───────────────────────────+
0                           P1                          P2                          P3 (bytes)

.idx File (Fixed 16-Byte Index Entries):
+───────────────────────────+───────────────────────────+───────────────────────────+
| Entry 0                   | Entry 1                   | Entry 2                   |
| [Offset 0: u64 BE]        | [Offset 1: u64 BE]        | [Offset 2: u64 BE]        |
| [Pos 0:    u64 BE = 0]    | [Pos 1:    u64 BE = P1]   | [Pos 2:    u64 BE = P2]   |
+───────────────────────────+───────────────────────────+───────────────────────────+
0                           16                          32                          48 (bytes)
```

1. **Fixed-Size 16-Byte Index Entries**:
   Every written record appends exactly 16 bytes to the `.idx` file:
   * `[0..8]`: Logical 64-bit Big-Endian Offset (`u64`).
   * `[8..16]`: Physical 64-bit Big-Endian Byte Position (`u64`) within the `.log` file.
2. **Sub-Microsecond Binary Search**:
   Because every index entry is exactly 16 bytes, locating any offset $O$ requires **zero file parsing**. AeroStream executes binary search directly over the `.idx` file:
   $$\text{Entry Index} = \frac{\text{File Length}}{16}$$
   $$\text{Byte Offset in Index} = \text{mid} \times 16$$
   Once the entry is located, reading `pos` immediately yields the file seek location for the data payload.

---

### 3.7 Zero-Copy Engine & Linux Page Cache Interaction

The Fetch path in [`Conn::send_file_region`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/server.rs#L125) provides true kernel-level zero-copy data transfer:

```
Standard Read/Write (2 Context Switches, 2 Memory Copies):
Disk ──► Page Cache ──[Kernel-to-User Copy]──► App Buffer ──[User-to-Kernel Copy]──► Socket Buffer ──► NIC

AeroStream Plaintext Fetch via sendfile(2) (Zero Userspace Copies):
Disk ──► Page Cache ──────────────────[DMA Direct Transfer]────────────────────────► Socket / NIC
```

1. **`sendfile(2)` Fast Path**:
   When client connections are plaintext, AeroStream switches the socket to blocking mode and invokes:
   ```rust
   libc::sendfile(socket_fd, file_fd, &mut offset, remaining);
   ```
   Data pages move directly from the Linux Page Cache to the network interface controller (NIC) via Direct Memory Access (DMA), completely bypassing userspace buffers.
2. **TLS Fallback**:
   When TLS is enabled on the data plane, encryption requires userspace modification. AeroStream gracefully falls back to reading the segment range into a pinned buffer and streaming through `tokio-rustls`.

---

### 3.8 Kafka Wire Protocol Compatibility Engine

![AeroStream Dual-Protocol Engine: Native vs. Kafka Wire Protocol](images/native_and_kafka_dual_protocol.png)

AeroStream implements a high-performance native parser for the Apache Kafka wire protocol in [`kafka_server.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs#L12), [`handlers.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/handlers.rs), and [`admin.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/admin.rs):

* **4-Byte Frame Delimiter**: Incoming frames are framed with a 32-bit big-endian length prefix.
* **34+ Supported Kafka API Keys**:
  * **Core Ingress & Discovery**: `Produce` (Key 0), `Fetch` (Key 1, KIP-392 rack awareness), `ListOffsets` (Key 2), `Metadata` (Key 3), `ApiVersions` (Key 18).
  * **Consumer Group Coordination**: `OffsetCommit` (Key 8), `OffsetFetch` (Key 9), `FindCoordinator` (Key 10), `JoinGroup` (Key 11), `Heartbeat` (Key 12), `LeaveGroup` (Key 13), `SyncGroup` (Key 14), `DescribeGroups` (Key 15), `ListGroups` (Key 16), `DeleteGroups` (Key 42).
  * **SASL Authentication**: `SaslHandshake` (Key 17), `SaslAuthenticate` (Key 36) supporting both `PLAIN` and `SCRAM-SHA-256`.
  * **Transactions & EOS**: `InitProducerId` (Key 22), `AddPartitionsToTxn` (Key 24), `AddOffsetsToTxn` (Key 25), `EndTxn` (Key 26), `TxnOffsetCommit` (Key 28).
  * **Topic & Partition Administration**: `CreateTopics` (Key 19), `DeleteTopics` (Key 20), `CreatePartitions` (Key 37), `DescribeConfigs` (Key 32), `AlterConfigs` (Key 33), `IncrementalAlterConfigs` (Key 44), `ElectLeaders` (Key 43), `DescribeCluster` (Key 60).
  * **Share Groups (KIP-932)**: `ShareGroupHeartbeat` (Key 76), `ShareGroupDescribe` (Key 77), `ShareFetch` (Key 78), `ShareAcknowledge` (Key 79).

---

### 3.9 Exactly-Once Semantics & Idempotent Producer State Tracker

To prevent duplicate records from network retries and producer re-sends, AeroStream implements [`ProducerStateTracker`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/producer_state.rs#L64):

1. **State Tracking**: Tracks `(producer_id, epoch)` mapping to `last_sequence`, `last_offset`, and `last_timestamp`.
2. **Sequence Check Algorithm ([`check_and_update_sequence`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/producer_state.rs#L88))**:
   * **`ValidNext`**: If `base_sequence == last_sequence + 1` (or `base_sequence == 0` for a new PID), state is updated and the record is appended.
   * **`Duplicate`**: If `base_sequence <= last_sequence`, the record is identified as a duplicate retry. The log write is **bypassed entirely**, and AeroStream immediately returns the cached `last_offset` ACK to the producer.
   * **`OutOfOrder`**: If `base_sequence > last_sequence + 1`, a sequence gap has occurred. AeroStream rejects the request with Kafka error code `45` (`OutOfOrderSequenceNumber`), preserving strict ordering.

---

### 3.10 Background Log Compactor & Cleaner Thread

For compaction-enabled topics, AeroStream runs an autonomous background cleaner loop ([`spawn_cleaner_loop`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L734)):

1. **Active Head Segment Protection**: The active head segment receiving live writes is **never compacted**. Compaction runs exclusively on closed segments.
2. **Key-to-Offset Hash Index ([`build_key_offset_map`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/compactor.rs#L174))**:
   Scans closed segments to construct an in-memory index mapping each record key to its latest offset:
   $$\text{Map}: \text{Key} \longrightarrow (\text{latest\_offset}, \text{is\_tombstone})$$
3. **Dirty Ratio Threshold**:
   Computes the ratio of redundant/superseded bytes to total bytes across closed segments:
   $$\text{DirtyRatio} = \frac{\text{RedundantBytes} + \text{ExpiredTombstoneBytes}}{\text{TotalClosedBytes}}$$
   Compaction triggers only when `DirtyRatio >= dirty_ratio_threshold` (default `0.5`).
4. **Atomic Clean Swap**:
   Surviving records are written to temporary files: `<offset>.clean.log` and `<offset>.clean.idx`. Once complete, the clean files are atomically renamed over the original segment files via `fs::rename`.
5. **Tombstone Garbage Collection**:
   Tombstone records (records with empty/null payload indicating key deletion) are preserved for `tombstone_retention` (default 24 hours) to allow consumers to observe deletions, after which they are evicted.

---

### 3.11 Multi-Cloud Tiered Storage Pipeline

To decouple storage costs from local disk capacity, AeroStream integrates an asynchronous tiered storage architecture ([`storage/`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/)):

![AeroStream Multi-Cloud Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

The tiered offload pipeline operates continuously in the background, entirely decoupled from latency-sensitive client write operations:
* **Segment Sealing**: Active head segments roll over based on size (`max_segment_size = 1 GiB`) or time (`segment_ms = 604800000`). Once sealed, segments become strictly immutable.
* **Hard-Link Staging**: The broker creates a hard link in `cold_storage/` within microseconds without copying data bytes, ensuring that compaction or retention pruning does not prematurely unlink the active file while offloading is pending.
* **Asynchronous Queue Dispatch**: An `OffloadTask` tuple `(Topic, Partition, BaseOffset, LogPath, IdxPath)` is enqueued onto an unbounded lock-free channel.
* **Parallel Cloud Upload**: Worker tasks consume the queue and invoke parallel multipart or streaming PUT requests against the target object storage bucket.


1. **The Provider Trait ([`TieredStorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/provider.rs#L61))**:
   Exposes standard async operations: `put_segment`, `get_segment`, `delete_segment`, `exists`, and `list_segments`.
2. **Offload Task Pipeline ([`TieredStorageOffloader`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/offloader.rs#L57))**:
   When a segment rolls over, an `OffloadTask` is sent through an asynchronous queue. The worker reads the `.log` and `.idx` files from disk and uploads them using standard object keys:
   `tiered/<topic>/partition_<id>/<base_offset:020>.log`
   `tiered/<topic>/partition_<id>/<base_offset:020>.idx`
3. **Supported Storage Backends ([`StorageProviderFactory`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/factory.rs))**:
   * **AWS S3 / MinIO** ([`S3StorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/s3.rs)): Configurable custom endpoints, path-style addressing, and AWS regions.
   * **Google Cloud Storage** ([`GcsStorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/gcs.rs)): Native GCS JSON API authentication and chunked uploads.
   * **Azure Blob Storage** ([`AzureBlobStorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/azure.rs)): SharedKey and bearer token authentication.
   * **Local / NFS** ([`LocalStorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/local.rs)): High-speed network filesystem mounts.
4. **Transparent Cold Segment Retrieval**:
   When a consumer requests an offset that has been pruned locally, [`find_cold_segment`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L471) binary-searches the cold storage directory, allowing reads of historical segments without manual operator intervention.

### 3.12 KIP-932 Share Groups & Cooperative Queue Semantics

Standard partition-based consumer groups enforce a strict 1:1 mapping between a topic partition and an active consumer instance. When partition counts are lower than consumer worker scale, or when individual message processing latencies exhibit high variance, partition head-of-line blocking degrades overall system throughput. AeroStream implements **Share Groups (KIP-932)** to provide cooperative, queue-like record delivery directly over partitioned append-only logs:

1. **Share-Partition Coordinator**:
   Within the storage daemon, each partition assigned to a Share Group is wrapped by an in-memory `SharePartitionState` actor. The coordinator decouples offset progression from single-consumer ownership:
   * **`Available`**: Records residing between the share-partition start offset and the High Watermark ($HW$) that have not yet been leased.
   * **`Acquired`**: Records leased to a consumer instance for a configurable `acquisition_timeout_ms` (default: 30,000 ms). An active acquisition lock prevents duplicate delivery to concurrent consumers.
   * **`Acknowledged`**: Records successfully processed and acknowledged via `ShareAcknowledge`. Once contiguous offset sequences reach terminal acknowledgment, the share group base offset advances.
   * **`Archived`**: Records whose delivery attempt counter exceeds `max_delivery_attempts` (default: 5) are marked archived and routed to an internal dead-letter queue (DLQ) topic, preventing poisonous records from blocking queue progression.
2. **ShareFetch & ShareAcknowledge Wire Protocol**:
   * On port `9092` (Kafka protocol) and port `9091` (Native protocol), clients issue batch `ShareFetch` requests. The broker atomically leases an offset interval $[O_{\text{start}}, O_{\text{end}}]$ to the requesting client connection.
   * As processing completes, the consumer issues pipelined `ShareAcknowledge` packets specifying acknowledgment types (`ACK = 1`, `REJECT = 2`, `RELEASE = 3`).
   * If a consumer crashes or fails to heartbeat before `acquisition_timeout_ms` expires, the acquisition lock times out, automatically transitioning the records back to `Available` for redelivery to another worker.

### 3.13 Streaming Apache Iceberg Lakehouse Offloading & Parquet Vectorization

To bridge operational event streaming with modern analytical lakehouses (Trino, DuckDB, Apache Spark, Snowflake, Databricks), AeroStream provides native, continuous log-to-columnar offloading into **Apache Iceberg Table Format v2**:

1. **In-Memory Columnar Transcoding**:
   As active log segments are sealed and hard-linked, the background offloader streams raw binary records into Apache Arrow columnar record batches using the vectorized Rust `arrow` and `parquet` engines:
   * Schema mappings are resolved dynamically against the Control Plane Schema Registry ([`Registry`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/registry.go#L105)), translating Avro or JSON schemas into strict Arrow datatypes.
   * Batches are encoded as columnar Parquet files compressed with Zstandard (`zstd`) level 3 or Snappy, generating column-level dictionary encoding, bloom filters, and min/max statistics.
2. **Iceberg Table Spec v2 Commits**:
   * The offloader generates Iceberg data file manifests (`manifest-list.avro`) recording byte lengths, partition tuples, record counts, and lower/upper column bounds.
   * Fast metadata commits are posted atomically to the designated Iceberg catalog (REST Catalog, AWS Glue, or Project Nessie).
   * Downstream query engines can query the streaming topic data directly in object storage with millisecond partition pruning, eliminating external ETL connectors and micro-batch pipelines.

### 3.14 Two-Phase Commit (2PC) Distributed Transaction Coordinator

AeroStream guarantees end-to-end **Exactly-Once Semantics (EOS)** across multi-partition and cross-topic message workflows via an embedded Two-Phase Commit (2PC) transaction coordinator:

1. **Transaction Lifecycle State Machine**:
   Transactional producers register via `InitProducerId`, receiving an assigned Producer ID ($PID$) and incremented Producer Epoch. Transactions progress through deterministic state transitions:
   $$\text{Empty} \longrightarrow \text{Ongoing} \longrightarrow \text{PrepareCommit / PrepareAbort} \longrightarrow \text{CompleteCommit / CompleteAbort}$$
2. **Write-Ahead Log Markers (`TxnMarkerBatch`)**:
   When a producer invokes `EndTxn(commit = true/false)`, the transaction coordinator writes a transactional control batch (Record Type `0x02`) directly to the commit logs of all participating topic partitions:
   * **Commit Marker**: Authorizes consumers to observe all preceding records in the transaction.
   * **Abort Marker**: Instructs consumers to discard all preceding records associated with the aborted $PID$.
3. **Log Stable Offset ($LSO$) & Read Isolation**:
   The storage kernel maintains two distinct watermarks per partition:
   * **High Watermark ($HW$)**: The highest offset replicated across all In-Sync Replicas (ISR).
   * **Log Stable Offset ($LSO$)**: The offset of the earliest ongoing (uncommitted) transaction.
   * Consumers configured with `isolation_level = read_committed` only receive messages up to $LSO$. Any aborted transaction records prior to $LSO$ are stripped from the fetch stream in-memory by [`handle_fetch`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/handlers.rs#L1353), guaranteeing strict transactional isolation without performance overhead on non-transactional reads.

---

## 4. Architectural Workflows & Operational Execution Pipelines

![AeroStream Zero-Copy Produce & Fetch Pipeline](images/produce_fetch_pipeline.png)


### 4.1 End-to-End Produce Flow

The end-to-end produce lifecycle depicted in the diagram above proceeds through four deterministic stages:

1. **Ingress & Sequence Validation**: The client sends a `ProduceRequest` over port 9092 (Kafka protocol) or port 9091 (Native protocol). The broker worker extracts the Producer ID (PID), Producer Epoch, and Sequence Number, passing them to [`ProducerStateTracker`](file:///home/uttam/projects/AeroMQ/rust-broker/src/txn/tracker.rs).
2. **Duplicate & Out-of-Order Detection**: If the sequence number is a duplicate retry, the cached last offset is immediately acknowledged without disk I/O. If out of order, status code 45 (`OutOfOrderSequenceNumber`) is returned.
3. **Paced Writeback & Indexing**: For sequential records, the partition engine appends the payload to the active `.log` segment in Linux page cache, immediately writes the 16-byte sparse offset/position index entry into `.idx`, and periodically invokes `sync_file_range` to smooth disk I/O pressure.
4. **Offset Assignment & Telemetry**: The log end offset ($LEO$) and High Watermark ($HW$) are advanced, and the committed base offset is returned to the client in the `ProduceResponse`. The broker's periodic 2-second heartbeat asynchronously transmits the updated $LEO$ telemetry to the Go Raft Controller.

### 4.2 End-to-End Zero-Copy Fetch Flow

The consumer read path minimizes CPU memory copies by leveraging direct kernel DMA:

1. **Request Ingestion & High Watermark Gate**: The consumer issues a `FetchRequest` specifying topic, partition, starting offset, and maximum byte budget. The broker verifies that $\text{StartOffset} < HW$; requests beyond the High Watermark return 0 records without blocking.
2. **Binary Index Lookup**: The partition engine performs a fast binary search on the memory-mapped `.idx` file to pinpoint the exact segment and physical byte offset corresponding to the requested log offset.
3. **Zero-Copy DMA Transfer**: The broker writes the protocol response header into the socket buffer and invokes `libc::sendfile(socket_fd, file_fd, offset, bytes)`. The Linux kernel transfers data directly from page cache into the network interface controller (NIC) ring buffer via DMA, bypassing userspace entirely.

### 4.3 Control Plane Raft Quorum & Broker Failover Flow

![AeroStream Controller-Broker Orchestration](images/controller_broker_orchestration.png)

![AeroStream Cluster Topology & Zero-Downtime Scale-Down](images/cluster_topology_scale_down.png)

Leader election, membership consensus, and automated broker failover are orchestrated through the following sequence:

1. **Heartbeat Monitoring**: The Go Raft Controller tracks periodic 2-second heartbeats from all active brokers.
2. **Lease Expiry Detection**: If a broker fails to heartbeat within the 8-second lease window, the failure detector triggers `CmdCleanInactive`.
3. **Raft Quorum Commit**: The Raft leader proposes the membership update to all follower controller nodes. Once a quorum majority acknowledges via `AppendEntries`, the state machine commits the transition.
4. **ISR Recalculation & Failover**: The controller removes the dead broker from the In-Sync Replicas (ISR) set, promotes an eligible surviving in-sync replica as the new partition leader, and pushes the updated cluster topology to the remaining brokers on their next heartbeat poll.

### 4.4 Tiered Storage Segment Rollover & Offload Pipeline

![AeroStream Multi-Cloud Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

1. **Active Segment Rollover**: When the active log segment reaches `max_segment_size` (default: 1 GB) or the roll timeout elapses, the broker seals the current `.log` and `.idx` files, copies them to the cold storage stage, and creates a fresh active segment starting at the next offset.
2. **Asynchronous Offload Queue**: An `OffloadTask` containing segment coordinates is submitted to a lock-free background channel, decoupled from the hot produce path.
3. **Multi-Cloud Upload**: The background offloader streams `.log` and `.idx` objects directly to AWS S3, MinIO, Google Cloud Storage, or Azure Blob Storage.
4. **Transparent Tiered Read**: When historical consumers request offsets that have been rolled to object storage, the broker fetches the cold chunk transparently, caches it locally in the LRU read cache, and streams records back to the consumer.

---

## 5. UI, Client & Cloud-Native Deployment Topology

### 5.1 Angular Web Console UI Architecture

The AeroStream Web Console resides in [`ui/src/app/`](file:///home/uttam/projects/AeroMQ/ui/src/app/). It is an enterprise Single Page Application built with **Angular 21**, TypeScript, Tailwind CSS, and SCSS:

* **Real-Time Reactive Architecture**: Consumes the Go Controller REST API via Angular injectable services:
  * [`AeromqService`](file:///home/uttam/projects/AeroMQ/ui/src/app/services/aeromq.service.ts): Cluster topology visualizer, live broker heartbeats, partition distribution, topic metrics.
  * [`SchemaService`](file:///home/uttam/projects/AeroMQ/ui/src/app/services/schema.service.ts): Subject browser, schema diff viewer, compatibility level configurator.
  * [`AclService`](file:///home/uttam/projects/AeroMQ/ui/src/app/services/acl.service.ts): User role management, ACL rule builder, policy simulator.
  * [`TransformService`](file:///home/uttam/projects/AeroMQ/ui/src/app/services/transform.service.ts): Real-time stream transform orchestrator, filter visualizer, PII masking rules.
  * [`ConnectorService`](file:///home/uttam/projects/AeroMQ/ui/src/app/services/connector.service.ts): Kafka Connect task runner, plugin deployment modal.
* **Component Hierarchy**:
  * [`ClusterOverviewComponent`](file:///home/uttam/projects/AeroMQ/ui/src/app/components/cluster-overview/cluster-overview.component.ts): Live node health, Raft leader indicator, disk usage metrics.
  * [`TopicsComponent`](file:///home/uttam/projects/AeroMQ/ui/src/app/components/topics/topics.component.ts): Partition inspection, replication health, message throughput graphs.
  * [`MessagesComponent`](file:///home/uttam/projects/AeroMQ/ui/src/app/components/messages/messages.component.ts): Real-time message streaming viewer, JSON pretty printer, offset seek inspector.
  * [`ConsumerGroupsComponent`](file:///home/uttam/projects/AeroMQ/ui/src/app/components/consumer-groups/consumer-groups.component.ts): Lag monitoring per topic-partition ($HW - \text{Committed Offset}$).

### 5.2 Client SDK Ecosystem (Native Protocol Port 9091 & Kafka Port 9092)

1. **Official AeroStream Native Client SDKs (`github.com/gradientgeeks/aerostream-sdk`)**:
   Production-grade native binary protocol client libraries (`0xAE 0x01` framing, sub-millisecond tail latency):
   * 🦫 **Go**: [`github.com/gradientgeeks/aerostream-sdk/go`](file:///home/uttam/projects/AeroMQ/sdks/go/) (`v0.1.0-preview`)
   * 🦀 **Rust**: [`aerostream-client`](file:///home/uttam/projects/AeroMQ/sdks/rust/) (`v0.1.0-preview`)
   * ☕ **Java**: [`org.gradientgeeks.aerostream:aerostream-client`](file:///home/uttam/projects/AeroMQ/sdks/java/) (`v0.1.0-preview`)
   * 🔷 **.NET (C#)**: [`GradientGeeks.AeroStream.Client`](file:///home/uttam/projects/AeroMQ/sdks/dotnet/) (`v0.1.0-preview`)
   * 🟩 **Node.js / TypeScript**: [`@gradientgeeks/aerostream-client`](file:///home/uttam/projects/AeroMQ/sdks/nodejs/) (`v0.1.0-preview`)

2. **AeroStream Native CLI ([`client/main.go`](file:///home/uttam/projects/AeroMQ/client/main.go))**:
   * Commands: `metadata`, `create-topic`, `produce`, `consume`, `benchmark`.
   * Directly interfaces with the gRPC Control Plane for discovery and opens high-speed TCP connections to storage brokers with framing:
     `[0xAE 0x01 (Magic)][Cmd: 1 byte][Length: 4 bytes BE][Payload]`

3. **Universal Apache Kafka Client Compatibility (Port 9092)**:
   Any standard Apache Kafka client connects seamlessly to port `9092`:
   * **Java**: `org.apache.kafka:kafka-clients`, `Spring Kafka`
   * **Python**: `confluent-kafka` (librdkafka), `kafka-python`
   * **Go**: `github.com/segmentio/kafka-go`, `github.com/twmb/franz-go`
   * **.NET**: `Confluent.Kafka`
   * **Node.js**: `kafkajs`
   * **Rust**: `rdkafka`
   * **CLI**: `kcat` (`kafkacat`), console producer/consumer scripts

### 5.2.1 Native Client SDK Architecture & Zero-Copy Wire Mechanics

The official AeroStream Client SDKs under [`github.com/gradientgeeks/aerostream-sdk`](https://github.com/gradientgeeks/aerostream-sdk) provide first-party client runtimes engineered for maximum throughput, predictable sub-millisecond tail latency, and minimal CPU overhead:

1. **Architectural Parity Across Ecosystems**:
   Each SDK adheres to a unified internal architecture tailored to the concurrency primitives of its host runtime:
   * 🦫 **Go (`github.com/gradientgeeks/aerostream-sdk/go`)**: Leverages Go channels and lock-free rings for high-throughput goroutine message dispatch with zero GC allocation in steady state.
   * 🦀 **Rust (`aerostream-client`)**: Pure async implementation built on Tokio and `bytes::Bytes`, employing lock-free atomics and zero-copy slice borrowing.
   * ☕ **Java (`org.gradientgeeks.aerostream:aerostream-client`)**: Optimized for Java 21+ Project Loom virtual threads and off-heap `ByteBuffer` pools (`sun.misc.Unsafe` / foreign memory API).
   * 🔷 **.NET (`GradientGeeks.AeroStream.Client`)**: Built on modern C# 13 and .NET 9 using `System.Threading.Channels`, `ValueTask`, and `Memory<byte>` memory pooling.
   * 🟩 **Node.js / TypeScript (`@gradientgeeks/aerostream-client`)**: Written in TypeScript with native Node.js buffer pools, stream backpressure (`drain`), and high-performance libuv asynchronous I/O.

2. **Native Wire Protocol Framing (`0xAE 0x01`)**:
   Clients bypass Kafka protocol conversion overhead by speaking directly to the storage broker daemon on port `9091`. The native frame consists of a deterministic binary layout:
   * **Magic Byte Prefix (2 bytes)**: `0xAE 0x01` identifies the packet as an AeroStream native frame.
   * **Command ID (1 byte)**: Identifies the operation (`0x01` Produce, `0x02` Fetch, `0x03` Replicate, `0x04` Metadata, `0x05` Heartbeat, `0x06` ShareFetch, `0x07` ShareAck).
   * **Correlation ID (4 bytes, Big-Endian)**: Opaque identifier echoed back in responses for non-blocking asynchronous pipelining.
   * **Payload Length (4 bytes, Big-Endian uint32)**: Byte length of the following payload.
   * **Payload**: Compact binary serialized request or response body.

3. **Client-Side Record Accumulator & Batching Engine**:
   To minimize network system call overhead, client SDK producers aggregate individual records into partition batches:
   * **Batch Formation**: In-memory ring buffers buffer incoming records up to `batch.size` (default: 64 KiB) or until `linger.ms` (default: 5 ms) expires.
   * **Non-Blocking Ingress**: If producer queues fill up under heavy load, backpressure is exerted via channel blocking or bounded promises, preventing unconstrained memory growth.
   * **Adaptive Connection Multiplexing**: A thread-safe connection pool maintains persistent TCP keepalive sockets to each storage broker in the cluster, dynamically load-balancing partitions across socket channels.


### 5.3 Cloud-Native Kubernetes & Docker Compose Topology

* **Docker Compose Multi-Node Cluster ([`docker-compose.yml`](file:///home/uttam/projects/AeroMQ/docker-compose.yml))**:
  Runs a complete 5-node production cluster:
  * 3 Go Controller instances forming a Raft quorum (`controller-1`, `controller-2`, `controller-3` on ports 7001, 8001, 9001).
  * 2 Rust Broker instances providing zero-copy storage (`broker-1`, `broker-2` on data ports 9091/9093, Kafka ports 9092/9094).
  * 1 MinIO S3 object storage instance on port 9000 for tiered storage offloading.
* **Kubernetes StatefulSets ([`deploy/k8s/`](file:///home/uttam/projects/AeroMQ/deploy/k8s/))**:
  * [`controller-statefulset.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/controller-statefulset.yaml): 3-replica StatefulSet with headless services for deterministic DNS peer discovery (`controller-0.controller-headless...`).
  * [`broker-statefulset.yaml`](file:///home/uttam/projects/AeroMQ/deploy/k8s/broker-statefulset.yaml): High-performance DaemonSet/StatefulSet mounting local NVMe PersistentVolumeClaims, auto-configuring CPU thread pinning and host port bindings.

---

## 6. Comprehensive Code Symbol Reference Index

| Subsystem | Symbol | Source File Location | Architectural Responsibility |
| :--- | :--- | :--- | :--- |
| **Control Plane** | [`RaftNode`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/raft.go#L14) | `go-controller/pkg/consensus/raft.go` | HashiCorp Raft node wrapper, cluster bootstrap, and voter management |
| **Control Plane** | [`FSM`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L78) | `go-controller/pkg/consensus/fsm.go` | Raft Finite State Machine implementing `Apply`, `Snapshot`, and `Restore` |
| **Control Plane** | [`ClusterState`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L70) | `go-controller/pkg/consensus/fsm.go` | Unified state container replicating brokers, topics, groups, and offsets |
| **Control Plane** | [`PartitionState`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L30) | `go-controller/pkg/consensus/fsm.go` | Tracks partition leader, replica IDs, ISR set, HW, and replica offsets |
| **Control Plane** | [`rebalanceGroup`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L497) | `go-controller/pkg/consensus/fsm.go` | Cooperative sticky rebalance algorithm avoiding partition revocation storms |
| **Control Plane** | [`updateISRAndHW`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/consensus/fsm.go#L404) | `go-controller/pkg/consensus/fsm.go` | Recomputes In-Sync Replicas and calculates High-Watermark barrier |
| **Control Plane** | [`Registry`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/registry.go#L105) | `go-controller/pkg/schemaregistry/registry.go` | Thread-safe, Raft-compatible Confluent Schema Registry engine |
| **Control Plane** | [`checkCompatibility`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/schemaregistry/compatibility.go#L13) | `go-controller/pkg/schemaregistry/compatibility.go` | Avro, JSON, and Protobuf Backward/Forward/Full compatibility rules |
| **Control Plane** | [`Engine`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L52) | `go-controller/pkg/transform/engine.go` | In-broker stream transform execution engine |
| **Control Plane** | [`executeMaskPII`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L270) | `go-controller/pkg/transform/engine.go` | Recursive JSON PII field masking |
| **Control Plane** | [`executeFilter`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L330) | `go-controller/pkg/transform/engine.go` | Fast expression evaluator for stream record filtering |
| **Control Plane** | [`executeWASM`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/transform/engine.go#L578) | `go-controller/pkg/transform/engine.go` | Sandboxed WASM WASI binary execution runtime |
| **Control Plane** | [`AclManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/auth/acls.go#L66) | `go-controller/pkg/auth/acls.go` | RBAC authorization manager with wildcard and glob policy matching |
| **Control Plane** | [`ConnectorManager`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/connect/manager.go#L53) | `go-controller/pkg/connect/manager.go` | Kafka Connect-compatible connector and worker task coordinator |
| **Control Plane** | [`Server` (gRPC)](file:///home/uttam/projects/AeroMQ/go-controller/pkg/grpcserver/server.go#L17) | `go-controller/pkg/grpcserver/server.go` | gRPC server handling broker registration, health checks, and metadata |
| **Control Plane** | [`Server` (REST)](file:///home/uttam/projects/AeroMQ/go-controller/pkg/rest/api.go#L24) | `go-controller/pkg/rest/api.go` | REST HTTP management daemon serving Web UI and administration APIs |
| **Data Plane** | [`PartitionLog`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L19) | `rust-broker/src/log/manager.rs` | Storage kernel managing append-only log segments, indices, and HW |
| **Data Plane** | [`LogManager`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L637) | `rust-broker/src/log/manager.rs` | Coordinates partition logs, cleaner loop triggers, and cold storage |
| **Data Plane** | [`ProducerStateTracker`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/producer_state.rs#L64) | `rust-broker/src/log/producer_state.rs` | Idempotent producer sequence tracker preventing duplicate appends |
| **Data Plane** | [`compact_segments`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/compactor.rs#L314) | `rust-broker/src/log/compactor.rs` | Background log compactor executing key deduplication and tombstone GC |
| **Data Plane** | [`compute_dirty_ratio`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/compactor.rs#L223) | `rust-broker/src/log/compactor.rs` | Computes ratio of dirty/redundant bytes in closed segments |
| **Data Plane** | [`DataServer`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/server.rs#L20) | `rust-broker/src/net/server.rs` | High-throughput native TCP server with zero-copy `sendfile(2)` |
| **Data Plane** | [`KafkaServer`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs#L12) | `rust-broker/src/net/kafka_server.rs` | Kafka wire protocol listener on port 9092 handling Kafka API frames |
| **Data Plane** | [`handle_produce`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/handlers.rs#L1172) | `rust-broker/src/kafka/handlers.rs` | Handles Kafka ApiKey 0 (Produce), validating Castagnoli CRC32C |
| **Data Plane** | [`handle_fetch`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/handlers.rs#L1353) | `rust-broker/src/kafka/handlers.rs` | Handles Kafka ApiKey 1 (Fetch) with high-watermark blocking |
| **Data Plane** | [`crc32` & `crc32c`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/handlers.rs#L45) | `rust-broker/src/kafka/handlers.rs` | Table-driven IEEE 802.3 and Castagnoli CRC checksum calculators |
| **Data Plane** | [`TieredStorageProvider`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/provider.rs#L61) | `rust-broker/src/storage/provider.rs` | Trait definition for multi-cloud tiered object storage backends |
| **Data Plane** | [`TieredStorageOffloader`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/offloader.rs#L57) | `rust-broker/src/storage/offloader.rs` | Background worker uploading rolled segments to object storage |
| **Data Plane** | [`StorageProviderFactory`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/factory.rs) | `rust-broker/src/storage/factory.rs` | Factory constructing S3, GCS, Azure Blob, or Local storage providers |
| **Data Plane** | [`ShardEngine`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/engine.rs#L77) | `rust-broker/src/shard/engine.rs` | Thread-per-core actor managing isolated partition logs without locks |
| **Data Plane** | [`ShardRouter`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/router.rs#L5) | `rust-broker/src/shard/router.rs` | Deterministic hash router dispatching `(topic, partition)` to shards |
| **Data Plane** | [`ShardHandle`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/engine.rs#L195) | `rust-broker/src/shard/engine.rs` | Async multi-sender proxy managing flume actor request channels |
| **Data Plane** | [`spawn_shards`](file:///home/uttam/projects/AeroMQ/rust-broker/src/shard/engine.rs#L305) | `rust-broker/src/shard/engine.rs` | Spawns and pins shard threads to CPU cores via `libc::sched_setaffinity` |
| **Data Plane** | [`append_batch_slice`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L367) | `rust-broker/src/log/manager.rs` | In-place disk base offset patching avoiding heap clones on produce |
| **Data Plane** | [`handle_fetch_with_topo`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs#L686) | `rust-broker/src/net/kafka_server.rs` | Long-polling Kafka fetch engine with out-of-lock reads and lazy Notify |
| **Data Plane** | [`archive_sealed_segment`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/manager.rs#L519) | `rust-broker/src/log/manager.rs` | Fast-path hard-linking (`fs::hard_link`) of sealed segments to cold storage |
| **Data Plane** | [`SharePartitionState`](file:///home/uttam/projects/AeroMQ/rust-broker/src/share/state.rs) | `rust-broker/src/share/state.rs` | Cooperative record leasing and acquisition lock coordinator (KIP-932) |
| **Data Plane** | [`IcebergOffloader`](file:///home/uttam/projects/AeroMQ/rust-broker/src/storage/iceberg.rs) | `rust-broker/src/storage/iceberg.rs` | Streaming Apache Parquet vectorization and Iceberg v2 manifest committer |
| **Data Plane** | [`TxnCoordinator`](file:///home/uttam/projects/AeroMQ/rust-broker/src/txn/coordinator.rs) | `rust-broker/src/txn/coordinator.rs` | Two-Phase Commit coordinator managing Producer IDs, epochs, and markers |
| **Data Plane** | [`TxnMarkerBatch`](file:///home/uttam/projects/AeroMQ/rust-broker/src/txn/marker.rs) | `rust-broker/src/txn/marker.rs` | Commit and Abort control batch records enforcing Log Stable Offset ($LSO$) |
| **Client SDK** | [`AeroStreamClient` (Go)](file:///home/uttam/projects/AeroMQ/sdks/go/) | `sdks/go/client.go` | Official Go native client runtime with zero-allocation message pipelines |
| **Client SDK** | [`AeroStreamClient` (Rust)](file:///home/uttam/projects/AeroMQ/sdks/rust/) | `sdks/rust/src/client.rs` | Official Rust client library with async Tokio record accumulators |
| **Client SDK** | [`AeroStreamClient` (Java)](file:///home/uttam/projects/AeroMQ/sdks/java/) | `sdks/java/src/main/java/org/gradientgeeks/aerostream/` | Official Java client optimized for Project Loom virtual threads |
| **Client SDK** | [`AeroStreamClient` (.NET)](file:///home/uttam/projects/AeroMQ/sdks/dotnet/) | `sdks/dotnet/src/GradientGeeks.AeroStream/` | Official C# .NET client leveraging `System.Threading.Channels` |
| **Client SDK** | [`AeroStreamClient` (Node.js)](file:///home/uttam/projects/AeroMQ/sdks/nodejs/) | `sdks/nodejs/src/index.ts` | Official TypeScript client with native buffer streaming backpressure |


