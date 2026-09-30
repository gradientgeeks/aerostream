# AeroStream API & Protocol Reference

This document provides the definitive specification for interacting with **AeroStream** across both its **HTTP REST Control & Data Plane** and the **Apache Kafka Binary Wire Protocol Gateway**.

---

## Table of Contents

1. [HTTP REST API Reference](#1-http-rest-api-reference)
   - [Cluster & Brokers (`/api/cluster`, `/api/brokers`, `/leave`, `/join`)](#11-cluster--brokers)
   - [Topics (`/api/topics`)](#12-topics)
   - [Message Ingestion & Retrieval (`/api/produce`, `/api/messages`)](#13-message-ingestion--retrieval)
   - [Consumer Groups & Lag (`/api/groups`, `/api/lag`)](#14-consumer-groups--lag)
   - [Schema Registry (`/subjects`, `/schemas`, `/compatibility`, `/config`)](#15-schema-registry-confluent-compatible)
   - [Stream Transforms (`/api/transforms`)](#16-stream-transforms)
   - [RBAC & ACLs (`/api/acls`, `/api/users`)](#17-rbac--access-control-lists)
   - [Connectors Ecosystem (`/api/connectors`, `/connectors`, `/connector-plugins`)](#18-connectors-ecosystem)
   - [Stream Processing Engine (`/api/streams`)](#19-stream-processing-engine)
   - [Dynamic Data Plane Quotas & Compression (`/api/quotas`, `/api/topic-compression`)](#110-dynamic-data-plane-quotas--compression)
2. [Kafka Wire Protocol Reference](#2-kafka-wire-protocol-reference)
   - [TCP Frame Structure & Byte Ordering](#21-tcp-frame-structure--byte-ordering)
   - [Headers & Envelope Types](#22-headers--envelope-types)
   - [Kafka API Key Coverage Table (34+ API Keys)](#23-kafka-api-key-coverage-table)
   - [Core Messaging APIs (Produce 0, Fetch 1, ListOffsets 2, Metadata 3, ApiVersions 18)](#24-core-messaging-apis)
   - [SASL Security APIs (Keys 17 & 36: PLAIN & SCRAM-SHA-256)](#25-sasl-security-apis)
   - [Transactional & Exactly-Once APIs (Keys 22, 24, 25, 26, 28)](#26-transactional--exactly-once-apis)
   - [Consumer Group Coordination APIs (Keys 8-16, 42)](#27-consumer-group-coordination-apis)
   - [Cluster & Topic Admin APIs (Keys 19, 20, 32, 33, 37, 43, 44, 60)](#28-cluster--topic-admin-apis)
   - [Next-Gen Share Groups (Keys 76-79, KIP-932)](#29-next-gen-share-groups-kip-932)
   - [Protocol Data Types & Error Codes](#210-protocol-data-types--error-codes)
3. [Native Binary TCP Protocol Reference](#3-native-binary-tcp-protocol-reference)
   - [Binary Framing Architecture](#31-binary-framing-architecture)
   - [Command 0: Authentication Handshake](#32-command-0-authentication-handshake)
   - [Command 1: High-Speed Produce / Append](#33-command-1-high-speed-produce--append)
   - [Command 2: Consumer Fetch with High-Watermark Barrier](#34-command-2-consumer-fetch-with-high-watermark-barrier)
   - [Command 3: Replica Fetch & Synchronization](#35-command-3-replica-fetch--synchronization)
   - [Command 4: Multi-Entry Long-Polling Fetch](#36-command-4-multi-entry-long-polling-fetch)

---

## 1. HTTP REST API Reference

The HTTP API is exposed on the Go Controller's HTTP port (default `:9001`). CORS headers (`Access-Control-Allow-Origin: *`, `GET, POST, PUT, DELETE, OPTIONS`) are automatically handled on all endpoints.

---

### 1.1 Cluster & Brokers

#### Get Cluster Status
- **Method / Path**: `GET /api/cluster`
- **Description**: Returns Raft leader metadata, node state, registered broker counts, and individual broker health.
- **Request**: None
- **Response**: `200 OK`
```json
{
  "node_id": "node1",
  "raft_state": "Leader",
  "raft_leader": "127.0.0.1:7001",
  "brokers_count": 2,
  "topics_count": 4,
  "groups_count": 1,
  "brokers": [
    {
      "id": 1,
      "host": "broker-1",
      "port": 9091,
      "active": true,
      "last_seen": 1727394820
    },
    {
      "id": 2,
      "host": "broker-2",
      "port": 9091,
      "active": true,
      "last_seen": 1727394821
    }
  ]
}
```

#### List Brokers
- **Method / Path**: `GET /api/brokers`
- **Description**: Returns the list of all registered storage brokers and their state.
- **Response**: `200 OK`
```json
[
  {
    "id": 1,
    "host": "10.0.0.11",
    "port": 9091,
    "active": true,
    "last_seen": 1727394820
  }
]
```

#### Drain Broker (Evacuate Partitions)
- **Method / Path**: `POST /api/brokers/{id}/drain`
- **Description**: Gracefully evacuates a broker by migrating partition leadership to other ISR members, reassigning replicas, and recalculating ISR/high watermarks.
- **URL Parameter**: `id` (uint32) — Broker ID to drain.
- **Response Codes**:
  - `200 OK`: Drain initiated and completed.
  - `400 Bad Request`: Invalid broker ID.
  - `500 Internal Server Error`: Raft proposal failed.
```json
{
  "success": true,
  "message": "broker 2 drained and partitions reassigned"
}
```

#### Node Liveness / Health Check
- **Method / Path**: `GET /status`
- **Description**: Lightweight health endpoint for Kubernetes liveness/readiness probes.
- **Response**: `200 OK`
```json
{
  "node_id": "node1",
  "state": "Leader",
  "leader": "127.0.0.1:7001",
  "brokers_count": 2,
  "topics_count": 4
}
```

#### Dynamic Raft Join
- **Method / Path**: `GET /join?id={node_id}&addr={raft_addr}`
- **Description**: Used by new controller nodes during bootstrapping to join the Raft consensus group.
- **Query Parameters**:
  - `id`: Unique node ID (e.g. `node2`)
  - `addr`: Peer Raft address (e.g. `10.0.0.2:7001`)
- **Response**: `200 OK` (`joined successfully`)

#### Dynamic Raft Leave
- **Method / Path**: `POST /leave?id={node_id}` or `DELETE /leave`
- **Description**: Gracefully demotes a controller node from the active Raft voter configuration before pod termination (used by Kubernetes `preStop` hooks).
- **Request Body** (optional if `?id=` query param provided):
```json
{
  "node_id": "node3"
}
```
- **Response**: `200 OK`
```json
{
  "success": true,
  "message": "node node3 removed from raft configuration"
}
```

---

### 1.2 Topics

#### List Topics with Partition Topology
- **Method / Path**: `GET /api/topics`
- **Description**: Returns all topics along with detailed partition leadership, replica assignment, ISR membership, high watermarks, and replica offsets.
- **Response**: `200 OK`
```json
[
  {
    "name": "orders",
    "partitions": [
      {
        "partition_id": 0,
        "leader_id": 1,
        "replica_ids": [1, 2],
        "isr": [1, 2],
        "high_watermark": 420,
        "replica_offsets": {
          "1": 420,
          "2": 420
        }
      }
    ]
  }
]
```

#### Create Topic
- **Method / Path**: `POST /api/topics`
- **Description**: Proposes topic creation across the Raft cluster with partition distribution and storage retention overrides.
- **Request JSON Schema**:
```json
{
  "name": "telemetry",
  "partitions": 3,
  "replication_factor": 2,
  "cleanup_policy": "compact,delete",
  "retention_period": "604800s",
  "retention_size": "1073741824",
  "segment_size": "134217728",
  "tombstone_retention": "86400s"
}
```
| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Unique topic name. |
| `partitions` | uint32 | Yes | Number of partitions to provision. |
| `replication_factor`| uint32 | No | Number of replicas (defaults to `1`). |
| `cleanup_policy` | string | No | `"delete"`, `"compact"`, or `"compact,delete"`. |
| `retention_period` | string | No | Duration before segment expiration (e.g. `"7d"`). |
| `retention_size` | string | No | Max byte size per partition before old segments are removed. |
| `segment_size` | string | No | Max segment file size before rolling over (e.g. `"128MB"`). |
| `tombstone_retention`| string | No | Time to retain delete tombstones in compacted logs. |

- **Response Codes**:
  - `201 Created`: Topic successfully created.
  - `400 Bad Request`: Missing topic name or zero partitions.
  - `500 Internal Server Error`: Cluster consensus failure.
```json
{
  "success": true,
  "message": "topic telemetry created successfully"
}
```

---

### 1.3 Message Ingestion & Retrieval

#### Produce Record via HTTP
- **Method / Path**: `POST /api/produce`
- **Description**: Ingests an event record into a specific topic and partition. The controller discovers the partition leader and forwards the record over the zero-copy binary TCP channel.
- **Request JSON Schema**:
```json
{
  "topic": "orders",
  "partition": 0,
  "message": "{\"order_id\":\"ord_9901\",\"amount\":129.50}"
}
```
- **Response**: `200 OK`
```json
{
  "success": true,
  "topic": "orders",
  "partition": 0,
  "offset": 421
}
```
- **Error Codes**:
  - `404 Not Found`: Topic or partition does not exist.
  - `502 Bad Gateway`: Leader broker is offline or unreachable.

#### Fetch Messages via HTTP
- **Method / Path**: `GET /api/messages`
- **Description**: Retrieves sequentially committed records from a topic partition.
- **Query Parameters**:
  - `topic` (string, required): Topic name.
  - `partition` (uint32, required): Partition index.
  - `offset` (uint64, optional, default: `0`): Starting offset.
  - `limit` (int, optional, default: `50`, max: `200`): Maximum records to fetch.
- **Response**: `200 OK`
```json
{
  "topic": "orders",
  "partition": 0,
  "count": 2,
  "messages": [
    {
      "offset": 420,
      "length": 43,
      "payload": "{\"order_id\":\"ord_9900\",\"amount\":45.00}"
    },
    {
      "offset": 421,
      "length": 44,
      "payload": "{\"order_id\":\"ord_9901\",\"amount\":129.50}"
    }
  ]
}
```

---

### 1.4 Consumer Groups & Lag

#### List Consumer Groups
- **Method / Path**: `GET /api/groups`
- **Description**: Returns all registered consumer groups, protocol types (`COOPERATIVE_STICKY`), current group state (`STABLE`, `PREPARING_REBALANCE`), generation ID, active members, and partition assignments.
- **Response**: `200 OK`
```json
[
  {
    "group_id": "order-processors",
    "protocol": "COOPERATIVE_STICKY",
    "state": "STABLE",
    "generation": 4,
    "rebalance_count": 3,
    "leader_id": "member-client-1",
    "last_rebalance_time": "2026-09-26T22:30:00Z",
    "members": [
      {
        "id": "member-client-1",
        "client_host": "10.0.0.42",
        "user_agent": "aerostream-go/1.0",
        "topics": ["orders"],
        "assigned_partitions": [
          {"topic": "orders", "partition": 0}
        ],
        "revoking_partitions": [],
        "last_seen": 1727394825
      }
    ],
    "assignments": {
      "member-client-1": [
        {"topic": "orders", "partition": 0}
      ]
    }
  }
]
```

#### Trigger Cooperative Rebalance
- **Method / Path**: `POST /api/groups/{id}/rebalance`
- **Description**: Triggers an explicit rebalance for a consumer group using the cooperative sticky partition assignment strategy.
- **Response**: `200 OK`
```json
{
  "success": true,
  "message": "cooperative rebalance triggered successfully for group order-processors",
  "group_id": "order-processors",
  "protocol": "COOPERATIVE_STICKY",
  "state": "STABLE",
  "generation": 5,
  "leader_id": "member-client-1",
  "rebalance_count": 4
}
```

#### Query Consumer Group Lag
- **Method / Path**: `GET /api/lag`
- **Description**: Calculates offset lag per consumer group, topic, and partition by comparing committed offsets against partition high watermarks.
- **Response**: `200 OK`
```json
[
  {
    "group_id": "order-processors",
    "topic": "orders",
    "partition": 0,
    "committed_offset": 418,
    "high_watermark": 421,
    "lag": 3
  }
]
```

---

### 1.5 Schema Registry (Confluent-Compatible)

AeroStream incorporates a fully Confluent Schema Registry v1 compatible API supporting Avro, JSON Schema, and Protocol Buffers.

#### List Registered Subjects
- **Method / Path**: `GET /subjects`
- **Response**: `200 OK`
```json
["orders-value", "telemetry-value"]
```

#### List Versions for a Subject
- **Method / Path**: `GET /subjects/{subject}/versions`
- **Response**: `200 OK`
```json
[1, 2, 3]
```

#### Register Schema
- **Method / Path**: `POST /subjects/{subject}/versions`
- **Description**: Registers a new schema under the subject. Enforces the subject's active compatibility level before accepting.
- **Request JSON Schema**:
```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}",
  "schemaType": "AVRO"
}
```
*(Valid `schemaType` values: `"AVRO"`, `"JSON"`, `"PROTOBUF"`. Defaults to `"AVRO"` if omitted).*
- **Response Codes**:
  - `200 OK`: Registered successfully.
  ```json
  {"id": 1}
  ```
  - `409 Conflict`: Schema is incompatible with the existing registered versions for this subject.
  - `422 Unprocessable Entity`: Invalid or malformed schema syntax.

#### Get Schema by Subject and Version
- **Method / Path**: `GET /subjects/{subject}/versions/{version}`
- **URL Parameter**: `version` can be an integer (`1`, `2`) or the string `"latest"`.
- **Response**: `200 OK`
```json
{
  "subject": "orders-value",
  "version": 1,
  "id": 1,
  "schemaType": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
}
```

#### Get Schema by Global ID
- **Method / Path**: `GET /schemas/ids/{id}`
- **Response**: `200 OK`
```json
{
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
}
```

#### Test Schema Compatibility
- **Method / Path**: `POST /compatibility/subjects/{subject}/versions/{version}`
- **Description**: Dry-run tests whether a proposed candidate schema is compatible with an existing registered version or `"latest"`.
- **Request Body**:
```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"status\",\"type\":\"string\",\"default\":\"PENDING\"}]}",
  "schemaType": "AVRO"
}
```
- **Response**: `200 OK`
```json
{
  "is_compatible": true
}
```

#### Get or Update Compatibility Level
- **Global**: `GET /config`, `PUT /config`
- **Subject-Specific**: `GET /config/{subject}`, `PUT /config/{subject}`
- **Supported Levels**: `BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE`, `NONE`.
- **Update Request Body**:
```json
{
  "compatibility": "FULL"
}
```
- **Response**: `200 OK`
```json
{
  "compatibility": "FULL",
  "compatibilityLevel": "FULL"
}
```

---

### 1.6 Stream Transforms

AeroStream provides lightweight, in-broker and in-flight stream processing rules for payload mapping, PII redaction, predicate filtering, and WASM runtime execution.

#### List Transforms
- **Method / Path**: `GET /api/transforms`
- **Response**: `200 OK`
```json
[
  {
    "id": "tf-9a4f21",
    "name": "redact-customer-ssn",
    "source_topic": "raw-customers",
    "target_topic": "sanitized-customers",
    "type": "MASK_PII",
    "config": {
      "fields": "ssn,tax_id,credit_card"
    },
    "status": "RUNNING",
    "messages_processed": 142050,
    "messages_filtered": 0,
    "created_at": "2026-09-26T18:00:00Z"
  }
]
```

#### Create Stream Transform
- **Method / Path**: `POST /api/transforms`
- **Request JSON Schema**:
```json
{
  "name": "filter-failed-payments",
  "source_topic": "transactions",
  "target_topic": "fraud-alerts",
  "type": "FILTER",
  "config": {
    "predicate": "status == 'FAILED' && amount > 5000"
  }
}
```
| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Unique transform name. |
| `source_topic` | string | Yes | Ingestion topic name. |
| `target_topic` | string | Yes | Egress topic for transformed output. |
| `type` | string | Yes | `"FILTER"`, `"MASK_PII"`, `"JSON_MAP"`, or `"WASM"`. |
| `config` | map[string]string | Yes | Type-specific configuration parameters. |
| `code` | string | No | Raw WASM binary (base64) or script expression. |

- **Response**: `201 Created`

#### Manage Transform Lifecycle
- **Get Transform**: `GET /api/transforms/{name}`
- **Delete Transform**: `DELETE /api/transforms/{name}`
- **Pause Transform**: `POST /api/transforms/{name}/pause`
- **Resume Transform**: `POST /api/transforms/{name}/resume`

#### Test Transform Execution (Dry-Run Sandbox)
- **Method / Path**: `POST /api/transforms/test`
- **Description**: Executes a transform against a candidate payload in-memory without publishing, reporting latency and filtered status.
- **Request**:
```json
{
  "transform_name": "filter-failed-payments",
  "payload": {
    "tx_id": "tx_881",
    "status": "FAILED",
    "amount": 7500
  }
}
```
- **Response**: `200 OK`
```json
{
  "status": "success",
  "drop": false,
  "output": "{\"tx_id\":\"tx_881\",\"status\":\"FAILED\",\"amount\":7500}",
  "latency_ms": 0.082
}
```

---

### 1.7 RBAC & Access Control Lists

#### List Active ACL Rules
- **Method / Path**: `GET /api/acls`
- **Response**: `200 OK`
```json
[
  {
    "id": "acl_1a9b",
    "principal": "User:order-service",
    "resource_type": "TOPIC",
    "resource_name": "orders-*",
    "operation": "WRITE",
    "permission": "ALLOW",
    "host": "*"
  }
]
```

#### Add ACL Rule
- **Method / Path**: `POST /api/acls`
- **Request JSON Schema**:
```json
{
  "principal": "User:analytics-reader",
  "resource_type": "TOPIC",
  "resource_name": "telemetry",
  "operation": "READ",
  "permission": "ALLOW",
  "host": "*"
}
```
| Field | Allowed Values |
|---|---|
| `resource_type` | `TOPIC`, `GROUP`, `CLUSTER` |
| `operation` | `READ`, `WRITE`, `DESCRIBE`, `ALTER`, `ALL` |
| `permission` | `ALLOW`, `DENY` |

- **Response**: `201 Created`

#### Delete ACL Rule
- **Method / Path**: `DELETE /api/acls/{id}`
- **Response**: `200 OK` (`{"deleted": true, "id": "acl_1a9b"}`)

#### Test Live Authorization
- **Method / Path**: `POST /api/acls/test`
- **Description**: Validates whether a principal has permission to perform an operation on a resource based on existing ACLs and default deny policies.
- **Request**:
```json
{
  "principal": "User:order-service",
  "resource_type": "TOPIC",
  "resource_name": "orders-eu",
  "operation": "WRITE"
}
```
- **Response**: `200 OK`
```json
{
  "allowed": true,
  "reason": "matched allow rule acl_1a9b pattern 'orders-*'"
}
```

#### User Management
- **List Users**: `GET /api/users`
- **Add User**: `POST /api/users`
```json
{
  "username": "developer_alice",
  "role": "PRODUCER"
}
```
*(Roles: `SUPER_ADMIN`, `OPERATOR`, `PRODUCER`, `CONSUMER`, `AUDITOR`).*

---

### 1.8 Connectors Ecosystem

Compatible with the Apache Kafka Connect REST API as well as native AeroStream endpoints.

#### List Connector Names
- **Method / Path**: `GET /api/connectors` or `GET /connectors`
- **Response**: `200 OK` (`["s3-archival-sink", "postgres-cdc-source"]`)

#### List Detailed Connectors
- **Method / Path**: `GET /api/connectors-detail`
- **Response**: `200 OK`
```json
[
  {
    "name": "s3-archival-sink",
    "type": "SINK",
    "class": "S3ArchivalSinkConnector",
    "topic": "orders",
    "state": "RUNNING",
    "tasks_count": 2,
    "records_processed": 1058200,
    "bytes_transferred": 54289012,
    "config": {
      "s3.bucket": "corporate-datalake",
      "s3.region": "us-east-1"
    }
  }
]
```

#### Register / Deploy Connector
- **Method / Path**: `POST /api/connectors` or `POST /connectors`
- **Request (Supports both native and Kafka Connect formats)**:
```json
{
  "name": "elastic-indexer",
  "config": {
    "connector.class": "ElasticSearchSinkConnector",
    "tasks.max": "2",
    "topics": "audit-log",
    "elastic.url": "http://elasticsearch:9200"
  }
}
```
- **Response**: `201 Created`

#### Connector Status & Metrics
- **Method / Path**: `GET /api/connectors/{name}/status`
- **Response**: `200 OK`
```json
{
  "name": "elastic-indexer",
  "state": "RUNNING",
  "type": "sink",
  "records_processed": 58940,
  "bytes_transferred": 2984102,
  "connector": {
    "state": "RUNNING",
    "worker_id": "aeromq-controller-0"
  },
  "tasks": [
    {"id": 0, "state": "RUNNING", "worker_id": "aeromq-controller-0"},
    {"id": 1, "state": "RUNNING", "worker_id": "aeromq-controller-0"}
  ]
}
```

#### Pause / Resume / Delete Connector
- **Pause**: `PUT /api/connectors/{name}/pause` (`202 Accepted`)
- **Resume**: `PUT /api/connectors/{name}/resume` (`202 Accepted`)
- **Delete**: `DELETE /api/connectors/{name}` (`200 OK`)

#### List Available Plugins
- **Method / Path**: `GET /api/connector-plugins` or `GET /connector-plugins`
- **Response**: `200 OK`
```json
[
  {
    "class": "HttpWebhookSinkConnector",
    "type": "SINK",
    "version": "1.0.0",
    "description": "Dispatches topic records to external HTTP REST endpoints."
  },
  {
    "class": "S3ArchivalSinkConnector",
    "type": "SINK",
    "version": "1.0.0",
    "description": "Streams records to AWS S3 / MinIO object storage."
  },
  {
    "class": "DatabaseCdcSourceConnector",
    "type": "SOURCE",
    "version": "1.0.0",
    "description": "Ingests simulated Change Data Capture (CDC) events from PostgreSQL/MySQL."
  },
  {
    "class": "ElasticSearchSinkConnector",
    "type": "SINK",
    "version": "1.0.0",
    "description": "Streams records to Elasticsearch / OpenSearch index."
  }
]
```

---

### 1.9 Stream Processing Engine

The AeroStream Controller hosts an embedded, stateful stream processing engine ([`go-controller/pkg/rest/api_streams.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/rest/api_streams.go)) that enables real-time stream topologies, tumbling and sliding window aggregations, stateless filtering, and multi-topic joins directly within the control quorum.

#### List Stream Processing Jobs
- **Method / Path**: `GET /api/streams`
- **Response**: `200 OK`
```json
[
  {
    "id": "stream_user_rollup",
    "name": "User Transaction Rollup",
    "source_topic": "transactions",
    "target_topic": "user_hourly_spend",
    "window_type": "TUMBLING",
    "window_duration_secs": 3600,
    "aggregation": "SUM(amount)",
    "status": "RUNNING",
    "records_in": 128490,
    "records_out": 4210
  }
]
```

#### Create Stream Processing Job
- **Method / Path**: `POST /api/streams`
- **Request JSON Schema**:
```json
{
  "name": "Realtime Fraud Detector",
  "source_topic": "payments",
  "target_topic": "fraud_alerts",
  "filter_expression": "amount > 10000 && country != user_home_country",
  "window_type": "SLIDING",
  "window_duration_secs": 300,
  "aggregation": "COUNT(*)"
}
```
- **Response**: `201 Created` (`{"id": "stream_9a2b", "status": "STARTING"}`)

#### Lifecycle Management
- **Inspect Stream**: `GET /api/streams/{id}`
- **Pause Stream**: `POST /api/streams/{id}/pause`
- **Resume Stream**: `POST /api/streams/{id}/resume`
- **Delete Stream**: `DELETE /api/streams/{id}`

---

### 1.10 Dynamic Data Plane Quotas & Compression

Dynamic configurations allow operators to adjust broker performance parameters and client bandwidth limits on the fly without broker restarts ([`go-controller/pkg/rest/dataplane.go`](file:///home/uttam/projects/AeroMQ/go-controller/pkg/rest/dataplane.go)). Updated policies are automatically propagated to all storage brokers via the 2-second gRPC heartbeat response.

#### List Active Client Quotas
- **Method / Path**: `GET /api/quotas`
- **Response**: `200 OK`
```json
[
  {
    "user": "analytics_worker",
    "client_id": "spark-cluster-01",
    "producer_byte_rate": 10485760.0,
    "consumer_byte_rate": 52428800.0,
    "request_percentage": 25.0
  }
]
```

#### Upsert Client Quota
- **Method / Path**: `POST /api/quotas` or `PUT /api/quotas`
- **Request JSON**:
```json
{
  "user": "billing_service",
  "client_id": "billing-*",
  "producer_byte_rate": 20971520.0,
  "consumer_byte_rate": 41943040.0,
  "request_percentage": 50.0
}
```
- **Response**: `201 Created`

#### Delete Client Quota
- **Method / Path**: `DELETE /api/quotas?user={user}&client_id={client_id}`
- **Response**: `204 No Content`

#### Manage Topic Compression Codecs
Enforces compression algorithms per-topic at the broker ingress gateway.
- **List Topic Codecs**: `GET /api/topic-compression` (`200 OK`: `{"orders": "zstd", "telemetry": "lz4"}`)
- **Set Topic Codec**: `POST /api/topic-compression`
```json
{
  "topic": "orders",
  "compression_type": "zstd"
}
```
  *(Supported codecs: `producer`, `uncompressed`, `gzip`, `snappy`, `lz4`, `zstd`).*
- **Delete Topic Codec Override**: `DELETE /api/topic-compression/{topic}` (`204 No Content`)

---

## 2. Kafka Wire Protocol Reference

AeroStream brokers listen for binary Kafka frames on TCP port `9093`. The protocol parser is implemented in Rust ([`rust-broker/src/kafka/protocol.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/protocol.rs)), with frame routing in [`rust-broker/src/net/kafka_server.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs), admin APIs in [`rust-broker/src/kafka/admin.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/admin.rs), transactions in [`rust-broker/src/txn/api.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/txn/api.rs), and share groups in [`rust-broker/src/share/api.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/share/api.rs).

---

### 2.1 TCP Frame Structure & Byte Ordering

All requests and responses use **Big-Endian (network byte order)** and are delimited by a 4-byte frame length prefix:

| Length (4 Bytes) | Packet Payload (1 to 64 MiB) |
|:---|:---|
| Frame Length (`int32` BE) | Encoded Header + API-specific Request/Response Body |

1. **Length**: An `int32` specifying the total size of the message following this field. Maximum supported frame size is **67,108,864 bytes (64 MiB)**.
2. **Payload**: Header followed by the API-specific body.

---

### 2.2 Headers & Envelope Types

#### Request Header (Classic v0 – v1)

| Field | Type | Wire Size | Description |
|:---|:---|:---|:---|
| `api_key` | `int16` | 2 Bytes | Identifies API operation (e.g., 0=Produce, 1=Fetch) |
| `api_version` | `int16` | 2 Bytes | Wire schema version of request body |
| `correlation_id` | `int32` | 4 Bytes | Echoed in corresponding response |
| `client_id` | `NullableString` | 2+N Bytes | Logical client application identifier |

#### Flexible Request Header (v2+)

Modern Kafka versions (e.g. `ApiVersions v3+`, `Produce v9+`, `ShareGroup`) include tagged fields:

| Field | Type | Wire Size | Description |
|:---|:---|:---|:---|
| `api_key` | `int16` | 2 Bytes | Identifies API operation |
| `api_version` | `int16` | 2 Bytes | Wire schema version of request body |
| `correlation_id` | `int32` | 4 Bytes | Echoed in corresponding response |
| `client_id` | `NullableString` | 2+N Bytes | Logical client application identifier |
| `tagged_fields` | `UVarInt` | Varint | Extensible tagged field buffer (KIP-482) |

#### Response Header (v0 – v1)
- `v0`: `[correlation_id: int32]`
- `v1` (Flexible): `[correlation_id: int32][tagged_fields: varint = 0]`

---

### 2.3 Kafka API Key Coverage Table

AeroStream implements **34+ Apache Kafka API Keys**, providing full compatibility with enterprise Kafka client libraries:

| API Key | API Name | Supported Versions | Subsystem & Module | Description & Capabilities |
| :---: | :--- | :---: | :--- | :--- |
| **0** | `Produce` | `0 – 8` | `net::kafka_server` | Ingress produce records with Castagnoli CRC32C, transactional batches (magic 2), in-place base offset patching. |
| **1** | `Fetch` | `0 – 11` | `net::kafka_server` | Long-polling consumer fetch with `max_wait_ms`/`min_bytes` purgatory, KIP-392 preferred rack replica routing, out-of-lock reads. |
| **2** | `ListOffsets` | `0 – 7` | `kafka::admin` | Queries partition earliest, latest, and timestamp-based offsets. |
| **3** | `Metadata` | `0 – 9` | `net::kafka_server` | Returns cluster brokers, rack IDs, partition leaders, replicas, and ISR topology. |
| **8** | `OffsetCommit` | `0 – 8` | `kafka::admin` | Persists committed consumer group offsets to storage. |
| **9** | `OffsetFetch` | `0 – 7` | `kafka::admin` | Fetches committed partition offsets for consumer groups. |
| **10** | `FindCoordinator` | `0 – 4` | `kafka::admin` | Resolves the coordinator broker for consumer groups or transactional IDs. |
| **11** | `JoinGroup` | `0 – 9` | `kafka::admin` | Dynamic consumer group membership and protocol negotiation. |
| **12** | `Heartbeat` | `0 – 4` | `kafka::admin` | Consumer group member liveness heartbeats preventing session timeouts. |
| **13** | `LeaveGroup` | `0 – 5` | `kafka::admin` | Orderly consumer group member departure triggering group rebalance. |
| **14** | `SyncGroup` | `0 – 5` | `kafka::admin` | Synchronizes partition assignments from leader to follower members. |
| **15** | `DescribeGroups` | `0 – 5` | `kafka::admin` | Inspects consumer group state, protocol type, and member assignments. |
| **16** | `ListGroups` | `0 – 4` | `kafka::admin` | Lists all consumer groups hosted across the cluster. |
| **17** | `SaslHandshake` | `0 – 1` | `kafka::sasl` | Initial authentication handshake negotiating SASL mechanisms (`PLAIN`, `SCRAM-SHA-256`). |
| **18** | `ApiVersions` | `0 – 3` | `net::kafka_server` | Advertises all supported API keys, version bounds, and flexible features. |
| **19** | `CreateTopics` | `0 – 7` | `kafka::admin` | Dynamically provisions topics with partition layouts and config overrides. |
| **20** | `DeleteTopics` | `0 – 6` | `kafka::admin` | Deletes topic partitions and removes physical log segments. |
| **22** | `InitProducerId` | `0 – 4` | `txn::api` | Allocates monotonic 64-bit Producer IDs (`PID`) for EOS and transactional sessions. |
| **24** | `AddPartitionsToTxn`| `0 – 3` | `txn::api` | Registers target partitions participating in an active transaction. |
| **25** | `AddOffsetsToTxn` | `0 – 3` | `txn::api` | Binds consumer group offset commits to an active transaction. |
| **26** | `EndTxn` | `0 – 4` | `txn::api` | Two-phase commit resolution (`0 = COMMIT`, `1 = ABORT`) writing control markers. |
| **28** | `TxnOffsetCommit` | `0 – 3` | `txn::api` | Commits transactional consumer offsets visible only under `READ_COMMITTED`. |
| **32** | `DescribeConfigs` | `0 – 4` | `kafka::admin` | Queries cluster, broker, and topic configuration properties. |
| **33** | `AlterConfigs` | `0 – 2` | `kafka::admin` | Modifies topic retention, segment size, and broker configuration overrides. |
| **36** | `SaslAuthenticate` | `0 – 2` | `kafka::sasl` | Executes cryptographic authentication tokens (SASL PLAIN or SCRAM-SHA-256 challenge/response). |
| **37** | `CreatePartitions`| `0 – 3` | `kafka::admin` | Scales topic partition count dynamically across the cluster. |
| **42** | `DeleteGroups` | `0 – 2` | `kafka::admin` | Deletes empty or dead consumer group states. |
| **43** | `ElectLeaders` | `0 – 2` | `kafka::admin` | Triggers preferred or unclean partition leader elections. |
| **44** | `IncrementalAlterConfigs`| `0 – 1` | `kafka::admin` | Granular SET/DELETE configuration operations per topic. |
| **60** | `DescribeCluster` | `0 – 1` | `kafka::admin` | High-level cluster overview (KIP-554) reporting cluster ID and endpoints. |
| **76** | `ShareGroupHeartbeat`| `1 – 1` | `share::api` | Next-generation share group heartbeat and membership coordination (KIP-932). |
| **77** | `ShareGroupDescribe` | `1 – 1` | `share::api` | Inspects share group queue state and consumer consumption status. |
| **78** | `ShareFetch` | `1 – 2` | `share::api` | Competitive point-to-point record consumption with record delivery lock timeouts. |
| **79** | `ShareAcknowledge` | `1 – 2` | `share::api` | Acknowledges record processing (`ACCEPT`, `RELEASE`, `REJECT`). |

---

### 2.4 Core Messaging APIs

#### ApiKey 0: Produce (`v0 – v8`)
- **Base Offset Patching**: On receive, the broker patches the base offset directly in-place on disk using `write_all_at`, bypassing heap clones.
- **Idempotence Tracking**: Validates `(producer_id, epoch, sequence)` via [`ProducerStateTracker`](file:///home/uttam/projects/AeroMQ/rust-broker/src/log/producer_state.rs#L64). Duplicate sequences return cached offset acknowledgments immediately.
- **Request Layout**:
```
[transactional_id]    # Nullable String (v3+)
acks                  # int16 (0 = No Ack, 1 = Leader Ack, -1 / ALL = Quorum Ack)
timeout_ms            # int32
[topics]              # Array
  name                # String
  [partitions]        # Array
    partition_index   # int32
    records_size      # int32
    records           # Bytes: RecordBatch (magic 2) or MessageSet (v0-v1)
```

#### ApiKey 1: Fetch (`v0 – v11`)
- **Long Polling**: If available bytes $< \text{min\_bytes}$, consumer waits up to `max_wait_ms` for an append notify.
- **KIP-392 Read from Closest Replica**: Version 11 carries `rack_id`. When configured, leader directs clients to closest preferred replica.
- **Isolation Levels**: Supports `0 = READ_UNCOMMITTED` and `1 = READ_COMMITTED` (hiding aborted transactions).
- **Out-of-Lock Reads**: Partition lock is held only to resolve the range specification; physical reads occur out-of-lock.

---

### 2.5 SASL Security APIs (Keys 17 & 36)

AeroStream implements wire-level SASL authentication in [`rust-broker/src/kafka/sasl.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/sasl.rs):

1. **`SaslHandshake` (Key 17)**:
   * Client requests mechanism (`PLAIN` or `SCRAM-SHA-256`).
   * Broker returns `ErrorCode = 0` and enabled mechanism list.
2. **`SaslAuthenticate` (Key 36)**:
   * **`PLAIN`**: Validates `\0username\0password` token against the controller's ACL user directory.
   * **`SCRAM-SHA-256`**: Performs RFC 5802 challenge/response with cryptographic salt, iteration count (4096), client-proof, and server-signature validation.

---

### 2.6 Transactional & Exactly-Once APIs (Keys 22, 24, 25, 26, 28)

Implemented in [`rust-broker/src/txn/`](file:///home/uttam/projects/AeroMQ/rust-broker/src/txn/):
- **Two-Phase Commit (2PC)**: `InitProducerId` assigns `PID`. Producer appends transaction markers. When `EndTxn` is called with `COMMIT` or `ABORT`, the transaction coordinator appends control commit/abort markers into participating partition logs.
- **Read Committed Fetch**: Consumers filtering by `isolation_level = 1` read strictly up to the **Last Stable Offset (LSO)**, and skip records belonging to aborted transaction lists.

---

### 2.7 Next-Gen Share Groups (KIP-932: Keys 76 – 79)

AeroStream implements competitive queue-like consumption over topics via Share Groups:
- Multiple consumers concurrently share consumption of a single partition.
- Records are assigned with an acquisition lock timeout (`lock_timeout_ms`).
- Consumers acknowledge via `ShareAcknowledge`:
  * `0 = ACCEPT`: Record successfully processed.
  * `1 = RELEASE`: Release record for redelivery to another consumer.
  * `2 = REJECT`: Poison pill dead-letter handling.

---

### 2.8 Protocol Data Types & Error Codes

#### Primitive Data Types
| Type | Wire Size | Description |
|---|---|---|
| `int8` | 1 byte | Signed 8-bit integer. |
| `int16` | 2 bytes | Signed 16-bit big-endian integer. |
| `int32` | 4 bytes | Signed 32-bit big-endian integer. |
| `int64` | 8 bytes | Signed 64-bit big-endian integer. |
| `varint` | 1 to 5 bytes | Zigzag-encoded variable-length 32-bit integer. |
| `compact_string` | Varint + N | Unsigned varint (length + 1) followed by UTF-8 bytes. |
| `bytes` | 4 bytes + N | `int32` length prefix followed by raw payload bytes. |

#### Standard Kafka Protocol Error Codes
| Code | Constant | Meaning |
|---|---|---|
| `0` | `NONE` | Success. |
| `1` | `OFFSET_OUT_OF_RANGE` | Offset is lower than log start or higher than High Watermark. |
| `2` | `CORRUPT_MESSAGE` | Checksum verification (CRC32/CRC32C) failed. |
| `3` | `UNKNOWN_TOPIC_OR_PARTITION`| Topic or partition index does not exist. |
| `6` | `NOT_LEADER_OR_FOLLOWER` | Broker is not the active leader for this partition. |
| `7` | `REQUEST_TIMED_OUT` | Request expired before minimum bytes were satisfied. |
| `35` | `UNSUPPORTED_VERSION` | Requested ApiVersion is not supported. |
| `45` | `OUT_OF_ORDER_SEQUENCE` | Producer sequence gap detected by idempotence tracker. |
| `48` | `INVALID_PRODUCER_EPOCH` | Fencing older producer epoch. |

---

## 3. Native Binary TCP Protocol Reference

In addition to Kafka wire compatibility, AeroStream provides an ultra-lightweight, zero-overhead native TCP protocol on port `9091`. The native protocol eliminates protocol translation layers and unlocks direct Linux `sendfile(2)` streaming without memory copies.

---

### 3.1 Binary Framing Architecture

Every native TCP packet is structured with a fixed 7-byte header:

```
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
| Magic Bytes (2B)    | Command (1B) | Body Length (4B BE)  | Variable Payload (N B)   |
| 0xAE 0x01           | uint8 (0..4) | uint32               | Command-Specific Bytes   |
+─────────────────────+──────────────+──────────────────────+──────────────────────────+
```

1. **Magic Bytes**: `0xAE 0x01` (`0xAE` = AeroMQ magic, `0x01` = Protocol Version 1). Packets missing this prefix are terminated immediately.
2. **Command Byte**:
   * `0`: Authentication Handshake
   * `1`: High-Speed Produce / Append
   * `2`: Consumer Fetch (High-Watermark Bounded)
   * `3`: Replica Fetch / Sync (Follower LEO Tracking)
   * `4`: Multi-Entry Long-Polling Fetch
3. **Body Length**: 32-bit big-endian integer indicating total payload bytes following the header.

---

### 3.2 Command 0: Authentication Handshake

Establishes connection security using a bearer auth token.

#### Request Body
```
[token_bytes: UTF-8 string]
```

#### Response Frame
```
+─────────────────────+──────────────+
| Magic Bytes (2B)    | Status (1B)  |
| 0xAE 0x01           | 0 = Success  |
|                     | 3 = Failed   |
+─────────────────────+──────────────+
```
If token verification fails, the broker closes the socket connection immediately.

---

### 3.3 Command 1: High-Speed Produce / Append

Appends raw record batches to the physical commit log.

#### Request Body
```
[topic_len: uint16 BE][topic: UTF-8 string][partition: uint32 BE][payload_bytes: raw]
```

#### Response Frame
```
+─────────────────────+──────────────+────────────────────────────+
| Magic Bytes (2B)    | Status (1B)  | Committed Offset (8B BE)   |
| 0xAE 0x01           | 0 = Success  | uint64 assigned log offset |
+─────────────────────+──────────────+────────────────────────────+
```
*(Status `45` returned on out-of-order sequence).*

---

### 3.4 Command 2: Consumer Fetch with High-Watermark Barrier

Reads committed records up to the partition High Watermark using zero-copy `sendfile(2)`.

#### Request Body
```
[topic_len: uint16 BE][topic: UTF-8 string][partition: uint32 BE][start_offset: uint64 BE][max_bytes: uint32 BE]
```

#### Response Frame
If data is available (`start_offset < HighWatermark`):
```
+──────────────────+─────────────+──────────────────────+───────────────────────────────+
| Magic Bytes (2B) | Status (1B) | Bytes To Read (4B BE)| DMA Data Stream (sendfile(2)) |
| 0xAE 0x01        | 0 = Data    | uint32 length        | Segment bytes direct from NIC |
+──────────────────+─────────────+──────────────────────+───────────────────────────────+
```
If no data is available (`start_offset >= HighWatermark`):
```
+──────────────────+─────────────+
| Magic Bytes (2B) | Status (1B) |
| 0xAE 0x01        | 1 = Empty   |
+──────────────────+─────────────+
```

---

### 3.5 Command 3: Replica Fetch & Synchronization

Used exclusively by follower storage brokers replicating from partition leaders.

#### Request Body
```
[replica_id: uint32 BE][topic_len: uint16 BE][topic: UTF-8 string][partition: uint32 BE][start_offset: uint64 BE][max_bytes: uint32 BE]
```
- **Bypasses High Watermark**: Allows followers to replicate all written records up to the leader's active Log End Offset (LEO).
- **Progress Tracking**: Leader invokes `update_follower_offset(replica_id, start_offset)` to recalculate ISR membership and advance the partition High Watermark.

---

### 3.6 Command 4: Multi-Entry Long-Polling Fetch

Allows consumers to retrieve multiple records in a single round-trip with long-polling wait semantics.

#### Request Body
```
[topic_len: uint16 BE][topic: UTF-8 string][partition: uint32 BE][start_offset: uint64 BE][max_bytes: uint32 BE][max_wait_ms: uint32 BE]
```
- Returns all sequential records between `start_offset` and `HighWatermark` fitting within `max_bytes`.
- If no records are present, suspends asynchronously for up to `max_wait_ms` awaiting `append_notify`.

