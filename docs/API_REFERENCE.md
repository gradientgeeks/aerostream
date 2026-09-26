# AeroStream API & Protocol Reference

This document provides the definitive specification for interacting with **AeroStream** across both its **HTTP REST Control & Data Plane** and the **Apache Kafka Binary Wire Protocol Gateway**.

---

## Table of Contents

1. [HTTP REST API Reference](#1-http-rest-api-reference)
   - [Cluster & Brokers (`/api/cluster`, `/api/brokers`, `/leave`)](#11-cluster--brokers)
   - [Topics (`/api/topics`)](#12-topics)
   - [Message Ingestion & Retrieval (`/api/produce`, `/api/messages`)](#13-message-ingestion--retrieval)
   - [Consumer Groups & Lag (`/api/groups`, `/api/lag`)](#14-consumer-groups--lag)
   - [Schema Registry (`/subjects`, `/schemas`, `/compatibility`, `/config`)](#15-schema-registry-confluent-compatible)
   - [Stream Transforms (`/api/transforms`)](#16-stream-transforms)
   - [RBAC & ACLs (`/api/acls`, `/api/users`)](#17-rbac--access-control-lists)
   - [Connectors Ecosystem (`/api/connectors`, `/api/connector-plugins`)](#18-connectors-ecosystem)
2. [Kafka Wire Protocol Reference](#2-kafka-wire-protocol-reference)
   - [TCP Frame Structure & Byte Ordering](#21-tcp-frame-structure--byte-ordering)
   - [Headers & Envelope Types](#22-headers--envelope-types)
   - [ApiKey 0: Produce (v0 – v8)](#23-apikey-0-produce)
   - [ApiKey 1: Fetch (v0 – v11)](#24-apikey-1-fetch)
   - [ApiKey 3: Metadata (v0 – v9)](#25-apikey-3-metadata)
   - [ApiKey 18: ApiVersions (v0 – v3)](#26-apikey-18-apiversions)
   - [ApiKey 22: InitProducerId (v0 – v4)](#27-apikey-22-initproducerid)
   - [Protocol Data Types & Error Codes](#28-protocol-data-types--error-codes)

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

## 2. Kafka Wire Protocol Reference

AeroStream brokers listen for binary Kafka frames on TCP port `9093`. The protocol parser is implemented in Rust ([`rust-broker/src/kafka/protocol.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/kafka/protocol.rs)) and handled by [`rust-broker/src/net/kafka_server.rs`](file:///home/uttam/projects/AeroMQ/rust-broker/src/net/kafka_server.rs).

---

### 2.1 TCP Frame Structure & Byte Ordering

All requests and responses use **Big-Endian (network byte order)** and are delimited by a 4-byte frame length prefix:

```
+------------------------------------+----------------------------------------+
| 4 Bytes                            | Variable Size (1 to 64 MiB)           |
| Frame Length (int32)               | Packet Payload (Header + Request/Resp) |
+------------------------------------+----------------------------------------+
```

1. **Length**: An `int32` specifying the total size of the message following this field. Maximum supported frame size is **67,108,864 bytes (64 MiB)**.
2. **Payload**: Header followed by the API-specific body.

---

### 2.2 Headers & Envelope Types

#### Request Header (v0 – v2)
Every request starts with:
```
+---------------+---------------+--------------------+---------------------+
| api_key (i16) | version (i16) | correlation_id(i32)| client_id (String)  |
+---------------+---------------+--------------------+---------------------+
```
- `api_key` (`int16`): The numeric identifier of the API request.
- `version` (`int16`): Protocol version for this ApiKey.
- `correlation_id` (`int32`): Client-assigned request identifier, echoed back in the response.
- `client_id` (`string`): Nullable UTF-8 string prefixed by `int16` length (`-1` = null).

#### Response Header (v0 – v1)
```
+--------------------+
| correlation_id(i32)|
+--------------------+
```
Echoes the matching client request `correlation_id`.

---

### 2.3 ApiKey 0: Produce

**Supported Versions**: `v0` through `v8` (supports Legacy MessageSet format v0-v1 and modern RecordBatch format v2+).

#### Request Layout (v0 to v8)
```
[transactional_id]    # (Nullable String, v3+)
acks                  # int16 (0 = No Ack, 1 = Leader Ack, -1 / ALL = Quorum Ack)
timeout_ms            # int32
[topics]              # Array
  name                # String
  [partitions]        # Array
    partition_index   # int32
    record_set        # Bytes: MessageSet (v0-v1) or RecordBatch (v2+)
```

#### Response Layout
```
[responses]           # Array
  name                # String
  [partitions]        # Array
    partition_index   # int32
    error_code        # int16 (0 = None, 3 = UnknownTopic, 6 = NotLeader)
    base_offset       # int64 (Committed offset assigned by broker)
    log_append_time   # int64 (v2+)
    log_start_offset  # int64 (v5+)
throttle_time_ms      # int32 (v1+)
```

---

### 2.4 ApiKey 1: Fetch

**Supported Versions**: `v0` through `v11`.

#### Request Layout (v0 to v11)
```
replica_id            # int32 (-1 for regular consumers)
max_wait_ms           # int32 (Broker waits up to this duration for data)
min_bytes             # int32 (Min accumulation threshold)
[max_bytes]           # int32 (v3+)
isolation_level       # int8 (v4+, 0 = ReadUncommitted, 1 = ReadCommitted)
session_id            # int32 (v7+)
session_epoch         # int32 (v7+)
[topics]              # Array
  name                # String
  [partitions]        # Array
    partition_index   # int32
    fetch_offset      # int64 (Starting offset to fetch)
    partition_max_bytes # int32
```

#### Response Layout
```
throttle_time_ms      # int32 (v1+)
error_code            # int16 (v7+)
session_id            # int32 (v7+)
[responses]           # Array
  name                # String
  [partitions]        # Array
    partition_index   # int32
    error_code        # int16
    high_watermark    # int64
    last_stable_offset# int64 (v4+)
    records           # Bytes (Binary RecordBatch / MessageSet stream)
```

---

### 2.5 ApiKey 3: Metadata

**Supported Versions**: `v0` through `v9`.

#### Request Layout
```
[topics]              # Array of strings (null / empty fetches all topics)
allow_auto_topic_creation # bool (v4+)
```

#### Response Layout
```
throttle_time_ms      # int32 (v3+)
[brokers]             # Array
  node_id             # int32
  host                # String
  port                # int32
  rack                # Nullable String (v1+)
cluster_id            # Nullable String (v2+)
controller_id         # int32 (v1+)
[topics]              # Array
  error_code          # int16
  name                # String
  is_internal         # bool (v1+)
  [partitions]        # Array
    error_code        # int16
    partition_index   # int32
    leader_id         # int32
    [replica_nodes]   # Array of int32
    [isr_nodes]       # Array of int32
```

---

### 2.6 ApiKey 18: ApiVersions

**Supported Versions**: `v0` through `v3`.
Used during client initialization to query the broker's protocol capabilities.

#### Response Layout
```
error_code            # int16 (0 = Success)
[api_keys]            # Array
  api_key             # int16
  min_version         # int16
  max_version         # int16
throttle_time_ms      # int32 (v1+)
```

AeroStream returns:
- `ApiKey 0` (Produce): `min_version: 0`, `max_version: 8`
- `ApiKey 1` (Fetch): `min_version: 0`, `max_version: 11`
- `ApiKey 3` (Metadata): `min_version: 0`, `max_version: 9`
- `ApiKey 18` (ApiVersions): `min_version: 0`, `max_version: 3`
- `ApiKey 22` (InitProducerId): `min_version: 0`, `max_version: 4`

---

### 2.7 ApiKey 22: InitProducerId

**Supported Versions**: `v0` through `v4`.
Used by idempotent and transactional Kafka producers to acquire a monotonic Producer ID (`PID`) and epoch.

#### Request Layout
```
transactional_id         # Nullable String
transaction_timeout_ms   # int32
producer_id              # int64 (v3+)
producer_epoch           # int16 (v3+)
```

#### Response Layout
```
throttle_time_ms      # int32
error_code            # int16 (0 = Success)
producer_id           # int64 (Monotonically incremented unique PID)
producer_epoch        # int16 (Initialized to 0)
```

---

### 2.8 Protocol Data Types & Error Codes

#### Primitive Data Types

| Type | Size | Description |
|---|---|---|
| `int8` | 1 byte | Signed 8-bit integer. |
| `int16` | 2 bytes | Signed 16-bit big-endian integer. |
| `int32` | 4 bytes | Signed 32-bit big-endian integer. |
| `int64` | 8 bytes | Signed 64-bit big-endian integer. |
| `varint` | 1 to 5 bytes | Zigzag-encoded variable-length 32-bit integer. |
| `varlong` | 1 to 10 bytes | Zigzag-encoded variable-length 64-bit integer. |
| `string` | 2 bytes + N | `int16` length prefix followed by UTF-8 bytes (`-1` = null). |
| `compact_string` | Varint + N | Unsigned varint (length + 1) followed by UTF-8 bytes (`0` = null). |
| `bytes` | 4 bytes + N | `int32` length prefix followed by raw payload bytes. |
| `array` | 4 bytes + N*M | `int32` count prefix followed by elements. |

#### Standard Kafka Error Codes

| Code | Constant | Meaning |
|---|---|---|
| `0` | `NONE` | Success. |
| `1` | `OFFSET_OUT_OF_RANGE` | Requested fetch offset is lower than log start or higher than high watermark. |
| `2` | `CORRUPT_MESSAGE` | Checksum verification (CRC32/CRC32C) failed. |
| `3` | `UNKNOWN_TOPIC_OR_PARTITION` | Topic does not exist or partition index is out of bounds. |
| `6` | `NOT_LEADER_OR_FOLLOWER` | Broker is not the active leader for this partition. |
| `7` | `REQUEST_TIMED_OUT` | Request could not be fulfilled within specified client timeout. |
| `35` | `UNSUPPORTED_VERSION` | Requested ApiVersion is not supported by this broker. |
