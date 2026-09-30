---
icon: material/database-outline
---
# Built-in Schema Registry

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Confluent API</span>
<span class="md-tag">5 min read</span>
<span class="md-tag">Avro / JSON / Protobuf</span>
</div>

AeroStream embeds a fully **Confluent-compatible Schema Registry** directly into the Go Control Plane (listening on HTTP port **`9001`**).

In traditional streaming architectures, running a Schema Registry requires deploying separate Java containers, backing them with internal Kafka topics or relational databases, and maintaining additional monitoring. In AeroStream, the Schema Registry is natively integrated, with schemas replicated across controllers using the Raft consensus quorum.

---

## REST API Endpoints

The embedded registry implements the standard Confluent Schema Registry HTTP interface:

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/subjects/{subject}/versions` | Registers a new schema version under the subject. Returns the globally unique schema ID. |
| `GET` | `/subjects` | Lists all registered subject names in the cluster. |
| `GET` | `/subjects/{subject}/versions` | Returns an array of all registered version integers for the subject. |
| `GET` | `/subjects/{subject}/versions/latest` | Retrieves the latest registered schema metadata, version ID, and canonical schema string. |
| `GET` | `/subjects/{subject}/versions/{version}` | Retrieves schema metadata for a specific version number. |
| `GET` | `/schemas/ids/{id}` | Fetches the canonical schema definition by its immutable global ID. |
| `POST` | `/compatibility/subjects/{subject}/versions/{version}` | Tests whether a proposed schema is compatible with the specified subject version. |
| `GET` | `/config` / `GET /config/{subject}` | Queries global or subject-level compatibility mode. |
| `PUT` | `/config` / `PUT /config/{subject}` | Sets global or subject-level compatibility mode (`BACKWARD`, `FORWARD`, `FULL`, `NONE`). |

---

## Compatibility Enforcement Modes

AeroStream automatically validates proposed schemas before accepting registration, safeguarding downstream consumers from breaking deserialization errors:

![Dual Engine Architecture](images/dual_engine_architecture.png)

* **`BACKWARD` (Default)**: Consumers using the new schema can deserialize data written by producers using previous versions. Fields may only be deleted, or optional fields (with defaults) may be added.
* **`FORWARD`**: Consumers using previous schema versions can deserialize data produced by the new schema. Fields may only be added, or optional fields deleted.
* **`FULL`**: Both backward and forward compatible. Ensures seamless upgrades regardless of whether producers or consumers are redeployed first.
* **`NONE`**: Schema validation is disabled. Any valid schema syntax is accepted.

---

## Avro, JSON, & Protobuf Examples

### 1. Registering an Avro Schema via REST

```bash
curl -X POST http://localhost:9001/subjects/orders-value/versions \
  -H "Content-Type: application/json" \
  -d '{
    "schemaType": "AVRO",
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.aerostream.commerce\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"}]}"
  }'
```

*Response:*
```json
{
  "id": 1
}
```

### 2. Testing Schema Compatibility

Before rolling out an updated schema, test it against the latest registered version:

```bash
curl -X POST http://localhost:9001/compatibility/subjects/orders-value/versions/latest \
  -H "Content-Type: application/json" \
  -d '{
    "schemaType": "AVRO",
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.aerostream.commerce\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"customer_tier\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'
```

*Response:*
```json
{
  "is_compatible": true
}
```

### 3. Wire Format Specification

AeroStream follows the standard Confluent framing standard for schema-encoded record payloads:

| Magic Byte (`0x00`) | 4-Byte Global Schema ID | Avro / Protobuf / JSON Binary Body |
|:---:|:---:|:---:|
| 1 byte | 4 bytes (32-bit big-endian) | Variable length payload |

Standard serialization libraries (e.g. `confluent_kafka.schema_registry`) automatically prepend and decode this header seamlessly when communicating with `http://localhost:9001`.
