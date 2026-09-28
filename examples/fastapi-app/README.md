# AeroStream FastAPI Microservice Example

This sample application demonstrates how developers can integrate Python FastAPI applications directly with **AeroStream** using:
1. **Kafka Wire Protocol**: High-throughput, binary framing over TCP (`port 9093`) with zero modification required on the message layer.
2. **HTTP REST API**: Standard JSON REST endpoints for rapid integration and monitoring (`port 9001`).
3. **Continuous Background Streaming**: Asynchronous event consumer polling topics and maintaining real-time event buffers.

---

## Architecture Overview

```
                        ┌───────────────────────────────┐
                        │       FastAPI Microservice    │
                        │       (Python 3.10+ / ASGI)   │
                        └───────┬───────────────┬───────┘
                                │               │
          Kafka Wire Protocol   │               │   HTTP REST API
          (TCP Port 9093)       │               │   (Port 9001)
                                ▼               ▼
                 ┌──────────────────────────────────────┐
                 │          AeroStream Cluster          │
                 │  ┌────────────────────────────────┐  │
                 │  │ Rust Broker (Kafka Protocol)   │  │
                 │  └────────────────────────────────┘  │
                 │  ┌────────────────────────────────┐  │
                 │  │ Go Controller (Raft Consensus) │  │
                 │  └────────────────────────────────┘  │
                 └──────────────────────────────────────┘
```

---

## Setup & Running

### 1. Prerequisites
Ensure AeroStream is running locally:
- Go Controller: `http://127.0.0.1:9001`
- Rust Broker 1: Kafka TCP `127.0.0.1:9093`

### 2. Install Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Automated Test Suite
```bash
python test_api.py
```

### 4. Start the FastAPI Development Server
```bash
uvicorn main:app --reload --port 8000
```
Open your browser at `http://127.0.0.1:8000/docs` to test endpoints interactively via Swagger UI.

---

## Key Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/orders?protocol=kafka` | Places an order using binary Kafka Wire Protocol (ApiKey 0) |
| `POST` | `/orders?protocol=http` | Places an order using AeroStream HTTP REST API |
| `GET` | `/orders?source=consumer` | Retrieves orders processed by the async background consumer worker |
| `GET` | `/orders?source=cluster` | Direct fetch of messages from AeroStream partition log |
| `POST` | `/telemetry` | Ingests high-frequency sensor/device metrics |
| `GET` | `/telemetry` | Fetches recent telemetry stream |
| `GET` | `/cluster/health` | Inspects AeroStream Raft leadership, broker states, and topic counts |
