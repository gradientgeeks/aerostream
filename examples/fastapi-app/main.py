"""
AeroStream FastAPI Sample Backend
Demonstrates event-driven architecture with AeroStream using both
Kafka wire-protocol binary framing (TCP) and HTTP REST endpoints.
"""

import asyncio
from contextlib import asynccontextmanager
from collections import deque
from datetime import datetime
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

from aerostream_client import AeroStreamClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fastapi-aerostream")

# -----------------------------------------------------------------------------
# Pydantic Schemas
# -----------------------------------------------------------------------------
class OrderCreate(BaseModel):
    customer_id: str = Field(..., example="cust_1001")
    item: str = Field(..., example="Apple MacBook Pro M3 Max")
    quantity: int = Field(default=1, gt=0, example=1)
    price: float = Field(..., gt=0, example=2499.99)
    currency: str = Field(default="USD", example="USD")

class OrderRecord(BaseModel):
    order_id: str
    customer_id: str
    item: str
    quantity: int
    price: float
    currency: str
    status: str
    created_at: str
    offset: Optional[int] = None
    protocol_used: str

class TelemetryPayload(BaseModel):
    device_id: str = Field(..., example="sensor-node-042")
    temperature: float = Field(..., example=24.5)
    cpu_utilization: float = Field(..., example=42.1)
    memory_percent: float = Field(..., example=68.3)
    status: str = Field(default="HEALTHY", example="HEALTHY")
    metadata: Optional[Dict[str, Any]] = None

class HealthResponse(BaseModel):
    app_status: str
    aerostream_cluster: Dict[str, Any]
    consumer_stats: Dict[str, Any]


# -----------------------------------------------------------------------------
# Global State & Client
# -----------------------------------------------------------------------------
client = AeroStreamClient(
    kafka_host="127.0.0.1",
    kafka_port=9093,
    http_url="http://127.0.0.1:9001"
)

# In-memory streaming buffer of recently consumed events
processed_orders: deque = deque(maxlen=100)
processed_telemetry: deque = deque(maxlen=100)
consumer_running = True


async def background_event_consumer():
    """
    Background worker that continuously fetches events from AeroStream
    and processes them asynchronously.
    """
    logger.info("Starting AeroStream background consumer worker...")
    last_order_offset = 0
    last_telemetry_offset = 0

    while consumer_running:
        try:
            # Poll orders
            order_msgs = await client.fetch_http("orders", partition=0, offset=last_order_offset, limit=10)
            for msg in order_msgs:
                offset = msg.get("offset", 0)
                payload_raw = msg.get("payload", "")
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                    processed_orders.append({"offset": offset, "event": payload, "received_at": datetime.utcnow().isoformat()})
                except Exception:
                    processed_orders.append({"offset": offset, "raw": payload_raw})
                last_order_offset = max(last_order_offset, offset + 1)

            # Poll telemetry
            telemetry_msgs = await client.fetch_http("telemetry", partition=0, offset=last_telemetry_offset, limit=10)
            for msg in telemetry_msgs:
                offset = msg.get("offset", 0)
                payload_raw = msg.get("payload", "")
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                    processed_telemetry.append({"offset": offset, "event": payload, "received_at": datetime.utcnow().isoformat()})
                except Exception:
                    processed_telemetry.append({"offset": offset, "raw": payload_raw})
                last_telemetry_offset = max(last_telemetry_offset, offset + 1)

        except Exception as e:
            logger.debug("Background poll transient pause: %s", e)

        await asyncio.sleep(1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_running
    consumer_running = True
    consumer_task = asyncio.create_task(background_event_consumer())
    logger.info("FastAPI application started. Background consumer initialized.")
    try:
        yield
    finally:
        consumer_running = False
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("FastAPI application shut down.")


# -----------------------------------------------------------------------------
# FastAPI App
# -----------------------------------------------------------------------------
app = FastAPI(
    title="AeroStream FastAPI Microservice",
    description="High-throughput event streaming backend powered by AeroStream (Kafka Wire Protocol & HTTP REST).",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/", tags=["General"])
async def root():
    return {
        "service": "AeroStream FastAPI Microservice",
        "version": "1.0.0",
        "description": "Event-driven architecture integrated with AeroStream",
        "endpoints": {
            "create_order": "POST /orders?protocol=kafka|http",
            "list_orders": "GET /orders",
            "ingest_telemetry": "POST /telemetry",
            "list_telemetry": "GET /telemetry",
            "cluster_health": "GET /cluster/health",
        },
    }


@app.post("/orders", response_model=OrderRecord, status_code=status.HTTP_201_CREATED, tags=["Orders"])
async def create_order(
    order: OrderCreate,
    protocol: str = Query("kafka", pattern="^(kafka|http)$", description="Transport protocol to use ('kafka' wire framing or 'http' REST)")
):
    """
    Submits a new order and publishes an event to AeroStream's `orders` topic.
    Demonstrates zero-copy binary Kafka wire framing vs standard HTTP REST.
    """
    order_id = f"ord-{uuid.uuid4().hex[:8]}"
    created_at = datetime.utcnow().isoformat() + "Z"
    
    event_payload = {
        "order_id": order_id,
        "customer_id": order.customer_id,
        "item": order.item,
        "quantity": order.quantity,
        "price": order.price,
        "currency": order.currency,
        "status": "CONFIRMED",
        "created_at": created_at,
    }

    try:
        if protocol == "kafka":
            # Direct binary TCP Kafka wire protocol (ApiKey 0)
            offset = client.produce_kafka("orders", partition=0, message=json.dumps(event_payload))
        else:
            # HTTP REST protocol
            resp = await client.produce_http("orders", partition=0, payload=event_payload)
            offset = resp.get("offset", 0)

        return OrderRecord(
            order_id=order_id,
            customer_id=order.customer_id,
            item=order.item,
            quantity=order.quantity,
            price=order.price,
            currency=order.currency,
            status="CONFIRMED",
            created_at=created_at,
            offset=offset,
            protocol_used=protocol,
        )
    except Exception as e:
        logger.error(f"Failed to produce order event to AeroStream: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish event to AeroStream: {str(e)}")


@app.get("/orders", tags=["Orders"])
async def get_orders(
    source: str = Query("consumer", pattern="^(consumer|cluster)$", description="'consumer' for locally processed events or 'cluster' for direct AeroStream fetch"),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Returns recently ingested orders either from the background streaming worker or directly from AeroStream.
    """
    if source == "consumer":
        return {
            "source": "background_consumer_buffer",
            "count": min(len(processed_orders), limit),
            "orders": list(processed_orders)[-limit:],
        }
    else:
        raw_msgs = await client.fetch_http("orders", partition=0, offset=0, limit=limit)
        return {
            "source": "aerostream_direct_fetch",
            "count": len(raw_msgs),
            "messages": raw_msgs,
        }


@app.post("/telemetry", status_code=status.HTTP_202_ACCEPTED, tags=["Telemetry"])
async def ingest_telemetry(payload: TelemetryPayload):
    """
    Ingests high-frequency IoT/system telemetry and streams it into the `telemetry` topic.
    """
    event_data = {
        "event_id": str(uuid.uuid4()),
        "device_id": payload.device_id,
        "temperature": payload.temperature,
        "cpu_utilization": payload.cpu_utilization,
        "memory_percent": payload.memory_percent,
        "status": payload.status,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "metadata": payload.metadata or {},
    }

    try:
        # Publish via high-performance binary Kafka wire protocol
        offset = client.produce_kafka("telemetry", partition=0, message=json.dumps(event_data))
        return {
            "status": "accepted",
            "offset": offset,
            "device_id": payload.device_id,
            "timestamp": event_data["timestamp"]
        }
    except Exception as e:
        logger.error(f"Failed to ingest telemetry: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stream telemetry: {str(e)}")


@app.get("/telemetry", tags=["Telemetry"])
async def get_telemetry(limit: int = Query(20, ge=1, le=100)):
    """
    Retrieves recent telemetry events received by the background consumer.
    """
    return {
        "count": min(len(processed_telemetry), limit),
        "events": list(processed_telemetry)[-limit:],
    }


@app.get("/cluster/health", response_model=HealthResponse, tags=["Health"])
async def cluster_health():
    """
    Queries cluster topology, raft consensus state, and active brokers from AeroStream.
    """
    try:
        cluster_info = await client.get_cluster_status()
        return HealthResponse(
            app_status="UP",
            aerostream_cluster=cluster_info,
            consumer_stats={
                "orders_buffered": len(processed_orders),
                "telemetry_buffered": len(processed_telemetry),
                "consumer_active": consumer_running,
            }
        )
    except Exception as e:
        logger.error(f"Failed to query AeroStream health: {e}")
        raise HTTPException(status_code=503, detail=f"AeroStream unavailable: {str(e)}")
