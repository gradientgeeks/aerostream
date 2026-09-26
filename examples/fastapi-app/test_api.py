"""
End-to-End Integration Test for AeroStream FastAPI Sample Backend
Validates Kafka Wire Protocol, HTTP REST streaming, and background consumer worker.
"""

import asyncio
import sys
from httpx import AsyncClient, ASGITransport

from main import app

async def run_tests():
    print("=" * 70)
    print("  AeroStream FastAPI Integration Test Suite")
    print("=" * 70)

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            # 1. Test Root Metadata
            print("\n[1/6] Testing GET / (Root Service Metadata)...", end=" ")
            resp = await client.get("/")
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
            data = resp.json()
            assert "AeroStream" in data["service"]
            print(f"PASSED (Version: {data['version']})")

            # 2. Test Cluster Health
            print("[2/6] Testing GET /cluster/health...", end=" ")
            resp = await client.get("/cluster/health")
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
            health = resp.json()
            assert health["app_status"] == "UP"
            cluster = health["aerostream_cluster"]
            print(f"PASSED (Raft State: {cluster.get('raft_state')}, Brokers: {cluster.get('brokers_count')}, Topics: {cluster.get('topics_count')})")

            # 3. Test Order Creation via Kafka Wire Protocol
            print("[3/6] Testing POST /orders?protocol=kafka (Binary Kafka Wire Protocol)...", end=" ")
            order_payload_kafka = {
                "customer_id": "cust_1001",
                "item": "Sony WH-1000XM5 Noise-Canceling Headphones",
                "quantity": 1,
                "price": 349.99,
                "currency": "USD"
            }
            resp = await client.post("/orders?protocol=kafka", json=order_payload_kafka)
            assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
            order_k = resp.json()
            assert order_k["protocol_used"] == "kafka"
            assert order_k["status"] == "CONFIRMED"
            print(f"PASSED (Order ID: {order_k['order_id']}, Base Offset: {order_k['offset']})")

            # 4. Test Order Creation via HTTP REST
            print("[4/6] Testing POST /orders?protocol=http (HTTP REST Protocol)...", end=" ")
            order_payload_http = {
                "customer_id": "cust_1002",
                "item": "Apple MacBook Pro M3 Max",
                "quantity": 2,
                "price": 3499.00,
                "currency": "USD"
            }
            resp = await client.post("/orders?protocol=http", json=order_payload_http)
            assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
            order_h = resp.json()
            assert order_h["protocol_used"] == "http"
            assert order_h["status"] == "CONFIRMED"
            print(f"PASSED (Order ID: {order_h['order_id']}, Base Offset: {order_h['offset']})")

            # 5. Test Telemetry Ingestion via Kafka Wire Protocol
            print("[5/6] Testing POST /telemetry (IoT Telemetry Stream)...", end=" ")
            telemetry_payload = {
                "device_id": "edge-turbine-alpha-09",
                "temperature": 78.4,
                "cpu_utilization": 88.5,
                "memory_percent": 62.1,
                "status": "OPERATIONAL",
                "metadata": {"region": "us-east-1", "firmware": "v2.4.1"}
            }
            resp = await client.post("/telemetry", json=telemetry_payload)
            assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"
            telem = resp.json()
            print(f"PASSED (Device: {telem['device_id']}, Offset: {telem['offset']})")

            # 6. Wait for background consumer to process and verify retrieval
            print("[6/6] Verifying Stream Processing & Message Fetching...", end=" ")
            await asyncio.sleep(2.0)  # Allow background worker to poll

            # Direct cluster fetch verification
            resp = await client.get("/orders?source=cluster&limit=5")
            assert resp.status_code == 200
            cluster_orders = resp.json()
            assert cluster_orders["count"] > 0, "Expected non-empty orders in cluster"

            # Consumer buffer fetch verification
            resp = await client.get("/orders?source=consumer&limit=5")
            assert resp.status_code == 200
            consumer_orders = resp.json()

            # Telemetry events verification
            resp = await client.get("/telemetry?limit=5")
            assert resp.status_code == 200
            telem_events = resp.json()

            print(f"PASSED (Direct Cluster Messages: {cluster_orders['count']}, Consumed Orders: {consumer_orders['count']}, Consumed Telemetry: {telem_events['count']})")

    print("\n" + "=" * 70)
    print("  ALL TESTS PASSED! AeroStream FastAPI integration verified.")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_tests())
