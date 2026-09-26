#!/usr/bin/env bash
set -eo pipefail

echo "======================================================"
echo " Starting AeroStream Full-Stack Cluster"
echo " Engine: Dual Go Consensus + Rust Zero-Copy Broker"
echo " Console: http://0.0.0.0:${HTTP_PORT:-9001}/aerostream/console"
echo " Data Plane: port ${DATA_PORT:-9091}"
echo "======================================================"

DATA_DIR="${DATA_DIR:-/data}"
mkdir -p "${DATA_DIR}/controller" "${DATA_DIR}/broker"

NODE_ID="${NODE_ID:-node1}"
RAFT_PORT="${RAFT_PORT:-7001}"
GRPC_PORT="${GRPC_PORT:-8001}"
HTTP_PORT="${HTTP_PORT:-9001}"
DATA_PORT="${DATA_PORT:-9091}"
BROKER_ID="${BROKER_ID:-1}"

# Cleanup handler on exit
cleanup() {
    echo "Shutting down AeroMQ processes..."
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    kill -TERM "$CONTROLLER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    wait "$CONTROLLER_PID" 2>/dev/null || true
    echo "AeroMQ gracefully stopped."
    exit 0
}
trap cleanup SIGTERM SIGINT

# 1. Start Go Controller
echo "[1/2] Starting Go Controller (Raft & Management)..."
controller \
    -id "${NODE_ID}" \
    -raft-addr "127.0.0.1:${RAFT_PORT}" \
    -grpc-addr "0.0.0.0:${GRPC_PORT}" \
    -http-addr "0.0.0.0:${HTTP_PORT}" \
    -data-dir "${DATA_DIR}/controller" \
    -ui-dir "/app/ui" \
    -bootstrap &
CONTROLLER_PID=$!

# Wait for controller gRPC / HTTP to become ready
echo "Waiting for controller to initialize..."
for i in {1..30}; do
    if curl -s "http://127.0.0.1:${HTTP_PORT}/status" > /dev/null 2>&1; then
        echo "Go Controller is online."
        break
    fi
    sleep 0.2
done

# 2. Start Rust Broker
echo "[2/2] Starting Rust Zero-Copy Storage Broker..."
rust-broker \
    --id "${BROKER_ID}" \
    --host "0.0.0.0" \
    --data-port "${DATA_PORT}" \
    --controller "http://127.0.0.1:${GRPC_PORT}" \
    --storage-dir "${DATA_DIR}/broker" &
BROKER_PID=$!

echo "======================================================"
echo " AeroStream Full-Stack is READY!"
echo " Web UI:     http://localhost:${HTTP_PORT}/aerostream/console"
echo " REST API:   http://localhost:${HTTP_PORT}/api/cluster"
echo " Data Plane: localhost:${DATA_PORT}"
echo "======================================================"

# Wait for either process to terminate
wait -n "$CONTROLLER_PID" "$BROKER_PID"
cleanup
