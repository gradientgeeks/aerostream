# Multi-stage Dockerfile for AeroStream Full-Stack Container (Controller + Broker + Console UI)

# --- Stage 1: Build Angular Web UI Console ---
FROM node:20-slim AS ui-builder
WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build -- --base-href /aerostream/console/

# --- Stage 2: Build Go Controller ---
FROM golang:1.26-alpine AS controller-builder
RUN apk add --no-cache ca-certificates git tzdata
WORKDIR /app
COPY go-controller/go.mod go-controller/go.sum ./go-controller/
RUN cd go-controller && go mod download
COPY proto ./proto
COPY go-controller ./go-controller
RUN cd go-controller && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/controller cmd/controller/main.go

# --- Stage 3: Build Rust Zero-Copy Broker ---
FROM rust:slim-bookworm AS broker-builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler pkg-config && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY proto/ ./proto/
COPY rust-broker/Cargo.toml rust-broker/Cargo.lock ./rust-broker/
COPY rust-broker/build.rs ./rust-broker/
COPY rust-broker/src/ ./rust-broker/src/
RUN cd rust-broker && cargo build --release

# --- Stage 4: Minimal Debian Runtime ---
FROM debian:trixie-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl procps bash && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /data /app/ui

WORKDIR /app

# Install compiled binaries
COPY --from=controller-builder /bin/controller /usr/local/bin/controller
COPY --from=broker-builder /app/rust-broker/target/release/rust-broker /usr/local/bin/rust-broker

# Install Web UI static assets
COPY --from=ui-builder /app/ui/dist/ui/browser/ /app/ui/

# Install entrypoint supervisor script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 9091: Native TCP Data Plane
# 9092: Kafka Wire Protocol TCP Listener
# 8001: gRPC Cluster Control
# 9001: HTTP REST API & Web UI Console (/aerostream/console)
# 7001: Raft Consensus
EXPOSE 9091 9092 8001 9001 7001

VOLUME ["/data"]

ENTRYPOINT ["/entrypoint.sh"]
