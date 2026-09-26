#!/usr/bin/env python3
"""
AeroMQ Kafka Wire Protocol Verification Test
Validates ApiVersions, Metadata, Produce, and Fetch frames over TCP.
"""

import socket
import struct
import sys
import time
import zlib
import argparse

def put_string(s: str) -> bytes:
    if s is None:
        return struct.pack(">h", -1)
    b = s.encode("utf-8")
    return struct.pack(">h", len(b)) + b

def read_string(data: bytes, offset: int):
    str_len = struct.unpack_from(">h", data, offset)[0]
    offset += 2
    if str_len < 0:
        return None, offset
    val = data[offset : offset + str_len].decode("utf-8")
    offset += str_len
    return val, offset

def wrap_kafka_message(offset: int, payload: bytes) -> bytes:
    # Inner: magic(1) + attr(1) + key_len(4) + val_len(4) + val
    inner = struct.pack(">bbii", 0, 0, -1, len(payload)) + payload
    crc = zlib.crc32(inner) & 0xFFFFFFFF
    msg_size = 4 + len(inner)
    return struct.pack(">qii", offset, msg_size, crc) + inner

def send_and_recv(sock: socket.socket, payload: bytes) -> bytes:
    frame = struct.pack(">i", len(payload)) + payload
    sock.sendall(frame)

    len_prefix = sock.recv(4)
    if len(len_prefix) < 4:
        raise RuntimeError("Failed to read response length prefix")
    resp_len = struct.unpack(">i", len_prefix)[0]

    received = bytearray()
    while len(received) < resp_len:
        chunk = sock.recv(resp_len - len(received))
        if not chunk:
            raise RuntimeError("Unexpected EOF while reading frame body")
        received.extend(chunk)
    return bytes(received)

def test_api_versions(sock: socket.socket):
    print("Testing ApiVersions (ApiKey 18)...", end=" ")
    corr_id = 1001
    # Header: api_key(2), api_version(2), corr_id(4), client_id
    req = struct.pack(">hhi", 18, 0, corr_id) + put_string("py-kafka-client")
    resp = send_and_recv(sock, req)

    resp_corr_id, error_code, num_keys = struct.unpack_from(">ihi", resp, 0)
    assert resp_corr_id == corr_id, f"Correlation ID mismatch: got {resp_corr_id}, expected {corr_id}"
    assert error_code == 0, f"Error code non-zero: {error_code}"
    assert num_keys >= 4, f"Expected at least 4 API keys, got {num_keys}"

    offset = 10
    keys = []
    for _ in range(num_keys):
        k, min_v, max_v = struct.unpack_from(">hhh", resp, offset)
        keys.append((k, min_v, max_v))
        offset += 6
    print(f"PASSED (Found {num_keys} supported APIs: {keys})")

def test_metadata(sock: socket.socket, topic: str):
    print(f"Testing Metadata (ApiKey 3) for topic '{topic}'...", end=" ")
    corr_id = 1002
    req = struct.pack(">hhi", 3, 0, corr_id) + put_string("py-kafka-client")
    # Topics array: count=1, topic
    req += struct.pack(">i", 1) + put_string(topic)

    resp = send_and_recv(sock, req)
    offset = 0
    resp_corr_id = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert resp_corr_id == corr_id, f"Correlation ID mismatch: got {resp_corr_id}, expected {corr_id}"

    brokers_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert brokers_count >= 1, "Expected at least 1 broker"
    node_id = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    host, offset = read_string(resp, offset)
    port = struct.unpack_from(">i", resp, offset)[0]
    offset += 4

    topics_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert topics_count >= 1, "Expected at least 1 topic"
    t_err = struct.unpack_from(">h", resp, offset)[0]
    offset += 2
    assert t_err == 0, f"Topic error code non-zero: {t_err}"
    t_name, offset = read_string(resp, offset)
    assert t_name == topic, f"Topic name mismatch: {t_name} vs {topic}"

    parts_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert parts_count >= 1, "Expected at least 1 partition"
    p_err, p_idx, leader_id = struct.unpack_from(">hii", resp, offset)
    assert p_err == 0, f"Partition error code non-zero: {p_err}"
    assert p_idx == 0, f"Expected partition 0, got {p_idx}"

    print(f"PASSED (Broker {node_id} @ {host}:{port}, Topic: {t_name}, Partition 0 Leader: {leader_id})")

def test_produce(sock: socket.socket, topic: str, message: str) -> int:
    print(f"Testing Produce (ApiKey 0) message '{message}' to '{topic}'...", end=" ")
    corr_id = 1003
    payload = message.encode("utf-8")
    record_set = wrap_kafka_message(0, payload)

    req = struct.pack(">hhi", 0, 0, corr_id) + put_string("py-kafka-client")
    req += struct.pack(">hi", 1, 1000) # acks=1, timeout=1000ms
    req += struct.pack(">i", 1) + put_string(topic) # 1 topic
    req += struct.pack(">iii", 1, 0, len(record_set)) + record_set # 1 part, part 0, size, records

    resp = send_and_recv(sock, req)
    offset = 0
    resp_corr_id = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert resp_corr_id == corr_id, f"Correlation ID mismatch: got {resp_corr_id}"

    t_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert t_count == 1, f"Expected 1 topic response, got {t_count}"
    t_name, offset = read_string(resp, offset)
    assert t_name == topic

    p_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert p_count == 1
    p_idx, p_err, base_offset = struct.unpack_from(">ihq", resp, offset)
    assert p_idx == 0
    assert p_err == 0, f"Produce partition error: {p_err}"

    print(f"PASSED (Base Offset: {base_offset})")
    return base_offset

def test_fetch(sock: socket.socket, topic: str, expected_message: str):
    print(f"Testing Fetch (ApiKey 1) from '{topic}'...", end=" ")
    corr_id = 1004
    req = struct.pack(">hhi", 1, 0, corr_id) + put_string("py-kafka-client")
    req += struct.pack(">iii", -1, 500, 1) # replica_id=-1, max_wait=500ms, min_bytes=1
    req += struct.pack(">i", 1) + put_string(topic) # 1 topic
    req += struct.pack(">iiqi", 1, 0, 0, 65536) # 1 part, part 0, fetch_offset 0, max_bytes 64k

    resp = send_and_recv(sock, req)
    offset = 0
    resp_corr_id = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert resp_corr_id == corr_id, f"Correlation ID mismatch: got {resp_corr_id}"

    t_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert t_count == 1
    t_name, offset = read_string(resp, offset)
    assert t_name == topic

    p_count = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert p_count == 1
    p_idx, p_err, hw = struct.unpack_from(">ihq", resp, offset)
    offset += 14
    assert p_idx == 0
    assert p_err == 0, f"Fetch partition error: {p_err}"
    assert hw >= 1, f"Expected high_watermark >= 1, got {hw}"

    records_len = struct.unpack_from(">i", resp, offset)[0]
    offset += 4
    assert records_len > 0, "Expected non-empty records buffer"
    records_data = resp[offset : offset + records_len]
    assert expected_message.encode("utf-8") in records_data, f"Expected message '{expected_message}' not found in records"

    print(f"PASSED (High Watermark: {hw}, Records Bytes: {records_len}, Verified payload presence)")

def main():
    parser = argparse.ArgumentParser(description="Test Kafka Wire Protocol listener")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=9093, help="Broker Kafka wire port (default 9093)")
    parser.add_argument("--topic", default="aeromq-wire-topic", help="Topic name")
    args = parser.parse_args()

    print(f"Connecting to AeroMQ Kafka Wire listener at {args.host}:{args.port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((args.host, args.port))
    except Exception as e:
        print(f"FAILED to connect to {args.host}:{args.port}: {e}")
        sys.exit(1)

    try:
        test_api_versions(sock)
        test_metadata(sock, args.topic)
        msg = f"hello-aeromq-kafka-{int(time.time())}"
        test_produce(sock, args.topic, msg)
        test_fetch(sock, args.topic, msg)
        print("\nAll Kafka wire protocol tests passed successfully!")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
