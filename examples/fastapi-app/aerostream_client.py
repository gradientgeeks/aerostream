"""
AeroStream Python Client
Provides high-performance Kafka wire-protocol binary framing (TCP) and HTTP REST communication.
"""

import json
import socket
import struct
import time
import zlib
from typing import Any, Dict, List, Optional
import httpx


class AeroStreamClient:
    def __init__(
        self,
        kafka_host: str = "127.0.0.1",
        kafka_port: int = 9093,
        http_url: str = "http://127.0.0.1:9001",
    ):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.http_url = http_url.rstrip("/")

    # -------------------------------------------------------------------------
    # Kafka Wire Protocol Helpers
    # -------------------------------------------------------------------------
    def _send_recv_kafka(self, payload: bytes) -> bytes:
        frame = struct.pack(">i", len(payload)) + payload
        with socket.create_connection((self.kafka_host, self.kafka_port), timeout=5.0) as sock:
            sock.sendall(frame)
            len_bytes = sock.recv(4)
            if len(len_bytes) < 4:
                raise ConnectionError("Failed to read Kafka length prefix")
            resp_len = struct.unpack(">i", len_bytes)[0]
            received = bytearray()
            while len(received) < resp_len:
                chunk = sock.recv(resp_len - len(received))
                if not chunk:
                    break
                received.extend(chunk)
            return bytes(received)

    @staticmethod
    def _put_string(s: Optional[str]) -> bytes:
        if s is None:
            return struct.pack(">h", -1)
        b = s.encode("utf-8")
        return struct.pack(">h", len(b)) + b

    @staticmethod
    def _read_string(data: bytes, offset: int):
        str_len = struct.unpack_from(">h", data, offset)[0]
        offset += 2
        if str_len < 0:
            return None, offset
        val = data[offset : offset + str_len].decode("utf-8", errors="replace")
        offset += str_len
        return val, offset

    def produce_kafka(self, topic: str, partition: int, message: str) -> int:
        """Publishes a record via binary Kafka Wire Protocol (ApiKey 0)."""
        msg_bytes = message.encode("utf-8")
        # Format inner message: magic=0, attributes=0, key_len=-1, value_len, value
        inner = struct.pack(">bbii", 0, 0, -1, len(msg_bytes)) + msg_bytes
        crc = zlib.crc32(inner) & 0xFFFFFFFF
        record = struct.pack(">qiI", 0, 4 + len(inner), crc) + inner

        corr_id = 1003
        req = struct.pack(">hhi", 0, 0, corr_id) + self._put_string("fastapi-app")
        req += struct.pack(">hi", 1, 3000)  # acks=1, timeout_ms=3000
        req += struct.pack(">i", 1) + self._put_string(topic)  # 1 topic
        req += struct.pack(">iii", 1, partition, len(record)) + record  # 1 partition

        resp = self._send_recv_kafka(req)
        offset = 0
        resp_corr_id = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        t_count = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        t_name, offset = self._read_string(resp, offset)
        p_count = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        p_idx, p_err, base_offset = struct.unpack_from(">ihq", resp, offset)
        if p_err != 0:
            raise RuntimeError(f"Kafka produce error code: {p_err}")
        return base_offset

    def fetch_kafka(self, topic: str, partition: int = 0, fetch_offset: int = 0, max_bytes: int = 65536) -> List[bytes]:
        """Fetches raw message records via binary Kafka Wire Protocol (ApiKey 1)."""
        corr_id = 1004
        req = struct.pack(">hhi", 1, 0, corr_id) + self._put_string("fastapi-app")
        req += struct.pack(">iii", -1, 500, 1)  # replica_id=-1, max_wait=500ms, min_bytes=1
        req += struct.pack(">i", 1) + self._put_string(topic)
        req += struct.pack(">iiqi", 1, partition, fetch_offset, max_bytes)

        resp = self._send_recv_kafka(req)
        offset = 0
        resp_corr_id = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        t_count = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        t_name, offset = self._read_string(resp, offset)
        p_count = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        p_idx, p_err, hw = struct.unpack_from(">ihq", resp, offset)
        offset += 14
        if p_err != 0:
            raise RuntimeError(f"Kafka fetch error code: {p_err}")

        records_len = struct.unpack_from(">i", resp, offset)[0]
        offset += 4
        if records_len <= 0:
            return []

        records_data = resp[offset : offset + records_len]
        # Parse standard Kafka MessageSet records
        records = []
        rec_offset = 0
        while rec_offset + 12 <= len(records_data):
            rec_off, msg_size = struct.unpack_from(">qi", records_data, rec_offset)
            rec_offset += 12
            if rec_offset + msg_size > len(records_data):
                break
            msg_data = records_data[rec_offset : rec_offset + msg_size]
            rec_offset += msg_size
            if len(msg_data) >= 14:
                # crc(4), magic(1), attr(1), key_len(4), key, val_len(4), val
                key_len = struct.unpack_from(">i", msg_data, 6)[0]
                idx = 10
                if key_len > 0:
                    idx += key_len
                if idx + 4 <= len(msg_data):
                    val_len = struct.unpack_from(">i", msg_data, idx)[0]
                    idx += 4
                    if val_len > 0 and idx + val_len <= len(msg_data):
                        records.append(msg_data[idx : idx + val_len])
        return records

    # -------------------------------------------------------------------------
    # HTTP REST Protocol Methods
    # -------------------------------------------------------------------------
    async def produce_http(self, topic: str, partition: int, payload: Any) -> Dict[str, Any]:
        """Publishes a record via AeroStream HTTP REST API."""
        if not isinstance(payload, str):
            payload_str = json.dumps(payload)
        else:
            payload_str = payload

        url = f"{self.http_url}/api/produce"
        data = {
            "topic": topic,
            "partition": partition,
            "payload": payload_str,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=data)
            resp.raise_for_status()
            return resp.json()

    async def fetch_http(self, topic: str, partition: int = 0, offset: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieves records via AeroStream HTTP REST API."""
        url = f"{self.http_url}/api/messages"
        params = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "limit": limit,
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("messages", [])

    async def get_cluster_status(self) -> Dict[str, Any]:
        """Queries cluster topology, brokers, and topics."""
        url = f"{self.http_url}/api/cluster"
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
