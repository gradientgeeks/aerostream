"""Low-level Kafka wire protocol codec and socket helpers.

Supports constructing and parsing Kafka frames for precision testing of wire
semantics (ApiKey 0 Produce, 1 Fetch, 3 Metadata, 8 OffsetCommit, 9 OffsetFetch,
10 FindCoordinator, 11 JoinGroup, 12 Heartbeat, 13 LeaveGroup, 14 SyncGroup,
17 SaslHandshake, 18 ApiVersions, 22 InitProducerId, 24 AddPartitionsToTxn,
26 EndTxn, 36 SaslAuthenticate).
"""

from __future__ import annotations

import io
import socket
import struct
import time
import zlib
from typing import Any, Dict, List, Optional, Tuple


# --- Varint / Varlong helpers (Kafka RecordBatch format) ---

def encode_varint(val: int) -> bytes:
    """Zigzag varint encoding (32-bit)."""
    raw = (val << 1) ^ (val >> 31)
    raw &= 0xFFFFFFFF
    out = bytearray()
    while raw >= 0x80:
        out.append((raw & 0x7F) | 0x80)
        raw >>= 7
    out.append(raw & 0x7F)
    return bytes(out)


def decode_varint(stream: io.BytesIO) -> int:
    """Zigzag varint decoding (32-bit)."""
    res = 0
    shift = 0
    while True:
        b = stream.read(1)
        if not b:
            raise EOFError("Unexpected EOF reading varint")
        val = b[0]
        res |= (val & 0x7F) << shift
        if not (val & 0x80):
            break
        shift += 7
    return (res >> 1) ^ (-(res & 1))


def encode_varlong(val: int) -> bytes:
    """Zigzag varlong encoding (64-bit)."""
    raw = (val << 1) ^ (val >> 63)
    raw &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while raw >= 0x80:
        out.append((raw & 0x7F) | 0x80)
        raw >>= 7
    out.append(raw & 0x7F)
    return bytes(out)


def decode_varlong(stream: io.BytesIO) -> int:
    """Zigzag varlong decoding (64-bit)."""
    res = 0
    shift = 0
    while True:
        b = stream.read(1)
        if not b:
            raise EOFError("Unexpected EOF reading varlong")
        val = b[0]
        res |= (val & 0x7F) << shift
        if not (val & 0x80):
            break
        shift += 7
    return (res >> 1) ^ (-(res & 1))


# --- Basic type serialization ---

def put_string(s: Optional[str]) -> bytes:
    if s is None:
        return struct.pack(">h", -1)
    b = s.encode("utf-8")
    return struct.pack(">h", len(b)) + b


def read_string(data: bytes, offset: int) -> Tuple[Optional[str], int]:
    str_len = struct.unpack_from(">h", data, offset)[0]
    offset += 2
    if str_len < 0:
        return None, offset
    val = data[offset : offset + str_len].decode("utf-8")
    offset += str_len
    return val, offset


def put_bytes(b: Optional[bytes]) -> bytes:
    if b is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(b)) + b


def read_bytes(data: bytes, offset: int) -> Tuple[Optional[bytes], int]:
    b_len = struct.unpack_from(">i", data, offset)[0]
    offset += 4
    if b_len < 0:
        return None, offset
    val = data[offset : offset + b_len]
    offset += b_len
    return val, offset


# --- CRC32C Table (Castagnoli 0x82F63B78) for RecordBatch ---

CRC32C_TABLE = []
for i in range(256):
    curr = i
    for _ in range(8):
        if curr & 1:
            curr = (curr >> 1) ^ 0x82F63B78
        else:
            curr >>= 1
    CRC32C_TABLE.append(curr)


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc = CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFF) & 0xFFFFFFFF


# --- Kafka Message & Record Batch Encoders ---

def wrap_kafka_v0_message(offset: int, payload: bytes, key: Optional[bytes] = None) -> bytes:
    """Kafka MessageSet v0 format."""
    key_bytes = key if key is not None else b""
    key_len = len(key_bytes) if key is not None else -1
    inner = bytearray(struct.pack(">bbii", 0, 0, key_len, len(payload)))
    if key is not None:
        inner.extend(key_bytes)
    inner.extend(payload)
    crc = zlib.crc32(inner) & 0xFFFFFFFF
    msg_size = 4 + len(inner)
    return struct.pack(">qiI", offset, msg_size, crc) + bytes(inner)


def wrap_kafka_v2_record_batch(
    base_offset: int,
    records: List[Tuple[Optional[bytes], Optional[bytes], List[Tuple[str, bytes]]]],
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
    is_transactional: bool = False,
    is_control: bool = False,
) -> bytes:
    """Builds a Kafka Magic 2 RecordBatch with support for headers, transactions, and control batches."""
    recs_buf = bytearray()
    now_ms = int(time.time() * 1000)

    for idx, (key, value, headers) in enumerate(records):
        rec_inner = bytearray()
        # attributes
        rec_inner.append(0)
        # timestamp delta
        rec_inner.extend(encode_varlong(0))
        # offset delta
        rec_inner.extend(encode_varint(idx))
        # key
        if key is None:
            rec_inner.extend(encode_varint(-1))
        else:
            rec_inner.extend(encode_varint(len(key)))
            rec_inner.extend(key)
        # value
        if value is None:
            rec_inner.extend(encode_varint(-1))
        else:
            rec_inner.extend(encode_varint(len(value)))
            rec_inner.extend(value)
        # headers count
        rec_inner.extend(encode_varint(len(headers)))
        for hk, hv in headers:
            hk_bytes = hk.encode("utf-8")
            rec_inner.extend(encode_varint(len(hk_bytes)))
            rec_inner.extend(hk_bytes)
            rec_inner.extend(encode_varint(len(hv)))
            rec_inner.extend(hv)

        recs_buf.extend(encode_varint(len(rec_inner)))
        recs_buf.extend(rec_inner)

    batch_len = 49 + len(recs_buf)
    leader_epoch = 0
    magic = 2

    # attributes: bit 4 = transactional (0x0010), bit 5 = control (0x0020)
    attributes = 0
    if is_transactional:
        attributes |= 0x0010
    if is_control:
        attributes |= 0x0020

    last_offset_delta = max(0, len(records) - 1)

    batch_header = struct.pack(
        ">qiib",
        base_offset,
        batch_len,
        leader_epoch,
        magic,
    )
    # 4 bytes for CRC at offset 17
    body_to_crc = struct.pack(
        ">hiqqqhi",
        attributes,
        last_offset_delta,
        now_ms,
        now_ms,
        producer_id,
        producer_epoch,
        base_sequence,
    ) + struct.pack(">i", len(records)) + bytes(recs_buf)

    computed_crc = crc32c(body_to_crc)
    return batch_header + struct.pack(">I", computed_crc) + body_to_crc


# --- Socket Network Transport ---

class KafkaWireClient:
    """Synchronous socket client for raw Kafka wire frames."""

    def __init__(self, host: str, port: int, timeout: float = 10.0, ssl_context: Any = None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.sock: Optional[socket.socket] = None
        self._corr_id = 1000

    def connect(self):
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.settimeout(self.timeout)
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        raw_sock.connect((self.host, self.port))
        if self.ssl_context:
            self.sock = self.ssl_context.wrap_socket(raw_sock, server_hostname=self.host)
        else:
            self.sock = raw_sock

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def next_corr_id(self) -> int:
        self._corr_id += 1
        return self._corr_id

    def send_and_recv(self, payload: bytes) -> bytes:
        if not self.sock:
            raise RuntimeError("KafkaWireClient not connected")
        frame = struct.pack(">i", len(payload)) + payload
        self.sock.sendall(frame)

        len_buf = self._recv_exact(4)
        resp_len = struct.unpack(">i", len_buf)[0]
        if resp_len < 0 or resp_len > 64 * 1024 * 1024:
            raise RuntimeError(f"Invalid frame response length: {resp_len}")
        return self._recv_exact(resp_len)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise EOFError("Socket closed prematurely during read")
            buf.extend(chunk)
        return bytes(buf)

    # --- High-level Kafka API operations ---

    def api_versions(self, version: int = 0) -> List[Tuple[int, int, int]]:
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 18, version, cid) + put_string("wire-client")
        resp = self.send_and_recv(req)
        rcid, err_code = struct.unpack_from(">ih", resp, 0)
        assert rcid == cid, f"Correlation ID mismatch: {rcid} != {cid}"
        if err_code != 0:
            raise RuntimeError(f"ApiVersions returned error code: {err_code}")
        num_apis = struct.unpack_from(">i", resp, 6)[0]
        apis = []
        off = 10
        for _ in range(num_apis):
            k, min_v, max_v = struct.unpack_from(">hhh", resp, off)
            apis.append((k, min_v, max_v))
            off += 6
        return apis

    def metadata(self, topics: List[str]) -> Dict[str, Any]:
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 3, 0, cid) + put_string("wire-client")
        req += struct.pack(">i", len(topics))
        for t in topics:
            req += put_string(t)
        resp = self.send_and_recv(req)
        rcid = struct.unpack_from(">i", resp, 0)[0]
        assert rcid == cid
        off = 4
        num_brokers = struct.unpack_from(">i", resp, off)[0]
        off += 4
        brokers = []
        for _ in range(num_brokers):
            bid, off = struct.unpack_from(">i", resp, off)[0], off + 4
            host, off = read_string(resp, off)
            bport, off = struct.unpack_from(">i", resp, off)[0], off + 4
            brokers.append({"node_id": bid, "host": host, "port": bport})

        num_topics = struct.unpack_from(">i", resp, off)[0]
        off += 4
        topic_info = {}
        for _ in range(num_topics):
            terr, off = struct.unpack_from(">h", resp, off)[0], off + 2
            tname, off = read_string(resp, off)
            num_parts, off = struct.unpack_from(">i", resp, off)[0], off + 4
            parts = []
            for _ in range(num_parts):
                perr, pidx, leader = struct.unpack_from(">hii", resp, off)
                off += 10
                num_reps, off = struct.unpack_from(">i", resp, off)[0], off + 4
                reps = struct.unpack_from(f">{num_reps}i", resp, off) if num_reps > 0 else ()
                off += 4 * num_reps
                num_isr, off = struct.unpack_from(">i", resp, off)[0], off + 4
                isr = struct.unpack_from(f">{num_isr}i", resp, off) if num_isr > 0 else ()
                off += 4 * num_isr
                parts.append({"partition": pidx, "error": perr, "leader": leader})
            topic_info[tname] = {"error": terr, "partitions": parts}
        return {"brokers": brokers, "topics": topic_info}

    def produce(
        self,
        topic: str,
        partition: int,
        records_data: bytes,
        acks: int = 1,
        timeout_ms: int = 2000,
        transactional_id: Optional[str] = None,
    ) -> int:
        cid = self.next_corr_id()
        # Produce v3 carries transactional_id
        if transactional_id is not None:
            req = struct.pack(">hhi", 0, 3, cid) + put_string("wire-client")
            req += put_string(transactional_id)
        else:
            req = struct.pack(">hhi", 0, 0, cid) + put_string("wire-client")
        req += struct.pack(">hi", acks, timeout_ms)
        req += struct.pack(">i", 1) + put_string(topic)
        req += struct.pack(">iii", 1, partition, len(records_data)) + records_data

        resp = self.send_and_recv(req)
        rcid = struct.unpack_from(">i", resp, 0)[0]
        assert rcid == cid
        off = 4
        num_topics, off = struct.unpack_from(">i", resp, off)[0], off + 4
        _, off = read_string(resp, off)
        num_parts, off = struct.unpack_from(">i", resp, off)[0], off + 4
        pidx, perr, base_offset = struct.unpack_from(">ihq", resp, off)
        if perr != 0:
            raise RuntimeError(f"Produce returned partition error code {perr}")
        return base_offset

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_bytes: int = 65536,
        max_wait_ms: int = 500,
        min_bytes: int = 1,
        isolation_level: int = 0,
    ) -> Dict[str, Any]:
        """Fetch request. Uses Fetch v4 to support isolation_level (0=uncommitted, 1=committed)."""
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 1, 4, cid) + put_string("wire-client")
        req += struct.pack(">iiii", -1, max_wait_ms, min_bytes, max_bytes)
        req += struct.pack(">b", isolation_level)
        req += struct.pack(">i", 1) + put_string(topic)
        req += struct.pack(">iiqi", 1, partition, offset, max_bytes)

        t0 = time.perf_counter()
        resp = self.send_and_recv(req)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        rcid = struct.unpack_from(">i", resp, 0)[0]
        assert rcid == cid
        off = 4
        throttle_time, off = struct.unpack_from(">i", resp, off)[0], off + 4
        num_topics, off = struct.unpack_from(">i", resp, off)[0], off + 4
        tname, off = read_string(resp, off)
        num_parts, off = struct.unpack_from(">i", resp, off)[0], off + 4
        pidx, perr, hw, lso = struct.unpack_from(">ihqq", resp, off)
        off += 22

        # Aborted transactions array
        num_aborted, off = struct.unpack_from(">i", resp, off)[0], off + 4
        aborted = []
        for _ in range(max(0, num_aborted)):
            apid, afirst_off = struct.unpack_from(">qq", resp, off)
            aborted.append({"producer_id": apid, "first_offset": afirst_off})
            off += 16

        records_len, off = struct.unpack_from(">i", resp, off)[0], off + 4
        records_raw = resp[off : off + records_len] if records_len > 0 else b""

        return {
            "elapsed_ms": elapsed_ms,
            "error_code": perr,
            "high_watermark": hw,
            "last_stable_offset": lso,
            "aborted": aborted,
            "records_len": records_len,
            "records_data": records_raw,
        }

    # --- Transaction APIs ---

    def init_producer_id(self, transactional_id: Optional[str] = None, timeout_ms: int = 60000) -> Tuple[int, int]:
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 22, 0, cid) + put_string("wire-client")
        req += put_string(transactional_id)
        req += struct.pack(">i", timeout_ms)

        resp = self.send_and_recv(req)
        rcid, throttle, err_code, pid, epoch = struct.unpack_from(">iihqh", resp, 0)
        assert rcid == cid
        if err_code != 0:
            raise RuntimeError(f"InitProducerId returned error code {err_code}")
        return pid, epoch

    def add_partitions_to_txn(self, transactional_id: str, producer_id: int, producer_epoch: int, topic: str, partitions: List[int]):
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 24, 0, cid) + put_string("wire-client")
        req += put_string(transactional_id)
        req += struct.pack(">qh", producer_id, producer_epoch)
        req += struct.pack(">i", 1) + put_string(topic)
        req += struct.pack(">i", len(partitions))
        for p in partitions:
            req += struct.pack(">i", p)

        resp = self.send_and_recv(req)
        rcid, throttle, num_topics = struct.unpack_from(">iii", resp, 0)
        assert rcid == cid

    def end_txn(self, transactional_id: str, producer_id: int, producer_epoch: int, commit: bool):
        cid = self.next_corr_id()
        req = struct.pack(">hhi", 26, 0, cid) + put_string("wire-client")
        req += put_string(transactional_id)
        req += struct.pack(">qhb", producer_id, producer_epoch, 1 if commit else 0)

        resp = self.send_and_recv(req)
        rcid, throttle, err_code = struct.unpack_from(">iih", resp, 0)
        assert rcid == cid
        if err_code != 0:
            raise RuntimeError(f"EndTxn returned error code {err_code}")
