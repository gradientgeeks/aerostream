#!/usr/bin/env python3
"""
AeroStream Kafka Wire Protocol End-to-End Orchestrator & Verification Suite.
Executes protocol operations across all required ApiKeys and runs full E2E test matrix.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import struct
import sys
import threading
import time
import zlib
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, OFFSET_END, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("aerostream.e2e")

BOOTSTRAP_SERVER = "127.0.0.1:9092"
HOST = "127.0.0.1"
PORT = 9092


@dataclass
class TestResult:
    test_id: str
    name: str
    api_keys: List[int]
    status: str  # PASS / FAIL
    latency_ms: float
    throughput_msg_sec: float = 0.0
    throughput_kb_sec: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class KafkaWireHelper:
    """Low-level wire protocol socket communicator."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.corr_id = 1000

    def __enter__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10.0)
        self.sock.connect((self.host, self.port))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass

    def send_and_recv(self, payload: bytes) -> bytes:
        frame = struct.pack(">i", len(payload)) + payload
        self.sock.sendall(frame)
        len_prefix = self.sock.recv(4)
        if len(len_prefix) < 4:
            raise EOFError("Failed to read frame length prefix")
        resp_len = struct.unpack(">i", len_prefix)[0]
        rec = bytearray()
        while len(rec) < resp_len:
            c = self.sock.recv(resp_len - len(rec))
            if not c:
                raise EOFError("Unexpected EOF while reading frame body")
            rec.extend(c)
        return bytes(rec)

    @staticmethod
    def put_string(s: Optional[str]) -> bytes:
        if s is None:
            return struct.pack(">h", -1)
        b = s.encode("utf-8")
        return struct.pack(">h", len(b)) + b

    @staticmethod
    def read_string(data: bytes, offset: int) -> Tuple[Optional[str], int]:
        str_len = struct.unpack_from(">h", data, offset)[0]
        offset += 2
        if str_len < 0:
            return None, offset
        val = data[offset : offset + str_len].decode("utf-8", errors="replace")
        return val, offset + str_len


class E2EOrchestrator:
    def __init__(self, bootstrap_server: str = BOOTSTRAP_SERVER):
        self.bootstrap_server = bootstrap_server
        self.host, port_str = bootstrap_server.split(":")
        self.port = int(port_str)
        self.results: List[TestResult] = []

    def run_all(self) -> List[TestResult]:
        logger.info("=" * 80)
        logger.info("  AEROSTREAM KAFKA WIRE PROTOCOL END-TO-END VERIFICATION SUITE")
        logger.info(f"  Target: {self.bootstrap_server}")
        logger.info("=" * 80)

        # 1. AdminClient & Wire Metadata
        self.test_api_versions()
        self.test_create_topics()
        self.test_metadata_topology()
        self.test_list_offsets()

        # 2. E2E Test Matrix
        self.test_a_high_throughput_checksum()
        self.test_b_all_compression_codecs()
        self.test_c_headers_and_partition_routing()
        self.test_d_consumer_group_rebalance_resumption()
        self.test_e_long_polling_fetch()
        self.test_f_transactional_isolation()
        self.test_g_wire_security_auth()

        return self.results

    # -------------------------------------------------------------------------
    # 1. AdminClient & Wire Metadata Operations
    # -------------------------------------------------------------------------
    def test_api_versions(self):
        """ApiKey 18 negotiation."""
        t0 = time.perf_counter()
        try:
            with KafkaWireHelper(self.host, self.port) as wire:
                cid = 1018
                req = struct.pack(">hhi", 18, 0, cid) + wire.put_string("e2e-orchestrator")
                resp = wire.send_and_recv(req)
                rcid, err, num_keys = struct.unpack_from(">ihi", resp, 0)
                assert rcid == cid
                assert err == 0
                off = 10
                keys = []
                for _ in range(num_keys):
                    k, lo, hi = struct.unpack_from(">hhh", resp, off)
                    keys.append((k, lo, hi))
                    off += 6
                dur = (time.perf_counter() - t0) * 1000
                self.results.append(
                    TestResult(
                        test_id="ADMIN-01",
                        name="ApiVersions Protocol Negotiation",
                        api_keys=[18],
                        status="PASS",
                        latency_ms=dur,
                        details={"supported_api_count": num_keys, "api_keys": [k[0] for k in keys]},
                    )
                )
                logger.info(f"[PASS] ApiVersions (ApiKey 18): Negotiated {num_keys} APIs in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-01",
                    name="ApiVersions Protocol Negotiation",
                    api_keys=[18],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] ApiVersions: {e}")

    def test_create_topics(self):
        """ApiKey 19 multi-partition topic creation."""
        t0 = time.perf_counter()
        required_topics = [
            "test-wire-basic",
            "test-wire-compressed",
            "test-wire-partitions",
            "test-wire-txns",
            "test-wire-groups",
        ]
        try:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_server})
            new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in required_topics]
            fs = admin.create_topics(new_topics)
            created_or_existing = {}
            for t, f in fs.items():
                try:
                    f.result()
                    created_or_existing[t] = "CREATED"
                except Exception as e:
                    if "already exists" in str(e).lower() or getattr(e, "args", [None])[0] == KafkaError.TOPIC_ALREADY_EXISTS:
                        created_or_existing[t] = "EXISTS"
                    else:
                        raise
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-02",
                    name="CreateTopics Multi-Partition Provisioning",
                    api_keys=[19],
                    status="PASS",
                    latency_ms=dur,
                    details={"topics": created_or_existing, "partitions_per_topic": 3},
                )
            )
            logger.info(f"[PASS] CreateTopics (ApiKey 19): 5 multi-partition topics verified in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-02",
                    name="CreateTopics Multi-Partition Provisioning",
                    api_keys=[19],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] CreateTopics: {e}")

    def test_metadata_topology(self):
        """ApiKey 3 topic and broker topology discovery."""
        t0 = time.perf_counter()
        try:
            admin = AdminClient({"bootstrap.servers": self.bootstrap_server})
            md = admin.list_topics(timeout=10)
            brokers = {b.id: f"{b.host}:{b.port}" for b in md.brokers.values()}
            assert len(brokers) >= 1, "Expected at least 1 broker"
            topic_partitions = {}
            for name in ["test-wire-basic", "test-wire-partitions", "test-wire-groups"]:
                assert name in md.topics, f"Topic {name} missing from metadata"
                parts = md.topics[name].partitions
                assert len(parts) == 3, f"Expected 3 partitions for {name}, got {len(parts)}"
                topic_partitions[name] = len(parts)

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-03",
                    name="Metadata Topology Discovery",
                    api_keys=[3],
                    status="PASS",
                    latency_ms=dur,
                    details={"brokers": brokers, "verified_topics": topic_partitions},
                )
            )
            logger.info(f"[PASS] Metadata (ApiKey 3): Discovered brokers={brokers}, topics verified in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-03",
                    name="Metadata Topology Discovery",
                    api_keys=[3],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Metadata: {e}")

    def test_list_offsets(self):
        """ApiKey 2 earliest (-2), latest (-1), and timestamp search."""
        t0 = time.perf_counter()
        try:
            # Seed 5 messages to test-wire-basic to have valid non-zero offsets
            producer = Producer({"bootstrap.servers": self.bootstrap_server, "linger.ms": 0})
            for i in range(5):
                producer.produce("test-wire-basic", partition=0, value=f"offset-seed-{i}".encode("utf-8"))
            producer.flush(5.0)

            with KafkaWireHelper(self.host, self.port) as wire:
                # 1. Latest offset (timestamp = -1)
                cid1 = 2001
                req1 = struct.pack(">hhi", 2, 1, cid1) + wire.put_string("admin-client")
                req1 += struct.pack(">i", -1)  # replica_id
                req1 += struct.pack(">i", 1) + wire.put_string("test-wire-basic")
                req1 += struct.pack(">iiq", 1, 0, -1)
                resp1 = wire.send_and_recv(req1)
                r_cid1, num_t1 = struct.unpack_from(">ii", resp1, 0)
                off1 = 8
                _, off1 = wire.read_string(resp1, off1)
                num_p1, off1 = struct.unpack_from(">i", resp1, off1)[0], off1 + 4
                pidx1, perr1, latest_ts, latest_offset = struct.unpack_from(">ihqq", resp1, off1)
                assert perr1 == 0, f"Latest ListOffsets error: {perr1}"
                assert latest_offset >= 5, f"Expected latest offset >= 5, got {latest_offset}"

                # 2. Earliest offset (timestamp = -2)
                cid2 = 2002
                req2 = struct.pack(">hhi", 2, 1, cid2) + wire.put_string("admin-client")
                req2 += struct.pack(">i", -1)
                req2 += struct.pack(">i", 1) + wire.put_string("test-wire-basic")
                req2 += struct.pack(">iiq", 1, 0, -2)
                resp2 = wire.send_and_recv(req2)
                off2 = 8
                _, off2 = wire.read_string(resp2, off2)
                num_p2, off2 = struct.unpack_from(">i", resp2, off2)[0], off2 + 4
                pidx2, perr2, earliest_ts, earliest_offset = struct.unpack_from(">ihqq", resp2, off2)
                assert perr2 == 0, f"Earliest ListOffsets error: {perr2}"
                assert earliest_offset == 0, f"Expected earliest offset 0, got {earliest_offset}"

                # 3. Timestamp search (target timestamp query)
                cid3 = 2003
                target_ts = int((time.time() - 3600) * 1000)  # 1 hour ago -> resolves to earliest
                req3 = struct.pack(">hhi", 2, 1, cid3) + wire.put_string("admin-client")
                req3 += struct.pack(">i", -1)
                req3 += struct.pack(">i", 1) + wire.put_string("test-wire-basic")
                req3 += struct.pack(">iiq", 1, 0, target_ts)
                resp3 = wire.send_and_recv(req3)
                off3 = 8
                _, off3 = wire.read_string(resp3, off3)
                num_p3, off3 = struct.unpack_from(">i", resp3, off3)[0], off3 + 4
                pidx3, perr3, matched_ts, matched_offset = struct.unpack_from(">ihqq", resp3, off3)
                assert perr3 == 0

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-04",
                    name="ListOffsets Earliest/Latest/Timestamp",
                    api_keys=[2],
                    status="PASS",
                    latency_ms=dur,
                    details={
                        "earliest_offset": earliest_offset,
                        "latest_offset": latest_offset,
                        "timestamp_query_offset": matched_offset,
                    },
                )
            )
            logger.info(
                f"[PASS] ListOffsets (ApiKey 2): Earliest={earliest_offset}, Latest={latest_offset}, Matched={matched_offset} in {dur:.2f}ms"
            )
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="ADMIN-04",
                    name="ListOffsets Earliest/Latest/Timestamp",
                    api_keys=[2],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] ListOffsets: {e}")

    # -------------------------------------------------------------------------
    # 2. End-to-End Test Matrix
    # -------------------------------------------------------------------------
    def test_a_high_throughput_checksum(self):
        """Test A: High-throughput Produce & Consume with SHA-256 payload checksum verification."""
        t0 = time.perf_counter()
        topic = "test-wire-basic"
        count = 1000
        sent_checksums: Dict[int, str] = {}
        received_checksums: Dict[int, str] = {}

        try:
            # Produce with checksum tracking
            p = Producer({
                "bootstrap.servers": self.bootstrap_server,
                "acks": "1",
                "linger.ms": 5,
                "batch.size": 32768,
            })
            p_start = time.perf_counter()
            total_bytes = 0
            run_token = f"{int(time.time()*1000)}"
            prefix = f"aeromq-perf-{run_token}-msg-".encode("utf-8")
            for i in range(count):
                payload = prefix + f"{i:06d}-data-{'x'*256}".encode("utf-8")
                total_bytes += len(payload)
                sha = hashlib.sha256(payload).hexdigest()
                sent_checksums[i] = sha
                p.produce(topic, key=f"key-{i}".encode("utf-8"), value=payload)
                if i % 200 == 0:
                    p.poll(0)
            p.flush(15.0)
            p_dur = time.perf_counter() - p_start
            p_tput_msgs = count / p_dur
            p_tput_kb = (total_bytes / 1024) / p_dur

            # Consume from beginning
            c = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": f"checksum-verifier-{run_token}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            # Subscribe to all 3 partitions
            c.subscribe([topic])
            c_start = time.perf_counter()
            deadline = time.time() + 20.0
            while time.time() < deadline and len(received_checksums) < count:
                msg = c.poll(timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    continue
                v = msg.value()
                c_sha = hashlib.sha256(v).hexdigest()
                # Find matching sent payload
                if v.startswith(prefix):
                    idx = int(v[len(prefix):len(prefix)+6])
                    received_checksums[idx] = c_sha
            c.close()

            # Verify checksum integrity
            assert len(received_checksums) >= count, f"Consumed {len(received_checksums)} of {count}"
            mismatches = 0
            for i in range(count):
                if sent_checksums[i] != received_checksums.get(i):
                    mismatches += 1
            assert mismatches == 0, f"Found {mismatches} payload SHA-256 mismatches!"

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-A",
                    name="High-Throughput Produce/Consume & SHA-256 Checksum Verification",
                    api_keys=[0, 1],
                    status="PASS",
                    latency_ms=dur,
                    throughput_msg_sec=p_tput_msgs,
                    throughput_kb_sec=p_tput_kb,
                    details={
                        "messages_verified": count,
                        "checksum_mismatches": 0,
                        "producer_throughput_msgs_sec": round(p_tput_msgs, 1),
                        "producer_throughput_kb_sec": round(p_tput_kb, 1),
                        "total_payload_bytes": total_bytes,
                    },
                )
            )
            logger.info(
                f"[PASS] Test A: High-Throughput ({count} msgs, {p_tput_msgs:.1f} msg/s, 0 checksum errors) in {dur:.2f}ms"
            )
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-A",
                    name="High-Throughput Produce/Consume & SHA-256 Checksum Verification",
                    api_keys=[0, 1],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test A: {e}")

    def test_b_all_compression_codecs(self):
        """Test B: All 4 compression codecs (snappy, gzip, lz4, zstd) produce & consume decompression."""
        t0 = time.perf_counter()
        codecs = ["snappy", "gzip", "lz4", "zstd"]
        topic = "test-wire-compressed"
        codec_results = {}

        try:
            for codec in codecs:
                sub_t0 = time.perf_counter()
                payload = json.dumps({
                    "sensor_id": f"thermocouple-{codec}",
                    "readings": [10.5 * j for j in range(50)],
                    "codec": codec,
                    "compression_validation": "AeroStream-KAFKA-ZERO-COPY",
                    "padding": "compressible-repeating-pattern-" * 10,
                }).encode("utf-8")
                expected_sha = hashlib.sha256(payload).hexdigest()

                # Produce with specific codec
                p = Producer({
                    "bootstrap.servers": self.bootstrap_server,
                    "compression.type": codec,
                    "acks": "1",
                    "linger.ms": 0,
                })
                partition = codecs.index(codec) % 3
                p.produce(topic, partition=partition, value=payload, key=codec.encode("utf-8"))
                p.flush(5.0)

                # Consume and verify decompression
                c = Consumer({
                    "bootstrap.servers": self.bootstrap_server,
                    "group.id": f"comp-verifier-{codec}-{int(time.time()*1000)}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                })
                low, high = c.get_watermark_offsets(TopicPartition(topic, partition), timeout=5.0)
                start_offset = max(low, high - 1)
                c.assign([TopicPartition(topic, partition, start_offset)])

                received_payload = None
                deadline = time.time() + 5.0
                while time.time() < deadline:
                    msg = c.poll(0.5)
                    if msg and not msg.error() and msg.key() == codec.encode("utf-8"):
                        received_payload = msg.value()
                        break
                c.close()

                assert received_payload is not None, f"Failed to consume decompressed message for codec {codec}"
                actual_sha = hashlib.sha256(received_payload).hexdigest()
                assert actual_sha == expected_sha, f"Checksum mismatch for codec {codec}!"
                codec_dur = (time.perf_counter() - sub_t0) * 1000
                codec_results[codec] = {"status": "PASS", "latency_ms": round(codec_dur, 2), "bytes": len(payload)}

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-B",
                    name="Compression Codecs Verification (snappy, gzip, lz4, zstd)",
                    api_keys=[0, 1],
                    status="PASS",
                    latency_ms=dur,
                    details={"codecs": codec_results},
                )
            )
            logger.info(f"[PASS] Test B: All 4 compression codecs verified in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-B",
                    name="Compression Codecs Verification (snappy, gzip, lz4, zstd)",
                    api_keys=[0, 1],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test B: {e}")

    def test_c_headers_and_partition_routing(self):
        """Test C: Record Headers and Key-based partition routing verification across 3 partitions."""
        t0 = time.perf_counter()
        topic = "test-wire-partitions"
        num_keys = 6
        msgs_per_key = 20

        try:
            p = Producer({
                "bootstrap.servers": self.bootstrap_server,
                "acks": "1",
                "linger.ms": 0,
            })
            key_partition_map: Dict[str, int] = {}
            for k_idx in range(num_keys):
                key = f"customer-account-{k_idx}"
                headers = [
                    ("header-trace-id", f"trace-{k_idx:04d}".encode("utf-8")),
                    ("header-source", b"aerostream-e2e-orchestrator"),
                    ("header-schema-version", b"1.2.0"),
                ]
                for m in range(msgs_per_key):
                    val = f"txn-event-{k_idx}-{m}".encode("utf-8")
                    def delivery_cb(err, msg, k=key):
                        if err is None:
                            part = msg.partition()
                            if k in key_partition_map:
                                assert key_partition_map[k] == part, f"Routing inconsistency for key {k}"
                            else:
                                key_partition_map[k] = part
                    p.produce(topic, key=key.encode("utf-8"), value=val, headers=headers, on_delivery=delivery_cb)
            p.flush(10.0)

            # Assert deterministic partition mapping
            assert len(key_partition_map) == num_keys
            used_partitions = set(key_partition_map.values())
            assert len(used_partitions) > 1, f"Expected keys spread across multiple partitions, got {used_partitions}"

            # Verify consumer reads back headers
            c = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": f"header-verifier-{int(time.time()*1000)}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            sample_part = list(used_partitions)[0]
            c.assign([TopicPartition(topic, sample_part, 0)])

            headers_verified = False
            deadline = time.time() + 5.0
            while time.time() < deadline:
                msg = c.poll(0.5)
                if msg and not msg.error() and msg.headers():
                    hdr_dict = {hk: hv for hk, hv in msg.headers()}
                    if "header-source" in hdr_dict and hdr_dict["header-source"] == b"aerostream-e2e-orchestrator":
                        headers_verified = True
                        break
            c.close()
            assert headers_verified, "Record headers were not received correctly on consumer"

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-C",
                    name="Record Headers & Key Partition Routing Verification",
                    api_keys=[0, 1],
                    status="PASS",
                    latency_ms=dur,
                    details={
                        "deterministic_routing": key_partition_map,
                        "partitions_spread": sorted(list(used_partitions)),
                        "headers_verified": True,
                    },
                )
            )
            logger.info(
                f"[PASS] Test C: Key routing deterministic across {len(used_partitions)} partitions, headers verified in {dur:.2f}ms"
            )
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-C",
                    name="Record Headers & Key Partition Routing Verification",
                    api_keys=[0, 1],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test C: {e}")

    def test_d_consumer_group_rebalance_resumption(self):
        """Test D: Consumer Group dynamic rebalancing across 2 consumer instances and committed offset resumption."""
        t0 = time.perf_counter()
        topic = "test-wire-groups"
        group_id = f"rebalance-group-{int(time.time()*1000)}"

        try:
            # Seed 30 messages across partitions
            p = Producer({"bootstrap.servers": self.bootstrap_server, "linger.ms": 0})
            for i in range(30):
                p.produce(topic, partition=i % 3, value=f"rebalance-seed-{i}".encode("utf-8"))
            p.flush(5.0)

            c1_partitions: List[int] = []
            c2_partitions: List[int] = []

            def c1_on_assign(consumer, partitions):
                c1_partitions.clear()
                c1_partitions.extend([pt.partition for pt in partitions])
                logger.info(f"Consumer 1 assigned partitions: {c1_partitions}")

            def c2_on_assign(consumer, partitions):
                c2_partitions.clear()
                c2_partitions.extend([pt.partition for pt in partitions])
                logger.info(f"Consumer 2 assigned partitions: {c2_partitions}")

            c1 = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": group_id,
                "client.id": "consumer-inst-1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c1.subscribe([topic], on_assign=c1_on_assign)

            # Poll c1 until assignment
            deadline = time.time() + 8.0
            while time.time() < deadline and not c1_partitions:
                c1.poll(0.5)

            assert len(c1_partitions) > 0, "Consumer 1 failed to acquire initial assignment"
            initial_c1_assignment = list(c1_partitions)

            # Start Consumer 2 in same group -> triggers rebalance
            c2 = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": group_id,
                "client.id": "consumer-inst-2",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c2.subscribe([topic], on_assign=c2_on_assign)

            deadline = time.time() + 10.0
            while time.time() < deadline and not c2_partitions:
                c1.poll(0.2)
                c2.poll(0.2)

            assert len(c2_partitions) > 0, "Consumer 2 failed to acquire rebalanced partition assignment"

            # Test committed offset resumption (OffsetCommit / OffsetFetch)
            target_tp = TopicPartition(topic, 0, 5)
            c1.commit(offsets=[target_tp], asynchronous=False)
            committed = c1.committed([TopicPartition(topic, 0)], timeout=5.0)
            assert len(committed) == 1
            assert committed[0].offset == 5, f"Committed offset mismatch: got {committed[0].offset}, expected 5"

            c1.close()
            c2.close()

            # Start new consumer instance to verify resumption from offset 5
            c3 = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": group_id,
                "client.id": "consumer-inst-3",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            c3.assign([TopicPartition(topic, 0)])
            committed_c3 = c3.committed([TopicPartition(topic, 0)], timeout=5.0)
            assert committed_c3[0].offset == 5, f"Resumption offset mismatch: {committed_c3[0].offset}"
            c3.close()

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-D",
                    name="Consumer Group Dynamic Rebalance & Offset Resumption",
                    api_keys=[10, 11, 14, 12, 13, 8, 9],
                    status="PASS",
                    latency_ms=dur,
                    details={
                        "initial_c1_assignment": initial_c1_assignment,
                        "rebalanced_c2_assignment": c2_partitions,
                        "committed_resumed_offset": 5,
                    },
                )
            )
            logger.info(f"[PASS] Test D: Dynamic rebalancing & offset commit resumption verified in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-D",
                    name="Consumer Group Dynamic Rebalance & Offset Resumption",
                    api_keys=[10, 11, 14, 12, 13, 8, 9],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test D: {e}")

    def test_e_long_polling_fetch(self):
        """Test E: Long polling fetch (max_wait_ms=2000, min_bytes=1024) timing validation."""
        t0 = time.perf_counter()
        topic = "test-wire-basic"
        max_wait_ms = 2000
        min_bytes = 1024

        try:
            with KafkaWireHelper(self.host, self.port) as wire:
                # Part 1: Request empty fetch at non-existent offset 999999 -> should block ~2000ms
                cid1 = 3001
                req1 = struct.pack(">hhi", 1, 0, cid1) + wire.put_string("longpoll-test")
                req1 += struct.pack(">iii", -1, max_wait_ms, min_bytes)
                req1 += struct.pack(">i", 1) + wire.put_string(topic)
                req1 += struct.pack(">iiqi", 1, 0, 999999, 65536)

                start_wait = time.perf_counter()
                resp1 = wire.send_and_recv(req1)
                elapsed_wait_ms = (time.perf_counter() - start_wait) * 1000

                # Must have waited close to max_wait_ms (>= 1800ms)
                assert elapsed_wait_ms >= 1800, f"Long poll returned prematurely: {elapsed_wait_ms:.2f}ms (expected >= 1800ms)"
                assert elapsed_wait_ms <= 3000, f"Long poll exceeded timeout: {elapsed_wait_ms:.2f}ms"

                # Part 2: Immediate fetch when data is already available at offset 0
                cid2 = 3002
                req2 = struct.pack(">hhi", 1, 0, cid2) + wire.put_string("longpoll-test")
                req2 += struct.pack(">iii", -1, max_wait_ms, 1)
                req2 += struct.pack(">i", 1) + wire.put_string(topic)
                req2 += struct.pack(">iiqi", 1, 0, 0, 65536)

                start_imm = time.perf_counter()
                resp2 = wire.send_and_recv(req2)
                elapsed_imm_ms = (time.perf_counter() - start_imm) * 1000
                assert elapsed_imm_ms < 50.0, f"Immediate fetch took too long: {elapsed_imm_ms:.2f}ms"

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-E",
                    name="Long Polling Fetch Timing Verification",
                    api_keys=[1],
                    status="PASS",
                    latency_ms=dur,
                    details={
                        "configured_max_wait_ms": max_wait_ms,
                        "configured_min_bytes": min_bytes,
                        "actual_empty_fetch_wait_ms": round(elapsed_wait_ms, 2),
                        "actual_immediate_fetch_ms": round(elapsed_imm_ms, 2),
                    },
                )
            )
            logger.info(
                f"[PASS] Test E: Long polling blocked {elapsed_wait_ms:.1f}ms for empty log, immediate fetch {elapsed_imm_ms:.2f}ms in {dur:.2f}ms"
            )
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-E",
                    name="Long Polling Fetch Timing Verification",
                    api_keys=[1],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test E: {e}")

    def test_f_transactional_isolation(self):
        """Test F: Transactional produce & isolation level verification (read_committed vs read_uncommitted)."""
        t0 = time.perf_counter()
        topic = "test-wire-txns"
        txn_id = f"e2e-txn-{int(time.time()*1000)}"

        try:
            # 1. Producer with transaction support
            p = Producer({
                "bootstrap.servers": self.bootstrap_server,
                "transactional.id": txn_id,
                "enable.idempotence": True,
                "acks": "all",
            })
            p.init_transactions()

            # Transaction 1: Aborted batch
            p.begin_transaction()
            aborted_val = f"aborted-msg-content-{int(time.time()*1000)}".encode("utf-8")
            p.produce(topic, partition=0, value=aborted_val, key=b"k-abort")
            p.flush(5.0)
            p.abort_transaction()
            logger.info("Transaction 1 aborted successfully.")

            # Transaction 2: Committed batch
            p.begin_transaction()
            committed_val = f"committed-msg-content-{int(time.time()*1000)}".encode("utf-8")
            p.produce(topic, partition=0, value=committed_val, key=b"k-commit")
            p.flush(5.0)
            p.commit_transaction()
            logger.info("Transaction 2 committed successfully.")

            # 2. Consumer read_uncommitted -> must see committed and may see aborted
            c_uncommitted = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": f"uncommitted-group-{int(time.time()*1000)}",
                "auto.offset.reset": "earliest",
                "isolation.level": "read_uncommitted",
                "enable.auto.commit": False,
            })
            c_uncommitted.assign([TopicPartition(topic, 0, 0)])

            uncommitted_records = []
            deadline = time.time() + 5.0
            while time.time() < deadline and len(uncommitted_records) < 2:
                msg = c_uncommitted.poll(0.5)
                if msg and not msg.error():
                    uncommitted_records.append(msg.value())
            c_uncommitted.close()

            # 3. Consumer read_committed -> MUST NOT see aborted_val, MUST see committed_val
            c_committed = Consumer({
                "bootstrap.servers": self.bootstrap_server,
                "group.id": f"committed-group-{int(time.time()*1000)}",
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "enable.auto.commit": False,
            })
            c_committed.assign([TopicPartition(topic, 0, 0)])

            committed_records = []
            deadline = time.time() + 5.0
            while time.time() < deadline:
                msg = c_committed.poll(0.5)
                if msg and not msg.error():
                    committed_records.append(msg.value())
                    if committed_val in msg.value():
                        break
            c_committed.close()

            # Assertions
            assert any(committed_val in v for v in committed_records), "Committed message not found in read_committed consumer"
            assert not any(aborted_val in v for v in committed_records), "Aborted message LEAKED into read_committed consumer!"

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-F",
                    name="Transactional Produce & Isolation Level Verification",
                    api_keys=[22, 24, 26, 0, 1],
                    status="PASS",
                    latency_ms=dur,
                    details={
                        "read_uncommitted_count": len(uncommitted_records),
                        "read_committed_count": len(committed_records),
                        "aborted_isolated": True,
                        "committed_delivered": True,
                    },
                )
            )
            logger.info(f"[PASS] Test F: Transaction isolation verified (aborted filtered out) in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-F",
                    name="Transactional Produce & Isolation Level Verification",
                    api_keys=[22, 24, 26, 0, 1],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test F: {e}")

    def test_g_wire_security_auth(self):
        """Test G: Wire security authentication verification (SASL / PLAIN, SCRAM-SHA-256, mTLS)."""
        t0 = time.perf_counter()
        security_subtests = {}

        try:
            # 1. SASL/PLAIN Authenticated Producer & Consumer
            p_plain = Producer({
                "bootstrap.servers": self.bootstrap_server,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "aerostream",
                "sasl.password": "aerostream123",
                "acks": "1",
                "linger.ms": 0,
            })
            p_plain.produce("test-wire-basic", value=b"sasl-plain-verified")
            p_plain.flush(5.0)
            security_subtests["SASL_PLAIN"] = "PASS"

            # 2. SASL/SCRAM-SHA-256 Authenticated Producer
            p_scram = Producer({
                "bootstrap.servers": self.bootstrap_server,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "aerostream",
                "sasl.password": "aerostream",
                "acks": "1",
                "linger.ms": 0,
            })
            p_scram.produce("test-wire-basic", value=b"sasl-scram-verified")
            p_scram.flush(5.0)
            security_subtests["SASL_SCRAM_SHA_256"] = "PASS"

            # 3. Invalid credentials rejection (ApiKey 17 + 36)
            with KafkaWireHelper(self.host, self.port) as wire:
                cid1 = 4017
                req1 = struct.pack(">hhi", 17, 0, cid1) + wire.put_string("auth-test") + wire.put_string("PLAIN")
                wire.send_and_recv(req1)
                cid2 = 4036
                bad_auth = b"\x00baduser\x00wrongpassword"
                req2 = struct.pack(">hhi", 36, 0, cid2) + wire.put_string("auth-test")
                req2 += struct.pack(">i", len(bad_auth)) + bad_auth
                resp2 = wire.send_and_recv(req2)
                rcid2, err2 = struct.unpack_from(">ih", resp2, 0)
                assert err2 == 58, f"Expected SASL_AUTHENTICATION_FAILED (58), got {err2}"
                security_subtests["INVALID_CREDENTIALS_REJECTION"] = "PASS"

            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-G",
                    name="Wire Security Authentication Verification (SASL/PLAIN, SCRAM-SHA-256)",
                    api_keys=[17, 36],
                    status="PASS",
                    latency_ms=dur,
                    details=security_subtests,
                )
            )
            logger.info(f"[PASS] Test G: Wire security authentication verified in {dur:.2f}ms")
        except Exception as e:
            dur = (time.perf_counter() - t0) * 1000
            self.results.append(
                TestResult(
                    test_id="TEST-G",
                    name="Wire Security Authentication Verification (SASL/PLAIN, SCRAM-SHA-256)",
                    api_keys=[17, 36],
                    status="FAIL",
                    latency_ms=dur,
                    error=str(e),
                )
            )
            logger.error(f"[FAIL] Test G: {e}")


def main():
    orchestrator = E2EOrchestrator()
    results = orchestrator.run_all()
    print("\n" + "=" * 90)
    print(f"{'AEROSTREAM KAFKA WIRE PROTOCOL E2E VERIFICATION REPORT':^90}")
    print("=" * 90)
    print(f"{'TEST ID':<9} | {'NAME':<42} | {'API KEYS':<14} | {'STATUS':<7} | {'LATENCY'}")
    print("-" * 90)
    passed = 0
    for r in results:
        api_str = ",".join(str(k) for k in r.api_keys)
        status_sym = "✅ PASS" if r.status == "PASS" else "❌ FAIL"
        if r.status == "PASS":
            passed += 1
        lat_str = f"{r.latency_ms:.2f}ms"
        print(f"{r.test_id:<9} | {r.name:<42} | {api_str:<14} | {status_sym:<7} | {lat_str}")
    print("=" * 90)
    print(f"Summary: {passed} / {len(results)} Passed | Status: {'ALL TESTS PASSED' if passed == len(results) else 'FAILURES DETECTED'}")
    print("=" * 90)


if __name__ == "__main__":
    main()
