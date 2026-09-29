"""
Scenario 3: Message Headers.
Validates attaching custom headers (e.g. trace-id, source=aerostream-producer, timestamp,
binary tokens, multi-byte UTF-8) and confirms byte-level Kafka wire encoding.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload
from utils.producer_factory import ProducerOptions
from utils.reporter import ScenarioResult


class MessageHeadersScenario(BaseScenario):
    """Exercises record headers (KIP-98 magic v2) support."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or self.default_count
        results: List[ScenarioResult] = []

        # Subtest 1: Standard Distributed Tracing & Routing Headers
        records_standard: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = []
        for i in range(n):
            trace_id = str(uuid.uuid4())
            ts_str = str(int(time.time() * 1000))
            hdrs = [
                ("trace-id", trace_id.encode("utf-8")),
                ("source", b"aerostream-producer"),
                ("timestamp", ts_str.encode("utf-8")),
                ("correlation-id", f"req-{i:06d}".encode("utf-8")),
                ("env", b"production"),
            ]
            key = f"trace-key-{i}".encode("utf-8")
            val = generate_payload(128, prefix=f"header-test-{i}")
            records_standard.append((key, val, hdrs))

        opts = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-headers-std",
            acks=1,
            batch_size=16384,
            linger_ms=5,
        )
        r1 = self.produce_batch(
            opts,
            records_standard,
            subtest_name="standard_tracing_headers",
            scenario_name="Message Headers",
        )
        r1.details["sample_headers"] = ["trace-id", "source=aerostream-producer", "timestamp", "correlation-id", "env"]
        results.append(r1)

        # Subtest 2: Binary & Edge-Case Headers (empty value, binary tokens, high-UTF8)
        records_edge: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = []
        for i in range(min(n, 50)):
            hdrs = [
                ("empty-header", b""),
                ("binary-token", bytes([0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x42])),
                ("unicode-header", "🚀 AeroStream ⚡ Fast Queue 日本語".encode("utf-8")),
                ("single-byte", b"A"),
            ]
            key = f"edge-key-{i}".encode("utf-8")
            val = generate_payload(64, prefix=f"edge-header-{i}")
            records_edge.append((key, val, hdrs))

        opts_edge = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-headers-edge",
            acks=1,
        )
        r2 = self.produce_batch(
            opts_edge,
            records_edge,
            subtest_name="binary_and_unicode_headers",
            scenario_name="Message Headers",
        )
        r2.details["tested_types"] = ["empty-bytes", "raw-binary-bytes", "utf8-multibyte"]
        results.append(r2)

        return results
