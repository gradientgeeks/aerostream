"""
Scenario 2: Compression Codecs.
Validates production of compressed batches using 'none', 'snappy', 'gzip', 'lz4', and 'zstd'.
Measures compression effectiveness, payload delivery, and wire transmission.
"""

from __future__ import annotations

import json
from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario
from utils.producer_factory import ProducerOptions
from utils.reporter import ScenarioResult


def generate_compressible_record(idx: int) -> bytes:
    """Generates realistic structured JSON payload that benefits from compression."""
    doc = {
        "event_id": f"evt-{idx:08d}",
        "timestamp_ms": 1727500000000 + idx * 10,
        "source": "aerostream.sensor.telemetry",
        "device": {
            "model": "EdgeRouter-XG9",
            "firmware": "v2.14.8-rc3",
            "datacenter": "us-east-1a",
            "rack": f"rack-{idx % 8}",
        },
        "metrics": {
            "cpu_utilization": 0.42 + (idx % 100) * 0.005,
            "memory_used_mb": 4096 + (idx % 512),
            "rx_bytes": 10485760 + idx * 1024,
            "tx_bytes": 8388608 + idx * 512,
            "packet_loss_rate": 0.0001,
            "status": "HEALTHY",
        },
        "tags": ["production", "edge-telemetry", "kafka-wire", "aeromq"],
    }
    return json.dumps(doc).encode("utf-8")


class CompressionScenario(BaseScenario):
    """Evaluates Kafka compression codecs across uncompressed and compressed batches."""

    SUPPORTED_CODECS = ["none", "gzip", "snappy", "lz4", "zstd"]

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or self.default_count
        results: List[ScenarioResult] = []

        codecs_to_test = self.SUPPORTED_CODECS
        user_codec = kwargs.get("compression")
        if user_codec and user_codec.lower() in self.SUPPORTED_CODECS:
            codecs_to_test = [user_codec.lower()]

        for codec in codecs_to_test:
            records: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = [
                (f"key-{codec}-{i}".encode("utf-8"), generate_compressible_record(i), None)
                for i in range(n)
            ]

            opts = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"aerostream-comp-{codec}",
                acks=1,
                compression_type=codec,
                batch_size=32768,
                linger_ms=10,
            )

            res = self.produce_batch(
                opts,
                records,
                subtest_name=f"codec_{codec}",
                scenario_name="Compression Codecs",
            )
            res.details["codec"] = codec
            results.append(res)

        return results
