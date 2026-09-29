"""
Scenario 5: Large Payloads.
Validates producer frame fragmentation, memory buffer management, and wire transport
across variable payload tiers: 100B, 1KB, 64KB, 1MB, and 5MB.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload, parse_size_str
from utils.producer_factory import ProducerOptions
from utils.reporter import ScenarioResult


class LargePayloadScenario(BaseScenario):
    """Stress tests producer buffer limits and network framing across distinct message payload sizes."""

    DEFAULT_SIZES = [
        ("100B", 100, 50),
        ("1KB", 1024, 50),
        ("64KB", 64 * 1024, 20),
        ("1MB", 1024 * 1024, 5),
        ("5MB", 5 * 1024 * 1024, 2),
    ]

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        results: List[ScenarioResult] = []

        custom_payload_str = kwargs.get("payload_size")
        if custom_payload_str:
            custom_bytes = parse_size_str(custom_payload_str)
            test_matrix = [(custom_payload_str, custom_bytes, count or 10)]
        else:
            test_matrix = self.DEFAULT_SIZES

        for size_label, size_bytes, default_n in test_matrix:
            num_msgs = count if count is not None else default_n

            records: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = [
                (
                    f"key-large-{size_label}-{i}".encode("utf-8"),
                    generate_payload(size_bytes, prefix=f"large-{size_label}-{i}"),
                    [("payload-size-label", size_label.encode("utf-8")), ("payload-bytes", str(size_bytes).encode("utf-8"))],
                )
                for i in range(num_msgs)
            ]

            opts = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"aerostream-large-{size_label}",
                acks=1,
                max_request_size=max(10 * 1024 * 1024, size_bytes + 1048576),
                batch_size=max(size_bytes, 16384),
                linger_ms=5,
                request_timeout_ms=30000,
            )

            res = self.produce_batch(
                opts,
                records,
                subtest_name=f"payload_{size_label}",
                scenario_name="Large Payloads",
            )
            res.details["target_size_bytes"] = size_bytes
            res.details["target_size_label"] = size_label
            results.append(res)

        return results
