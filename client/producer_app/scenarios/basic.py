"""
Scenario 1: Standard & Batch Produce.
Validates Kafka producer acknowledgment modes (acks: 0, 1, -1/all),
batch sizing, linger.ms throttling, and throughput/latency characteristics.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload
from utils.producer_factory import ProducerOptions
from utils.reporter import ScenarioResult


class BasicProduceScenario(BaseScenario):
    """Exercises standard message production across various acks and batch configurations."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or self.default_count
        results: List[ScenarioResult] = []

        # Check if user passed specific overrides
        user_acks = kwargs.get("acks")
        user_batch_size = kwargs.get("batch_size")
        user_linger_ms = kwargs.get("linger_ms")

        # 1. Standard Leader Ack (acks=1)
        records: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = [
            (f"key-{i}".encode("utf-8"), generate_payload(256, prefix=f"basic-acks1-{i}"), None)
            for i in range(n)
        ]
        opts = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-basic-acks1",
            acks=user_acks if user_acks is not None else 1,
            batch_size=user_batch_size if user_batch_size is not None else 16384,
            linger_ms=user_linger_ms if user_linger_ms is not None else 5,
        )
        r1 = self.produce_batch(opts, records, subtest_name=f"acks_{opts.acks}_standard", scenario_name="Basic Produce")
        results.append(r1)

        # If user didn't force a specific ack, run the full matrix:
        if user_acks is None:
            # 2. Strict Durability (acks=all / -1)
            records_all = [
                (f"key-{i}".encode("utf-8"), generate_payload(256, prefix=f"basic-acksall-{i}"), None)
                for i in range(n)
            ]
            opts_all = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id="aerostream-basic-acksall",
                acks="all",
                batch_size=16384,
                linger_ms=5,
            )
            r2 = self.produce_batch(opts_all, records_all, subtest_name="acks_all_durability", scenario_name="Basic Produce")
            results.append(r2)

            # 3. Fire-and-Forget (acks=0)
            records_zero = [
                (f"key-{i}".encode("utf-8"), generate_payload(256, prefix=f"basic-acks0-{i}"), None)
                for i in range(n)
            ]
            opts_zero = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id="aerostream-basic-acks0",
                acks=0,
                batch_size=16384,
                linger_ms=0,
            )
            r3 = self.produce_batch(opts_zero, records_zero, subtest_name="acks_0_fire_and_forget", scenario_name="Basic Produce")
            results.append(r3)

        # 4. Batching Optimization (batch.size=65536, linger.ms=20)
        records_batch = [
            (f"key-{i}".encode("utf-8"), generate_payload(512, prefix=f"batch-opt-{i}"), None)
            for i in range(max(n * 2, 200))
        ]
        opts_batch = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-batch-opt",
            acks=1,
            batch_size=65536,
            linger_ms=20,
        )
        r4 = self.produce_batch(opts_batch, records_batch, subtest_name="high_throughput_batching", scenario_name="Basic Produce")
        results.append(r4)

        return results
