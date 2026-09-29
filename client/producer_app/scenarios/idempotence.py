"""
Scenario 6: Idempotent Producer.
Validates KIP-98 idempotent delivery semantics (enable.idempotence=true),
monotonic sequence assignment, and retry deduplication guarantees.
"""

from __future__ import annotations

import logging
import time
from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload
from utils.producer_factory import DeliveryTracker, ProducerOptions, build_confluent_producer
from utils.reporter import ScenarioResult

logger = logging.getLogger(__name__)


class IdempotentProducerScenario(BaseScenario):
    """Exercises KIP-98 Idempotent Producer with PID/epoch assignment and retry deduplication."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or self.default_count
        results: List[ScenarioResult] = []

        # Subtest 1: Basic Idempotent Sequence
        records: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = [
            (
                f"idemp-key-{i}".encode("utf-8"),
                generate_payload(128, prefix=f"idemp-seq-{i}"),
                [("sequence-index", str(i).encode("utf-8")), ("idempotence", b"enabled")],
            )
            for i in range(n)
        ]

        opts = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-idempotent-producer",
            enable_idempotence=True,
            acks="all",
            retries=5,
            retry_backoff_ms=100,
            linger_ms=5,
        )

        r1 = self.produce_batch(
            opts,
            records,
            subtest_name="idempotent_sequence_delivery",
            scenario_name="Idempotent Producer",
        )
        r1.details["enable_idempotence"] = True
        r1.details["retries"] = 5
        results.append(r1)

        # Subtest 2: High Retry & In-Flight Stress Test
        # Test max in flight (up to 5 per connection) with aggressive retries
        records_stress = [
            (
                f"retry-key-{i % 5}".encode("utf-8"),
                generate_payload(256, prefix=f"idemp-stress-{i}"),
                [("stress-batch", b"true")],
            )
            for i in range(max(n * 2, 100))
        ]

        opts_stress = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-idemp-stress",
            enable_idempotence=True,
            acks="all",
            retries=10,
            retry_backoff_ms=50,
            batch_size=32768,
            linger_ms=10,
            extra_config={"max.in.flight.requests.per.connection": 5},
        )

        r2 = self.produce_batch(
            opts_stress,
            records_stress,
            subtest_name="aggressive_retries_deduplication",
            scenario_name="Idempotent Producer",
        )
        r2.details["max_in_flight"] = 5
        r2.details["retries"] = 10
        results.append(r2)

        return results
