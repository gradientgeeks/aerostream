"""
Scenario 4: Partition Key Routing.
Validates keyed message deterministic partition hashing (consistent placement)
and null-key message distribution across multi-partition topics.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload
from utils.producer_factory import ProducerOptions, build_confluent_producer, build_kafka_python_producer
from utils.reporter import ScenarioResult

logger = logging.getLogger(__name__)


class PartitionRoutingScenario(BaseScenario):
    """Verifies partition hash routing determinism and null-key round-robin/sticky behavior."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or self.default_count
        results: List[ScenarioResult] = []

        # Subtest 1: Deterministic Key Placement Consistency
        # We produce messages with 5 distinct keys multiple times and verify each key's partition never changes.
        test_keys = [f"entity-account-{k:03d}" for k in range(5)]
        key_partition_map: Dict[str, Optional[int]] = {k: None for k in test_keys}
        key_consistency_violations = 0
        total_keyed = 0
        total_bytes = 0

        opts = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-key-routing",
            acks=1,
            linger_ms=0,
        )

        import time
        start_time = time.perf_counter()
        errors = []

        try:
            if self.backend == "confluent":
                producer = build_confluent_producer(opts)

                def make_delivery_callback(k_str: str):
                    def cb(err, msg):
                        nonlocal key_consistency_violations
                        if err is not None:
                            errors.append(str(err))
                            return
                        p = msg.partition()
                        if key_partition_map[k_str] is None:
                            key_partition_map[k_str] = p
                        elif key_partition_map[k_str] != p:
                            key_consistency_violations += 1
                            logger.error(
                                f"Partition routing inconsistency for key {k_str}: "
                                f"previously P{key_partition_map[k_str]}, now P{p}"
                            )
                    return cb

                for round_idx in range(n // len(test_keys) + 1):
                    for k_str in test_keys:
                        k_bytes = k_str.encode("utf-8")
                        v_bytes = generate_payload(128, prefix=f"keyed-{k_str}-{round_idx}")
                        total_bytes += len(k_bytes) + len(v_bytes)
                        total_keyed += 1
                        producer.produce(
                            topic=self.topic,
                            key=k_bytes,
                            value=v_bytes,
                            on_delivery=make_delivery_callback(k_str),
                        )
                    producer.poll(0)

                producer.flush(timeout=self.timeout_sec)
            else:
                producer = build_kafka_python_producer(opts)
                for round_idx in range(n // len(test_keys) + 1):
                    for k_str in test_keys:
                        k_bytes = k_str.encode("utf-8")
                        v_bytes = generate_payload(128, prefix=f"keyed-{k_str}-{round_idx}")
                        total_bytes += len(k_bytes) + len(v_bytes)
                        total_keyed += 1
                        meta = producer.send(self.topic, key=k_bytes, value=v_bytes).get(timeout=self.timeout_sec)
                        p = meta.partition
                        if key_partition_map[k_str] is None:
                            key_partition_map[k_str] = p
                        elif key_partition_map[k_str] != p:
                            key_consistency_violations += 1
                producer.flush()

        except Exception as e:
            errors.append(str(e))

        dur = time.perf_counter() - start_time
        keyed_success = len(errors) == 0 and key_consistency_violations == 0

        r1 = ScenarioResult(
            scenario="Partition Key Routing",
            subtest="deterministic_key_placement",
            success=keyed_success,
            messages_sent=total_keyed,
            bytes_sent=total_bytes,
            duration_sec=dur,
            details={
                "distinct_keys_tested": len(test_keys),
                "key_to_partition_mapping": key_partition_map,
                "consistency_violations": key_consistency_violations,
            },
            error="; ".join(errors) if errors else None,
        )
        results.append(r1)

        # Subtest 2: Null-Key Distribution
        # Messages without a key should distribute across available partitions (round-robin / sticky)
        records_null_key: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]] = [
            (None, generate_payload(128, prefix=f"null-key-{i}"), None)
            for i in range(n)
        ]
        opts_null = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-null-keys",
            acks=1,
            batch_size=1024,
            linger_ms=0,
        )
        r2 = self.produce_batch(
            opts_null,
            records_null_key,
            subtest_name="null_key_distribution",
            scenario_name="Partition Key Routing",
        )
        r2.details["partitions_utilized"] = len(r2.partition_distribution)
        results.append(r2)

        return results
