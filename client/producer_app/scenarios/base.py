"""
AeroStream Kafka Producer Scenarios - Base Interface & Shared Machinery.
"""

from __future__ import annotations

import logging
import math
import string
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from utils.producer_factory import (
    DeliveryTracker,
    ProducerOptions,
    build_confluent_producer,
    build_kafka_python_producer,
)
from utils.reporter import ScenarioResult

logger = logging.getLogger(__name__)


def generate_payload(size_bytes: int, prefix: str = "") -> bytes:
    """Generates deterministic or random byte payload of specified size."""
    if size_bytes <= 0:
        return b""
    header = f"{prefix}:ts={int(time.time()*1000)}:len={size_bytes}:".encode("utf-8")
    if len(header) >= size_bytes:
        return header[:size_bytes]
    filler = b"X" * (size_bytes - len(header))
    return header + filler


def parse_size_str(val: Union[str, int]) -> int:
    """Parses strings like '100B', '1KB', '64KB', '1MB', '5MB' into byte count."""
    if isinstance(val, int):
        return val
    s = val.strip().upper()
    if s.endswith("KB") or s.endswith("K"):
        num = float(s.rstrip("KB").rstrip("K"))
        return int(num * 1024)
    elif s.endswith("MB") or s.endswith("M"):
        num = float(s.rstrip("MB").rstrip("M"))
        return int(num * 1024 * 1024)
    elif s.endswith("GB") or s.endswith("G"):
        num = float(s.rstrip("GB").rstrip("G"))
        return int(num * 1024 * 1024 * 1024)
    elif s.endswith("B"):
        return int(s.rstrip("B"))
    return int(s)


class BaseScenario(ABC):
    """Abstract base class for all producer test scenarios."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "aerostream-test",
        default_count: int = 100,
        backend: str = "confluent",
        timeout_sec: float = 10.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.default_count = default_count
        self.backend = backend.lower()
        self.timeout_sec = timeout_sec

    @abstractmethod
    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        """Executes the scenario and returns a list of ScenarioResults."""
        pass

    def produce_batch(
        self,
        opts: ProducerOptions,
        records: List[Tuple[Optional[bytes], Optional[bytes], Optional[List[Tuple[str, bytes]]]]],
        subtest_name: str,
        scenario_name: str,
    ) -> ScenarioResult:
        """
        Produces a sequence of (key, value, headers) tuples, tracks asynchronous delivery,
        flushes the producer, and computes latency/throughput percentiles.
        """
        tracker = DeliveryTracker(expected_count=len(records))
        total_bytes = 0

        start_time = time.perf_counter()
        producer = None
        try:
            if self.backend == "confluent":
                producer = build_confluent_producer(opts)
                for idx, (k, v, hdrs) in enumerate(records):
                    val_len = len(v) if v else 0
                    key_len = len(k) if k else 0
                    total_bytes += (val_len + key_len)

                    tracker.mark_sent(idx)

                    # Delivery callback closure
                    def make_cb(msg_id: int):
                        return lambda err, msg: tracker.on_delivery(err, msg, msg_id)

                    producer.produce(
                        topic=self.topic,
                        value=v,
                        key=k,
                        headers=hdrs,
                        on_delivery=make_cb(idx),
                    )
                    # Poll periodically to trigger delivery callbacks
                    if idx % 100 == 0:
                        producer.poll(0)

                # Flush all outstanding messages
                remaining = producer.flush(timeout=self.timeout_sec)
                if remaining > 0:
                    tracker.errors.append(f"{remaining} messages failed to flush within {self.timeout_sec}s")

            elif self.backend in ("kafka-python", "kafka_python"):
                producer = build_kafka_python_producer(opts)
                futures = []
                for idx, (k, v, hdrs) in enumerate(records):
                    val_len = len(v) if v else 0
                    key_len = len(k) if k else 0
                    total_bytes += (val_len + key_len)

                    tracker.mark_sent(idx)
                    # Convert headers if provided
                    h_list = [(h_k, h_v) for h_k, h_v in (hdrs or [])]
                    f = producer.send(self.topic, value=v, key=k, headers=h_list)
                    futures.append((idx, f))

                producer.flush(timeout=self.timeout_sec)
                for idx, f in futures:
                    try:
                        record_meta = f.get(timeout=self.timeout_sec)
                        tracker.on_delivery(None, record_meta, idx)
                    except Exception as ex:
                        tracker.on_delivery(ex, None, idx)
            else:
                raise ValueError(f"Unsupported producer backend: {self.backend}")

        except Exception as e:
            logger.error(f"Error in {scenario_name} :: {subtest_name}: {e}", exc_info=True)
            tracker.errors.append(str(e))
        finally:
            duration = time.perf_counter() - start_time

        # Calculate latency statistics
        avg_lat = 0.0
        p95_lat = 0.0
        p99_lat = 0.0
        if tracker.latencies_ms:
            sorted_lat = sorted(tracker.latencies_ms)
            avg_lat = sum(sorted_lat) / len(sorted_lat)
            p95_idx = int(len(sorted_lat) * 0.95)
            p99_idx = int(len(sorted_lat) * 0.99)
            p95_lat = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]
            p99_lat = sorted_lat[min(p99_idx, len(sorted_lat) - 1)]

        success = tracker.failed_count == 0 and tracker.delivered_count == len(records)
        if str(opts.acks) == "0":
            # For acks=0, broker does not send acknowledgments; success is sending without exception
            success = len(tracker.errors) == 0

        err_msg = "; ".join(tracker.errors[:3]) if tracker.errors else None

        return ScenarioResult(
            scenario=scenario_name,
            subtest=subtest_name,
            success=success,
            messages_sent=tracker.delivered_count if str(opts.acks) != "0" else len(records),
            bytes_sent=total_bytes,
            duration_sec=duration,
            avg_latency_ms=avg_lat,
            p95_latency_ms=p95_lat,
            p99_latency_ms=p99_lat,
            partition_distribution=tracker.partition_counts,
            details={
                "acks": opts.acks,
                "linger_ms": opts.linger_ms,
                "batch_size": opts.batch_size,
                "compression": opts.compression_type,
            },
            error=err_msg,
        )
