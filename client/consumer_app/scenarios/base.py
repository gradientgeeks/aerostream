"""Base scenario implementation and helper utilities for consumer tests."""

from __future__ import annotations

import abc
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord, ScenarioResult
from ..utils.wire_protocol import (
    KafkaWireClient,
    wrap_kafka_v0_message,
    wrap_kafka_v2_record_batch,
)

logger = logging.getLogger("aeromq-consumer")


class BaseScenario(abc.ABC):
    """Abstract base class for all Kafka consumer test scenarios."""

    def __init__(
        self,
        bootstrap_server: str = "127.0.0.1:9093",
        topic: str = "test-consumer-topic",
        group_id: Optional[str] = None,
        timeout: float = 10.0,
        expected_count: int = 1,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id or f"test-group-{int(time.time())}"
        self.timeout = timeout
        self.expected_count = expected_count
        self.extra_config = extra_config or {}

    @property
    @abc.abstractmethod
    def scenario_name(self) -> str:
        """Name of the scenario."""
        pass

    @property
    @abc.abstractmethod
    def description(self) -> str:
        """Brief human-readable description of what this scenario tests."""
        pass

    @abc.abstractmethod
    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        """Executes the scenario logic.

        Returns:
            (success, records_consumed_count, records_list, details_dict, error_msg_if_failed)
        """
        pass

    def run(self) -> ScenarioResult:
        """Runs the scenario and captures execution time, results, and exceptions."""
        logger.info(f"==> Starting Scenario: {self.scenario_name} ({self.description})")
        t0 = time.perf_counter()
        try:
            success, count, records, details, error = self.execute()
        except Exception as e:
            logger.exception(f"Unhandled exception in scenario {self.scenario_name}: {e}")
            success = False
            count = 0
            records = []
            details = {}
            error = f"Exception: {type(e).__name__}: {str(e)}"

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            f"<== Completed Scenario: {self.scenario_name} | Success: {success} | Records: {count} | Took: {elapsed_ms:.2f}ms"
        )
        return ScenarioResult(
            scenario_name=self.scenario_name,
            success=success,
            duration_ms=elapsed_ms,
            records_consumed=count,
            records=records,
            details=details,
            error=error,
        )

    # --- Producer & Seed Helpers ---

    def produce_seed_messages(
        self,
        count: int = 5,
        prefix: str = "msg",
        headers: Optional[List[Tuple[str, bytes]]] = None,
        partition: int = 0,
    ) -> List[Tuple[str, str, str]]:
        """Produces seed messages directly over the wire for predictable consumer verification.

        Returns:
            List of (key_str, value_str, sha256_hex)
        """
        host, port_str = self.bootstrap_server.split(":")
        port = int(port_str)
        produced = []
        with KafkaWireClient(host, port, timeout=5.0) as client:
            records_tuples = []
            for i in range(count):
                val_str = f"{prefix}-{int(time.time()*1000)}-{i}"
                val_bytes = val_str.encode("utf-8")
                key_str = f"key-{i}"
                key_bytes = key_str.encode("utf-8")
                checksum = hashlib.sha256(val_bytes).hexdigest()
                produced.append((key_str, val_str, checksum))
                records_tuples.append((key_bytes, val_bytes, headers or []))

            # Encode as Magic 2 record batch
            batch = wrap_kafka_v2_record_batch(0, records_tuples)
            client.produce(self.topic, partition, batch)

        logger.debug(f"Produced {len(produced)} seed messages to {self.topic}:{partition}")
        return produced

    def create_confluent_consumer(self, settings: KafkaConsumerSettings) -> Consumer:
        """Creates a confluent-kafka Consumer from settings."""
        conf = settings.to_confluent_config()
        if self.extra_config:
            conf.update(self.extra_config)
        return Consumer(conf)
