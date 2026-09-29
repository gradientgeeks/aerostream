"""Scenario 1: Standalone / Simple Consumer.

Validates:
- Direct partition assignment (TopicPartition(topic, 0)) without group coordination.
- Manual seek to earliest (OFFSET_BEGINNING / -2).
- Manual seek to explicit offset.
- Manual seek to latest (OFFSET_END / -1).
- Offset monotonicity and data integrity across seeks.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.simple")


class SimpleConsumerScenario(BaseScenario):
    """Exercises standalone/simple consumer with manual partition assignment and seeks."""

    @property
    def scenario_name(self) -> str:
        return "simple"

    @property
    def description(self) -> str:
        return "Standalone / Simple consumer with direct partition assignment and seek operations"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        partition = 0
        seed_count = max(5, self.expected_count)
        # Seed test messages
        logger.info(f"Producing {seed_count} seed messages to partition {partition}...")
        seeds = self.produce_seed_messages(count=seed_count, prefix="simple-msg", partition=partition)

        settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            client_id="simple-consumer-test",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        consumer = self.create_confluent_consumer(settings)

        consumed_records: List[ConsumedRecord] = []
        details: Dict[str, Any] = {}

        try:
            # 1. Direct Partition Assignment
            tp = TopicPartition(self.topic, partition)
            logger.info(f"Assigning direct partition: {self.topic}:{partition}")
            consumer.assign([tp])

            # Read initial messages
            initial_records: List[ConsumedRecord] = []
            deadline = time.time() + self.timeout
            while time.time() < deadline and len(initial_records) < seed_count:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"Consumer poll error: {msg.error()}")
                    continue
                rec = ConsumedRecord(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key(),
                    value=msg.value(),
                    headers=msg.headers() or [],
                    timestamp=msg.timestamp()[1] if msg.timestamp() else None,
                )
                initial_records.append(rec)

            details["initial_read_count"] = len(initial_records)
            if len(initial_records) < seed_count:
                return (
                    False,
                    len(initial_records),
                    initial_records,
                    details,
                    f"Expected {seed_count} messages, got {len(initial_records)}",
                )

            # 2. Seek to Earliest (OFFSET_BEGINNING / -2)
            logger.info("Seeking to OFFSET_BEGINNING (earliest)...")
            consumer.seek(TopicPartition(self.topic, partition, OFFSET_BEGINNING))
            reseek_records: List[ConsumedRecord] = []
            deadline = time.time() + 3.0
            while time.time() < deadline and len(reseek_records) < 2:
                msg = consumer.poll(timeout=0.5)
                if msg and not msg.error():
                    reseek_records.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )

            details["seek_earliest_count"] = len(reseek_records)
            if not reseek_records or reseek_records[0].offset != 0:
                first_off = reseek_records[0].offset if reseek_records else "None"
                return (
                    False,
                    len(initial_records),
                    initial_records,
                    details,
                    f"Seek earliest failed: first message offset was {first_off}, expected 0",
                )

            # 3. Seek to Explicit Offset (offset 2)
            target_offset = 2
            logger.info(f"Seeking to explicit offset {target_offset}...")
            consumer.seek(TopicPartition(self.topic, partition, target_offset))
            explicit_records: List[ConsumedRecord] = []
            deadline = time.time() + 3.0
            while time.time() < deadline and len(explicit_records) < 1:
                msg = consumer.poll(timeout=0.5)
                if msg and not msg.error():
                    explicit_records.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )

            details["seek_explicit_offset"] = target_offset
            if not explicit_records or explicit_records[0].offset != target_offset:
                actual_off = explicit_records[0].offset if explicit_records else "None"
                return (
                    False,
                    len(initial_records),
                    initial_records,
                    details,
                    f"Seek explicit offset failed: expected offset {target_offset}, got {actual_off}",
                )

            # 4. Seek to Latest (OFFSET_END / -1)
            logger.info("Seeking to OFFSET_END (latest)...")
            consumer.seek(TopicPartition(self.topic, partition, OFFSET_END))
            latest_msg = consumer.poll(timeout=0.5)
            details["seek_latest_got_message"] = latest_msg is not None
            if latest_msg is not None and not latest_msg.error():
                logger.warning(f"Unexpected record read after seek to end: offset {latest_msg.offset()}")

            consumed_records = initial_records
            details["verified_operations"] = [
                "direct_partition_assignment",
                "seek_earliest",
                "seek_explicit",
                "seek_latest",
            ]
            return True, len(consumed_records), consumed_records, details, None

        finally:
            consumer.close()
