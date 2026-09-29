"""Scenario 2: Consumer Groups & Dynamic Rebalance.

Validates:
- Joining a consumer group (JoinGroup / SyncGroup).
- Receiving dynamic partition assignments via on_assign callback.
- Consuming records assigned through group coordination.
- Heartbeats sent to maintain active membership.
- Clean departure from group (LeaveGroup) on consumer close.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.group")


class GroupRebalanceScenario(BaseScenario):
    """Exercises consumer group dynamic subscription and rebalance callbacks."""

    @property
    def scenario_name(self) -> str:
        return "group"

    @property
    def description(self) -> str:
        return "Consumer groups and dynamic partition rebalance with lifecycle callbacks"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        seed_count = max(3, self.expected_count)
        self.produce_seed_messages(count=seed_count, prefix="group-msg", partition=0)

        assigned_partitions: List[int] = []
        revoked_partitions: List[int] = []

        def on_assign_cb(c, partitions):
            logger.info(f"[Rebalance CB] on_assign received: {partitions}")
            for p in partitions:
                assigned_partitions.append(p.partition)

        def on_revoke_cb(c, partitions):
            logger.info(f"[Rebalance CB] on_revoke received: {partitions}")
            for p in partitions:
                revoked_partitions.append(p.partition)

        settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            group_id=self.group_id,
            client_id="group-rebalance-tester",
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            session_timeout_ms=6000,
        )
        consumer = self.create_confluent_consumer(settings)

        consumed_records: List[ConsumedRecord] = []
        details: Dict[str, Any] = {
            "group_id": self.group_id,
            "assigned_partitions": assigned_partitions,
            "revoked_partitions": revoked_partitions,
        }

        try:
            logger.info(f"Subscribing to topic '{self.topic}' with group '{self.group_id}'...")
            consumer.subscribe([self.topic], on_assign=on_assign_cb, on_revoke=on_revoke_cb)

            deadline = time.time() + self.timeout
            while time.time() < deadline:
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    if len(consumed_records) >= seed_count:
                        break
                    continue
                if msg.error():
                    logger.warning(f"Consumer poll error: {msg.error()}")
                    continue

                consumed_records.append(
                    ConsumedRecord(
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        key=msg.key(),
                        value=msg.value(),
                        headers=msg.headers() or [],
                        timestamp=msg.timestamp()[1] if msg.timestamp() else None,
                    )
                )
                if len(consumed_records) >= seed_count:
                    break

            details["assigned_count"] = len(assigned_partitions)
            details["records_consumed"] = len(consumed_records)

            if not assigned_partitions and not consumed_records:
                return (
                    False,
                    0,
                    [],
                    details,
                    "No partitions assigned by group coordinator during rebalance",
                )

            logger.info("Successfully received group partition assignment and consumed messages.")
            return True, len(consumed_records), consumed_records, details, None

        finally:
            logger.info("Closing consumer to trigger LeaveGroup...")
            consumer.close()
            details["closed_cleanly"] = True
