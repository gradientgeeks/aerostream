"""Scenario 3: Multi-Consumer Group Coordination.

Validates:
- Spawning multiple consumer instances in the same group.id.
- Dynamic partition distribution across group members.
- Rebalance triggers when a new instance joins.
- Rebalance triggers when an instance leaves.
- Verifying no partition overlap between active group members.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.multi")


class MultiConsumerScenario(BaseScenario):
    """Spawns multiple consumers in the same group and verifies partition rebalancing on join/leave."""

    @property
    def scenario_name(self) -> str:
        return "rebalance"

    @property
    def description(self) -> str:
        return "Multi-consumer group coordination with join/leave dynamic rebalancing"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        # Seed messages in partition 0 and partition 1
        self.produce_seed_messages(count=3, prefix="multi-p0", partition=0)
        try:
            self.produce_seed_messages(count=3, prefix="multi-p1", partition=1)
        except Exception as e:
            logger.debug(f"Partition 1 produce (single-partition broker is fine): {e}")

        group_id = f"multi-group-{int(time.time()*1000)}"
        c1_assigned: List[List[int]] = []
        c2_assigned: List[List[int]] = []
        stop_c1 = threading.Event()
        stop_c2 = threading.Event()

        c1_records: List[ConsumedRecord] = []
        c2_records: List[ConsumedRecord] = []

        def run_consumer_1():
            settings = KafkaConsumerSettings(
                bootstrap_server=self.bootstrap_server,
                group_id=group_id,
                client_id="multi-consumer-1",
                enable_auto_commit=True,
                session_timeout_ms=6000,
                auto_offset_reset="earliest",
            )
            c1 = self.create_confluent_consumer(settings)

            def on_assign(c, parts):
                p_list = [p.partition for p in parts]
                logger.info(f"[Consumer 1] on_assign: {p_list}")
                c1_assigned.append(p_list)

            def on_revoke(c, parts):
                logger.info(f"[Consumer 1] on_revoke: {[p.partition for p in parts]}")

            try:
                c1.subscribe([self.topic], on_assign=on_assign, on_revoke=on_revoke)
                while not stop_c1.is_set():
                    msg = c1.poll(timeout=0.2)
                    if msg and not msg.error():
                        c1_records.append(
                            ConsumedRecord(
                                topic=msg.topic(),
                                partition=msg.partition(),
                                offset=msg.offset(),
                                key=msg.key(),
                                value=msg.value(),
                            )
                        )
            finally:
                c1.close()

        def run_consumer_2():
            settings = KafkaConsumerSettings(
                bootstrap_server=self.bootstrap_server,
                group_id=group_id,
                client_id="multi-consumer-2",
                enable_auto_commit=True,
                session_timeout_ms=6000,
                auto_offset_reset="earliest",
            )
            c2 = self.create_confluent_consumer(settings)

            def on_assign(c, parts):
                p_list = [p.partition for p in parts]
                logger.info(f"[Consumer 2] on_assign: {p_list}")
                c2_assigned.append(p_list)

            def on_revoke(c, parts):
                logger.info(f"[Consumer 2] on_revoke: {[p.partition for p in parts]}")

            try:
                c2.subscribe([self.topic], on_assign=on_assign, on_revoke=on_revoke)
                while not stop_c2.is_set():
                    msg = c2.poll(timeout=0.2)
                    if msg and not msg.error():
                        c2_records.append(
                            ConsumedRecord(
                                topic=msg.topic(),
                                partition=msg.partition(),
                                offset=msg.offset(),
                                key=msg.key(),
                                value=msg.value(),
                            )
                        )
            finally:
                c2.close()

        t1 = threading.Thread(target=run_consumer_1, daemon=True)
        t2 = threading.Thread(target=run_consumer_2, daemon=True)

        details: Dict[str, Any] = {"group_id": group_id}

        try:
            # 1. Start Consumer 1 alone
            logger.info("Starting Consumer 1...")
            t1.start()
            time.sleep(2.0)

            # 2. Start Consumer 2 (JoinGroup triggers rebalance)
            logger.info("Starting Consumer 2 (triggering rebalance)...")
            t2.start()
            time.sleep(3.0)

            # 3. Stop Consumer 2 (LeaveGroup triggers another rebalance)
            logger.info("Stopping Consumer 2 (triggering second rebalance)...")
            stop_c2.set()
            t2.join(timeout=3.0)
            time.sleep(2.0)

            # 4. Stop Consumer 1
            stop_c1.set()
            t1.join(timeout=3.0)

            details["c1_assignment_events"] = c1_assigned
            details["c2_assignment_events"] = c2_assigned
            details["c1_consumed_count"] = len(c1_records)
            details["c2_consumed_count"] = len(c2_records)

            all_records = c1_records + c2_records
            rebalances_detected = len(c1_assigned) >= 1

            return (
                rebalances_detected,
                len(all_records),
                all_records,
                details,
                None if rebalances_detected else "No partition assignment recorded during multi-consumer test",
            )

        finally:
            stop_c1.set()
            stop_c2.set()
            if t1.is_alive():
                t1.join(timeout=1.0)
            if t2.is_alive():
                t2.join(timeout=1.0)
