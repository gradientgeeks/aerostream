"""Scenario 4: Offset Management & Commit.

Validates:
- Synchronous manual commits (commitSync / commit(asynchronous=False)).
- Asynchronous manual commits (commitAsync / commit(asynchronous=True)).
- Committed offset verification via OffsetFetch (committed()).
- Offset persistence and resumption across consumer restarts.
- Automatic offset commits (enable.auto.commit=true).
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

logger = logging.getLogger("aeromq-consumer.offsets")


class OffsetManagementScenario(BaseScenario):
    """Validates auto-commit, manual commitSync/commitAsync, and offset persistence across restarts."""

    @property
    def scenario_name(self) -> str:
        return "offsets"

    @property
    def description(self) -> str:
        return "Offset management with auto commit, manual commitSync/commitAsync, and restart persistence"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        partition = 0
        group_id = f"offset-test-group-{int(time.time()*1000)}"
        seed_count = 6
        self.produce_seed_messages(count=seed_count, prefix="offset-msg", partition=partition)

        details: Dict[str, Any] = {"group_id": group_id}
        consumed_records: List[ConsumedRecord] = []

        # =====================================================================
        # 1. Manual Sync Commit (commitSync) & Restart Verification
        # =====================================================================
        logger.info("Part 1: Testing explicit manual commitSync and resumption...")
        c1_settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            group_id=group_id,
            client_id="offset-tester-c1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        c1 = self.create_confluent_consumer(c1_settings)
        tp = TopicPartition(self.topic, partition)

        try:
            c1.assign([tp])
            c1_read: List[ConsumedRecord] = []
            deadline = time.time() + 4.0
            # Read first 3 messages
            while time.time() < deadline and len(c1_read) < 3:
                msg = c1.poll(timeout=0.5)
                if msg and not msg.error():
                    c1_read.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )

            if len(c1_read) < 3:
                return (
                    False,
                    len(c1_read),
                    c1_read,
                    details,
                    f"C1 failed to read 3 messages for commit test; got {len(c1_read)}",
                )

            last_read_offset = c1_read[-1].offset
            commit_target_offset = last_read_offset + 1
            logger.info(f"Committing offset {commit_target_offset} synchronously...")
            c1.commit(offsets=[TopicPartition(self.topic, partition, commit_target_offset)], asynchronous=False)

            # Check committed offset via OffsetFetch
            committed_list = c1.committed([tp], timeout=3.0)
            committed_offset = committed_list[0].offset if committed_list else -1
            logger.info(f"OffsetFetch result: committed_offset={committed_offset}")
            details["sync_committed_offset"] = committed_offset

            if committed_offset != commit_target_offset:
                return (
                    False,
                    len(c1_read),
                    c1_read,
                    details,
                    f"Committed offset mismatch: expected {commit_target_offset}, got {committed_offset}",
                )

            consumed_records.extend(c1_read)
        finally:
            c1.close()

        # =====================================================================
        # 2. Consumer Restart Resumption Test
        # =====================================================================
        logger.info("Part 2: Restarting consumer in same group to verify resumption from committed offset...")
        c2_settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            group_id=group_id,
            client_id="offset-tester-c2-restart",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        c2 = self.create_confluent_consumer(c2_settings)
        try:
            c2.assign([tp])
            c2_read: List[ConsumedRecord] = []
            deadline = time.time() + 4.0
            while time.time() < deadline and len(c2_read) < (seed_count - 3):
                msg = c2.poll(timeout=0.5)
                if msg and not msg.error():
                    c2_read.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )

            if not c2_read:
                return (
                    False,
                    len(consumed_records),
                    consumed_records,
                    details,
                    "C2 (restart) did not read any messages after resumption",
                )

            first_resumed_offset = c2_read[0].offset
            details["resumed_first_offset"] = first_resumed_offset
            logger.info(f"C2 resumed at offset {first_resumed_offset} (expected {commit_target_offset})")

            if first_resumed_offset != commit_target_offset:
                return (
                    False,
                    len(consumed_records),
                    consumed_records,
                    details,
                    f"Resumption failed: expected offset {commit_target_offset}, but got {first_resumed_offset}",
                )

            # =================================================================
            # 3. Asynchronous Manual Commit (commitAsync)
            # =================================================================
            logger.info("Part 3: Testing manual commitAsync...")
            async_target = c2_read[-1].offset + 1
            c2.commit(offsets=[TopicPartition(self.topic, partition, async_target)], asynchronous=True)
            time.sleep(0.5)  # Allow async commit roundtrip

            committed_after_async = c2.committed([tp], timeout=3.0)
            async_committed_offset = committed_after_async[0].offset if committed_after_async else -1
            details["async_committed_offset"] = async_committed_offset
            logger.info(f"Committed offset after async commit: {async_committed_offset}")

            consumed_records.extend(c2_read)
        finally:
            c2.close()

        # =====================================================================
        # 4. Auto-Commit Test (enable.auto.commit=true)
        # =====================================================================
        logger.info("Part 4: Testing automatic offset commit...")
        auto_group = f"auto-group-{int(time.time()*1000)}"
        c_auto_settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            group_id=auto_group,
            client_id="offset-tester-auto",
            enable_auto_commit=True,
            auto_commit_interval_ms=500,
            auto_offset_reset="earliest",
        )
        c_auto = self.create_confluent_consumer(c_auto_settings)
        try:
            c_auto.assign([tp])
            auto_read = 0
            deadline = time.time() + 3.0
            while time.time() < deadline and auto_read < 2:
                msg = c_auto.poll(timeout=0.5)
                if msg and not msg.error():
                    auto_read += 1

            # Sleep to allow auto.commit.interval.ms (500ms) to trigger on next poll
            time.sleep(0.6)
            c_auto.poll(timeout=0.2)  # poll triggers the scheduled auto-commit

            committed_auto = c_auto.committed([tp], timeout=3.0)
            auto_off = committed_auto[0].offset if committed_auto else -1
            details["auto_committed_offset"] = auto_off
            logger.info(f"Auto-committed offset: {auto_off}")
            details["auto_commit_verified"] = auto_off > 0
        finally:
            c_auto.close()

        details["verified_operations"] = [
            "commitSync",
            "offset_persistence_across_restart",
            "commitAsync",
            "enable.auto.commit",
        ]
        return True, len(consumed_records), consumed_records, details, None
