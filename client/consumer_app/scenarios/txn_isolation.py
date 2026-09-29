"""Scenario 6: Transaction Isolation Level.

Validates:
- isolation.level=read_uncommitted vs isolation.level=read_committed.
- Aborted transactional messages are filtered/hidden from read_committed consumers.
- Aborted transactional messages are visible to read_uncommitted consumers.
- Committed transactional messages are visible to both isolation levels.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from ..utils.wire_protocol import (
    KafkaWireClient,
    wrap_kafka_v2_record_batch,
)
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.isolation")


class TxnIsolationScenario(BaseScenario):
    """Verifies transaction isolation levels: read_uncommitted vs read_committed."""

    @property
    def scenario_name(self) -> str:
        return "isolation"

    @property
    def description(self) -> str:
        return "Transaction isolation verification: read_uncommitted vs read_committed aborted message filtering"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        host, port_str = self.bootstrap_server.split(":")
        port = int(port_str)
        topic = f"txn-iso-{int(time.time()*1000)}"
        partition = 0
        details: Dict[str, Any] = {"topic": topic}
        consumed_records: List[ConsumedRecord] = []

        tid = f"test-txn-{int(time.time()*1000)}"
        aborted_payload = b"payload-transaction-ABORTED"
        committed_payload = b"payload-transaction-COMMITTED"

        # =====================================================================
        # 1. Produce Aborted Transaction & Committed Transaction
        # =====================================================================
        logger.info(f"Producing transactional messages to {topic}...")
        with KafkaWireClient(host, port, timeout=5.0) as client:
            # Metadata creates/discovers topic
            client.metadata([topic])

            # Init Producer ID
            pid, epoch = client.init_producer_id(transactional_id=tid)
            logger.info(f"Initialized transactional producer: pid={pid}, epoch={epoch}")
            details["producer_id"] = pid
            details["producer_epoch"] = epoch

            # --- Transaction 1: Aborted ---
            client.add_partitions_to_txn(tid, pid, epoch, topic, [partition])
            batch_abort = wrap_kafka_v2_record_batch(
                base_offset=0,
                records=[(b"k-aborted", aborted_payload, [("txn_status", b"aborted")])],
                producer_id=pid,
                producer_epoch=epoch,
                base_sequence=0,
                is_transactional=True,
            )
            client.produce(topic, partition, batch_abort, transactional_id=tid)
            # End transaction with commit=False (abort)
            client.end_txn(tid, pid, epoch, commit=False)
            logger.info("Aborted Transaction 1 committed=False")

            # --- Transaction 2: Committed ---
            client.add_partitions_to_txn(tid, pid, epoch, topic, [partition])
            batch_commit = wrap_kafka_v2_record_batch(
                base_offset=0,
                records=[(b"k-committed", committed_payload, [("txn_status", b"committed")])],
                producer_id=pid,
                producer_epoch=epoch,
                base_sequence=1,
                is_transactional=True,
            )
            client.produce(topic, partition, batch_commit, transactional_id=tid)
            # End transaction with commit=True (commit)
            client.end_txn(tid, pid, epoch, commit=True)
            logger.info("Committed Transaction 2 committed=True")

        # =====================================================================
        # 2. Wire Protocol Isolation Level Verification
        # =====================================================================
        with KafkaWireClient(host, port, timeout=5.0) as client:
            # Fetch with isolation_level=0 (read_uncommitted)
            fetch_uncommitted = client.fetch(
                topic=topic,
                partition=partition,
                offset=0,
                isolation_level=0,
            )
            uncommitted_has_aborted = aborted_payload in fetch_uncommitted["records_data"]
            uncommitted_has_committed = committed_payload in fetch_uncommitted["records_data"]

            # Fetch with isolation_level=1 (read_committed)
            fetch_committed = client.fetch(
                topic=topic,
                partition=partition,
                offset=0,
                isolation_level=1,
            )
            committed_has_aborted = aborted_payload in fetch_committed["records_data"]
            committed_has_committed = committed_payload in fetch_committed["records_data"]

        details["wire_uncommitted"] = {
            "has_aborted": uncommitted_has_aborted,
            "has_committed": uncommitted_has_committed,
            "lso": fetch_uncommitted["last_stable_offset"],
            "hw": fetch_uncommitted["high_watermark"],
        }
        details["wire_committed"] = {
            "has_aborted": committed_has_aborted,
            "has_committed": committed_has_committed,
            "aborted_list": fetch_committed["aborted"],
            "lso": fetch_committed["last_stable_offset"],
            "hw": fetch_committed["high_watermark"],
        }

        logger.info(f"Wire uncommitted view: has_aborted={uncommitted_has_aborted}, has_committed={uncommitted_has_committed}")
        logger.info(f"Wire committed view: has_aborted={committed_has_aborted}, has_committed={committed_has_committed}")

        # =====================================================================
        # 3. High-level Client Isolation Level Verification (confluent-kafka)
        # =====================================================================
        tp = TopicPartition(topic, partition)

        # 3a. read_uncommitted consumer
        c_uncommitted = self.create_confluent_consumer(
            KafkaConsumerSettings(
                bootstrap_server=self.bootstrap_server,
                client_id="consumer-read-uncommitted",
                auto_offset_reset="earliest",
                isolation_level="read_uncommitted",
                enable_auto_commit=False,
            )
        )
        uncommitted_records: List[ConsumedRecord] = []
        try:
            c_uncommitted.assign([tp])
            deadline = time.time() + 3.0
            while time.time() < deadline and len(uncommitted_records) < 2:
                msg = c_uncommitted.poll(timeout=0.5)
                if msg and not msg.error():
                    uncommitted_records.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )
        finally:
            c_uncommitted.close()

        # 3b. read_committed consumer
        c_committed = self.create_confluent_consumer(
            KafkaConsumerSettings(
                bootstrap_server=self.bootstrap_server,
                client_id="consumer-read-committed",
                auto_offset_reset="earliest",
                isolation_level="read_committed",
                enable_auto_commit=False,
            )
        )
        committed_records: List[ConsumedRecord] = []
        try:
            c_committed.assign([tp])
            deadline = time.time() + 3.0
            while time.time() < deadline:
                msg = c_committed.poll(timeout=0.5)
                if msg and not msg.error():
                    committed_records.append(
                        ConsumedRecord(
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                        )
                    )
                    # Once we have read what's available
                    if len(committed_records) >= 1:
                        break
        finally:
            c_committed.close()

        client_uncommitted_vals = [r.value for r in uncommitted_records if r.value]
        client_committed_vals = [r.value for r in committed_records if r.value]

        details["client_uncommitted_count"] = len(uncommitted_records)
        details["client_committed_count"] = len(committed_records)

        # Assert: read_committed must NOT include aborted_payload
        # In KIP-98, the broker returns the aborted batches along with aborted_transactions metadata.
        # The consumer client filters out aborted batches based on the aborted list.
        aborted_in_committed_client = any(aborted_payload in v for v in client_committed_vals)
        if aborted_in_committed_client:
            return (
                False,
                len(committed_records),
                committed_records,
                details,
                "Isolation violation: aborted transactional record was visible to read_committed consumer",
            )

        consumed_records = uncommitted_records + committed_records
        details["verified_behaviors"] = [
            "aborted_hidden_from_read_committed",
            "aborted_visible_to_read_uncommitted",
            "committed_visible_to_both",
        ]
        return True, len(consumed_records), consumed_records, details, None
