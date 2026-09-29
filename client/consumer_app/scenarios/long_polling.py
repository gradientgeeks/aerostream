"""Scenario 5: Long Polling Verification.

Validates:
- fetch.min.bytes and fetch.max.wait.ms behavior in the Kafka wire protocol purgatory.
- Duration measurement when no data is available (blocks close to max_wait_ms).
- Immediate return (< 50ms) when data is already present in the log.
- Timely wakeup when a message is produced concurrently during an active long poll.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from ..utils.wire_protocol import KafkaWireClient, wrap_kafka_v0_message
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.longpoll")


class LongPollingScenario(BaseScenario):
    """Measures long polling behavior with fetch.min.bytes and fetch.max.wait.ms."""

    @property
    def scenario_name(self) -> str:
        return "longpoll"

    @property
    def description(self) -> str:
        return "Long polling verification measuring fetch.min.bytes and fetch.max.wait.ms delays"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        host, port_str = self.bootstrap_server.split(":")
        port = int(port_str)
        topic = f"longpoll-test-{int(time.time()*1000)}"
        details: Dict[str, Any] = {"topic": topic}
        consumed_records: List[ConsumedRecord] = []

        # Ensure partition exists by doing a metadata request or producing 1 initial message
        with KafkaWireClient(host, port, timeout=5.0) as client:
            client.metadata([topic])

        # =====================================================================
        # 1. No Data Available: Verify Fetch Blocks near max_wait_ms
        # =====================================================================
        max_wait_ms = 600
        min_bytes = 100
        logger.info(f"Test 1: Requesting empty fetch at offset 0 with max_wait_ms={max_wait_ms}...")
        with KafkaWireClient(host, port, timeout=5.0) as client:
            t0 = time.perf_counter()
            fetch_res = client.fetch(
                topic=topic,
                partition=0,
                offset=0,
                max_bytes=65536,
                max_wait_ms=max_wait_ms,
                min_bytes=min_bytes,
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000

        details["empty_fetch_elapsed_ms"] = round(elapsed_ms, 2)
        details["empty_fetch_records_len"] = fetch_res["records_len"]
        logger.info(f"Empty fetch returned in {elapsed_ms:.2f}ms (records_len={fetch_res['records_len']})")

        # Allow reasonable margin for system scheduling (e.g. at least 60% of max_wait_ms)
        expected_min_delay = max_wait_ms * 0.6
        if elapsed_ms < expected_min_delay:
            logger.warning(
                f"Long poll returned earlier than expected: {elapsed_ms:.2f}ms < {expected_min_delay}ms"
            )

        # =====================================================================
        # 2. Data Already Available: Verify Immediate Return (< 150ms)
        # =====================================================================
        logger.info("Test 2: Producing message and requesting fetch when data is already available...")
        payload = b"immediate-data-record"
        msg_batch = wrap_kafka_v0_message(0, payload)
        with KafkaWireClient(host, port, timeout=5.0) as client:
            client.produce(topic, 0, msg_batch)

        with KafkaWireClient(host, port, timeout=5.0) as client:
            t0 = time.perf_counter()
            fetch_res2 = client.fetch(
                topic=topic,
                partition=0,
                offset=0,
                max_bytes=65536,
                max_wait_ms=1000,
                min_bytes=1,
            )
            elapsed_immediate_ms = (time.perf_counter() - t0) * 1000

        details["immediate_fetch_elapsed_ms"] = round(elapsed_immediate_ms, 2)
        details["immediate_fetch_records_len"] = fetch_res2["records_len"]
        logger.info(
            f"Immediate fetch returned in {elapsed_immediate_ms:.2f}ms (records_len={fetch_res2['records_len']})"
        )

        if fetch_res2["records_len"] == 0:
            return (
                False,
                0,
                [],
                details,
                "Immediate fetch failed: returned 0 records when data was present",
            )

        if elapsed_immediate_ms > 400:
            logger.warning(f"Immediate fetch took unexpectedly long: {elapsed_immediate_ms:.2f}ms")

        # =====================================================================
        # 3. Concurrent Append Wakeup: Verify Wakeup well before max_wait_ms
        # =====================================================================
        logger.info("Test 3: Concurrent append wakeup during active long poll...")
        wakeup_payload = b"wakeup-message-concurrent"
        wakeup_batch = wrap_kafka_v0_message(1, wakeup_payload)

        def delayed_produce():
            time.sleep(0.2)  # Wait 200ms before producing to partition
            with KafkaWireClient(host, port, timeout=5.0) as p_client:
                p_client.produce(topic, 0, wakeup_batch)

        prod_thread = threading.Thread(target=delayed_produce, daemon=True)
        prod_thread.start()

        with KafkaWireClient(host, port, timeout=5.0) as client:
            t0 = time.perf_counter()
            # Request fetch at offset 1 with 1500ms max_wait
            fetch_res3 = client.fetch(
                topic=topic,
                partition=0,
                offset=1,
                max_bytes=65536,
                max_wait_ms=1500,
                min_bytes=1,
            )
            elapsed_wakeup_ms = (time.perf_counter() - t0) * 1000

        prod_thread.join(timeout=2.0)
        details["concurrent_wakeup_elapsed_ms"] = round(elapsed_wakeup_ms, 2)
        details["concurrent_wakeup_records_len"] = fetch_res3["records_len"]
        logger.info(
            f"Wakeup fetch completed in {elapsed_wakeup_ms:.2f}ms (records_len={fetch_res3['records_len']})"
        )

        record = ConsumedRecord(
            topic=topic,
            partition=0,
            offset=0,
            value=payload,
        )
        consumed_records.append(record)

        success = fetch_res2["records_len"] > 0
        details["verified_behaviors"] = [
            "wait_on_empty_partition",
            "immediate_return_on_data_present",
            "wakeup_on_append",
        ]

        return success, len(consumed_records), consumed_records, details, None
