"""Scenario 7: Header & Payload Validation.

Validates:
- Precise Kafka header inspection (key-value pairs encoded in Magic 2 RecordBatch).
- Payload integrity and SHA256 checksum verification against producer payload.
- Decompressed payloads integrity across varied payload sizes.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

from ..config import KafkaConsumerSettings
from ..models import ConsumedRecord
from ..utils.wire_protocol import KafkaWireClient, wrap_kafka_v2_record_batch
from .base import BaseScenario

logger = logging.getLogger("aeromq-consumer.headers")


class HeaderPayloadScenario(BaseScenario):
    """Validates message headers and payload SHA256 checksums."""

    @property
    def scenario_name(self) -> str:
        return "headers"

    @property
    def description(self) -> str:
        return "Header & decompressed payload verification with SHA256 checksum validation"

    def execute(self) -> Tuple[bool, int, List[ConsumedRecord], Dict[str, Any], Optional[str]]:
        host, port_str = self.bootstrap_server.split(":")
        port = int(port_str)
        topic = f"header-payload-{int(time.time()*1000)}"
        partition = 0
        details: Dict[str, Any] = {"topic": topic}

        # Expected test messages with specific headers & payloads
        test_entries = [
            {
                "key": b"user-profile-1001",
                "value": b'{"user_id": 1001, "name": "Alice Developer", "active": true}',
                "headers": [
                    ("content-type", b"application/json"),
                    ("trace_id", b"trace-0001-xyz"),
                    ("app_source", b"aerostream-client"),
                ],
            },
            {
                "key": b"audit-event-2002",
                "value": b"AUDIT_RECORD: Action=AUTH_LOGIN User=Alice Status=SUCCESS" * 10,
                "headers": [
                    ("content-type", b"text/plain"),
                    ("correlation_id", b"corr-9999-alpha"),
                    ("priority", b"high"),
                ],
            },
            {
                "key": b"binary-data-3003",
                "value": bytes([i % 256 for i in range(1024)]),  # 1 KB binary pattern
                "headers": [
                    ("content-type", b"application/octet-stream"),
                    ("checksum_algo", b"sha256"),
                ],
            },
        ]

        expected_checksums: Dict[int, str] = {}
        expected_headers: Dict[int, Dict[str, bytes]] = {}

        # 1. Produce via KafkaWireClient with Magic 2 batches
        records_tuples = []
        for idx, entry in enumerate(test_entries):
            val = entry["value"]
            csum = hashlib.sha256(val).hexdigest()
            expected_checksums[idx] = csum
            expected_headers[idx] = {k: v for k, v in entry["headers"]}
            records_tuples.append((entry["key"], val, entry["headers"]))

        batch = wrap_kafka_v2_record_batch(base_offset=0, records=records_tuples)
        with KafkaWireClient(host, port, timeout=5.0) as client:
            client.metadata([topic])
            base_off = client.produce(topic, partition, batch)
            logger.info(f"Produced {len(test_entries)} record batch at base offset {base_off}")

        # 2. Consume using confluent-kafka
        settings = KafkaConsumerSettings(
            bootstrap_server=self.bootstrap_server,
            client_id="header-payload-validator",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        consumer = self.create_confluent_consumer(settings)
        tp = TopicPartition(topic, partition)

        consumed_records: List[ConsumedRecord] = []
        verified_checksums: List[Dict[str, Any]] = []

        try:
            consumer.assign([tp])

            deadline = time.time() + self.timeout
            while time.time() < deadline and len(consumed_records) < len(test_entries):
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"Consumer poll error: {msg.error()}")
                    continue

                idx = len(consumed_records)
                raw_val = msg.value() or b""
                calc_csum = hashlib.sha256(raw_val).hexdigest()
                exp_csum = expected_checksums.get(idx, "")

                if calc_csum != exp_csum:
                    return (
                        False,
                        len(consumed_records),
                        consumed_records,
                        details,
                        f"Checksum mismatch for record {idx}: got {calc_csum}, expected {exp_csum}",
                    )

                # Check headers
                msg_headers = msg.headers() or []
                msg_hdr_dict = {k: v for k, v in msg_headers}
                exp_hdr_dict = expected_headers.get(idx, {})

                for k, v in exp_hdr_dict.items():
                    if k not in msg_hdr_dict:
                        return (
                            False,
                            len(consumed_records),
                            consumed_records,
                            details,
                            f"Missing expected header '{k}' in record {idx}",
                        )
                    if msg_hdr_dict[k] != v:
                        return (
                            False,
                            len(consumed_records),
                            consumed_records,
                            details,
                            f"Header '{k}' value mismatch: got {msg_hdr_dict[k]}, expected {v}",
                        )

                rec = ConsumedRecord(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key(),
                    value=msg.value(),
                    headers=msg_headers,
                    sha256_checksum=calc_csum,
                )
                consumed_records.append(rec)
                verified_checksums.append({
                    "record_index": idx,
                    "sha256": calc_csum,
                    "headers_count": len(msg_headers),
                    "bytes_length": len(raw_val),
                })

            details["verified_records"] = verified_checksums
            if len(consumed_records) < len(test_entries):
                return (
                    False,
                    len(consumed_records),
                    consumed_records,
                    details,
                    f"Expected {len(test_entries)} records with headers, but consumed {len(consumed_records)}",
                )

            logger.info(f"Successfully validated all {len(consumed_records)} records and headers.")
            return True, len(consumed_records), consumed_records, details, None

        finally:
            consumer.close()
