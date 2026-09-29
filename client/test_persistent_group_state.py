#!/usr/bin/env python3
"""
AeroStream Persistent Consumer Group State Across Cluster Restarts Test.
Validates end-to-end consumer group offset persistence across full broker/controller restart:
- Seeds 100 messages into topic 'test-group-persistence'.
- Consumer Group A joins, consumes 50 out of 100 messages (0-49), and explicitly commits offset 50 (commitSync).
- Queries and verifies committed offset 50 via OffsetFetch API.
- Gracefully restarts the AeroStream cluster container (docker restart).
- Awaits controller health and Kafka broker socket readiness.
- Consumer Group A resumes with a brand new consumer instance using the identical group.id.
- Asserts resumption begins EXACTLY at offset 50 (does not start at 0, does not skip messages).
- Consumes remaining messages 50-99 and commits final offset 100.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("aerostream.group_persistence")

BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
CONTROLLER_HTTP = os.environ.get("CONTROLLER_HTTP", "http://127.0.0.1:9001")
TOPIC_NAME = "test-group-persistence"
DEFAULT_TOTAL_MESSAGES = 100
DEFAULT_CHECKPOINT_OFFSET = 50
CONTAINER_NAME = "aerostream-test-cluster"


@dataclass
class PersistenceMetrics:
    total_seeded: int = 0
    phase1_consumed: int = 0
    phase1_last_offset: int = -1
    offset_fetch_pre_restart: int = -1

    restart_duration_sec: float = 0.0
    offset_fetch_post_restart: int = -1

    phase2_resumed_offset: int = -1
    phase2_consumed: int = 0
    phase2_last_offset: int = -1
    final_committed_offset: int = -1

    resumed_from_exact_checkpoint: bool = False
    no_restart_at_zero: bool = False
    no_skipped_messages: bool = False
    all_messages_accounted_for: bool = False


def setup_topic(admin: AdminClient, topic: str):
    """Recreate topic with 1 partition to guarantee deterministic offset tracking."""
    logger.info(f"Setting up clean topic '{topic}' (1 partition)...")
    try:
        del_fs = admin.delete_topics([topic], operation_timeout=5.0)
        for t, f in del_fs.items():
            try:
                f.result()
                logger.info(f"Deleted previous topic '{t}'")
            except Exception:
                pass
        time.sleep(1.0)
    except Exception as e:
        logger.warning(f"Delete topic note: {e}")

    create_fs = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)], operation_timeout=10.0)
    for t, f in create_fs.items():
        try:
            f.result()
            logger.info(f"Created topic '{t}' successfully")
        except Exception as e:
            if "exists" in str(e).lower():
                logger.info(f"Topic '{t}' already exists")
            else:
                raise
    time.sleep(1.0)


def seed_messages(bootstrap: str, topic: str, count: int) -> Dict[int, dict]:
    """Produces count sequential records with checksums into partition 0."""
    logger.info(f"--- Phase 1: Seeding {count} records into '{topic}' ---")
    p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 0, "acks": "all"})
    seeded = {}

    for i in range(count):
        payload_data = f"persistent-group-state-record-{i:04d}"
        csum = hashlib.sha256(payload_data.encode("utf-8")).hexdigest()
        msg_obj = {"seq": i, "data": payload_data, "checksum": csum}
        seeded[i] = msg_obj
        p.produce(
            topic,
            partition=0,
            key=str(i).encode("utf-8"),
            value=json.dumps(msg_obj).encode("utf-8"),
        )

    p.flush(10.0)
    logger.info(f"Successfully seeded {count} messages (offsets 0..{count - 1}).")
    return seeded


def run_phase1_consumption(
    bootstrap: str, topic: str, group_id: str, limit: int
) -> tuple[List[dict], int, int]:
    """
    Consumer Group A joins, consumes `limit` messages (0..limit-1),
    and explicitly commits offset `limit` (commitSync).
    Returns (consumed_records, last_offset, committed_offset).
    """
    logger.info(
        f"--- Phase 2: Consumer Group '{group_id}' Consuming first {limit} messages ---"
    )
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "client.id": "consumer-instance-phase-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    consumed: List[dict] = []
    deadline = time.time() + 15.0

    while time.time() < deadline and len(consumed) < limit:
        msg = consumer.poll(0.2)
        if msg and not msg.error():
            rec = json.loads(msg.value().decode("utf-8"))
            rec["_offset"] = msg.offset()
            consumed.append(rec)

    assert len(consumed) == limit, f"Phase 1 consumption incomplete: got {len(consumed)}/{limit}"
    last_offset = consumed[-1]["_offset"]
    logger.info(
        f"Consumer Instance 1 read {len(consumed)} records. "
        f"First offset: {consumed[0]['_offset']}, Last offset: {last_offset}"
    )

    # Explicitly commit offset (limit) synchronously (commitSync equivalent)
    target_offset = limit
    tp = TopicPartition(topic, 0, target_offset)
    logger.info(f"Explicitly committing offset {target_offset} (commitSync)...")
    consumer.commit(offsets=[tp], asynchronous=False)

    # Query committed offset via OffsetFetch API
    committed_tps = consumer.committed([TopicPartition(topic, 0)], timeout=5.0)
    committed_offset = committed_tps[0].offset if committed_tps else -1
    logger.info(f"OffsetFetch API returned committed offset: {committed_offset}")
    assert committed_offset == target_offset, (
        f"Committed offset mismatch: got {committed_offset}, expected {target_offset}"
    )

    consumer.close()
    logger.info("Consumer Instance 1 closed cleanly.")
    return consumed, last_offset, committed_offset


def restart_aerostream_cluster(container_name: str) -> float:
    """Gracefully restarts the AeroStream container and waits for broker & controller readiness."""
    logger.info(f"--- Phase 3: Gracefully Restarting AeroStream Cluster ({container_name}) ---")
    t0 = time.perf_counter()

    # Execute docker restart
    res = subprocess.run(
        ["docker", "restart", container_name],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"Failed to restart container: {res.stderr}")

    restart_dur = time.perf_counter() - t0
    logger.info(f"Container restart signal completed in {restart_dur:.2f}s. Awaiting service readiness...")

    # Wait for Kafka wire port (9092) and Controller HTTP port (9001)
    deadline = time.time() + 30.0
    ready = False
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", 9092), timeout=1.0) as s:
                pass
            with socket.create_connection(("127.0.0.1", 9001), timeout=1.0) as s:
                pass
            ready = True
            break
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.3)

    if not ready:
        raise TimeoutError("AeroStream cluster failed to come back online within 30s")

    # Additional grace period for broker-controller heartbeat and topology exchange
    time.sleep(2.5)
    total_dur = time.perf_counter() - t0
    logger.info(f"AeroStream Cluster is fully ONLINE and HEALTHY (total elapsed: {total_dur:.2f}s)")
    return total_dur


def verify_offset_fetch_after_restart(bootstrap: str, topic: str, group_id: str) -> int:
    """Queries OffsetFetch API immediately after restart to confirm durable persistence."""
    logger.info(f"Querying OffsetFetch API immediately after cluster restart for group '{group_id}'...")
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    # Query committed offset without consuming
    committed_tps = consumer.committed([TopicPartition(topic, 0)], timeout=10.0)
    committed_offset = committed_tps[0].offset if committed_tps else -1
    consumer.close()
    logger.info(f"Post-restart OffsetFetch API confirmed persisted offset: {committed_offset}")
    return committed_offset


def run_phase2_resumption(
    bootstrap: str, topic: str, group_id: str, expected_start: int, total_count: int
) -> tuple[List[dict], PersistenceMetrics]:
    """
    Consumer Group A resumes with a brand new consumer instance.
    Validates:
    - Resumes EXACTLY from expected_start (offset 50).
    - Does NOT start at 0 (no state loss).
    - Does NOT skip messages (sequential 50..99).
    - Consumes all remaining messages to completion.
    """
    logger.info(
        f"--- Phase 4: Consumer Group '{group_id}' Resuming with New Consumer Instance ---"
    )
    expected_remaining = total_count - expected_start
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "client.id": "consumer-instance-phase-2-resumed",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    consumed: List[dict] = []
    deadline = time.time() + 20.0

    while time.time() < deadline and len(consumed) < expected_remaining:
        msg = consumer.poll(0.2)
        if msg and not msg.error():
            rec = json.loads(msg.value().decode("utf-8"))
            rec["_offset"] = msg.offset()
            consumed.append(rec)

    metrics = PersistenceMetrics()
    metrics.phase2_consumed = len(consumed)
    assert len(consumed) == expected_remaining, (
        f"Phase 2 resumption incomplete: got {len(consumed)}/{expected_remaining}"
    )

    first_offset = consumed[0]["_offset"]
    last_offset = consumed[-1]["_offset"]
    metrics.phase2_resumed_offset = first_offset
    metrics.phase2_last_offset = last_offset

    logger.info(
        f"Consumer Instance 2 read {len(consumed)} records. "
        f"First offset: {first_offset}, Last offset: {last_offset}"
    )

    # 1. Assert resumption does NOT start at 0
    metrics.no_restart_at_zero = (first_offset != 0)
    assert first_offset != 0, (
        f"CRITICAL STATE LOSS: Consumer resumed at offset 0 instead of committed offset {expected_start}!"
    )

    # 2. Assert resumption begins EXACTLY at offset 50
    metrics.resumed_from_exact_checkpoint = (first_offset == expected_start)
    assert first_offset == expected_start, (
        f"OFFSET DRIFT: Expected resumption at offset {expected_start}, but started at {first_offset}!"
    )

    # 3. Assert no skipped messages (strictly sequential from 50 to 99)
    sequential = True
    for idx, r in enumerate(consumed):
        expected_off = expected_start + idx
        if r["_offset"] != expected_off or r["seq"] != expected_off:
            sequential = False
            logger.error(
                f"SEQUENCE GAP: Index {idx} has offset={r['_offset']}, seq={r['seq']}, expected {expected_off}"
            )
            break
    metrics.no_skipped_messages = sequential
    assert sequential, "Consumer skipped messages during resumption!"

    # Final commit of offset 100
    tp_final = TopicPartition(topic, 0, total_count)
    consumer.commit(offsets=[tp_final], asynchronous=False)
    final_committed = consumer.committed([TopicPartition(topic, 0)], timeout=5.0)
    metrics.final_committed_offset = final_committed[0].offset if final_committed else -1
    logger.info(f"Committed final offset {metrics.final_committed_offset} to complete stream.")

    consumer.close()
    return consumed, metrics


def run_persistent_group_state_test(
    total_messages: int = DEFAULT_TOTAL_MESSAGES,
    checkpoint_offset: int = DEFAULT_CHECKPOINT_OFFSET,
) -> bool:
    print("\n" + "=" * 80)
    print(" AEROSTREAM PERSISTENT CONSUMER GROUP STATE ACROSS CLUSTER RESTARTS")
    print(f" Target: {BOOTSTRAP_SERVER} | Topic: {TOPIC_NAME}")
    print(f" Total Messages: {total_messages} | Checkpoint Offset: {checkpoint_offset}")
    print("=" * 80 + "\n")

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
    setup_topic(admin, TOPIC_NAME)

    # 1. Seed messages
    seeded_records = seed_messages(BOOTSTRAP_SERVER, TOPIC_NAME, total_messages)

    # 2. Phase 1 Consumption & Explicit Commit
    group_id = f"persistent-state-group-{int(time.time() * 1000)}"
    (
        p1_records,
        p1_last_off,
        p1_committed_off,
    ) = run_phase1_consumption(
        BOOTSTRAP_SERVER, TOPIC_NAME, group_id, checkpoint_offset
    )

    # 3. Restart Cluster
    restart_dur = restart_aerostream_cluster(CONTAINER_NAME)

    # 4. Immediate OffsetFetch verification after restart
    post_restart_committed = verify_offset_fetch_after_restart(
        BOOTSTRAP_SERVER, TOPIC_NAME, group_id
    )

    # 5. Phase 2 Resumption with fresh consumer instance
    p2_records, metrics = run_phase2_resumption(
        BOOTSTRAP_SERVER, TOPIC_NAME, group_id, checkpoint_offset, total_messages
    )

    metrics.total_seeded = total_messages
    metrics.phase1_consumed = len(p1_records)
    metrics.phase1_last_offset = p1_last_off
    metrics.offset_fetch_pre_restart = p1_committed_off
    metrics.restart_duration_sec = restart_dur
    metrics.offset_fetch_post_restart = post_restart_committed
    metrics.all_messages_accounted_for = (
        len(p1_records) + len(p2_records) == total_messages
    )

    passed = (
        metrics.resumed_from_exact_checkpoint
        and metrics.no_restart_at_zero
        and metrics.no_skipped_messages
        and metrics.all_messages_accounted_for
        and (metrics.offset_fetch_post_restart == checkpoint_offset)
    )

    # Print Summary Table
    print("\n" + "=" * 80)
    print("                 PERSISTENT GROUP STATE VERIFICATION RESULTS                ")
    print("=" * 80)
    print(f" {'Metric':<42} | {'Value':<33}")
    print("-" * 80)
    print(f" {'Total Seeded Messages':<42} | {metrics.total_seeded}")
    print(f" {'Phase 1 Consumed Messages (0-49)':<42} | {metrics.phase1_consumed}")
    print(f" {'Phase 1 Last Consumed Offset':<42} | {metrics.phase1_last_offset}")
    print(f" {'Pre-Restart OffsetFetch Committed Offset':<42} | {metrics.offset_fetch_pre_restart}")
    print(f" {'Cluster Graceful Restart Duration':<42} | {metrics.restart_duration_sec:.2f} seconds")
    print(f" {'Post-Restart OffsetFetch Committed Offset':<42} | {metrics.offset_fetch_post_restart}")
    print(f" {'Phase 2 Resumption Start Offset':<42} | {metrics.phase2_resumed_offset} (Expected: {checkpoint_offset})")
    print(f" {'Phase 2 Consumed Messages (50-99)':<42} | {metrics.phase2_consumed}")
    print(f" {'Phase 2 Last Consumed Offset':<42} | {metrics.phase2_last_offset}")
    print(f" {'Final Stream Committed Offset':<42} | {metrics.final_committed_offset}")
    print(f" {'Resumed Exactly at Checkpoint 50':<42} | {'YES' if metrics.resumed_from_exact_checkpoint else 'NO'}")
    print(f" {'Did NOT Restart at Offset 0':<42} | {'YES (No state loss)' if metrics.no_restart_at_zero else 'NO (FAILED)'}")
    print(f" {'Zero Skipped / Zero Missing Messages':<42} | {'YES (Sequential 50..99)' if metrics.no_skipped_messages else 'NO (FAILED)'}")
    print(f" {'100% of Messages Accounted For':<42} | {'YES (50 + 50 = 100)' if metrics.all_messages_accounted_for else 'NO'}")
    print(f" {'Overall Test Status':<42} | {'PASSED (PERSISTENCE VERIFIED)' if passed else 'FAILED'}")
    print("=" * 80 + "\n")

    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AeroStream Persistent Group State Verification")
    parser.add_argument("--messages", type=int, default=DEFAULT_TOTAL_MESSAGES, help="Total messages (default: 100)")
    parser.add_argument("--checkpoint", type=int, default=DEFAULT_CHECKPOINT_OFFSET, help="Checkpoint offset (default: 50)")
    args = parser.parse_args()

    success = run_persistent_group_state_test(args.messages, args.checkpoint)
    sys.exit(0 if success else 1)
