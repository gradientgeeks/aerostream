#!/usr/bin/env python3
"""
AeroStream Transactional Exactly-Once Semantics (EOS) at Scale Test.
Validates KIP-98 transactions across multi-partition input and output topics:
- 5,000+ messages produced with SHA-256 payload checksums.
- Read-Process-Write stream with transactional producer.
- Atomic commit of consumer offsets via send_offsets_to_transaction().
- Deliberate transaction aborts (every 5th batch) to test rollback.
- Strict validation with read_committed isolation level:
  * Zero duplicated records.
  * Zero aborted records leaked.
  * 100% committed records present.
  * 100% SHA-256 checksum verification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("aerostream.eos_scale")

BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
INPUT_TOPIC = "test-scale-eos-input"
OUTPUT_TOPIC = "test-scale-eos-output"
NUM_PARTITIONS = 3
DEFAULT_NUM_MESSAGES = 5000
DEFAULT_BATCH_SIZE = 100
ABORT_INTERVAL = 5  # Every 5th transaction is deliberately aborted


@dataclass
class EOSScaleMetrics:
    total_input_messages: int = 0
    seed_duration_sec: float = 0.0
    seed_throughput_msg_sec: float = 0.0
    seed_throughput_mb_sec: float = 0.0

    total_batches: int = 0
    committed_transactions: int = 0
    aborted_transactions: int = 0
    aborted_records_rolled_back: int = 0
    total_committed_records: int = 0

    processing_duration_sec: float = 0.0
    processing_throughput_msg_sec: float = 0.0

    read_committed_count: int = 0
    read_uncommitted_count: int = 0
    duplicates_detected: int = 0
    aborted_records_leaked: int = 0
    checksum_mismatches: int = 0
    validation_duration_sec: float = 0.0


def setup_topics(admin: AdminClient, input_topic: str, output_topic: str, num_partitions: int):
    """Ensure clean input and output topics with exactly num_partitions."""
    logger.info(f"Setting up clean topics '{input_topic}' and '{output_topic}' with {num_partitions} partitions...")
    
    # Try deleting existing topics for a clean test run
    try:
        del_fs = admin.delete_topics([input_topic, output_topic], operation_timeout=5.0)
        for t, f in del_fs.items():
            try:
                f.result()
                logger.info(f"Deleted previous topic '{t}'")
            except Exception:
                pass
        time.sleep(1.0)
    except Exception as e:
        logger.warning(f"Topic deletion note: {e}")

    # Create topics
    new_topics = [
        NewTopic(input_topic, num_partitions=num_partitions, replication_factor=1),
        NewTopic(output_topic, num_partitions=num_partitions, replication_factor=1),
    ]
    create_fs = admin.create_topics(new_topics, operation_timeout=10.0)
    for t, f in create_fs.items():
        try:
            f.result()
            logger.info(f"Successfully created topic '{t}' with {num_partitions} partitions")
        except Exception as e:
            if "exists" in str(e).lower():
                logger.info(f"Topic '{t}' already exists")
            else:
                raise

    # Wait for metadata propagation
    time.sleep(1.0)


def seed_input_topic(
    bootstrap: str, topic: str, num_messages: int, num_partitions: int
) -> tuple[Dict[int, dict], float, float]:
    """Produces 5,000+ messages with SHA-256 checksums into the input topic."""
    logger.info(f"--- Phase 1: Seeding {num_messages} messages into '{topic}' ---")
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 5,
        "batch.num.messages": 1000,
        "acks": "all",
    })

    records: Dict[int, dict] = {}
    total_bytes = 0
    t0 = time.perf_counter()

    for i in range(num_messages):
        partition = i % num_partitions
        payload_data = f"aeromq-eos-scale-seq-{i:07d}-entropy-{os.urandom(12).hex()}"
        sha256_csum = hashlib.sha256(payload_data.encode("utf-8")).hexdigest()
        msg_obj = {
            "id": i,
            "data": payload_data,
            "checksum": sha256_csum,
            "timestamp": time.time(),
        }
        val_bytes = json.dumps(msg_obj).encode("utf-8")
        total_bytes += len(val_bytes)
        records[i] = msg_obj

        producer.produce(
            topic,
            partition=partition,
            key=str(i).encode("utf-8"),
            value=val_bytes,
        )
        if i % 1000 == 0:
            producer.poll(0)

    producer.flush(15.0)
    elapsed = time.perf_counter() - t0
    throughput_msg = num_messages / elapsed if elapsed > 0 else 0
    throughput_mb = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0

    logger.info(
        f"Seeded {num_messages} messages ({total_bytes / 1024:.1f} KB) in {elapsed:.2f}s "
        f"({throughput_msg:.1f} msg/sec, {throughput_mb:.2f} MB/sec)"
    )
    return records, elapsed, total_bytes


def run_transactional_processor(
    bootstrap: str,
    input_topic: str,
    output_topic: str,
    num_messages: int,
    batch_size: int,
    num_partitions: int,
    abort_interval: int,
) -> tuple[Dict[int, dict], Set[int], EOSScaleMetrics]:
    """
    Executes Read-Process-Write stream with KIP-98 transactions:
    - Consumes batch from input topic.
    - begin_transaction()
    - Produces transformed records to output topic.
    - send_offsets_to_transaction() to atomically commit input offsets.
    - Deliberately aborts every `abort_interval`-th batch, rolls back, and retries.
    - Commits valid batches.
    """
    logger.info(f"--- Phase 2: Transactional Read-Process-Write Stream (Scale EOS) ---")
    metrics = EOSScaleMetrics()
    metrics.total_input_messages = num_messages

    run_id = int(time.time() * 1000)
    group_id = f"scale-eos-stream-group-{run_id}"
    txn_id = f"scale-eos-txn-coord-{run_id}"

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    consumer.subscribe([input_topic])

    txn_producer = Producer({
        "bootstrap.servers": bootstrap,
        "transactional.id": txn_id,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 0,
    })
    txn_producer.init_transactions()
    logger.info(f"Initialized transactional producer with transactional.id='{txn_id}'")

    committed_records: Dict[int, dict] = {}
    aborted_record_ids: Set[int] = set()

    processed_count = 0
    batch_index = 0
    t0 = time.perf_counter()

    # Pre-poll to ensure consumer partition assignment is established
    for _ in range(10):
        m = consumer.poll(0.2)
        if m:
            break

    while processed_count < num_messages:
        batch_index += 1
        batch_msgs = []
        start_offsets: Dict[int, int] = {}
        deadline = time.time() + 8.0

        # Collect batch
        while len(batch_msgs) < batch_size and time.time() < deadline:
            msg = consumer.poll(0.2)
            if msg and not msg.error():
                p = msg.partition()
                if p not in start_offsets:
                    start_offsets[p] = msg.offset()
                batch_msgs.append(msg)
                if (processed_count + len(batch_msgs)) >= num_messages:
                    break

        if not batch_msgs:
            logger.warning("Consumer poll timed out before collecting full batch.")
            break

        should_abort = (batch_index % abort_interval == 0)

        # -------------------------------------------------------------------
        # ATTEMPT 1: Process Batch
        # -------------------------------------------------------------------
        txn_producer.begin_transaction()

        for m in batch_msgs:
            in_obj = json.loads(m.value().decode("utf-8"))
            mid = in_obj["id"]
            transformed_payload = f"EOS_PROCESSED::{in_obj['data']}::TXN_{batch_index}"
            transformed_csum = hashlib.sha256(transformed_payload.encode("utf-8")).hexdigest()

            out_obj = {
                "id": mid,
                "data": transformed_payload,
                "checksum": transformed_csum,
                "source_checksum": in_obj["checksum"],
                "batch_index": batch_index,
                "is_aborted_attempt": should_abort,
            }

            out_partition = mid % num_partitions
            txn_producer.produce(
                output_topic,
                partition=out_partition,
                key=str(mid).encode("utf-8"),
                value=json.dumps(out_obj).encode("utf-8"),
            )

        txn_producer.flush()

        # Calculate consumer input partition offsets
        max_offsets: Dict[int, int] = {}
        for m in batch_msgs:
            p = m.partition()
            max_offsets[p] = max(max_offsets.get(p, -1), m.offset())
        tp_offsets = [TopicPartition(input_topic, p, off + 1) for p, off in max_offsets.items()]

        txn_producer.send_offsets_to_transaction(tp_offsets, consumer.consumer_group_metadata())

        if should_abort:
            # ---------------------------------------------------------------
            # DELIBERATE TRANSACTION ABORT: Rollback data and offsets
            # ---------------------------------------------------------------
            txn_producer.abort_transaction()
            metrics.aborted_transactions += 1
            metrics.aborted_records_rolled_back += len(batch_msgs)
            for m in batch_msgs:
                in_obj = json.loads(m.value().decode("utf-8"))
                aborted_record_ids.add(in_obj["id"])

            logger.info(
                f"[ABORT] Batch #{batch_index}: Aborted txn with {len(batch_msgs)} records. "
                f"Rolling back consumer offsets across partitions {list(start_offsets.keys())}..."
            )

            # Seek consumer back to start offsets of this batch
            for p, off in start_offsets.items():
                consumer.seek(TopicPartition(input_topic, p, off))

            # Re-read the batch
            retry_msgs = []
            retry_deadline = time.time() + 8.0
            while len(retry_msgs) < len(batch_msgs) and time.time() < retry_deadline:
                rm = consumer.poll(0.2)
                if rm and not rm.error():
                    retry_msgs.append(rm)

            assert len(retry_msgs) == len(batch_msgs), (
                f"Seek rollback re-read mismatch: got {len(retry_msgs)}, expected {len(batch_msgs)}"
            )

            # ---------------------------------------------------------------
            # RETRY & COMMIT: Exactly-once re-processing
            # ---------------------------------------------------------------
            txn_producer.begin_transaction()
            for rm in retry_msgs:
                in_obj = json.loads(rm.value().decode("utf-8"))
                mid = in_obj["id"]
                transformed_payload = f"EOS_PROCESSED::{in_obj['data']}::TXN_{batch_index}_COMMITTED"
                transformed_csum = hashlib.sha256(transformed_payload.encode("utf-8")).hexdigest()

                out_obj = {
                    "id": mid,
                    "data": transformed_payload,
                    "checksum": transformed_csum,
                    "source_checksum": in_obj["checksum"],
                    "batch_index": batch_index,
                    "is_aborted_attempt": False,
                }
                committed_records[mid] = out_obj

                out_partition = mid % num_partitions
                txn_producer.produce(
                    output_topic,
                    partition=out_partition,
                    key=str(mid).encode("utf-8"),
                    value=json.dumps(out_obj).encode("utf-8"),
                )

            txn_producer.flush()

            max_retry_offsets: Dict[int, int] = {}
            for rm in retry_msgs:
                p = rm.partition()
                max_retry_offsets[p] = max(max_retry_offsets.get(p, -1), rm.offset())
            retry_tp_offsets = [TopicPartition(input_topic, p, off + 1) for p, off in max_retry_offsets.items()]

            txn_producer.send_offsets_to_transaction(retry_tp_offsets, consumer.consumer_group_metadata())
            txn_producer.commit_transaction()

            metrics.committed_transactions += 1
            processed_count += len(retry_msgs)
            logger.info(
                f"[COMMIT-RETRY] Batch #{batch_index}: Successfully retried and committed "
                f"{len(retry_msgs)} records. Progress: {processed_count}/{num_messages}"
            )
        else:
            # Commit clean batch
            for m in batch_msgs:
                in_obj = json.loads(m.value().decode("utf-8"))
                mid = in_obj["id"]
                transformed_payload = f"EOS_PROCESSED::{in_obj['data']}::TXN_{batch_index}"
                transformed_csum = hashlib.sha256(transformed_payload.encode("utf-8")).hexdigest()
                out_obj = {
                    "id": mid,
                    "data": transformed_payload,
                    "checksum": transformed_csum,
                    "source_checksum": in_obj["checksum"],
                    "batch_index": batch_index,
                    "is_aborted_attempt": False,
                }
                committed_records[mid] = out_obj

            txn_producer.commit_transaction()
            metrics.committed_transactions += 1
            processed_count += len(batch_msgs)
            if batch_index % 10 == 0 or processed_count >= num_messages:
                logger.info(
                    f"[COMMIT] Batch #{batch_index}: Committed {len(batch_msgs)} records. "
                    f"Progress: {processed_count}/{num_messages}"
                )

    consumer.close()
    elapsed = time.perf_counter() - t0
    metrics.total_batches = batch_index
    metrics.total_committed_records = len(committed_records)
    metrics.processing_duration_sec = elapsed
    metrics.processing_throughput_msg_sec = processed_count / elapsed if elapsed > 0 else 0

    logger.info(
        f"Read-Process-Write Stream completed: {metrics.committed_transactions} committed transactions, "
        f"{metrics.aborted_transactions} aborted transactions, {processed_count} messages in {elapsed:.2f}s "
        f"({metrics.processing_throughput_msg_sec:.1f} msg/sec)"
    )
    return committed_records, aborted_record_ids, metrics


def validate_read_committed(
    bootstrap: str,
    output_topic: str,
    expected_committed: Dict[int, dict],
    aborted_ids: Set[int],
    input_records: Dict[int, dict],
    num_messages: int,
) -> tuple[int, int, int, int, float]:
    """
    Validates with isolation.level=read_committed:
    - Exactly 100% of committed records present.
    - ZERO duplicates.
    - ZERO aborted records leaked.
    - 100% SHA-256 checksums verified.
    """
    logger.info("--- Phase 3: Validating Output with isolation.level=read_committed ---")
    val_group = f"scale-eos-validator-{int(time.time() * 1000)}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": val_group,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.auto.commit": False,
    })
    consumer.subscribe([output_topic])

    received_records: List[dict] = []
    seen_ids: Set[int] = set()
    duplicates = 0
    aborted_leaked = 0
    checksum_errors = 0

    t0 = time.perf_counter()
    deadline = time.time() + 15.0

    while time.time() < deadline and len(received_records) < num_messages:
        msg = consumer.poll(0.2)
        if msg and not msg.error():
            rec = json.loads(msg.value().decode("utf-8"))
            mid = rec["id"]

            if mid in seen_ids:
                duplicates += 1
                logger.error(f"DUPLICATE RECORD DETECTED: id={mid}")
            seen_ids.add(mid)

            if rec.get("is_aborted_attempt", False):
                aborted_leaked += 1
                logger.error(f"ABORTED RECORD LEAKED TO READ_COMMITTED: id={mid}")

            # Verify checksum
            expected_csum = hashlib.sha256(rec["data"].encode("utf-8")).hexdigest()
            if rec["checksum"] != expected_csum:
                checksum_errors += 1
                logger.error(f"CHECKSUM MISMATCH for id={mid}: got {rec['checksum']}, expected {expected_csum}")

            if rec["source_checksum"] != input_records[mid]["checksum"]:
                checksum_errors += 1
                logger.error(f"SOURCE CHECKSUM MISMATCH for id={mid}")

            received_records.append(rec)

    consumer.close()
    elapsed = time.perf_counter() - t0

    logger.info(
        f"Read-committed Consumer received {len(received_records)} records in {elapsed:.2f}s "
        f"({len(received_records) / elapsed if elapsed > 0 else 0:.1f} msg/sec)"
    )
    return len(received_records), duplicates, aborted_leaked, checksum_errors, elapsed


def validate_read_uncommitted_audit(
    bootstrap: str,
    output_topic: str,
    expected_total_physical: int,
) -> int:
    """
    Audits the log with isolation.level=read_uncommitted to confirm
    that aborted transaction records are physically present in the log and
    were actively filtered by read_committed.
    """
    logger.info("--- Phase 4: Auditing with isolation.level=read_uncommitted (Proof of Physical Storage) ---")
    audit_group = f"scale-eos-uncommitted-audit-{int(time.time() * 1000)}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": audit_group,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_uncommitted",
        "enable.auto.commit": False,
    })
    consumer.subscribe([output_topic])

    count = 0
    deadline = time.time() + 8.0
    while time.time() < deadline:
        msg = consumer.poll(0.2)
        if msg and not msg.error():
            count += 1
            if count >= expected_total_physical:
                break
        elif count > 0 and time.time() > deadline - 3.0:
            # Drain done
            break

    consumer.close()
    logger.info(f"Read-uncommitted Consumer audited {count} total physical records (including aborted batches).")
    return count


def run_eos_scale_test(num_messages: int = DEFAULT_NUM_MESSAGES, batch_size: int = DEFAULT_BATCH_SIZE) -> bool:
    print("\n" + "=" * 80)
    print(" AEROSTREAM EXACTLY-ONCE SEMANTICS (EOS) AT SCALE VERIFICATION")
    print(f" Target: {BOOTSTRAP_SERVER}")
    print(f" Messages: {num_messages:,} | Partitions: {NUM_PARTITIONS} | Batch Size: {batch_size}")
    print("=" * 80 + "\n")

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})

    # Setup clean topics
    setup_topics(admin, INPUT_TOPIC, OUTPUT_TOPIC, NUM_PARTITIONS)

    # 1. Seed input topic
    input_records, seed_dur, total_bytes = seed_input_topic(
        BOOTSTRAP_SERVER, INPUT_TOPIC, num_messages, NUM_PARTITIONS
    )

    # 2. Transactional Processor (Read-Process-Write stream)
    committed_records, aborted_ids, metrics = run_transactional_processor(
        BOOTSTRAP_SERVER,
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        num_messages,
        batch_size,
        NUM_PARTITIONS,
        ABORT_INTERVAL,
    )
    metrics.seed_duration_sec = seed_dur
    metrics.seed_throughput_msg_sec = num_messages / seed_dur if seed_dur > 0 else 0
    metrics.seed_throughput_mb_sec = (total_bytes / (1024 * 1024)) / seed_dur if seed_dur > 0 else 0

    # 3. Read Committed Validation
    (
        rc_count,
        dups,
        leaked,
        csum_errs,
        val_dur,
    ) = validate_read_committed(
        BOOTSTRAP_SERVER,
        OUTPUT_TOPIC,
        committed_records,
        aborted_ids,
        input_records,
        num_messages,
    )
    metrics.read_committed_count = rc_count
    metrics.duplicates_detected = dups
    metrics.aborted_records_leaked = leaked
    metrics.checksum_mismatches = csum_errs
    metrics.validation_duration_sec = val_dur

    # 4. Read Uncommitted Audit (Proof of Aborted Records in Log)
    expected_physical = num_messages + metrics.aborted_records_rolled_back
    uncommitted_count = validate_read_uncommitted_audit(
        BOOTSTRAP_SERVER, OUTPUT_TOPIC, expected_physical
    )
    metrics.read_uncommitted_count = uncommitted_count

    # Assertions
    passed = True
    assert rc_count == num_messages, f"Expected {num_messages} committed records, got {rc_count}"
    assert dups == 0, f"Detected {dups} duplicated records!"
    assert leaked == 0, f"Detected {leaked} leaked aborted records!"
    assert csum_errs == 0, f"Detected {csum_errs} checksum mismatches!"
    assert uncommitted_count > rc_count, (
        f"Expected read_uncommitted ({uncommitted_count}) > read_committed ({rc_count}) due to aborted batches!"
    )

    # Summary Report Table
    print("\n" + "=" * 80)
    print("                      EOS SCALE VERIFICATION RESULTS                      ")
    print("=" * 80)
    print(f" {'Metric':<40} | {'Value':<35}")
    print("-" * 80)
    print(f" {'Input Topic Messages Seeded':<40} | {num_messages:,}")
    print(f" {'Seed Throughput':<40} | {metrics.seed_throughput_msg_sec:,.1f} msg/sec ({metrics.seed_throughput_mb_sec:.2f} MB/sec)")
    print(f" {'Total Processing Batches':<40} | {metrics.total_batches}")
    print(f" {'Committed Transactions':<40} | {metrics.committed_transactions}")
    print(f" {'Deliberately Aborted Transactions':<40} | {metrics.aborted_transactions}")
    print(f" {'Aborted Records Rolled Back & Retried':<40} | {metrics.aborted_records_rolled_back:,}")
    print(f" {'Transactional Processor Throughput':<40} | {metrics.processing_throughput_msg_sec:,.1f} msg/sec")
    print(f" {'Read-Committed Consumer Count':<40} | {metrics.read_committed_count:,} / {num_messages:,} (100.0%)")
    print(f" {'Read-Uncommitted Audit Count (Log Total)':<40} | {metrics.read_uncommitted_count:,}")
    print(f" {'Duplicated Records Detected':<40} | {metrics.duplicates_detected} (ZERO)")
    print(f" {'Aborted Records Leaked':<40} | {metrics.aborted_records_leaked} (ZERO)")
    print(f" {'SHA-256 Checksum Mismatches':<40} | {metrics.checksum_mismatches} (ZERO)")
    print(f" {'Overall Test Status':<40} | {'PASSED (EXACTLY-ONCE GUARANTEED)' if passed else 'FAILED'}")
    print("=" * 80 + "\n")

    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AeroStream Scale EOS Verification")
    parser.add_argument("--messages", type=int, default=DEFAULT_NUM_MESSAGES, help="Total input messages (default: 5000)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size per transaction (default: 100)")
    args = parser.parse_args()

    success = run_eos_scale_test(args.messages, args.batch_size)
    sys.exit(0 if success else 1)
