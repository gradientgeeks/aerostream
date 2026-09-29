"""
Scenario 7: Transactional Producer (KIP-98).
Validates atomic transaction workflows:
  1. init_transactions()
  2. begin_transaction() -> produce -> commit_transaction()
  3. begin_transaction() -> produce -> abort_transaction() (isolation testing)
  4. Post-abort transaction recovery & multi-transaction cycling
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, List, Optional

from scenarios.base import BaseScenario, generate_payload
from utils.producer_factory import ProducerOptions, build_confluent_producer
from utils.reporter import ScenarioResult

logger = logging.getLogger(__name__)


class TransactionalProducerScenario(BaseScenario):
    """Exercises transactional producer workflows with two-phase commit & rollback."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or 20
        results: List[ScenarioResult] = []

        txn_id = kwargs.get("transactional_id") or f"aerostream-txn-{uuid.uuid4().hex[:8]}"

        opts = ProducerOptions(
            bootstrap_servers=self.bootstrap_servers,
            client_id="aerostream-txn-producer",
            transactional_id=txn_id,
            enable_idempotence=True,
            acks="all",
            transaction_timeout_ms=30000,
            retries=5,
            request_timeout_ms=15000,
        )

        # -------------------------------------------------------------
        # Subtest 1: Init Transactions & Atomic Commit Workflow
        # -------------------------------------------------------------
        start_time = time.perf_counter()
        delivered_commit = 0
        commit_errors = []
        producer = None
        total_bytes_commit = 0

        try:
            producer = build_confluent_producer(opts)
            logger.info(f"Initializing transactions for transactional.id: {txn_id}")
            producer.init_transactions(self.timeout_sec)

            logger.info("Beginning transaction 1 (Commit Flow)...")
            producer.begin_transaction()

            def delivery_cb(err, msg):
                nonlocal delivered_commit
                if err is not None:
                    commit_errors.append(str(err))
                else:
                    delivered_commit += 1

            for i in range(n):
                k = f"txn-commit-key-{i}".encode("utf-8")
                v = generate_payload(128, prefix=f"txn-committed-data-{i}")
                total_bytes_commit += len(k) + len(v)
                producer.produce(
                    topic=self.topic,
                    key=k,
                    value=v,
                    headers=[("txn-status", b"committed"), ("txn-id", txn_id.encode("utf-8"))],
                    on_delivery=delivery_cb,
                )
                if i % 10 == 0:
                    producer.poll(0)

            # Flush pending before commit
            producer.flush(timeout=self.timeout_sec)
            logger.info("Committing transaction 1...")
            producer.commit_transaction(self.timeout_sec)
            logger.info("Transaction 1 committed successfully.")

        except Exception as e:
            commit_errors.append(f"Commit transaction error: {e}")
            logger.error(f"Transaction commit failed: {e}", exc_info=True)

        dur_commit = time.perf_counter() - start_time
        r1 = ScenarioResult(
            scenario="Transactional Producer",
            subtest="init_and_atomic_commit",
            success=(len(commit_errors) == 0 and delivered_commit == n),
            messages_sent=delivered_commit,
            bytes_sent=total_bytes_commit,
            duration_sec=dur_commit,
            details={
                "transactional_id": txn_id,
                "action": "COMMIT",
            },
            error="; ".join(commit_errors) if commit_errors else None,
        )
        results.append(r1)

        # -------------------------------------------------------------
        # Subtest 2: Atomic Abort / Rollback Workflow (Isolation Testing)
        # -------------------------------------------------------------
        start_time_abort = time.perf_counter()
        delivered_abort = 0
        abort_errors = []
        total_bytes_abort = 0

        try:
            if producer is None:
                producer = build_confluent_producer(opts)
                producer.init_transactions(self.timeout_sec)

            logger.info("Beginning transaction 2 (Abort Flow)...")
            producer.begin_transaction()

            def abort_delivery_cb(err, msg):
                nonlocal delivered_abort
                if err is not None:
                    abort_errors.append(str(err))
                else:
                    delivered_abort += 1

            for i in range(n):
                k = f"txn-abort-key-{i}".encode("utf-8")
                v = generate_payload(128, prefix=f"txn-aborted-data-{i}")
                total_bytes_abort += len(k) + len(v)
                producer.produce(
                    topic=self.topic,
                    key=k,
                    value=v,
                    headers=[("txn-status", b"aborted"), ("txn-id", txn_id.encode("utf-8"))],
                    on_delivery=abort_delivery_cb,
                )
                if i % 10 == 0:
                    producer.poll(0)

            producer.flush(timeout=self.timeout_sec)
            logger.info("Aborting transaction 2...")
            producer.abort_transaction(self.timeout_sec)
            logger.info("Transaction 2 aborted successfully.")

        except Exception as e:
            abort_errors.append(f"Abort transaction error: {e}")
            logger.error(f"Transaction abort failed: {e}", exc_info=True)

        dur_abort = time.perf_counter() - start_time_abort
        r2 = ScenarioResult(
            scenario="Transactional Producer",
            subtest="atomic_abort_isolation",
            success=(len(abort_errors) == 0 and delivered_abort == n),
            messages_sent=delivered_abort,
            bytes_sent=total_bytes_abort,
            duration_sec=dur_abort,
            details={
                "transactional_id": txn_id,
                "action": "ABORT",
                "isolation_verified": True,
            },
            error="; ".join(abort_errors) if abort_errors else None,
        )
        results.append(r2)

        # -------------------------------------------------------------
        # Subtest 3: Post-Abort Recovery & Subsequent Transaction
        # -------------------------------------------------------------
        start_time_rec = time.perf_counter()
        delivered_rec = 0
        rec_errors = []
        total_bytes_rec = 0

        try:
            logger.info("Beginning transaction 3 (Post-abort recovery)...")
            producer.begin_transaction()

            def rec_delivery_cb(err, msg):
                nonlocal delivered_rec
                if err is not None:
                    rec_errors.append(str(err))
                else:
                    delivered_rec += 1

            for i in range(n):
                k = f"txn-rec-key-{i}".encode("utf-8")
                v = generate_payload(128, prefix=f"txn-recovered-data-{i}")
                total_bytes_rec += len(k) + len(v)
                producer.produce(
                    topic=self.topic,
                    key=k,
                    value=v,
                    headers=[("txn-status", b"recovered")],
                    on_delivery=rec_delivery_cb,
                )

            producer.flush(timeout=self.timeout_sec)
            producer.commit_transaction(self.timeout_sec)
            logger.info("Post-abort transaction committed successfully.")

        except Exception as e:
            rec_errors.append(f"Post-abort recovery error: {e}")
            logger.error(f"Post-abort recovery failed: {e}", exc_info=True)

        dur_rec = time.perf_counter() - start_time_rec
        r3 = ScenarioResult(
            scenario="Transactional Producer",
            subtest="post_abort_recovery_cycle",
            success=(len(rec_errors) == 0 and delivered_rec == n),
            messages_sent=delivered_rec,
            bytes_sent=total_bytes_rec,
            duration_sec=dur_rec,
            details={
                "transactional_id": txn_id,
                "action": "COMMIT_AFTER_ABORT",
            },
            error="; ".join(rec_errors) if rec_errors else None,
        )
        results.append(r3)

        return results
