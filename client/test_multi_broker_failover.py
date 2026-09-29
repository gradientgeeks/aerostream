#!/usr/bin/env python3
"""
AeroStream Multi-Broker High Availability & Automated Leader Failover Test Suite.

Tests:
1. Multi-broker coordination: Spawns Broker 2 and Broker 3, verifies registrations.
2. Topic Provisioning with Replicas: Multi-partition topic with RF=2.
3. Replication validation: Verifies follower replicates from partition leader.
4. Failure Injection: Kills partition leader (Broker 2).
5. Controller Failure Detection: Measures broker inactive timeout & heartbeat lease expiry.
6. Automatic Leader Election: Validates promotion of in-sync replica (Broker 3).
7. Client Seamless Reconnect & Zero Message Loss: Produces & consumes across failover boundary.
8. Broker Recovery: Restarts killed broker, validates re-registration and ISR catchup.
9. Cleanup: Gracefully terminates test broker instances and cleans temp files.
"""

import os
import sys
import time
import json
import signal
import shutil
import subprocess
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

# Ensure confluent-kafka is available
try:
    import confluent_kafka
    from confluent_kafka import Producer, Consumer, TopicPartition, KafkaError
except ImportError:
    print("FATAL: confluent-kafka not found. Run with `uv run --with confluent-kafka python3 ...`")
    sys.exit(1)


# Configuration constants
CONTROLLER_HTTP = "http://127.0.0.1:9001"
CONTROLLER_GRPC = "http://127.0.0.1:8001"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BROKER_BIN = os.path.join(PROJECT_ROOT, "rust-broker", "target", "release", "rust-broker")

BROKER1_BOOTSTRAP = "127.0.0.1:9092"
BROKER2_BOOTSTRAP = "127.0.0.1:9094"
BROKER3_BOOTSTRAP = "127.0.0.1:9096"
ALL_BOOTSTRAP = f"{BROKER1_BOOTSTRAP},{BROKER2_BOOTSTRAP},{BROKER3_BOOTSTRAP}"

BROKER2_DIR = "/tmp/broker2_failover_data"
BROKER3_DIR = "/tmp/broker3_failover_data"

TOPIC_NAME = "ha-failover-replicated-test"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 2


def http_get(endpoint: str, timeout: float = 3.0) -> Any:
    url = f"{CONTROLLER_HTTP}{endpoint}"
    req = urllib.request.Request(url, headers={"User-Agent": "AeroStream-Failover-Test"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post(endpoint: str, payload: dict, timeout: float = 5.0) -> Any:
    url = f"{CONTROLLER_HTTP}{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "AeroStream-Failover-Test"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def log_step(step_num: int, title: str):
    print(f"\n{'='*75}")
    print(f" [STEP {step_num}] {title}")
    print(f"{'='*75}")


class BrokerProcessManager:
    def __init__(self):
        self.procs: Dict[int, subprocess.Popen] = {}
        self.log_files: Dict[int, Any] = {}

    def start_broker(self, broker_id: int, data_port: int, kafka_port: int, storage_dir: str) -> subprocess.Popen:
        os.makedirs(storage_dir, exist_ok=True)
        log_path = f"/tmp/broker_{broker_id}_test.log"
        log_f = open(log_path, "w")
        self.log_files[broker_id] = log_f

        cmd = [
            BROKER_BIN,
            "--id", str(broker_id),
            "--host", "127.0.0.1",
            "--data-port", str(data_port),
            "--kafka-port", str(kafka_port),
            "--controller", CONTROLLER_GRPC,
            "--storage-dir", storage_dir,
        ]
        print(f"Starting Broker {broker_id} (Data: {data_port}, Kafka: {kafka_port}, Dir: {storage_dir})...")
        proc = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid
        )
        self.procs[broker_id] = proc
        print(f"Broker {broker_id} started with PID: {proc.pid}")
        return proc

    def stop_broker(self, broker_id: int, sig=signal.SIGKILL):
        proc = self.procs.get(broker_id)
        if proc and proc.poll() is None:
            print(f"Killing Broker {broker_id} (PID: {proc.pid}) with signal {sig}...")
            try:
                os.killpg(os.getpgid(proc.pid), sig)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)
            print(f"Broker {broker_id} stopped.")

    def cleanup_all(self):
        print("\nCleaning up managed broker processes...")
        for broker_id in list(self.procs.keys()):
            self.stop_broker(broker_id, signal.SIGKILL)
        for f in self.log_files.values():
            try:
                f.close()
            except Exception:
                pass
        for d in [BROKER2_DIR, BROKER3_DIR]:
            if os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)
        print("Cleanup complete.")


def wait_for_broker_state(broker_id: int, expected_active: bool, timeout_sec: float = 20.0) -> float:
    """Poll controller until broker has expected active status. Returns elapsed time."""
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        try:
            brokers = http_get("/api/brokers")
            for b in brokers:
                if b["id"] == broker_id:
                    if b.get("active") == expected_active:
                        elapsed = time.time() - start_time
                        return elapsed
        except Exception as e:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"Broker {broker_id} did not reach active={expected_active} within {timeout_sec}s")


def get_partition_state(topic: str, partition: int) -> dict:
    topics = http_get("/api/topics")
    for t in topics:
        if t["name"] == topic:
            for p in t["partitions"]:
                if p["partition_id"] == partition:
                    return p
    raise ValueError(f"Partition {topic}[{partition}] not found in metadata")


def run_failover_test():
    mgr = BrokerProcessManager()
    metrics = {}

    try:
        # ---------------------------------------------------------
        # Step 1: Examine Multi-Broker Coordination & Startup
        # ---------------------------------------------------------
        log_step(1, "Cluster Discovery & Multi-Broker Orchestration")
        if not os.path.isfile(BROKER_BIN):
            raise FileNotFoundError(f"rust-broker binary not found at {BROKER_BIN}")

        # Clean existing test data dirs
        for d in [BROKER2_DIR, BROKER3_DIR]:
            shutil.rmtree(d, ignore_errors=True)

        # Check existing cluster state
        cluster_info = http_get("/api/cluster")
        print(f"Controller Leader: {cluster_info.get('raft_leader')}, Raft State: {cluster_info.get('raft_state')}")

        # Start Broker 2 and Broker 3
        mgr.start_broker(broker_id=2, data_port=19092, kafka_port=9094, storage_dir=BROKER2_DIR)
        mgr.start_broker(broker_id=3, data_port=19093, kafka_port=9096, storage_dir=BROKER3_DIR)

        # Wait for all 3 brokers to report active
        print("Waiting for all 3 brokers to register and heartbeat with Controller...")
        for bid in [1, 2, 3]:
            wait_for_broker_state(bid, expected_active=True, timeout_sec=10.0)

        brokers = http_get("/api/brokers")
        print("\nRegistered Cluster Brokers:")
        for b in brokers:
            print(f"  * Broker {b['id']}: host={b['host']}, data_port={b['port']}, kafka_port={b.get('kafka_port')}, active={b['active']}")

        assert len(brokers) >= 3, f"Expected 3 active brokers, got {len(brokers)}"

        # ---------------------------------------------------------
        # Step 2: Provision Multi-Partition Replicated Topic
        # ---------------------------------------------------------
        log_step(2, "Topic Provisioning with Replicas")
        test_topic = f"{TOPIC_NAME}-{int(time.time())}"
        topic_payload = {
            "name": test_topic,
            "partitions": NUM_PARTITIONS,
            "replication_factor": REPLICATION_FACTOR
        }
        create_resp = http_post("/api/topics", topic_payload)
        print(f"Topic creation response: {create_resp}")

        # Wait for topic to propagate to controller and broker topology caches (~2s refresh interval)
        print("Waiting for topic topology to propagate across cluster...")
        time.sleep(2.5)

        topics = http_get("/api/topics")
        target_topic = next((t for t in topics if t["name"] == test_topic), None)
        assert target_topic is not None, f"Topic {test_topic} was not created!"

        print(f"\nTopic '{test_topic}' Partition Layout:")
        target_partition = None

        for p in target_topic["partitions"]:
            pid = p["partition_id"]
            leader = p["leader_id"]
            reps = p["replica_ids"]
            isr = p["isr"]
            print(f"  * Partition {pid}: Leader={leader}, Replicas={reps}, ISR={isr}")
            # Target partition where leader is Broker 2 and Broker 3 is in replicas
            if leader == 2 and 3 in reps:
                target_partition = pid

        if target_partition is None:
            for p in target_topic["partitions"]:
                if p["leader_id"] == 2:
                    target_partition = p["partition_id"]
                    break

        if target_partition is None:
            target_partition = 1

        initial_p_state = get_partition_state(test_topic, target_partition)
        initial_leader = initial_p_state["leader_id"]
        initial_replicas = initial_p_state["replica_ids"]
        initial_isr = initial_p_state["isr"]
        backup_replica = [r for r in initial_replicas if r != initial_leader][0]

        print(f"\n>>> Selected Target Partition for Failover: Partition {target_partition}")
        print(f"    Active Leader: Broker {initial_leader}")
        print(f"    Replica Set:   {initial_replicas}")
        print(f"    Initial ISR:   {initial_isr}")
        print(f"    Target Follower for Promotion: Broker {backup_replica}")

        # ---------------------------------------------------------
        # Step 3: Pre-Failover Active Message Flow & Replication
        # ---------------------------------------------------------
        log_step(3, "Pre-Failover Message Flow & Follower Replication Verification")
        producer_conf = {
            'bootstrap.servers': ALL_BOOTSTRAP,
            'topic.metadata.refresh.interval.ms': 1000,
            'metadata.max.age.ms': 1000,
            'retries': 30,
            'retry.backoff.ms': 300,
            'socket.timeout.ms': 5000,
            'message.timeout.ms': 30000,
        }
        producer = Producer(producer_conf)

        # Warm up producer metadata cache so it knows topic partition layout
        print("Warming up producer metadata cache...")
        for attempt in range(10):
            try:
                md = producer.list_topics(test_topic, timeout=3.0)
                if test_topic in md.topics and target_partition in md.topics[test_topic].partitions:
                    print(f"Metadata verified for {test_topic}: {len(md.topics[test_topic].partitions)} partitions available.")
                    break
            except Exception as e:
                print(f"Metadata fetch retry: {e}")
            time.sleep(1.0)

        pre_failover_count = 5
        delivered_offsets = []

        def ack_cb(err, msg):
            if err:
                print(f"Produce delivery error: {err}")
            else:
                delivered_offsets.append(msg.offset())

        print(f"Producing {pre_failover_count} messages to {test_topic}[{target_partition}]...")
        for i in range(pre_failover_count):
            key = f"key-pre-{i}".encode("utf-8")
            val = f"msg-pre-failover-payload-{i}".encode("utf-8")
            producer.produce(test_topic, key=key, value=val, partition=target_partition, callback=ack_cb)
        producer.flush(timeout=10)

        assert len(delivered_offsets) == pre_failover_count, f"Failed to deliver all pre-failover messages: got {len(delivered_offsets)}/{pre_failover_count}"
        print(f"Delivered {len(delivered_offsets)} messages. Highest offset: {delivered_offsets[-1]}")

        # Wait for follower replica to sync
        print("Waiting for follower replica to sync with leader...")
        replication_synced = False
        for attempt in range(60):
            time.sleep(0.5)
            p_state = get_partition_state(test_topic, target_partition)
            offsets = p_state.get("replica_offsets", {})
            leader_off = offsets.get(str(initial_leader), 0)
            follower_off = offsets.get(str(backup_replica), 0)
            hw = p_state.get("high_watermark", 0)
            if attempt % 4 == 0:
                print(f"  [Sync Poll {attempt}] Leader {initial_leader} off={leader_off}, Follower {backup_replica} off={follower_off}, HW={hw}")
            if follower_off >= pre_failover_count and hw >= pre_failover_count:
                replication_synced = True
                print(f"Replication verified: Leader {initial_leader} off={leader_off}, Follower {backup_replica} off={follower_off}, HW={hw}")
                break

        assert replication_synced, "Replication did not sync to follower within timeout!"

        # ---------------------------------------------------------
        # Step 4: Failure Injection & Controller Failover Latency
        # ---------------------------------------------------------
        log_step(4, f"Failure Injection: Killing Partition Leader (Broker {initial_leader})")
        t_kill = time.time()
        mgr.stop_broker(initial_leader, signal.SIGKILL)
        print(f"Broker {initial_leader} terminated at t=0.000s. Monitoring Controller for failure detection & election...")

        t_detected = None
        t_reelected = None
        new_leader = None
        post_failover_isr = None

        timeout_sec = 20.0
        while time.time() - t_kill < timeout_sec:
            now = time.time()
            elapsed = now - t_kill

            # Check broker active status
            if t_detected is None:
                brokers = http_get("/api/brokers")
                for b in brokers:
                    if b["id"] == initial_leader and not b.get("active", True):
                        t_detected = now
                        detection_latency = (t_detected - t_kill) * 1000.0
                        print(f"[{elapsed:.2f}s] Controller marked Broker {initial_leader} INACTIVE (Detection Latency: {detection_latency:.1f} ms)")

            # Check partition leader re-election & ISR shrinkage
            p_state = get_partition_state(test_topic, target_partition)
            cur_leader = p_state["leader_id"]
            cur_isr = p_state["isr"]

            if cur_leader != initial_leader and t_reelected is None:
                t_reelected = now
                new_leader = cur_leader
                post_failover_isr = cur_isr
                reelection_latency = (t_reelected - t_kill) * 1000.0
                print(f"[{elapsed:.2f}s] Automatic Leader Election SUCCESS! Partition {target_partition} new leader: Broker {new_leader} (Re-election Latency: {reelection_latency:.1f} ms)")
                print(f"[{elapsed:.2f}s] ISR Dynamics: Shrunk from {initial_isr} -> {cur_isr}")

            if t_detected and t_reelected:
                break

            time.sleep(0.15)

        assert t_detected is not None, f"Controller failed to detect broker {initial_leader} failure within {timeout_sec}s!"
        assert t_reelected is not None, f"Controller failed to re-elect new leader within {timeout_sec}s!"
        assert new_leader == backup_replica, f"Expected new leader {backup_replica}, got {new_leader}"

        failover_latency_ms = (t_reelected - t_kill) * 1000.0
        detection_latency_ms = (t_detected - t_kill) * 1000.0
        metrics["failure_detection_latency_ms"] = detection_latency_ms
        metrics["leader_reelection_latency_ms"] = failover_latency_ms
        metrics["initial_leader"] = initial_leader
        metrics["new_leader"] = new_leader
        metrics["initial_isr"] = initial_isr
        metrics["post_failover_isr"] = post_failover_isr

        # ---------------------------------------------------------
        # Step 5: Post-Failover Traffic & Zero Message Loss Verification
        # ---------------------------------------------------------
        log_step(5, "Kafka Client Reconnection & Zero Message Loss Verification")
        post_failover_count = 5
        post_delivered_offsets = []

        def ack_post_cb(err, msg):
            if err:
                print(f"Post-failover produce delivery error: {err}")
            else:
                post_delivered_offsets.append(msg.offset())

        # Refresh producer cluster metadata to discover new leader immediately
        producer.list_topics(test_topic, timeout=3.0)

        print(f"Producing {post_failover_count} post-failover messages to {test_topic}[{target_partition}]...")
        # Producing to survivors (bootstrap will connect to surviving brokers)
        for i in range(post_failover_count):
            seq = pre_failover_count + i
            key = f"key-post-{seq}".encode("utf-8")
            val = f"msg-post-failover-payload-{seq}".encode("utf-8")
            producer.produce(test_topic, key=key, value=val, partition=target_partition, callback=ack_post_cb)

        # Flush will automatically retry and succeed once metadata resolves new leader
        t_produce_start = time.time()
        producer.flush(timeout=15)
        produce_reconnect_time = (time.time() - t_produce_start) * 1000.0

        assert len(post_delivered_offsets) == post_failover_count, f"Expected {post_failover_count} post-failover messages delivered, got {len(post_delivered_offsets)}"
        print(f"Producer successfully reconnected and delivered {len(post_delivered_offsets)} messages to new leader Broker {new_leader}!")
        print(f"Post-failover offsets: {post_delivered_offsets}")

        # Now consume ALL messages (pre-failover and post-failover)
        total_expected_messages = pre_failover_count + post_failover_count
        print(f"\nConsuming all {total_expected_messages} messages from offset 0 to verify ZERO message loss...")

        consumer_conf = {
            'bootstrap.servers': ALL_BOOTSTRAP,
            'group.id': f'failover-verification-group-{int(time.time())}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'topic.metadata.refresh.interval.ms': 1000,
            'metadata.max.age.ms': 1000,
        }
        consumer = Consumer(consumer_conf)
        # Warm up consumer metadata to discover active cluster topology
        consumer.list_topics(test_topic, timeout=5.0)

        tp = TopicPartition(test_topic, target_partition, 0)
        consumer.assign([tp])

        consumed_messages = []
        poll_start = time.time()
        while time.time() - poll_start < 20.0 and len(consumed_messages) < total_expected_messages:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            consumed_messages.append({
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8"),
                "value": msg.value().decode("utf-8")
            })
            print(f"  [Consumer] Received message {len(consumed_messages)}/{total_expected_messages}: offset={msg.offset()}, key={msg.key().decode('utf-8')}")

        consumer.close()

        print(f"Consumed {len(consumed_messages)}/{total_expected_messages} messages.")
        assert len(consumed_messages) == total_expected_messages, f"Message loss detected! Consumed {len(consumed_messages)} of {total_expected_messages}"

        # Verify integrity and sequence
        for idx, m in enumerate(consumed_messages):
            assert m["offset"] == idx, f"Offset mismatch at index {idx}: got {m['offset']}"
            if idx < pre_failover_count:
                assert f"key-pre-{idx}" in m["key"]
            else:
                assert f"key-post-{idx}" in m["key"]

        print(f">>> 100% Message Integrity Verified: Exact sequence 0..{total_expected_messages-1}, ZERO message loss, ZERO corruption!")
        metrics["total_messages_verified"] = len(consumed_messages)

        # ---------------------------------------------------------
        # Step 6: Broker Recovery & ISR Expansion
        # ---------------------------------------------------------
        log_step(6, f"Broker Recovery: Restarting Broker {initial_leader}")
        t_restart = time.time()
        if initial_leader == 2:
            mgr.start_broker(broker_id=2, data_port=19092, kafka_port=9094, storage_dir=BROKER2_DIR)
        else:
            mgr.start_broker(broker_id=3, data_port=19093, kafka_port=9096, storage_dir=BROKER3_DIR)

        print(f"Waiting for Broker {initial_leader} to re-register with Controller...")
        wait_for_broker_state(initial_leader, expected_active=True, timeout_sec=15.0)
        t_recovered = time.time()
        recovery_latency_ms = (t_recovered - t_restart) * 1000.0
        print(f"Broker {initial_leader} re-registered successfully in {recovery_latency_ms:.1f} ms!")

        print(f"Waiting for Broker {initial_leader} to catch up replication and re-join ISR...")
        isr_restored = False
        final_isr = None
        for attempt in range(40):
            time.sleep(0.5)
            p_state = get_partition_state(test_topic, target_partition)
            cur_isr = p_state.get("isr", [])
            offsets = p_state.get("replica_offsets", {})
            recovered_off = offsets.get(str(initial_leader), 0)
            if attempt % 4 == 0:
                print(f"  [Recovery Poll {attempt}] Broker {initial_leader} off={recovered_off}/{total_expected_messages}, ISR={cur_isr}")
            if initial_leader in cur_isr and recovered_off >= total_expected_messages:
                isr_restored = True
                final_isr = cur_isr
                print(f"Broker {initial_leader} caught up to offset {recovered_off} and re-joined ISR: {cur_isr}")
                break

        assert isr_restored, f"Broker {initial_leader} failed to rejoin ISR within timeout! Current ISR: {p_state.get('isr')}"
        metrics["recovery_latency_ms"] = recovery_latency_ms
        metrics["final_restored_isr"] = final_isr

        # ---------------------------------------------------------
        # Final Summary Report
        # ---------------------------------------------------------
        print(f"\n{'='*75}")
        print(" AEROSTREAM MULTI-BROKER HA & FAILOVER TEST REPORT")
        print(f"{'='*75}")
        print(f"Cluster Configuration:      3 Brokers, Go Raft Controller")
        print(f"Topic Evaluated:            {test_topic} (Partitions: {NUM_PARTITIONS}, RF: {REPLICATION_FACTOR})")
        print(f"Target Partition Tested:    Partition {target_partition}")
        print(f"Initial Leader:             Broker {initial_leader}")
        print(f"Initial ISR Set:            {initial_isr}")
        print(f"Failure Injected:           SIGKILL on Broker {initial_leader}")
        print(f"Failure Detection Latency:  {metrics['failure_detection_latency_ms']:.2f} ms")
        print(f"Leader Re-Election Latency: {metrics['leader_reelection_latency_ms']:.2f} ms")
        print(f"Newly Elected Leader:       Broker {metrics['new_leader']}")
        print(f"Post-Failover ISR Set:      {metrics['post_failover_isr']}")
        print(f"Traffic Continuity:         5 pre-failover + 5 post-failover = 10 messages")
        print(f"Data Loss Rate:             0.00% (ZERO message loss)")
        print(f"Broker Recovery Time:       {metrics['recovery_latency_ms']:.2f} ms")
        print(f"Final Restored ISR Set:     {metrics['final_restored_isr']}")
        print(f"OVERALL STATUS:             PASS (ALL 8 CHECKS PASSED)")
        print(f"{'='*75}\n")

    finally:
        # Step 7: Clean up secondary brokers and temp directories
        mgr.cleanup_all()


if __name__ == "__main__":
    run_failover_test()
