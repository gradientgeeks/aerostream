#!/usr/bin/env python3
"""
Live Consumer Group Demonstration Script for AeroStream Web Console.
Continuously produces and consumes messages with an active Consumer Group so you can
watch it live in real time in the Web UI:
  URL: http://localhost:9001/aerostream/console/consumer-groups
"""

import sys
import time
import json
import threading
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVER = "127.0.0.1:9092"
TOPIC_NAME = "live-ui-stream-demo"
GROUP_ID = "live-ui-consumer-group-active"
RUN_DURATION_SECONDS = 300  # 5 minutes

stop_event = threading.Event()

def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVER})
    try:
        new_topic = NewTopic(TOPIC_NAME, num_partitions=3, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"[Topic Provisioning] Topic '{t}' created with 3 partitions.")
            except Exception as e:
                print(f"[Topic Provisioning] Topic '{t}' ready (exists or created).")
    except Exception as e:
        print(f"[Topic Provisioning] Note: {e}")

def run_producer():
    p = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "acks": "1",
        "linger.ms": 5,
    })
    msg_id = 0
    print("[Producer] Started continuous producer stream (producing 1 message every 1.5s)...")
    while not stop_event.is_set():
        msg_id += 1
        payload = json.dumps({
            "msg_id": msg_id,
            "timestamp": time.time(),
            "source": "live-ui-demo-producer",
            "message": f"Real-time event #{msg_id} for AeroStream Web Console",
        }).encode("utf-8")
        
        partition = msg_id % 3
        key = f"key-{partition}".encode("utf-8")
        
        p.produce(TOPIC_NAME, key=key, value=payload, partition=partition)
        p.poll(0)
        time.sleep(1.5)
    p.flush(5)

def run_consumer():
    def on_assign_cb(c, partitions):
        print(f"👉 [Consumer Group Event] Partitions Assigned: {[p.partition for p in partitions]}")

    def on_revoke_cb(c, partitions):
        print(f"👉 [Consumer Group Event] Partitions Revoked: {[p.partition for p in partitions]}")

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000,
        "client.id": "live-web-console-consumer-member-1",
    })
    
    c.subscribe([TOPIC_NAME], on_assign=on_assign_cb, on_revoke=on_revoke_cb)
    print(f"[Consumer] Joined consumer group '{GROUP_ID}'. Subscribed to '{TOPIC_NAME}'.")
    
    consumed_count = 0
    while not stop_event.is_set():
        msg = c.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[Consumer Error] {msg.error()}")
            continue
            
        consumed_count += 1
        # Commit every message or every 2 messages
        c.commit(msg, asynchronous=False)
        print(f"✅ [Consumer Progress] Consumed & Committed | Group: {GROUP_ID} | Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")
            
    c.close()
    print("[Consumer] Consumer closed cleanly.")

def main():
    print("=" * 80)
    print("        AEROSTREAM LIVE CONSUMER GROUP DEMONSTRATION")
    print("=" * 80)
    print(f"Target Cluster    : {BOOTSTRAP_SERVER}")
    print(f"Web Console URL   : http://localhost:9001/aerostream/console/consumer-groups")
    print(f"Active Topic      : {TOPIC_NAME} (3 partitions)")
    print(f"Consumer Group ID : {GROUP_ID}")
    print(f"Run Duration      : {RUN_DURATION_SECONDS} seconds")
    print("=" * 80)
    print("Open your browser now at:")
    print("  👉 http://localhost:9001/aerostream/console/consumer-groups")
    print("You will see the consumer group actively appear, update offsets, and show lag.")
    print("=" * 80)
    
    ensure_topic()
    
    p_thread = threading.Thread(target=run_producer, daemon=True)
    c_thread = threading.Thread(target=run_consumer, daemon=True)
    
    p_thread.start()
    time.sleep(1)
    c_thread.start()
    
    start_time = time.time()
    try:
        while time.time() - start_time < RUN_DURATION_SECONDS:
            remaining = int(RUN_DURATION_SECONDS - (time.time() - start_time))
            if remaining % 15 == 0:
                print(f"[Status] Stream active. Remaining demo time: {remaining}s | Open UI: http://localhost:9001/aerostream/console/consumer-groups")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping demo early on user interrupt...")
    finally:
        stop_event.set()
        p_thread.join(timeout=3)
        c_thread.join(timeout=3)
        print("[Demo Complete] Finished successfully.")

if __name__ == "__main__":
    main()
