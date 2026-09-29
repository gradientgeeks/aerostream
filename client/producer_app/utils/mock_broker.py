"""
AeroStream Embedded Mock Kafka Broker.
Provides a lightweight, in-process Kafka wire-protocol server (ApiKey 0, 3, 10, 17, 18, 22, 24, 26, 36)
for standalone self-tests, CI verification, and offline testing without an external broker.
"""

from __future__ import annotations

import logging
import select
import socket
import struct
import threading
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def put_string(s: Optional[str]) -> bytes:
    if s is None:
        return struct.pack(">h", -1)
    b = s.encode("utf-8")
    return struct.pack(">h", len(b)) + b


def write_unsigned_varint(val: int) -> bytes:
    buf = bytearray()
    while val >= 0x80:
        buf.append((val & 0x7F) | 0x80)
        val >>= 7
    buf.append(val & 0x7F)
    return bytes(buf)


class MockKafkaBroker:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self.server_sock: Optional[socket.socket] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Statistics & received frames
        self.produce_requests_count = 0
        self.messages_received_count = 0
        self.received_topics: Dict[str, int] = {}
        self.received_partitions: Dict[int, int] = {}
        self.transaction_commits_count = 0
        self.transaction_aborts_count = 0
        self.idempotent_init_count = 0

    def start(self) -> int:
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.port = self.server_sock.getsockname()[1]
        self.server_sock.listen(50)
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        logger.info(f"Mock Kafka Broker listening on {self.host}:{self.port}")
        return self.port

    def stop(self) -> None:
        self.running = False
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)

    def _run_loop(self) -> None:
        while self.running:
            try:
                r, _, _ = select.select([self.server_sock], [], [], 0.2)
                if not r:
                    continue
                client_sock, _ = self.server_sock.accept()
                threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True).start()
            except Exception:
                if not self.running:
                    break

    def _handle_client(self, sock: socket.socket) -> None:
        sock.settimeout(5.0)
        try:
            while self.running:
                hdr = sock.recv(4)
                if not hdr or len(hdr) < 4:
                    break
                frame_len = struct.unpack(">i", hdr)[0]
                body = bytearray()
                while len(body) < frame_len:
                    chunk = sock.recv(frame_len - len(body))
                    if not chunk:
                        break
                    body.extend(chunk)

                if len(body) < frame_len:
                    break

                response = self._dispatch(bytes(body))
                if response:
                    sock.sendall(struct.pack(">i", len(response)) + response)
        except Exception as e:
            logger.debug(f"Mock broker client connection closed: {e}")
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def _dispatch(self, data: bytes) -> Optional[bytes]:
        api_key, api_version, correlation_id = struct.unpack_from(">hhi", data, 0)
        offset = 8

        # Read client_id
        client_id_len = struct.unpack_from(">h", data, offset)[0]
        offset += 2
        if client_id_len > 0:
            offset += client_id_len

        # Flexible version tagged fields in request header (v3+ ApiVersions, v2+ InitPid)
        if api_key == 18 and api_version >= 3:
            # skip tagged fields in request header
            if offset < len(data):
                offset += 1

        if api_key == 18:   # ApiVersions
            return self._handle_api_versions(correlation_id, api_version)
        elif api_key == 3:  # Metadata
            return self._handle_metadata(correlation_id, api_version, data, offset)
        elif api_key == 10: # FindCoordinator
            return self._handle_find_coordinator(correlation_id, api_version)
        elif api_key == 0:  # Produce
            return self._handle_produce(correlation_id, api_version, data, offset)
        elif api_key == 22: # InitProducerId
            return self._handle_init_producer_id(correlation_id, api_version)
        elif api_key == 24: # AddPartitionsToTxn
            return self._handle_add_partitions_to_txn(correlation_id, api_version)
        elif api_key == 26: # EndTxn
            return self._handle_end_txn(correlation_id, api_version, data, offset)
        elif api_key == 17: # SaslHandshake
            return self._handle_sasl_handshake(correlation_id, api_version)
        elif api_key == 36: # SaslAuthenticate
            return self._handle_sasl_authenticate(correlation_id, api_version, data, offset)
        else:
            return struct.pack(">ih", correlation_id, 35)

    def _handle_api_versions(self, corr_id: int, version: int) -> bytes:
        apis = [
            (0, 0, 7),   # Produce
            (1, 0, 11),  # Fetch
            (2, 0, 5),   # ListOffsets
            (3, 0, 8),   # Metadata
            (10, 0, 2),  # FindCoordinator
            (17, 0, 1),  # SaslHandshake
            (18, 0, 3),  # ApiVersions
            (22, 0, 3),  # InitProducerId
            (24, 0, 2),  # AddPartitionsToTxn
            (25, 0, 2),  # AddOffsetsToTxn
            (26, 0, 2),  # EndTxn
            (28, 0, 2),  # TxnOffsetCommit
            (36, 0, 1),  # SaslAuthenticate
        ]

        if version >= 3:
            resp = struct.pack(">ih", corr_id, 0)
            resp += write_unsigned_varint(len(apis) + 1)
            for k, min_v, max_v in apis:
                resp += struct.pack(">hhh", k, min_v, max_v) + b"\x00"
            resp += struct.pack(">i", 0) + b"\x00"  # throttle_time_ms + tagged_fields
            return resp
        else:
            resp = struct.pack(">ih", corr_id, 0)
            resp += struct.pack(">i", len(apis))
            for k, min_v, max_v in apis:
                resp += struct.pack(">hhh", k, min_v, max_v)
            if version >= 1:
                resp += struct.pack(">i", 0)  # throttle_time_ms
            return resp

    def _handle_metadata(self, corr_id: int, version: int, data: bytes, offset: int) -> bytes:
        topics_requested = []
        if offset + 4 <= len(data):
            num_topics = struct.unpack_from(">i", data, offset)[0]
            offset += 4
            for _ in range(max(0, num_topics)):
                if offset + 2 > len(data):
                    break
                t_len = struct.unpack_from(">h", data, offset)[0]
                offset += 2
                if t_len > 0 and offset + t_len <= len(data):
                    t_name = data[offset : offset + t_len].decode("utf-8", "ignore")
                    topics_requested.append(t_name)
                    offset += t_len

        if not topics_requested:
            topics_requested = ["aerostream-test"]

        resp = struct.pack(">i", corr_id)
        if version >= 3:
            resp += struct.pack(">i", 0)  # throttle_time_ms

        # Brokers array
        resp += struct.pack(">i", 1)  # count = 1
        resp += struct.pack(">i", 1)  # node_id = 1
        resp += put_string(self.host)
        resp += struct.pack(">i", self.port)
        if version >= 1:
            resp += put_string("rack-1")

        if version >= 2:
            resp += put_string("aeromq-cluster-1")
        if version >= 1:
            resp += struct.pack(">i", 1)  # controller_id = 1

        # Topics array
        resp += struct.pack(">i", len(topics_requested))
        for topic in topics_requested:
            resp += struct.pack(">h", 0)  # error_code = 0
            resp += put_string(topic)
            if version >= 1:
                resp += struct.pack(">b", 0)  # is_internal = false

            # 3 Partitions: 0, 1, 2
            resp += struct.pack(">i", 3)
            for p in range(3):
                resp += struct.pack(">hii", 0, p, 1)  # err=0, p_id=p, leader=1
                if version >= 7:
                    resp += struct.pack(">i", 0)      # leader_epoch = 0
                resp += struct.pack(">ii", 1, 1)      # replicas count=1, [1]
                resp += struct.pack(">ii", 1, 1)      # isr count=1, [1]
                if version >= 5:
                    resp += struct.pack(">i", 0)      # offline_replicas count=0

            if version >= 8:
                resp += struct.pack(">i", -2147483648)  # topic_authorized_operations

        if version >= 8:
            resp += struct.pack(">i", -2147483648)      # cluster_authorized_operations

        return resp

    def _handle_find_coordinator(self, corr_id: int, version: int) -> bytes:
        resp = struct.pack(">i", corr_id)
        if version >= 1:
            resp += struct.pack(">i", 0)  # throttle_time_ms
        resp += struct.pack(">h", 0)      # error_code = 0
        if version >= 1:
            resp += put_string(None)      # error_message = None
        resp += struct.pack(">i", 1)      # node_id = 1
        resp += put_string(self.host)
        resp += struct.pack(">i", self.port)
        if version >= 3:
            resp += b"\x00"               # tagged fields
        return resp

    def _handle_produce(self, corr_id: int, version: int, data: bytes, offset: int) -> bytes:
        self.produce_requests_count += 1

        # transactional_id if v3+
        if version >= 3:
            t_len = struct.unpack_from(">h", data, offset)[0]
            offset += 2
            if t_len > 0:
                offset += t_len

        # acks (i16), timeout_ms (i32)
        acks, timeout_ms = struct.unpack_from(">hi", data, offset)
        offset += 6

        # Topics array
        num_topics = struct.unpack_from(">i", data, offset)[0]
        offset += 4

        topic_responses = []

        for _ in range(max(0, num_topics)):
            t_len = struct.unpack_from(">h", data, offset)[0]
            offset += 2
            t_name = data[offset : offset + t_len].decode("utf-8", "ignore")
            offset += t_len

            num_parts = struct.unpack_from(">i", data, offset)[0]
            offset += 4

            part_responses = []
            for _ in range(max(0, num_parts)):
                part_id, recs_len = struct.unpack_from(">ii", data, offset)
                offset += 8
                recs_data = data[offset : offset + recs_len]
                offset += recs_len

                self.received_topics[t_name] = self.received_topics.get(t_name, 0) + 1
                self.received_partitions[part_id] = self.received_partitions.get(part_id, 0) + 1
                self.messages_received_count += 1

                if len(recs_data) >= 61 and recs_data[16] == 2:
                    num_records = struct.unpack_from(">i", recs_data, 57)[0]
                    if num_records > 1:
                        self.messages_received_count += (num_records - 1)

                part_responses.append((part_id, 0, 100 + self.messages_received_count))

            topic_responses.append((t_name, part_responses))

        if acks == 0:
            return b""

        resp = struct.pack(">i", corr_id)
        resp += struct.pack(">i", len(topic_responses))
        for t_name, part_resps in topic_responses:
            resp += put_string(t_name)
            resp += struct.pack(">i", len(part_resps))
            for p_id, err_code, base_offset in part_resps:
                resp += struct.pack(">ihqqq", p_id, err_code, base_offset, int(time.time() * 1000), 0)
                if version >= 8:
                    resp += struct.pack(">i", 0)  # record_errors count
                    resp += put_string(None)      # error_message = None

        if version >= 1:
            resp += struct.pack(">i", 0)  # throttle_time_ms

        return resp

    def _handle_init_producer_id(self, corr_id: int, version: int) -> bytes:
        self.idempotent_init_count += 1
        resp = struct.pack(">i", corr_id)
        resp += struct.pack(">i", 0)  # throttle_time_ms
        resp += struct.pack(">hqh", 0, 1000 + self.idempotent_init_count, 0)
        if version >= 2:
            resp += b"\x00"  # tagged fields
        return resp

    def _handle_add_partitions_to_txn(self, corr_id: int, version: int) -> bytes:
        resp = struct.pack(">i", corr_id)
        resp += struct.pack(">i", 0)  # throttle_time_ms
        resp += struct.pack(">i", 1)  # 1 topic
        resp += put_string("aerostream-test")
        resp += struct.pack(">i", 3)  # 3 partitions
        for p in range(3):
            resp += struct.pack(">ih", p, 0)
        if version >= 3:
            resp += b"\x00"
        return resp

    def _handle_end_txn(self, corr_id: int, version: int, data: bytes, offset: int) -> bytes:
        t_len = struct.unpack_from(">h", data, offset)[0]
        offset += 2
        if t_len > 0:
            offset += t_len

        if offset + 11 <= len(data):
            _, _, res_bool = struct.unpack_from(">qhb", data, offset)
            if res_bool == 1:
                self.transaction_commits_count += 1
            else:
                self.transaction_aborts_count += 1
        else:
            self.transaction_commits_count += 1

        resp = struct.pack(">i", corr_id)
        resp += struct.pack(">i", 0)  # throttle_time_ms
        resp += struct.pack(">h", 0)  # error_code = 0
        if version >= 3:
            resp += b"\x00"
        return resp

    def _handle_sasl_handshake(self, corr_id: int, version: int) -> bytes:
        resp = struct.pack(">ih", corr_id, 0)
        resp += struct.pack(">i", 2)
        resp += put_string("PLAIN")
        resp += put_string("SCRAM-SHA-256")
        return resp

    def _handle_sasl_authenticate(self, corr_id: int, version: int, data: bytes, offset: int) -> bytes:
        import base64
        import hashlib
        import hmac

        auth_bytes = b""
        if offset + 4 <= len(data):
            b_len = struct.unpack_from(">i", data, offset)[0]
            offset += 4
            if b_len > 0 and offset + b_len <= len(data):
                auth_bytes = data[offset : offset + b_len]

        auth_str = auth_bytes.decode("utf-8", "ignore")
        if auth_str.startswith("n,,") or auth_str.startswith("y,,"):
            # SCRAM Client-first-message: 'n,,n=user,r=client_nonce'
            parts = auth_str.split(",")
            c_nonce = ""
            user = ""
            for p in parts:
                if p.startswith("r="):
                    c_nonce = p[2:]
                elif p.startswith("n="):
                    user = p[2:]
            srv_nonce = c_nonce + "aeromq_mock_srv_nonce"
            salt = b"aerostream_salt"
            salt_b64 = base64.b64encode(salt).decode("utf-8")
            srv_first = f"r={srv_nonce},s={salt_b64},i=4096"

            client_first_bare = auth_str.split(",", 2)[2]
            auth_msg = f"{client_first_bare},{srv_first}"
            self._scram_state = {
                "auth_msg": auth_msg,
                "salt": salt,
                "nonce": srv_nonce,
                "user": user,
            }
            resp_bytes = srv_first.encode("utf-8")
        elif auth_str.startswith("c="):
            # SCRAM Client-final-message: 'c=biws,r=...,p=...'
            if hasattr(self, "_scram_state") and self._scram_state:
                state = self._scram_state
                parts = auth_str.split(",")
                c_final_no_p = ",".join([p for p in parts if not p.startswith("p=")])
                auth_msg = state["auth_msg"] + "," + c_final_no_p
                password = b"aerostream123"
                sp = hashlib.pbkdf2_hmac("sha256", password, state["salt"], 4096)
                server_key = hmac.new(sp, b"Server Key", hashlib.sha256).digest()
                server_sig = hmac.new(server_key, auth_msg.encode("utf-8"), hashlib.sha256).digest()
                server_sig_b64 = base64.b64encode(server_sig).decode("utf-8")
                srv_final = f"v={server_sig_b64}"
                resp_bytes = srv_final.encode("utf-8")
            else:
                resp_bytes = b""
        else:
            # SASL PLAIN
            resp_bytes = b""

        resp = struct.pack(">ih", corr_id, 0)
        resp += put_string(None)  # error_message = None
        resp += struct.pack(">i", len(resp_bytes)) + resp_bytes
        if version >= 1:
            resp += struct.pack(">q", 0)  # session_lifetime_ms
        return resp

