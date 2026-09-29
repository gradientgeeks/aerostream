"""Configuration builder for Kafka consumers (confluent-kafka and kafka-python-ng)."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class KafkaConsumerSettings:
    bootstrap_server: str = "127.0.0.1:9093"
    group_id: Optional[str] = None
    client_id: str = "aerostream-consumer-app"
    auto_offset_reset: str = "earliest"  # earliest | latest
    enable_auto_commit: bool = False
    auto_commit_interval_ms: int = 1000
    isolation_level: str = "read_uncommitted"  # read_uncommitted | read_committed
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    session_timeout_ms: int = 10000
    max_poll_interval_ms: int = 300000
    
    # Security options
    security_protocol: str = "PLAINTEXT"  # PLAINTEXT | SASL_PLAINTEXT | SSL | SASL_SSL
    sasl_mechanism: Optional[str] = None  # PLAIN | SCRAM-SHA-256
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None

    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert settings to confluent-kafka configuration dictionary."""
        conf: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_server,
            "client.id": self.client_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "auto.commit.interval.ms": self.auto_commit_interval_ms,
            "isolation.level": self.isolation_level,
            "fetch.min.bytes": self.fetch_min_bytes,
            "fetch.wait.max.ms": self.fetch_max_wait_ms,
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
        }
        conf["group.id"] = self.group_id or f"standalone-{self.client_id}"

        # Security
        proto = self.security_protocol.upper()
        conf["security.protocol"] = proto.lower()
        if "SASL" in proto:
            if self.sasl_mechanism:
                conf["sasl.mechanism"] = self.sasl_mechanism.upper()
            if self.sasl_username:
                conf["sasl.username"] = self.sasl_username
            if self.sasl_password:
                conf["sasl.password"] = self.sasl_password

        if "SSL" in proto:
            if self.ssl_cafile:
                conf["ssl.ca.location"] = self.ssl_cafile
            if self.ssl_certfile:
                conf["ssl.certificate.location"] = self.ssl_certfile
            if self.ssl_keyfile:
                conf["ssl.key.location"] = self.ssl_keyfile

        return conf

    def to_kafka_python_config(self) -> Dict[str, Any]:
        """Convert settings to kafka-python-ng configuration dictionary."""
        conf: Dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_server,
            "client_id": self.client_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "auto_commit_interval_ms": self.auto_commit_interval_ms,
            "isolation_level": self.isolation_level,
            "fetch_min_bytes": self.fetch_min_bytes,
            "fetch_max_wait_ms": self.fetch_max_wait_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
        }
        if self.group_id:
            conf["group_id"] = self.group_id

        # Security
        proto = self.security_protocol.upper()
        conf["security_protocol"] = proto
        if "SASL" in proto:
            if self.sasl_mechanism:
                conf["sasl_mechanism"] = self.sasl_mechanism.upper()
            if self.sasl_username:
                conf["sasl_plain_username"] = self.sasl_username
            if self.sasl_password:
                conf["sasl_plain_password"] = self.sasl_password

        if "SSL" in proto:
            if self.ssl_cafile:
                conf["ssl_cafile"] = self.ssl_cafile
            if self.ssl_certfile:
                conf["ssl_certfile"] = self.ssl_certfile
            if self.ssl_keyfile:
                conf["ssl_keyfile"] = self.ssl_keyfile

        return conf
