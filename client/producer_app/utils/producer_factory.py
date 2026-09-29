"""
AeroStream Kafka Producer Factory.
Provides unified instantiation and delivery tracking for both confluent-kafka (librdkafka)
and kafka-python-ng client backends.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


@dataclass
class ProducerOptions:
    bootstrap_servers: str = "127.0.0.1:9092"
    client_id: str = "aerostream-producer"
    acks: Union[str, int] = "all"
    linger_ms: int = 5
    batch_size: int = 16384  # 16KB
    compression_type: str = "none"  # none, gzip, snappy, lz4, zstd
    max_request_size: int = 10 * 1024 * 1024  # 10MB
    enable_idempotence: bool = False
    transactional_id: Optional[str] = None
    transaction_timeout_ms: int = 60000
    retries: int = 5
    retry_backoff_ms: int = 100
    request_timeout_ms: int = 15000

    # Security options
    security_protocol: str = "PLAINTEXT"  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    sasl_mechanism: Optional[str] = None  # PLAIN, SCRAM-SHA-256
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ca_cert_path: Optional[str] = None
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None

    # Custom extra config overrides
    extra_config: Dict[str, Any] = field(default_factory=dict)


class DeliveryTracker:
    """Tracks asynchronous message delivery confirmations, latencies, and partition distribution."""
    def __init__(self, expected_count: int):
        self.expected_count = expected_count
        self.delivered_count = 0
        self.failed_count = 0
        self.latencies_ms: List[float] = []
        self.partition_counts: Dict[int, int] = field(default_factory=dict)
        self.partition_counts = {}
        self.errors: List[str] = []
        self._start_times: Dict[int, float] = {}

    def mark_sent(self, msg_id: int) -> None:
        self._start_times[msg_id] = time.perf_counter()

    def on_delivery(self, err: Any, msg: Any, msg_id: Optional[int] = None) -> None:
        now = time.perf_counter()
        if msg_id is not None and msg_id in self._start_times:
            lat = (now - self._start_times.pop(msg_id)) * 1000.0
            self.latencies_ms.append(lat)

        if err is not None:
            self.failed_count += 1
            self.errors.append(str(err))
        else:
            self.delivered_count += 1
            # msg might be confluent_kafka.Message or RecordMetadata
            part = getattr(msg, "partition", None)
            if callable(part):
                p_id = part()
            elif isinstance(part, int):
                p_id = part
            else:
                p_id = 0
            self.partition_counts[p_id] = self.partition_counts.get(p_id, 0) + 1


def build_confluent_producer(opts: ProducerOptions):
    """Instantiates a confluent_kafka.Producer with the supplied options."""
    import confluent_kafka

    # Convert acks
    acks_val = str(opts.acks)
    if acks_val == "-1":
        acks_val = "all"

    conf: Dict[str, Any] = {
        "bootstrap.servers": opts.bootstrap_servers,
        "client.id": opts.client_id,
        "acks": acks_val,
        "linger.ms": opts.linger_ms,
        "batch.size": opts.batch_size,
        "compression.type": opts.compression_type,
        "message.max.bytes": opts.max_request_size,
        "request.timeout.ms": opts.request_timeout_ms,
        "retries": opts.retries,
        "retry.backoff.ms": opts.retry_backoff_ms,
    }

    if opts.enable_idempotence or opts.transactional_id:
        conf["enable.idempotence"] = True
        conf["acks"] = "all"

    if opts.transactional_id:
        conf["transactional.id"] = opts.transactional_id
        conf["transaction.timeout.ms"] = opts.transaction_timeout_ms

    # Security configuration
    sec_proto = opts.security_protocol.upper()
    conf["security.protocol"] = sec_proto

    if "SASL" in sec_proto:
        if opts.sasl_mechanism:
            conf["sasl.mechanism"] = opts.sasl_mechanism.upper()
        if opts.sasl_username:
            conf["sasl.username"] = opts.sasl_username
        if opts.sasl_password:
            conf["sasl.password"] = opts.sasl_password

    if "SSL" in sec_proto:
        if opts.ca_cert_path:
            conf["ssl.ca.location"] = opts.ca_cert_path
        if opts.client_cert_path:
            conf["ssl.certificate.location"] = opts.client_cert_path
        if opts.client_key_path:
            conf["ssl.key.location"] = opts.client_key_path

    # Merge custom overrides
    conf.update(opts.extra_config)
    logger.debug(f"Initializing confluent_kafka.Producer with config: {conf}")
    return confluent_kafka.Producer(conf)


def build_kafka_python_producer(opts: ProducerOptions):
    """Instantiates a kafka.KafkaProducer (kafka-python-ng) with the supplied options."""
    import kafka

    acks_val: Union[int, str] = opts.acks
    if isinstance(acks_val, str):
        if acks_val.lower() == "all" or acks_val == "-1":
            acks_val = "all"
        else:
            try:
                acks_val = int(acks_val)
            except ValueError:
                pass

    comp: Optional[str] = opts.compression_type
    if comp == "none":
        comp = None

    kwargs: Dict[str, Any] = {
        "bootstrap_servers": opts.bootstrap_servers.split(","),
        "client_id": opts.client_id,
        "acks": acks_val,
        "linger_ms": opts.linger_ms,
        "batch_size": opts.batch_size,
        "compression_type": comp,
        "max_request_size": opts.max_request_size,
        "request_timeout_ms": opts.request_timeout_ms,
        "retries": opts.retries,
        "retry_backoff_ms": opts.retry_backoff_ms,
        "security_protocol": opts.security_protocol.upper(),
    }

    sec_proto = opts.security_protocol.upper()
    if "SASL" in sec_proto:
        if opts.sasl_mechanism:
            kwargs["sasl_mechanism"] = opts.sasl_mechanism.upper()
        if opts.sasl_username:
            kwargs["sasl_plain_username"] = opts.sasl_username
        if opts.sasl_password:
            kwargs["sasl_plain_password"] = opts.sasl_password

    if "SSL" in sec_proto:
        if opts.ca_cert_path:
            kwargs["ssl_cafile"] = opts.ca_cert_path
        if opts.client_cert_path:
            kwargs["ssl_certfile"] = opts.client_cert_path
        if opts.client_key_path:
            kwargs["ssl_keyfile"] = opts.client_key_path

    kwargs.update(opts.extra_config)
    logger.debug(f"Initializing kafka.KafkaProducer with kwargs: {kwargs}")
    return kafka.KafkaProducer(**kwargs)
