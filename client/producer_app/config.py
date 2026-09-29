"""
AeroStream Kafka Producer Application - Configuration & CLI Arguments Parser.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional

VALID_SCENARIOS = [
    "all",
    "basic",
    "compression",
    "headers",
    "keys",
    "large",
    "idempotence",
    "transactions",
    "security",
]


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aerostream-producer",
        description="Production-grade modular Kafka Producer test application suite for AeroStream (port 9092/9093)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Core Parameters
    parser.add_argument(
        "--scenario", "-s",
        type=str,
        default="all",
        choices=VALID_SCENARIOS,
        help="Test scenario to execute",
    )
    parser.add_argument(
        "--bootstrap-server", "-b",
        type=str,
        default="127.0.0.1:9092",
        help="Kafka bootstrap server address (host:port)",
    )
    parser.add_argument(
        "--topic", "-t",
        type=str,
        default="aerostream-test",
        help="Kafka topic name to produce to",
    )
    parser.add_argument(
        "--count", "-n",
        type=int,
        default=100,
        help="Number of messages to produce per test scenario/subtest",
    )
    parser.add_argument(
        "--json-output", "-j",
        action="store_true",
        help="Output test results and metrics in structured JSON format",
    )
    parser.add_argument(
        "--backend",
        type=str,
        default="confluent",
        choices=["confluent", "kafka-python"],
        help="Client library backend engine",
    )

    # Scenario Tuning & Overrides
    parser.add_argument(
        "--acks",
        type=str,
        default=None,
        help="Acknowledgment mode override: '0', '1', 'all' or '-1'",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Batch size in bytes override (e.g. 16384, 65536)",
    )
    parser.add_argument(
        "--linger-ms",
        type=int,
        default=None,
        help="Linger duration in milliseconds override (e.g. 0, 5, 20)",
    )
    parser.add_argument(
        "--compression",
        type=str,
        default=None,
        choices=["none", "gzip", "snappy", "lz4", "zstd"],
        help="Compression codec override",
    )
    parser.add_argument(
        "--payload-size",
        type=str,
        default=None,
        help="Payload size string override (e.g. '100B', '1KB', '64KB', '1MB', '5MB')",
    )
    parser.add_argument(
        "--transactional-id",
        type=str,
        default=None,
        help="Transactional ID override for KIP-98 transactions",
    )

    # Security & Authentication
    parser.add_argument(
        "--security-protocol",
        type=str,
        default=None,
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
        help="Protocol used to communicate with brokers",
    )
    parser.add_argument(
        "--sasl-mechanism",
        type=str,
        default=None,
        choices=["PLAIN", "SCRAM-SHA-256"],
        help="SASL mechanism for authentication",
    )
    parser.add_argument(
        "--sasl-username",
        type=str,
        default=None,
        help="SASL username credential",
    )
    parser.add_argument(
        "--sasl-password",
        type=str,
        default=None,
        help="SASL password credential",
    )
    parser.add_argument(
        "--ca-cert",
        type=str,
        default=None,
        help="Path to CA certificate PEM file for SSL/TLS verification",
    )
    parser.add_argument(
        "--client-cert",
        type=str,
        default=None,
        help="Path to client certificate PEM file for mutual TLS (mTLS)",
    )
    parser.add_argument(
        "--client-key",
        type=str,
        default=None,
        help="Path to client private key PEM file for mutual TLS (mTLS)",
    )

    # Operational Options
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Produce delivery and flush timeout in seconds",
    )
    parser.add_argument(
        "--mock-broker",
        action="store_true",
        help="Spawn embedded in-process Mock Kafka Broker for testing without external cluster",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging output",
    )

    return parser
