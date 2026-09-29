#!/usr/bin/env python3
"""
AeroStream Kafka Producer CLI.
Production-grade test suite exercising Kafka wire protocol features on AeroStream (port 9092/9093).
"""

from __future__ import annotations

import logging
import sys
from typing import List

from config import create_parser
from scenarios import SCENARIO_REGISTRY
from utils.mock_broker import MockKafkaBroker
from utils.reporter import ScenarioResult, TestReporter


def main() -> int:
    parser = create_parser()
    args = parser.parse_args()

    # Logging setup
    if args.verbose:
        log_level = logging.DEBUG
    elif args.json_output:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Silence third-party verbose logs unless debug is explicitly requested
    if not args.verbose:
        logging.getLogger("kafka").setLevel(logging.WARNING)

    # Optional Mock Broker startup
    mock_broker = None
    bootstrap = args.bootstrap_server
    if args.mock_broker:
        mock_broker = MockKafkaBroker(host="127.0.0.1", port=0)
        mock_port = mock_broker.start()
        bootstrap = f"127.0.0.1:{mock_port}"
        if not args.json_output:
            print(f"\033[94m[INFO] Spawned embedded Mock Kafka Broker on {bootstrap}\033[0m")

    reporter = TestReporter(json_output=args.json_output)

    # Determine scenarios to run
    scenario_keys: List[str] = []
    if args.scenario == "all":
        scenario_keys = [
            "basic",
            "compression",
            "headers",
            "keys",
            "large",
            "idempotence",
            "transactions",
            "security",
        ]
    else:
        scenario_keys = [args.scenario]

    kwargs = {
        "acks": args.acks,
        "batch_size": args.batch_size,
        "linger_ms": args.linger_ms,
        "compression": args.compression,
        "payload_size": args.payload_size,
        "transactional_id": args.transactional_id,
        "security_protocol": args.security_protocol,
        "sasl_mechanism": args.sasl_mechanism,
        "sasl_username": args.sasl_username,
        "sasl_password": args.sasl_password,
        "ca_cert": args.ca_cert,
        "client_cert": args.client_cert,
        "client_key": args.client_key,
    }

    if not args.json_output:
        print("\n" + "=" * 78)
        print("          AEROSTREAM KAFKA WIRE PROTOCOL PRODUCER SUITE")
        print(f"  Target: {bootstrap} | Topic: {args.topic} | Backend: {args.backend}")
        print("=" * 78)

    all_passed = True
    try:
        for sc_name in scenario_keys:
            sc_cls = SCENARIO_REGISTRY[sc_name]
            instance = sc_cls(
                bootstrap_servers=bootstrap,
                topic=args.topic,
                default_count=args.count,
                backend=args.backend,
                timeout_sec=args.timeout,
            )

            results: List[ScenarioResult] = instance.run(count=args.count, **kwargs)
            for r in results:
                reporter.record(r)
                if not r.success:
                    all_passed = False

    finally:
        if mock_broker:
            mock_broker.stop()
            if not args.json_output:
                print("\033[94m[INFO] Stopped embedded Mock Kafka Broker\033[0m")

    reporter.print_summary()
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
