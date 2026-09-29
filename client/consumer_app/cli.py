#!/usr/bin/env python3
"""AeroStream Kafka Consumer Test Suite CLI.

Exercises and verifies all Kafka consumer features supported by AeroStream
(direct partition assignment, consumer group rebalance, multi-consumer coordination,
offset commits, long polling, transaction isolation, headers, and security).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import List

from .models import ScenarioResult, SuiteReport
from .scenarios import SCENARIO_MAP
from .utils.broker_runner import EphemeralBroker


def setup_logging(verbose: bool, json_output: bool = False):
    level = logging.DEBUG if verbose else (logging.WARNING if json_output else logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%H:%M:%S", stream=sys.stderr)


def print_suite_table(report: SuiteReport):
    """Prints a clean ASCII table of scenario results."""
    print("\n" + "=" * 80)
    print(f"{'AEROSTREAM KAFKA CONSUMER TEST SUITE REPORT':^80}")
    print("=" * 80)
    print(f"Bootstrap Server : {report.bootstrap_server}")
    print(f"Total Scenarios  : {len(report.scenarios)}")
    print(f"Passed           : {report.passed}")
    print(f"Failed           : {report.failed}")
    print(f"Total Duration   : {report.total_duration_ms:.2f} ms")
    print("-" * 80)
    print(f"{'STATUS':<8} | {'SCENARIO':<14} | {'DURATION':<12} | {'RECORDS':<8} | {'DETAILS'}")
    print("-" * 80)

    for s in report.scenarios:
        status_str = "✅ PASS" if s.success else "❌ FAIL"
        dur_str = f"{s.duration_ms:.2f} ms"
        rec_str = str(s.records_consumed)
        detail_msg = s.error if s.error else f"{len(s.details)} check(s) verified"
        print(f"{status_str:<8} | {s.scenario_name:<14} | {dur_str:<12} | {rec_str:<8} | {detail_msg}")

    print("=" * 80)
    if report.all_successful:
        print(f"{'🎉 ALL SCENARIOS COMPLETED SUCCESSFULLY':^80}")
    else:
        print(f"{'⚠️  SOME SCENARIOS FAILED':^80}")
    print("=" * 80 + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AeroStream Kafka Consumer Test Suite (port 9092/9093)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        default="all",
        choices=["all"] + list(SCENARIO_MAP.keys()),
        help="Test scenario to execute (all, simple, group, rebalance, offsets, isolation, longpoll, headers, security)",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9093",
        help="Kafka bootstrap server address (host:port)",
    )
    parser.add_argument(
        "--topic",
        default="aeromq-consumer-test-topic",
        help="Target topic name for consumer test execution",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        help="Consumer group.id (defaults to dynamically generated unique id)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="Operation timeout in seconds",
    )
    parser.add_argument(
        "--expected-count",
        type=int,
        default=3,
        help="Expected record count to consume",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output results strictly as a formatted JSON document",
    )
    parser.add_argument(
        "--auto-start-broker",
        action="store_true",
        help="Automatically spawn an ephemeral rust-broker instance if bootstrap-server is unreachable",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )
    return parser


def is_server_reachable(bootstrap_server: str) -> bool:
    import socket
    try:
        host, port_str = bootstrap_server.split(":")
        with socket.create_connection((host, int(port_str)), timeout=0.3):
            return True
    except OSError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.verbose, json_output=args.json_output)
    logger = logging.getLogger("aeromq-consumer")

    scenarios_to_run: List[str] = (
        list(SCENARIO_MAP.keys()) if args.scenario == "all" else [args.scenario]
    )

    broker_handle: EphemeralBroker | None = None
    server_reachable = is_server_reachable(args.bootstrap_server)

    if not server_reachable:
        if args.auto_start_broker or args.bootstrap_server == "127.0.0.1:9093":
            logger.info(
                f"Broker not responding on {args.bootstrap_server}. Spawning ephemeral rust-broker..."
            )
            host, port_str = args.bootstrap_server.split(":")
            broker_handle = EphemeralBroker(
                host=host,
                data_port=19091,
                kafka_port=int(port_str),
            )
            try:
                broker_handle.start()
                logger.info(f"Ephemeral rust-broker listening on {args.bootstrap_server}")
            except Exception as e:
                logger.error(f"Failed to spawn ephemeral broker: {e}")
                if not args.json_output:
                    print(f"Error: Unable to connect to {args.bootstrap_server} and ephemeral broker start failed: {e}")
                return 1
        else:
            logger.error(f"Cannot reach broker at {args.bootstrap_server}. Use --auto-start-broker or start broker.")
            return 1

    report = SuiteReport(bootstrap_server=args.bootstrap_server)
    t_start = time.perf_counter()

    try:
        for name in scenarios_to_run:
            scenario_cls = SCENARIO_MAP[name]
            instance = scenario_cls(
                bootstrap_server=args.bootstrap_server,
                topic=f"{args.topic}-{name}-{int(time.time()*1000) % 100000}",
                group_id=args.group_id,
                timeout=args.timeout,
                expected_count=args.expected_count,
            )
            res = instance.run()
            report.scenarios.append(res)
    finally:
        if broker_handle:
            logger.info("Stopping ephemeral rust-broker...")
            broker_handle.stop()

    report.total_duration_ms = (time.perf_counter() - t_start) * 1000

    if args.json_output:
        print(report.to_json())
    else:
        print_suite_table(report)

    return 0 if report.all_successful else 1


if __name__ == "__main__":
    sys.exit(main())
