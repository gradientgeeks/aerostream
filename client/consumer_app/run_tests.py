#!/usr/bin/env python3
"""AeroStream Kafka Consumer Test Suite Self-Test Runner.

Runs:
1. Unit tests: Syntax, model validation, config mappings, wire codec encoding/decoding.
2. Integration self-tests: Automatically boots an ephemeral rust-broker and executes
   all consumer scenarios (simple, group, rebalance, offsets, longpoll, isolation, headers, security).
"""

from __future__ import annotations

import argparse
import os
import sys
import unittest
from pathlib import Path

# Add package directory to PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from consumer_app.utils.broker_runner import EphemeralBroker


def run_unit_tests() -> bool:
    print("\n" + "=" * 70)
    print("PHASE 1: RUNNING UNIT TESTS (SYNTAX, IMPORTS, CODECS, MODELS)")
    print("=" * 70)
    loader = unittest.TestLoader()
    suite = loader.discover(
        start_dir=str(Path(__file__).parent / "tests"),
        pattern="test_*.py",
        top_level_dir=str(root_dir),
    )
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_e2e_scenarios(bootstrap_server: str = "127.0.0.1:19093") -> bool:
    print("\n" + "=" * 70)
    print("PHASE 2: RUNNING LIVE E2E SCENARIOS AGAINST BROKER")
    print("=" * 70)
    from consumer_app.cli import main as cli_main

    argv = [
        "--scenario", "all",
        "--bootstrap-server", bootstrap_server,
        "--auto-start-broker",
        "--expected-count", "3",
    ]
    exit_code = cli_main(argv)
    return exit_code == 0


def main() -> int:
    parser = argparse.ArgumentParser(description="AeroStream Kafka Consumer Self-Test Suite")
    parser.add_argument("--unit-only", action="store_true", help="Run only unit tests without live broker")
    parser.add_argument("--bootstrap-server", default="127.0.0.1:19093", help="Target broker address for live tests")
    args = parser.parse_args()

    unit_ok = run_unit_tests()
    if not unit_ok:
        print("\n❌ Unit tests FAILED!")
        return 1
    print("\n✅ All unit tests PASSED successfully!")

    if args.unit_only:
        return 0

    e2e_ok = run_e2e_scenarios(bootstrap_server=args.bootstrap_server)
    if not e2e_ok:
        print("\n❌ E2E scenario execution FAILED!")
        return 1

    print("\n🎉 ALL UNIT AND E2E CONSUMER TESTS PASSED SUCCESSFULLY!\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
