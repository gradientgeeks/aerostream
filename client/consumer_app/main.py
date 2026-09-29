#!/usr/bin/env python3
"""Main entry point for AeroStream Kafka Consumer Test Suite."""

import sys
from pathlib import Path

# Add parent directory to path so relative or direct imports work seamlessly
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from consumer_app.cli import main

if __name__ == "__main__":
    sys.exit(main())
