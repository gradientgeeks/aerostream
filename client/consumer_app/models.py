"""Data models and schemas for AeroStream Kafka Consumer Test Suite."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Any, Dict, List, Optional


@dataclass
class ConsumedRecord:
    topic: str
    partition: int
    offset: int
    key: Optional[bytes] = None
    value: Optional[bytes] = None
    headers: List[tuple[str, bytes]] = field(default_factory=list)
    timestamp: Optional[int] = None
    sha256_checksum: str = ""

    def __post_init__(self):
        if self.value is not None and not self.sha256_checksum:
            self.sha256_checksum = hashlib.sha256(self.value).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key.decode("utf-8", errors="replace") if self.key else None,
            "value_preview": (
                self.value.decode("utf-8", errors="replace")[:100]
                if self.value
                else None
            ),
            "value_len": len(self.value) if self.value else 0,
            "headers": [
                (k, v.decode("utf-8", errors="replace")) for k, v in self.headers
            ],
            "timestamp": self.timestamp,
            "sha256": self.sha256_checksum,
        }


@dataclass
class ScenarioResult:
    scenario_name: str
    success: bool
    duration_ms: float
    records_consumed: int = 0
    records: List[ConsumedRecord] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scenario": self.scenario_name,
            "success": self.success,
            "duration_ms": round(self.duration_ms, 2),
            "records_consumed": self.records_consumed,
            "details": self.details,
            "error": self.error,
            "warnings": self.warnings,
            "sample_records": [r.to_dict() for r in self.records[:5]],
        }


@dataclass
class SuiteReport:
    timestamp: float = field(default_factory=time.time)
    bootstrap_server: str = ""
    scenarios: List[ScenarioResult] = field(default_factory=list)
    total_duration_ms: float = 0.0

    @property
    def passed(self) -> int:
        return sum(1 for s in self.scenarios if s.success)

    @property
    def failed(self) -> int:
        return sum(1 for s in self.scenarios if not s.success)

    @property
    def all_successful(self) -> bool:
        return len(self.scenarios) > 0 and self.failed == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "bootstrap_server": self.bootstrap_server,
            "total_scenarios": len(self.scenarios),
            "passed": self.passed,
            "failed": self.failed,
            "all_passed": self.all_successful,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "scenarios": [s.to_dict() for s in self.scenarios],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
