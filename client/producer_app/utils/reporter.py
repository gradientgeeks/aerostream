"""
AeroStream Kafka Producer Test Suite - Result Reporting & Metrics.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ScenarioResult:
    scenario: str
    subtest: str
    success: bool
    messages_sent: int = 0
    bytes_sent: int = 0
    duration_sec: float = 0.0
    throughput_msgs_sec: float = 0.0
    throughput_kb_sec: float = 0.0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    partition_distribution: Dict[int, int] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if self.duration_sec > 0 and self.messages_sent > 0:
            d["throughput_msgs_sec"] = round(self.messages_sent / self.duration_sec, 2)
            d["throughput_kb_sec"] = round((self.bytes_sent / 1024.0) / self.duration_sec, 2)
        return d


class TestReporter:
    def __init__(self, json_output: bool = False):
        self.json_output = json_output
        self.results: List[ScenarioResult] = []

    def record(self, result: ScenarioResult) -> None:
        self.results.append(result)
        if not self.json_output:
            self._print_result(result)

    def _print_result(self, r: ScenarioResult) -> None:
        status_color = "\033[92m[PASS]\033[0m" if r.success else "\033[91m[FAIL]\033[0m"
        print(f"\n{status_color} \033[1m{r.scenario} :: {r.subtest}\033[0m")
        print(f"  • Sent: {r.messages_sent:,} msgs ({r.bytes_sent / 1024:.2f} KB) in {r.duration_sec:.3f}s")
        if r.duration_sec > 0 and r.messages_sent > 0:
            tp_msg = r.messages_sent / r.duration_sec
            tp_kb = (r.bytes_sent / 1024.0) / r.duration_sec
            print(f"  • Throughput: {tp_msg:,.1f} msgs/sec | {tp_kb:,.2f} KB/sec")
        if r.avg_latency_ms > 0:
            print(f"  • Latency: avg={r.avg_latency_ms:.2f}ms, p95={r.p95_latency_ms:.2f}ms, p99={r.p99_latency_ms:.2f}ms")
        if r.partition_distribution:
            dist_str = ", ".join(f"P{p}: {c}" for p, c in sorted(r.partition_distribution.items()))
            print(f"  • Partitions: {dist_str}")
        if r.details:
            for k, v in r.details.items():
                print(f"  • {k}: {v}")
        if r.error:
            print(f"  \033[91m• Error: {r.error}\033[0m")

    def print_summary(self) -> None:
        if self.json_output:
            out = {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.success),
                "failed": sum(1 for r in self.results if not r.success),
                "results": [r.to_dict() for r in self.results],
            }
            print(json.dumps(out, indent=2))
            return

        total = len(self.results)
        passed = sum(1 for r in self.results if r.success)
        failed = total - passed

        print("\n" + "=" * 78)
        print("                  AEROSTREAM PRODUCER TEST SUITE SUMMARY")
        print("=" * 78)
        
        # Try importing tabulate for pretty table
        try:
            from tabulate import tabulate
            table_data = []
            for r in self.results:
                status = "PASS" if r.success else "FAIL"
                tp = f"{r.messages_sent / r.duration_sec:,.0f} m/s" if r.duration_sec > 0 and r.messages_sent > 0 else "-"
                lat = f"{r.avg_latency_ms:.1f}ms" if r.avg_latency_ms > 0 else "-"
                table_data.append([
                    r.scenario,
                    r.subtest,
                    status,
                    f"{r.messages_sent:,}",
                    f"{r.duration_sec:.2f}s",
                    tp,
                    lat,
                ])
            headers = ["Scenario", "Subtest", "Status", "Msgs", "Duration", "Throughput", "Avg Latency"]
            print(tabulate(table_data, headers=headers, tablefmt="github"))
        except ImportError:
            # Fallback simple table
            print(f"{'Scenario':<18} | {'Subtest':<22} | {'Status':<6} | {'Msgs':<8} | {'Duration':<8}")
            print("-" * 78)
            for r in self.results:
                status = "PASS" if r.success else "FAIL"
                print(f"{r.scenario:<18} | {r.subtest:<22} | {status:<6} | {r.messages_sent:<8} | {r.duration_sec:.2f}s")

        print("=" * 78)
        summary_color = "\033[92m" if failed == 0 else "\033[91m"
        print(f"Total: {total} | {summary_color}Passed: {passed}\033[0m | Failed: {failed}")
        print("=" * 78 + "\n")
