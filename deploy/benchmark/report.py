#!/usr/bin/env python3
"""Turn results.json (from run_bench.py) into the markdown tables used in docs/BENCHMARK_RESULTS.md."""
import json, sys

R = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "results.json"))
ORDER = ["kafka", "redpanda", "pulsar", "aerostream-kafka", "aerostream-native"]
SYS = [k for k in ORDER if k in R]
LABELS = {k: R[k]["label"] for k in SYS}
WL = ["100B", "1KB", "1MB", "10MB", "50MB"]


def med(k, w):
    return (R[k]["workloads"].get(w) or {}).get("median")


def fmt(v, d=1):
    return "n/a" if v is None else (f"{v:,.{d}f}")


def ms(v):
    if v is None:
        return "n/a"
    return f"{v:,.3f}" if v < 1 else (f"{v:,.1f}" if v < 100 else f"{v:,.0f}")


def table(title, rows):
    print(f"\n#### {title}\n")
    print("| Metric | " + " | ".join(LABELS[k] for k in SYS) + " |")
    print("| :--- | " + " | ".join("---:" for _ in SYS) + " |")
    for name, fn in rows:
        print(f"| **{name}** | " + " | ".join(fn(k) for k in SYS) + " |")


print("### Throughput summary (MB/s, median of runs)\n")
print("| Payload | " + " | ".join(LABELS[k] for k in SYS) + " |")
print("| :--- | " + " | ".join("---:" for _ in SYS) + " |")
for w in WL:
    cells = []
    for k in SYS:
        m = med(k, w)
        cells.append(fmt(m["mb_per_sec"]) if m else "failed")
    print(f"| {w} | " + " | ".join(cells) + " |")

for w in WL:
    def cell(key, d=1):
        return lambda k: fmt((med(k, w) or {}).get(key), d) if med(k, w) else "failed"

    def latcell(key):
        return lambda k: ms((med(k, w) or {}).get(key)) if med(k, w) else "failed"
    table(f"{w} payload", [
        ("Data throughput (MB/s)", cell("mb_per_sec", 1)),
        ("Messages / s", cell("rec_per_sec", 0)),
        ("Avg latency (ms)", latcell("avg_ms")),
        ("p50 latency (ms)", latcell("p50_ms")),
        ("p95 latency (ms)", latcell("p95_ms")),
        ("p99 latency (ms)", latcell("p99_ms")),
        ("Max latency (ms)", latcell("max_ms")),
        ("Failed runs", lambda k, w=w: str((R[k]["workloads"].get(w) or {}).get("failed", "n/a"))),
    ])

table("Resource footprint and cold boot (container limit: 2 CPU / 2 GiB)", [
    ("Idle memory (MiB)", lambda k: fmt(R[k]["idle_mem_mib"], 1)),
    ("Peak memory under load (MiB)", lambda k: fmt(R[k]["peak_mem_mib"], 1)),
    ("Peak memory (% of 2 GiB)", lambda k: fmt(R[k]["peak_mem_mib"] / 2048 * 100, 1)),
    ("Peak OS threads / PIDs", lambda k: str(R[k]["peak_pids"])),
    ("Peak CPU (% of one core, limit 200)", lambda k: fmt(R[k]["peak_cpu_pct"], 0)),
    ("Cold boot to ready (s, median of 3)", lambda k: fmt(R[k]["cold_boot_s"], 2)),
])
