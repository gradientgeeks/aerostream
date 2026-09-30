#!/usr/bin/env python3
"""Summarise an EC2 benchmark run:  summarize.py results/<run-id>  ->  summary.md + summary.csv

For every (workload, system) it aggregates the rounds (median, plus min-max so run-to-run variance is visible) and reports
publish/consume rate, publish and end-to-end latency percentiles, errors, broker peak memory/CPU (docker stats) and the
load generator's average CPU busy% (if that is near 100% the client, not the broker, was the bottleneck)."""
import csv, glob, json, os, re, statistics as st, sys
from collections import defaultdict

root = sys.argv[1]
info = json.load(open(os.path.join(root, "run-info.json"))) if os.path.exists(os.path.join(root, "run-info.json")) else {}
avg = lambda v: sum(v) / len(v) if v else 0.0

def sar_busy(path):
    try:
        for line in open(path):
            if line.startswith("Average:") and " all " in line:
                return 100.0 - float(line.split()[-1])
    except OSError:
        pass
    return None

def broker_peak(path):
    mem = cpu = 0.0
    try:
        for line in open(path):
            f = line.strip().split(",")
            if len(f) < 2: continue
            m = re.match(r"([\d.]+)\s*([KMG]i?B)", f[0])
            if m: mem = max(mem, float(m.group(1)) * {"KiB": 1/1024, "MiB": 1, "GiB": 1024, "KB": 1/1024, "MB": 1, "GB": 1024}.get(m.group(2), 1))
            try: cpu = max(cpu, float(f[1].rstrip("%")))
            except ValueError: pass
    except OSError:
        pass
    return mem, cpu

runs = defaultdict(list)   # (workload, system) -> [metrics per round]
for rj in sorted(glob.glob(os.path.join(root, "client", "*", "result.json"))):
    label = os.path.basename(os.path.dirname(rj)); wl, sysname, rnd = label.split("__")
    d = json.load(open(rj)); p, c, sz = d["publishRate"], d["consumeRate"], d["messageSize"]
    # OMB reports one sample per 10 s; drop the first sample of the measured window (ramp) only if there are enough
    mem, cpu = broker_peak(os.path.join(root, "broker", label, "broker-stats.csv"))
    runs[(wl, sysname)].append(dict(
        round=rnd, pub=avg(p), pub_mb=avg(p) * sz / 1048576, cons=avg(c), peak=max(p) if p else 0,
        p50=d["aggregatedPublishLatency50pct"], p95=d["aggregatedPublishLatency95pct"], p99=d["aggregatedPublishLatency99pct"],
        p999=d["aggregatedPublishLatency999pct"], e2e50=d["aggregatedEndToEndLatency50pct"], e2e99=d["aggregatedEndToEndLatency99pct"],
        err=sum(d["publishErrorRate"]), mem=mem, cpu=cpu, gen=sar_busy(os.path.join(root, "client", label, "sar-cpu.txt")),
        bcpu=sar_busy(os.path.join(root, "client", label, "sar-cpu-broker.txt"))))

med = lambda rs, k: st.median([r[k] for r in rs])
rng = lambda rs, k, f="{:,.0f}": f"{f.format(min(r[k] for r in rs))}-{f.format(max(r[k] for r in rs))}"
out = ["# AeroStream EC2 benchmark summary", ""]
if info:
    machine = info.get("broker_type") if info.get("topology") == "single" else f"broker {info.get('broker_type')} + client {info.get('client_type')}"
    images = info.get("images") or {k[:-6]: v for k, v in info.items() if k.endswith("_image") and v}
    out += [f"- run `{info.get('run_id')}` | `{machine}` ({info.get('region')}/{info.get('az')}) | mode `{info.get('mode')}`",
            f"- profile `{info.get('profile')}`: {info.get('rounds')} round(s), warm-up {info.get('warmup_min')} min, test {info.get('test_min')} min, acks=1, 1 broker, no replication",
            f"- build {info.get('aerostream_git', '')} | images " + ", ".join(f"{k} `{v}`" for k, v in images.items()) + f" | OMB `{str(info.get('omb_commit'))[:9]}`", ""]
rows = []
for wl in sorted({k[0] for k in runs}):
    out += [f"## Workload `{wl}`", "",
            "| system | rounds | publish msg/s (median) | range | MB/s | consume msg/s | pub p50 ms | p95 ms | p99 ms | p99.9 ms | e2e p99 ms | errors | broker peak mem MiB | broker CPU % (docker peak) | broker pinned-cores busy % | load-gen CPU % |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for (w, s), rs in sorted(runs.items()):
        if w != wl: continue
        gens = [r["gen"] for r in rs if r["gen"] is not None]
        out.append(f"| {s} | {len(rs)} | {med(rs,'pub'):,.0f} | {rng(rs,'pub')} | {med(rs,'pub_mb'):.1f} | {med(rs,'cons'):,.0f} | {med(rs,'p50'):.1f} | {med(rs,'p95'):.1f} | "
                   f"{med(rs,'p99'):.1f} | {med(rs,'p999'):.1f} | {med(rs,'e2e99'):.1f} | {sum(r['err'] for r in rs):.0f} | {med(rs,'mem'):.0f} | {med(rs,'cpu'):.0f} | {(avg(b) if (b:=[r['bcpu'] for r in rs if r['bcpu'] is not None]) else float('nan')):.0f} | {(avg(gens) if gens else float('nan')):.0f} |")
        for r in rs: rows.append([w, s, r["round"], f"{r['pub']:.0f}", f"{r['cons']:.0f}", f"{r['p50']:.2f}", f"{r['p95']:.2f}", f"{r['p99']:.2f}", f"{r['p999']:.2f}", f"{r['e2e99']:.2f}", f"{r['err']:.0f}", f"{r['mem']:.0f}", f"{r['cpu']:.0f}", "" if r["gen"] is None else f"{r['gen']:.0f}"])
    out.append("")
sat = [(k, r["gen"]) for k, rs in runs.items() for r in rs if r["gen"] and r["gen"] > 85]
out += ["## Validity notes", "",
        f"- load generator saturated (>85% CPU) in {len(sat)} run(s): " + (", ".join(f"{k[0]}/{k[1]}" for k, _ in sat[:6]) if sat else "none, so the client was not the bottleneck") + ".",
        "- max-rate workloads measure a saturation point; fixed-rate workloads are the fair place to compare latency (equal offered load).",
        "- rounds rotate the system order; compare the min-max range before trusting a difference smaller than that range.", ""]
open(os.path.join(root, "summary.md"), "w").write("\n".join(out))
with open(os.path.join(root, "summary.csv"), "w", newline="") as f:
    w = csv.writer(f); w.writerow("workload system round pub_msg_s cons_msg_s pub_p50_ms p95_ms p99_ms p999_ms e2e_p99_ms errors broker_mem_mib broker_cpu_pct loadgen_cpu_pct".split()); w.writerows(rows)
print("\n".join(out)); print(f"\nwrote {root}/summary.md and summary.csv")
