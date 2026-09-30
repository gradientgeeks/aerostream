#!/usr/bin/env python3
"""Splits this machine's PHYSICAL cores in half (keeping hyperthread siblings together) and writes ~/bench/cpusets.env:
   BROKER_CPUSET = first half of the cores, CLIENT_CPUSET = second half. Used for the single-machine topology so the broker
   and the OMB load generator never share a core."""
import collections, subprocess
rows = [l.split(",") for l in subprocess.check_output(["lscpu", "-p=CPU,CORE"]).decode().splitlines() if not l.startswith("#")]
cores = collections.OrderedDict()
for cpu, core in rows:
    cores.setdefault(int(core), []).append(int(cpu))
ids = sorted(cores); half = len(ids) // 2
fmt = lambda ks: ",".join(str(c) for c in sorted(c for k in ks for c in cores[k]))
open("/home/ec2-user/bench/cpusets.env", "w").write(f"BROKER_CPUSET={fmt(ids[:half])}\nCLIENT_CPUSET={fmt(ids[half:])}\n")
print(open("/home/ec2-user/bench/cpusets.env").read().strip().replace("\n", "  "))
