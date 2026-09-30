# AeroStream benchmark on AWS EC2

One command creates an EC2 machine, runs the OpenMessaging Benchmark (OMB) against AeroStream's Kafka wire port, copies the
results to your machine with `scp`, and **destroys every AWS resource**. Everything is plain `bash` + the `aws` CLI; read the
scripts in order to see exactly how the test is performed.

```
./run-aerostream-8core.sh          # ONE 8 vCPU / 16 GiB machine; prints the plan and estimate, creates nothing
./run-aerostream-8core.sh --yes    # runs it (about 80 minutes, roughly $0.55)

./run-e2e.sh                       # two-machine topology (broker node + load-generator node); plan only
./run-e2e.sh --yes                 # runs it
PROFILE=quick|fair|full MODE=parity|fullnode ./run-e2e.sh --yes
```

Latest results: [`../BENCHMARK.md`](../BENCHMARK.md) and [`../omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/`](../omb-results/aws-c6id-2xlarge-aerostream-2026-09-30/).
On the 8 vCPU machine AeroStream sustained **271,350 msg/s** (265 MB/s of 1 KB messages) and held publish $p_{99}$ at **1.4 ms** at 100,000 msg/s and **1.7 ms** at 200,000 msg/s.

## Topologies

**Single machine (`run-aerostream-8core.sh`)** - one `c6id.2xlarge` (Xeon Platinum 8375C, 8 vCPU = 4 physical cores, 16 GiB, local NVMe):

```
 your machine --ssh/scp-->  c6id.2xlarge
                             physical cores 0-1 (vCPUs 0,1,4,5): AeroStream container, 8 GiB, host networking
                             physical cores 2-3 (vCPUs 2,3,6,7): OMB load generator, pinned with taskset
```

The broker and the load generator never share a core (hyperthread siblings are kept together), and CPU is measured separately on
each set of cores, so the results show which side was busier.

**Two machines (`run-e2e.sh`)** - a `c6id.xlarge` broker node and a `c6i.xlarge` load-generator node (4 vCPU, 8 GiB each) in the
same availability zone and subnet.

## What makes the results trustworthy

| Rule | Why |
|---|---|
| Load generator and broker on **separate cores** (or machines) | the load generator cannot take the broker's CPU |
| Identical OMB client settings in every run (`drivers/*.yaml`: `acks=1`, `linger.ms=1`, 128 KiB batches) | a run differs only by its workload |
| Fresh data directory and dropped page cache before every run, host networking, local NVMe | no leftovers, no docker-proxy in the data path |
| Several rounds per workload; the min-max range is reported | shows run-to-run repeatability |
| Warm-up excluded from the measurement | steady state, not start-up |
| Max-rate **and fixed-rate** workloads | max-rate finds the saturation point; fixed-rate (same offered load in every run) is the right place to read latency |
| Load-generator CPU sampled and reported | if the load generator is over 85% busy the summary flags it |
| Image digest and OMB commit recorded (`run-info.json`, `image-digests.txt`) | reproducible |

## Steps

| Script | Does | Time |
|---|---|---|
| `01-create-service-account.sh` | one-time: IAM user `aerostream-bench` + tag-scoped policies (already done) | seconds |
| `02-preflight.sh` | identity, EC2 dry-runs, AMI, subnet; creates nothing | ~10 s |
| `03-provision.sh` | key pair, security group (SSH from your IP only), tagged instance(s) with a self-destruct timer | ~3 min |
| `04-bootstrap.sh` | Docker, Java 17, Maven 3.9, OMB build at a pinned commit; ships the AeroStream image from your machine; splits the cores | ~6 min |
| `05-run-benchmarks.sh` | the measurement loop; each run's files are copied back as soon as it finishes | workload dependent |
| `06-collect.sh` | `scp` of the remaining files + host info, writes `summary.md` / `summary.csv` | ~1 min |
| `07-destroy.sh` | terminate, delete the security group and key pair, **verify nothing tagged remains** | ~1 min |
| `monitor.sh` | prints a status line every 5 minutes for a run (`RUN_ID=<id> ./monitor.sh`) | |

| Profile | Runs | Test time | Wall time (estimate) |
|---|---|---|---|
| `aero8` (single machine, default of `run-aerostream-8core.sh`) | 2 rounds x 3 workloads (max rate, fixed 100k and 200k msg/s; 8 producers, 8 consumers, 32 partitions) = 6 | 2 min warm-up + 5 min | about 80-100 min (estimate 101 min; the run took 82) |
| `quick` (two machines) | 1 round x 2 workloads = 2 | 1 + 3 min | about 48 min |
| `fair` (two machines) | 2 rounds x 2 workloads = 4 | 2 + 5 min | about 78 min |
| `full` (two machines) | 3 rounds x 4 workloads = 12 | 2 + 10 min | about 3 h 50 min |

Exact numbers for your settings are printed by `./run-aerostream-8core.sh` / `./run-e2e.sh` before anything is created.
`PROFILE=aero8` uses 8 producers, 8 consumers and 32 partitions so the load generator is not the limit; the other profiles use
2 producers, 2 consumers and 16 partitions.

## Safety

- The service account can only launch `c6i.xlarge`, `c6id.xlarge`, `c6i.2xlarge`, `c6id.2xlarge` (plus the pre-existing
  `c7i-flex.large`), only when tagged `Project=aerostream-bench`; it can only terminate/delete tagged resources and cannot touch IAM.
- Each instance is launched with *shutdown = terminate* and `shutdown -h +N`: it deletes itself after N minutes even if your
  machine loses power. The run scripts set N = estimate + 75 minutes.
- The run script destroys everything in an `EXIT/INT/TERM/HUP` trap, after collecting whatever results exist. `KEEP=1` disables that.
- SSH is open to your current public IP only. Keys live in `state/` (git-ignored) and are shredded on destroy.
- The account's on-demand quota in `us-east-1` is 8 vCPUs, so one 8 vCPU machine, or two 4 vCPU machines, is the most that fits.
- Interrupted run? `RESUME=1 RUN_ID=<id> ./run-e2e.sh --yes` continues on the existing instances (skips provisioning).

## Files

`config.env` settings and profiles - `lib.sh` helpers - `drivers/` OMB drivers - `workloads/` OMB workloads -
`remote/` scripts that run on the machines - `summarize.py` - `results/<run-id>/` output (`summary.md`, raw JSON, logs, host info).
