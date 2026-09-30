# AeroStream EC2 benchmark summary

- run `20260930-124623` | `c6id.2xlarge` (us-east-1/us-east-1a) | single machine: broker pinned to vCPUs 0,1,4,5 (8 GiB), OMB pinned to vCPUs 2,3,6,7
- profile `aero8`: 2 round(s), warm-up 2 min, test 5 min, acks=1, 1 broker, no replication
- build 99bbdbb + batched-append optimization | image `quay.io/gradientgeeks/aerostream:latest` (sha256:1fd1a44a7c8c) | OMB `5b1fa7095`

## Workload `1kb-32p-100k-8x8`

| system | rounds | publish msg/s (median) | range | MB/s | consume msg/s | pub p50 ms | p95 ms | p99 ms | p99.9 ms | e2e p99 ms | errors | broker peak mem MiB | broker CPU % (docker peak) | broker pinned-cores busy % | load-gen CPU % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aerostream | 2 | 100,082 | 100,080-100,084 | 97.7 | 100,082 | 0.7 | 1.2 | 1.4 | 2.3 | 2.0 | 0 | 7409 | 79 | 14 | 48 |

## Workload `1kb-32p-200k-8x8`

| system | rounds | publish msg/s (median) | range | MB/s | consume msg/s | pub p50 ms | p95 ms | p99 ms | p99.9 ms | e2e p99 ms | errors | broker peak mem MiB | broker CPU % (docker peak) | broker pinned-cores busy % | load-gen CPU % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aerostream | 2 | 200,175 | 200,157-200,194 | 195.5 | 200,175 | 0.7 | 1.3 | 1.7 | 3.0 | 2.0 | 0 | 7662 | 97 | 22 | 59 |

## Workload `1kb-32p-maxrate-8x8`

| system | rounds | publish msg/s (median) | range | MB/s | consume msg/s | pub p50 ms | p95 ms | p99 ms | p99.9 ms | e2e p99 ms | errors | broker peak mem MiB | broker CPU % (docker peak) | broker pinned-cores busy % | load-gen CPU % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| aerostream | 2 | 271,350 | 271,231-271,469 | 265.0 | 271,352 | 105.1 | 812.7 | 1103.9 | 1376.2 | 1118.5 | 0 | 7470 | 118 | 55 | 62 |

## Validity notes

- load generator saturated (>85% CPU) in 0 run(s): none, so the client was not the bottleneck.
- max-rate workloads measure a saturation point; fixed-rate workloads are the fair place to compare latency (equal offered load).
- rounds rotate the system order; compare the min-max range before trusting a difference smaller than that range.
