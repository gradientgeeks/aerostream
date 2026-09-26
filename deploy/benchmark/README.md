# Benchmark harness

Reproduces `docs/BENCHMARK_RESULTS.md`: single-node produce benchmark of Apache Kafka, Redpanda, Apache Pulsar and AeroStream
(Kafka port and native port), each in its own `--cpus=2 --memory=2g` container.

* `run_bench.py` - orchestrates containers, load generators, resource sampling, cold-boot timing; writes `results.json`
* `PulsarGen.java` - pipelined Pulsar producer (same semantics as `kafka-producer-perf-test`), run inside the Pulsar image
* `report.py` - renders the markdown tables from `results.json`
* `results-2026-09-26.json` - raw data behind the published numbers

Requires Docker, Python 3 and a static build of the repo's Go client (`cd client && CGO_ENABLED=0 go build`).
See section 7 of `docs/BENCHMARK_RESULTS.md` for the exact commands.
