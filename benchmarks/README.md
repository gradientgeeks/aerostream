# Benchmarks

* [BENCHMARK.md](BENCHMARK.md) - results and analysis of the Kafka / Redpanda / AeroStream comparison, including the Kafka-port investigation
* [KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md) - why the Kafka port was slow, what was fixed (with measurements), what is left, and the sources read
* [comparison/PROCESS.md](comparison/PROCESS.md) - how the comparison is run and why (principles, deliberate choices, caveats)
* [comparison/COMMANDS.md](comparison/COMMANDS.md) - every command (`docker run`, topic creation, workloads, stats)
* `comparison/scripts/` - the bash scripts behind those commands (`run-all.sh` runs a full session)
* `comparison/results/` - raw tool output, per-run summaries, resource samples and the profiling evidence, one directory per session
