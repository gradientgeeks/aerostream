# Benchmarks

* [BENCHMARK.md](BENCHMARK.md) - AeroStream OpenMessaging Benchmark (OMB) results: AWS EC2 (`c6id.2xlarge`), resource-capped container runs, and partition-density measurements
* [omb-results/](omb-results/) - Raw OMB test runs, telemetry and per-workload summaries (the latest EC2 dataset is `aws-c6id-2xlarge-aerostream-2026-09-30/`)
* [aws-ec2/](aws-ec2/README.md) - Scripts that create an EC2 machine, run OMB against AeroStream, copy the results back and destroy the machine
* [../core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md](../core/docs/OPENMESSAGING_BENCHMARK_GUIDE.md) - Reproducible OMB compilation and execution guide
* [KAFKA_PORT_PERFORMANCE.md](KAFKA_PORT_PERFORMANCE.md) - Performance engineering write-up on the Kafka protocol port: zero-copy, socket and storage-path optimizations
* `openmessaging-benchmark/` - Upstream OpenMessaging Benchmark framework and driver definitions
