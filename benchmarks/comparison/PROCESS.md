# Process

How the 3-way comparison (Apache Kafka, Redpanda, AeroStream) is run, and why. The exact commands are in
[COMMANDS.md](COMMANDS.md); results and analysis are in [../BENCHMARK.md](../BENCHMARK.md).

## Principles
1. **Same limits for every broker**: `--cpus=2.0 --memory=2g`, one fresh container per system, single node,
   1 partition, replication factor 1, `acks=1`.
2. **Vendor tools only, no custom load generators.** Kafka and Redpanda are driven by Kafka's own
   `kafka-producer-perf-test.sh` (from the `apache/kafka` image, same properties for both). AeroStream is driven by the
   repository's own client (`client/bin/client bench`). Resource usage comes from `docker stats`.
3. **Fresh topic per run, 3 runs per workload**; the tables report the median run and the raw logs are kept.
4. **Everything scripted, everything logged**: each run's raw tool output is stored unchanged under `results/<label>/`.

## Steps
1. Build the AeroStream controller and broker images and the native client from the source under test
   (`scripts/build-aerostream.sh <tag> <repo>`). A second checkout of `main` is used to measure the previous release.
2. For each system, in turn: remove all benchmark containers, start the system (`scripts/start-*.sh`), wait until it
   answers, wait 10 s, record idle memory.
3. Start a background `docker stats` sampler, run all workloads x 3 runs (`scripts/run-workloads.sh`), stop the sampler.
4. Summarise (`scripts/summarize.sh`, `scripts/peaks.sh`): per-run throughput and latency, peak memory / CPU / PIDs.
5. Compare the medians in `BENCHMARK.md`.

## Deliberate choices (differences from a naive setup)
* **Host networking for all containers.** Kafka and Redpanda used to run on Docker's bridge with published ports while
  AeroStream used host networking; loopback traffic to a published port passes through Docker's userland proxy, a cost only
  the bridge-networked systems pay. All four now use `--network host`, with ports picked not to clash with a local cluster
  (Kafka 9094 / 19192 / 19093, Redpanda 19092, AeroStream 9095 / 9096 / 27001 / 28001 / 29001).
* **A dedicated AeroStream controller** on its own ports. The benchmark broker registers there, so a locally running
  AeroStream cluster (`:8001`) never receives benchmark topics or partitions. The controller container is not CPU-limited;
  it is idle during the runs. Only the broker container has the 2 CPU / 2 GB limit.
* **Fresh images from source.** AeroStream images are built from the exact commit under test, not taken from a stale
  local `latest` tag.
* **Redpanda message-size settings**: `kafka_batch_max_bytes` and `kafka_request_max_bytes` are raised (the earlier
  `kafka_max_message_bytes` is not a Redpanda cluster property) so 50 MB messages are accepted; topics also set
  `max.message.bytes`.

## What the numbers do and do not mean
* **AeroStream's native client is closed-loop**: each of its N producers sends one message, waits for the ack, then sends
  the next. Its latencies are round-trip times of a lightly loaded broker. `kafka-producer-perf-test.sh` pipelines many
  requests, so its latency includes queueing under saturation. Do not rank the two latency columns against each other;
  compare throughput.
* **The native client uses AeroStream's native data-plane protocol, not the Kafka protocol.** Kafka clients use the Kafka
  port, which is measured separately (see BENCHMARK.md, "Kafka port").
* **Durability is not equalised.** Kafka `acks=1` and AeroStream acknowledge from the OS page cache; Redpanda by default
  flushes to disk before acknowledging.
* **Concurrency differs by design**: the AeroStream client is run with 10 (or 5 for 50 MB) concurrent producers, following
  the original benchmark, while the Kafka tool runs one producer with internal batching.
* **Host effects**: a laptop-class CPU with frequency scaling and other load; run-to-run spread is real (see the per-run
  TSV files). Use medians and treat differences under ~20% as noise.
* **Not measured**: consume path, multiple partitions, replication (RF > 1), fsync-durable acknowledgements.
