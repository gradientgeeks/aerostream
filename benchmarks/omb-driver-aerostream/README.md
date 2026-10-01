# OMB driver for AeroStream's native protocol

An [OpenMessaging Benchmark](https://github.com/openmessaging/benchmark) driver that talks to AeroStream's **native data-plane
protocol** (TCP port `9091`, framing documented in `src/main/java/.../NativeProtocol.java`) and creates topics through the
controller's REST API (port `9001`). It uses plain TCP, so it needs no client library. Each producer uses one pipelined connection
(`connectionsPerProducer`, default 1) that carries all partitions, batching (`lingerMicros`, default 1000 = Kafka's `linger.ms=1`) and grouping requests by partition; consumers use
one multi-entry long-poll fetch thread per partition.

This is a benchmark tool kept in the AeroStream repository; it is applied to an unmodified upstream OMB checkout at build time
(no fork, no upstream pull request):

```bash
git clone https://github.com/openmessaging/openmessaging-benchmark.git omb && cd omb && git checkout 5b1fa70951a323da26bd587174b58bb2c65b0b5c
../benchmarks/omb-driver-aerostream/apply.sh .        # copies the driver in and registers the Maven module
mvn -q -B clean install -DskipTests -Dlicense.skip=true -Dspotless.check.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true \
    -pl benchmark-framework,driver-kafka,driver-aerostream,package -am
```

`aerostream-native.yaml` is the driver configuration (`controllerHttpUrl`, `brokerAddress`, `maxInFlightPerConnection`,
`fetchMaxBytes`, `fetchMaxWaitMs`). The EC2 scripts in [`../aws-ec2/`](../aws-ec2/README.md) do all of this automatically.
