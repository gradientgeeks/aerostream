#!/usr/bin/env python3
"""Single-node produce benchmark: Apache Kafka, Redpanda, Apache Pulsar, AeroStream.

Every broker runs in its own fresh container with identical limits (--cpus=2 --memory=2g, no swap).
The load generator runs in a separate, unconstrained container that shares the broker's network
namespace, so there is no NAT and no advertised-listener trouble.

  Kafka / Redpanda / AeroStream (Kafka port) : kafka-producer-perf-test.sh (Kafka 4.x client), acks=1
  Pulsar                                     : PulsarGen.java (pipelined sendAsync, same semantics)
  AeroStream native data plane               : the repo's Go `bench` client (1 producer, closed loop)

usage: run_bench.py --aero-image aerostream:tag --aero-client ./aeroclient [--runs 3] [--systems ...]
"""
import argparse, json, os, re, socket, statistics, struct, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
CPUS, MEM = "2.0", "2g"
KAFKA_IMG = "apache/kafka:latest"
RP_IMG = "redpandadata/redpanda:latest"
PULSAR_IMG = "apachepulsar/pulsar:latest"
MAXMSG = 104857600

# label, record size (bytes), record count. Large-message tests move 500 MB in total.
WORKLOADS = [
    ("100B", 100, 500_000),
    ("1KB", 1024, 300_000),
    ("1MB", 1 << 20, 500),
    ("10MB", 10 << 20, 50),
    ("50MB", 50 << 20, 10),
]
PRODUCER_PROPS = ["acks=1", "linger.ms=1", "batch.size=262144", f"max.request.size={MAXMSG}", "buffer.memory=268435456"]


def sh(cmd, timeout=900, check=False):
    r = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"{cmd}\n{r.stdout}\n{r.stderr}")
    return r.stdout + r.stderr


def limits():
    return ["--cpus", CPUS, "--memory", MEM, "--memory-swap", MEM]


def container_ip(name):
    return sh(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name]).strip()


def kafka_api_versions_ok(ip, port=9092):
    """Raw Kafka ApiVersions v0 request; True once the broker answers."""
    try:
        with socket.create_connection((ip, port), timeout=1) as s:
            body = struct.pack(">hhi", 18, 0, 1) + struct.pack(">h", 4) + b"bench"
            s.sendall(struct.pack(">i", len(body)) + body)
            s.settimeout(1)
            hdr = s.recv(4)
            return len(hdr) == 4 and struct.unpack(">i", hdr)[0] > 0
    except OSError:
        return False


def http_ok(url):
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=1) as r:
            return r.status == 200
    except Exception:
        return False


class Sampler(threading.Thread):
    """Polls `docker stats` for peak memory / process count / CPU."""

    def __init__(self, name):
        super().__init__(daemon=True)
        self.name, self.stop, self.mem, self.pids, self.cpu = name, threading.Event(), [], [], []

    def run(self):
        while not self.stop.is_set():
            out = sh(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}|{{.PIDs}}|{{.CPUPerc}}", self.name], timeout=30).strip()
            try:
                m, p, c = out.split("|")
                self.mem.append(to_mib(m.split("/")[0].strip()))
                self.pids.append(int(p))
                self.cpu.append(float(c.strip("%")))
            except Exception:
                pass


def to_mib(s):
    m = re.match(r"([\d.]+)\s*([KMG]i?B)", s)
    v, u = float(m.group(1)), m.group(2)
    return v * {"KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}[u]


def parse_kafka_perf(out):
    m = re.search(r"([\d]+) records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, "
                  r"([\d.]+) ms 50th, ([\d.]+) ms 95th, ([\d.]+) ms 99th", out)
    if not m:
        return {"error": out.strip().splitlines()[-1][:200] if out.strip() else "no output"}
    n, rps, mbps, avg, mx, p50, p95, p99 = m.groups()
    return {"records": int(n), "rec_per_sec": float(rps), "mb_per_sec": float(mbps), "avg_ms": float(avg),
            "p50_ms": float(p50), "p95_ms": float(p95), "p99_ms": float(p99), "max_ms": float(mx),
            "seconds": int(n) / float(rps) if float(rps) else None}


def to_ms(v, unit):
    return float(v) * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}[unit]


def parse_aero_native(out, size, count):
    def grab(label):
        m = re.search(label + r"\s*:\s*([\d.,]+)\s*(ns|µs|us|ms|s)\b", out)
        return to_ms(m.group(1).replace(",", ""), m.group(2)) if m else None
    dur = grab("Total Duration")
    ok = re.search(r"Successful Writes\s*:\s*([\d,]+)", out)
    if dur is None or not ok:
        return {"error": (out.strip().splitlines() or ["no output"])[-1][:200]}
    n = int(ok.group(1).replace(",", ""))
    secs = dur / 1000.0
    return {"records": n, "seconds": secs, "rec_per_sec": n / secs, "mb_per_sec": n * size / secs / 1048576,
            "avg_ms": grab("Avg Latency"), "p50_ms": grab(r"p50 \(Median\)"), "p95_ms": grab("p95"),
            "p99_ms": grab("p99"), "max_ms": grab("Max Latency")}


class System:
    name = ""
    label = ""

    def __init__(self, args):
        self.args, self.cname = args, f"b-{self.name}"

    def start(self): raise NotImplementedError
    def ready(self, ip): raise NotImplementedError
    def topic(self, name, size): pass
    def produce(self, topic, size, count): raise NotImplementedError

    def kafka_perf(self, topic, size, count, extra_topic_setup=None):
        cmd = ["docker", "run", "--rm", "--network", f"container:{self.cname}", KAFKA_IMG, "/opt/kafka/bin/kafka-producer-perf-test.sh",
               "--topic", topic, "--num-records", str(count), "--record-size", str(size), "--throughput", "-1",
               "--command-property", "bootstrap.servers=localhost:9092"] + PRODUCER_PROPS
        return parse_kafka_perf(sh(cmd, timeout=1200))


class Kafka(System):
    name, label = "kafka", "Apache Kafka (KRaft)"

    def start(self):
        env = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller", "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
               "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://localhost:9092", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
               "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
               "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
               "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
               "KAFKA_MESSAGE_MAX_BYTES": str(MAXMSG), "KAFKA_REPLICA_FETCH_MAX_BYTES": str(MAXMSG),
               "KAFKA_SOCKET_REQUEST_MAX_BYTES": str(2 * MAXMSG), "KAFKA_HEAP_OPTS": "-Xms1g -Xmx1g"}
        cmd = ["docker", "run", "-d", "--name", self.cname] + limits()
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        sh(cmd + [KAFKA_IMG], check=True)

    def ready(self, ip): return kafka_api_versions_ok(ip)

    def topic(self, name, size):
        sh(["docker", "run", "--rm", "--network", f"container:{self.cname}", KAFKA_IMG, "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092", "--create", "--topic", name, "--partitions", "1", "--replication-factor", "1",
            "--config", f"max.message.bytes={MAXMSG}"], check=True)

    def produce(self, topic, size, count): return self.kafka_perf(topic, size, count)


class Redpanda(System):
    name, label = "redpanda", "Redpanda"

    def start(self):
        sh(["docker", "run", "-d", "--name", self.cname] + limits() + [RP_IMG, "redpanda", "start", "--overprovisioned", "--smp", "2",
            "--memory", "1G", "--reserve-memory", "0M", "--node-id", "0", "--check=false",
            "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://localhost:9092",
            "--set", f"redpanda.kafka_batch_max_bytes={MAXMSG}", "--set", f"redpanda.kafka_request_max_bytes={2 * MAXMSG}"], check=True)

    def ready(self, ip): return kafka_api_versions_ok(ip)

    def topic(self, name, size):
        sh(["docker", "exec", self.cname, "rpk", "topic", "create", name, "-p", "1", "-r", "1", "-c", f"max.message.bytes={MAXMSG}"], check=True)

    def produce(self, topic, size, count): return self.kafka_perf(topic, size, count)


class Pulsar(System):
    name, label = "pulsar", "Apache Pulsar (standalone)"

    def start(self):
        env = {"PULSAR_MEM": "-Xms384m -Xmx512m -XX:MaxDirectMemorySize=1100m",
               "PULSAR_PREFIX_dbStorage_writeCacheMaxSizeMb": "64", "PULSAR_PREFIX_dbStorage_readAheadCacheMaxSizeMb": "32",
               "PULSAR_PREFIX_dbStorage_rocksDB_blockCacheSize": "33554432"}
        cmd = ["docker", "run", "-d", "--name", self.cname] + limits()
        for k, v in env.items():
            cmd += ["-e", f"{k}={v}"]
        sh(cmd + [PULSAR_IMG, "bash", "-c", "bin/apply-config-from-env.py conf/standalone.conf && bin/apply-config-from-env.py conf/bookkeeper.conf; "
                  "bin/pulsar standalone --no-functions-worker --no-stream-storage"], check=True)

    def ready(self, ip): return http_ok(f"http://{ip}:8080/admin/v2/brokers/health")

    def produce(self, topic, size, count):
        # Payloads above the default 5 MB max message size use Pulsar's chunking, the supported mechanism.
        chunk = "chunk" if size > (4 << 20) else "nochunk"
        pending = "2" if size >= (10 << 20) else "1000"
        cmd = ["docker", "run", "--rm", "--network", f"container:{self.cname}", "-v", f"{HERE}:/b:ro", "--entrypoint", "bash", PULSAR_IMG, "-c",
               f"cd /tmp && cp /b/PulsarGen.java . && javac -proc:none -cp '/pulsar/lib/*' PulsarGen.java && "
               f"java -cp '.:/pulsar/lib/*' PulsarGen persistent://public/default/{topic} {size} {count} {chunk} {pending}"]
        out = sh(cmd, timeout=1200)
        m = re.search(r"RESULT (\{.*\})", out)
        return json.loads(m.group(1)) if m else {"error": out.strip().splitlines()[-1][:200] if out.strip() else "no output"}


def aero_broker_active(ip):
    """AeroStream is usable only after its broker has registered with the Raft controller."""
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://{ip}:9001/api/brokers", timeout=1) as r:
            return any(b.get("active") for b in json.load(r))
    except Exception:
        return False


class AeroBase(System):
    def ready(self, ip): return aero_broker_active(ip)

    def start(self):
        sh(["docker", "run", "-d", "--name", self.cname] + limits() + [self.args.aero_image], check=True)

    def create_topic_rest(self, name):
        ip = container_ip(self.cname)
        import urllib.request
        req = urllib.request.Request(f"http://{ip}:9001/api/topics", method="POST", headers={"Content-Type": "application/json"},
                                     data=json.dumps({"name": name, "partitions": 1, "replication_factor": 1}).encode())
        urllib.request.urlopen(req, timeout=10).read()
        time.sleep(1.0)


class AeroKafka(AeroBase):
    name, label = "aerostream-kafka", "AeroStream (Kafka port)"

    def topic(self, name, size): self.create_topic_rest(name)
    def produce(self, topic, size, count): return self.kafka_perf(topic, size, count)


class AeroNative(AeroBase):
    name, label = "aerostream-native", "AeroStream (native port)"

    def topic(self, name, size): self.create_topic_rest(name)

    def produce(self, topic, size, count):
        cmd = ["docker", "run", "--rm", "--network", f"container:{self.cname}", "-v", f"{os.path.abspath(self.args.aero_client)}:/aeroclient:ro",
               "--entrypoint", "/aeroclient", self.args.aero_image, "-controller", "127.0.0.1:8001",
               "bench", "--producers", "1", "--messages", str(count), "--size", str(size), "--topic", topic]
        return parse_aero_native(sh(cmd, timeout=1200), size, count)


SYSTEMS = {c.name: c for c in (Kafka, Redpanda, Pulsar, AeroKafka, AeroNative)}


def cold_boot(cls, args):
    """Seconds from `docker run -d` returning until the service answers its readiness probe."""
    s = cls(args)
    sh(["docker", "rm", "-f", s.cname])
    t0 = time.perf_counter()
    s.start()
    ip = container_ip(s.cname)
    while time.perf_counter() - t0 < 180:
        if s.ready(ip):
            return s, time.perf_counter() - t0
        time.sleep(0.02)
    raise RuntimeError(f"{s.name} never became ready")


def run_system(cls, args):
    s, boot = cold_boot(cls, args)
    print(f"[{s.name}] ready in {boot:.2f}s", flush=True)
    time.sleep(10)  # let start-up work settle before measuring the idle footprint
    idle = Sampler(s.cname); idle.start(); time.sleep(3); idle.stop.set(); idle.join()
    res = {"label": s.label, "cold_boot_s": boot, "idle_mem_mib": max(idle.mem) if idle.mem else None,
           "idle_pids": max(idle.pids) if idle.pids else None, "workloads": {}}
    samp = Sampler(s.cname); samp.start()
    for label, size, count in WORKLOADS:
        runs = []
        for i in range(args.runs):
            topic = f"bench-{label.lower()}-{i}"
            try:
                s.topic(topic, size)
                r = s.produce(topic, size, count)
            except Exception as e:  # noqa: BLE001
                r = {"error": str(e)[:200]}
            print(f"[{s.name}] {label} run {i + 1}: " + (f"{r['mb_per_sec']:.1f} MB/s {r['rec_per_sec']:.0f} rec/s p50={r['p50_ms']}ms" if "error" not in r else "ERROR " + r["error"]), flush=True)
            runs.append(r)
        good = sorted([r for r in runs if "error" not in r], key=lambda r: r["mb_per_sec"])
        res["workloads"][label] = {"runs": runs, "median": good[len(good) // 2] if good else None, "failed": len(runs) - len(good)}
    samp.stop.set(); samp.join()
    res["peak_mem_mib"] = max(samp.mem) if samp.mem else None
    res["peak_pids"] = max(samp.pids) if samp.pids else None
    res["peak_cpu_pct"] = max(samp.cpu) if samp.cpu else None
    sh(["docker", "rm", "-f", s.cname])
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aero-image", required=True)
    ap.add_argument("--aero-client", required=True, help="static Go `client` binary")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--systems", nargs="*", default=list(SYSTEMS))
    ap.add_argument("--cold-boot-runs", type=int, default=3)
    ap.add_argument("--workloads", nargs="*", help="subset of workload labels (default: all)")
    ap.add_argument("--out", default="results.json")
    args = ap.parse_args()
    if args.workloads:
        WORKLOADS[:] = [w for w in WORKLOADS if w[0] in args.workloads]
    out = json.load(open(args.out)) if os.path.exists(args.out) else {}
    for name in args.systems:
        cls = SYSTEMS[name]
        out[name] = run_system(cls, args)
        boots = [out[name]["cold_boot_s"]]
        for _ in range(args.cold_boot_runs - 1):
            s, b = cold_boot(cls, args)
            boots.append(b)
            sh(["docker", "rm", "-f", s.cname])
        out[name]["cold_boot_runs_s"] = boots
        out[name]["cold_boot_s"] = statistics.median(boots)
        json.dump(out, open(args.out, "w"), indent=2)
    print("done ->", args.out)


if __name__ == "__main__":
    sys.exit(main())
