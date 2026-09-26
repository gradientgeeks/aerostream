# AeroStream Helm chart

Deploys AeroStream on Kubernetes as two StatefulSets built from the single all-in-one image
(`quay.io/gradientgeeks/aerostream`):

| Component  | Kind        | Default | Role |
|------------|-------------|---------|------|
| controller | StatefulSet | 3       | Raft quorum, REST API, gRPC control plane, web console (`/aerostream/console`) |
| broker     | StatefulSet | 3       | Rust storage engine: native data plane (9091) + Kafka wire protocol (9092) |

## Install

```bash
# production-shaped: 3 controllers + 3 brokers with persistent volumes
helm install aero deploy/helm/aerostream --set image.tag=<immutable-tag>

# rack / zone aware (reads the node's topology.kubernetes.io/zone label)
helm install aero deploy/helm/aerostream --set image.tag=<tag> --set broker.rack.enabled=true

# single-node dev profile (kind / minikube), no persistence
helm install aero deploy/helm/aerostream -f deploy/helm/aerostream/values-dev.yaml

helm test aero          # waits until all brokers are registered and active
```

Pin `image.tag` to an immutable tag (e.g. a commit SHA). It defaults to `appVersion` (`latest`).

Console: `kubectl port-forward svc/aero-aerostream-controller 9001:9001`, then
<http://localhost:9001/aerostream/console>. Kafka clients inside the cluster use
`aero-aerostream-broker:9092` as the bootstrap address.

## Configuration

See `values.yaml` for every option (it is commented). The main ones:

| Value | Purpose |
|-------|---------|
| `controller.replicas`, `broker.replicas` | Use an odd controller count (1, 3, 5) |
| `controller.persistence.*`, `broker.persistence.*` | PVC size / storage class (`enabled: false` uses `emptyDir`) |
| `broker.rack.enabled` | Set `--rack` from the node's zone label; enables rack-aware placement and KIP-392 follower fetch |
| `broker.compressionType` | Default topic `compression.type` |
| `broker.storage.*`, `broker.txn.*`, `broker.share.*` | Retention, transactions (KIP-98), share groups (KIP-932) |
| `broker.extraConfig` | Raw TOML tables appended to `broker.toml`: client quotas, tiered storage, Iceberg topics |
| `broker.extraEnv` / `extraEnvFrom` | e.g. AWS credentials for S3 tiered storage |
| `auth.token` | Shared bearer token for the controller gRPC API (stored in a Secret) |
| `controller.service.type`, `controller.ingress.*` | Expose the console / REST API |
| `*.podAntiAffinityPreset`, `*.topologySpreadConstraints`, `*.podDisruptionBudget` | Scheduling and disruption |

Example - quotas and S3 tiered storage:

```yaml
broker:
  extraConfig: |
    [[quotas]]
    client_id = "noisy-app"
    producer_byte_rate = 1048576

    [tiered_storage]
    enabled = true
    provider = "s3"
    [tiered_storage.s3]
    bucket = "aerostream-cold"
    region = "us-east-1"
  extraEnv:
    - name: AWS_ACCESS_KEY_ID
      valueFrom: {secretKeyRef: {name: aws-creds, key: access-key}}
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom: {secretKeyRef: {name: aws-creds, key: secret-key}}
```

## How it works

* **Controllers** start in order (`OrderedReady`). Pod 0 bootstraps the Raft cluster **only on its first
  start** (a marker file on its volume, or always when `controller.replicas=1`); every other start - and
  every other pod - asks the current leader to (re)add it as a voter, retrying against all controllers.
* **Readiness** of a controller requires that it sees a Raft leader, so a rolling update never replaces
  the next controller before the previous one has rejoined the quorum.
* **Brokers** get `--id` from the pod ordinal and use their **pod IP** as `--host` (the broker binds to it
  and advertises it in Kafka Metadata). With `broker.rack.enabled` an init container reads the node's
  topology label through the Kubernetes API (the chart creates a `nodes/get` ClusterRole for this).
* Config changes roll the pods automatically (checksum annotation on the config Secret).

## Operations

**Upgrade:** `helm upgrade aero deploy/helm/aerostream --reuse-values --set image.tag=<new>`.
Verified: controllers roll one at a time and the cluster keeps its leader, brokers and topics.

**Recover from total controller loss.** The controller keeps its Raft log in memory (only snapshots go
to disk), so if *all* controllers are lost at once the cluster metadata (topics, broker registry) is lost
and, because the bootstrap marker exists, nothing re-forms a cluster. To rebuild it:

```bash
kubectl exec <release>-aerostream-controller-0 -- rm /data/.aerostream-bootstrapped
kubectl delete pod -l app.kubernetes.io/component=controller      # recreate all controllers
```

Topics must then be re-created. Broker data stays on the broker volumes; whether it is served again depends on the new partition-to-broker assignment, so treat this as a metadata-loss event.

**Scaling controllers down:** set `controller.leaveOnShutdown=true` for the scale-down so pods remove
themselves from the Raft configuration, then turn it off again.

## Known limitations

* **Raft state is in memory** (see above): never run more than one controller without persistence, and
  never take all controllers down at once. Prefer `maxUnavailable: 1` and drain nodes one at a time.
* **Quotas and topic compression set through the REST API live in each controller process**, not in Raft;
  with 3 controllers a request lands on an arbitrary replica. Configure them declaratively with
  `broker.extraConfig` instead.
* **Brokers advertise pod IPs** (there is no `advertised.listeners`), so Kafka clients must be inside the
  cluster / reach the pod network; external access is not supported yet. IPv4 only.
* TLS for the control/data plane is not wired into the chart (defaults to plaintext; `auth.token` is
  available). The chart was tested on a 4-node kind cluster with plaintext transport, with and without `auth.token`.
