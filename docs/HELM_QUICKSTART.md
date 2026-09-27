# AeroStream Helm Quickstart Guide

This guide explains how to deploy and manage production-grade **AeroStream** clusters on Kubernetes using **Helm v3** and the official Helm chart located in [`deploy/helm/aerostream/`](file:///home/uttam/projects/AeroMQ/deploy/helm/aerostream).

---

## 1. Prerequisites

* **Kubernetes cluster** (v1.24+).
* **Helm v3** installed (`helm version`).
* **kubectl** configured with cluster access.

---

## 2. Quickstart Installation

### Option A: Development / Local Testing (kind / minikube)
Deploys a lightweight single-controller, single-broker instance with ephemeral storage:

```bash
helm install aerostream deploy/helm/aerostream \
  -n aerostream --create-namespace \
  -f deploy/helm/aerostream/values-dev.yaml
```

### Option B: Production Cluster (High Availability)
Deploys 3 Go Controllers (Raft Quorum) + 3 Rust Brokers with Persistent Volume Claims:

```bash
helm install aerostream deploy/helm/aerostream \
  -n aerostream --create-namespace \
  --set image.tag=latest \
  --set controller.replicas=3 \
  --set broker.replicas=3
```

---

## 3. Post-Installation Verification

Run the built-in Helm test to verify that all broker nodes registered and the cluster reached quorum:

```bash
helm test aerostream -n aerostream
```

Check all deployed workloads and services:
```bash
kubectl get all,pvc -n aerostream
```

---

## 4. Connecting Clients & Accessing the Console

### Access the Web Management Console
Port-forward the controller service:
```bash
kubectl port-forward -n aerostream svc/aerostream-aerostream-controller 9001:9001
```
Open **[http://localhost:9001/aerostream/console](http://localhost:9001/aerostream/console)** in your browser to view:
* Real-time cluster throughput and active brokers.
* Interactive Topic Manager with custom retention and partition disk quotas.
* Schema Registry and In-Broker Stream Transforms playground.
* Consumer Group lag meters and Cooperative Sticky Rebalance management.
* Enterprise ACL Policies and RBAC users.

### Connect Kafka Ingest Applications
Inside your Kubernetes cluster, configure your producers and consumers:

* **Bootstrap Servers**: `aerostream-aerostream-broker.aerostream.svc.cluster.local:9092`
* **Schema Registry URL**: `http://aerostream-aerostream-controller.aerostream.svc.cluster.local:9001`

```python
from confluent_kafka import Producer

p = Producer({
    'bootstrap.servers': 'aerostream-aerostream-broker.aerostream.svc.cluster.local:9092',
    'acks': 'all'
})
p.produce('orders', key='user_42', value='{"item": "book", "qty": 1}')
p.flush()
```

---

## 5. Production Helm Customizations

Customize your deployment by overriding settings in a custom `my-values.yaml`:

### Multi-Cloud Tiered Storage (AWS S3 / MinIO)
Automatically offload cold segments (>128 MB) to object storage:

```yaml
# my-values.yaml
broker:
  extraConfig: |
    [tiered_storage]
    enabled = true
    provider = "s3"
    bucket = "company-aerostream-archives"
    region = "us-east-1"
    endpoint = "https://s3.amazonaws.com"
  extraEnv:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: aws-creds
          key: access-key
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: aws-creds
          key: secret-key
```

### Multi-Zone & Rack Awareness (KIP-392)
Distribute partition replicas across Kubernetes availability zones and enable follower fetching:

```yaml
broker:
  rack:
    enabled: true  # Reads topology.kubernetes.io/zone from the host node
  podAntiAffinityPreset: "hard"
```

Apply your custom configuration:
```bash
helm upgrade --install aerostream deploy/helm/aerostream -n aerostream -f my-values.yaml
```

---

## 6. Key `values.yaml` Reference

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `image.repository` | `quay.io/gradientgeeks/aerostream` | Full-stack multi-architecture image |
| `image.tag` | `latest` | Image tag |
| `controller.replicas` | `3` | Raft quorum size (odd number: 1, 3, 5) |
| `controller.persistence.size` | `10Gi` | Raft WAL and snapshot storage volume |
| `broker.replicas` | `3` | Storage engine worker nodes |
| `broker.persistence.size` | `100Gi` | Hot NVMe commit log storage volume |
| `broker.rack.enabled` | `false` | Enable K8s availability zone rack labeling |
| `broker.storage.maxSegmentSize` | `134217728` | Segment rolling size (128 MB default) |
| `broker.txn.enabled` | `true` | Exactly-Once Semantics & 2PC transactions |
| `controller.service.type` | `ClusterIP` | Set to `LoadBalancer` for external console |

---

## 7. Upgrades & Uninstallation

### Upgrade Cluster Version
To upgrade AeroStream image or configuration with zero downtime:
```bash
helm upgrade aerostream deploy/helm/aerostream -n aerostream --set image.tag=v1.1.0
```

### Uninstall Release
```bash
helm uninstall aerostream -n aerostream
kubectl delete namespace aerostream
```
