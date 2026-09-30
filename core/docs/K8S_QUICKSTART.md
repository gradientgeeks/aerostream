# AeroStream Kubernetes Quickstart Guide

This guide explains how to deploy, operate, and scale **AeroStream** on Kubernetes using standard `kubectl` manifests and the official all-in-one production container (`quay.io/gradientgeeks/aerostream:latest`).

---

## 1. Architecture Overview

AeroStream runs on Kubernetes with a clean separation of concerns:

![AeroStream Cluster Topology & Kubernetes Deployment](images/cluster_topology_scale_down.png)

* **3 Go Controllers** form a Raft consensus quorum for metadata, Schema Registry, RBAC, Connectors, and Stream Transforms.
* **N Rust Brokers** handle high-throughput zero-copy I/O (`sendfile(2)`, `mmap`), KIP-98 transactions, and Kafka wire protocol compatibility.
* Automated **PreStop lifecycle hooks** handle graceful Raft departure (`/leave`) and broker partition draining.

---

## 2. Prerequisites

* A running Kubernetes cluster (v1.24+ recommended, e.g. EKS, GKE, AKS, or local `minikube` / `kind`).
* `kubectl` installed and configured with cluster admin context.

---

## 3. Quick Deployment

All production Kubernetes manifests are located in the repository under [`deploy/k8s/`](file:///home/uttam/projects/AeroMQ/deploy/k8s).

### Step 1: Create the Dedicated Namespace
```bash
kubectl apply -f deploy/k8s/namespace.yaml
```

### Step 2: Deploy ConfigMaps and Core Services
```bash
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/services.yaml
```

### Step 3: Deploy the Controller Quorum (3 Replicas)
```bash
kubectl apply -f deploy/k8s/controller-statefulset.yaml
```

Wait until all 3 controller pods are `Running`:
```bash
kubectl rollout status statefulset/controller -n aerostream
```

### Step 4: Deploy the Storage Broker Fleet (2+ Replicas)
```bash
kubectl apply -f deploy/k8s/broker-statefulset.yaml
```

Wait until the broker fleet is active:
```bash
kubectl rollout status statefulset/broker -n aerostream
```

### Step 5: Verify Cluster Health
```bash
kubectl get pods,svc,pvc -n aerostream
```

*Expected output:*
```text
NAME               READY   STATUS    RESTARTS   AGE
pod/controller-0   1/1     Running   0          45s
pod/controller-1   1/1     Running   0          38s
pod/controller-2   1/1     Running   0          30s
pod/broker-0       1/1     Running   0          22s
pod/broker-1       1/1     Running   0          15s

NAME                             TYPE           CLUSTER-IP      PORT(S)
service/controller-headless      ClusterIP      None            7001/TCP,8001/TCP,9001/TCP
service/broker-headless          ClusterIP      None            9091/TCP,9092/TCP
service/aerostream-ui-external   LoadBalancer   10.96.120.44    9001:31450/TCP
```

---

## 4. Accessing the Cluster

### A. Access the Web Management Console

* **Via Port-Forward (Local/Dev)**:
  ```bash
  kubectl port-forward svc/controller-headless 9001:9001 -n aerostream
  ```
  Open your browser to:
  ```text
  http://localhost:9001/aerostream/console
  ```

* **Via LoadBalancer / Ingress (Production)**:
  Use the external IP assigned to `service/aerostream-ui-external`:
  ```bash
  EXTERNAL_IP=$(kubectl get svc aerostream-ui-external -n aerostream -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
  echo "Web Console: http://${EXTERNAL_IP}:9001/aerostream/console"
  ```

### B. Connecting Kafka Clients Inside Kubernetes

In-cluster workloads connect directly using standard Kafka client libraries (Python, Go, Java Spring Boot, Node.js):

* **Bootstrap Server**:
  ```text
  broker-headless.aerostream.svc.cluster.local:9092
  ```
* **Schema Registry**:
  ```text
  http://controller-headless.aerostream.svc.cluster.local:9001
  ```

---

## 5. Operations & Scaling

### Scale Out Brokers
Scale your data storage and partition throughput horizontally:
```bash
kubectl scale statefulset/broker --replicas=5 -n aerostream
```
New brokers register with the controller quorum over gRPC within milliseconds and begin hosting newly allocated partitions.

### Graceful Scale Down & Automated Draining

AeroStream Kubernetes manifests include built-in zero-downtime draining:

1. **Broker Partition Draining**:
   Before scaling down a broker, drain its partitions to remaining active nodes:
   ```bash
   kubectl exec -it controller-0 -n aerostream -- \
     curl -s -X POST http://localhost:9001/api/brokers/2/drain
   ```
2. **Controller PreStop Hook**:
   When scaling down `controller`, the StatefulSet `lifecycle.preStop` hook automatically invokes `POST /leave?id=nodeX` on the leader so Raft membership shrinks cleanly without quorum loss:
   ```bash
   kubectl scale statefulset/controller --replicas=3 -n aerostream
   ```

---

## 6. Running In-Cluster Benchmarks

You can run automated producer performance tests directly inside your cluster using the included Kubernetes benchmark job:

```bash
kubectl apply -f deploy/k8s/benchmark-job.yaml
kubectl logs -f job/aerostream-benchmark -n aerostream
```

---

## 7. Teardown

To delete the cluster and associated PersistentVolumeClaims:

```bash
kubectl delete -f deploy/k8s/broker-statefulset.yaml
kubectl delete -f deploy/k8s/controller-statefulset.yaml
kubectl delete -f deploy/k8s/services.yaml
kubectl delete -f deploy/k8s/configmap.yaml
kubectl delete pvc -l app.kubernetes.io/name=aerostream -n aerostream
kubectl delete -f deploy/k8s/namespace.yaml
```
