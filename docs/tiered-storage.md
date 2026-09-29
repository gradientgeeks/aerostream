# Multi-Cloud Tiered Storage Pipeline

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Tiered Storage</span>
<span class="md-tag">6 min read</span>
<span class="md-tag">AWS S3 / GCS / Azure</span>
</div>

AeroStream separates compute and local NVMe storage from long-term data retention through an **automated multi-cloud tiered storage pipeline**. 

With tiered storage enabled, clusters can retain months or years of historical event streams with virtually infinite capacity at a fraction of the cost of physical NVMe disks.

![Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

---

## Hot vs. Cold Storage Tiers

Event retention in AeroStream is organized into two distinct tiers:

```mermaid
flowchart TD
    Ingress["Producers / Kafka Ingress"] --> Hot["Hot Tier (Local NVMe SSD)<br/>Active Segments, Recent Data, Microsecond Latency"]
    Hot --> Roll{"Segment Roll Trigger<br/>128 MB Size or Max Age"}
    Roll --> Staging["Hard-Link Staging Directory<br/>fs::hard_link (Zero Disk Copy)"]
    Staging --> BG["Async Background Offloader Thread"]
    BG --> Cold["Cold Tier (Cloud Object Storage)<br/>AWS S3, MinIO, GCS, Azure Blob"]

    ConsumersRecent["Real-time Consumers"] -->|Zero-Copy sendfile DMA| Hot
    ConsumersHist["Historical Batch Consumers"] -->|Transparent Cache Fetch| Cold
```

### Hot Tier (Local NVMe SSD)

* Stores currently active segments and recently closed segments.
* Delivers sub-millisecond produce latency and zero-copy `sendfile(2)` streaming for real-time consumer tails.
* High watermark and index updates occur in memory and append-only local files.

### Cold Tier (Cloud Object Storage)

* When an active segment reaches its rolling threshold (default **128 MB**) or time boundary, it is closed and sealed.
* A background worker thread uses Linux hard links (`fs::hard_link`) to stage the `.log` and `.idx` files without copying bytes on disk.
* The segment is uploaded asynchronously in multipart chunks to the configured cloud bucket.
* Once safely confirmed in object storage, the local hot copy can be reclaimed according to retention policies.

---

## Configuring Storage Providers

AeroStream supports all major enterprise object storage backends via native asynchronous Rust drivers.

=== "AWS S3 / MinIO"

    Configure S3 or S3-compatible storage (e.g. MinIO, Ceph):

    ```ini
    # AeroStream Broker Configuration
    TIERED_STORAGE_ENABLED=true
    TIERED_PROVIDER=s3
    TIERED_S3_BUCKET=aerostream-cold-tier
    TIERED_S3_REGION=us-east-1
    TIERED_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com # Or http://minio:9000
    TIERED_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
    TIERED_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    TIERED_ROLLING_BYTES=134217728 # 128 MB
    ```

=== "Google Cloud Storage (GCS)"

    Configure GCS bucket archival:

    ```ini
    TIERED_STORAGE_ENABLED=true
    TIERED_PROVIDER=gcs
    TIERED_GCS_BUCKET=corp-events-archive
    TIERED_GCS_SERVICE_ACCOUNT_KEY=/etc/aerostream/gcs-credentials.json
    TIERED_ROLLING_BYTES=134217728
    ```

=== "Azure Blob Storage"

    Configure Azure Blob Storage container:

    ```ini
    TIERED_STORAGE_ENABLED=true
    TIERED_PROVIDER=azure
    TIERED_AZURE_ACCOUNT_NAME=aerostreamevents
    TIERED_AZURE_ACCOUNT_KEY=secret_storage_key==
    TIERED_AZURE_CONTAINER=cold-segments
    TIERED_ROLLING_BYTES=134217728
    ```

---

## Transparent Historical Fetch

When an analytical workload (e.g. Apache Spark, Trino, Snowflake, or an ML training pipeline) requests historical offsets that have already been purged from the local NVMe hot tier:

1. **Automatic Routing**: The broker identifies from its in-memory segment directory that the target offset resides in the cold tier.
2. **Chunked Prefetching**: The broker streams the target segment range directly from object storage into an in-memory chunk cache.
3. **Transparent Delivery**: Records are returned to the client using standard Kafka `FetchResponse` frames. 

The client application requires **no special flags or custom code**—historical retrieval behaves identically to reading from local disk, only bounded by cloud object storage read latency.
