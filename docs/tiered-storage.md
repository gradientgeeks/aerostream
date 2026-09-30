# Multi-Cloud Tiered Storage Pipeline

<div class="doc-badge-row" markdown>
<span class="md-tag md-tag--primary">Tiered Storage</span>
<span class="md-tag">6 min read</span>
<span class="md-tag">AWS S3 / MinIO / GCS / Azure</span>
</div>

AeroStream separates compute and fast local NVMe storage from long-term data retention through an **automated multi-cloud tiered storage pipeline**. 

With tiered storage enabled, clusters can retain months or years of historical event streams with virtually infinite capacity at a fraction of the cost of physical NVMe disks.

![Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

---

## 1. Hot vs. Cold Storage Tiers

Event retention in AeroStream is organized into two distinct physical tiers:

![Tiered Storage Pipeline](images/tiered_storage_pipeline.png)

### Hot Tier (Local NVMe SSD)

* Stores currently active segments and recently closed segments.
* Delivers sub-millisecond produce latency and zero-copy `sendfile(2)` streaming for real-time consumer tails.
* High watermark and index updates occur in memory and append-only local files.

### Cold Tier (Cloud Object Storage)

* When an active segment reaches its rolling threshold (default **128 MB**) or time boundary, it is closed and sealed into immutable `.log` and `.idx` files.
* A background worker thread uses Linux hard links (`fs::hard_link`) to stage the files without copying bytes on disk.
* The segment is uploaded asynchronously in multipart chunks to the configured cloud bucket.
* Once safely confirmed in object storage, the local hot copy can be reclaimed according to retention policies.

---

## 2. Configuring Storage Providers

AeroStream supports all major enterprise object storage backends via native asynchronous Rust drivers configured in `broker.toml`:

=== "AWS S3 / MinIO"

    Configure AWS S3 or S3-compatible storage (such as MinIO, Ceph, or Cloudflare R2):

    ```toml
    [tiered_storage]
    enabled = true
    provider = "s3"

    [tiered_storage.s3]
    bucket = "aerostream-cold-tier"
    prefix = "cluster-prod-01/"
    region = "us-east-1"
    endpoint_url = "https://s3.us-east-1.amazonaws.com" # Or http://minio:9000 for MinIO
    access_key_id = "AKIAIOSFODNN7EXAMPLE"
    secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    force_path_style = false # Set to true for MinIO
    ```

=== "Google Cloud Storage (GCS)"

    Configure GCS bucket archival:

    ```toml
    [tiered_storage]
    enabled = true
    provider = "gcs"

    [tiered_storage.gcs]
    bucket = "corp-events-archive"
    prefix = "analytics/"
    service_account_path = "/etc/aerostream/gcs-credentials.json"
    endpoint = "" # Optional custom GCS endpoint for testing/emulator
    mock_mode = false
    ```

=== "Azure Blob Storage"

    Configure Azure Blob Storage container:

    ```toml
    [tiered_storage]
    enabled = true
    provider = "azure"

    [tiered_storage.azure]
    container_name = "aerostream-segments"
    account_name = "aerostreamstorage"
    prefix = "cold-tier/"
    access_key = "secret_storage_key=="
    endpoint = "" # Optional custom endpoint for Azurite emulator
    mock_mode = false
    ```

=== "Local Filesystem / NFS Fallback"

    Configure secondary high-capacity block storage or NFS mounts:

    ```toml
    [tiered_storage]
    enabled = true
    provider = "local"

    [tiered_storage.local]
    root_path = "/mnt/cold-storage/aerostream"
    ```

---

## 3. Segment Rollover & Hard-Link Staging

To prevent blocking real-time message appends during multi-part network uploads, AeroStream decouples segment sealing from cloud transmission:

1. **Active Segment Sealing**: When a log segment exceeds `max_segment_size` (default `134217728` bytes / 128 MiB), the shard engine unmaps the segment and commits an immutable seal marker.
2. **Zero-Copy Staging**: The background offloader creates an immediate Linux hard link (`std::fs::hard_link`) in the staging directory:
   ```rust
   std::fs::hard_link(&segment_path, &staged_path)?;
   ```
   This operation executes in microsecond time because it only allocates an inode reference without copying physical disk blocks.
3. **Async Streaming Upload**: The offloader thread uploads the staged segment to object storage with exponential backoff retries.
4. **Local Eviction**: Once confirmed by the cloud provider with an MD5/ETag checksum, the segment is eligible for local NVMe disk cleanup while remaining fully accessible through cloud index pointers.

---

## 4. Transparent Historical Fetch

When an analytical workload (e.g. Apache Spark, Trino, Snowflake, or an ML training pipeline) requests historical offsets that have already been purged from the local NVMe hot tier:


1. **Automatic Routing**: The broker identifies from its in-memory segment index that the target offset resides in the cold tier.
2. **Chunked Prefetching**: The broker streams the target segment range directly from object storage into an in-memory chunk cache.
3. **Transparent Delivery**: Records are returned to the client using standard Kafka `FetchResponse` frames or Native protocol frames.

The client application requires **no special flags or custom code**—historical retrieval behaves identically to reading from local disk, only bounded by cloud object storage read latency.
