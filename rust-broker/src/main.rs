use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tracing::info;

mod config;
mod log;
mod net;
mod grpc;
mod txn;
mod share;
pub mod kafka;
pub mod storage;

use config::BrokerConfig;

#[derive(Parser, Debug)]
#[command(author, version, about = "AeroMQ High-Performance Rust Broker")]
struct Args {
    /// Path to a TOML config file. Built-in defaults are used when omitted;
    /// any explicit CLI flag below overrides the corresponding config value.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Unique Broker ID
    #[arg(long)]
    id: Option<u32>,

    /// Bind IP Address
    #[arg(long)]
    host: Option<String>,

    /// TCP port for high-throughput client data operations
    #[arg(long)]
    data_port: Option<i32>,

    /// TCP port for Kafka wire protocol compatibility
    #[arg(long)]
    kafka_port: Option<i32>,

    /// gRPC Endpoint of the Go Control Plane
    #[arg(long)]
    controller: Option<String>,

    /// Path to store physical partition log files
    #[arg(long)]
    storage_dir: Option<PathBuf>,

    /// Tiered Storage Provider: s3, gcs, azure, local, disabled
    #[arg(long)]
    tiered_storage_provider: Option<String>,

    /// AWS S3 / MinIO bucket for tiered storage
    #[arg(long)]
    s3_bucket: Option<String>,

    /// Custom S3 endpoint URL (for MinIO, LocalStack, Ceph)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// AWS region for S3 tiered storage
    #[arg(long)]
    s3_region: Option<String>,

    /// Local directory path for tiered storage
    #[arg(long)]
    tiered_storage_dir: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize premium logging format
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    // Load config file (or defaults), then apply explicit CLI overrides.
    let mut cfg = BrokerConfig::load(args.config.as_deref())?;
    if let Some(id) = args.id {
        cfg.id = id;
    }
    if let Some(host) = args.host {
        cfg.host = host;
    }
    if let Some(data_port) = args.data_port {
        cfg.data_port = data_port;
    }
    if let Some(kafka_port) = args.kafka_port {
        cfg.kafka_port = kafka_port;
    }
    if let Some(controller) = args.controller {
        cfg.controller = controller;
    }
    if args.storage_dir.is_some() {
        cfg.storage_dir = args.storage_dir;
    }
    if let Some(ref p) = args.tiered_storage_provider {
        if let Ok(ptype) = p.parse::<storage::ProviderType>() {
            cfg.tiered_storage.provider = ptype;
            cfg.tiered_storage.enabled = ptype != storage::ProviderType::Disabled;
        }
    }
    if let Some(b) = args.s3_bucket {
        cfg.tiered_storage.s3.bucket = b;
    }
    if let Some(ep) = args.s3_endpoint {
        cfg.tiered_storage.s3.endpoint_url = Some(ep);
    }
    if let Some(r) = args.s3_region {
        cfg.tiered_storage.s3.region = Some(r);
    }
    if let Some(d) = args.tiered_storage_dir {
        cfg.tiered_storage.local.root_path = d;
    }

    let storage_dir = cfg.resolved_storage_dir();

    info!("[AeroMQ Broker] Initializing Storage Broker {}...", cfg.id);
    info!("[AeroMQ Broker] Commit Log Storage: {:?}", storage_dir);
    info!(
        "[AeroMQ Broker] data_port={}, kafka_port={}, max_segment_size={} bytes, data-plane TLS={}, auth={}",
        cfg.data_port,
        cfg.kafka_port,
        cfg.storage.max_segment_size,
        cfg.tls.enabled,
        cfg.auth.token.is_some()
    );

    // Build Tokio runtime with CPU affinity/thread pinning
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| {
            static THREAD_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let tid = THREAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            
            let num_cores = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let core_id = tid % num_cores;

            unsafe {
                let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_SET(core_id, &mut cpuset);
                let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
                if ret == 0 {
                    tracing::info!("[AeroMQ Broker] Pinned worker thread {} to CPU core {}", tid, core_id);
                } else {
                    tracing::warn!("[AeroMQ Broker] Failed to pin worker thread {} to CPU core {}: {}", tid, core_id, std::io::Error::last_os_error());
                }
            }
        })
        .build()?;

    let cfg = Arc::new(cfg);

    runtime.block_on(async move {
        // Initialize Tiered Storage Provider via Factory
        let tiered_provider = storage::StorageProviderFactory::create(&cfg.tiered_storage)
            .unwrap_or_else(|e| {
                tracing::warn!("[AeroMQ Broker] Failed to initialize tiered storage provider: {}, falling back to NullStorageProvider", e);
                Arc::new(storage::NullStorageProvider::new())
            });

        // Initialize channel and offloader pipeline
        let (offload_tx, offload_rx) = tokio::sync::mpsc::channel(1024);
        if cfg.tiered_storage.enabled {
            let offloader = storage::TieredStorageOffloader::new(tiered_provider.clone(), offload_rx);
            offloader.spawn();
            info!(
                "[AeroMQ Broker] Tiered Storage enabled with provider '{}'",
                tiered_provider.provider_name()
            );
        }

        let mut log_manager_builder = log::LogManager::new(storage_dir, cfg.id)
            .with_limits(
                cfg.storage.max_segment_size,
                cfg.storage.max_retention_size,
                cfg.max_retention_age(),
            )
            .with_compaction(
                cfg.storage.compaction_enabled,
                cfg.storage.dirty_ratio_threshold,
                std::time::Duration::from_secs(cfg.storage.tombstone_retention_secs),
            );

        if cfg.tiered_storage.enabled {
            log_manager_builder = log_manager_builder.with_tiered_storage(tiered_provider.clone(), offload_tx);
        }

        let log_manager = Arc::new(log_manager_builder);

        // Spawn background log compaction cleaner loop (runs every 30 seconds)
        if cfg.storage.compaction_enabled {
            log_manager.clone().spawn_cleaner_loop(std::time::Duration::from_secs(30));
            info!(
                "[AeroMQ Broker] Background Log Compaction cleaner loop spawned (interval=30s, threshold={})",
                cfg.storage.dirty_ratio_threshold
            );
        }

        // Spawn client registration & control plane heartbeat worker
        let grpc_log_manager = log_manager.clone();
        let grpc_cfg = cfg.clone();
        tokio::spawn(async move {
            grpc::run_control_plane_loop(grpc_cfg, grpc_log_manager).await;
        });

        // Spawn Kafka Wire Protocol TCP listener (bind 0.0.0.0 if host is a hostname/FQDN)
        let kafka_bind_addr: SocketAddr = format!("{}:{}", cfg.host, cfg.kafka_port)
            .parse()
            .unwrap_or_else(|_| format!("0.0.0.0:{}", cfg.kafka_port).parse().unwrap());
        let kafka_server = net::KafkaServer::new(kafka_bind_addr, log_manager.clone(), cfg.clone());
        tokio::spawn(async move {
            if let Err(e) = kafka_server.run().await {
                tracing::error!("[AeroMQ Broker] Kafka server error: {:?}", e);
            }
        });

        // Run TCP Data Plane server (bind 0.0.0.0 if host is a hostname/FQDN)
        let bind_addr: SocketAddr = format!("{}:{}", cfg.host, cfg.data_port)
            .parse()
            .unwrap_or_else(|_| format!("0.0.0.0:{}", cfg.data_port).parse().unwrap());
        let server = net::DataServer::new(bind_addr, log_manager, cfg.clone());

        server.run().await?;

        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
