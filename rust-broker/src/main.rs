use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tracing::info;

mod config;
mod log;
mod net;
mod grpc;
pub mod kafka;

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
        let log_manager = Arc::new(
            log::LogManager::new(storage_dir, cfg.id).with_limits(
                cfg.storage.max_segment_size,
                cfg.storage.max_retention_size,
                cfg.max_retention_age(),
            ),
        );

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
