use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, ClientTlsConfig, Certificate, Identity};
use tonic::{Request, metadata::MetadataValue};
use tracing::{info, error, warn, debug};

pub mod aeromq {
    tonic::include_proto!("aeromq");
}

use aeromq::control_service_client::ControlServiceClient;
use aeromq::discovery_service_client::DiscoveryServiceClient;
use aeromq::{RegisterBrokerRequest, HeartbeatRequest, MetadataRequest};
use crate::config::{self, BrokerConfig};
use crate::log::LogManager;

/// Wrap a message in a `tonic::Request`, attaching the `authorization` bearer
/// token metadata when one is configured.
fn authed<T>(msg: T, token: &Option<String>) -> Request<T> {
    let mut req = Request::new(msg);
    if let Some(t) = token {
        if let Ok(val) = MetadataValue::try_from(format!("Bearer {}", t)) {
            req.metadata_mut().insert("authorization", val);
        }
    }
    req
}

pub async fn run_control_plane_loop(
    cfg: Arc<BrokerConfig>,
    log_manager: Arc<LogManager>,
) {
    let broker_id = cfg.id;
    let my_host = cfg.host.clone();
    let data_port = cfg.data_port;
    let storage_dir = cfg.resolved_storage_dir();

    let controller_uri = if !cfg.controller.starts_with("http://") && !cfg.controller.starts_with("https://") {
        format!("http://{}", cfg.controller)
    } else {
        cfg.controller.clone()
    };

    info!("[AeroMQ Broker] Connecting to controller at {}...", controller_uri);

    let auth_token = cfg.auth.token.clone();

    // Retry with exponential backoff (200 ms .. 3 s) so a broker started before the controller has elected a
    // leader registers within a fraction of a second instead of waiting a fixed 3 s.
    let mut backoff = Duration::from_millis(200);
    loop {
        // Build endpoint, attaching TLS config for https:// controllers.
        let endpoint = match Channel::from_shared(controller_uri.clone()) {
            Ok(ep) => ep,
            Err(e) => {
                error!("[AeroMQ Broker] Invalid controller URI: {:?}. Retrying...", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                continue;
            }
        };

        let endpoint = if controller_uri.starts_with("https://") {
            let mut tls = ClientTlsConfig::new();
            if let Some(ca_path) = &cfg.tls.ca_file {
                match std::fs::read(ca_path) {
                    Ok(pem) => tls = tls.ca_certificate(Certificate::from_pem(pem)),
                    Err(e) => {
                        error!("[AeroMQ Broker] Failed to read controller CA {:?}: {}. Retrying...", ca_path, e);
                        sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                        continue;
                    }
                }
            }
            // Present our own client certificate (mTLS) when configured, so a controller that
            // enforces `RequireAndVerifyClientCert` can authenticate this broker.
            if let (Some(cert_path), Some(key_path)) = (&cfg.tls.client_cert_file, &cfg.tls.client_key_file) {
                match (std::fs::read(cert_path), std::fs::read(key_path)) {
                    (Ok(cert_pem), Ok(key_pem)) => {
                        tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        error!("[AeroMQ Broker] Failed to read client cert/key ({:?}, {:?}): {}. Retrying...", cert_path, key_path, e);
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(3));
                        continue;
                    }
                }
            }
            match endpoint.tls_config(tls) {
                Ok(ep) => ep,
                Err(e) => {
                    error!("[AeroMQ Broker] Invalid TLS config: {:?}. Retrying...", e);
                    sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                }
            }
        } else {
            endpoint
        };

        // Connect to control plane
        let channel = match endpoint.connect().await {
            Ok(ch) => ch,
            Err(e) => {
                error!("[AeroMQ Broker] Failed to connect to controller: {:?}. Retrying...", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                continue;
            }
        };

        let mut control_client = ControlServiceClient::new(channel.clone());
        let discovery_client = DiscoveryServiceClient::new(channel.clone());

        // 1. Register with Go controller
        let reg_req = RegisterBrokerRequest {
            broker_id,
            host: my_host.clone(),
            data_port,
            rack: cfg.rack.clone().unwrap_or_default(),
            kafka_port: cfg.kafka_port,
        };

        match control_client.register_broker(authed(reg_req, &auth_token)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.success {
                    info!("[AeroMQ Broker] Successfully registered broker {} with control plane", broker_id);
                    backoff = Duration::from_millis(200);
                } else {
                    error!("[AeroMQ Broker] Control plane rejected registration: {}", resp.message);
                    sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                    continue;
                }
            }
            Err(e) => {
                error!("[AeroMQ Broker] gRPC registration failed: {:?}. Retrying...", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(3));
                continue;
            }
        }

        // 2. Heartbeat Loop
        loop {
            let offsets_raw = log_manager.get_all_offsets().await;
            let mut replica_offsets = Vec::new();
            for (topic, partition, offset) in offsets_raw {
                replica_offsets.push(aeromq::ReplicaOffset {
                    topic,
                    partition,
                    offset,
                });
            }

            let (disk_usage_bytes, disk_free_bytes) = config::disk_stats(&storage_dir);

            let hb_req = HeartbeatRequest {
                broker_id,
                disk_usage_bytes,
                disk_free_bytes,
                replica_offsets,
            };

            match control_client.heartbeat(authed(hb_req, &auth_token)).await {
                Ok(resp) => {
                    let resp = resp.into_inner();
                    debug!("[AeroMQ Broker] Heartbeat acknowledged");
                    apply_dataplane_config(&resp);

                    // Reconcile Leaders (Ensure directories/logs exist)
                    for leader in &resp.assigned_leaders {
                        match log_manager.get_partition(&leader.topic, leader.partition).await {
                            Ok(part_log) => {
                                debug!("[AeroMQ Broker] Leader partition active: {}/{}", leader.topic, leader.partition);
                                let mut guard = part_log.lock().await;
                                guard.replica_ids = leader.replica_ids.clone();
                                if let Some(info) = crate::topology::TopologyCache::global().partition(&leader.topic, leader.partition as i32) {
                                    let other_isr = info.isr.iter().any(|&r| r != broker_id as i32);
                                    if !other_isr {
                                        guard.replica_ids = vec![broker_id];
                                    }
                                }
                                guard.recompute_high_watermark();
                            }
                            Err(e) => {
                                error!("[AeroMQ Broker] Failed to init leader partition {}/{}: {:?}", leader.topic, leader.partition, e);
                            }
                        }
                    }

                    // Reconcile Followers (Replicate data from assigned leaders)
                    for follower in &resp.assigned_followers {
                        let log_mgr = log_manager.clone();
                        let disc_client = discovery_client.clone();
                        let topic = follower.topic.clone();
                        let partition = follower.partition;
                        let leader_id = follower.leader_id;
                        let task_cfg = cfg.clone();

                        // Spawn async replication task for this follower partition
                        tokio::spawn(async move {
                            if let Err(e) = replicate_partition(
                                broker_id,
                                topic,
                                partition,
                                leader_id,
                                disc_client,
                                log_mgr,
                                task_cfg,
                            ).await {
                                debug!("[AeroMQ Broker] Replication error for partition: {:?}", e);
                            }
                        });
                    }
                }
                Err(e) => {
                    warn!("[AeroMQ Broker] Heartbeat failed: {:?}. Re-registering...", e);
                    break; // break heartbeat loop to trigger registration retry
                }
            }

            sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Applies controller-pushed quotas and per-topic compression.type (full replacement).
pub fn apply_dataplane_config(resp: &aeromq::HeartbeatResponse) {
    use crate::kafka::compression::{registry, CompressionType};
    use crate::kafka::quota::{manager, QuotaEntry};
    if !resp.dataplane_config_present {
        return;
    }
    let opt = |has: bool, v: f64| if has { Some(v) } else { None };
    let nz = |s: &str| if s.is_empty() { None } else { Some(s.to_string()) };
    let entries: Vec<QuotaEntry> = resp
        .client_quotas
        .iter()
        .map(|q| QuotaEntry {
            user: nz(&q.user),
            client_id: nz(&q.client_id),
            producer_byte_rate: opt(q.has_producer_byte_rate, q.producer_byte_rate),
            consumer_byte_rate: opt(q.has_consumer_byte_rate, q.consumer_byte_rate),
            request_percentage: opt(q.has_request_percentage, q.request_percentage),
        })
        .collect();
    manager().set_entries(entries);
    let topics = resp
        .topic_compression
        .iter()
        .filter_map(|(t, c)| CompressionType::parse(c).map(|ct| (t.clone(), ct)))
        .collect();
    registry().replace_topics(topics);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_apply_dataplane_config() {
        use super::*;
        let _g = crate::kafka::quota::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut resp = aeromq::HeartbeatResponse::default();
        resp.dataplane_config_present = true;
        resp.client_quotas.push(aeromq::ClientQuota {
            client_id: "grpc-client".into(),
            has_producer_byte_rate: true,
            producer_byte_rate: 123.0,
            ..Default::default()
        });
        resp.topic_compression.insert("grpc-topic".into(), "gzip".into());
        apply_dataplane_config(&resp);
        assert!(crate::kafka::quota::manager().entries().iter().any(|e| e.client_id.as_deref() == Some("grpc-client") && e.producer_byte_rate == Some(123.0)));
        assert_eq!(
            crate::kafka::compression::registry().for_topic("grpc-topic"),
            crate::kafka::compression::CompressionType::Codec(crate::kafka::compression::Codec::Gzip)
        );
        crate::kafka::quota::manager().set_entries(vec![]);
    }

    use super::*;

    #[test]
    fn test_authed_attaches_bearer_token_when_configured() {
        let token = Some("secret-token".to_string());
        let req = authed(MetadataRequest { topics: vec!["t1".into()] }, &token);

        let header = req
            .metadata()
            .get("authorization")
            .expect("expected authorization metadata to be set");
        assert_eq!(header.to_str().unwrap(), "Bearer secret-token");
    }

    #[test]
    fn test_authed_omits_metadata_when_no_token_configured() {
        let token: Option<String> = None;
        let req = authed(MetadataRequest { topics: vec![] }, &token);

        assert!(req.metadata().get("authorization").is_none());
    }

    #[test]
    fn test_authed_preserves_inner_message() {
        let token: Option<String> = None;
        let req = authed(MetadataRequest { topics: vec!["a".into(), "b".into()] }, &token);
        assert_eq!(req.get_ref().topics, vec!["a".to_string(), "b".to_string()]);
    }
}

async fn replicate_partition(
    broker_id: u32,
    topic: String,
    partition: u32,
    leader_id: u32,
    mut discovery_client: DiscoveryServiceClient<Channel>,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
) -> Result<(), Box<dyn std::error::Error>> {
    if leader_id == broker_id {
        return Ok(());
    }

    // 1. Get leader address from metadata
    let meta_resp = discovery_client.get_metadata(authed(MetadataRequest {
        topics: vec![topic.clone()],
    }, &cfg.auth.token)).await?.into_inner();

    let leader_broker = meta_resp.brokers.iter().find(|b| b.broker_id == leader_id);
    let leader = match leader_broker {
        Some(b) => b,
        None => return Err(format!("Leader broker {} not found in cluster metadata", leader_id).into()),
    };

    let leader_host = leader.host.clone();
    let leader_addr = format!("{}:{}", leader.host, leader.port);

    // 2. Open local partition to check current next_offset
    let part_log = log_manager.get_partition(&topic, partition).await?;
    let next_offset = {
        let guard = part_log.lock().await;
        guard.next_offset
    };

    // 3. Connect to leader's data plane (TLS + AUTH handshake handled by helper)
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = crate::net::client::connect_data_plane(
        &leader_addr,
        &leader_host,
        &cfg.tls,
        &cfg.auth.token,
    ).await?;

    // 4. Send Replica Fetch Request
    // Payload: [replica_id (4)] [topic_len (2)] [topic] [partition (4)] [start_offset (8)] [max_bytes (4)]
    let topic_bytes = topic.as_bytes();
    let body_len = 4 + 2 + topic_bytes.len() + 4 + 8 + 4;

    let mut req = Vec::new();
    req.extend_from_slice(&[0xAE, 0x01]); // magic
    req.push(3); // cmd (Replica Fetch)
    req.extend_from_slice(&(body_len as u32).to_be_bytes());

    req.extend_from_slice(&broker_id.to_be_bytes());
    req.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    req.extend_from_slice(topic_bytes);
    req.extend_from_slice(&partition.to_be_bytes());
    req.extend_from_slice(&next_offset.to_be_bytes());
    req.extend_from_slice(&65536u32.to_be_bytes()); // max_bytes

    stream.write_all(&req).await?;

    // 5. Read response header
    let mut resp_header = [0u8; 3];
    stream.read_exact(&mut resp_header).await?;
    if resp_header[0] != 0xAE || resp_header[1] != 0x01 {
        return Err("Invalid protocol magic in replica response".into());
    }

    let status = resp_header[2];
    if status == 2 {
        // Success with Data
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let data_len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; data_len];
        stream.read_exact(&mut payload).await?;

        // Append to local log
        let mut guard = part_log.lock().await;
        guard.append(&payload)?;
        info!("[AeroMQ Broker] Replicated partition {}/{} offset {} from leader {} ({} bytes)", 
            topic, partition, next_offset, leader_id, data_len);
    }

    Ok(())
}
