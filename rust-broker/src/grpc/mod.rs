use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, ClientTlsConfig, Certificate};
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

    loop {
        // Build endpoint, attaching TLS config for https:// controllers.
        let endpoint = match Channel::from_shared(controller_uri.clone()) {
            Ok(ep) => ep,
            Err(e) => {
                error!("[AeroMQ Broker] Invalid controller URI: {:?}. Retrying...", e);
                sleep(Duration::from_secs(3)).await;
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
                        sleep(Duration::from_secs(3)).await;
                        continue;
                    }
                }
            }
            match endpoint.tls_config(tls) {
                Ok(ep) => ep,
                Err(e) => {
                    error!("[AeroMQ Broker] Invalid TLS config: {:?}. Retrying...", e);
                    sleep(Duration::from_secs(3)).await;
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
                sleep(Duration::from_secs(3)).await;
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
        };

        match control_client.register_broker(authed(reg_req, &auth_token)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.success {
                    info!("[AeroMQ Broker] Successfully registered broker {} with control plane", broker_id);
                } else {
                    error!("[AeroMQ Broker] Control plane rejected registration: {}", resp.message);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            }
            Err(e) => {
                error!("[AeroMQ Broker] gRPC registration failed: {:?}. Retrying...", e);
                sleep(Duration::from_secs(3)).await;
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

                    // Reconcile Leaders (Ensure directories/logs exist)
                    for leader in &resp.assigned_leaders {
                        match log_manager.get_partition(&leader.topic, leader.partition).await {
                            Ok(part_log) => {
                                debug!("[AeroMQ Broker] Leader partition active: {}/{}", leader.topic, leader.partition);
                                let mut guard = part_log.lock().await;
                                guard.replica_ids = leader.replica_ids.clone();
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

#[cfg(test)]
mod tests {
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
