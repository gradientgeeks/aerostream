use bytes::Bytes;
use tonic::transport::Channel;
use tonic::{metadata::MetadataValue, Request};
use tracing::{debug, warn};

pub mod protocol;
pub mod handlers;

pub use handlers::{
    handle_fetch, handle_produce, parse_records, encode_records_batch,
    encode_single_record_batch, FetchPartition, FetchPartitionResponse,
    FetchRequest, FetchResponse, FetchTopic, FetchTopicResponse,
    KafkaRecord, PartitionProduceData, PartitionProduceResponse,
    PartitionRecords, ProduceRequest, ProduceResponse, TopicProduceData,
    TopicProduceResponse, ZeroCopyFetchEnvelope,
};
pub use protocol::{
    decode_frame, encode_frame, encode_response_envelope, read_compact_string,
    read_nullable_compact_string, read_nullable_string, read_string, read_unsigned_varint,
    skip_tagged_fields, write_compact_string, write_nullable_compact_string,
    write_nullable_string, write_string, write_unsigned_varint, ApiKey, ApiVersionKey,
    ApiVersionsRequest, ApiVersionsResponse, BrokerMetadata, KafkaProtocolError, KafkaRequest,
    KafkaRequestBody, KafkaResponse, KafkaResponseBody, MetadataRequest, MetadataResponse,
    PartitionMetadata, RequestHeader, ResponseHeader, TopicMetadata,
};

use crate::config::BrokerConfig;
use crate::grpc::aeromq::{self, discovery_service_client::DiscoveryServiceClient};
use crate::log::LogManager;

/// Wrap a gRPC message with authorization metadata when a token is configured.
fn authed_req<T>(msg: T, token: &Option<String>) -> Request<T> {
    let mut req = Request::new(msg);
    if let Some(t) = token {
        if let Ok(val) = MetadataValue::try_from(format!("Bearer {}", t)) {
            req.metadata_mut().insert("authorization", val);
        }
    }
    req
}

/// Builds a Kafka `MetadataResponse` derived entirely from local broker state.
/// This ensures partition 0 is returned with this broker as the leader.
pub fn build_local_metadata(
    cfg: &BrokerConfig,
    requested_topics: Option<&[String]>,
) -> MetadataResponse {
    let broker_id = cfg.id as i32;
    let brokers = vec![BrokerMetadata {
        node_id: broker_id,
        host: cfg.host.clone(),
        port: cfg.data_port,
        rack: None,
    }];

    let default_topics = vec!["default".to_string()];
    let topics_to_include = match requested_topics {
        Some(topics) if !topics.is_empty() => topics,
        _ => &default_topics[..],
    };

    let topics = topics_to_include
        .iter()
        .map(|t| TopicMetadata {
            error_code: 0,
            topic: t.clone(),
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader_id: broker_id,
                leader_epoch: 0,
                replica_nodes: vec![broker_id],
                isr_nodes: vec![broker_id],
                offline_replicas: vec![],
            }],
        })
        .collect();

    MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("aeromq-cluster".to_string()),
        controller_id: broker_id,
        topics,
    }
}

/// Builds metadata from local state, taking into account any active partition logs.
pub async fn query_local_metadata(
    cfg: &BrokerConfig,
    log_manager: Option<&LogManager>,
    requested_topics: Option<&[String]>,
) -> MetadataResponse {
    let broker_id = cfg.id as i32;
    let brokers = vec![BrokerMetadata {
        node_id: broker_id,
        host: cfg.host.clone(),
        port: cfg.data_port,
        rack: None,
    }];

    // Discover topics from log_manager if available
    let mut known_topics = Vec::new();
    if let Some(mgr) = log_manager {
        let offsets = mgr.get_all_offsets().await;
        for (topic, _, _) in offsets {
            if !known_topics.contains(&topic) {
                known_topics.push(topic);
            }
        }
    }

    let topics_to_process = match requested_topics {
        Some(topics) if !topics.is_empty() => topics.to_vec(),
        _ => {
            if known_topics.is_empty() {
                vec!["default".to_string()]
            } else {
                known_topics
            }
        }
    };

    let topics = topics_to_process
        .into_iter()
        .map(|topic_name| TopicMetadata {
            error_code: 0,
            topic: topic_name,
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader_id: broker_id,
                leader_epoch: 0,
                replica_nodes: vec![broker_id],
                isr_nodes: vec![broker_id],
                offline_replicas: vec![],
            }],
        })
        .collect();

    MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("aeromq-cluster".to_string()),
        controller_id: broker_id,
        topics,
    }
}

/// Queries the Go control plane gRPC service for cluster metadata, mapping the result
/// to Kafka wire protocol `MetadataResponse`.
pub async fn query_controller_metadata(
    controller_uri: &str,
    auth_token: &Option<String>,
    requested_topics: Vec<String>,
) -> Result<MetadataResponse, Box<dyn std::error::Error + Send + Sync>> {
    let uri = if !controller_uri.starts_with("http://") && !controller_uri.starts_with("https://") {
        format!("http://{}", controller_uri)
    } else {
        controller_uri.to_string()
    };

    let channel = Channel::from_shared(uri)?.connect().await?;
    let mut client = DiscoveryServiceClient::new(channel);

    let req = aeromq::MetadataRequest {
        topics: requested_topics.clone(),
    };

    let resp = client
        .get_metadata(authed_req(req, auth_token))
        .await?
        .into_inner();

    let brokers: Vec<BrokerMetadata> = resp
        .brokers
        .into_iter()
        .map(|b| BrokerMetadata {
            node_id: b.broker_id as i32,
            host: b.host,
            port: b.port,
            rack: None,
        })
        .collect();

    let controller_id = if let Some(b) = brokers.first() {
        b.node_id
    } else {
        1
    };

    let mut topics = Vec::new();
    for t in resp.topics {
        let mut partitions: Vec<PartitionMetadata> = t
            .partitions
            .into_iter()
            .map(|p| PartitionMetadata {
                error_code: 0,
                partition_id: p.partition_id as i32,
                leader_id: p.leader_id as i32,
                leader_epoch: 0,
                replica_nodes: p.replica_ids.into_iter().map(|id| id as i32).collect(),
                isr_nodes: p.isr.into_iter().map(|id| id as i32).collect(),
                offline_replicas: vec![],
            })
            .collect();

        // If no partition metadata exists yet, guarantee partition 0 with controller/first broker as leader
        if partitions.is_empty() {
            partitions.push(PartitionMetadata {
                error_code: 0,
                partition_id: 0,
                leader_id: controller_id,
                leader_epoch: 0,
                replica_nodes: vec![controller_id],
                isr_nodes: vec![controller_id],
                offline_replicas: vec![],
            });
        }

        topics.push(TopicMetadata {
            error_code: 0,
            topic: t.topic,
            is_internal: false,
            partitions,
        });
    }

    Ok(MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("aeromq-cluster".to_string()),
        controller_id,
        topics,
    })
}

/// Resolves cluster metadata by attempting to query the Go controller, falling back
/// gracefully to local broker state if the controller is unreachable.
pub async fn resolve_metadata(
    cfg: &BrokerConfig,
    log_manager: Option<&LogManager>,
    requested_topics: Option<Vec<String>>,
) -> MetadataResponse {
    let topics_for_query = requested_topics.clone().unwrap_or_default();

    match query_controller_metadata(&cfg.controller, &cfg.auth.token, topics_for_query).await {
        Ok(resp) => {
            debug!("[AeroMQ Kafka] Retrieved metadata from Go controller");
            resp
        }
        Err(e) => {
            warn!(
                "[AeroMQ Kafka] Failed to query Go controller ({:?}), falling back to local state",
                e
            );
            query_local_metadata(cfg, log_manager, requested_topics.as_deref()).await
        }
    }
}

// ---------------------------------------------------------------------------
// High-Level Decode / Encode & Request Handlers
// ---------------------------------------------------------------------------

/// Decodes an entire Kafka request payload from raw frame bytes.
pub fn decode_request(buf: Bytes) -> Result<KafkaRequest, KafkaProtocolError> {
    KafkaRequest::decode(buf)
}

/// Encodes an entire Kafka response into framed bytes with correlation_id.
pub fn encode_response(resp: &KafkaResponse, version: i16) -> Bytes {
    resp.encode(version)
}

/// Handles an `ApiVersions` request (ApiKey 18), generating the standard supported versions response.
pub fn handle_api_versions(
    header: &RequestHeader,
    _req: &ApiVersionsRequest,
) -> KafkaResponse {
    let body = ApiVersionsResponse::default_supported();
    KafkaResponse::new(header.correlation_id, KafkaResponseBody::ApiVersions(body))
}

/// Handles a `Metadata` request (ApiKey 3), producing a KafkaResponse with the provided metadata.
pub fn handle_metadata(
    header: &RequestHeader,
    metadata: MetadataResponse,
) -> KafkaResponse {
    KafkaResponse::new(header.correlation_id, KafkaResponseBody::Metadata(metadata))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_local_metadata_partition_0_and_leader() {
        let mut cfg = BrokerConfig::default();
        cfg.id = 42;
        cfg.host = "10.0.0.5".to_string();
        cfg.data_port = 9092;

        let meta = build_local_metadata(&cfg, Some(&["orders".to_string(), "payments".to_string()]));

        assert_eq!(meta.brokers.len(), 1);
        assert_eq!(meta.brokers[0].node_id, 42);
        assert_eq!(meta.brokers[0].host, "10.0.0.5");
        assert_eq!(meta.brokers[0].port, 9092);

        assert_eq!(meta.topics.len(), 2);
        for t in &meta.topics {
            assert_eq!(t.partitions.len(), 1);
            let p0 = &t.partitions[0];
            assert_eq!(p0.partition_id, 0);
            assert_eq!(p0.leader_id, 42);
            assert_eq!(p0.replica_nodes, vec![42]);
            assert_eq!(p0.isr_nodes, vec![42]);
        }
    }

    #[test]
    fn test_handle_api_versions() {
        let header = RequestHeader::new(18, 2, 777, Some("client-app".to_string()));
        let req = ApiVersionsRequest::default();
        let resp = handle_api_versions(&header, &req);

        assert_eq!(resp.header.correlation_id, 777);
        if let KafkaResponseBody::ApiVersions(v_resp) = resp.body {
            assert_eq!(v_resp.error_code, 0);
            assert_eq!(v_resp.api_keys.len(), 4);
        } else {
            panic!("Expected ApiVersions response body");
        }
    }

    #[test]
    fn test_handle_metadata() {
        let header = RequestHeader::new(3, 1, 888, Some("client-app".to_string()));
        let cfg = BrokerConfig::default();
        let meta = build_local_metadata(&cfg, Some(&["test-topic".to_string()]));
        let resp = handle_metadata(&header, meta);

        assert_eq!(resp.header.correlation_id, 888);
        if let KafkaResponseBody::Metadata(m_resp) = resp.body {
            assert_eq!(m_resp.topics.len(), 1);
            assert_eq!(m_resp.topics[0].topic, "test-topic");
            assert_eq!(m_resp.topics[0].partitions[0].partition_id, 0);
        } else {
            panic!("Expected Metadata response body");
        }
    }

    #[test]
    fn test_full_pipeline_api_versions_wire_roundtrip() {
        use bytes::BytesMut;

        // 1. Build an incoming ApiVersions request frame: [i32 len] [header] [body]
        let header = RequestHeader::new(18, 0, 101, Some("test-client".to_string()));
        let mut req_body = BytesMut::new();
        header.encode(&mut req_body);
        let framed_req = encode_frame(&req_body);

        // 2. Decode frame
        let mut stream_buf = BytesMut::from(&framed_req[..]);
        let frame = decode_frame(&mut stream_buf).unwrap().expect("frame available");
        let decoded_req = decode_request(frame).unwrap();

        assert_eq!(decoded_req.header.api_key, 18);
        assert_eq!(decoded_req.header.correlation_id, 101);

        // 3. Process request
        let req_inner = match decoded_req.body {
            KafkaRequestBody::ApiVersions(r) => r,
            _ => panic!("Expected ApiVersions request"),
        };
        let response = handle_api_versions(&decoded_req.header, &req_inner);

        // 4. Encode response frame
        let resp_framed = encode_response(&response, 0);

        // 5. Decode response frame
        let mut resp_stream = BytesMut::from(&resp_framed[..]);
        let resp_frame = decode_frame(&mut resp_stream).unwrap().expect("response frame available");

        let mut read_resp = resp_frame;
        let resp_header = ResponseHeader::decode(&mut read_resp).unwrap();
        assert_eq!(resp_header.correlation_id, 101);

        let decoded_api_versions = ApiVersionsResponse::decode(&mut read_resp, 0).unwrap();
        assert_eq!(decoded_api_versions.error_code, 0);
        assert_eq!(decoded_api_versions.api_keys.len(), 4);
    }

    #[test]
    fn test_full_pipeline_metadata_wire_roundtrip() {
        use bytes::BytesMut;

        // 1. Build incoming Metadata request frame
        let header = RequestHeader::new(3, 1, 202, Some("meta-client".to_string()));
        let req = MetadataRequest::new(Some(vec!["topic-alpha".to_string()]), false);
        let mut req_payload = BytesMut::new();
        header.encode(&mut req_payload);
        req.encode(1, &mut req_payload);
        let framed_req = encode_frame(&req_payload);

        // 2. Decode incoming frame
        let mut stream_buf = BytesMut::from(&framed_req[..]);
        let frame = decode_frame(&mut stream_buf).unwrap().expect("frame available");
        let decoded_req = decode_request(frame).unwrap();

        assert_eq!(decoded_req.header.api_key, 3);
        assert_eq!(decoded_req.header.correlation_id, 202);

        // 3. Resolve metadata via local state
        let cfg = BrokerConfig {
            id: 7,
            host: "192.168.1.50".to_string(),
            data_port: 9092,
            ..Default::default()
        };
        let meta_resp = build_local_metadata(&cfg, Some(&["topic-alpha".to_string()]));
        let response = handle_metadata(&decoded_req.header, meta_resp);

        // 4. Encode response
        let resp_framed = encode_response(&response, 1);

        // 5. Decode response frame
        let mut resp_stream = BytesMut::from(&resp_framed[..]);
        let resp_frame = decode_frame(&mut resp_stream).unwrap().expect("response frame available");

        let mut read_resp = resp_frame;
        let resp_header = ResponseHeader::decode(&mut read_resp).unwrap();
        assert_eq!(resp_header.correlation_id, 202);

        let decoded_meta = MetadataResponse::decode(&mut read_resp, 1).unwrap();
        assert_eq!(decoded_meta.brokers.len(), 1);
        assert_eq!(decoded_meta.brokers[0].node_id, 7);
        assert_eq!(decoded_meta.brokers[0].host, "192.168.1.50");
        assert_eq!(decoded_meta.brokers[0].port, 9092);

        assert_eq!(decoded_meta.topics.len(), 1);
        assert_eq!(decoded_meta.topics[0].topic, "topic-alpha");
        assert_eq!(decoded_meta.topics[0].partitions.len(), 1);
        assert_eq!(decoded_meta.topics[0].partitions[0].partition_id, 0);
        assert_eq!(decoded_meta.topics[0].partitions[0].leader_id, 7);
    }
}
