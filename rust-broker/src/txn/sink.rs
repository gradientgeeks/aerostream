//! Where committed transactional consumer offsets go when a transaction commits.
//! The authoritative group offsets live in the Go controller (Raft FSM), so the
//! production sink calls `DiscoveryService.CommitOffsets`.

use std::sync::Mutex;

use async_trait::async_trait;

#[async_trait]
pub trait OffsetSink: Send + Sync {
    /// Commit `(topic, partition, offset)` triples for `group`.
    async fn commit(&self, group: &str, offsets: &[(String, i32, i64)]) -> Result<(), String>;
}

/// Captures commits in memory (tests / standalone mode).
#[derive(Default)]
pub struct InMemorySink {
    pub committed: Mutex<Vec<(String, Vec<(String, i32, i64)>)>>,
}

#[async_trait]
impl OffsetSink for InMemorySink {
    async fn commit(&self, group: &str, offsets: &[(String, i32, i64)]) -> Result<(), String> {
        self.committed.lock().unwrap().push((group.to_string(), offsets.to_vec()));
        Ok(())
    }
}

/// Commits to the controller over gRPC (lazy connection).
pub struct GrpcOffsetSink {
    controller_uri: String,
    token: Option<String>,
}

impl GrpcOffsetSink {
    pub fn new(controller: &str, token: Option<String>) -> Self {
        let uri = if controller.starts_with("http://") || controller.starts_with("https://") {
            controller.to_string()
        } else {
            format!("http://{}", controller)
        };
        Self { controller_uri: uri, token }
    }
}

#[async_trait]
impl OffsetSink for GrpcOffsetSink {
    async fn commit(&self, group: &str, offsets: &[(String, i32, i64)]) -> Result<(), String> {
        use crate::grpc::aeromq::{discovery_service_client::DiscoveryServiceClient, CommitOffsetsRequest, TopicPartitionOffset};
        let channel = tonic::transport::Channel::from_shared(self.controller_uri.clone())
            .map_err(|e| e.to_string())?
            .connect_timeout(std::time::Duration::from_secs(3))
            .connect()
            .await
            .map_err(|e| e.to_string())?;
        let mut client = DiscoveryServiceClient::new(channel);
        let mut req = tonic::Request::new(CommitOffsetsRequest {
            group_id: group.to_string(),
            member_id: String::new(),
            generation_id: 0,
            offsets: offsets
                .iter()
                .map(|(t, p, o)| TopicPartitionOffset { topic: t.clone(), partition: *p as u32, offset: *o })
                .collect(),
        });
        if let Some(t) = &self.token {
            if let Ok(v) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", t)) {
                req.metadata_mut().insert("authorization", v);
            }
        }
        let resp = client.commit_offsets(req).await.map_err(|e| e.to_string())?.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err("controller rejected CommitOffsets".into())
        }
    }
}
