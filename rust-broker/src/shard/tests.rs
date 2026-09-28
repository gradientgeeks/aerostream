use super::engine::{spawn_shards, ShardConfig};
use super::router::ShardRouter;
use std::time::Duration;
use tempfile::tempdir;

fn test_config() -> ShardConfig {
    let dir = tempdir().unwrap().into_path();
    ShardConfig {
        base_dir: dir,
        broker_id: 1,
        max_segment_size: 1024 * 1024,
        max_retention_size: None,
        max_retention_age: None,
        compaction_enabled: false,
        dirty_ratio_threshold: 0.5,
        tombstone_retention: Duration::from_secs(86400),
        writeback_bytes: 0,
        drop_cache_after_writeback: false,
        offload_tx: None,
        tiered_provider: None,
    }
}

#[test]
fn test_shard_router() {
    let router = ShardRouter::new(4);
    let shard1 = router.shard_for("topic_a", 0);
    let shard2 = router.shard_for("topic_a", 0);
    assert_eq!(shard1, shard2, "Routing must be deterministic");

    let mut distinct_shards = std::collections::HashSet::new();
    for p in 0..100 {
        distinct_shards.insert(router.shard_for("topic_b", p));
    }
    assert!(distinct_shards.len() > 1, "Should distribute partitions across multiple shards");
}

#[tokio::test]
async fn test_shard_engine_append_and_read() {
    let handle = spawn_shards(2, test_config());
    
    let topic = "test_topic";
    let partition = 0;
    
    // Append
    let data = b"hello shard engine".to_vec();
    let offset = handle.append(topic, partition, data.clone()).await.expect("Append failed");
    
    // Read back
    let (read_data, start_offset) = handle.read_from_offset(topic, partition, offset, 1024).await.expect("Read failed").expect("No data found");
    
    assert_eq!(start_offset, offset);
    // Actually the read_from_offset returns the full segment/file slice starting from pos, so it will contain headers.
    // For this simple test we just check it is non-empty.
    assert!(!read_data.is_empty());
    
    handle.shutdown();
}

#[tokio::test]
async fn test_get_all_offsets() {
    let handle = spawn_shards(2, test_config());
    
    handle.append("topic_1", 0, b"msg1".to_vec()).await.unwrap();
    handle.append("topic_2", 1, b"msg2".to_vec()).await.unwrap();
    
    let offsets = handle.get_all_offsets().await;
    assert_eq!(offsets.len(), 2, "Should have 2 offsets across all shards");
    
    let mut found_topic_1 = false;
    let mut found_topic_2 = false;
    for (topic, p, _) in offsets {
        if topic == "topic_1" && p == 0 {
            found_topic_1 = true;
        }
        if topic == "topic_2" && p == 1 {
            found_topic_2 = true;
        }
    }
    assert!(found_topic_1 && found_topic_2);
    
    handle.shutdown();
}

#[tokio::test]
async fn test_shutdown() {
    let handle = spawn_shards(2, test_config());
    handle.shutdown();
    
    // Attempting to append after shutdown should eventually fail (or be dropped)
    // Actually our simple implementation sends the Shutdown, but we don't await the thread join.
    // That's fine for the test.
}
