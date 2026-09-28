//! Integration test: verifies that the shard engine can be spawned,
//! partitions are distributed across shards, and basic produce/consume
//! works through the ShardHandle API.

use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn shard_engine_produce_and_read() {
    let dir = tempdir().unwrap();
    let config = rust_broker::shard::ShardConfig {
        base_dir: dir.path().to_path_buf(),
        broker_id: 1,
        max_segment_size: 1 << 20,
        max_retention_size: None,
        max_retention_age: None,
        compaction_enabled: false,
        dirty_ratio_threshold: 0.5,
        tombstone_retention: std::time::Duration::from_secs(86400),
        writeback_bytes: 0,
        drop_cache_after_writeback: false,
        offload_tx: None,
        tiered_provider: None,
    };
    
    let handle = rust_broker::shard::spawn_shards(2, config);
    
    // Use a tokio runtime for the oneshot channels
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Append to partition 0 of topic "test"
        let offset = handle.append("test", 0, vec![1, 2, 3, 4]).await.unwrap();
        assert_eq!(offset, 0);
        
        let offset2 = handle.append("test", 0, vec![5, 6, 7, 8]).await.unwrap();
        assert_eq!(offset2, 1);
        
        // Read back
        let data = handle.read_from_offset("test", 0, 0, 4096).await.unwrap();
        assert!(data.is_some());
        
        // Check offsets
        let next = handle.get_next_offset("test", 0).await.unwrap();
        assert_eq!(next, 2);
        
        // Different partition goes to potentially different shard
        let offset3 = handle.append("test", 1, vec![9, 10]).await.unwrap();
        assert_eq!(offset3, 0);
        
        // Get all offsets
        let all = handle.get_all_offsets().await;
        assert_eq!(all.len(), 2); // two partitions
    });
    
    handle.shutdown();
}
