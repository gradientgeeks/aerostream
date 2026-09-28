use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct ShardRouter {
    num_shards: usize,
}

impl ShardRouter {
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }

    pub fn shard_for(&self, topic: &str, partition: u32) -> usize {
        if self.num_shards == 0 {
            return 0;
        }
        let mut hasher = DefaultHasher::new();
        topic.hash(&mut hasher);
        partition.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }
}
