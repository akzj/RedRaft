//! Shard data structure with metadata
//!
//! Contains both data and metadata (like Raft apply index) for each shard
//!
//! ShardData contains all data structures (List, Set, ZSet) in a unified HashMap

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Shard metadata
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    /// Raft log apply index (last applied log entry index)
    /// This is set when creating a snapshot
    pub apply_index: Option<u64>,

    /// Last snapshot index (for incremental snapshots)
    pub last_snapshot_index: Option<u64>,

    /// Shard creation time
    pub created_at: u64,

    /// Last update time
    pub last_updated: u64,
}

impl ShardMetadata {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            apply_index: None,
            last_snapshot_index: None,
            created_at: now,
            last_updated: now,
        }
    }

    /// Update apply index (called during snapshot creation)
    pub fn set_apply_index(&mut self, index: u64) {
        self.apply_index = Some(index);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Update snapshot index
    pub fn set_snapshot_index(&mut self, index: u64) {
        self.last_snapshot_index = Some(index);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

impl Default for ShardMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Shard data container
///
/// Contains all data structures (List, Set, ZSet) for a shard
/// Key -> RedisValue mapping stores all types of data
pub struct ShardData {
    /// All data structures in this shard: key -> RedisValue
    /// RedisValue can be List, Set, ZSet, etc.
    pub data: HashMap<Vec<u8>, crate::memory::RedisValue>,
    /// Shard metadata
    pub metadata: ShardMetadata,
}

impl ShardData {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            metadata: ShardMetadata::new(),
        }
    }

    pub fn with_metadata(metadata: ShardMetadata) -> Self {
        Self {
            data: HashMap::new(),
            metadata,
        }
    }

    /// Get mutable reference to data
    pub fn data_mut(&mut self) -> &mut HashMap<Vec<u8>, crate::memory::RedisValue> {
        &mut self.data
    }

    /// Get reference to data
    pub fn data(&self) -> &HashMap<Vec<u8>, crate::memory::RedisValue> {
        &self.data
    }

    /// Get mutable reference to metadata
    pub fn metadata_mut(&mut self) -> &mut ShardMetadata {
        &mut self.metadata
    }

    /// Get reference to metadata
    pub fn metadata(&self) -> &ShardMetadata {
        &self.metadata
    }
}

impl Default for ShardData {
    fn default() -> Self {
        Self::new()
    }
}

/// Shard data with RwLock for thread-safe access
pub type LockedShardData = Arc<RwLock<ShardData>>;
