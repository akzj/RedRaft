//! Shard routing information
//!
//! Defines shard routing metadata and slot range management

use serde::{Deserialize, Serialize};

/// Total number of slots (consistent with Redis Cluster)
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard ID type
pub type ShardId = String;

/// Shard routing information
///
/// Contains slot range information for a shard, used for key-to-shard mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRouting {
    /// Shard identifier
    pub shard_id: ShardId,
    /// Start slot (inclusive)
    pub slot_start: u32,
    /// End slot (exclusive)
    pub slot_end: u32,
    /// Current apply index (optional, for metadata tracking)
    pub apply_index: Option<u64>,
}

impl ShardRouting {
    /// Create a new ShardRouting
    pub fn new(shard_id: ShardId, slot_start: u32, slot_end: u32) -> Self {
        Self {
            shard_id,
            slot_start,
            slot_end,
            apply_index: None,
        }
    }

    /// Create with apply index
    pub fn with_apply_index(
        shard_id: ShardId,
        slot_start: u32,
        slot_end: u32,
        apply_index: u64,
    ) -> Self {
        Self {
            shard_id,
            slot_start,
            slot_end,
            apply_index: Some(apply_index),
        }
    }

    /// Check if a slot belongs to this shard
    pub fn contains_slot(&self, slot: u32) -> bool {
        slot >= self.slot_start && slot < self.slot_end
    }

    /// Get slot count for this shard
    pub fn slot_count(&self) -> u32 {
        self.slot_end.saturating_sub(self.slot_start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_routing_contains_slot() {
        let routing = ShardRouting::new("shard_0".to_string(), 0, 4096);
        assert!(routing.contains_slot(0));
        assert!(routing.contains_slot(4095));
        assert!(!routing.contains_slot(4096));
        assert!(!routing.contains_slot(10000));
    }

    #[test]
    fn test_shard_routing_slot_count() {
        let routing = ShardRouting::new("shard_0".to_string(), 0, 4096);
        assert_eq!(routing.slot_count(), 4096);
    }
}

