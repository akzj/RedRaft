//! Routing table definitions
//!
//! Used to map key hash to shards, and shards to nodes

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{KeyRange, NodeId, ShardId, TOTAL_SLOTS};

/// Splitting shard information
///
/// Used by Node to determine if requests need special handling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplittingShardInfo {
    /// Source shard ID
    pub source_shard: ShardId,
    /// Target shard ID
    pub target_shard: ShardId,
    /// Split point slot - target shard will be responsible for [split_slot, source.end)
    pub split_slot: u32,
    /// Source shard's original range
    pub source_range: KeyRange,
    /// Target shard node addresses (leader first)
    pub target_nodes: Vec<NodeId>,
    /// Current split phase
    pub phase: SplitPhase,
}

impl SplittingShardInfo {
    /// Check if slot belongs to range that will be moved
    pub fn is_slot_moving(&self, slot: u32) -> bool {
        slot >= self.split_slot && slot < self.source_range.end
    }

    /// Get range that target shard will be responsible for
    pub fn target_range(&self) -> KeyRange {
        KeyRange::new(self.split_slot, self.source_range.end)
    }

    /// Get source shard's range after split
    pub fn source_after_split_range(&self) -> KeyRange {
        KeyRange::new(self.source_range.start, self.split_slot)
    }
}

/// Split phase (for routing table propagation)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPhase {
    /// Preparation/snapshot transfer/catch-up - source shard handles all requests normally
    Preparing,
    /// Buffering phase - source shard buffers write requests for target range
    Buffering,
    /// Switch completed - return MOVED
    Switched,
}

/// Routing table
///
/// Contains complete mapping information from keys to nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// Routing table version number (incremented on each change)
    pub version: u64,
    /// Slot to shard mapping (slot_id -> shard_id)
    /// Fixed length of TOTAL_SLOTS
    pub slots: Vec<Option<ShardId>>,
    /// Shard to nodes mapping (shard_id -> [node_ids])
    /// First node is leader
    pub shard_nodes: HashMap<ShardId, Vec<NodeId>>,
    /// Node address mapping (node_id -> grpc_addr)
    pub node_addrs: HashMap<NodeId, String>,
    /// Splitting shards (source_shard_id -> info)
    #[serde(default)]
    pub splitting_shards: HashMap<ShardId, SplittingShardInfo>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    /// Create empty routing table
    pub fn new() -> Self {
        Self {
            version: 0,
            slots: vec![None; TOTAL_SLOTS as usize],
            shard_nodes: HashMap::new(),
            node_addrs: HashMap::new(),
            splitting_shards: HashMap::new(),
        }
    }

    /// Increment version number
    pub fn bump_version(&mut self) {
        self.version += 1;
    }

    /// Calculate slot for key
    pub fn slot_for_key(key: &[u8]) -> u32 {
        // Use CRC16 algorithm (compatible with Redis Cluster)
        crc16(key) as u32 % TOTAL_SLOTS
    }

    /// Assign slots to shard
    pub fn assign_slots(&mut self, shard_id: &ShardId, start: u32, end: u32) {
        for slot in start..end {
            if (slot as usize) < self.slots.len() {
                self.slots[slot as usize] = Some(shard_id.clone());
            }
        }
    }

    /// Set shard's node list
    pub fn set_shard_nodes(&mut self, shard_id: ShardId, nodes: Vec<NodeId>) {
        self.shard_nodes.insert(shard_id, nodes);
    }

    /// Set node address
    pub fn set_node_addr(&mut self, node_id: NodeId, addr: String) {
        self.node_addrs.insert(node_id, addr);
    }

    /// Remove node
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.node_addrs.remove(node_id);
        // Remove node from all shards
        for nodes in self.shard_nodes.values_mut() {
            nodes.retain(|n| n != node_id);
        }
    }

    /// Get target nodes for key
    pub fn get_nodes_for_key(&self, key: &[u8]) -> Option<&Vec<NodeId>> {
        let slot = Self::slot_for_key(key);
        let shard_id = self.slots.get(slot as usize)?.as_ref()?;
        self.shard_nodes.get(shard_id)
    }

    /// Get leader node for key
    pub fn get_leader_for_key(&self, key: &[u8]) -> Option<&NodeId> {
        self.get_nodes_for_key(key)?.first()
    }

    /// Get leader node address
    pub fn get_leader_addr_for_key(&self, key: &[u8]) -> Option<&String> {
        let leader = self.get_leader_for_key(key)?;
        self.node_addrs.get(leader)
    }

    /// Get all shard IDs
    pub fn shard_ids(&self) -> Vec<&ShardId> {
        self.shard_nodes.keys().collect()
    }

    /// Check if routing table is complete (all slots assigned)
    pub fn is_complete(&self) -> bool {
        self.slots.iter().all(|s| s.is_some())
    }

    /// Count unassigned slots
    pub fn unassigned_slot_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_none()).count()
    }

    /// Get shard ID for specified slot
    pub fn get_shard_for_slot(&self, slot: u32) -> Option<&ShardId> {
        self.slots.get(slot as usize)?.as_ref()
    }

    // ==================== Split-related methods ====================

    /// Add splitting shard information
    pub fn add_splitting_shard(&mut self, info: SplittingShardInfo) {
        self.splitting_shards
            .insert(info.source_shard.clone(), info);
        self.bump_version();
    }

    /// Update split phase
    pub fn update_split_phase(&mut self, source_shard: &ShardId, phase: SplitPhase) -> bool {
        if let Some(info) = self.splitting_shards.get_mut(source_shard) {
            info.phase = phase;
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// Remove split information (called when split completes or is cancelled)
    pub fn remove_splitting_shard(&mut self, source_shard: &ShardId) -> Option<SplittingShardInfo> {
        let info = self.splitting_shards.remove(source_shard);
        if info.is_some() {
            self.bump_version();
        }
        info
    }

    /// Get split information
    pub fn get_splitting_shard(&self, source_shard: &ShardId) -> Option<&SplittingShardInfo> {
        self.splitting_shards.get(source_shard)
    }

    /// Check if slot is in split based on slot
    /// Returns Some((source_shard_id, info)) if this slot belongs to a splitting range
    pub fn get_split_info_for_slot(&self, slot: u32) -> Option<(&ShardId, &SplittingShardInfo)> {
        for (shard_id, info) in &self.splitting_shards {
            if info.is_slot_moving(slot) {
                return Some((shard_id, info));
            }
        }
        None
    }

    /// Check if there are active split tasks
    pub fn has_active_splits(&self) -> bool {
        !self.splitting_shards.is_empty()
    }

    /// Get all splitting shards
    pub fn splitting_shard_ids(&self) -> Vec<&ShardId> {
        self.splitting_shards.keys().collect()
    }
}

/// CRC16 implementation (XMODEM variant, compatible with Redis Cluster)
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_for_key() {
        // Test slot calculation
        let slot1 = RoutingTable::slot_for_key(b"hello");
        let slot2 = RoutingTable::slot_for_key(b"hello");
        assert_eq!(slot1, slot2);

        let slot3 = RoutingTable::slot_for_key(b"world");
        // Different keys may have different slots (but not guaranteed)
        assert!(slot1 < TOTAL_SLOTS);
        assert!(slot3 < TOTAL_SLOTS);
    }

    #[test]
    fn test_routing() {
        let mut rt = RoutingTable::new();

        // Assign slots
        rt.assign_slots(&"shard1".to_string(), 0, 8192);
        rt.assign_slots(&"shard2".to_string(), 8192, 16384);

        // Set shard nodes
        rt.set_shard_nodes(
            "shard1".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
        );
        rt.set_shard_nodes(
            "shard2".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );

        // Set node addresses
        rt.set_node_addr("node1".to_string(), "127.0.0.1:50051".to_string());
        rt.set_node_addr("node2".to_string(), "127.0.0.1:50052".to_string());
        rt.set_node_addr("node3".to_string(), "127.0.0.1:50053".to_string());

        assert!(rt.is_complete());
        assert_eq!(rt.unassigned_slot_count(), 0);
    }
}
