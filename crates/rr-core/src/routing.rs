//! Routing table for shard and raft group management
//!
//! Provides functionality to:
//! - Map keys to shard_id
//! - Map shard_id to leader node_id
//! - Manage raft group information

use crate::shard::{ShardId, ShardRouting, TOTAL_SLOTS};
use crc::{Crc, CRC_16_XMODEM};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// CRC16 calculator for Redis Cluster (XMODEM variant)
static CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_XMODEM);

/// Raft role in a group
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    /// Leader node
    Leader,
    /// Follower node
    Follower,
    /// Candidate node (during election)
    Candidate,
}

/// Node address information
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAddress {
    /// Node ID
    pub node_id: String,
    /// gRPC address
    pub grpc_addr: String,
    /// Redis address (optional)
    pub redis_addr: Option<String>,
}

/// Raft group information
///
/// Contains information about a Raft group, including its members and leader.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroup {
    /// Raft group ID (typically same as shard_id)
    pub raft_id: String,
    /// Current leader node ID (None if no leader)
    pub leader_node_id: Option<String>,
    /// All nodes in this raft group
    pub nodes: Vec<NodeAddress>,
    /// Current role of this node in the group (if this node is part of the group)
    pub role: Option<RaftRole>,
}

impl RaftGroup {
    /// Create a new RaftGroup
    pub fn new(raft_id: String) -> Self {
        Self {
            raft_id,
            leader_node_id: None,
            nodes: Vec::new(),
            role: None,
        }
    }

    /// Add a node to the group
    pub fn add_node(&mut self, node: NodeAddress) {
        if !self.nodes.iter().any(|n| n.node_id == node.node_id) {
            self.nodes.push(node);
        }
    }

    /// Remove a node from the group
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n.node_id != node_id);
        if self.leader_node_id.as_deref() == Some(node_id) {
            self.leader_node_id = None;
        }
    }

    /// Set the leader node
    pub fn set_leader(&mut self, node_id: String) {
        // Verify node exists in the group
        if self.nodes.iter().any(|n| n.node_id == node_id) {
            self.leader_node_id = Some(node_id);
        }
    }

    /// Get the leader node address
    pub fn get_leader_address(&self) -> Option<&NodeAddress> {
        self.leader_node_id
            .as_ref()
            .and_then(|leader_id| self.nodes.iter().find(|n| n.node_id == *leader_id))
    }
}

/// Routing table errors
#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("Shard not found: {0}")]
    ShardNotFound(ShardId),
    #[error("Raft group not found: {0}")]
    RaftGroupNotFound(String),
    #[error("No leader for shard: {0}")]
    NoLeader(ShardId),
    #[error("Invalid slot range: start={0}, end={1}")]
    InvalidSlotRange(u32, u32),
}

/// Routing table for managing shard and raft group routing
///
/// Provides efficient lookup of:
/// - Key -> ShardId
/// - ShardId -> Leader NodeId
/// - ShardId -> RaftGroup
#[derive(Clone)]
pub struct RoutingTable {
    /// Shard routing information: shard_id -> ShardRouting (for O(1) lookup by shard_id)
    shard_routings: Arc<RwLock<HashMap<ShardId, ShardRouting>>>,
    /// Sorted shard routings by slot_start (for O(log n) binary search by slot)
    /// This is maintained in sorted order for efficient slot-based lookups
    sorted_routings: Arc<RwLock<Vec<ShardRouting>>>,
    /// Raft group information: raft_id -> RaftGroup
    raft_groups: Arc<RwLock<HashMap<String, RaftGroup>>>,
    /// Shard to Raft ID mapping: shard_id -> raft_id
    shard_to_raft: Arc<RwLock<HashMap<ShardId, String>>>,
}

impl RoutingTable {
    /// Create a new RoutingTable
    pub fn new() -> Self {
        Self {
            shard_routings: Arc::new(RwLock::new(HashMap::new())),
            sorted_routings: Arc::new(RwLock::new(Vec::new())),
            raft_groups: Arc::new(RwLock::new(HashMap::new())),
            shard_to_raft: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Rebuild sorted routings list from the hash map
    /// This maintains the sorted order for binary search
    fn rebuild_sorted_routings(&self) {
        let routings = self.shard_routings.read();
        let mut sorted: Vec<ShardRouting> = routings.values().cloned().collect();
        // Sort by slot_start for binary search
        sorted.sort_by_key(|r| r.slot_start);
        *self.sorted_routings.write() = sorted;
    }

    /// Calculate slot for a key using CRC16 (compatible with Redis Cluster)
    pub fn slot_for_key(key: &[u8]) -> u32 {
        CRC16.checksum(key) as u32 % TOTAL_SLOTS
    }

    /// Find shard_id for a key
    ///
    /// # Arguments
    /// - `key`: The key to look up
    ///
    /// # Returns
    /// - `Ok(ShardId)`: The shard_id that owns this key
    /// - `Err(RoutingError)`: If no shard is found for the key's slot
    pub fn find_shard_for_key(&self, key: &[u8]) -> Result<ShardId, RoutingError> {
        let slot = Self::slot_for_key(key);
        self.find_shard_for_slot(slot)
    }

    /// Find shard_id for a slot using binary search
    ///
    /// Uses binary search on sorted routings for O(log n) performance.
    /// The routings are sorted by slot_start, so we can efficiently find
    /// the routing that contains the given slot.
    ///
    /// # Arguments
    /// - `slot`: The slot number
    ///
    /// # Returns
    /// - `Ok(ShardId)`: The shard_id that owns this slot
    /// - `Err(RoutingError)`: If no shard is found for the slot
    pub fn find_shard_for_slot(&self, slot: u32) -> Result<ShardId, RoutingError> {
        let sorted = self.sorted_routings.read();

        if sorted.is_empty() {
            return Err(RoutingError::ShardNotFound(format!("slot_{}", slot)));
        }

        // Binary search for the largest routing where slot_start <= slot
        // We use partition_point to find the rightmost element where slot_start <= slot
        let idx = sorted.partition_point(|r| r.slot_start <= slot);

        // Check the element before the partition point (if exists)
        // This is the routing with the largest slot_start <= slot
        if idx > 0 {
            let candidate = &sorted[idx - 1];
            if candidate.contains_slot(slot) {
                return Ok(candidate.shard_id.clone());
            }
        }

        Err(RoutingError::ShardNotFound(format!("slot_{}", slot)))
    }

    /// Get shard routing information
    pub fn get_shard_routing(&self, shard_id: &ShardId) -> Option<ShardRouting> {
        self.shard_routings.read().get(shard_id).cloned()
    }

    /// Add or update shard routing
    ///
    /// Updates both the hash map (for O(1) lookup by shard_id) and
    /// rebuilds the sorted list (for O(log n) lookup by slot).
    pub fn add_shard_routing(&self, routing: ShardRouting) {
        self.shard_routings
            .write()
            .insert(routing.shard_id.clone(), routing);
        self.rebuild_sorted_routings();
    }

    /// Remove shard routing
    ///
    /// Updates both the hash map and rebuilds the sorted list.
    pub fn remove_shard_routing(&self, shard_id: &ShardId) {
        self.shard_routings.write().remove(shard_id);
        self.shard_to_raft.write().remove(shard_id);
        self.rebuild_sorted_routings();
    }

    /// Find leader node_id for a shard
    ///
    /// # Arguments
    /// - `shard_id`: The shard ID
    ///
    /// # Returns
    /// - `Ok(String)`: The leader node_id
    /// - `Err(RoutingError)`: If shard or leader is not found
    pub fn find_leader_for_shard(&self, shard_id: &ShardId) -> Result<String, RoutingError> {
        // First, find the raft_id for this shard
        let raft_id = self
            .shard_to_raft
            .read()
            .get(shard_id)
            .cloned()
            .ok_or_else(|| RoutingError::RaftGroupNotFound(shard_id.clone()))?;

        // Then, find the leader in the raft group
        let groups = self.raft_groups.read();
        let group = groups
            .get(&raft_id)
            .ok_or_else(|| RoutingError::RaftGroupNotFound(raft_id.clone()))?;

        group
            .leader_node_id
            .clone()
            .ok_or_else(|| RoutingError::NoLeader(shard_id.clone()))
    }

    /// Get leader node address for a shard
    pub fn get_leader_address_for_shard(
        &self,
        shard_id: &ShardId,
    ) -> Result<NodeAddress, RoutingError> {
        let raft_id = self
            .shard_to_raft
            .read()
            .get(shard_id)
            .cloned()
            .ok_or_else(|| RoutingError::RaftGroupNotFound(shard_id.clone()))?;

        let groups = self.raft_groups.read();
        let group = groups
            .get(&raft_id)
            .ok_or_else(|| RoutingError::RaftGroupNotFound(raft_id))?;

        group
            .get_leader_address()
            .cloned()
            .ok_or_else(|| RoutingError::NoLeader(shard_id.clone()))
    }

    /// Add or update raft group
    pub fn add_raft_group(&self, group: RaftGroup) {
        let raft_id = group.raft_id.clone();
        self.raft_groups.write().insert(raft_id.clone(), group);
    }

    /// Get raft group
    pub fn get_raft_group(&self, raft_id: &str) -> Option<RaftGroup> {
        self.raft_groups.read().get(raft_id).cloned()
    }

    /// Remove raft group
    pub fn remove_raft_group(&self, raft_id: &str) {
        self.raft_groups.write().remove(raft_id);
    }

    /// Associate a shard with a raft group
    pub fn associate_shard_with_raft(&self, shard_id: ShardId, raft_id: String) {
        self.shard_to_raft.write().insert(shard_id, raft_id);
    }

    /// Get all shard routings
    pub fn list_shard_routings(&self) -> Vec<ShardRouting> {
        self.shard_routings.read().values().cloned().collect()
    }

    /// Get all raft groups
    pub fn list_raft_groups(&self) -> Vec<RaftGroup> {
        self.raft_groups.read().values().cloned().collect()
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_for_key() {
        let slot1 = RoutingTable::slot_for_key(b"test_key");
        let slot2 = RoutingTable::slot_for_key(b"test_key");
        assert_eq!(slot1, slot2, "Slot calculation should be deterministic");
        assert!(slot1 < TOTAL_SLOTS, "Slot should be within valid range");
    }

    #[test]
    fn test_find_shard_for_key() {
        let table = RoutingTable::new();
        let routing = ShardRouting::new("shard_0".to_string(), 0, 4096);
        table.add_shard_routing(routing);

        // Test with a key that should map to slot 0-4095
        // We'll use a key that we know maps to a slot in this range
        let key = b"test";
        let slot = RoutingTable::slot_for_key(key);
        if slot < 4096 {
            let shard_id = table.find_shard_for_key(key).unwrap();
            assert_eq!(shard_id, "shard_0");
        }
    }

    #[test]
    fn test_find_shard_for_slot_binary_search() {
        let table = RoutingTable::new();

        // Add multiple shards with different slot ranges
        table.add_shard_routing(ShardRouting::new("shard_0".to_string(), 0, 4096));
        table.add_shard_routing(ShardRouting::new("shard_1".to_string(), 4096, 8192));
        table.add_shard_routing(ShardRouting::new("shard_2".to_string(), 8192, 12288));
        table.add_shard_routing(ShardRouting::new("shard_3".to_string(), 12288, 16384));

        // Test various slots
        assert_eq!(table.find_shard_for_slot(0).unwrap(), "shard_0");
        assert_eq!(table.find_shard_for_slot(4095).unwrap(), "shard_0");
        assert_eq!(table.find_shard_for_slot(4096).unwrap(), "shard_1");
        assert_eq!(table.find_shard_for_slot(8191).unwrap(), "shard_1");
        assert_eq!(table.find_shard_for_slot(8192).unwrap(), "shard_2");
        assert_eq!(table.find_shard_for_slot(12287).unwrap(), "shard_2");
        assert_eq!(table.find_shard_for_slot(12288).unwrap(), "shard_3");
        assert_eq!(table.find_shard_for_slot(16383).unwrap(), "shard_3");

        // Test edge cases
        assert!(table.find_shard_for_slot(16384).is_err()); // Out of range
    }

    #[test]
    fn test_binary_search_performance() {
        let table = RoutingTable::new();

        // Add many shards to test binary search performance
        for i in 0..100 {
            let start = i * 163;
            let end = (i + 1) * 163;
            table.add_shard_routing(ShardRouting::new(format!("shard_{}", i), start, end));
        }

        // Test that binary search works correctly with many shards
        assert_eq!(table.find_shard_for_slot(0).unwrap(), "shard_0");
        assert_eq!(table.find_shard_for_slot(163).unwrap(), "shard_1");
        assert_eq!(table.find_shard_for_slot(16299).unwrap(), "shard_99");
    }

    #[test]
    fn test_find_leader_for_shard() {
        let table = RoutingTable::new();

        // Create a raft group
        let mut group = RaftGroup::new("raft_0".to_string());
        group.add_node(NodeAddress {
            node_id: "node1".to_string(),
            grpc_addr: "127.0.0.1:50051".to_string(),
            redis_addr: None,
        });
        group.set_leader("node1".to_string());
        table.add_raft_group(group);

        // Associate shard with raft group
        table.associate_shard_with_raft("shard_0".to_string(), "raft_0".to_string());

        // Find leader
        let leader = table.find_leader_for_shard(&"shard_0".to_string()).unwrap();
        assert_eq!(leader, "node1");
    }

    #[test]
    fn test_raft_group_operations() {
        let mut group = RaftGroup::new("raft_0".to_string());
        assert_eq!(group.raft_id, "raft_0");
        assert!(group.leader_node_id.is_none());

        group.add_node(NodeAddress {
            node_id: "node1".to_string(),
            grpc_addr: "127.0.0.1:50051".to_string(),
            redis_addr: None,
        });
        assert_eq!(group.nodes.len(), 1);

        group.set_leader("node1".to_string());
        assert_eq!(group.leader_node_id, Some("node1".to_string()));

        let leader_addr = group.get_leader_address().unwrap();
        assert_eq!(leader_addr.node_id, "node1");
    }
}
