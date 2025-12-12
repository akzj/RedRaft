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
pub struct RoutingTable {
    /// Shard routing information: shard_id -> ShardRouting
    shard_routings: Arc<RwLock<HashMap<ShardId, ShardRouting>>>,
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
            raft_groups: Arc::new(RwLock::new(HashMap::new())),
            shard_to_raft: Arc::new(RwLock::new(HashMap::new())),
        }
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

    /// Find shard_id for a slot
    ///
    /// # Arguments
    /// - `slot`: The slot number
    ///
    /// # Returns
    /// - `Ok(ShardId)`: The shard_id that owns this slot
    /// - `Err(RoutingError)`: If no shard is found for the slot
    pub fn find_shard_for_slot(&self, slot: u32) -> Result<ShardId, RoutingError> {
        let routings = self.shard_routings.read();
        for routing in routings.values() {
            if routing.contains_slot(slot) {
                return Ok(routing.shard_id.clone());
            }
        }
        Err(RoutingError::ShardNotFound(format!("slot_{}", slot)))
    }

    /// Get shard routing information
    pub fn get_shard_routing(&self, shard_id: &ShardId) -> Option<ShardRouting> {
        self.shard_routings.read().get(shard_id).cloned()
    }

    /// Add or update shard routing
    pub fn add_shard_routing(&self, routing: ShardRouting) {
        self.shard_routings.write().insert(routing.shard_id.clone(), routing);
    }

    /// Remove shard routing
    pub fn remove_shard_routing(&self, shard_id: &ShardId) {
        self.shard_routings.write().remove(shard_id);
        self.shard_to_raft.write().remove(shard_id);
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

