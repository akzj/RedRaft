//! Shard Router - Key-value routing module
//!
//! Responsible for routing key-value pairs to corresponding Raft groups (Shards)
//! Supports two modes:
//! 1. Local mode: Uses fixed shard count hashing
//! 2. Pilot mode: Uses routing table fetched from Pilot

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use raft::RaftId;
use crate::pilot_client::{RoutingTable, ShardSplitInfo};

/// Total number of slots (consistent with Pilot)
pub const TOTAL_SLOTS: u32 = 16384;

/// Shard Router
/// Responsible for mapping keys to Raft groups
#[derive(Clone)]
pub struct ShardRouter {
    /// Shard count (for local mode)
    shard_count: usize,
    /// Active shard list
    active_shards: Arc<RwLock<HashSet<String>>>,
    /// Shard ID to node list mapping
    shard_locations: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Node address mapping
    node_addrs: Arc<RwLock<HashMap<String, String>>>,
    /// Slot to shard mapping (for Pilot mode)
    slots: Arc<RwLock<Vec<Option<String>>>>,
    /// Routing table version
    routing_version: Arc<RwLock<u64>>,
    /// Whether to use Pilot routing
    use_pilot_routing: Arc<RwLock<bool>>,
    /// Splitting shards (source_shard_id -> SplitInfo)
    splitting_shards: Arc<RwLock<HashMap<String, ShardSplitInfo>>>,
}

impl ShardRouter {
    /// Create local mode router
    pub fn new(shard_count: usize) -> Self {
        let mut active_shards = HashSet::new();
        for i in 0..shard_count {
            active_shards.insert(format!("shard_{}", i));
        }

        Self {
            shard_count,
            active_shards: Arc::new(RwLock::new(active_shards)),
            shard_locations: Arc::new(RwLock::new(HashMap::new())),
            node_addrs: Arc::new(RwLock::new(HashMap::new())),
            slots: Arc::new(RwLock::new(vec![None; TOTAL_SLOTS as usize])),
            routing_version: Arc::new(RwLock::new(0)),
            use_pilot_routing: Arc::new(RwLock::new(false)),
            splitting_shards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Calculate slot for key (CRC16, compatible with Redis Cluster)
    pub fn slot_for_key(key: &[u8]) -> u32 {
        crc16(key) as u32 % TOTAL_SLOTS
    }

    /// Calculate Shard ID from key
    pub fn route_key(&self, key: &[u8]) -> String {
        // If using Pilot routing, prefer slot mapping
        if *self.use_pilot_routing.read() {
            let slot = Self::slot_for_key(key);
            let slots = self.slots.read();
            if let Some(Some(shard_id)) = slots.get(slot as usize) {
                return shard_id.clone();
            }
        }
        
        // Fallback to local hash routing
        let mut hasher = Sha256::new();
        hasher.update(key);
        let hash = hasher.finalize();
        
        let hash_u64 = u64::from_be_bytes([
            hash[0], hash[1], hash[2], hash[3],
            hash[4], hash[5], hash[6], hash[7],
        ]);
        
        let shard_id = (hash_u64 % self.shard_count as u64) as usize;
        format!("shard_{}", shard_id)
    }

    /// Get Raft group ID for key
    pub fn get_raft_group_id(&self, key: &[u8], node_id: &str) -> RaftId {
        let shard_id = self.route_key(key);
        RaftId::new(shard_id, node_id.to_string())
    }

    /// Get leader node address for key
    pub fn get_leader_addr_for_key(&self, key: &[u8]) -> Option<String> {
        let shard_id = self.route_key(key);
        let shard_locations = self.shard_locations.read();
        let node_addrs = self.node_addrs.read();
        
        let nodes = shard_locations.get(&shard_id)?;
        let leader = nodes.first()?;
        node_addrs.get(leader).cloned()
    }

    /// Check if current node is the leader for a key
    pub fn is_leader_for_key(&self, key: &[u8], node_id: &str) -> bool {
        let shard_id = self.route_key(key);
        let shard_locations = self.shard_locations.read();
        
        if let Some(nodes) = shard_locations.get(&shard_id) {
            nodes.first().map(|s| s == node_id).unwrap_or(false)
        } else {
            // When no configuration, default to current node handling
            true
        }
    }

    /// Update from Pilot routing table
    pub fn update_from_pilot(&self, routing_table: &RoutingTable) {
        let new_version = routing_table.version;
        let current_version = *self.routing_version.read();
        
        if new_version <= current_version {
            return;
        }

        // Update slot mapping
        {
            let mut slots = self.slots.write();
            *slots = routing_table.slots.clone();
        }

        // Update shard locations
        {
            let mut shard_locations = self.shard_locations.write();
            let mut active_shards = self.active_shards.write();
            
            shard_locations.clear();
            active_shards.clear();
            
            for (shard_id, nodes) in &routing_table.shard_nodes {
                shard_locations.insert(shard_id.clone(), nodes.clone());
                active_shards.insert(shard_id.clone());
            }
        }

        // Update node addresses
        {
            let mut node_addrs = self.node_addrs.write();
            *node_addrs = routing_table.node_addrs.clone();
        }

        // Update split status
        {
            let mut splitting = self.splitting_shards.write();
            *splitting = routing_table.splitting_shards.clone();
            
            if !splitting.is_empty() {
                info!(
                    "Router: {} shards are splitting",
                    splitting.len()
                );
            }
        }

        // Update version and enable Pilot routing
        *self.routing_version.write() = new_version;
        *self.use_pilot_routing.write() = true;

        info!(
            "Router updated from pilot: version {}, {} shards, {} nodes",
            new_version,
            routing_table.shard_nodes.len(),
            routing_table.node_addrs.len()
        );
    }

    /// Get routing table version
    pub fn routing_version(&self) -> u64 {
        *self.routing_version.read()
    }

    /// Whether using Pilot routing
    pub fn is_pilot_routing(&self) -> bool {
        *self.use_pilot_routing.read()
    }

    /// Add shard configuration (local mode)
    pub fn add_shard(&self, shard_id: String, nodes: Vec<String>) {
        let node_count = nodes.len();
        self.active_shards.write().insert(shard_id.clone());
        self.shard_locations.write().insert(shard_id, nodes);
        debug!("Added shard with {} nodes", node_count);
    }

    /// Remove shard
    pub fn remove_shard(&self, shard_id: &str) {
        self.active_shards.write().remove(shard_id);
        self.shard_locations.write().remove(shard_id);
        debug!("Removed shard: {}", shard_id);
    }

    /// Get all active shards
    pub fn get_active_shards(&self) -> Vec<String> {
        self.active_shards.read().iter().cloned().collect()
    }

    /// Get node list for shard
    pub fn get_shard_nodes(&self, shard_id: &str) -> Option<Vec<String>> {
        self.shard_locations.read().get(shard_id).cloned()
    }

    /// Update shard configuration
    pub fn update_shard(&self, shard_id: String, nodes: Vec<String>) {
        self.shard_locations.write().insert(shard_id.clone(), nodes);
        debug!("Updated shard: {}", shard_id);
    }

    /// Set node address
    pub fn set_node_addr(&self, node_id: String, addr: String) {
        self.node_addrs.write().insert(node_id, addr);
    }

    /// Check if shard exists
    pub fn has_shard(&self, shard_id: &str) -> bool {
        self.active_shards.read().contains(shard_id)
    }

    /// Get shard count
    pub fn shard_count(&self) -> usize {
        if *self.use_pilot_routing.read() {
            self.active_shards.read().len()
        } else {
            self.shard_count
        }
    }

    /// Get all node addresses
    pub fn get_all_node_addrs(&self) -> HashMap<String, String> {
        self.node_addrs.read().clone()
    }

    /// Check if shard is splitting
    pub fn is_shard_splitting(&self, shard_id: &str) -> bool {
        self.splitting_shards.read().contains_key(shard_id)
    }

    /// Get shard split information
    pub fn get_split_info(&self, shard_id: &str) -> Option<ShardSplitInfo> {
        self.splitting_shards.read().get(shard_id).cloned()
    }

    /// Check if key should be MOVED to new shard (after split completes)
    /// 
    /// Returns Some((target_shard, target_addr)) if should redirect
    pub fn should_move_for_split(&self, key: &[u8], shard_id: &str) -> Option<(String, String)> {
        let splitting = self.splitting_shards.read();
        let split_info = splitting.get(shard_id)?;
        
        let slot = Self::slot_for_key(key);
        
        // If slot >= split_slot, and split status is switching or completed
        // then should move to target shard
        if slot >= split_info.split_slot 
            && (split_info.status == "switching" || split_info.status == "completed") 
        {
            let shard_locations = self.shard_locations.read();
            let node_addrs = self.node_addrs.read();
            
            let target_nodes = shard_locations.get(&split_info.target_shard)?;
            let target_leader = target_nodes.first()?;
            let target_addr = node_addrs.get(target_leader)?;
            
            Some((split_info.target_shard.clone(), target_addr.clone()))
        } else {
            None
        }
    }

    /// Check if key is in split buffer range (should buffer request)
    pub fn should_buffer_for_split(&self, key: &[u8], shard_id: &str) -> bool {
        let splitting = self.splitting_shards.read();
        if let Some(split_info) = splitting.get(shard_id) {
            let slot = Self::slot_for_key(key);
            // In buffering phase, and key is in split range
            slot >= split_info.split_slot && split_info.status == "buffering"
        } else {
            false
        }
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
    fn test_route_key() {
        let router = ShardRouter::new(3);
        
        let key1 = b"key1";
        let key2 = b"key2";
        
        let shard1 = router.route_key(key1);
        let shard2 = router.route_key(key2);
        
        // Same key should route to same shard
        assert_eq!(router.route_key(key1), shard1);
        
        // Different keys may route to different shards (depends on hash)
        println!("key1 -> {}, key2 -> {}", shard1, shard2);
    }

    #[test]
    fn test_shard_management() {
        let router = ShardRouter::new(3);
        
        router.add_shard("shard_0".to_string(), vec!["node1".to_string(), "node2".to_string()]);
        assert!(router.has_shard("shard_0"));
        
        let nodes = router.get_shard_nodes("shard_0");
        assert_eq!(nodes, Some(vec!["node1".to_string(), "node2".to_string()]));
        
        router.remove_shard("shard_0");
        assert!(!router.has_shard("shard_0"));
    }

    #[test]
    fn test_slot_calculation() {
        // Test slot calculation consistency
        let slot1 = ShardRouter::slot_for_key(b"hello");
        let slot2 = ShardRouter::slot_for_key(b"hello");
        assert_eq!(slot1, slot2);
        
        // Ensure within range
        assert!(slot1 < TOTAL_SLOTS);
    }

    #[test]
    fn test_pilot_routing() {
        let router = ShardRouter::new(3);
        
        // Create mock routing table
        let mut routing_table = RoutingTable::default();
        routing_table.version = 1;
        
        // Assign slots
        for i in 0..8192 {
            routing_table.slots[i] = Some("shard_0000".to_string());
        }
        for i in 8192..16384 {
            routing_table.slots[i] = Some("shard_0001".to_string());
        }
        
        routing_table.shard_nodes.insert(
            "shard_0000".to_string(),
            vec!["node1".to_string(), "node2".to_string()],
        );
        routing_table.shard_nodes.insert(
            "shard_0001".to_string(),
            vec!["node2".to_string(), "node3".to_string()],
        );
        
        routing_table.node_addrs.insert("node1".to_string(), "127.0.0.1:6379".to_string());
        routing_table.node_addrs.insert("node2".to_string(), "127.0.0.1:6380".to_string());
        
        // Update routing
        router.update_from_pilot(&routing_table);
        
        assert!(router.is_pilot_routing());
        assert_eq!(router.routing_version(), 1);
        assert_eq!(router.shard_count(), 2);
    }
}
