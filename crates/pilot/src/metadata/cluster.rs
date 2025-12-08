//! Cluster metadata

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{KeyRange, NodeId, NodeInfo, RoutingTable, ShardId, ShardInfo, TOTAL_SLOTS};

/// Cluster metadata
///
/// Contains complete cluster state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    /// Cluster name
    pub name: String,
    /// Creation time
    pub created_at: DateTime<Utc>,
    /// Last update time
    pub updated_at: DateTime<Utc>,
    /// Node information
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// Shard information
    pub shards: HashMap<ShardId, ShardInfo>,
    /// Routing table
    pub routing_table: RoutingTable,
    /// Default replica factor
    pub default_replica_factor: u32,
    /// Pre-created shard count
    pub initial_shard_count: u32,
}

impl ClusterMetadata {
    /// Create new cluster
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            name,
            created_at: now,
            updated_at: now,
            nodes: HashMap::new(),
            shards: HashMap::new(),
            routing_table: RoutingTable::new(),
            default_replica_factor: 3,
            initial_shard_count: 16,
        }
    }

    /// Initialize shards (pre-create)
    pub fn init_shards(&mut self) {
        let shard_count = self.initial_shard_count;
        let slots_per_shard = TOTAL_SLOTS / shard_count;

        for i in 0..shard_count {
            let shard_id = format!("shard_{:04}", i);
            let start = i * slots_per_shard;
            let end = if i == shard_count - 1 {
                TOTAL_SLOTS // Last shard includes remaining slots
            } else {
                (i + 1) * slots_per_shard
            };

            let key_range = KeyRange::new(start, end);
            let shard = ShardInfo::new(shard_id.clone(), key_range, self.default_replica_factor);

            self.shards.insert(shard_id.clone(), shard);
            self.routing_table.assign_slots(&shard_id, start, end);
        }

        self.touch();
    }

    /// Create new shard
    ///
    /// # Arguments
    /// - `shard_id`: Shard ID (auto-generated if None)
    /// - `replica_nodes`: Initial replica node list
    ///
    /// # Returns
    /// - Ok(ShardInfo): Creation successful
    /// - Err(String): Creation failure reason
    ///
    /// # Note
    /// Newly created shards are empty with no assigned slots. Slots need to be assigned via data migration later.
    pub fn create_shard(
        &mut self,
        shard_id: Option<String>,
        replica_nodes: Vec<NodeId>,
    ) -> Result<ShardInfo, String> {
        // Validate nodes exist
        for node_id in &replica_nodes {
            if !self.nodes.contains_key(node_id) {
                return Err(format!("Node {} does not exist", node_id));
            }
        }

        // Generate shard ID
        let shard_id = shard_id.unwrap_or_else(|| {
            let max_id = self
                .shards
                .keys()
                .filter_map(|k| k.strip_prefix("shard_").and_then(|n| n.parse::<u32>().ok()))
                .max()
                .unwrap_or(0);
            format!("shard_{:04}", max_id + 1)
        });

        // Check if shard ID already exists
        if self.shards.contains_key(&shard_id) {
            return Err(format!("Shard {} already exists", shard_id));
        }

        // Create empty shard (no assigned slots)
        let key_range = KeyRange::empty();
        let mut shard = ShardInfo::new(shard_id.clone(), key_range, self.default_replica_factor);

        // Add replica nodes
        for node_id in &replica_nodes {
            shard.add_replica(node_id.clone());
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.add_shard(shard_id.clone());
            }
        }

        // Set first node as leader (if any)
        if let Some(leader) = replica_nodes.first() {
            shard.set_leader(leader.clone());
            shard.status = super::ShardStatus::Normal;
        }

        // Update routing table (only set node mapping, don't assign slots)
        self.routing_table
            .set_shard_nodes(shard_id.clone(), replica_nodes);

        // Store shard
        let result = shard.clone();
        self.shards.insert(shard_id, shard);

        self.touch();
        Ok(result)
    }

    /// Update timestamp
    fn touch(&mut self) {
        self.updated_at = Utc::now();
        self.routing_table.bump_version();
    }

    /// Register node
    pub fn register_node(&mut self, node: NodeInfo) -> bool {
        let node_id = node.id.clone();
        let grpc_addr = node.grpc_addr.clone();

        let is_new = !self.nodes.contains_key(&node_id);
        self.nodes.insert(node_id.clone(), node);
        self.routing_table.set_node_addr(node_id, grpc_addr);
        self.touch();

        is_new
    }

    /// Remove node
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        let node = self.nodes.remove(node_id)?;
        self.routing_table.remove_node(node_id);

        // Remove node from all shards
        for shard in self.shards.values_mut() {
            shard.remove_replica(node_id);
        }

        self.touch();
        Some(node)
    }

    /// Update node heartbeat
    pub fn node_heartbeat(&mut self, node_id: &NodeId) -> bool {
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.touch();
            true
        } else {
            false
        }
    }

    /// Get online nodes
    pub fn online_nodes(&self) -> Vec<&NodeInfo> {
        use super::NodeStatus;
        self.nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .collect()
    }

    /// Assign shard to node
    pub fn assign_shard_to_node(&mut self, shard_id: &ShardId, node_id: &NodeId) -> bool {
        // Check if node and shard exist
        if !self.nodes.contains_key(node_id) || !self.shards.contains_key(shard_id) {
            return false;
        }

        // Update shard
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.add_replica(node_id.clone());
        }

        // Update node
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.add_shard(shard_id.clone());
        }

        // Update routing table
        if let Some(shard) = self.shards.get(shard_id) {
            self.routing_table
                .set_shard_nodes(shard_id.clone(), shard.replicas.clone());
        }

        self.touch();
        true
    }

    /// Set shard leader
    pub fn set_shard_leader(&mut self, shard_id: &ShardId, leader_id: &NodeId) -> bool {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            if shard.replicas.contains(leader_id) {
                shard.set_leader(leader_id.clone());

                // Update routing table, put leader first
                let mut nodes = shard.replicas.clone();
                if let Some(pos) = nodes.iter().position(|n| n == leader_id) {
                    nodes.remove(pos);
                    nodes.insert(0, leader_id.clone());
                }
                self.routing_table.set_shard_nodes(shard_id.clone(), nodes);

                self.touch();
                return true;
            }
        }
        false
    }

    /// Update shard status
    pub fn set_shard_status(&mut self, shard_id: &ShardId, status: super::ShardStatus) -> bool {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.status = status;
            self.touch();
            true
        } else {
            false
        }
    }

    /// Get shards needing replicas (insufficient replica count)
    pub fn shards_needing_replicas(&self) -> Vec<&ShardInfo> {
        self.shards
            .values()
            .filter(|s| !s.is_replica_satisfied())
            .collect()
    }

    /// Get shards without leader
    pub fn shards_without_leader(&self) -> Vec<&ShardInfo> {
        self.shards
            .values()
            .filter(|s| s.leader.is_none() && !s.replicas.is_empty())
            .collect()
    }

    /// Get cluster statistics
    pub fn stats(&self) -> ClusterStats {
        use super::{NodeStatus, ShardStatus};

        ClusterStats {
            total_nodes: self.nodes.len(),
            online_nodes: self
                .nodes
                .values()
                .filter(|n| n.status == NodeStatus::Online)
                .count(),
            total_shards: self.shards.len(),
            healthy_shards: self.shards.values().filter(|s| s.is_healthy()).count(),
            migrating_shards: self
                .shards
                .values()
                .filter(|s| s.status == ShardStatus::Migrating)
                .count(),
            routing_version: self.routing_table.version,
            unassigned_slots: self.routing_table.unassigned_slot_count(),
        }
    }
}

/// Cluster statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub online_nodes: usize,
    pub total_shards: usize,
    pub healthy_shards: usize,
    pub migrating_shards: usize,
    pub routing_version: u64,
    pub unassigned_slots: usize,
}
