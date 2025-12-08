//! Node information definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::ShardId;

/// Node ID
pub type NodeId = String;

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Online, can accept requests
    Online,
    /// Offline, heartbeat timeout
    Offline,
    /// Draining, migrating data
    Draining,
    /// Unknown status (just registered)
    Unknown,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeStatus::Online => write!(f, "online"),
            NodeStatus::Offline => write!(f, "offline"),
            NodeStatus::Draining => write!(f, "draining"),
            NodeStatus::Unknown => write!(f, "unknown"),
        }
    }
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub id: NodeId,
    /// gRPC address (host:port)
    pub grpc_addr: String,
    /// Redis protocol address (host:port)
    pub redis_addr: String,
    /// Node status
    pub status: NodeStatus,
    /// Last heartbeat time
    pub last_heartbeat: DateTime<Utc>,
    /// Registration time
    pub registered_at: DateTime<Utc>,
    /// List of shards hosted by this node
    pub hosted_shards: HashSet<ShardId>,
    /// Node capacity (maximum number of shards it can host)
    pub capacity: u32,
    /// Node labels (for placement strategy)
    pub labels: std::collections::HashMap<String, String>,
}

impl NodeInfo {
    /// Create new node
    pub fn new(id: NodeId, grpc_addr: String, redis_addr: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            grpc_addr,
            redis_addr,
            status: NodeStatus::Unknown,
            last_heartbeat: now,
            registered_at: now,
            hosted_shards: HashSet::new(),
            capacity: 100, // Default capacity
            labels: std::collections::HashMap::new(),
        }
    }

    /// Update heartbeat
    pub fn touch(&mut self) {
        self.last_heartbeat = Utc::now();
        if self.status == NodeStatus::Unknown || self.status == NodeStatus::Offline {
            self.status = NodeStatus::Online;
        }
    }

    /// Check if heartbeat timeout
    pub fn is_heartbeat_timeout(&self, timeout_secs: i64) -> bool {
        let elapsed = Utc::now().signed_duration_since(self.last_heartbeat);
        elapsed.num_seconds() > timeout_secs
    }

    /// Add shard
    pub fn add_shard(&mut self, shard_id: ShardId) {
        self.hosted_shards.insert(shard_id);
    }

    /// Remove shard
    pub fn remove_shard(&mut self, shard_id: &ShardId) {
        self.hosted_shards.remove(shard_id);
    }

    /// Get load (hosted shard count / capacity)
    pub fn load(&self) -> f64 {
        self.hosted_shards.len() as f64 / self.capacity as f64
    }
}
