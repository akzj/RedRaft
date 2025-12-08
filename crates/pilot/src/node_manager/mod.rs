//! Node management module
//!
//! Responsible for node registration, heartbeat detection, and failure detection

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::metadata::{ClusterMetadata, NodeId, NodeInfo, NodeStatus};

/// Node manager configuration
#[derive(Debug, Clone)]
pub struct NodeManagerConfig {
    /// Heartbeat timeout (seconds)
    pub heartbeat_timeout_secs: i64,
    /// Heartbeat check interval (seconds)
    pub check_interval_secs: u64,
}

impl Default for NodeManagerConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_secs: 30,
            check_interval_secs: 10,
        }
    }
}

/// Node manager
pub struct NodeManager {
    config: NodeManagerConfig,
    metadata: Arc<RwLock<ClusterMetadata>>,
}

impl NodeManager {
    /// Create node manager
    pub fn new(config: NodeManagerConfig, metadata: Arc<RwLock<ClusterMetadata>>) -> Self {
        Self { config, metadata }
    }

    /// Register node
    pub async fn register(&self, node: NodeInfo) -> RegisterResult {
        let mut metadata = self.metadata.write().await;
        let node_id = node.id.clone();
        let is_new = metadata.register_node(node);
        
        if is_new {
            info!("New node registered: {}", node_id);
            RegisterResult::NewNode
        } else {
            info!("Node re-registered: {}", node_id);
            RegisterResult::Reconnected
        }
    }

    /// Handle heartbeat
    pub async fn heartbeat(&self, node_id: &NodeId) -> bool {
        let mut metadata = self.metadata.write().await;
        let result = metadata.node_heartbeat(node_id);
        if result {
            debug!("Heartbeat from node: {}", node_id);
        } else {
            warn!("Heartbeat from unknown node: {}", node_id);
        }
        result
    }

    /// Actively drain node
    pub async fn drain_node(&self, node_id: &NodeId) -> bool {
        let mut metadata = self.metadata.write().await;
        if let Some(node) = metadata.nodes.get_mut(node_id) {
            node.status = NodeStatus::Draining;
            info!("Node {} is now draining", node_id);
            true
        } else {
            false
        }
    }

    /// Remove node
    pub async fn remove_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let mut metadata = self.metadata.write().await;
        let node = metadata.remove_node(node_id);
        if node.is_some() {
            info!("Node {} removed from cluster", node_id);
        }
        node
    }

    /// Get node information
    pub async fn get_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata.nodes.get(node_id).cloned()
    }

    /// Get all nodes
    pub async fn list_nodes(&self) -> Vec<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata.nodes.values().cloned().collect()
    }

    /// Get online nodes
    pub async fn online_nodes(&self) -> Vec<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .cloned()
            .collect()
    }

    /// Start heartbeat checker task
    pub fn start_heartbeat_checker(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let check_interval = Duration::from_secs(self.config.check_interval_secs);
        
        tokio::spawn(async move {
            let mut interval = interval(check_interval);
            loop {
                interval.tick().await;
                self.check_heartbeats().await;
            }
        })
    }

    /// Check heartbeats for all nodes
    async fn check_heartbeats(&self) {
        let mut metadata = self.metadata.write().await;
        let timeout_secs = self.config.heartbeat_timeout_secs;
        
        let mut offline_nodes = Vec::new();
        
        for (node_id, node) in metadata.nodes.iter_mut() {
            if node.status == NodeStatus::Online && node.is_heartbeat_timeout(timeout_secs) {
                warn!(
                    "Node {} heartbeat timeout, marking as offline",
                    node_id
                );
                node.status = NodeStatus::Offline;
                offline_nodes.push(node_id.clone());
            }
        }

        if !offline_nodes.is_empty() {
            info!("{} nodes marked as offline", offline_nodes.len());
            // TODO: Trigger shard reassignment
        }
    }
}

/// Registration result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterResult {
    /// New node
    NewNode,
    /// Reconnected node
    Reconnected,
}
