//! 节点管理模块
//!
//! 负责节点注册、心跳检测、故障检测

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::metadata::{ClusterMetadata, NodeId, NodeInfo, NodeStatus};

/// 节点管理器配置
#[derive(Debug, Clone)]
pub struct NodeManagerConfig {
    /// 心跳超时时间（秒）
    pub heartbeat_timeout_secs: i64,
    /// 心跳检查间隔（秒）
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

/// 节点管理器
pub struct NodeManager {
    config: NodeManagerConfig,
    metadata: Arc<RwLock<ClusterMetadata>>,
}

impl NodeManager {
    /// 创建节点管理器
    pub fn new(config: NodeManagerConfig, metadata: Arc<RwLock<ClusterMetadata>>) -> Self {
        Self { config, metadata }
    }

    /// 注册节点
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

    /// 处理心跳
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

    /// 主动下线节点
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

    /// 移除节点
    pub async fn remove_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let mut metadata = self.metadata.write().await;
        let node = metadata.remove_node(node_id);
        if node.is_some() {
            info!("Node {} removed from cluster", node_id);
        }
        node
    }

    /// 获取节点信息
    pub async fn get_node(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata.nodes.get(node_id).cloned()
    }

    /// 获取所有节点
    pub async fn list_nodes(&self) -> Vec<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata.nodes.values().cloned().collect()
    }

    /// 获取在线节点
    pub async fn online_nodes(&self) -> Vec<NodeInfo> {
        let metadata = self.metadata.read().await;
        metadata
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .cloned()
            .collect()
    }

    /// 启动心跳检查任务
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

    /// 检查所有节点的心跳
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
            // TODO: 触发分片重新分配
        }
    }
}

/// 注册结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterResult {
    /// 新节点
    NewNode,
    /// 重新连接的节点
    Reconnected,
}
