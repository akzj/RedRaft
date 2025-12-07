//! 节点信息定义

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use super::ShardId;

/// 节点 ID
pub type NodeId = String;

/// 节点状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// 在线，可接受请求
    Online,
    /// 离线，心跳超时
    Offline,
    /// 下线中，正在迁移数据
    Draining,
    /// 未知状态（刚注册）
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

/// 节点信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// 节点 ID
    pub id: NodeId,
    /// gRPC 地址 (host:port)
    pub grpc_addr: String,
    /// Redis 协议地址 (host:port)
    pub redis_addr: String,
    /// 节点状态
    pub status: NodeStatus,
    /// 最后心跳时间
    pub last_heartbeat: DateTime<Utc>,
    /// 注册时间
    pub registered_at: DateTime<Utc>,
    /// 该节点托管的分片列表
    pub hosted_shards: HashSet<ShardId>,
    /// 节点容量（可托管的最大分片数）
    pub capacity: u32,
    /// 节点标签（用于放置策略）
    pub labels: std::collections::HashMap<String, String>,
}

impl NodeInfo {
    /// 创建新节点
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
            capacity: 100, // 默认容量
            labels: std::collections::HashMap::new(),
        }
    }

    /// 更新心跳
    pub fn touch(&mut self) {
        self.last_heartbeat = Utc::now();
        if self.status == NodeStatus::Unknown || self.status == NodeStatus::Offline {
            self.status = NodeStatus::Online;
        }
    }

    /// 检查是否心跳超时
    pub fn is_heartbeat_timeout(&self, timeout_secs: i64) -> bool {
        let elapsed = Utc::now().signed_duration_since(self.last_heartbeat);
        elapsed.num_seconds() > timeout_secs
    }

    /// 添加分片
    pub fn add_shard(&mut self, shard_id: ShardId) {
        self.hosted_shards.insert(shard_id);
    }

    /// 移除分片
    pub fn remove_shard(&mut self, shard_id: &ShardId) {
        self.hosted_shards.remove(shard_id);
    }

    /// 获取负载（托管分片数 / 容量）
    pub fn load(&self) -> f64 {
        self.hosted_shards.len() as f64 / self.capacity as f64
    }
}
