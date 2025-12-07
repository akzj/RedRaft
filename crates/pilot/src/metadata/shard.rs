//! 分片信息定义

use serde::{Deserialize, Serialize};

use super::NodeId;

/// 分片 ID
pub type ShardId = String;

/// 哈希槽总数（类似 Redis Cluster 的 16384）
pub const TOTAL_SLOTS: u32 = 16384;

/// 键范围 [start, end)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    /// 起始槽位（包含）
    pub start: u32,
    /// 结束槽位（不包含）
    pub end: u32,
}

impl KeyRange {
    pub fn new(start: u32, end: u32) -> Self {
        assert!(start < end, "start must be less than end");
        assert!(end <= TOTAL_SLOTS, "end must not exceed TOTAL_SLOTS");
        Self { start, end }
    }

    /// 槽位数量
    pub fn slot_count(&self) -> u32 {
        self.end - self.start
    }

    /// 检查槽位是否在范围内
    pub fn contains(&self, slot: u32) -> bool {
        slot >= self.start && slot < self.end
    }
}

impl std::fmt::Display for KeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// 分片状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    /// 正常服务
    Normal,
    /// 创建中
    Creating,
    /// 迁移中
    Migrating,
    /// 删除中
    Deleting,
    /// 错误状态
    Error,
}

impl Default for ShardStatus {
    fn default() -> Self {
        Self::Creating
    }
}

impl std::fmt::Display for ShardStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardStatus::Normal => write!(f, "normal"),
            ShardStatus::Creating => write!(f, "creating"),
            ShardStatus::Migrating => write!(f, "migrating"),
            ShardStatus::Deleting => write!(f, "deleting"),
            ShardStatus::Error => write!(f, "error"),
        }
    }
}

/// 分片信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    /// 分片 ID
    pub id: ShardId,
    /// 负责的键范围
    pub key_range: KeyRange,
    /// 副本节点列表（第一个为 preferred leader）
    pub replicas: Vec<NodeId>,
    /// 当前 Raft leader
    pub leader: Option<NodeId>,
    /// 分片状态
    pub status: ShardStatus,
    /// 副本因子
    pub replica_factor: u32,
}

impl ShardInfo {
    /// 创建新分片
    pub fn new(id: ShardId, key_range: KeyRange, replica_factor: u32) -> Self {
        Self {
            id,
            key_range,
            replicas: Vec::new(),
            leader: None,
            status: ShardStatus::Creating,
            replica_factor,
        }
    }

    /// 添加副本节点
    pub fn add_replica(&mut self, node_id: NodeId) {
        if !self.replicas.contains(&node_id) {
            self.replicas.push(node_id);
        }
    }

    /// 移除副本节点
    pub fn remove_replica(&mut self, node_id: &NodeId) {
        self.replicas.retain(|n| n != node_id);
        if self.leader.as_ref() == Some(node_id) {
            self.leader = None;
        }
    }

    /// 设置 leader
    pub fn set_leader(&mut self, node_id: NodeId) {
        if self.replicas.contains(&node_id) {
            self.leader = Some(node_id);
        }
    }

    /// 检查副本数是否满足要求
    pub fn is_replica_satisfied(&self) -> bool {
        self.replicas.len() >= self.replica_factor as usize
    }

    /// 检查是否健康（有 leader 且副本数满足）
    pub fn is_healthy(&self) -> bool {
        self.status == ShardStatus::Normal 
            && self.leader.is_some() 
            && self.is_replica_satisfied()
    }
}
