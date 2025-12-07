//! 分片信息定义

use chrono::{DateTime, Utc};
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
    /// 分裂中
    Splitting,
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
            ShardStatus::Splitting => write!(f, "splitting"),
            ShardStatus::Deleting => write!(f, "deleting"),
            ShardStatus::Error => write!(f, "error"),
        }
    }
}

// ==================== 分裂相关定义 ====================

/// 分裂任务状态
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitStatus {
    /// 准备阶段 - 创建目标分片
    Preparing,
    /// 快照传输阶段
    SnapshotTransfer,
    /// 增量追赶阶段
    CatchingUp,
    /// 缓存请求阶段
    Buffering,
    /// 路由切换阶段
    Switching,
    /// 清理阶段
    Cleanup,
    /// 已完成
    Completed,
    /// 失败
    Failed(String),
    /// 已取消
    Cancelled,
}

impl std::fmt::Display for SplitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitStatus::Preparing => write!(f, "preparing"),
            SplitStatus::SnapshotTransfer => write!(f, "snapshot_transfer"),
            SplitStatus::CatchingUp => write!(f, "catching_up"),
            SplitStatus::Buffering => write!(f, "buffering"),
            SplitStatus::Switching => write!(f, "switching"),
            SplitStatus::Cleanup => write!(f, "cleanup"),
            SplitStatus::Completed => write!(f, "completed"),
            SplitStatus::Failed(reason) => write!(f, "failed: {}", reason),
            SplitStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// 分裂任务进度
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SplitProgress {
    /// 快照传输是否完成
    pub snapshot_done: bool,
    /// 快照截止的日志索引
    pub snapshot_index: u64,
    /// 源分片最新日志索引
    pub source_last_index: u64,
    /// 目标分片已应用的日志索引
    pub target_applied_index: u64,
    /// 当前缓存的请求数
    pub buffered_requests: usize,
}

impl SplitProgress {
    /// 计算延迟（日志条数）
    pub fn delay(&self) -> u64 {
        self.source_last_index.saturating_sub(self.target_applied_index)
    }
}

/// 分裂任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitTask {
    /// 任务 ID
    pub id: String,
    /// 源分片 ID
    pub source_shard: ShardId,
    /// 目标分片 ID
    pub target_shard: ShardId,
    /// 分裂点槽位（目标分片负责 [split_slot, source.end)）
    pub split_slot: u32,
    /// 任务状态
    pub status: SplitStatus,
    /// 进度信息
    pub progress: SplitProgress,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 更新时间
    pub updated_at: DateTime<Utc>,
}

impl SplitTask {
    /// 创建新的分裂任务
    pub fn new(
        source_shard: ShardId,
        target_shard: ShardId,
        split_slot: u32,
    ) -> Self {
        let now = Utc::now();
        let id = format!(
            "split_{}_{}_{}", 
            source_shard, 
            split_slot,
            now.timestamp_millis()
        );
        
        Self {
            id,
            source_shard,
            target_shard,
            split_slot,
            status: SplitStatus::Preparing,
            progress: SplitProgress::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// 更新状态
    pub fn set_status(&mut self, status: SplitStatus) {
        self.status = status;
        self.updated_at = Utc::now();
    }

    /// 更新进度
    pub fn update_progress(&mut self, progress: SplitProgress) {
        self.progress = progress;
        self.updated_at = Utc::now();
    }

    /// 是否已完成（成功或失败）
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            SplitStatus::Completed | SplitStatus::Failed(_) | SplitStatus::Cancelled
        )
    }

    /// 是否成功完成
    pub fn is_successful(&self) -> bool {
        matches!(self.status, SplitStatus::Completed)
    }
}

/// 分片在分裂中的角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitRole {
    /// 源分片（被分裂的分片）
    Source,
    /// 目标分片（新创建的分片）
    Target,
}

/// 分片的分裂状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSplitState {
    /// 关联的分裂任务 ID
    pub split_task_id: String,
    /// 分裂点槽位
    pub split_slot: u32,
    /// 在分裂中的角色
    pub role: SplitRole,
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
    /// 分裂状态（如果正在分裂）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_state: Option<ShardSplitState>,
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
            split_state: None,
        }
    }

    /// 检查是否正在分裂
    pub fn is_splitting(&self) -> bool {
        self.split_state.is_some()
    }

    /// 设置分裂状态
    pub fn set_split_state(&mut self, state: ShardSplitState) {
        self.split_state = Some(state);
        self.status = ShardStatus::Splitting;
    }

    /// 清除分裂状态
    pub fn clear_split_state(&mut self) {
        self.split_state = None;
        self.status = ShardStatus::Normal;
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
