//! 迁移任务管理

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::metadata::{NodeId, ShardId};

/// 迁移状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// 等待执行
    Pending,
    /// 准备中（目标节点准备接收）
    Preparing,
    /// 迁移中（数据传输）
    InProgress,
    /// 完成
    Completed,
    /// 失败
    Failed,
    /// 已取消
    Cancelled,
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationStatus::Pending => write!(f, "pending"),
            MigrationStatus::Preparing => write!(f, "preparing"),
            MigrationStatus::InProgress => write!(f, "in_progress"),
            MigrationStatus::Completed => write!(f, "completed"),
            MigrationStatus::Failed => write!(f, "failed"),
            MigrationStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// 迁移任务
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTask {
    /// 任务 ID
    pub id: String,
    /// 分片 ID
    pub shard_id: ShardId,
    /// 源节点
    pub from_node: NodeId,
    /// 目标节点
    pub to_node: NodeId,
    /// 任务状态
    pub status: MigrationStatus,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 开始时间
    pub started_at: Option<DateTime<Utc>>,
    /// 完成时间
    pub completed_at: Option<DateTime<Utc>>,
    /// 进度 (0-100)
    pub progress: u8,
    /// 错误信息
    pub error: Option<String>,
}

impl MigrationTask {
    /// 创建新任务
    pub fn new(shard_id: ShardId, from_node: NodeId, to_node: NodeId) -> Self {
        let id = format!(
            "migration_{}_{}_{}",
            shard_id,
            Utc::now().timestamp_millis(),
            rand_suffix()
        );
        
        Self {
            id,
            shard_id,
            from_node,
            to_node,
            status: MigrationStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            progress: 0,
            error: None,
        }
    }

    /// 开始迁移
    pub fn start(&mut self) {
        self.status = MigrationStatus::InProgress;
        self.started_at = Some(Utc::now());
    }

    /// 更新进度
    pub fn update_progress(&mut self, progress: u8) {
        self.progress = progress.min(100);
    }

    /// 完成迁移
    pub fn complete(&mut self) {
        self.status = MigrationStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.progress = 100;
    }

    /// 迁移失败
    pub fn fail(&mut self, error: String) {
        self.status = MigrationStatus::Failed;
        self.completed_at = Some(Utc::now());
        self.error = Some(error);
    }

    /// 取消迁移
    pub fn cancel(&mut self) {
        self.status = MigrationStatus::Cancelled;
        self.completed_at = Some(Utc::now());
    }

    /// 检查是否已完成（包括失败和取消）
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            MigrationStatus::Completed | MigrationStatus::Failed | MigrationStatus::Cancelled
        )
    }
}

/// 迁移管理器
pub struct MigrationManager {
    /// 所有迁移任务
    tasks: RwLock<HashMap<String, MigrationTask>>,
}

impl MigrationManager {
    /// 创建迁移管理器
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
        }
    }

    /// 创建迁移任务
    pub fn create_task(&self, shard_id: ShardId, from_node: NodeId, to_node: NodeId) -> MigrationTask {
        let task = MigrationTask::new(shard_id, from_node, to_node);
        let task_clone = task.clone();
        self.tasks.write().insert(task.id.clone(), task);
        task_clone
    }

    /// 获取任务
    pub fn get_task(&self, task_id: &str) -> Option<MigrationTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// 获取分片的活动迁移任务
    pub fn get_active_task_for_shard(&self, shard_id: &ShardId) -> Option<MigrationTask> {
        self.tasks
            .read()
            .values()
            .find(|t| t.shard_id == *shard_id && !t.is_finished())
            .cloned()
    }

    /// 获取所有活动任务
    pub fn active_tasks(&self) -> Vec<MigrationTask> {
        self.tasks
            .read()
            .values()
            .filter(|t| !t.is_finished())
            .cloned()
            .collect()
    }

    /// 获取所有任务
    pub fn all_tasks(&self) -> Vec<MigrationTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// 开始任务
    pub fn start_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.start();
            true
        } else {
            false
        }
    }

    /// 更新任务进度
    pub fn update_progress(&self, task_id: &str, progress: u8) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.update_progress(progress);
            true
        } else {
            false
        }
    }

    /// 完成任务
    pub fn complete_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.complete();
            true
        } else {
            false
        }
    }

    /// 任务失败
    pub fn fail_task(&self, task_id: &str, error: String) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.fail(error);
            true
        } else {
            false
        }
    }

    /// 取消任务
    pub fn cancel_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.cancel();
            true
        } else {
            false
        }
    }

    /// 清理已完成的任务（保留最近 N 个）
    pub fn cleanup(&self, keep_count: usize) {
        let mut tasks = self.tasks.write();
        
        // 获取已完成任务并按完成时间排序
        let mut finished: Vec<_> = tasks
            .values()
            .filter(|t| t.is_finished())
            .cloned()
            .collect();
        
        finished.sort_by(|a, b| {
            b.completed_at.cmp(&a.completed_at)
        });

        // 删除超出保留数量的任务
        for task in finished.into_iter().skip(keep_count) {
            tasks.remove(&task.id);
        }
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 生成随机后缀
fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}", nanos)
}
