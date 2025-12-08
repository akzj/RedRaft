//! 分片分裂管理器
//!
//! 负责协调分片分裂的完整流程：
//! 1. 准备阶段 - 创建目标分片
//! 2. 快照传输 - 传输历史数据
//! 3. 增量追赶 - 重放增量日志
//! 4. 缓存切换 - 缓存请求并切换路由
//! 5. 清理阶段 - 清理旧数据

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{debug, error, info, warn};

use crate::metadata::{
    ClusterMetadata, KeyRange, ShardId, ShardSplitState, ShardStatus,
    SplitPhase, SplitProgress, SplitRole, SplitStatus, SplitTask, SplittingShardInfo,
};

/// 分裂管理器配置
#[derive(Debug, Clone)]
pub struct SplitManagerConfig {
    /// 追赶阈值 - 延迟低于此值进入缓存模式
    pub catch_up_threshold: u64,
    /// 缓存超时（秒）
    pub buffer_timeout_secs: u64,
    /// 最大缓存请求数
    pub buffer_max_size: usize,
    /// 进度上报间隔（秒）
    pub progress_interval_secs: u64,
}

impl Default for SplitManagerConfig {
    fn default() -> Self {
        Self {
            catch_up_threshold: 100,
            buffer_timeout_secs: 5,
            buffer_max_size: 10000,
            progress_interval_secs: 1,
        }
    }
}

/// 分裂管理器
pub struct SplitManager {
    /// 配置
    config: SplitManagerConfig,
    /// 活动的分裂任务 (task_id -> task)
    tasks: RwLock<HashMap<String, SplitTask>>,
    /// 集群元数据
    metadata: Arc<AsyncRwLock<ClusterMetadata>>,
}

impl SplitManager {
    /// 创建分裂管理器
    pub fn new(config: SplitManagerConfig, metadata: Arc<AsyncRwLock<ClusterMetadata>>) -> Self {
        Self {
            config,
            tasks: RwLock::new(HashMap::new()),
            metadata,
        }
    }

    /// 触发分片分裂
    ///
    /// # 参数
    /// - `source_shard_id`: 源分片 ID
    /// - `split_slot`: 分裂点槽位
    /// - `target_shard_id`: 目标分片 ID（必须提供，且目标分片必须已存在且健康）
    ///
    /// # 要求
    /// - 源分片必须存在且状态为 Normal
    /// - 目标分片必须已存在且状态为 Normal（健康状态：有 leader 且副本数满足）
    /// - 分裂点必须在源分片范围内
    /// - 目标分片的 key_range 必须与分裂点匹配：[split_slot, source_shard.end)
    pub async fn trigger_split(
        &self,
        source_shard_id: &ShardId,
        split_slot: u32,
        target_shard_id: ShardId,
    ) -> Result<SplitTask, String> {
        let mut metadata = self.metadata.write().await;

        // 1. 验证源分片存在且状态正常
        let source_shard = metadata
            .shards
            .get(source_shard_id)
            .ok_or_else(|| format!("Source shard {} not found", source_shard_id))?;

        if source_shard.status != ShardStatus::Normal {
            return Err(format!(
                "Source shard {} is not in normal status: {}",
                source_shard_id, source_shard.status
            ));
        }

        if source_shard.is_splitting() {
            return Err(format!(
                "Source shard {} is already splitting",
                source_shard_id
            ));
        }

        // 2. 验证分裂点在源分片范围内
        let key_range = source_shard.key_range;
        if split_slot <= key_range.start || split_slot >= key_range.end {
            return Err(format!(
                "Split slot {} must be within range ({}, {})",
                split_slot, key_range.start, key_range.end
            ));
        }

        // 3. 验证目标分片存在
        let target_shard = metadata
            .shards
            .get(&target_shard_id)
            .ok_or_else(|| format!("Target shard {} not found. Target shard must be created and healthy before split", target_shard_id))?;

        // 4. 验证目标分片状态为 Normal（健康状态）
        if target_shard.status != ShardStatus::Normal {
            return Err(format!(
                "Target shard {} is not in normal status: {}. Target shard must be healthy before split",
                target_shard_id, target_shard.status
            ));
        }

        // 5. 验证目标分片健康（有 leader 且副本数满足）
        if !target_shard.is_healthy() {
            return Err(format!(
                "Target shard {} is not healthy. It must have a leader and satisfy replica factor before split",
                target_shard_id
            ));
        }

        // 6. 验证目标分片不在分裂中
        if target_shard.is_splitting() {
            return Err(format!(
                "Target shard {} is already splitting",
                target_shard_id
            ));
        }

        // 7. 验证目标分片的 key_range 与分裂点匹配
        let expected_target_range = KeyRange::new(split_slot, key_range.end);
        if target_shard.key_range != expected_target_range {
            return Err(format!(
                "Target shard {} key_range {:?} does not match expected range {:?} for split at slot {}",
                target_shard_id, target_shard.key_range, expected_target_range, split_slot
            ));
        }

        // 8. 创建分裂任务
        let task = SplitTask::new(
            source_shard_id.clone(),
            target_shard_id.clone(),
            split_slot,
        );

        info!(
            "Creating split task {}: {} -> {} at slot {}",
            task.id, source_shard_id, target_shard_id, split_slot
        );

        // 9. 更新目标分片的分裂状态
        let target_shard = metadata.shards.get_mut(&target_shard_id).unwrap();
        target_shard.set_split_state(ShardSplitState {
            split_task_id: task.id.clone(),
            split_slot,
            role: SplitRole::Target,
        });
        let target_nodes = target_shard.replicas.clone();

        // 10. 更新源分片状态
        let source_shard = metadata.shards.get_mut(source_shard_id).unwrap();
        source_shard.set_split_state(ShardSplitState {
            split_task_id: task.id.clone(),
            split_slot,
            role: SplitRole::Source,
        });

        // 11. 添加分裂信息到路由表（用于 Node 感知分裂状态）
        let splitting_info = SplittingShardInfo {
            source_shard: source_shard_id.clone(),
            target_shard: target_shard_id.clone(),
            split_slot,
            source_range: key_range,
            target_nodes: target_nodes.clone(),
            phase: SplitPhase::Preparing,
        };
        metadata.routing_table.add_splitting_shard(splitting_info);

        // 12. 存储任务
        let task_clone = task.clone();
        self.tasks.write().insert(task.id.clone(), task);

        info!(
            "Split task {} created successfully, using existing healthy target shard {}",
            task_clone.id, target_shard_id
        );

        Ok(task_clone)
    }

    /// 获取分裂任务
    pub fn get_task(&self, task_id: &str) -> Option<SplitTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// 获取所有分裂任务
    pub fn all_tasks(&self) -> Vec<SplitTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// 获取活动的分裂任务
    pub fn active_tasks(&self) -> Vec<SplitTask> {
        self.tasks
            .read()
            .values()
            .filter(|t| !t.is_finished())
            .cloned()
            .collect()
    }

    /// 更新任务状态
    pub fn update_task_status(&self, task_id: &str, status: SplitStatus) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            info!("Split task {} status: {} -> {}", task_id, task.status, status);
            task.set_status(status);
            true
        } else {
            warn!("Split task {} not found", task_id);
            false
        }
    }

    /// 更新任务进度
    pub fn update_task_progress(&self, task_id: &str, progress: SplitProgress) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            debug!(
                "Split task {} progress: delay={}, snapshot_done={}",
                task_id,
                progress.delay(),
                progress.snapshot_done
            );
            task.update_progress(progress);
            true
        } else {
            false
        }
    }

    /// 完成分裂 - 更新路由表
    pub async fn complete_split(&self, task_id: &str) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.status != SplitStatus::Switching {
            return Err(format!(
                "Task {} is not in switching status: {}",
                task_id, task.status
            ));
        }

        let mut metadata = self.metadata.write().await;

        // 1. 更新源分片的键范围
        let source_shard = metadata
            .shards
            .get_mut(&task.source_shard)
            .ok_or_else(|| format!("Source shard {} not found", task.source_shard))?;

        let old_range = source_shard.key_range;
        source_shard.key_range = KeyRange::new(old_range.start, task.split_slot);
        source_shard.clear_split_state();

        // 2. 更新目标分片状态
        let target_shard = metadata
            .shards
            .get_mut(&task.target_shard)
            .ok_or_else(|| format!("Target shard {} not found", task.target_shard))?;

        target_shard.status = ShardStatus::Normal;
        target_shard.clear_split_state();

        // 设置第一个节点为 leader（临时）
        if let Some(first_node) = target_shard.replicas.first().cloned() {
            target_shard.set_leader(first_node);
        }

        // 3. 更新路由表
        // 源分片：[old_start, split_slot)
        metadata
            .routing_table
            .assign_slots(&task.source_shard, old_range.start, task.split_slot);

        // 目标分片：[split_slot, old_end)
        metadata
            .routing_table
            .assign_slots(&task.target_shard, task.split_slot, old_range.end);

        // 设置目标分片的节点
        let target_nodes = metadata
            .shards
            .get(&task.target_shard)
            .map(|s| s.replicas.clone())
            .unwrap_or_default();
        metadata
            .routing_table
            .set_shard_nodes(task.target_shard.clone(), target_nodes);

        // 4. 移除分裂信息
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        // 5. 更新任务状态
        self.update_task_status(task_id, SplitStatus::Completed);

        info!(
            "Split task {} completed: {} [{}..{}) -> {} [{}..{})",
            task_id,
            task.source_shard,
            old_range.start,
            task.split_slot,
            task.target_shard,
            task.split_slot,
            old_range.end
        );

        Ok(())
    }

    /// 取消分裂
    pub async fn cancel_split(&self, task_id: &str) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.is_finished() {
            return Err(format!("Task {} is already finished: {}", task_id, task.status));
        }

        info!("Cancelling split task {}", task_id);

        let mut metadata = self.metadata.write().await;

        // 1. 清除源分片的分裂状态
        if let Some(source_shard) = metadata.shards.get_mut(&task.source_shard) {
            source_shard.clear_split_state();
        }

        // 2. 删除目标分片
        if let Some(target_shard) = metadata.shards.remove(&task.target_shard) {
            // 从节点中移除分片
            for node_id in &target_shard.replicas {
                if let Some(node) = metadata.nodes.get_mut(node_id) {
                    node.remove_shard(&task.target_shard);
                }
            }
        }

        // 3. 从路由表移除分裂信息
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        // 4. 更新任务状态
        self.update_task_status(task_id, SplitStatus::Cancelled);

        info!("Split task {} cancelled", task_id);

        Ok(())
    }

    /// 标记任务失败
    pub async fn fail_split(&self, task_id: &str, reason: String) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.is_finished() {
            return Err(format!("Task {} is already finished: {}", task_id, task.status));
        }

        error!("Split task {} failed: {}", task_id, reason);

        // 清理状态（类似取消）
        let mut metadata = self.metadata.write().await;

        if let Some(source_shard) = metadata.shards.get_mut(&task.source_shard) {
            source_shard.clear_split_state();
        }

        if let Some(target_shard) = metadata.shards.remove(&task.target_shard) {
            for node_id in &target_shard.replicas {
                if let Some(node) = metadata.nodes.get_mut(node_id) {
                    node.remove_shard(&task.target_shard);
                }
            }
        }

        // 从路由表移除分裂信息
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        self.update_task_status(task_id, SplitStatus::Failed(reason));

        Ok(())
    }

    /// 获取配置
    pub fn config(&self) -> &SplitManagerConfig {
        &self.config
    }

    /// 检查是否应该进入缓存模式
    pub fn should_start_buffering(&self, delay: u64) -> bool {
        delay < self.config.catch_up_threshold
    }

    /// 进入缓冲阶段
    pub async fn start_buffering(&self, task_id: &str) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.status != SplitStatus::CatchingUp {
            return Err(format!(
                "Task {} is not in catching_up status: {}",
                task_id, task.status
            ));
        }

        info!("Split task {} entering buffering phase", task_id);

        let mut metadata = self.metadata.write().await;

        // 更新路由表中的分裂阶段
        metadata
            .routing_table
            .update_split_phase(&task.source_shard, SplitPhase::Buffering);

        // 更新任务状态
        drop(metadata);
        self.update_task_status(task_id, SplitStatus::Buffering);

        Ok(())
    }

    /// 进入切换阶段
    pub async fn start_switching(&self, task_id: &str) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.status != SplitStatus::Buffering {
            return Err(format!(
                "Task {} is not in buffering status: {}",
                task_id, task.status
            ));
        }

        info!("Split task {} entering switching phase", task_id);

        let mut metadata = self.metadata.write().await;

        // 更新路由表中的分裂阶段
        metadata
            .routing_table
            .update_split_phase(&task.source_shard, SplitPhase::Switched);

        // 更新任务状态
        drop(metadata);
        self.update_task_status(task_id, SplitStatus::Switching);

        Ok(())
    }

    /// 进入快照传输阶段
    pub fn start_snapshot_transfer(&self, task_id: &str) -> bool {
        self.update_task_status(task_id, SplitStatus::SnapshotTransfer)
    }

    /// 进入增量追赶阶段
    pub fn start_catching_up(&self, task_id: &str) -> bool {
        self.update_task_status(task_id, SplitStatus::CatchingUp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_split_task_creation() {
        let task = SplitTask::new(
            "shard_0001".to_string(),
            "shard_0002".to_string(),
            4096,
        );

        assert!(task.id.starts_with("split_shard_0001_4096_"));
        assert_eq!(task.source_shard, "shard_0001");
        assert_eq!(task.target_shard, "shard_0002");
        assert_eq!(task.split_slot, 4096);
        assert_eq!(task.status, SplitStatus::Preparing);
        assert!(!task.is_finished());
    }

    #[test]
    fn test_split_progress_delay() {
        let mut progress = SplitProgress::default();
        progress.source_last_index = 1000;
        progress.target_applied_index = 950;

        assert_eq!(progress.delay(), 50);
    }
}
