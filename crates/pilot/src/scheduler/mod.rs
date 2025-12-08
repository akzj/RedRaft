//! 调度器模块
//!
//! 负责分片放置策略、迁移任务管理和分片分裂

mod placement;
mod migration;
mod split;

pub use placement::PlacementStrategy;
pub use migration::{MigrationTask, MigrationStatus, MigrationManager};
pub use split::{SplitManager, SplitManagerConfig};

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::metadata::{ClusterMetadata, NodeId, NodeStatus, ShardId, ShardStatus, SplitTask};

/// 调度器
pub struct Scheduler {
    metadata: Arc<RwLock<ClusterMetadata>>,
    placement: PlacementStrategy,
    migration_manager: MigrationManager,
    split_manager: SplitManager,
}

impl Scheduler {
    /// 创建调度器
    pub fn new(metadata: Arc<RwLock<ClusterMetadata>>) -> Self {
        let split_manager = SplitManager::new(
            SplitManagerConfig::default(),
            metadata.clone(),
        );
        
        Self {
            metadata,
            placement: PlacementStrategy::default(),
            migration_manager: MigrationManager::new(),
            split_manager,
        }
    }

    /// 执行分片分配
    /// 
    /// 为所有需要副本的分片分配节点
    pub async fn schedule_shard_placement(&self) -> Vec<(ShardId, NodeId)> {
        let mut metadata = self.metadata.write().await;
        let mut assignments = Vec::new();

        // 获取在线节点
        let online_nodes: Vec<_> = metadata
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .cloned()
            .collect();

        if online_nodes.is_empty() {
            warn!("No online nodes available for shard placement");
            return assignments;
        }

        // 获取需要副本的分片
        let shards_needing_replicas: Vec<_> = metadata
            .shards
            .values()
            .filter(|s| !s.is_replica_satisfied())
            .cloned()
            .collect();

        for shard in shards_needing_replicas {
            let needed = shard.replica_factor as usize - shard.replicas.len();
            
            // 选择节点
            let candidates = self.placement.select_nodes(
                &shard,
                &online_nodes,
                needed,
            );

            for node_id in candidates {
                if metadata.assign_shard_to_node(&shard.id, &node_id) {
                    info!("Assigned shard {} to node {}", shard.id, node_id);
                    assignments.push((shard.id.clone(), node_id));
                }
            }
        }

        // 为没有 leader 的分片选举 leader
        let shards_without_leader: Vec<_> = metadata
            .shards
            .values()
            .filter(|s| s.leader.is_none() && !s.replicas.is_empty())
            .cloned()
            .collect();

        for shard in shards_without_leader {
            if let Some(leader) = shard.replicas.first() {
                metadata.set_shard_leader(&shard.id, leader);
                info!("Elected leader {} for shard {}", leader, shard.id);
            }
        }

        // 更新分片状态
        for shard in metadata.shards.values_mut() {
            if shard.status == ShardStatus::Creating && shard.is_replica_satisfied() {
                shard.status = ShardStatus::Normal;
                info!("Shard {} is now normal", shard.id);
            }
        }

        assignments
    }

    /// 触发分片迁移
    pub async fn migrate_shard(
        &self,
        shard_id: &ShardId,
        from_node: &NodeId,
        to_node: &NodeId,
    ) -> Result<MigrationTask, String> {
        let mut metadata = self.metadata.write().await;

        // 验证
        let shard = metadata.shards.get(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;
        
        if !shard.replicas.contains(from_node) {
            return Err(format!("Shard {} is not on node {}", shard_id, from_node));
        }

        if !metadata.nodes.contains_key(to_node) {
            return Err(format!("Target node {} not found", to_node));
        }

        if metadata.nodes.get(to_node).map(|n| n.status) != Some(NodeStatus::Online) {
            return Err(format!("Target node {} is not online", to_node));
        }

        // 创建迁移任务
        let task = self.migration_manager.create_task(
            shard_id.clone(),
            from_node.clone(),
            to_node.clone(),
        );

        // 更新分片状态
        metadata.set_shard_status(shard_id, ShardStatus::Migrating);

        info!(
            "Created migration task for shard {}: {} -> {}",
            shard_id, from_node, to_node
        );

        Ok(task)
    }

    /// 完成迁移
    pub async fn complete_migration(&self, task_id: &str) -> Result<(), String> {
        let task = self.migration_manager.get_task(task_id)
            .ok_or_else(|| format!("Migration task {} not found", task_id))?;

        let mut metadata = self.metadata.write().await;

        // 添加新副本
        metadata.assign_shard_to_node(&task.shard_id, &task.to_node);

        // 移除旧副本
        if let Some(shard) = metadata.shards.get_mut(&task.shard_id) {
            shard.remove_replica(&task.from_node);
        }
        if let Some(node) = metadata.nodes.get_mut(&task.from_node) {
            node.remove_shard(&task.shard_id);
        }

        // 更新路由表
        let replicas = metadata.shards.get(&task.shard_id).map(|s| s.replicas.clone());
        if let Some(replicas) = replicas {
            metadata.routing_table.set_shard_nodes(task.shard_id.clone(), replicas);
        }

        // 恢复分片状态
        metadata.set_shard_status(&task.shard_id, ShardStatus::Normal);

        // 完成任务
        self.migration_manager.complete_task(task_id);

        info!(
            "Completed migration of shard {}: {} -> {}",
            task.shard_id, task.from_node, task.to_node
        );

        Ok(())
    }

    /// 获取迁移管理器
    pub fn migration_manager(&self) -> &MigrationManager {
        &self.migration_manager
    }

    /// 获取分裂管理器
    pub fn split_manager(&self) -> &SplitManager {
        &self.split_manager
    }

    /// 触发分片分裂
    ///
    /// # 参数
    /// - `source_shard_id`: 源分片 ID
    /// - `split_slot`: 分裂点槽位
    /// - `target_shard_id`: 目标分片 ID（必须提供，且目标分片必须已存在且健康）
    ///
    /// # 要求
    /// - 目标分片必须已创建且状态为 Normal（健康状态）
    pub async fn split_shard(
        &self,
        source_shard_id: &ShardId,
        split_slot: u32,
        target_shard_id: String,
    ) -> Result<SplitTask, String> {
        self.split_manager
            .trigger_split(source_shard_id, split_slot, target_shard_id)
            .await
    }

    /// Rebalance：均衡分片分布
    pub async fn rebalance(&self) -> Vec<MigrationTask> {
        let metadata = self.metadata.read().await;
        let tasks = Vec::new();

        // 计算每个节点的负载
        let mut node_loads: Vec<_> = metadata
            .nodes
            .values()
            .filter(|n| n.status == NodeStatus::Online)
            .map(|n| (n.id.clone(), n.hosted_shards.len()))
            .collect();

        if node_loads.len() < 2 {
            return tasks;
        }

        node_loads.sort_by_key(|(_, load)| *load);

        let avg_load = metadata.shards.len() / node_loads.len();
        let _max_load = avg_load + 1;
        let _min_load = avg_load.saturating_sub(1);

        // TODO: 实现 rebalance 逻辑
        // 从负载高的节点迁移分片到负载低的节点

        tasks
    }
}
