//! Scheduler module
//!
//! Responsible for shard placement strategy, migration task management, and shard splitting

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

/// Scheduler
pub struct Scheduler {
    metadata: Arc<RwLock<ClusterMetadata>>,
    placement: PlacementStrategy,
    migration_manager: MigrationManager,
    split_manager: SplitManager,
}

impl Scheduler {
    /// Create scheduler
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

    /// Execute shard placement
    /// 
    /// Assign nodes to all shards that need replicas
    pub async fn schedule_shard_placement(&self) -> Vec<(ShardId, NodeId)> {
        let mut metadata = self.metadata.write().await;
        let mut assignments = Vec::new();

        // Get online nodes
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

        // Get shards needing replicas
        let shards_needing_replicas: Vec<_> = metadata
            .shards
            .values()
            .filter(|s| !s.is_replica_satisfied())
            .cloned()
            .collect();

        for shard in shards_needing_replicas {
            let needed = shard.replica_factor as usize - shard.replicas.len();
            
            // Select nodes
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

        // Elect leader for shards without leader
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

        // Update shard status
        for shard in metadata.shards.values_mut() {
            if shard.status == ShardStatus::Creating && shard.is_replica_satisfied() {
                shard.status = ShardStatus::Normal;
                info!("Shard {} is now normal", shard.id);
            }
        }

        assignments
    }

    /// Trigger shard migration
    pub async fn migrate_shard(
        &self,
        shard_id: &ShardId,
        from_node: &NodeId,
        to_node: &NodeId,
    ) -> Result<MigrationTask, String> {
        let mut metadata = self.metadata.write().await;

        // Validate
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

        // Create migration task
        let task = self.migration_manager.create_task(
            shard_id.clone(),
            from_node.clone(),
            to_node.clone(),
        );

        // Update shard status
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

        // Add new replica
        metadata.assign_shard_to_node(&task.shard_id, &task.to_node);
        
        // Remove old replica
        if let Some(shard) = metadata.shards.get_mut(&task.shard_id) {
            shard.remove_replica(&task.from_node);
        }
        if let Some(node) = metadata.nodes.get_mut(&task.from_node) {
            node.remove_shard(&task.shard_id);
        }

        // Update routing table
        let replicas = metadata.shards.get(&task.shard_id).map(|s| s.replicas.clone());
        if let Some(replicas) = replicas {
            metadata.routing_table.set_shard_nodes(task.shard_id.clone(), replicas);
        }

        // Restore shard status
        metadata.set_shard_status(&task.shard_id, ShardStatus::Normal);

        // Complete task
        self.migration_manager.complete_task(task_id);

        info!(
            "Completed migration of shard {}: {} -> {}",
            task.shard_id, task.from_node, task.to_node
        );

        Ok(())
    }

    /// Get migration manager
    pub fn migration_manager(&self) -> &MigrationManager {
        &self.migration_manager
    }

    /// Get split manager
    pub fn split_manager(&self) -> &SplitManager {
        &self.split_manager
    }

    /// Trigger shard split
    /// 
    /// # Arguments
    /// - `source_shard_id`: Source shard ID
    /// - `split_slot`: Split point slot
    /// - `target_shard_id`: Target shard ID (must be provided, and target shard must exist and be healthy)
    /// 
    /// # Requirements
    /// - Target shard must be created and in Normal status (healthy status)
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

    /// Rebalance: balance shard distribution
    pub async fn rebalance(&self) -> Vec<MigrationTask> {
        let metadata = self.metadata.read().await;
        let tasks = Vec::new();

        // Calculate load for each node
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

        // TODO: Implement rebalance logic
        // Migrate shards from high-load nodes to low-load nodes

        tasks
    }
}
