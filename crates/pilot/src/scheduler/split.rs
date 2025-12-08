//! Shard split manager
//!
//! Responsible for coordinating the complete shard split process:
//! 1. Preparation phase - Create target shard
//! 2. Snapshot transfer - Transfer historical data
//! 3. Incremental catch-up - Replay incremental logs
//! 4. Buffer switch - Buffer requests and switch routing
//! 5. Cleanup phase - Clean up old data

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;
use tracing::{debug, error, info, warn};

use crate::metadata::{
    ClusterMetadata, KeyRange, ShardId, ShardSplitState, ShardStatus,
    SplitPhase, SplitProgress, SplitRole, SplitStatus, SplitTask, SplittingShardInfo,
};

/// Split manager configuration
#[derive(Debug, Clone)]
pub struct SplitManagerConfig {
    /// Catch-up threshold - enter buffer mode when delay is below this value
    pub catch_up_threshold: u64,
    /// Buffer timeout (seconds)
    pub buffer_timeout_secs: u64,
    /// Maximum buffer request count
    pub buffer_max_size: usize,
    /// Progress report interval (seconds)
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

/// Split manager
pub struct SplitManager {
    /// Configuration
    config: SplitManagerConfig,
    /// Active split tasks (task_id -> task)
    tasks: RwLock<HashMap<String, SplitTask>>,
    /// Cluster metadata
    metadata: Arc<AsyncRwLock<ClusterMetadata>>,
}

impl SplitManager {
    /// Create split manager
    pub fn new(config: SplitManagerConfig, metadata: Arc<AsyncRwLock<ClusterMetadata>>) -> Self {
        Self {
            config,
            tasks: RwLock::new(HashMap::new()),
            metadata,
        }
    }

    /// Trigger shard split
    /// 
    /// # Arguments
    /// - `source_shard_id`: Source shard ID
    /// - `split_slot`: Split point slot
    /// - `target_shard_id`: Target shard ID (must be provided, and target shard must exist and be healthy)
    /// 
    /// # Requirements
    /// - Source shard must exist and be in Normal status
    /// - Target shard must exist and be in Normal status (healthy: has leader and satisfies replica factor)
    /// - Split point must be within source shard range
    /// - Target shard's key_range must match split point: [split_slot, source_shard.end)
    pub async fn trigger_split(
        &self,
        source_shard_id: &ShardId,
        split_slot: u32,
        target_shard_id: ShardId,
    ) -> Result<SplitTask, String> {
        let mut metadata = self.metadata.write().await;

        // 1. Verify source shard exists and is in normal status
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

        // 2. Verify split point is within source shard range
        let key_range = source_shard.key_range;
        if split_slot <= key_range.start || split_slot >= key_range.end {
            return Err(format!(
                "Split slot {} must be within range ({}, {})",
                split_slot, key_range.start, key_range.end
            ));
        }

        // 3. Verify target shard exists
        let target_shard = metadata
            .shards
            .get(&target_shard_id)
            .ok_or_else(|| format!("Target shard {} not found. Target shard must be created and healthy before split", target_shard_id))?;

        // 4. Verify target shard status is Normal (healthy status)
        if target_shard.status != ShardStatus::Normal {
            return Err(format!(
                "Target shard {} is not in normal status: {}. Target shard must be healthy before split",
                target_shard_id, target_shard.status
            ));
        }

        // 5. Verify target shard is healthy (has leader and satisfies replica factor)
        if !target_shard.is_healthy() {
            return Err(format!(
                "Target shard {} is not healthy. It must have a leader and satisfy replica factor before split",
                target_shard_id
            ));
        }

        // 6. Verify target shard is not splitting
        if target_shard.is_splitting() {
            return Err(format!(
                "Target shard {} is already splitting",
                target_shard_id
            ));
        }

        // 7. Verify target shard's key_range matches split point
        let expected_target_range = KeyRange::new(split_slot, key_range.end);
        if target_shard.key_range != expected_target_range {
            return Err(format!(
                "Target shard {} key_range {:?} does not match expected range {:?} for split at slot {}",
                target_shard_id, target_shard.key_range, expected_target_range, split_slot
            ));
        }

        // 8. Create split task
        let task = SplitTask::new(
            source_shard_id.clone(),
            target_shard_id.clone(),
            split_slot,
        );

        info!(
            "Creating split task {}: {} -> {} at slot {}",
            task.id, source_shard_id, target_shard_id, split_slot
        );

        // 9. Update target shard's split state
        let target_shard = metadata.shards.get_mut(&target_shard_id).unwrap();
        target_shard.set_split_state(ShardSplitState {
            split_task_id: task.id.clone(),
            split_slot,
            role: SplitRole::Target,
        });
        let target_nodes = target_shard.replicas.clone();

        // 10. Update source shard state
        let source_shard = metadata.shards.get_mut(source_shard_id).unwrap();
        source_shard.set_split_state(ShardSplitState {
            split_task_id: task.id.clone(),
            split_slot,
            role: SplitRole::Source,
        });

        // 11. Add split information to routing table (for Node to detect split status)
        let splitting_info = SplittingShardInfo {
            source_shard: source_shard_id.clone(),
            target_shard: target_shard_id.clone(),
            split_slot,
            source_range: key_range,
            target_nodes: target_nodes.clone(),
            phase: SplitPhase::Preparing,
        };
        metadata.routing_table.add_splitting_shard(splitting_info);

        // 12. Store task
        let task_clone = task.clone();
        self.tasks.write().insert(task.id.clone(), task);

        info!(
            "Split task {} created successfully, using existing healthy target shard {}",
            task_clone.id, target_shard_id
        );

        Ok(task_clone)
    }

    /// Get split task
    pub fn get_task(&self, task_id: &str) -> Option<SplitTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// Get all split tasks
    pub fn all_tasks(&self) -> Vec<SplitTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// Get active split tasks
    pub fn active_tasks(&self) -> Vec<SplitTask> {
        self.tasks
            .read()
            .values()
            .filter(|t| !t.is_finished())
            .cloned()
            .collect()
    }

    /// Update task status
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

    /// Update task progress
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

    /// Complete split - update routing table
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

        // 1. Update source shard's key range
        let source_shard = metadata
            .shards
            .get_mut(&task.source_shard)
            .ok_or_else(|| format!("Source shard {} not found", task.source_shard))?;

        let old_range = source_shard.key_range;
        source_shard.key_range = KeyRange::new(old_range.start, task.split_slot);
        source_shard.clear_split_state();

        // 2. Update target shard status
        let target_shard = metadata
            .shards
            .get_mut(&task.target_shard)
            .ok_or_else(|| format!("Target shard {} not found", task.target_shard))?;

        target_shard.status = ShardStatus::Normal;
        target_shard.clear_split_state();

        // Set first node as leader (temporary)
        if let Some(first_node) = target_shard.replicas.first().cloned() {
            target_shard.set_leader(first_node);
        }

        // 3. Update routing table
        // Source shard: [old_start, split_slot)
        metadata
            .routing_table
            .assign_slots(&task.source_shard, old_range.start, task.split_slot);

        // Target shard: [split_slot, old_end)
        metadata
            .routing_table
            .assign_slots(&task.target_shard, task.split_slot, old_range.end);

        // Set target shard's nodes
        let target_nodes = metadata
            .shards
            .get(&task.target_shard)
            .map(|s| s.replicas.clone())
            .unwrap_or_default();
        metadata
            .routing_table
            .set_shard_nodes(task.target_shard.clone(), target_nodes);

        // 4. Remove split information
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        // 5. Update task status
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

    /// Cancel split
    pub async fn cancel_split(&self, task_id: &str) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.is_finished() {
            return Err(format!("Task {} is already finished: {}", task_id, task.status));
        }

        info!("Cancelling split task {}", task_id);

        let mut metadata = self.metadata.write().await;

        // 1. Clear source shard's split state
        if let Some(source_shard) = metadata.shards.get_mut(&task.source_shard) {
            source_shard.clear_split_state();
        }

        // 2. Remove target shard
        if let Some(target_shard) = metadata.shards.remove(&task.target_shard) {
            // Remove shard from nodes
            for node_id in &target_shard.replicas {
                if let Some(node) = metadata.nodes.get_mut(node_id) {
                    node.remove_shard(&task.target_shard);
                }
            }
        }

        // 3. Remove split information from routing table
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        // 4. Update task status
        self.update_task_status(task_id, SplitStatus::Cancelled);

        info!("Split task {} cancelled", task_id);

        Ok(())
    }

    /// Mark task as failed
    pub async fn fail_split(&self, task_id: &str, reason: String) -> Result<(), String> {
        let task = self
            .get_task(task_id)
            .ok_or_else(|| format!("Task {} not found", task_id))?;

        if task.is_finished() {
            return Err(format!("Task {} is already finished: {}", task_id, task.status));
        }

        error!("Split task {} failed: {}", task_id, reason);

        // Clean up state (similar to cancel)
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

        // Remove split information from routing table
        metadata.routing_table.remove_splitting_shard(&task.source_shard);

        self.update_task_status(task_id, SplitStatus::Failed(reason));

        Ok(())
    }

    /// Get configuration
    pub fn config(&self) -> &SplitManagerConfig {
        &self.config
    }

    /// Check if should enter buffer mode
    pub fn should_start_buffering(&self, delay: u64) -> bool {
        delay < self.config.catch_up_threshold
    }

    /// Enter buffering phase
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

        // Update split phase in routing table
        metadata
            .routing_table
            .update_split_phase(&task.source_shard, SplitPhase::Buffering);

        // Update task status
        drop(metadata);
        self.update_task_status(task_id, SplitStatus::Buffering);

        Ok(())
    }

    /// Enter switching phase
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

        // Update split phase in routing table
        metadata
            .routing_table
            .update_split_phase(&task.source_shard, SplitPhase::Switched);

        // Update task status
        drop(metadata);
        self.update_task_status(task_id, SplitStatus::Switching);

        Ok(())
    }

    /// Enter snapshot transfer phase
    pub fn start_snapshot_transfer(&self, task_id: &str) -> bool {
        self.update_task_status(task_id, SplitStatus::SnapshotTransfer)
    }

    /// Enter incremental catch-up phase
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
