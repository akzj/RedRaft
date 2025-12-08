//! Migration task management

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::metadata::{NodeId, ShardId};

/// Migration status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Pending execution
    Pending,
    /// Preparing (target node preparing to receive)
    Preparing,
    /// In progress (data transfer)
    InProgress,
    /// Completed
    Completed,
    /// Failed
    Failed,
    /// Cancelled
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

/// Migration task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationTask {
    /// Task ID
    pub id: String,
    /// Shard ID
    pub shard_id: ShardId,
    /// Source node
    pub from_node: NodeId,
    /// Target node
    pub to_node: NodeId,
    /// Task status
    pub status: MigrationStatus,
    /// Creation time
    pub created_at: DateTime<Utc>,
    /// Start time
    pub started_at: Option<DateTime<Utc>>,
    /// Completion time
    pub completed_at: Option<DateTime<Utc>>,
    /// Progress (0-100)
    pub progress: u8,
    /// Error message
    pub error: Option<String>,
}

impl MigrationTask {
    /// Create new task
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

    /// Start migration
    pub fn start(&mut self) {
        self.status = MigrationStatus::InProgress;
        self.started_at = Some(Utc::now());
    }

    /// Update progress
    pub fn update_progress(&mut self, progress: u8) {
        self.progress = progress.min(100);
    }

    /// Complete migration
    pub fn complete(&mut self) {
        self.status = MigrationStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.progress = 100;
    }

    /// Migration failed
    pub fn fail(&mut self, error: String) {
        self.status = MigrationStatus::Failed;
        self.completed_at = Some(Utc::now());
        self.error = Some(error);
    }

    /// Cancel migration
    pub fn cancel(&mut self) {
        self.status = MigrationStatus::Cancelled;
        self.completed_at = Some(Utc::now());
    }

    /// Check if completed (including failed and cancelled)
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            MigrationStatus::Completed | MigrationStatus::Failed | MigrationStatus::Cancelled
        )
    }
}

/// Migration manager
pub struct MigrationManager {
    /// All migration tasks
    tasks: RwLock<HashMap<String, MigrationTask>>,
}

impl MigrationManager {
    /// Create migration manager
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
        }
    }

    /// Create migration task
    pub fn create_task(&self, shard_id: ShardId, from_node: NodeId, to_node: NodeId) -> MigrationTask {
        let task = MigrationTask::new(shard_id, from_node, to_node);
        let task_clone = task.clone();
        self.tasks.write().insert(task.id.clone(), task);
        task_clone
    }

    /// Get task
    pub fn get_task(&self, task_id: &str) -> Option<MigrationTask> {
        self.tasks.read().get(task_id).cloned()
    }

    /// Get active migration task for shard
    pub fn get_active_task_for_shard(&self, shard_id: &ShardId) -> Option<MigrationTask> {
        self.tasks
            .read()
            .values()
            .find(|t| t.shard_id == *shard_id && !t.is_finished())
            .cloned()
    }

    /// Get all active tasks
    pub fn active_tasks(&self) -> Vec<MigrationTask> {
        self.tasks
            .read()
            .values()
            .filter(|t| !t.is_finished())
            .cloned()
            .collect()
    }

    /// Get all tasks
    pub fn all_tasks(&self) -> Vec<MigrationTask> {
        self.tasks.read().values().cloned().collect()
    }

    /// Start task
    pub fn start_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.start();
            true
        } else {
            false
        }
    }

    /// Update task progress
    pub fn update_progress(&self, task_id: &str, progress: u8) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.update_progress(progress);
            true
        } else {
            false
        }
    }

    /// Complete task
    pub fn complete_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.complete();
            true
        } else {
            false
        }
    }

    /// Task failed
    pub fn fail_task(&self, task_id: &str, error: String) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.fail(error);
            true
        } else {
            false
        }
    }

    /// Cancel task
    pub fn cancel_task(&self, task_id: &str) -> bool {
        if let Some(task) = self.tasks.write().get_mut(task_id) {
            task.cancel();
            true
        } else {
            false
        }
    }

    /// Clean up completed tasks (keep most recent N)
    pub fn cleanup(&self, keep_count: usize) {
        let mut tasks = self.tasks.write();
        
        // Get completed tasks and sort by completion time
        let mut finished: Vec<_> = tasks
            .values()
            .filter(|t| t.is_finished())
            .cloned()
            .collect();
        
        finished.sort_by(|a, b| {
            b.completed_at.cmp(&a.completed_at)
        });

        // Remove tasks beyond keep count
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

/// Generate random suffix
fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{:08x}", nanos)
}
