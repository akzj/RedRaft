//! Split Service implementation
//!
//! Implements the gRPC service for shard splitting operations.
//! Supports starting, tracking, canceling, and completing split operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::node::RRNode;
use proto::split_service::{
    split_service_server::SplitService, CancelSplitRequest, CancelSplitResponse,
    CompleteSplitRequest, CompleteSplitResponse, GetSplitProgressRequest,
    GetSplitProgressResponse, SplitPhase, SplitProgress, SplitStatus, StartSplitRequest,
    StartSplitResponse,
};

/// Split task state
#[derive(Debug, Clone)]
pub struct SplitTask {
    /// Task ID
    pub task_id: String,
    /// Source shard ID
    pub source_shard_id: String,
    /// Target shard IDs
    pub target_shard_ids: Vec<String>,
    /// Split slot
    pub split_slot: u32,
    /// Source slot range
    pub source_slot_start: u32,
    pub source_slot_end: u32,
    /// Target nodes
    pub target_nodes: Vec<String>,
    /// Split status (as i32)
    pub status: i32,
    /// Split phase (as i32)
    pub phase: i32,
    /// Progress information
    pub progress: SplitProgress,
    /// Created at timestamp (seconds since epoch)
    pub created_at: u64,
    /// Updated at timestamp (seconds since epoch)
    pub updated_at: u64,
    /// Error message (if failed)
    pub error_message: String,
}

impl SplitTask {
    fn new(
        task_id: String,
        source_shard_id: String,
        target_shard_ids: Vec<String>,
        split_slot: u32,
        source_slot_start: u32,
        source_slot_end: u32,
        target_nodes: Vec<String>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            task_id,
            source_shard_id,
            target_shard_ids,
            split_slot,
            source_slot_start,
            source_slot_end,
            target_nodes,
            status: SplitStatus::Preparing as i32,
            phase: SplitPhase::Preparing as i32,
            progress: SplitProgress {
                current_phase: SplitPhase::Preparing as i32,
                snapshot_progress_percent: 0,
                log_replay_progress_percent: 0,
                bytes_transferred: 0,
                bytes_total: 0,
                keys_transferred: 0,
                keys_total: 0,
                current_replay_index: 0,
                target_replay_index: 0,
                estimated_seconds_remaining: 0,
            },
            created_at: now,
            updated_at: now,
            error_message: String::new(),
        }
    }

    fn update_timestamp(&mut self) {
        self.updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

/// Split task manager
pub struct SplitTaskManager {
    /// Tasks: task_id -> SplitTask
    tasks: Arc<RwLock<HashMap<String, SplitTask>>>,
}

impl SplitTaskManager {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_task(
        &self,
        task_id: String,
        source_shard_id: String,
        target_shard_ids: Vec<String>,
        split_slot: u32,
        source_slot_start: u32,
        source_slot_end: u32,
        target_nodes: Vec<String>,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if tasks.contains_key(&task_id) {
            return Err(format!("Split task {} already exists", task_id));
        }

        let task = SplitTask::new(
            task_id.clone(),
            source_shard_id,
            target_shard_ids,
            split_slot,
            source_slot_start,
            source_slot_end,
            target_nodes,
        );

        tasks.insert(task_id, task);
        Ok(())
    }

    pub fn get_task(&self, task_id: &str) -> Option<SplitTask> {
        self.tasks.read().get(task_id).cloned()
    }

    pub fn update_task_status(
        &self,
        task_id: &str,
        status: SplitStatus,
        phase: SplitPhase,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = status as i32;
            task.phase = phase as i32;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Split task {} not found", task_id))
        }
    }

    pub fn update_task_progress(
        &self,
        task_id: &str,
        progress: SplitProgress,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.progress = progress;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Split task {} not found", task_id))
        }
    }

    pub fn set_task_error(&self, task_id: &str, error_message: String) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.error_message = error_message;
            task.status = SplitStatus::Failed as i32;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Split task {} not found", task_id))
        }
    }

    pub fn remove_task(&self, task_id: &str) -> bool {
        self.tasks.write().remove(task_id).is_some()
    }
}

impl Default for SplitTaskManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Split Service implementation
pub struct SplitServiceImpl {
    /// Node reference
    node: Arc<RRNode>,
    /// Split task manager
    task_manager: Arc<SplitTaskManager>,
}

impl SplitServiceImpl {
    pub fn new(node: Arc<RRNode>) -> Self {
        Self {
            node,
            task_manager: Arc::new(SplitTaskManager::new()),
        }
    }

    /// Get task manager reference
    pub fn task_manager(&self) -> &Arc<SplitTaskManager> {
        &self.task_manager
    }
}

#[tonic::async_trait]
impl SplitService for SplitServiceImpl {
    async fn start_split(
        &self,
        request: Request<StartSplitRequest>,
    ) -> Result<Response<StartSplitResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.split_task_id.clone();

        info!(
            "StartSplit request: task_id={}, source_shard={}, target_shards={:?}, split_slot={}",
            task_id, req.source_shard_id, req.target_shard_ids, req.split_slot
        );

        // Validate request
        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }
        if req.source_shard_id.is_empty() {
            return Err(Status::invalid_argument("source_shard_id is required"));
        }
        if req.target_shard_ids.is_empty() {
            return Err(Status::invalid_argument("target_shard_ids is required"));
        }

        // Check if source shard exists
        let source_shard_exists = self.node.get_raft_group(&req.source_shard_id).is_some();

        if !source_shard_exists {
            return Ok(Response::new(StartSplitResponse {
                split_task_id: task_id,
                success: false,
                error_message: format!("Source shard {} not found", req.source_shard_id),
            }));
        }

        // Create split task
        match self.task_manager.create_task(
            task_id.clone(),
            req.source_shard_id,
            req.target_shard_ids,
            req.split_slot,
            req.source_slot_start,
            req.source_slot_end,
            req.target_nodes,
        ) {
            Ok(()) => {
                info!("Created split task: {}", task_id);
                // TODO: Start actual split operation in background
                // This would involve:
                // 1. Creating target shards
                // 2. Creating snapshot with slot range filter
                // 3. Transferring snapshot to target shards
                // 4. Replaying incremental logs
                // 5. Updating routing table
                // 6. Completing split

                Ok(Response::new(StartSplitResponse {
                    split_task_id: task_id,
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!("Failed to create split task {}: {}", task_id, e);
                Ok(Response::new(StartSplitResponse {
                    split_task_id: task_id,
                    success: false,
                    error_message: e,
                }))
            }
        }
    }

    async fn get_split_progress(
        &self,
        request: Request<GetSplitProgressRequest>,
    ) -> Result<Response<GetSplitProgressResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.split_task_id.clone();

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        match self.task_manager.get_task(&task_id) {
            Some(task) => {
                // TODO: Update progress from actual split operation state
                // This would query:
                // - Snapshot transfer progress
                // - Log replay progress
                // - Current Raft indices

                Ok(Response::new(GetSplitProgressResponse {
                    split_task_id: task.task_id.clone(),
                    status: task.status,
                    phase: task.phase,
                    progress: Some(task.progress),
                    error_message: task.error_message.clone(),
                }))
            }
            None => {
                let task_id_clone = task_id.clone();
                Ok(Response::new(GetSplitProgressResponse {
                    split_task_id: task_id,
                    status: SplitStatus::NotFound as i32,
                    phase: SplitPhase::Unspecified as i32,
                    progress: None,
                    error_message: format!("Split task {} not found", task_id_clone),
                }))
            }
        }
    }

    async fn cancel_split(
        &self,
        request: Request<CancelSplitRequest>,
    ) -> Result<Response<CancelSplitResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.split_task_id.clone();

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        info!("CancelSplit request: task_id={}, rollback={}", task_id, req.rollback);

        match self.task_manager.get_task(&task_id) {
            Some(task) => {
                // Check if task can be cancelled
                let can_cancel = matches!(
                    task.status,
                    x if x == SplitStatus::Preparing as i32
                        || x == SplitStatus::InProgress as i32
                );

                if !can_cancel {
                    let task_id_clone = task_id.clone();
                    return Ok(Response::new(CancelSplitResponse {
                        split_task_id: task_id,
                        success: false,
                        error_message: format!(
                            "Split task {} cannot be cancelled in current status",
                            task_id_clone
                        ),
                    }));
                }

                // Update task status
                // Keep current phase (stored as i32, convert back to enum)
                let current_phase = match task.phase {
                    x if x == SplitPhase::Preparing as i32 => SplitPhase::Preparing,
                    x if x == SplitPhase::SnapshotTransfer as i32 => SplitPhase::SnapshotTransfer,
                    x if x == SplitPhase::LogReplay as i32 => SplitPhase::LogReplay,
                    x if x == SplitPhase::Switching as i32 => SplitPhase::Switching,
                    x if x == SplitPhase::Completing as i32 => SplitPhase::Completing,
                    _ => SplitPhase::Unspecified,
                };
                if let Err(e) = self.task_manager.update_task_status(
                    &task_id,
                    SplitStatus::Cancelled,
                    current_phase,
                ) {
                    return Ok(Response::new(CancelSplitResponse {
                        split_task_id: task_id,
                        success: false,
                        error_message: e,
                    }));
                }

                // TODO: Perform rollback if requested
                // This would involve:
                // 1. Stopping ongoing split operations
                // 2. Cleaning up target shards if rollback=true
                // 3. Removing split state from routing table
                // 4. Restoring source shard to normal state

                if req.rollback {
                    info!("Rollback requested for split task: {}", task_id);
                    // TODO: Implement rollback logic
                }

                Ok(Response::new(CancelSplitResponse {
                    split_task_id: task_id,
                    success: true,
                    error_message: String::new(),
                }))
            }
            None => {
                let task_id_clone = task_id.clone();
                Ok(Response::new(CancelSplitResponse {
                    split_task_id: task_id,
                    success: false,
                    error_message: format!("Split task {} not found", task_id_clone),
                }))
            }
        }
    }

    async fn complete_split(
        &self,
        request: Request<CompleteSplitRequest>,
    ) -> Result<Response<CompleteSplitResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.split_task_id.clone();

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        info!("CompleteSplit request: task_id={}", task_id);

        match self.task_manager.get_task(&task_id) {
            Some(task) => {
                // Check if task can be completed
                let can_complete = matches!(
                    task.status,
                    x if x == SplitStatus::InProgress as i32
                        || x == SplitStatus::Preparing as i32
                );

                if !can_complete {
                    let task_id_clone = task_id.clone();
                    return Ok(Response::new(CompleteSplitResponse {
                        split_task_id: task_id,
                        success: false,
                        error_message: format!(
                            "Split task {} cannot be completed in current status",
                            task_id_clone
                        ),
                    }));
                }

                // Update task status
                if let Err(e) = self.task_manager.update_task_status(
                    &task_id,
                    SplitStatus::Completed,
                    SplitPhase::Completing,
                ) {
                    return Ok(Response::new(CompleteSplitResponse {
                        split_task_id: task_id.clone(),
                        success: false,
                        error_message: e,
                    }));
                }

                // TODO: Perform finalization
                // This would involve:
                // 1. Finalizing routing table updates
                // 2. Cleaning up temporary resources
                // 3. Removing split state from shards
                // 4. Notifying other nodes

                Ok(Response::new(CompleteSplitResponse {
                    split_task_id: task_id,
                    success: true,
                    error_message: String::new(),
                }))
            }
            None => {
                let task_id_clone = task_id.clone();
                Ok(Response::new(CompleteSplitResponse {
                    split_task_id: task_id,
                    success: false,
                    error_message: format!("Split task {} not found", task_id_clone),
                }))
            }
        }
    }
}

