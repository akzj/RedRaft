//! Split Service implementation
//!
//! Implements the gRPC service for shard splitting operations.
//! Supports starting, tracking, canceling, and completing split operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::node::RRNode;
use crate::snapshot_transfer::{self, SnapshotTransferManager};
use proto::split_service::{
    split_service_server::SplitService, CancelSplitRequest, CancelSplitResponse,
    CompleteSplitRequest, CompleteSplitResponse, GetSplitProgressRequest,
    GetSplitProgressResponse, SplitPhase, SplitProgress, SplitStatus, StartSplitRequest,
    StartSplitResponse,
};
use rr_core::shard::ShardRouting;

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
    /// Snapshot transfer manager
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
}

impl SplitServiceImpl {
    pub fn new(
        node: Arc<RRNode>,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    ) -> Self {
        Self {
            node,
            task_manager: Arc::new(SplitTaskManager::new()),
            snapshot_transfer_manager,
        }
    }

    /// Get task manager reference
    pub fn task_manager(&self) -> &Arc<SplitTaskManager> {
        &self.task_manager
    }

    /// Execute split task in background
    async fn execute_split_task(
        node: Arc<RRNode>,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
        task_manager: Arc<SplitTaskManager>,
        task_id: String,
        source_shard_id: String,
        target_shard_ids: Vec<String>,
        split_slot: u32,
        source_slot_start: u32,
        source_slot_end: u32,
        target_nodes: Vec<String>,
    ) {
        info!(
            "Starting split task {}: source_shard={}, target_shards={:?}, split_slot={}",
            task_id, source_shard_id, target_shard_ids, split_slot
        );

        // Phase 1: Preparing - Create target shards
        if let Err(e) = task_manager.update_task_status(
            &task_id,
            SplitStatus::InProgress,
            SplitPhase::Preparing,
        ) {
            error!("Failed to update split task status: {}", e);
            return;
        }

        if let Err(e) = Self::create_target_shards(&node, &task_manager, &task_id, &target_shard_ids, &target_nodes).await {
            error!("Failed to create target shards for task {}: {}", task_id, e);
            let _ = task_manager.set_task_error(&task_id, e);
            return;
        }

        // Phase 2: Snapshot Transfer - Create and transfer snapshot
        if let Err(e) = task_manager.update_task_status(
            &task_id,
            SplitStatus::InProgress,
            SplitPhase::SnapshotTransfer,
        ) {
            error!("Failed to update split task phase: {}", e);
        }

        // Calculate slot ranges for each target shard
        // For simplicity, we'll split the range [split_slot, source_slot_end) evenly
        // In production, this should be more sophisticated
        let slot_range_per_shard = (source_slot_end - split_slot) / target_shard_ids.len() as u32;
        let mut current_slot = split_slot;

        for (idx, target_shard_id) in target_shard_ids.iter().enumerate() {
            let target_slot_start = current_slot;
            let target_slot_end = if idx == target_shard_ids.len() - 1 {
                source_slot_end
            } else {
                current_slot + slot_range_per_shard
            };

            if let Err(e) = Self::create_and_transfer_snapshot(
                &node,
                &snapshot_transfer_manager,
                &task_manager,
                &task_id,
                &source_shard_id,
                target_shard_id,
                target_slot_start,
                target_slot_end,
            )
            .await
            {
                error!(
                    "Failed to transfer snapshot to target shard {} for task {}: {}",
                    target_shard_id, task_id, e
                );
                let _ = task_manager.set_task_error(&task_id, e);
                return;
            }

            current_slot = target_slot_end;
        }

        // Phase 3: Log Replay - Replay incremental logs
        if let Err(e) = task_manager.update_task_status(
            &task_id,
            SplitStatus::InProgress,
            SplitPhase::LogReplay,
        ) {
            error!("Failed to update split task phase: {}", e);
        }

        // TODO: Implement log replay
        // This would involve:
        // - Getting current Raft index from source shard
        // - Replaying logs from snapshot index to current index
        // - Applying logs to target shards
        info!("Log replay phase for task {} (TODO: implement)", task_id);

        // Phase 4: Switching - Update routing table
        if let Err(e) = task_manager.update_task_status(
            &task_id,
            SplitStatus::InProgress,
            SplitPhase::Switching,
        ) {
            error!("Failed to update split task phase: {}", e);
        }

        if let Err(e) = Self::update_routing_table(
            &node,
            &task_manager,
            &task_id,
            &source_shard_id,
            &target_shard_ids,
            split_slot,
            source_slot_start,
            source_slot_end,
        )
        .await
        {
            error!("Failed to update routing table for task {}: {}", task_id, e);
            let _ = task_manager.set_task_error(&task_id, e);
            return;
        }

        // Phase 5: Completing - Mark as completed
        if let Err(e) = task_manager.update_task_status(
            &task_id,
            SplitStatus::Completed,
            SplitPhase::Completing,
        ) {
            error!("Failed to mark split task as completed: {}", e);
        } else {
            info!("Split task {} completed successfully", task_id);
        }
    }

    /// Create target shards (Raft groups)
    async fn create_target_shards(
        node: &Arc<RRNode>,
        task_manager: &Arc<SplitTaskManager>,
        task_id: &str,
        target_shard_ids: &[String],
        target_nodes: &[String],
    ) -> Result<(), String> {
        info!(
            "Creating target shards for task {}: {:?}",
            task_id, target_shard_ids
        );

        for target_shard_id in target_shard_ids {
            // Check if shard already exists
            if node.get_raft_group(target_shard_id).is_some() {
                warn!("Target shard {} already exists, skipping creation", target_shard_id);
                continue;
            }

            // Create Raft group for target shard
            match node.create_raft_group(target_shard_id.clone(), target_nodes.to_vec()).await {
                Ok(_) => {
                    info!("Created target shard {} for task {}", target_shard_id, task_id);
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to create target shard {}: {}",
                        target_shard_id, e
                    ));
                }
            }
        }

        Ok(())
    }

    /// Create snapshot with slot range filter and transfer to target shard
    async fn create_and_transfer_snapshot(
        node: &Arc<RRNode>,
        snapshot_transfer_manager: &Arc<SnapshotTransferManager>,
        task_manager: &Arc<SplitTaskManager>,
        task_id: &str,
        source_shard_id: &str,
        target_shard_id: &str,
        slot_start: u32,
        slot_end: u32,
    ) -> Result<(), String> {
        info!(
            "Creating and transferring snapshot for task {}: source={}, target={}, slot_range=[{}, {})",
            task_id, source_shard_id, target_shard_id, slot_start, slot_end
        );

        // Get source shard RaftId (for validation)
        let _source_raft_id = node
            .get_raft_group(source_shard_id)
            .ok_or_else(|| format!("Source shard {} not found", source_shard_id))?;

        // Get source state machine and store
        let store = {
            let state_machines = node.state_machines.lock();
            let source_state_machine = state_machines
                .get(source_shard_id)
                .ok_or_else(|| format!("Source state machine {} not found", source_shard_id))?;
            source_state_machine.store().clone()
        };

        // Generate transfer_id for this snapshot transfer
        let transfer_id = Uuid::new_v4().to_string();
        info!(
            "Creating snapshot file for split task {}: transfer_id={}, source={}, target={}, slot_range=[{}, {})",
            task_id, transfer_id, source_shard_id, target_shard_id, slot_start, slot_end
        );

        // Get snapshot config from node (clone to avoid holding reference)
        let snapshot_config = {
            let config = node.config();
            config.snapshot.clone()
        };

        // Create snapshot directory and file path
        let snapshot_dir = snapshot_config.transfer_dir.join(&transfer_id);
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            format!("Failed to create snapshot directory for transfer {}: {}", transfer_id, e)
        })?;
        let snapshot_file = snapshot_dir.join("snapshot.dat");

        // Create chunk index
        let chunk_index = Arc::new(parking_lot::RwLock::new(
            snapshot_transfer::ChunkIndex::new()
        ));

        // Create channel for receiving snapshot data
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        // Create snapshot with slot range filter
        let shard_id_str = source_shard_id.to_string();
        let key_range = Some((slot_start, slot_end));

        use storage::SnapshotStore;
        store
            .create_snapshot(&shard_id_str, tx, key_range)
            .await
            .map_err(|e| format!("Failed to create snapshot: {}", e))?;

        // Register transfer as active
        snapshot_transfer_manager.register_transfer(
            transfer_id.clone(),
            snapshot_transfer::SnapshotTransferState::new(
                snapshot_file.clone(),
                chunk_index.clone(),
            ),
        );

        // Spawn background task to generate snapshot file from channel
        let transfer_id_clone = transfer_id.clone();
        let source_shard_id_clone = source_shard_id.to_string();
        let task_id_clone = task_id.to_string();
        let snapshot_transfer_manager_clone = snapshot_transfer_manager.clone();
        let snapshot_config_clone = snapshot_config.clone();

        tokio::spawn(async move {
            // Generate snapshot file in background using channel
            if let Err(e) = snapshot_transfer::generate_snapshot_file_async(
                &source_shard_id_clone,
                &transfer_id_clone,
                &snapshot_transfer_manager_clone,
                rx,
                snapshot_config_clone,
            )
            .await
            {
                error!(
                    "Failed to generate snapshot file for split task {} transfer {}: {}",
                    task_id_clone, transfer_id_clone, e
                );
                snapshot_transfer_manager_clone.mark_transfer_failed(&transfer_id_clone, e);
            } else {
                info!(
                    "Snapshot file generated successfully for split task {} transfer {}",
                    task_id_clone, transfer_id_clone
                );
            }
        });

        // Wait for snapshot file generation to complete (with timeout)
        use tokio::time::{sleep, Duration, Instant};
        let start = Instant::now();
        let timeout = Duration::from_secs(300); // 5 minutes timeout

        loop {
            let chunk_index_guard = chunk_index.read();
            if chunk_index_guard.is_complete {
                // Update progress
                let mut progress = task_manager
                    .get_task(task_id)
                    .map(|t| t.progress)
                    .unwrap_or_default();
                progress.snapshot_progress_percent = 100;
                progress.bytes_total = chunk_index_guard.total_uncompressed_size;
                progress.bytes_transferred = chunk_index_guard.total_uncompressed_size;
                let _ = task_manager.update_task_progress(task_id, progress);
                info!(
                    "Snapshot file generation completed for split task {} transfer {}: {} chunks, {} bytes",
                    task_id, transfer_id, chunk_index_guard.generated_chunks, chunk_index_guard.total_uncompressed_size
                );
                break;
            }
            if let Some(ref error) = chunk_index_guard.error {
                return Err(format!("Snapshot generation failed: {}", error));
            }
            if start.elapsed() > timeout {
                return Err(format!("Timeout waiting for snapshot file generation ({}s)", timeout.as_secs()));
            }
            drop(chunk_index_guard);
            sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Update routing table with new shard routings
    async fn update_routing_table(
        node: &Arc<RRNode>,
        task_manager: &Arc<SplitTaskManager>,
        task_id: &str,
        source_shard_id: &str,
        target_shard_ids: &[String],
        split_slot: u32,
        source_slot_start: u32,
        source_slot_end: u32,
    ) -> Result<(), String> {
        info!(
            "Updating routing table for task {}: source={}, targets={:?}, split_slot={}",
            task_id, source_shard_id, target_shard_ids, split_slot
        );

        let routing_table = node.routing_table();

        // Get current source shard routing (for validation)
        let source_shard_id_string = source_shard_id.to_string();
        let _source_routing = routing_table
            .get_shard_routing(&source_shard_id_string)
            .ok_or_else(|| format!("Source shard routing {} not found", source_shard_id))?;

        // Update source shard routing: [source_slot_start, split_slot)
        let updated_source_routing = ShardRouting::new(
            source_shard_id.to_string(),
            source_slot_start,
            split_slot,
        );
        routing_table.add_shard_routing(updated_source_routing);

        // Add target shard routings
        // Calculate slot ranges for each target shard
        let slot_range_per_shard = (source_slot_end - split_slot) / target_shard_ids.len() as u32;
        let mut current_slot = split_slot;

        for (idx, target_shard_id) in target_shard_ids.iter().enumerate() {
            let target_slot_start = current_slot;
            let target_slot_end = if idx == target_shard_ids.len() - 1 {
                source_slot_end
            } else {
                current_slot + slot_range_per_shard
            };

            let target_routing = ShardRouting::new(
                target_shard_id.clone(),
                target_slot_start,
                target_slot_end,
            );
            routing_table.add_shard_routing(target_routing);

            info!(
                "Added routing for target shard {}: slot_range=[{}, {})",
                target_shard_id, target_slot_start, target_slot_end
            );

            current_slot = target_slot_end;
        }

        info!("Routing table updated for task {}", task_id);
        Ok(())
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

                // Start split operation in background
                let task_manager_clone = self.task_manager.clone();
                let node_clone = self.node.clone();
                let snapshot_transfer_manager_clone = self.snapshot_transfer_manager.clone();
                let task_id_clone = task_id.clone();

                // Get task details for background execution
                let task = self.task_manager.get_task(&task_id_clone).unwrap();

                tokio::spawn(async move {
                    Self::execute_split_task(
                        node_clone,
                        snapshot_transfer_manager_clone,
                        task_manager_clone,
                        task_id_clone,
                        task.source_shard_id.clone(),
                        task.target_shard_ids.clone(),
                        task.split_slot,
                        task.source_slot_start,
                        task.source_slot_end,
                        task.target_nodes.clone(),
                    )
                    .await;
                });

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

