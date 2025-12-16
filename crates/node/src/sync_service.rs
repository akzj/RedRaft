//! Sync Service implementation
//!
//! Implements the gRPC service for data synchronization and migration.
//! Supports pull-based data transfer with snapshot chunks and entry logs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::node::RRNode;
use crate::snapshot_transfer::SnapshotTransferManager;
use proto::sync_service::{
    sync_service_server::SyncService, GetSyncStatusRequest, GetSyncStatusResponse,
    PullSyncDataRequest, PullSyncDataResponse, SyncDataType, SyncPhase, SyncProgress,
    SyncStatus, SyncType, StartSyncRequest, StartSyncResponse,
};

/// Sync task state
#[derive(Debug, Clone)]
pub struct SyncTask {
    /// Task ID
    pub task_id: String,
    /// Source shard ID
    pub source_shard_id: String,
    /// Target shard ID
    pub target_shard_id: String,
    /// Source node ID
    pub source_node_id: String,
    /// Target node ID
    pub target_node_id: String,
    /// Sync type
    pub sync_type: i32,
    /// Starting Raft index
    pub start_index: u64,
    /// Ending Raft index (0 means to end)
    pub end_index: u64,
    /// Slot range to sync
    pub slot_start: u32,
    pub slot_end: u32,
    /// Sync status (as i32)
    pub status: i32,
    /// Sync phase (as i32)
    pub phase: i32,
    /// Progress information
    pub progress: SyncProgress,
    /// Created at timestamp (seconds since epoch)
    pub created_at: u64,
    /// Updated at timestamp (seconds since epoch)
    pub updated_at: u64,
    /// Error message (if failed)
    pub error_message: String,
}

impl SyncTask {
    fn new(
        task_id: String,
        source_shard_id: String,
        target_shard_id: String,
        source_node_id: String,
        target_node_id: String,
        sync_type: SyncType,
        start_index: u64,
        end_index: u64,
        slot_start: u32,
        slot_end: u32,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            task_id,
            source_shard_id,
            target_shard_id,
            source_node_id,
            target_node_id,
            sync_type: sync_type as i32,
            start_index,
            end_index,
            slot_start,
            slot_end,
            status: SyncStatus::Preparing as i32,
            phase: SyncPhase::Preparing as i32,
            progress: SyncProgress {
                current_phase: SyncPhase::Preparing as i32,
                snapshot_progress_percent: 0,
                log_replay_progress_percent: 0,
                bytes_transferred: 0,
                bytes_total: 0,
                entries_transferred: 0,
                entries_total: 0,
                current_index: start_index,
                target_index: end_index,
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

/// Sync task manager
pub struct SyncTaskManager {
    /// Tasks: task_id -> SyncTask
    tasks: Arc<RwLock<HashMap<String, SyncTask>>>,
}

impl SyncTaskManager {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_task(
        &self,
        task_id: String,
        source_shard_id: String,
        target_shard_id: String,
        source_node_id: String,
        target_node_id: String,
        sync_type: SyncType,
        start_index: u64,
        end_index: u64,
        slot_start: u32,
        slot_end: u32,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if tasks.contains_key(&task_id) {
            return Err(format!("Sync task {} already exists", task_id));
        }

        let task = SyncTask::new(
            task_id.clone(),
            source_shard_id,
            target_shard_id,
            source_node_id,
            target_node_id,
            sync_type,
            start_index,
            end_index,
            slot_start,
            slot_end,
        );

        tasks.insert(task_id, task);
        Ok(())
    }

    pub fn get_task(&self, task_id: &str) -> Option<SyncTask> {
        self.tasks.read().get(task_id).cloned()
    }

    pub fn update_task_status(
        &self,
        task_id: &str,
        status: SyncStatus,
        phase: SyncPhase,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = status as i32;
            task.phase = phase as i32;
            task.progress.current_phase = phase as i32;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Sync task {} not found", task_id))
        }
    }

    pub fn update_task_progress(
        &self,
        task_id: &str,
        progress: SyncProgress,
    ) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.progress = progress;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Sync task {} not found", task_id))
        }
    }

    pub fn set_task_error(&self, task_id: &str, error_message: String) -> Result<(), String> {
        let mut tasks = self.tasks.write();
        if let Some(task) = tasks.get_mut(task_id) {
            task.error_message = error_message;
            task.status = SyncStatus::Failed as i32;
            task.update_timestamp();
            Ok(())
        } else {
            Err(format!("Sync task {} not found", task_id))
        }
    }

    pub fn remove_task(&self, task_id: &str) -> bool {
        self.tasks.write().remove(task_id).is_some()
    }
}

impl Default for SyncTaskManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Sync Service implementation
pub struct SyncServiceImpl {
    /// Node reference
    node: Arc<RRNode>,
    /// Sync task manager
    task_manager: Arc<SyncTaskManager>,
    /// Snapshot transfer manager (for snapshot chunk streaming)
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
}

impl SyncServiceImpl {
    pub fn new(
        node: Arc<RRNode>,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    ) -> Self {
        Self {
            node,
            task_manager: Arc::new(SyncTaskManager::new()),
            snapshot_transfer_manager,
        }
    }

    /// Get task manager reference
    pub fn task_manager(&self) -> &Arc<SyncTaskManager> {
        &self.task_manager
    }
}

#[tonic::async_trait]
impl SyncService for SyncServiceImpl {
    type PullSyncDataStream =
        tokio_stream::wrappers::ReceiverStream<Result<PullSyncDataResponse, Status>>;

    async fn start_sync(
        &self,
        request: Request<StartSyncRequest>,
    ) -> Result<Response<StartSyncResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        info!(
            "StartSync request: task_id={}, source_shard={}, target_shard={}, sync_type={:?}",
            task_id, req.source_shard_id, req.target_shard_id, req.sync_type
        );

        // Validate request
        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }
        if req.source_shard_id.is_empty() {
            return Err(Status::invalid_argument("source_shard_id is required"));
        }
        if req.target_shard_id.is_empty() {
            return Err(Status::invalid_argument("target_shard_id is required"));
        }

        // Check if source shard exists
        let source_shard_exists = self.node.get_raft_group(&req.source_shard_id).is_some();

        if !source_shard_exists {
            return Ok(Response::new(StartSyncResponse {
                task_id: task_id.clone(),
                success: false,
                error_message: format!("Source shard {} not found", req.source_shard_id),
            }));
        }

        // Convert sync type
        let sync_type = match req.sync_type {
            x if x == SyncType::FullSync as i32 => SyncType::FullSync,
            x if x == SyncType::IncrementalSync as i32 => SyncType::IncrementalSync,
            x if x == SyncType::SnapshotOnly as i32 => SyncType::SnapshotOnly,
            _ => SyncType::Unspecified,
        };

        // Create sync task
        match self.task_manager.create_task(
            task_id.clone(),
            req.source_shard_id,
            req.target_shard_id,
            req.source_node_id,
            req.target_node_id,
            sync_type,
            req.start_index,
            req.end_index,
            req.slot_start,
            req.slot_end,
        ) {
            Ok(()) => {
                info!("Created sync task: {}", task_id);
                // TODO: Start actual sync operation in background
                // This would involve:
                // 1. Creating snapshot if needed (for FULL_SYNC or SNAPSHOT_ONLY)
                // 2. Preparing entry log streaming (for FULL_SYNC or INCREMENTAL_SYNC)
                // 3. Updating progress as data is transferred

                Ok(Response::new(StartSyncResponse {
                    task_id,
                    success: true,
                    error_message: String::new(),
                }))
            }
            Err(e) => {
                error!("Failed to create sync task {}: {}", task_id, e);
                Ok(Response::new(StartSyncResponse {
                    task_id,
                    success: false,
                    error_message: e,
                }))
            }
        }
    }

    async fn pull_sync_data(
        &self,
        request: Request<PullSyncDataRequest>,
    ) -> Result<Response<Self::PullSyncDataStream>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        info!(
            "PullSyncData request: task_id={}, data_type={:?}, offset={}, start_index={}",
            task_id, req.data_type, req.offset, req.start_index
        );

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        // Get sync task
        let task = self
            .task_manager
            .get_task(&task_id)
            .ok_or_else(|| Status::not_found(format!("Sync task {} not found", task_id)))?;

        // Check if task is in valid state
        if task.status == SyncStatus::Failed as i32 {
            return Err(Status::failed_precondition(format!(
                "Sync task {} has failed: {}",
                task_id, task.error_message
            )));
        }

        if task.status == SyncStatus::Cancelled as i32 {
            return Err(Status::cancelled(format!("Sync task {} was cancelled", task_id)));
        }

        // Create channel for streaming responses
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        // Convert data type
        let data_type = match req.data_type {
            x if x == SyncDataType::SnapshotChunk as i32 => SyncDataType::SnapshotChunk,
            x if x == SyncDataType::EntryLog as i32 => SyncDataType::EntryLog,
            _ => {
                return Err(Status::invalid_argument("Invalid data_type"));
            }
        };

        // Spawn task to stream data
        let task_manager_clone = self.task_manager.clone();
        let snapshot_transfer_manager_clone = self.snapshot_transfer_manager.clone();
        let node_clone = self.node.clone();
        let task_clone = task.clone();
        tokio::spawn(async move {
            let result = match data_type {
                SyncDataType::SnapshotChunk => {
                    stream_snapshot_chunks(
                        task_manager_clone,
                        snapshot_transfer_manager_clone,
                        task_clone,
                        task_id.clone(),
                        req.offset,
                        req.max_chunk_size,
                        tx,
                    )
                    .await
                }
                SyncDataType::EntryLog => {
                    stream_entry_logs(
                        task_manager_clone,
                        node_clone,
                        task_clone,
                        task_id.clone(),
                        req.start_index,
                        req.max_entries,
                        tx,
                    )
                    .await
                }
                _ => {
                    error!("Invalid data type for sync task {}", task_id);
                    Ok(())
                }
            };

            if let Err(e) = result {
                error!("Error streaming sync data for task {}: {}", task_id, e);
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn get_sync_status(
        &self,
        request: Request<GetSyncStatusRequest>,
    ) -> Result<Response<GetSyncStatusResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        match self.task_manager.get_task(&task_id) {
            Some(task) => {
                // TODO: Update progress from actual sync operation state
                // This would query:
                // - Snapshot transfer progress
                // - Entry log transfer progress
                // - Current Raft indices

                Ok(Response::new(GetSyncStatusResponse {
                    task_id: task.task_id.clone(),
                    status: task.status,
                    phase: task.phase,
                    progress: Some(task.progress),
                    error_message: task.error_message.clone(),
                }))
            }
            None => {
                let task_id_clone = task_id.clone();
                Ok(Response::new(GetSyncStatusResponse {
                    task_id,
                    status: SyncStatus::NotFound as i32,
                    phase: SyncPhase::Unspecified as i32,
                    progress: None,
                    error_message: format!("Sync task {} not found", task_id_clone),
                }))
            }
        }
    }
}

/// Stream snapshot chunks for sync task
#[allow(unused_variables)]
async fn stream_snapshot_chunks(
    task_manager: Arc<SyncTaskManager>,
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    task: SyncTask,
    task_id: String,
    offset: u64,
    max_chunk_size: u32,
    tx: tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
) -> Result<(), String> {
    // TODO: Implement snapshot chunk streaming
    // This would:
    // 1. Create or get snapshot for the source shard
    // 2. Read snapshot file in chunks
    // 3. Send chunks via channel
    // 4. Update task progress

    warn!(
        "Snapshot chunk streaming not yet implemented for sync task {}",
        task_id
    );

    // For now, send an error response
    let _ = tx
        .send(Ok(PullSyncDataResponse {
            task_id: task_id.clone(),
            data_type: SyncDataType::SnapshotChunk as i32,
            chunk_data: vec![],
            entry_logs: vec![],
            offset: 0,
            chunk_size: 0,
            is_last_chunk: true,
            total_size: 0,
            checksum: vec![],
            error_message: "Snapshot chunk streaming not yet implemented".to_string(),
        }))
        .await;

    Ok(())
}

/// Stream entry logs for sync task
#[allow(unused_variables)]
async fn stream_entry_logs(
    task_manager: Arc<SyncTaskManager>,
    node: Arc<RRNode>,
    task: SyncTask,
    task_id: String,
    start_index: u64,
    max_entries: u32,
    tx: tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
) -> Result<(), String> {
    // TODO: Implement entry log streaming
    // This would:
    // 1. Get RaftId for source shard
    // 2. Read log entries from storage starting from start_index
    // 3. Convert LogEntry to EntryLog proto messages
    // 4. Send entry logs in batches via channel
    // 5. Update task progress

    warn!(
        "Entry log streaming not yet implemented for sync task {}",
        task_id
    );

    // Get RaftId for source shard
    let raft_id = match node.get_raft_group(&task.source_shard_id) {
        Some(id) => id,
        None => {
            let _ = tx
                .send(Ok(PullSyncDataResponse {
                    task_id: task_id.clone(),
                    data_type: SyncDataType::EntryLog as i32,
                    chunk_data: vec![],
                    entry_logs: vec![],
                    offset: 0,
                    chunk_size: 0,
                    is_last_chunk: true,
                    total_size: 0,
                    checksum: vec![],
                    error_message: format!("Source shard {} not found", task.source_shard_id),
                }))
                .await;
            return Ok(());
        }
    };

    // TODO: Read log entries from storage
    // Example:
    // let storage = node.storage(); // Need to expose storage
    // let entries = storage.get_log_entries(&raft_id, start_index, max_entries).await?;
    // Convert to EntryLog and send

    // For now, send an empty response
    let _ = tx
        .send(Ok(PullSyncDataResponse {
            task_id: task_id.clone(),
            data_type: SyncDataType::EntryLog as i32,
            chunk_data: vec![],
            entry_logs: vec![],
            offset: 0,
            chunk_size: 0,
            is_last_chunk: true,
            total_size: 0,
            checksum: vec![],
            error_message: "Entry log streaming not yet implemented".to_string(),
        }))
        .await;

    Ok(())
}

