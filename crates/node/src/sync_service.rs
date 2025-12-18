//! Sync Service implementation
//!
//! Implements the gRPC service for data synchronization and migration.
//! Supports pull-based data transfer with snapshot chunks and entry logs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use parking_lot::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::node::RRNode;
use crate::snapshot_transfer::SnapshotTransferManager;
use proto::sync_service::{
    sync_service_client::SyncServiceClient, sync_service_server::SyncService, GetSyncStatusRequest,
    GetSyncStatusResponse, PullSyncDataRequest, PullSyncDataResponse, StartSyncRequest,
    StartSyncResponse, SyncDataType, SyncPhase, SyncProgress, SyncStatus, SyncType,
};
use raft::{Event, RequestId};
use storage::store::HybridStore;
use storage::traits::{
    HashStore, KeyStore, ListStore, SetStore, SnapshotStoreEntry, StringStore, ZSetStore,
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
#[derive(Clone)]
pub struct SyncServiceImpl {
    /// Node reference
    node: Arc<RRNode>,
    /// Sync task manager
    task_manager: Arc<SyncTaskManager>,
    /// Snapshot transfer manager (for snapshot chunk streaming)
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
}

impl SyncServiceImpl {
    pub fn new(node: Arc<RRNode>, snapshot_transfer_manager: Arc<SnapshotTransferManager>) -> Self {
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

    /// Get or create SyncService client for a node
    ///
    /// Uses connection pool from RRNode to reuse existing connections.
    /// If connection doesn't exist, creates a new one and adds it to the pool.
    async fn get_or_create_sync_client(
        &self,
        node_id: &str,
    ) -> Result<SyncServiceClient<tonic::transport::Channel>, String> {
        // Get or create gRPC channel from node's connection pool
        let channel_arc = self.node.get_or_create_grpc_channel(node_id).await?;

        // Clone the channel (Channel implements Clone and is cheap)
        // Create SyncService client from the cloned channel
        Ok(SyncServiceClient::new((*channel_arc).clone()))
    }

    /// Process snapshot chunk data
    /// Decompresses chunk data and applies entries to target shard store
    /// Uses spawn_blocking for CPU-intensive operations (decompression, deserialization, and applying entries)
    async fn process_snapshot_chunk_data(
        store: &Arc<HybridStore>,
        chunk_data: Vec<u8>,
        is_last_chunk: bool,
    ) -> anyhow::Result<u64> {
        let store_clone = store.clone();

        // Spawn blocking task for CPU-intensive operations: decompression, deserialization, and applying entries
        tokio::task::spawn_blocking(move || {
            // Decompress chunk data (CPU-intensive operation)
            let decompressed = zstd::decode_all(&chunk_data[..])
                .map_err(|e| anyhow::anyhow!("Failed to decompress chunk: {}", e))?;

            // Deserialize and apply entries sequentially to maintain order and reduce memory usage
            let mut entry_count = 0u64;
            let mut cursor = 0;

            while cursor < decompressed.len() {
                // Deserialize entry
                let (entry, bytes_read) =
                    match bincode::serde::decode_from_slice::<SnapshotStoreEntry, _>(
                        &decompressed[cursor..],
                        bincode::config::standard(),
                    ) {
                        Ok((entry, bytes_read)) => (entry, bytes_read),
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "Failed to deserialize snapshot entry at offset {}: {}",
                                cursor,
                                e
                            ));
                        }
                    };

                cursor += bytes_read;

                // Apply entry to store immediately
                match Self::apply_snapshot_entry_to_store(&store_clone, entry) {
                    Ok(should_return) => {
                        if should_return {
                            // Completed or error signal received
                            return Ok(entry_count);
                        }
                        entry_count += 1;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to apply snapshot entry: {}", e));
                    }
                }
            }

            Ok(entry_count)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Blocking task failed: {}", e))?
    }

    /// Apply a single snapshot entry to store
    /// Returns true if we should return early (e.g., Completed or Error)
    fn apply_snapshot_entry_to_store(
        store: &Arc<HybridStore>,
        entry: SnapshotStoreEntry,
    ) -> anyhow::Result<bool> {
        match entry {
            SnapshotStoreEntry::Completed => {
                info!("Received completion signal, snapshot restore finished");
                return Ok(true);
            }
            SnapshotStoreEntry::Error(err) => {
                return Err(anyhow::anyhow!("Snapshot restore error: {}", err));
            }
            SnapshotStoreEntry::String(key, value, apply_index) => {
                store
                    .set(&key, value, apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore String entry: {}", e))?;
            }
            SnapshotStoreEntry::Hash(key, field, value, apply_index) => {
                store
                    .hset(&key, &field, value, apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore Hash entry: {}", e))?;
            }
            SnapshotStoreEntry::List(key, element, apply_index) => {
                store
                    .rpush(&key, vec![element], apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore List entry: {}", e))?;
            }
            SnapshotStoreEntry::Set(key, member, apply_index) => {
                store
                    .sadd(&key, vec![member], apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore Set entry: {}", e))?;
            }
            SnapshotStoreEntry::ZSet(key, score, member, apply_index) => {
                store
                    .zadd(&key, vec![(score, member)], apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore ZSet entry: {}", e))?;
            }
            SnapshotStoreEntry::Bitmap(key, bitmap, apply_index) => {
                // Bitmap is stored as bytes, need to set bits
                // For now, we'll store it as a string value
                // TODO: Implement proper bitmap restoration
                store
                    .set(&key, bitmap, apply_index)
                    .map_err(|e| anyhow::anyhow!("Failed to restore Bitmap entry: {}", e))?;
            }
        }

        Ok(false)
    }

    /// Handle snapshot transfer for sync task
    async fn handle_snapshot_transfer(
        node: &Arc<RRNode>,
        task_manager: &Arc<SyncTaskManager>,
        sync_client: &mut SyncServiceClient<tonic::transport::Channel>,
        task_id: &str,
    ) -> Result<(), String> {
        // Get target shard ID from task
        let task = task_manager
            .get_task(task_id)
            .ok_or_else(|| format!("Sync task {} not found", task_id))?;

        // Get target shard store
        let store = node
            .get_state_machine(&task.target_shard_id)
            .ok_or_else(|| format!("Target shard {} not found", task.target_shard_id))?
            .store()
            .clone();

        if let Err(e) = task_manager.update_task_status(
            task_id,
            SyncStatus::InProgress,
            SyncPhase::SnapshotTransfer,
        ) {
            error!("Failed to update sync task phase: {}", e);
        }

        // Pull snapshot chunks from source node
        let pull_request = PullSyncDataRequest {
            task_id: task_id.to_string(),
            data_type: SyncDataType::SnapshotChunk as i32,
            chunk_index: 0,              // Start from first chunk
            max_chunk_size: 1024 * 1024, // 1MB chunks
            start_index: 0,
        };

        match sync_client.pull_sync_data(Request::new(pull_request)).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                let mut total_bytes = 0u64;
                let mut total_entries = 0u64;

                while let Some(chunk_result) = stream.message().await.transpose() {
                    match chunk_result {
                        Ok(chunk) => {
                            total_bytes += chunk.chunk_size as u64;

                            // Process chunk_data: decompress and apply entries
                            if !chunk.chunk_data.is_empty() {
                                match Self::process_snapshot_chunk_data(
                                    &store,
                                    chunk.chunk_data,
                                    chunk.is_last_chunk,
                                )
                                .await
                                {
                                    Ok(entry_count) => {
                                        total_entries += entry_count;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to process snapshot chunk for task {}: {}",
                                            task_id, e
                                        );
                                        let _ = task_manager.set_task_error(
                                            task_id,
                                            format!("Failed to process chunk: {}", e),
                                        );
                                        return Err(format!("Failed to process chunk: {}", e));
                                    }
                                }
                            }

                            // Update progress
                            let mut progress = task_manager
                                .get_task(task_id)
                                .map(|t| t.progress)
                                .unwrap_or_default();
                            progress.bytes_transferred = total_bytes;
                            let _ = task_manager.update_task_progress(task_id, progress);

                            if chunk.is_last_chunk {
                                info!(
                                    "Completed snapshot transfer for task {}: {} bytes",
                                    task_id, total_bytes
                                );
                                break;
                            }

                            if !chunk.error_message.is_empty() {
                                error!(
                                    "Error in snapshot chunk for task {}: {}",
                                    task_id, chunk.error_message
                                );
                                return Err(chunk.error_message);
                            }
                        }
                        Err(e) => {
                            error!("Error receiving snapshot chunk for task {}: {}", task_id, e);
                            return Err(format!("Stream error: {}", e));
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to start snapshot pull for task {}: {}", task_id, e);
                Err(format!("Failed to pull snapshot: {}", e))
            }
        }
    }

    /// Handle entry log transfer for sync task
    async fn handle_entry_log_transfer(
        node: &Arc<RRNode>,
        task_manager: &Arc<SyncTaskManager>,
        sync_client: &mut SyncServiceClient<tonic::transport::Channel>,
        task_id: &str,
        start_index: u64,
        end_index: u64,
    ) -> Result<(), String> {
        // Get target shard ID from task
        let task = task_manager
            .get_task(task_id)
            .ok_or_else(|| format!("Sync task {} not found", task_id))?;
        let target_shard_id = task.target_shard_id.clone();

        // Get target shard store
        let store = node
            .get_state_machine(&target_shard_id)
            .ok_or_else(|| format!("Target shard {} not found", target_shard_id))?
            .store()
            .clone();

        if let Err(e) =
            task_manager.update_task_status(task_id, SyncStatus::InProgress, SyncPhase::LogReplay)
        {
            error!("Failed to update sync task phase: {}", e);
        }

        // Pull entry logs from source node
        let mut current_index = start_index;
        let mut total_entries = 0u64;

        loop {
            let pull_request = PullSyncDataRequest {
                task_id: task_id.to_string(),
                data_type: SyncDataType::EntryLog as i32,
                chunk_index: 0, // Not used for entry logs
                max_chunk_size: 0,
                start_index: current_index,
            };

            match sync_client.pull_sync_data(Request::new(pull_request)).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let mut batch_entries = 0u64;

                    while let Some(chunk_result) = stream.message().await.transpose() {
                        match chunk_result {
                            Ok(chunk) => {
                                // Process chunk_data if present (for snapshot chunks)
                                if !chunk.chunk_data.is_empty() {
                                    match Self::process_snapshot_chunk_data(
                                        &store,
                                        chunk.chunk_data,
                                        chunk.is_last_chunk,
                                    )
                                    .await
                                    {
                                        Ok(_entry_count) => {
                                            // Chunk processed successfully
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to process snapshot chunk for task {}: {}",
                                                task_id, e
                                            );
                                            let _ = task_manager.set_task_error(
                                                task_id,
                                                format!("Failed to process chunk: {}", e),
                                            );
                                            return Err(format!("Failed to process chunk: {}", e));
                                        }
                                    }
                                }

                                if !chunk.entry_logs.is_empty() {
                                    // Get target shard RaftId
                                    let raft_id = node
                                        .get_raft_group(&target_shard_id)
                                        .ok_or_else(|| {
                                            format!("Target shard {} not found", target_shard_id)
                                        })?;

                                    // Apply entry logs to target shard through Raft
                                    for entry_log in &chunk.entry_logs {
                                        // Convert EntryLog to Event::ClientPropose
                                        // EntryLog contains: index, term, command (bytes)
                                        // We use the command directly and let Raft assign new index/term
                                        let event = Event::ClientPropose {
                                            cmd: entry_log.command.clone(),
                                            request_id: RequestId::new(),
                                        };

                                        // Dispatch event to Raft group
                                        match node.driver().dispatch_event(raft_id.clone(), event) {
                                            raft::multi_raft_driver::SendEventResult::Success => {
                                                // Event sent successfully
                                            }
                                            _ => {
                                                warn!(
                                                    "Failed to dispatch entry log event for task {} at index {}",
                                                    task_id, entry_log.index
                                                );
                                                // Continue processing other entries
                                            }
                                        }
                                    }

                                    batch_entries += chunk.entry_logs.len() as u64;
                                    total_entries += chunk.entry_logs.len() as u64;

                                    // Update current index
                                    if let Some(last_entry) = chunk.entry_logs.last() {
                                        current_index = last_entry.index + 1;
                                    }

                                    // Update progress
                                    let mut progress = task_manager
                                        .get_task(task_id)
                                        .map(|t| t.progress)
                                        .unwrap_or_default();
                                    progress.entries_transferred = total_entries;
                                    progress.current_index = current_index;
                                    if end_index > 0 && end_index > start_index {
                                        progress.entries_total = end_index - start_index;
                                        progress.log_replay_progress_percent =
                                            (((current_index - start_index) * 100)
                                                / (end_index - start_index))
                                                as u32;
                                    }
                                    let _ = task_manager.update_task_progress(task_id, progress);
                                }

                                if chunk.is_last_chunk {
                                    info!(
                                        "Completed entry log pull for task {}: {} entries",
                                        task_id, total_entries
                                    );
                                    return Ok(());
                                }

                                if !chunk.error_message.is_empty() {
                                    error!(
                                        "Error in entry log chunk for task {}: {}",
                                        task_id, chunk.error_message
                                    );
                                    return Err(chunk.error_message);
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Error receiving entry log chunk for task {}: {}",
                                    task_id, e
                                );
                                return Err(format!("Stream error: {}", e));
                            }
                        }
                    }

                    // If no entries were received, we're done
                    if batch_entries == 0 {
                        break;
                    }

                    // Check if we've reached the end index
                    if end_index > 0 && current_index >= end_index {
                        info!("Reached end index {} for task {}", end_index, task_id);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to pull entry logs for task {}: {}", task_id, e);
                    return Err(format!("Failed to pull entry logs: {}", e));
                }
            }
        }

        Ok(())
    }

    /// Execute sync task in background
    async fn execute_sync_task(
        sync_service_impl: SyncServiceImpl,
        task_manager: Arc<SyncTaskManager>,
        task_id: String,
        source_node_id: String,
        source_shard_id: String,
        sync_type: SyncType,
        start_index: u64,
        end_index: u64,
    ) {
        info!(
            "Background sync task started: task_id={}, will pull from source_node={}, source_shard={}",
            task_id, source_node_id, source_shard_id
        );

        // Update task status to IN_PROGRESS
        if let Err(e) =
            task_manager.update_task_status(&task_id, SyncStatus::InProgress, SyncPhase::Preparing)
        {
            error!("Failed to update sync task status: {}", e);
            return;
        }

        // 1. Get or create SyncService client from connection pool
        let mut sync_client = match sync_service_impl
            .get_or_create_sync_client(&source_node_id)
            .await
        {
            Ok(client) => client,
            Err(e) => {
                error!(
                    "Failed to get SyncService client for source node {} (task {}): {}",
                    source_node_id, task_id, e
                );
                let _ = task_manager.set_task_error(&task_id, e);
                return;
            }
        };

        info!(
            "Using SyncService client for source node {} (task {})",
            source_node_id, task_id
        );

        // 2. Pull snapshot chunks if needed (for FULL_SYNC or SNAPSHOT_ONLY)
        if sync_type == SyncType::FullSync || sync_type == SyncType::SnapshotOnly {
            if let Err(e) = Self::handle_snapshot_transfer(
                &sync_service_impl.node,
                &task_manager,
                &mut sync_client,
                &task_id,
            )
            .await
            {
                error!("Failed to transfer snapshot for task {}: {}", task_id, e);
                let _ = task_manager.set_task_error(&task_id, e);
                return;
            }
        }

        // 3. Pull entry logs if needed (for FULL_SYNC or INCREMENTAL_SYNC)
        if sync_type == SyncType::FullSync || sync_type == SyncType::IncrementalSync {
            if let Err(e) = Self::handle_entry_log_transfer(
                &sync_service_impl.node,
                &task_manager,
                &mut sync_client,
                &task_id,
                start_index,
                end_index,
            )
            .await
            {
                error!("Failed to transfer entry logs for task {}: {}", task_id, e);
                let _ = task_manager.set_task_error(&task_id, e);
                return;
            }
        }

        // 4. Mark sync as completed
        if let Err(e) =
            task_manager.update_task_status(&task_id, SyncStatus::Completed, SyncPhase::Completing)
        {
            error!("Failed to mark sync task as completed: {}", e);
        } else {
            info!("Sync task {} completed successfully", task_id);
        }
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
            "StartSync request (PULL side): task_id={}, source_shard={}, target_shard={}, sync_type={:?}, source_node={}, target_node={}",
            task_id, req.source_shard_id, req.target_shard_id, req.sync_type, req.source_node_id, req.target_node_id
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

        // This is called on PULL side (target node)
        // Check if target shard exists (we are the target)
        let target_shard_exists = self.node.get_raft_group(&req.target_shard_id).is_some();

        if !target_shard_exists {
            return Ok(Response::new(StartSyncResponse {
                task_id: task_id.clone(),
                success: false,
                error_message: format!(
                    "Target shard {} not found on this node",
                    req.target_shard_id
                ),
            }));
        }

        // Convert sync type
        let sync_type = match req.sync_type {
            x if x == SyncType::FullSync as i32 => SyncType::FullSync,
            x if x == SyncType::IncrementalSync as i32 => SyncType::IncrementalSync,
            x if x == SyncType::SnapshotOnly as i32 => SyncType::SnapshotOnly,
            _ => SyncType::Unspecified,
        };

        // Create sync task on PULL side (target node)
        let source_shard_id = req.source_shard_id.clone();
        let target_shard_id = req.target_shard_id.clone();
        let source_node_id = req.source_node_id.clone();
        let target_node_id = req.target_node_id.clone();

        match self.task_manager.create_task(
            task_id.clone(),
            source_shard_id.clone(),
            target_shard_id.clone(),
            source_node_id.clone(),
            target_node_id.clone(),
            sync_type,
            req.start_index,
            req.end_index,
            req.slot_start,
            req.slot_end,
        ) {
            Ok(()) => {
                info!("Created sync task on PULL side: {}", task_id);

                // Spawn background task to pull data from PUSH side (source node)
                let task_manager_clone = self.task_manager.clone();
                let sync_service_impl = self.clone();
                let task_id_clone = task_id.clone();
                let sync_type_clone = sync_type;
                let start_index = req.start_index;
                let end_index = req.end_index;

                tokio::spawn(async move {
                    Self::execute_sync_task(
                        sync_service_impl,
                        task_manager_clone,
                        task_id_clone,
                        source_node_id,
                        source_shard_id,
                        sync_type_clone,
                        start_index,
                        end_index,
                    )
                    .await;
                });

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
            "PullSyncData request: task_id={}, data_type={:?}, chunk_index={}, start_index={}",
            task_id, req.data_type, req.chunk_index, req.start_index
        );

        if task_id.is_empty() {
            return Err(Status::invalid_argument("task_id is required"));
        }

        // This is called on PUSH side (source node)
        // PULL side (target node) requests data from us
        // We don't need to have the task locally, as we are the data source

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

        // Spawn task to stream data from PUSH side
        let snapshot_transfer_manager_clone = self.snapshot_transfer_manager.clone();
        let node_clone = self.node.clone();
        tokio::spawn(async move {
            let result = match data_type {
                SyncDataType::SnapshotChunk => {
                    stream_snapshot_chunks_from_source(
                        snapshot_transfer_manager_clone,
                        node_clone,
                        task_id.clone(),
                        req.chunk_index,
                        req.max_chunk_size,
                        tx,
                    )
                    .await
                }
                SyncDataType::EntryLog => {
                    stream_entry_logs_from_source(node_clone, task_id.clone(), req.start_index, tx)
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

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
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

        // This is called on PULL side (target node) by PUSH side (source node)
        // PUSH side periodically queries status to monitor sync progress
        match self.task_manager.get_task(&task_id) {
            Some(task) => {
                // TODO: Update progress from actual sync operation state
                // This would query:
                // - Snapshot transfer progress
                // - Entry log transfer progress
                // - Current Raft indices
                // - Bytes/entries transferred

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

/// Stream snapshot chunks from PUSH side (source node)
/// Streams chunks as they become available, without waiting for entire file generation
async fn stream_snapshot_chunks_from_source(
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    _node: Arc<RRNode>,
    task_id: String,
    chunk_index: u32,
    _max_chunk_size: u32,
    tx: tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
) -> Result<()> {
    use crate::snapshot_transfer::{get_transfer_state_info, read_chunk_from_file, wait_for_chunk};
    use std::time::Duration;

    info!(
        "Streaming snapshot chunks from source for task {} starting at chunk {}",
        task_id, chunk_index
    );

    // Get transfer state (snapshot path and chunk index)
    // Note: task_id should match the transfer_id used when creating the snapshot
    let (snapshot_path, chunk_index_arc) =
        get_transfer_state_info(&snapshot_transfer_manager, &task_id).map_err(|e| {
            anyhow::anyhow!("Failed to get transfer state for task {}: {}", task_id, e)
        })?;

    info!(
        "Starting to stream chunks for task {} from chunk {}",
        task_id, chunk_index
    );

    // Stream chunks starting from chunk_index
    // Process chunks one by one as they become available (streaming mode)
    let mut current_chunk_index = chunk_index;

    // Helper function to send error response and return error
    async fn send_error_and_return(
        tx: &tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
        task_id: &str,
        error_msg: String,
        error: anyhow::Error,
    ) -> Result<()> {
        let _ = tx
            .send(Ok(PullSyncDataResponse {
                task_id: task_id.to_string(),
                error_message: error_msg.clone(),
                data_type: SyncDataType::SnapshotChunk as i32,
                ..Default::default()
            }))
            .await;
        error!("{}: {:?}", error_msg, error);
        Err(error)
    }

    loop {
        // Wait for chunk to be available and get its metadata
        let chunk_metadata = match wait_for_chunk(
            &chunk_index_arc,
            current_chunk_index,
            Duration::from_millis(100), // Check every 100ms
            Duration::from_secs(60),    // 60s timeout per chunk
        )
        .await
        {
            Ok(metadata) => metadata,
            Err(e) => {
                let error_msg = format!("Failed to wait for chunk {}", current_chunk_index);
                return send_error_and_return(&tx, &task_id, error_msg.clone(), e).await;
            }
        };

        // Read chunk from file
        let compressed_data = match read_chunk_from_file(
            &snapshot_path,
            current_chunk_index,
            &chunk_metadata,
        )
        .await
        {
            Ok(data) => data,
            Err(e) => {
                let error_msg = format!("Failed to read chunk {}", current_chunk_index);
                return send_error_and_return(&tx, &task_id, error_msg.clone(), e).await;
            }
        };

        // Determine if this is the last chunk
        let is_last = chunk_metadata.is_last;

        // Create response
        // Use checksum from chunk_metadata (already calculated when chunk was written)
        let response = PullSyncDataResponse {
            task_id: task_id.clone(),
            data_type: SyncDataType::SnapshotChunk as i32,
            chunk_data: compressed_data,
            entry_logs: vec![],
            chunk_size: chunk_metadata.compressed_size,
            is_last_chunk: is_last,
            checksum: chunk_metadata.crc32.to_le_bytes().to_vec(),
            error_message: String::new(),
        };

        // Send response
        if tx.send(Ok(response)).await.is_err() {
            // Receiver dropped, client disconnected
            info!(
                "Client disconnected during snapshot transfer for task {}",
                task_id
            );
            return Ok(());
        }

        if is_last {
            info!("Completed streaming snapshot for task {}", task_id);
            break;
        }

        current_chunk_index += 1;
    }

    Ok(())
}

/// Helper function to send entry log response
async fn send_entry_log_response(
    tx: &tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
    task_id: &str,
    entry_logs: Vec<proto::sync_service::EntryLog>,
    is_last_chunk: bool,
    error_message: String,
) {
    let _ = tx
        .send(Ok(PullSyncDataResponse {
            task_id: task_id.to_string(),
            data_type: SyncDataType::EntryLog as i32,
            chunk_data: vec![],
            entry_logs,
            chunk_size: 0,
            is_last_chunk,
            checksum: vec![],
            error_message,
        }))
        .await;
}

/// Stream entry logs from PUSH side (source node)
/// Reads from log replay writer file created during split operation
async fn stream_entry_logs_from_source(
    node: Arc<RRNode>,
    task_id: String,
    start_index: u64,
    tx: tokio::sync::mpsc::Sender<Result<PullSyncDataResponse, Status>>,
) -> Result<()> {
    // Create iterator starting from start_index
    let mut iterator = match node.create_log_replay_iterator(&task_id, start_index).await {
        Ok(iter) => iter,
        Err(e) => {
            let error_msg = format!("Failed to create log replay iterator: {}", e);
            warn!("{}", error_msg);
            send_entry_log_response(&tx, &task_id, vec![], true, error_msg).await;
            return Ok(());
        }
    };

    const BATCH_SIZE: usize = 100; // Number of entries to send per batch
    let mut entry_logs = Vec::new();

    // Read entries using iterator
    loop {
        // Only call next() if there might be more data
        match iterator.next().await {
            Ok(Some(entry_log)) => {
                entry_logs.push(entry_log);

                // Send batch when full
                if entry_logs.len() >= BATCH_SIZE {
                    send_entry_log_response(&tx, &task_id, entry_logs, false, String::new()).await;
                    entry_logs = Vec::new();
                }
            }
            Ok(None) => {
                // Iterator exhausted (should not happen with current implementation)
                break;
            }
            Err(e) => {
                let error_msg = format!("Error reading log replay entry: {}", e);
                error!("{}", error_msg);
                send_entry_log_response(&tx, &task_id, vec![], true, error_msg).await;
                return Ok(());
            }
        }
    }

    // Send remaining entries
    if !entry_logs.is_empty() {
        send_entry_log_response(&tx, &task_id, entry_logs, false, String::new()).await;
    }

    Ok(())
}
