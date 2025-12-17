//! KV state machine implementation
//!
//! Applies Raft logs to key-value storage, using RedisStore trait for storage

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use raft::{
    message::{PreVoteRequest, PreVoteResponse},
    traits::ClientResult,
    ApplyResult, ClusterConfig, ClusterConfigStorage, Event, HardStateStorage, LogEntryStorage,
    Network, RaftId, RequestId, SnapshotStorage, StateMachine, Storage, StorageResult,
    TimerService,
};
use resp::Command;
use storage::{
    store::HybridStore,
    traits::{ApplyResult as StoreApplyResult, StoreError},
};

use crate::config::Config;
use crate::node::PendingRequests;
use crate::snapshot_transfer::SnapshotTransferManager;
use rr_core::routing::RoutingTable;

/// Log replay configuration for split operations
#[derive(Clone)]
pub struct ReplayLogConfig {
    /// Channel sender for forwarding commands
    pub tx: tokio::sync::mpsc::Sender<(u64, u64, resp::Command)>,
    /// Slot range (begin, end) - commands with slot in [begin, end) will be forwarded
    pub slot_range: (u32, u32),
    /// Split task ID
    pub task_id: String,
}

/// KV state machine
#[derive(Clone)]
pub struct KVStateMachine {
    /// Storage backend (supports memory or persistent storage)
    store: Arc<storage::store::HybridStore>,
    /// Last applied index (monotonically increasing, tracks Raft apply_index)
    apply_index: Arc<std::sync::atomic::AtomicU64>,
    term: Arc<std::sync::atomic::AtomicU64>,
    /// Pending request tracker
    pending_requests: Option<PendingRequests>,
    /// Apply result cache (queue of (index, result)), used to return actual results in client_response
    /// Uses queue because index is monotonically increasing, allowing easy cleanup of expired entries
    apply_results: Arc<parking_lot::Mutex<std::collections::VecDeque<(u64, StoreApplyResult)>>>,
    /// Raft storage backend
    storage: Arc<dyn Storage>,
    /// Network layer
    network: Arc<dyn Network>,
    /// Timer service
    timers: raft::multi_raft_driver::Timers,
    /// Snapshot transfer manager
    snapshot_transfer_manager: Arc<SnapshotTransferManager>,
    /// Configuration
    config: Config,
    /// Routing table for node address lookup
    routing_table: Arc<rr_core::routing::RoutingTable>,
    /// Active log replay configurations for split operations
    /// Each entry contains channel, slot range, and task_id
    replay_logs: Arc<parking_lot::RwLock<Vec<ReplayLogConfig>>>,
}

impl KVStateMachine {
    /// Create KV state machine with all dependencies
    pub fn new(
        store: Arc<HybridStore>,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        timers: raft::multi_raft_driver::Timers,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
        routing_table: Arc<rr_core::routing::RoutingTable>,
        config: Config,
    ) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: None,
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            storage,
            network,
            timers,
            snapshot_transfer_manager,
            config,
            routing_table,
            replay_logs: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    /// Create KV state machine with request tracking
    pub fn with_pending_requests(
        store: Arc<HybridStore>,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        timers: raft::multi_raft_driver::Timers,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
        pending_requests: PendingRequests,
        routing_table: Arc<rr_core::routing::RoutingTable>,
        config: Config,
    ) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: Some(pending_requests),
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            storage,
            network,
            timers,
            snapshot_transfer_manager,
            config,
            routing_table,
            replay_logs: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }

    /// Add a log replay configuration for split operation
    pub fn add_replay_log(&self, config: ReplayLogConfig) {
        self.replay_logs.write().push(config);
    }

    /// Remove a log replay configuration by task_id
    pub fn remove_replay_log(&self, task_id: &str) {
        self.replay_logs.write().retain(|c| c.task_id != task_id);
    }

    /// Get storage backend reference (for read operations)
    pub fn store(&self) -> &Arc<HybridStore> {
        &self.store
    }

    /// Get apply_index reference
    pub fn apply_index(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.apply_index
    }

    /// Get term reference
    pub fn term(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.term
    }

    /// Create gRPC client to snapshot service
    async fn create_snapshot_client(
        routing_table: &RoutingTable,
        from: &RaftId,
        _config: &Config,
    ) -> Result<
        proto::snapshot_service::snapshot_service_client::SnapshotServiceClient<
            tonic::transport::Channel,
        >,
        anyhow::Error,
    > {
        use proto::snapshot_service::snapshot_service_client::SnapshotServiceClient;
        use tonic::transport::Endpoint;

        // Get shard_id from RaftId (shard_id == group_id in this system)
        let shard_id = &from.group;

        // Get leader node_id for this shard
        let leader_node_id = routing_table
            .find_leader_for_shard(shard_id)
            .map_err(|e| anyhow::anyhow!("Failed to find leader for shard {}: {}", shard_id, e))?;

        // Get gRPC address for leader node
        let leader_addr = routing_table
            .get_grpc_address(&leader_node_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No gRPC address found for leader node {} (shard {})",
                    leader_node_id,
                    shard_id
                )
            })?;

        // Ensure the address has a protocol prefix
        // If it already has http:// or https://, use it as-is
        // Otherwise, assume http:// (for development/non-TLS environments)
        // Note: gRPC uses HTTP/2, so http:// is valid for non-TLS connections
        let endpoint_uri =
            if leader_addr.starts_with("http://") || leader_addr.starts_with("https://") {
                leader_addr.clone()
            } else {
                format!("http://{}", leader_addr)
            };

        let endpoint = Endpoint::from_shared(endpoint_uri.clone())
            .map_err(|e| anyhow::anyhow!("Invalid leader endpoint {}: {}", endpoint_uri, e))?;

        let client = SnapshotServiceClient::connect(endpoint)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to leader {}: {}", endpoint_uri, e))?;

        Ok(client)
    }

    /// Pull snapshot chunks from gRPC stream asynchronously
    async fn pull_snapshot_chunks_async(
        mut client: proto::snapshot_service::snapshot_service_client::SnapshotServiceClient<
            tonic::transport::Channel,
        >,
        metadata: &crate::snapshot_transfer::SnapshotMetadata,
        index: u64,
        term: u64,
        request_id: RequestId,
        config: &Config,
        blocking_tx: std::sync::mpsc::Sender<Result<(Vec<u8>, bool), String>>,
    ) -> tokio::task::JoinHandle<Result<(), String>> {
        use proto::snapshot_service::PullSnapshotDataRequest;
        use tonic::Request;

        let metadata = metadata.clone();
        let config = config.clone();
        let transfer_id = metadata.transfer_id.clone();

        tokio::spawn(async move {
            let mut chunk_index = 0u32;

            loop {
                // Request next chunk
                let request = PullSnapshotDataRequest {
                    raft_group_id: metadata.raft_group_id.clone(),
                    snapshot_index: index,
                    snapshot_term: term,
                    snapshot_request_id: request_id.into(),
                    chunk_index,
                    max_chunk_size: config.snapshot.chunk_size as u32,
                    transfer_id: transfer_id.clone(),
                };

                let mut stream = match client.pull_snapshot_data(Request::new(request)).await {
                    Ok(response) => response.into_inner(),
                    Err(e) => {
                        error!("Failed to start pull snapshot stream: {}", e);
                        let _ = blocking_tx
                            .send(Err(format!("Failed to start pull snapshot stream: {}", e)));
                        break;
                    }
                };

                // Receive chunks from stream
                loop {
                    let response_result = match stream.message().await {
                        Ok(Some(response)) => Ok(response),
                        Ok(None) => break, // Stream ended
                        Err(e) => Err(e),
                    };

                    match response_result {
                        Ok(response) => {
                            let chunk_data = response.chunk_data;
                            let is_last = response.is_last_chunk;

                            // Send compressed chunk data to blocking channel
                            // Convert Bytes to Vec<u8> for blocking channel
                            if blocking_tx
                                .send(Ok((chunk_data.to_vec(), is_last)))
                                .is_err()
                            {
                                // Receiver dropped
                                return Err("Receiver dropped during snapshot restore".to_string());
                            }

                            if is_last {
                                break;
                            }

                            // Increment chunk index for next request
                            chunk_index += 1;
                        }
                        Err(e) => {
                            error!("Error receiving chunk from stream: {}", e);
                            let _ = blocking_tx.send(Err(format!("Stream error: {}", e)));
                            break;
                        }
                    }
                }

                // Check if we received completion signal
                break;
            }

            Ok(())
        })
    }
}

#[async_trait]
impl StateMachine for KVStateMachine {
    async fn apply_command(
        &self,
        _from: &RaftId,
        index: u64,
        term: u64,
        cmd: raft::Command,
    ) -> ApplyResult<()> {
        let store = Arc::clone(&self.store);

        // Execute command with apply_index (for WAL logging) in blocking thread pool
        // to avoid blocking async runtime since storage operations are synchronous
        let (result, command_opt) = tokio::task::spawn_blocking(move || {
            // Deserialize to Command
            let command: Command = match bincode::serde::decode_from_slice(
                cmd.as_slice(),
                bincode::config::standard(),
            ) {
                Ok((cmd, _)) => cmd,
                Err(e) => {
                    warn!("Failed to deserialize command at index {}: {}", index, e);
                    return (
                        StoreApplyResult::Error(StoreError::Internal(format!(
                            "Failed to deserialize command at index {}: {}",
                            index, e
                        ))),
                        None,
                    );
                }
            };
            (
                store.apply_with_index(index, index, &command),
                Some(command),
            )
        })
        .await
        .map_err(|e| raft::ApplyError::Internal(format!("Failed to apply command: {}", e)))?;

        // Cache result for use in client_response (push to queue since index is monotonically increasing)
        self.apply_results.lock().push_back((index, result));

        // Update apply_index and term (this is the last applied index and term)
        self.apply_index
            .store(index, std::sync::atomic::Ordering::SeqCst);
        self.term.store(term, std::sync::atomic::Ordering::SeqCst);

        // Check if command should be forwarded to log replay channels
        // If key's slot matches any replay log slot range, forward command
        if let Some(command) = command_opt {
            if let Some(key) = command.get_key() {
                let slot = rr_core::routing::RoutingTable::slot_for_key(key);
                // Collect matching replay configs (clone tx to avoid holding lock across await)
                let matching_configs: Vec<_> = {
                    let replay_logs = self.replay_logs.read();
                    replay_logs
                        .iter()
                        .filter_map(|replay_config| {
                            let (slot_begin, slot_end) = replay_config.slot_range;
                            if slot >= slot_begin && slot < slot_end {
                                Some((replay_config.tx.clone(), replay_config.task_id.clone()))
                            } else {
                                None
                            }
                        })
                        .collect()
                };

                // Send to matching channels (without holding the lock)
                for (tx, task_id) in matching_configs {
                    let cmd_clone = command.clone();
                    if let Err(e) = tx.send((index, term, cmd_clone)).await {
                        warn!("Failed to send command to log replay channel at index {} for task {}: {}",
                    index, task_id, e
                );
                    } else {
                        debug!(
                    "Forwarded command for key {:?} (slot {}) to log replay at index {} for task {}",
                    key, slot, index, task_id
                );
                    }
                }
            }
        }
        Ok(())
    }

    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        _config: ClusterConfig,
        request_id: RequestId,
        oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
    ) {
        use crate::snapshot_transfer::SnapshotMetadata;

        let store = self.store.clone();
        let apply_index = self.apply_index.clone();
        let term_atomic = self.term.clone();
        let apply_results = self.apply_results.clone();
        let from = from.clone();
        let config = self.config.clone();

        // Parse snapshot metadata from data
        let metadata = match SnapshotMetadata::deserialize(&data) {
            Ok(meta) => meta,
            Err(e) => {
                warn!(
                    "Failed to parse snapshot metadata for {} at index {}: {}",
                    from, index, e
                );
                let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                    anyhow::anyhow!("Failed to parse snapshot metadata: {}", e),
                ))));
                return;
            }
        };

        info!(
            "Installing snapshot for {} at index {}, term {}, request_id: {:?}, transfer_id: {}",
            from, index, term, request_id, metadata.transfer_id
        );

        // Spawn async task to pull snapshot data and restore
        let routing_table = self.routing_table.clone();
        let from_clone = from.clone();
        tokio::spawn(async move {
            // Create gRPC client to leader
            let client =
                match Self::create_snapshot_client(&routing_table, &from_clone, &config).await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(e))));
                        return;
                    }
                };

            // Create blocking channel for receiving compressed chunks
            let (blocking_tx, blocking_rx) = std::sync::mpsc::channel();

            // Spawn async task to receive chunks from gRPC stream
            let receive_handle = Self::pull_snapshot_chunks_async(
                client,
                &metadata,
                index,
                term,
                request_id,
                &config,
                blocking_tx,
            );

            // Restore from snapshot entries in blocking context
            let restore_result =
                crate::snapshot_restore::restore_snapshot_from_chunks(store, blocking_rx).await;

            // Wait for receive task
            let _ = receive_handle.await;

            // Handle restore result
            crate::snapshot_restore::handle_snapshot_restore_result(
                restore_result,
                apply_index,
                term_atomic,
                apply_results,
                from,
                index,
                term,
                oneshot,
            );
        });
    }

    async fn create_snapshot(
        &self,
        from: &RaftId,
        config: ClusterConfig,
        saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        // Flush store in blocking thread pool to avoid blocking async runtime
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.flush())
            .await
            .map_err(|e| {
                raft::StorageError::SnapshotCreationFailed(format!(
                    "Failed to join flush task: {}",
                    e
                ))
            })?
            .map_err(|e| {
                raft::StorageError::SnapshotCreationFailed(format!("Failed to flush store: {}", e))
            })?;

        let apply_index = self.apply_index().load(std::sync::atomic::Ordering::SeqCst);
        let term = self.term().load(std::sync::atomic::Ordering::SeqCst);

        saver
            .save_snapshot(
                from,
                raft::Snapshot {
                    config,
                    index: apply_index,
                    term: term,
                    data: Vec::new(),
                },
            )
            .await?;

        Ok((apply_index, term))
    }

    async fn client_response(
        &self,
        _from: &RaftId,
        request_id: RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()> {
        // Notify waiting clients when Raft commit completes
        if let Some(ref pending) = self.pending_requests {
            let store_result = match result {
                Ok(index) => {
                    let current_apply_index =
                        self.apply_index.load(std::sync::atomic::Ordering::SeqCst);
                    let mut cache = self.apply_results.lock();

                    // Process queue: clean up expired entries and find matching entry in one pass
                    // Since index is monotonically increasing, we can process from front
                    let mut found_result = None;
                    while let Some((cached_index, cached_result)) = cache.front().cloned() {
                        if cached_index < current_apply_index {
                            // Expired entry, remove it
                            cache.pop_front();
                        } else if cached_index == index {
                            // Found matching entry, remove and return it
                            found_result = Some(cached_result);
                            cache.pop_front();
                            break;
                        } else {
                            // cached_index >= current_apply_index && cached_index != index
                            // Since queue is ordered and index is monotonically increasing,
                            // if we haven't found the match yet, it doesn't exist
                            break;
                        }
                    }

                    found_result.unwrap_or(StoreApplyResult::Ok)
                }
                Err(e) => StoreApplyResult::Error(StoreError::Internal(format!("{:?}", e))),
            };
            pending.complete(request_id, store_result);
        }
        Ok(())
    }

    async fn read_index_response(
        &self,
        _from: &RaftId,
        _request_id: raft::RequestId,
        _result: ClientResult<u64>,
    ) -> ClientResult<()> {
        Ok(())
    }
}

// Implement Network trait
#[async_trait]
impl Network for KVStateMachine {
    async fn send_request_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::RequestVoteRequest,
    ) -> raft::RpcResult<()> {
        self.network
            .send_request_vote_request(from, target, args)
            .await
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::RequestVoteResponse,
    ) -> raft::RpcResult<()> {
        self.network
            .send_request_vote_response(from, target, args)
            .await
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesRequest,
    ) -> raft::RpcResult<()> {
        self.network
            .send_append_entries_request(from, target, args)
            .await
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesResponse,
    ) -> raft::RpcResult<()> {
        self.network
            .send_append_entries_response(from, target, args)
            .await
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotRequest,
    ) -> raft::RpcResult<()> {
        self.network
            .send_install_snapshot_request(from, target, args)
            .await
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotResponse,
    ) -> raft::RpcResult<()> {
        self.network
            .send_install_snapshot_response(from, target, args)
            .await
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_pre_vote_request(from, target, args).await
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> raft::RpcResult<()> {
        self.network
            .send_pre_vote_response(from, target, args)
            .await
    }
}

// Implement Storage trait (empty implementation, delegates to self.storage)
#[async_trait]
impl Storage for KVStateMachine {}

// Implement HardStateStorage
#[async_trait]
impl HardStateStorage for KVStateMachine {
    async fn save_hard_state(
        &self,
        from: &RaftId,
        hard_state: raft::HardState,
    ) -> StorageResult<()> {
        self.storage.save_hard_state(from, hard_state).await
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<raft::HardState>> {
        self.storage.load_hard_state(from).await
    }
}

// Implement SnapshotStorage
#[async_trait]
impl SnapshotStorage for KVStateMachine {
    async fn save_snapshot(&self, from: &RaftId, snap: raft::Snapshot) -> StorageResult<()> {
        self.storage.save_snapshot(from, snap).await
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<raft::Snapshot>> {
        use crate::snapshot_transfer::{SnapshotMetadata, SnapshotTransferManager};
        use parking_lot::RwLock;
        use std::sync::Arc;

        // Load existing snapshot to get cluster_config
        let existing_snapshot = self.storage.load_snapshot(from).await?;

        // Get latest apply_index and term from state machine
        // Note: Raft state machine is single-threaded, safe to access here
        let apply_index = self.apply_index().load(std::sync::atomic::Ordering::SeqCst);
        let term = self.term().load(std::sync::atomic::Ordering::SeqCst);

        // Generate transfer ID
        let transfer_id = SnapshotTransferManager::generate_transfer_id();

        // Get cluster_config from existing snapshot or use default
        let cluster_config = existing_snapshot
            .as_ref()
            .map(|s| s.config.clone())
            .unwrap_or_else(|| {
                // Default config if no existing snapshot
                raft::ClusterConfig::simple(std::collections::HashSet::new(), 0)
            });

        // Create snapshot metadata
        let metadata = SnapshotMetadata {
            index: apply_index,
            term,
            config: cluster_config.clone(),
            transfer_id: transfer_id.clone(),
            raft_group_id: from.group.clone(),
        };

        // Serialize metadata
        let metadata_bytes = metadata.serialize().map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!(
                "Failed to serialize snapshot metadata: {}",
                e.to_string()
            ))
        })?;

        // Create snapshot directory and file path (use configured transfer_dir)
        let snapshot_dir = self.config.snapshot.transfer_dir.join(&transfer_id);
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!(
                "Failed to create snapshot directory: {}",
                e.to_string()
            ))
        })?;
        let snapshot_file = snapshot_dir.join("snapshot.dat");

        // Create chunk index
        let chunk_index = Arc::new(RwLock::new(crate::snapshot_transfer::ChunkIndex::new()));

        // Create channel for receiving snapshot data
        // This channel will be used by create_snapshot to send snapshot entries
        let (tx, rx) = std::sync::mpsc::sync_channel(128);

        // Synchronously create snapshot object in load_snapshot context
        // This ensures state consistency - snapshot object (RocksDB snapshot + Memory COW)
        // is created before returning metadata, preventing state changes
        // Note: Raft state machine is single-threaded, safe to create snapshot here
        use storage::SnapshotStore;
        let shard_id_str = from.group.clone();

        info!(
            "Creating snapshot object for shard {} transfer {} (synchronous)",
            shard_id_str, transfer_id
        );

        // Block on create_snapshot to ensure snapshot object is created synchronously
        // create_snapshot uses spawn_blocking internally and signals via oneshot when snapshot is ready
        // We block here to ensure snapshot object is created in synchronous context
        self.store
            .create_snapshot(&shard_id_str, tx, None)
            .await
            .map_err(|e| {
                raft::StorageError::SnapshotCreationFailed(format!(
                    "Failed to create snapshot object: {}",
                    e
                ))
            })?;

        info!(
            "Snapshot object created for shard {} transfer {}",
            shard_id_str, transfer_id
        );

        // Register transfer as active (snapshot object is already created)
        // The snapshot object is now consistent and won't change
        // Transfer can start immediately even while file is still being generated
        self.snapshot_transfer_manager.register_transfer(
            transfer_id.clone(),
            crate::snapshot_transfer::SnapshotTransferState::new(
                snapshot_file.clone(),
                chunk_index.clone(),
            ),
        );

        // Spawn background task to generate snapshot file from channel
        // Snapshot object is already created, now we just need to write data to file
        let transfer_id_clone = transfer_id.clone();
        let shard_id_clone = from.group.clone();
        let snapshot_transfer_manager = self.snapshot_transfer_manager.clone();
        let snapshot_config = self.config.snapshot.clone();

        info!(
            "Spawning background task to generate snapshot file for shard {} transfer {}",
            shard_id_clone, transfer_id_clone
        );

        tokio::spawn(async move {
            // Generate snapshot file in background using channel
            if let Err(e) = crate::snapshot_transfer::generate_snapshot_file_async(
                &shard_id_clone,
                &transfer_id_clone,
                &snapshot_transfer_manager,
                rx,
                snapshot_config,
            )
            .await
            {
                error!(
                    "Failed to generate snapshot file for transfer {}: {}",
                    transfer_id_clone, e
                );
                snapshot_transfer_manager.mark_transfer_failed(&transfer_id_clone, e.to_string());
            }
        });

        // Return snapshot with metadata in data field
        Ok(Some(raft::Snapshot {
            index: apply_index,
            term,
            data: metadata_bytes,
            config: cluster_config,
        }))
    }
}

// Implement ClusterConfigStorage
#[async_trait]
impl ClusterConfigStorage for KVStateMachine {
    async fn save_cluster_config(&self, from: &RaftId, conf: ClusterConfig) -> StorageResult<()> {
        self.storage.save_cluster_config(from, conf).await
    }

    async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig> {
        self.storage.load_cluster_config(from).await
    }
}

// Implement LogEntryStorage
#[async_trait]
impl LogEntryStorage for KVStateMachine {
    async fn append_log_entries(
        &self,
        from: &RaftId,
        entries: &[raft::LogEntry],
    ) -> StorageResult<()> {
        self.storage.append_log_entries(from, entries).await
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<raft::LogEntry>> {
        self.storage.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        self.storage.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_suffix(from, idx).await
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_prefix(from, idx).await
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        self.storage.get_last_log_index(from).await
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        self.storage.get_log_term(from, idx).await
    }
}

// Implement TimerService
impl TimerService for KVStateMachine {
    fn del_timer(&self, _from: &RaftId, timer_id: raft::TimerId) {
        self.timers.del_timer(timer_id);
    }

    fn set_leader_transfer_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        // Use configured timeout if duration is zero, otherwise use the passed duration
        let timeout = if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
            self.config.raft.leader_transfer_timeout()
        } else {
            dur
        };
        self.timers
            .add_timer(from, Event::LeaderTransferTimeout, timeout)
    }

    fn set_election_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        // Use configured timeout if duration is zero, otherwise use the passed duration
        let timeout = if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
            self.config.raft.election_timeout()
        } else {
            dur
        };
        self.timers.add_timer(from, Event::ElectionTimeout, timeout)
    }

    fn set_heartbeat_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        // Use configured timeout if duration is zero, otherwise use the passed duration
        let timeout = if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
            self.config.raft.heartbeat_timeout()
        } else {
            dur
        };
        self.timers
            .add_timer(from, Event::HeartbeatTimeout, timeout)
    }

    fn set_apply_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        // Use configured timeout if duration is zero, otherwise use the passed duration
        let timeout = if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
            self.config.raft.apply_log_timeout()
        } else {
            dur
        };
        self.timers.add_timer(from, Event::ApplyLogTimeout, timeout)
    }

    fn set_config_change_timer(&self, from: &RaftId, dur: Duration) -> raft::TimerId {
        // Use configured timeout if duration is zero, otherwise use the passed duration
        let timeout = if dur.as_secs() == 0 && dur.subsec_nanos() == 0 {
            self.config.raft.config_change_timeout()
        } else {
            dur
        };
        self.timers
            .add_timer(from, Event::ConfigChangeTimeout, timeout)
    }
}

// Implement EventNotify
#[async_trait]
impl raft::traits::EventNotify for KVStateMachine {
    async fn on_state_changed(
        &self,
        _from: &RaftId,
        _role: raft::Role,
    ) -> Result<(), raft::error::StateChangeError> {
        Ok(())
    }

    async fn on_node_removed(
        &self,
        _node_id: &RaftId,
    ) -> Result<(), raft::error::StateChangeError> {
        Ok(())
    }
}

// Implement EventSender
#[async_trait]
impl raft::traits::EventSender for KVStateMachine {
    async fn send(&self, _target: RaftId, _event: Event) -> anyhow::Result<()> {
        Ok(())
    }
}

// Implement RaftCallbacks (marker trait)
impl raft::RaftCallbacks for KVStateMachine {}
