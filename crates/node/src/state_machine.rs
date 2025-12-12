//! KV state machine implementation
//!
//! Applies Raft logs to key-value storage, using RedisStore trait for storage

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use raft::{
    message::{PreVoteRequest, PreVoteResponse},
    traits::ClientResult, ApplyResult, ClusterConfig, ClusterConfigStorage, Event,
    HardStateStorage, LogEntryStorage, Network, RaftId, RequestId, SnapshotStorage, StateMachine,
    Storage, StorageResult, TimerService,
};
use resp::Command;
use storage::{
    store::HybridStore,
    traits::{ApplyResult as StoreApplyResult, StoreError, KeyStore, SnapshotStore},
};

use crate::config::Config;
use crate::node::PendingRequests;
use crate::snapshot_transfer::SnapshotTransferManager;

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
    /// Apply result cache (index -> result), used to return actual results in client_response
    apply_results: Arc<parking_lot::Mutex<std::collections::HashMap<u64, StoreApplyResult>>>,
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
}

impl KVStateMachine {
    /// Create KV state machine with all dependencies
    pub fn new(
        store: Arc<HybridStore>,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network>,
        timers: raft::multi_raft_driver::Timers,
        snapshot_transfer_manager: Arc<SnapshotTransferManager>,
        config: Config,
    ) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: None,
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            storage,
            network,
            timers,
            snapshot_transfer_manager,
            config,
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
        config: Config,
    ) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: Some(pending_requests),
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            storage,
            network,
            timers,
            snapshot_transfer_manager,
            config,
        }
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

    /// Update apply_index (called after applying a command)
    fn update_apply_index(&self, index: u64) {
        self.apply_index
            .store(index, std::sync::atomic::Ordering::SeqCst);
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
        // Deserialize to Command
        let command: Command =
            match bincode::serde::decode_from_slice(&cmd, bincode::config::standard()) {
                Ok((cmd, _)) => cmd,
                Err(e) => {
                    warn!("Failed to deserialize command at index {}: {}", index, e);
                    return Err(raft::ApplyError::Internal(format!(
                        "Invalid command: {}",
                        e
                    )));
                }
            };

        debug!(
            "Applying command at index {}, term {}: {:?}",
            index, term, command
        );

        // Execute command with apply_index (for WAL logging) and save result
        let result = self.store.apply_with_index(index, &command);

        // Cache result for use in client_response
        self.apply_results.lock().insert(index, result);

        // Update apply_index and term (this is the last applied index and term)
        self.update_apply_index(index);
        self.term.store(term, std::sync::atomic::Ordering::SeqCst);

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
        let store = self.store.clone();
        let apply_index = self.apply_index.clone();
        let term_atomic = self.term.clone();
        let apply_results = self.apply_results.clone();
        let from = from.clone();

        // Use spawn_blocking to avoid blocking async runtime
        tokio::task::spawn_blocking(move || {
            info!(
                "Installing snapshot for {} at index {}, term {}, request_id: {:?}, data_size: {} bytes",
                from, index, term, request_id, data.len()
            );

            match store.restore_from_snapshot(&data) {
                Ok(()) => {
                    // Update apply_index and term to snapshot values
                    apply_index.store(index, std::sync::atomic::Ordering::SeqCst);
                    term_atomic.store(term, std::sync::atomic::Ordering::SeqCst);

                    // Clear old apply result cache (results before snapshot are meaningless)
                    apply_results.lock().clear();

                    let key_count = store.dbsize().unwrap_or(0);
                    info!(
                        "Snapshot installed successfully for {} at index {}, {} keys restored",
                        from, index, key_count
                    );

                    let _ = oneshot.send(Ok(()));
                }
                Err(e) => {
                    warn!(
                        "Failed to install snapshot for {} at index {}: {}",
                        from, index, e
                    );
                    let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                        anyhow::anyhow!("{}", e),
                    ))));
                }
            }
        });
    }

    async fn create_snapshot(
        &self,
        _from: &RaftId,
        _config: ClusterConfig,
        _saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        // Note: create_snapshot is now async and requires a channel
        // This method is deprecated in favor of the new channel-based approach
        // For now, return a placeholder - actual snapshot creation happens in load_snapshot
        // TODO: Update this method to use the new channel-based approach or remove it
        warn!("create_snapshot called but not implemented - use load_snapshot instead");
        Ok((0, 0))
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
                    // Get apply result from cache
                    self.apply_results
                        .lock()
                        .remove(&index)
                        .unwrap_or(StoreApplyResult::Ok)
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
        self.network.send_request_vote_request(from, target, args).await
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::RequestVoteResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_request_vote_response(from, target, args).await
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_append_entries_request(from, target, args).await
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::AppendEntriesResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_append_entries_response(from, target, args).await
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotRequest,
    ) -> raft::RpcResult<()> {
        self.network.send_install_snapshot_request(from, target, args).await
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::InstallSnapshotResponse,
    ) -> raft::RpcResult<()> {
        self.network.send_install_snapshot_response(from, target, args).await
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
        self.network.send_pre_vote_response(from, target, args).await
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
        use std::sync::Arc;
        use parking_lot::RwLock;

        // Load existing snapshot to get cluster_config
        let existing_snapshot = self.storage.load_snapshot(from).await?;

        // Get latest apply_index and term from state machine
        // Note: Raft state machine is single-threaded, safe to access here
        let apply_index = self
            .apply_index()
            .load(std::sync::atomic::Ordering::SeqCst);
        let term = self
            .term()
            .load(std::sync::atomic::Ordering::SeqCst);

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
                e
            ))
        })?;

        // Create snapshot directory and file path (use configured transfer_dir)
        let snapshot_dir = self.config.snapshot.transfer_dir.join(&transfer_id);
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!(
                "Failed to create snapshot directory: {}",
                e
            ))
        })?;
        let snapshot_file = snapshot_dir.join("snapshot.dat");

        // Create chunk index
        let chunk_index = Arc::new(RwLock::new(
            crate::snapshot_transfer::ChunkIndex::new(),
        ));

        // Register transfer as preparing (will be updated to Ready when file is generated)
        self.snapshot_transfer_manager.register_transfer(
            transfer_id.clone(),
            crate::snapshot_transfer::SnapshotTransferState::Preparing {
                snapshot: crate::snapshot_transfer::SnapshotObject {
                    shard_id: from.group.clone(),
                    rocksdb_entries: Vec::new(),
                    memory_snapshot: None,
                },
                snapshot_path: snapshot_file.clone(),
                chunk_index: chunk_index.clone(),
            },
        );

        // Spawn background task to generate snapshot file using channel
        let transfer_id_clone = transfer_id.clone();
        let shard_id_clone = from.group.clone();
        let snapshot_transfer_manager = self.snapshot_transfer_manager.clone();
        // Clone store - HybridStore implements both RedisStore and SnapshotStore
        let store = self.store.clone();

        let snapshot_config = self.config.snapshot.clone();
        tokio::spawn(async move {
            // Generate snapshot file in background using channel
            if let Err(e) = crate::node::generate_snapshot_file_async(
                &shard_id_clone,
                &transfer_id_clone,
                &snapshot_transfer_manager,
                store,
                snapshot_config,
            )
            .await
            {
                error!(
                    "Failed to generate snapshot file for transfer {}: {}",
                    transfer_id_clone, e
                );
                snapshot_transfer_manager.update_transfer_state(
                    &transfer_id_clone,
                    crate::snapshot_transfer::SnapshotTransferState::Failed(e),
                );
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
        self.timers.add_timer(from, Event::HeartbeatTimeout, timeout)
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
