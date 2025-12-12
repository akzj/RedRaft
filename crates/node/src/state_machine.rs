//! KV state machine implementation
//!
//! Applies Raft logs to key-value storage, using RedisStore trait for storage

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, warn};

use raft::{
    traits::ClientResult, ApplyResult, ClusterConfig, RaftId, RequestId, SnapshotStorage,
    StateMachine, StorageResult,
};
use resp::Command;
use storage::{
    store::HybridStore,
    traits::{ApplyResult as StoreApplyResult, RedisStore, StoreError},
};

use crate::node::PendingRequests;

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
}

impl KVStateMachine {
    /// Create new KV state machine with specified storage backend
    pub fn new(store: Arc<HybridStore>) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: None,
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Create KV state machine with request tracking
    pub fn with_pending_requests(
        store: Arc<HybridStore>,
        pending_requests: PendingRequests,
    ) -> Self {
        Self {
            store,
            apply_index: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            term: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: Some(pending_requests),
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
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
                        anyhow::anyhow!(e),
                    ))));
                }
            }
        });
    }

    async fn create_snapshot(
        &self,
        from: &RaftId,
        config: ClusterConfig,
        saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        // Extract shard_id from RaftId.group (in multi-raft, group == shard_id)
        let shard_id = &from.group;

        // Only flush data to disk (data is already stored on disk via WAL + Segment)
        self.store.create_snapshot(shard_id).map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!(
                "Failed to flush snapshot for shard {}: {}",
                shard_id, e
            ))
        })?;

        // Get apply_index and term (last applied index and term)
        let apply_index = self.apply_index.load(std::sync::atomic::Ordering::SeqCst);
        let term = self.term.load(std::sync::atomic::Ordering::SeqCst);

        // Create snapshot with empty data (data is already on disk)
        let snapshot = raft::Snapshot {
            index: apply_index,
            term,
            data: Vec::new(), // Empty - data is already on disk (WAL + Segment)
            config,
        };

        saver.save_snapshot(from, snapshot).await?;

        info!(
            "Created snapshot for {} at index {}, term {} (data flushed to disk)",
            from, apply_index, term
        );

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
