//! KV state machine implementation
//!
//! Applies Raft logs to key-value storage, using RedisStore trait for storage

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info, warn};

use raft::{ApplyResult, ClusterConfig, RaftId, RequestId, SnapshotStorage, StateMachine, StorageResult, traits::ClientResult};
use storage::{ApplyResult as StoreApplyResult, RedisStore};
use resp::Command;

use crate::node::PendingRequests;

/// KV state machine
#[derive(Clone)]
pub struct KVStateMachine {
    /// Storage backend (supports memory or persistent storage)
    store: Arc<dyn RedisStore>,
    /// Version number (monotonically increasing)
    version: Arc<std::sync::atomic::AtomicU64>,
    /// Pending request tracker
    pending_requests: Option<PendingRequests>,
    /// Apply result cache (index -> result), used to return actual results in client_response
    apply_results: Arc<parking_lot::Mutex<std::collections::HashMap<u64, StoreApplyResult>>>,
}

impl KVStateMachine {
    /// Create new KV state machine with specified storage backend
    pub fn new(store: Arc<dyn RedisStore>) -> Self {
        Self {
            store,
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: None,
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Create KV state machine with request tracking
    pub fn with_pending_requests(store: Arc<dyn RedisStore>, pending_requests: PendingRequests) -> Self {
        Self {
            store,
            version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            pending_requests: Some(pending_requests),
            apply_results: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Get storage backend reference (for read operations)
    pub fn store(&self) -> &Arc<dyn RedisStore> {
        &self.store
    }

    /// Get key-value pair count
    pub fn size(&self) -> usize {
        self.store.dbsize()
    }

    fn inc_version(&self) {
        self.version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        let command: Command = match bincode::serde::decode_from_slice(&cmd, bincode::config::standard()) {
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

        // Execute command and save result
        let result = self.store.apply(&command);
        
        // Cache result for use in client_response
        self.apply_results.lock().insert(index, result);
        self.inc_version();

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
        let version = self.version.clone();
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
                    // Update version number to snapshot index
                    version.store(index, std::sync::atomic::Ordering::SeqCst);
                    
                    // Clear old apply result cache (results before snapshot are meaningless)
                    apply_results.lock().clear();
                    
                    info!(
                        "Snapshot installed successfully for {} at index {}, {} keys restored",
                        from, index, store.dbsize()
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
        let snapshot_data = self.store.create_snapshot().map_err(|e| {
            raft::StorageError::SnapshotCreationFailed(format!("Failed to create snapshot: {}", e))
        })?;

        // Get current version as snapshot index
        let version = self.version.load(std::sync::atomic::Ordering::SeqCst);
        let last_index = version; // Use version number as index

        // Create snapshot
        let snapshot = raft::Snapshot {
            index: last_index,
            term: 0, // Snapshot does not contain term information
            data: snapshot_data,
            config,
        };

        saver.save_snapshot(from, snapshot).await?;

        info!(
            "Created snapshot for {} at index {}, {} keys",
            from,
            last_index,
            self.size()
        );

        Ok((last_index, 0))
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
                Err(e) => {
                    StoreApplyResult::Error(storage::StoreError::Internal(format!("{:?}", e)))
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage::MemoryStore;
    use resp::Command;

    #[tokio::test]
    async fn test_set_and_get() {
        let store = Arc::new(MemoryStore::default());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let cmd = Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();

        let result = sm.apply_command(&raft_id, 1, 1, command).await;
        assert!(result.is_ok());

        assert_eq!(sm.store().get(b"key1"), Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_del() {
        let store = Arc::new(MemoryStore::default());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        // Insert first
        let cmd = Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        // Delete
        let cmd = Command::Del {
            keys: vec![b"key1".to_vec()],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 2, 1, command).await.unwrap();

        assert_eq!(sm.store().get(b"key1"), None);
    }

    #[tokio::test]
    async fn test_list_operations() {
        let store = Arc::new(MemoryStore::default());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let cmd = Command::RPush {
            key: b"list".to_vec(),
            values: vec![b"a".to_vec(), b"b".to_vec()],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        assert_eq!(
            sm.store().lrange(b"list", 0, -1),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let store = Arc::new(MemoryStore::default());
        let sm = KVStateMachine::new(store);
        let raft_id = RaftId::new("test".to_string(), "node1".to_string());

        let cmd = Command::HSet {
            key: b"hash".to_vec(),
            fvs: vec![(b"field1".to_vec(), b"value1".to_vec())],
        };
        let command = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        sm.apply_command(&raft_id, 1, 1, command).await.unwrap();

        assert_eq!(
            sm.store().hget(b"hash", b"field1"),
            Some(b"value1".to_vec())
        );
    }
}
