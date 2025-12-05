//! Unified file-based storage implementation for Raft consensus.
//!
//! This module provides a complete `Storage` implementation that combines:
//! - Log entry storage (via `ManagedLogEntryStore`)
//! - Hard state storage (via `ManagedLogEntryStore`)
//! - Cluster config storage (via `ManagedLogEntryStore`)
//! - Snapshot storage (via `FileSnapshotStorage`)

use std::path::PathBuf;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use crate::cluster_config::ClusterConfig;
use crate::error::StorageError;
use crate::message::{HardState, LogEntry, Snapshot};
use crate::traits::{
    ClusterConfigStorage, HardStateStorage, LogEntryStorage, SnapshotStorage, Storage,
    StorageResult,
};
use crate::RaftId;

use super::log::{LogEntryOpRequest, ManagedLogEntryStore, ManagedLogEntryStoreOptions};
use super::snapshot::{FileSnapshotStorage, SnapshotStorageOptions};

/// Configuration options for the unified file storage.
#[derive(Debug, Clone)]
pub struct FileStorageOptions {
    /// Base directory for all storage data.
    /// Subdirectories will be created:
    /// - `{base_dir}/logs` for log segments
    /// - `{base_dir}/snapshots` for snapshots
    pub base_dir: PathBuf,

    /// Maximum log segment size in bytes before rotation (default: 64MB)
    pub max_segment_size: u64,

    /// Maximum number of I/O threads for log operations
    pub max_io_threads: usize,

    /// Whether to sync data to disk after each write
    pub sync_on_write: bool,

    /// Batch size for processing log operations
    pub batch_size: usize,

    /// Cache size for recent log entries
    pub cache_entries_size: usize,

    /// Minimum free disk space to maintain (bytes)
    pub min_free_disk_space: u64,

    /// Whether to verify snapshot checksums when loading
    pub verify_snapshot_checksum: bool,
}

impl Default for FileStorageOptions {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./data"),
            max_segment_size: 64 * 1024 * 1024, // 64MB
            max_io_threads: 4,
            sync_on_write: true,
            batch_size: 100,
            cache_entries_size: 1000,
            min_free_disk_space: 100 * 1024 * 1024, // 100MB
            verify_snapshot_checksum: true,
        }
    }
}

impl FileStorageOptions {
    /// Create options with a custom base directory.
    pub fn with_base_dir<P: Into<PathBuf>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.into(),
            ..Default::default()
        }
    }

    /// Get the log directory path.
    pub fn log_dir(&self) -> PathBuf {
        self.base_dir.join("logs")
    }

    /// Get the snapshot directory path.
    pub fn snapshot_dir(&self) -> PathBuf {
        self.base_dir.join("snapshots")
    }
}

/// Unified file-based storage for Raft consensus.
///
/// This struct implements the complete `Storage` trait by composing:
/// - `ManagedLogEntryStore` for log entries, hard state, and cluster config
/// - `FileSnapshotStorage` for snapshots
///
/// # Example
///
/// ```rust,ignore
/// use raft::storage::FileStorage;
///
/// let options = FileStorageOptions::with_base_dir("./my_raft_data");
/// let (storage, rx) = FileStorage::new(options)?;
///
/// // Start the background processor
/// storage.start(rx);
///
/// // Now use storage for Raft operations
/// storage.save_hard_state(&raft_id, hard_state).await?;
/// ```
#[derive(Clone)]
pub struct FileStorage {
    log_store: ManagedLogEntryStore,
    snapshot_store: FileSnapshotStorage,
}

impl FileStorage {
    /// Create a new unified file storage.
    ///
    /// Returns the storage instance and a receiver for the background operation processor.
    /// You must call `start(rx)` to begin processing operations.
    pub fn new(
        options: FileStorageOptions,
    ) -> Result<(Self, mpsc::UnboundedReceiver<LogEntryOpRequest>), StorageError> {
        // Create log store options
        let log_options = ManagedLogEntryStoreOptions {
            dir: options.log_dir(),
            max_segment_size: options.max_segment_size,
            max_io_threads: options.max_io_threads,
            sync_on_write: options.sync_on_write,
            batch_size: options.batch_size,
            cache_entries_size: options.cache_entries_size,
            min_free_disk_space: options.min_free_disk_space,
        };

        // Create snapshot store options
        let snapshot_options = SnapshotStorageOptions {
            base_dir: options.snapshot_dir(),
            verify_checksum: options.verify_snapshot_checksum,
            sync_on_write: options.sync_on_write,
        };

        // Initialize log store
        let (log_store, rx) = ManagedLogEntryStore::new(log_options)?;

        // Initialize snapshot store
        let snapshot_store = FileSnapshotStorage::new(snapshot_options)?;

        info!(
            "FileStorage initialized: base_dir={:?}",
            options.base_dir
        );

        Ok((
            Self {
                log_store,
                snapshot_store,
            },
            rx,
        ))
    }

    /// Start the background operation processor.
    ///
    /// This must be called before performing any write operations.
    pub fn start(&self, receiver: mpsc::UnboundedReceiver<LogEntryOpRequest>) {
        self.log_store.start(receiver);
    }

    /// Get disk usage statistics for log storage.
    pub fn get_log_disk_stats(&self) -> super::log::DiskStats {
        self.log_store.get_disk_stats()
    }

    /// Get total log disk usage in bytes.
    pub fn get_log_disk_usage(&self) -> u64 {
        self.log_store.get_disk_usage()
    }

    /// Get number of log segments.
    pub fn log_segment_count(&self) -> usize {
        self.log_store.segment_count()
    }

    /// Force log segment rotation.
    pub fn force_log_rotation(&self) -> Result<(), StorageError> {
        self.log_store.force_rotation()
    }

    /// Check if disk space is low.
    pub fn is_disk_space_low(&self) -> bool {
        self.log_store.is_disk_space_low()
    }

    /// List all snapshots in storage.
    pub fn list_snapshots(&self) -> StorageResult<Vec<RaftId>> {
        self.snapshot_store.list_snapshots()
    }

    /// Delete a specific snapshot.
    pub fn delete_snapshot(&self, from: &RaftId) -> StorageResult<()> {
        self.snapshot_store.delete_snapshot(from)
    }
}

// Delegate HardStateStorage to log_store
#[async_trait]
impl HardStateStorage for FileStorage {
    async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        self.log_store.save_hard_state(from, hard_state).await
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        self.log_store.load_hard_state(from).await
    }
}

// Delegate LogEntryStorage to log_store
#[async_trait]
impl LogEntryStorage for FileStorage {
    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        self.log_store.append_log_entries(from, entries).await
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        self.log_store.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        self.log_store.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.log_store.truncate_log_suffix(from, idx).await
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.log_store.truncate_log_prefix(from, idx).await
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        self.log_store.get_last_log_index(from).await
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        self.log_store.get_log_term(from, idx).await
    }
}

// Delegate ClusterConfigStorage to log_store
#[async_trait]
impl ClusterConfigStorage for FileStorage {
    async fn save_cluster_config(&self, from: &RaftId, conf: ClusterConfig) -> StorageResult<()> {
        self.log_store.save_cluster_config(from, conf).await
    }

    async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig> {
        self.log_store.load_cluster_config(from).await
    }
}

// Delegate SnapshotStorage to snapshot_store
#[async_trait]
impl SnapshotStorage for FileStorage {
    async fn save_snapshot(&self, from: &RaftId, snap: Snapshot) -> StorageResult<()> {
        self.snapshot_store.save_snapshot(from, snap).await
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<Snapshot>> {
        self.snapshot_store.load_snapshot(from).await
    }
}

// Implement the unified Storage trait
impl Storage for FileStorage {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_raft_id(name: &str) -> RaftId {
        RaftId::new(format!("group_{}", name), format!("node_{}", name))
    }

    #[tokio::test]
    async fn test_file_storage_hard_state() {
        let temp_dir = TempDir::new().unwrap();
        let options = FileStorageOptions::with_base_dir(temp_dir.path());
        let (storage, rx) = FileStorage::new(options).unwrap();

        // Start background processor
        storage.start(rx);

        let raft_id = test_raft_id("1");

        // Initially no hard state
        let result = storage.load_hard_state(&raft_id).await.unwrap();
        assert!(result.is_none());

        // Save hard state
        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 10,
            voted_for: Some(test_raft_id("2")),
        };
        storage
            .save_hard_state(&raft_id, hard_state.clone())
            .await
            .unwrap();

        // Load hard state
        let loaded = storage.load_hard_state(&raft_id).await.unwrap().unwrap();
        assert_eq!(loaded.term, 10);
        assert_eq!(loaded.voted_for, Some(test_raft_id("2")));
    }

    #[tokio::test]
    async fn test_file_storage_cluster_config() {
        let temp_dir = TempDir::new().unwrap();
        let options = FileStorageOptions::with_base_dir(temp_dir.path());
        let (storage, rx) = FileStorage::new(options).unwrap();

        storage.start(rx);

        let raft_id = test_raft_id("1");

        // Initially empty config
        let config = storage.load_cluster_config(&raft_id).await.unwrap();
        assert!(config.voters.is_empty());

        // Save config
        let mut voters = std::collections::HashSet::new();
        voters.insert(test_raft_id("1"));
        voters.insert(test_raft_id("2"));
        voters.insert(test_raft_id("3"));
        let config = ClusterConfig::simple(voters.clone(), 1);

        storage
            .save_cluster_config(&raft_id, config)
            .await
            .unwrap();

        // Load config
        let loaded = storage.load_cluster_config(&raft_id).await.unwrap();
        assert_eq!(loaded.voters.len(), 3);
        assert_eq!(loaded.log_index, 1);
    }

    #[tokio::test]
    async fn test_file_storage_log_entries() {
        let temp_dir = TempDir::new().unwrap();
        let options = FileStorageOptions::with_base_dir(temp_dir.path());
        let (storage, rx) = FileStorage::new(options).unwrap();

        storage.start(rx);

        let raft_id = test_raft_id("1");

        // Append entries
        let entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: b"cmd1".to_vec(),
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                index: 2,
                term: 1,
                command: b"cmd2".to_vec(),
                is_config: false,
                client_request_id: None,
            },
        ];

        storage
            .append_log_entries(&raft_id, &entries)
            .await
            .unwrap();

        // Get entries
        let loaded = storage.get_log_entries(&raft_id, 1, 3).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].index, 1);
        assert_eq!(loaded[1].index, 2);

        // Get last log index
        let (last_idx, last_term) = storage.get_last_log_index(&raft_id).await.unwrap();
        assert_eq!(last_idx, 2);
        assert_eq!(last_term, 1);
    }

    #[tokio::test]
    async fn test_file_storage_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let options = FileStorageOptions::with_base_dir(temp_dir.path());
        let (storage, rx) = FileStorage::new(options).unwrap();

        storage.start(rx);

        let raft_id = test_raft_id("1");

        // Initially no snapshot
        let result = storage.load_snapshot(&raft_id).await.unwrap();
        assert!(result.is_none());

        // Save snapshot
        let snapshot = Snapshot {
            index: 100,
            term: 5,
            data: b"snapshot data".to_vec(),
            config: ClusterConfig::empty(),
        };

        storage
            .save_snapshot(&raft_id, snapshot.clone())
            .await
            .unwrap();

        // Load snapshot
        let loaded = storage.load_snapshot(&raft_id).await.unwrap().unwrap();
        assert_eq!(loaded.index, 100);
        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.data, b"snapshot data".to_vec());
    }

    #[tokio::test]
    async fn test_file_storage_implements_storage_trait() {
        let temp_dir = TempDir::new().unwrap();
        let options = FileStorageOptions::with_base_dir(temp_dir.path());
        let (storage, rx) = FileStorage::new(options).unwrap();

        storage.start(rx);

        // This function accepts any type that implements Storage
        async fn use_storage<S: Storage>(storage: &S, raft_id: &RaftId) {
            let _ = storage.load_hard_state(raft_id).await;
            let _ = storage.load_cluster_config(raft_id).await;
            let _ = storage.load_snapshot(raft_id).await;
            let _ = storage.get_last_log_index(raft_id).await;
        }

        let raft_id = test_raft_id("1");
        use_storage(&storage, &raft_id).await;
    }
}

