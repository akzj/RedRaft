//! File-based snapshot storage implementation.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, error, info};

use crate::cluster_config::ClusterConfig;
use crate::message::Snapshot;
use crate::traits::{SnapshotStorage, StorageResult};
use crate::error::StorageError;
use crate::RaftId;

/// Snapshot storage configuration options.
#[derive(Debug, Clone)]
pub struct SnapshotStorageOptions {
    /// Base directory for storing snapshots.
    pub base_dir: PathBuf,
    /// Whether to verify checksums when loading snapshots.
    pub verify_checksum: bool,
    /// Whether to sync data to disk after writes.
    pub sync_on_write: bool,
}

impl Default for SnapshotStorageOptions {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./data/snapshots"),
            verify_checksum: true,
            sync_on_write: true,
        }
    }
}

impl SnapshotStorageOptions {
    /// Create options with a custom base directory.
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }
}

/// Snapshot metadata stored separately for quick access.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotMeta {
    /// Snapshot index (last included index).
    pub index: u64,
    /// Snapshot term (last included term).
    pub term: u64,
    /// Cluster configuration at snapshot time.
    pub config: ClusterConfig,
    /// SHA256 checksum of the data file.
    pub checksum: String,
    /// Size of the data file in bytes.
    pub data_size: u64,
    /// Timestamp when snapshot was created.
    pub created_at: u64,
}

/// File-based snapshot storage for Raft consensus.
///
/// Provides persistent storage for Raft snapshots with:
/// - Atomic writes using temporary files
/// - Checksum verification for data integrity
/// - Support for multiple Raft groups
///
/// Note: No in-memory caching is used since snapshots can be very large
/// (tens or hundreds of MB). Each load reads directly from disk.
#[derive(Clone)]
pub struct FileSnapshotStorage {
    options: SnapshotStorageOptions,
}

impl FileSnapshotStorage {
    /// Create a new file-based snapshot storage.
    pub fn new(options: SnapshotStorageOptions) -> StorageResult<Self> {
        // Ensure base directory exists
        fs::create_dir_all(&options.base_dir).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to create snapshot base directory {:?}: {}",
                options.base_dir,
                e
            )))
        })?;

        info!(
            "FileSnapshotStorage initialized with base_dir: {:?}",
            options.base_dir
        );

        Ok(Self { options })
    }

    /// Create with default options.
    pub fn with_default_options() -> StorageResult<Self> {
        Self::new(SnapshotStorageOptions::default())
    }

    /// Get the directory path for a specific Raft group's snapshots.
    fn get_snapshot_dir(&self, raft_id: &RaftId) -> PathBuf {
        let dir_name = format!("{}_{}", raft_id.group, raft_id.node);
        self.options.base_dir.join(dir_name).join("snapshot")
    }

    /// Get the metadata file path.
    fn get_meta_path(&self, raft_id: &RaftId) -> PathBuf {
        self.get_snapshot_dir(raft_id).join("meta.json")
    }

    /// Get the data file path.
    fn get_data_path(&self, raft_id: &RaftId) -> PathBuf {
        self.get_snapshot_dir(raft_id).join("data.bin")
    }

    /// Get the checksum file path.
    fn get_checksum_path(&self, raft_id: &RaftId) -> PathBuf {
        self.get_snapshot_dir(raft_id).join("checksum.sha256")
    }

    /// Calculate SHA256 checksum of data.
    pub(crate) fn calculate_checksum(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Verify checksum of data.
    pub(crate) fn verify_checksum(data: &[u8], expected: &str) -> bool {
        Self::calculate_checksum(data) == expected
    }

    /// Write snapshot atomically using temporary files.
    fn write_snapshot_atomic(&self, raft_id: &RaftId, snapshot: &Snapshot) -> StorageResult<()> {
        let snapshot_dir = self.get_snapshot_dir(raft_id);

        // Ensure directory exists
        fs::create_dir_all(&snapshot_dir).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to create snapshot directory {:?}: {}",
                snapshot_dir,
                e
            )))
        })?;

        // Calculate checksum
        let checksum = Self::calculate_checksum(&snapshot.data);

        // Create metadata
        let meta = SnapshotMeta {
            index: snapshot.index,
            term: snapshot.term,
            config: snapshot.config.clone(),
            checksum: checksum.clone(),
            data_size: snapshot.data.len() as u64,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Write data file first (temporary)
        let data_path = self.get_data_path(raft_id);
        let data_tmp_path = data_path.with_extension("bin.tmp");

        let mut data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&data_tmp_path)
            .map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to create temp data file {:?}: {}",
                    data_tmp_path,
                    e
                )))
            })?;

        data_file.write_all(&snapshot.data).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to write snapshot data: {}",
                e
            )))
        })?;

        if self.options.sync_on_write {
            data_file.sync_all().map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to sync data file: {}",
                    e
                )))
            })?;
        }

        // Write metadata file (temporary)
        let meta_path = self.get_meta_path(raft_id);
        let meta_tmp_path = meta_path.with_extension("json.tmp");

        let meta_json = serde_json::to_string_pretty(&meta).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to serialize metadata: {}",
                e
            )))
        })?;

        let mut meta_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&meta_tmp_path)
            .map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to create temp meta file {:?}: {}",
                    meta_tmp_path,
                    e
                )))
            })?;

        meta_file.write_all(meta_json.as_bytes()).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to write metadata: {}",
                e
            )))
        })?;

        if self.options.sync_on_write {
            meta_file.sync_all().map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to sync meta file: {}",
                    e
                )))
            })?;
        }

        // Write checksum file
        let checksum_path = self.get_checksum_path(raft_id);
        fs::write(&checksum_path, &checksum).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to write checksum file: {}",
                e
            )))
        })?;

        // Atomic rename: data first, then metadata
        fs::rename(&data_tmp_path, &data_path).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to rename data file: {}",
                e
            )))
        })?;

        fs::rename(&meta_tmp_path, &meta_path).map_err(|e| {
            // Try to clean up if metadata rename fails
            let _ = fs::remove_file(&data_path);
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to rename meta file: {}",
                e
            )))
        })?;

        debug!(
            "Snapshot saved for {:?}: index={}, term={}, size={}",
            raft_id, snapshot.index, snapshot.term, snapshot.data.len()
        );

        Ok(())
    }

    /// Read snapshot from disk.
    fn read_snapshot(&self, raft_id: &RaftId) -> StorageResult<Option<Snapshot>> {
        let meta_path = self.get_meta_path(raft_id);
        let data_path = self.get_data_path(raft_id);

        // Check if snapshot exists
        if !meta_path.exists() || !data_path.exists() {
            return Ok(None);
        }

        // Read metadata
        let meta_content = fs::read_to_string(&meta_path).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to read metadata file {:?}: {}",
                meta_path,
                e
            )))
        })?;

        let meta: SnapshotMeta = serde_json::from_str(&meta_content).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to parse metadata: {}",
                e
            )))
        })?;

        // Read data
        let mut data_file = File::open(&data_path).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to open data file {:?}: {}",
                data_path,
                e
            )))
        })?;

        let mut data = Vec::with_capacity(meta.data_size as usize);
        data_file.read_to_end(&mut data).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to read snapshot data: {}",
                e
            )))
        })?;

        // Verify checksum if enabled
        if self.options.verify_checksum {
            if !Self::verify_checksum(&data, &meta.checksum) {
                error!(
                    "Snapshot checksum verification failed for {:?}",
                    raft_id
                );
                return Err(StorageError::DataCorruption(meta.index));
            }
        }

        let snapshot = Snapshot {
            index: meta.index,
            term: meta.term,
            data,
            config: meta.config,
        };

        debug!(
            "Snapshot loaded for {:?}: index={}, term={}",
            raft_id, snapshot.index, snapshot.term
        );

        Ok(Some(snapshot))
    }

    /// Delete snapshot for a Raft group.
    pub fn delete_snapshot(&self, raft_id: &RaftId) -> StorageResult<()> {
        let snapshot_dir = self.get_snapshot_dir(raft_id);

        if snapshot_dir.exists() {
            fs::remove_dir_all(&snapshot_dir).map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to delete snapshot directory {:?}: {}",
                    snapshot_dir,
                    e
                )))
            })?;

            info!("Snapshot deleted for {:?}", raft_id);
        }

        Ok(())
    }

    /// Get snapshot metadata without loading data.
    pub fn get_snapshot_meta(&self, raft_id: &RaftId) -> StorageResult<Option<(u64, u64)>> {
        let meta_path = self.get_meta_path(raft_id);

        if !meta_path.exists() {
            return Ok(None);
        }

        let meta_content = fs::read_to_string(&meta_path).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to read metadata file {:?}: {}",
                meta_path,
                e
            )))
        })?;

        let meta: SnapshotMeta = serde_json::from_str(&meta_content).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to parse metadata: {}",
                e
            )))
        })?;

        Ok(Some((meta.index, meta.term)))
    }

    /// List all Raft groups with snapshots.
    pub fn list_snapshots(&self) -> StorageResult<Vec<RaftId>> {
        let mut result = Vec::new();

        let entries = fs::read_dir(&self.options.base_dir).map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Failed to read snapshot directory: {}",
                e
            )))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                StorageError::Io(Arc::new(anyhow::anyhow!(
                    "Failed to read directory entry: {}",
                    e
                )))
            })?;

            let name = entry.file_name().to_string_lossy().to_string();
            if let Some((group, node)) = name.split_once('_') {
                let raft_id = RaftId::new(group.to_string(), node.to_string());
                let meta_path = self.get_meta_path(&raft_id);
                if meta_path.exists() {
                    result.push(raft_id);
                }
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl SnapshotStorage for FileSnapshotStorage {
    async fn save_snapshot(&self, from: &RaftId, snap: Snapshot) -> StorageResult<()> {
        // Clone data for async operation
        let storage = self.clone();
        let raft_id = from.clone();

        // Perform I/O in blocking task
        tokio::task::spawn_blocking(move || {
            storage.write_snapshot_atomic(&raft_id, &snap)
        })
        .await
        .map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Snapshot save task failed: {}",
                e
            )))
        })?
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<Snapshot>> {
        // Clone data for async operation
        let storage = self.clone();
        let raft_id = from.clone();

        // Perform I/O in blocking task
        tokio::task::spawn_blocking(move || {
            storage.read_snapshot(&raft_id)
        })
        .await
        .map_err(|e| {
            StorageError::Io(Arc::new(anyhow::anyhow!(
                "Snapshot load task failed: {}",
                e
            )))
        })?
    }
}

