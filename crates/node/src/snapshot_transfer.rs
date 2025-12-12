//! Snapshot transfer implementation
//!
//! Handles pull-based snapshot transfer where Follower actively pulls snapshot data
//! from Leader after receiving InstallSnapshotRequest.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use raft::ClusterConfig;
use storage::memory::DataCow;

/// Snapshot metadata for pull-based transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Snapshot index
    pub index: u64,
    /// Snapshot term
    pub term: u64,
    /// Cluster configuration
    pub config: ClusterConfig,
    /// Transfer ID (unique identifier for this transfer)
    pub transfer_id: String,
    /// Raft group ID (shard ID in multi-raft context)
    pub raft_group_id: String,
}

impl SnapshotMetadata {
    /// Serialize metadata to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    /// Deserialize metadata from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        bincode::serde::decode_from_slice(data, bincode::config::standard())
            .map(|(metadata, _)| metadata)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    /// Check if data is metadata (magic number)
    pub fn is_metadata(data: &[u8]) -> bool {
        // Use magic number to identify metadata
        data.len() >= 4 && &data[0..4] == b"SMET"  // Snapshot METadata
    }
}

/// Snapshot object (created synchronously to ensure consistency)
/// Contains references to RocksDB data and Memory COW snapshot
#[derive(Clone)]
pub struct SnapshotObject {
    /// Shard ID
    pub shard_id: String,
    /// RocksDB entries for this shard (String, Hash data)
    pub rocksdb_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Memory COW snapshot (List, Set, ZSet data)
    pub memory_snapshot: Option<Arc<parking_lot::RwLock<std::collections::HashMap<Vec<u8>, storage::memory::DataCow>>>>,
}

impl std::fmt::Debug for SnapshotObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotObject")
            .field("shard_id", &self.shard_id)
            .field("rocksdb_entries_count", &self.rocksdb_entries.len())
            .field("has_memory_snapshot", &self.memory_snapshot.is_some())
            .finish()
    }
}

/// Snapshot transfer state
#[derive(Debug, Clone)]
pub enum SnapshotTransferState {
    /// Transfer is being prepared (snapshot file is being generated)
    Preparing {
        /// Snapshot object (created synchronously)
        snapshot: SnapshotObject,
    },
    /// Transfer is ready (snapshot file is ready for pull)
    Ready {
        /// Snapshot file path
        snapshot_path: std::path::PathBuf,
        /// Total size in bytes
        total_size: u64,
    },
    /// Transfer is in progress
    InProgress {
        snapshot_path: std::path::PathBuf,
        total_size: u64,
    },
    /// Transfer completed
    Completed,
    /// Transfer failed
    Failed(String),
}

/// Snapshot transfer manager
/// Manages snapshot transfers and tracks their state
pub struct SnapshotTransferManager {
    /// Active transfers: transfer_id -> state
    transfers: Arc<RwLock<HashMap<String, SnapshotTransferState>>>,
}

impl SnapshotTransferManager {
    pub fn new() -> Self {
        Self {
            transfers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate a new transfer ID
    pub fn generate_transfer_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// Register a new transfer
    pub fn register_transfer(&self, transfer_id: String, state: SnapshotTransferState) {
        let transfer_id_clone = transfer_id.clone();
        self.transfers.write().insert(transfer_id, state);
        info!("Registered snapshot transfer: {}", transfer_id_clone);
    }

    /// Update transfer state
    pub fn update_transfer_state(&self, transfer_id: &str, state: SnapshotTransferState) {
        let mut transfers = self.transfers.write();
        if transfers.contains_key(transfer_id) {
            transfers.insert(transfer_id.to_string(), state);
            info!("Updated snapshot transfer {} state", transfer_id);
        } else {
            warn!("Transfer {} not found", transfer_id);
        }
    }

    /// Get transfer state
    pub fn get_transfer_state(&self, transfer_id: &str) -> Option<SnapshotTransferState> {
        self.transfers.read().get(transfer_id).cloned()
    }

    /// Remove completed or failed transfer
    pub fn remove_transfer(&self, transfer_id: &str) {
        self.transfers.write().remove(transfer_id);
        info!("Removed snapshot transfer: {}", transfer_id);
    }

    /// Clean up old transfers (older than specified duration)
    pub fn cleanup_old_transfers(&self, _max_age: std::time::Duration) {
        // TODO: Implement cleanup based on timestamp
        // For now, just log
        let count = self.transfers.read().len();
        if count > 100 {
            warn!("Snapshot transfer manager has {} active transfers, consider cleanup", count);
        }
    }
}

impl Default for SnapshotTransferManager {
    fn default() -> Self {
        Self::new()
    }
}

