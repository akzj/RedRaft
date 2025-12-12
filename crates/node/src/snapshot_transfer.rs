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
use tokio::sync::Notify;

use raft::ClusterConfig;

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

/// Chunk metadata (header information)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Chunk index (0-based)
    pub chunk_index: u32,
    /// Uncompressed chunk size in bytes
    pub uncompressed_size: u32,
    /// Compressed chunk size in bytes
    pub compressed_size: u32,
    /// CRC32 checksum of compressed data
    pub crc32: u32,
    /// File offset where this chunk starts (including header)
    pub file_offset: u64,
    /// Whether this is the last chunk
    pub is_last: bool,
}

/// Chunk index information (in-memory)
#[derive(Debug, Clone)]
pub struct ChunkIndex {
    /// Chunk metadata list (ordered by chunk_index)
    pub chunks: Vec<ChunkMetadata>,
    /// Total uncompressed size
    pub total_uncompressed_size: u64,
    /// Total compressed size
    pub total_compressed_size: u64,
    /// Number of chunks generated so far
    pub generated_chunks: u32,
    /// Whether generation is complete
    pub is_complete: bool,
    /// Error message if generation failed
    pub error: Option<String>,
    /// Notify when new chunk is available
    pub notify: Arc<Notify>,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_uncompressed_size: 0,
            total_compressed_size: 0,
            generated_chunks: 0,
            is_complete: false,
            error: None,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Get chunk metadata by index
    pub fn get_chunk(&self, chunk_index: u32) -> Option<&ChunkMetadata> {
        self.chunks.get(chunk_index as usize)
    }

    /// Get the highest available chunk index
    pub fn highest_chunk_index(&self) -> Option<u32> {
        if self.chunks.is_empty() {
            None
        } else {
            Some(self.chunks.len() as u32 - 1)
        }
    }
}

impl Default for ChunkIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot transfer state
#[derive(Debug, Clone)]
pub enum SnapshotTransferState {
    /// Transfer is being prepared (snapshot file is being generated)
    Preparing {
        /// Snapshot object (created synchronously)
        snapshot: SnapshotObject,
        /// Snapshot file path
        snapshot_path: std::path::PathBuf,
        /// Chunk index (in-memory)
        chunk_index: Arc<RwLock<ChunkIndex>>,
    },
    /// Transfer is ready (snapshot file is ready for pull)
    Ready {
        /// Snapshot file path
        snapshot_path: std::path::PathBuf,
        /// Total size in bytes
        total_size: u64,
        /// Chunk index (in-memory)
        chunk_index: Arc<RwLock<ChunkIndex>>,
    },
    /// Transfer is in progress
    InProgress {
        snapshot_path: std::path::PathBuf,
        total_size: u64,
        chunk_index: Arc<RwLock<ChunkIndex>>,
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

    /// Get chunk index for a transfer
    pub fn get_chunk_index(&self, transfer_id: &str) -> Option<Arc<RwLock<ChunkIndex>>> {
        self.transfers.read().get(transfer_id).and_then(|state| {
            match state {
                SnapshotTransferState::Preparing { chunk_index, .. } => Some(chunk_index.clone()),
                SnapshotTransferState::Ready { chunk_index, .. } => Some(chunk_index.clone()),
                SnapshotTransferState::InProgress { chunk_index, .. } => Some(chunk_index.clone()),
                _ => None,
            }
        })
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

/// Read a chunk from snapshot file by index
pub fn read_chunk_from_file(
    snapshot_path: &std::path::Path,
    chunk_index: u32,
    chunk_metadata: &ChunkMetadata,
) -> Result<Vec<u8>, String> {
    use std::io::{Read, Seek, SeekFrom};
    use std::fs::File;
    use crc32fast::Hasher as Crc32Hasher;

    let mut file = File::open(snapshot_path)
        .map_err(|e| format!("Failed to open snapshot file: {}", e))?;

    // Seek to chunk start (skip header)
    // Header format: header_len (u32) + header_bytes
    file.seek(SeekFrom::Start(chunk_metadata.file_offset))
        .map_err(|e| format!("Failed to seek to chunk offset: {}", e))?;

    // Read header length
    let mut header_len_bytes = [0u8; 4];
    file.read_exact(&mut header_len_bytes)
        .map_err(|e| format!("Failed to read header length: {}", e))?;
    let header_len = u32::from_le_bytes(header_len_bytes) as usize;

    // Skip header (we already have metadata)
    file.seek(SeekFrom::Current(header_len as i64))
        .map_err(|e| format!("Failed to skip header: {}", e))?;

    // Read compressed data
    let mut compressed_data = vec![0u8; chunk_metadata.compressed_size as usize];
    file.read_exact(&mut compressed_data)
        .map_err(|e| format!("Failed to read compressed data: {}", e))?;

    // Verify CRC32
    let mut hasher = Crc32Hasher::new();
    hasher.update(&compressed_data);
    let calculated_crc32 = hasher.finalize();
    if calculated_crc32 != chunk_metadata.crc32 {
        return Err(format!(
            "CRC32 mismatch for chunk {}: expected {}, got {}",
            chunk_index, chunk_metadata.crc32, calculated_crc32
        ));
    }

    // Decompress
    let decompressed = zstd::decode_all(compressed_data.as_slice())
        .map_err(|e| format!("Failed to decompress chunk: {}", e))?;

    if decompressed.len() != chunk_metadata.uncompressed_size as usize {
        return Err(format!(
            "Decompressed size mismatch for chunk {}: expected {}, got {}",
            chunk_index,
            chunk_metadata.uncompressed_size,
            decompressed.len()
        ));
    }

    Ok(decompressed)
}

/// Wait for a chunk to be available (with timeout and error checking)
pub async fn wait_for_chunk(
    chunk_index: &Arc<RwLock<ChunkIndex>>,
    target_chunk_index: u32,
    check_interval: std::time::Duration,
    timeout: std::time::Duration,
) -> Result<(), String> {
    use tokio::time::{sleep, Instant};

    let start = Instant::now();

    loop {
        // Check if chunk is available
        {
            let index = chunk_index.read();
            
            // Check for errors
            if let Some(ref error) = index.error {
                return Err(format!("Snapshot generation failed: {}", error));
            }

            // Check if chunk exists
            if let Some(_) = index.get_chunk(target_chunk_index) {
                return Ok(());
            }

            // Check if generation is complete but chunk doesn't exist
            if index.is_complete {
                return Err(format!(
                    "Chunk {} not found, but generation is complete (only {} chunks generated)",
                    target_chunk_index,
                    index.generated_chunks
                ));
            }

            // Check timeout
            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout waiting for chunk {} (waited {:?})",
                    target_chunk_index,
                    timeout
                ));
            }
        }

        // Wait for notification or check interval
        let notify = {
            let index = chunk_index.read();
            index.notify.clone()
        };
        
        tokio::select! {
            _ = notify.notified() => {
                // New chunk available, check again
                continue;
            }
            _ = sleep(check_interval) => {
                // Periodic check
                continue;
            }
        }
    }
}

