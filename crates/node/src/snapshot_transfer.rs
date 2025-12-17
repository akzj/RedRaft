//! Snapshot transfer implementation
//!
//! Handles pull-based snapshot transfer where Follower actively pulls snapshot data
//! from Leader after receiving InstallSnapshotRequest.

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{error, info, warn};
use uuid::Uuid;

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
        data.len() >= 4 && &data[0..4] == b"SMET" // Snapshot METadata
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
/// Supports concurrent generation and transfer (streaming while generating)
/// Both generation and transfer can update progress in this shared state
#[derive(Debug, Clone)]
pub struct SnapshotTransferState {
    /// Snapshot file path
    pub snapshot_path: std::path::PathBuf,
    /// Chunk index (in-memory, updated as chunks are generated/transferred)
    pub chunk_index: Arc<RwLock<ChunkIndex>>,
    /// Total compressed size (available after generation completes, 0 if still generating)
    pub total_size: u64,
    /// Error message (if failed)
    pub error: Option<String>,
}

impl SnapshotTransferState {
    /// Create a new transfer state
    pub fn new(snapshot_path: std::path::PathBuf, chunk_index: Arc<RwLock<ChunkIndex>>) -> Self {
        Self {
            snapshot_path,
            chunk_index,
            total_size: 0,
            error: None,
        }
    }

    /// Check if transfer is completed (generation complete and no error)
    pub fn is_completed(&self) -> bool {
        self.error.is_none() && {
            let index = self.chunk_index.read();
            index.is_complete && self.total_size > 0
        }
    }

    /// Check if transfer has failed
    pub fn is_failed(&self) -> bool {
        self.error.is_some()
    }

    /// Mark transfer as failed
    pub fn mark_failed(&mut self, error: String) {
        self.error = Some(error);
    }

    /// Update total size (called when generation completes)
    pub fn update_total_size(&mut self, total_size: u64) {
        self.total_size = total_size;
    }
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

    /// Update transfer state (replaces entire state)
    /// For updating specific fields, use update_total_size() or mark_transfer_failed()
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
        self.transfers
            .read()
            .get(transfer_id)
            .map(|state| state.chunk_index.clone())
    }

    /// Update total size for a transfer (called when generation completes)
    pub fn update_total_size(&self, transfer_id: &str, total_size: u64) {
        let mut transfers = self.transfers.write();
        if let Some(state) = transfers.get_mut(transfer_id) {
            state.update_total_size(total_size);
            info!(
                "Updated total size for transfer {}: {} bytes",
                transfer_id, total_size
            );
        } else {
            warn!("Transfer {} not found for total size update", transfer_id);
        }
    }

    /// Mark transfer as failed
    pub fn mark_transfer_failed(&self, transfer_id: &str, error: String) {
        let mut transfers = self.transfers.write();
        if let Some(state) = transfers.get_mut(transfer_id) {
            state.mark_failed(error.clone());
            info!("Marked transfer {} as failed: {}", transfer_id, error);
        } else {
            warn!("Transfer {} not found for marking as failed", transfer_id);
        }
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
            warn!(
                "Snapshot transfer manager has {} active transfers, consider cleanup",
                count
            );
        }
    }
}

impl Default for SnapshotTransferManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Read a chunk from snapshot file by index
pub async fn read_chunk_from_file(
    snapshot_path: &std::path::Path,
    chunk_index: u32,
    chunk_metadata: &ChunkMetadata,
) -> Result<Vec<u8>> {
    let snapshot_path = snapshot_path.to_path_buf();
    let file_offset = chunk_metadata.file_offset;
    let compressed_size = chunk_metadata.compressed_size as usize;
    let expected_crc32 = chunk_metadata.crc32;
    let chunk_index_for_error = chunk_index;

    // Use spawn_blocking to run file I/O and CPU-intensive CRC32 calculation in blocking thread pool
    let (compressed_data, calculated_crc32) = tokio::task::spawn_blocking(move || {
        use crc32fast::Hasher as Crc32Hasher;
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};

        let mut file = File::open(&snapshot_path)
            .with_context(|| format!("Failed to open snapshot file: {:?}", snapshot_path))?;

        // Seek to chunk start (skip header)
        // Header format: header_len (u32) + header_bytes
        file.seek(SeekFrom::Start(file_offset))
            .with_context(|| format!("Failed to seek to chunk offset: {}", file_offset))?;

        // Read header length
        let mut header_len_bytes = [0u8; 4];
        file.read_exact(&mut header_len_bytes)
            .context("Failed to read header length")?;
        let header_len = u32::from_le_bytes(header_len_bytes) as usize;

        // Skip header (we already have metadata)
        file.seek(SeekFrom::Current(header_len as i64))
            .with_context(|| format!("Failed to skip header: {} bytes", header_len))?;

        // Read compressed data
        let mut compressed_data = vec![0u8; compressed_size];
        file.read_exact(&mut compressed_data).with_context(|| {
            format!("Failed to read compressed data: {} bytes", compressed_size)
        })?;

        // Calculate CRC32 (CPU-intensive operation)
        let mut hasher = Crc32Hasher::new();
        hasher.update(&compressed_data);
        let calculated_crc32 = hasher.finalize();

        Ok::<(Vec<u8>, u32), anyhow::Error>((compressed_data, calculated_crc32))
    })
    .await
    .context("Failed to join file read task")??;

    // Verify CRC32 outside spawn_blocking
    if calculated_crc32 != expected_crc32 {
        return Err(anyhow::anyhow!(
            "CRC32 mismatch for chunk {}: expected {}, got {}",
            chunk_index_for_error,
            expected_crc32,
            calculated_crc32
        ));
    }

    // Return compressed data (no decompression needed, data is sent in compressed form)
    Ok(compressed_data)
}

/// Wait for a chunk to be available (with timeout and error checking)
/// Returns the ChunkMetadata when the chunk is available
pub async fn wait_for_chunk(
    chunk_index: &Arc<RwLock<ChunkIndex>>,
    target_chunk_index: u32,
    check_interval: std::time::Duration,
    timeout: std::time::Duration,
) -> Result<ChunkMetadata> {
    use tokio::time::{sleep, Instant};
    let start = Instant::now();
    loop {
        // Check if chunk is available
        {
            let index = chunk_index.read();
            if let Some(ref error) = index.error {
                return Err(anyhow::anyhow!("Snapshot generation failed: {}", error));
            }

            // Check if chunk exists
            if let Some(chunk_metadata) = index.get_chunk(target_chunk_index) {
                return Ok(chunk_metadata.clone());
            }

            // Check if generation is complete but chunk doesn't exist
            if index.is_complete {
                return Err(anyhow::anyhow!(
                    "Chunk {} not found, but generation is complete (only {} chunks generated)",
                    target_chunk_index,
                    index.generated_chunks
                ));
            }

            // Check timeout
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(
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
                continue;
            }
            _ = sleep(check_interval) => {
                // Periodic check
                continue;
            }
        }
    }
}

/// Get transfer state information (snapshot path and chunk index)
pub fn get_transfer_state_info(
    transfer_manager: &SnapshotTransferManager,
    transfer_id: &str,
) -> anyhow::Result<(std::path::PathBuf, Arc<RwLock<ChunkIndex>>)> {
    let state = transfer_manager
        .get_transfer_state(transfer_id)
        .context(format!("Transfer {} not found", transfer_id))?;

    Ok((state.snapshot_path, state.chunk_index))
}

/// Spawn async task to receive snapshot entries and forward to blocking channel
fn spawn_async_receiver_task(
    mut rx: tokio::sync::mpsc::Receiver<storage::SnapshotStoreEntry>,
    blocking_tx: std::sync::mpsc::Sender<anyhow::Result<(Vec<u8>, bool)>>,
    chunk_index: Arc<RwLock<ChunkIndex>>,
    chunk_size: usize,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut chunk_buffer = Vec::new();

        // Collect entries into chunks in async context
        while let Some(entry) = rx.recv().await {
            match entry {
                storage::SnapshotStoreEntry::Error(err) => {
                    error!("Received error from snapshot creation: {}", err);
                    let mut index = chunk_index.write();
                    index.error = Some(format!("Snapshot creation error: {}", err));
                    index.notify.notify_waiters();
                    // Send error signal to blocking task
                    let _ =
                        blocking_tx.send(Err(anyhow::anyhow!("Snapshot creation error: {}", err)));
                    return Ok(());
                }
                storage::SnapshotStoreEntry::Completed => {
                    // Send remaining buffer and completion signal
                    if !chunk_buffer.is_empty() {
                        if blocking_tx.send(Ok((chunk_buffer, true))).is_err() {
                            error!("Failed to send final chunk to blocking task");
                            return Err(anyhow::anyhow!(
                                "Failed to send final chunk to blocking task"
                            ));
                        }
                    } else {
                        // Send empty chunk to signal completion
                        if blocking_tx.send(Ok((Vec::new(), true))).is_err() {
                            error!("Failed to send completion signal to blocking task");
                            return Err(anyhow::anyhow!(
                                "Failed to send completion signal to blocking task"
                            ));
                        }
                    }
                    info!("Snapshot data transfer completed");
                    break;
                }
                _ => {
                    // Serialize entry using bincode (lightweight operation, can stay in async)
                    let serialized =
                        match bincode::serde::encode_to_vec(&entry, bincode::config::standard()) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Failed to serialize entry: {}", e);
                                let _ = blocking_tx
                                    .send(Err(anyhow::anyhow!("Failed to serialize entry: {}", e)));
                                return Err(anyhow::anyhow!("Failed to serialize entry: {}", e));
                            }
                        };

                    // Check if adding this entry would exceed chunk size
                    if chunk_buffer.len() + serialized.len() > chunk_size
                        && !chunk_buffer.is_empty()
                    {
                        // Send current chunk to blocking task for compression and writing
                        if blocking_tx.send(Ok((chunk_buffer, false))).is_err() {
                            let error = format!("Failed to send chunk to blocking task");
                            error!("{}", error);
                            return Err(anyhow::anyhow!("{}", error));
                        }
                        chunk_buffer = Vec::new();
                    }

                    // Add entry to chunk buffer
                    // Write entry length (u32) followed by serialized data
                    let entry_len = serialized.len() as u32;
                    chunk_buffer.extend_from_slice(&entry_len.to_le_bytes());
                    chunk_buffer.extend_from_slice(&serialized);
                }
            }
        }
        Ok(())
    })
}

/// Write a single chunk to file (compression, CRC32, header, and file I/O)
fn write_chunk_to_file(
    writer: &mut std::io::BufWriter<std::fs::File>,
    buffer: &[u8],
    chunk_index_counter: u32,
    current_file_offset: &mut u64,
    zstd_level: i32,
    chunk_index: Arc<RwLock<ChunkIndex>>,
    transfer_id: &str,
    is_last: bool,
) -> anyhow::Result<u64> {
    use crc32fast::Hasher as Crc32Hasher;
    use std::io::Write;

    // Compress with zstd (CPU-intensive)
    let compressed = zstd::encode_all(buffer, zstd_level)
        .map_err(|e| anyhow::anyhow!("Failed to compress chunk: {}", e))?;

    // Calculate CRC32 of compressed data (CPU-intensive)
    let mut hasher = Crc32Hasher::new();
    hasher.update(&compressed);
    let crc32 = hasher.finalize();

    // Write chunk header (metadata)
    let header = ChunkMetadata {
        chunk_index: chunk_index_counter,
        uncompressed_size: buffer.len() as u32,
        compressed_size: compressed.len() as u32,
        crc32,
        file_offset: *current_file_offset,
        is_last,
    };

    // Serialize header with bincode (CPU-intensive)
    let header_bytes = bincode::serde::encode_to_vec(&header, bincode::config::standard())
        .map_err(|e| anyhow::anyhow!("Failed to serialize chunk header: {}", e))?;

    // Write header length (u32) + header + compressed data
    let header_len = header_bytes.len() as u32;
    writer
        .write_all(&header_len.to_le_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to write header length: {}", e))?;
    writer
        .write_all(&header_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to write header: {}", e))?;
    writer
        .write_all(&compressed)
        .map_err(|e| anyhow::anyhow!("Failed to write compressed data: {}", e))?;

    // Update file offset (header_len (4) + header_bytes + compressed)
    let chunk_size = 4 + header_bytes.len() + compressed.len();
    *current_file_offset += chunk_size as u64;

    // Update chunk index
    {
        let mut index = chunk_index.write();
        index.chunks.push(header.clone());
        index.total_uncompressed_size += buffer.len() as u64;
        index.total_compressed_size += compressed.len() as u64;
        index.generated_chunks = chunk_index_counter + 1;
        if is_last {
            index.is_complete = true;
        }
    }

    // Notify that new chunk is available
    chunk_index.read().notify.notify_waiters();

    info!(
        "Chunk {} written for transfer {}: {} bytes (uncompressed) -> {} bytes (compressed)",
        chunk_index_counter,
        transfer_id,
        buffer.len(),
        compressed.len()
    );

    Ok(compressed.len() as u64)
}

/// Spawn CPU-intensive task to compress and write chunks to file
fn spawn_blocking_writer_task(
    file_path: std::path::PathBuf,
    blocking_rx: std::sync::mpsc::Receiver<anyhow::Result<(Vec<u8>, bool)>>,
    chunk_index: Arc<RwLock<ChunkIndex>>,
    transfer_id: String,
    zstd_level: i32,
) -> tokio::task::JoinHandle<anyhow::Result<(u64, u64)>> {
    tokio::task::spawn_blocking(move || {
        use std::fs::File;
        use std::io::{BufWriter, Write};

        let file = File::create(&file_path)
            .map_err(|e| anyhow::anyhow!("Failed to create snapshot file: {}", e))?;
        let mut writer = BufWriter::new(file);

        let mut chunk_index_counter = 0u32;
        let mut current_file_offset = 0u64;
        let mut total_uncompressed_size = 0u64;
        let mut total_compressed_size = 0u64;

        // Receive chunks from blocking channel and process them
        loop {
            match blocking_rx.recv() {
                Ok(Ok((chunk_buffer, is_last))) => {
                    let compressed_size = write_chunk_to_file(
                        &mut writer,
                        &chunk_buffer,
                        chunk_index_counter,
                        &mut current_file_offset,
                        zstd_level,
                        chunk_index.clone(),
                        &transfer_id,
                        is_last,
                    )?;

                    total_uncompressed_size += chunk_buffer.len() as u64;
                    total_compressed_size += compressed_size;

                    chunk_index_counter += 1;
                    if is_last {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("Blocking channel error: {}", e));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Blocking channel error: {}", e));
                }
            }
        }

        writer
            .flush()
            .map_err(|e| anyhow::anyhow!("Failed to flush snapshot file: {}", e))?;

        // Sync file to disk
        writer
            .get_ref()
            .sync_all()
            .map_err(|e| anyhow::anyhow!("Failed to sync snapshot file: {}", e))?;

        Ok((total_uncompressed_size, total_compressed_size))
    })
}

/// Wait for tasks to complete and update transfer state
async fn wait_for_tasks_and_update_state(
    async_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    write_handle: tokio::task::JoinHandle<anyhow::Result<(u64, u64)>>,
    transfer_manager: &SnapshotTransferManager,
    transfer_id: &str,
    snapshot_path: std::path::PathBuf,
    chunk_index: Arc<RwLock<ChunkIndex>>,
) -> anyhow::Result<()> {
    // Wait for async receiving task to complete (it forwards data to blocking channel)
    async_handle
        .await
        .map_err(|e| anyhow::anyhow!("Async receiving task failed: {:?}", e))??;

    // Wait for file writing to complete
    let (total_uncompressed, total_compressed) = write_handle
        .await
        .map_err(|e| anyhow::anyhow!("File writing task failed: {:?}", e))??;

    // Update transfer state: mark generation as complete by updating total_size
    // Transfer can continue while we update the state
    transfer_manager.update_total_size(transfer_id, total_compressed);

    info!(
        "Snapshot file generated for transfer {}: {} bytes (uncompressed) -> {} bytes (compressed)",
        transfer_id, total_uncompressed, total_compressed
    );

    Ok(())
}

/// Async task to generate snapshot file from channel (chunk-based with zstd compression)
/// Snapshot object is already created in load_snapshot, this function just reads from channel and writes to file
pub async fn generate_snapshot_file_async(
    _shard_id: &str,
    transfer_id: &str,
    transfer_manager: &SnapshotTransferManager,
    rx: tokio::sync::mpsc::Receiver<storage::SnapshotStoreEntry>,
    snapshot_config: crate::config::SnapshotConfig,
) -> anyhow::Result<()> {
    info!("Generating snapshot file for transfer {}", transfer_id);

    // Get transfer state information
    let (snapshot_path, chunk_index) = get_transfer_state_info(transfer_manager, transfer_id)
        .context("Failed to get transfer state info")?;

    // Create a blocking channel to pass data to CPU-intensive task
    // This allows async channel receiving without blocking tokio runtime
    let (blocking_tx, blocking_rx) = std::sync::mpsc::channel();

    // Spawn async task to receive from async channel and forward to blocking channel
    let async_handle = spawn_async_receiver_task(
        rx,
        blocking_tx,
        chunk_index.clone(),
        snapshot_config.chunk_size,
    );

    // Spawn CPU-intensive task to compress and write chunks to file
    let write_handle = spawn_blocking_writer_task(
        snapshot_path.clone(),
        blocking_rx,
        chunk_index.clone(),
        transfer_id.to_string(),
        snapshot_config.zstd_level,
    );

    // Wait for tasks to complete and update state
    wait_for_tasks_and_update_state(
        async_handle,
        write_handle,
        transfer_manager,
        transfer_id,
        snapshot_path,
        chunk_index,
    )
    .await
}
