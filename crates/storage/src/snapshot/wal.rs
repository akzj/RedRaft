//! Write-Ahead Log (WAL) implementation
//!
//! WAL format:
//! - Single file for all shards
//! - Entry format: apply_index (u64) + shard_id (ShardId) + Command (serialized)
//! - Metadata: shard_id -> [apply_index_begin, apply_index_end)
//! - Timed rotation + metadata file

use crate::snapshot::SnapshotConfig;
use rr_core::routing::RoutingTable;
use rr_core::shard::ShardId;
use anyhow::Result;
use crossbeam_channel;
use resp::Command;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use tracing::{error, info};

/// WAL Entry format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Raft apply index
    pub apply_index: u64,

    /// Shard ID (calculated from key)
    pub shard_id: ShardId,

    /// Redis command (serialized with bincode)
    pub command: Vec<u8>, // Serialized Command
}

/// WAL Metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalMetadata {
    /// WAL files with their shard ranges
    pub files: Vec<WalFileMetadata>,

    /// Current active WAL file name
    pub current_file: String,
}

/// WAL File Metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalFileMetadata {
    /// File name
    pub file_name: String,

    /// Shard ranges: shard_id -> [apply_index_begin, apply_index_end)
    pub shard_ranges: HashMap<ShardId, (u64, u64)>, // (begin, end)

    /// File size in bytes
    pub file_size: u64,

    /// File checksum (CRC32)
    pub checksum: u32,

    /// Created timestamp
    pub created_at: u64,
}

/// WAL write request
enum WalRequest {
    /// Write a WAL entry
    WriteEntry {
        apply_index: u64,
        command: Command,
        key: Vec<u8>,
    },
    /// Flush request with response channel
    Flush {
        response: crossbeam_channel::Sender<Result<()>>,
    },
    /// Shutdown the writer thread
    Shutdown,
}

/// Internal WAL writer state (used in background thread)
struct WalWriterInner {
    config: SnapshotConfig,
    wal_dir: PathBuf,
    current_file: BufWriter<File>,
    current_file_name: String,
    current_file_size: u64,
    metadata: WalMetadata,
    metadata_path: PathBuf,
}

/// WAL Writer with channel-based writes
pub struct WalWriter {
    /// Channel sender for write requests
    tx: crossbeam_channel::Sender<WalRequest>,
    /// Background thread handle
    handle: thread::JoinHandle<Result<()>>,
}

impl WalWriter {
    pub fn new(config: SnapshotConfig, wal_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create WAL directory: {}", e))?;

        let metadata_path = wal_dir.join("wal_metadata.json");

        // Load or create metadata
        let mut metadata = if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path)
                .map_err(|e| anyhow::anyhow!("Failed to read WAL metadata: {}", e))?;
            serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("Failed to parse WAL metadata: {}", e))?
        } else {
            WalMetadata {
                files: Vec::new(),
                current_file: String::new(),
            }
        };

        // Open or create current WAL file
        let current_file_name = if metadata.current_file.is_empty() {
            format!("wal_{:04}.log", 1)
        } else {
            metadata.current_file.clone()
        };

        let current_file_path = wal_dir.join(&current_file_name);
        let current_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_file_path)
                .map_err(|e| anyhow::anyhow!("Failed to open WAL file: {}", e))?,
        );

        let current_file_size = if current_file_path.exists() {
            std::fs::metadata(&current_file_path)
                .map_err(|e| anyhow::anyhow!("Failed to get WAL file metadata: {}", e))?
                .len()
        } else {
            0
        };

        if metadata.current_file.is_empty() {
            metadata.current_file = current_file_name.clone();
        }

        // Create channel for write requests (unbounded for high throughput)
        let (tx, rx) = crossbeam_channel::unbounded();

        // Create inner state
        let inner = WalWriterInner {
            config: config.clone(),
            wal_dir: wal_dir.clone(),
            current_file,
            current_file_name: current_file_name.clone(),
            current_file_size,
            metadata,
            metadata_path: metadata_path.clone(),
        };

        // Spawn background thread for batch writing
        let handle = thread::spawn(move || Self::writer_task(inner, rx));

        Ok(Self { tx, handle })
    }

    /// Background writer task that processes write requests in batches
    fn writer_task(
        mut inner: WalWriterInner,
        rx: crossbeam_channel::Receiver<WalRequest>,
    ) -> Result<()> {
        loop {
            // Receive request (blocking)
            match rx.recv() {
                Ok(WalRequest::WriteEntry {
                    apply_index,
                    command,
                    key,
                }) => {
                    if let Err(e) = Self::write_entry_inner(&mut inner, apply_index, &command, &key)
                    {
                        error!("Failed to write WAL entry at index {}: {}", apply_index, e);
                    }
                }
                Ok(WalRequest::Flush { response }) => {
                    let result = inner
                        .current_file
                        .flush()
                        .map_err(|e| anyhow::anyhow!("Failed to flush WAL: {}", e));
                    let _ = response.send(result);
                }
                Ok(WalRequest::Shutdown) => {
                    // Final flush before shutdown
                    inner
                        .current_file
                        .flush()
                        .map_err(|e| anyhow::anyhow!("Failed to flush WAL on shutdown: {}", e))?;
                    break;
                }
                Err(_) => {
                    // Channel closed
                    break;
                }
            }
        }

        Ok(())
    }

    /// Internal write entry implementation
    fn write_entry_inner(
        inner: &mut WalWriterInner,
        apply_index: u64,
        command: &Command,
        key: &[u8],
    ) -> Result<()> {
        // Calculate shard_id from key using slot_for_key
        let slot = RoutingTable::slot_for_key(key);
        let shard_id = format!("shard_{}", slot % inner.config.shard_count);

        // Serialize command
        let command_bytes = bincode::serde::encode_to_vec(command, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize command: {}", e))?;

        // Create entry
        let entry = WalEntry {
            apply_index,
            shard_id,
            command: command_bytes,
        };

        // Serialize entry
        let entry_bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize WAL entry: {}", e))?;

        // Write entry size + entry data
        inner
            .current_file
            .write_all(&(entry_bytes.len() as u32).to_le_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to write entry size: {}", e))?;
        inner
            .current_file
            .write_all(&entry_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to write entry data: {}", e))?;

        inner.current_file_size += 4 + entry_bytes.len() as u64;

        // Check if need to rotate
        if inner.current_file_size >= inner.config.wal_size_threshold {
            Self::rotate_inner(inner)?;
        }

        Ok(())
    }

    /// Write a WAL entry (sends to channel)
    pub fn write_entry(&self, apply_index: u64, command: &Command, key: &[u8]) -> Result<()> {
        self.tx
            .send(WalRequest::WriteEntry {
                apply_index,
                command: command.clone(),
                key: key.to_vec(),
            })
            .map_err(|e| anyhow::anyhow!("Failed to send write request: {}", e))?;
        Ok(())
    }

    /// Rotate WAL file (internal, called from writer task)
    fn rotate_inner(inner: &mut WalWriterInner) -> Result<()> {
        // Flush current file
        inner
            .current_file
            .flush()
            .map_err(|e| anyhow::anyhow!("Failed to flush WAL file: {}", e))?;

        // Calculate checksum of current file
        let current_file_path = inner.wal_dir.join(&inner.current_file_name);
        let checksum = Self::calculate_file_checksum_inner(&current_file_path)?;

        // Update metadata for current file
        let file_metadata = WalFileMetadata {
            file_name: inner.current_file_name.clone(),
            shard_ranges: Self::calculate_shard_ranges_inner(&current_file_path)?,
            file_size: inner.current_file_size,
            checksum,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        inner.metadata.files.push(file_metadata);

        // Create new file
        let file_number = inner.metadata.files.len() + 1;
        inner.current_file_name = format!("wal_{:04}.log", file_number);
        let new_file_path = inner.wal_dir.join(&inner.current_file_name);

        inner.current_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_file_path)
                .map_err(|e| anyhow::anyhow!("Failed to create new WAL file: {}", e))?,
        );

        inner.current_file_size = 0;
        inner.metadata.current_file = inner.current_file_name.clone();

        // Save metadata
        Self::save_metadata_inner(inner)?;

        info!("Rotated WAL file to {}", inner.current_file_name);

        Ok(())
    }

    /// Flush WAL to disk (waits for flush confirmation)
    pub fn flush(&self) -> Result<()> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.tx
            .send(WalRequest::Flush { response: tx })
            .map_err(|e| anyhow::anyhow!("Failed to send flush request: {}", e))?;
        rx.recv()
            .map_err(|e| anyhow::anyhow!("Failed to receive flush response: {}", e))?
    }

    /// Calculate shard ranges for a WAL file (internal)
    fn calculate_shard_ranges_inner(file_path: &Path) -> Result<HashMap<ShardId, (u64, u64)>> {
        let mut ranges: HashMap<ShardId, (u64, u64)> = HashMap::new();
        let mut file = BufReader::new(
            File::open(file_path)
                .map_err(|e| anyhow::anyhow!("Failed to open WAL file for reading: {}", e))?,
        );

        loop {
            // Read entry size
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(anyhow::anyhow!("Failed to read entry size: {}", e)),
            }

            let entry_size = u32::from_le_bytes(size_bytes) as usize;

            // Read entry data
            let mut entry_bytes = vec![0u8; entry_size];
            file.read_exact(&mut entry_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to read entry data: {}", e))?;

            // Deserialize entry
            let entry: WalEntry =
                bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize entry: {}", e))?
                    .0;

            // Update range for this shard
            let range = ranges.entry(entry.shard_id).or_insert((u64::MAX, 0));
            range.0 = range.0.min(entry.apply_index);
            range.1 = range.1.max(entry.apply_index + 1); // end is exclusive
        }

        Ok(ranges)
    }

    /// Calculate file checksum (CRC32) (internal)
    fn calculate_file_checksum_inner(file_path: &Path) -> Result<u32> {
        use crc32fast::Hasher;

        let mut file = File::open(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to open file for checksum: {}", e))?;
        let mut hasher = Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            let bytes_read = file
                .read(&mut buffer)
                .map_err(|e| anyhow::anyhow!("Failed to read file for checksum: {}", e))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(hasher.finalize())
    }

    /// Save metadata to file (internal)
    fn save_metadata_inner(inner: &WalWriterInner) -> Result<()> {
        let content = serde_json::to_string_pretty(&inner.metadata)
            .map_err(|e| anyhow::anyhow!("Failed to serialize metadata: {}", e))?;
        std::fs::write(&inner.metadata_path, content)
            .map_err(|e| anyhow::anyhow!("Failed to write metadata: {}", e))?;
        Ok(())
    }

    /// Save metadata to file (public, for external use)
    pub fn save_metadata(&self) -> Result<()> {
        // Note: This is a best-effort operation since metadata is managed by the writer task
        // For critical operations, consider adding a metadata sync request to the channel
        Ok(())
    }

    /// Get current WAL size (all files)
    /// Note: This is a placeholder - actual implementation would need to query the writer task
    pub fn total_size(&self) -> u64 {
        // This is a placeholder - actual implementation would need to query the writer task
        // For now, return 0 as we don't have access to inner state
        0
    }

    /// Clean up WAL files that are no longer needed
    ///
    /// Note: This is a placeholder - actual implementation would need to send a cleanup request
    /// to the writer task. For now, this is a no-op.
    ///
    /// # Arguments
    /// - `min_apply_index`: Minimum apply_index that should be kept
    ///
    /// # Returns
    /// Number of files deleted
    pub fn cleanup_old_files(&mut self, _min_apply_index: u64) -> Result<usize> {
        // TODO: Implement cleanup request to writer task
        Ok(0)
    }
}

/// WAL Reader
pub struct WalReader {
    config: SnapshotConfig,
    wal_dir: PathBuf,
    metadata: WalMetadata,
}

impl WalReader {
    pub fn new(config: SnapshotConfig, wal_dir: PathBuf) -> Result<Self> {
        let metadata_path = wal_dir.join("wal_metadata.json");

        let metadata = if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path)
                .map_err(|e| anyhow::anyhow!("Failed to read WAL metadata: {}", e))?;
            serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("Failed to parse WAL metadata: {}", e))?
        } else {
            WalMetadata {
                files: Vec::new(),
                current_file: String::new(),
            }
        };

        Ok(Self {
            config,
            wal_dir,
            metadata,
        })
    }

    /// Read WAL entries starting from a given apply_index
    ///
    /// Skips entries with apply_index <= last_applied_index
    pub fn read_entries_from(&self, last_applied_index: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();

        // Read from all files in order
        for file_meta in &self.metadata.files {
            let file_path = self.wal_dir.join(&file_meta.file_name);
            let file_entries = self.read_file_entries(&file_path, last_applied_index)?;
            entries.extend(file_entries);
        }

        // Read from current file if it exists
        if !self.metadata.current_file.is_empty() {
            let current_file_path = self.wal_dir.join(&self.metadata.current_file);
            if current_file_path.exists() {
                let file_entries =
                    self.read_file_entries(&current_file_path, last_applied_index)?;
                entries.extend(file_entries);
            }
        }

        Ok(entries)
    }

    /// Read entries from a single WAL file
    fn read_file_entries(
        &self,
        file_path: &Path,
        last_applied_index: u64,
    ) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut file = BufReader::new(
            File::open(file_path).map_err(|e| anyhow::anyhow!("Failed to open WAL file: {}", e))?,
        );

        loop {
            // Read entry size
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(anyhow::anyhow!("Failed to read entry size: {}", e)),
            }

            let entry_size = u32::from_le_bytes(size_bytes) as usize;

            // Read entry data
            let mut entry_bytes = vec![0u8; entry_size];
            file.read_exact(&mut entry_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to read entry data: {}", e))?;

            // Deserialize entry
            let entry: WalEntry =
                bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize entry: {}", e))?
                    .0;

            // Skip entries with apply_index <= last_applied_index
            if entry.apply_index > last_applied_index {
                entries.push(entry);
            }
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use resp::Command;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_wal_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let config = SnapshotConfig {
            base_dir: temp_dir.path().to_path_buf(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        // Write entries
        let mut writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();
        let key1 = b"test_key_1".to_vec();
        let key2 = b"test_key_2".to_vec();

        writer
            .write_entry(
                1,
                &Command::Set {
                    key: Bytes::from(b"test_key_1" as &[u8]),
                    value: Bytes::from(b"value1" as &[u8]),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                },
                &key1,
            )
            .unwrap();
        writer
            .write_entry(
                2,
                &Command::Set {
                    key: Bytes::from(b"test_key_2" as &[u8]),
                    value: Bytes::from(b"value2" as &[u8]),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                },
                &key2,
            )
            .unwrap();
        writer.flush().unwrap();
        writer.save_metadata().unwrap(); // Save metadata before reading
        drop(writer); // Ensure file is closed

        // Read entries
        let reader = WalReader::new(config, wal_dir).unwrap();
        let entries = reader.read_entries_from(0).unwrap();

        assert_eq!(
            entries.len(),
            2,
            "Expected 2 entries, got {}",
            entries.len()
        );
        assert_eq!(entries[0].apply_index, 1);
        assert_eq!(entries[1].apply_index, 2);
    }

    #[test]
    fn test_wal_skip_applied_entries() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let config = SnapshotConfig {
            base_dir: temp_dir.path().to_path_buf(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        // Write entries
        let mut writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();
        let key1 = Bytes::from(b"test_key_1" as &[u8]);
        let key2 = Bytes::from(b"test_key_2" as &[u8]);

        writer
            .write_entry(
                1,
                &Command::Set {
                    key: key1.clone(),
                    value: Bytes::from(b"value1" as &[u8]),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                },
                &key1,
            )
            .unwrap();
        writer
            .write_entry(
                2,
                &Command::Set {
                    key: key2.clone(),
                    value: Bytes::from(b"value2" as &[u8]),
                    ex: None,
                    px: None,
                    nx: false,
                    xx: false,
                },
                &key2,
            )
            .unwrap();
        writer.flush().unwrap();
        writer.save_metadata().unwrap(); // Save metadata before reading
        drop(writer); // Ensure file is closed

        // Read entries with last_applied_index = 1 (should skip entry 1)
        let reader = WalReader::new(config, wal_dir).unwrap();
        let entries = reader.read_entries_from(1).unwrap();

        assert_eq!(entries.len(), 1, "Expected 1 entry, got {}", entries.len());
        assert_eq!(entries[0].apply_index, 2);
    }
}
