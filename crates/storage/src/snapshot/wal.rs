//! Write-Ahead Log (WAL) implementation
//!
//! WAL format:
//! - Single file for all shards
//! - Entry format: apply_index (u64) + shard_id (ShardId) + Command (serialized)
//! - Metadata: shard_id -> [apply_index_begin, apply_index_end)
//! - Timed rotation + metadata file

use crate::shard::{slot_for_key, ShardId};
use crate::snapshot::SnapshotConfig;
use resp::Command;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::info;

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

/// WAL Writer
pub struct WalWriter {
    config: SnapshotConfig,
    wal_dir: PathBuf,
    current_file: BufWriter<File>,
    current_file_name: String,
    current_file_size: u64,
    metadata: WalMetadata,
    metadata_path: PathBuf,
}

impl WalWriter {
    pub fn new(config: SnapshotConfig, wal_dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| format!("Failed to create WAL directory: {}", e))?;

        let metadata_path = wal_dir.join("wal_metadata.json");

        // Load or create metadata
        let mut metadata = if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path)
                .map_err(|e| format!("Failed to read WAL metadata: {}", e))?;
            serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse WAL metadata: {}", e))?
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
                .map_err(|e| format!("Failed to open WAL file: {}", e))?,
        );

        let current_file_size = if current_file_path.exists() {
            std::fs::metadata(&current_file_path)
                .map_err(|e| format!("Failed to get WAL file metadata: {}", e))?
                .len()
        } else {
            0
        };

        if metadata.current_file.is_empty() {
            metadata.current_file = current_file_name.clone();
        }

        Ok(Self {
            config,
            wal_dir,
            current_file,
            current_file_name,
            current_file_size,
            metadata,
            metadata_path,
        })
    }

    /// Write a WAL entry
    pub fn write_entry(
        &mut self,
        apply_index: u64,
        command: &Command,
        key: &[u8],
    ) -> Result<(), String> {
        // Calculate shard_id from key using slot_for_key
        // For WAL, we use a simple hash-based shard calculation
        let slot = slot_for_key(key);
        let shard_id = format!("shard_{}", slot % self.config.shard_count);

        // Serialize command
        let command_bytes = bincode::serde::encode_to_vec(command, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize command: {}", e))?;

        // Create entry
        let entry = WalEntry {
            apply_index,
            shard_id,
            command: command_bytes,
        };

        // Serialize entry
        let entry_bytes = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize WAL entry: {}", e))?;

        // Write entry size + entry data
        self.current_file
            .write_all(&(entry_bytes.len() as u32).to_le_bytes())
            .map_err(|e| format!("Failed to write entry size: {}", e))?;
        self.current_file
            .write_all(&entry_bytes)
            .map_err(|e| format!("Failed to write entry data: {}", e))?;

        self.current_file_size += 4 + entry_bytes.len() as u64;

        // Check if need to rotate
        if self.current_file_size >= self.config.wal_size_threshold {
            self.rotate()?;
        }

        Ok(())
    }

    /// Rotate WAL file
    fn rotate(&mut self) -> Result<(), String> {
        // Flush current file
        self.current_file
            .flush()
            .map_err(|e| format!("Failed to flush WAL file: {}", e))?;

        // Calculate checksum of current file
        let current_file_path = self.wal_dir.join(&self.current_file_name);
        let checksum = self.calculate_file_checksum(&current_file_path)?;

        // Update metadata for current file
        let file_metadata = WalFileMetadata {
            file_name: self.current_file_name.clone(),
            shard_ranges: self.calculate_shard_ranges(&current_file_path)?,
            file_size: self.current_file_size,
            checksum,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        self.metadata.files.push(file_metadata);

        // Create new file
        let file_number = self.metadata.files.len() + 1;
        self.current_file_name = format!("wal_{:04}.log", file_number);
        let new_file_path = self.wal_dir.join(&self.current_file_name);

        self.current_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&new_file_path)
                .map_err(|e| format!("Failed to create new WAL file: {}", e))?,
        );

        self.current_file_size = 0;
        self.metadata.current_file = self.current_file_name.clone();

        // Save metadata
        self.save_metadata()?;

        info!("Rotated WAL file to {}", self.current_file_name);

        Ok(())
    }

    /// Flush WAL to disk (called when Raft triggers snapshot)
    pub fn flush(&mut self) -> Result<(), String> {
        self.current_file
            .flush()
            .map_err(|e| format!("Failed to flush WAL: {}", e))?;
        Ok(())
    }

    /// Calculate shard ranges for a WAL file
    fn calculate_shard_ranges(
        &self,
        file_path: &Path,
    ) -> Result<HashMap<ShardId, (u64, u64)>, String> {
        let mut ranges: HashMap<ShardId, (u64, u64)> = HashMap::new();
        let mut file = BufReader::new(
            File::open(file_path)
                .map_err(|e| format!("Failed to open WAL file for reading: {}", e))?,
        );

        loop {
            // Read entry size
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Failed to read entry size: {}", e)),
            }

            let entry_size = u32::from_le_bytes(size_bytes) as usize;

            // Read entry data
            let mut entry_bytes = vec![0u8; entry_size];
            file.read_exact(&mut entry_bytes)
                .map_err(|e| format!("Failed to read entry data: {}", e))?;

            // Deserialize entry
            let entry: WalEntry =
                bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())
                    .map_err(|e| format!("Failed to deserialize entry: {}", e))?
                    .0;

            // Update range for this shard
            let range = ranges.entry(entry.shard_id).or_insert((u64::MAX, 0));
            range.0 = range.0.min(entry.apply_index);
            range.1 = range.1.max(entry.apply_index + 1); // end is exclusive
        }

        Ok(ranges)
    }

    /// Calculate file checksum (CRC32)
    fn calculate_file_checksum(&self, file_path: &Path) -> Result<u32, String> {
        use crc32fast::Hasher;

        let mut file = File::open(file_path)
            .map_err(|e| format!("Failed to open file for checksum: {}", e))?;
        let mut hasher = Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            let bytes_read = file
                .read(&mut buffer)
                .map_err(|e| format!("Failed to read file for checksum: {}", e))?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(hasher.finalize())
    }

    /// Save metadata to file
    pub fn save_metadata(&self) -> Result<(), String> {
        let content = serde_json::to_string_pretty(&self.metadata)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
        std::fs::write(&self.metadata_path, content)
            .map_err(|e| format!("Failed to write metadata: {}", e))?;
        Ok(())
    }

    /// Get current WAL size (all files)
    pub fn total_size(&self) -> u64 {
        let mut total = self.current_file_size;
        for file_meta in &self.metadata.files {
            total += file_meta.file_size;
        }
        total
    }

    /// Clean up WAL files that are no longer needed
    ///
    /// Removes WAL files where all entries have apply_index < min_apply_index.
    /// This should be called after segment generation to free up disk space.
    ///
    /// # Arguments
    /// - `min_apply_index`: Minimum apply_index that should be kept
    ///
    /// # Returns
    /// Number of files deleted
    pub fn cleanup_old_files(&mut self, min_apply_index: u64) -> Result<usize, String> {
        let mut deleted_count = 0;
        let mut files_to_keep = Vec::new();

        // Check each WAL file
        for file_meta in &self.metadata.files {
            // Check if all shards in this file have max apply_index < min_apply_index
            let mut can_delete = true;
            for (_, (_begin, end)) in &file_meta.shard_ranges {
                // If any shard has entries >= min_apply_index, keep the file
                if *end > min_apply_index {
                    can_delete = false;
                    break;
                }
            }

            if can_delete {
                // Delete the file
                let file_path = self.wal_dir.join(&file_meta.file_name);
                if file_path.exists() {
                    std::fs::remove_file(&file_path).map_err(|e| {
                        format!("Failed to delete WAL file {}: {}", file_meta.file_name, e)
                    })?;
                    deleted_count += 1;
                    info!("Deleted old WAL file: {}", file_meta.file_name);
                }
            } else {
                // Keep the file
                files_to_keep.push(file_meta.clone());
            }
        }

        // Update metadata
        self.metadata.files = files_to_keep;
        self.save_metadata()?;

        Ok(deleted_count)
    }
}

/// WAL Reader
pub struct WalReader {
    config: SnapshotConfig,
    wal_dir: PathBuf,
    metadata: WalMetadata,
}

impl WalReader {
    pub fn new(config: SnapshotConfig, wal_dir: PathBuf) -> Result<Self, String> {
        let metadata_path = wal_dir.join("wal_metadata.json");

        let metadata = if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path)
                .map_err(|e| format!("Failed to read WAL metadata: {}", e))?;
            serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse WAL metadata: {}", e))?
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
    pub fn read_entries_from(&self, last_applied_index: u64) -> Result<Vec<WalEntry>, String> {
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
    ) -> Result<Vec<WalEntry>, String> {
        let mut entries = Vec::new();
        let mut file = BufReader::new(
            File::open(file_path).map_err(|e| format!("Failed to open WAL file: {}", e))?,
        );

        loop {
            // Read entry size
            let mut size_bytes = [0u8; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Failed to read entry size: {}", e)),
            }

            let entry_size = u32::from_le_bytes(size_bytes) as usize;

            // Read entry data
            let mut entry_bytes = vec![0u8; entry_size];
            file.read_exact(&mut entry_bytes)
                .map_err(|e| format!("Failed to read entry data: {}", e))?;

            // Deserialize entry
            let entry: WalEntry =
                bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())
                    .map_err(|e| format!("Failed to deserialize entry: {}", e))?
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
