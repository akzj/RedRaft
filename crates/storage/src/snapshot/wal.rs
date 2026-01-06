//! Write-Ahead Log (WAL) implementation
//!
//! WAL format:
//! - Single file for all slots
//! - Entry format: apply_index (u64) + log_seq (u64) + Command (serialized)
//! - Timed rotation + metadata file

use crate::snapshot::SnapshotConfig;
use anyhow::Result;
use crossbeam_channel;
use resp::Command;
use serde::{Deserialize, Serialize};
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

    /// Log sequence number (per-slot sequence number for WAL entries)
    pub log_seq: u64,

    /// Redis command (serialized with bincode)
    pub command: Vec<u8>, // Serialized Command
}

/// WAL write request
enum WalRequest {
    /// Write a WAL entry
    WriteEntry {
        apply_index: u64,
        log_seq: u64,
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
    first_log_seq: Option<u64>,
    last_log_seq: Option<u64>,
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

        // Current writing file is always "write.log"
        let current_file_name = "write.log".to_string();
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

        // Create channel for write requests (unbounded for high throughput)
        let (tx, rx) = crossbeam_channel::unbounded();

        // Create inner state
        let inner = WalWriterInner {
            config: config.clone(),
            wal_dir: wal_dir.clone(),
            current_file,
            current_file_name: current_file_name.clone(),
            current_file_size,
            first_log_seq: None,
            last_log_seq: None,
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
                    log_seq,
                    command,
                    key,
                }) => {
                    if let Err(e) =
                        Self::write_entry_inner(&mut inner, apply_index, log_seq, &command, &key)
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
        log_seq: u64,
        command: &Command,
        _key: &[u8],
    ) -> Result<()> {
        // Serialize command
        let command_bytes = bincode::serde::encode_to_vec(command, bincode::config::standard())
            .map_err(|e| anyhow::anyhow!("Failed to serialize command: {}", e))?;

        // Create entry
        let entry = WalEntry {
            apply_index,
            log_seq,
            command: command_bytes,
        };

        // Update first and last log_seq
        if inner.first_log_seq.is_none() {
            inner.first_log_seq = Some(log_seq);
        }
        inner.last_log_seq = Some(log_seq);

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
    pub fn write_entry(
        &self,
        apply_index: u64,
        log_seq: u64,
        command: &Command,
        key: &[u8],
    ) -> Result<()> {
        self.tx
            .send(WalRequest::WriteEntry {
                apply_index,
                log_seq,
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

        // Get the file path before dropping the BufWriter
        let write_log_path = inner.wal_dir.join("write.log");

        // Get first and last log_seq from tracked values
        let (first_log_seq, last_log_seq) = match (inner.first_log_seq, inner.last_log_seq) {
            (Some(first), Some(last)) => (first, last),
            _ => {
                // Empty file, nothing to rotate
                return Ok(());
            }
        };

        // Drop the BufWriter to close the file
        drop(std::mem::replace(
            &mut inner.current_file,
            BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&write_log_path)
                    .map_err(|e| anyhow::anyhow!("Failed to create temporary file: {}", e))?,
            ),
        ));

        // Rename write.log to {first_log_seq}-{last_log_seq}.log
        let new_file_name = format!("{}-{}.log", first_log_seq, last_log_seq);
        let new_file_path = inner.wal_dir.join(&new_file_name);
        std::fs::rename(&write_log_path, &new_file_path)
            .map_err(|e| anyhow::anyhow!("Failed to rename WAL file: {}", e))?;

        // Create new write.log file
        inner.current_file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&write_log_path)
                .map_err(|e| anyhow::anyhow!("Failed to create new WAL file: {}", e))?,
        );

        inner.current_file_size = 0;
        inner.current_file_name = "write.log".to_string();
        // Reset log_seq tracking for new file
        inner.first_log_seq = None;
        inner.last_log_seq = None;

        info!("Rotated WAL file to {}", new_file_name);

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
}

impl WalReader {
    pub fn new(config: SnapshotConfig, wal_dir: PathBuf) -> Result<Self> {
        Ok(Self { config, wal_dir })
    }

    /// Find all WAL files in the directory, sorted by first log_seq
    /// Returns read-only files (n-m.log format) sorted by first log_seq, then write.log if it exists
    fn find_wal_files(&self) -> Result<Vec<PathBuf>> {
        let entries = std::fs::read_dir(&self.wal_dir)
            .map_err(|e| anyhow::anyhow!("Failed to read WAL directory: {}", e))?;

        let mut read_only_files: Vec<(u64, PathBuf)> = Vec::new();
        let mut write_log_path: Option<PathBuf> = None;

        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name == "write.log" {
                    write_log_path = Some(path);
                } else if file_name.ends_with(".log") {
                    // Parse n-m.log format
                    if let Some(name_without_ext) = file_name.strip_suffix(".log") {
                        if let Some((first_str, _)) = name_without_ext.split_once('-') {
                            if let Ok(first_log_seq) = first_str.parse::<u64>() {
                                read_only_files.push((first_log_seq, path));
                            }
                        }
                    }
                }
            }
        }

        // Sort read-only files by first log_seq
        read_only_files.sort_by_key(|(first_log_seq, _)| *first_log_seq);
        let mut result: Vec<PathBuf> = read_only_files.into_iter().map(|(_, path)| path).collect();

        // Append write.log at the end if it exists
        if let Some(path) = write_log_path {
            result.push(path);
        }

        Ok(result)
    }

    /// Read WAL entries starting from a given apply_index
    ///
    /// Skips entries with apply_index <= last_applied_index
    pub fn read_entries_from(&self, last_applied_index: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();

        // Read from all files in order
        let wal_files = self.find_wal_files()?;
        for file_path in wal_files {
            let file_entries = self.read_file_entries(&file_path, last_applied_index)?;
            entries.extend(file_entries);
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

    /// Iterate over WAL entries starting from a given apply_index
    ///
    /// Returns an iterator that yields entries one at a time, avoiding loading all entries into memory.
    /// Skips entries with apply_index <= last_applied_index.
    pub fn iter_entries_from(&self, last_applied_index: u64) -> WalEntryIterator {
        let wal_files = match self.find_wal_files() {
            Ok(files) => files,
            Err(_) => Vec::new(),
        };
        WalEntryIterator {
            reader: self,
            last_applied_index,
            wal_files,
            file_index: 0,
            current_file_reader: None,
        }
    }
}

/// Iterator over WAL entries
pub struct WalEntryIterator<'a> {
    reader: &'a WalReader,
    last_applied_index: u64,
    wal_files: Vec<PathBuf>,
    file_index: usize,
    current_file_reader: Option<BufReader<File>>,
}

impl<'a> Iterator for WalEntryIterator<'a> {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to read from current file
            if let Some(ref mut reader) = self.current_file_reader {
                match Self::read_next_entry(reader, self.last_applied_index) {
                    Ok(Some(entry)) => return Some(Ok(entry)),
                    Ok(None) => {
                        // End of current file, move to next
                        self.current_file_reader = None;
                        self.file_index += 1;
                    }
                    Err(e) => return Some(Err(e)),
                }
            } else {
                // Need to open next file
                if self.file_index < self.wal_files.len() {
                    let file_path = &self.wal_files[self.file_index];
                    match File::open(file_path) {
                        Ok(file) => {
                            self.current_file_reader = Some(BufReader::new(file));
                            continue; // Try reading from this file
                        }
                        Err(e) => {
                            return Some(Err(anyhow::anyhow!(
                                "Failed to open WAL file {:?}: {}",
                                file_path,
                                e
                            )));
                        }
                    }
                } else {
                    // No more files
                    return None;
                }
            }
        }
    }
}

impl<'a> WalEntryIterator<'a> {
    fn read_next_entry(
        reader: &mut BufReader<File>,
        last_applied_index: u64,
    ) -> Result<Option<WalEntry>> {
        // Read entry size
        let mut size_bytes = [0u8; 4];
        match reader.read_exact(&mut size_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(anyhow::anyhow!("Failed to read entry size: {}", e)),
        }

        let entry_size = u32::from_le_bytes(size_bytes) as usize;

        // Read entry data
        let mut entry_bytes = vec![0u8; entry_size];
        reader
            .read_exact(&mut entry_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to read entry data: {}", e))?;

        // Deserialize entry
        let entry: WalEntry =
            bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize entry: {}", e))?
                .0;

        // Skip entries with apply_index <= last_applied_index
        if entry.apply_index > last_applied_index {
            Ok(Some(entry))
        } else {
            // Continue reading next entry
            Self::read_next_entry(reader, last_applied_index)
        }
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
            keep_rounds: 2,
            zstd_level: 3,
        };

        // Write entries
        let mut writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();
        let key1 = b"test_key_1".to_vec();
        let key2 = b"test_key_2".to_vec();

        writer
            .write_entry(
                1,
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
            keep_rounds: 2,
            zstd_level: 3,
        };

        // Write entries
        let mut writer = WalWriter::new(config.clone(), wal_dir.clone()).unwrap();
        let key1 = Bytes::from(b"test_key_1" as &[u8]);
        let key2 = Bytes::from(b"test_key_2" as &[u8]);

        writer
            .write_entry(
                1,
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
        drop(writer); // Ensure file is closed

        // Read entries with last_applied_index = 1 (should skip entry 1)
        let reader = WalReader::new(config, wal_dir).unwrap();
        let entries = reader.read_entries_from(1).unwrap();

        assert_eq!(entries.len(), 1, "Expected 1 entry, got {}", entries.len());
        assert_eq!(entries[0].apply_index, 2);
    }
}
