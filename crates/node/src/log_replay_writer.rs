//! Log replay writer for split operations
//!
//! Reads commands from a tokio channel and writes them to a file in batches,
//! updating metadata with the latest index.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use proto::sync_service::EntryLog;
use resp::Command;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info};

/// Metadata for log replay file
#[derive(Debug, Clone)]
pub struct LogReplayMetadata {
    /// Latest Raft index written
    pub latest_raft_index: u64,
    /// Latest Raft term written
    pub latest_raft_term: u64,
    /// Total number of commands written
    pub command_count: u64,
    /// Sequential index of last written entry (starts from 1, increments for each entry)
    pub last_seq_index: u64,
    /// File size in bytes
    pub file_size: u64,
}

impl Default for LogReplayMetadata {
    fn default() -> Self {
        Self {
            latest_raft_index: 0,
            latest_raft_term: 0,
            command_count: 0,
            last_seq_index: 0,
            file_size: 0,
        }
    }
}

/// Cached log entry
/// The index field matches the apply_index written to file, ensuring consistency
/// Same index means same content, allowing detection of missing or duplicate entries
#[derive(Debug, Clone)]
pub struct CachedLogEntry {
    /// Sequential entry index (starts from 1, increments for each entry)
    pub index: u64,
    /// Raft apply_index (same as written to file, used for consistency check)
    pub raft_apply_index: u64,
    /// Raft term
    pub raft_apply_term: u64,
    /// Serialized command data
    pub command_data: Vec<u8>,
}

/// Log replay writer information for storage in node
/// Contains references to the writer's components that can be accessed after the writer is started
#[derive(Debug, Clone)]
pub struct LogReplayWriterInfo {
    /// File path
    pub file_path: PathBuf,
    /// Index file path for storing (seq_index, offset) mappings
    pub index_file_path: PathBuf,
    /// Metadata
    pub metadata: Arc<RwLock<LogReplayMetadata>>,
    /// Notification for new data available
    pub notify: Arc<Notify>,
    /// Cache for latest entries (max 128 entries)
    pub cache: Arc<RwLock<VecDeque<CachedLogEntry>>>,
    /// Snapshot apply_index - entries with index <= this value are already in snapshot
    pub snapshot_index: Arc<RwLock<Option<u64>>>,
}

/// Log replay writer
pub struct LogReplayWriter {
    /// File path
    file_path: PathBuf,
    /// Index file path for storing (seq_index, offset) mappings
    index_file_path: PathBuf,
    /// Metadata
    metadata: Arc<RwLock<LogReplayMetadata>>,
    /// Notification for new data available
    notify: Arc<Notify>,
    /// Cache for latest entries (max 128 entries)
    cache: Arc<RwLock<VecDeque<CachedLogEntry>>>,
    /// Snapshot apply_index - entries with index <= this value are already in snapshot
    snapshot_index: Arc<RwLock<Option<u64>>>,
    /// Batch size (number of commands to buffer before flushing)
    batch_size: usize,
    /// Flush interval (flush even if batch is not full)
    flush_interval: Duration,
}

impl LogReplayWriter {
    /// Create a new log replay writer
    pub async fn new(
        file_path: PathBuf,
        batch_size: usize,
        flush_interval: Duration,
    ) -> anyhow::Result<Self> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create or truncate file
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .open(&file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create log replay file: {}", e))?;

        // Create index file path (append .idx to the file path)
        let index_file_path = {
            let mut path = file_path.clone();
            path.set_extension("idx");
            path
        };
        // Create or truncate index file
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .open(&index_file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create index file: {}", e))?;

        Ok(Self {
            file_path,
            index_file_path,
            metadata: Arc::new(RwLock::new(LogReplayMetadata::default())),
            notify: Arc::new(Notify::new()),
            cache: Arc::new(RwLock::new(VecDeque::with_capacity(128))),
            snapshot_index: Arc::new(RwLock::new(None)),
            batch_size,
            flush_interval,
        })
    }

    /// Get metadata reference
    pub fn metadata(&self) -> Arc<RwLock<LogReplayMetadata>> {
        self.metadata.clone()
    }

    /// Get notification handle for new data
    pub fn notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    /// Get cache reference
    pub fn cache(&self) -> Arc<RwLock<VecDeque<CachedLogEntry>>> {
        self.cache.clone()
    }

    /// Get file path
    pub fn file_path(&self) -> &PathBuf {
        &self.file_path
    }

    /// Set snapshot index (apply_index of the snapshot)
    /// Entries with index <= snapshot_index are already included in the snapshot
    pub async fn set_snapshot_index(&self, snapshot_index: u64) {
        *self.snapshot_index.write().await = Some(snapshot_index);
    }

    /// Get snapshot index reference
    pub fn snapshot_index(&self) -> Arc<RwLock<Option<u64>>> {
        self.snapshot_index.clone()
    }

    /// Extract writer info for storage
    /// This allows storing the writer's components after the writer is moved into start()
    pub fn into_info(self) -> LogReplayWriterInfo {
        LogReplayWriterInfo {
            file_path: self.file_path,
            index_file_path: self.index_file_path,
            metadata: self.metadata,
            notify: self.notify,
            cache: self.cache,
            snapshot_index: self.snapshot_index,
        }
    }

    /// Start the writer task
    /// Reads commands from channel and writes them to file in batches
    /// This method does not consume self, allowing the writer to be stored elsewhere
    pub fn start(
        &self,
        mut rx: tokio::sync::mpsc::Receiver<(u64, u64, Command)>,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        let file_path = self.file_path.clone();
        let index_file_path = self.index_file_path.clone();
        let notify = self.notify.clone();
        let cache = self.cache.clone();
        let metadata = self.metadata.clone();
        let batch_size = self.batch_size;
        let flush_interval = self.flush_interval;
        tokio::spawn(async move {
            // Open file for appending (don't truncate, as it was already created in new())
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open log replay file: {}", e))?;
            let mut writer = tokio::io::BufWriter::new(file);

            // Open index file for appending (don't truncate, as it was already created in new())
            let index_file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&index_file_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open index file: {}", e))?;
            let mut index_writer = tokio::io::BufWriter::new(index_file);
            // Batch format: (entry_index, apply_index, term, data)
            let mut batch = Vec::with_capacity(batch_size);
            let mut last_flush = tokio::time::Instant::now();
            let mut recv_buffer = Vec::with_capacity(batch_size);

            loop {
                tokio::select! {
                    // Receive commands from channel in batch
                    count = rx.recv_many(&mut recv_buffer, batch_size) => {
                        if count == 0 {
                            // Channel closed, flush remaining batch and exit
                            info!("Log replay channel closed, flushing remaining batch");
                            if !batch.is_empty() {
                                if let Err(e) = Self::flush_batch(&mut writer, &mut index_writer, &mut batch, &metadata, &cache).await {
                                    error!("Failed to flush final batch: {}", e);
                                    return Err(e);
                                }
                            }
                            break;
                        }

                        // Process received commands
                        for (apply_index, term, command) in recv_buffer.drain(..) {
                            // Serialize command
                            let serialized = match bincode::serde::encode_to_vec(
                                &command,
                                bincode::config::standard(),
                            ) {
                                Ok(data) => data,
                                Err(e) => {
                                    error!("Failed to serialize command at index {}: {}", apply_index, e);
                                    continue;
                                }
                            };

                            // Update metadata first to get entry_index
                            let entry_index = {
                                let mut meta = metadata.write().await;
                                if apply_index > meta.latest_raft_index {
                                    meta.latest_raft_index = apply_index;
                                    meta.latest_raft_term = term;
                                }
                                meta.command_count += 1;
                                meta.last_seq_index += 1;
                                meta.last_seq_index
                            };

                            // Add to batch with entry_index (entry_index first)
                            batch.push((entry_index, apply_index, term, serialized.clone()));

                            // Update cache (keep latest 128 entries)
                            // Use sequential index for offset calculation
                            {
                                let mut cache_guard = cache.write().await;
                                cache_guard.push_back(CachedLogEntry {
                                    index: entry_index, // Sequential index for offset calculation
                                    raft_apply_index: apply_index, // Raft apply_index for matching
                                    raft_apply_term: term,
                                    command_data: serialized,
                                });
                                // Keep only latest 128 entries
                                while cache_guard.len() as usize > 128 {
                                    cache_guard.pop_front();
                                }
                            }

                            // Notify readers that new data is available
                            notify.notify_waiters();
                        }

                        // Flush if batch is full
                        if batch.len() >= batch_size {
                            if let Err(e) = Self::flush_batch(&mut writer, &mut index_writer, &mut batch, &metadata, &cache).await {
                                error!("Failed to flush batch: {}", e);
                                return Err(e);
                            }
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    // Periodic flush
                    _ = tokio::time::sleep(flush_interval) => {
                        if !batch.is_empty() && last_flush.elapsed() >= flush_interval {
                            if let Err(e) = Self::flush_batch(&mut writer, &mut index_writer, &mut batch, &metadata, &cache).await {
                                error!("Failed to flush batch: {}", e);
                                return Err(e);
                            }
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                }
            }

            // Final flush
            writer
                .get_mut()
                .flush()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to flush writer: {}", e))?;

            info!("Log replay writer completed");
            Ok(())
        })
    }

    /// Flush a batch of commands to file
    async fn flush_batch(
        writer: &mut tokio::io::BufWriter<File>,
        index_writer: &mut tokio::io::BufWriter<File>,
        batch: &mut Vec<(u64, u64, u64, Vec<u8>)>, // (entry_index, apply_index, term, data)
        metadata: &Arc<RwLock<LogReplayMetadata>>,
        _cache: &Arc<RwLock<VecDeque<CachedLogEntry>>>,
    ) -> anyhow::Result<()> {
        for (entry_index, apply_index, term, data) in batch.iter() {
            // Get current file position before writing (this is the offset for this entry)
            let offset = writer
                .get_mut()
                .stream_position()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to get file position: {}", e))?;

            // Write entry format: [entry_index (u64)][apply_index (u64)][term (u64)][data_len (u32)][data]
            writer
                .get_mut()
                .write_all(&entry_index.to_le_bytes())
                .await?; // entry_index (sequential index)
            writer
                .get_mut()
                .write_all(&apply_index.to_le_bytes())
                .await?; // apply_index
            writer.get_mut().write_all(&term.to_le_bytes()).await?;
            writer
                .get_mut()
                .write_all(&(data.len() as u32).to_le_bytes())
                .await?;
            writer.get_mut().write_all(data).await?;

            // Write index entry: (seq_index, offset)
            index_writer
                .get_mut()
                .write_all(&entry_index.to_le_bytes())
                .await?; // seq_index
            index_writer
                .get_mut()
                .write_all(&offset.to_le_bytes())
                .await?; // offset
        }

        writer.flush().await?;
        index_writer.flush().await?;

        // Update metadata file size
        {
            let mut meta = metadata.write().await;
            meta.file_size = writer
                .get_mut()
                .metadata()
                .await
                .map(|m| m.len())
                .unwrap_or(0);
        }

        debug!("Flushed {} commands to log replay file", batch.len());
        batch.clear();

        Ok(())
    }
}

/// Async iterator for reading log replay entries
pub struct LogReplayIterator {
    pub(crate) next_seq_index: u64, // Next sequential index to read (starts from 1)
    file_position: u64,
    file_reader: tokio::io::BufReader<File>,
    index_reader: tokio::io::BufReader<File>, // Index file reader for quick offset lookup
    pub(crate) writer_info: LogReplayWriterInfo,
}

impl LogReplayIterator {
    /// Create a new iterator starting from the specified sequential index
    /// If start_seq_index is 0, it will be initialized to 1
    pub async fn new(
        writer_info: LogReplayWriterInfo,
        start_seq_index: u64,
    ) -> anyhow::Result<Self> {
        // Open file for reading
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&writer_info.file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open log replay file: {}", e))?;
        let file_reader = tokio::io::BufReader::new(file);

        // Open index file for reading
        let index_file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&writer_info.index_file_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open index file: {}", e))?;
        let index_reader = tokio::io::BufReader::new(index_file);

        let next_seq_index = if start_seq_index == 0 {
            1
        } else {
            start_seq_index
        };

        Ok(Self {
            next_seq_index,
            file_position: 0,
            file_reader,
            index_reader,
            writer_info,
        })
    }

    /// Get offset from index file for a specific seq_index
    /// Returns None if not found or error occurred
    async fn get_offset_from_index(&mut self, seq_index: u64) -> Option<u64> {
        // Boundary check: seq_index must be >= 1
        if seq_index == 0 {
            return None;
        }

        // Seek to the position of the index entry in the index file
        // Each entry is 16 bytes: (seq_index: u64, offset: u64)
        // Index entries are sequential, so position = (seq_index - 1) * 16
        if self
            .index_reader
            .get_mut()
            .seek(SeekFrom::Start((seq_index - 1) * 16))
            .await
            .is_err()
        {
            return None;
        }

        // Read the index entry
        let mut buf = [0u8; 16];
        if self
            .index_reader
            .get_mut()
            .read_exact(&mut buf)
            .await
            .is_err()
        {
            return None;
        }

        let entry_seq_index = u64::from_le_bytes(buf[0..8].try_into().ok()?);
        let offset = u64::from_le_bytes(buf[8..16].try_into().ok()?);

        // Verify the seq_index matches
        if entry_seq_index == seq_index {
            Some(offset)
        } else {
            None
        }
    }

    /// Try to find entry in cache using sequential index offset calculation
    /// Returns Some(EntryLog) if found, None otherwise
    async fn try_get_from_cache(&mut self) -> Option<EntryLog> {
        let cache_guard = self.writer_info.cache.read().await;

        // Cache is empty, nothing to find
        if cache_guard.is_empty() {
            return None;
        }

        // Get the first entry's sequential index
        let first_seq_index = cache_guard.front()?.index;
        let last_seq_index = cache_guard.back()?.index;

        // Check if next_seq_index is within cache range
        if self.next_seq_index < first_seq_index || self.next_seq_index > last_seq_index {
            return None; // Not in cache range
        }

        // Calculate offset: next_seq_index - first_seq_index
        let offset = self.next_seq_index - first_seq_index;

        // Check if offset is within cache bounds
        if offset >= cache_guard.len() as u64 {
            return None; // Offset out of bounds
        }

        // Direct access using offset
        let entry = &cache_guard[offset as usize];

        // Verify the entry matches (cache might have gaps)
        if entry.index != self.next_seq_index {
            return None; // Entry not found at expected position
        }

        // Create EntryLog and increment seq_index for next lookup
        let entry_log = EntryLog {
            index: entry.raft_apply_index,
            term: entry.raft_apply_term,
            command: entry.command_data.clone(),
        };

        self.next_seq_index += 1;
        Some(entry_log)
    }

    /// Try to read entry from file
    /// Returns Some(EntryLog) if found and matches next_seq_index, None otherwise
    /// This function does not wait - it only reads what's available
    async fn try_read_from_file(&mut self) -> anyhow::Result<Option<EntryLog>> {
        // Try to get offset from index file first for faster positioning
        // This must be done before reading from file
        if self.file_position == 0 {
            // Try to get initial offset from index file
            if let Some(offset) = self.get_offset_from_index(self.next_seq_index).await {
                if self.file_reader.seek(SeekFrom::Start(offset)).await.is_ok() {
                    self.file_position = offset;
                }
            }
        } else {
            // Seek to last known position
            if self
                .file_reader
                .seek(SeekFrom::Start(self.file_position))
                .await
                .is_err()
            {
                // If seek fails, reset position
                self.file_position = 0;
            }
        }

        // File format: [entry_index (u64)][apply_index (u64)][term (u64)][data_len (u32)][data]
        // Header size: 8 + 8 + 8 + 4 = 28 bytes
        const HEADER_SIZE: usize = 8 + 8 + 8 + 4;
        let mut header_buf = [0u8; HEADER_SIZE];

        loop {
            // Read header (28 bytes)
            if self
                .file_reader
                .get_mut()
                .read_exact(&mut header_buf)
                .await
                .is_err()
            {
                // End of file, nothing to read
                return Ok(None);
            }

            // Parse header
            let entry_index = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());
            let apply_index = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());
            let term = u64::from_le_bytes(header_buf[16..24].try_into().unwrap());
            let data_len = u32::from_le_bytes(header_buf[24..28].try_into().unwrap()) as usize;

            // Skip entries before next_seq_index
            if entry_index < self.next_seq_index {
                // Try to get offset from index file for faster skipping
                if let Some(offset) = self.get_offset_from_index(self.next_seq_index).await {
                    // Found offset in index file, seek directly to it
                    if self
                        .file_reader
                        .get_mut()
                        .seek(SeekFrom::Start(offset))
                        .await
                        .is_err()
                    {
                        return Ok(None);
                    }
                    self.file_position = offset;
                    continue; // Continue loop to read the target entry
                }

                // Index file doesn't have the entry yet, skip data part using seek
                let current_pos = self.file_position + HEADER_SIZE as u64;
                if self
                    .file_reader
                    .get_mut()
                    .seek(SeekFrom::Start(current_pos + data_len as u64))
                    .await
                    .is_err()
                {
                    return Ok(None);
                }

                // Update file position and continue to next entry
                self.file_position = current_pos + data_len as u64;
                continue;
            }

            if entry_index > self.next_seq_index {
                // Entry not ready yet (missing entry), return None
                return Ok(None);
            }

            // This is the entry we need (entry_index == next_seq_index)
            // Read command data
            let mut command_data = vec![0u8; data_len];
            if self
                .file_reader
                .get_mut()
                .read_exact(&mut command_data)
                .await
                .is_err()
            {
                return Ok(None);
            }

            // Update file position
            self.file_position += HEADER_SIZE as u64 + data_len as u64;

            // Create EntryLog and increment seq_index
            let entry_log = EntryLog {
                index: apply_index,
                term,
                command: command_data,
            };

            self.next_seq_index += 1;
            return Ok(Some(entry_log));
        }
    }

    /// Get the next entry, waiting for new data if needed
    /// Flow: 1. Try cache, 2. Try file, 3. Wait and retry
    pub async fn next(&mut self) -> anyhow::Result<Option<EntryLog>> {
        loop {
            // Check if we've reached the latest sequential index
            let last_seq_index = self.writer_info.metadata.read().await.last_seq_index;
            if self.next_seq_index > last_seq_index {
                // No more entries available, wait for new data
                tokio::select! {
                    _ = self.writer_info.notify.notified() => {
                        continue;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        continue;
                    }
                }
            }

            // Step 1: Try to find entry in cache
            if let Some(entry_log) = self.try_get_from_cache().await {
                return Ok(Some(entry_log));
            }

            // Step 2: Try to read from file (no waiting)
            match self.try_read_from_file().await? {
                Some(entry_log) => return Ok(Some(entry_log)),
                None => {
                    // Not found in cache or file, wait and retry
                }
            }

            // Step 3: Wait for new data and retry
            let notify = self.writer_info.notify.clone();
            tokio::select! {
                _ = notify.notified() => {
                    // New data available, continue loop
                    continue;
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    // Timeout, check again
                    continue;
                }
            }
        }
    }
}
