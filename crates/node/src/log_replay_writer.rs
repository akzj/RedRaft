//! Log replay writer for split operations
//!
//! Reads commands from a tokio channel and writes them to a file in batches,
//! updating metadata with the latest index.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use resp::Command;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

/// Metadata for log replay file
#[derive(Debug, Clone)]
pub struct LogReplayMetadata {
    /// Latest Raft index written
    pub latest_index: u64,
    /// Latest Raft term written
    pub latest_term: u64,
    /// Total number of commands written
    pub command_count: u64,
    /// File size in bytes
    pub file_size: u64,
}

impl Default for LogReplayMetadata {
    fn default() -> Self {
        Self {
            latest_index: 0,
            latest_term: 0,
            command_count: 0,
            file_size: 0,
        }
    }
}

/// Cached log entry
#[derive(Debug, Clone)]
pub struct CachedLogEntry {
    /// Raft index
    pub index: u64,
    /// Raft term
    pub term: u64,
    /// Serialized command data
    pub command_data: Vec<u8>,
}

/// Log replay writer information for storage in node
/// Contains references to the writer's components that can be accessed after the writer is started
#[derive(Debug, Clone)]
pub struct LogReplayWriterInfo {
    /// File path
    pub file_path: PathBuf,
    /// Metadata
    pub metadata: Arc<RwLock<LogReplayMetadata>>,
    /// Notification for new data available
    pub notify: Arc<Notify>,
    /// Cache for latest entries (max 128 entries)
    pub cache: Arc<RwLock<VecDeque<CachedLogEntry>>>,
}

/// Log replay writer
pub struct LogReplayWriter {
    /// File path
    file_path: PathBuf,
    /// Metadata
    metadata: Arc<RwLock<LogReplayMetadata>>,
    /// Notification for new data available
    notify: Arc<Notify>,
    /// Cache for latest entries (max 128 entries)
    cache: Arc<RwLock<VecDeque<CachedLogEntry>>>,
    /// Batch size (number of commands to buffer before flushing)
    batch_size: usize,
    /// Flush interval (flush even if batch is not full)
    flush_interval: Duration,
}

impl LogReplayWriter {
    /// Create a new log replay writer
    pub fn new(
        file_path: PathBuf,
        batch_size: usize,
        flush_interval: Duration,
    ) -> anyhow::Result<Self> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create or truncate file
        File::create(&file_path)?;

        Ok(Self {
            file_path,
            metadata: Arc::new(RwLock::new(LogReplayMetadata::default())),
            notify: Arc::new(Notify::new()),
            cache: Arc::new(RwLock::new(VecDeque::with_capacity(128))),
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

    /// Extract writer info for storage
    /// This allows storing the writer's components after the writer is moved into start()
    pub fn into_info(self) -> LogReplayWriterInfo {
        LogReplayWriterInfo {
            file_path: self.file_path,
            metadata: self.metadata,
            notify: self.notify,
            cache: self.cache,
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
        let notify = self.notify.clone();
        let cache = self.cache.clone();
        let metadata = self.metadata.clone();
        let batch_size = self.batch_size;
        let flush_interval = self.flush_interval;
        tokio::spawn(async move {
            let file = File::create(&file_path)
                .map_err(|e| anyhow::anyhow!("Failed to create log replay file: {}", e))?;
            let mut writer = BufWriter::new(file);
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
                                if let Err(e) = Self::flush_batch(&mut writer, &mut batch, &metadata, &cache).await {
                                    error!("Failed to flush final batch: {}", e);
                                    return Err(e);
                                }
                            }
                            break;
                        }

                        // Process received commands
                        for (index, term, command) in recv_buffer.drain(..) {
                            // Serialize command
                            let serialized = match bincode::serde::encode_to_vec(
                                &command,
                                bincode::config::standard(),
                            ) {
                                Ok(data) => data,
                                Err(e) => {
                                    error!("Failed to serialize command at index {}: {}", index, e);
                                    continue;
                                }
                            };

                            // Add to batch
                            batch.push((index, term, serialized.clone()));

                            // Update cache (keep latest 128 entries)
                            {
                                let mut cache_guard = cache.write();
                                cache_guard.push_back(CachedLogEntry {
                                    index,
                                    term,
                                    command_data: serialized,
                                });
                                // Keep only latest 128 entries
                                while cache_guard.len() > 128 {
                                    cache_guard.pop_front();
                                }
                            }

                            // Update metadata
                            {
                                let mut meta = metadata.write();
                                if index > meta.latest_index {
                                    meta.latest_index = index;
                                    meta.latest_term = term;
                                }
                                meta.command_count += 1;
                            }

                            // Notify readers that new data is available
                            notify.notify_waiters();
                        }

                        // Flush if batch is full
                        if batch.len() >= batch_size {
                            if let Err(e) = Self::flush_batch(&mut writer, &mut batch, &metadata, &cache).await {
                                error!("Failed to flush batch: {}", e);
                                return Err(e);
                            }
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    // Periodic flush
                    _ = tokio::time::sleep(flush_interval) => {
                        if !batch.is_empty() && last_flush.elapsed() >= flush_interval {
                            if let Err(e) = Self::flush_batch(&mut writer, &mut batch, &metadata, &cache).await {
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
                .flush()
                .map_err(|e| anyhow::anyhow!("Failed to flush writer: {}", e))?;
            info!("Log replay writer completed");
            Ok(())
        })
    }

    /// Flush a batch of commands to file
    async fn flush_batch(
        writer: &mut BufWriter<File>,
        batch: &mut Vec<(u64, u64, Vec<u8>)>,
        metadata: &Arc<RwLock<LogReplayMetadata>>,
        _cache: &Arc<RwLock<VecDeque<CachedLogEntry>>>,
    ) -> anyhow::Result<()> {
        use std::io::Write;

        for (index, term, data) in batch.iter() {
            // Write entry format: [index (u64)][term (u64)][data_len (u32)][data]
            writer.write_all(&index.to_le_bytes())?;
            writer.write_all(&term.to_le_bytes())?;
            writer.write_all(&(data.len() as u32).to_le_bytes())?;
            writer.write_all(data)?;
        }

        writer.flush()?;

        // Update metadata file size
        {
            let mut meta = metadata.write();
            meta.file_size = writer.get_ref().metadata().map(|m| m.len()).unwrap_or(0);
        }

        debug!("Flushed {} commands to log replay file", batch.len());
        batch.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resp::Command;
    use std::time::Duration;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_log_replay_writer() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("replay.log");

        let writer = LogReplayWriter::new(
            file_path.clone(),
            10,                         // batch size
            Duration::from_millis(100), // flush interval
        )
        .unwrap();

        let metadata = writer.metadata();
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Start writer task
        let handle = writer.start(rx);

        // Send some commands
        for i in 1..=5 {
            let cmd = Command::Set {
                key: format!("key{}", i).into(),
                value: format!("value{}", i).into(),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            };
            tx.send((i, 1, cmd)).await.unwrap();
        }

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check metadata
        let meta = metadata.read();
        assert_eq!(meta.command_count, 5);
        assert_eq!(meta.latest_index, 5);

        // Close channel
        drop(tx);

        // Wait for writer to finish
        handle.await.unwrap().unwrap();

        // Verify file exists and has content
        assert!(file_path.exists());
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(file_size > 0);
    }
}
