use std::{
    collections::{HashMap, VecDeque},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
};

use parking_lot::RwLock;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::sync::{self, Semaphore, mpsc};
use tracing::warn;

use crate::{
    RaftId,
    error::StorageError,
    message::{HardState, LogEntry},
    traits::{HardStateStorage, LogEntryStorage, StorageResult},
};

use super::{
    entry::{EntryMeta, LogEntryRecord},
    manager::{SegmentManager, SegmentManagerOptions, DiskStats},
    segment::LogSegment,
};

pub struct LogEntryStoreInner {
    #[allow(dead_code)]
    pub(crate) dir: PathBuf,
    pub(crate) cache_entries_size: usize,
    #[allow(dead_code)]
    pub(crate) io_semaphore: Arc<Semaphore>,
    // read_only segments
    #[allow(dead_code)]
    pub(crate) segments: RwLock<Vec<LogSegment>>,
    // current writeable segment
    pub(crate) current_segment: RwLock<LogSegment>,
    pub(crate) cache_table: RwLock<HashMap<RaftId, VecDeque<LogEntry>>>,
}

#[derive(Clone)]
pub struct LogEntryStoreOptions {
    #[allow(dead_code)]
    pub(crate) memtable_memory_size: usize,
    pub(crate) batch_size: usize,
    #[allow(dead_code)]
    pub(crate) cache_entries_size: usize,
    // max number of I/O threads for log segment file read
    #[allow(dead_code)]
    pub(crate) max_io_threads: usize,
    /// Maximum segment size in bytes before rotation (default: 64MB)
    #[allow(dead_code)]
    pub(crate) max_segment_size: u64,
    /// Directory for storing log segments
    #[allow(dead_code)]
    pub(crate) dir: PathBuf,
    /// Whether to sync after each write
    #[allow(dead_code)]
    pub(crate) sync_on_write: bool,
}

impl Default for LogEntryStoreOptions {
    fn default() -> Self {
        Self {
            memtable_memory_size: 64 * 1024 * 1024,
            batch_size: 100,
            cache_entries_size: 1000,
            max_io_threads: 4,
            max_segment_size: 64 * 1024 * 1024, // 64MB
            dir: PathBuf::from("./data/logs"),
            sync_on_write: true,
        }
    }
}

#[derive(Clone)]
pub struct LogEntryStore {
    pub(crate) options: LogEntryStoreOptions,
    pub(crate) inner: Arc<LogEntryStoreInner>,
    op_sender: mpsc::UnboundedSender<LogEntryOpRequest>,
}

impl LogEntryStore {
    /// Create a new LogEntryStore and return the store along with the receiver
    /// that should be passed to the `start` method.
    pub fn new(
        options: LogEntryStoreOptions,
        inner: LogEntryStoreInner,
    ) -> (Self, mpsc::UnboundedReceiver<LogEntryOpRequest>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let store = Self {
            options,
            inner: Arc::new(inner),
            op_sender: tx,
        };
        (store, rx)
    }
}

impl LogEntryStoreInner {
    pub(crate) fn append_to_cache(&self, log_entries: Vec<(&RaftId, &Vec<LogEntry>)>) {
        // write to cache
        for (from, entries) in log_entries {
            let mut cache_table = self.cache_table.write();
            let cache = cache_table.entry(from.clone()).or_default();
            for log_entry in entries {
                cache.push_back(log_entry.clone());
                if cache.len() > self.cache_entries_size {
                    cache.pop_front();
                }
            }
        }
    }

    pub(crate) fn truncate_cache_prefix(&self, from: &RaftId, index: u64) {
        let mut cache_table = self.cache_table.write();
        if let Some(cache) = cache_table.get_mut(from) {
            // Remove entries with index < truncate_index
            while let Some(front) = cache.front() {
                if front.index < index {
                    cache.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    pub(crate) fn truncate_cache_suffix(&self, from: &RaftId, index: u64) {
        let mut cache_table = self.cache_table.write();
        if let Some(cache) = cache_table.get_mut(from) {
            // Remove entries with index > truncate_index
            while let Some(back) = cache.back() {
                if back.index > index {
                    cache.pop_back();
                } else {
                    break;
                }
            }
        }
    }

    pub(crate) async fn append_to_segment(
        &self,
        log_entries: Vec<(&RaftId, &Vec<LogEntry>)>,
    ) -> StorageResult<()> {
        // write to log segment file
        let mut segment = self.current_segment.write();
        for (from, entries) in log_entries {
            segment
                .write_log_entries(from, entries.clone())
                .map_err(|e| {
                    warn!("Failed to append log entries to segment: {}", e);
                    StorageError::Io(Arc::new(e))
                })?;
        }

        // flush data
        segment.sync_data().map_err(|e| {
            warn!("Failed to sync log segment data: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        Ok(())
    }

    pub(crate) async fn append_log_entries(
        &self,
        log_entries: Vec<(&RaftId, &Vec<LogEntry>)>,
    ) -> StorageResult<()> {
        // write to cache
        self.append_to_cache(log_entries.clone());

        // write to log segment file
        self.append_to_segment(log_entries.clone()).await?;

        Ok(())
    }

    /// Truncate log entries before the given index
    /// This operation is append-only - it writes a truncate record to the segment
    /// and updates the in-memory index
    pub(crate) async fn truncate_log_prefix(&self, from: &RaftId, index: u64) -> StorageResult<()> {
        // Update cache first
        self.truncate_cache_prefix(from, index);

        // Write truncate operation to segment
        let mut segment = self.current_segment.write();
        segment.write_truncate_prefix(from, index).map_err(|e| {
            warn!("Failed to write truncate prefix to segment: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        // Sync data
        segment.sync_data().map_err(|e| {
            warn!("Failed to sync log segment data: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        Ok(())
    }

    /// Truncate log entries after the given index
    /// This operation is append-only - it writes a truncate record to the segment
    /// and updates the in-memory index
    pub(crate) async fn truncate_log_suffix(&self, from: &RaftId, index: u64) -> StorageResult<()> {
        // Update cache first
        self.truncate_cache_suffix(from, index);

        // Write truncate operation to segment
        let mut segment = self.current_segment.write();
        segment.write_truncate_suffix(from, index).map_err(|e| {
            warn!("Failed to write truncate suffix to segment: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        // Sync data
        segment.sync_data().map_err(|e| {
            warn!("Failed to sync log segment data: {}", e);
            StorageError::Io(Arc::new(e))
        })?;

        Ok(())
    }

    /// Get log entries from cache if available
    fn get_from_cache(&self, from: &RaftId, low: u64, high: u64) -> Option<Vec<LogEntry>> {
        let cache_table = self.cache_table.read();
        let cache = cache_table.get(from)?;
        
        if cache.is_empty() {
            return None;
        }

        let first_cached = cache.front()?.index;
        let last_cached = cache.back()?.index;

        // Check if the requested range is fully in cache
        if low >= first_cached && high <= last_cached + 1 {
            let start_offset = (low - first_cached) as usize;
            let count = (high - low) as usize;
            
            let entries: Vec<LogEntry> = cache
                .iter()
                .skip(start_offset)
                .take(count)
                .cloned()
                .collect();
            
            if entries.len() == count {
                return Some(entries);
            }
        }
        None
    }

    /// Get log entry term from cache if available
    fn get_term_from_cache(&self, from: &RaftId, idx: u64) -> Option<u64> {
        let cache_table = self.cache_table.read();
        let cache = cache_table.get(from)?;
        
        if cache.is_empty() {
            return None;
        }

        let first_cached = cache.front()?.index;
        let last_cached = cache.back()?.index;

        if idx >= first_cached && idx <= last_cached {
            let offset = (idx - first_cached) as usize;
            return cache.get(offset).map(|e| e.term);
        }
        None
    }

    /// Get log entries from segment
    pub(crate) async fn get_log_entries(&self, from: &RaftId, low: u64, high: u64) -> StorageResult<Vec<LogEntry>> {
        // Try cache first
        if let Some(entries) = self.get_from_cache(from, low, high) {
            return Ok(entries);
        }

        // Get the data we need from segment without holding lock across await
        let (file, io_semaphore, entry_metas) = {
            let segment = self.current_segment.read();
            let file = segment.file.clone();
            let io_semaphore = segment.io_semaphore.clone();
            
            // Get entry metadata for the requested range
            let raft_index = match segment.entry_index.entries.get(from) {
                Some(idx) if !idx.entries.is_empty() => idx,
                _ => return Ok(Vec::new()),
            };
            
            // Clamp low and high to valid range
            let low = low.max(raft_index.first_log_index);
            let high = high.min(raft_index.last_log_index + 1);
            
            if low >= high {
                return Ok(Vec::new());
            }
            
            let count = (high - low) as usize;
            let begin = (low - raft_index.first_log_index) as usize;
            let metas: Vec<EntryMeta> = raft_index.entries[begin..(begin + count)].to_vec();
            
            (file, io_semaphore, metas)
        };
        
        if entry_metas.is_empty() {
            return Ok(Vec::new());
        }
        
        // Now perform async I/O without holding the lock
        let permit = io_semaphore.acquire_owned().await.unwrap();
        
        let entries = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut log_entries = Vec::with_capacity(entry_metas.len());
            
            for meta in entry_metas {
                let mut buf = vec![0u8; meta.size as usize];
                file.read_exact_at(&mut buf, meta.offset)?;
                
                match LogEntryRecord::deserialize(&buf) {
                    Ok((record, _)) => log_entries.push(record.entry),
                    Err(e) => {
                        warn!("Failed to deserialize log entry at index {}: {}", meta.log_index, e);
                        return Err(e);
                    }
                }
            }
            Ok(log_entries)
        })
        .await
        .map_err(|e| StorageError::Io(Arc::new(anyhow::anyhow!("Task join error: {}", e))))?
        .map_err(|e| StorageError::Io(Arc::new(e)))?;
        
        Ok(entries)
    }

    /// Get log entry terms from segment
    pub(crate) async fn get_log_entries_term(&self, from: &RaftId, low: u64, high: u64) -> StorageResult<Vec<(u64, u64)>> {
        let entries = self.get_log_entries(from, low, high).await?;
        Ok(entries.into_iter().map(|e| (e.index, e.term)).collect())
    }

    /// Get the last log index and term for a raft node
    pub(crate) fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        // Check cache first
        {
            let cache_table = self.cache_table.read();
            if let Some(cache) = cache_table.get(from) {
                if let Some(last) = cache.back() {
                    return Ok((last.index, last.term));
                }
            }
        }

        // Check segment index
        let segment = self.current_segment.read();
        if let Some(meta) = segment.last_entry(from) {
            return Ok((meta.log_index, meta.term));
        }

        // No entries found - return (0, 0) as default
        Ok((0, 0))
    }

    /// Get the term for a specific log index
    pub(crate) async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        // Try cache first
        if let Some(term) = self.get_term_from_cache(from, idx) {
            return Ok(term);
        }

        // Get the data we need from segment without holding lock across await
        let (file, io_semaphore, meta) = {
            let segment = self.current_segment.read();
            let file = segment.file.clone();
            let io_semaphore = segment.io_semaphore.clone();
            
            // Get entry metadata for the requested index
            let raft_index = match segment.entry_index.entries.get(from) {
                Some(idx) if !idx.entries.is_empty() => idx,
                _ => return Err(StorageError::LogNotFound(idx)),
            };
            
            let meta = match raft_index.get_entry(idx) {
                Some(m) => m.clone(),
                None => return Err(StorageError::LogNotFound(idx)),
            };
            
            (file, io_semaphore, meta)
        };
        
        // Now perform async I/O without holding the lock
        let permit = io_semaphore.acquire_owned().await.unwrap();
        
        let term = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut buf = vec![0u8; meta.size as usize];
            file.read_exact_at(&mut buf, meta.offset)?;
            
            match LogEntryRecord::deserialize(&buf) {
                Ok((record, _)) => Ok(record.entry.term),
                Err(e) => {
                    warn!("Failed to deserialize log entry at index {}: {}", meta.log_index, e);
                    Err(e)
                }
            }
        })
        .await
        .map_err(|e| StorageError::Io(Arc::new(anyhow::anyhow!("Task join error: {}", e))))?
        .map_err(|e| StorageError::Io(Arc::new(e)))?;
        
        Ok(term)
    }

    /// Save hard state for a raft node to the log segment.
    /// Hard state is persisted to disk and cached in memory.
    pub(crate) fn save_hard_state(&self, hard_state: &HardState) -> StorageResult<()> {
        let mut segment = self.current_segment.write();
        segment.write_hard_state(hard_state).map_err(|e| {
            warn!("Failed to write hard state to segment: {}", e);
            StorageError::Io(Arc::new(e))
        })?;
        segment.sync_data().map_err(|e| {
            warn!("Failed to sync hard state: {}", e);
            StorageError::Io(Arc::new(e))
        })?;
        Ok(())
    }

    /// Load hard state for a raft node
    pub(crate) fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        let segment = self.current_segment.read();
        let hard_states = segment.hard_states.read();
        Ok(hard_states.get(from).cloned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum LogEntryOp {
    Append(Vec<LogEntry>),
    TruncateLogPrefix(u64),
    TruncateLogSuffix(u64),
}

pub struct LogEntryOpRequest {
    pub from: RaftId,
    pub log_entry_op: LogEntryOp,
    pub response_tx: sync::oneshot::Sender<StorageResult<()>>,
}

impl LogEntryStore {
    /// Start the log entry store with proper handling of all LogEntryOp types.
    /// 
    /// Operations are processed in order to maintain consistency:
    /// - Consecutive Append operations are batched together for efficiency
    /// - TruncateLogPrefix and TruncateLogSuffix are processed individually
    /// 
    /// All operations are append-only to the log segment:
    /// - Append: writes log entries to segment and updates index
    /// - TruncateLogPrefix: writes a truncate record and updates index to mark entries as deleted
    /// - TruncateLogSuffix: writes a truncate record and updates index to mark entries as deleted
    /// 
    /// On restart, the index is replayed from the segment to reconstruct valid entries.
    pub fn start(&self, mut receiver: mpsc::UnboundedReceiver<LogEntryOpRequest>) {
        let self_clone = self.clone();
        let batch_size = self.options.batch_size;

        tokio::spawn(async move {
            loop {
                let mut buf = Vec::with_capacity(batch_size);
                let size = receiver.recv_many(&mut buf, batch_size).await;
                if size == 0 {
                    warn!("Log entry receiver closed");
                    break;
                }

                // Store results for each request
                let mut results: Vec<StorageResult<()>> = Vec::with_capacity(buf.len());
                
                // Process operations maintaining order
                // Group consecutive appends for batching to improve I/O efficiency
                let mut i = 0;
                while i < buf.len() {
                    match &buf[i].log_entry_op {
                        LogEntryOp::Append(_) => {
                            // Find all consecutive append operations
                            let start = i;
                            while i < buf.len() && matches!(&buf[i].log_entry_op, LogEntryOp::Append(_)) {
                                i += 1;
                            }

                            // Batch all consecutive appends for efficient I/O
                            let append_log_entries: Vec<_> = buf[start..i]
                                .iter()
                                .filter_map(|req| match &req.log_entry_op {
                                    LogEntryOp::Append(entries) => Some((&req.from, entries)),
                                    _ => None,
                                })
                                .collect();

                            let result = self_clone
                                .inner
                                .append_log_entries(append_log_entries)
                                .await;

                            // All appends in this batch share the same result
                            for _ in start..i {
                                results.push(result.clone());
                            }
                        }
                        LogEntryOp::TruncateLogPrefix(index) => {
                            // Write truncate prefix operation to segment
                            // This marks all entries before index as deleted in the index
                            let result = self_clone
                                .inner
                                .truncate_log_prefix(&buf[i].from, *index)
                                .await;
                            results.push(result);
                            i += 1;
                        }
                        LogEntryOp::TruncateLogSuffix(index) => {
                            // Write truncate suffix operation to segment
                            // This marks all entries after index as deleted in the index
                            let result = self_clone
                                .inner
                                .truncate_log_suffix(&buf[i].from, *index)
                                .await;
                            results.push(result);
                            i += 1;
                        }
                    }
                }

                // Send all results back to callers
                for (req, result) in buf.into_iter().zip(results.into_iter()) {
                    let _ = req.response_tx.send(result);
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl HardStateStorage for LogEntryStore {
    async fn save_hard_state(&self, _from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        // Note: `from` is ignored since HardState contains raft_id
        self.inner.save_hard_state(&hard_state)
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        self.inner.load_hard_state(from)
    }
}

#[async_trait::async_trait]
impl LogEntryStorage for LogEntryStore {
    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::Append(entries.to_vec()),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send append log entries request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive append log entries response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        self.inner.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        self.inner.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::TruncateLogSuffix(idx),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send truncate log suffix request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive truncate log suffix response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::TruncateLogPrefix(idx),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send truncate log prefix request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive truncate log prefix response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        self.inner.get_last_log_index(from)
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        self.inner.get_log_term(from, idx).await
    }
}

// =============================================================================
// ManagedLogEntryStore - New implementation using SegmentManager
// =============================================================================

/// Log entry store with automatic segment management.
/// 
/// This store provides:
/// - Automatic segment rotation when max size is reached
/// - Cross-segment read operations
/// - Obsolete segment cleanup after truncatePrefix
/// - Disk space monitoring
/// 
/// Use this for production deployments where disk management is important.
#[derive(Clone)]
pub struct ManagedLogEntryStore {
    options: LogEntryStoreOptions,
    manager: Arc<SegmentManager>,
    cache_table: Arc<RwLock<HashMap<RaftId, VecDeque<LogEntry>>>>,
    op_sender: mpsc::UnboundedSender<LogEntryOpRequest>,
}

/// Options for creating a ManagedLogEntryStore
#[derive(Clone, Debug)]
pub struct ManagedLogEntryStoreOptions {
    /// Maximum segment size in bytes before rotation (default: 64MB)
    pub max_segment_size: u64,
    /// Directory for storing log segments
    pub dir: PathBuf,
    /// Maximum number of I/O threads
    pub max_io_threads: usize,
    /// Whether to sync after each write
    pub sync_on_write: bool,
    /// Batch size for processing operations
    pub batch_size: usize,
    /// Cache size for recent entries
    pub cache_entries_size: usize,
    /// Minimum free disk space to maintain (bytes)
    pub min_free_disk_space: u64,
}

impl Default for ManagedLogEntryStoreOptions {
    fn default() -> Self {
        Self {
            max_segment_size: 64 * 1024 * 1024, // 64MB
            dir: PathBuf::from("./data/logs"),
            max_io_threads: 4,
            sync_on_write: true,
            batch_size: 100,
            cache_entries_size: 1000,
            min_free_disk_space: 100 * 1024 * 1024, // 100MB
        }
    }
}

impl ManagedLogEntryStore {
    /// Create a new managed log entry store
    pub fn new(options: ManagedLogEntryStoreOptions) -> Result<(Self, mpsc::UnboundedReceiver<LogEntryOpRequest>), StorageError> {
        let manager_options = SegmentManagerOptions {
            dir: options.dir.clone(),
            max_segment_size: options.max_segment_size,
            max_io_threads: options.max_io_threads,
            sync_on_write: options.sync_on_write,
            min_free_disk_space: options.min_free_disk_space,
        };

        let manager = SegmentManager::new(manager_options)
            .map_err(|e| StorageError::Io(Arc::new(e)))?;

        let (tx, rx) = mpsc::unbounded_channel();
        
        let store_options = LogEntryStoreOptions {
            memtable_memory_size: 64 * 1024 * 1024,
            batch_size: options.batch_size,
            cache_entries_size: options.cache_entries_size,
            max_io_threads: options.max_io_threads,
            max_segment_size: options.max_segment_size,
            dir: options.dir,
            sync_on_write: options.sync_on_write,
        };

        Ok((
            Self {
                options: store_options,
                manager: Arc::new(manager),
                cache_table: Arc::new(RwLock::new(HashMap::new())),
                op_sender: tx,
            },
            rx,
        ))
    }

    /// Start the background operation processor
    pub fn start(&self, mut receiver: mpsc::UnboundedReceiver<LogEntryOpRequest>) {
        let manager = self.manager.clone();
        let cache_table = self.cache_table.clone();
        let cache_size = self.options.cache_entries_size;
        let batch_size = self.options.batch_size;

        tokio::spawn(async move {
            loop {
                let mut buf = Vec::with_capacity(batch_size);
                let size = receiver.recv_many(&mut buf, batch_size).await;
                if size == 0 {
                    warn!("Managed log entry receiver closed");
                    break;
                }

                let mut results: Vec<StorageResult<()>> = Vec::with_capacity(buf.len());
                let mut i = 0;

                while i < buf.len() {
                    match &buf[i].log_entry_op {
                        LogEntryOp::Append(_) => {
                            // Batch consecutive appends
                            let start = i;
                            while i < buf.len() && matches!(&buf[i].log_entry_op, LogEntryOp::Append(_)) {
                                i += 1;
                            }

                            // Process all appends
                            let mut batch_result = Ok(());
                            for req in &buf[start..i] {
                                if let LogEntryOp::Append(entries) = &req.log_entry_op {
                                    // Write to manager (with rotation check)
                                    if let Err(e) = manager.write_log_entries(&req.from, entries.clone()) {
                                        warn!("Failed to append log entries: {}", e);
                                        batch_result = Err(StorageError::Io(Arc::new(e)));
                                        break;
                                    }

                                    // Update cache
                                    let mut cache = cache_table.write();
                                    let entry_cache = cache.entry(req.from.clone()).or_default();
                                    for entry in entries {
                                        entry_cache.push_back(entry.clone());
                                        if entry_cache.len() > cache_size {
                                            entry_cache.pop_front();
                                        }
                                    }
                                }
                            }

                            // All appends in this batch share the result
                            for _ in start..i {
                                results.push(batch_result.clone());
                            }
                        }
                        LogEntryOp::TruncateLogPrefix(index) => {
                            let result = manager
                                .write_truncate_prefix(&buf[i].from, *index)
                                .map_err(|e| StorageError::Io(Arc::new(e)));

                            // Update cache
                            if result.is_ok() {
                                let mut cache = cache_table.write();
                                if let Some(entry_cache) = cache.get_mut(&buf[i].from) {
                                    while let Some(front) = entry_cache.front() {
                                        if front.index < *index {
                                            entry_cache.pop_front();
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }

                            results.push(result);
                            i += 1;
                        }
                        LogEntryOp::TruncateLogSuffix(index) => {
                            let result = manager
                                .write_truncate_suffix(&buf[i].from, *index)
                                .map_err(|e| StorageError::Io(Arc::new(e)));

                            // Update cache
                            if result.is_ok() {
                                let mut cache = cache_table.write();
                                if let Some(entry_cache) = cache.get_mut(&buf[i].from) {
                                    while let Some(back) = entry_cache.back() {
                                        if back.index > *index {
                                            entry_cache.pop_back();
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }

                            results.push(result);
                            i += 1;
                        }
                    }
                }

                // Send results back
                for (req, result) in buf.into_iter().zip(results.into_iter()) {
                    let _ = req.response_tx.send(result);
                }
            }
        });
    }

    /// Get disk usage statistics
    pub fn get_disk_stats(&self) -> DiskStats {
        self.manager.get_disk_stats()
    }

    /// Get total disk usage in bytes
    pub fn get_disk_usage(&self) -> u64 {
        self.manager.get_disk_usage()
    }

    /// Get number of segments (active + sealed)
    pub fn segment_count(&self) -> usize {
        self.manager.segment_count()
    }

    /// Force segment rotation
    pub fn force_rotation(&self) -> Result<(), StorageError> {
        self.manager
            .rotate_segment()
            .map_err(|e| StorageError::Io(Arc::new(e)))
    }

    /// Cleanup obsolete segments
    pub fn cleanup_obsolete_segments(&self) -> Result<usize, StorageError> {
        self.manager
            .cleanup_obsolete_segments()
            .map_err(|e| StorageError::Io(Arc::new(e)))
    }

    /// Check if disk space is low
    pub fn is_disk_space_low(&self) -> bool {
        self.manager.is_disk_space_low()
    }

    /// Get entries from cache if available
    fn get_from_cache(&self, from: &RaftId, low: u64, high: u64) -> Option<Vec<LogEntry>> {
        let cache = self.cache_table.read();
        let entry_cache = cache.get(from)?;

        if entry_cache.is_empty() {
            return None;
        }

        let first_cached = entry_cache.front()?.index;
        let last_cached = entry_cache.back()?.index;

        if low >= first_cached && high <= last_cached + 1 {
            let start_offset = (low - first_cached) as usize;
            let count = (high - low) as usize;

            let entries: Vec<LogEntry> = entry_cache
                .iter()
                .skip(start_offset)
                .take(count)
                .cloned()
                .collect();

            if entries.len() == count {
                return Some(entries);
            }
        }
        None
    }

    /// Get term from cache if available
    fn get_term_from_cache(&self, from: &RaftId, idx: u64) -> Option<u64> {
        let cache = self.cache_table.read();
        let entry_cache = cache.get(from)?;

        if entry_cache.is_empty() {
            return None;
        }

        let first_cached = entry_cache.front()?.index;
        let last_cached = entry_cache.back()?.index;

        if idx >= first_cached && idx <= last_cached {
            let offset = (idx - first_cached) as usize;
            return entry_cache.get(offset).map(|e| e.term);
        }
        None
    }
}

#[async_trait::async_trait]
impl HardStateStorage for ManagedLogEntryStore {
    async fn save_hard_state(&self, _from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        // Note: `from` is ignored since HardState contains raft_id
        self.manager
            .save_hard_state(&hard_state)
            .map_err(|e| StorageError::Io(Arc::new(e)))
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        Ok(self.manager.load_hard_state(from))
    }
}

#[async_trait::async_trait]
impl LogEntryStorage for ManagedLogEntryStore {
    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::Append(entries.to_vec()),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send append log entries request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive append log entries response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        // Try cache first
        if let Some(entries) = self.get_from_cache(from, low, high) {
            return Ok(entries);
        }

        // Read from manager (handles cross-segment reads)
        self.manager
            .get_log_entries(from, low, high)
            .await
            .map_err(|e| StorageError::Io(Arc::new(e)))
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        let entries = self.get_log_entries(from, low, high).await?;
        Ok(entries.into_iter().map(|e| (e.index, e.term)).collect())
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::TruncateLogSuffix(idx),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send truncate log suffix request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive truncate log suffix response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        let (tx, rx) = sync::oneshot::channel();
        let request = LogEntryOpRequest {
            from: from.clone(),
            log_entry_op: LogEntryOp::TruncateLogPrefix(idx),
            response_tx: tx,
        };

        self.op_sender.send(request).map_err(|e| {
            warn!("Failed to send truncate log prefix request: {}", e);
            StorageError::ChannelClosed
        })?;

        rx.await.map_err(|e| {
            warn!("Failed to receive truncate log prefix response: {}", e);
            StorageError::ChannelClosed
        })?
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        // Try cache first
        {
            let cache = self.cache_table.read();
            if let Some(entry_cache) = cache.get(from) {
                if let Some(last) = entry_cache.back() {
                    return Ok((last.index, last.term));
                }
            }
        }

        // Fall back to manager
        Ok(self.manager.get_last_log_index(from))
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        // Try cache first
        if let Some(term) = self.get_term_from_cache(from, idx) {
            return Ok(term);
        }

        // Read from manager
        let entry = self.manager
            .read_entry(from, idx)
            .await
            .map_err(|e| StorageError::Io(Arc::new(e)))?;

        Ok(entry.term)
    }
}

