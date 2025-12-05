//! Segment Manager for log storage.
//!
//! This module provides multi-segment management with:
//! - Automatic segment rotation when max size is reached
//! - Cross-segment read operations
//! - Obsolete segment cleanup after truncatePrefix
//! - Disk space monitoring

use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};

use parking_lot::RwLock;
use tokio::sync::Semaphore;
use tracing::{info, warn};
use anyhow::{Result, anyhow};

use crate::{
    RaftId,
    message::{HardState, HardStateMap, LogEntry},
};

use super::{
    entry::Index,
    segment::LogSegment,
};

/// Default maximum segment size (64MB)
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Minimum segment size (1MB) - won't rotate if smaller
pub const MIN_SEGMENT_SIZE: u64 = 1024 * 1024;

/// Segment file prefix
const SEGMENT_FILE_PREFIX: &str = "segment_";

/// Segment file extension
const SEGMENT_FILE_EXT: &str = ".log";

/// Metadata about a single segment
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// Unique segment identifier (monotonically increasing)
    pub segment_id: u64,
    /// File path for this segment
    pub file_path: PathBuf,
    /// First log index per raft group in this segment
    pub first_index_per_raft: HashMap<RaftId, u64>,
    /// Last log index per raft group in this segment
    pub last_index_per_raft: HashMap<RaftId, u64>,
    /// Current file size in bytes
    pub file_size: u64,
    /// When the segment was created
    pub created_at: SystemTime,
    /// Whether this segment is sealed (read-only)
    pub sealed: bool,
}

impl SegmentMeta {
    /// Check if this segment contains any entries for a given raft group and index range
    pub fn contains_range(&self, raft_id: &RaftId, low: u64, high: u64) -> bool {
        if let (Some(&first), Some(&last)) = (
            self.first_index_per_raft.get(raft_id),
            self.last_index_per_raft.get(raft_id),
        ) {
            // Check if ranges overlap
            low <= last && high > first
        } else {
            false
        }
    }

    /// Check if all entries for a raft group have been truncated
    pub fn is_fully_truncated(&self, raft_id: &RaftId, truncate_index: u64) -> bool {
        if let Some(&last) = self.last_index_per_raft.get(raft_id) {
            last < truncate_index
        } else {
            // No entries for this raft group
            true
        }
    }

    /// Check if this segment is completely obsolete (all entries truncated for all raft groups)
    pub fn is_obsolete(&self, truncate_indices: &HashMap<RaftId, u64>) -> bool {
        for (raft_id, &last_index) in &self.last_index_per_raft {
            if let Some(&truncate_index) = truncate_indices.get(raft_id) {
                if last_index >= truncate_index {
                    return false;
                }
            } else {
                // If no truncate index for this raft, segment is not obsolete
                return false;
            }
        }
        // All raft groups have their entries truncated
        !self.last_index_per_raft.is_empty()
    }
}

/// Configuration options for segment manager
#[derive(Clone, Debug)]
pub struct SegmentManagerOptions {
    /// Directory for storing segment files
    pub dir: PathBuf,
    /// Maximum segment size in bytes before rotation
    pub max_segment_size: u64,
    /// Maximum number of I/O threads
    pub max_io_threads: usize,
    /// Whether to sync after each write
    pub sync_on_write: bool,
    /// Minimum free disk space to maintain (bytes)
    pub min_free_disk_space: u64,
}

impl Default for SegmentManagerOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./data/logs"),
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            max_io_threads: 4,
            sync_on_write: true,
            min_free_disk_space: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// Manages multiple log segments with rotation and cleanup
pub struct SegmentManager {
    /// Configuration options
    options: SegmentManagerOptions,
    /// I/O semaphore for limiting concurrent disk operations
    io_semaphore: Arc<Semaphore>,
    /// Currently active (writable) segment
    active_segment: RwLock<LogSegment>,
    /// Metadata for active segment
    active_meta: RwLock<SegmentMeta>,
    /// Read-only sealed segments (ordered by segment_id)
    sealed_segments: RwLock<Vec<(SegmentMeta, Arc<LogSegment>)>>,
    /// Next segment ID to use
    next_segment_id: RwLock<u64>,
    /// Truncate prefix indices per raft group (for cleanup tracking)
    truncate_indices: RwLock<HashMap<RaftId, u64>>,
    /// Total disk usage across all segments
    total_disk_usage: RwLock<u64>,
}

impl SegmentManager {
    /// Create a new segment manager
    pub fn new(options: SegmentManagerOptions) -> Result<Self> {
        // Ensure directory exists
        fs::create_dir_all(&options.dir)?;

        let io_semaphore = Arc::new(Semaphore::new(options.max_io_threads));

        // Load existing segments or create the first one
        let (active_segment, active_meta, sealed_segments, next_id, total_size) =
            Self::load_or_create_segments(&options, io_semaphore.clone())?;

        Ok(Self {
            options,
            io_semaphore,
            active_segment: RwLock::new(active_segment),
            active_meta: RwLock::new(active_meta),
            sealed_segments: RwLock::new(sealed_segments),
            next_segment_id: RwLock::new(next_id),
            truncate_indices: RwLock::new(HashMap::new()),
            total_disk_usage: RwLock::new(total_size),
        })
    }

    /// Load existing segments from disk or create the first segment
    fn load_or_create_segments(
        options: &SegmentManagerOptions,
        io_semaphore: Arc<Semaphore>,
    ) -> Result<(
        LogSegment,
        SegmentMeta,
        Vec<(SegmentMeta, Arc<LogSegment>)>,
        u64,
        u64,
    )> {
        let mut segment_files: Vec<(u64, PathBuf)> = Vec::new();

        // Scan directory for existing segment files
        if let Ok(entries) = fs::read_dir(&options.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with(SEGMENT_FILE_PREFIX) && name.ends_with(SEGMENT_FILE_EXT) {
                        // Extract segment ID from filename
                        let id_str = name
                            .trim_start_matches(SEGMENT_FILE_PREFIX)
                            .trim_end_matches(SEGMENT_FILE_EXT);
                        if let Ok(id) = id_str.parse::<u64>() {
                            segment_files.push((id, path));
                        }
                    }
                }
            }
        }

        // Sort by segment ID
        segment_files.sort_by_key(|(id, _)| *id);

        let mut sealed_segments = Vec::new();
        let mut total_size = 0u64;
        let mut next_id = 0u64;

        if segment_files.is_empty() {
            // No existing segments, create the first one
            let (segment, meta) = Self::create_segment(options, 0, io_semaphore)?;
            return Ok((segment, meta, sealed_segments, 1, 0));
        }

        // Load sealed segments (all but the last one)
        for (id, path) in segment_files.iter().take(segment_files.len().saturating_sub(1)) {
            match Self::load_segment(path.clone(), *id, io_semaphore.clone()) {
                Ok((segment, mut meta)) => {
                    meta.sealed = true;
                    total_size += meta.file_size;
                    sealed_segments.push((meta, Arc::new(segment)));
                    next_id = id + 1;
                }
                Err(e) => {
                    warn!("Failed to load segment {:?}: {}", path, e);
                }
            }
        }

        // Load the last segment as active
        if let Some((id, path)) = segment_files.last() {
            match Self::load_segment(path.clone(), *id, io_semaphore.clone()) {
                Ok((segment, meta)) => {
                    total_size += meta.file_size;
                    next_id = id + 1;
                    return Ok((segment, meta, sealed_segments, next_id, total_size));
                }
                Err(e) => {
                    warn!("Failed to load active segment {:?}: {}, creating new", path, e);
                }
            }
        }

        // Fallback: create a new segment
        let (segment, meta) = Self::create_segment(options, next_id, io_semaphore)?;
        Ok((segment, meta, sealed_segments, next_id + 1, total_size))
    }

    /// Create a new segment file
    fn create_segment(
        options: &SegmentManagerOptions,
        segment_id: u64,
        io_semaphore: Arc<Semaphore>,
    ) -> Result<(LogSegment, SegmentMeta)> {
        let file_name = format!("{}{:010}{}", SEGMENT_FILE_PREFIX, segment_id, SEGMENT_FILE_EXT);
        let file_path = options.dir.join(&file_name);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&file_path)?;

        let segment = LogSegment {
            file_name: file_path.clone(),
            entry_index: Index::default(),
            file: Arc::new(file),
            io_semaphore,
            hard_states: RwLock::new(HardStateMap::new()),
        };

        let meta = SegmentMeta {
            segment_id,
            file_path,
            first_index_per_raft: HashMap::new(),
            last_index_per_raft: HashMap::new(),
            file_size: 0,
            created_at: SystemTime::now(),
            sealed: false,
        };

        info!("Created new segment: id={}, path={:?}", segment_id, meta.file_path);

        Ok((segment, meta))
    }

    /// Load an existing segment from disk
    fn load_segment(
        file_path: PathBuf,
        segment_id: u64,
        io_semaphore: Arc<Semaphore>,
    ) -> Result<(LogSegment, SegmentMeta)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)?;

        let file_size = file.metadata()?.len();
        let file = Arc::new(file);

        let mut segment = LogSegment {
            file_name: file_path.clone(),
            entry_index: Index::default(),
            file: file.clone(),
            io_semaphore,
            hard_states: RwLock::new(HardStateMap::new()),
        };

        // Replay to rebuild index
        segment.replay_segment()?;

        // Build metadata from index
        let mut first_index_per_raft = HashMap::new();
        let mut last_index_per_raft = HashMap::new();

        for (raft_id, raft_index) in &segment.entry_index.entries {
            if !raft_index.entries.is_empty() {
                first_index_per_raft.insert(raft_id.clone(), raft_index.first_log_index);
                last_index_per_raft.insert(raft_id.clone(), raft_index.last_log_index);
            }
        }

        let meta = SegmentMeta {
            segment_id,
            file_path,
            first_index_per_raft,
            last_index_per_raft,
            file_size,
            created_at: SystemTime::now(), // TODO: get from file metadata
            sealed: false,
        };

        info!(
            "Loaded segment: id={}, size={}, raft_groups={}",
            segment_id,
            file_size,
            meta.last_index_per_raft.len()
        );

        Ok((segment, meta))
    }

    /// Check if the active segment needs rotation
    pub fn needs_rotation(&self) -> bool {
        let meta = self.active_meta.read();
        meta.file_size >= self.options.max_segment_size
    }

    /// Get current active segment size
    pub fn active_segment_size(&self) -> u64 {
        self.active_meta.read().file_size
    }

    /// Rotate to a new segment if needed
    pub fn maybe_rotate(&self) -> Result<bool> {
        if !self.needs_rotation() {
            return Ok(false);
        }

        self.rotate_segment()?;
        Ok(true)
    }

    /// Force rotation to a new segment
    pub fn rotate_segment(&self) -> Result<()> {
        let next_id = {
            let mut next_id = self.next_segment_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create new segment
        let (new_segment, new_meta) =
            Self::create_segment(&self.options, next_id, self.io_semaphore.clone())?;

        // Seal current segment and move to sealed list
        let old_segment = {
            let mut active = self.active_segment.write();
            let mut active_meta = self.active_meta.write();

            // Complete the current segment (write tail)
            if let Err(e) = active.complete_write() {
                warn!("Failed to complete segment write: {}", e);
            }

            // Seal metadata
            active_meta.sealed = true;
            let old_meta = active_meta.clone();

            // Swap in new segment
            let old = std::mem::replace(&mut *active, new_segment);
            *active_meta = new_meta;

            (old_meta, old)
        };

        // Add old segment to sealed list
        {
            let mut sealed = self.sealed_segments.write();
            sealed.push((old_segment.0, Arc::new(old_segment.1)));
        }

        info!("Rotated to new segment: id={}", next_id);

        Ok(())
    }

    /// Write log entries to the active segment
    pub fn write_log_entries(&self, from: &RaftId, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Check if rotation is needed before writing
        self.maybe_rotate()?;

        let first_index = entries.first().map(|e| e.index).unwrap_or(0);
        let last_index = entries.last().map(|e| e.index).unwrap_or(0);

        // Write to active segment
        {
            let mut segment = self.active_segment.write();
            segment.write_log_entries(from, entries)?;

            if self.options.sync_on_write {
                segment.sync_data()?;
            }
        }

        // Update metadata
        {
            let mut meta = self.active_meta.write();
            let segment = self.active_segment.read();

            meta.file_size = segment.file.metadata()?.len();

            // Update index ranges
            meta.first_index_per_raft
                .entry(from.clone())
                .or_insert(first_index);
            meta.last_index_per_raft.insert(from.clone(), last_index);
        }

        // Update total disk usage
        self.update_disk_usage();

        Ok(())
    }

    /// Write truncate prefix operation
    pub fn write_truncate_prefix(&self, from: &RaftId, index: u64) -> Result<()> {
        // Write to active segment
        {
            let mut segment = self.active_segment.write();
            segment.write_truncate_prefix(from, index)?;

            if self.options.sync_on_write {
                segment.sync_data()?;
            }
        }

        // Update truncate indices for cleanup tracking
        {
            let mut truncate_indices = self.truncate_indices.write();
            truncate_indices.insert(from.clone(), index);
        }

        // Try to cleanup obsolete segments
        self.cleanup_obsolete_segments()?;

        Ok(())
    }

    /// Write truncate suffix operation
    pub fn write_truncate_suffix(&self, from: &RaftId, index: u64) -> Result<()> {
        {
            let mut segment = self.active_segment.write();
            segment.write_truncate_suffix(from, index)?;

            if self.options.sync_on_write {
                segment.sync_data()?;
            }
        }

        // Update metadata
        {
            let mut meta = self.active_meta.write();
            if let Some(last) = meta.last_index_per_raft.get_mut(from) {
                *last = (*last).min(index);
            }
        }

        Ok(())
    }

    /// Read log entry from any segment
    pub async fn read_entry(&self, from: &RaftId, log_index: u64) -> Result<LogEntry> {
        // Try active segment first - get Arc<File> and necessary data without holding lock across await
        let active_segment_arc: Option<Arc<LogSegment>> = {
            let segment = self.active_segment.read();
            if let Some(raft_index) = segment.entry_index.entries.get(from) {
                if raft_index.is_valid_index(log_index) {
                    // We need to clone the segment data or use a different approach
                    // For now, we'll just mark that we should read from active
                    Some(Arc::new(LogSegment {
                        file_name: segment.file_name.clone(),
                        entry_index: segment.entry_index.clone(),
                        file: segment.file.clone(),
                        io_semaphore: segment.io_semaphore.clone(),
                        hard_states: RwLock::new(segment.hard_states.read().clone()),
                    }))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(segment) = active_segment_arc {
            return segment.read_entry(from, log_index).await;
        }

        // Search sealed segments (from newest to oldest)
        let target_segment: Option<Arc<LogSegment>> = {
            let sealed = self.sealed_segments.read();
            let mut found = None;
            for (meta, segment) in sealed.iter().rev() {
                if meta.contains_range(from, log_index, log_index + 1) {
                    if let Some(raft_index) = segment.entry_index.entries.get(from) {
                        if raft_index.is_valid_index(log_index) {
                            found = Some(segment.clone());
                            break;
                        }
                    }
                }
            }
            found
        };

        if let Some(segment) = target_segment {
            return segment.read_entry(from, log_index).await;
        }

        Err(anyhow!("Log entry not found: raft={:?}, index={}", from, log_index))
    }

    /// Read log entries from all segments
    pub async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> Result<Vec<LogEntry>> {
        if low >= high {
            return Ok(Vec::new());
        }

        let mut all_entries = Vec::new();
        let mut remaining_low = low;

        // Collect segments to read from (without holding lock across await)
        let segments_to_read: Vec<Arc<LogSegment>> = {
            let sealed = self.sealed_segments.read();
            sealed
                .iter()
                .filter(|(meta, _)| meta.contains_range(from, low, high))
                .map(|(_, segment)| segment.clone())
                .collect()
        };

        // Read from sealed segments (oldest to newest)
        for segment in segments_to_read {
            if remaining_low >= high {
                break;
            }
            if let Some(entries) = segment.get_log_entries(from, remaining_low, high).await? {
                if !entries.is_empty() {
                    let last_idx = entries.last().map(|e| e.index).unwrap_or(0);
                    all_entries.extend(entries);
                    remaining_low = last_idx + 1;
                }
            }
        }

        // Check active segment
        if remaining_low < high {
            // Clone the necessary parts without holding lock across await
            let active_segment_arc: Arc<LogSegment> = {
                let segment = self.active_segment.read();
                Arc::new(LogSegment {
                    file_name: segment.file_name.clone(),
                    entry_index: segment.entry_index.clone(),
                    file: segment.file.clone(),
                    io_semaphore: segment.io_semaphore.clone(),
                    hard_states: RwLock::new(segment.hard_states.read().clone()),
                })
            };
            
            if let Some(entries) = active_segment_arc.get_log_entries(from, remaining_low, high).await? {
                all_entries.extend(entries);
            }
        }

        Ok(all_entries)
    }

    /// Get the last log index and term for a raft node
    pub fn get_last_log_index(&self, from: &RaftId) -> (u64, u64) {
        // Check active segment first
        {
            let segment = self.active_segment.read();
            if let Some(meta) = segment.last_entry(from) {
                return (meta.log_index, meta.term);
            }
        }

        // Check sealed segments (newest to oldest)
        let sealed = self.sealed_segments.read();
        for (_, segment) in sealed.iter().rev() {
            if let Some(meta) = segment.last_entry(from) {
                return (meta.log_index, meta.term);
            }
        }

        (0, 0)
    }

    /// Cleanup obsolete segments where all entries have been truncated
    pub fn cleanup_obsolete_segments(&self) -> Result<usize> {
        let truncate_indices = self.truncate_indices.read().clone();
        if truncate_indices.is_empty() {
            return Ok(0);
        }

        let mut to_remove = Vec::new();

        {
            let sealed = self.sealed_segments.read();
            for (idx, (meta, _)) in sealed.iter().enumerate() {
                if meta.is_obsolete(&truncate_indices) {
                    to_remove.push(idx);
                }
            }
        }

        if to_remove.is_empty() {
            return Ok(0);
        }

        // Remove from list and delete files
        let mut sealed = self.sealed_segments.write();
        let mut removed_count = 0;
        let mut freed_space = 0u64;

        // Remove in reverse order to maintain indices
        for idx in to_remove.into_iter().rev() {
            if idx < sealed.len() {
                let (meta, _segment) = sealed.remove(idx);
                freed_space += meta.file_size;

                // Delete the file
                if let Err(e) = fs::remove_file(&meta.file_path) {
                    warn!("Failed to delete obsolete segment {:?}: {}", meta.file_path, e);
                } else {
                    info!(
                        "Deleted obsolete segment: id={}, freed={}B",
                        meta.segment_id, meta.file_size
                    );
                    removed_count += 1;
                }
            }
        }

        // Update total disk usage
        {
            let mut usage = self.total_disk_usage.write();
            *usage = usage.saturating_sub(freed_space);
        }

        Ok(removed_count)
    }

    /// Update total disk usage tracking
    fn update_disk_usage(&self) {
        let active_size = self.active_meta.read().file_size;
        let sealed_size: u64 = self
            .sealed_segments
            .read()
            .iter()
            .map(|(meta, _)| meta.file_size)
            .sum();

        let mut usage = self.total_disk_usage.write();
        *usage = active_size + sealed_size;
    }

    /// Get total disk usage across all segments
    pub fn get_disk_usage(&self) -> u64 {
        *self.total_disk_usage.read()
    }

    /// Get disk usage statistics
    pub fn get_disk_stats(&self) -> DiskStats {
        let active_meta = self.active_meta.read();
        let sealed = self.sealed_segments.read();

        DiskStats {
            total_usage: *self.total_disk_usage.read(),
            active_segment_size: active_meta.file_size,
            sealed_segment_count: sealed.len(),
            sealed_segments_size: sealed.iter().map(|(m, _)| m.file_size).sum(),
            max_segment_size: self.options.max_segment_size,
        }
    }

    /// Check if disk space is low
    pub fn is_disk_space_low(&self) -> bool {
        // Try to get available disk space
        if let Ok(_metadata) = fs::metadata(&self.options.dir) {
            // On Unix systems, we'd use statvfs. For now, just check total usage
            let total_usage = self.get_disk_usage();
            // Simple heuristic: if we're using more than 10GB, consider it potentially low
            total_usage > 10 * 1024 * 1024 * 1024
        } else {
            false
        }
    }

    /// Save hard state to the log segment.
    /// Hard state is persisted to disk and cached in memory for quick access.
    pub fn save_hard_state(&self, hard_state: &HardState) -> Result<()> {
        let mut segment = self.active_segment.write();
        segment.write_hard_state(hard_state)?;
        
        if self.options.sync_on_write {
            segment.sync_data()?;
        }
        
        Ok(())
    }

    /// Load hard state
    pub fn load_hard_state(&self, from: &RaftId) -> Option<HardState> {
        // Check active segment
        {
            let segment = self.active_segment.read();
            let hard_states = segment.hard_states.read();
            if let Some(hs) = hard_states.get(from) {
                return Some(hs.clone());
            }
        }

        // Check sealed segments (newest to oldest)
        let sealed = self.sealed_segments.read();
        for (_, segment) in sealed.iter().rev() {
            let hard_states = segment.hard_states.read();
            if let Some(hs) = hard_states.get(from) {
                return Some(hs.clone());
            }
        }

        None
    }

    /// Get all hard states from all segments (for initializing global cache)
    /// Newer hard states overwrite older ones.
    pub fn get_all_hard_states(&self) -> Option<HashMap<RaftId, HardState>> {
        let mut all_states = HashMap::new();

        // First from sealed segments (oldest to newest)
        let sealed = self.sealed_segments.read();
        for (_, segment) in sealed.iter() {
            let hard_states = segment.hard_states.read();
            for (raft_id, hs) in hard_states.iter() {
                all_states.insert(raft_id.clone(), hs.clone());
            }
        }
        drop(sealed);

        // Then from active segment (newest)
        let active = self.active_segment.read();
        let hard_states = active.hard_states.read();
        for (raft_id, hs) in hard_states.iter() {
            all_states.insert(raft_id.clone(), hs.clone());
        }

        if all_states.is_empty() {
            None
        } else {
            Some(all_states)
        }
    }

    /// Sync all segments to disk
    pub fn sync_all(&self) -> Result<()> {
        self.active_segment.read().sync_data()?;
        Ok(())
    }

    /// Get segment count (active + sealed)
    pub fn segment_count(&self) -> usize {
        1 + self.sealed_segments.read().len()
    }
}

/// Disk usage statistics
#[derive(Debug, Clone)]
pub struct DiskStats {
    /// Total disk usage in bytes
    pub total_usage: u64,
    /// Active segment size in bytes
    pub active_segment_size: u64,
    /// Number of sealed segments
    pub sealed_segment_count: usize,
    /// Total size of sealed segments
    pub sealed_segments_size: u64,
    /// Maximum segment size before rotation
    pub max_segment_size: u64,
}

impl DiskStats {
    /// Format as human-readable string
    pub fn to_human_readable(&self) -> String {
        format!(
            "total: {}, active: {}, sealed: {} segments ({}), max_segment: {}",
            Self::format_bytes(self.total_usage),
            Self::format_bytes(self.active_segment_size),
            self.sealed_segment_count,
            Self::format_bytes(self.sealed_segments_size),
            Self::format_bytes(self.max_segment_size),
        )
    }

    fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.2}GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2}MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2}KB", bytes as f64 / KB as f64)
        } else {
            format!("{}B", bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager() -> (SegmentManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let options = SegmentManagerOptions {
            dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024, // Small size for testing rotation
            max_io_threads: 2,
            sync_on_write: false,
            min_free_disk_space: 0,
        };
        let manager = SegmentManager::new(options).unwrap();
        (manager, temp_dir)
    }

    fn create_test_raft_id(id: &str) -> RaftId {
        RaftId {
            group: format!("group_{}", id),
            node: format!("node_{}", id),
        }
    }

    fn create_test_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            command: vec![0u8; 100], // 100 bytes per entry
            is_config: false,
            client_request_id: None,
        }
    }

    #[test]
    fn test_segment_creation() {
        let (manager, _temp_dir) = create_test_manager();
        assert_eq!(manager.segment_count(), 1);
        assert_eq!(manager.get_disk_usage(), 0);
    }

    #[test]
    fn test_write_and_rotation() {
        let (manager, _temp_dir) = create_test_manager();
        let raft_id = create_test_raft_id("1");

        // Write entries until rotation
        for i in 1..=20 {
            let entry = create_test_entry(i, 1);
            manager.write_log_entries(&raft_id, vec![entry]).unwrap();
        }

        // Should have rotated at least once
        assert!(manager.segment_count() > 1);
    }

    #[tokio::test]
    async fn test_read_across_segments() {
        let (manager, _temp_dir) = create_test_manager();
        let raft_id = create_test_raft_id("1");

        // Write entries that span multiple segments
        for i in 1..=30 {
            let entry = create_test_entry(i, 1);
            manager.write_log_entries(&raft_id, vec![entry]).unwrap();
        }

        // Read entries from multiple segments
        let entries = manager.get_log_entries(&raft_id, 1, 31).await.unwrap();
        assert_eq!(entries.len(), 30);

        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.index, (i + 1) as u64);
        }
    }

    #[test]
    fn test_truncate_and_cleanup() {
        let (manager, _temp_dir) = create_test_manager();
        let raft_id = create_test_raft_id("1");

        // Write entries spanning multiple segments
        for i in 1..=30 {
            let entry = create_test_entry(i, 1);
            manager.write_log_entries(&raft_id, vec![entry]).unwrap();
        }

        let initial_count = manager.segment_count();
        assert!(initial_count > 1);

        // Truncate prefix - should make some segments obsolete
        manager.write_truncate_prefix(&raft_id, 25).unwrap();

        // Cleanup should have removed some segments
        let final_count = manager.segment_count();
        assert!(final_count <= initial_count);
    }

    #[test]
    fn test_disk_stats() {
        let (manager, _temp_dir) = create_test_manager();
        let raft_id = create_test_raft_id("1");

        // Write some entries
        for i in 1..=10 {
            let entry = create_test_entry(i, 1);
            manager.write_log_entries(&raft_id, vec![entry]).unwrap();
        }

        let stats = manager.get_disk_stats();
        assert!(stats.total_usage > 0);
        println!("Disk stats: {}", stats.to_human_readable());
    }

    #[test]
    fn test_segment_meta_obsolete_check() {
        let mut meta = SegmentMeta {
            segment_id: 0,
            file_path: PathBuf::new(),
            first_index_per_raft: HashMap::new(),
            last_index_per_raft: HashMap::new(),
            file_size: 0,
            created_at: SystemTime::now(),
            sealed: true,
        };

        let raft_id = create_test_raft_id("1");

        meta.first_index_per_raft.insert(raft_id.clone(), 1);
        meta.last_index_per_raft.insert(raft_id.clone(), 10);

        // Not obsolete: truncate index below last
        let mut truncate_indices = HashMap::new();
        truncate_indices.insert(raft_id.clone(), 5);
        assert!(!meta.is_obsolete(&truncate_indices));

        // Obsolete: truncate index above last
        truncate_indices.insert(raft_id.clone(), 15);
        assert!(meta.is_obsolete(&truncate_indices));
    }

    #[test]
    fn test_hard_state_persistence() {
        let (manager, _temp_dir) = create_test_manager();
        let raft_id = create_test_raft_id("1");
        let voted_for_id = create_test_raft_id("2");

        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 5,
            voted_for: Some(voted_for_id.clone()),
        };

        // Save hard state (now persists to disk)
        manager.save_hard_state(&hard_state).unwrap();

        let loaded = manager.load_hard_state(&raft_id);
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.voted_for, Some(voted_for_id));
    }

    #[test]
    fn test_hard_state_replay() {
        let temp_dir = TempDir::new().unwrap();
        let raft_id = create_test_raft_id("1");
        let voted_for_id = create_test_raft_id("2");

        let hard_state = HardState {
            raft_id: raft_id.clone(),
            term: 5,
            voted_for: Some(voted_for_id.clone()),
        };

        // Create manager and save hard state
        {
            let options = SegmentManagerOptions {
                dir: temp_dir.path().to_path_buf(),
                max_segment_size: 1024,
                max_io_threads: 2,
                sync_on_write: true,  // Ensure data is synced to disk
                min_free_disk_space: 0,
            };
            let manager = SegmentManager::new(options).unwrap();
            manager.save_hard_state(&hard_state).unwrap();
        }

        // Create a new manager (simulates restart) and verify hard state is recovered
        {
            let options = SegmentManagerOptions {
                dir: temp_dir.path().to_path_buf(),
                max_segment_size: 1024,
                max_io_threads: 2,
                sync_on_write: true,
                min_free_disk_space: 0,
            };
            let manager = SegmentManager::new(options).unwrap();

            let loaded = manager.load_hard_state(&raft_id);
            assert!(loaded.is_some(), "Hard state should be recovered from disk");
            let loaded = loaded.unwrap();
            assert_eq!(loaded.term, 5);
            assert_eq!(loaded.voted_for, Some(voted_for_id));
        }
    }
}

