//! Hybrid Storage Manager
//!
//! Unified interface for multiple storage backends:
//! - RocksDB: String, Hash (persistent, don't need WAL)
//! - Memory: List, Set, ZSet, Bitmap, and all other data structures (volatile, need WAL for recovery)
//!
//! ## Storage Architecture
//!
//! Only String and Hash are stored in RocksDB (persistent storage).
//! All other data structures (List, Set, ZSet, Bitmap, etc.) are stored in Memory store
//! and require WAL logging for recovery.
//!
//! ## Path Structure
//!
//! All backends follow: slot -> key -> value

// Import implementations (they implement traits on HybridStore)
mod hash;
mod key;
mod list;
mod redis;
mod set;
mod snapshot;
mod string;
mod zset;

use crate::memory::MemStoreCow;
use crate::rocksdb::SlotRocksDB;
use crate::snapshot::{reload_memstore, SegmentGenerator, SnapshotConfig, WalWriter};
use crate::traits::StoreError;
use anyhow::Result;
use parking_lot::RwLock;
use resp::Command;
use rr_core::routing::RoutingTable;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{error, info, warn};

/// Slot metadata (stored in memory)
#[derive(Debug, Clone)]
pub struct SlotMetadata {
    /// Slot number
    pub slot: u32,
    /// Last applied log index for this slot
    pub applied_index: u64,
    /// Last log sequence number
    pub log_seq: u64,
    /// Last log sequence number in segment (for log compression)
    /// If log_seq == segment_log_seq, the slot hasn't changed since last segment generation
    pub segment_log_seq: u64,
}

impl SlotMetadata {
    pub fn new(slot: u32) -> Self {
        Self {
            slot,
            applied_index: 0,
            log_seq: 0,
            segment_log_seq: 0,
        }
    }

    /// Update metadata from ApplyContext
    ///
    /// Updates the slot's metadata (log_seq and applied_index) from the ApplyContext.
    /// Called after write operations to keep metadata synchronized with the Raft log.
    pub fn update_from_context(&mut self, ctx: &crate::traits::ApplyContext) {
        if let Some(log_seq) = ctx.log_seq {
            self.log_seq = log_seq;
        }
        if let Some(apply_index) = ctx.apply_index {
            self.applied_index = apply_index;
        }
    }
}

/// Sharded Store with RocksDB and Memory backends
///
/// Lock Strategy:
/// - Read/Write operations: Use read lock (shared access)
/// - Snapshot operations: Use write lock (exclusive access, released immediately after snapshot creation)
#[derive(Clone)]
pub struct SlotStore {
    rocksdb: SlotRocksDB,
    memory: MemStoreCow,
    metadata: SlotMetadata,
}

/// Locked Sharded Store with RwLock protection
///
/// Lock usage:
/// - Normal operations (get, set, etc.): Use `.read()` for read lock
/// - Snapshot creation: Use `.write()` for write lock, release immediately after snapshot
pub type LockedSlotStore = Arc<RwLock<SlotStore>>;

impl SlotStore {
    /// Create a new SlotStore
    pub fn new(rocksdb: SlotRocksDB, memory: MemStoreCow, slot: u32) -> Self {
        Self {
            rocksdb,
            memory,
            metadata: SlotMetadata::new(slot),
        }
    }

    /// Get reference to RocksDB (for read operations)
    pub fn rocksdb(&self) -> &SlotRocksDB {
        &self.rocksdb
    }

    /// Get mutable reference to RocksDB (for write operations)
    pub fn rocksdb_mut(&mut self) -> &mut SlotRocksDB {
        &mut self.rocksdb
    }

    /// Get reference to Memory store (for read operations)
    pub fn memory(&self) -> &MemStoreCow {
        &self.memory
    }

    /// Get mutable reference to Memory store (for write operations)
    pub fn memory_mut(&mut self) -> &mut MemStoreCow {
        &mut self.memory
    }

    /// Get reference to metadata
    pub fn metadata(&self) -> &SlotMetadata {
        &self.metadata
    }

    /// Get mutable reference to metadata
    pub fn metadata_mut(&mut self) -> &mut SlotMetadata {
        &mut self.metadata
    }
}

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
///
/// Storage architecture:
/// - RocksDB: Only String and Hash (persistent, don't need WAL)
/// - Memory store: All other data structures (List, Set, ZSet, Bitmap, etc.) - need WAL for recovery
///
#[derive(Clone)]
pub struct HybridStore {
    /// RocksDB for String and Hash only (shared across all slots)
    pub(crate) rocksdb: Arc<SlotRocksDB>,

    /// Slots: RocksDB + Memory per slot
    /// Key is slot number (u32), value is the slot store
    pub(crate) slots: Arc<RwLock<HashMap<u32, LockedSlotStore>>>,

    /// WAL Writer for logging all write operations
    wal_writer: Arc<RwLock<WalWriter>>,

    /// Segment Generator for periodic full snapshots
    segment_generator: Arc<RwLock<SegmentGenerator>>,

    /// Global log sequence number (incremented for each WAL entry written)
    /// Used for log compression and segment generation
    log_seq: Arc<AtomicU64>,
}

impl HybridStore {
    /// Create a new HybridStore
    ///
    /// # Arguments
    /// - `snapshot_config`: Snapshot configuration
    /// - `data_dir`: Data directory path
    /// - `routing_table`: Routing table for slot calculation (only used for RocksDB initialization)
    /// - `start_background_task`: Whether to start background segment generation task
    ///
    /// # Returns
    /// - `Ok((store, handle))`: Store instance and optional background task handle
    ///   If `start_background_task` is false, handle will be None
    pub fn new(
        snapshot_config: SnapshotConfig,
        data_dir: PathBuf,
        routing_table: Arc<rr_core::routing::RoutingTable>,
        start_background_task: bool,
    ) -> Result<(Self, Option<std::thread::JoinHandle<()>>), String> {
        // Initialize RocksDB
        let rocksdb_path = data_dir.join("rocksdb");
        let rocksdb = Arc::new(
            SlotRocksDB::new(&rocksdb_path, routing_table.clone())
                .map_err(|e| format!("Failed to initialize RocksDB: {}", e))?,
        );

        // Initialize WAL writer
        let wal_dir = snapshot_config.base_dir.join("wal");
        let wal_writer = WalWriter::new(snapshot_config.clone(), wal_dir.clone())
            .map_err(|e| format!("Failed to initialize WAL writer: {}", e))?;

        // Initialize Segment Generator
        let segments_dir = snapshot_config.base_dir.join("segments");
        let segment_generator = SegmentGenerator::new(snapshot_config.clone(), segments_dir);

        // Initialize slots map
        let slots = Arc::new(RwLock::new(HashMap::new()));

        // Initialize log_seq (will be updated after reload if data exists)
        let log_seq = Arc::new(AtomicU64::new(0));

        // Reload MemStore from segments and WAL if they exist
        match reload_memstore(snapshot_config.clone()) {
            Ok(restored_slots) => {
                let mut max_log_seq = 0u64;
                let mut slots_guard = slots.write();

                for (slot, (mem_store, metadata)) in restored_slots {
                    // Track max log_seq for global log_seq initialization
                    max_log_seq = max_log_seq.max(metadata.log_seq);

                    // Clone RocksDB for this slot
                    let rocksdb = rocksdb.as_ref().clone();
                    let slot_store = SlotStore {
                        rocksdb,
                        memory: mem_store,
                        metadata,
                    };
                    let locked_store = Arc::new(RwLock::new(slot_store));
                    slots_guard.insert(slot, locked_store);
                }

                // Set global log_seq to max restored log_seq (next write will increment)
                // If no data was restored, log_seq remains 0
                if max_log_seq > 0 {
                    log_seq.store(max_log_seq, std::sync::atomic::Ordering::SeqCst);
                }

                info!(
                    "Reloaded {} slots from segments and WAL, max log_seq: {}",
                    slots_guard.len(),
                    max_log_seq
                );
            }
            Err(e) => {
                // If reload fails (e.g., no segments/WAL exist), start fresh
                // This is normal for first-time startup
                warn!(
                    "Failed to reload from segments/WAL (this is normal for first startup): {}",
                    e
                );
            }
        }

        let store = Self {
            rocksdb,
            log_seq,
            slots,
            wal_writer: Arc::new(RwLock::new(wal_writer)),
            segment_generator: Arc::new(RwLock::new(segment_generator)),
        };

        // Start background segment generation task if requested
        let handle = if start_background_task {
            let check_interval = snapshot_config.segment_interval_secs;
            Some(store.start_segment_generation_task(check_interval))
        } else {
            None
        };

        Ok((store, handle))
    }

    /// Get slot number for a key
    pub(crate) fn slot_for_key(&self, key: &[u8]) -> u32 {
        RoutingTable::slot_for_key(key)
    }

    /// Get or create slot store for a key
    /// Returns the slot store for the slot that the key belongs to
    /// Creates a new slot store if it doesn't exist
    pub(crate) fn get_slot_store(&self, key: &[u8]) -> Result<LockedSlotStore, StoreError> {
        let slot = self.slot_for_key(key);

        // Try to get existing slot store
        {
            let slots = self.slots.read();
            if let Some(slot_store) = slots.get(&slot) {
                return Ok(Arc::clone(slot_store));
            }
        }

        // Create new slot store if it doesn't exist
        let mut slots = self.slots.write();
        // Double-check after acquiring write lock (another thread might have created it)
        if let Some(slot_store) = slots.get(&slot) {
            return Ok(Arc::clone(slot_store));
        }

        // Create new slot store
        // RocksDB is shared across all slots, so we clone it
        use crate::memory::MemStore;
        let memory = MemStore::new();
        // Clone SlotRocksDB (which clones Arc<DB>, sharing the same DB instance)
        let rocksdb = self.rocksdb.as_ref().clone();
        let slot_store = SlotStore::new(rocksdb, memory, slot);
        let locked_store = Arc::new(RwLock::new(slot_store));
        slots.insert(slot, Arc::clone(&locked_store));
        Ok(locked_store)
    }

    /// Check if command needs WAL logging
    ///
    /// Storage architecture:
    /// - RocksDB: Only String and Hash (persistent, don't need WAL)
    /// - Memory store: All other data structures (List, Set, ZSet, Bitmap, etc.) - need WAL for recovery
    ///
    /// Only memory store write commands need WAL logging.
    /// RocksDB commands (String, Hash) are already persistent and don't need WAL.
    ///
    /// # Arguments
    /// - `command`: Command to check
    ///
    /// # Returns
    /// `true` if the command operates on memory store and is a write command
    pub(crate) fn needs_wal_logging(command: &Command) -> bool {
        if !command.is_write() {
            return false;
        }

        // Exclude RocksDB write commands (String, Hash) - they don't need WAL
        // All other write commands (List, Set, ZSet, Key commands, etc.) need WAL
        !matches!(
            command,
            // String write commands (RocksDB)
            Command::Set { .. }
            | Command::SetNx { .. }
            | Command::SetEx { .. }
            | Command::PSetEx { .. }
            | Command::MSet { .. }
            | Command::MSetNx { .. }
            | Command::Incr { .. }
            | Command::IncrBy { .. }
            | Command::IncrByFloat { .. }
            | Command::Decr { .. }
            | Command::DecrBy { .. }
            | Command::Append { .. }
            | Command::GetSet { .. }
            | Command::SetRange { .. }
            // Hash write commands (RocksDB)
            | Command::HSet { .. }
            | Command::HSetNx { .. }
            | Command::HMSet { .. }
            | Command::HDel { .. }
            | Command::HIncrBy { .. }
            | Command::HIncrByFloat { .. }
        )
    }

    /// Write command to WAL if needed
    ///
    /// Storage architecture:
    /// - RocksDB: Only String and Hash (don't need WAL)
    /// - Memory store: All other data structures (List, Set, ZSet, Bitmap, etc.) - need WAL
    ///
    /// Only writes to WAL if the command operates on memory store.
    /// RocksDB commands (String, Hash) don't need WAL as they are already persistent.
    ///
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    /// - `log_seq`: Log sequence number for this slot
    /// - `command`: Command to write
    pub fn write_wal_if_needed(
        &self,
        apply_index: u64,
        log_seq: u64,
        command: &Command,
    ) -> Result<()> {
        if !Self::needs_wal_logging(command) {
            return Ok(());
        }

        if let Some(key) = command.get_key() {
            if let Err(e) = self
                .wal_writer
                .write()
                .write_entry(apply_index, log_seq, command, key)
            {
                error!("Failed to write WAL entry at index {}: {}", apply_index, e);
                // Don't fail the command execution if WAL write fails
                return Err(e);
            }
        }

        Ok(())
    }

    /// Apply command with apply_index (for WAL logging)
    ///
    /// This method executes the command and writes it to WAL for recovery.
    ///
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (used for read operations)
    /// - `apply_index`: Raft apply index for WAL logging (used for write operations)
    /// - `command`: Command to execute
    pub fn apply_with_index(
        &self,
        read_index: u64,
        apply_index: u64,
        command: &Command,
    ) -> crate::traits::ApplyResult {
        // 1. Generate log_seq before applying (for write commands that need WAL)
        let log_seq = if Self::needs_wal_logging(command) {
            // Increment global log sequence number atomically before applying
            self.log_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1
        } else {
            0
        };

        // 2. Create context with all metadata
        let mut ctx = crate::traits::ApplyContext {
            read_index: Some(read_index),
            apply_index: Some(apply_index),
            log_seq: Some(log_seq),
            ..Default::default()
        };

        // 3. Set slot if command has a key
        if let Some(key) = command.get_key() {
            ctx.slot = Some(self.slot_for_key(key));
        }

        // 4. Execute command using RedisStore trait's apply_with_context method
        let result = crate::traits::RedisStore::apply_with_context(self, &ctx, command);

        // 5. Write to WAL if needed (only for memory store write commands)
        // Note: log_seq was generated above, use it here
        if Self::needs_wal_logging(command) {
            if let Err(e) = self.write_wal_if_needed(apply_index, log_seq, command) {
                error!("Failed to write WAL entry at index {}: {}", apply_index, e);
                return crate::traits::ApplyResult::Error(StoreError::Internal(e.to_string()));
            }
        }

        result
    }

    /// Flush WAL to disk (called when Raft triggers snapshot)
    /// Flush all data to disk (WAL and RocksDB)
    ///
    /// Ensures all writes are persisted to disk by flushing both WAL and RocksDB.
    pub fn flush(&self) -> Result<()> {
        // Flush WAL to ensure all writes are persisted
        self.flush_wal()?;

        // Flush RocksDB to ensure all writes are persisted
        self.rocksdb
            .flush()
            .map_err(|e| anyhow::anyhow!("Failed to flush RocksDB: {}", e))?;

        Ok(())
    }

    /// Flush WAL only
    pub fn flush_wal(&self) -> Result<()> {
        self.wal_writer.write().flush()
    }

    /// Check if segment generation should be triggered
    pub fn should_generate_segment(&self) -> bool {
        let wal_writer = self.wal_writer.read();
        let wal_size = wal_writer.total_size();
        drop(wal_writer);

        let segment_generator = self.segment_generator.read();
        let should = segment_generator.should_generate(wal_size);
        drop(segment_generator);

        should
    }

    /// Generate segments for all slots (background task)
    ///
    /// This method:
    /// 1. Generates segments for all slots (using read lock, doesn't block writes)
    /// 2. Cleans up old WAL files after segment generation
    ///
    /// # Returns
    /// Number of segments generated
    pub fn generate_segments(&self) -> Result<usize, String> {
        let mut segment_generator = self.segment_generator.write();

        // Check if should generate
        let wal_writer = self.wal_writer.read();
        let wal_size = wal_writer.total_size();
        drop(wal_writer);

        if !segment_generator.should_generate(wal_size) {
            return Ok(0);
        }

        let slots = self.slots.read();

        // Generate segments for all slots (new implementation)
        // Convert HashMap to format expected by new SegmentGenerator
        let slots_map: HashMap<u32, LockedSlotStore> =
            slots.iter().map(|(k, v)| (*k, Arc::clone(v))).collect();

        let (segments_generated, min_log_seq) =
            match segment_generator.generate_segments(&slots_map) {
                Ok(segments) => {
                    let count = segments.len();
                    // Find minimum log_seq across all segments (for WAL cleanup)
                    // This is the minimum log_seq that should be kept in WAL files
                    let min_log_seq = segments
                        .iter()
                        .flat_map(|s| s.slot_infos.values().map(|info| info.log_seq))
                        .min()
                        .unwrap_or(u64::MAX);
                    info!(
                        "Generated {} segments for {} slots, min_log_seq: {}",
                        count,
                        slots_map.len(),
                        if min_log_seq == u64::MAX {
                            0
                        } else {
                            min_log_seq
                        }
                    );
                    (count, min_log_seq)
                }
                Err(e) => {
                    error!("Failed to generate segments: {}", e);
                    return Err(e);
                }
            };

        // Mark generation as complete
        segment_generator.mark_complete();

        // Clean up old WAL files if we generated any segments
        if segments_generated > 0 && min_log_seq < u64::MAX {
            let mut wal_writer = self.wal_writer.write();
            match wal_writer.cleanup_old_files(min_log_seq) {
                Ok(deleted_count) => {
                    info!(
                        "Cleaned up {} old WAL files after segment generation (min_log_seq: {})",
                        deleted_count, min_log_seq
                    );
                }
                Err(e) => {
                    error!("Failed to cleanup old WAL files: {}", e);
                }
            }
        }

        Ok(segments_generated)
    }

    /// Start background task for segment generation
    ///
    /// This spawns a thread that periodically checks if segment generation
    /// should be triggered and generates segments if needed.
    ///
    /// # Arguments
    /// - `check_interval_secs`: How often to check (in seconds)
    ///
    /// # Returns
    /// Handle to the background thread (can be used to join it)
    pub fn start_segment_generation_task(
        &self,
        check_interval_secs: u64,
    ) -> std::thread::JoinHandle<()> {
        let store = self.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(check_interval_secs));

                // Check if should generate segments
                if store.should_generate_segment() {
                    match store.generate_segments() {
                        Ok(count) => {
                            if count > 0 {
                                info!(
                                    "Background segment generation: generated {} segments",
                                    count
                                );
                            }
                        }
                        Err(e) => {
                            error!("Background segment generation failed: {}", e);
                        }
                    }
                }
            }
        })
    }

    /// Check if command is a write command (needs WAL logging)
    pub(crate) fn is_write_command(command: &Command) -> bool {
        // Use Command's built-in method
        command.is_write()
    }
}
