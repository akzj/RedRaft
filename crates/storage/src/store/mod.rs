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
use crate::snapshot::{SegmentGenerator, SnapshotConfig, WalWriter};
use crate::traits::StoreError;
use anyhow::Result;
use parking_lot::RwLock;
use resp::Command;
use rr_core::routing::RoutingTable;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::{error, info};

/// Sharded Store with RocksDB and Memory backends
///
/// Lock Strategy:
/// - Read/Write operations: Use read lock (shared access)
/// - Snapshot operations: Use write lock (exclusive access, released immediately after snapshot creation)
#[derive(Clone)]
pub struct SlotStore {
    rocksdb: SlotRocksDB,
    memory: MemStoreCow,
}

/// Locked Sharded Store with RwLock protection
///
/// Lock usage:
/// - Normal operations (get, set, etc.): Use `.read()` for read lock
/// - Snapshot creation: Use `.write()` for write lock, release immediately after snapshot
pub type LockedSlotStore = Arc<RwLock<SlotStore>>;

impl SlotStore {
    /// Create a new SlotStore
    pub fn new(rocksdb: SlotRocksDB, memory: MemStoreCow) -> Self {
        Self { rocksdb, memory }
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
}

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
///
/// Storage architecture:
/// - RocksDB: Only String and Hash (persistent, don't need WAL)
/// - Memory store: All other data structures (List, Set, ZSet, Bitmap, etc.) - need WAL for recovery
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

    routing_table: Arc<RoutingTable>,

    last_applied_index: Arc<AtomicU64>,

    /// Snapshot configuration
    snapshot_config: SnapshotConfig,

    /// RocksDB path (for creating new slots)
    rocksdb_path: PathBuf,
}

impl HybridStore {
    /// Create a new HybridStore
    ///
    /// # Arguments
    /// - `snapshot_config`: Snapshot configuration
    /// - `data_dir`: Data directory path
    /// - `routing_table`: Routing table for slot management (managed by node)
    pub fn new(
        snapshot_config: SnapshotConfig,
        data_dir: PathBuf,
        routing_table: Arc<rr_core::routing::RoutingTable>,
    ) -> Result<Self, String> {
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

        Ok(Self {
            rocksdb,
            rocksdb_path,
            snapshot_config,
            last_applied_index: Arc::new(AtomicU64::new(0)),
            slots: Arc::new(RwLock::new(HashMap::new())),
            wal_writer: Arc::new(RwLock::new(wal_writer)),
            segment_generator: Arc::new(RwLock::new(segment_generator)),
            routing_table,
        })
    }

    /// Get slot number for a key
    pub(crate) fn slot_for_key(&self, key: &[u8]) -> u32 {
        RoutingTable::slot_for_key(key)
    }

    /// Get shard ID for a key using routing table
    /// Note: This is kept for backward compatibility (e.g., snapshot interface)
    /// Storage layer no longer uses shard_id internally
    pub(crate) fn shard_for_key(&self, key: &[u8]) -> Result<String, StoreError> {
        self.routing_table
            .find_shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    /// Get or create slot store for a key
    /// Returns the slot store for the slot that the key belongs to
    pub(crate) fn get_slot_store(&self, key: &[u8]) -> Result<LockedSlotStore, StoreError> {
        let slot = self.slot_for_key(key);
        let slots = self.slots.read();

        let Some(slot_store) = slots.get(&slot) else {
            return Err(StoreError::Internal(format!(
                "Slot store not found for slot {} (key: {:?})",
                slot,
                String::from_utf8_lossy(key)
            )));
        };
        Ok(Arc::clone(slot_store))
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
    /// - `command`: Command to write
    pub fn write_wal_if_needed(&self, apply_index: u64, command: &Command) -> Result<()> {
        if !Self::needs_wal_logging(command) {
            return Ok(());
        }

        if let Some(key) = command.get_key() {
            if let Err(e) = self
                .wal_writer
                .write()
                .write_entry(apply_index, command, key)
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
        // 1. Execute command using RedisStore trait's apply method
        let result = crate::traits::RedisStore::apply(self, read_index, apply_index, command);

        // 2. Write to WAL if needed (only for memory store write commands)
        if let Err(e) = self.write_wal_if_needed(apply_index, command) {
            error!("Failed to write WAL entry at index {}: {}", apply_index, e);
            return crate::traits::ApplyResult::Error(StoreError::Internal(e.to_string()));
        }

        // 3. Update last_applied_index for write commands
        if command.is_write() {
            self.last_applied_index
                .store(apply_index, std::sync::atomic::Ordering::SeqCst);
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
        let mut segments_generated = 0;
        let mut min_apply_index = u64::MAX;

        // Generate segment for each slot
        for (slot, slot_store) in slots.iter() {
            let slot_guard = slot_store.read();
            let memory_store = slot_guard.memory();

            // Get current apply_index from WAL
            // TODO: Apply index should be managed at business layer, not storage layer
            let apply_index = 0;

            let slot_id = format!("slot_{}", slot);
            match segment_generator.generate_segment(&slot_id, memory_store, apply_index) {
                Ok(metadata) => {
                    segments_generated += 1;
                    min_apply_index = min_apply_index.min(metadata.apply_index);
                    info!(
                        "Generated segment for slot {} at apply_index {}",
                        slot, metadata.apply_index
                    );
                }
                Err(e) => {
                    error!("Failed to generate segment for slot {}: {}", slot, e);
                    // Continue with other slots
                }
            }
        }

        // Mark generation as complete
        segment_generator.mark_complete();

        // Clean up old WAL files if we generated any segments
        if segments_generated > 0 && min_apply_index < u64::MAX {
            let mut wal_writer = self.wal_writer.write();
            match wal_writer.cleanup_old_files(min_apply_index) {
                Ok(deleted_count) => {
                    info!(
                        "Cleaned up {} old WAL files after segment generation",
                        deleted_count
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
    /// This spawns a tokio task that periodically checks if segment generation
    /// should be triggered and generates segments if needed.
    ///
    /// # Arguments
    /// - `check_interval_secs`: How often to check (in seconds)
    ///
    /// # Returns
    /// Handle to the background task (can be used to cancel it)
    pub fn start_segment_generation_task(
        self: &Arc<Self>,
        check_interval_secs: u64,
    ) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(check_interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

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
