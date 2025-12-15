//! Hybrid Storage Manager
//!
//! Unified interface for multiple storage backends:
//! - RocksDB: String, Hash (persistent)
//! - Memory: List, Set, ZSet (volatile)
//! - StreamStore: Stream (persistent)
//!
//! ## Path Structure
//!
//! All backends follow: shard_id -> key -> value

// Import implementations (they implement traits on HybridStore)
mod hash;
mod key;
mod list;
mod redis;
mod set;
mod snapshot;
mod string;

use crate::memory;
use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use crate::snapshot::{SegmentGenerator, SnapshotConfig, WalWriter};
use crate::traits::StoreError;
use anyhow::Result;
use parking_lot::RwLock;
use resp::Command;
use rr_core::routing::RoutingTable;
use rr_core::shard::{ShardRouting, TOTAL_SLOTS};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

/// Sharded Store with RocksDB and Memory backends
///
/// Lock Strategy:
/// - Read/Write operations: Use read lock (shared access)
/// - Snapshot operations: Use write lock (exclusive access, released immediately after snapshot creation)
#[derive(Clone)]
pub struct ShardedStore {
    rocksdb: ShardedRocksDB,
    memory: memory::ShardStore,
}

/// Locked Sharded Store with RwLock protection
///
/// Lock usage:
/// - Normal operations (get, set, etc.): Use `.read()` for read lock
/// - Snapshot creation: Use `.write()` for write lock, release immediately after snapshot
pub type LockedShardedStore = Arc<RwLock<ShardedStore>>;

impl ShardedStore {
    /// Create a new ShardedStore
    pub fn new(rocksdb: ShardedRocksDB, memory: memory::ShardStore) -> Self {
        Self { rocksdb, memory }
    }

    /// Get reference to RocksDB (for read operations)
    pub fn rocksdb(&self) -> &ShardedRocksDB {
        &self.rocksdb
    }

    /// Get mutable reference to RocksDB (for write operations)
    pub fn rocksdb_mut(&mut self) -> &mut ShardedRocksDB {
        &mut self.rocksdb
    }

    /// Get reference to Memory store (for read operations)
    pub fn memory(&self) -> &memory::ShardStore {
        &self.memory
    }

    /// Get mutable reference to Memory store (for write operations)
    pub fn memory_mut(&mut self) -> &mut memory::ShardStore {
        &mut self.memory
    }
}

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
pub struct HybridStore {
    /// RocksDB for String, Hash (shared across all shards)
    pub(crate) rocksdb: Arc<ShardedRocksDB>,

    /// Shards: RocksDB + Memory per shard
    pub(crate) shards: Arc<RwLock<HashMap<ShardId, LockedShardedStore>>>,

    /// WAL Writer for logging all write operations
    wal_writer: Arc<RwLock<WalWriter>>,

    /// Segment Generator for periodic full snapshots
    segment_generator: Arc<RwLock<SegmentGenerator>>,

    routing_table: Arc<RoutingTable>,

    /// Snapshot configuration
    snapshot_config: SnapshotConfig,

    /// RocksDB path (for creating new shards)
    rocksdb_path: PathBuf,
}

impl HybridStore {
    /// Create a new HybridStore
    ///
    /// # Arguments
    /// - `snapshot_config`: Snapshot configuration
    /// - `data_dir`: Data directory path
    /// - `routing_table`: Routing table for shard management (managed by node)
    pub fn new(
        snapshot_config: SnapshotConfig,
        data_dir: PathBuf,
        routing_table: Arc<rr_core::routing::RoutingTable>,
    ) -> Result<Self, String> {
        // Initialize RocksDB
        let rocksdb_path = data_dir.join("rocksdb");
        let rocksdb = Arc::new(
            ShardedRocksDB::new(&rocksdb_path, routing_table.clone())
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
            shards: Arc::new(RwLock::new(HashMap::new())),
            wal_writer: Arc::new(RwLock::new(wal_writer)),
            segment_generator: Arc::new(RwLock::new(segment_generator)),
            routing_table,
        })
    }

    /// Get shard ID for a key using routing table
    pub(crate) fn shard_for_key(&self, key: &[u8]) -> Result<ShardId, StoreError> {
        self.routing_table
            .find_shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    /// Get or create shard for a key
    pub(crate) fn get_shard(&self, key: &[u8]) -> Result<LockedShardedStore, StoreError> {
        let shard_id = self.shard_for_key(key)?;
        let shards = self.shards.read();

        let Some(shard) = shards.get(&shard_id) else {
            return Err(StoreError::ShardNotFound(shard_id));
        };
        Ok(Arc::clone(shard))
    }

    /// Apply command with apply_index (for WAL logging)
    ///
    /// This method executes the command and writes it to WAL for recovery.
    pub fn apply_with_index(
        &self,
        apply_index: u64,
        command: &Command,
    ) -> crate::traits::ApplyResult {
        // 1. Execute command using RedisStore trait's apply method
        let result = crate::traits::RedisStore::apply(self, command);

        // 2. Extract key and write to WAL (only for write commands)
        if Self::is_write_command(command) {
            if let Some(key) = command.get_key() {
                if let Err(e) = self
                    .wal_writer
                    .write()
                    .write_entry(apply_index, command, key)
                {
                    error!("Failed to write WAL entry at index {}: {}", apply_index, e);
                    // Don't fail the command execution if WAL write fails
                }
            }
        }

        result
    }

    /// Flush WAL to disk (called when Raft triggers snapshot)
    /// Flush all data to disk (WAL and RocksDB)
    ///
    /// Ensures all writes are persisted to disk by flushing both WAL and RocksDB.
    pub fn flush(&self) -> Result<(), String> {
        // Flush WAL to ensure all writes are persisted
        self.flush_wal()?;

        // Flush RocksDB to ensure all writes are persisted
        self.rocksdb
            .flush()
            .map_err(|e| format!("Failed to flush RocksDB: {}", e))?;

        Ok(())
    }

    /// Flush WAL only
    pub fn flush_wal(&self) -> Result<(), String> {
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

    /// Generate segments for all shards (background task)
    ///
    /// This method:
    /// 1. Generates segments for all shards (using read lock, doesn't block writes)
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

        let shards = self.shards.read();
        let mut segments_generated = 0;
        let mut min_apply_index = u64::MAX;

        // Generate segment for each shard
        for (shard_id, shard) in shards.iter() {
            let shard_guard = shard.read();
            let memory_store = &shard_guard.memory().store;

            // Get current apply_index from shard metadata (if available)
            // For now, use WAL size as a proxy for apply_index
            // TODO: Track actual apply_index per shard
            let apply_index = shard_guard.memory().metadata.apply_index.unwrap_or(0);

            match segment_generator.generate_segment(shard_id, memory_store, apply_index) {
                Ok(metadata) => {
                    segments_generated += 1;
                    min_apply_index = min_apply_index.min(metadata.apply_index);
                    info!(
                        "Generated segment for shard {} at apply_index {}",
                        shard_id, metadata.apply_index
                    );
                }
                Err(e) => {
                    error!("Failed to generate segment for shard {}: {}", shard_id, e);
                    // Continue with other shards
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotConfig;
    use bytes::Bytes;
    use resp::Command;
    use tempfile::TempDir;

    #[test]
    fn test_hybrid_store_new() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let config = SnapshotConfig {
            base_dir: data_dir.clone(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let routing_table = Arc::new(rr_core::routing::RoutingTable::new());
        let store = HybridStore::new(config, data_dir, routing_table);
        assert!(store.is_ok());
    }

    #[test]
    fn test_apply_with_index_writes_to_wal() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let config = SnapshotConfig {
            base_dir: data_dir.clone(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let routing_table = Arc::new(rr_core::routing::RoutingTable::new());
        // Add a test shard routing that covers all slots for testing
        routing_table.add_shard_routing(ShardRouting::new(
            "shard_0".to_string(),
            0,
            TOTAL_SLOTS - 1,
        ));

        let store = HybridStore::new(config, data_dir.clone(), routing_table).unwrap();

        // Apply a write command
        let key = b"test_key".to_vec();
        let command = Command::Set {
            key: Bytes::from(b"test_key" as &[u8]),
            value: Bytes::from(b"test_value" as &[u8]),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };

        let result = store.apply_with_index(1, &command);
        assert!(matches!(result, crate::traits::ApplyResult::Ok));

        // Flush WAL and verify it was written
        store.flush_wal().unwrap();

        // Verify WAL file exists
        let wal_dir = data_dir.join("wal");
        assert!(wal_dir.exists());
    }

    #[test]
    fn test_command_get_key() {
        // Test string commands
        let key = Bytes::from(b"test_key" as &[u8]);
        let value = Bytes::from(b"value" as &[u8]);
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        assert_eq!(command.get_key(), Some(key.as_ref()));

        // Test hash commands
        let hset = Command::HSet {
            key: key.clone(),
            fvs: vec![(
                Bytes::from(b"field" as &[u8]),
                Bytes::from(b"value" as &[u8]),
            )],
        };
        assert_eq!(hset.get_key(), Some(key.as_ref()));

        // Test read-only commands (should still extract key for WAL)
        let get = Command::Get { key: key.clone() };
        assert_eq!(get.get_key(), Some(key.as_ref()));

        // Test no-key commands
        let ping = Command::Ping { message: None };
        assert_eq!(ping.get_key(), None);
    }

    #[test]
    fn test_is_write_command() {
        let key = Bytes::from(b"test_key" as &[u8]);

        // Write command
        let set = Command::Set {
            key: key.clone(),
            value: Bytes::from(b"value" as &[u8]),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        assert!(HybridStore::is_write_command(&set));

        // Read command
        let get = Command::Get { key: key.clone() };
        assert!(!HybridStore::is_write_command(&get));
    }
}
