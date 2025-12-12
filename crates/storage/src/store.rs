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

use crate::memory;
use crate::memory::DataCow;
use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use crate::snapshot::{SegmentGenerator, SnapshotConfig, WalWriter};
use crate::traits::{
    HashStore, KeyStore, ListStore, RedisStore, SetStore, SnapshotStore, SnapshotStoreEntry,
    StoreError, StoreResult, StringStore,
};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use resp::Command;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync;
use tracing::{error, info};

/// Sharded Store with RocksDB and Memory backends
///
/// Lock Strategy:
/// - Read/Write operations: Use read lock (shared access)
/// - Snapshot operatio
/// ns: Use write lock (exclusive access, released immediately after snapshot creation)
///
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
    rocksdb: Arc<ShardedRocksDB>,

    /// Shards: RocksDB + Memory per shard
    shards: Arc<RwLock<HashMap<ShardId, LockedShardedStore>>>,

    /// WAL Writer for logging all write operations
    wal_writer: Arc<RwLock<WalWriter>>,

    /// Segment Generator for periodic full snapshots
    segment_generator: Arc<RwLock<SegmentGenerator>>,

    /// Snapshot configuration
    snapshot_config: SnapshotConfig,

    /// RocksDB path (for creating new shards)
    rocksdb_path: PathBuf,
}

impl HybridStore {
    /// Create a new HybridStore
    pub fn new(snapshot_config: SnapshotConfig, data_dir: PathBuf) -> Result<Self, String> {
        // Initialize RocksDB
        let rocksdb_path = data_dir.join("rocksdb");
        let rocksdb = Arc::new(
            ShardedRocksDB::new(&rocksdb_path)
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
        })
    }

    /// Get or create shard for a key
    fn get_create_shard(&self, key: &[u8]) -> StoreResult<LockedShardedStore> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
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
    fn is_write_command(command: &Command) -> bool {
        // Use Command's built-in method
        command.is_write()
    }
}

// ============================================================================
// RedisStore Trait Implementation
// ============================================================================

impl StringStore for HybridStore {
    fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .get(&shard_id, key)
            .map(|v| Bytes::from(v)))
    }

    fn set(&self, key: &[u8], value: Bytes) -> StoreResult<()> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e))
    }

    fn setnx(&self, key: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        if shard_guard.rocksdb().get(&shard_id, key).is_some() {
            return Ok(false);
        }
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e))?;
        Ok(true)
    }

    fn setex(&self, key: &[u8], value: Bytes, _ttl_secs: u64) -> StoreResult<()> {
        // TODO: Implement expiration
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e))
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .incrby(&shard_id, key, delta)
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn append(&self, key: &[u8], value: &[u8]) -> StoreResult<usize> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().append(&shard_id, key, value))
    }

    fn strlen(&self, key: &[u8]) -> StoreResult<usize> {
        match self.get(key)? {
            Some(v) => Ok(v.len()),
            None => Ok(0),
        }
    }
}

impl HashStore for HybridStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .hget(&shard_id, key, field)
            .map(|v| Bytes::from(v)))
    }

    fn hset(&self, key: &[u8], field: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .hset(&shard_id, key, field.as_ref(), value))
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> StoreResult<Vec<Option<Bytes>>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(fields
            .iter()
            .map(|f| {
                shard_guard
                    .rocksdb()
                    .hget(&shard_id, key, f)
                    .map(|v| Bytes::from(v))
            })
            .collect())
    }

    fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>) -> StoreResult<()> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.rocksdb().hmset(&shard_id, key, fvs);
        Ok(())
    }

    fn hgetall(&self, key: &[u8]) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hgetall(&shard_id, key);
        Ok(v.into_iter()
            .map(|(f, v)| (Bytes::from(f), Bytes::from(v)))
            .collect())
    }

    fn hkeys(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hkeys(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hvals(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hvals(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hsetnx(&self, key: &[u8], field: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        if shard_guard.rocksdb().hget(&shard_id, key, field).is_some() {
            return Ok(false);
        }
        Ok(shard_guard.rocksdb().hset(&shard_id, key, field, value))
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> StoreResult<usize> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().hdel(&shard_id, key, fields))
    }

    fn hlen(&self, key: &[u8]) -> StoreResult<usize> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().hlen(&shard_id, key))
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.rocksdb().hincrby(&shard_id, key, field, delta)
    }
}

impl ListStore for HybridStore {
    fn lpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lpush(key, values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.rpush(key, values)
    }

    fn lpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lpop(key)
    }

    fn rpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.rpop(key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.lrange(key, start, stop)
    }

    fn llen(&self, key: &[u8]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.llen(key)
    }

    fn lindex(&self, key: &[u8], index: i64) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.lindex(key, index)
    }

    fn lset(&self, key: &[u8], index: i64, value: Bytes) -> StoreResult<()> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lset(key, index, value)
    }

    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<()> {
        // TODO: Implement ltrim
        let _ = (key, start, stop);
        Err(StoreError::NotSupported)
    }

    fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> StoreResult<usize> {
        // TODO: Implement lrem
        let _ = (key, count, value);
        Ok(0)
    }
}

impl SetStore for HybridStore {
    fn sadd(&self, key: &[u8], members: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().store.add(key.to_vec(), member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().store.remove(key, member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn smembers(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        // Get DataCow for the key and extract members
        if let Some(DataCow::Set(set)) = shard_guard.memory().store.get(key) {
            Ok(set.members())
        } else {
            Err(StoreError::WrongType)
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> StoreResult<bool> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.memory().store.contains(key, member))
    }

    fn scard(&self, key: &[u8]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .memory()
            .store
            .len(key)
            .ok_or(StoreError::WrongType)
    }
}

impl KeyStore for HybridStore {
    fn del(&self, keys: &[&[u8]]) -> StoreResult<usize> {
        let mut count = 0;
        for key in keys {
            let shard_id = self
                .rocksdb
                .shard_for_key(key)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            let shard = self.get_create_shard(key)?;
            let mut shard_guard = shard.write();

            // Delete from RocksDB
            if shard_guard.rocksdb().get(&shard_id, key).is_some() {
                let _ = shard_guard.rocksdb_mut().del(&shard_id, key);
                count += 1;
            }

            // Delete from Memory
            if shard_guard.memory().store.contains_key(key) {
                shard_guard.memory_mut().del(key);
                count += 1;
            }
        }
        Ok(count)
    }

    fn exists(&self, keys: &[&[u8]]) -> StoreResult<usize> {
        Ok(keys
            .iter()
            .filter(|key| {
                let Ok(shard_id) = self.rocksdb.shard_for_key(key) else {
                    return false;
                };
                let Ok(shard) = self.get_create_shard(key) else {
                    return false;
                };
                let shard_guard = shard.read();
                shard_guard.rocksdb().get(&shard_id, key).is_some()
                    || shard_guard.memory().store.contains_key(key)
            })
            .count())
    }

    fn keys(&self, _pattern: &[u8]) -> StoreResult<Vec<Bytes>> {
        // TODO: Implement pattern matching
        Ok(Vec::new())
    }

    fn key_type(&self, key: &[u8]) -> StoreResult<Option<&'static str>> {
        let shard_id = self
            .rocksdb
            .shard_for_key(key)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();

        // Check RocksDB first
        if shard_guard.rocksdb().get(&shard_id, key).is_some() {
            return Ok(Some("string"));
        }

        // Check Memory
        Ok(shard_guard.memory().key_type(key))
    }

    fn ttl(&self, _key: &[u8]) -> StoreResult<i64> {
        // TODO: Implement TTL
        Ok(-1)
    }

    fn expire(&self, _key: &[u8], _ttl_secs: u64) -> StoreResult<bool> {
        // TODO: Implement expiration
        Ok(false)
    }

    fn persist(&self, _key: &[u8]) -> StoreResult<bool> {
        // TODO: Implement persistence
        Ok(false)
    }

    fn dbsize(&self) -> StoreResult<usize> {
        let shards = self.shards.read();
        let mut count = 0;
        for shard in shards.values() {
            let shard_guard = shard.read();
            count += shard_guard.memory().key_count();
            // TODO: Count RocksDB keys
        }
        Ok(count)
    }

    fn flushdb(&self) -> StoreResult<()> {
        let mut shards = self.shards.write();
        for shard in shards.values_mut() {
            let mut shard_guard = shard.write();
            // TODO: Flush RocksDB
            // Clear all memory data
            let mut base = shard_guard.memory_mut().store.base.write();
            base.clear();
        }
        Ok(())
    }

    fn rename(&self, _key: &[u8], _new_key: &[u8]) -> StoreResult<()> {
        // TODO: Implement rename
        Err(StoreError::NotSupported)
    }
}

#[async_trait]
impl SnapshotStore for HybridStore {
    /// Flush data to disk for snapshot creation
    ///
    /// Only flushes WAL and RocksDB to ensure all writes are persisted.
    /// Data is already stored on disk (WAL + Segment), so no need to generate or return data.
    async fn create_snapshot(
        &self,
        shard_id: &ShardId,
        channel: tokio::sync::mpsc::Sender<SnapshotStoreEntry>,
    ) -> Result<()> {
        // Get shard (must be done before moving into closure)
        let shard = {
            let shards = self.shards.read();
            shards
                .get(shard_id)
                .cloned()
                .ok_or(StoreError::ShardNotFound(shard_id.clone()))?
        };

        // Clone DB and prepare CF name for the closure
        let db = self.rocksdb.db.clone();
        let cf_name = format!("shard_{}", shard_id);

        // notify the main thread that the snapshot object is created
        // This allows load_snapshot to wait synchronously for snapshot object creation
        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = tokio::task::spawn_blocking(move || {
            // Helper function to send error through channel
            let send_error = |err: StoreError| {
                let _ = channel.blocking_send(SnapshotStoreEntry::Error(err));
            };

            // Create snapshot inside the closure (after db is moved)
            // Note: db.snapshot() returns a snapshot directly, not a Result
            let snapshot = db.snapshot();

            // Get Column Family handle from db (CF handle is valid as long as db exists)
            let cf_handler = match db.cf_handle(&cf_name) {
                Some(handler) => handler,
                None => {
                    let err = StoreError::Internal(format!("Column Family {} not found", cf_name));
                    send_error(err.clone());
                    return;
                }
            };

            // Signal that snapshot object is created (RocksDB snapshot + Memory COW will be created next)
            // This allows load_snapshot to wait synchronously for snapshot object creation
            // Create Memory COW snapshot BEFORE signaling snapshot object is ready
            // This ensures state consistency - snapshot object is fully created before signaling
            let mut shard_guard = shard.write();
            let memory_snapshot = shard_guard.memory_mut().store.make_snapshot();
            drop(shard_guard); // Release write lock immediately
            
            // Signal that snapshot object is fully created (RocksDB snapshot + Memory COW)
            // This allows load_snapshot to wait synchronously for snapshot object creation
            let _ = tx.send(());

            // Iterate RocksDB entries (String and Hash)
            let iter = snapshot.iterator_cf(cf_handler, rocksdb::IteratorMode::Start);
            for item in iter {
                let (key, value) = match item {
                    Ok(kv) => kv,
                    Err(e) => {
                        let err = StoreError::Internal(format!("RocksDB iteration error: {}", e));
                        send_error(err.clone());
                        return;
                    }
                };

                // Skip apply_index key
                if key.starts_with(b"@:") {
                    continue;
                }

                // Parse key to determine type
                if key.starts_with(b"s:") {
                    // String: s:{key} -> value
                    let original_key = &key[2..];
                    let _ = channel.send(SnapshotStoreEntry::String(
                        bytes::Bytes::copy_from_slice(original_key),
                        bytes::Bytes::copy_from_slice(&value),
                    ));
                } else if key.starts_with(b"h:") {
                    // Hash field: h:{key}:{field} -> value
                    // Find the second colon
                    let key_part = &key[2..];
                    if let Some(colon_pos) = key_part.iter().position(|&b| b == b':') {
                        let hash_key = &key_part[..colon_pos];
                        let field = &key_part[colon_pos + 1..];
                        if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Hash(
                            bytes::Bytes::copy_from_slice(hash_key),
                            bytes::Bytes::copy_from_slice(field),
                            bytes::Bytes::copy_from_slice(&value),
                        )) {
                            let err =
                                StoreError::Internal(format!("Failed to send Hash entry: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    }
                }
                // Skip hash metadata (H:{key}) as we only need fields
            }

            // Iterate Memory store entries (List, Set, ZSet, Bitmap)
            // Note: memory_snapshot was already created above before signaling
            let memory_data = memory_snapshot.read();
            for (key, data_cow) in memory_data.iter() {
                match data_cow {
                    crate::memory::DataCow::List(list) => {
                        // Send all list elements
                        for element in list.iter() {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::List(
                                bytes::Bytes::copy_from_slice(key),
                                element.clone(),
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send List entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::Set(set) => {
                        // Send all set members
                        for member in set.members() {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Set(
                                bytes::Bytes::copy_from_slice(key),
                                member.clone(),
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send Set entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::ZSet(zset) => {
                        // Send all zset elements with scores
                        // Use range_by_score to get all (member, score) pairs
                        let members_with_scores =
                            zset.range_by_score(f64::NEG_INFINITY, f64::INFINITY);
                        for (member, score) in members_with_scores {
                            if let Err(e) = channel.blocking_send(SnapshotStoreEntry::ZSet(
                                bytes::Bytes::copy_from_slice(key),
                                score,
                                member,
                            )) {
                                let err = StoreError::Internal(format!(
                                    "Failed to send ZSet entry: {}",
                                    e
                                ));
                                send_error(err.clone());
                                return;
                            }
                        }
                    }
                    crate::memory::DataCow::Bitmap(bitmap) => {
                        // Send bitmap data (BitmapData is Vec<u8>)
                        if let Err(e) = channel.blocking_send(SnapshotStoreEntry::Bitmap(
                            bytes::Bytes::copy_from_slice(key),
                            bytes::Bytes::copy_from_slice(bitmap),
                        )) {
                            let err =
                                StoreError::Internal(format!("Failed to send Bitmap entry: {}", e));
                            send_error(err.clone());
                            return;
                        }
                    }
                }
            }
            drop(memory_data);

            // Send completion signal
            let _ = channel.blocking_send(SnapshotStoreEntry::Completed);

            ()
        })
        .await;

        rx.blocking_recv()?;

        Ok(())
    }

    fn restore_from_snapshot(&self, _snapshot: &[u8]) -> Result<(), String> {
        // TODO: Implement snapshot restoration
        Ok(())
    }

    fn create_split_snapshot(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        // TODO: Implement split snapshot
        Ok(Vec::new())
    }

    fn merge_from_snapshot(&self, _snapshot: &[u8]) -> Result<usize, String> {
        // TODO: Implement merge snapshot
        Ok(0)
    }

    fn delete_keys_in_slot_range(
        &self,
        _slot_start: u32,
        _slot_end: u32,
        _total_slots: u32,
    ) -> usize {
        // TODO: Implement delete keys in slot range
        0
    }
}

// HybridStore implements RedisStore trait (uses default apply method from trait)
impl RedisStore for HybridStore {
    /// Override apply_with_index to use WAL logging
    fn apply_with_index(&self, apply_index: u64, cmd: &Command) -> crate::traits::ApplyResult {
        // Use the existing apply_with_index implementation
        HybridStore::apply_with_index(self, apply_index, cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotConfig;
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

        let store = HybridStore::new(config, data_dir);
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

        let store = HybridStore::new(config, data_dir.clone()).unwrap();

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
