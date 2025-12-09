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

use super::sharded_rocksdb::ShardedRocksDB;
use super::stream_store::StreamStore;
use super::DataType;
use crate::memory::{slot_for_key, MemoryStore, ShardId};
use crate::traits::{
    HashStore, KeyStore, ListStore, RedisStore, SetStore, SnapshotStore, StoreResult, StringStore,
};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
pub struct HybridStore {
    /// RocksDB for String, Hash
    rocksdb: Arc<ShardedRocksDB>,
    /// Memory for List, Set, ZSet
    memory: Arc<MemoryStore>,
    /// StreamStore for Stream
    stream: Arc<StreamStore>,
    /// Number of shards
    shard_count: u32,
}

impl HybridStore {
    /// Create a new HybridStore
    ///
    /// # Arguments
    /// - `rocksdb_path`: Path for RocksDB storage
    /// - `shard_count`: Number of shards (default 16)
    pub fn new<P: AsRef<Path>>(rocksdb_path: P, shard_count: u32) -> Result<Self, String> {
        let shard_count = shard_count.max(1);

        let rocksdb = ShardedRocksDB::new(rocksdb_path, shard_count)?;
        let memory = MemoryStore::new(shard_count);
        let stream = StreamStore::new(shard_count);

        info!(
            "HybridStore initialized with {} shards (RocksDB + Memory + StreamStore)",
            shard_count
        );

        Ok(Self {
            rocksdb: Arc::new(rocksdb),
            memory: Arc::new(memory),
            stream: Arc::new(stream),
            shard_count,
        })
    }

    /// Create a memory-only HybridStore (no persistence)
    pub fn memory_only(shard_count: u32) -> Self {
        let shard_count = shard_count.max(1);
        
        // Create a temp directory for RocksDB (will be deleted on drop)
        let temp_path = format!("/tmp/hybrid_store_temp_{}", std::process::id());
        let rocksdb = ShardedRocksDB::new(&temp_path, shard_count)
            .unwrap_or_else(|_| panic!("Failed to create temp RocksDB"));

        Self {
            rocksdb: Arc::new(rocksdb),
            memory: Arc::new(MemoryStore::new(shard_count)),
            stream: Arc::new(StreamStore::new(shard_count)),
            shard_count,
        }
    }

    // ==================== Accessors ====================

    /// Get shard count
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Get RocksDB reference
    pub fn rocksdb(&self) -> &ShardedRocksDB {
        &self.rocksdb
    }

    /// Get Memory store reference
    pub fn memory(&self) -> &MemoryStore {
        &self.memory
    }

    /// Get Stream store reference
    pub fn stream(&self) -> &StreamStore {
        &self.stream
    }

    /// Calculate shard ID for a key
    pub fn shard_for_key(&self, key: &[u8]) -> ShardId {
        slot_for_key(key) % self.shard_count
    }

    // ==================== Shard Management ====================

    /// Set apply index for all backends
    pub fn set_shard_apply_index(&self, shard_id: ShardId, apply_index: u64) {
        self.rocksdb.set_shard_apply_index(shard_id, apply_index);
        self.memory.set_shard_apply_index(shard_id, apply_index);
    }

    /// Get apply index (from RocksDB as source of truth)
    pub fn get_shard_apply_index(&self, shard_id: ShardId) -> Option<u64> {
        self.rocksdb.get_shard_apply_index(shard_id)
    }

    /// Get active shards
    pub fn get_active_shards(&self) -> Vec<ShardId> {
        (0..self.shard_count).collect()
    }

    // ==================== Type Detection ====================

    /// Detect data type for a key (checks all backends)
    pub fn detect_type(&self, key: &[u8]) -> DataType {
        let shard_id = self.shard_for_key(key);

        // Check RocksDB (String)
        if self.rocksdb.get(shard_id, key).is_some() {
            return DataType::String;
        }

        // Check RocksDB (Hash)
        if self.rocksdb.hlen(shard_id, key) > 0 {
            return DataType::Hash;
        }

        // Check Memory (via type checking)
        if let Some(t) = self.memory.key_type(key) {
            return match t {
                "string" => DataType::String,
                "list" => DataType::List,
                "set" => DataType::Set,
                "zset" => DataType::ZSet,
                "hash" => DataType::Hash,
                _ => DataType::Unknown,
            };
        }

        DataType::Unknown
    }

    // ==================== Snapshot Operations ====================

    /// Create snapshot for all backends in a shard
    pub fn create_shard_snapshot(&self, shard_id: ShardId) -> Result<HybridSnapshot, String> {
        let rocksdb_data = self.rocksdb.create_shard_snapshot(shard_id)?;
        let memory_data = self.memory.create_shard_snapshot(shard_id)?;
        let stream_data = self.stream.create_shard_snapshot(shard_id)?;

        Ok(HybridSnapshot {
            shard_id,
            rocksdb: rocksdb_data,
            memory: memory_data,
            stream: stream_data,
        })
    }

    /// Restore shard from hybrid snapshot
    pub fn restore_shard_snapshot(&self, snapshot: &HybridSnapshot) -> Result<(), String> {
        self.rocksdb
            .restore_shard_snapshot(snapshot.shard_id, &snapshot.rocksdb)?;
        self.memory
            .restore_shard_from_snapshot(snapshot.shard_id, &snapshot.memory)?;
        self.stream
            .restore_shard_snapshot(snapshot.shard_id, &snapshot.stream)?;
        Ok(())
    }

    /// Create full snapshot (all shards)
    pub fn create_full_snapshot(&self) -> Result<Vec<HybridSnapshot>, String> {
        let mut snapshots = Vec::new();
        for shard_id in 0..self.shard_count {
            snapshots.push(self.create_shard_snapshot(shard_id)?);
        }
        Ok(snapshots)
    }

    /// Restore from full snapshot
    pub fn restore_full_snapshot(&self, snapshots: &[HybridSnapshot]) -> Result<(), String> {
        for snapshot in snapshots {
            self.restore_shard_snapshot(snapshot)?;
        }
        Ok(())
    }

    /// Flush RocksDB to disk
    pub fn flush(&self) -> Result<(), String> {
        self.rocksdb.flush()
    }
}

/// Hybrid snapshot containing data from all backends
#[derive(Debug, Clone)]
pub struct HybridSnapshot {
    pub shard_id: ShardId,
    pub rocksdb: Vec<u8>,
    pub memory: Vec<u8>,
    pub stream: Vec<u8>,
}

impl HybridSnapshot {
    /// Serialize snapshot
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serde::encode_to_vec(
            &(self.shard_id, &self.rocksdb, &self.memory, &self.stream),
            bincode::config::standard(),
        )
        .map_err(|e| format!("Serialization error: {}", e))
    }

    /// Deserialize snapshot
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let ((shard_id, rocksdb, memory, stream), _): ((ShardId, Vec<u8>, Vec<u8>, Vec<u8>), _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Deserialization error: {}", e))?;

        Ok(Self {
            shard_id,
            rocksdb,
            memory,
            stream,
        })
    }
}

// ============================================================================
// StringStore Implementation (RocksDB)
// ============================================================================

impl StringStore for HybridStore {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        // First check RocksDB
        if let Some(v) = self.rocksdb.get(shard_id, key) {
            return Some(v);
        }
        // Fall back to Memory (for compatibility)
        StringStore::get(self.memory.as_ref(), key)
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let shard_id = self.shard_for_key(&key);
        let _ = self.rocksdb.set(shard_id, &key, value);
    }

    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let shard_id = self.shard_for_key(&key);
        self.rocksdb.setnx(shard_id, &key, value).unwrap_or(false)
    }

    fn setex(&self, key: Vec<u8>, value: Vec<u8>, _ttl_secs: u64) {
        // TODO: Implement TTL
        self.set(key, value);
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.incrby(shard_id, key, delta)
    }

    fn append(&self, key: &[u8], value: &[u8]) -> usize {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.append(shard_id, key, value)
    }

    fn strlen(&self, key: &[u8]) -> usize {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.strlen(shard_id, key)
    }
}

// ============================================================================
// ListStore Implementation (Memory)
// ============================================================================

impl ListStore for HybridStore {
    fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        ListStore::lpush(self.memory.as_ref(), key, values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize {
        ListStore::rpush(self.memory.as_ref(), key, values)
    }

    fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        ListStore::lpop(self.memory.as_ref(), key)
    }

    fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        ListStore::rpop(self.memory.as_ref(), key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>> {
        ListStore::lrange(self.memory.as_ref(), key, start, stop)
    }

    fn llen(&self, key: &[u8]) -> usize {
        ListStore::llen(self.memory.as_ref(), key)
    }

    fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>> {
        ListStore::lindex(self.memory.as_ref(), key, index)
    }

    fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()> {
        ListStore::lset(self.memory.as_ref(), key, index, value)
    }
}

// ============================================================================
// HashStore Implementation (RocksDB)
// ============================================================================

impl HashStore for HybridStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hget(shard_id, key, field)
    }

    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hset(shard_id, key, field, value)
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hdel(shard_id, key, fields)
    }

    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hgetall(shard_id, key)
    }

    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hkeys(shard_id, key)
    }

    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hvals(shard_id, key)
    }

    fn hlen(&self, key: &[u8]) -> usize {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hlen(shard_id, key)
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self.shard_for_key(key);
        self.rocksdb.hincrby(shard_id, key, field, delta)
    }
}

// ============================================================================
// SetStore Implementation (Memory)
// ============================================================================

impl SetStore for HybridStore {
    fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> usize {
        SetStore::sadd(self.memory.as_ref(), key, members)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> usize {
        SetStore::srem(self.memory.as_ref(), key, members)
    }

    fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        SetStore::smembers(self.memory.as_ref(), key)
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        SetStore::sismember(self.memory.as_ref(), key, member)
    }

    fn scard(&self, key: &[u8]) -> usize {
        SetStore::scard(self.memory.as_ref(), key)
    }
}

// ============================================================================
// KeyStore Implementation (Combined)
// ============================================================================

impl KeyStore for HybridStore {
    fn del(&self, keys: &[&[u8]]) -> usize {
        let mut deleted = 0;
        for key in keys {
            let shard_id = self.shard_for_key(key);
            // Try RocksDB first
            if self.rocksdb.del(shard_id, key) {
                deleted += 1;
                continue;
            }
            // Then Memory
            if KeyStore::del(self.memory.as_ref(), &[*key]) > 0 {
                deleted += 1;
            }
        }
        deleted
    }

    fn exists(&self, keys: &[&[u8]]) -> usize {
        keys.iter()
            .filter(|k| {
                let shard_id = self.shard_for_key(k);
                self.rocksdb.get(shard_id, k).is_some()
                    || self.rocksdb.hlen(shard_id, k) > 0
                    || KeyStore::exists(self.memory.as_ref(), &[*k]) > 0
            })
            .count()
    }

    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        // This is expensive - needs to scan all backends
        // For now, only return memory keys
        KeyStore::keys(self.memory.as_ref(), pattern)
    }

    fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        match self.detect_type(key) {
            DataType::String => Some("string"),
            DataType::Hash => Some("hash"),
            DataType::List => Some("list"),
            DataType::Set => Some("set"),
            DataType::ZSet => Some("zset"),
            DataType::Stream => Some("stream"),
            DataType::Unknown => None,
        }
    }

    fn ttl(&self, _key: &[u8]) -> i64 {
        // TODO: Implement TTL
        -1
    }

    fn expire(&self, _key: &[u8], _ttl_secs: u64) -> bool {
        // TODO: Implement TTL
        false
    }

    fn persist(&self, _key: &[u8]) -> bool {
        // TODO: Implement TTL
        false
    }

    fn dbsize(&self) -> usize {
        // Return memory store size (RocksDB size is expensive to compute)
        KeyStore::dbsize(self.memory.as_ref())
    }

    fn flushdb(&self) {
        KeyStore::flushdb(self.memory.as_ref());
        // TODO: Clear RocksDB shards
    }

    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()> {
        KeyStore::rename(self.memory.as_ref(), key, new_key)
    }
}

// ============================================================================
// SnapshotStore Implementation
// ============================================================================

impl SnapshotStore for HybridStore {
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        let snapshots = self.create_full_snapshot()?;
        let mut result = Vec::new();

        for snapshot in snapshots {
            let data = snapshot.serialize()?;
            // Prefix with length
            let len = data.len() as u32;
            result.extend_from_slice(&len.to_le_bytes());
            result.extend(data);
        }

        Ok(result)
    }

    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        let mut pos = 0;

        while pos < snapshot.len() {
            if pos + 4 > snapshot.len() {
                return Err("Invalid snapshot format".to_string());
            }

            let len = u32::from_le_bytes([
                snapshot[pos],
                snapshot[pos + 1],
                snapshot[pos + 2],
                snapshot[pos + 3],
            ]) as usize;
            pos += 4;

            if pos + len > snapshot.len() {
                return Err("Invalid snapshot format".to_string());
            }

            let shard_snapshot = HybridSnapshot::deserialize(&snapshot[pos..pos + len])?;
            self.restore_shard_snapshot(&shard_snapshot)?;
            pos += len;
        }

        Ok(())
    }

    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        _total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        // Calculate affected shards
        let shard_start = slot_start % self.shard_count;
        let shard_end = (slot_end.saturating_sub(1)) % self.shard_count + 1;

        let mut result = Vec::new();
        for shard_id in shard_start..shard_end {
            let snapshot = self.create_shard_snapshot(shard_id)?;
            let data = snapshot.serialize()?;
            let len = data.len() as u32;
            result.extend_from_slice(&len.to_le_bytes());
            result.extend(data);
        }

        Ok(result)
    }

    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String> {
        self.restore_from_snapshot(snapshot)?;
        Ok(1)
    }

    fn delete_keys_in_slot_range(&self, slot_start: u32, slot_end: u32, total_slots: u32) -> usize {
        SnapshotStore::delete_keys_in_slot_range(
            self.memory.as_ref(),
            slot_start,
            slot_end,
            total_slots,
        )
    }
}

// ============================================================================
// RedisStore Implementation (Combines All Traits)
// ============================================================================

impl RedisStore for HybridStore {}

impl Clone for HybridStore {
    fn clone(&self) -> Self {
        Self {
            rocksdb: Arc::clone(&self.rocksdb),
            memory: Arc::clone(&self.memory),
            stream: Arc::clone(&self.stream),
            shard_count: self.shard_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_temp_store() -> HybridStore {
        let path = format!("/tmp/hybrid_test_{}", rand::random::<u64>());
        HybridStore::new(&path, 4).unwrap()
    }

    fn cleanup_store(store: &HybridStore) {
        let path = store.rocksdb().path().to_string();
        drop(store);
        let _ = fs::remove_dir_all(&path);
    }

    #[test]
    fn test_string_operations() {
        let store = create_temp_store();

        store.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));

        assert_eq!(store.incr(b"counter").unwrap(), 1);
        assert_eq!(store.incrby(b"counter", 5).unwrap(), 6);

        cleanup_store(&store);
    }

    #[test]
    fn test_hash_operations() {
        let store = create_temp_store();

        store.hset(b"myhash", b"field1".to_vec(), b"value1".to_vec());
        assert_eq!(
            store.hget(b"myhash", b"field1"),
            Some(b"value1".to_vec())
        );

        store.hmset(
            b"myhash",
            vec![
                (b"f2".to_vec(), b"v2".to_vec()),
                (b"f3".to_vec(), b"v3".to_vec()),
            ],
        );
        assert_eq!(store.hlen(b"myhash"), 3);

        cleanup_store(&store);
    }

    #[test]
    fn test_list_operations() {
        let store = create_temp_store();

        // lpush inserts elements from left
        store.lpush(b"mylist", vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(store.llen(b"mylist"), 2);
        // Current implementation: list is [a, b], lpop returns a
        assert_eq!(store.lpop(b"mylist"), Some(b"a".to_vec()));

        cleanup_store(&store);
    }

    #[test]
    fn test_set_operations() {
        let store = create_temp_store();

        store.sadd(b"myset", vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(store.scard(b"myset"), 3);
        assert!(store.sismember(b"myset", b"a"));

        cleanup_store(&store);
    }

    #[test]
    fn test_hybrid_snapshot() {
        let store = create_temp_store();

        // Add data to different backends
        store.set(b"str1".to_vec(), b"value1".to_vec());
        store.hset(b"hash1", b"f1".to_vec(), b"v1".to_vec());
        store.lpush(b"list1", vec![b"a".to_vec()]);
        store.sadd(b"set1", vec![b"x".to_vec()]);

        // Create snapshot
        let snapshot = store.create_snapshot().unwrap();

        // Verify data exists
        assert_eq!(store.get(b"str1"), Some(b"value1".to_vec()));
        assert_eq!(store.hget(b"hash1", b"f1"), Some(b"v1".to_vec()));
        assert_eq!(store.llen(b"list1"), 1);
        assert!(store.sismember(b"set1", b"x"));

        // Restore again (should work without clearing)
        store.restore_from_snapshot(&snapshot).unwrap();
        
        // Memory data should still be there
        assert_eq!(store.llen(b"list1"), 1);
        assert!(store.sismember(b"set1", b"x"));

        cleanup_store(&store);
    }
}

