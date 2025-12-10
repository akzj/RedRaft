//! Per-Shard Storage Implementation
//!
//! Each shard contains data types (Hash, List, Set, ZSet, Stream),
//! enabling atomic snapshot generation at the shard level.
//!
//! Note: String data is stored separately in StringStore (not in ShardStore).

use crate::memory::ShardId;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Independent String storage (per shard)
///
/// String data is stored separately from other data types.
/// This allows different storage backends (e.g., RocksDB) to be used for strings.
pub struct StringStore {
    /// Shard identifier
    pub shard_id: ShardId,
    /// String data (key -> value)
    pub data: HashMap<Vec<u8>, Vec<u8>>,
}

impl StringStore {
    /// Create a new empty string store
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            data: HashMap::new(),
        }
    }

    /// Get value for key
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    /// Set value for key
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    /// Delete key
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.data.remove(key).is_some()
    }

    /// Check if key exists
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.data.contains_key(key)
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.data.keys().cloned().collect()
    }

    /// Get key count
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

/// Per-shard storage containing data types (Hash, List, Set, ZSet)
pub struct ShardStore {
    /// Shard identifier
    pub shard_id: ShardId,

    /// Hash data (key -> (field -> value))
    pub hashes: HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<u8>>>,

    /// List data (key -> deque)
    pub lists: HashMap<Vec<u8>, VecDeque<Vec<u8>>>,

    /// Set data (key -> set)
    pub sets: HashMap<Vec<u8>, HashSet<Vec<u8>>>,

    /// ZSet data (key -> (member -> score, score -> members))
    pub zsets: HashMap<Vec<u8>, ZSetData>,

    /// Metadata
    pub metadata: ShardMetadata,
}

/// ZSet internal data structure
#[derive(Debug, Clone, Default)]
pub struct ZSetData {
    /// member -> score
    pub scores: HashMap<Vec<u8>, f64>,
    /// score -> members (for range queries)
    pub by_score: BTreeMap<OrderedFloat, HashSet<Vec<u8>>>,
}

/// Ordered float for BTreeMap key
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// Shard metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    /// Last applied Raft log index
    pub apply_index: u64,
    /// Last snapshot index
    pub last_snapshot_index: u64,
    /// Shard creation time
    pub created_at: u64,
    /// Last update time
    pub last_updated: u64,
}

impl Default for ShardMetadata {
    fn default() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            apply_index: 0,
            last_snapshot_index: 0,
            created_at: now,
            last_updated: now,
        }
    }
}

impl ShardStore {
    /// Create a new empty shard store
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            hashes: HashMap::new(),
            lists: HashMap::new(),
            sets: HashMap::new(),
            zsets: HashMap::new(),
            metadata: ShardMetadata::default(),
        }
    }

    /// Get total key count across all data types
    pub fn key_count(&self) -> usize {
        self.hashes.len() + self.lists.len() + self.sets.len() + self.zsets.len()
    }

    /// Check if key exists in any data type
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key)
            || self.lists.contains_key(key)
            || self.sets.contains_key(key)
            || self.zsets.contains_key(key)
    }

    /// Get key type
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        if self.hashes.contains_key(key) {
            Some("hash")
        } else if self.lists.contains_key(key) {
            Some("list")
        } else if self.sets.contains_key(key) {
            Some("set")
        } else if self.zsets.contains_key(key) {
            Some("zset")
        } else {
            None
        }
    }

    /// Delete key from any data type
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.hashes.remove(key).is_some()
            || self.lists.remove(key).is_some()
            || self.sets.remove(key).is_some()
            || self.zsets.remove(key).is_some()
    }

    /// Get all keys
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        keys.extend(self.hashes.keys().cloned());
        keys.extend(self.lists.keys().cloned());
        keys.extend(self.sets.keys().cloned());
        keys.extend(self.zsets.keys().cloned());
        keys
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.hashes.clear();
        self.lists.clear();
        self.sets.clear();
        self.zsets.clear();
    }

    /// Update metadata timestamp
    pub fn touch(&mut self) {
        self.metadata.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Set apply index
    pub fn set_apply_index(&mut self, index: u64) {
        self.metadata.apply_index = index;
        self.touch();
    }
}

/// Shard snapshot data (serializable)
///
/// Note: String data is not included - it's stored separately in StringStore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub shard_id: ShardId,
    pub hashes: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>,
    pub lists: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    pub sets: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    pub zsets: Vec<(Vec<u8>, Vec<(Vec<u8>, f64)>)>,
    pub metadata: ShardMetadata,
}

/// String snapshot data (serializable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringSnapshot {
    pub shard_id: ShardId,
    pub strings: Vec<(Vec<u8>, Vec<u8>)>,
}

impl StringSnapshot {
    /// Serialize to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| format!("Serialization error: {}", e))
    }

    /// Deserialize from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::serde::decode_from_slice(data, bincode::config::standard())
            .map(|(s, _)| s)
            .map_err(|e| format!("Deserialization error: {}", e))
    }
}

impl StringStore {
    /// Create snapshot of string store (atomic - caller holds lock)
    pub fn create_snapshot(&self) -> StringSnapshot {
        StringSnapshot {
            shard_id: self.shard_id,
            strings: self
                .data
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    /// Restore from snapshot
    pub fn restore_from_snapshot(&mut self, snapshot: StringSnapshot) {
        self.clear();
        self.shard_id = snapshot.shard_id;

        for (k, v) in snapshot.strings {
            self.data.insert(k, v);
        }
    }
}

impl ShardStore {
    /// Create snapshot of this shard (atomic - caller holds lock)
    pub fn create_snapshot(&self) -> ShardSnapshot {
        ShardSnapshot {
            shard_id: self.shard_id,
            hashes: self
                .hashes
                .iter()
                .map(|(k, h)| {
                    (
                        k.clone(),
                        h.iter().map(|(f, v)| (f.clone(), v.clone())).collect(),
                    )
                })
                .collect(),
            lists: self
                .lists
                .iter()
                .map(|(k, l)| (k.clone(), l.iter().cloned().collect()))
                .collect(),
            sets: self
                .sets
                .iter()
                .map(|(k, s)| (k.clone(), s.iter().cloned().collect()))
                .collect(),
            zsets: self
                .zsets
                .iter()
                .map(|(k, z)| {
                    (
                        k.clone(),
                        z.scores.iter().map(|(m, s)| (m.clone(), *s)).collect(),
                    )
                })
                .collect(),
            metadata: self.metadata.clone(),
        }
    }

    /// Restore from snapshot
    pub fn restore_from_snapshot(&mut self, snapshot: ShardSnapshot) {
        self.clear();
        self.shard_id = snapshot.shard_id;

        // Restore hashes
        for (k, fields) in snapshot.hashes {
            let mut hash = HashMap::new();
            for (f, v) in fields {
                hash.insert(f, v);
            }
            self.hashes.insert(k, hash);
        }

        // Restore lists
        for (k, items) in snapshot.lists {
            self.lists.insert(k, items.into_iter().collect());
        }

        // Restore sets
        for (k, members) in snapshot.sets {
            self.sets.insert(k, members.into_iter().collect());
        }

        // Restore zsets
        for (k, members) in snapshot.zsets {
            let mut zset = ZSetData::default();
            for (member, score) in members {
                zset.scores.insert(member.clone(), score);
                zset.by_score
                    .entry(OrderedFloat(score))
                    .or_default()
                    .insert(member);
            }
            self.zsets.insert(k, zset);
        }

        self.metadata = snapshot.metadata;
    }
}

impl ShardSnapshot {
    /// Serialize to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| format!("Serialization error: {}", e))
    }

    /// Deserialize from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        bincode::serde::decode_from_slice(data, bincode::config::standard())
            .map(|(s, _)| s)
            .map_err(|e| format!("Deserialization error: {}", e))
    }
}

// ============================================================================
// Sharded Hybrid Store - Top Level
// ============================================================================

/// Sharded Hybrid Store with per-shard locking
///
/// Architecture:
/// ```text
/// ShardedHybridStore
/// ├── string_stores: RwLock<HashMap<ShardId, Arc<RwLock<StringStore>>>>
/// │   └── Shard 0: RwLock<StringStore> { data: HashMap }
/// └── shards: RwLock<HashMap<ShardId, Arc<RwLock<ShardStore>>>>
///     └── Shard 0: RwLock<ShardStore>
///         ├── hashes: HashMap
///         ├── lists: HashMap
///         ├── sets: HashMap
///         ├── zsets: HashMap
///         └── metadata
/// ```
///
/// Benefits:
/// - Atomic snapshot per shard (lock only one shard)
/// - Concurrent access to different shards
/// - Clean isolation between shards
/// - String data stored separately (can use different backend)
pub struct ShardedHybridStore {
    /// String stores (per shard)
    string_stores: RwLock<HashMap<ShardId, Arc<RwLock<StringStore>>>>,
    /// Shards map with per-shard locking (Hash, List, Set, ZSet)
    shards: RwLock<HashMap<ShardId, Arc<RwLock<ShardStore>>>>,
    /// Total number of shards
    shard_count: u32,
}

impl ShardedHybridStore {
    /// Create a new sharded hybrid store
    pub fn new(shard_count: u32) -> Self {
        let shard_count = shard_count.max(1);
        let mut string_stores = HashMap::new();
        let mut shards = HashMap::new();

        for shard_id in 0..shard_count {
            string_stores.insert(shard_id, Arc::new(RwLock::new(StringStore::new(shard_id))));
            shards.insert(shard_id, Arc::new(RwLock::new(ShardStore::new(shard_id))));
        }

        Self {
            string_stores: RwLock::new(string_stores),
            shards: RwLock::new(shards),
            shard_count,
        }
    }

    /// Get shard count
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Calculate shard ID for key using CRC16
    pub fn shard_for_key(&self, key: &[u8]) -> ShardId {
        crate::memory::slot_for_key(key) % self.shard_count
    }

    /// Get string store (read access)
    pub fn get_string_store(&self, shard_id: ShardId) -> Option<Arc<RwLock<StringStore>>> {
        self.string_stores.read().get(&shard_id).cloned()
    }

    /// Get shard (read access)
    pub fn get_shard(&self, shard_id: ShardId) -> Option<Arc<RwLock<ShardStore>>> {
        self.shards.read().get(&shard_id).cloned()
    }

    /// Execute operation on string store with read lock
    pub fn with_string_store_read<F, R>(&self, shard_id: ShardId, f: F) -> Option<R>
    where
        F: FnOnce(&StringStore) -> R,
    {
        self.get_string_store(shard_id).map(|store| f(&store.read()))
    }

    /// Execute operation on string store with write lock
    pub fn with_string_store_write<F, R>(&self, shard_id: ShardId, f: F) -> Option<R>
    where
        F: FnOnce(&mut StringStore) -> R,
    {
        self.get_string_store(shard_id).map(|store| f(&mut store.write()))
    }

    /// Execute operation on shard with read lock
    pub fn with_shard_read<F, R>(&self, shard_id: ShardId, f: F) -> Option<R>
    where
        F: FnOnce(&ShardStore) -> R,
    {
        self.get_shard(shard_id).map(|shard| f(&shard.read()))
    }

    /// Execute operation on shard with write lock
    pub fn with_shard_write<F, R>(&self, shard_id: ShardId, f: F) -> Option<R>
    where
        F: FnOnce(&mut ShardStore) -> R,
    {
        self.get_shard(shard_id).map(|shard| f(&mut shard.write()))
    }

    // ==================== Atomic Snapshot Operations ====================

    /// Create atomic snapshot for string store in a single shard
    pub fn create_string_snapshot(&self, shard_id: ShardId) -> Result<StringSnapshot, String> {
        let store = self
            .get_string_store(shard_id)
            .ok_or_else(|| format!("String store for shard {} not found", shard_id))?;

        let guard = store.read();
        Ok(guard.create_snapshot())
    }

    /// Restore string store from snapshot (atomic)
    pub fn restore_string_snapshot(&self, snapshot: StringSnapshot) -> Result<(), String> {
        let shard_id = snapshot.shard_id;
        let store = self
            .get_string_store(shard_id)
            .ok_or_else(|| format!("String store for shard {} not found", shard_id))?;

        let mut guard = store.write();
        guard.restore_from_snapshot(snapshot);
        Ok(())
    }

    /// Create atomic snapshot for a single shard (Hash, List, Set, ZSet)
    ///
    /// This locks only the target shard, allowing other shards to continue operating.
    pub fn create_shard_snapshot(&self, shard_id: ShardId) -> Result<ShardSnapshot, String> {
        let shard = self
            .get_shard(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;

        // Lock shard for atomic snapshot
        let guard = shard.read();
        Ok(guard.create_snapshot())
    }

    /// Restore shard from snapshot (atomic)
    pub fn restore_shard_snapshot(&self, snapshot: ShardSnapshot) -> Result<(), String> {
        let shard_id = snapshot.shard_id;
        let shard = self
            .get_shard(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;

        // Lock shard for atomic restore
        let mut guard = shard.write();
        guard.restore_from_snapshot(snapshot);
        Ok(())
    }

    /// Create full snapshot (all shards, excluding strings)
    pub fn create_full_snapshot(&self) -> Result<Vec<ShardSnapshot>, String> {
        let shards = self.shards.read();
        let mut snapshots = Vec::with_capacity(shards.len());

        for (_, shard) in shards.iter() {
            let guard = shard.read();
            snapshots.push(guard.create_snapshot());
        }

        Ok(snapshots)
    }

    /// Create full string snapshot (all shards)
    pub fn create_full_string_snapshot(&self) -> Result<Vec<StringSnapshot>, String> {
        let stores = self.string_stores.read();
        let mut snapshots = Vec::with_capacity(stores.len());

        for (_, store) in stores.iter() {
            let guard = store.read();
            snapshots.push(guard.create_snapshot());
        }

        Ok(snapshots)
    }

    /// Restore all shards from snapshots
    pub fn restore_full_snapshot(&self, snapshots: Vec<ShardSnapshot>) -> Result<(), String> {
        for snapshot in snapshots {
            self.restore_shard_snapshot(snapshot)?;
        }
        Ok(())
    }

    /// Restore all string stores from snapshots
    pub fn restore_full_string_snapshot(
        &self,
        snapshots: Vec<StringSnapshot>,
    ) -> Result<(), String> {
        for snapshot in snapshots {
            self.restore_string_snapshot(snapshot)?;
        }
        Ok(())
    }

    // ==================== Statistics ====================

    /// Get total key count across all shards (excluding strings)
    pub fn dbsize(&self) -> usize {
        self.shards
            .read()
            .values()
            .map(|s| s.read().key_count())
            .sum()
    }

    /// Get total string count across all shards
    pub fn string_dbsize(&self) -> usize {
        self.string_stores
            .read()
            .values()
            .map(|s| s.read().len())
            .sum()
    }

    /// Get shard statistics
    pub fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let string_count = self
            .with_string_store_read(shard_id, |store| store.len())
            .unwrap_or(0);

        self.with_shard_read(shard_id, |shard| ShardStats {
            shard_id,
            string_count,
            hash_count: shard.hashes.len(),
            list_count: shard.lists.len(),
            set_count: shard.sets.len(),
            zset_count: shard.zsets.len(),
            apply_index: shard.metadata.apply_index,
        })
    }
}

/// Shard statistics
#[derive(Debug, Clone)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub string_count: usize,
    pub hash_count: usize,
    pub list_count: usize,
    pub set_count: usize,
    pub zset_count: usize,
    pub apply_index: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_store_operations() {
        let mut store = StringStore::new(0);

        store.set(b"key1".to_vec(), b"value1".to_vec());
        assert!(store.contains_key(b"key1"));
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store.len(), 1);

        assert!(store.del(b"key1"));
        assert!(!store.contains_key(b"key1"));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_string_snapshot() {
        let mut store = StringStore::new(0);
        store.set(b"key1".to_vec(), b"value1".to_vec());

        let snapshot = store.create_snapshot();
        assert_eq!(snapshot.shard_id, 0);
        assert_eq!(snapshot.strings.len(), 1);

        // Serialize/Deserialize
        let data = snapshot.serialize().unwrap();
        let restored = StringSnapshot::deserialize(&data).unwrap();
        assert_eq!(restored.shard_id, 0);
        assert_eq!(restored.strings.len(), 1);
    }

    #[test]
    fn test_shard_store_operations() {
        let mut store = ShardStore::new(0);

        // Hash
        let mut hash = HashMap::new();
        hash.insert(b"field1".to_vec(), b"value1".to_vec());
        store.hashes.insert(b"hash1".to_vec(), hash);
        assert_eq!(store.key_type(b"hash1"), Some("hash"));

        // List
        store
            .lists
            .insert(b"list1".to_vec(), VecDeque::from(vec![b"item1".to_vec()]));
        assert_eq!(store.key_type(b"list1"), Some("list"));

        assert_eq!(store.key_count(), 2);
    }

    #[test]
    fn test_shard_snapshot() {
        let mut store = ShardStore::new(0);
        let mut hash = HashMap::new();
        hash.insert(b"field1".to_vec(), b"value1".to_vec());
        store.hashes.insert(b"hash1".to_vec(), hash);
        store.metadata.apply_index = 100;

        // Create snapshot
        let snapshot = store.create_snapshot();
        assert_eq!(snapshot.shard_id, 0);
        assert_eq!(snapshot.hashes.len(), 1);
        assert_eq!(snapshot.metadata.apply_index, 100);

        // Serialize/Deserialize
        let data = snapshot.serialize().unwrap();
        let restored = ShardSnapshot::deserialize(&data).unwrap();
        assert_eq!(restored.shard_id, 0);
        assert_eq!(restored.hashes.len(), 1);
        assert_eq!(restored.metadata.apply_index, 100);
    }

    #[test]
    fn test_sharded_hybrid_store() {
        let store = ShardedHybridStore::new(4);

        // Write to string store
        store.with_string_store_write(0, |string_store| {
            string_store.set(b"key1".to_vec(), b"value1".to_vec());
        });

        // Read from string store
        let value = store.with_string_store_read(0, |string_store| {
            string_store.get(b"key1")
        });
        assert_eq!(value, Some(Some(b"value1".to_vec())));

        // Write to shard
        store.with_shard_write(0, |shard| {
            let mut hash = HashMap::new();
            hash.insert(b"field1".to_vec(), b"value1".to_vec());
            shard.hashes.insert(b"hash1".to_vec(), hash);
        });

        // Snapshot
        let snapshot = store.create_shard_snapshot(0).unwrap();
        assert_eq!(snapshot.hashes.len(), 1);

        let string_snapshot = store.create_string_snapshot(0).unwrap();
        assert_eq!(string_snapshot.strings.len(), 1);
    }
}
