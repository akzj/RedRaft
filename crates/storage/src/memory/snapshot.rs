//! Snapshot operations for MemoryStore
//!
//! Provides serialization and deserialization of memory store data
//! for Raft snapshots and shard splitting operations.

use super::shard::slot_for_key;
use super::store::MemoryStore;
use super::RedisValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Snapshot data format for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Version for backward compatibility
    pub version: u32,
    /// All key-value pairs
    pub entries: HashMap<Vec<u8>, RedisValue>,
}

impl SnapshotData {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            entries: HashMap::new(),
        }
    }

    /// Rebuild ZSet sorted indexes after deserialization
    pub fn rebuild_zset_indexes(&mut self) {
        for value in self.entries.values_mut() {
            if let RedisValue::ZSet(zset) = value {
                zset.rebuild_sorted_index();
            }
        }
    }
}

impl Default for SnapshotData {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Create a full snapshot of all data
    pub fn create_snapshot_impl(&self) -> Result<Vec<u8>, String> {
        let mut snapshot = SnapshotData::new();

        // Collect all data from all shards
        let shards = self.shards.read();
        for shard_data in shards.values() {
            let shard = shard_data.read();
            for (key, value) in shard.data().iter() {
                snapshot.entries.insert(key.clone(), value.clone());
            }
        }

        // Serialize using bincode
        bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))
    }

    /// Restore from a full snapshot
    pub fn restore_from_snapshot_impl(&self, data: &[u8]) -> Result<(), String> {
        // Deserialize snapshot
        let (mut snapshot, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Check version
        if snapshot.version > SnapshotData::CURRENT_VERSION {
            return Err(format!(
                "Unsupported snapshot version: {} (max: {})",
                snapshot.version,
                SnapshotData::CURRENT_VERSION
            ));
        }

        // Rebuild ZSet indexes
        snapshot.rebuild_zset_indexes();

        // Clear existing data
        {
            let mut shards = self.shards.write();
            shards.clear();
        }

        // Restore data to appropriate shards
        for (key, value) in snapshot.entries {
            let shard_id = self.get_shard_id(&key);
            let shard_data = self.get_or_create_shard(shard_id);
            let mut shard = shard_data.write();
            shard.data_mut().insert(key, value);
        }

        Ok(())
    }

    /// Create a split snapshot containing only keys in specified slot range
    ///
    /// # Arguments
    /// - `slot_start`: Start slot (inclusive)
    /// - `slot_end`: End slot (exclusive)
    /// - `total_slots`: Total number of slots (used for slot calculation)
    pub fn create_split_snapshot_impl(
        &self,
        slot_start: u32,
        slot_end: u32,
        _total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        let mut snapshot = SnapshotData::new();

        // Collect data from all shards, filtering by slot range
        let shards = self.shards.read();
        for shard_data in shards.values() {
            let shard = shard_data.read();
            for (key, value) in shard.data().iter() {
                let slot = slot_for_key(key);
                if slot >= slot_start && slot < slot_end {
                    snapshot.entries.insert(key.clone(), value.clone());
                }
            }
        }

        // Serialize using bincode
        bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize split snapshot: {}", e))
    }

    /// Merge snapshot data into existing store (does not clear existing data)
    ///
    /// Returns the number of keys merged
    pub fn merge_from_snapshot_impl(&self, data: &[u8]) -> Result<usize, String> {
        // Deserialize snapshot
        let (mut snapshot, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // Check version
        if snapshot.version > SnapshotData::CURRENT_VERSION {
            return Err(format!(
                "Unsupported snapshot version: {} (max: {})",
                snapshot.version,
                SnapshotData::CURRENT_VERSION
            ));
        }

        // Rebuild ZSet indexes
        snapshot.rebuild_zset_indexes();

        let mut merged_count = 0;

        // Merge data into appropriate shards
        for (key, value) in snapshot.entries {
            let shard_id = self.get_shard_id(&key);
            let shard_data = self.get_or_create_shard(shard_id);
            let mut shard = shard_data.write();
            shard.data_mut().insert(key, value);
            merged_count += 1;
        }

        Ok(merged_count)
    }

    /// Delete all keys in specified slot range
    ///
    /// Used by source shard to clean up transferred data after splitting
    ///
    /// Returns the number of keys deleted
    pub fn delete_keys_in_slot_range_impl(
        &self,
        slot_start: u32,
        slot_end: u32,
        _total_slots: u32,
    ) -> usize {
        let mut deleted_count = 0;

        let shards = self.shards.read();
        for shard_data in shards.values() {
            let mut shard = shard_data.write();

            // Collect keys to delete (to avoid borrowing issues)
            let keys_to_delete: Vec<Vec<u8>> = shard
                .data()
                .keys()
                .filter(|key| {
                    let slot = slot_for_key(key);
                    slot >= slot_start && slot < slot_end
                })
                .cloned()
                .collect();

            // Delete the keys
            for key in keys_to_delete {
                if shard.data_mut().remove(&key).is_some() {
                    deleted_count += 1;
                }
            }
        }

        deleted_count
    }

    /// Create a snapshot of a specific shard (for shard-aware snapshots)
    pub fn create_shard_snapshot(&self, shard_id: u32) -> Result<Vec<u8>, String> {
        let mut snapshot = SnapshotData::new();

        if let Some(shard_data) = self.get_shard_data(shard_id) {
            let shard = shard_data.read();
            for (key, value) in shard.data().iter() {
                snapshot.entries.insert(key.clone(), value.clone());
            }
        }

        bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize shard snapshot: {}", e))
    }

    /// Restore a specific shard from snapshot
    pub fn restore_shard_from_snapshot(&self, shard_id: u32, data: &[u8]) -> Result<(), String> {
        let (mut snapshot, _): (SnapshotData, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Failed to deserialize shard snapshot: {}", e))?;

        snapshot.rebuild_zset_indexes();

        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();

        // Clear existing shard data
        shard.data_mut().clear();

        // Restore data
        for (key, value) in snapshot.entries {
            shard.data_mut().insert(key, value);
        }

        Ok(())
    }

    /// Get total memory usage estimate
    pub fn memory_usage(&self) -> usize {
        let shards = self.shards.read();
        let mut total = 0;

        for shard_data in shards.values() {
            let shard = shard_data.read();
            for (key, value) in shard.data().iter() {
                total += key.len() + value.estimated_size();
            }
        }

        total
    }

    /// Get key count per shard (for monitoring)
    pub fn shard_stats(&self) -> HashMap<u32, usize> {
        let shards = self.shards.read();
        shards
            .iter()
            .map(|(id, shard_data)| {
                let shard = shard_data.read();
                (*id, shard.data().len())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_roundtrip() {
        let store = MemoryStore::new(4);

        // Add some data
        store.set_string(b"key1", b"value1".to_vec());
        store.set_string(b"key2", b"value2".to_vec());
        store.lpush(b"list1".to_vec(), vec![b"a".to_vec(), b"b".to_vec()]);
        store.sadd(b"set1".to_vec(), vec![b"m1".to_vec(), b"m2".to_vec()]);
        store.zadd(
            b"zset1".to_vec(),
            vec![(1.0, b"z1".to_vec()), (2.0, b"z2".to_vec())],
        );

        // Create snapshot
        let snapshot = store.create_snapshot_impl().unwrap();

        // Create new store and restore
        let store2 = MemoryStore::new(4);
        store2.restore_from_snapshot_impl(&snapshot).unwrap();

        // Verify data
        assert_eq!(store2.get_string(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store2.get_string(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(store2.llen(b"list1"), 2);
        assert_eq!(store2.scard(b"set1"), 2);
        assert_eq!(store2.zcard(b"zset1"), 2);
    }

    #[test]
    fn test_split_snapshot() {
        let store = MemoryStore::new(4);

        // Add many keys that will be distributed across slots
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            store.set_string(key.as_bytes(), value.into_bytes());
        }

        // Create split snapshot for first half of slots
        let snapshot = store
            .create_split_snapshot_impl(0, 8192, 16384)
            .unwrap();

        // Merge into new store
        let store2 = MemoryStore::new(4);
        let merged = store2.merge_from_snapshot_impl(&snapshot).unwrap();

        // Should have merged some keys
        assert!(merged > 0);
        assert!(merged < 100);
    }
}

impl MemoryStore {
    /// Helper: Set string value (for tests)
    #[cfg(test)]
    fn set_string(&self, key: &[u8], value: Vec<u8>) {
        let shard_id = self.get_shard_id(key);
        let shard_data = self.get_or_create_shard(shard_id);
        let mut shard = shard_data.write();
        shard
            .data_mut()
            .insert(key.to_vec(), RedisValue::String(value));
    }

    /// Helper: Get string value (for tests)
    #[cfg(test)]
    fn get_string(&self, key: &[u8]) -> Option<Vec<u8>> {
        let shard_id = self.get_shard_id(key);
        if let Some(shard_data) = self.get_shard(shard_id) {
            let shard = shard_data.read();
            if let Some(RedisValue::String(s)) = shard.data().get(key) {
                return Some(s.clone());
            }
        }
        None
    }
}

