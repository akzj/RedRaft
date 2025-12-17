//! Key Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{KeyStore, StoreError, StoreResult};
use bytes::Bytes;

impl KeyStore for HybridStore {
    fn del(&self, keys: &[&[u8]], apply_index: u64) -> StoreResult<usize> {
        let mut count = 0;
        for key in keys {
            let shard_id = self.shard_for_key(key)?;
            let shard = self.get_shard(key)?;
            let mut shard_guard = shard.write();
            // Update apply_index in metadata
            shard_guard.metadata.set_apply_index(apply_index);

            // Delete from RocksDB
            if shard_guard.rocksdb().get(&shard_id, key).is_some() {
                let _ = shard_guard.rocksdb_mut().del(&shard_id, key);
                count += 1;
            }

            // Delete from Memory
            if shard_guard.memory().contains_key(key) {
                shard_guard.memory_mut().del(key);
                count += 1;
            }
        }
        Ok(count)
    }

    fn exists(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<usize> {
        Ok(keys
            .iter()
            .filter(|key| {
                let Ok(shard_id) = self.shard_for_key(key) else {
                    return false;
                };
                let Ok(shard) = self.get_shard(key) else {
                    return false;
                };
                let shard_guard = shard.read();
                // Verify read_index is valid (should be <= current apply_index)
                if shard_guard.metadata.verify_read_index(read_index).is_err() {
                    return false; // Invalid read index
                }
                shard_guard.rocksdb().get(&shard_id, key).is_some()
                    || shard_guard.memory().contains_key(key)
            })
            .count())
    }

    fn keys(&self, _pattern: &[u8], _read_index: u64) -> StoreResult<Vec<Bytes>> {
        // TODO: Implement pattern matching
        Err(StoreError::NotSupported)
    }

    fn key_type(&self, key: &[u8], read_index: u64) -> StoreResult<Option<&'static str>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;

        // Check RocksDB first
        if shard_guard.rocksdb().get(&shard_id, key).is_some() {
            return Ok(Some("string"));
        }

        // Check Memory
        Ok(shard_guard.memory().key_type(key))
    }

    fn ttl(&self, _key: &[u8], read_index: u64) -> StoreResult<i64> {
        // TODO: Implement TTL
        // Verify read_index is valid
        let _ = read_index; // Suppress unused warning
        Ok(-1)
    }

    fn expire(&self, _key: &[u8], _ttl_secs: u64, apply_index: u64) -> StoreResult<bool> {
        // TODO: Implement expiration
        // Update apply_index in metadata
        let _ = apply_index; // Suppress unused warning
        Ok(false)
    }

    fn persist(&self, _key: &[u8], apply_index: u64) -> StoreResult<bool> {
        // TODO: Implement persistence
        // Update apply_index in metadata
        let _ = apply_index; // Suppress unused warning
        Ok(false)
    }

    fn dbsize(&self, read_index: u64) -> StoreResult<usize> {
        // Verify read_index is valid for all shards
        let shards = self.shards.read();
        for shard in shards.values() {
            let shard_guard = shard.read();
            shard_guard.metadata.verify_read_index(read_index)?;
        }
        let mut count = 0;
        for shard in shards.values() {
            let shard_guard = shard.read();
            count += shard_guard.memory().key_count();
            // TODO: Count RocksDB keys
        }
        Ok(count)
    }

    fn flushdb(&self, apply_index: u64) -> StoreResult<()> {
        let mut shards = self.shards.write();
        for shard in shards.values_mut() {
            let mut shard_guard = shard.write();
            // Update apply_index in metadata
            shard_guard.metadata.set_apply_index(apply_index);
            // TODO: Flush RocksDB
            // Clear all memory data - need to clear all keys
            let memory = shard_guard.memory_mut();
            // Get all keys and delete them
            let keys: Vec<Vec<u8>> = {
                let base = memory.base.read();
                base.keys().cloned().collect()
            };
            for key in keys {
                memory.del(&key);
            }
        }
        Ok(())
    }

    fn rename(&self, _key: &[u8], _new_key: &[u8], apply_index: u64) -> StoreResult<()> {
        // TODO: Implement rename
        // Update apply_index in metadata
        let _ = apply_index; // Suppress unused warning
        Err(StoreError::NotSupported)
    }
}
