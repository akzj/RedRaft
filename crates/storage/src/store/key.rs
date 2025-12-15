//! Key Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{KeyStore, StoreError, StoreResult};
use bytes::Bytes;

impl KeyStore for HybridStore {
    fn del(&self, keys: &[&[u8]]) -> StoreResult<usize> {
        let mut count = 0;
        for key in keys {
            let shard_id = self
                .shard_for_key(key)?;
            let shard = self.get_shard(key)?;
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
                let Ok(shard_id) = self.shard_for_key(key) else {
                    return false;
                };
                let Ok(shard) = self.get_shard(key) else {
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
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
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

