//! Key Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{KeyStore, StoreError, StoreResult};
use bytes::Bytes;

impl KeyStore for HybridStore {
    fn del(&self, keys: &[&[u8]]) -> StoreResult<usize> {
        let mut count = 0;
        for key in keys {
            let slot_store = self.get_slot_store(key)?;
            let mut store_guard = slot_store.write();

            // Delete from RocksDB
            if store_guard.rocksdb().get(key).is_some() {
                let _ = store_guard.rocksdb_mut().del(key);
                count += 1;
            }

            // Delete from Memory
            if store_guard.memory().contains_key(key) {
                store_guard.memory_mut().del(key);
                count += 1;
            }
        }
        Ok(count)
    }

    fn exists(&self, keys: &[&[u8]]) -> StoreResult<usize> {
        Ok(keys
            .iter()
            .filter(|key| {
                let Ok(slot_store) = self.get_slot_store(key) else {
                    return false;
                };
                let store_guard = slot_store.read();
                store_guard.rocksdb().get(key).is_some()
                    || store_guard.memory().contains_key(key)
            })
            .count())
    }

    fn keys(&self, _pattern: &[u8]) -> StoreResult<Vec<Bytes>> {
        // TODO: Implement pattern matching
        Err(StoreError::NotSupported)
    }

    fn key_type(&self, key: &[u8]) -> StoreResult<Option<&'static str>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();

        // Check RocksDB first
        if store_guard.rocksdb().get(key).is_some() {
            return Ok(Some("string"));
        }

        // Check Memory
        Ok(store_guard.memory().key_type(key))
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
        let slots = self.slots.read();
        let mut count = 0;
        for slot_store in slots.values() {
            let store_guard = slot_store.read();
            count += store_guard.memory().key_count();
            // TODO: Count RocksDB keys
        }
        Ok(count)
    }

    fn flushdb(&self) -> StoreResult<()> {
        let mut slots = self.slots.write();
        for slot_store in slots.values_mut() {
            let mut store_guard = slot_store.write();
            // TODO: Flush RocksDB
            // Clear all memory data - need to clear all keys
            let memory = store_guard.memory_mut();
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

    fn rename(&self, _key: &[u8], _new_key: &[u8]) -> StoreResult<()> {
        // TODO: Implement rename
        Err(StoreError::NotSupported)
    }
}
