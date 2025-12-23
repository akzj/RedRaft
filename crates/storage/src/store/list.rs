//! List Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ListStore, StoreResult};
use bytes::Bytes;

impl ListStore for HybridStore {
    fn lpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.memory_mut().lpush(key, values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.memory_mut().rpush(key, values)
    }

    fn lpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.memory_mut().lpop(key)
    }

    fn rpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.memory_mut().rpop(key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<Vec<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        store_guard.memory().lrange(key, start, stop)
    }

    fn llen(&self, key: &[u8]) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        store_guard.memory().llen(key)
    }

    fn lindex(&self, key: &[u8], index: i64) -> StoreResult<Option<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        store_guard.memory().lindex(key, index)
    }

    fn lset(&self, key: &[u8], index: i64, value: Bytes) -> StoreResult<()> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.memory_mut().lset(key, index, value)
    }

    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<()> {
        // TODO: Implement ltrim
        let _ = (key, start, stop);
        Err(crate::traits::StoreError::NotSupported)
    }

    fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> StoreResult<usize> {
        // TODO: Implement lrem
        let _ = (key, count, value);
        Ok(0)
    }
}
