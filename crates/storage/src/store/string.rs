//! String Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ApplyContext, StoreError, StoreResult, StringStore};
use bytes::Bytes;

impl StringStore for HybridStore {
    fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        Ok(store_guard
            .rocksdb()
            .get(key)
            .map(|v| Bytes::from(v)))
    }

    fn set(&self, key: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<()> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard
            .rocksdb_mut()
            .set(key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(())
    }

    fn setnx(&self, key: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<bool> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Check if key exists
        if store_guard.rocksdb().get(key).is_some() {
            return Ok(false);
        }
        store_guard
            .rocksdb_mut()
            .set(key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(true)
    }

    fn setex(&self, key: &[u8], value: Bytes, _ttl_secs: u64, ctx: &ApplyContext) -> StoreResult<()> {
        // TODO: Implement expiration
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard
            .rocksdb_mut()
            .set(key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(())
    }

    fn incrby(&self, key: &[u8], delta: i64, ctx: &ApplyContext) -> StoreResult<i64> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let result = store_guard
            .rocksdb_mut()
            .incrby(key, delta)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn append(&self, key: &[u8], value: &[u8], ctx: &ApplyContext) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let result = store_guard.rocksdb_mut().append(key, value);
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn strlen(&self, key: &[u8]) -> StoreResult<usize> {
        match self.get(key)? {
            Some(v) => Ok(v.len()),
            None => Ok(0),
        }
    }
}
