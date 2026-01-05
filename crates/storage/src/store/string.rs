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
        // Use set_with_context to atomically write data and slot metadata
        store_guard
            .rocksdb_mut()
            .set_with_context(key, value.to_vec(), ctx)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        // Also update in-memory metadata for fast access
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(())
    }

    fn setnx(&self, key: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<bool> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Use setnx_with_context to atomically write data and slot metadata
        let result = store_guard
            .rocksdb_mut()
            .setnx_with_context(key, value.to_vec(), ctx)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        // Also update in-memory metadata for fast access
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn setex(&self, key: &[u8], value: Bytes, _ttl_secs: u64, ctx: &ApplyContext) -> StoreResult<()> {
        // TODO: Implement expiration
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Use set_with_context to atomically write data and slot metadata
        store_guard
            .rocksdb_mut()
            .set_with_context(key, value.to_vec(), ctx)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        // Also update in-memory metadata for fast access
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(())
    }

    fn incrby(&self, key: &[u8], delta: i64, ctx: &ApplyContext) -> StoreResult<i64> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Use incrby_with_context to atomically write data and slot metadata
        let result = store_guard
            .rocksdb_mut()
            .incrby_with_context(key, delta, ctx)
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        // Also update in-memory metadata for fast access
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn append(&self, key: &[u8], value: &[u8], ctx: &ApplyContext) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Use append_with_context to atomically write data and slot metadata
        let result = store_guard
            .rocksdb_mut()
            .append_with_context(key, value, ctx);
        // Also update in-memory metadata for fast access
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
