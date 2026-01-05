//! Hash Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ApplyContext, HashStore, StoreError, StoreResult};
use bytes::Bytes;

impl HashStore for HybridStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> StoreResult<Option<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        Ok(store_guard
            .rocksdb()
            .hget(key, field)
            .map(|v| Bytes::from(v)))
    }

    fn hset(&self, key: &[u8], field: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<bool> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let result = store_guard
            .rocksdb_mut()
            .hset(key, field.as_ref(), value);
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> StoreResult<Vec<Option<Bytes>>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        Ok(fields
            .iter()
            .map(|f| {
                store_guard
                    .rocksdb()
                    .hget(key, f)
                    .map(|v| Bytes::from(v))
            })
            .collect())
    }

    fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>, ctx: &ApplyContext) -> StoreResult<()> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        store_guard.rocksdb_mut().hmset(key, fvs);
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(())
    }

    fn hgetall(&self, key: &[u8]) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        let v = store_guard.rocksdb().hgetall(key);
        Ok(v.into_iter()
            .map(|(f, v)| (Bytes::from(f), Bytes::from(v)))
            .collect())
    }

    fn hkeys(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        let v = store_guard.rocksdb().hkeys(key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hvals(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        let v = store_guard.rocksdb().hvals(key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hsetnx(&self, key: &[u8], field: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<bool> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        // Check if field exists
        if store_guard.rocksdb().hget(key, field).is_some() {
            return Ok(false);
        }
        let result = store_guard.rocksdb_mut().hset(key, field, value);
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]], ctx: &ApplyContext) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let result = store_guard.rocksdb_mut().hdel(key, fields);
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }

    fn hlen(&self, key: &[u8]) -> StoreResult<usize> {
        let slot_store = self.get_slot_store(key)?;
        let store_guard = slot_store.read();
        Ok(store_guard.rocksdb().hlen(key))
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64, ctx: &ApplyContext) -> StoreResult<i64> {
        let slot_store = self.get_slot_store(key)?;
        let mut store_guard = slot_store.write();
        let result = store_guard
            .rocksdb_mut()
            .hincrby(key, field, delta)?;
        store_guard.metadata_mut().update_from_context(ctx);
        Ok(result)
    }
}
