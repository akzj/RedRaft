//! String Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{StoreError, StoreResult, StringStore};
use bytes::Bytes;

impl StringStore for HybridStore {
    fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .get(&shard_id, key)
            .map(|v| Bytes::from(v)))
    }

    fn set(&self, key: &[u8], value: Bytes) -> StoreResult<()> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn setnx(&self, key: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        if shard_guard.rocksdb().get(&shard_id, key).is_some() {
            return Ok(false);
        }
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(true)
    }

    fn setex(&self, key: &[u8], value: Bytes, _ttl_secs: u64) -> StoreResult<()> {
        // TODO: Implement expiration
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .rocksdb()
            .incrby(&shard_id, key, delta)
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn append(&self, key: &[u8], value: &[u8]) -> StoreResult<usize> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().append(&shard_id, key, value))
    }

    fn strlen(&self, key: &[u8]) -> StoreResult<usize> {
        match self.get(key)? {
            Some(v) => Ok(v.len()),
            None => Ok(0),
        }
    }
}

