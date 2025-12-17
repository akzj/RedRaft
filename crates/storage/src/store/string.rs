//! String Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{StoreError, StoreResult, StringStore};
use bytes::Bytes;

impl StringStore for HybridStore {
    fn get(&self, key: &[u8], read_index: u64) -> StoreResult<Option<Bytes>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        Ok(shard_guard
            .rocksdb()
            .get(&shard_id, key)
            .map(|v| Bytes::from(v)))
    }

    fn set(&self, key: &[u8], value: Bytes, apply_index: u64) -> StoreResult<()> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard
            .rocksdb_mut()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn setnx(&self, key: &[u8], value: Bytes, apply_index: u64) -> StoreResult<bool> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Check if key exists (no read_index needed for existence check in write operation)
        if shard_guard.rocksdb().get(&shard_id, key).is_some() {
            return Ok(false);
        }
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard
            .rocksdb_mut()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(true)
    }

    fn setex(&self, key: &[u8], value: Bytes, _ttl_secs: u64, apply_index: u64) -> StoreResult<()> {
        // TODO: Implement expiration
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard
            .rocksdb_mut()
            .set(&shard_id, key, value.to_vec())
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn incrby(&self, key: &[u8], delta: i64, apply_index: u64) -> StoreResult<i64> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard
            .rocksdb_mut()
            .incrby(&shard_id, key, delta)
            .map_err(|e| StoreError::Internal(e.to_string()))
    }

    fn append(&self, key: &[u8], value: &[u8], apply_index: u64) -> StoreResult<usize> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        Ok(shard_guard.rocksdb_mut().append(&shard_id, key, value))
    }

    fn strlen(&self, key: &[u8], read_index: u64) -> StoreResult<usize> {
        // get() already verifies read_index, so we can just call it
        match self.get(key, read_index)? {
            Some(v) => Ok(v.len()),
            None => Ok(0),
        }
    }
}
