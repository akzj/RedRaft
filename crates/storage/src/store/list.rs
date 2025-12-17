//! List Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ListStore, StoreResult};
use bytes::Bytes;

impl ListStore for HybridStore {
    fn lpush(&self, key: &[u8], values: Vec<Bytes>, apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.memory_mut().lpush(key, values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Bytes>, apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.memory_mut().rpush(key, values)
    }

    fn lpop(&self, key: &[u8], apply_index: u64) -> StoreResult<Option<Bytes>> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.memory_mut().lpop(key)
    }

    fn rpop(&self, key: &[u8], apply_index: u64) -> StoreResult<Option<Bytes>> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.memory_mut().rpop(key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64, read_index: u64) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        shard_guard.memory().lrange(key, start, stop)
    }

    fn llen(&self, key: &[u8], read_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        shard_guard.memory().llen(key)
    }

    fn lindex(&self, key: &[u8], index: i64, read_index: u64) -> StoreResult<Option<Bytes>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        shard_guard.memory().lindex(key, index)
    }

    fn lset(&self, key: &[u8], index: i64, value: Bytes, apply_index: u64) -> StoreResult<()> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.memory_mut().lset(key, index, value)
    }

    fn ltrim(&self, key: &[u8], start: i64, stop: i64, apply_index: u64) -> StoreResult<()> {
        // TODO: Implement ltrim
        let _ = (key, start, stop, apply_index);
        Err(crate::traits::StoreError::NotSupported)
    }

    fn lrem(&self, key: &[u8], count: i64, value: &[u8], apply_index: u64) -> StoreResult<usize> {
        // TODO: Implement lrem
        let _ = (key, count, value, apply_index);
        Ok(0)
    }
}

