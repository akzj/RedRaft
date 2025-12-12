//! List Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{ListStore, StoreResult};
use bytes::Bytes;

impl ListStore for HybridStore {
    fn lpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lpush(key, values)
    }

    fn rpush(&self, key: &[u8], values: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.rpush(key, values)
    }

    fn lpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lpop(key)
    }

    fn rpop(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.rpop(key)
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.lrange(key, start, stop)
    }

    fn llen(&self, key: &[u8]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.llen(key)
    }

    fn lindex(&self, key: &[u8], index: i64) -> StoreResult<Option<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.memory().store.lindex(key, index)
    }

    fn lset(&self, key: &[u8], index: i64, value: Bytes) -> StoreResult<()> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        shard_guard.memory_mut().store.lset(key, index, value)
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

