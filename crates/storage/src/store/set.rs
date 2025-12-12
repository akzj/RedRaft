//! Set Store implementation for HybridStore

use crate::memory::DataCow;
use crate::store::HybridStore;
use crate::traits::{SetStore, StoreError, StoreResult};
use bytes::Bytes;

impl SetStore for HybridStore {
    fn sadd(&self, key: &[u8], members: Vec<Bytes>) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().store.add(key.to_vec(), member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let mut shard_guard = shard.write();
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().store.remove(key, member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn smembers(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        // Get DataCow for the key and extract members
        if let Some(DataCow::Set(set)) = shard_guard.memory().store.get(key) {
            Ok(set.members())
        } else {
            Err(StoreError::WrongType)
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> StoreResult<bool> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.memory().store.contains(key, member))
    }

    fn scard(&self, key: &[u8]) -> StoreResult<usize> {
        let shard = self.get_create_shard(key)?;
        let shard_guard = shard.read();
        shard_guard
            .memory()
            .store
            .len(key)
            .ok_or(StoreError::WrongType)
    }
}

