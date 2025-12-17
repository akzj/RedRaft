//! Set Store implementation for HybridStore

use crate::memory::DataCow;
use crate::store::HybridStore;
use crate::traits::{SetStore, StoreError, StoreResult};
use bytes::Bytes;

impl SetStore for HybridStore {
    fn sadd(&self, key: &[u8], members: Vec<Bytes>, apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().add(key.to_vec(), member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]], apply_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        let mut count = 0;
        for member in members {
            if shard_guard.memory_mut().remove(key, member)? {
                count += 1;
            }
        }
        Ok(count)
    }

    fn smembers(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        // Get DataCow for the key and extract members
        if let Some(DataCow::Set(set)) = shard_guard.memory().get(key) {
            Ok(set.members())
        } else {
            Err(StoreError::WrongType)
        }
    }

    fn sismember(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<bool> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        Ok(shard_guard.memory().contains(key, member))
    }

    fn scard(&self, key: &[u8], read_index: u64) -> StoreResult<usize> {
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        shard_guard
            .memory()
            .len(key)
            .ok_or(StoreError::WrongType)
    }
}

