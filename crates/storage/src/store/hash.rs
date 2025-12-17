//! Hash Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{HashStore, StoreError, StoreResult};
use bytes::Bytes;

impl HashStore for HybridStore {
    fn hget(&self, key: &[u8], field: &[u8], read_index: u64) -> StoreResult<Option<Bytes>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        Ok(shard_guard
            .rocksdb()
            .hget(&shard_id, key, field)
            .map(|v| Bytes::from(v)))
    }

    fn hset(&self, key: &[u8], field: &[u8], value: Bytes, apply_index: u64) -> StoreResult<bool> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        Ok(shard_guard
            .rocksdb_mut()
            .hset(&shard_id, key, field.as_ref(), value))
    }

    fn hmget(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        read_index: u64,
    ) -> StoreResult<Vec<Option<Bytes>>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        Ok(fields
            .iter()
            .map(|f| {
                shard_guard
                    .rocksdb()
                    .hget(&shard_id, key, f)
                    .map(|v| Bytes::from(v))
            })
            .collect())
    }

    fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>, apply_index: u64) -> StoreResult<()> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard.rocksdb_mut().hmset(&shard_id, key, fvs);
        Ok(())
    }

    fn hgetall(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        let v = shard_guard.rocksdb().hgetall(&shard_id, key);
        Ok(v.into_iter()
            .map(|(f, v)| (Bytes::from(f), Bytes::from(v)))
            .collect())
    }

    fn hkeys(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        let v = shard_guard.rocksdb().hkeys(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hvals(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        let v = shard_guard.rocksdb().hvals(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hsetnx(
        &self,
        key: &[u8],
        field: &[u8],
        value: Bytes,
        read_index: u64,
        apply_index: u64,
    ) -> StoreResult<bool> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Verify read_index is valid (should be <= current apply_index) for the existence check
        shard_guard.metadata.verify_read_index(read_index)?;
        // Check if field exists
        if shard_guard.rocksdb().hget(&shard_id, key, field).is_some() {
            return Ok(false);
        }
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        Ok(shard_guard.rocksdb_mut().hset(&shard_id, key, field, value))
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]], apply_index: u64) -> StoreResult<usize> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        Ok(shard_guard.rocksdb_mut().hdel(&shard_id, key, fields))
    }

    fn hlen(&self, key: &[u8], read_index: u64) -> StoreResult<usize> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        // Verify read_index is valid (should be <= current apply_index)
        shard_guard.metadata.verify_read_index(read_index)?;
        Ok(shard_guard.rocksdb().hlen(&shard_id, key))
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64, apply_index: u64) -> StoreResult<i64> {
        let shard_id = self.shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let mut shard_guard = shard.write();
        // Update apply_index in metadata
        shard_guard.metadata.set_apply_index(apply_index);
        shard_guard
            .rocksdb_mut()
            .hincrby(&shard_id, key, field, delta)
    }
}
