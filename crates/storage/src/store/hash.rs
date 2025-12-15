//! Hash Store implementation for HybridStore

use crate::store::HybridStore;
use crate::traits::{HashStore, StoreError, StoreResult};
use bytes::Bytes;

impl HashStore for HybridStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> StoreResult<Option<Bytes>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .hget(&shard_id, key, field)
            .map(|v| Bytes::from(v)))
    }

    fn hset(&self, key: &[u8], field: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard
            .rocksdb()
            .hset(&shard_id, key, field.as_ref(), value))
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> StoreResult<Vec<Option<Bytes>>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
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

    fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>) -> StoreResult<()> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.rocksdb().hmset(&shard_id, key, fvs);
        Ok(())
    }

    fn hgetall(&self, key: &[u8]) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hgetall(&shard_id, key);
        Ok(v.into_iter()
            .map(|(f, v)| (Bytes::from(f), Bytes::from(v)))
            .collect())
    }

    fn hkeys(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hkeys(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hvals(&self, key: &[u8]) -> StoreResult<Vec<Bytes>> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        let v = shard_guard.rocksdb().hvals(&shard_id, key);
        Ok(v.into_iter().map(Bytes::from).collect())
    }

    fn hsetnx(&self, key: &[u8], field: &[u8], value: Bytes) -> StoreResult<bool> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        if shard_guard.rocksdb().hget(&shard_id, key, field).is_some() {
            return Ok(false);
        }
        Ok(shard_guard.rocksdb().hset(&shard_id, key, field, value))
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> StoreResult<usize> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().hdel(&shard_id, key, fields))
    }

    fn hlen(&self, key: &[u8]) -> StoreResult<usize> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        Ok(shard_guard.rocksdb().hlen(&shard_id, key))
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let shard_id = self
            .shard_for_key(key)?;
        let shard = self.get_shard(key)?;
        let shard_guard = shard.read();
        shard_guard.rocksdb().hincrby(&shard_id, key, field, delta)
    }
}

