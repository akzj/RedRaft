//! Hash operations for ShardedRocksDB
//!
//! Provides Redis-compatible hash operations:
//! - HGET, HSET, HMSET
//! - HDEL
//! - HEXISTS
//! - HGETALL, HKEYS, HVALS
//! - HLEN
//! - HINCRBY

use crate::rocksdb::key_encoding::{
    extract_hash_field, hash_field_key, hash_field_prefix, hash_meta_key,
};
use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use crate::traits::{StoreError, StoreResult};
use rocksdb::{ColumnFamily, WriteBatch};

impl ShardedRocksDB {
    /// HGET
    pub fn hget(&self, shard_id: ShardId, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let cf = self.get_cf(shard_id)?;
        let db_key = hash_field_key(key, field);
        self.db.get_cf(cf, &db_key).ok().flatten()
    }

    /// HSET
    pub fn hset(&self, shard_id: ShardId, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        self.hset_with_index(shard_id, key, field, value, None)
    }

    /// HSET with apply_index: Atomically set hash field and update apply_index
    pub fn hset_with_index(
        &self,
        shard_id: ShardId,
        key: &[u8],
        field: Vec<u8>,
        value: Vec<u8>,
        apply_index: Option<u64>,
    ) -> bool {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            let db_key = hash_field_key(key, &field);
            let is_new = self.db.get_cf(cf, &db_key).ok().flatten().is_none();

            if let Some(new_index) = apply_index {
                if self
                    .should_skip_apply_index(shard_id, new_index)
                    .unwrap_or(false)
                {
                    return is_new; // Already applied, skip
                }

                // Atomic write: data + apply_index + field_count in single batch
                let mut batch = WriteBatch::default();
                batch.put_cf(cf, &db_key, &value);
                self.add_apply_index_to_batch(&mut batch, cf, new_index);
                if is_new {
                    self.add_hash_field_count_to_batch(&mut batch, cf, key, 1);
                }

                if self.db.write_opt(batch, &self.write_opts).is_ok() {
                    // Update in-memory cache
                    self.set_shard_apply_index(shard_id, new_index);
                    return is_new;
                }
            } else {
                // Normal write without apply_index - use batch for atomicity
                let mut batch = WriteBatch::default();
                batch.put_cf(cf, &db_key, &value);
                if is_new {
                    self.add_hash_field_count_to_batch(&mut batch, cf, key, 1);
                }
                if self.db.write_opt(batch, &self.write_opts).is_ok() {
                    return is_new;
                }
            }
        }
        false
    }

    /// HMSET
    pub fn hmset(&self, shard_id: ShardId, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>) {
        self.hmset_with_index(shard_id, key, fvs, None)
    }

    /// HMSET with apply_index: Atomically set multiple hash fields and update apply_index
    pub fn hmset_with_index(
        &self,
        shard_id: ShardId,
        key: &[u8],
        fvs: Vec<(Vec<u8>, Vec<u8>)>,
        apply_index: Option<u64>,
    ) {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            // Check apply_index first (idempotent)
            let should_skip = if let Some(new_index) = apply_index {
                self.should_skip_apply_index(shard_id, new_index)
                    .unwrap_or(false)
            } else {
                false
            };

            if should_skip {
                return; // Already applied, skip
            }

            let mut batch = WriteBatch::default();
            let mut new_fields = 0;

            for (field, value) in fvs {
                let db_key = hash_field_key(key, &field);
                if self.db.get_cf(cf, &db_key).ok().flatten().is_none() {
                    new_fields += 1;
                }
                batch.put_cf(cf, &db_key, &value);
            }

            if let Some(new_index) = apply_index {
                self.add_apply_index_to_batch(&mut batch, cf, new_index);
            }
            if new_fields > 0 {
                self.add_hash_field_count_to_batch(&mut batch, cf, key, new_fields);
            }

            if self.db.write_opt(batch, &self.write_opts).is_ok() {
                if let Some(new_index) = apply_index {
                    // Update in-memory cache
                    self.set_shard_apply_index(shard_id, new_index);
                }
            }
        }
    }

    /// HDEL
    pub fn hdel(&self, shard_id: ShardId, key: &[u8], fields: &[&[u8]]) -> usize {
        self.hdel_with_index(shard_id, key, fields, None)
    }

    /// HDEL with apply_index: Atomically delete hash fields and update apply_index
    pub fn hdel_with_index(
        &self,
        shard_id: ShardId,
        key: &[u8],
        fields: &[&[u8]],
        apply_index: Option<u64>,
    ) -> usize {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            // Check apply_index first (idempotent)
            let should_skip = if let Some(new_index) = apply_index {
                self.should_skip_apply_index(shard_id, new_index)
                    .unwrap_or(false)
            } else {
                false
            };

            if should_skip {
                // Count existing fields to return correct count
                let mut count = 0;
                for field in fields {
                    let db_key = hash_field_key(key, field);
                    if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                        count += 1;
                    }
                }
                return count;
            }

            let mut batch = WriteBatch::default();
            let mut deleted = 0;

            for field in fields {
                let db_key = hash_field_key(key, field);
                if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                    batch.delete_cf(cf, &db_key);
                    deleted += 1;
                }
            }

            if deleted > 0 {
                if let Some(new_index) = apply_index {
                    self.add_apply_index_to_batch(&mut batch, cf, new_index);
                }
                self.add_hash_field_count_to_batch(&mut batch, cf, key, -(deleted as i64));

                if self.db.write_opt(batch, &self.write_opts).is_ok() {
                    if let Some(new_index) = apply_index {
                        // Update in-memory cache
                        self.set_shard_apply_index(shard_id, new_index);
                    }
                    return deleted;
                }
            }
        }
        0
    }

    /// HEXISTS
    pub fn hexists(&self, shard_id: ShardId, key: &[u8], field: &[u8]) -> bool {
        if let Some(cf) = self.get_cf(shard_id) {
            let db_key = hash_field_key(key, field);
            return self.db.get_cf(cf, &db_key).ok().flatten().is_some();
        }
        false
    }

    /// HGETALL
    pub fn hgetall(&self, shard_id: ShardId, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        if let Some(cf) = self.get_cf(shard_id) {
            let prefix = hash_field_prefix(key);
            let iter = self.db.prefix_iterator_cf(cf, &prefix);

            for item in iter {
                if let Ok((k, v)) = item {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    if let Some(field) = extract_hash_field(&k, key) {
                        result.push((field.to_vec(), v.to_vec()));
                    }
                }
            }
        }
        result
    }

    /// HKEYS
    pub fn hkeys(&self, shard_id: ShardId, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        if let Some(cf) = self.get_cf(shard_id) {
            let prefix = hash_field_prefix(key);
            let iter = self.db.prefix_iterator_cf(cf, &prefix);

            for item in iter {
                if let Ok((k, _)) = item {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    if let Some(field) = extract_hash_field(&k, key) {
                        result.push(field.to_vec());
                    }
                }
            }
        }
        result
    }

    /// HVALS
    pub fn hvals(&self, shard_id: ShardId, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        if let Some(cf) = self.get_cf(shard_id) {
            let prefix = hash_field_prefix(key);
            let iter = self.db.prefix_iterator_cf(cf, &prefix);

            for item in iter {
                if let Ok((k, v)) = item {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    result.push(v.to_vec());
                }
            }
        }
        result
    }

    /// HLEN
    pub fn hlen(&self, shard_id: ShardId, key: &[u8]) -> usize {
        if let Some(cf) = self.get_cf(shard_id) {
            let meta_key = hash_meta_key(key);
            if let Ok(Some(value)) = self.db.get_cf(cf, &meta_key) {
                let s = String::from_utf8_lossy(&value);
                return s.parse::<usize>().unwrap_or(0);
            }
        }
        0
    }

    /// HINCRBY
    pub fn hincrby(
        &self,
        shard_id: ShardId,
        key: &[u8],
        field: &[u8],
        delta: i64,
    ) -> StoreResult<i64> {
        self.hincrby_with_index(shard_id, key, field, delta, None)
    }

    /// HINCRBY with apply_index: Atomically increment hash field and update apply_index
    pub fn hincrby_with_index(
        &self,
        shard_id: ShardId,
        key: &[u8],
        field: &[u8],
        delta: i64,
        apply_index: Option<u64>,
    ) -> StoreResult<i64> {
        let cf = self
            .get_or_create_cf(shard_id)
            .map_err(|e| StoreError::Internal(e))?;
        let db_key = hash_field_key(key, field);

        let (current, is_new) = match self.db.get_cf(cf, &db_key) {
            Ok(Some(value)) => {
                let s = String::from_utf8_lossy(&value);
                let val = s.parse::<i64>().map_err(|_| {
                    StoreError::InvalidArgument("hash value is not an integer".to_string())
                })?;
                (val, false)
            }
            Ok(None) => (0, true),
            Err(e) => return Err(StoreError::Internal(e.to_string())),
        };

        let new_value = current
            .checked_add(delta)
            .ok_or_else(|| StoreError::InvalidArgument("integer overflow".to_string()))?;

        if let Some(new_index) = apply_index {
            if self
                .should_skip_apply_index(shard_id, new_index)
                .map_err(|e| StoreError::Internal(e))?
            {
                return Ok(new_value); // Already applied, skip
            }

            // Atomic write: data + apply_index + field_count in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, new_value.to_string().as_bytes());
            self.add_apply_index_to_batch(&mut batch, cf, new_index);
            if is_new {
                self.add_hash_field_count_to_batch(&mut batch, cf, key, 1);
            }

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| StoreError::Internal(e.to_string()))?;

            // Update in-memory cache
            self.set_shard_apply_index(shard_id, new_index);
        } else {
            // Normal write without apply_index - use batch for atomicity
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, new_value.to_string().as_bytes());
            if is_new {
                self.add_hash_field_count_to_batch(&mut batch, cf, key, 1);
            }
            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        Ok(new_value)
    }

    /// Helper: Add hash field count update to batch (atomic)
    /// This reads the current count before adding to batch, ensuring atomicity
    fn add_hash_field_count_to_batch(
        &self,
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        key: &[u8],
        delta: i64,
    ) {
        let meta_key = hash_meta_key(key);

        // Read current count before batch commit
        let current = self
            .db
            .get_cf(cf, &meta_key)
            .ok()
            .flatten()
            .and_then(|v| String::from_utf8_lossy(&v).parse::<i64>().ok())
            .unwrap_or(0);

        let new_count = (current + delta).max(0);
        if new_count == 0 {
            batch.delete_cf(cf, &meta_key);
        } else {
            batch.put_cf(cf, &meta_key, new_count.to_string().as_bytes());
        }
    }
}

