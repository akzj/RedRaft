//! String operations for ShardedRocksDB
//!
//! Provides Redis-compatible string operations:
//! - GET, SET, SETNX
//! - DEL
//! - INCR, INCRBY
//! - APPEND
//! - STRLEN

use crate::rocksdb::key_encoding::string_key;
use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use crate::traits::{StoreError, StoreResult};
use rocksdb::WriteBatch;
use tracing::error;

impl ShardedRocksDB {
    /// GET: Get string value from specific shard
    pub fn get(&self, shard_id: &ShardId, key: &[u8]) -> Option<Vec<u8>> {
        let cf = self.get_cf(shard_id)?;
        let db_key = string_key(key);
        match self.db.get_cf(cf, &db_key) {
            Ok(value) => value,
            Err(e) => {
                error!("RocksDB GET error: {}", e);
                None
            }
        }
    }

    /// SET: Set string value in specific shard
    pub fn set(&self, shard_id: &ShardId, key: &[u8], value: Vec<u8>) -> Result<(), String> {
        self.set_with_index(shard_id, key, value, None)
    }

    /// SET with apply_index: Atomically set value and update apply_index
    ///
    /// This ensures that apply_index is updated atomically with the data write,
    /// preventing duplicate commits. If apply_index is provided and is <= current
    /// apply_index, the write is skipped (idempotent).
    pub fn set_with_index(
        &self,
        shard_id: &ShardId,
        key: &[u8],
        value: Vec<u8>,
        apply_index: Option<u64>,
    ) -> Result<(), String> {
        let cf = self
            .get_or_create_cf(shard_id)
            .map_err(|e| format!("RocksDB SET (with index) error: {}", e))?;
        let db_key = string_key(key);

        // If apply_index is provided, check for duplicate commit
        if let Some(new_index) = apply_index {
            if self.should_skip_apply_index(shard_id, new_index)? {
                return Ok(()); // Already applied, skip (idempotent)
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, &value);
            self.add_apply_index_to_batch(&mut batch, cf, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| format!("RocksDB SET (with index) error: {}", e))?;

            // Update in-memory cache
            self.set_shard_apply_index(shard_id, new_index);
        } else {
            // Normal write without apply_index
            self.db
                .put_cf_opt(cf, &db_key, &value, &self.write_opts)
                .map_err(|e| format!("RocksDB SET error: {}", e))?;
        }

        Ok(())
    }

    /// SETNX: Set if not exists
    pub fn setnx(&self, shard_id: &ShardId, key: &[u8], value: Vec<u8>) -> Result<bool, String> {
        self.setnx_with_index(shard_id, key, value, None)
    }

    /// SETNX with apply_index: Atomically set if not exists and update apply_index
    pub fn setnx_with_index(
        &self,
        shard_id: &ShardId,
        key: &[u8],
        value: Vec<u8>,
        apply_index: Option<u64>,
    ) -> Result<bool, String> {
        let cf = self.get_or_create_cf(shard_id)?;
        let db_key = string_key(key);

        if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
            return Ok(false);
        }

        if let Some(new_index) = apply_index {
            if self.should_skip_apply_index(shard_id, new_index)? {
                return Ok(false); // Already applied, skip
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, &value);
            self.add_apply_index_to_batch(&mut batch, cf, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| format!("RocksDB SETNX (with index) error: {}", e))?;

            // Update in-memory cache
            self.set_shard_apply_index(shard_id, new_index);
        } else {
            // Normal write without apply_index
            self.db
                .put_cf_opt(cf, &db_key, &value, &self.write_opts)
                .map_err(|e| format!("RocksDB SETNX error: {}", e))?;
        }

        Ok(true)
    }

    /// DEL: Delete key from specific shard
    pub fn del(&self, shard_id: &ShardId, key: &[u8]) -> bool {
        self.del_with_index(shard_id, key, None)
    }

    /// DEL with apply_index: Atomically delete key and update apply_index
    pub fn del_with_index(&self, shard_id: &ShardId, key: &[u8], apply_index: Option<u64>) -> bool {
        if let Some(cf) = self.get_cf(shard_id) {
            let db_key = string_key(key);
            if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                if let Some(new_index) = apply_index {
                    if self
                        .should_skip_apply_index(shard_id, new_index)
                        .unwrap_or(false)
                    {
                        return true; // Already applied, skip
                    }

                    // Atomic write: delete + apply_index in single batch
                    let mut batch = WriteBatch::default();
                    batch.delete_cf(cf, &db_key);
                    self.add_apply_index_to_batch(&mut batch, cf, new_index);

                    if self.db.write_opt(batch, &self.write_opts).is_ok() {
                        // Update in-memory cache
                        self.set_shard_apply_index(shard_id, new_index);
                        return true;
                    }
                } else {
                    // Normal delete without apply_index
                    return self.db.delete_cf_opt(cf, &db_key, &self.write_opts).is_ok();
                }
            }
        }
        false
    }

    /// INCR/INCRBY
    pub fn incrby(&self, shard_id: &ShardId, key: &[u8], delta: i64) -> StoreResult<i64> {
        self.incrby_with_index(shard_id, key, delta, None)
    }

    /// INCRBY with apply_index: Atomically increment and update apply_index
    pub fn incrby_with_index(
        &self,
        shard_id: &ShardId,
        key: &[u8],
        delta: i64,
        apply_index: Option<u64>,
    ) -> StoreResult<i64> {
        let cf = self
            .get_or_create_cf(shard_id)
            .map_err(|e| StoreError::Internal(e))?;
        let db_key = string_key(key);

        let current = match self.db.get_cf(cf, &db_key) {
            Ok(Some(value)) => {
                let s = String::from_utf8_lossy(&value);
                s.parse::<i64>().map_err(|_| {
                    StoreError::InvalidArgument("value is not an integer".to_string())
                })?
            }
            Ok(None) => 0,
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

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, new_value.to_string().as_bytes());
            self.add_apply_index_to_batch(&mut batch, cf, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| StoreError::Internal(e.to_string()))?;

            // Update in-memory cache
            self.set_shard_apply_index(shard_id, new_index);
        } else {
            // Normal write without apply_index
            self.db
                .put_cf_opt(
                    cf,
                    &db_key,
                    new_value.to_string().as_bytes(),
                    &self.write_opts,
                )
                .map_err(|e| StoreError::Internal(e.to_string()))?;
        }

        Ok(new_value)
    }

    /// APPEND
    pub fn append(&self, shard_id: &ShardId, key: &[u8], value: &[u8]) -> usize {
        self.append_with_index(shard_id, key, value, None)
    }

    /// APPEND with apply_index: Atomically append and update apply_index
    pub fn append_with_index(
        &self,
        shard_id: &ShardId,
        key: &[u8],
        value: &[u8],
        apply_index: Option<u64>,
    ) -> usize {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            let db_key = string_key(key);
            let new_value = match self.db.get_cf(cf, &db_key) {
                Ok(Some(mut existing)) => {
                    existing.extend_from_slice(value);
                    existing
                }
                _ => value.to_vec(),
            };

            let len = new_value.len();

            if let Some(new_index) = apply_index {
                if self
                    .should_skip_apply_index(shard_id, new_index)
                    .unwrap_or(false)
                {
                    return len; // Already applied, skip
                }

                // Atomic write: data + apply_index in single batch
                let mut batch = WriteBatch::default();
                batch.put_cf(cf, &db_key, &new_value);
                self.add_apply_index_to_batch(&mut batch, cf, new_index);

                if self.db.write_opt(batch, &self.write_opts).is_ok() {
                    // Update in-memory cache
                    self.set_shard_apply_index(shard_id, new_index);
                    return len;
                }
            } else {
                // Normal write without apply_index
                if self
                    .db
                    .put_cf_opt(cf, &db_key, &new_value, &self.write_opts)
                    .is_ok()
                {
                    return len;
                }
            }
        }
        0
    }

    /// STRLEN
    pub fn strlen(&self, shard_id: &ShardId, key: &[u8]) -> usize {
        if let Some(cf) = self.get_cf(shard_id) {
            let db_key = string_key(key);
            if let Ok(Some(value)) = self.db.get_cf(cf, &db_key) {
                return value.len();
            }
        }
        0
    }
}
