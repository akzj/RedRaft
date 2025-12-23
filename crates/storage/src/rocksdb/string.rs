//! String operations for ShardedRocksDB
//!
//! Provides Redis-compatible string operations:
//! - GET, SET, SETNX
//! - DEL
//! - INCR, INCRBY
//! - APPEND
//! - STRLEN

use crate::rocksdb::key_encoding::string_key;
use crate::rocksdb::SlotRocksDB;
use crate::traits::{StoreError, StoreResult};
use rr_core::routing::RoutingTable;
use anyhow::Result;
use rocksdb::WriteBatch;
use tracing::error;

impl SlotRocksDB {
    /// GET: Get string value
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cf = self.get_cf()?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);
        match self.db.get_cf(cf, &db_key) {
            Ok(value) => value,
            Err(e) => {
                error!("RocksDB GET error: {}", e);
                None
            }
        }
    }

    /// SET: Set string value
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.set_with_index(key, value, None)
    }

    /// SET with apply_index: Atomically set value and update apply_index
    ///
    /// This ensures that apply_index is updated atomically with the data write,
    /// preventing duplicate commits. If apply_index is provided and is <= current
    /// apply_index, the write is skipped (idempotent).
    pub fn set_with_index(
        &self,
        key: &[u8],
        value: Vec<u8>,
        apply_index: Option<u64>,
    ) -> Result<()> {
        let cf = self.get_cf()
            .ok_or_else(|| anyhow::anyhow!("Default column family not found"))?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);

        // If apply_index is provided, check for duplicate commit
        if let Some(new_index) = apply_index {
            if self.should_skip_apply_index(new_index)? {
                return Ok(()); // Already applied, skip (idempotent)
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, &value);
            self.add_apply_index_to_batch(&mut batch, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| anyhow::anyhow!("RocksDB SET (with index) error: {}", e))?;
        } else {
            // Normal write without apply_index
            self.db
                .put_cf_opt(cf, &db_key, &value, &self.write_opts)
                .map_err(|e| anyhow::anyhow!("RocksDB SET error: {}", e))?;
        }

        Ok(())
    }

    /// SETNX: Set if not exists
    pub fn setnx(&self, key: &[u8], value: Vec<u8>) -> Result<bool> {
        self.setnx_with_index(key, value, None)
    }

    /// SETNX with apply_index: Atomically set if not exists and update apply_index
    pub fn setnx_with_index(
        &self,
        key: &[u8],
        value: Vec<u8>,
        apply_index: Option<u64>,
    ) -> Result<bool> {
        let cf = self.get_cf()
            .ok_or_else(|| anyhow::anyhow!("Default column family not found"))?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);

        if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
            return Ok(false);
        }

        if let Some(new_index) = apply_index {
            if self.should_skip_apply_index(new_index)? {
                return Ok(false); // Already applied, skip
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, &value);
            self.add_apply_index_to_batch(&mut batch, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| anyhow::anyhow!("RocksDB SETNX (with index) error: {}", e))?;
        } else {
            // Normal write without apply_index
            self.db
                .put_cf_opt(cf, &db_key, &value, &self.write_opts)
                .map_err(|e| anyhow::anyhow!("RocksDB SETNX error: {}", e))?;
        }

        Ok(true)
    }

    /// DEL: Delete key
    pub fn del(&self, key: &[u8]) -> bool {
        self.del_with_index(key, None)
    }

    /// DEL with apply_index: Atomically delete key and update apply_index
    pub fn del_with_index(&self, key: &[u8], apply_index: Option<u64>) -> bool {
        let Some(cf) = self.get_cf() else {
            return false;
        };
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);
        if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
            if let Some(new_index) = apply_index {
                if self
                    .should_skip_apply_index(new_index)
                    .unwrap_or(false)
                {
                    return true; // Already applied, skip
                }

                // Atomic write: delete + apply_index in single batch
                let mut batch = WriteBatch::default();
                batch.delete_cf(cf, &db_key);
                self.add_apply_index_to_batch(&mut batch, new_index);

                if self.db.write_opt(batch, &self.write_opts).is_ok() {
                    return true;
                }
            } else {
                // Normal delete without apply_index
                return self.db.delete_cf_opt(cf, &db_key, &self.write_opts).is_ok();
            }
        }
        false
    }

    /// INCR/INCRBY
    pub fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        self.incrby_with_index(key, delta, None)
    }

    /// INCRBY with apply_index: Atomically increment and update apply_index
    pub fn incrby_with_index(
        &self,
        key: &[u8],
        delta: i64,
        apply_index: Option<u64>,
    ) -> StoreResult<i64> {
        let cf = self.get_cf()
            .ok_or_else(|| StoreError::Internal("Default column family not found".to_string()))?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);

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
                .should_skip_apply_index(new_index)
                .map_err(|e| StoreError::Internal(e.to_string()))?
            {
                return Ok(new_value); // Already applied, skip
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, new_value.to_string().as_bytes());
            self.add_apply_index_to_batch(&mut batch, new_index);

            self.db
                .write_opt(batch, &self.write_opts)
                .map_err(|e| StoreError::Internal(e.to_string()))?;
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
    pub fn append(&self, key: &[u8], value: &[u8]) -> usize {
        self.append_with_index(key, value, None)
    }

    /// APPEND with apply_index: Atomically append and update apply_index
    pub fn append_with_index(
        &self,
        key: &[u8],
        value: &[u8],
        apply_index: Option<u64>,
    ) -> usize {
        let Some(cf) = self.get_cf() else {
            return 0;
        };
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);
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
                .should_skip_apply_index(new_index)
                .unwrap_or(false)
            {
                return len; // Already applied, skip
            }

            // Atomic write: data + apply_index in single batch
            let mut batch = WriteBatch::default();
            batch.put_cf(cf, &db_key, &new_value);
            self.add_apply_index_to_batch(&mut batch, new_index);

            if self.db.write_opt(batch, &self.write_opts).is_ok() {
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
        0
    }

    /// STRLEN
    pub fn strlen(&self, key: &[u8]) -> usize {
        let Some(cf) = self.get_cf() else {
            return 0;
        };
        let slot = RoutingTable::slot_for_key(key);
        let db_key = string_key(slot, key);
        if let Ok(Some(value)) = self.db.get_cf(cf, &db_key) {
            return value.len();
        }
        0
    }
}
