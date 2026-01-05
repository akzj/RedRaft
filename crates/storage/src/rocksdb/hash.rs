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
use crate::rocksdb::SlotRocksDB;
use crate::traits::{ApplyContext, StoreError, StoreResult};
use bytes::Bytes;
use rocksdb::WriteBatch;
use rr_core::routing::RoutingTable;

impl SlotRocksDB {
    /// HGET
    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let cf = self.get_cf()?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = hash_field_key(slot, key, field);
        self.db.get_cf(cf, &db_key).ok().flatten()
    }

    /// HSET
    pub fn hset(&self, key: &[u8], field: &[u8], value: Bytes) -> bool {
        self.hset_with_context(key, field.as_ref(), value, &ApplyContext::default())
    }

    /// HSET with apply_index: Atomically set hash field and update apply_index
    pub fn hset_with_index(
        &self,
        key: &[u8],
        field: &[u8],
        value: Bytes,
        apply_index: Option<u64>,
    ) -> bool {
        let ctx = ApplyContext {
            apply_index,
            ..Default::default()
        };
        self.hset_with_context(key, field, value, &ctx)
    }

    /// HSET with ApplyContext: Atomically set hash field, update apply_index and slot metadata
    pub fn hset_with_context(
        &self,
        key: &[u8],
        field: &[u8],
        value: Bytes,
        ctx: &ApplyContext,
    ) -> bool {
        let Some(cf) = self.get_cf() else {
            return false;
        };
        let slot = RoutingTable::slot_for_key(key);
        let db_key = hash_field_key(slot, key, &field);
        let is_new = self.db.get_cf(cf, &db_key).ok().flatten().is_none();

        // Write data, slot metadata and field count atomically using batch
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, &db_key, &value);
        self.add_slot_meta_to_batch(&mut batch, slot, ctx);
        if is_new {
            self.add_hash_field_count_to_batch(&mut batch, key, 1);
        }

        if self.db.write_opt(batch, &self.write_opts).is_ok() {
            return is_new;
        }
        false
    }

    /// HMSET
    pub fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>) {
        self.hmset_with_context(key, fvs, &ApplyContext::default())
    }

    /// HMSET with apply_index: Atomically set multiple hash fields and update apply_index
    pub fn hmset_with_index(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>, apply_index: Option<u64>) {
        let ctx = ApplyContext {
            apply_index,
            ..Default::default()
        };
        self.hmset_with_context(key, fvs, &ctx)
    }

    /// HMSET with ApplyContext: Atomically set multiple hash fields, update apply_index and slot metadata
    pub fn hmset_with_context(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>, ctx: &ApplyContext) {
        let Some(cf) = self.get_cf() else {
            return;
        };
        // No idempotent check needed - apply_index is per-slot

        let slot = RoutingTable::slot_for_key(key);
        let mut batch = WriteBatch::default();
        let mut new_fields = 0;

        for (field, value) in fvs {
            let db_key = hash_field_key(slot, key, &field);
            if self.db.get_cf(cf, &db_key).ok().flatten().is_none() {
                new_fields += 1;
            }
            batch.put_cf(cf, &db_key, &value);
        }

        self.add_slot_meta_to_batch(&mut batch, slot, ctx);
        if new_fields > 0 {
            self.add_hash_field_count_to_batch(&mut batch, key, new_fields);
        }

        if self.db.write_opt(batch, &self.write_opts).is_ok() {
            // Write successful
        }
    }

    /// HDEL
    pub fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        self.hdel_with_context(key, fields, &ApplyContext::default())
    }

    /// HDEL with apply_index: Atomically delete hash fields and update apply_index
    pub fn hdel_with_index(&self, key: &[u8], fields: &[&[u8]], apply_index: Option<u64>) -> usize {
        let ctx = ApplyContext {
            apply_index,
            ..Default::default()
        };
        self.hdel_with_context(key, fields, &ctx)
    }

    /// HDEL with ApplyContext: Atomically delete hash fields, update apply_index and slot metadata
    pub fn hdel_with_context(&self, key: &[u8], fields: &[&[u8]], ctx: &ApplyContext) -> usize {
        let Some(cf) = self.get_cf() else {
            return 0;
        };
        // No idempotent check needed - apply_index is per-slot
        let slot = RoutingTable::slot_for_key(key);
        let mut batch = WriteBatch::default();
        let mut deleted = 0;

        for field in fields {
            let db_key = hash_field_key(slot, key, field);
            if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                batch.delete_cf(cf, &db_key);
                deleted += 1;
            }
        }

        if deleted > 0 {
            if let Some(_new_index) = ctx.apply_index {
                self.add_slot_meta_to_batch(&mut batch, slot, ctx);
            }
            self.add_hash_field_count_to_batch(&mut batch, key, -(deleted as i64));

            if self.db.write_opt(batch, &self.write_opts).is_ok() {
                return deleted;
            }
        }
        0
    }

    /// HEXISTS
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> bool {
        let Some(cf) = self.get_cf() else {
            return false;
        };
        let slot = RoutingTable::slot_for_key(key);
        let db_key = hash_field_key(slot, key, field);
        self.db.get_cf(cf, &db_key).ok().flatten().is_some()
    }

    /// HGETALL
    pub fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        let Some(cf) = self.get_cf() else {
            return result;
        };
        let slot = RoutingTable::slot_for_key(key);
        let prefix = hash_field_prefix(slot, key);
        let iter = self.db.prefix_iterator_cf(cf, &prefix);

        for item in iter {
            if let Ok((k, v)) = item {
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(field) = extract_hash_field(&k, slot, key) {
                    result.push((field.to_vec(), v.to_vec()));
                }
            }
        }
        result
    }

    /// HKEYS
    pub fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        let Some(cf) = self.get_cf() else {
            return result;
        };
        let slot = RoutingTable::slot_for_key(key);
        let prefix = hash_field_prefix(slot, key);
        let iter = self.db.prefix_iterator_cf(cf, &prefix);

        for item in iter {
            if let Ok((k, _)) = item {
                if !k.starts_with(&prefix) {
                    break;
                }
                if let Some(field) = extract_hash_field(&k, slot, key) {
                    result.push(field.to_vec());
                }
            }
        }
        result
    }

    /// HVALS
    pub fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();
        let Some(cf) = self.get_cf() else {
            return result;
        };
        let slot = RoutingTable::slot_for_key(key);
        let prefix = hash_field_prefix(slot, key);
        let iter = self.db.prefix_iterator_cf(cf, &prefix);

        for item in iter {
            if let Ok((k, v)) = item {
                if !k.starts_with(&prefix) {
                    break;
                }
                result.push(v.to_vec());
            }
        }
        result
    }

    /// HLEN
    pub fn hlen(&self, key: &[u8]) -> usize {
        let Some(cf) = self.get_cf() else {
            return 0;
        };
        let slot = RoutingTable::slot_for_key(key);
        let meta_key = hash_meta_key(slot, key);
        if let Ok(Some(value)) = self.db.get_cf(cf, &meta_key) {
            let s = String::from_utf8_lossy(&value);
            return s.parse::<usize>().unwrap_or(0);
        }
        0
    }

    /// HINCRBY
    pub fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        self.hincrby_with_context(key, field, delta, &ApplyContext::default())
    }

    /// HINCRBY with apply_index: Atomically increment hash field and update apply_index
    pub fn hincrby_with_index(
        &self,
        key: &[u8],
        field: &[u8],
        delta: i64,
        apply_index: Option<u64>,
    ) -> StoreResult<i64> {
        let ctx = ApplyContext {
            apply_index,
            ..Default::default()
        };
        self.hincrby_with_context(key, field, delta, &ctx)
    }

    /// HINCRBY with ApplyContext: Atomically increment hash field, update apply_index and slot metadata
    pub fn hincrby_with_context(
        &self,
        key: &[u8],
        field: &[u8],
        delta: i64,
        ctx: &ApplyContext,
    ) -> StoreResult<i64> {
        let cf = self.get_cf()
            .ok_or_else(|| StoreError::Internal("Default column family not found".to_string()))?;
        let slot = RoutingTable::slot_for_key(key);
        let db_key = hash_field_key(slot, key, field);

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

        // Write data, slot metadata and field count atomically using batch
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, &db_key, new_value.to_string().as_bytes());
        self.add_slot_meta_to_batch(&mut batch, slot, ctx);
        if is_new {
            self.add_hash_field_count_to_batch(&mut batch, key, 1);
        }

        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(new_value)
    }

    /// Helper: Add hash field count update to batch (atomic)
    /// This reads the current count before adding to batch, ensuring atomicity
    fn add_hash_field_count_to_batch(&self, batch: &mut WriteBatch, key: &[u8], delta: i64) {
        let Some(cf) = self.get_cf() else {
            return;
        };
        let slot = RoutingTable::slot_for_key(key);
        let meta_key = hash_meta_key(slot, key);

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
