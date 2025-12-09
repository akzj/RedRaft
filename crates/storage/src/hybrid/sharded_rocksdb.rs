//! Sharded RocksDB Storage
//!
//! Uses RocksDB Column Families for native sharding support.
//! Each shard has its own Column Family for isolation and efficient snapshot.
//!
//! ## Key Encoding
//!
//! Keys within each shard's Column Family:
//! - String: `s:{key}`
//! - Hash field: `h:{key}:{field}`
//! - Hash metadata: `H:{key}`

use crate::memory::{slot_for_key, ShardId, TOTAL_SLOTS};
use crate::traits::{StoreError, StoreResult};
use parking_lot::RwLock;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info};

/// Key type prefixes
mod key_prefix {
    pub const STRING: u8 = b's';
    pub const HASH: u8 = b'h';
    pub const HASH_META: u8 = b'H';
}

/// Build string key
fn string_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::STRING);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field key
fn hash_field_key(key: &[u8], field: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len() + field.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result.extend_from_slice(field);
    result
}

/// Build hash metadata key
fn hash_meta_key(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(2 + key.len());
    result.push(key_prefix::HASH_META);
    result.push(b':');
    result.extend_from_slice(key);
    result
}

/// Build hash field prefix for iteration
fn hash_field_prefix(key: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(3 + key.len());
    result.push(key_prefix::HASH);
    result.push(b':');
    result.extend_from_slice(key);
    result.push(b':');
    result
}

/// Extract field from hash key
fn extract_hash_field<'a>(encoded: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let prefix_len = 2 + key.len() + 1;
    if encoded.len() <= prefix_len {
        return None;
    }
    Some(&encoded[prefix_len..])
}

/// Shard metadata stored in RocksDB
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    pub shard_id: ShardId,
    pub slot_start: u32,
    pub slot_end: u32,
    pub apply_index: Option<u64>,
}

/// Column Family name for a shard
fn shard_cf_name(shard_id: ShardId) -> String {
    format!("shard_{:04x}", shard_id)
}

/// Sharded RocksDB Storage
///
/// Uses Column Families for native sharding.
/// Path: shard_id (Column Family) -> key -> value
pub struct ShardedRocksDB {
    /// RocksDB instance
    db: Arc<DB>,
    /// Database path
    path: String,
    /// Number of shards
    shard_count: u32,
    /// Write options
    write_opts: WriteOptions,
    /// Shard metadata cache
    shard_metadata: Arc<RwLock<HashMap<ShardId, ShardMetadata>>>,
}

impl ShardedRocksDB {
    /// Create a new ShardedRocksDB
    pub fn new<P: AsRef<Path>>(path: P, shard_count: u32) -> Result<Self, String> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let shard_count = shard_count.max(1);

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_background_jobs(4);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // Create Column Family descriptors for each shard
        let cf_names: Vec<String> = std::iter::once("default".to_string())
            .chain((0..shard_count).map(|id| shard_cf_name(id)))
            .collect();

        // Try to open with existing CFs, or create new
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();
            
        let db = match DB::open_cf_descriptors(&opts, &path_str, cf_descriptors) {
            Ok(db) => db,
            Err(_) => {
                // First time open, create the database
                let mut db = DB::open(&opts, &path_str)
                    .map_err(|e| format!("Failed to open RocksDB: {}", e))?;
                
                // Create shard Column Families
                for shard_id in 0..shard_count {
                    let cf_name = shard_cf_name(shard_id);
                    db.create_cf(&cf_name, &Options::default())
                        .map_err(|e| format!("Failed to create CF {}: {}", cf_name, e))?;
                }
                db
            }
        };

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        // Initialize shard metadata
        let mut shard_metadata = HashMap::new();
        let slots_per_shard = TOTAL_SLOTS / shard_count;
        for shard_id in 0..shard_count {
            let slot_start = shard_id * slots_per_shard;
            let slot_end = if shard_id == shard_count - 1 {
                TOTAL_SLOTS
            } else {
                (shard_id + 1) * slots_per_shard
            };
            shard_metadata.insert(
                shard_id,
                ShardMetadata {
                    shard_id,
                    slot_start,
                    slot_end,
                    apply_index: None,
                },
            );
        }

        info!(
            "ShardedRocksDB opened at: {} with {} shards",
            path_str, shard_count
        );

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            shard_count,
            write_opts,
            shard_metadata: Arc::new(RwLock::new(shard_metadata)),
        })
    }

    /// Get shard count
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Get database path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Calculate shard ID for a key
    pub fn shard_for_key(&self, key: &[u8]) -> ShardId {
        let slot = slot_for_key(key);
        slot % self.shard_count
    }

    /// Get Column Family handle for shard
    fn get_cf(&self, shard_id: ShardId) -> Option<&ColumnFamily> {
        let cf_name = shard_cf_name(shard_id);
        self.db.cf_handle(&cf_name)
    }

    /// Get or create Column Family for shard
    fn get_or_create_cf(&self, shard_id: ShardId) -> Result<&ColumnFamily, String> {
        let cf_name = shard_cf_name(shard_id);
        if let Some(cf) = self.db.cf_handle(&cf_name) {
            Ok(cf)
        } else {
            Err(format!("Column Family {} not found", cf_name))
        }
    }

    // ==================== Shard Management ====================

    /// Set apply index for shard
    pub fn set_shard_apply_index(&self, shard_id: ShardId, apply_index: u64) {
        let mut metadata = self.shard_metadata.write();
        if let Some(meta) = metadata.get_mut(&shard_id) {
            meta.apply_index = Some(apply_index);
        }
    }

    /// Get apply index for shard
    pub fn get_shard_apply_index(&self, shard_id: ShardId) -> Option<u64> {
        let metadata = self.shard_metadata.read();
        metadata.get(&shard_id).and_then(|m| m.apply_index)
    }

    /// Get shard metadata
    pub fn get_shard_metadata(&self, shard_id: ShardId) -> Option<ShardMetadata> {
        let metadata = self.shard_metadata.read();
        metadata.get(&shard_id).cloned()
    }

    /// Get all active shards
    pub fn get_active_shards(&self) -> Vec<ShardId> {
        (0..self.shard_count).collect()
    }

    // ==================== String Operations ====================

    /// GET: Get string value from specific shard
    pub fn get(&self, shard_id: ShardId, key: &[u8]) -> Option<Vec<u8>> {
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
    pub fn set(&self, shard_id: ShardId, key: &[u8], value: Vec<u8>) -> Result<(), String> {
        let cf = self.get_or_create_cf(shard_id)?;
        let db_key = string_key(key);
        self.db
            .put_cf_opt(cf, &db_key, &value, &self.write_opts)
            .map_err(|e| format!("RocksDB SET error: {}", e))
    }

    /// SETNX: Set if not exists
    pub fn setnx(&self, shard_id: ShardId, key: &[u8], value: Vec<u8>) -> Result<bool, String> {
        let cf = self.get_or_create_cf(shard_id)?;
        let db_key = string_key(key);

        if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
            return Ok(false);
        }

        self.db
            .put_cf_opt(cf, &db_key, &value, &self.write_opts)
            .map_err(|e| format!("RocksDB SETNX error: {}", e))?;
        Ok(true)
    }

    /// DEL: Delete key from specific shard
    pub fn del(&self, shard_id: ShardId, key: &[u8]) -> bool {
        if let Some(cf) = self.get_cf(shard_id) {
            let db_key = string_key(key);
            if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                return self.db.delete_cf_opt(cf, &db_key, &self.write_opts).is_ok();
            }
        }
        false
    }

    /// INCR/INCRBY
    pub fn incrby(&self, shard_id: ShardId, key: &[u8], delta: i64) -> StoreResult<i64> {
        let cf = self.get_or_create_cf(shard_id).map_err(|e| StoreError::Internal(e))?;
        let db_key = string_key(key);

        let current = match self.db.get_cf(cf, &db_key) {
            Ok(Some(value)) => {
                let s = String::from_utf8_lossy(&value);
                s.parse::<i64>()
                    .map_err(|_| StoreError::InvalidArgument("value is not an integer".to_string()))?
            }
            Ok(None) => 0,
            Err(e) => return Err(StoreError::Internal(e.to_string())),
        };

        let new_value = current
            .checked_add(delta)
            .ok_or_else(|| StoreError::InvalidArgument("integer overflow".to_string()))?;

        self.db
            .put_cf_opt(cf, &db_key, new_value.to_string().as_bytes(), &self.write_opts)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(new_value)
    }

    /// APPEND
    pub fn append(&self, shard_id: ShardId, key: &[u8], value: &[u8]) -> usize {
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
            if self.db.put_cf_opt(cf, &db_key, &new_value, &self.write_opts).is_ok() {
                return len;
            }
        }
        0
    }

    /// STRLEN
    pub fn strlen(&self, shard_id: ShardId, key: &[u8]) -> usize {
        if let Some(cf) = self.get_cf(shard_id) {
            let db_key = string_key(key);
            if let Ok(Some(value)) = self.db.get_cf(cf, &db_key) {
                return value.len();
            }
        }
        0
    }

    // ==================== Hash Operations ====================

    /// HGET
    pub fn hget(&self, shard_id: ShardId, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let cf = self.get_cf(shard_id)?;
        let db_key = hash_field_key(key, field);
        self.db.get_cf(cf, &db_key).ok().flatten()
    }

    /// HSET
    pub fn hset(&self, shard_id: ShardId, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            let db_key = hash_field_key(key, &field);
            let is_new = self.db.get_cf(cf, &db_key).ok().flatten().is_none();

            if self.db.put_cf_opt(cf, &db_key, &value, &self.write_opts).is_ok() {
                if is_new {
                    self.hash_incr_field_count(shard_id, key, 1);
                }
                return is_new;
            }
        }
        false
    }

    /// HMSET
    pub fn hmset(&self, shard_id: ShardId, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>) {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            let mut batch = WriteBatch::default();
            let mut new_fields = 0;

            for (field, value) in fvs {
                let db_key = hash_field_key(key, &field);
                if self.db.get_cf(cf, &db_key).ok().flatten().is_none() {
                    new_fields += 1;
                }
                batch.put_cf(cf, &db_key, &value);
            }

            if self.db.write_opt(batch, &self.write_opts).is_ok() && new_fields > 0 {
                self.hash_incr_field_count(shard_id, key, new_fields);
            }
        }
    }

    /// HDEL
    pub fn hdel(&self, shard_id: ShardId, key: &[u8], fields: &[&[u8]]) -> usize {
        if let Ok(cf) = self.get_or_create_cf(shard_id) {
            let mut batch = WriteBatch::default();
            let mut deleted = 0;

            for field in fields {
                let db_key = hash_field_key(key, field);
                if self.db.get_cf(cf, &db_key).ok().flatten().is_some() {
                    batch.delete_cf(cf, &db_key);
                    deleted += 1;
                }
            }

            if deleted > 0 && self.db.write_opt(batch, &self.write_opts).is_ok() {
                self.hash_incr_field_count(shard_id, key, -(deleted as i64));
                return deleted;
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
        let cf = self.get_or_create_cf(shard_id).map_err(|e| StoreError::Internal(e))?;
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

        self.db
            .put_cf_opt(cf, &db_key, new_value.to_string().as_bytes(), &self.write_opts)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if is_new {
            self.hash_incr_field_count(shard_id, key, 1);
        }

        Ok(new_value)
    }

    /// Helper: Update hash field count
    fn hash_incr_field_count(&self, shard_id: ShardId, key: &[u8], delta: i64) {
        if let Some(cf) = self.get_cf(shard_id) {
            let meta_key = hash_meta_key(key);

            let current = self
                .db
                .get_cf(cf, &meta_key)
                .ok()
                .flatten()
                .and_then(|v| String::from_utf8_lossy(&v).parse::<i64>().ok())
                .unwrap_or(0);

            let new_count = (current + delta).max(0);
            if new_count == 0 {
                let _ = self.db.delete_cf_opt(cf, &meta_key, &self.write_opts);
            } else {
                let _ = self.db.put_cf_opt(
                    cf,
                    &meta_key,
                    new_count.to_string().as_bytes(),
                    &self.write_opts,
                );
            }
        }
    }

    // ==================== Snapshot Operations ====================

    /// Create snapshot for a specific shard
    pub fn create_shard_snapshot(&self, shard_id: ShardId) -> Result<Vec<u8>, String> {
        let cf = self
            .get_cf(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            match item {
                Ok((k, v)) => {
                    entries.push((k.to_vec(), v.to_vec()));
                }
                Err(e) => {
                    return Err(format!("Snapshot iteration error: {}", e));
                }
            }
        }

        bincode::serde::encode_to_vec(&entries, bincode::config::standard())
            .map_err(|e| format!("Snapshot serialization error: {}", e))
    }

    /// Restore shard from snapshot
    pub fn restore_shard_snapshot(&self, shard_id: ShardId, data: &[u8]) -> Result<(), String> {
        let cf = self
            .get_cf(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;

        let (entries, _): (Vec<(Vec<u8>, Vec<u8>)>, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Snapshot deserialization error: {}", e))?;

        // Clear existing shard data
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut batch = WriteBatch::default();
        for item in iter {
            if let Ok((k, _)) = item {
                batch.delete_cf(cf, &k);
            }
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| format!("Clear shard error: {}", e))?;

        // Restore data
        let mut batch = WriteBatch::default();
        for (k, v) in entries {
            batch.put_cf(cf, &k, &v);
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| format!("Restore shard error: {}", e))?;

        Ok(())
    }

    /// Flush to disk
    pub fn flush(&self) -> Result<(), String> {
        self.db.flush().map_err(|e| format!("Flush error: {}", e))
    }
}

impl Clone for ShardedRocksDB {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
            shard_count: self.shard_count,
            write_opts: {
                let mut opts = WriteOptions::default();
                opts.set_sync(false);
                opts
            },
            shard_metadata: Arc::clone(&self.shard_metadata),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_temp_db() -> ShardedRocksDB {
        let path = format!("/tmp/sharded_rocksdb_test_{}", rand::random::<u64>());
        ShardedRocksDB::new(&path, 4).unwrap()
    }

    fn cleanup_db(db: &ShardedRocksDB) {
        let path = db.path().to_string();
        drop(db);
        let _ = fs::remove_dir_all(&path);
    }

    #[test]
    fn test_string_operations() {
        let db = create_temp_db();

        // Test in shard 0
        db.set(0, b"key1", b"value1".to_vec()).unwrap();
        assert_eq!(db.get(0, b"key1"), Some(b"value1".to_vec()));

        // Test SETNX
        assert!(!db.setnx(0, b"key1", b"value2".to_vec()).unwrap());
        assert!(db.setnx(0, b"key2", b"value2".to_vec()).unwrap());

        // Test INCR
        db.set(0, b"counter", b"10".to_vec()).unwrap();
        assert_eq!(db.incrby(0, b"counter", 5).unwrap(), 15);

        cleanup_db(&db);
    }

    #[test]
    fn test_hash_operations() {
        let db = create_temp_db();

        // Test in shard 1
        assert!(db.hset(1, b"myhash", b"field1".to_vec(), b"value1".to_vec()));
        assert_eq!(db.hget(1, b"myhash", b"field1"), Some(b"value1".to_vec()));

        // Test HMSET
        db.hmset(
            1,
            b"myhash",
            vec![
                (b"field2".to_vec(), b"value2".to_vec()),
                (b"field3".to_vec(), b"value3".to_vec()),
            ],
        );

        assert_eq!(db.hlen(1, b"myhash"), 3);

        // Test HGETALL
        let all = db.hgetall(1, b"myhash");
        assert_eq!(all.len(), 3);

        cleanup_db(&db);
    }

    #[test]
    fn test_shard_snapshot() {
        let db = create_temp_db();

        // Add data to shard 2
        db.set(2, b"key1", b"value1".to_vec()).unwrap();
        db.hset(2, b"hash1", b"f1".to_vec(), b"v1".to_vec());

        // Create snapshot
        let snapshot = db.create_shard_snapshot(2).unwrap();

        // Clear and restore
        db.del(2, b"key1");
        assert!(db.get(2, b"key1").is_none());

        db.restore_shard_snapshot(2, &snapshot).unwrap();
        assert_eq!(db.get(2, b"key1"), Some(b"value1".to_vec()));

        cleanup_db(&db);
    }
}

