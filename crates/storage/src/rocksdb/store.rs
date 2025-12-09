//! RocksDB Store implementation
//!
//! Implements String and Hash operations using RocksDB as the storage backend.

use super::{
    extract_hash_field, hash_field_key, hash_field_prefix, hash_meta_key, key_prefix, string_key,
};
use crate::memory::slot_for_key;
use crate::traits::{
    HashStore, KeyStore, ListStore, RedisStore, SetStore, SnapshotStore, StoreError, StoreResult,
    StringStore,
};
use rocksdb::{
    checkpoint::Checkpoint, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info};

/// RocksDB-based storage for String and Hash data types
pub struct RocksDBStore {
    /// RocksDB instance
    db: Arc<DB>,
    /// Database path
    path: String,
    /// Write options with sync disabled for performance
    write_opts: WriteOptions,
    /// Write options with sync enabled for durability
    sync_write_opts: WriteOptions,
}

impl RocksDBStore {
    /// Create a new RocksDBStore at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB SST files
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_background_jobs(4);

        // Enable compression
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = DB::open(&opts, &path_str).map_err(|e| format!("Failed to open RocksDB: {}", e))?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Async writes for performance

        let mut sync_write_opts = WriteOptions::default();
        sync_write_opts.set_sync(true); // Sync writes for durability

        info!("RocksDB opened at: {}", path_str);

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            write_opts,
            sync_write_opts,
        })
    }

    /// Get the database path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Flush WAL to disk
    pub fn flush(&self) -> Result<(), String> {
        self.db
            .flush()
            .map_err(|e| format!("Failed to flush: {}", e))
    }

    // ==================== String Operations ====================

    /// GET: Get string value
    fn string_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let db_key = string_key(key);
        match self.db.get(&db_key) {
            Ok(Some(value)) => Some(value),
            Ok(None) => None,
            Err(e) => {
                error!("RocksDB GET error: {}", e);
                None
            }
        }
    }

    /// SET: Set string value
    fn string_set(&self, key: &[u8], value: Vec<u8>) {
        let db_key = string_key(key);
        if let Err(e) = self.db.put_opt(&db_key, &value, &self.write_opts) {
            error!("RocksDB SET error: {}", e);
        }
    }

    /// SETNX: Set if not exists
    fn string_setnx(&self, key: &[u8], value: Vec<u8>) -> bool {
        let db_key = string_key(key);
        match self.db.get(&db_key) {
            Ok(Some(_)) => false, // Key exists
            Ok(None) => {
                if let Err(e) = self.db.put_opt(&db_key, &value, &self.write_opts) {
                    error!("RocksDB SETNX error: {}", e);
                    false
                } else {
                    true
                }
            }
            Err(e) => {
                error!("RocksDB SETNX check error: {}", e);
                false
            }
        }
    }

    /// DEL: Delete string key
    fn string_del(&self, key: &[u8]) -> bool {
        let db_key = string_key(key);
        match self.db.get(&db_key) {
            Ok(Some(_)) => {
                if let Err(e) = self.db.delete_opt(&db_key, &self.write_opts) {
                    error!("RocksDB DEL error: {}", e);
                    false
                } else {
                    true
                }
            }
            Ok(None) => false,
            Err(e) => {
                error!("RocksDB DEL check error: {}", e);
                false
            }
        }
    }

    /// INCR/INCRBY: Increment integer value
    fn string_incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        let db_key = string_key(key);

        let current = match self.db.get(&db_key) {
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

        let value_bytes = new_value.to_string().into_bytes();
        self.db
            .put_opt(&db_key, &value_bytes, &self.write_opts)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(new_value)
    }

    /// APPEND: Append to string value
    fn string_append(&self, key: &[u8], value: &[u8]) -> usize {
        let db_key = string_key(key);

        let new_value = match self.db.get(&db_key) {
            Ok(Some(mut existing)) => {
                existing.extend_from_slice(value);
                existing
            }
            Ok(None) => value.to_vec(),
            Err(e) => {
                error!("RocksDB APPEND error: {}", e);
                return 0;
            }
        };

        let len = new_value.len();
        if let Err(e) = self.db.put_opt(&db_key, &new_value, &self.write_opts) {
            error!("RocksDB APPEND write error: {}", e);
            return 0;
        }

        len
    }

    /// STRLEN: Get string length
    fn string_strlen(&self, key: &[u8]) -> usize {
        let db_key = string_key(key);
        match self.db.get(&db_key) {
            Ok(Some(value)) => value.len(),
            Ok(None) => 0,
            Err(e) => {
                error!("RocksDB STRLEN error: {}", e);
                0
            }
        }
    }

    /// GETSET: Set and return old value
    fn string_getset(&self, key: &[u8], value: Vec<u8>) -> Option<Vec<u8>> {
        let old = self.string_get(key);
        self.string_set(key, value);
        old
    }

    // ==================== Hash Operations ====================

    /// HGET: Get hash field value
    fn hash_get(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        let db_key = hash_field_key(key, field);
        match self.db.get(&db_key) {
            Ok(Some(value)) => Some(value),
            Ok(None) => None,
            Err(e) => {
                error!("RocksDB HGET error: {}", e);
                None
            }
        }
    }

    /// HSET: Set hash field value
    fn hash_set(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        let db_key = hash_field_key(key, &field);

        // Check if field exists
        let is_new = match self.db.get(&db_key) {
            Ok(Some(_)) => false,
            Ok(None) => true,
            Err(e) => {
                error!("RocksDB HSET check error: {}", e);
                true
            }
        };

        // Write the field
        if let Err(e) = self.db.put_opt(&db_key, &value, &self.write_opts) {
            error!("RocksDB HSET error: {}", e);
            return false;
        }

        // Update field count if new field
        if is_new {
            self.hash_incr_field_count(key, 1);
        }

        is_new
    }

    /// HMSET: Set multiple hash fields
    fn hash_mset(&self, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut batch = WriteBatch::default();
        let mut new_fields = 0;

        for (field, value) in fvs {
            let db_key = hash_field_key(key, &field);

            // Check if field exists
            if let Ok(None) = self.db.get(&db_key) {
                new_fields += 1;
            }

            batch.put(&db_key, &value);
        }

        if let Err(e) = self.db.write_opt(batch, &self.write_opts) {
            error!("RocksDB HMSET error: {}", e);
            return;
        }

        if new_fields > 0 {
            self.hash_incr_field_count(key, new_fields);
        }
    }

    /// HDEL: Delete hash fields
    fn hash_del(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        let mut batch = WriteBatch::default();
        let mut deleted = 0;

        for field in fields {
            let db_key = hash_field_key(key, field);
            if let Ok(Some(_)) = self.db.get(&db_key) {
                batch.delete(&db_key);
                deleted += 1;
            }
        }

        if deleted > 0 {
            if let Err(e) = self.db.write_opt(batch, &self.write_opts) {
                error!("RocksDB HDEL error: {}", e);
                return 0;
            }
            self.hash_incr_field_count(key, -(deleted as i64));
        }

        deleted
    }

    /// HEXISTS: Check if field exists
    fn hash_exists(&self, key: &[u8], field: &[u8]) -> bool {
        let db_key = hash_field_key(key, field);
        matches!(self.db.get(&db_key), Ok(Some(_)))
    }

    /// HGETALL: Get all fields and values
    fn hash_getall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = hash_field_prefix(key);
        let mut result = Vec::new();

        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            match item {
                Ok((k, v)) => {
                    // Check if key still has the prefix
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    // Extract field from key
                    if let Some(field) = extract_hash_field(&k, key) {
                        result.push((field.to_vec(), v.to_vec()));
                    }
                }
                Err(e) => {
                    error!("RocksDB HGETALL iteration error: {}", e);
                    break;
                }
            }
        }

        result
    }

    /// HKEYS: Get all field names
    fn hash_keys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let prefix = hash_field_prefix(key);
        let mut result = Vec::new();

        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            match item {
                Ok((k, _)) => {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    if let Some(field) = extract_hash_field(&k, key) {
                        result.push(field.to_vec());
                    }
                }
                Err(e) => {
                    error!("RocksDB HKEYS iteration error: {}", e);
                    break;
                }
            }
        }

        result
    }

    /// HVALS: Get all values
    fn hash_vals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let prefix = hash_field_prefix(key);
        let mut result = Vec::new();

        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            match item {
                Ok((k, v)) => {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    result.push(v.to_vec());
                }
                Err(e) => {
                    error!("RocksDB HVALS iteration error: {}", e);
                    break;
                }
            }
        }

        result
    }

    /// HLEN: Get number of fields
    fn hash_len(&self, key: &[u8]) -> usize {
        let meta_key = hash_meta_key(key);
        match self.db.get(&meta_key) {
            Ok(Some(value)) => {
                let s = String::from_utf8_lossy(&value);
                s.parse::<usize>().unwrap_or(0)
            }
            Ok(None) => 0,
            Err(e) => {
                error!("RocksDB HLEN error: {}", e);
                0
            }
        }
    }

    /// HINCRBY: Increment hash field by integer
    fn hash_incrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        let db_key = hash_field_key(key, field);

        let (current, is_new) = match self.db.get(&db_key) {
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

        let value_bytes = new_value.to_string().into_bytes();
        self.db
            .put_opt(&db_key, &value_bytes, &self.write_opts)
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if is_new {
            self.hash_incr_field_count(key, 1);
        }

        Ok(new_value)
    }

    /// Helper: Increment/decrement hash field count in metadata
    fn hash_incr_field_count(&self, key: &[u8], delta: i64) {
        let meta_key = hash_meta_key(key);

        let current = match self.db.get(&meta_key) {
            Ok(Some(value)) => {
                let s = String::from_utf8_lossy(&value);
                s.parse::<i64>().unwrap_or(0)
            }
            Ok(None) => 0,
            Err(_) => 0,
        };

        let new_count = (current + delta).max(0);
        if new_count == 0 {
            // Delete metadata if no fields
            let _ = self.db.delete_opt(&meta_key, &self.write_opts);
        } else {
            let _ = self
                .db
                .put_opt(&meta_key, new_count.to_string().as_bytes(), &self.write_opts);
        }
    }

    /// Delete entire hash (all fields)
    fn hash_delete_all(&self, key: &[u8]) -> bool {
        let prefix = hash_field_prefix(key);
        let mut batch = WriteBatch::default();
        let mut found = false;

        let iter = self.db.prefix_iterator(&prefix);
        for item in iter {
            match item {
                Ok((k, _)) => {
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    batch.delete(&k);
                    found = true;
                }
                Err(_) => break,
            }
        }

        // Delete metadata
        let meta_key = hash_meta_key(key);
        batch.delete(&meta_key);

        if found {
            if let Err(e) = self.db.write_opt(batch, &self.write_opts) {
                error!("RocksDB hash delete error: {}", e);
                return false;
            }
        }

        found
    }

    // ==================== Snapshot Operations ====================

    /// Create a checkpoint (snapshot)
    pub fn create_checkpoint(&self, checkpoint_path: &str) -> Result<(), String> {
        let checkpoint = Checkpoint::new(&self.db)
            .map_err(|e| format!("Failed to create checkpoint: {}", e))?;

        checkpoint
            .create_checkpoint(checkpoint_path)
            .map_err(|e| format!("Failed to save checkpoint: {}", e))?;

        info!("Created checkpoint at: {}", checkpoint_path);
        Ok(())
    }

    /// Create snapshot data (serialized)
    fn create_snapshot_data(&self) -> Result<Vec<u8>, String> {
        let mut entries = Vec::new();

        // Iterate all keys
        let iter = self.db.iterator(IteratorMode::Start);
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

        // Serialize
        bincode::serde::encode_to_vec(&entries, bincode::config::standard())
            .map_err(|e| format!("Snapshot serialization error: {}", e))
    }

    /// Restore from snapshot data
    fn restore_snapshot_data(&self, data: &[u8]) -> Result<(), String> {
        let (entries, _): (Vec<(Vec<u8>, Vec<u8>)>, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Snapshot deserialization error: {}", e))?;

        // Clear existing data
        let iter = self.db.iterator(IteratorMode::Start);
        let mut batch = WriteBatch::default();
        for item in iter {
            if let Ok((k, _)) = item {
                batch.delete(&k);
            }
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| format!("Clear error: {}", e))?;

        // Restore data
        let mut batch = WriteBatch::default();
        for (k, v) in entries {
            batch.put(&k, &v);
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| format!("Restore error: {}", e))?;

        Ok(())
    }

    /// Create split snapshot (slot range)
    fn create_split_snapshot_data(
        &self,
        slot_start: u32,
        slot_end: u32,
        _total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        let mut entries = Vec::new();

        // Iterate all keys and filter by slot
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            match item {
                Ok((k, v)) => {
                    // Extract original key for slot calculation
                    if let Some(original_key) = self.extract_original_key(&k) {
                        let slot = slot_for_key(original_key);
                        if slot >= slot_start && slot < slot_end {
                            entries.push((k.to_vec(), v.to_vec()));
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Split snapshot iteration error: {}", e));
                }
            }
        }

        bincode::serde::encode_to_vec(&entries, bincode::config::standard())
            .map_err(|e| format!("Split snapshot serialization error: {}", e))
    }

    /// Merge snapshot data (does not clear existing data)
    fn merge_snapshot_data(&self, data: &[u8]) -> Result<usize, String> {
        let (entries, _): (Vec<(Vec<u8>, Vec<u8>)>, _) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| format!("Merge snapshot deserialization error: {}", e))?;

        let count = entries.len();
        let mut batch = WriteBatch::default();
        for (k, v) in entries {
            batch.put(&k, &v);
        }
        self.db
            .write_opt(batch, &self.write_opts)
            .map_err(|e| format!("Merge error: {}", e))?;

        Ok(count)
    }

    /// Delete keys in slot range
    fn delete_slot_range(
        &self,
        slot_start: u32,
        slot_end: u32,
        _total_slots: u32,
    ) -> usize {
        let mut batch = WriteBatch::default();
        let mut deleted = 0;

        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            if let Ok((k, _)) = item {
                if let Some(original_key) = self.extract_original_key(&k) {
                    let slot = slot_for_key(original_key);
                    if slot >= slot_start && slot < slot_end {
                        batch.delete(&k);
                        deleted += 1;
                    }
                }
            }
        }

        if deleted > 0 {
            if let Err(e) = self.db.write_opt(batch, &self.write_opts) {
                error!("Delete slot range error: {}", e);
                return 0;
            }
        }

        deleted
    }

    /// Extract original key from encoded key (for slot calculation)
    fn extract_original_key<'a>(&self, encoded: &'a [u8]) -> Option<&'a [u8]> {
        if encoded.len() < 2 || encoded[1] != b':' {
            return None;
        }

        match encoded[0] {
            key_prefix::STRING => Some(&encoded[2..]),
            key_prefix::HASH => {
                // h:{key}:{field} - extract key part
                let rest = &encoded[2..];
                if let Some(pos) = rest.iter().position(|&b| b == b':') {
                    Some(&rest[..pos])
                } else {
                    None
                }
            }
            key_prefix::HASH_META => Some(&encoded[2..]),
            _ => None,
        }
    }

    // ==================== Generic Operations ====================

    /// Check key type
    fn get_key_type(&self, key: &[u8]) -> Option<&'static str> {
        // Check if string exists
        let string_key = string_key(key);
        if self.db.get(&string_key).ok().flatten().is_some() {
            return Some("string");
        }

        // Check if hash exists (has metadata or fields)
        let meta_key = hash_meta_key(key);
        if self.db.get(&meta_key).ok().flatten().is_some() {
            return Some("hash");
        }

        // Check if hash has any fields
        let prefix = hash_field_prefix(key);
        let mut iter = self.db.prefix_iterator(&prefix);
        if iter.next().is_some() {
            return Some("hash");
        }

        None
    }

    /// Check if key exists
    fn key_exists(&self, key: &[u8]) -> bool {
        self.get_key_type(key).is_some()
    }

    /// Delete key (any type)
    fn delete_key(&self, key: &[u8]) -> bool {
        let mut deleted = false;

        // Try to delete as string
        if self.string_del(key) {
            deleted = true;
        }

        // Try to delete as hash
        if self.hash_delete_all(key) {
            deleted = true;
        }

        deleted
    }

    /// Get all keys matching pattern
    fn get_keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let pattern_str = String::from_utf8_lossy(pattern);
        let mut result = std::collections::HashSet::new();

        // Iterate all string keys
        let iter = self.db.prefix_iterator(&[key_prefix::STRING, b':']);
        for item in iter {
            if let Ok((k, _)) = item {
                if k.len() > 2 && k[0] == key_prefix::STRING && k[1] == b':' {
                    let key = &k[2..];
                    if self.matches_pattern(&pattern_str, key) {
                        result.insert(key.to_vec());
                    }
                } else {
                    break;
                }
            }
        }

        // Iterate all hash metadata keys
        let iter = self.db.prefix_iterator(&[key_prefix::HASH_META, b':']);
        for item in iter {
            if let Ok((k, _)) = item {
                if k.len() > 2 && k[0] == key_prefix::HASH_META && k[1] == b':' {
                    let key = &k[2..];
                    if self.matches_pattern(&pattern_str, key) {
                        result.insert(key.to_vec());
                    }
                } else {
                    break;
                }
            }
        }

        result.into_iter().collect()
    }

    /// Simple pattern matching (supports * wildcard)
    fn matches_pattern(&self, pattern: &str, key: &[u8]) -> bool {
        let key_str = String::from_utf8_lossy(key);

        if pattern == "*" {
            return true;
        }

        if pattern.starts_with('*') && pattern.ends_with('*') {
            let inner = &pattern[1..pattern.len() - 1];
            return key_str.contains(inner);
        }

        if pattern.starts_with('*') {
            let suffix = &pattern[1..];
            return key_str.ends_with(suffix);
        }

        if pattern.ends_with('*') {
            let prefix = &pattern[..pattern.len() - 1];
            return key_str.starts_with(prefix);
        }

        key_str == pattern
    }

    /// Get database size (number of keys)
    fn db_size(&self) -> usize {
        let mut count = 0;
        let iter = self.db.iterator(IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        count
    }

    /// Clear all data
    fn flush_db(&self) {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut batch = WriteBatch::default();
        for item in iter {
            if let Ok((k, _)) = item {
                batch.delete(&k);
            }
        }
        let _ = self.db.write_opt(batch, &self.write_opts);
    }
}

// ============================================================================
// StringStore Implementation
// ============================================================================

impl StringStore for RocksDBStore {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.string_get(key)
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        self.string_set(&key, value);
    }

    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        self.string_setnx(&key, value)
    }

    fn setex(&self, key: Vec<u8>, value: Vec<u8>, _ttl_secs: u64) {
        // TODO: Implement TTL support
        self.string_set(&key, value);
    }

    fn mset(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
        let mut batch = WriteBatch::default();
        for (key, value) in kvs {
            let db_key = string_key(&key);
            batch.put(&db_key, &value);
        }
        if let Err(e) = self.db.write_opt(batch, &self.write_opts) {
            error!("RocksDB MSET error: {}", e);
        }
    }

    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        self.string_incrby(key, delta)
    }

    fn append(&self, key: &[u8], value: &[u8]) -> usize {
        self.string_append(key, value)
    }

    fn strlen(&self, key: &[u8]) -> usize {
        self.string_strlen(key)
    }
}

// ============================================================================
// ListStore Implementation (Stub - Not Supported)
// ============================================================================

impl ListStore for RocksDBStore {
    fn lpush(&self, _key: &[u8], _values: Vec<Vec<u8>>) -> usize {
        0 // Not supported
    }

    fn rpush(&self, _key: &[u8], _values: Vec<Vec<u8>>) -> usize {
        0 // Not supported
    }

    fn lpop(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None // Not supported
    }

    fn rpop(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None // Not supported
    }

    fn lrange(&self, _key: &[u8], _start: i64, _stop: i64) -> Vec<Vec<u8>> {
        Vec::new() // Not supported
    }

    fn llen(&self, _key: &[u8]) -> usize {
        0 // Not supported
    }

    fn lindex(&self, _key: &[u8], _index: i64) -> Option<Vec<u8>> {
        None // Not supported
    }

    fn lset(&self, _key: &[u8], _index: i64, _value: Vec<u8>) -> StoreResult<()> {
        Err(StoreError::NotSupported)
    }
}

// ============================================================================
// HashStore Implementation
// ============================================================================

impl HashStore for RocksDBStore {
    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        self.hash_get(key, field)
    }

    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        self.hash_set(key, field, value)
    }

    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize {
        self.hash_del(key, fields)
    }

    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.hash_getall(key)
    }

    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        self.hash_keys(key)
    }

    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        self.hash_vals(key)
    }

    fn hlen(&self, key: &[u8]) -> usize {
        self.hash_len(key)
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64> {
        self.hash_incrby(key, field, delta)
    }
}

// ============================================================================
// SetStore Implementation (Stub - Not Supported)
// ============================================================================

impl SetStore for RocksDBStore {
    fn sadd(&self, _key: &[u8], _members: Vec<Vec<u8>>) -> usize {
        0 // Not supported
    }

    fn srem(&self, _key: &[u8], _members: &[&[u8]]) -> usize {
        0 // Not supported
    }

    fn smembers(&self, _key: &[u8]) -> Vec<Vec<u8>> {
        Vec::new() // Not supported
    }

    fn sismember(&self, _key: &[u8], _member: &[u8]) -> bool {
        false // Not supported
    }

    fn scard(&self, _key: &[u8]) -> usize {
        0 // Not supported
    }
}

// ============================================================================
// KeyStore Implementation
// ============================================================================

impl KeyStore for RocksDBStore {
    fn del(&self, keys: &[&[u8]]) -> usize {
        let mut deleted = 0;
        for key in keys {
            if self.delete_key(key) {
                deleted += 1;
            }
        }
        deleted
    }

    fn exists(&self, keys: &[&[u8]]) -> usize {
        keys.iter().filter(|k| self.key_exists(k)).count()
    }

    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        self.get_keys(pattern)
    }

    fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        self.get_key_type(key)
    }

    fn ttl(&self, _key: &[u8]) -> i64 {
        -1 // TTL not supported yet
    }

    fn expire(&self, _key: &[u8], _ttl_secs: u64) -> bool {
        false // TTL not supported yet
    }

    fn persist(&self, _key: &[u8]) -> bool {
        false // TTL not supported yet
    }

    fn dbsize(&self) -> usize {
        self.db_size()
    }

    fn flushdb(&self) {
        self.flush_db();
    }

    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()> {
        // Get old value
        if let Some(value) = self.string_get(key) {
            self.string_del(key);
            self.string_set(&new_key, value);
            return Ok(());
        }

        // Try hash rename
        let old_fields = self.hash_getall(key);
        if !old_fields.is_empty() {
            self.hash_delete_all(key);
            self.hash_mset(&new_key, old_fields);
            return Ok(());
        }

        Err(StoreError::KeyNotFound)
    }
}

// ============================================================================
// SnapshotStore Implementation
// ============================================================================

impl SnapshotStore for RocksDBStore {
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        self.create_snapshot_data()
    }

    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        self.restore_snapshot_data(snapshot)
    }

    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        self.create_split_snapshot_data(slot_start, slot_end, total_slots)
    }

    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String> {
        self.merge_snapshot_data(snapshot)
    }

    fn delete_keys_in_slot_range(&self, slot_start: u32, slot_end: u32, total_slots: u32) -> usize {
        self.delete_slot_range(slot_start, slot_end, total_slots)
    }
}

// ============================================================================
// RedisStore Implementation (Combines All)
// ============================================================================

impl RedisStore for RocksDBStore {}

impl Clone for RocksDBStore {
    fn clone(&self) -> Self {
        // Note: This creates a new reference to the same DB
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
            write_opts: {
                let mut opts = WriteOptions::default();
                opts.set_sync(false);
                opts
            },
            sync_write_opts: {
                let mut opts = WriteOptions::default();
                opts.set_sync(true);
                opts
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_temp_store() -> RocksDBStore {
        let path = format!("/tmp/rocksdb_test_{}", rand::random::<u64>());
        RocksDBStore::new(&path).unwrap()
    }

    fn cleanup_store(store: &RocksDBStore) {
        let path = store.path().to_string();
        drop(store);
        let _ = fs::remove_dir_all(&path);
    }

    #[test]
    fn test_string_operations() {
        let store = create_temp_store();

        // SET and GET
        store.set(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));

        // SETNX
        assert!(!store.setnx(b"key1".to_vec(), b"value2".to_vec()));
        assert!(store.setnx(b"key2".to_vec(), b"value2".to_vec()));

        // INCR
        store.set(b"counter".to_vec(), b"10".to_vec());
        assert_eq!(store.incr(b"counter").unwrap(), 11);
        assert_eq!(store.incrby(b"counter", 5).unwrap(), 16);

        // APPEND
        store.set(b"str".to_vec(), b"hello".to_vec());
        assert_eq!(store.append(b"str", b" world"), 11);
        assert_eq!(store.get(b"str"), Some(b"hello world".to_vec()));

        // DEL
        assert_eq!(store.del(&[b"key1", b"key2", b"nonexistent"]), 2);

        cleanup_store(&store);
    }

    #[test]
    fn test_hash_operations() {
        let store = create_temp_store();

        // HSET and HGET
        assert!(store.hset(b"myhash", b"field1".to_vec(), b"value1".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field1"), Some(b"value1".to_vec()));

        // HMSET
        store.hmset(
            b"myhash",
            vec![
                (b"field2".to_vec(), b"value2".to_vec()),
                (b"field3".to_vec(), b"value3".to_vec()),
            ],
        );

        // HGETALL
        let all = store.hgetall(b"myhash");
        assert_eq!(all.len(), 3);

        // HKEYS and HVALS
        let keys = store.hkeys(b"myhash");
        assert_eq!(keys.len(), 3);
        let vals = store.hvals(b"myhash");
        assert_eq!(vals.len(), 3);

        // HLEN
        assert_eq!(store.hlen(b"myhash"), 3);

        // HDEL
        assert_eq!(store.hdel(b"myhash", &[b"field1", b"field2"]), 2);
        assert_eq!(store.hlen(b"myhash"), 1);

        // HINCRBY
        store.hset(b"counters", b"count".to_vec(), b"10".to_vec());
        assert_eq!(store.hincrby(b"counters", b"count", 5).unwrap(), 15);

        cleanup_store(&store);
    }

    #[test]
    fn test_snapshot() {
        let store = create_temp_store();

        // Add some data
        store.set(b"key1".to_vec(), b"value1".to_vec());
        store.hset(b"hash1", b"field1".to_vec(), b"hvalue1".to_vec());

        // Create snapshot
        let snapshot = store.create_snapshot().unwrap();

        // Clear data
        store.flushdb();
        assert!(store.get(b"key1").is_none());

        // Restore snapshot
        store.restore_from_snapshot(&snapshot).unwrap();
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(
            store.hget(b"hash1", b"field1"),
            Some(b"hvalue1".to_vec())
        );

        cleanup_store(&store);
    }
}

