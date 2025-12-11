//! Sharded RocksDB Storage
//!
//! Uses RocksDB Column Families for native sharding support.
//! Each shard has its own Column Family for isolation and efficient snapshot.
//!
//! ## Architecture
//!
//! - **Key Encoding**: See `key_encoding.rs` for key encoding schemes
//! - **String Operations**: See `string.rs` for string operations
//! - **Hash Operations**: See `hash.rs` for hash operations
//! - **Snapshot Operations**: See `snapshot.rs` for snapshot operations

use crate::rocksdb::key_encoding::apply_index_key;
use crate::shard::{slot_for_key, ShardId, TOTAL_SLOTS};
use parking_lot::RwLock;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

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
    pub(crate) db: Arc<DB>,
    /// Database path
    path: String,
    /// Number of shards
    shard_count: u32,
    /// Write options
    pub(crate) write_opts: WriteOptions,
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
    pub(crate) fn get_cf(&self, shard_id: ShardId) -> Option<&ColumnFamily> {
        let cf_name = shard_cf_name(shard_id);
        self.db.cf_handle(&cf_name)
    }

    /// Get or create Column Family for shard
    pub(crate) fn get_or_create_cf(&self, shard_id: ShardId) -> Result<&ColumnFamily, String> {
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

    /// Get apply index for shard (from cache, fallback to DB)
    pub fn get_shard_apply_index(&self, shard_id: ShardId) -> Option<u64> {
        // Try cache first
        let metadata = self.shard_metadata.read();
        if let Some(index) = metadata.get(&shard_id).and_then(|m| m.apply_index) {
            return Some(index);
        }
        drop(metadata);

        // Fallback to DB
        self.get_apply_index_from_db(shard_id).ok().flatten()
    }

    /// Get apply_index from RocksDB (not from cache)
    fn get_apply_index_from_db(&self, shard_id: ShardId) -> Result<Option<u64>, String> {
        let cf = self
            .get_cf(shard_id)
            .ok_or_else(|| format!("Shard {} not found", shard_id))?;
        let index_key = apply_index_key();

        match self.db.get_cf(cf, &index_key) {
            Ok(Some(bytes)) => {
                if bytes.len() == 8 {
                    let index = u64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    Ok(Some(index))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("Failed to read apply_index: {}", e)),
        }
    }

    /// Helper: Check if apply_index should be skipped (idempotent check)
    pub(crate) fn should_skip_apply_index(
        &self,
        shard_id: ShardId,
        new_index: u64,
    ) -> Result<bool, String> {
        let current_index = self.get_apply_index_from_db(shard_id)?;
        if let Some(current) = current_index {
            if new_index <= current {
                return Ok(true); // Already applied, skip
            }
        }
        Ok(false)
    }

    /// Helper: Add apply_index to WriteBatch
    pub(crate) fn add_apply_index_to_batch(
        &self,
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        apply_index: u64,
    ) {
        let index_key = apply_index_key();
        batch.put_cf(cf, &index_key, apply_index.to_le_bytes().as_slice());
    }

    /// Atomically update apply_index in RocksDB (without data write)
    pub fn update_apply_index(&self, shard_id: ShardId, apply_index: u64) -> Result<(), String> {
        let cf = self.get_or_create_cf(shard_id)?;
        let index_key = apply_index_key();

        // Check for duplicate commit
        let current_index = self.get_apply_index_from_db(shard_id)?;
        if let Some(current) = current_index {
            if apply_index <= current {
                // Already applied, skip (idempotent)
                return Ok(());
            }
        }

        // Write apply_index
        self.db
            .put_cf_opt(
                cf,
                &index_key,
                apply_index.to_le_bytes().as_slice(),
                &self.write_opts,
            )
            .map_err(|e| format!("Failed to update apply_index: {}", e))?;

        // Update in-memory cache
        self.set_shard_apply_index(shard_id, apply_index);
        Ok(())
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
    use bytes::Bytes;

    use super::*;
    use std::fs;

    fn create_temp_db() -> ShardedRocksDB {
        let path = format!("/tmp/sharded_rocksdb_test_{}", rand::random::<u64>());
        ShardedRocksDB::new(&path, 4).unwrap()
    }

    fn cleanup_db(db: &ShardedRocksDB) {
        let path = db.path().to_string();
        let _ = db; // Keep reference alive
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
        assert!(db.hset(
            1,
            b"myhash",
            b"field1".as_ref(),
            Bytes::from(b"value1".to_vec())
        ));
        assert_eq!(db.hget(1, b"myhash", b"field1"), Some(b"value1".to_vec()));

        // Test HMSET
        db.hmset(
            1,
            b"myhash",
            vec![
                (b"field2".as_ref(), Bytes::from(b"value2".to_vec())),
                (b"field3".as_ref(), Bytes::from(b"value3".to_vec())),
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
        db.hset(2, b"hash1", b"f1".as_ref(), Bytes::from("v1"));

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
