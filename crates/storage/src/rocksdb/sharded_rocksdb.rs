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
use anyhow::Result;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use rr_core::routing::RoutingTable;
use rr_core::shard::{ShardId, ShardRouting, TOTAL_SLOTS};
use std::path::Path;
use std::sync::Arc;

/// Shard metadata stored in RocksDB
///
/// This is a type alias for ShardRouting for backward compatibility.
/// The routing information is now managed by RoutingTable.
pub type ShardMetadata = ShardRouting;

/// Column Family name for a shard
fn shard_cf_name(shard_id: &ShardId) -> String {
    format!("shard_{}", shard_id)
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
    /// Write options
    pub(crate) write_opts: WriteOptions,
    /// Routing table for shard management
    routing_table: Arc<RoutingTable>,
}

impl ShardedRocksDB {
    /// Create a new ShardedRocksDB
    ///
    /// # Arguments
    /// - `path`: Database path
    /// - `routing_table`: Routing table for shard management (managed by node)
    pub fn new<P: AsRef<Path>>(path: P, routing_table: Arc<RoutingTable>) -> Result<Self, String> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_background_jobs(4);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // List existing column families
        let existing_cfs = DB::list_cf(&opts, &path_str).unwrap_or_default();

        // Open database with column families
        let db = if existing_cfs.is_empty() {
            // First time open, create the database with default column family
            DB::open(&opts, &path_str).map_err(|e| format!("Failed to open RocksDB: {}", e))?
        } else {
            // Open with existing column families
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = existing_cfs
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect();
            DB::open_cf_descriptors(&opts, &path_str, cf_descriptors)
                .map_err(|e| format!("Failed to open RocksDB with column families: {}", e))?
        };

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        // Auto-register shard routings from existing column families
        // Column families are named as "shard_{shard_id}"
        for cf_name in existing_cfs {
            if cf_name.starts_with("shard_") {
                let shard_id = cf_name.strip_prefix("shard_").unwrap().to_string();
                // Check if routing already exists (to avoid duplicates)
                if routing_table.get_shard_routing(&shard_id).is_none() {
                    // Try to infer slot range from shard_id (for now, use a default range)
                    // In production, this should be loaded from metadata or pilot
                    let slot_start = 0;
                    let slot_end = TOTAL_SLOTS;
                    routing_table
                        .add_shard_routing(ShardRouting::new(shard_id, slot_start, slot_end));
                }
            }
        }

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            write_opts,
            routing_table,
        })
    }

    /// Get shard count
    pub fn shard_count(&self) -> u32 {
        self.routing_table.list_shard_routings().len() as u32
    }

    /// Get database path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Calculate shard ID for a key
    pub fn shard_for_key(&self, key: &[u8]) -> Result<ShardId> {
        self.routing_table
            .find_shard_for_key(key)
            .map_err(|e| anyhow::anyhow!("Shard not found: {}", e))
    }

    /// Get Column Family handle for shard
    pub(crate) fn get_cf(&self, shard_id: &ShardId) -> Option<&ColumnFamily> {
        let cf_name = shard_cf_name(shard_id);
        self.db.cf_handle(&cf_name)
    }

    /// Get apply index for shard (from DB)
    ///
    /// Note: apply_index is no longer stored in ShardRouting cache.
    /// This method reads directly from RocksDB.
    pub fn get_shard_apply_index(&self, shard_id: &ShardId) -> Option<u64> {
        // Read directly from DB
        self.get_apply_index_from_db(shard_id).ok().flatten()
    }

    /// Get apply_index from RocksDB (not from cache)
    fn get_apply_index_from_db(&self, shard_id: &ShardId) -> Result<Option<u64>, String> {
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
        shard_id: &ShardId,
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
    pub fn update_apply_index(&self, shard_id: &ShardId, apply_index: u64) -> Result<(), String> {
        let cf = self
            .get_cf(shard_id)
            .ok_or_else(|| format!("Column Family not found for shard {}", shard_id))?;
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

        Ok(())
    }

    /// Flush to disk
    pub fn flush(&self) -> Result<(), String> {
        self.db.flush().map_err(|e| format!("Flush error: {}", e))
    }
    /// Get shard metadata
    pub fn get_shard_metadata(&self, shard_id: &ShardId) -> Option<ShardMetadata> {
        self.routing_table.get_shard_routing(shard_id)
    }

    /// Get all active shards
    pub fn get_active_shards(&self) -> Vec<ShardId> {
        self.routing_table
            .list_shard_routings()
            .into_iter()
            .map(|r| r.shard_id)
            .collect()
    }

    /// Add or update shard routing
    pub fn add_shard_routing(&self, routing: ShardRouting) {
        self.routing_table.add_shard_routing(routing);
    }

    /// Remove shard routing
    pub fn remove_shard_routing(&self, shard_id: &ShardId) {
        self.routing_table.remove_shard_routing(shard_id);
    }

    /// Get reference to routing table
    pub fn routing_table(&self) -> &Arc<RoutingTable> {
        &self.routing_table
    }
}

impl Clone for ShardedRocksDB {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
            write_opts: {
                let mut opts = WriteOptions::default();
                opts.set_sync(false);
                opts
            },
            routing_table: Arc::clone(&self.routing_table),
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

        // Create database with test Column Families
        // Note: Column Family names use shard_cf_name format: "shard_{shard_id}"
        let path_str = path.clone();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Create test Column Families with correct naming
        let test_cfs = vec![
            ColumnFamilyDescriptor::new("shard_shard_0", Options::default()),
            ColumnFamilyDescriptor::new("shard_shard_1", Options::default()),
            ColumnFamilyDescriptor::new("shard_shard_2", Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, &path_str, test_cfs)
            .expect("Failed to create test database");

        // Create ShardedRocksDB manually with the pre-created DB
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        let routing_table = Arc::new(RoutingTable::new());
        routing_table.add_shard_routing(ShardRouting::new("shard_0".to_string(), 0, 4096));
        routing_table.add_shard_routing(ShardRouting::new("shard_1".to_string(), 4096, 8192));
        routing_table.add_shard_routing(ShardRouting::new("shard_2".to_string(), 8192, 12288));

        ShardedRocksDB {
            db: Arc::new(db),
            path: path_str,
            write_opts,
            routing_table,
        }
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
        db.set(&"shard_0".into(), b"key1", b"value1".to_vec())
            .unwrap();
        assert_eq!(db.get(&"shard_0".into(), b"key1"), Some(b"value1".to_vec()));

        // Test SETNX
        assert!(!db
            .setnx(&"shard_0".into(), b"key1", b"value2".to_vec())
            .unwrap());
        assert!(db
            .setnx(&"shard_0".into(), b"key2", b"value2".to_vec())
            .unwrap());

        // Test INCR
        db.set(&"shard_0".into(), b"counter", b"10".to_vec())
            .unwrap();
        assert_eq!(db.incrby(&"shard_0".into(), b"counter", 5).unwrap(), 15);

        cleanup_db(&db);
    }

    #[test]
    fn test_hash_operations() {
        let db = create_temp_db();

        // Test in shard 1
        assert!(db.hset(
            &"shard_1".into(),
            b"myhash",
            b"field1".as_ref(),
            Bytes::from(b"value1".to_vec())
        ));
        assert_eq!(
            db.hget(&"shard_1".into(), b"myhash", b"field1"),
            Some(b"value1".to_vec())
        );

        // Test HMSET
        db.hmset(
            &"shard_1".into(),
            b"myhash",
            vec![
                (b"field2".as_ref(), Bytes::from(b"value2".to_vec())),
                (b"field3".as_ref(), Bytes::from(b"value3".to_vec())),
            ],
        );

        assert_eq!(db.hlen(&"shard_1".into(), b"myhash"), 3);

        // Test HGETALL
        let all = db.hgetall(&"shard_1".into(), b"myhash");
        assert_eq!(all.len(), 3);

        cleanup_db(&db);
    }
}
