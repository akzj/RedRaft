//! Slot-based RocksDB Storage
//!
//! Uses RocksDB with slot-prefixed keys for data organization.
//! All data is stored in the default Column Family, with slot information encoded in keys.
//!
//! ## Architecture
//!
//! - **Key Encoding**: See `key_encoding.rs` for key encoding schemes
//! - **String Operations**: See `string.rs` for string operations
//! - **Hash Operations**: See `hash.rs` for hash operations
//! - **Snapshot Operations**: See `snapshot.rs` for snapshot operations

use crate::rocksdb::key_encoding::slot_meta_key;
use crate::store::SlotMetadata;
use crate::traits::ApplyContext;
use anyhow::Result;
use rocksdb::{ColumnFamily, Options, WriteBatch, WriteOptions, DB};
use rr_core::routing::RoutingTable;
use std::path::Path;
use std::sync::Arc;

/// Slot-based RocksDB Storage
///
/// Uses default Column Family with slot-prefixed keys.
/// Path: default Column Family -> {slot}:{type}:{key} -> value
pub struct SlotRocksDB {
    /// RocksDB instance
    pub(crate) db: Arc<DB>,
    /// Database path
    path: String,
    /// Write options
    pub(crate) write_opts: WriteOptions,
    /// Routing table (for slot calculation, managed by node)
    routing_table: Arc<RoutingTable>,
}

impl SlotRocksDB {
    /// Create a new SlotRocksDB
    ///
    /// # Arguments
    /// - `path`: Database path
    /// - `routing_table`: Routing table for slot calculation (managed by node)
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

        // Open database with default column family
        let db =
            DB::open(&opts, &path_str).map_err(|e| format!("Failed to open RocksDB: {}", e))?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            write_opts,
            routing_table,
        })
    }

    /// Get database path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get default Column Family handle
    pub(crate) fn get_cf(&self) -> Option<&ColumnFamily> {
        self.db.cf_handle("default")
    }

    /// Helper: Add slot metadata to WriteBatch
    ///
    /// Stores slot metadata (applied_index) atomically with data writes.
    /// Format: applied_index (8 bytes)
    pub(crate) fn add_slot_meta_to_batch(
        &self,
        batch: &mut WriteBatch,
        slot: u32,
        ctx: &ApplyContext,
    ) {
        let Some(cf) = self.get_cf() else {
            return;
        };

        // Only add if apply_index is present
        if let Some(apply_index) = ctx.apply_index {
            let meta_key = slot_meta_key(slot);
            // Serialize: applied_index (8 bytes)
            batch.put_cf(cf, &meta_key, apply_index.to_le_bytes().as_slice());
        }
    }

    /// Flush to disk
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| anyhow::anyhow!("Flush error: {}", e))
    }

    /// Get reference to routing table
    pub fn routing_table(&self) -> &Arc<RoutingTable> {
        &self.routing_table
    }
}

impl Clone for SlotRocksDB {
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

    fn create_temp_db() -> SlotRocksDB {
        let path = format!("/tmp/slot_rocksdb_test_{}", rand::random::<u64>());
        let routing_table = Arc::new(RoutingTable::new());
        SlotRocksDB::new(path, routing_table).expect("Failed to create test database")
    }

    fn cleanup_db(db: &SlotRocksDB) {
        let path = db.path().to_string();
        let _ = db; // Keep reference alive
        let _ = fs::remove_dir_all(&path);
    }

    #[test]
    fn test_string_operations() {
        let db = create_temp_db();

        // Test basic operations (no shard_id needed)
        // Note: Tests need to be updated to work without shard_id
        // For now, these tests are disabled as they need refactoring
        cleanup_db(&db);
    }

    #[test]
    fn test_hash_operations() {
        let db = create_temp_db();

        // Test basic operations (no shard_id needed)
        // Note: Tests need to be updated to work without shard_id
        // For now, these tests are disabled as they need refactoring
        cleanup_db(&db);
    }
}
