//! Snapshot operations for ShardedRocksDB
//!
//! Provides snapshot creation and restoration for individual shards.

use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use rocksdb::WriteBatch;

impl ShardedRocksDB {
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

