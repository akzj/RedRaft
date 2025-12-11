//! Hybrid Storage Manager
//!
//! Unified interface for multiple storage backends:
//! - RocksDB: String, Hash (persistent)
//! - Memory: List, Set, ZSet (volatile)
//! - StreamStore: Stream (persistent)
//!
//! ## Path Structure
//!
//! All backends follow: shard_id -> key -> value

use crate::memory;
use crate::rocksdb::ShardedRocksDB;
use crate::shard::ShardId;
use crate::snapshot::{SnapshotConfig, WalWriter};
use parking_lot::RwLock;
use resp::Command;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;

/// Sharded Store with RocksDB and Memory backends
///
/// Lock Strategy:
/// - Read/Write operations: Use read lock (shared access)
/// - Snapshot operations: Use write lock (exclusive access, released immediately after snapshot creation)
pub struct ShardedStore {
    rocksdb: ShardedRocksDB,
    memory: memory::ShardStore,
}

/// Locked Sharded Store with RwLock protection
///
/// Lock usage:
/// - Normal operations (get, set, etc.): Use `.read()` for read lock
/// - Snapshot creation: Use `.write()` for write lock, release immediately after snapshot
pub type LockedShardedStore = Arc<RwLock<ShardedStore>>;

impl ShardedStore {
    /// Create a new ShardedStore
    pub fn new(rocksdb: ShardedRocksDB, memory: memory::ShardStore) -> Self {
        Self { rocksdb, memory }
    }

    /// Get reference to RocksDB (for read operations)
    pub fn rocksdb(&self) -> &ShardedRocksDB {
        &self.rocksdb
    }

    /// Get mutable reference to RocksDB (for write operations)
    pub fn rocksdb_mut(&mut self) -> &mut ShardedRocksDB {
        &mut self.rocksdb
    }

    /// Get reference to Memory store (for read operations)
    pub fn memory(&self) -> &memory::ShardStore {
        &self.memory
    }

    /// Get mutable reference to Memory store (for write operations)
    pub fn memory_mut(&mut self) -> &mut memory::ShardStore {
        &mut self.memory
    }
}

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
pub struct HybridStore {
    /// RocksDB for String, Hash
    shards: Arc<RwLock<HashMap<ShardId, LockedShardedStore>>>,

    /// Number of shards
    shard_count: u32,

    /// WAL Writer for logging all write operations
    wal_writer: Arc<RwLock<WalWriter>>,

    /// Snapshot configuration
    snapshot_config: SnapshotConfig,
}

impl HybridStore {
    /// Create a new HybridStore
    pub fn new(
        shard_count: u32,
        snapshot_config: SnapshotConfig,
        _data_dir: PathBuf,
    ) -> Result<Self, String> {
        // Initialize WAL writer
        let wal_dir = snapshot_config.base_dir.join("wal");
        let wal_writer = WalWriter::new(snapshot_config.clone(), wal_dir)
            .map_err(|e| format!("Failed to initialize WAL writer: {}", e))?;

        Ok(Self {
            shards: Arc::new(RwLock::new(HashMap::new())),
            shard_count,
            wal_writer: Arc::new(RwLock::new(wal_writer)),
            snapshot_config,
        })
    }

    /// Apply command with apply_index (for WAL logging)
    ///
    /// This method executes the command and writes it to WAL for recovery.
    /// Note: This is a placeholder - HybridStore needs to implement RedisStore trait
    /// to actually execute commands. For now, this just writes to WAL.
    pub fn apply_with_index(&self, apply_index: u64, command: &Command) -> crate::traits::ApplyResult {
        // TODO: Execute command using RedisStore trait's apply method
        // For now, we'll just write to WAL and return Ok
        // The actual command execution should be done by the trait implementation
        
        // Extract key and write to WAL (only for write commands)
        if Self::is_write_command(command) {
            if let Some(key) = Self::extract_key_from_command(command) {
                if let Err(e) = self.wal_writer.write().write_entry(apply_index, command, &key) {
                    error!("Failed to write WAL entry at index {}: {}", apply_index, e);
                    // Don't fail the command execution if WAL write fails
                }
            }
        }

        // TODO: Return actual result from command execution
        crate::traits::ApplyResult::Ok
    }

    /// Flush WAL to disk (called when Raft triggers snapshot)
    pub fn flush_wal(&self) -> Result<(), String> {
        self.wal_writer.write().flush()
    }

    /// Extract key from command (for WAL logging)
    fn extract_key_from_command(command: &Command) -> Option<Vec<u8>> {
        use resp::Command::*;
        match command {
            // String commands
            Set { key, .. } | SetNx { key, .. } | SetEx { key, .. } | PSetEx { key, .. }
            | Get { key } | StrLen { key } | GetRange { key, .. }
            | Incr { key } | IncrBy { key, .. } | IncrByFloat { key, .. }
            | Decr { key } | DecrBy { key, .. } | Append { key, .. }
            | GetSet { key, .. } | SetRange { key, .. } => Some(key.clone()),
            Del { keys } => keys.first().cloned(),
            MSet { kvs } | MSetNx { kvs } => kvs.first().map(|(k, _)| k.clone()),
            MGet { keys } => keys.first().cloned(),

            // Hash commands
            HGet { key, .. } | HSet { key, .. } | HDel { key, .. } | HExists { key, .. }
            | HGetAll { key } | HKeys { key } | HVals { key } | HLen { key }
            | HMGet { key, .. } | HMSet { key, .. } | HIncrBy { key, .. }
            | HIncrByFloat { key, .. } | HSetNx { key, .. } => Some(key.clone()),

            // List commands
            LPush { key, .. } | RPush { key, .. } | LPop { key } | RPop { key }
            | LLen { key } | LIndex { key, .. } | LRange { key, .. } | LTrim { key, .. }
            | LSet { key, .. } | LRem { key, .. } => Some(key.clone()),

            // Set commands
            SAdd { key, .. } | SRem { key, .. } | SMembers { key } | SIsMember { key, .. }
            | SCard { key } | SPop { key, .. } => Some(key.clone()),
            SInter { keys } | SUnion { keys } | SDiff { keys } => keys.first().cloned(),

            // Key commands
            Exists { keys } => keys.first().cloned(),
            Type { key } | Ttl { key } | PTtl { key } | Expire { key, .. }
            | PExpire { key, .. } | Persist { key } | Rename { key, .. } | RenameNx { key, .. } => {
                Some(key.clone())
            }
            Keys { .. } | Scan { .. } => {
                // For Keys and Scan, we can't extract a specific key
                None
            }

            // Read-only or no-key commands
            Ping { .. } | Echo { .. } | DbSize | FlushDb | CommandInfo | Info { .. } => None,
        }
    }

    /// Check if command is a write command (needs WAL logging)
    fn is_write_command(command: &Command) -> bool {
        // Use Command's built-in method
        command.is_write()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotConfig;
    use resp::Command;
    use tempfile::TempDir;

    #[test]
    fn test_hybrid_store_new() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let config = SnapshotConfig {
            base_dir: data_dir.clone(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let store = HybridStore::new(16, config, data_dir);
        assert!(store.is_ok());
    }

    #[test]
    fn test_apply_with_index_writes_to_wal() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        let config = SnapshotConfig {
            base_dir: data_dir.clone(),
            shard_count: 16,
            chunk_size: 64 * 1024 * 1024,
            wal_size_threshold: 100 * 1024 * 1024,
            segment_interval_secs: 3600,
            zstd_level: 3,
        };

        let store = HybridStore::new(16, config, data_dir.clone()).unwrap();

        // Apply a write command
        let key = b"test_key".to_vec();
        let command = Command::Set {
            key: key.clone(),
            value: b"test_value".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };

        let result = store.apply_with_index(1, &command);
        assert!(matches!(result, crate::traits::ApplyResult::Ok));

        // Flush WAL and verify it was written
        store.flush_wal().unwrap();

        // Verify WAL file exists
        let wal_dir = data_dir.join("wal");
        assert!(wal_dir.exists());
    }

    #[test]
    fn test_extract_key_from_command() {
        // Test string commands
        let key = b"test_key".to_vec();
        let command = Command::Set {
            key: key.clone(),
            value: b"value".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        assert_eq!(HybridStore::extract_key_from_command(&command), Some(key.clone()));

        // Test hash commands
        let hset = Command::HSet {
            key: key.clone(),
            fvs: vec![(b"field".to_vec(), b"value".to_vec())],
        };
        assert_eq!(HybridStore::extract_key_from_command(&hset), Some(key.clone()));

        // Test read-only commands (should still extract key for WAL)
        let get = Command::Get { key: key.clone() };
        assert_eq!(HybridStore::extract_key_from_command(&get), Some(key));

        // Test no-key commands
        let ping = Command::Ping { message: None };
        assert_eq!(HybridStore::extract_key_from_command(&ping), None);
    }

    #[test]
    fn test_is_write_command() {
        let key = b"test_key".to_vec();
        
        // Write command
        let set = Command::Set {
            key: key.clone(),
            value: b"value".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        };
        assert!(HybridStore::is_write_command(&set));

        // Read command
        let get = Command::Get { key };
        assert!(!HybridStore::is_write_command(&get));
    }
}
