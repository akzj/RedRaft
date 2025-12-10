//! Hybrid Storage Architecture
//!
//! Unified storage manager combining multiple storage backends:
//! - **RocksDB**: Persistent storage for String, Hash (with Column Family sharding)
//! - **Memory**: In-memory storage for List, Set, ZSet (with HashMap sharding)
//! - **StreamStore**: Disk-based storage for Stream (future)
//!
//! ## Architecture Options
//!
//! ### Option 1: HybridStore (Legacy)
//! Separate backends with independent shard management.
//! ```text
//! HybridStore
//! ├── ShardedRocksDB (manages all RocksDB shards)
//! ├── MemoryStore (manages all memory shards)
//! └── StreamStore (manages all stream shards)
//! ```
//!
//! ### Option 2: ShardedHybridStore (Recommended)
//! Per-shard locking for atomic snapshot generation.
//! ```text
//! ShardedHybridStore
//! └── RwLock<HashMap<ShardId, Arc<RwLock<ShardStore>>>>
//!     ├── Shard 0: ShardStore { strings, hashes, lists, sets, zsets }
//!     ├── Shard 1: ShardStore { ... }
//!     └── ...
//! ```
//!
//! ## Data Type Routing
//!
//! | Data Type | Storage Backend | Persistence |
//! |-----------|-----------------|-------------|
//! | String    | RocksDB         | ✅ Yes      |
//! | Hash      | RocksDB         | ✅ Yes      |
//! | List      | Memory          | ❌ No       |
//! | Set       | Memory          | ❌ No       |
//! | ZSet      | Memory          | ❌ No       |
//! | Stream    | StreamStore     | ✅ Yes      |
//!
//! ## Sharding
//!
//! - Uses CRC16 hash to calculate slot (0-16383)
//! - Slot maps to shard_id based on shard_count
//! - Per-shard locking enables atomic snapshot generation

mod sharded_rocksdb;
mod shard_store;
mod store;
mod stream_store;

pub use sharded_rocksdb::ShardedRocksDB;
pub use shard_store::{
    ShardStore, ShardSnapshot, ShardedHybridStore, ShardStats, StringStore, StringSnapshot, ZSetData,
};
pub use store::{HybridSnapshot, HybridStore};
pub use stream_store::StreamStore;

/// Data type for routing to correct storage backend
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// String type -> RocksDB
    String,
    /// Hash type -> RocksDB
    Hash,
    /// List type -> Memory
    List,
    /// Set type -> Memory
    Set,
    /// ZSet type -> Memory
    ZSet,
    /// Stream type -> StreamStore
    Stream,
    /// Unknown type
    Unknown,
}

impl DataType {
    /// Get storage backend name
    pub fn backend_name(&self) -> &'static str {
        match self {
            DataType::String | DataType::Hash => "rocksdb",
            DataType::List | DataType::Set | DataType::ZSet => "memory",
            DataType::Stream => "stream",
            DataType::Unknown => "unknown",
        }
    }

    /// Check if type is persistent
    pub fn is_persistent(&self) -> bool {
        matches!(self, DataType::String | DataType::Hash | DataType::Stream)
    }
}

