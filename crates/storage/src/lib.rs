//! Redis storage abstraction layer
//!
//! Defines storage traits, supporting multiple storage backends.
//!
//! # Storage Architecture
//!
//! ## Hybrid Storage (Recommended)
//!
//! The `HybridStore` combines multiple backends for optimal performance:
//! - **RocksDB**: String, Hash (persistent, durable)
//! - **Memory**: List, Set, ZSet (high-performance, volatile)
//! - **StreamStore**: Stream (persistent, large capacity)
//!
//! ## Path Structure
//!
//! All backends use the same path: `shard_id -> key -> value`
//!
//! ## Data Type Routing
//!
//! | Data Type | Backend     | Persistence |
//! |-----------|-------------|-------------|
//! | String    | RocksDB     | ✅ Yes      |
//! | Hash      | RocksDB     | ✅ Yes      |
//! | List      | Memory      | ❌ No       |
//! | Set       | Memory      | ❌ No       |
//! | ZSet      | Memory      | ❌ No       |
//! | Stream    | StreamStore | ✅ Yes      |
//!
//! # Example (HybridStore - Recommended)
//!
//! ```rust,ignore
//! use storage::{HybridStore, RedisStore};
//!
//! // Create hybrid store with 16 shards
//! let store = HybridStore::new("/tmp/mydb", 16).unwrap();
//!
//! // String (RocksDB - persistent)
//! store.set(b"key".to_vec(), b"value".to_vec());
//!
//! // Hash (RocksDB - persistent)
//! store.hset(b"hash", b"field".to_vec(), b"value".to_vec());
//!
//! // List (Memory - volatile)
//! store.lpush(b"list", vec![b"item".to_vec()]);
//!
//! // Set (Memory - volatile)
//! store.sadd(b"set", vec![b"member".to_vec()]);
//! ```
//!
//! # Example (MemoryStore - Standalone)
//!
//! ```rust
//! use storage::{MemoryStore, StringStore};
//!
//! let store = MemoryStore::new(16);
//! store.set(b"key".to_vec(), b"value".to_vec());
//! ```
//!
//! # Example (ShardedRocksDB - Standalone)
//!
//! ```rust,ignore
//! use storage::{ShardedRocksDB, StringStore};
//!
//! // Create sharded RocksDB with 16 shards
//! let store = ShardedRocksDB::new("/tmp/mydb", 16).unwrap();
//! let shard_id = store.shard_for_key(b"key");
//! store.set(shard_id, b"key", b"value".to_vec()).unwrap();
//! ```

pub mod hybrid;
pub mod memory;
pub mod rocksdb;
mod traits;

// Hybrid storage (recommended)
pub use hybrid::{DataType, HybridSnapshot, HybridStore, StreamStore};

// Individual stores
pub use memory::{
    shard_for_key,
    shard_slot_range,
    // Shard utilities
    slot_for_key,
    // Data types
    ListData,
    PubSubStore,
    SetData,
    ShardId,
    ZSetData,
    TOTAL_SLOTS,
};
pub use rocksdb::{ShardMetadata, ShardedRocksDB};

// Traits (organized by data structure)
pub use traits::{
    // Error types
    ApplyResult,
    StoreError,
    StoreResult,
    // Data structure traits
    HashStore,
    KeyStore,
    ListStore,
    SetStore,
    SnapshotStore,
    StringStore,
    ZSetStore,
    // Combined trait
    RedisStore,
};
