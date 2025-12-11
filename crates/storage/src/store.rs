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
use crate::shard::{slot_for_key, ShardId};
use crate::traits::{
    HashStore, KeyStore, ListStore, RedisStore, SetStore, SnapshotStore, StoreResult, StringStore,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub struct ShardedStore {
    rocksdb: Arc<ShardedRocksDB>,
    memory: memory::ShardStore,
}

/// Hybrid Storage Manager
///
/// Combines multiple storage backends with automatic routing.
pub struct HybridStore {
    /// RocksDB for String, Hash
    shards: Arc<RwLock<HashMap<ShardId, ShardedStore>>>,

    /// Number of shards
    shard_count: u32,
}
