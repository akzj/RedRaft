//! RocksDB-based persistent storage implementation
//!
//! Provides sharded persistent storage for Redis data structures:
//! - String: Simple key-value storage with O(1) operations
//! - Hash: Field-based storage using key prefixes
//!
//! ## Key Encoding
//!
//! Keys within each shard's Column Family:
//! - String: `s:{key}`
//! - Hash field: `h:{key}:{field}`
//! - Hash metadata: `H:{key}` (stores field count)
//! - Apply index: `@:apply_index` (stores shard apply_index)
//!
//! ## Features
//! - Persistent storage with WAL
//! - Column Family-based sharding for efficient snapshots
//! - Atomic apply_index commits
//! - Per-shard metadata management
//!
//! ## Module Structure
//!
//! - `sharded_rocksdb.rs`: Main structure and apply_index management
//! - `key_encoding.rs`: Key encoding/decoding utilities
//! - `string.rs`: String operations (GET, SET, INCR, etc.)
//! - `hash.rs`: Hash operations (HGET, HSET, HINCRBY, etc.)
//! - `snapshot.rs`: Snapshot creation and restoration

mod hash;
mod key_encoding;
mod sharded_rocksdb;
mod snapshot;
mod string;

pub use sharded_rocksdb::{ShardMetadata, ShardedRocksDB};
