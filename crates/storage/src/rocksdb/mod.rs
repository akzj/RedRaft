//! RocksDB-based persistent storage implementation
//!
//! Provides slot-based persistent storage for Redis data structures:
//! - String: Simple key-value storage with O(1) operations
//! - Hash: Field-based storage using key prefixes
//!
//! ## Key Encoding
//!
//! Keys in default Column Family:
//! - String: `{slot(4字节)}:s:{key}`
//! - Hash field: `{slot(4字节)}:h:{key}:{field}`
//! - Hash metadata: `{slot(4字节)}:H:{key}` (stores field count)
//! - Apply index: `@:apply_index` (no slot prefix, stores apply_index)
//!
//! Slot is encoded as 4-byte big-endian u32 at the beginning of each key.
//! This enables efficient slot-based range queries and data migration.
//!
//! ## Features
//! - Persistent storage with WAL
//! - Slot-based key organization for efficient range queries
//! - Atomic apply_index commits
//!
//! ## Module Structure
//!
//! - `slot_rocksdb.rs`: Main structure and apply_index management
//! - `key_encoding.rs`: Key encoding/decoding utilities
//! - `string.rs`: String operations (GET, SET, INCR, etc.)
//! - `hash.rs`: Hash operations (HGET, HSET, HINCRBY, etc.)

mod hash;
mod key_encoding;
mod slot_rocksdb;
mod string;

pub use key_encoding::key_prefix;
pub use key_encoding::{
    parse_string_key, parse_hash_field_key, parse_hash_meta_key, slot_range_prefix,
};
pub use slot_rocksdb::SlotRocksDB;
