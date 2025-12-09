//! Redis storage abstraction layer
//! 
//! Defines storage traits, supporting in-memory and persistent storage (like RocksDB)
//! 
//! # Supported Redis Data Types
//! - String: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN
//! - List: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET
//! - Hash: HGET, HSET, HMGET, HMSET, HDEL, HGETALL, HKEYS, HVALS
//! - Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
//! 
//! # Example
//! ```rust
//! use storage::{MemoryStore, RedisStore};
//! 
//! let store = MemoryStore::new();
//! store.set(b"key".to_vec(), b"value".to_vec());
//! assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
//! ```

mod traits;
mod memory;

pub use traits::{ApplyResult, RedisStore, StoreError, StoreResult};
pub use memory::{MemoryStore, RedisValue};
