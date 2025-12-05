//! Redis 存储抽象层
//!
//! 定义存储 trait，支持内存和持久化存储（如 RocksDB）
//!
//! # 支持的 Redis 数据类型
//! - String: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN
//! - List: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET
//! - Hash: HGET, HSET, HMGET, HMSET, HDEL, HGETALL, HKEYS, HVALS
//! - Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
//!
//! # 示例
//! ```rust
//! use redisstore::{MemoryStore, RedisStore};
//!
//! let store = MemoryStore::new();
//! store.set(b"key".to_vec(), b"value".to_vec());
//! assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
//! ```

mod operation;
mod traits;
mod memory;

pub use operation::KVOperation;
pub use traits::{RedisStore, StoreError, StoreResult};
pub use memory::{MemoryStore, RedisValue};
