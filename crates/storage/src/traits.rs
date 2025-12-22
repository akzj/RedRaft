//! Redis storage trait definitions
//!
//! Traits are organized by data structure for flexible implementation:
//!
//! - `StringStore`: String operations (GET, SET, INCR, etc.)
//! - `ListStore`: List operations (LPUSH, RPUSH, LPOP, etc.)
//! - `HashStore`: Hash operations (HGET, HSET, HDEL, etc.)
//! - `SetStore`: Set operations (SADD, SREM, SMEMBERS, etc.)
//! - `ZSetStore`: Sorted Set operations (ZADD, ZREM, ZRANGE, etc.)
//! - `KeyStore`: Generic key operations (DEL, EXISTS, TYPE, TTL, etc.)
//! - `SnapshotStore`: Snapshot operations
//! - `RedisStore`: Combines all traits with command execution

use anyhow::Result;
use async_trait::async_trait;
use resp::Command;

use bytes::Bytes;

use rr_core::shard::ShardId;

// ============================================================================
// Error Types
// ============================================================================

/// Command execution result
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    /// Simple string response (OK, PONG, etc.)
    Ok,
    /// PONG response
    Pong(Option<Bytes>),
    /// Integer response
    Integer(i64),
    /// String response (may be nil)
    Value(Option<Bytes>),
    /// Array response
    Array(Vec<Option<Bytes>>),
    /// Key-value array (used for HGETALL, etc.)
    KeyValues(Vec<(Bytes, Bytes)>),
    /// Type string
    Type(Option<&'static str>),
    /// Error response
    Error(StoreError),
}

/// Redis storage error
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StoreError {
    /// Key not found
    KeyNotFound,
    /// Type mismatch (e.g., performing List operations on String)
    WrongType,
    /// Index out of range
    IndexOutOfRange,
    /// Invalid argument
    InvalidArgument(String),
    /// Internal error
    Internal(String),
    /// Operation not supported
    NotSupported,

    /// Shard not found
    ShardNotFound(ShardId),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::KeyNotFound => write!(f, "key not found"),
            StoreError::WrongType => {
                write!(
                    f,
                    "WRONGTYPE Operation against a key holding the wrong kind of value"
                )
            }
            StoreError::IndexOutOfRange => write!(f, "index out of range"),
            StoreError::NotSupported => write!(f, "operation not supported"),
            StoreError::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
            StoreError::Internal(msg) => write!(f, "internal error: {}", msg),
            StoreError::ShardNotFound(shard_id) => write!(f, "shard not found: {}", shard_id),
        }
    }
}

impl std::error::Error for StoreError {}

pub type StoreResult<T> = Result<T, StoreError>;

// ============================================================================
// String Store Trait
// ============================================================================

/// String data structure operations
///
/// Supports: GET, SET, SETNX, SETEX, MGET, MSET, INCR, DECR, APPEND, STRLEN
///
/// Recommended backend: RocksDB (persistent)
pub trait StringStore: Send + Sync {
    /// GET: Get string value
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn get(&self, key: &[u8], read_index: u64) -> StoreResult<Option<Bytes>>;

    /// SET: Set string value
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn set(&self, key: &[u8], value: Bytes, apply_index: u64) -> StoreResult<()>;

    /// SETNX: Set only if key does not exist
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn setnx(&self, key: &[u8], value: Bytes, apply_index: u64) -> StoreResult<bool>;

    /// SETEX: Set value with expiration time (seconds)
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn setex(&self, key: &[u8], value: Bytes, ttl_secs: u64, apply_index: u64) -> StoreResult<()>;

    /// MGET: Batch get
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn mget(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<Vec<Option<Bytes>>> {
        let mut results = Vec::new();
        for k in keys {
            match self.get(k, read_index) {
                Ok(val) => results.push(val),
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    /// MSET: Batch set
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn mset(&self, kvs: Vec<(&[u8], Bytes)>, apply_index: u64) -> StoreResult<()> {
        for (k, v) in kvs {
            self.set(k, v, apply_index)?;
        }
        Ok(())
    }

    /// INCR: Increment integer by 1
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn incr(&self, key: &[u8], apply_index: u64) -> StoreResult<i64> {
        self.incrby(key, 1, apply_index)
    }

    /// INCRBY: Increment integer by specified value
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn incrby(&self, key: &[u8], delta: i64, apply_index: u64) -> StoreResult<i64>;

    /// DECR: Decrement integer by 1
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn decr(&self, key: &[u8], apply_index: u64) -> StoreResult<i64> {
        self.incrby(key, -1, apply_index)
    }

    /// DECRBY: Decrement integer by specified value
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn decrby(&self, key: &[u8], delta: i64, apply_index: u64) -> StoreResult<i64> {
        self.incrby(key, -delta, apply_index)
    }

    /// APPEND: Append string
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn append(&self, key: &[u8], value: &[u8], apply_index: u64) -> StoreResult<usize>;

    /// STRLEN: Get string length
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn strlen(&self, key: &[u8], read_index: u64) -> StoreResult<usize>;

    /// GETSET: Set new value and return old value
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (for get)
    /// - `apply_index`: Raft apply index for WAL logging (for set)
    fn getset(
        &self,
        key: &[u8],
        value: Bytes,
        read_index: u64,
        apply_index: u64,
    ) -> StoreResult<Option<Bytes>> {
        let old = self.get(&key, read_index)?;
        self.set(key, value, apply_index)?;
        Ok(old)
    }
}

// ============================================================================
// List Store Trait
// ============================================================================

/// List data structure operations
///
/// Supports: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET
///
/// Recommended backend: Memory (high performance)
pub trait ListStore: Send + Sync {
    /// LPUSH: Insert elements from left
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn lpush(&self, key: &[u8], values: Vec<Bytes>, apply_index: u64) -> StoreResult<usize>;

    /// RPUSH: Insert elements from right
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn rpush(&self, key: &[u8], values: Vec<Bytes>, apply_index: u64) -> StoreResult<usize>;

    /// LPOP: Pop element from left
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn lpop(&self, key: &[u8], apply_index: u64) -> StoreResult<Option<Bytes>>;

    /// RPOP: Pop element from right
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn rpop(&self, key: &[u8], apply_index: u64) -> StoreResult<Option<Bytes>>;

    /// LRANGE: Get list range
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn lrange(&self, key: &[u8], start: i64, stop: i64, read_index: u64)
        -> StoreResult<Vec<Bytes>>;

    /// LLEN: Get list length
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn llen(&self, key: &[u8], read_index: u64) -> StoreResult<usize>;

    /// LINDEX: Get element at specified index
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn lindex(&self, key: &[u8], index: i64, read_index: u64) -> StoreResult<Option<Bytes>>;

    /// LSET: Set element at specified index
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn lset(&self, key: &[u8], index: i64, value: Bytes, apply_index: u64) -> StoreResult<()>;

    /// LTRIM: Trim list to specified range
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn ltrim(&self, key: &[u8], start: i64, stop: i64, apply_index: u64) -> StoreResult<()> {
        let _ = (key, start, stop, apply_index);
        Err(StoreError::NotSupported)
    }

    /// LREM: Remove elements from list
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn lrem(&self, key: &[u8], count: i64, value: &[u8], apply_index: u64) -> StoreResult<usize> {
        let _ = (key, count, value, apply_index);
        Ok(0)
    }

    /// LINSERT: Insert element before or after pivot
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn linsert(
        &self,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: Bytes,
        apply_index: u64,
    ) -> StoreResult<i64> {
        let _ = (key, before, pivot, value, apply_index);
        Err(StoreError::NotSupported)
    }

    /// RPOPLPUSH: Pop from right, push to left of another list
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn rpoplpush(
        &self,
        source: &[u8],
        destination: &[u8],
        apply_index: u64,
    ) -> StoreResult<Option<Bytes>> {
        let _ = (source, destination, apply_index);
        Ok(None)
    }
}

// ============================================================================
// Hash Store Trait
// ============================================================================

/// Hash data structure operations
///
/// Supports: HGET, HSET, HMGET, HMSET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY
///
/// Recommended backend: RocksDB (persistent)
pub trait HashStore: Send + Sync {
    /// HGET: Get hash field value
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hget(&self, key: &[u8], field: &[u8], read_index: u64) -> StoreResult<Option<Bytes>>;

    /// HSET: Set hash field value, returns true if field is new
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn hset(&self, key: &[u8], field: &[u8], value: Bytes, apply_index: u64) -> StoreResult<bool>;

    /// HMGET: Batch get hash fields
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hmget(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        read_index: u64,
    ) -> StoreResult<Vec<Option<Bytes>>> {
        let mut results = Vec::new();
        for f in fields {
            match self.hget(key, f, read_index) {
                Ok(val) => results.push(val),
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    /// HMSET: Batch set hash fields
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn hmset(&self, key: &[u8], fvs: Vec<(&[u8], Bytes)>, apply_index: u64) -> StoreResult<()> {
        for (f, v) in fvs {
            self.hset(key, f, v, apply_index)?;
        }
        Ok(())
    }

    /// HDEL: Delete hash fields
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn hdel(&self, key: &[u8], fields: &[&[u8]], apply_index: u64) -> StoreResult<usize>;

    /// HEXISTS: Check if hash field exists
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hexists(&self, key: &[u8], field: &[u8], read_index: u64) -> StoreResult<bool> {
        Ok(self.hget(key, field, read_index)?.is_some())
    }

    /// HGETALL: Get all hash fields and values
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hgetall(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<(Bytes, Bytes)>>;

    /// HKEYS: Get all hash field names
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hkeys(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>>;

    /// HVALS: Get all hash field values
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hvals(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>>;

    /// HLEN: Get number of hash fields
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn hlen(&self, key: &[u8], read_index: u64) -> StoreResult<usize>;

    /// HINCRBY: Increment hash field integer
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64, apply_index: u64) -> StoreResult<i64>;

    /// HSETNX: Set hash field only if it does not exist
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (for exists check)
    /// - `apply_index`: Raft apply index for WAL logging (for set)
    fn hsetnx(
        &self,
        key: &[u8],
        field: &[u8],
        value: Bytes,
        read_index: u64,
        apply_index: u64,
    ) -> StoreResult<bool> {
        if self.hexists(key, &field, read_index)? {
            Ok(false)
        } else {
            self.hset(key, field, value, apply_index)?;
            Ok(true)
        }
    }
}

// ============================================================================
// Set Store Trait
// ============================================================================

/// Set data structure operations
///
/// Supports: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER
///
/// Recommended backend: Memory (high performance)
pub trait SetStore: Send + Sync {
    /// SADD: Add set members
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn sadd(&self, key: &[u8], members: Vec<Bytes>, apply_index: u64) -> StoreResult<usize>;

    /// SREM: Remove set members
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn srem(&self, key: &[u8], members: &[&[u8]], apply_index: u64) -> StoreResult<usize>;

    /// SMEMBERS: Get all set members
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn smembers(&self, key: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>>;

    /// SISMEMBER: Check if set member exists
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn sismember(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<bool>;

    /// SCARD: Get set size
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn scard(&self, key: &[u8], read_index: u64) -> StoreResult<usize>;

    /// SPOP: Remove and return random member(s)
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn spop(&self, key: &[u8], count: usize, apply_index: u64) -> StoreResult<Vec<Bytes>> {
        let _ = (key, count, apply_index);
        Ok(Vec::new())
    }

    /// SRANDMEMBER: Get random member(s) without removing
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn srandmember(&self, key: &[u8], count: i64, read_index: u64) -> StoreResult<Vec<Bytes>> {
        let _ = (key, count, read_index);
        Ok(Vec::new())
    }

    /// SINTER: Intersection of multiple sets
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn sinter(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let _ = (keys, read_index);
        Ok(Vec::new())
    }

    /// SUNION: Union of multiple sets
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn sunion(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let _ = (keys, read_index);
        Ok(Vec::new())
    }

    /// SDIFF: Difference of multiple sets
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn sdiff(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<Vec<Bytes>> {
        let _ = (keys, read_index);
        Ok(Vec::new())
    }
}

// ============================================================================
// ZSet (Sorted Set) Store Trait
// ============================================================================

/// Sorted Set data structure operations
///
/// Supports: ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZINCRBY
///
/// Recommended backend: Memory (high performance)
pub trait ZSetStore: Send + Sync {
    /// ZADD: Add members with scores
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn zadd(&self, key: &[u8], members: Vec<(f64, Bytes)>, apply_index: u64) -> StoreResult<usize>;

    /// ZREM: Remove members
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn zrem(&self, key: &[u8], members: &[&[u8]], apply_index: u64) -> StoreResult<usize>;

    /// ZSCORE: Get member score
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zscore(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<f64>>;

    /// ZRANK: Get member rank (0-based)
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zrank(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<usize>>;

    /// ZREVRANK: Get member reverse rank
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zrevrank(&self, key: &[u8], member: &[u8], read_index: u64) -> StoreResult<Option<usize>> {
        let _ = (key, member, read_index);
        Ok(None)
    }

    /// ZRANGE: Get members by rank range
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>>;

    /// ZREVRANGE: Get members by reverse rank range
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zrevrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>> {
        let _ = (key, start, stop, with_scores, read_index);
        Ok(Vec::new())
    }

    /// ZRANGEBYSCORE: Get members by score range
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
        read_index: u64,
    ) -> StoreResult<Vec<(Bytes, f64)>> {
        let _ = (key, min, max, with_scores, offset, count, read_index);
        Ok(Vec::new())
    }

    /// ZCARD: Get sorted set size
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zcard(&self, key: &[u8], read_index: u64) -> StoreResult<usize>;

    /// ZCOUNT: Count members in score range
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn zcount(&self, key: &[u8], min: f64, max: f64, read_index: u64) -> StoreResult<usize> {
        let _ = (key, min, max, read_index);
        Ok(0)
    }

    /// ZINCRBY: Increment member score
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn zincrby(&self, key: &[u8], delta: f64, member: &[u8], apply_index: u64) -> StoreResult<f64>;

    /// ZINTERSTORE: Store intersection of sorted sets
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn zinterstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        weights: Option<&[f64]>,
        apply_index: u64,
    ) -> StoreResult<usize> {
        let _ = (destination, keys, weights, apply_index);
        Ok(0)
    }

    /// ZUNIONSTORE: Store union of sorted sets
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn zunionstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        weights: Option<&[f64]>,
        apply_index: u64,
    ) -> StoreResult<usize> {
        let _ = (destination, keys, weights, apply_index);
        Ok(0)
    }
}

// ============================================================================
// Key Store Trait
// ============================================================================

/// Generic key operations (cross data structure)
///
/// Supports: DEL, EXISTS, KEYS, TYPE, TTL, EXPIRE, PERSIST, DBSIZE, FLUSHDB, RENAME
pub trait KeyStore: Send + Sync {
    /// DEL: Delete keys (supports multiple)
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn del(&self, keys: &[&[u8]], apply_index: u64) -> StoreResult<usize>;

    /// EXISTS: Check if keys exist (supports multiple)
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn exists(&self, keys: &[&[u8]], read_index: u64) -> StoreResult<usize>;

    /// KEYS: Get all keys matching pattern (simplified, only * wildcard)
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn keys(&self, pattern: &[u8], read_index: u64) -> StoreResult<Vec<Bytes>>;

    /// TYPE: Get key type
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn key_type(&self, key: &[u8], read_index: u64) -> StoreResult<Option<&'static str>>;

    /// TTL: Get remaining expiration time (seconds), -1 for never expire, -2 for key not found
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn ttl(&self, key: &[u8], read_index: u64) -> StoreResult<i64>;

    /// EXPIRE: Set expiration time (seconds)
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn expire(&self, key: &[u8], ttl_secs: u64, apply_index: u64) -> StoreResult<bool>;

    /// PERSIST: Remove expiration time
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn persist(&self, key: &[u8], apply_index: u64) -> StoreResult<bool>;

    /// DBSIZE: Get number of key-value pairs
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification
    fn dbsize(&self, read_index: u64) -> StoreResult<usize>;

    /// FLUSHDB: Clear all data
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn flushdb(&self, apply_index: u64) -> StoreResult<()>;

    /// RENAME: Rename key
    /// # Arguments
    /// - `apply_index`: Raft apply index for WAL logging
    fn rename(&self, key: &[u8], new_key: &[u8], apply_index: u64) -> StoreResult<()>;

    /// RENAMENX: Rename key only if new key does not exist
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (for exists check)
    /// - `apply_index`: Raft apply index for WAL logging (for rename)
    fn renamenx(
        &self,
        key: &[u8],
        new_key: &[u8],
        read_index: u64,
        apply_index: u64,
    ) -> StoreResult<bool> {
        if self.exists(&[&new_key], read_index)? > 0 {
            Ok(false)
        } else {
            self.rename(key, new_key.as_ref(), apply_index)?;
            Ok(true)
        }
    }
}

// ============================================================================
// Snapshot Store Trait
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SnapshotStoreEntry {
    // key, field, value, apply_index
    Hash(bytes::Bytes, bytes::Bytes, bytes::Bytes, u64),
    // key, first element, apply_index
    List(bytes::Bytes, bytes::Bytes, u64),
    // key, element, apply_index
    Set(bytes::Bytes, bytes::Bytes, u64),
    // key, score, element, apply_index
    ZSet(bytes::Bytes, f64, bytes::Bytes, u64),

    // key, bitmap, apply_index
    Bitmap(bytes::Bytes, bytes::Bytes, u64),
    // key, value, apply_index
    String(bytes::Bytes, bytes::Bytes, u64),

    // Error
    Error(StoreError),

    // Success
    Completed,
}

/// Snapshot operations for persistence and replication
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Flush data to disk for snapshot creation
    ///
    /// In multi-raft architecture, each Raft group corresponds to one shard.
    /// This method only flushes data to disk (WAL and RocksDB), as data is already stored on disk.
    /// No actual snapshot data is generated or returned - the data field should be empty.
    ///
    /// # Arguments
    /// - `shard_id`: Shard ID (from RaftId.group, e.g., "shard_0")
    ///
    /// # Returns
    /// Empty vector (data is already on disk, no need to return it)
    async fn create_snapshot(
        &self,
        shard_id: &ShardId,
        channel: std::sync::mpsc::SyncSender<SnapshotStoreEntry>,
        key_range: Option<(u32, u32)>,
    ) -> anyhow::Result<u64>;

    /// Restore from snapshot data
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> anyhow::Result<()>;

    /// Create split snapshot - only contains data within specified slot range
    ///
    /// # Arguments
    /// - `slot_start`: Start slot (inclusive)
    /// - `slot_end`: End slot (exclusive)
    /// - `total_slots`: Total number of slots (used to calculate key slots)
    ///
    /// # Returns
    /// Snapshot data containing only keys with slot ∈ [slot_start, slot_end)
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> anyhow::Result<Vec<u8>>;

    /// Restore from split snapshot - merge into existing data
    ///
    /// Unlike restore_from_snapshot, this method does not clear existing data,
    /// but merges the snapshot data into it.
    fn merge_from_snapshot(&self, snapshot: &[u8]) -> anyhow::Result<usize>;

    /// Delete all keys within specified slot range
    ///
    /// Used by source shard to clean up transferred data after splitting
    ///
    /// # Arguments
    /// - `slot_start`: Start slot (inclusive)
    /// - `slot_end`: End slot (exclusive)
    /// - `total_slots`: Total number of slots
    ///
    /// # Returns
    /// Number of keys deleted
    fn delete_keys_in_slot_range(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> anyhow::Result<usize>;
}

// ============================================================================
// Redis Store Trait (Combines All)
// ============================================================================

/// Complete Redis-compatible storage trait
///
/// Combines all data structure traits and provides unified command execution.
///
/// Implementations can be:
/// - Full implementation: Implement all traits directly
/// - Hybrid implementation: Delegate to specialized backends
///
/// # Example
///
/// ```rust,ignore
/// // Full implementation
/// impl RedisStore for MemoryStore {}
///
/// // Hybrid implementation
/// impl RedisStore for HybridStore {
///     // String/Hash -> RocksDB
///     // List/Set/ZSet -> Memory
/// }
/// ```
pub trait RedisStore:
    StringStore + ListStore + HashStore + SetStore + ZSetStore + KeyStore + SnapshotStore + Send + Sync
{
    /// Execute Redis command
    ///
    /// Unified command execution entry, calls corresponding operation method based on Command type
    ///
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (used for read operations)
    /// - `apply_index`: Raft apply index for WAL logging (used for write operations)
    /// - `cmd`: Command to execute
    ///
    /// For read-only commands, `apply_index` can be 0.
    /// For write commands, `read_index` can be 0 (or same as `apply_index` for consistency checks).
    fn apply(&self, read_index: u64, apply_index: u64, cmd: &Command) -> ApplyResult {
        match cmd {
            // ==================== Connection/Management Commands ====================
            Command::Ping { message } => {
                ApplyResult::Pong(message.as_ref().map(|m| Bytes::from(m.clone())))
            }
            Command::Echo { message } => ApplyResult::Value(Some(Bytes::from(message.clone()))),
            Command::DbSize => match self.dbsize(read_index) {
                Ok(size) => ApplyResult::Integer(size as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::FlushDb => match self.flushdb(apply_index) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::CommandInfo | Command::Info { .. } => {
                // These commands are handled by the upper layer
                ApplyResult::Ok
            }

            // ==================== String Read Commands ====================
            Command::Get { key } => match self.get(key, read_index) {
                Ok(val) => ApplyResult::Value(val),
                Err(e) => ApplyResult::Error(e),
            },
            Command::MGet { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.mget(&keys_refs, read_index) {
                    Ok(vals) => ApplyResult::Array(vals),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::StrLen { key } => match self.strlen(key, read_index) {
                Ok(len) => ApplyResult::Integer(len as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::GetRange { key, start, end } => {
                // TODO: Implement GETRANGE
                let _ = (key, start, end);
                ApplyResult::Value(None)
            }

            // ==================== String Write Commands ====================
            Command::Set {
                key,
                value,
                ex,
                px,
                nx,
                xx,
            } => {
                // Handle XX condition: key must exist
                if *xx {
                    match self.get(key, read_index) {
                        Ok(None) => return ApplyResult::Value(None),
                        Err(e) => return ApplyResult::Error(e),
                        Ok(Some(_)) => {}
                    }
                }
                // Handle NX condition: key must not exist
                if *nx {
                    match self.setnx(key.as_ref(), Bytes::from(value.clone()), apply_index) {
                        Ok(false) => return ApplyResult::Value(None),
                        Err(e) => return ApplyResult::Error(e),
                        Ok(true) => {}
                    }
                } else {
                    if let Err(e) = self.set(key.as_ref(), Bytes::from(value.clone()), apply_index)
                    {
                        return ApplyResult::Error(e);
                    }
                }
                // Handle expiration time
                if let Some(secs) = ex {
                    let _ = self.expire(key, *secs, apply_index);
                } else if let Some(ms) = px {
                    let _ = self.expire(key, *ms / 1000, apply_index);
                }
                ApplyResult::Ok
            }
            Command::SetNx { key, value } => {
                match self.setnx(key.as_ref(), Bytes::from(value.clone()), apply_index) {
                    Ok(result) => ApplyResult::Integer(if result { 1 } else { 0 }),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SetEx {
                key,
                seconds,
                value,
            } => match self.setex(
                key.as_ref(),
                Bytes::from(value.clone()),
                *seconds,
                apply_index,
            ) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::PSetEx {
                key,
                milliseconds,
                value,
            } => match self.setex(
                key.as_ref(),
                Bytes::from(value.clone()),
                *milliseconds / 1000,
                apply_index,
            ) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::MSet { kvs } => {
                let kvs_converted: Vec<(&[u8], Bytes)> = kvs
                    .iter()
                    .map(|(k, v)| (k.as_ref(), Bytes::from(v.clone())))
                    .collect();
                match self.mset(kvs_converted, apply_index) {
                    Ok(()) => ApplyResult::Ok,
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::MSetNx { kvs } => {
                // Check if all keys do not exist
                let mut all_new = true;
                for (k, _) in kvs.iter() {
                    match self.get(k, read_index) {
                        Ok(Some(_)) => {
                            all_new = false;
                            break;
                        }
                        Err(e) => return ApplyResult::Error(e),
                        Ok(None) => {}
                    }
                }
                if all_new {
                    let kvs_converted: Vec<(&[u8], Bytes)> = kvs
                        .iter()
                        .map(|(k, v)| (k.as_ref(), Bytes::from(v.clone())))
                        .collect();
                    match self.mset(kvs_converted, apply_index) {
                        Ok(()) => ApplyResult::Integer(1),
                        Err(e) => ApplyResult::Error(e),
                    }
                } else {
                    ApplyResult::Integer(0)
                }
            }
            Command::Incr { key } => match self.incr(key, apply_index) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::IncrBy { key, delta } => match self.incrby(key, *delta, apply_index) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::IncrByFloat { .. } => {
                // TODO: Implement INCRBYFLOAT
                ApplyResult::Error(StoreError::Internal("INCRBYFLOAT not implemented".into()))
            }
            Command::Decr { key } => match self.decr(key, apply_index) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::DecrBy { key, delta } => match self.decrby(key, *delta, apply_index) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Append { key, value } => match self.append(key, value, apply_index) {
                Ok(len) => ApplyResult::Integer(len as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::GetSet { key, value } => {
                match self.getset(
                    key.as_ref(),
                    Bytes::from(value.clone()),
                    read_index,
                    apply_index,
                ) {
                    Ok(old) => ApplyResult::Value(old),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SetRange { key, offset, value } => {
                // TODO: Implement SETRANGE
                let _ = (key, offset, value);
                ApplyResult::Integer(0)
            }

            // ==================== List Read Commands ====================
            Command::LLen { key } => match self.llen(key, read_index) {
                Ok(len) => ApplyResult::Integer(len as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::LIndex { key, index } => match self.lindex(key, *index, read_index) {
                Ok(val) => ApplyResult::Value(val),
                Err(e) => ApplyResult::Error(e),
            },
            Command::LRange { key, start, stop } => {
                match self.lrange(key, *start, *stop, read_index) {
                    Ok(list) => ApplyResult::Array(list.into_iter().map(Some).collect()),
                    Err(e) => ApplyResult::Error(e),
                }
            }

            // ==================== List Write Commands ====================
            Command::LPush { key, values } => {
                let values_converted: Vec<Bytes> =
                    values.iter().map(|v| Bytes::from(v.clone())).collect();
                match self.lpush(key, values_converted, apply_index) {
                    Ok(len) => ApplyResult::Integer(len as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::RPush { key, values } => {
                let values_converted: Vec<Bytes> =
                    values.iter().map(|v| Bytes::from(v.clone())).collect();
                match self.rpush(key, values_converted, apply_index) {
                    Ok(len) => ApplyResult::Integer(len as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::LPop { key } => match self.lpop(key, apply_index) {
                Ok(val) => ApplyResult::Value(val),
                Err(e) => ApplyResult::Error(e),
            },
            Command::RPop { key } => match self.rpop(key, apply_index) {
                Ok(val) => ApplyResult::Value(val),
                Err(e) => ApplyResult::Error(e),
            },
            Command::LSet { key, index, value } => {
                match self.lset(key, *index, Bytes::from(value.clone()), apply_index) {
                    Ok(()) => ApplyResult::Ok,
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::LTrim { key, start, stop } => {
                match self.ltrim(key, *start, *stop, apply_index) {
                    Ok(()) => ApplyResult::Ok,
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::LRem { key, count, value } => match self.lrem(key, *count, value, apply_index)
            {
                Ok(removed) => ApplyResult::Integer(removed as i64),
                Err(e) => ApplyResult::Error(e),
            },

            // ==================== Hash Read Commands ====================
            Command::HGet { key, field } => match self.hget(key, field, read_index) {
                Ok(val) => ApplyResult::Value(val),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HMGet { key, fields } => {
                let fields_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_ref()).collect();
                match self.hmget(key, &fields_refs, read_index) {
                    Ok(vals) => ApplyResult::Array(vals),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HGetAll { key } => match self.hgetall(key, read_index) {
                Ok(kvs) => ApplyResult::KeyValues(kvs),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HKeys { key } => match self.hkeys(key, read_index) {
                Ok(keys) => ApplyResult::Array(keys.into_iter().map(Some).collect()),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HVals { key } => match self.hvals(key, read_index) {
                Ok(vals) => ApplyResult::Array(vals.into_iter().map(Some).collect()),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HLen { key } => match self.hlen(key, read_index) {
                Ok(len) => ApplyResult::Integer(len as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HExists { key, field } => match self.hexists(key, field, read_index) {
                Ok(exists) => ApplyResult::Integer(if exists { 1 } else { 0 }),
                Err(e) => ApplyResult::Error(e),
            },

            // ==================== Hash Write Commands ====================
            Command::HSet { key, fvs } => {
                let fvs_converted: Vec<(&[u8], Bytes)> = fvs
                    .iter()
                    .map(|(f, v)| (f.as_ref(), Bytes::from(v.clone())))
                    .collect();
                match self.hmset(key, fvs_converted, apply_index) {
                    Ok(()) => ApplyResult::Integer(fvs.len() as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HSetNx { key, field, value } => {
                match self.hsetnx(
                    key.as_ref(),
                    field.as_ref(),
                    Bytes::from(value.clone()),
                    read_index,
                    apply_index,
                ) {
                    Ok(result) => ApplyResult::Integer(if result { 1 } else { 0 }),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HMSet { key, fvs } => {
                let fvs_converted: Vec<(&[u8], Bytes)> = fvs
                    .iter()
                    .map(|(f, v)| (f.as_ref(), Bytes::from(v.clone())))
                    .collect();
                match self.hmset(key, fvs_converted, apply_index) {
                    Ok(()) => ApplyResult::Ok,
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HDel { key, fields } => {
                let fields_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_ref()).collect();
                match self.hdel(key, &fields_refs, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HIncrBy { key, field, delta } => {
                match self.hincrby(key, field, *delta, apply_index) {
                    Ok(v) => ApplyResult::Integer(v),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::HIncrByFloat { .. } => {
                // TODO: Implement HINCRBYFLOAT
                ApplyResult::Error(StoreError::Internal("HINCRBYFLOAT not implemented".into()))
            }

            // ==================== Set Read Commands ====================
            Command::SMembers { key } => match self.smembers(key, read_index) {
                Ok(members) => ApplyResult::Array(members.into_iter().map(Some).collect()),
                Err(e) => ApplyResult::Error(e),
            },
            Command::SIsMember { key, member } => match self.sismember(key, member, read_index) {
                Ok(exists) => ApplyResult::Integer(if exists { 1 } else { 0 }),
                Err(e) => ApplyResult::Error(e),
            },
            Command::SCard { key } => match self.scard(key, read_index) {
                Ok(card) => ApplyResult::Integer(card as i64),
                Err(e) => ApplyResult::Error(e),
            },
            Command::SInter { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.sinter(&keys_refs, read_index) {
                    Ok(result) => ApplyResult::Array(result.into_iter().map(Some).collect()),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SUnion { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.sunion(&keys_refs, read_index) {
                    Ok(result) => ApplyResult::Array(result.into_iter().map(Some).collect()),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SDiff { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.sdiff(&keys_refs, read_index) {
                    Ok(result) => ApplyResult::Array(result.into_iter().map(Some).collect()),
                    Err(e) => ApplyResult::Error(e),
                }
            }

            // ==================== Set Write Commands ====================
            Command::SAdd { key, members } => {
                let members_converted: Vec<Bytes> =
                    members.iter().map(|m| Bytes::from(m.clone())).collect();
                match self.sadd(key, members_converted, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SRem { key, members } => {
                let members_refs: Vec<&[u8]> = members.iter().map(|m| m.as_ref()).collect();
                match self.srem(key, &members_refs, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::SPop { key, count } => {
                match self.spop(key, count.unwrap_or(1) as usize, apply_index) {
                    Ok(result) => {
                        if result.len() == 1 {
                            ApplyResult::Value(result.into_iter().next())
                        } else {
                            ApplyResult::Array(result.into_iter().map(Some).collect())
                        }
                    }
                    Err(e) => ApplyResult::Error(e),
                }
            }

            // ==================== ZSet Read Commands ====================
            Command::ZScore { key, member } => match self.zscore(key, member, read_index) {
                Ok(Some(score)) => ApplyResult::Value(Some(Bytes::from(score.to_string()))),
                Ok(None) => ApplyResult::Value(None),
                Err(e) => ApplyResult::Error(e),
            },
            Command::ZRank { key, member } => match self.zrank(key, member, read_index) {
                Ok(Some(rank)) => ApplyResult::Integer(rank as i64),
                Ok(None) => ApplyResult::Value(None),
                Err(e) => ApplyResult::Error(e),
            },
            Command::ZRange {
                key,
                start,
                stop,
                with_scores,
            } => match self.zrange(key, *start, *stop, *with_scores, read_index) {
                Ok(members) => {
                    if *with_scores {
                        // Return as array of [member, score, member, score, ...]
                        let mut result = Vec::new();
                        for (member, score) in members {
                            result.push(Some(Bytes::from(member)));
                            result.push(Some(Bytes::from(score.to_string())));
                        }
                        ApplyResult::Array(result)
                    } else {
                        // Return as array of members only
                        ApplyResult::Array(
                            members
                                .into_iter()
                                .map(|(member, _)| Some(Bytes::from(member)))
                                .collect(),
                        )
                    }
                }
                Err(e) => ApplyResult::Error(e),
            },
            Command::ZCard { key } => match self.zcard(key, read_index) {
                Ok(count) => ApplyResult::Integer(count as i64),
                Err(e) => ApplyResult::Error(e),
            },

            // ==================== ZSet Write Commands ====================
            Command::ZAdd { key, members } => {
                let members_vec: Vec<(f64, Bytes)> = members
                    .iter()
                    .map(|(score, member)| (*score, Bytes::from(member.clone())))
                    .collect();
                match self.zadd(key, members_vec, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::ZRem { key, members } => {
                let members_refs: Vec<&[u8]> = members.iter().map(|m| m.as_ref()).collect();
                match self.zrem(key, &members_refs, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::ZIncrBy { key, increment, member } => {
                // ZINCRBY is implemented as ZADD with the current score + increment
                // First get current score, then add increment
                match self.zscore(key, member, read_index) {
                    Ok(Some(current_score)) => {
                        let new_score = current_score + increment;
                        let members_vec = vec![(new_score, Bytes::from(member.clone()))];
                        match self.zadd(key, members_vec, apply_index) {
                            Ok(_) => ApplyResult::Value(Some(Bytes::from(new_score.to_string()))),
                            Err(e) => ApplyResult::Error(e),
                        }
                    }
                    Ok(None) => {
                        // Member doesn't exist, add with increment as score
                        let members_vec = vec![(*increment, Bytes::from(member.clone()))];
                        match self.zadd(key, members_vec, apply_index) {
                            Ok(_) => ApplyResult::Value(Some(Bytes::from(increment.to_string()))),
                            Err(e) => ApplyResult::Error(e),
                        }
                    }
                    Err(e) => ApplyResult::Error(e),
                }
            }

            // ==================== Key Read Commands ====================
            Command::Exists { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.exists(&keys_refs, read_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::Type { key } => match self.key_type(key, read_index) {
                Ok(typ) => ApplyResult::Type(typ),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Ttl { key } => match self.ttl(key, read_index) {
                Ok(ttl) => ApplyResult::Integer(ttl),
                Err(e) => ApplyResult::Error(e),
            },
            Command::PTtl { key } => match self.ttl(key, read_index) {
                Ok(ttl) => ApplyResult::Integer(ttl * 1000),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Keys { pattern } => match self.keys(pattern, read_index) {
                Ok(keys) => ApplyResult::Array(keys.into_iter().map(Some).collect()),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Scan { .. } => {
                // TODO: Implement SCAN command
                ApplyResult::Array(vec![])
            }

            // ==================== Key Write Commands ====================
            Command::Del { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
                match self.del(&keys_refs, apply_index) {
                    Ok(count) => ApplyResult::Integer(count as i64),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::Expire { key, seconds } => match self.expire(key, *seconds, apply_index) {
                Ok(result) => ApplyResult::Integer(if result { 1 } else { 0 }),
                Err(e) => ApplyResult::Error(e),
            },
            Command::PExpire { key, milliseconds } => {
                match self.expire(key, *milliseconds / 1000, apply_index) {
                    Ok(result) => ApplyResult::Integer(if result { 1 } else { 0 }),
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::Persist { key } => match self.persist(key, apply_index) {
                Ok(result) => ApplyResult::Integer(if result { 1 } else { 0 }),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Rename { key, new_key } => {
                match self.rename(key.as_ref(), new_key.as_ref(), apply_index) {
                    Ok(()) => ApplyResult::Ok,
                    Err(e) => ApplyResult::Error(e),
                }
            }
            Command::RenameNx { key, new_key } => {
                match self.renamenx(key.as_ref(), new_key.as_ref(), read_index, apply_index) {
                    Ok(true) => ApplyResult::Integer(1),
                    Ok(false) => ApplyResult::Integer(0),
                    Err(e) => ApplyResult::Error(e),
                }
            }
        }
    }

    /// Apply command with apply_index (for WAL logging)
    ///
    /// This method executes the command and optionally writes it to WAL for recovery.
    /// For stores that don't support WAL (like MemoryStore), this is equivalent to `apply`.
    ///
    /// # Arguments
    /// - `read_index`: Raft read index for linearizability verification (used for read operations)
    /// - `apply_index`: Raft log apply index (used for WAL logging and recovery)
    /// - `cmd`: Command to execute
    ///
    /// # Returns
    /// Command execution result
    fn apply_with_index(&self, read_index: u64, apply_index: u64, cmd: &Command) -> ApplyResult {
        // Default implementation: just call apply (for stores without WAL support)
        // Stores with WAL support (like HybridStore) should override this method
        self.apply(read_index, apply_index, cmd)
    }
}
