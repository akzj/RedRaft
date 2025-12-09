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

use resp::Command;

// ============================================================================
// Error Types
// ============================================================================

/// Command execution result
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    /// Simple string response (OK, PONG, etc.)
    Ok,
    /// PONG response
    Pong(Option<Vec<u8>>),
    /// Integer response
    Integer(i64),
    /// String response (may be nil)
    Value(Option<Vec<u8>>),
    /// Array response
    Array(Vec<Option<Vec<u8>>>),
    /// Key-value array (used for HGETALL, etc.)
    KeyValues(Vec<(Vec<u8>, Vec<u8>)>),
    /// Type string
    Type(Option<&'static str>),
    /// Error response
    Error(StoreError),
}

/// Redis storage error
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::KeyNotFound => write!(f, "key not found"),
            StoreError::WrongType => {
                write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            StoreError::IndexOutOfRange => write!(f, "index out of range"),
            StoreError::NotSupported => write!(f, "operation not supported"),
            StoreError::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
            StoreError::Internal(msg) => write!(f, "internal error: {}", msg),
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
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// SET: Set string value
    fn set(&self, key: Vec<u8>, value: Vec<u8>);

    /// SETNX: Set only if key does not exist
    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool;

    /// SETEX: Set value with expiration time (seconds)
    fn setex(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64);

    /// MGET: Batch get
    fn mget(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// MSET: Batch set
    fn mset(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) {
        for (k, v) in kvs {
            self.set(k, v);
        }
    }

    /// INCR: Increment integer by 1
    fn incr(&self, key: &[u8]) -> StoreResult<i64> {
        self.incrby(key, 1)
    }

    /// INCRBY: Increment integer by specified value
    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64>;

    /// DECR: Decrement integer by 1
    fn decr(&self, key: &[u8]) -> StoreResult<i64> {
        self.incrby(key, -1)
    }

    /// DECRBY: Decrement integer by specified value
    fn decrby(&self, key: &[u8], delta: i64) -> StoreResult<i64> {
        self.incrby(key, -delta)
    }

    /// APPEND: Append string
    fn append(&self, key: &[u8], value: &[u8]) -> usize;

    /// STRLEN: Get string length
    fn strlen(&self, key: &[u8]) -> usize;

    /// GETSET: Set new value and return old value
    fn getset(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        let old = self.get(&key);
        self.set(key, value);
        old
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
    fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize;

    /// RPUSH: Insert elements from right
    fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize;

    /// LPOP: Pop element from left
    fn lpop(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// RPOP: Pop element from right
    fn rpop(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// LRANGE: Get list range
    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>>;

    /// LLEN: Get list length
    fn llen(&self, key: &[u8]) -> usize;

    /// LINDEX: Get element at specified index
    fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>>;

    /// LSET: Set element at specified index
    fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()>;

    /// LTRIM: Trim list to specified range
    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> StoreResult<()> {
        let _ = (key, start, stop);
        Err(StoreError::NotSupported)
    }

    /// LREM: Remove elements from list
    fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> usize {
        let _ = (key, count, value);
        0
    }

    /// LINSERT: Insert element before or after pivot
    fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], value: Vec<u8>) -> StoreResult<i64> {
        let _ = (key, before, pivot, value);
        Err(StoreError::NotSupported)
    }

    /// RPOPLPUSH: Pop from right, push to left of another list
    fn rpoplpush(&self, source: &[u8], destination: &[u8]) -> Option<Vec<u8>> {
        let _ = (source, destination);
        None
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
    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>>;

    /// HSET: Set hash field value, returns true if field is new
    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool;

    /// HMGET: Batch get hash fields
    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        fields.iter().map(|f| self.hget(key, f)).collect()
    }

    /// HMSET: Batch set hash fields
    fn hmset(&self, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>) {
        for (f, v) in fvs {
            self.hset(key, f, v);
        }
    }

    /// HDEL: Delete hash fields
    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize;

    /// HEXISTS: Check if hash field exists
    fn hexists(&self, key: &[u8], field: &[u8]) -> bool {
        self.hget(key, field).is_some()
    }

    /// HGETALL: Get all hash fields and values
    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// HKEYS: Get all hash field names
    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// HVALS: Get all hash field values
    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// HLEN: Get number of hash fields
    fn hlen(&self, key: &[u8]) -> usize;

    /// HINCRBY: Increment hash field integer
    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64>;

    /// HSETNX: Set hash field only if it does not exist
    fn hsetnx(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool {
        if self.hexists(key, &field) {
            false
        } else {
            self.hset(key, field, value);
            true
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
    fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> usize;

    /// SREM: Remove set members
    fn srem(&self, key: &[u8], members: &[&[u8]]) -> usize;

    /// SMEMBERS: Get all set members
    fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// SISMEMBER: Check if set member exists
    fn sismember(&self, key: &[u8], member: &[u8]) -> bool;

    /// SCARD: Get set size
    fn scard(&self, key: &[u8]) -> usize;

    /// SPOP: Remove and return random member(s)
    fn spop(&self, key: &[u8], count: usize) -> Vec<Vec<u8>> {
        let _ = (key, count);
        Vec::new()
    }

    /// SRANDMEMBER: Get random member(s) without removing
    fn srandmember(&self, key: &[u8], count: i64) -> Vec<Vec<u8>> {
        let _ = (key, count);
        Vec::new()
    }

    /// SINTER: Intersection of multiple sets
    fn sinter(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        let _ = keys;
        Vec::new()
    }

    /// SUNION: Union of multiple sets
    fn sunion(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        let _ = keys;
        Vec::new()
    }

    /// SDIFF: Difference of multiple sets
    fn sdiff(&self, keys: &[&[u8]]) -> Vec<Vec<u8>> {
        let _ = keys;
        Vec::new()
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
    fn zadd(&self, key: &[u8], members: Vec<(f64, Vec<u8>)>) -> usize;

    /// ZREM: Remove members
    fn zrem(&self, key: &[u8], members: &[&[u8]]) -> usize;

    /// ZSCORE: Get member score
    fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64>;

    /// ZRANK: Get member rank (0-based)
    fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize>;

    /// ZREVRANK: Get member reverse rank
    fn zrevrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        let _ = (key, member);
        None
    }

    /// ZRANGE: Get members by rank range
    fn zrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Vec<(Vec<u8>, f64)>;

    /// ZREVRANGE: Get members by reverse rank range
    fn zrevrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Vec<(Vec<u8>, f64)> {
        let _ = (key, start, stop, with_scores);
        Vec::new()
    }

    /// ZRANGEBYSCORE: Get members by score range
    fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
        with_scores: bool,
        offset: Option<usize>,
        count: Option<usize>,
    ) -> Vec<(Vec<u8>, f64)> {
        let _ = (key, min, max, with_scores, offset, count);
        Vec::new()
    }

    /// ZCARD: Get sorted set size
    fn zcard(&self, key: &[u8]) -> usize;

    /// ZCOUNT: Count members in score range
    fn zcount(&self, key: &[u8], min: f64, max: f64) -> usize {
        let _ = (key, min, max);
        0
    }

    /// ZINCRBY: Increment member score
    fn zincrby(&self, key: &[u8], delta: f64, member: &[u8]) -> StoreResult<f64>;

    /// ZINTERSTORE: Store intersection of sorted sets
    fn zinterstore(&self, destination: &[u8], keys: &[&[u8]], weights: Option<&[f64]>) -> usize {
        let _ = (destination, keys, weights);
        0
    }

    /// ZUNIONSTORE: Store union of sorted sets
    fn zunionstore(&self, destination: &[u8], keys: &[&[u8]], weights: Option<&[f64]>) -> usize {
        let _ = (destination, keys, weights);
        0
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
    fn del(&self, keys: &[&[u8]]) -> usize;

    /// EXISTS: Check if keys exist (supports multiple)
    fn exists(&self, keys: &[&[u8]]) -> usize;

    /// KEYS: Get all keys matching pattern (simplified, only * wildcard)
    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>>;

    /// TYPE: Get key type
    fn key_type(&self, key: &[u8]) -> Option<&'static str>;

    /// TTL: Get remaining expiration time (seconds), -1 for never expire, -2 for key not found
    fn ttl(&self, key: &[u8]) -> i64;

    /// EXPIRE: Set expiration time (seconds)
    fn expire(&self, key: &[u8], ttl_secs: u64) -> bool;

    /// PERSIST: Remove expiration time
    fn persist(&self, key: &[u8]) -> bool;

    /// DBSIZE: Get number of key-value pairs
    fn dbsize(&self) -> usize;

    /// FLUSHDB: Clear all data
    fn flushdb(&self);

    /// RENAME: Rename key
    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()>;

    /// RENAMENX: Rename key only if new key does not exist
    fn renamenx(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<bool> {
        if self.exists(&[&new_key]) > 0 {
            Ok(false)
        } else {
            self.rename(key, new_key)?;
            Ok(true)
        }
    }
}

// ============================================================================
// Snapshot Store Trait
// ============================================================================

/// Snapshot operations for persistence and replication
pub trait SnapshotStore: Send + Sync {
    /// Create snapshot data
    fn create_snapshot(&self) -> Result<Vec<u8>, String>;

    /// Restore from snapshot data
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String>;

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
    ) -> Result<Vec<u8>, String>;

    /// Restore from split snapshot - merge into existing data
    ///
    /// Unlike restore_from_snapshot, this method does not clear existing data,
    /// but merges the snapshot data into it.
    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String>;

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
    ) -> usize;
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
    StringStore + ListStore + HashStore + SetStore + KeyStore + SnapshotStore + Send + Sync
{
    /// Execute Redis command
    ///
    /// Unified command execution entry, calls corresponding operation method based on Command type
    fn apply(&self, cmd: &Command) -> ApplyResult {
        match cmd {
            // ==================== Connection/Management Commands ====================
            Command::Ping { message } => ApplyResult::Pong(message.clone()),
            Command::Echo { message } => ApplyResult::Value(Some(message.clone())),
            Command::DbSize => ApplyResult::Integer(self.dbsize() as i64),
            Command::FlushDb => {
                self.flushdb();
                ApplyResult::Ok
            }
            Command::CommandInfo | Command::Info { .. } => {
                // These commands are handled by the upper layer
                ApplyResult::Ok
            }

            // ==================== String Read Commands ====================
            Command::Get { key } => ApplyResult::Value(self.get(key)),
            Command::MGet { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                ApplyResult::Array(self.mget(&keys_refs))
            }
            Command::StrLen { key } => ApplyResult::Integer(self.strlen(key) as i64),
            Command::GetRange { key, start, end } => {
                // TODO: Implement GETRANGE
                let _ = (key, start, end);
                ApplyResult::Value(None)
            }

            // ==================== String Write Commands ====================
            Command::Set { key, value, ex, px, nx, xx } => {
                // Handle XX condition: key must exist
                if *xx && self.get(key).is_none() {
                    return ApplyResult::Value(None);
                }
                // Handle NX condition: key must not exist
                if *nx {
                    if !self.setnx(key.clone(), value.clone()) {
                        return ApplyResult::Value(None);
                    }
                } else {
                    self.set(key.clone(), value.clone());
                }
                // Handle expiration time
                if let Some(secs) = ex {
                    self.expire(key, *secs);
                } else if let Some(ms) = px {
                    self.expire(key, *ms / 1000);
                }
                ApplyResult::Ok
            }
            Command::SetNx { key, value } => {
                let result = self.setnx(key.clone(), value.clone());
                ApplyResult::Integer(if result { 1 } else { 0 })
            }
            Command::SetEx { key, seconds, value } => {
                self.setex(key.clone(), value.clone(), *seconds);
                ApplyResult::Ok
            }
            Command::PSetEx { key, milliseconds, value } => {
                self.setex(key.clone(), value.clone(), *milliseconds / 1000);
                ApplyResult::Ok
            }
            Command::MSet { kvs } => {
                self.mset(kvs.clone());
                ApplyResult::Ok
            }
            Command::MSetNx { kvs } => {
                // Check if all keys do not exist
                let all_new = kvs.iter().all(|(k, _)| self.get(k).is_none());
                if all_new {
                    self.mset(kvs.clone());
                    ApplyResult::Integer(1)
                } else {
                    ApplyResult::Integer(0)
                }
            }
            Command::Incr { key } => match self.incr(key) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::IncrBy { key, delta } => match self.incrby(key, *delta) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::IncrByFloat { .. } => {
                // TODO: Implement INCRBYFLOAT
                ApplyResult::Error(StoreError::Internal("INCRBYFLOAT not implemented".into()))
            }
            Command::Decr { key } => match self.decr(key) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::DecrBy { key, delta } => match self.decrby(key, *delta) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::Append { key, value } => {
                let len = self.append(key, value);
                ApplyResult::Integer(len as i64)
            }
            Command::GetSet { key, value } => {
                let old = self.getset(key.clone(), value.clone());
                ApplyResult::Value(old)
            }
            Command::SetRange { key, offset, value } => {
                // TODO: Implement SETRANGE
                let _ = (key, offset, value);
                ApplyResult::Integer(0)
            }

            // ==================== List Read Commands ====================
            Command::LLen { key } => ApplyResult::Integer(self.llen(key) as i64),
            Command::LIndex { key, index } => ApplyResult::Value(self.lindex(key, *index)),
            Command::LRange { key, start, stop } => {
                let list = self.lrange(key, *start, *stop);
                ApplyResult::Array(list.into_iter().map(Some).collect())
            }

            // ==================== List Write Commands ====================
            Command::LPush { key, values } => {
                let len = self.lpush(key, values.clone());
                ApplyResult::Integer(len as i64)
            }
            Command::RPush { key, values } => {
                let len = self.rpush(key, values.clone());
                ApplyResult::Integer(len as i64)
            }
            Command::LPop { key } => ApplyResult::Value(self.lpop(key)),
            Command::RPop { key } => ApplyResult::Value(self.rpop(key)),
            Command::LSet { key, index, value } => match self.lset(key, *index, value.clone()) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::LTrim { key, start, stop } => match self.ltrim(key, *start, *stop) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::LRem { key, count, value } => {
                let removed = self.lrem(key, *count, value);
                ApplyResult::Integer(removed as i64)
            }

            // ==================== Hash Read Commands ====================
            Command::HGet { key, field } => ApplyResult::Value(self.hget(key, field)),
            Command::HMGet { key, fields } => {
                let fields_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_slice()).collect();
                ApplyResult::Array(self.hmget(key, &fields_refs))
            }
            Command::HGetAll { key } => ApplyResult::KeyValues(self.hgetall(key)),
            Command::HKeys { key } => {
                let keys = self.hkeys(key);
                ApplyResult::Array(keys.into_iter().map(Some).collect())
            }
            Command::HVals { key } => {
                let vals = self.hvals(key);
                ApplyResult::Array(vals.into_iter().map(Some).collect())
            }
            Command::HLen { key } => ApplyResult::Integer(self.hlen(key) as i64),
            Command::HExists { key, field } => {
                ApplyResult::Integer(if self.hexists(key, field) { 1 } else { 0 })
            }

            // ==================== Hash Write Commands ====================
            Command::HSet { key, fvs } => {
                self.hmset(key, fvs.clone());
                ApplyResult::Integer(fvs.len() as i64)
            }
            Command::HSetNx { key, field, value } => {
                let result = self.hsetnx(key, field.clone(), value.clone());
                ApplyResult::Integer(if result { 1 } else { 0 })
            }
            Command::HMSet { key, fvs } => {
                self.hmset(key, fvs.clone());
                ApplyResult::Ok
            }
            Command::HDel { key, fields } => {
                let fields_refs: Vec<&[u8]> = fields.iter().map(|f| f.as_slice()).collect();
                let count = self.hdel(key, &fields_refs);
                ApplyResult::Integer(count as i64)
            }
            Command::HIncrBy { key, field, delta } => match self.hincrby(key, field, *delta) {
                Ok(v) => ApplyResult::Integer(v),
                Err(e) => ApplyResult::Error(e),
            },
            Command::HIncrByFloat { .. } => {
                // TODO: Implement HINCRBYFLOAT
                ApplyResult::Error(StoreError::Internal("HINCRBYFLOAT not implemented".into()))
            }

            // ==================== Set Read Commands ====================
            Command::SMembers { key } => {
                let members = self.smembers(key);
                ApplyResult::Array(members.into_iter().map(Some).collect())
            }
            Command::SIsMember { key, member } => {
                ApplyResult::Integer(if self.sismember(key, member) { 1 } else { 0 })
            }
            Command::SCard { key } => ApplyResult::Integer(self.scard(key) as i64),
            Command::SInter { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let result = self.sinter(&keys_refs);
                ApplyResult::Array(result.into_iter().map(Some).collect())
            }
            Command::SUnion { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let result = self.sunion(&keys_refs);
                ApplyResult::Array(result.into_iter().map(Some).collect())
            }
            Command::SDiff { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let result = self.sdiff(&keys_refs);
                ApplyResult::Array(result.into_iter().map(Some).collect())
            }

            // ==================== Set Write Commands ====================
            Command::SAdd { key, members } => {
                let count = self.sadd(key, members.clone());
                ApplyResult::Integer(count as i64)
            }
            Command::SRem { key, members } => {
                let members_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                let count = self.srem(key, &members_refs);
                ApplyResult::Integer(count as i64)
            }
            Command::SPop { key, count } => {
                let result = self.spop(key, count.unwrap_or(1) as usize);
                if result.len() == 1 {
                    ApplyResult::Value(result.into_iter().next())
                } else {
                    ApplyResult::Array(result.into_iter().map(Some).collect())
                }
            }

            // ==================== Key Read Commands ====================
            Command::Exists { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                ApplyResult::Integer(self.exists(&keys_refs) as i64)
            }
            Command::Type { key } => ApplyResult::Type(self.key_type(key)),
            Command::Ttl { key } => ApplyResult::Integer(self.ttl(key)),
            Command::PTtl { key } => ApplyResult::Integer(self.ttl(key) * 1000),
            Command::Keys { pattern } => {
                let keys = self.keys(pattern);
                ApplyResult::Array(keys.into_iter().map(Some).collect())
            }
            Command::Scan { .. } => {
                // TODO: Implement SCAN command
                ApplyResult::Array(vec![])
            }

            // ==================== Key Write Commands ====================
            Command::Del { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                let count = self.del(&keys_refs);
                ApplyResult::Integer(count as i64)
            }
            Command::Expire { key, seconds } => {
                let result = self.expire(key, *seconds);
                ApplyResult::Integer(if result { 1 } else { 0 })
            }
            Command::PExpire { key, milliseconds } => {
                let result = self.expire(key, *milliseconds / 1000);
                ApplyResult::Integer(if result { 1 } else { 0 })
            }
            Command::Persist { key } => {
                let result = self.persist(key);
                ApplyResult::Integer(if result { 1 } else { 0 })
            }
            Command::Rename { key, new_key } => match self.rename(key, new_key.clone()) {
                Ok(()) => ApplyResult::Ok,
                Err(e) => ApplyResult::Error(e),
            },
            Command::RenameNx { key, new_key } => match self.renamenx(key, new_key.clone()) {
                Ok(true) => ApplyResult::Integer(1),
                Ok(false) => ApplyResult::Integer(0),
                Err(e) => ApplyResult::Error(e),
            },
        }
    }
}
