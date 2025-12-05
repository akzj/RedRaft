//! KV 操作类型定义

use serde::{Deserialize, Serialize};

/// Redis 操作类型
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVOperation {
    // ==================== String 操作 ====================
    /// SET key value
    Set { key: Vec<u8>, value: Vec<u8> },
    /// SETNX key value
    SetNx { key: Vec<u8>, value: Vec<u8> },
    /// SETEX key seconds value
    SetEx { key: Vec<u8>, value: Vec<u8>, ttl_secs: u64 },
    /// MSET key value [key value ...]
    MSet { kvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// INCR key
    Incr { key: Vec<u8> },
    /// INCRBY key delta
    IncrBy { key: Vec<u8>, delta: i64 },
    /// DECR key
    Decr { key: Vec<u8> },
    /// DECRBY key delta
    DecrBy { key: Vec<u8>, delta: i64 },
    /// APPEND key value
    Append { key: Vec<u8>, value: Vec<u8> },
    /// GETSET key value (deprecated, use SET with GET option)
    GetSet { key: Vec<u8>, value: Vec<u8> },

    // ==================== List 操作 ====================
    /// LPUSH key value [value ...]
    LPush { key: Vec<u8>, values: Vec<Vec<u8>> },
    /// RPUSH key value [value ...]
    RPush { key: Vec<u8>, values: Vec<Vec<u8>> },
    /// LPOP key
    LPop { key: Vec<u8> },
    /// RPOP key
    RPop { key: Vec<u8> },
    /// LSET key index value
    LSet { key: Vec<u8>, index: i64, value: Vec<u8> },

    // ==================== Hash 操作 ====================
    /// HSET key field value
    HSet { key: Vec<u8>, field: Vec<u8>, value: Vec<u8> },
    /// HMSET key field value [field value ...]
    HMSet { key: Vec<u8>, fvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// HDEL key field [field ...]
    HDel { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HINCRBY key field delta
    HIncrBy { key: Vec<u8>, field: Vec<u8>, delta: i64 },

    // ==================== Set 操作 ====================
    /// SADD key member [member ...]
    SAdd { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SREM key member [member ...]
    SRem { key: Vec<u8>, members: Vec<Vec<u8>> },

    // ==================== 通用操作 ====================
    /// DEL key [key ...]
    Del { keys: Vec<Vec<u8>> },
    /// EXPIRE key seconds
    Expire { key: Vec<u8>, ttl_secs: u64 },
    /// PERSIST key
    Persist { key: Vec<u8> },
    /// RENAME key newkey
    Rename { key: Vec<u8>, new_key: Vec<u8> },
    /// FLUSHDB
    FlushDb,

    /// NoOp - 用于配置变更等
    NoOp,
}
