//! KV 操作类型定义
//!
//! KVOperation 表示需要通过 Raft 共识的写操作。
//! 可以从 resp::Command 转换而来。

use resp::Command;
use serde::{Deserialize, Serialize};

/// 转换错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotWriteCommandError;

impl std::fmt::Display for NotWriteCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command is not a write operation")
    }
}

impl std::error::Error for NotWriteCommandError {}

/// Redis 写操作类型（用于 Raft 日志）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KVOperation {
    // ==================== String 操作 ====================
    /// SET key value
    Set { key: Vec<u8>, value: Vec<u8>, ex_secs: Option<u64> },
    /// SETNX key value
    SetNx { key: Vec<u8>, value: Vec<u8> },
    /// SETEX key seconds value
    SetEx { key: Vec<u8>, value: Vec<u8>, ttl_secs: u64 },
    /// MSET key value [key value ...]
    MSet { kvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// MSETNX key value [key value ...]
    MSetNx { kvs: Vec<(Vec<u8>, Vec<u8>)> },
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
    /// GETSET key value
    GetSet { key: Vec<u8>, value: Vec<u8> },
    /// SETRANGE key offset value
    SetRange { key: Vec<u8>, offset: i64, value: Vec<u8> },

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
    /// LTRIM key start stop
    LTrim { key: Vec<u8>, start: i64, stop: i64 },
    /// LREM key count value
    LRem { key: Vec<u8>, count: i64, value: Vec<u8> },

    // ==================== Hash 操作 ====================
    /// HSET key field value [field value ...]
    HSet { key: Vec<u8>, fvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// HSETNX key field value
    HSetNx { key: Vec<u8>, field: Vec<u8>, value: Vec<u8> },
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
    /// SPOP key [count]
    SPop { key: Vec<u8>, count: Option<u64> },

    // ==================== 通用操作 ====================
    /// DEL key [key ...]
    Del { keys: Vec<Vec<u8>> },
    /// EXPIRE key seconds
    Expire { key: Vec<u8>, ttl_secs: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Vec<u8>, ttl_ms: u64 },
    /// PERSIST key
    Persist { key: Vec<u8> },
    /// RENAME key newkey
    Rename { key: Vec<u8>, new_key: Vec<u8> },
    /// RENAMENX key newkey
    RenameNx { key: Vec<u8>, new_key: Vec<u8> },
    /// FLUSHDB
    FlushDb,

    /// NoOp - 用于配置变更等
    NoOp,
}

impl TryFrom<Command> for KVOperation {
    type Error = NotWriteCommandError;

    fn try_from(cmd: Command) -> Result<Self, Self::Error> {
        Self::try_from(&cmd)
    }
}

impl TryFrom<&Command> for KVOperation {
    type Error = NotWriteCommandError;

    fn try_from(cmd: &Command) -> Result<Self, Self::Error> {
        match cmd {
            // String 写命令
            Command::Set { key, value, ex, px, nx, xx: _ } => {
                // 如果有 NX 标志，使用 SetNx
                if *nx {
                    Ok(KVOperation::SetNx { key: key.clone(), value: value.clone() })
                } else {
                    // 计算过期时间（秒）
                    let ex_secs = ex.or(px.map(|ms| ms / 1000));
                    Ok(KVOperation::Set { key: key.clone(), value: value.clone(), ex_secs })
                }
            }
            Command::SetNx { key, value } => {
                Ok(KVOperation::SetNx { key: key.clone(), value: value.clone() })
            }
            Command::SetEx { key, seconds, value } => {
                Ok(KVOperation::SetEx { key: key.clone(), value: value.clone(), ttl_secs: *seconds })
            }
            Command::PSetEx { key, milliseconds, value } => {
                Ok(KVOperation::SetEx { key: key.clone(), value: value.clone(), ttl_secs: *milliseconds / 1000 })
            }
            Command::MSet { kvs } => {
                Ok(KVOperation::MSet { kvs: kvs.clone() })
            }
            Command::MSetNx { kvs } => {
                Ok(KVOperation::MSetNx { kvs: kvs.clone() })
            }
            Command::Incr { key } => {
                Ok(KVOperation::Incr { key: key.clone() })
            }
            Command::IncrBy { key, delta } => {
                Ok(KVOperation::IncrBy { key: key.clone(), delta: *delta })
            }
            Command::IncrByFloat { .. } => {
                // TODO: 需要在 KVOperation 中添加 IncrByFloat 支持
                Err(NotWriteCommandError)
            }
            Command::Decr { key } => {
                Ok(KVOperation::Decr { key: key.clone() })
            }
            Command::DecrBy { key, delta } => {
                Ok(KVOperation::DecrBy { key: key.clone(), delta: *delta })
            }
            Command::Append { key, value } => {
                Ok(KVOperation::Append { key: key.clone(), value: value.clone() })
            }
            Command::GetSet { key, value } => {
                Ok(KVOperation::GetSet { key: key.clone(), value: value.clone() })
            }
            Command::SetRange { key, offset, value } => {
                Ok(KVOperation::SetRange { key: key.clone(), offset: *offset, value: value.clone() })
            }

            // List 写命令
            Command::LPush { key, values } => {
                Ok(KVOperation::LPush { key: key.clone(), values: values.clone() })
            }
            Command::RPush { key, values } => {
                Ok(KVOperation::RPush { key: key.clone(), values: values.clone() })
            }
            Command::LPop { key } => {
                Ok(KVOperation::LPop { key: key.clone() })
            }
            Command::RPop { key } => {
                Ok(KVOperation::RPop { key: key.clone() })
            }
            Command::LSet { key, index, value } => {
                Ok(KVOperation::LSet { key: key.clone(), index: *index, value: value.clone() })
            }
            Command::LTrim { key, start, stop } => {
                Ok(KVOperation::LTrim { key: key.clone(), start: *start, stop: *stop })
            }
            Command::LRem { key, count, value } => {
                Ok(KVOperation::LRem { key: key.clone(), count: *count, value: value.clone() })
            }

            // Hash 写命令
            Command::HSet { key, fvs } => {
                Ok(KVOperation::HSet { key: key.clone(), fvs: fvs.clone() })
            }
            Command::HSetNx { key, field, value } => {
                Ok(KVOperation::HSetNx { key: key.clone(), field: field.clone(), value: value.clone() })
            }
            Command::HMSet { key, fvs } => {
                Ok(KVOperation::HMSet { key: key.clone(), fvs: fvs.clone() })
            }
            Command::HDel { key, fields } => {
                Ok(KVOperation::HDel { key: key.clone(), fields: fields.clone() })
            }
            Command::HIncrBy { key, field, delta } => {
                Ok(KVOperation::HIncrBy { key: key.clone(), field: field.clone(), delta: *delta })
            }
            Command::HIncrByFloat { .. } => {
                // TODO: 需要在 KVOperation 中添加 HIncrByFloat 支持
                Err(NotWriteCommandError)
            }

            // Set 写命令
            Command::SAdd { key, members } => {
                Ok(KVOperation::SAdd { key: key.clone(), members: members.clone() })
            }
            Command::SRem { key, members } => {
                Ok(KVOperation::SRem { key: key.clone(), members: members.clone() })
            }
            Command::SPop { key, count } => {
                Ok(KVOperation::SPop { key: key.clone(), count: *count })
            }

            // Key 写命令
            Command::Del { keys } => {
                Ok(KVOperation::Del { keys: keys.clone() })
            }
            Command::Expire { key, seconds } => {
                Ok(KVOperation::Expire { key: key.clone(), ttl_secs: *seconds })
            }
            Command::PExpire { key, milliseconds } => {
                Ok(KVOperation::PExpire { key: key.clone(), ttl_ms: *milliseconds })
            }
            Command::Persist { key } => {
                Ok(KVOperation::Persist { key: key.clone() })
            }
            Command::Rename { key, new_key } => {
                Ok(KVOperation::Rename { key: key.clone(), new_key: new_key.clone() })
            }
            Command::RenameNx { key, new_key } => {
                Ok(KVOperation::RenameNx { key: key.clone(), new_key: new_key.clone() })
            }
            Command::FlushDb => {
                Ok(KVOperation::FlushDb)
            }

            // 读命令 - 不需要通过 Raft
            _ => Err(NotWriteCommandError),
        }
    }
}
