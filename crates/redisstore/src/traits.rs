//! Redis 存储 trait 定义
//!
//! 支持 Redis 的多种数据类型和操作

use resp::Command;

/// 命令执行结果
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyResult {
    /// 简单字符串响应 (OK, PONG 等)
    Ok,
    /// PONG 响应
    Pong(Option<Vec<u8>>),
    /// 整数响应
    Integer(i64),
    /// 字符串响应（可能为 nil）
    Value(Option<Vec<u8>>),
    /// 数组响应
    Array(Vec<Option<Vec<u8>>>),
    /// 键值对数组 (用于 HGETALL 等)
    KeyValues(Vec<(Vec<u8>, Vec<u8>)>),
    /// 类型字符串
    Type(Option<&'static str>),
    /// 错误响应
    Error(StoreError),
}

/// Redis 存储错误
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// 键不存在
    KeyNotFound,
    /// 类型不匹配（如对 String 执行 List 操作）
    WrongType,
    /// 索引越界
    IndexOutOfRange,
    /// 无效参数
    InvalidArgument(String),
    /// 内部错误
    Internal(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::KeyNotFound => write!(f, "key not found"),
            StoreError::WrongType => {
                write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            StoreError::IndexOutOfRange => write!(f, "index out of range"),
            StoreError::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
            StoreError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for StoreError {}

pub type StoreResult<T> = Result<T, StoreError>;

/// Redis 存储抽象 trait
///
/// 定义了 Redis 兼容的键值存储操作，支持：
/// - String: GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN
/// - List: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX
/// - Hash: HGET, HSET, HDEL, HGETALL, HKEYS, HVALS, HLEN
/// - Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
/// - 通用: DEL, EXISTS, KEYS, TYPE, TTL, EXPIRE, DBSIZE, FLUSHDB
///
/// 后续可以轻松替换为 RocksDB 等持久化存储
pub trait RedisStore: Send + Sync {
    // ==================== String 操作 ====================

    /// GET: 获取字符串值
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// SET: 设置字符串值
    fn set(&self, key: Vec<u8>, value: Vec<u8>);

    /// SETNX: 仅当键不存在时设置
    fn setnx(&self, key: Vec<u8>, value: Vec<u8>) -> bool;

    /// SETEX: 设置值并指定过期时间（秒）
    fn setex(&self, key: Vec<u8>, value: Vec<u8>, ttl_secs: u64);

    /// MGET: 批量获取
    fn mget(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>>;

    /// MSET: 批量设置
    fn mset(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>);

    /// INCR: 整数自增 1
    fn incr(&self, key: &[u8]) -> StoreResult<i64>;

    /// INCRBY: 整数自增指定值
    fn incrby(&self, key: &[u8], delta: i64) -> StoreResult<i64>;

    /// DECR: 整数自减 1
    fn decr(&self, key: &[u8]) -> StoreResult<i64>;

    /// DECRBY: 整数自减指定值
    fn decrby(&self, key: &[u8], delta: i64) -> StoreResult<i64>;

    /// APPEND: 追加字符串
    fn append(&self, key: &[u8], value: &[u8]) -> usize;

    /// STRLEN: 获取字符串长度
    fn strlen(&self, key: &[u8]) -> usize;

    /// GETSET: 设置新值并返回旧值
    fn getset(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>>;

    // ==================== List 操作 ====================

    /// LPUSH: 从左侧插入元素
    fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize;

    /// RPUSH: 从右侧插入元素
    fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> usize;

    /// LPOP: 从左侧弹出元素
    fn lpop(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// RPOP: 从右侧弹出元素
    fn rpop(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// LRANGE: 获取列表范围
    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Vec<u8>>;

    /// LLEN: 获取列表长度
    fn llen(&self, key: &[u8]) -> usize;

    /// LINDEX: 获取指定索引的元素
    fn lindex(&self, key: &[u8], index: i64) -> Option<Vec<u8>>;

    /// LSET: 设置指定索引的元素
    fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> StoreResult<()>;

    // ==================== Hash 操作 ====================

    /// HGET: 获取 hash 字段值
    fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>>;

    /// HSET: 设置 hash 字段值
    fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> bool;

    /// HMGET: 批量获取 hash 字段
    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> Vec<Option<Vec<u8>>>;

    /// HMSET: 批量设置 hash 字段
    fn hmset(&self, key: &[u8], fvs: Vec<(Vec<u8>, Vec<u8>)>);

    /// HDEL: 删除 hash 字段
    fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> usize;

    /// HEXISTS: 检查 hash 字段是否存在
    fn hexists(&self, key: &[u8], field: &[u8]) -> bool;

    /// HGETALL: 获取所有 hash 字段和值
    fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// HKEYS: 获取所有 hash 字段名
    fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// HVALS: 获取所有 hash 字段值
    fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// HLEN: 获取 hash 字段数量
    fn hlen(&self, key: &[u8]) -> usize;

    /// HINCRBY: hash 字段整数自增
    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> StoreResult<i64>;

    // ==================== Set 操作 ====================

    /// SADD: 添加集合成员
    fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> usize;

    /// SREM: 删除集合成员
    fn srem(&self, key: &[u8], members: &[&[u8]]) -> usize;

    /// SMEMBERS: 获取所有集合成员
    fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>>;

    /// SISMEMBER: 检查是否为集合成员
    fn sismember(&self, key: &[u8], member: &[u8]) -> bool;

    /// SCARD: 获取集合大小
    fn scard(&self, key: &[u8]) -> usize;

    // ==================== 通用操作 ====================

    /// DEL: 删除键（支持多个）
    fn del(&self, keys: &[&[u8]]) -> usize;

    /// EXISTS: 检查键是否存在（支持多个）
    fn exists(&self, keys: &[&[u8]]) -> usize;

    /// KEYS: 获取匹配模式的所有键（简化版，只支持 * 通配符）
    fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>>;

    /// TYPE: 获取键的类型
    fn key_type(&self, key: &[u8]) -> Option<&'static str>;

    /// TTL: 获取剩余过期时间（秒），-1 表示永不过期，-2 表示键不存在
    fn ttl(&self, key: &[u8]) -> i64;

    /// EXPIRE: 设置过期时间（秒）
    fn expire(&self, key: &[u8], ttl_secs: u64) -> bool;

    /// PERSIST: 移除过期时间
    fn persist(&self, key: &[u8]) -> bool;

    /// DBSIZE: 获取键值对数量
    fn dbsize(&self) -> usize;

    /// FLUSHDB: 清空所有数据
    fn flushdb(&self);

    /// RENAME: 重命名键
    fn rename(&self, key: &[u8], new_key: Vec<u8>) -> StoreResult<()>;

    // ==================== 快照操作 ====================

    /// 从快照数据恢复
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String>;

    /// 创建快照数据
    fn create_snapshot(&self) -> Result<Vec<u8>, String>;

    /// 创建分裂快照 - 只包含指定槽位范围内的数据
    ///
    /// # 参数
    /// - `slot_start`: 起始槽位（包含）
    /// - `slot_end`: 结束槽位（不包含）
    /// - `total_slots`: 总槽位数（用于计算 key 的槽位）
    ///
    /// # 返回
    /// 只包含 slot ∈ [slot_start, slot_end) 的 key 的快照数据
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String>;

    /// 从分裂快照恢复 - 合并到现有数据
    ///
    /// 与 restore_from_snapshot 不同，此方法不会清空现有数据，
    /// 而是将快照中的数据合并进来。
    fn merge_from_snapshot(&self, snapshot: &[u8]) -> Result<usize, String>;

    /// 删除指定槽位范围内的所有 key
    ///
    /// 用于分裂完成后源分片清理已转移的数据
    ///
    /// # 参数
    /// - `slot_start`: 起始槽位（包含）
    /// - `slot_end`: 结束槽位（不包含）
    /// - `total_slots`: 总槽位数
    ///
    /// # 返回
    /// 删除的 key 数量
    fn delete_keys_in_slot_range(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> usize;

    // ==================== 命令执行 ====================

    /// 执行 Redis 命令
    ///
    /// 统一的命令执行入口，根据 Command 类型调用对应的操作方法
    fn apply(&self, cmd: &Command) -> ApplyResult {
        match cmd {
            // ==================== 连接/管理命令 ====================
            Command::Ping { message } => ApplyResult::Pong(message.clone()),
            Command::Echo { message } => ApplyResult::Value(Some(message.clone())),
            Command::DbSize => ApplyResult::Integer(self.dbsize() as i64),
            Command::FlushDb => {
                self.flushdb();
                ApplyResult::Ok
            }
            Command::CommandInfo | Command::Info { .. } => {
                // 这些命令由上层处理
                ApplyResult::Ok
            }

            // ==================== String 读命令 ====================
            Command::Get { key } => ApplyResult::Value(self.get(key)),
            Command::MGet { keys } => {
                let keys_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                ApplyResult::Array(self.mget(&keys_refs))
            }
            Command::StrLen { key } => ApplyResult::Integer(self.strlen(key) as i64),
            Command::GetRange { key, start, end } => {
                // TODO: 实现 GETRANGE
                let _ = (key, start, end);
                ApplyResult::Value(None)
            }

            // ==================== String 写命令 ====================
            Command::Set { key, value, ex, px, nx, xx } => {
                // 处理 XX 条件：键必须存在
                if *xx && self.get(key).is_none() {
                    return ApplyResult::Value(None);
                }
                // 处理 NX 条件：键必须不存在
                if *nx {
                    if !self.setnx(key.clone(), value.clone()) {
                        return ApplyResult::Value(None);
                    }
                } else {
                    self.set(key.clone(), value.clone());
                }
                // 处理过期时间
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
                // 检查所有键是否都不存在
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
                // TODO: 实现 INCRBYFLOAT
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
                // TODO: 实现 SETRANGE
                let _ = (key, offset, value);
                ApplyResult::Integer(0)
            }

            // ==================== List 读命令 ====================
            Command::LLen { key } => ApplyResult::Integer(self.llen(key) as i64),
            Command::LIndex { key, index } => ApplyResult::Value(self.lindex(key, *index)),
            Command::LRange { key, start, stop } => {
                let list = self.lrange(key, *start, *stop);
                ApplyResult::Array(list.into_iter().map(Some).collect())
            }

            // ==================== List 写命令 ====================
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
            Command::LTrim { key, start, stop } => {
                // TODO: 实现 LTRIM
                let _ = (key, start, stop);
                ApplyResult::Ok
            }
            Command::LRem { key, count, value } => {
                // TODO: 实现 LREM
                let _ = (key, count, value);
                ApplyResult::Integer(0)
            }

            // ==================== Hash 读命令 ====================
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

            // ==================== Hash 写命令 ====================
            Command::HSet { key, fvs } => {
                self.hmset(key, fvs.clone());
                ApplyResult::Integer(fvs.len() as i64)
            }
            Command::HSetNx { key, field, value } => {
                if !self.hexists(key, field) {
                    self.hset(key, field.clone(), value.clone());
                    ApplyResult::Integer(1)
                } else {
                    ApplyResult::Integer(0)
                }
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
                // TODO: 实现 HINCRBYFLOAT
                ApplyResult::Error(StoreError::Internal("HINCRBYFLOAT not implemented".into()))
            }

            // ==================== Set 读命令 ====================
            Command::SMembers { key } => {
                let members = self.smembers(key);
                ApplyResult::Array(members.into_iter().map(Some).collect())
            }
            Command::SIsMember { key, member } => {
                ApplyResult::Integer(if self.sismember(key, member) { 1 } else { 0 })
            }
            Command::SCard { key } => ApplyResult::Integer(self.scard(key) as i64),
            Command::SInter { .. } | Command::SUnion { .. } | Command::SDiff { .. } => {
                // TODO: 实现集合运算
                ApplyResult::Array(vec![])
            }

            // ==================== Set 写命令 ====================
            Command::SAdd { key, members } => {
                let count = self.sadd(key, members.clone());
                ApplyResult::Integer(count as i64)
            }
            Command::SRem { key, members } => {
                let members_refs: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
                let count = self.srem(key, &members_refs);
                ApplyResult::Integer(count as i64)
            }
            Command::SPop { .. } => {
                // TODO: 实现 SPOP
                ApplyResult::Value(None)
            }

            // ==================== Key 读命令 ====================
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
                // TODO: 实现 SCAN 命令
                ApplyResult::Array(vec![])
            }

            // ==================== Key 写命令 ====================
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
            Command::RenameNx { key, new_key } => {
                if self.get(new_key).is_some() {
                    ApplyResult::Integer(0)
                } else {
                    match self.rename(key, new_key.clone()) {
                        Ok(()) => ApplyResult::Integer(1),
                        Err(e) => ApplyResult::Error(e),
                    }
                }
            }
        }
    }
}
