//! Redis 存储 trait 定义
//!
//! 支持 Redis 的多种数据类型和操作

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
}
