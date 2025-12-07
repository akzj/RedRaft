//! Redis 命令解析模块
//!
//! 将 RespValue 解析为类型安全的 Command 结构

mod error;
mod result;

pub use error::{CommandError, CommandErrorKind};
pub use result::CommandResult;

use crate::RespValue;

/// 命令类型标记
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    /// 读命令 - 可以本地读取或通过 ReadIndex
    Read,
    /// 写命令 - 必须通过 Raft 共识
    Write,
}

/// Redis 命令
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    // ==================== 连接/管理命令 ====================
    /// PING [message]
    Ping { message: Option<Vec<u8>> },
    /// ECHO message
    Echo { message: Vec<u8> },
    /// COMMAND
    CommandInfo,
    /// INFO [section]
    Info { section: Option<String> },
    /// DBSIZE
    DbSize,
    /// FLUSHDB
    FlushDb,

    // ==================== String 读命令 ====================
    /// GET key
    Get { key: Vec<u8> },
    /// MGET key [key ...]
    MGet { keys: Vec<Vec<u8>> },
    /// STRLEN key
    StrLen { key: Vec<u8> },
    /// GETRANGE key start end
    GetRange { key: Vec<u8>, start: i64, end: i64 },

    // ==================== String 写命令 ====================
    /// SET key value [EX seconds] [PX milliseconds] [NX|XX]
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    },
    /// SETNX key value
    SetNx { key: Vec<u8>, value: Vec<u8> },
    /// SETEX key seconds value
    SetEx { key: Vec<u8>, seconds: u64, value: Vec<u8> },
    /// PSETEX key milliseconds value
    PSetEx { key: Vec<u8>, milliseconds: u64, value: Vec<u8> },
    /// MSET key value [key value ...]
    MSet { kvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// MSETNX key value [key value ...]
    MSetNx { kvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// INCR key
    Incr { key: Vec<u8> },
    /// INCRBY key increment
    IncrBy { key: Vec<u8>, delta: i64 },
    /// INCRBYFLOAT key increment
    IncrByFloat { key: Vec<u8>, delta: f64 },
    /// DECR key
    Decr { key: Vec<u8> },
    /// DECRBY key decrement
    DecrBy { key: Vec<u8>, delta: i64 },
    /// APPEND key value
    Append { key: Vec<u8>, value: Vec<u8> },
    /// GETSET key value
    GetSet { key: Vec<u8>, value: Vec<u8> },
    /// SETRANGE key offset value
    SetRange { key: Vec<u8>, offset: i64, value: Vec<u8> },

    // ==================== List 读命令 ====================
    /// LLEN key
    LLen { key: Vec<u8> },
    /// LINDEX key index
    LIndex { key: Vec<u8>, index: i64 },
    /// LRANGE key start stop
    LRange { key: Vec<u8>, start: i64, stop: i64 },

    // ==================== List 写命令 ====================
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

    // ==================== Hash 读命令 ====================
    /// HGET key field
    HGet { key: Vec<u8>, field: Vec<u8> },
    /// HMGET key field [field ...]
    HMGet { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HGETALL key
    HGetAll { key: Vec<u8> },
    /// HKEYS key
    HKeys { key: Vec<u8> },
    /// HVALS key
    HVals { key: Vec<u8> },
    /// HLEN key
    HLen { key: Vec<u8> },
    /// HEXISTS key field
    HExists { key: Vec<u8>, field: Vec<u8> },

    // ==================== Hash 写命令 ====================
    /// HSET key field value [field value ...]
    HSet { key: Vec<u8>, fvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// HSETNX key field value
    HSetNx { key: Vec<u8>, field: Vec<u8>, value: Vec<u8> },
    /// HMSET key field value [field value ...]
    HMSet { key: Vec<u8>, fvs: Vec<(Vec<u8>, Vec<u8>)> },
    /// HDEL key field [field ...]
    HDel { key: Vec<u8>, fields: Vec<Vec<u8>> },
    /// HINCRBY key field increment
    HIncrBy { key: Vec<u8>, field: Vec<u8>, delta: i64 },
    /// HINCRBYFLOAT key field increment
    HIncrByFloat { key: Vec<u8>, field: Vec<u8>, delta: f64 },

    // ==================== Set 读命令 ====================
    /// SMEMBERS key
    SMembers { key: Vec<u8> },
    /// SISMEMBER key member
    SIsMember { key: Vec<u8>, member: Vec<u8> },
    /// SCARD key
    SCard { key: Vec<u8> },
    /// SINTER key [key ...]
    SInter { keys: Vec<Vec<u8>> },
    /// SUNION key [key ...]
    SUnion { keys: Vec<Vec<u8>> },
    /// SDIFF key [key ...]
    SDiff { keys: Vec<Vec<u8>> },

    // ==================== Set 写命令 ====================
    /// SADD key member [member ...]
    SAdd { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SREM key member [member ...]
    SRem { key: Vec<u8>, members: Vec<Vec<u8>> },
    /// SPOP key [count]
    SPop { key: Vec<u8>, count: Option<u64> },

    // ==================== Key 命令 ====================
    /// DEL key [key ...]
    Del { keys: Vec<Vec<u8>> },
    /// EXISTS key [key ...]
    Exists { keys: Vec<Vec<u8>> },
    /// EXPIRE key seconds
    Expire { key: Vec<u8>, seconds: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Vec<u8>, milliseconds: u64 },
    /// TTL key
    Ttl { key: Vec<u8> },
    /// PTTL key
    PTtl { key: Vec<u8> },
    /// PERSIST key
    Persist { key: Vec<u8> },
    /// TYPE key
    Type { key: Vec<u8> },
    /// RENAME key newkey
    Rename { key: Vec<u8>, new_key: Vec<u8> },
    /// RENAMENX key newkey
    RenameNx { key: Vec<u8>, new_key: Vec<u8> },
    /// KEYS pattern
    Keys { pattern: Vec<u8> },
    /// SCAN cursor [MATCH pattern] [COUNT count]
    Scan { cursor: u64, pattern: Option<Vec<u8>>, count: Option<u64> },
}

impl Command {
    /// 获取命令类型（读/写）
    pub fn command_type(&self) -> CommandType {
        match self {
            // 管理命令
            Command::Ping { .. } | Command::Echo { .. } | Command::CommandInfo |
            Command::Info { .. } | Command::DbSize => CommandType::Read,
            Command::FlushDb => CommandType::Write,

            // String 命令
            Command::Get { .. } | Command::MGet { .. } | Command::StrLen { .. } |
            Command::GetRange { .. } => CommandType::Read,
            Command::Set { .. } | Command::SetNx { .. } | Command::SetEx { .. } |
            Command::PSetEx { .. } | Command::MSet { .. } | Command::MSetNx { .. } |
            Command::Incr { .. } | Command::IncrBy { .. } | Command::IncrByFloat { .. } |
            Command::Decr { .. } | Command::DecrBy { .. } | Command::Append { .. } |
            Command::GetSet { .. } | Command::SetRange { .. } => CommandType::Write,

            // List 命令
            Command::LLen { .. } | Command::LIndex { .. } | Command::LRange { .. } => CommandType::Read,
            Command::LPush { .. } | Command::RPush { .. } | Command::LPop { .. } |
            Command::RPop { .. } | Command::LSet { .. } | Command::LTrim { .. } |
            Command::LRem { .. } => CommandType::Write,

            // Hash 命令
            Command::HGet { .. } | Command::HMGet { .. } | Command::HGetAll { .. } |
            Command::HKeys { .. } | Command::HVals { .. } | Command::HLen { .. } |
            Command::HExists { .. } => CommandType::Read,
            Command::HSet { .. } | Command::HSetNx { .. } | Command::HMSet { .. } |
            Command::HDel { .. } | Command::HIncrBy { .. } | Command::HIncrByFloat { .. } => CommandType::Write,

            // Set 命令
            Command::SMembers { .. } | Command::SIsMember { .. } | Command::SCard { .. } |
            Command::SInter { .. } | Command::SUnion { .. } | Command::SDiff { .. } => CommandType::Read,
            Command::SAdd { .. } | Command::SRem { .. } | Command::SPop { .. } => CommandType::Write,

            // Key 命令
            Command::Exists { .. } | Command::Ttl { .. } | Command::PTtl { .. } |
            Command::Type { .. } | Command::Keys { .. } | Command::Scan { .. } => CommandType::Read,
            Command::Del { .. } | Command::Expire { .. } | Command::PExpire { .. } |
            Command::Persist { .. } | Command::Rename { .. } | Command::RenameNx { .. } => CommandType::Write,
        }
    }

    /// 是否为写命令
    pub fn is_write(&self) -> bool {
        self.command_type() == CommandType::Write
    }

    /// 是否为读命令
    pub fn is_read(&self) -> bool {
        self.command_type() == CommandType::Read
    }

    /// 获取命令名称
    pub fn name(&self) -> &'static str {
        match self {
            Command::Ping { .. } => "PING",
            Command::Echo { .. } => "ECHO",
            Command::CommandInfo => "COMMAND",
            Command::Info { .. } => "INFO",
            Command::DbSize => "DBSIZE",
            Command::FlushDb => "FLUSHDB",
            Command::Get { .. } => "GET",
            Command::MGet { .. } => "MGET",
            Command::StrLen { .. } => "STRLEN",
            Command::GetRange { .. } => "GETRANGE",
            Command::Set { .. } => "SET",
            Command::SetNx { .. } => "SETNX",
            Command::SetEx { .. } => "SETEX",
            Command::PSetEx { .. } => "PSETEX",
            Command::MSet { .. } => "MSET",
            Command::MSetNx { .. } => "MSETNX",
            Command::Incr { .. } => "INCR",
            Command::IncrBy { .. } => "INCRBY",
            Command::IncrByFloat { .. } => "INCRBYFLOAT",
            Command::Decr { .. } => "DECR",
            Command::DecrBy { .. } => "DECRBY",
            Command::Append { .. } => "APPEND",
            Command::GetSet { .. } => "GETSET",
            Command::SetRange { .. } => "SETRANGE",
            Command::LLen { .. } => "LLEN",
            Command::LIndex { .. } => "LINDEX",
            Command::LRange { .. } => "LRANGE",
            Command::LPush { .. } => "LPUSH",
            Command::RPush { .. } => "RPUSH",
            Command::LPop { .. } => "LPOP",
            Command::RPop { .. } => "RPOP",
            Command::LSet { .. } => "LSET",
            Command::LTrim { .. } => "LTRIM",
            Command::LRem { .. } => "LREM",
            Command::HGet { .. } => "HGET",
            Command::HMGet { .. } => "HMGET",
            Command::HGetAll { .. } => "HGETALL",
            Command::HKeys { .. } => "HKEYS",
            Command::HVals { .. } => "HVALS",
            Command::HLen { .. } => "HLEN",
            Command::HExists { .. } => "HEXISTS",
            Command::HSet { .. } => "HSET",
            Command::HSetNx { .. } => "HSETNX",
            Command::HMSet { .. } => "HMSET",
            Command::HDel { .. } => "HDEL",
            Command::HIncrBy { .. } => "HINCRBY",
            Command::HIncrByFloat { .. } => "HINCRBYFLOAT",
            Command::SMembers { .. } => "SMEMBERS",
            Command::SIsMember { .. } => "SISMEMBER",
            Command::SCard { .. } => "SCARD",
            Command::SInter { .. } => "SINTER",
            Command::SUnion { .. } => "SUNION",
            Command::SDiff { .. } => "SDIFF",
            Command::SAdd { .. } => "SADD",
            Command::SRem { .. } => "SREM",
            Command::SPop { .. } => "SPOP",
            Command::Del { .. } => "DEL",
            Command::Exists { .. } => "EXISTS",
            Command::Expire { .. } => "EXPIRE",
            Command::PExpire { .. } => "PEXPIRE",
            Command::Ttl { .. } => "TTL",
            Command::PTtl { .. } => "PTTL",
            Command::Persist { .. } => "PERSIST",
            Command::Type { .. } => "TYPE",
            Command::Rename { .. } => "RENAME",
            Command::RenameNx { .. } => "RENAMENX",
            Command::Keys { .. } => "KEYS",
            Command::Scan { .. } => "SCAN",
        }
    }
}

impl TryFrom<RespValue> for Command {
    type Error = CommandError;

    fn try_from(value: RespValue) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&RespValue> for Command {
    type Error = CommandError;

    fn try_from(value: &RespValue) -> Result<Self, Self::Error> {
        let args = extract_args(value)?;
        if args.is_empty() {
            return Err(CommandError::new(CommandErrorKind::EmptyCommand, "Empty command"));
        }

        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
        let args = &args[1..];

        parse_command(&cmd_name, args)
    }
}

/// 从 RespValue 提取参数列表
fn extract_args(value: &RespValue) -> Result<Vec<Vec<u8>>, CommandError> {
    match value {
        RespValue::Array(items) => {
            let mut args = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RespValue::BulkString(Some(bytes)) => args.push(bytes.clone()),
                    RespValue::SimpleString(s) => args.push(s.as_bytes().to_vec()),
                    RespValue::Integer(n) => args.push(n.to_string().into_bytes()),
                    _ => return Err(CommandError::new(
                        CommandErrorKind::InvalidArgument,
                        "Invalid argument type",
                    )),
                }
            }
            Ok(args)
        }
        _ => Err(CommandError::new(
            CommandErrorKind::InvalidFormat,
            "Command must be an array",
        )),
    }
}

/// 解析整数参数
fn parse_int(arg: &[u8], name: &str) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be valid UTF-8", name)))?
        .parse::<i64>()
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be an integer", name)))
}

/// 解析无符号整数参数
fn parse_uint(arg: &[u8], name: &str) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be valid UTF-8", name)))?
        .parse::<u64>()
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be a non-negative integer", name)))
}

/// 解析浮点数参数
fn parse_float(arg: &[u8], name: &str) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be valid UTF-8", name)))?
        .parse::<f64>()
        .map_err(|_| CommandError::new(CommandErrorKind::InvalidArgument, format!("{} must be a float", name)))
}

/// 检查参数数量
fn check_arity(args: &[Vec<u8>], min: usize, max: Option<usize>, cmd: &str) -> Result<(), CommandError> {
    if args.len() < min {
        return Err(CommandError::new(
            CommandErrorKind::WrongArity,
            format!("wrong number of arguments for '{}' command", cmd),
        ));
    }
    if let Some(max) = max {
        if args.len() > max {
            return Err(CommandError::new(
                CommandErrorKind::WrongArity,
                format!("wrong number of arguments for '{}' command", cmd),
            ));
        }
    }
    Ok(())
}

/// 解析命令
fn parse_command(cmd: &str, args: &[Vec<u8>]) -> Result<Command, CommandError> {
    match cmd {
        // 连接/管理命令
        "PING" => {
            check_arity(args, 0, Some(1), cmd)?;
            Ok(Command::Ping {
                message: args.first().cloned(),
            })
        }
        "ECHO" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Echo { message: args[0].clone() })
        }
        "COMMAND" => Ok(Command::CommandInfo),
        "INFO" => {
            check_arity(args, 0, Some(1), cmd)?;
            Ok(Command::Info {
                section: args.first().map(|a| String::from_utf8_lossy(a).to_string()),
            })
        }
        "DBSIZE" => {
            check_arity(args, 0, Some(0), cmd)?;
            Ok(Command::DbSize)
        }
        "FLUSHDB" => {
            check_arity(args, 0, Some(0), cmd)?;
            Ok(Command::FlushDb)
        }

        // String 读命令
        "GET" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Get { key: args[0].clone() })
        }
        "MGET" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::MGet { keys: args.to_vec() })
        }
        "STRLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::StrLen { key: args[0].clone() })
        }
        "GETRANGE" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::GetRange {
                key: args[0].clone(),
                start: parse_int(&args[1], "start")?,
                end: parse_int(&args[2], "end")?,
            })
        }

        // String 写命令
        "SET" => {
            check_arity(args, 2, None, cmd)?;
            let key = args[0].clone();
            let value = args[1].clone();
            let mut ex = None;
            let mut px = None;
            let mut nx = false;
            let mut xx = false;

            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "EX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(CommandErrorKind::SyntaxError, "EX requires an argument"));
                        }
                        ex = Some(parse_uint(&args[i], "EX")?);
                    }
                    "PX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(CommandErrorKind::SyntaxError, "PX requires an argument"));
                        }
                        px = Some(parse_uint(&args[i], "PX")?);
                    }
                    "NX" => nx = true,
                    "XX" => xx = true,
                    _ => return Err(CommandError::new(CommandErrorKind::SyntaxError, format!("unknown option '{}'", opt))),
                }
                i += 1;
            }

            Ok(Command::Set { key, value, ex, px, nx, xx })
        }
        "SETNX" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::SetNx { key: args[0].clone(), value: args[1].clone() })
        }
        "SETEX" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::SetEx {
                key: args[0].clone(),
                seconds: parse_uint(&args[1], "seconds")?,
                value: args[2].clone(),
            })
        }
        "PSETEX" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::PSetEx {
                key: args[0].clone(),
                milliseconds: parse_uint(&args[1], "milliseconds")?,
                value: args[2].clone(),
            })
        }
        "MSET" => {
            check_arity(args, 2, None, cmd)?;
            if args.len() % 2 != 0 {
                return Err(CommandError::new(CommandErrorKind::WrongArity, "wrong number of arguments for 'MSET' command"));
            }
            let kvs: Vec<_> = args.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
            Ok(Command::MSet { kvs })
        }
        "MSETNX" => {
            check_arity(args, 2, None, cmd)?;
            if args.len() % 2 != 0 {
                return Err(CommandError::new(CommandErrorKind::WrongArity, "wrong number of arguments for 'MSETNX' command"));
            }
            let kvs: Vec<_> = args.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
            Ok(Command::MSetNx { kvs })
        }
        "INCR" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Incr { key: args[0].clone() })
        }
        "INCRBY" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::IncrBy {
                key: args[0].clone(),
                delta: parse_int(&args[1], "increment")?,
            })
        }
        "INCRBYFLOAT" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::IncrByFloat {
                key: args[0].clone(),
                delta: parse_float(&args[1], "increment")?,
            })
        }
        "DECR" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Decr { key: args[0].clone() })
        }
        "DECRBY" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::DecrBy {
                key: args[0].clone(),
                delta: parse_int(&args[1], "decrement")?,
            })
        }
        "APPEND" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::Append { key: args[0].clone(), value: args[1].clone() })
        }
        "GETSET" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::GetSet { key: args[0].clone(), value: args[1].clone() })
        }
        "SETRANGE" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::SetRange {
                key: args[0].clone(),
                offset: parse_int(&args[1], "offset")?,
                value: args[2].clone(),
            })
        }

        // List 命令
        "LLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::LLen { key: args[0].clone() })
        }
        "LINDEX" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::LIndex {
                key: args[0].clone(),
                index: parse_int(&args[1], "index")?,
            })
        }
        "LRANGE" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::LRange {
                key: args[0].clone(),
                start: parse_int(&args[1], "start")?,
                stop: parse_int(&args[2], "stop")?,
            })
        }
        "LPUSH" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::LPush {
                key: args[0].clone(),
                values: args[1..].to_vec(),
            })
        }
        "RPUSH" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::RPush {
                key: args[0].clone(),
                values: args[1..].to_vec(),
            })
        }
        "LPOP" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::LPop { key: args[0].clone() })
        }
        "RPOP" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::RPop { key: args[0].clone() })
        }
        "LSET" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::LSet {
                key: args[0].clone(),
                index: parse_int(&args[1], "index")?,
                value: args[2].clone(),
            })
        }
        "LTRIM" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::LTrim {
                key: args[0].clone(),
                start: parse_int(&args[1], "start")?,
                stop: parse_int(&args[2], "stop")?,
            })
        }
        "LREM" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::LRem {
                key: args[0].clone(),
                count: parse_int(&args[1], "count")?,
                value: args[2].clone(),
            })
        }

        // Hash 命令
        "HGET" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::HGet { key: args[0].clone(), field: args[1].clone() })
        }
        "HMGET" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::HMGet {
                key: args[0].clone(),
                fields: args[1..].to_vec(),
            })
        }
        "HGETALL" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HGetAll { key: args[0].clone() })
        }
        "HKEYS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HKeys { key: args[0].clone() })
        }
        "HVALS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HVals { key: args[0].clone() })
        }
        "HLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HLen { key: args[0].clone() })
        }
        "HEXISTS" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::HExists { key: args[0].clone(), field: args[1].clone() })
        }
        "HSET" => {
            check_arity(args, 3, None, cmd)?;
            if (args.len() - 1) % 2 != 0 {
                return Err(CommandError::new(CommandErrorKind::WrongArity, "wrong number of arguments for 'HSET' command"));
            }
            let fvs: Vec<_> = args[1..].chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
            Ok(Command::HSet { key: args[0].clone(), fvs })
        }
        "HSETNX" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::HSetNx {
                key: args[0].clone(),
                field: args[1].clone(),
                value: args[2].clone(),
            })
        }
        "HMSET" => {
            check_arity(args, 3, None, cmd)?;
            if (args.len() - 1) % 2 != 0 {
                return Err(CommandError::new(CommandErrorKind::WrongArity, "wrong number of arguments for 'HMSET' command"));
            }
            let fvs: Vec<_> = args[1..].chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
            Ok(Command::HMSet { key: args[0].clone(), fvs })
        }
        "HDEL" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::HDel {
                key: args[0].clone(),
                fields: args[1..].to_vec(),
            })
        }
        "HINCRBY" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::HIncrBy {
                key: args[0].clone(),
                field: args[1].clone(),
                delta: parse_int(&args[2], "increment")?,
            })
        }
        "HINCRBYFLOAT" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::HIncrByFloat {
                key: args[0].clone(),
                field: args[1].clone(),
                delta: parse_float(&args[2], "increment")?,
            })
        }

        // Set 命令
        "SMEMBERS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::SMembers { key: args[0].clone() })
        }
        "SISMEMBER" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::SIsMember { key: args[0].clone(), member: args[1].clone() })
        }
        "SCARD" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::SCard { key: args[0].clone() })
        }
        "SINTER" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::SInter { keys: args.to_vec() })
        }
        "SUNION" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::SUnion { keys: args.to_vec() })
        }
        "SDIFF" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::SDiff { keys: args.to_vec() })
        }
        "SADD" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::SAdd {
                key: args[0].clone(),
                members: args[1..].to_vec(),
            })
        }
        "SREM" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::SRem {
                key: args[0].clone(),
                members: args[1..].to_vec(),
            })
        }
        "SPOP" => {
            check_arity(args, 1, Some(2), cmd)?;
            Ok(Command::SPop {
                key: args[0].clone(),
                count: args.get(1).map(|a| parse_uint(a, "count")).transpose()?,
            })
        }

        // Key 命令
        "DEL" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::Del { keys: args.to_vec() })
        }
        "EXISTS" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::Exists { keys: args.to_vec() })
        }
        "EXPIRE" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::Expire {
                key: args[0].clone(),
                seconds: parse_uint(&args[1], "seconds")?,
            })
        }
        "PEXPIRE" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::PExpire {
                key: args[0].clone(),
                milliseconds: parse_uint(&args[1], "milliseconds")?,
            })
        }
        "TTL" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Ttl { key: args[0].clone() })
        }
        "PTTL" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::PTtl { key: args[0].clone() })
        }
        "PERSIST" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Persist { key: args[0].clone() })
        }
        "TYPE" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Type { key: args[0].clone() })
        }
        "RENAME" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::Rename { key: args[0].clone(), new_key: args[1].clone() })
        }
        "RENAMENX" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::RenameNx { key: args[0].clone(), new_key: args[1].clone() })
        }
        "KEYS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Keys { pattern: args[0].clone() })
        }
        "SCAN" => {
            check_arity(args, 1, None, cmd)?;
            let cursor = parse_uint(&args[0], "cursor")?;
            let mut pattern = None;
            let mut count = None;
            
            let mut i = 1;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "MATCH" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(CommandErrorKind::SyntaxError, "MATCH requires an argument"));
                        }
                        pattern = Some(args[i].clone());
                    }
                    "COUNT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(CommandErrorKind::SyntaxError, "COUNT requires an argument"));
                        }
                        count = Some(parse_uint(&args[i], "COUNT")?);
                    }
                    _ => return Err(CommandError::new(CommandErrorKind::SyntaxError, format!("unknown option '{}'", opt))),
                }
                i += 1;
            }
            
            Ok(Command::Scan { cursor, pattern, count })
        }

        _ => Err(CommandError::new(
            CommandErrorKind::UnknownCommand,
            format!("unknown command '{}'", cmd),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cmd(args: &[&str]) -> RespValue {
        RespValue::Array(
            args.iter()
                .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                .collect(),
        )
    }

    #[test]
    fn test_parse_get() {
        let cmd = Command::try_from(make_cmd(&["GET", "key1"])).unwrap();
        assert_eq!(cmd, Command::Get { key: b"key1".to_vec() });
        assert!(cmd.is_read());
    }

    #[test]
    fn test_parse_set() {
        let cmd = Command::try_from(make_cmd(&["SET", "key1", "value1"])).unwrap();
        assert_eq!(cmd, Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: None,
            px: None,
            nx: false,
            xx: false,
        });
        assert!(cmd.is_write());
    }

    #[test]
    fn test_parse_set_with_options() {
        let cmd = Command::try_from(make_cmd(&["SET", "key1", "value1", "EX", "60", "NX"])).unwrap();
        assert_eq!(cmd, Command::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            ex: Some(60),
            px: None,
            nx: true,
            xx: false,
        });
    }

    #[test]
    fn test_parse_mset() {
        let cmd = Command::try_from(make_cmd(&["MSET", "k1", "v1", "k2", "v2"])).unwrap();
        assert_eq!(cmd, Command::MSet {
            kvs: vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ],
        });
    }

    #[test]
    fn test_parse_lpush() {
        let cmd = Command::try_from(make_cmd(&["LPUSH", "list", "a", "b", "c"])).unwrap();
        assert_eq!(cmd, Command::LPush {
            key: b"list".to_vec(),
            values: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        });
    }

    #[test]
    fn test_parse_hset() {
        let cmd = Command::try_from(make_cmd(&["HSET", "hash", "f1", "v1", "f2", "v2"])).unwrap();
        assert_eq!(cmd, Command::HSet {
            key: b"hash".to_vec(),
            fvs: vec![
                (b"f1".to_vec(), b"v1".to_vec()),
                (b"f2".to_vec(), b"v2".to_vec()),
            ],
        });
    }

    #[test]
    fn test_wrong_arity() {
        let result = Command::try_from(make_cmd(&["GET"]));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), CommandErrorKind::WrongArity);
    }

    #[test]
    fn test_unknown_command() {
        let result = Command::try_from(make_cmd(&["UNKNOWN"]));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), CommandErrorKind::UnknownCommand);
    }
}
