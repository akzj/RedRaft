//! Redis command parsing module
//!
//! Parses RespValue into type-safe Command structures

mod error;
mod result;

pub use error::{CommandError, CommandErrorKind};
pub use result::CommandResult;

use crate::RespValue;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Command type marker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandType {
    /// Read command - can be read locally or via ReadIndex
    Read,
    /// Write command - must go through Raft consensus
    Write,
}

/// Redis command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    // ==================== Connection/Management Commands ====================
    /// PING [message]
    Ping { message: Option<Bytes> },
    /// ECHO message
    Echo { message: Bytes },
    /// COMMAND
    CommandInfo,
    /// INFO [section]
    Info { section: Option<String> },
    /// DBSIZE
    DbSize,
    /// FLUSHDB
    FlushDb,

    // ==================== String Read Commands ====================
    /// GET key
    Get { key: Bytes },
    /// MGET key [key ...]
    MGet { keys: Vec<Bytes> },
    /// STRLEN key
    StrLen { key: Bytes },
    /// GETRANGE key start end
    GetRange { key: Bytes, start: i64, end: i64 },

    // ==================== String Write Commands ====================
    /// SET key value [EX seconds] [PX milliseconds] [NX|XX]
    Set {
        key: Bytes,
        value: Bytes,
        ex: Option<u64>,
        px: Option<u64>,
        nx: bool,
        xx: bool,
    },
    /// SETNX key value
    SetNx { key: Bytes, value: Bytes },
    /// SETEX key seconds value
    SetEx {
        key: Bytes,
        seconds: u64,
        value: Bytes,
    },
    /// PSETEX key milliseconds value
    PSetEx {
        key: Bytes,
        milliseconds: u64,
        value: Bytes,
    },
    /// MSET key value [key value ...]
    MSet { kvs: Vec<(Bytes, Bytes)> },
    /// MSETNX key value [key value ...]
    MSetNx { kvs: Vec<(Bytes, Bytes)> },
    /// INCR key
    Incr { key: Bytes },
    /// INCRBY key increment
    IncrBy { key: Bytes, delta: i64 },
    /// INCRBYFLOAT key increment
    IncrByFloat { key: Bytes, delta: f64 },
    /// DECR key
    Decr { key: Bytes },
    /// DECRBY key decrement
    DecrBy { key: Bytes, delta: i64 },
    /// APPEND key value
    Append { key: Bytes, value: Bytes },
    /// GETSET key value
    GetSet { key: Bytes, value: Bytes },
    /// SETRANGE key offset value
    SetRange {
        key: Bytes,
        offset: i64,
        value: Bytes,
    },

    // ==================== List Read Commands ====================
    /// LLEN key
    LLen { key: Bytes },
    /// LINDEX key index
    LIndex { key: Bytes, index: i64 },
    /// LRANGE key start stop
    LRange { key: Bytes, start: i64, stop: i64 },

    // ==================== List Write Commands ====================
    /// LPUSH key value [value ...]
    LPush { key: Bytes, values: Vec<Bytes> },
    /// RPUSH key value [value ...]
    RPush { key: Bytes, values: Vec<Bytes> },
    /// LPOP key
    LPop { key: Bytes },
    /// RPOP key
    RPop { key: Bytes },
    /// LSET key index value
    LSet {
        key: Bytes,
        index: i64,
        value: Bytes,
    },
    /// LTRIM key start stop
    LTrim { key: Bytes, start: i64, stop: i64 },
    /// LREM key count value
    LRem {
        key: Bytes,
        count: i64,
        value: Bytes,
    },

    // ==================== Hash Read Commands ====================
    /// HGET key field
    HGet { key: Bytes, field: Bytes },
    /// HMGET key field [field ...]
    HMGet { key: Bytes, fields: Vec<Bytes> },
    /// HGETALL key
    HGetAll { key: Bytes },
    /// HKEYS key
    HKeys { key: Bytes },
    /// HVALS key
    HVals { key: Bytes },
    /// HLEN key
    HLen { key: Bytes },
    /// HEXISTS key field
    HExists { key: Bytes, field: Bytes },

    // ==================== Hash Write Commands ====================
    /// HSET key field value [field value ...]
    HSet {
        key: Bytes,
        fvs: Vec<(Bytes, Bytes)>,
    },
    /// HSETNX key field value
    HSetNx {
        key: Bytes,
        field: Bytes,
        value: Bytes,
    },
    /// HMSET key field value [field value ...]
    HMSet {
        key: Bytes,
        fvs: Vec<(Bytes, Bytes)>,
    },
    /// HDEL key field [field ...]
    HDel { key: Bytes, fields: Vec<Bytes> },
    /// HINCRBY key field increment
    HIncrBy {
        key: Bytes,
        field: Bytes,
        delta: i64,
    },
    /// HINCRBYFLOAT key field increment
    HIncrByFloat {
        key: Bytes,
        field: Bytes,
        delta: f64,
    },

    // ==================== Set Read Commands ====================
    /// SMEMBERS key
    SMembers { key: Bytes },
    /// SISMEMBER key member
    SIsMember { key: Bytes, member: Bytes },
    /// SCARD key
    SCard { key: Bytes },
    /// SINTER key [key ...]
    SInter { keys: Vec<Bytes> },
    /// SUNION key [key ...]
    SUnion { keys: Vec<Bytes> },
    /// SDIFF key [key ...]
    SDiff { keys: Vec<Bytes> },

    // ==================== Set Write Commands ====================
    /// SADD key member [member ...]
    SAdd { key: Bytes, members: Vec<Bytes> },
    /// SREM key member [member ...]
    SRem { key: Bytes, members: Vec<Bytes> },
    /// SPOP key [count]
    SPop { key: Bytes, count: Option<u64> },

    // ==================== ZSet Read Commands ====================
    /// ZSCORE key member
    ZScore { key: Bytes, member: Bytes },
    /// ZRANK key member
    ZRank { key: Bytes, member: Bytes },
    /// ZRANGE key start stop [WITHSCORES]
    ZRange {
        key: Bytes,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    /// ZCARD key
    ZCard { key: Bytes },

    // ==================== ZSet Write Commands ====================
    /// ZADD key score member [score member ...]
    ZAdd {
        key: Bytes,
        members: Vec<(f64, Bytes)>,
    },
    /// ZREM key member [member ...]
    ZRem { key: Bytes, members: Vec<Bytes> },
    /// ZINCRBY key increment member
    ZIncrBy {
        key: Bytes,
        increment: f64,
        member: Bytes,
    },

    // ==================== Key Commands ====================
    /// DEL key [key ...]
    Del { keys: Vec<Bytes> },
    /// EXISTS key [key ...]
    Exists { keys: Vec<Bytes> },
    /// EXPIRE key seconds
    Expire { key: Bytes, seconds: u64 },
    /// PEXPIRE key milliseconds
    PExpire { key: Bytes, milliseconds: u64 },
    /// TTL key
    Ttl { key: Bytes },
    /// PTTL key
    PTtl { key: Bytes },
    /// PERSIST key
    Persist { key: Bytes },
    /// TYPE key
    Type { key: Bytes },
    /// RENAME key newkey
    Rename { key: Bytes, new_key: Bytes },
    /// RENAMENX key newkey
    RenameNx { key: Bytes, new_key: Bytes },
    /// KEYS pattern
    Keys { pattern: Bytes },
    /// SCAN cursor [MATCH pattern] [COUNT count]
    Scan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: Option<u64>,
    },
}

impl Command {
    /// Get command type (read/write)
    pub fn command_type(&self) -> CommandType {
        match self {
            // Management commands
            Command::Ping { .. }
            | Command::Echo { .. }
            | Command::CommandInfo
            | Command::Info { .. }
            | Command::DbSize => CommandType::Read,
            Command::FlushDb => CommandType::Write,

            // String commands
            Command::Get { .. }
            | Command::MGet { .. }
            | Command::StrLen { .. }
            | Command::GetRange { .. } => CommandType::Read,
            Command::Set { .. }
            | Command::SetNx { .. }
            | Command::SetEx { .. }
            | Command::PSetEx { .. }
            | Command::MSet { .. }
            | Command::MSetNx { .. }
            | Command::Incr { .. }
            | Command::IncrBy { .. }
            | Command::IncrByFloat { .. }
            | Command::Decr { .. }
            | Command::DecrBy { .. }
            | Command::Append { .. }
            | Command::GetSet { .. }
            | Command::SetRange { .. } => CommandType::Write,

            // List commands
            Command::LLen { .. } | Command::LIndex { .. } | Command::LRange { .. } => {
                CommandType::Read
            }
            Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
            | Command::LSet { .. }
            | Command::LTrim { .. }
            | Command::LRem { .. } => CommandType::Write,

            // Hash commands
            Command::HGet { .. }
            | Command::HMGet { .. }
            | Command::HGetAll { .. }
            | Command::HKeys { .. }
            | Command::HVals { .. }
            | Command::HLen { .. }
            | Command::HExists { .. } => CommandType::Read,
            Command::HSet { .. }
            | Command::HSetNx { .. }
            | Command::HMSet { .. }
            | Command::HDel { .. }
            | Command::HIncrBy { .. }
            | Command::HIncrByFloat { .. } => CommandType::Write,

            // Set commands
            Command::SMembers { .. }
            | Command::SIsMember { .. }
            | Command::SCard { .. }
            | Command::SInter { .. }
            | Command::SUnion { .. }
            | Command::SDiff { .. } => CommandType::Read,
            Command::SAdd { .. } | Command::SRem { .. } | Command::SPop { .. } => {
                CommandType::Write
            }

            // ZSet commands
            Command::ZScore { .. }
            | Command::ZRank { .. }
            | Command::ZRange { .. }
            | Command::ZCard { .. } => CommandType::Read,
            Command::ZAdd { .. } | Command::ZRem { .. } | Command::ZIncrBy { .. } => {
                CommandType::Write
            }

            // Key commands
            Command::Exists { .. }
            | Command::Ttl { .. }
            | Command::PTtl { .. }
            | Command::Type { .. }
            | Command::Keys { .. }
            | Command::Scan { .. } => CommandType::Read,
            Command::Del { .. }
            | Command::Expire { .. }
            | Command::PExpire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. } => CommandType::Write,
        }
    }

    /// Whether it's a write command
    pub fn is_write(&self) -> bool {
        self.command_type() == CommandType::Write
    }

    /// Whether it's a read command
    pub fn is_read(&self) -> bool {
        self.command_type() == CommandType::Read
    }

    /// Get the command's primary key (for routing)
    /// Returns None if the command doesn't involve a specific key (like PING, DBSIZE, etc.)
    pub fn get_key(&self) -> Option<&[u8]> {
        match self {
            // No key commands
            Command::Ping { .. }
            | Command::Echo { .. }
            | Command::CommandInfo
            | Command::Info { .. }
            | Command::DbSize
            | Command::FlushDb => None,

            // Single key commands
            Command::Get { key }
            | Command::StrLen { key }
            | Command::GetRange { key, .. }
            | Command::Set { key, .. }
            | Command::SetNx { key, .. }
            | Command::SetEx { key, .. }
            | Command::PSetEx { key, .. }
            | Command::Incr { key }
            | Command::IncrBy { key, .. }
            | Command::IncrByFloat { key, .. }
            | Command::Decr { key }
            | Command::DecrBy { key, .. }
            | Command::Append { key, .. }
            | Command::GetSet { key, .. }
            | Command::SetRange { key, .. }
            | Command::LLen { key }
            | Command::LIndex { key, .. }
            | Command::LRange { key, .. }
            | Command::LPush { key, .. }
            | Command::RPush { key, .. }
            | Command::LPop { key }
            | Command::RPop { key }
            | Command::LSet { key, .. }
            | Command::LTrim { key, .. }
            | Command::LRem { key, .. }
            | Command::HGet { key, .. }
            | Command::HMGet { key, .. }
            | Command::HGetAll { key }
            | Command::HKeys { key }
            | Command::HVals { key }
            | Command::HLen { key }
            | Command::HExists { key, .. }
            | Command::HSet { key, .. }
            | Command::HSetNx { key, .. }
            | Command::HMSet { key, .. }
            | Command::HDel { key, .. }
            | Command::HIncrBy { key, .. }
            | Command::HIncrByFloat { key, .. }
            |             Command::SMembers { key }
            | Command::SIsMember { key, .. }
            | Command::SCard { key }
            | Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SPop { key, .. }
            | Command::ZScore { key, .. }
            | Command::ZRank { key, .. }
            | Command::ZRange { key, .. }
            | Command::ZCard { key }
            | Command::ZAdd { key, .. }
            | Command::ZRem { key, .. }
            | Command::ZIncrBy { key, .. }
            | Command::Expire { key, .. }
            | Command::PExpire { key, .. }
            | Command::Ttl { key }
            | Command::PTtl { key }
            | Command::Persist { key }
            | Command::Type { key }
            | Command::Rename { key, .. }
            | Command::RenameNx { key, .. } => Some(key),

            // Multi-key commands, take the first key
            Command::MGet { keys }
            | Command::Del { keys }
            | Command::Exists { keys }
            | Command::SInter { keys }
            | Command::SUnion { keys }
            | Command::SDiff { keys } => keys.first().map(|k| k.as_ref()),

            // Multi key-value pairs, take the first key
            Command::MSet { kvs } | Command::MSetNx { kvs } => kvs.first().map(|(k, _)| k.as_ref()),

            // Global scan commands
            Command::Keys { .. } | Command::Scan { .. } => None,
        }
    }

    /// Get command name
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
            Command::ZScore { .. } => "ZSCORE",
            Command::ZRank { .. } => "ZRANK",
            Command::ZRange { .. } => "ZRANGE",
            Command::ZCard { .. } => "ZCARD",
            Command::ZAdd { .. } => "ZADD",
            Command::ZRem { .. } => "ZREM",
            Command::ZIncrBy { .. } => "ZINCRBY",
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
            return Err(CommandError::new(
                CommandErrorKind::EmptyCommand,
                "Empty command",
            ));
        }

        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
        let args = &args[1..];

        parse_command(&cmd_name, args)
    }
}

/// Extract argument list from RespValue
fn extract_args(value: &RespValue) -> Result<Vec<Bytes>, CommandError> {
    match value {
        RespValue::Array(items) => {
            let mut args = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RespValue::BulkString(Some(bytes)) => args.push(bytes.clone()),
                    RespValue::SimpleString(s) => args.push(s.clone()),
                    RespValue::Integer(n) => args.push(Bytes::from(n.to_string())),
                    _ => {
                        return Err(CommandError::new(
                            CommandErrorKind::InvalidArgument,
                            "Invalid argument type",
                        ))
                    }
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

/// Parse integer argument
fn parse_int(arg: &[u8], name: &str) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be valid UTF-8", name),
            )
        })?
        .parse::<i64>()
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be an integer", name),
            )
        })
}

/// Parse unsigned integer argument
fn parse_uint(arg: &[u8], name: &str) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be valid UTF-8", name),
            )
        })?
        .parse::<u64>()
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be a non-negative integer", name),
            )
        })
}

/// Parse float argument
fn parse_float(arg: &[u8], name: &str) -> Result<f64, CommandError> {
    std::str::from_utf8(arg)
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be valid UTF-8", name),
            )
        })?
        .parse::<f64>()
        .map_err(|_| {
            CommandError::new(
                CommandErrorKind::InvalidArgument,
                format!("{} must be a float", name),
            )
        })
}

/// Check argument count
fn check_arity(
    args: &[Bytes],
    min: usize,
    max: Option<usize>,
    cmd: &str,
) -> Result<(), CommandError> {
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

/// Parse command
fn parse_command(cmd: &str, args: &[Bytes]) -> Result<Command, CommandError> {
    match cmd {
        // Connection/Management commands
        "PING" => {
            check_arity(args, 0, Some(1), cmd)?;
            Ok(Command::Ping {
                message: args.first().cloned(),
            })
        }
        "ECHO" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Echo {
                message: args[0].clone(),
            })
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

        // String read commands
        "GET" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Get {
                key: args[0].clone(),
            })
        }
        "MGET" => {
            check_arity(args, 1, None, cmd)?;
            Ok(Command::MGet { keys: args.to_vec() })
        }
        "STRLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::StrLen {
                key: args[0].clone(),
            })
        }
        "GETRANGE" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::GetRange {
                key: args[0].clone(),
                start: parse_int(&args[1], "start")?,
                end: parse_int(&args[2], "end")?,
            })
        }

        // String write commands
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
                            return Err(CommandError::new(
                                CommandErrorKind::SyntaxError,
                                "EX requires an argument",
                            ));
                        }
                        ex = Some(parse_uint(&args[i], "EX")?);
                    }
                    "PX" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(
                                CommandErrorKind::SyntaxError,
                                "PX requires an argument",
                            ));
                        }
                        px = Some(parse_uint(&args[i], "PX")?);
                    }
                    "NX" => nx = true,
                    "XX" => xx = true,
                    _ => {
                        return Err(CommandError::new(
                            CommandErrorKind::SyntaxError,
                            format!("unknown option '{}'", opt),
                        ))
                    }
                }
                i += 1;
            }

            Ok(Command::Set {
                key,
                value,
                ex,
                px,
                nx,
                xx,
            })
        }
        "SETNX" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::SetNx {
                key: args[0].clone(),
                value: args[1].clone(),
            })
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
                return Err(CommandError::new(
                    CommandErrorKind::WrongArity,
                    "wrong number of arguments for 'MSET' command",
                ));
            }
            let kvs: Vec<_> = args
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::MSet { kvs })
        }
        "MSETNX" => {
            check_arity(args, 2, None, cmd)?;
            if args.len() % 2 != 0 {
                return Err(CommandError::new(
                    CommandErrorKind::WrongArity,
                    "wrong number of arguments for 'MSETNX' command",
                ));
            }
            let kvs: Vec<_> = args
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::MSetNx { kvs })
        }
        "INCR" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Incr {
                key: args[0].clone(),
            })
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
            Ok(Command::Decr {
                key: args[0].clone(),
            })
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
            Ok(Command::Append {
                key: args[0].clone(),
                value: args[1].clone(),
            })
        }
        "GETSET" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::GetSet {
                key: args[0].clone(),
                value: args[1].clone(),
            })
        }
        "SETRANGE" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::SetRange {
                key: args[0].clone(),
                offset: parse_int(&args[1], "offset")?,
                value: args[2].clone(),
            })
        }

        // List commands
        "LLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::LLen {
                key: args[0].clone(),
            })
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
            Ok(Command::LPop {
                key: args[0].clone(),
            })
        }
        "RPOP" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::RPop {
                key: args[0].clone(),
            })
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

        // Hash commands
        "HGET" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::HGet {
                key: args[0].clone(),
                field: args[1].clone(),
            })
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
            Ok(Command::HGetAll {
                key: args[0].clone(),
            })
        }
        "HKEYS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HKeys {
                key: args[0].clone(),
            })
        }
        "HVALS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HVals {
                key: args[0].clone(),
            })
        }
        "HLEN" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::HLen {
                key: args[0].clone(),
            })
        }
        "HEXISTS" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::HExists {
                key: args[0].clone(),
                field: args[1].clone(),
            })
        }
        "HSET" => {
            check_arity(args, 3, None, cmd)?;
            if (args.len() - 1) % 2 != 0 {
                return Err(CommandError::new(
                    CommandErrorKind::WrongArity,
                    "wrong number of arguments for 'HSET' command",
                ));
            }
            let fvs: Vec<_> = args[1..]
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::HSet {
                key: args[0].clone(),
                fvs,
            })
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
                return Err(CommandError::new(
                    CommandErrorKind::WrongArity,
                    "wrong number of arguments for 'HMSET' command",
                ));
            }
            let fvs: Vec<_> = args[1..]
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::HMSet {
                key: args[0].clone(),
                fvs,
            })
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

        // Set commands
        "SMEMBERS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::SMembers {
                key: args[0].clone(),
            })
        }
        "SISMEMBER" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::SIsMember {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "SCARD" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::SCard {
                key: args[0].clone(),
            })
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

        // ZSet commands
        "ZADD" => {
            check_arity(args, 3, None, cmd)?;
            if args.len() % 2 != 1 {
                return Err(CommandError::new(
                    CommandErrorKind::WrongArity,
                    "wrong number of arguments for 'ZADD' command",
                ));
            }
            let mut members = Vec::new();
            for i in (1..args.len()).step_by(2) {
                let score = parse_float(&args[i], "score")?;
                let member = args[i + 1].clone();
                members.push((score, member));
            }
            Ok(Command::ZAdd {
                key: args[0].clone(),
                members,
            })
        }
        "ZREM" => {
            check_arity(args, 2, None, cmd)?;
            Ok(Command::ZRem {
                key: args[0].clone(),
                members: args[1..].to_vec(),
            })
        }
        "ZSCORE" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::ZScore {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "ZRANK" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::ZRank {
                key: args[0].clone(),
                member: args[1].clone(),
            })
        }
        "ZRANGE" => {
            check_arity(args, 3, Some(4), cmd)?;
            let start = parse_int(&args[1], "start")?;
            let stop = parse_int(&args[2], "stop")?;
            let with_scores = args.len() == 4
                && std::str::from_utf8(&args[3])
                    .map(|s| s.to_uppercase() == "WITHSCORES")
                    .unwrap_or(false);
            Ok(Command::ZRange {
                key: args[0].clone(),
                start,
                stop,
                with_scores,
            })
        }
        "ZCARD" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::ZCard {
                key: args[0].clone(),
            })
        }
        "ZINCRBY" => {
            check_arity(args, 3, Some(3), cmd)?;
            Ok(Command::ZIncrBy {
                key: args[0].clone(),
                increment: parse_float(&args[1], "increment")?,
                member: args[2].clone(),
            })
        }

        // Key commands
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
            Ok(Command::Ttl {
                key: args[0].clone(),
            })
        }
        "PTTL" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::PTtl {
                key: args[0].clone(),
            })
        }
        "PERSIST" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Persist {
                key: args[0].clone(),
            })
        }
        "TYPE" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Type {
                key: args[0].clone(),
            })
        }
        "RENAME" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::Rename {
                key: args[0].clone(),
                new_key: args[1].clone(),
            })
        }
        "RENAMENX" => {
            check_arity(args, 2, Some(2), cmd)?;
            Ok(Command::RenameNx {
                key: args[0].clone(),
                new_key: args[1].clone(),
            })
        }
        "KEYS" => {
            check_arity(args, 1, Some(1), cmd)?;
            Ok(Command::Keys {
                pattern: args[0].clone(),
            })
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
                            return Err(CommandError::new(
                                CommandErrorKind::SyntaxError,
                                "MATCH requires an argument",
                            ));
                        }
                        pattern = Some(args[i].clone());
                    }
                    "COUNT" => {
                        i += 1;
                        if i >= args.len() {
                            return Err(CommandError::new(
                                CommandErrorKind::SyntaxError,
                                "COUNT requires an argument",
                            ));
                        }
                        count = Some(parse_uint(&args[i], "COUNT")?);
                    }
                    _ => {
                        return Err(CommandError::new(
                            CommandErrorKind::SyntaxError,
                            format!("unknown option '{}'", opt),
                        ))
                    }
                }
                i += 1;
            }

            Ok(Command::Scan {
                cursor,
                pattern,
                count,
            })
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
    use bytes::Bytes;

    fn make_cmd(args: &[&str]) -> RespValue {
        RespValue::Array(
            args.iter()
                .map(|s| RespValue::BulkString(Some(Bytes::from(s.to_string()))))
                .collect(),
        )
    }

    #[test]
    fn test_parse_get() {
        let cmd = Command::try_from(make_cmd(&["GET", "key1"])).unwrap();
        assert_eq!(
            cmd,
            Command::Get {
                key: Bytes::from(b"key1" as &[u8])
            }
        );
        assert!(cmd.is_read());
    }

    #[test]
    fn test_parse_set() {
        let cmd = Command::try_from(make_cmd(&["SET", "key1", "value1"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from(b"key1" as &[u8]),
                value: Bytes::from(b"value1" as &[u8]),
                ex: None,
                px: None,
                nx: false,
                xx: false,
            }
        );
        assert!(cmd.is_write());
    }

    #[test]
    fn test_parse_set_with_options() {
        let cmd =
            Command::try_from(make_cmd(&["SET", "key1", "value1", "EX", "60", "NX"])).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: Bytes::from(b"key1" as &[u8]),
                value: Bytes::from(b"value1" as &[u8]),
                ex: Some(60),
                px: None,
                nx: true,
                xx: false,
            }
        );
    }

    #[test]
    fn test_parse_mset() {
        let cmd = Command::try_from(make_cmd(&["MSET", "k1", "v1", "k2", "v2"])).unwrap();
        assert_eq!(
            cmd,
            Command::MSet {
                kvs: vec![
                    (Bytes::from(b"k1" as &[u8]), Bytes::from(b"v1" as &[u8])),
                    (Bytes::from(b"k2" as &[u8]), Bytes::from(b"v2" as &[u8])),
                ],
            }
        );
    }

    #[test]
    fn test_parse_lpush() {
        let cmd = Command::try_from(make_cmd(&["LPUSH", "list", "a", "b", "c"])).unwrap();
        assert_eq!(
            cmd,
            Command::LPush {
                key: Bytes::from(b"list" as &[u8]),
                values: vec![
                    Bytes::from(b"a" as &[u8]),
                    Bytes::from(b"b" as &[u8]),
                    Bytes::from(b"c" as &[u8]),
                ],
            }
        );
    }

    #[test]
    fn test_parse_hset() {
        let cmd = Command::try_from(make_cmd(&["HSET", "hash", "f1", "v1", "f2", "v2"])).unwrap();
        assert_eq!(
            cmd,
            Command::HSet {
                key: Bytes::from(b"hash" as &[u8]),
                fvs: vec![
                    (Bytes::from(b"f1" as &[u8]), Bytes::from(b"v1" as &[u8])),
                    (Bytes::from(b"f2" as &[u8]), Bytes::from(b"v2" as &[u8])),
                ],
            }
        );
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
