//! Redis 协议 (RESP) 支持库
//!
//! 实现 Redis 序列化协议 (RESP) 的解析和编码，支持同步和异步操作

mod parser;
mod encoder;
mod async_parser;
mod async_encoder;

pub use parser::RespParser;
pub use encoder::RespEncoder;
pub use async_parser::{AsyncRespParser, DEFAULT_MAX_FRAME_SIZE};
pub use async_encoder::AsyncRespEncoder;

use std::io;

/// RESP 数据类型
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// 简单字符串: +OK\r\n
    SimpleString(String),
    /// 错误: -ERR message\r\n
    Error(String),
    /// 整数: :123\r\n
    Integer(i64),
    /// 批量字符串: $5\r\nhello\r\n
    BulkString(Option<Vec<u8>>),
    /// 数组: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    Array(Vec<RespValue>),
    /// Null: $-1\r\n
    Null,
}

impl RespValue {
    /// 转换为 Redis 命令字符串数组
    pub fn to_command(&self) -> Option<Vec<String>> {
        match self {
            RespValue::Array(items) => {
                let mut cmd = Vec::new();
                for item in items {
                    match item {
                        RespValue::BulkString(Some(bytes)) => {
                            cmd.push(String::from_utf8_lossy(bytes).to_string());
                        }
                        RespValue::SimpleString(s) => {
                            cmd.push(s.clone());
                        }
                        _ => return None,
                    }
                }
                Some(cmd)
            }
            _ => None,
        }
    }

    /// 从命令创建 RESP 数组
    pub fn from_command(cmd: Vec<String>) -> Self {
        RespValue::Array(
            cmd.into_iter()
                .map(|s| RespValue::BulkString(Some(s.into_bytes())))
                .collect(),
        )
    }
}

/// RESP 解析错误
#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid RESP format: {0}")]
    InvalidFormat(String),
    #[error("Unexpected end of input")]
    UnexpectedEof,
    #[error("Integer overflow")]
    IntegerOverflow,
    #[error("Frame too large: {0} bytes (max: {1} bytes)")]
    FrameTooLarge(usize, usize),
}
