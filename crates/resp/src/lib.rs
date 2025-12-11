//! RESP (REdis Serialization Protocol) support library
//!
//! Implements RESP protocol parsing, encoding, and type-safe command parsing

mod async_encoder;
mod async_parser;
pub mod command;
mod encoder;
mod parser;

pub use async_encoder::AsyncRespEncoder;
pub use async_parser::{AsyncRespParser, DEFAULT_MAX_FRAME_SIZE};
pub use command::{Command, CommandError, CommandErrorKind, CommandResult, CommandType};
pub use encoder::RespEncoder;
pub use parser::RespParser;

use bytes::Bytes;
use std::io;

/// RESP data type
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string: +OK\r\n
    SimpleString(Bytes),
    /// Error: -ERR message\r\n
    Error(Bytes),
    /// Integer: :123\r\n
    Integer(i64),
    /// Bulk string: $5\r\nhello\r\n
    BulkString(Option<Bytes>),
    /// Array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    Array(Vec<RespValue>),
    /// Null: $-1\r\n
    Null,
}

impl RespValue {
    /// Convert to Redis command string array
    pub fn to_command(&self) -> Option<Vec<Bytes>> {
        match self {
            RespValue::Array(items) => {
                let mut cmd = Vec::new();
                for item in items {
                    match item {
                        RespValue::BulkString(Some(bytes)) => {
                            cmd.push(bytes.clone());
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

    /// Create RESP array from command
    pub fn from_command(cmd: Vec<Bytes>) -> Self {
        RespValue::Array(
            cmd.into_iter()
                .map(|s| RespValue::BulkString(Some(s)))
                .collect(),
        )
    }
}

/// RESP parsing error
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
    #[error("Invalid RESP type: {0}")]
    InvalidType(u8),
}
