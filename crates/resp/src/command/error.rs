//! 命令解析错误类型

use std::fmt;

/// 命令错误类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandErrorKind {
    /// 空命令
    EmptyCommand,
    /// 未知命令
    UnknownCommand,
    /// 参数数量错误
    WrongArity,
    /// 无效参数
    InvalidArgument,
    /// 无效格式
    InvalidFormat,
    /// 语法错误
    SyntaxError,
}

/// 命令解析错误
#[derive(Debug, Clone)]
pub struct CommandError {
    kind: CommandErrorKind,
    message: String,
}

impl CommandError {
    /// 创建新的命令错误
    pub fn new(kind: CommandErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// 获取错误类型
    pub fn kind(&self) -> CommandErrorKind {
        self.kind
    }

    /// 获取错误消息
    pub fn message(&self) -> &str {
        &self.message
    }

    /// 转换为 Redis 错误前缀
    pub fn error_prefix(&self) -> &'static str {
        match self.kind {
            CommandErrorKind::EmptyCommand => "ERR",
            CommandErrorKind::UnknownCommand => "ERR",
            CommandErrorKind::WrongArity => "ERR",
            CommandErrorKind::InvalidArgument => "ERR",
            CommandErrorKind::InvalidFormat => "ERR",
            CommandErrorKind::SyntaxError => "ERR",
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.error_prefix(), self.message)
    }
}

impl std::error::Error for CommandError {}
