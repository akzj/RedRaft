//! Command parsing error types

use std::fmt;

/// Command error kind
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandErrorKind {
    /// Empty command
    EmptyCommand,
    /// Unknown command
    UnknownCommand,
    /// Wrong argument count
    WrongArity,
    /// Invalid argument
    InvalidArgument,
    /// Invalid format
    InvalidFormat,
    /// Syntax error
    SyntaxError,
}

/// Command parsing error
#[derive(Debug, Clone)]
pub struct CommandError {
    kind: CommandErrorKind,
    message: String,
}

impl CommandError {
    /// Create a new command error
    pub fn new(kind: CommandErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Get error kind
    pub fn kind(&self) -> CommandErrorKind {
        self.kind
    }

    /// Get error message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Convert to Redis error prefix
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
