//! Command execution result type

use crate::RespValue;
use bytes::Bytes;

/// Command execution result
///
/// Represents all possible return types for Redis commands, which can be easily converted to RespValue
#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    /// OK response
    Ok,
    /// PONG response
    Pong,
    /// Custom simple string
    SimpleString(Bytes),
    /// Integer response
    Integer(i64),
    /// Single value (may be empty)
    Value(Option<Bytes>),
    /// Array of values
    Array(Vec<Option<Bytes>>),
    /// Key-value array (for HGETALL etc.)
    KeyValueArray(Vec<(Bytes, Bytes)>),
    /// Error response
    Error(String),
    /// Type error
    WrongType,
    /// Null response
    Null,
    /// SCAN result (cursor, keys)
    Scan(u64, Vec<Bytes>),
}

impl CommandResult {
    /// Create OK response
    pub fn ok() -> Self {
        CommandResult::Ok
    }

    /// Create integer response
    pub fn integer(n: i64) -> Self {
        CommandResult::Integer(n)
    }

    /// Create value response
    pub fn value(v: Option<Bytes>) -> Self {
        CommandResult::Value(v)
    }

    /// Create array response
    pub fn array(arr: Vec<Option<Bytes>>) -> Self {
        CommandResult::Array(arr)
    }

    /// Create error response
    pub fn error(msg: impl Into<String>) -> Self {
        CommandResult::Error(msg.into())
    }

    /// Create type error response
    pub fn wrong_type() -> Self {
        CommandResult::WrongType
    }
}

impl From<CommandResult> for RespValue {
    fn from(result: CommandResult) -> Self {
        match result {
            CommandResult::Ok => RespValue::SimpleString(Bytes::from("OK")),
            CommandResult::Pong => RespValue::SimpleString(Bytes::from("PONG")),
            CommandResult::SimpleString(s) => RespValue::SimpleString(s),
            CommandResult::Integer(n) => RespValue::Integer(n),
            CommandResult::Value(v) => match v {
                Some(bytes) => RespValue::BulkString(Some(bytes)),
                None => RespValue::Null,
            },
            CommandResult::Array(arr) => RespValue::Array(
                arr.into_iter()
                    .map(|v| match v {
                        Some(bytes) => RespValue::BulkString(Some(bytes)),
                        None => RespValue::Null,
                    })
                    .collect(),
            ),
            CommandResult::KeyValueArray(kvs) => RespValue::Array(
                kvs.into_iter()
                    .flat_map(|(k, v)| {
                        vec![
                            RespValue::BulkString(Some(k)),
                            RespValue::BulkString(Some(v)),
                        ]
                    })
                    .collect(),
            ),
            CommandResult::Error(msg) => RespValue::Error(Bytes::from(format!("ERR {}", msg))),
            CommandResult::WrongType => RespValue::Error(Bytes::from(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
            CommandResult::Null => RespValue::Null,
            CommandResult::Scan(cursor, keys) => RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from(cursor.to_string()))),
                RespValue::Array(
                    keys.into_iter()
                        .map(|k| RespValue::BulkString(Some(k)))
                        .collect(),
                ),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_to_resp() {
        let result = CommandResult::Ok;
        let resp: RespValue = result.into();
        assert_eq!(resp, RespValue::SimpleString(Bytes::from(b"OK" as &[u8])));
    }

    #[test]
    fn test_integer_to_resp() {
        let result = CommandResult::Integer(42);
        let resp: RespValue = result.into();
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[test]
    fn test_value_to_resp() {
        let result = CommandResult::Value(Some(Bytes::from(b"hello" as &[u8])));
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::BulkString(Some(bytes::Bytes::from(b"hello" as &[u8])))
        );
    }

    #[test]
    fn test_null_value_to_resp() {
        let result = CommandResult::Value(None);
        let resp: RespValue = result.into();
        assert_eq!(resp, RespValue::Null);
    }

    #[test]
    fn test_array_to_resp() {
        let result = CommandResult::Array(vec![
            Some(Bytes::from(b"a" as &[u8])),
            None,
            Some(Bytes::from(b"b" as &[u8])),
        ]);
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from(b"a" as &[u8]))),
                RespValue::Null,
                RespValue::BulkString(Some(bytes::Bytes::from(b"b" as &[u8]))),
            ])
        );
    }

    #[test]
    fn test_kv_array_to_resp() {
        let result = CommandResult::KeyValueArray(vec![
            (Bytes::from(b"field1" as &[u8]), Bytes::from(b"value1" as &[u8])),
            (Bytes::from(b"field2" as &[u8]), Bytes::from(b"value2" as &[u8])),
        ]);
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from(b"field1" as &[u8]))),
                RespValue::BulkString(Some(bytes::Bytes::from(b"value1" as &[u8]))),
                RespValue::BulkString(Some(bytes::Bytes::from(b"field2" as &[u8]))),
                RespValue::BulkString(Some(bytes::Bytes::from(b"value2" as &[u8]))),
            ])
        );
    }

    #[test]
    fn test_scan_to_resp() {
        let result = CommandResult::Scan(
            42,
            vec![Bytes::from(b"key1" as &[u8]), Bytes::from(b"key2" as &[u8])],
        );
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from(b"42" as &[u8]))),
                RespValue::Array(vec![
                    RespValue::BulkString(Some(bytes::Bytes::from(b"key1" as &[u8]))),
                    RespValue::BulkString(Some(bytes::Bytes::from(b"key2" as &[u8]))),
                ]),
            ])
        );
    }
}
