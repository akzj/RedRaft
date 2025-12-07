//! 命令执行结果类型

use crate::RespValue;

/// 命令执行结果
/// 
/// 表示 Redis 命令的所有可能返回类型，可以方便地转换为 RespValue
#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    /// OK 响应
    Ok,
    /// PONG 响应
    Pong,
    /// 自定义简单字符串
    SimpleString(String),
    /// 整数响应
    Integer(i64),
    /// 单个值（可能为空）
    Value(Option<Vec<u8>>),
    /// 值数组
    Array(Vec<Option<Vec<u8>>>),
    /// 键值对数组（用于 HGETALL 等）
    KeyValueArray(Vec<(Vec<u8>, Vec<u8>)>),
    /// 错误响应
    Error(String),
    /// 类型错误
    WrongType,
    /// Null 响应
    Null,
    /// SCAN 结果 (cursor, keys)
    Scan(u64, Vec<Vec<u8>>),
}

impl CommandResult {
    /// 创建 OK 响应
    pub fn ok() -> Self {
        CommandResult::Ok
    }

    /// 创建整数响应
    pub fn integer(n: i64) -> Self {
        CommandResult::Integer(n)
    }

    /// 创建值响应
    pub fn value(v: Option<Vec<u8>>) -> Self {
        CommandResult::Value(v)
    }

    /// 创建数组响应
    pub fn array(arr: Vec<Option<Vec<u8>>>) -> Self {
        CommandResult::Array(arr)
    }

    /// 创建错误响应
    pub fn error(msg: impl Into<String>) -> Self {
        CommandResult::Error(msg.into())
    }

    /// 创建类型错误响应
    pub fn wrong_type() -> Self {
        CommandResult::WrongType
    }
}

impl From<CommandResult> for RespValue {
    fn from(result: CommandResult) -> Self {
        match result {
            CommandResult::Ok => RespValue::SimpleString("OK".to_string()),
            CommandResult::Pong => RespValue::SimpleString("PONG".to_string()),
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
            CommandResult::Error(msg) => RespValue::Error(format!("ERR {}", msg)),
            CommandResult::WrongType => RespValue::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            ),
            CommandResult::Null => RespValue::Null,
            CommandResult::Scan(cursor, keys) => RespValue::Array(vec![
                RespValue::BulkString(Some(cursor.to_string().into_bytes())),
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
        assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_integer_to_resp() {
        let result = CommandResult::Integer(42);
        let resp: RespValue = result.into();
        assert_eq!(resp, RespValue::Integer(42));
    }

    #[test]
    fn test_value_to_resp() {
        let result = CommandResult::Value(Some(b"hello".to_vec()));
        let resp: RespValue = result.into();
        assert_eq!(resp, RespValue::BulkString(Some(b"hello".to_vec())));
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
            Some(b"a".to_vec()),
            None,
            Some(b"b".to_vec()),
        ]);
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"a".to_vec())),
                RespValue::Null,
                RespValue::BulkString(Some(b"b".to_vec())),
            ])
        );
    }

    #[test]
    fn test_kv_array_to_resp() {
        let result = CommandResult::KeyValueArray(vec![
            (b"field1".to_vec(), b"value1".to_vec()),
            (b"field2".to_vec(), b"value2".to_vec()),
        ]);
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"field1".to_vec())),
                RespValue::BulkString(Some(b"value1".to_vec())),
                RespValue::BulkString(Some(b"field2".to_vec())),
                RespValue::BulkString(Some(b"value2".to_vec())),
            ])
        );
    }

    #[test]
    fn test_scan_to_resp() {
        let result = CommandResult::Scan(42, vec![b"key1".to_vec(), b"key2".to_vec()]);
        let resp: RespValue = result.into();
        assert_eq!(
            resp,
            RespValue::Array(vec![
                RespValue::BulkString(Some(b"42".to_vec())),
                RespValue::Array(vec![
                    RespValue::BulkString(Some(b"key1".to_vec())),
                    RespValue::BulkString(Some(b"key2".to_vec())),
                ]),
            ])
        );
    }
}
