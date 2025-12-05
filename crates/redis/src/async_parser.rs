//! RESP 协议异步解析器

use crate::{RespError, RespValue};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

/// 默认最大帧大小：512MB（防止内存溢出攻击）
pub const DEFAULT_MAX_FRAME_SIZE: usize = 512 * 1024 * 1024;

/// RESP 协议异步解析器
pub struct AsyncRespParser<R: AsyncRead + Unpin> {
    reader: BufReader<R>,
    max_bytes: usize,
    bytes_read: usize,
}

impl<R: AsyncRead + Unpin> AsyncRespParser<R> {
    /// 创建新的异步解析器（使用默认最大帧大小）
    pub fn new(reader: R) -> Self {
        Self::with_max_bytes(reader, DEFAULT_MAX_FRAME_SIZE)
    }

    /// 创建新的异步解析器（指定最大帧大小）
    /// 
    /// # Arguments
    /// * `reader` - 异步读取器
    /// * `max_bytes` - 最大帧大小限制（字节），防止内存溢出攻击
    pub fn with_max_bytes(reader: R, max_bytes: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            max_bytes,
            bytes_read: 0,
        }
    }

    /// 检查并更新已读取字节数
    fn check_frame_size(&mut self, additional: usize) -> Result<(), RespError> {
        self.bytes_read = self.bytes_read.saturating_add(additional);
        if self.bytes_read > self.max_bytes {
            Err(RespError::FrameTooLarge(self.bytes_read, self.max_bytes))
        } else {
            Ok(())
        }
    }

    /// 重置字节计数器（用于 Pipeline 解析）
    pub fn reset_bytes_read(&mut self) {
        self.bytes_read = 0;
    }

    /// 从 EOF 缓冲区解析多个 RESP 值（Pipeline 支持）
    /// 
    /// 用于处理 `redis-benchmark -P 32` 等场景，一次读取可能包含多条命令
    pub async fn decode_eof(&mut self) -> Result<Vec<RespValue>, RespError> {
        let mut results = Vec::new();
        
        loop {
            match self.parse().await {
                Ok(value) => results.push(value),
                Err(RespError::UnexpectedEof) => {
                    // EOF 是正常的，表示没有更多数据
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok(results)
    }

    /// 解析下一个 RESP 值
    pub async fn parse(&mut self) -> Result<RespValue, RespError> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            return Err(RespError::UnexpectedEof);
        }

        // 检查帧大小
        self.check_frame_size(bytes_read)?;

        let line = line.trim_end();
        if line.is_empty() {
            return Err(RespError::InvalidFormat("Empty line".to_string()));
        }

        let prefix = line.chars().next().ok_or_else(|| {
            RespError::InvalidFormat("Empty line".to_string())
        })?;

        match prefix {
            '+' => {
                // Simple String: +OK\r\n
                // 验证不包含未转义的 CRLF
                let value = &line[1..];
                if value.contains('\r') || value.contains('\n') {
                    return Err(RespError::InvalidFormat(
                        "Simple string cannot contain CR or LF".to_string(),
                    ));
                }
                Ok(RespValue::SimpleString(value.to_string()))
            }
            '-' => {
                // Error: -ERR message\r\n
                let error = line[1..].to_string();
                Ok(RespValue::Error(error))
            }
            ':' => {
                // Integer: :123\r\n
                let num_str = &line[1..];
                // 检查整数溢出：先尝试解析为 i128 以检测溢出，再转换为 i64
                let num = num_str.parse::<i128>().map_err(|_| {
                    RespError::InvalidFormat(format!("Invalid integer: {}", num_str))
                })?;
                
                // 检查是否在 i64 范围内
                if num > i64::MAX as i128 || num < i64::MIN as i128 {
                    return Err(RespError::IntegerOverflow);
                }
                
                Ok(RespValue::Integer(num as i64))
            }
            '$' => {
                // Bulk String: $5\r\nhello\r\n
                let len_str = &line[1..];
                let len = len_str.parse::<i64>().map_err(|_| {
                    RespError::InvalidFormat(format!("Invalid bulk string length: {}", len_str))
                })?;

                if len == -1 {
                    // Null bulk string
                    Ok(RespValue::Null)
                } else if len < 0 {
                    Err(RespError::InvalidFormat(format!(
                        "Invalid bulk string length: {}",
                        len
                    )))
                } else {
                    let len = len as usize;
                    // 检查帧大小（包括数据 + CRLF）
                    self.check_frame_size(len + 2)?;
                    
                    let mut buffer = vec![0u8; len];
                    AsyncReadExt::read_exact(&mut self.reader, &mut buffer).await?;

                    // Read \r\n
                    let mut crlf = [0u8; 2];
                    AsyncReadExt::read_exact(&mut self.reader, &mut crlf).await?;
                    if crlf != [b'\r', b'\n'] {
                        return Err(RespError::InvalidFormat(
                            "Expected \\r\\n after bulk string".to_string(),
                        ));
                    }

                    Ok(RespValue::BulkString(Some(buffer)))
                }
            }
            '*' => {
                // Array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
                let count_str = &line[1..];
                let count = count_str.parse::<i64>().map_err(|_| {
                    RespError::InvalidFormat(format!("Invalid array length: {}", count_str))
                })?;

                if count == -1 {
                    // Null array
                    Ok(RespValue::Null)
                } else if count < 0 {
                    Err(RespError::InvalidFormat(format!("Invalid array length: {}", count)))
                } else {
                    let count = count as usize;
                    // 检查数组大小是否合理（防止恶意客户端发送超大数组）
                    if count > 1024 * 1024 {
                        return Err(RespError::InvalidFormat(
                            format!("Array too large: {} elements", count)
                        ));
                    }
                    
                    let mut array = Vec::with_capacity(count);
                    for _ in 0..count {
                        let parse_fut = async { self.parse().await };
                        array.push(Box::pin(parse_fut).await?);
                    }
                    Ok(RespValue::Array(array))
                }
            }
            _ => Err(RespError::InvalidFormat(format!(
                "Unknown RESP type: {}",
                prefix
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let result = parser.parse().await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let result = parser.parse().await.unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"hello".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_parse_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let result = parser.parse().await.unwrap();
        match result {
            RespValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(Some(b"GET".to_vec())));
                assert_eq!(items[1], RespValue::BulkString(Some(b"key".to_vec())));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_frame_too_large() {
        let data = b"$9999999999\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let result = parser.parse().await;
        assert!(matches!(result, Err(RespError::FrameTooLarge(_, _))));
    }

    #[tokio::test]
    async fn test_decode_eof_pipeline() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nSET\r\n$3\r\nval\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let results = parser.decode_eof().await.unwrap();
        assert_eq!(results.len(), 2);
    }
}
