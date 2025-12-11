//! RESP protocol async parser

use crate::{RespError, RespValue};
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

/// Default maximum frame size: 512MB (prevents memory overflow attacks)
pub const DEFAULT_MAX_FRAME_SIZE: usize = 512 * 1024 * 1024;

/// RESP protocol async parser
pub struct AsyncRespParser<R: AsyncRead + Unpin> {
    reader: BufReader<R>,
    max_bytes: usize,
    bytes_read: usize,
}

impl<R: AsyncRead + Unpin> AsyncRespParser<R> {
    /// Create a new async parser (using default max frame size)
    pub fn new(reader: R) -> Self {
        Self::with_max_bytes(reader, DEFAULT_MAX_FRAME_SIZE)
    }

    /// Create a new async parser (specify max frame size)
    ///
    /// # Arguments
    /// * `reader` - Async reader
    /// * `max_bytes` - Maximum frame size limit (bytes), prevents memory overflow attacks
    pub fn with_max_bytes(reader: R, max_bytes: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            max_bytes,
            bytes_read: 0,
        }
    }

    /// Check and update the number of bytes read
    fn check_frame_size(&mut self, additional: usize) -> Result<(), RespError> {
        self.bytes_read = self.bytes_read.saturating_add(additional);
        if self.bytes_read > self.max_bytes {
            Err(RespError::FrameTooLarge(self.bytes_read, self.max_bytes))
        } else {
            Ok(())
        }
    }

    /// Reset byte counter (for Pipeline parsing)
    pub fn reset_bytes_read(&mut self) {
        self.bytes_read = 0;
    }

    /// Parse multiple RESP values from EOF buffer (Pipeline support)
    ///
    /// Used for scenarios like `redis-benchmark -P 32`, where a single read may contain multiple commands
    pub async fn decode_eof(&mut self) -> Result<Vec<RespValue>, RespError> {
        let mut results = Vec::new();

        loop {
            match self.parse().await {
                Ok(value) => results.push(value),
                Err(RespError::UnexpectedEof) => {
                    // EOF is normal, indicating no more data
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }

    /// Parse next RESP value
    pub async fn parse(&mut self) -> Result<RespValue, RespError> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            return Err(RespError::UnexpectedEof);
        }

        // Check frame size
        self.check_frame_size(bytes_read)?;

        let line = line.trim_end();
        if line.is_empty() {
            return Err(RespError::InvalidFormat("Empty line".to_string()));
        }

        // Get the first byte as type identifier
        let buf = line.as_bytes();
        if buf.is_empty() {
            return Err(RespError::InvalidFormat("Empty line".to_string()));
        }

        match buf[0] {
            b'*' => self.parse_array(&line).await,
            b'$' => self.parse_bulk(&line).await,
            b':' => self.parse_int(&line).await,
            b'+' => self.parse_simple(&line).await,
            b'-' => self.parse_error(&line).await,
            _ => Err(RespError::InvalidType(buf[0])),
        }
    }

    /// Parse simple string: +OK\r\n
    async fn parse_simple(&mut self, line: &str) -> Result<RespValue, RespError> {
        let value = &line[1..];
        // Verify no unescaped CRLF
        if value.contains('\r') || value.contains('\n') {
            return Err(RespError::InvalidFormat(
                "Simple string cannot contain CR or LF".to_string(),
            ));
        }
        Ok(RespValue::SimpleString(Bytes::from(value.to_string())))
    }

    /// Parse error: -ERR message\r\n
    async fn parse_error(&mut self, line: &str) -> Result<RespValue, RespError> {
        let error = line[1..].to_string();
        Ok(RespValue::Error(Bytes::from(error)))
    }

    /// Parse integer: :123\r\n
    async fn parse_int(&mut self, line: &str) -> Result<RespValue, RespError> {
        let num_str = &line[1..];
        // Check integer overflow: first try parsing as i128 to detect overflow, then convert to i64
        let num = num_str
            .parse::<i128>()
            .map_err(|_| RespError::InvalidFormat(format!("Invalid integer: {}", num_str)))?;

        // Check if within i64 range
        if num > i64::MAX as i128 || num < i64::MIN as i128 {
            return Err(RespError::IntegerOverflow);
        }

        Ok(RespValue::Integer(num as i64))
    }

    /// Parse bulk string: $5\r\nhello\r\n
    async fn parse_bulk(&mut self, line: &str) -> Result<RespValue, RespError> {
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
            // Check frame size (including data + CRLF)
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

            Ok(RespValue::BulkString(Some(bytes::Bytes::from(buffer))))
        }
    }

    /// Parse array: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
    async fn parse_array(&mut self, line: &str) -> Result<RespValue, RespError> {
        let count_str = &line[1..];
        let count = count_str.parse::<i64>().map_err(|_| {
            RespError::InvalidFormat(format!("Invalid array length: {}", count_str))
        })?;

        if count == -1 {
            // Null array
            Ok(RespValue::Null)
        } else if count < 0 {
            Err(RespError::InvalidFormat(format!(
                "Invalid array length: {}",
                count
            )))
        } else {
            let count = count as usize;
            // Check array size is reasonable (prevent malicious clients from sending oversized arrays)
            if count > 1024 * 1024 {
                return Err(RespError::InvalidFormat(format!(
                    "Array too large: {} elements",
                    count
                )));
            }

            let mut array = Vec::with_capacity(count);
            for _ in 0..count {
                let parse_fut = Box::pin(async { self.parse().await });
                array.push(parse_fut.await?);
            }
            Ok(RespValue::Array(array))
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
        assert_eq!(result, RespValue::SimpleString(Bytes::from("OK")));
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let reader = Builder::new().read(data).build();
        let mut parser = AsyncRespParser::with_max_bytes(reader, 1024);
        let result = parser.parse().await.unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(bytes::Bytes::from(b"hello" as &[u8])))
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
                assert_eq!(
                    items[0],
                    RespValue::BulkString(Some(bytes::Bytes::from(b"GET" as &[u8])))
                );
                assert_eq!(
                    items[1],
                    RespValue::BulkString(Some(bytes::Bytes::from(b"key" as &[u8])))
                );
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
