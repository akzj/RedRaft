//! RESP protocol sync parser

use bytes::Bytes;

use crate::{RespError, RespValue};
use std::io::{BufRead, BufReader, Read};
use std::str;

/// RESP protocol sync parser
pub struct RespParser<R: Read> {
    reader: BufReader<R>,
    buffer: Vec<u8>,
}

impl<R: Read> RespParser<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            buffer: Vec::new(),
        }
    }

    /// Parse next RESP value
    pub fn parse(&mut self) -> Result<RespValue, RespError> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;

        if line.is_empty() {
            return Err(RespError::UnexpectedEof);
        }

        let line = line.trim_end();
        if line.is_empty() {
            return Err(RespError::InvalidFormat("Empty line".to_string()));
        }

        let prefix = line
            .chars()
            .next()
            .ok_or_else(|| RespError::InvalidFormat("Empty line".to_string()))?;

        match prefix {
            '+' => {
                // Simple String: +OK\r\n
                // Verify no unescaped CRLF
                let value = &line[1..];
                if value.contains('\r') || value.contains('\n') {
                    return Err(RespError::InvalidFormat(
                        "Simple string cannot contain CR or LF".to_string(),
                    ));
                }
                Ok(RespValue::SimpleString(Bytes::copy_from_slice(value.as_bytes())))
            }
            '-' => {
                // Error: -ERR message\r\n
                // Error message may contain CRLF, but read_line already handled it
                let error = Bytes::copy_from_slice(line[1..].as_bytes());
                Ok(RespValue::Error(error))
            }
            ':' => {
                // Integer: :123\r\n
                let num_str = &line[1..];
                // Check for integer overflow: first parse as i128 to detect overflow, then convert to i64
                let num = num_str.parse::<i128>().map_err(|_| {
                    RespError::InvalidFormat(format!("Invalid integer: {}", num_str))
                })?;

                // Check if within i64 range
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
                    let mut buffer = vec![0u8; len];
                    self.reader.read_exact(&mut buffer)?;

                    // Read \r\n
                    let mut crlf = [0u8; 2];
                    self.reader.read_exact(&mut crlf)?;
                    if crlf != [b'\r', b'\n'] {
                        return Err(RespError::InvalidFormat(
                            "Expected \\r\\n after bulk string".to_string(),
                        ));
                    }

                    Ok(RespValue::BulkString(Some(bytes::Bytes::from(buffer))))
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
                    Err(RespError::InvalidFormat(format!(
                        "Invalid array length: {}",
                        count
                    )))
                } else {
                    let mut array = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        array.push(self.parse()?);
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
    use bytes::Bytes;

    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let mut parser = RespParser::new(Cursor::new(data));
        let result = parser.parse().unwrap();
        assert_eq!(result, RespValue::SimpleString(Bytes::from(b"OK" as &[u8])));
    }

    #[test]
    fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let mut parser = RespParser::new(Cursor::new(data));
        let result = parser.parse().unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(Bytes::from(b"hello" as &[u8])))
        );
    }

    #[test]
    fn test_parse_array() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let mut parser = RespParser::new(Cursor::new(data));
        let result = parser.parse().unwrap();
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

    #[test]
    fn test_simple_string_crlf_validation() {
        // Test that simple string cannot contain CR
        let data = b"+OK\rHI\r\n";
        let mut parser = RespParser::new(Cursor::new(data));
        assert!(parser.parse().is_err());

        // Test that simple string cannot contain LF
        let data = b"+OK\nHI\r\n";
        let mut parser = RespParser::new(Cursor::new(data));
        assert!(parser.parse().is_err());
    }

    #[test]
    fn test_integer_overflow() {
        // Test integer overflow (exceeding i64::MAX)
        let data = format!(":{}\r\n", i64::MAX as i128 + 1);
        let mut parser = RespParser::new(Cursor::new(data.as_bytes()));
        assert!(matches!(parser.parse(), Err(RespError::IntegerOverflow)));

        // Test integer within normal range
        let data = format!(":{}\r\n", i64::MAX);
        let mut parser = RespParser::new(Cursor::new(data.as_bytes()));
        assert!(matches!(parser.parse(), Ok(RespValue::Integer(i64::MAX))));
    }
}
