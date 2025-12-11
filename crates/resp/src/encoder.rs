//! RESP protocol sync encoder

use crate::RespValue;
use std::io::{self, Write};

/// RESP protocol sync encoder
pub struct RespEncoder<W: Write> {
    writer: W,
}

impl<W: Write> RespEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Encode RESP value and write
    pub fn encode(&mut self, value: &RespValue) -> io::Result<()> {
        match value {
            RespValue::SimpleString(s) => {
                write!(self.writer, "+{}\r\n", String::from_utf8_lossy(&s))?;
            }
            RespValue::Error(e) => {
                write!(self.writer, "-{}\r\n", String::from_utf8_lossy(&e))?;
            }
            RespValue::Integer(i) => {
                write!(self.writer, ":{}\r\n", i)?;
            }
            RespValue::BulkString(Some(bytes)) => {
                write!(self.writer, "${}\r\n", bytes.len())?;
                self.writer.write_all(bytes)?;
                write!(self.writer, "\r\n")?;
            }
            RespValue::BulkString(None) | RespValue::Null => {
                write!(self.writer, "$-1\r\n")?;
            }
            RespValue::Array(items) => {
                write!(self.writer, "*{}\r\n", items.len())?;
                for item in items {
                    self.encode(item)?;
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}

/// Encode RESP value and return byte vector (for testing)
pub fn encode_to_vec(value: &RespValue) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut encoder = RespEncoder::new(&mut buffer);
    encoder.encode(value).unwrap();
    buffer
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let value = RespValue::SimpleString(Bytes::from("OK"));
        let result = encode_to_vec(&value);
        assert_eq!(String::from_utf8_lossy(&result), "+OK\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let value = RespValue::BulkString(Some(Bytes::from(b"hello" as &[u8])));
        let result = encode_to_vec(&value);
        assert_eq!(String::from_utf8_lossy(&result), "$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_array() {
        let value = RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from(b"GET" as &[u8]))),
            RespValue::BulkString(Some(bytes::Bytes::from(b"key" as &[u8]))),
        ]);
        let result = encode_to_vec(&value);
        assert_eq!(
            String::from_utf8_lossy(&result),
            "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
        );
    }
}
