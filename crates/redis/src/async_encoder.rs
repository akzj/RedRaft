//! RESP 协议异步编码器

use crate::RespValue;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

/// RESP 协议异步编码器
pub struct AsyncRespEncoder<W: AsyncWrite + Unpin> {
    writer: BufWriter<W>,
}

impl<W: AsyncWrite + Unpin> AsyncRespEncoder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }

    /// 编码 RESP 值并写入
    pub async fn encode(&mut self, value: &RespValue) -> std::io::Result<()> {
        match value {
            RespValue::SimpleString(s) => {
                write!(self.writer, "+{}\r\n", s).await?;
            }
            RespValue::Error(e) => {
                write!(self.writer, "-{}\r\n", e).await?;
            }
            RespValue::Integer(i) => {
                write!(self.writer, ":{}\r\n", i).await?;
            }
            RespValue::BulkString(Some(bytes)) => {
                write!(self.writer, "${}\r\n", bytes.len()).await?;
                self.writer.write_all(bytes).await?;
                write!(self.writer, "\r\n").await?;
            }
            RespValue::BulkString(None) | RespValue::Null => {
                write!(self.writer, "$-1\r\n").await?;
            }
            RespValue::Array(items) => {
                write!(self.writer, "*{}\r\n", items.len()).await?;
                for item in items {
                    self.encode(item).await?;
                }
            }
        }
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_test::io::Builder;

    #[tokio::test]
    async fn test_encode_simple_string() {
        let mut writer = Vec::new();
        let mut encoder = AsyncRespEncoder::new(&mut writer);
        let value = RespValue::SimpleString("OK".to_string());
        encoder.encode(&value).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&writer), "+OK\r\n");
    }

    #[tokio::test]
    async fn test_encode_bulk_string() {
        let mut writer = Vec::new();
        let mut encoder = AsyncRespEncoder::new(&mut writer);
        let value = RespValue::BulkString(Some(b"hello".to_vec()));
        encoder.encode(&value).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&writer), "$5\r\nhello\r\n");
    }

    #[tokio::test]
    async fn test_encode_array() {
        let mut writer = Vec::new();
        let mut encoder = AsyncRespEncoder::new(&mut writer);
        let value = RespValue::Array(vec![
            RespValue::BulkString(Some(b"GET".to_vec())),
            RespValue::BulkString(Some(b"key".to_vec())),
        ]);
        encoder.encode(&value).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(&writer),
            "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
        );
    }
}
