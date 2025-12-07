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
                let data = format!("+{}\r\n", s);
                self.writer.write_all(data.as_bytes()).await?;
            }
            RespValue::Error(e) => {
                let data = format!("-{}\r\n", e);
                self.writer.write_all(data.as_bytes()).await?;
            }
            RespValue::Integer(i) => {
                let data = format!(":{}\r\n", i);
                self.writer.write_all(data.as_bytes()).await?;
            }
            RespValue::BulkString(Some(bytes)) => {
                let header = format!("${}\r\n", bytes.len());
                self.writer.write_all(header.as_bytes()).await?;
                self.writer.write_all(bytes).await?;
                self.writer.write_all(b"\r\n").await?;
            }
            RespValue::BulkString(None) | RespValue::Null => {
                self.writer.write_all(b"$-1\r\n").await?;
            }
            RespValue::Array(items) => {
                let header = format!("*{}\r\n", items.len());
                self.writer.write_all(header.as_bytes()).await?;
                for item in items {
                    // Box the recursive call to avoid infinite future size
                    let fut = Box::pin(async { self.encode(&item).await });
                    fut.await?;
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
