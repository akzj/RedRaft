//! Redis 协议服务器
//!
//! 处理 Redis 客户端连接和命令

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::split;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::node::RedRaftNode;
use redis_protocol::{AsyncRespEncoder, AsyncRespParser, RespValue};

/// Redis 协议服务器
pub struct RedisServer {
    node: Arc<RedRaftNode>,
    addr: SocketAddr,
}

impl RedisServer {
    pub fn new(node: Arc<RedRaftNode>, addr: SocketAddr) -> Self {
        Self { node, addr }
    }

    /// 启动服务器
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Redis server listening on {}", self.addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New client connection from {}", addr);
                    let node = self.node.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, node).await {
                            warn!("Error handling client {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// 处理客户端连接
async fn handle_client(
    stream: TcpStream,
    node: Arc<RedRaftNode>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = split(stream);
    let mut parser = AsyncRespParser::new(reader);
    let mut encoder = AsyncRespEncoder::new(writer);

    loop {
        // 解析 RESP 命令
        let resp_value = match parser.parse().await {
            Ok(v) => v,
            Err(e) => {
                // 发送错误响应
                let error = RespValue::Error(format!("ERR {}", e));
                encoder.encode(&error).await?;
                break;
            }
        };

        // 转换为命令
        let command = match resp_value.to_command() {
            Some(cmd) => cmd,
            None => {
                let error = RespValue::Error("ERR invalid command format".to_string());
                encoder.encode(&error).await?;
                continue;
            }
        };

        if command.is_empty() {
            continue;
        }

        // 处理命令
        let result = node.handle_command(command).await;

        // 编码响应
        let response = match result {
            Ok(data) => {
                if data.is_empty() {
                    RespValue::Null
                } else {
                    RespValue::BulkString(Some(data))
                }
            }
            Err(e) => RespValue::Error(format!("ERR {}", e)),
        };

        encoder.encode(&response).await?;
    }

    Ok(())
}
