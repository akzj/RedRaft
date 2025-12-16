//! Redis protocol server
//!
//! Handles Redis client connections and commands

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::split;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::node::RRNode;
use resp::{AsyncRespEncoder, AsyncRespParser, Command, RespValue};

/// Redis protocol server
pub struct RedisServer {
    node: Arc<RRNode>,
    addr: SocketAddr,
}

impl RedisServer {
    pub fn new(node: Arc<RRNode>, addr: SocketAddr) -> Self {
        Self { node, addr }
    }

    /// Start server
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

/// Handle client connection
async fn handle_client(
    stream: TcpStream,
    node: Arc<RRNode>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = split(stream);
    let mut parser = AsyncRespParser::new(reader);
    let mut encoder = AsyncRespEncoder::new(writer);

    loop {
        // Parse RESP command
        let resp_value = match parser.parse().await {
            Ok(v) => v,
            Err(e) => {
                // Send error response
                let error = RespValue::Error(bytes::Bytes::from(format!("ERR {}", e)));
                encoder.encode(&error).await?;
                break;
            }
        };

        // Convert to type-safe Command
        let command = match Command::try_from(&resp_value) {
            Ok(cmd) => cmd,
            Err(e) => {
                let error = RespValue::Error(bytes::Bytes::from(format!("ERR {}", e)));
                encoder.encode(&error).await?;
                continue;
            }
        };

        // Handle command
        let response = match node.handle_command(command).await {
            Ok(resp) => resp,
            Err(e) => RespValue::Error(bytes::Bytes::from(format!("ERR {}", e))),
        };

        encoder.encode(&response).await?;
    }

    Ok(())
}
