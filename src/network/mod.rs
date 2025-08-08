// network.rs
use crate::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Network, RaftId, RequestVoteRequest, RequestVoteResponse, RpcError, RpcResult,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, timeout};
use tonic::transport::{Channel, Endpoint};

// 假设这是由 Protobuf 生成的 gRPC 客户端和服务 trait
// use your_crate::raft_service_client::RaftServiceClient;
// use your_crate::{RaftRpcRequest, RaftRpcResponse, ...};

// --- 配置和内部结构 ---

// 用于存储到远程节点的 gRPC 客户端
type GrpcClient = tonic::transport::Channel; // 简化，实际可能需要包装
type ClientMap = Arc<RwLock<HashMap<String, GrpcClient>>>;

// 用于将消息从 Network 实例发送到 gRPC 发送任务
#[derive(Debug)]
enum OutgoingMessage {
    RequestVote {
        target_node: String,
        args: Box<RequestVoteRequest>,
    },
    RequestVoteResponse {
        target_node: String,
        args: Box<RequestVoteResponse>,
    },
    AppendEntries {
        target_node: String,
        args: Box<AppendEntriesRequest>,
    },
    AppendEntriesResponse {
        target_node: String,
        args: Box<AppendEntriesResponse>,
    },
    InstallSnapshot {
        target_node: String,
        args: Box<InstallSnapshotRequest>,
    },
    InstallSnapshotResponse {
        target_node: String,
        args: Box<InstallSnapshotResponse>,
    },
}

// --- Network 实现 ---

pub struct MultiRaftNetwork {
    local_node_id: String, // 当前进程的 NodeId
    // sender 用于将消息从 RaftState 发送到网络层的发送任务
    outgoing_tx: mpsc::UnboundedSender<OutgoingMessage>,
    // receiver 用于接收来自 gRPC 服务的消息并分发
    // 注意：这个 receiver 可能需要被 Network 实例持有，或者通过其他方式传递给分发逻辑
    // 这里为了简化，假设它在初始化时被取出并用于启动分发任务
    // incoming_rx: Option<mpsc::UnboundedReceiver<(RaftId, Event)>>,
    clients: ClientMap,
    grpc_server_addr: String, // 本节点 gRPC 服务监听地址
}

impl MultiRaftNetwork {
    pub fn new(local_node_id: String, grpc_server_addr: String) -> Self {
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();
        let clients: ClientMap = Arc::new(RwLock::new(HashMap::new()));

        let network = Self {
            local_node_id,
            outgoing_tx,
            // incoming_rx: None, // 初始化时取出
            clients: clients.clone(),
            grpc_server_addr,
        };

        // 启动 gRPC 消息发送任务
        tokio::spawn(Self::run_message_sender(outgoing_rx, clients.clone()));

        // 启动 gRPC 服务端 (这通常在应用启动时调用一次)
        // tokio::spawn(Self::run_grpc_server(network.grpc_server_addr.clone(), /* 需要传入消息分发通道 */));

        network
    }

    // 获取或创建到远程节点的 gRPC 客户端
    async fn get_or_create_client(&self, target_node_id: &str) -> Result<GrpcClient, RpcError> {
        // 1. 尝试从缓存读取
        {
            let clients_read = self.clients.read().await;
            if let Some(client) = clients_read.get(target_node_id) {
                return Ok(client.clone());
            }
        }

        // 2. 如果没有，尝试创建 (需要知道 target_node_id 对应的 gRPC 地址)
        // 这里假设有一个方法可以根据 NodeId 获取地址
        let target_addr = Self::resolve_node_address(target_node_id)
            .await
            .map_err(|e| {
                RpcError::Network(format!(
                    "Failed to resolve address for {}: {}",
                    target_node_id, e
                ))
            })?;

        let endpoint = Endpoint::from_shared(target_addr)
            .map_err(|e| {
                RpcError::Network(format!("Invalid endpoint for {}: {}", target_node_id, e))
            })?
            .connect_timeout(StdDuration::from_secs(5))
            .timeout(StdDuration::from_secs(10));

        let channel = endpoint.connect().await.map_err(|e| {
            RpcError::Network(format!("Failed to connect to {}: {}", target_node_id, e))
        })?;

        // 3. 写入缓存
        {
            let mut clients_write = self.clients.write().await;
            // 再次检查，防止竞态
            if let Some(client) = clients_write.get(target_node_id) {
                Ok(client.clone())
            } else {
                clients_write.insert(target_node_id.to_string(), channel.clone());
                Ok(channel)
            }
        }
    }

    // 根据 NodeId 解析 gRPC 地址 (需要你实现)
    async fn resolve_node_address(
        node_id: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // 示例：从配置、服务发现等获取地址
        // 这里硬编码示例
        match node_id {
            "node1" => Ok("http://127.0.0.1:50051".to_string()),
            "node2" => Ok("http://127.0.0.1:50052".to_string()),
            "node3" => Ok("http://127.0.0.1:50053".to_string()),
            _ => Err(format!("Unknown node ID: {}", node_id).into()),
        }
    }

    // 运行异步任务，批量发送消息到远程节点
    async fn run_message_sender(
        mut rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        clients: ClientMap,
    ) {
        const BATCH_SIZE: usize = 10; // 批量大小
        const BATCH_TIMEOUT: Duration = Duration::from_millis(10); // 批量超时

        let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(BATCH_SIZE);

        loop {
            // 收集一批消息
            let collect_fut = async {
                while batch.len() < BATCH_SIZE {
                    match timeout(BATCH_TIMEOUT, rx.recv()).await {
                        Ok(Some(msg)) => batch.push(msg),
                        Ok(None) => return, // Channel closed
                        Err(_) => break,    // Timeout, send current batch
                    }
                }
            };

            collect_fut.await;

            if batch.is_empty() {
                continue;
            }

            // 按目标节点分组消息
            let mut messages_by_node: HashMap<String, Vec<OutgoingMessage>> = HashMap::new();
            for msg in batch.drain(..) {
                let target_node = match &msg {
                    OutgoingMessage::RequestVote { target_node, .. } => target_node.clone(),
                    OutgoingMessage::RequestVoteResponse { target_node, .. } => target_node.clone(),
                    OutgoingMessage::AppendEntries { target_node, .. } => target_node.clone(),
                    OutgoingMessage::AppendEntriesResponse { target_node, .. } => {
                        target_node.clone()
                    }
                    OutgoingMessage::InstallSnapshot { target_node, .. } => target_node.clone(),
                    OutgoingMessage::InstallSnapshotResponse { target_node, .. } => {
                        target_node.clone()
                    }
                };
                messages_by_node
                    .entry(target_node)
                    .or_insert_with(Vec::new)
                    .push(msg);
            }

            // 为每个目标节点启动一个任务发送消息
            for (target_node_id, msgs) in messages_by_node {
                let clients_clone = clients.clone();
                tokio::spawn(async move {
                    // 获取客户端
                    let client_result = {
                        let clients_read = clients_clone.read().await;
                        clients_read.get(&target_node_id).cloned() // 简化处理，实际可能需要重新获取
                    };

                    let mut client = match client_result {
                        Some(c) => c, // 假设 Channel 可以被克隆
                        None => {
                            // 如果没有客户端，尝试获取或创建
                            // 注意：这里简化了，实际可能需要更复杂的重试逻辑
                            // 并且需要处理 get_or_create_client 的错误
                            eprintln!(
                                "No client found for {} when trying to send batch",
                                target_node_id
                            );
                            return; // 或者尝试重新连接
                        }
                    };

                    // 发送消息 (这里以 RequestVote 为例，需要为其他类型添加逻辑)
                    for msg in msgs {
                        let send_result: RpcResult<()> = match msg {
                            OutgoingMessage::RequestVote { args, .. } => {
                                // let grpc_req = your_crate::RaftRpcRequest {
                                //     payload: Some(your_crate::raft_rpc_request::Payload::RequestVoteRequest(
                                //         args.into() // 需要实现转换
                                //     )),
                                // };
                                // let grpc_client = RaftServiceClient::new(client.clone());
                                // grpc_client.handle_rpc(grpc_req).await
                                // 模拟发送成功
                                println!("Sending RequestVote to {}", target_node_id);
                                Ok(())
                            }
                            // ... 为其他消息类型添加发送逻辑 ...
                            _ => {
                                eprintln!("Sending logic for message type not implemented yet");
                                Ok(())
                            }
                        };

                        if let Err(e) = send_result {
                            eprintln!("Failed to send message to {}: {:?}", target_node_id, e);
                            // 可能需要从缓存中移除失效的客户端
                            // let mut clients_write = clients_clone.write().await;
                            // clients_write.remove(&target_node_id);
                        }
                    }
                });
            }
        }
    }

    // 启动 gRPC 服务端 (通常在应用主函数中调用)
    // pub async fn run_grpc_server(addr: String, dispatch_tx: mpsc::UnboundedSender<(RaftId, Event)>) -> Result<(), Box<dyn std::error::Error>> {
    //     let addr = addr.parse()?;
    //     let service = RaftGrpcService::new(dispatch_tx); // 需要实现这个服务
    //     Server::builder().add_service(RaftServiceServer::new(service)).serve(addr).await?;
    //     Ok(())
    // }
}

#[async_trait]
impl Network for MultiRaftNetwork {
    async fn send_request_vote_request(
        &self,
        _from: RaftId, // 本地 RaftId，可能用于日志
        target: RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()> {
        // 注意：args 中应该已经包含了 from 和 target 信息
        if self
            .outgoing_tx
            .send(OutgoingMessage::RequestVote {
                target_node: target.node.clone(), // 使用 node 进行路由
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }

    async fn send_request_vote_response(
        &self,
        _from: RaftId,
        target: RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        if self
            .outgoing_tx
            .send(OutgoingMessage::RequestVoteResponse {
                target_node: target.node.clone(),
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }

    async fn send_append_entries_request(
        &self,
        _from: RaftId,
        target: RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        if self
            .outgoing_tx
            .send(OutgoingMessage::AppendEntries {
                target_node: target.node.clone(),
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }

    async fn send_append_entries_response(
        &self,
        _from: RaftId,
        target: RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        if self
            .outgoing_tx
            .send(OutgoingMessage::AppendEntriesResponse {
                target_node: target.node.clone(),
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }

    async fn send_install_snapshot_request(
        &self,
        _from: RaftId,
        target: RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        if self
            .outgoing_tx
            .send(OutgoingMessage::InstallSnapshot {
                target_node: target.node.clone(),
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }

    async fn send_install_snapshot_response(
        &self,
        _from: RaftId,
        target: RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        if self
            .outgoing_tx
            .send(OutgoingMessage::InstallSnapshotResponse {
                target_node: target.node.clone(),
                args: Box::new(args),
            })
            .is_err()
        {
            Err(RpcError::Network("Network channel closed".into()))
        } else {
            Ok(())
        }
    }
}

// --- gRPC 服务端实现 (示例) ---

// 这个服务需要实现 Protobuf 生成的 `RaftService` trait
// pub struct RaftGrpcService {
//     // 用于将接收到的消息分发到正确的 RaftState
//     dispatch_tx: mpsc::UnboundedSender<(RaftId, Event)>,
// }
//
// #[tonic::async_trait]
// impl your_crate::raft_service_server::RaftService for RaftGrpcService {
//     async fn handle_rpc(
//         &self,
//         request: tonic::Request<RaftRpcRequest>,
//     ) -> Result<tonic::Response<RaftRpcResponse>, tonic::Status> {
//         let grpc_req = request.into_inner();
//         let response_payload = match grpc_req.payload {
//             Some(your_crate::raft_rpc_request::Payload::RequestVoteRequest(rv_req)) => {
//                 // 1. 转换 Protobuf 消息为内部 Raft 消息
//                 let raft_req: RequestVoteRequest = rv_req.into(); // 需要实现
//                 let target_raft_id = raft_req.target_id.clone(); // 获取目标 Raft Group ID
//
//                 // 2. 创建 Event
//                 let event = Event::RequestVoteRequest(raft_req);
//
//                 // 3. 发送到分发通道
//                 if self.dispatch_tx.send((target_raft_id, event)).is_err() {
//                     return Err(tonic::Status::unavailable("Internal dispatch channel closed"));
//                 }
//
//                 // 4. 等待或构造响应 (取决于你的 RaftState 如何处理和响应)
//                 // 这可能需要一个关联请求 ID 的响应通道机制
//                 // 为简化，这里返回一个空的或默认的响应
//                 Some(your_crate::raft_rpc_response::Payload::RequestVoteResponse(
//                     your_crate::RequestVoteResponse { /* ... */ }
//                 ))
//             }
//             // ... 为其他消息类型添加处理逻辑 ...
//             None => return Err(tonic::Status::invalid_argument("Empty request payload")),
//         };
//
//         let response = RaftRpcResponse {
//             payload: response_payload,
//         };
//         Ok(tonic::Response::new(response))
//     }
// }
