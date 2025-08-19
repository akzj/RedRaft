use crate::network::pb::raft_service_client::RaftServiceClient;
use crate::network::pb::raft_service_server::{RaftService, RaftServiceServer};
// network.rs
use crate::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Network, NodeId, RaftId, RequestVoteRequest, RequestVoteResponse, RpcError, RpcResult,
};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tonic::transport::{Channel, Endpoint, Server};
use tracing::{debug, error, info, warn};

pub mod pb;

#[async_trait]
pub trait ResolveNodeAddress {
    fn resolve_node_address(
        &self,
        node_id: &str,
    ) -> impl std::future::Future<Output = Result<String>> + Send;
}

#[async_trait]
pub trait MessageDispatcher: Send + Sync {
    async fn dispatch(&self, msg: OutgoingMessage) -> Result<()>;
}

// 假设这是由 Protobuf 生成的 gRPC 客户端和服务 trait
// use your_crate::raft_service_client::RaftServiceClient;
// use your_crate::{RaftRpcRequest, RaftRpcResponse, ...};

// --- 配置和内部结构 ---

// 用于存储到远程节点的 gRPC 客户端
type GrpcClient = tonic::transport::Channel; // 简化，实际可能需要包装

// 用于将消息从 Network 实例发送到 gRPC 发送任务
#[derive(Debug)]
pub enum OutgoingMessage {
    RequestVote {
        from: RaftId,
        target: RaftId,
        args: RequestVoteRequest,
    },
    RequestVoteResponse {
        from: RaftId,
        target: RaftId,
        args: RequestVoteResponse,
    },
    AppendEntries {
        from: RaftId,
        target: RaftId,
        args: AppendEntriesRequest,
    },
    AppendEntriesResponse {
        from: RaftId,
        target: RaftId,
        args: AppendEntriesResponse,
    },
    InstallSnapshot {
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotRequest,
    },
    InstallSnapshotResponse {
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotResponse,
    },
}

#[derive(Debug, Clone)]
pub struct MultiRaftNetworkOptions {
    node_id: String,
    grpc_server_addr: String,
    node_map: HashMap<NodeId, String>,
    connect_timeout: Duration,
    batch_size: usize,
    send_retry_count: usize,
    send_retry_delay: StdDuration,
}

impl ResolveNodeAddress for MultiRaftNetworkOptions {
    async fn resolve_node_address(&self, node_id: &str) -> Result<String> {
        self.node_map
            .get(node_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Node ID not found: {}", node_id))
    }
}

#[derive(Clone)]
pub struct MultiRaftNetwork {
    options: MultiRaftNetworkOptions,
    // sender 用于将消息从 RaftState 发送到网络层的发送任务
    outgoing_tx: Arc<RwLock<HashMap<NodeId, mpsc::UnboundedSender<OutgoingMessage>>>>,
    // receiver 用于接收来自 gRPC 服务的消息并分发
    // 注意：这个 receiver 可能需要被 Network 实例持有，或者通过其他方式传递给分发逻辑
    // 这里为了简化，假设它在初始化时被取出并用于启动分发任务
    // incoming_rx: Option<mpsc::UnboundedReceiver<(RaftId, Event)>>,
    clients: Arc<RwLock<HashMap<String, GrpcClient>>>,

    dispatch: Option<Arc<dyn MessageDispatcher>>,
}

impl MultiRaftNetwork {
    pub fn new(config: MultiRaftNetworkOptions) -> Self {
        let network = Self {
            dispatch: None,
            options: config,
            outgoing_tx: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
        };

        // 启动 gRPC 消息发送任务
        //tokio::spawn(Self::run_message_sender(outgoing_rx, clients.clone()));

        // 启动 gRPC 服务端 (这通常在应用启动时调用一次)
        // tokio::spawn(Self::run_grpc_server(network.grpc_server_addr.clone(), /* 需要传入消息分发通道 */));

        network
    }

    fn get_outgoing_tx(&self, node_id: &NodeId) -> Option<mpsc::UnboundedSender<OutgoingMessage>> {
        self.outgoing_tx.read().unwrap().get(node_id).cloned()
    }

    // 获取或创建到远程节点的 gRPC 客户端
    async fn get_or_create_client(&self, node_id: &str) -> Result<GrpcClient, RpcError> {
        {
            let clients_read = self.clients.read().unwrap();
            if let Some(client) = clients_read.get(node_id) {
                return Ok(client.clone());
            }
        }

        let target_addr = self.resolve_node_address(node_id).await.map_err(|e| {
            RpcError::Network(format!("Failed to resolve address for {}: {}", node_id, e))
        })?;

        let endpoint = Endpoint::from_shared(target_addr)
            .map_err(|e| RpcError::Network(format!("Invalid endpoint for {}: {}", node_id, e)))?
            .connect_timeout(self.options.connect_timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| RpcError::Network(format!("Failed to connect to {}: {}", node_id, e)))?;

        {
            let mut clients_write = self.clients.write().unwrap();
            if let Some(client) = clients_write.get(node_id) {
                Ok(client.clone())
            } else {
                clients_write.insert(node_id.to_string(), channel.clone());
                Ok(channel)
            }
        }
    }

    async fn resolve_node_address(&self, node_id: &str) -> Result<String> {
        self.options.resolve_node_address(node_id).await
    }

    // 运行异步任务，批量发送消息到远程节点
    async fn run_message_sender(
        &self,
        mut rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        rpc_client: GrpcClient,
    ) -> Result<(), RpcError> {
        let batch_size = self.options.batch_size; // 批量大小

        let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(batch_size);
        let mut client = RaftServiceClient::new(rpc_client.clone());

        loop {
            // 收集一批消息
            async {
                let size = rx.recv_many(&mut batch, batch_size).await;
                if size == 0 {
                    warn!("No messages received, exiting sender task");
                    return;
                }

                while batch.len() < batch_size {
                    match rx.try_recv() {
                        Ok(msg) => batch.push(msg),
                        Err(err) => {
                            // check err is closed
                            if err == mpsc::error::TryRecvError::Empty {
                                debug!("Outgoing message channel empty");
                            } else if err == mpsc::error::TryRecvError::Disconnected {
                                warn!("Outgoing message channel disconnected");
                            }
                            break;
                        }
                    }
                }
            }
            .await;

            if batch.is_empty() {
                continue;
            }

            // 按目标节点分组消息
            let mut batch_requests = pb::BatchRequest {
                node_id: self.options.node_id.clone(),
                messages: Vec::with_capacity(batch.len()),
            };
            for msg in batch.drain(..) {
                batch_requests.messages.push(msg.into());
            }

            //

            // Send batch with retry logic
            let mut retries = 0;

            loop {
                match client.send_batch(batch_requests.clone()).await {
                    Ok(response) => {
                        if response.get_ref().success {
                            log::info!("Batch sent successfully");
                            break;
                        } else {
                            log::error!("Failed to send batch: {:?}", response.get_ref().error);
                            if retries >= self.options.send_retry_count {
                                log::error!(
                                    "Max retries reached, giving up. messages {}",
                                    batch_requests.messages.len()
                                );
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        log::error!("Failed to send batch: {}", err);
                        if retries >= self.options.send_retry_count {
                            log::error!("Max retries reached, giving up");
                            break;
                        }
                    }
                }

                retries += 1;
                log::warn!(
                    "Retrying batch send, attempt {}/{}",
                    retries,
                    self.options.send_retry_count
                );
                tokio::time::sleep(self.options.send_retry_delay * retries as u32).await;
            }
        }

        Ok(())
    }

    // 启动 gRPC 服务端 (通常在应用主函数中调用)
    async fn run_grpc_server(&mut self, dispatch: Arc<dyn MessageDispatcher>) -> Result<()> {
        assert!(self.dispatch.is_none(), "gRPC server already running");
        self.dispatch = Some(dispatch);
        let server_addr = self.options.grpc_server_addr.clone();
        Server::builder()
            .add_service(RaftServiceServer::new(self.clone()))
            .serve(server_addr.parse()?)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RaftService for MultiRaftNetwork {
    async fn send_batch(
        &self,
        request: tonic::Request<pb::BatchRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchResponse>, tonic::Status> {
        let response = pb::BatchResponse {
            success: true,
            error: "".into(),
        };

        // Consume the incoming request so we can take ownership of the messages
        // and avoid cloning each message.
        let batch = request.into_inner();
        for message in batch.messages {
            // Convert protobuf message into internal OutgoingMessage first.
            let outgoing: OutgoingMessage = match message.into() {
                Ok(msg) => msg,
                Err(err) => {
                    log::error!("Failed to convert message: {}", err);
                    continue;
                }
            };
            self.dispatch
                .as_ref()
                .unwrap()
                .dispatch(outgoing)
                .await
                .map_err(|e| tonic::Status::internal(format!("Failed to dispatch batch: {}", e)))?;
        }
        Ok(tonic::Response::new(response))
    }
}

#[async_trait]
impl Network for MultiRaftNetwork {
    async fn send_request_vote_request(
        &self,
        from: RaftId, // 本地 RaftId，可能用于日志
        target: RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()> {
        // 注意：args 中应该已经包含了 from 和 target 信息
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::RequestVote {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
        }
    }

    async fn send_request_vote_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::RequestVoteResponse {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
        }
    }

    async fn send_append_entries_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::AppendEntries {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
        }
    }

    async fn send_append_entries_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::AppendEntriesResponse {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
        }
    }

    async fn send_install_snapshot_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::InstallSnapshot {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
        }
    }

    async fn send_install_snapshot_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        if let Some(tx) = self.get_outgoing_tx(&target.node) {
            if tx
                .send(OutgoingMessage::InstallSnapshotResponse {
                    from: from,
                    target: target,
                    args: args,
                })
                .is_err()
            {
                Err(RpcError::Network("Network channel closed".into()))
            } else {
                Ok(())
            }
        } else {
            Err(RpcError::Network(
                "No outgoing channel found for target node".into(),
            ))
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
