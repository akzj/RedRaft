use crate::network::pb::raft_service_client::RaftServiceClient;
use crate::network::pb::raft_service_server::{RaftService, RaftServiceServer};
// network.rs
use crate::message::{PreVoteRequest, PreVoteResponse};
use crate::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Network, NodeId, RaftId, RequestVoteRequest, RequestVoteResponse, RpcError, RpcResult,
};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, Notify};
use tokio::time::{timeout, Duration};
use tonic::transport::{Endpoint, Server};
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

type GrpcClient = tonic::transport::Channel;

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
    PreVote {
        from: RaftId,
        target: RaftId,
        args: PreVoteRequest,
    },
    PreVoteResponse {
        from: RaftId,
        target: RaftId,
        args: PreVoteResponse,
    },
}

#[derive(Debug, Clone)]
pub struct MultiRaftNetworkOptions {
    node_id: String,
    grpc_server_addr: String,
    node_map: HashMap<NodeId, String>,
    connect_timeout: Duration,
    batch_size: usize,
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
    outgoing_tx: Arc<RwLock<HashMap<NodeId, mpsc::UnboundedSender<OutgoingMessage>>>>,
    node_map: Arc<tokio::sync::RwLock<HashMap<NodeId, String>>>,
    dispatch: Option<Arc<dyn MessageDispatcher>>,
    shutdown: Arc<Notify>,
}

impl MultiRaftNetwork {
    pub fn new(config: MultiRaftNetworkOptions) -> Self {
        let node_map = Arc::new(tokio::sync::RwLock::new(config.node_map.clone()));
        let network = Self {
            dispatch: None,
            node_map,
            options: config,
            outgoing_tx: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(Notify::new()),
        };
        network
    }

    fn get_outgoing_tx(&self, node_id: &NodeId) -> Option<mpsc::UnboundedSender<OutgoingMessage>> {
        self.outgoing_tx.read().unwrap().get(node_id).cloned()
    }

    /// Helper method to send an outgoing message to a target node.
    /// Reduces code duplication across all Network trait methods.
    fn send_message(&self, target: &RaftId, msg: OutgoingMessage) -> RpcResult<()> {
        self.get_outgoing_tx(&target.node)
            .ok_or_else(|| RpcError::Network("No outgoing channel found for target node".into()))?
            .send(msg)
            .map_err(|_| RpcError::Network("Network channel closed".into()))
    }

    pub async fn add_node(&self, node_id: NodeId, address: String) {
        info!("Node {} added with address {}", node_id, address);
        let mut node_map = self.node_map.write().await;
        node_map.insert(node_id.clone(), address);
    }

    pub async fn del_node(&self, node_id: &NodeId) {
        info!("Node {} removed", node_id);
        let mut node_map = self.node_map.write().await;
        node_map.remove(node_id);
    }

    // 获取或创建到远程节点的 gRPC 客户端
    async fn create_client(&self, node_id: &str) -> Result<GrpcClient, RpcError> {
        let target_addr = self.resolve_node_address(node_id).await.map_err(|e| {
            RpcError::Network(format!("Failed to resolve address for {}: {}", node_id, e))
        })?;

        let endpoint = Endpoint::from_shared(target_addr)
            .map_err(|e| RpcError::Network(format!("Invalid endpoint for {}: {}", node_id, e)))?
            .connect_timeout(self.options.connect_timeout);

        endpoint
            .connect()
            .await
            .map_err(|e| RpcError::Network(format!("Failed to connect to {}: {}", node_id, e)))
    }

    async fn resolve_node_address(&self, node_id: &str) -> Result<String> {
        self.options.resolve_node_address(node_id).await
    }

    // 运行异步任务，批量发送消息到远程节点
    async fn run_message_sender(
        &self,
        mut rx: mpsc::UnboundedReceiver<OutgoingMessage>,
        rpc_client: GrpcClient,
        shutdown: Arc<Notify>,
    ) {
        let batch_size = self.options.batch_size; // 批量大小

        let mut client = RaftServiceClient::new(rpc_client.clone());

        loop {
            let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(batch_size / 4);
            // 收集一批消息
            async {
                tokio::select! {
                    // 接收消息
                    size = rx.recv_many(&mut batch, batch_size) => {
                        if size == 0 {
                            warn!("No messages received, exiting sender task");
                            return;
                        }
                    }
                    // 检查关闭通知
                    _ = shutdown.notified() => {
                        warn!("Shutdown notified, exiting sender task");
                        return;
                    }
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
                //  No messages received, exiting sender task
                return;
            }

            // 按目标节点分组消息
            let mut batch_requests = pb::BatchRequest {
                node_id: self.options.node_id.clone(),
                messages: Vec::with_capacity(batch.len()),
            };
            for msg in batch {
                batch_requests.messages.push(msg.into());
            }

            // Send batch with retry logic
            let msg_len = batch_requests.messages.len();

            match client.send_batch(batch_requests).await {
                Ok(response) => {
                    if response.get_ref().success {
                        info!("Batch {} sent successfully", msg_len);
                        break;
                    } else {
                        error!("Failed to send batch: {:?}", response.get_ref().error);
                    }
                }
                Err(err) => {
                    log::error!("Failed to send batch: {}", err);
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.notify_waiters();
        info!("MultiRaftNetwork shutdown complete");
    }

    pub async fn start_sender(&self) {
        let clone_self = self.clone();
        tokio::spawn(async move {
            let mut notifies: HashMap<String, Arc<Notify>> = HashMap::new();
            loop {
                match timeout(Duration::from_secs(1), clone_self.shutdown.notified()).await {
                    Ok(_) => {
                        for (node_id, notify) in notifies {
                            notify.notify_one();
                            info!("Notified sender for {} to stop", node_id);
                        }
                        return;
                    }
                    Err(_) => {
                        let node_map = clone_self.node_map.read().await;
                        for (node_id, notify) in notifies.iter() {
                            if node_map.contains_key(node_id) {
                                continue;
                            }
                            notify.notify_one();
                            info!("no found node_id,notified sender for {} to stop", node_id);
                        }

                        for node_id in node_map.keys() {
                            if notifies.contains_key(node_id) {
                                continue;
                            }
                            match clone_self.create_client(node_id).await {
                                Ok(client) => {
                                    let (tx, rx) = mpsc::unbounded_channel();
                                    clone_self
                                        .outgoing_tx
                                        .write()
                                        .unwrap()
                                        .insert(node_id.clone(), tx);

                                    let notify = Arc::new(Notify::new());

                                    notifies.insert(node_id.clone(), notify.clone());
                                    let clone_self2 = clone_self.clone();
                                    tokio::spawn({
                                        async move {
                                            clone_self2
                                                .run_message_sender(rx, client, notify)
                                                .await;
                                        }
                                    });
                                }
                                Err(err) => {
                                    warn!("Failed to create client for {}: {}", node_id, err);
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // 启动 gRPC 服务端 (通常在应用主函数中调用)
    pub async fn start_grpc_server(&mut self, dispatch: Arc<dyn MessageDispatcher>) -> Result<()> {
        assert!(self.dispatch.is_none(), "gRPC server already running");
        self.dispatch = Some(dispatch);
        let server_addr = self.options.grpc_server_addr.clone();

        let clone_self = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(RaftServiceServer::new(clone_self))
                .serve_with_shutdown(server_addr.parse().unwrap(), shutdown.notified())
                .await
            {
                panic!("Failed to start gRPC server: {}", e);
            }
        });
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

        let dispatch = self.dispatch.as_ref().unwrap().clone();

        // Consume the incoming request so we can take ownership of the messages
        // and avoid cloning each message.
        let batch = request.into_inner();
        for message in batch.messages {
            // Convert protobuf message into internal OutgoingMessage first.
            let outgoing: OutgoingMessage = match message.into() {
                Ok(msg) => msg,
                Err(err) => {
                    error!("Failed to convert message: {}", err);
                    continue;
                }
            };

            dispatch
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
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::RequestVote {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::RequestVoteResponse {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::AppendEntries {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::AppendEntriesResponse {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::InstallSnapshot {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::InstallSnapshotResponse {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::PreVote {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> RpcResult<()> {
        self.send_message(target, OutgoingMessage::PreVoteResponse {
            from: from.clone(),
            target: target.clone(),
            args,
        })
    }
}
