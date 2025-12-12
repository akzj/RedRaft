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
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use tokio::time::{timeout, Duration};
use tonic::transport::{Endpoint, Server};
use tracing::{error, info, warn};

/// Network message channel capacity
const NETWORK_CHANNEL_CAPACITY: usize = 4096;

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

impl Default for MultiRaftNetworkOptions {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            grpc_server_addr: "0.0.0.0:50051".to_string(),
            node_map: HashMap::new(),
            connect_timeout: Duration::from_secs(5),
            batch_size: 100,
        }
    }
}

impl MultiRaftNetworkOptions {
    pub fn with_node_id(mut self, node_id: String) -> Self {
        self.node_id = node_id;
        self
    }

    pub fn with_grpc_addr(mut self, addr: String) -> Self {
        self.grpc_server_addr = addr;
        self
    }
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
    outgoing_tx: Arc<RwLock<HashMap<NodeId, mpsc::Sender<OutgoingMessage>>>>,
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

    fn get_outgoing_tx(&self, node_id: &NodeId) -> Option<mpsc::Sender<OutgoingMessage>> {
        self.outgoing_tx.read().get(node_id).cloned()
    }

    /// Helper method to send an outgoing message to a target node.
    /// Reduces code duplication across all Network trait methods.
    fn send_message(&self, target: &RaftId, msg: OutgoingMessage) -> RpcResult<()> {
        let tx = self
            .get_outgoing_tx(&target.node)
            .ok_or_else(|| RpcError::Network("No outgoing channel found for target node".into()))?;

        tx.try_send(msg).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                RpcError::Network("Network channel full (backpressure)".into())
            }
            mpsc::error::TrySendError::Closed(_) => {
                RpcError::Network("Network channel closed".into())
            }
        })
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

    // Get or create gRPC client to remote node
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

    // Run async task to batch send messages to remote node
    async fn run_message_sender(
        &self,
        mut rx: mpsc::Receiver<OutgoingMessage>,
        rpc_client: GrpcClient,
        shutdown: Arc<Notify>,
    ) {
        let batch_size = self.options.batch_size;
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY: Duration = Duration::from_millis(100);

        let mut client = RaftServiceClient::new(rpc_client.clone());

        loop {
            let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(batch_size);
            // Collect a batch of messages
            tokio::select! {
                // Receive messages (recv_many will receive up to batch_size messages)
                size = rx.recv_many(&mut batch, batch_size) => {
                    if size == 0 {
                        warn!("No messages received, exiting sender task");
                        return;
                    }
                }
                // Check shutdown notification
                _ = shutdown.notified() => {
                    warn!("Shutdown notified, exiting sender task");
                    return;
                }
            }

            if batch.is_empty() {
                // No messages received, exiting sender task
                return;
            }

            // Convert messages to batch request
            let mut batch_requests = pb::BatchRequest {
                node_id: self.options.node_id.clone(),
                messages: Vec::with_capacity(batch.len()),
            };
            for msg in batch {
                batch_requests.messages.push(msg.into());
            }

            // Send batch with retry logic
            let msg_len = batch_requests.messages.len();
            let mut retry_count = 0;
            let mut send_success = false;

            while retry_count < MAX_RETRIES && !send_success {
                match client.send_batch(batch_requests.clone()).await {
                    Ok(response) => {
                        if response.get_ref().success {
                            info!("Batch {} sent successfully", msg_len);
                            send_success = true;
                        } else {
                            error!(
                                "Failed to send batch (attempt {}/{}): {:?}",
                                retry_count + 1,
                                MAX_RETRIES,
                                response.get_ref().error
                            );
                            retry_count += 1;
                            if retry_count < MAX_RETRIES {
                                tokio::time::sleep(RETRY_DELAY).await;
                            }
                        }
                    }
                    Err(err) => {
                        error!(
                            "Failed to send batch (attempt {}/{}): {}",
                            retry_count + 1,
                            MAX_RETRIES,
                            err
                        );
                        retry_count += 1;
                        if retry_count < MAX_RETRIES {
                            tokio::time::sleep(RETRY_DELAY).await;
                        }
                    }
                }
            }

            if !send_success {
                error!(
                    "Failed to send batch after {} retries, messages may be lost",
                    MAX_RETRIES
                );
                // Continue processing next batch instead of exiting
            }
            // Continue loop to process more messages (removed break)
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

                        // Remove senders for nodes that no longer exist
                        let mut nodes_to_remove = Vec::new();
                        for (node_id, notify) in notifies.iter() {
                            if !node_map.contains_key(node_id) {
                                notify.notify_one();
                                info!("Node {} removed, notified sender to stop", node_id);
                                nodes_to_remove.push(node_id.clone());

                                // Remove from outgoing_tx
                                clone_self.outgoing_tx.write().remove(node_id);
                            }
                        }
                        // Remove from notifies map
                        for node_id in nodes_to_remove {
                            notifies.remove(&node_id);
                        }

                        // Create senders for new nodes
                        for node_id in node_map.keys() {
                            if notifies.contains_key(node_id) {
                                continue;
                            }
                            match clone_self.create_client(node_id).await {
                                Ok(client) => {
                                    let (tx, rx) = mpsc::channel(NETWORK_CHANNEL_CAPACITY);
                                    clone_self.outgoing_tx.write().insert(node_id.clone(), tx);

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

    /// Set message dispatcher (required before starting server or getting Raft service)
    pub fn set_dispatcher(&mut self, dispatch: Arc<dyn MessageDispatcher>) {
        self.dispatch = Some(dispatch);
    }

    /// Get Raft service for adding to a gRPC server builder
    /// 
    /// This allows the business layer to create a gRPC server and add both
    /// Raft services and business services to it, sharing the same port.
    /// 
    /// # Example
    /// ```rust,no_run
    /// use tonic::transport::Server;
    /// 
    /// let mut network = MultiRaftNetwork::new(options);
    /// network.set_dispatcher(dispatch);
    /// 
    /// let raft_service = network.get_raft_service();
    /// 
    /// // Business layer creates server and adds services
    /// Server::builder()
    ///     .add_service(raft_service)
    ///     .add_service(business_service)  // Business service
    ///     .serve(addr)
    ///     .await?;
    /// ```
    pub fn get_raft_service(&self) -> RaftServiceServer<MultiRaftNetwork> {
        if self.dispatch.is_none() {
            warn!("Raft service requested but dispatcher not set. Messages may fail to dispatch.");
        }
        RaftServiceServer::new(self.clone())
    }

    // Start gRPC server (usually called in application main function)
    // This method is kept for backward compatibility, but consider using
    // get_raft_service() instead to allow business layer to add additional services.
    pub async fn start_grpc_server(&mut self, dispatch: Arc<dyn MessageDispatcher>) -> Result<()> {
        if self.dispatch.is_some() {
            return Err(anyhow::anyhow!("gRPC server already running"));
        }
        self.dispatch = Some(dispatch);
        let server_addr = self.options.grpc_server_addr.clone();

        let server_addr_parsed = server_addr
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid server address {}: {}", server_addr, e))?;

        let clone_self = self.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(RaftServiceServer::new(clone_self))
                .serve_with_shutdown(server_addr_parsed, shutdown.notified())
                .await
            {
                error!("gRPC server error: {}", e);
                // Server task exits, but we don't panic to avoid crashing the process
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

        let dispatch = self
            .dispatch
            .as_ref()
            .ok_or_else(|| {
                tonic::Status::internal("Dispatcher not initialized - gRPC server not started")
            })?
            .clone();

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
        self.send_message(
            target,
            OutgoingMessage::RequestVote {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::RequestVoteResponse {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::AppendEntries {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::AppendEntriesResponse {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::InstallSnapshot {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::InstallSnapshotResponse {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::PreVote {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> RpcResult<()> {
        self.send_message(
            target,
            OutgoingMessage::PreVoteResponse {
                from: from.clone(),
                target: target.clone(),
                args,
            },
        )
    }
}
