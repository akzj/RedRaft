use crate::mutl_raft_driver::Network;

use super::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, RaftId, RequestVoteRequest, RequestVoteResponse,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

/// 模拟网络节点，用于测试
#[derive(Clone)]
pub struct MockNetworkNode {
    id: RaftId,
    network: Arc<MockNetwork>,
}

impl MockNetworkNode {
    pub fn new(id: RaftId, network: Arc<MockNetwork>) -> Self {
        Self { id, network }
    }

    /// 注册请求处理器
    pub fn register_request_vote_handler<F>(&self, handler: F)
    where
        F: Fn(RequestVoteRequest) -> Pin<Box<dyn Future<Output = RequestVoteResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.network
            .register_request_vote_handler(self.id.clone(), Arc::new(handler));
    }

    pub fn register_append_entries_handler<F>(&self, handler: F)
    where
        F: Fn(AppendEntriesRequest) -> Pin<Box<dyn Future<Output = AppendEntriesResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.network
            .register_append_entries_handler(self.id.clone(), Arc::new(handler));
    }

    pub fn register_install_snapshot_handler<F>(&self, handler: F)
    where
        F: Fn(
                InstallSnapshotRequest,
            ) -> Pin<Box<dyn Future<Output = InstallSnapshotResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.network
            .register_install_snapshot_handler(self.id.clone(), Arc::new(handler));
    }
}

/// 模拟网络实现
pub struct MockNetwork {
    nodes: Arc<RwLock<HashMap<RaftId, MockNetworkNodeInner>>>,
    latency: RwLock<Duration>, // 使用RwLock实现内部可变性
    drop_rate: RwLock<f64>,    // 使用RwLock实现内部可变性
}

struct MockNetworkNodeInner {
    request_vote_handler: Option<
        Arc<
            dyn Fn(RequestVoteRequest) -> Pin<Box<dyn Future<Output = RequestVoteResponse> + Send>>
                + Send
                + Sync,
        >,
    >,
    append_entries_handler: Option<
        Arc<
            dyn Fn(
                    AppendEntriesRequest,
                ) -> Pin<Box<dyn Future<Output = AppendEntriesResponse> + Send>>
                + Send
                + Sync,
        >,
    >,
    install_snapshot_handler: Option<
        Arc<
            dyn Fn(
                    InstallSnapshotRequest,
                ) -> Pin<Box<dyn Future<Output = InstallSnapshotResponse> + Send>>
                + Send
                + Sync,
        >,
    >,
}

impl MockNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            latency: RwLock::new(Duration::from_millis(10)),
            drop_rate: RwLock::new(0.0),
        })
    }

    /// 创建新的网络节点
    pub fn create_node(&self, id: RaftId) -> MockNetworkNode {
        let mut nodes = self.nodes.write().unwrap();
        nodes.entry(id.clone()).or_insert(MockNetworkNodeInner {
            request_vote_handler: None,
            append_entries_handler: None,
            install_snapshot_handler: None,
        });
        MockNetworkNode::new(id, Arc::new(self.clone()))
    }

    /// 注册请求处理器
    pub fn register_request_vote_handler<F>(&self, node_id: RaftId, handler: Arc<F>)
    where
        F: Fn(RequestVoteRequest) -> Pin<Box<dyn Future<Output = RequestVoteResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        if let Some(node) = self.nodes.write().unwrap().get_mut(&node_id) {
            node.request_vote_handler = Some(handler);
        }
    }

    pub fn register_append_entries_handler<F>(&self, node_id: RaftId, handler: Arc<F>)
    where
        F: Fn(AppendEntriesRequest) -> Pin<Box<dyn Future<Output = AppendEntriesResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        if let Some(node) = self.nodes.write().unwrap().get_mut(&node_id) {
            node.append_entries_handler = Some(handler);
        }
    }

    pub fn register_install_snapshot_handler<F>(&self, node_id: RaftId, handler: Arc<F>)
    where
        F: Fn(
                InstallSnapshotRequest,
            ) -> Pin<Box<dyn Future<Output = InstallSnapshotResponse> + Send>>
            + Send
            + Sync
            + 'static,
    {
        if let Some(node) = self.nodes.write().unwrap().get_mut(&node_id) {
            node.install_snapshot_handler = Some(handler);
        }
    }

    /// 设置网络延迟
    pub fn set_latency(&self, latency: Duration) {
        *self.latency.write().unwrap() = latency;
    }

    /// 设置丢包率
    pub fn set_drop_rate(&self, drop_rate: f64) {
        if drop_rate < 0.0 || drop_rate > 1.0 {
            panic!("丢包率必须在0-1之间");
        }
        *self.drop_rate.write().unwrap() = drop_rate;
    }

    /// 检查节点是否存在
    fn has_node(&self, node_id: &RaftId) -> bool {
        self.nodes.read().unwrap().contains_key(node_id)
    }
}

// 实现Clone以支持Arc内部的复制
impl Clone for MockNetwork {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            latency: RwLock::new(*self.latency.read().unwrap()),
            drop_rate: RwLock::new(*self.drop_rate.read().unwrap()),
        }
    }
}

#[async_trait]
impl Network for MockNetwork {
    async fn send_request_vote(
        &self,
        target: RaftId,
        args: RequestVoteRequest,
    ) -> RequestVoteResponse {
        // 查找目标节点和处理器
        let handler = {
            let nodes = self.nodes.read().unwrap();
            nodes
                .get(&target)
                .and_then(|node| node.request_vote_handler.as_ref())
                .cloned()
        };

        // 模拟网络延迟
        let latency = *self.latency.read().unwrap();
        tokio::time::sleep(latency).await;

        // 模拟丢包
        let drop_rate = *self.drop_rate.read().unwrap();
        if rand::random::<f64>() < drop_rate {
            return RequestVoteResponse {
                term: 0,
                vote_granted: false,
                request_id: args.request_id,
            };
        }

        if let Some(handler) = handler {
            return handler(args).await;
        }

        // 目标节点不存在或无处理器
        RequestVoteResponse {
            term: 0,
            vote_granted: false,
            request_id: args.request_id,
        }
    }

    async fn send_append_entries(
        &self,
        target: RaftId,
        args: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        // 查找目标节点和处理器
        let handler = {
            let nodes = self.nodes.read().unwrap();
            nodes
                .get(&target)
                .and_then(|node| node.append_entries_handler.as_ref())
                .cloned()
        };

        // 模拟网络延迟
        let latency = *self.latency.read().unwrap();
        tokio::time::sleep(latency).await;

        // 模拟丢包
        let drop_rate = *self.drop_rate.read().unwrap();
        if rand::random::<f64>() < drop_rate {
            return AppendEntriesResponse {
                term: 0,
                success: false,
                conflict_index: None,
                request_id: args.request_id,
                conflict_term: None,
            };
        }

        if let Some(handler) = handler {
            return handler(args).await;
        }

        // 目标节点不存在或无处理器
        AppendEntriesResponse {
            term: 0,
            success: false,
            conflict_index: None,
            request_id: args.request_id,
            conflict_term: None,
        }
    }

    async fn send_install_snapshot(
        &self,
        target: RaftId,
        args: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let handler = {
            let nodes = self.nodes.read().unwrap();
            nodes
                .get(&target)
                .and_then(|node| node.install_snapshot_handler.as_ref())
                .cloned()
        };

        // 模拟网络延迟
        let latency = *self.latency.read().unwrap();
        tokio::time::sleep(latency).await;

        // 模拟丢包
        let drop_rate = *self.drop_rate.read().unwrap();
        if rand::random::<f64>() < drop_rate {
            return InstallSnapshotResponse {
                term: 0,
                request_id: args.request_id,
                state: crate::InstallSnapshotState::Success,
            };
        }

        if let Some(handler) = handler {
            return handler(args).await;
        }

        // 目标节点不存在或无处理器
        InstallSnapshotResponse {
            term: 0,
            request_id: args.request_id,
            state: crate::InstallSnapshotState::Success,
        }
    }
}
