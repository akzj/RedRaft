// test_node.rs
use crate::common::test_statemachine::TestStateMachine;
use async_trait::async_trait;
use raft_lite::mock::mock_network::{MockNetworkHub, MockNodeNetwork, NetworkEvent};
use raft_lite::mock::mock_storage::{MockStorage, SnapshotStorage};
use raft_lite::mutl_raft_driver::{HandleEventTrait, MultiRaftDriver, Timers};
use raft_lite::{
    LogEntry, Network, RaftCallbacks, RaftId, RaftState, RaftStateOptions, RequestId, RpcResult,
    Storage, StorageResult, TimerId, TimerService,
};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;

pub struct TestNodeInner {
    pub id: RaftId,
    pub timers: Timers,
    pub state_machine: TestStateMachine,
    pub storage: MockStorage,
    pub network: MockNodeNetwork,
}

#[derive(Clone)]
pub struct TestNode {
    inner: Arc<TestNodeInner>,
    pub raft_state: Arc<Mutex<RaftState>>, // RaftState 需要是 Send + Sync
}

impl Deref for TestNode {
    type Target = TestNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TestNode {
    pub async fn new(
        id: RaftId,
        hub: MockNetworkHub,
        timer_service: Timers,
        snapshot_storage: SnapshotStorage,
        driver: MultiRaftDriver,
        initial_peers: Vec<RaftId>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!("Creating TestNode {:?}", id);

        let storage = MockStorage::new_with_snapshot_storage(snapshot_storage);
        let target_id = id.clone();
        let driver = driver.clone();
        // 初始化并保存集群配置，避免在 RaftState::new 时读取到 None
        {
            let voters: std::collections::HashSet<raft_lite::RaftId> = std::iter::once(id.clone())
                .chain(initial_peers.iter().cloned())
                .collect();
            let cluster_config = raft_lite::ClusterConfig::simple(voters, 0);
            storage
                .save_cluster_config(id.clone(), cluster_config)
                .await
                .expect("save_cluster_config before RaftState::new");
        }
        // 注册网络并获取 dispatch 回调
        let (network) = hub
            .register_node_with_dispatch(
                id.clone(),
                Box::new(move |event| {
                    // 将 NetworkEvent 转换为 Event
                    let event = match event {
                        NetworkEvent::RequestVote(source, _target, req) => {
                            raft_lite::Event::RequestVoteRequest(source, req)
                        }
                        NetworkEvent::RequestVoteResponse(source, _target, resp) => {
                            raft_lite::Event::RequestVoteResponse(source, resp)
                        }
                        NetworkEvent::AppendEntriesRequest(source, _target, req) => {
                            raft_lite::Event::AppendEntriesRequest(source, req)
                        }
                        NetworkEvent::AppendEntriesResponse(source, _target, resp) => {
                            raft_lite::Event::AppendEntriesResponse(source, resp)
                        }
                        NetworkEvent::InstallSnapshotRequest(source, _target, req) => {
                            raft_lite::Event::InstallSnapshotRequest(source, req)
                        }
                        NetworkEvent::InstallSnapshotResponse(source, _target, resp) => {
                            raft_lite::Event::InstallSnapshotResponse(source, resp)
                        }
                    };

                    //info!("Dispatching event from {} to {:?}", target_id, event);
                    let result = driver.send_event(target_id.clone(), event);
                    if !matches!(
                        result,
                        raft_lite::mutl_raft_driver::SendEventResult::Success
                    ) {
                        info!("send event failed {:?}", result);
                    }
                    ()
                }),
            )
            .await;

        // 创建状态机回调
        let state_machine = TestStateMachine::new();

        // 创建 RaftState 选项
        let options = RaftStateOptions {
            id: id.clone(),
            peers: initial_peers,
            ..Default::default()
        };

        let inner = Arc::new(TestNodeInner {
            id,
            timers: timer_service,
            state_machine,
            storage,
            network,
        });

        // 创建 RaftState 实例
        let raft_state = RaftState::new(options, inner.clone())
            .await
            .map_err(|e| format!("Failed to create RaftState: {:?}", e))?;

        // 设置初始选举定时器以开始 Raft 协议
        let election_timeout = std::time::Duration::from_millis(500 + rand::random::<u64>() % 500); // 500-1000ms
        let timer_id = inner.timers.add_timer(
            inner.id.clone(),
            raft_lite::Event::ElectionTimeout,
            election_timeout,
        );
        info!(
            "Started initial election timer for node {:?} with id {}",
            inner.id, timer_id
        );

        Ok(TestNode {
            inner,
            raft_state: Arc::new(Mutex::new(raft_state)),
        })
    }

    // 可以添加方法来查询状态机
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.state_machine.get_value(key)
    }

    // Get all stored data for verification
    pub fn get_all_data(&self) -> std::collections::HashMap<String, String> {
        self.state_machine.get_all_data()
    }

    pub async fn isolate(&self) {
        info!("Isolating node {:?}", self.id);
        self.network.isolate().await;
    }

    //restore
    pub async fn restore(&self) {
        info!("Restoring node {:?}", self.id);
        self.network.restore().await;
    }

    pub async fn handle_event(&self, event: raft_lite::Event) {
        info!(
            "Node {:?} handling event: {:?}",
            self.id,
            match &event {
                raft_lite::Event::ElectionTimeout => "ElectionTimeout".to_string(),
                raft_lite::Event::RequestVoteRequest(_, req) =>
                    format!("RequestVoteRequest(term={})", req.term),
                raft_lite::Event::RequestVoteResponse(_, resp) => {
                    format!(
                        "RequestVoteResponse(from={:?}, vote_granted={}, term={})",
                        resp.request_id, resp.vote_granted, resp.term
                    )
                }
                _ => format!("{:?}", event),
            }
        );
        self.raft_state.lock().await.handle_event(event).await;
    }

    pub fn get_role(&self) -> raft_lite::Role {
        // 使用 try_lock 来避免阻塞，如果锁被占用就返回 Follower
        match self.raft_state.try_lock() {
            Ok(state) => state.get_role(),
            Err(_) => raft_lite::Role::Follower, // 默认返回 Follower
        }
    }
}

#[async_trait::async_trait]
impl HandleEventTrait for TestNode {
    async fn handle_event(&self, event: raft_lite::Event) {
        self.raft_state.lock().await.handle_event(event).await;
    }
}

// Implement Network trait (delegate to MockNodeNetwork)
#[async_trait::async_trait]
impl Network for TestNodeInner {
    async fn send_request_vote_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::RequestVoteRequest,
    ) -> RpcResult<()> {
        info!(
            "Node {:?} sending RequestVote to {:?} for term {}",
            from, target, args.term
        );
        let result = self
            .network
            .send_request_vote_request(from.clone(), target.clone(), args)
            .await;
        if result.is_ok() {
            // info!(
            //     "Successfully sent RequestVote from {:?} to {:?}",
            //     from, target
            // );
        } else {
            info!(
                "Failed to send RequestVote from {:?} to {:?}: {:?}",
                from, target, result
            );
        }
        result
    }

    async fn send_request_vote_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::RequestVoteResponse,
    ) -> RpcResult<()> {
        info!(
            "Node {:?} sending RequestVoteResponse to {:?}: vote_granted={}, term={}",
            from, target, args.vote_granted, args.term
        );
        self.network
            .send_request_vote_response(from, target, args)
            .await
    }

    async fn send_append_entries_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.network
            .send_append_entries_request(from, target, args)
            .await
    }

    async fn send_append_entries_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.network
            .send_append_entries_response(from, target, args)
            .await
    }

    async fn send_install_snapshot_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.network
            .send_install_snapshot_request(from, target, args)
            .await
    }

    async fn send_install_snapshot_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: raft_lite::InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.network
            .send_install_snapshot_response(from, target, args)
            .await
    }
}

// Implement Storage trait (delegate to MockStorage)
#[async_trait::async_trait]
impl Storage for TestNodeInner {
    async fn save_hard_state(
        &self,
        from: RaftId,
        term: u64,
        voted_for: Option<RaftId>,
    ) -> StorageResult<()> {
        self.storage.save_hard_state(from, term, voted_for).await
    }

    async fn load_hard_state(&self, from: RaftId) -> StorageResult<Option<(u64, Option<RaftId>)>> {
        self.storage.load_hard_state(from).await
    }

    async fn append_log_entries(&self, from: RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        self.storage.append_log_entries(from, entries).await
    }

    async fn get_log_entries(
        &self,
        from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        self.storage.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(
        &self,
        from: RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        self.storage.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_suffix(from, idx).await
    }

    async fn truncate_log_prefix(&self, from: RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_prefix(from, idx).await
    }

    async fn get_last_log_index(&self, from: RaftId) -> StorageResult<(u64, u64)> {
        self.storage.get_last_log_index(from).await
    }

    async fn get_log_term(&self, from: RaftId, idx: u64) -> StorageResult<u64> {
        self.storage.get_log_term(from, idx).await
    }

    async fn save_snapshot(&self, from: RaftId, snap: raft_lite::Snapshot) -> StorageResult<()> {
        self.storage.save_snapshot(from, snap).await
    }

    async fn load_snapshot(&self, from: RaftId) -> StorageResult<Option<raft_lite::Snapshot>> {
        self.storage.load_snapshot(from).await
    }

    async fn create_snapshot(&self, from: RaftId) -> StorageResult<(u64, u64)> {
        self.storage.create_snapshot(from).await
    }

    async fn save_cluster_config(
        &self,
        from: RaftId,
        conf: raft_lite::ClusterConfig,
    ) -> StorageResult<()> {
        self.storage.save_cluster_config(from, conf).await
    }

    async fn load_cluster_config(&self, from: RaftId) -> StorageResult<raft_lite::ClusterConfig> {
        self.storage.load_cluster_config(from).await
    }
}

impl TimerService for TestNodeInner {
    fn del_timer(&self, _from: RaftId, timer_id: TimerId) {
        self.timers.del_timer(timer_id);
    }

    fn set_leader_transfer_timer(&self, from: RaftId, _dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft_lite::Event::LeaderTransferTimeout, _dur)
    }

    fn set_election_timer(&self, from: RaftId, _dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft_lite::Event::ElectionTimeout, _dur)
    }

    fn set_heartbeat_timer(&self, from: RaftId, _dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft_lite::Event::HeartbeatTimeout, _dur)
    }

    fn set_apply_timer(&self, from: RaftId, _dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft_lite::Event::ApplyLogTimeout, _dur)
    }

    fn set_config_change_timer(&self, from: RaftId, _dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft_lite::Event::ConfigChangeTimeout, _dur)
    }
}

#[async_trait::async_trait]
impl RaftCallbacks for TestNodeInner {
    async fn client_response(
        &self,
        _from: RaftId,
        _request_id: RequestId,
        _result: raft_lite::ClientResult<u64>,
    ) -> raft_lite::ClientResult<()> {
        Ok(())
    }

    async fn state_changed(
        &self,
        _from: RaftId,
        _role: raft_lite::Role,
    ) -> Result<(), raft_lite::StateChangeError> {
        Ok(())
    }

    async fn apply_command(
        &self,
        from: RaftId,
        index: u64,
        term: u64,
        cmd: raft_lite::Command,
    ) -> raft_lite::ApplyResult<()> {
        self.state_machine
            .apply_command(from, index, term, cmd)
            .await
    }

    async fn process_snapshot(
        &self,
        from: RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        request_id: RequestId,
    ) -> raft_lite::SnapshotResult<()> {
        self.state_machine
            .install_snapshot(from, index, term, data, request_id)
    }
}

// 实现 Drop trait 来清理资源（如果需要）
impl Drop for TestNodeInner {
    fn drop(&mut self) {
        // 可以在这里 abort task 或执行其他清理
        info!("Dropping TestNodeInner {:?}", self.id);
    }
}
