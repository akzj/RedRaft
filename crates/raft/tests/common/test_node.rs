// test_node.rs

use crate::common::test_statemachine::TestStateMachine;
use crate::mock::mock_network::{MockNetworkHub, MockNodeNetwork, NetworkEvent};
use crate::mock::mock_storage::{MockStorage, SnapshotMemStore};
use anyhow::Result;
use raft::cluster_config::ClusterConfig;
use raft::message::{HardState, LogEntry};
use raft::multi_raft_driver::{HandleEventTrait, MultiRaftDriver, Timers};
use raft::traits::*;
use raft::*;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

pub struct TestNodeInner {
    pub id: RaftId,
    pub timers: Timers,
    pub state_machine: TestStateMachine,
    pub storage: MockStorage,
    pub network: MockNodeNetwork,
    pub remove_node: Arc<Notify>,
    pub driver: MultiRaftDriver,
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
        snapshot_storage: SnapshotMemStore,
        driver: MultiRaftDriver,
        initial_peers: Vec<RaftId>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_with_role(
            id,
            hub,
            timer_service,
            snapshot_storage,
            driver,
            initial_peers,
            true,
        )
        .await
    }

    pub async fn new_learner(
        id: RaftId,
        hub: MockNetworkHub,
        timer_service: Timers,
        snapshot_storage: SnapshotMemStore,
        driver: MultiRaftDriver,
        initial_voters: Vec<RaftId>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_with_role(
            id,
            hub,
            timer_service,
            snapshot_storage,
            driver,
            initial_voters,
            false,
        )
        .await
    }

    async fn new_with_role(
        id: RaftId,
        hub: MockNetworkHub,
        timer_service: Timers,
        snapshot_storage: SnapshotMemStore,
        driver: MultiRaftDriver,
        initial_peers_or_voters: Vec<RaftId>,
        is_voter: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!("Creating TestNode {:?}", id);

        let storage = MockStorage::new_with_snapshot_storage(snapshot_storage);
        let target_id = id.clone();
        let driver = driver.clone();
        let driver2 = driver.clone();
        // Initialize and save cluster config to avoid reading None during RaftState::new
        {
            let (voters, learners) = if is_voter {
                // 如果是 voter，将自己和 initial_peers_or_voters 都添加到 voters
                let voters: std::collections::HashSet<raft::RaftId> = std::iter::once(id.clone())
                    .chain(initial_peers_or_voters.iter().cloned())
                    .collect();
                (voters, None)
            } else {
                // 如果是 learner，initial_peers_or_voters 是现有的 voters，自己是 learner
                let voters: std::collections::HashSet<raft::RaftId> =
                    initial_peers_or_voters.iter().cloned().collect();
                let learners: std::collections::HashSet<raft::RaftId> =
                    std::iter::once(id.clone()).collect();
                (voters, Some(learners))
            };
            let cluster_config =
                raft::cluster_config::ClusterConfig::with_learners(voters, learners, 0);
            storage
                .save_cluster_config(&id, cluster_config)
                .await
                .expect("save_cluster_config before RaftState::new");
        }
        // Register network and get dispatch callback
        let network = hub
            .register_node_with_dispatch(
                id.clone(),
                Box::new(move |event| {
                    // 将 NetworkEvent 转换为 Event
                    let event = match event {
                        NetworkEvent::RequestVote(source, _target, req) => {
                            raft::Event::RequestVoteRequest(source, req)
                        }
                        NetworkEvent::RequestVoteResponse(source, _target, resp) => {
                            raft::Event::RequestVoteResponse(source, resp)
                        }
                        NetworkEvent::AppendEntriesRequest(source, _target, req) => {
                            raft::Event::AppendEntriesRequest(source, req)
                        }
                        NetworkEvent::AppendEntriesResponse(source, _target, resp) => {
                            raft::Event::AppendEntriesResponse(source, resp)
                        }
                        NetworkEvent::InstallSnapshotRequest(source, _target, req) => {
                            raft::Event::InstallSnapshotRequest(source, req)
                        }
                        NetworkEvent::InstallSnapshotResponse(source, _target, resp) => {
                            raft::Event::InstallSnapshotResponse(source, resp)
                        }
                        NetworkEvent::PreVote(source, _target, req) => {
                            raft::Event::PreVoteRequest(source, req)
                        }
                        NetworkEvent::PreVoteResponse(source, _target, resp) => {
                            raft::Event::PreVoteResponse(source, resp)
                        }
                    };

                    //info!("Dispatching event from {} to {:?}", target_id, event);
                    let result = driver.dispatch_event(target_id.clone(), event);
                    if !matches!(result, raft::multi_raft_driver::SendEventResult::Success) {
                        info!("send event failed {:?}", result);
                    }
                    ()
                }),
            )
            .await;

        // 创建状态机回调
        let state_machine = TestStateMachine::new(id.clone());

        // 创建 RaftState 选项，使用超快的超时参数
        let options = RaftStateOptions {
            id: id.clone(),
            peers: initial_peers_or_voters,
            election_timeout_min: Duration::from_millis(150), // Faster election timeout
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(25), // More frequent heartbeats
            apply_interval: Duration::from_millis(1),      // Faster apply interval
            config_change_timeout: Duration::from_secs(1),
            leader_transfer_timeout: Duration::from_secs(1),
            apply_batch_size: 50,
            schedule_snapshot_probe_interval: Duration::from_secs(5),
            schedule_snapshot_probe_retries: 3,
            pre_vote_enabled: true,      // Enable Pre-Vote
            leader_lease_enabled: false, // Disable LeaderLease (test default)
            max_inflight_requests: 100,  // 调整InFlight限制
            initial_batch_size: 10,
            max_batch_size: 100,
            min_batch_size: 1,
            feedback_window_size: 10,
            // Ultra-fast smart timeout configuration - most aggressive fast timeouts and retries
            base_request_timeout: Duration::from_millis(25), // 基础超时25ms
            max_request_timeout: Duration::from_millis(5000), // 最大超时500ms
            min_request_timeout: Duration::from_millis(10),  // 最小超时10ms
            timeout_response_factor: 2.0,                    // 响应时间因子2.0倍
            target_response_time: Duration::from_millis(100), // 目标响应时间
        };

        let inner = Arc::new(TestNodeInner {
            id,
            timers: timer_service,
            state_machine,
            storage,
            remove_node: Arc::new(Notify::new()),
            network,
            driver: driver2,
        });

        // 创建 RaftState 实例
        let raft_state = RaftState::new(options, inner.clone())
            .await
            .map_err(|e| format!("Failed to create RaftState: {:?}", e))?;

        // 只为 voter 节点设置初始选举定时器
        if is_voter {
            let election_timeout =
                std::time::Duration::from_millis(500 + rand::random::<u64>() % 500); // 500-1000ms
            let timer_id =
                inner
                    .timers
                    .add_timer(&inner.id, raft::Event::ElectionTimeout, election_timeout);
            info!(
                "Started initial election timer for node {:?} with id {}",
                inner.id, timer_id
            );
        } else {
            info!("Learner node {:?} created without election timer", inner.id);
        }

        Ok(TestNode {
            inner,
            raft_state: Arc::new(Mutex::new(raft_state)),
        })
    }

    // Methods can be added to query the state machine
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

    pub async fn wait_remove_node(&self) {
        self.remove_node.notified().await;
    }

    // Restore network connection
    pub async fn restore(&self) {
        info!("Restoring node {:?}", self.id);
        self.network.restore().await;
    }

    async fn handle_event(&self, event: raft::Event) {
        info!(
            "Node {:?} handling event: {:?}",
            self.id,
            match &event {
                raft::Event::ElectionTimeout => "ElectionTimeout".to_string(),
                raft::Event::RequestVoteRequest(_, req) =>
                    format!("RequestVoteRequest(term={})", req.term),
                raft::Event::RequestVoteResponse(_, resp) => {
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

    pub fn get_role(&self) -> raft::Role {
        // Retry with try_lock to handle temporary contention
        for _ in 0..10 {
            if let Ok(state) = self.raft_state.try_lock() {
                return state.get_role();
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        // If still can't get lock after retries, return Follower as fallback
        raft::Role::Follower
    }

    pub async fn get_inflight_request_count(&self) -> usize {
        let state = self.raft_state.lock().await;
        state.get_inflight_request_count()
    }

    /// 获取当前节点的 term
    pub async fn get_term(&self) -> u64 {
        let state = self.raft_state.lock().await;
        state.get_current_term()
    }

    /// 获取当前节点的 commit_index
    pub async fn get_commit_index(&self) -> u64 {
        let state = self.raft_state.lock().await;
        state.get_commit_index()
    }

    /// 获取当前节点的 last_applied
    pub async fn get_last_applied(&self) -> u64 {
        let state = self.raft_state.lock().await;
        state.get_last_applied()
    }
}

#[async_trait::async_trait]
impl HandleEventTrait for TestNode {
    async fn handle_event(&self, event: raft::Event) {
        self.raft_state.lock().await.handle_event(event).await;
    }
}

// Implement Network trait (delegate to MockNodeNetwork)
#[async_trait::async_trait]
impl Network for TestNodeInner {
    async fn send_request_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::RequestVoteRequest,
    ) -> RpcResult<()> {
        info!(
            "Node {:?} sending RequestVote to {:?} for term {}",
            from, target, args.term
        );
        let result = self
            .network
            .send_request_vote_request(from, target, args)
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
        from: &RaftId,
        target: &RaftId,
        args: raft::message::RequestVoteResponse,
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
        from: &RaftId,
        target: &RaftId,
        args: raft::message::AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.network
            .send_append_entries_request(from, target, args)
            .await
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.network
            .send_append_entries_response(from, target, args)
            .await
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.network
            .send_install_snapshot_request(from, target, args)
            .await
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.network
            .send_install_snapshot_response(from, target, args)
            .await
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::PreVoteRequest,
    ) -> RpcResult<()> {
        info!(
            "Node {:?} sending PreVote to {:?} for prospective term {}",
            from, target, args.term
        );
        self.network.send_pre_vote_request(from, target, args).await
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: raft::message::PreVoteResponse,
    ) -> RpcResult<()> {
        info!(
            "Node {:?} sending PreVoteResponse to {:?}: vote_granted={}, term={}",
            from, target, args.vote_granted, args.term
        );
        self.network
            .send_pre_vote_response(from, target, args)
            .await
    }
}

// Implement Storage trait (delegate to MockStorage)
#[async_trait::async_trait]
impl SnapshotStorage for TestNodeInner {
    async fn save_snapshot(
        &self,
        from: &RaftId,
        snap: raft::message::Snapshot,
    ) -> StorageResult<()> {
        self.storage.save_snapshot(from, snap).await
    }

    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<raft::message::Snapshot>> {
        self.storage.load_snapshot(from).await
    }
}
#[async_trait::async_trait]
impl HardStateStorage for TestNodeInner {
    async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()> {
        self.storage.save_hard_state(from, hard_state).await
    }

    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
        self.storage.load_hard_state(from).await
    }
}
#[async_trait::async_trait]
impl ClusterConfigStorage for TestNodeInner {
    async fn save_cluster_config(
        &self,
        from: &RaftId,
        conf: raft::cluster_config::ClusterConfig,
    ) -> StorageResult<()> {
        self.storage.save_cluster_config(from, conf).await
    }

    async fn load_cluster_config(
        &self,
        from: &RaftId,
    ) -> StorageResult<raft::cluster_config::ClusterConfig> {
        self.storage.load_cluster_config(from).await
    }
}

#[async_trait::async_trait]
impl LogEntryStorage for TestNodeInner {
    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()> {
        self.storage.append_log_entries(from, entries).await
    }

    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        self.storage.get_log_entries(from, low, high).await
    }

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>> {
        self.storage.get_log_entries_term(from, low, high).await
    }

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_suffix(from, idx).await
    }

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
        self.storage.truncate_log_prefix(from, idx).await
    }

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
        self.storage.get_last_log_index(from).await
    }

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
        self.storage.get_log_term(from, idx).await
    }
}

impl TimerService for TestNodeInner {
    fn del_timer(&self, _from: &RaftId, timer_id: TimerId) {
        self.timers.del_timer(timer_id);
    }

    fn set_leader_transfer_timer(&self, from: &RaftId, dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft::Event::LeaderTransferTimeout, dur)
    }

    fn set_election_timer(&self, from: &RaftId, dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft::Event::ElectionTimeout, dur)
    }

    fn set_heartbeat_timer(&self, from: &RaftId, dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft::Event::HeartbeatTimeout, dur)
    }

    fn set_apply_timer(&self, from: &RaftId, dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft::Event::ApplyLogTimeout, dur)
    }

    fn set_config_change_timer(&self, from: &RaftId, dur: Duration) -> TimerId {
        self.timers
            .add_timer(from, raft::Event::ConfigChangeTimeout, dur)
    }
}

#[async_trait::async_trait]
impl EventSender for TestNodeInner {
    async fn send(&self, target: RaftId, event: Event) -> Result<()> {
        match self.driver.dispatch_event(target, event) {
            multi_raft_driver::SendEventResult::Success => Ok(()),
            multi_raft_driver::SendEventResult::NotFound => {
                Err(anyhow::anyhow!("Target not found"))
            }
            multi_raft_driver::SendEventResult::SendFailed
            | multi_raft_driver::SendEventResult::ChannelFull => {
                Err(anyhow::anyhow!("Failed to send event"))
            }
        }
    }
}

#[async_trait::async_trait]
impl EventNotify for TestNodeInner {
    async fn on_state_changed(
        &self,
        _from: &RaftId,
        _role: raft::Role,
    ) -> Result<(), raft::error::StateChangeError> {
        Ok(())
    }

    async fn on_node_removed(&self, node_id: &RaftId) -> Result<(), raft::error::StateChangeError> {
        warn!("Node removed: {}", node_id);
        self.remove_node.notify_one();
        Ok(())
    }
}

#[async_trait::async_trait]
impl RaftCallbacks for TestNodeInner {}

#[async_trait::async_trait]
impl Storage for TestNodeInner {}

#[async_trait::async_trait]
impl StateMachine for TestNodeInner {
    async fn client_response(
        &self,
        _from: &RaftId,
        _request_id: RequestId,
        _result: raft::traits::ClientResult<u64>,
    ) -> raft::traits::ClientResult<()> {
        Ok(())
    }

    async fn read_index_response(
        &self,
        _from: &RaftId,
        _request_id: RequestId,
        _result: raft::traits::ClientResult<u64>,
    ) -> raft::traits::ClientResult<()> {
        // ReadIndex 响应（测试环境下简单处理）
        Ok(())
    }

    async fn apply_command(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        cmd: raft::Command,
    ) -> raft::traits::ApplyResult<()> {
        self.state_machine
            .apply_command(from, index, term, cmd)
            .await
    }

    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        config: ClusterConfig,
        request_id: RequestId,
        oneshot: oneshot::Sender<SnapshotResult<()>>,
    ) {
        let state_machine = self.state_machine.clone();
        let from = from.clone();
        tokio::task::spawn_blocking(move || {
            let result = state_machine.install_snapshot(from, index, term, data, request_id);
            oneshot.send(result).unwrap_or(());
        });
    }

    async fn create_snapshot(
        &self,
        from: &RaftId,
        config: ClusterConfig,
        saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)> {
        // 使用TestStateMachine生成快照数据，它会返回已应用的索引、任期和数据
        let (snapshot_index, snapshot_term, snapshot_data) =
            match self.state_machine.create_snapshot(from.clone()) {
                Ok(data) => data,
                Err(e) => {
                    return Err(raft::error::StorageError::SnapshotCreationFailed(format!(
                        "State machine snapshot creation failed: {:?}",
                        e
                    )));
                }
            };

        // 创建快照对象
        let snapshot = raft::message::Snapshot {
            index: snapshot_index,
            term: snapshot_term,
            config,
            data: snapshot_data,
        };

        // 保存快照到存储
        saver.save_snapshot(from, snapshot).await.unwrap();

        Ok((snapshot_index, snapshot_term))
    }
}

// 实现 Drop trait 来清理资源（如果需要）
impl Drop for TestNodeInner {
    fn drop(&mut self) {
        // 可以在这里 abort task 或执行其他清理
        info!("Dropping TestNodeInner {:?}", self.id);
    }
}
