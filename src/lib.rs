use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

// 类型定义
pub type NodeId = String;

// === 事件定义（输入）===
#[derive(Debug)]
pub enum Event {
    // 定时器事件
    ElectionTimeout,  // 选举超时（Follower/Candidate 触发）
    HeartbeatTimeout, // 心跳超时（Leader 触发日志同步）

    // RPC 请求事件（来自其他节点）
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest),
    InstallSnapshot(InstallSnapshotRequest),

    // RPC 响应事件（其他节点对本节点请求的回复）
    RequestVoteReply(NodeId, RequestVoteResponse),
    AppendEntriesReply(NodeId, AppendEntriesResponse),
    InstallSnapshotReply(NodeId, InstallSnapshotReply),

    // 客户端事件
    ClientPropose(Vec<u8>), // 客户端提交命令
}

// === 回调接口定义（替代 Action，外部实现）===
pub trait RaftCallbacks: Send + Sync {
    // 发送 RPC 回调
    fn send_request_vote_request(
        &self,
        target: NodeId,
        args: RequestVoteRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn send_request_vote_response(
        &self,
        target: NodeId,
        args: RequestVoteResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn send_append_entries_request(
        &self,
        target: NodeId,
        args: AppendEntriesRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn send_append_entries_response(
        &self,
        target: NodeId,
        args: AppendEntriesResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    fn send_install_snapshot_request(
        &self,
        target: NodeId,
        args: InstallSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 持久化回调
    fn save_hard_state(
        &self,
        term: u64,
        voted_for: Option<NodeId>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn save_log_entries(&self, entries: Vec<LogEntry>) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn save_snapshot(&self, snap: Snapshot) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn save_cluster_config(&self, conf: ClusterConfig) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 定时器回调
    fn set_election_timer(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn set_heartbeat_timer(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 客户端响应回调
    fn client_response(
        &self,
        result: Result<u64, Error>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 状态变更通知回调
    fn state_changed(&self, role: Role) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

// === 集群配置 ===
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub voters: HashSet<NodeId>,
    pub joint: Option<JointConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JointConfig {
    pub old: HashSet<NodeId>,
    pub new: HashSet<NodeId>,
}

impl ClusterConfig {
    pub fn empty() -> Self {
        Self {
            voters: HashSet::new(),
            joint: None,
        }
    }
    pub fn simple(voters: HashSet<NodeId>) -> Self {
        Self {
            voters,
            joint: None,
        }
    }
    pub fn enter_joint(&mut self, old: HashSet<NodeId>, new: HashSet<NodeId>) {
        debug_assert!(self.joint.is_none(), "already in joint");
        self.joint = Some(JointConfig {
            old: old.clone(),
            new: new.clone(),
        });
        self.voters = old.union(&new).cloned().collect();
    }
    pub fn leave_joint(&mut self) -> Self {
        if let Some(j) = self.joint.take() {
            self.voters = j.new.clone();
            Self {
                voters: j.new,
                joint: None,
            }
        } else {
            self.clone()
        }
    }
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
    pub fn joint_quorum(&self) -> Option<(usize, usize)> {
        self.joint
            .as_ref()
            .map(|j| (j.old.len() / 2 + 1, j.new.len() / 2 + 1))
    }
    pub fn contains(&self, id: &NodeId) -> bool {
        self.voters.contains(id)
    }
    pub fn joint_majority(&self, votes: &HashSet<NodeId>) -> bool {
        if let Some(j) = &self.joint {
            votes.intersection(&j.old).count() >= j.old.len() / 2 + 1
                && votes.intersection(&j.new).count() >= j.new.len() / 2 + 1
        } else {
            true
        }
    }
}

// === 网络与存储接口 ===
#[derive(Debug, Clone)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub success: bool,
}

pub trait Network: Send + Sync {
    fn send_request_vote(
        &self,
        target: NodeId,
        args: RequestVoteRequest,
    ) -> Pin<Box<dyn Future<Output = RequestVoteResponse> + Send>>;

    fn send_append_entries(
        &self,
        target: NodeId,
        args: AppendEntriesRequest,
    ) -> Pin<Box<dyn Future<Output = AppendEntriesResponse> + Send>>;

    fn send_install_snapshot(
        &self,
        target: NodeId,
        args: InstallSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = InstallSnapshotReply> + Send>>;
}

#[derive(Debug)]
pub struct Error(pub String);

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}

pub trait Storage: Send + Sync {
    fn save_hard_state(&self, term: u64, voted_for: Option<NodeId>);
    fn load_hard_state(&self) -> (u64, Option<NodeId>);

    fn append(&self, entries: &[LogEntry]) -> Result<(), Error>;
    fn entries(&self, low: u64, high: u64) -> Result<Vec<LogEntry>, Error>;
    fn truncate_suffix(&self, idx: u64) -> Result<(), Error>;
    fn truncate_prefix(&self, idx: u64) -> Result<(), Error>;
    fn last_index(&self) -> Result<u64, Error>;
    fn term(&self, idx: u64) -> Result<u64, Error>;

    fn save_snapshot(&self, snap: &Snapshot) -> Result<(), Error>;
    fn load_snapshot(&self) -> Result<Snapshot, Error>;

    fn save_cluster_config(&self, conf: &ClusterConfig) -> Result<(), Error>;
    fn load_cluster_config(&self) -> Result<ClusterConfig, Error>;
}

// === 核心状态与逻辑 ===
#[derive(Debug, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
}

// === 状态机（可变状态，无 Clone）===
pub struct RaftState {
    // 节点标识与配置
    id: NodeId,
    peers: Vec<NodeId>,
    config: ClusterConfig,

    // 核心状态
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,

    // 日志与提交状态
    commit_index: u64,
    last_applied: u64,

    // Leader 专用状态
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // 定时器配置
    election_timeout: Duration,
    election_timeout_min: u64,
    election_timeout_max: u64,
    last_heartbeat: Instant,

    // 外部依赖
    storage: Arc<dyn Storage>,
    network: Arc<dyn Network + Send + Sync>,
    callbacks: Arc<dyn RaftCallbacks>, // 回调实例
}

impl RaftState {
    /// 初始化状态
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: Arc<dyn Storage>,
        network: Arc<dyn Network + Send + Sync>,
        election_timeout_min: u64,
        election_timeout_max: u64,
        callbacks: Arc<dyn RaftCallbacks>,
    ) -> Self {
        let (current_term, voted_for) = storage.load_hard_state();
        let loaded_config = match storage.load_cluster_config() {
            Ok(conf) => conf,
            Err(_) => ClusterConfig::empty(),
        };
        let timeout = election_timeout_min
            + rand::random::<u64>() % (election_timeout_max - election_timeout_min + 1);

        RaftState {
            id,
            peers,
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_timeout: Duration::from_millis(timeout),
            election_timeout_min,
            election_timeout_max,
            last_heartbeat: Instant::now(),
            storage,
            network,
            callbacks,
        }
    }

    /// 处理事件（直接修改自身状态，通过回调执行动作）
    pub async fn handle_event(&mut self, event: Event) {
        match event {
            Event::ElectionTimeout => self.handle_election_timeout().await,
            Event::RequestVote(args) => self.handle_request_vote(args).await,
            Event::AppendEntries(args) => self.handle_append_entries(args).await,
            Event::RequestVoteReply(peer, reply) => {
                self.handle_request_vote_reply(peer, reply).await
            }
            Event::AppendEntriesReply(peer, reply) => {
                self.handle_append_entries_reply(peer, reply).await
            }
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().await,
            Event::ClientPropose(cmd) => self.handle_client_propose(cmd).await,
            _ => {}
        }
    }

    /// 处理选举超时事件
    async fn handle_election_timeout(&mut self) {
        // 递增任期，切换为 Candidate
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());

        // 调用回调：持久化 hard state
        self.callbacks
            .save_hard_state(self.current_term, self.voted_for.clone())
            .await;

        // 重置选举定时器（通过回调）
        let new_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(new_timeout);
        self.callbacks
            .set_election_timer(self.election_timeout)
            .await;

        // 向所有有效节点发送投票请求（通过回调）
        let effective_voters: HashSet<NodeId> = match self.config.joint.as_ref() {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.config.voters.clone(),
        };
        let last_log_index = self.storage.last_index().unwrap_or(0);
        let last_log_term = self.storage.term(last_log_index).unwrap_or(0);
        let vote_args = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
        };
        for peer in &effective_voters {
            if *peer != self.id {
                let target = peer.clone();
                let args = vote_args.clone();
                self.callbacks.send_request_vote_request(target, args).await;
            }
        }

        // 状态变更通知
        self.callbacks.state_changed(Role::Candidate).await;
    }

    /// 处理心跳超时事件
    async fn handle_heartbeat_timeout(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        let current_term = self.current_term;
        let leader_id = self.id.clone();
        let leader_commit = self.commit_index;
        let last_log_index = self.storage.last_index().unwrap_or(0);
        let max_batch_size = 100;

        // 向所有有效节点发送 AppendEntries（通过回调）
        let effective_peers: Vec<NodeId> = match self.config.joint.as_ref() {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.peers.clone(),
        };
        for peer in effective_peers {
            if peer == self.id {
                continue;
            }

            // 获取该节点的 next_index
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1).max(&1);
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = self.storage.term(prev_log_index).unwrap_or(0);

            // 获取待发送日志
            let entries = match self.storage.entries(next_idx, last_log_index + 1) {
                Ok(entries) => entries.into_iter().take(max_batch_size).collect(),
                Err(_) => vec![],
            };

            // 调用回调发送 AppendEntries
            let args = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            };
            self.callbacks.send_append_entries_request(peer, args).await;
        }

        // 重置心跳定时器（通过回调）
        self.callbacks
            .set_heartbeat_timer(Duration::from_millis(100))
            .await;
    }

    /// 处理 RequestVote 请求
    async fn handle_request_vote(&mut self, args: RequestVoteRequest) {
        let mut vote_granted = false;

        // 1. 若对方 term 更高，更新自身状态
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
            // 调用回调持久化
            self.callbacks
                .save_hard_state(self.current_term, self.voted_for.clone())
                .await;
            self.callbacks.state_changed(Role::Follower).await;
        }

        // 2. 日志最新性检查
        let last_log_idx = self.storage.last_index().unwrap_or(0);
        let last_log_term = self.storage.term(last_log_idx).unwrap_or(0);
        let log_ok = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_idx);

        // 3. 满足条件则投票
        if args.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id.clone()))
            && log_ok
        {
            self.voted_for = Some(args.candidate_id.clone());
            vote_granted = true;
            self.callbacks
                .save_hard_state(self.current_term, self.voted_for.clone())
                .await;
        }

        // 4. 调用回调发送投票结果
        let resp = RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        };
        self.callbacks
            .send_request_vote_response(args.candidate_id, resp)
            .await;
    }

    /// 处理 AppendEntries 请求
    async fn handle_append_entries(&mut self, args: AppendEntriesRequest) {
        // 1. 若对方 term 更小，拒绝
        if args.term < self.current_term {
            let response = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(self.storage.last_index().unwrap_or(0) + 1),
            };
            self.callbacks
                .send_append_entries_response(args.leader_id, response)
                .await;
            return;
        }

        // 2. 更新自身状态为 Follower，重置心跳时间
        self.current_term = args.term;
        self.role = Role::Follower;
        self.last_heartbeat = Instant::now();
        self.callbacks.state_changed(Role::Follower).await;
        self.callbacks
            .set_election_timer(self.election_timeout)
            .await;

        // 3. 日志连续性检查
        let last_idx = self.storage.last_index().unwrap_or(0);
        if args.prev_log_index > last_idx {
            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(last_idx + 1),
            };
            self.callbacks
                .send_append_entries_response(args.leader_id, resp)
                .await;
            return;
        }

        // 4. 前序日志 term 匹配检查
        let prev_term = self.storage.term(args.prev_log_index).unwrap_or(0);
        if prev_term != args.prev_log_term {
            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(args.prev_log_index),
            };
            self.callbacks
                .send_append_entries_response(args.leader_id, resp)
                .await;
            return;
        }

        // 5. 截断冲突日志并追加新日志
        let _ = self.storage.truncate_suffix(args.prev_log_index + 1);
        if !args.entries.is_empty() {
            let _ = self.storage.append(&args.entries);
            self.callbacks.save_log_entries(args.entries.clone()).await;
        }

        // 6. 更新 commit_index
        let new_last_idx = self.storage.last_index().unwrap_or(0);
        self.commit_index = std::cmp::min(args.leader_commit, new_last_idx);

        // 7. 发送成功响应
        let resp = AppendEntriesResponse {
            term: self.current_term,
            success: true,
            conflict_index: None,
        };
        self.callbacks
            .send_append_entries_response(args.leader_id, resp)
            .await;
    }

    /// 处理 RequestVote 响应
    async fn handle_request_vote_reply(&mut self, peer: NodeId, reply: RequestVoteResponse) {
        if self.role != Role::Candidate {
            return;
        }

        // 若对方 term 更高，降级为 Follower
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.callbacks
                .save_hard_state(self.current_term, self.voted_for.clone())
                .await;
            self.callbacks.state_changed(Role::Follower).await;
            return;
        }

        // 统计选票（实际实现需维护投票计数）
        // ...

        // 检查是否赢得选举
        if self.check_election_win() {
            self.role = Role::Leader;
            // 初始化 next_index 和 match_index
            let last_idx = self.storage.last_index().unwrap_or(0);
            for peer in &self.peers {
                self.next_index.insert(peer.clone(), last_idx + 1);
                self.match_index.insert(peer.clone(), 0);
            }
            self.callbacks.state_changed(Role::Leader).await;
            self.callbacks
                .set_heartbeat_timer(Duration::from_millis(100))
                .await;
        }
    }

    /// 处理 AppendEntries 响应
    async fn handle_append_entries_reply(&mut self, peer: NodeId, reply: AppendEntriesResponse) {
        if self.role != Role::Leader {
            return;
        }

        // 若对方 term 更高，降级为 Follower
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.callbacks
                .save_hard_state(self.current_term, self.voted_for.clone())
                .await;
            self.callbacks.state_changed(Role::Follower).await;
            return;
        }

        if reply.success {
            // 同步成功：更新 match_index 和 next_index
            let prev_log_index = self
                .next_index
                .get(&peer)
                .cloned()
                .unwrap_or(1)
                .saturating_sub(1);
            let entries_len = self
                .storage
                .entries(
                    prev_log_index + 1,
                    self.storage.last_index().unwrap_or(0) + 1,
                )
                .unwrap_or_default()
                .len() as u64;
            let new_match_idx = prev_log_index + entries_len;
            let new_next_idx = new_match_idx + 1;

            self.match_index.insert(peer.clone(), new_match_idx);
            self.next_index.insert(peer.clone(), new_next_idx);
        } else {
            // 同步失败：回退 next_index
            let current_next = self.next_index.get(&peer).cloned().unwrap_or(1);
            let new_next_idx = reply
                .conflict_index
                .map(|conflict_idx| {
                    if conflict_idx < current_next {
                        conflict_idx
                    } else {
                        current_next.saturating_sub(1)
                    }
                })
                .unwrap_or_else(|| current_next.saturating_sub(1));
            self.next_index.insert(peer.clone(), new_next_idx);
        }
    }

    /// 处理客户端提交命令
    async fn handle_client_propose(&mut self, cmd: Vec<u8>) {
        if self.role != Role::Leader {
            // 非 Leader，通过回调响应客户端错误
            self.callbacks
                .client_response(Err(Error("not leader".into())))
                .await;
            return;
        }

        // 生成新日志条目
        let last_idx = self.storage.last_index().unwrap_or(0);
        let new_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: cmd,
        };

        // 追加到日志并调用回调持久化
        let _ = self.storage.append(&[new_entry.clone()]);
        self.callbacks.save_log_entries(vec![new_entry]).await;

        // 触发日志同步
        self.trigger_append_entries().await;
    }

    // === 辅助方法 ===
    /// 检查是否赢得选举
    fn check_election_win(&self) -> bool {
        // 简化实现：实际需统计有效选票是否满足 quorum
        false
    }

    /// 触发一次日志同步
    async fn trigger_append_entries(&mut self) {
        self.handle_heartbeat_timeout().await;
    }
}

// === 外部驱动层（实现回调接口）===
pub struct RaftDriver {
    state: RaftState,
    // 其他外部依赖（如定时器管理器、网络适配器等）
    // ...
}

impl RaftDriver {
    pub async fn run(&mut self) {
        loop {
            // 1. 等待外部事件（定时器、网络消息、客户端请求等）
            let event = self.receive_event().await;

            // 2. 调用状态机处理事件（状态机内部通过回调执行动作）
            self.state.handle_event(event).await;
        }
    }

    async fn receive_event(&self) -> Event {
        // 实际实现：从网络、定时器、客户端接收事件
        // 示例：等待选举超时
        Event::ElectionTimeout
    }
}

// === 回调接口的默认实现（示例）===
#[derive(Clone)]
pub struct DefaultCallbacks {
    network: Arc<dyn Network + Send + Sync>,
    storage: Arc<dyn Storage>,
    // 其他依赖（如定时器管理器、客户端响应通道等）
    // ...
}

impl DefaultCallbacks {
    pub fn new(network: Arc<dyn Network + Send + Sync>, storage: Arc<dyn Storage>) -> Self {
        DefaultCallbacks { network, storage }
    }
}

impl RaftCallbacks for DefaultCallbacks {
    fn send_request_vote_request(
        &self,
        target: NodeId,
        args: RequestVoteRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            // 发送RPC后，将响应转为Event并注入状态机（实际需通过驱动层）
            let _ = network.send_request_vote(target, args).await;
        })
    }

    fn send_append_entries_request(
        &self,
        target: NodeId,
        args: AppendEntriesRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let _ = network.send_append_entries(target, args).await;
        })
    }

    fn send_install_snapshot_request(
        &self,
        target: NodeId,
        args: InstallSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let _ = network.send_install_snapshot(target, args).await;
        })
    }

    fn save_hard_state(
        &self,
        term: u64,
        voted_for: Option<NodeId>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let storage = self.storage.clone();
        Box::pin(async move {
            storage.save_hard_state(term, voted_for);
        })
    }

    fn save_log_entries(&self, entries: Vec<LogEntry>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let storage = self.storage.clone();
        Box::pin(async move {
            let _ = storage.append(&entries);
        })
    }

    // 其他回调方法的实现...
    fn save_snapshot(&self, _snap: Snapshot) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn save_cluster_config(
        &self,
        _conf: ClusterConfig,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_election_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_heartbeat_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn client_response(
        &self,
        _result: Result<u64, Error>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn state_changed(&self, _role: Role) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn send_request_vote_response(
        &self,
        target: NodeId,
        args: RequestVoteResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn send_append_entries_response(
        &self,
        target: NodeId,
        args: AppendEntriesResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }
}
