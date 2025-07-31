use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use std::fmt;

// 类型定义
pub type NodeId = String;
pub type Command = Vec<u8>;

// 请求ID类型（用于过滤超时响应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self(rng.gen::<u64>())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// === 事件定义（输入）===
#[derive(Debug)]
pub enum Event {
    // 定时器事件
    ElectionTimeout,  // 选举超时（Follower/Candidate 触发）
    HeartbeatTimeout, // 心跳超时（Leader 触发日志同步）
    ApplyLogs,        // 定期将已提交日志应用到状态机

    // RPC 请求事件（来自其他节点）
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest),
    InstallSnapshot(InstallSnapshotRequest),

    // RPC 响应事件（其他节点对本节点请求的回复）
    RequestVoteReply(NodeId, RequestVoteResponse),
    AppendEntriesReply(NodeId, AppendEntriesResponse),
    InstallSnapshotReply(NodeId, InstallSnapshotReply),

    // 客户端事件
    ClientPropose {
        cmd: Command,
        request_id: RequestId, // 客户端请求ID，用于关联响应
    },
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

    fn send_install_snapshot_reply(
        &self,
        target: NodeId,
        args: InstallSnapshotReply,
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
    fn set_apply_timer(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>; // 日志应用定时器

    // 客户端响应回调（带请求ID）
    fn client_response(
        &self,
        request_id: RequestId,
        result: Result<u64, Error>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 状态变更通知回调
    fn state_changed(&self, role: Role) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // 日志应用到状态机的回调（由外部实现状态机逻辑）
    fn apply_command(&self, index: u64, term: u64, cmd: Command) -> Pin<Box<dyn Future<Output = ()> + Send>>;
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub success: bool,
    pub request_id: RequestId,
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

#[derive(Debug, Clone)]
pub struct Error(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    fn truncate_prefix(&self, idx: u64) -> Result<(), Error>; // 保留>=idx的日志
    fn last_index(&self) -> Result<u64, Error>;
    fn term(&self, idx: u64) -> Result<u64, Error>;

    fn save_snapshot(&self, snap: &Snapshot) -> Result<(), Error>;
    fn load_snapshot(&self) -> Result<Snapshot, Error>;

    fn save_cluster_config(&self, conf: &ClusterConfig) -> Result<(), Error>;
    fn load_cluster_config(&self) -> Result<ClusterConfig, Error>;
}

// === 核心状态与逻辑 ===
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub is_config: bool, // 标记是否为配置变更日志
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
    pub request_id: RequestId,
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
    last_snapshot_index: u64, // 最后一个快照的索引
    last_snapshot_term: u64,  // 最后一个快照的任期

    // Leader 专用状态
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    client_requests: HashMap<RequestId, u64>, // 客户端请求ID -> 日志索引

    // 定时器配置
    election_timeout: Duration,
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: Duration,
    apply_interval: Duration, // 日志应用到状态机的间隔
    last_heartbeat: Instant,

    // 外部依赖
    storage: Arc<dyn Storage>,
    network: Arc<dyn Network + Send + Sync>,
    callbacks: Arc<dyn RaftCallbacks>,

    // 选举跟踪（仅 Candidate 状态有效）
    election_votes: HashMap<NodeId, bool>,
    election_effective_voters: HashSet<NodeId>,
    election_joint_config: Option<JointConfig>,
    election_max_term: u64,
    current_election_id: Option<RequestId>,

    // 快照请求跟踪（仅 Follower 有效）
    current_snapshot_id: Option<RequestId>,
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
        heartbeat_interval: u64,
        apply_interval: u64,
        callbacks: Arc<dyn RaftCallbacks>,
    ) -> Self {
        let (current_term, voted_for) = storage.load_hard_state();
        let loaded_config = match storage.load_cluster_config() {
            Ok(conf) => conf,
            Err(_) => ClusterConfig::empty(),
        };
        let snap = storage.load_snapshot().unwrap_or(Snapshot {
            index: 0,
            term: 0,
            data: vec![],
            config: loaded_config.clone(),
        });
        let timeout = election_timeout_min
            + rand::random::<u64>() % (election_timeout_max - election_timeout_min + 1);

        RaftState {
            id,
            peers,
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            commit_index: snap.index, // 提交索引从快照开始
            last_applied: snap.index,
            last_snapshot_index: snap.index,
            last_snapshot_term: snap.term,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            client_requests: HashMap::new(),
            election_timeout: Duration::from_millis(timeout),
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval: Duration::from_millis(heartbeat_interval),
            apply_interval: Duration::from_millis(apply_interval),
            last_heartbeat: Instant::now(),
            storage,
            network,
            callbacks,
            election_votes: HashMap::new(),
            election_effective_voters: HashSet::new(),
            election_joint_config: None,
            election_max_term: current_term,
            current_election_id: None,
            current_snapshot_id: None,
        }
    }

    /// 处理事件（主入口）
    pub async fn handle_event(&mut self, event: Event) {
        match event {
            Event::ElectionTimeout => self.handle_election_timeout().await,
            Event::RequestVote(args) => self.handle_request_vote(args).await,
            Event::AppendEntries(args) => self.handle_append_entries(args).await,
            Event::RequestVoteReply(peer, reply) => self.handle_request_vote_reply(peer, reply).await,
            Event::AppendEntriesReply(peer, reply) => self.handle_append_entries_reply(peer, reply).await,
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().await,
            Event::ClientPropose { cmd, request_id } => self.handle_client_propose(cmd, request_id).await,
            Event::InstallSnapshot(args) => self.handle_install_snapshot(args).await,
            Event::InstallSnapshotReply(peer, reply) => self.handle_install_snapshot_reply(peer, reply).await,
            Event::ApplyLogs => self.apply_committed_logs().await,
        }
    }

    // === 选举相关逻辑 ===
    async fn handle_election_timeout(&mut self) {
        if self.role == Role::Leader {
            return; // Leader 不处理选举超时
        }

        // 切换为 Candidate 并递增任期
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;

        // 生成新选举ID并初始化跟踪状态
        let election_id = RequestId::new();
        self.current_election_id = Some(election_id);
        self.election_votes.clear();
        self.election_votes.insert(self.id.clone(), true);
        self.election_effective_voters = self.get_effective_voters();
        self.election_joint_config = self.config.joint.clone();
        self.election_max_term = self.current_term;

        // 重置选举定时器
        let new_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(new_timeout);
        self.callbacks.set_election_timer(self.election_timeout).await;

        // 发送投票请求
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();
        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: election_id,
        };

        for peer in &self.election_effective_voters {
            if *peer != self.id {
                let target = peer.clone();
                let args = req.clone();
                self.callbacks.send_request_vote_request(target, args).await;
            }
        }

        self.callbacks.state_changed(Role::Candidate).await;
    }

    async fn handle_request_vote(&mut self, args: RequestVoteRequest) {
        let mut vote_granted = false;

        // 处理更高任期
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;
            self.callbacks.state_changed(Role::Follower).await;
        }

        // 日志最新性检查
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();
        let log_ok = args.last_log_term > last_log_term 
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index);

        // 投票条件：同任期、未投票或投给同一人、日志最新
        if args.term == self.current_term 
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id.clone()))
            && log_ok
        {
            self.voted_for = Some(args.candidate_id.clone());
            vote_granted = true;
            self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;
        }

        // 发送响应
        let resp = RequestVoteResponse {
            term: self.current_term,
            vote_granted,
            request_id: args.request_id,
        };
        self.callbacks.send_request_vote_response(args.candidate_id, resp).await;
    }

    async fn handle_request_vote_reply(&mut self, peer: NodeId, reply: RequestVoteResponse) {
        // 过滤非候选人状态或过期请求
        if self.role != Role::Candidate || self.current_election_id != Some(reply.request_id) {
            return;
        }

        // 过滤无效投票者
        if !self.election_effective_voters.contains(&peer) {
            return;
        }

        // 处理更高任期
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.election_votes.clear();
            self.current_election_id = None;
            self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;
            self.callbacks.state_changed(Role::Follower).await;
            return;
        }

        // 记录投票结果
        if reply.term == self.current_term {
            self.election_votes.insert(peer, reply.vote_granted);
        }

        // 检查是否赢得选举
        self.check_election_result().await;
    }

    async fn check_election_result(&mut self) {
        let received = self.election_votes.len();
        let total = self.election_effective_voters.len();
        if received < total {
            return; // 等待所有投票
        }

        // 判断是否赢得选举
        let win = if let Some(joint) = &self.election_joint_config {
            let old_quorum = joint.old.len() / 2 + 1;
            let new_quorum = joint.new.len() / 2 + 1;
            let old_votes = self.election_votes.iter()
                .filter(|(id, granted)| **granted && joint.old.contains(*id))
                .count();
            let new_votes = self.election_votes.iter()
                .filter(|(id, granted)|** granted && joint.new.contains(*id))
                .count();
            old_votes >= old_quorum && new_votes >= new_quorum
        } else {
            let quorum = self.config.quorum();
            self.election_votes.values().filter(|&&v| v).count() >= quorum
        };

        if win {
            self.become_leader().await;
        } else {
            self.reset_election().await;
        }
    }

    async fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.current_election_id = None;

        // 初始化复制状态
        let last_log_index = self.get_last_log_index();
        self.next_index.clear();
        self.match_index.clear();
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }

        // 启动心跳和日志应用定时器
        self.callbacks.set_heartbeat_timer(self.heartbeat_interval).await;
        self.callbacks.set_apply_timer(self.apply_interval).await;
        self.callbacks.state_changed(Role::Leader).await;

        // 立即发送心跳
        self.broadcast_append_entries().await;
    }

    async fn reset_election(&mut self) {
        self.current_election_id = None;
        let new_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(new_timeout);
        self.callbacks.set_election_timer(self.election_timeout).await;
    }

    // === 日志同步相关逻辑 ===
    async fn handle_heartbeat_timeout(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self.broadcast_append_entries().await;
        self.callbacks.set_heartbeat_timer(self.heartbeat_interval).await;
    }

    async fn broadcast_append_entries(&mut self) {
        let current_term = self.current_term;
        let leader_id = self.id.clone();
        let leader_commit = self.commit_index;
        let last_log_index = self.get_last_log_index();
        let max_batch_size = 100;

        // 向所有节点发送日志
        let peers = self.get_effective_peers();
        for peer in peers {
            if peer == self.id {
                continue;
            }

            // 检查是否需要发送快照（日志差距过大）
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
            if next_idx <= self.last_snapshot_index {
                self.send_snapshot_to(peer.clone()).await;
                continue;
            }

            // 构造AppendEntries请求
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else if prev_log_index <= self.last_snapshot_index {
                self.last_snapshot_term
            } else {
                self.storage.term(prev_log_index).unwrap_or(0)
            };

            let entries = match self.storage.entries(next_idx, last_log_index + 1) {
                Ok(entries) => entries.into_iter().take(max_batch_size).collect(),
                Err(_) => vec![],
            };

            let req = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                request_id: RequestId::new(),
            };

            self.callbacks.send_append_entries_request(peer, req).await;
        }
    }

    async fn handle_append_entries(&mut self, args: AppendEntriesRequest) {
        // 处理更低任期的请求
        if args.term < self.current_term {
            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(self.get_last_log_index() + 1),
                request_id: args.request_id,
            };
            self.callbacks.send_append_entries_response(args.leader_id, resp).await;
            return;
        }

        // 切换为Follower并重置定时器
        self.role = Role::Follower;
        self.current_term = args.term;
        self.last_heartbeat = Instant::now();
        self.callbacks.set_election_timer(self.election_timeout).await;
        self.callbacks.state_changed(Role::Follower).await;

        // 日志连续性检查
        let prev_log_ok = if args.prev_log_index == 0 {
            true // 从0开始的日志无需检查
        } else if args.prev_log_index <= self.last_snapshot_index {
            // 快照覆盖的日志，检查任期是否匹配快照
            args.prev_log_term == self.last_snapshot_term
        } else {
            // 检查日志任期是否匹配
            self.storage.term(args.prev_log_index).unwrap_or(0) == args.prev_log_term
        };

        if !prev_log_ok {
            let conflict_idx = if args.prev_log_index > self.get_last_log_index() {
                self.get_last_log_index() + 1
            } else {
                args.prev_log_index
            };
            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index: Some(conflict_idx),
                request_id: args.request_id,
            };
            self.callbacks.send_append_entries_response(args.leader_id, resp).await;
            return;
        }

        // 截断冲突日志并追加新日志
        if args.prev_log_index < self.get_last_log_index() {
            let _ = self.storage.truncate_suffix(args.prev_log_index + 1);
        }
        if !args.entries.is_empty() {
            let _ = self.storage.append(&args.entries);
            self.callbacks.save_log_entries(args.entries.clone()).await;
            
            // 处理配置变更日志
            self.process_config_entries(&args.entries).await;
        }

        // 更新提交索引
        if args.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(args.leader_commit, self.get_last_log_index());
        }

        // 发送成功响应
        let resp = AppendEntriesResponse {
            term: self.current_term,
            success: true,
            conflict_index: None,
            request_id: args.request_id,
        };
        self.callbacks.send_append_entries_response(args.leader_id, resp).await;
    }

    async fn handle_append_entries_reply(&mut self, peer: NodeId, reply: AppendEntriesResponse) {
        if self.role != Role::Leader {
            return;
        }

        // 处理更高任期
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;
            self.callbacks.state_changed(Role::Follower).await;
            return;
        }

        // 更新复制状态
        if reply.success {
            let req_next_idx = self.next_index.get(&peer).copied().unwrap_or(1);
            let entries_len = self.storage.entries(req_next_idx, self.get_last_log_index() + 1)
                .unwrap_or_default().len() as u64;
            let new_match_idx = req_next_idx + entries_len - 1;
            let new_next_idx = new_match_idx + 1;

            self.match_index.insert(peer.clone(), new_match_idx);
            self.next_index.insert(peer.clone(), new_next_idx);

            // 尝试更新commit_index
            self.update_commit_index();
        } else {
            // 日志冲突，回退next_index
            let current_next = self.next_index.get(&peer).copied().unwrap_or(1);
            let new_next = reply.conflict_index.unwrap_or(current_next - 1);
            self.next_index.insert(peer.clone(), new_next.max(1));
        }
    }

    // === 快照相关逻辑 ===
    async fn send_snapshot_to(&mut self, target: NodeId) {
        let snap = match self.storage.load_snapshot() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("加载快照失败: {}", e.0);
                return;
            }
        };

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: snap.index,
            last_included_term: snap.term,
            data: snap.data.clone(),
            request_id: RequestId::new(),
        };

        self.callbacks.send_install_snapshot_request(target, req).await;
    }

    async fn handle_install_snapshot(&mut self, args: InstallSnapshotRequest) {
        // 处理更低任期的请求
        if args.term < self.current_term {
            let resp = InstallSnapshotReply {
                term: self.current_term,
                success: false,
                request_id: args.request_id,
            };
            self.callbacks.send_install_snapshot_reply(args.leader_id, resp).await;
            return;
        }

        // 切换为Follower并更新状态
        self.role = Role::Follower;
        self.current_term = args.term;
        self.last_heartbeat = Instant::now();
        self.current_snapshot_id = Some(args.request_id);
        self.callbacks.set_election_timer(self.election_timeout).await;

        // 仅处理比当前快照更新的快照
        if args.last_included_index <= self.last_snapshot_index {
            let resp = InstallSnapshotReply {
                term: self.current_term,
                success: true,
                request_id: args.request_id,
            };
            self.callbacks.send_install_snapshot_reply(args.leader_id, resp).await;
            return;
        }

        // 保存快照并截断日志
        let snap = Snapshot {
            index: args.last_included_index,
            term: args.last_included_term,
            data: args.data.clone(),
            config: self.config.clone(), // 快照包含当前配置
        };
        let _ = self.storage.save_snapshot(&snap);
        self.callbacks.save_snapshot(snap.clone()).await;

        // 截断快照之前的日志
        let _ = self.storage.truncate_prefix(args.last_included_index);
        self.last_snapshot_index = args.last_included_index;
        self.last_snapshot_term = args.last_included_term;
        self.commit_index = self.commit_index.max(args.last_included_index);
        self.last_applied = self.last_applied.max(args.last_included_index);

        // 发送成功响应
        let resp = InstallSnapshotReply {
            term: self.current_term,
            success: true,
            request_id: args.request_id,
        };
        self.callbacks.send_install_snapshot_reply(args.leader_id, resp).await;
    }

    async fn handle_install_snapshot_reply(&mut self, peer: NodeId, reply: InstallSnapshotReply) {
        if self.role != Role::Leader {
            return;
        }

        // 处理更高任期
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.callbacks.save_hard_state(self.current_term, self.voted_for.clone()).await;
            self.callbacks.state_changed(Role::Follower).await;
            return;
        }

        if reply.success {
            // 快照安装成功，更新复制状态
            let snap_index = self.last_snapshot_index;
            self.next_index.insert(peer.clone(), snap_index + 1);
            self.match_index.insert(peer.clone(), snap_index);
        }
    }

    // === 客户端请求与日志应用 ===
    async fn handle_client_propose(&mut self, cmd: Command, request_id: RequestId) {
        if self.role != Role::Leader {
            self.callbacks.client_response(request_id, Err(Error("not leader".into()))).await;
            return;
        }

        // 生成日志条目
        let last_idx = self.get_last_log_index();
        let new_entry = LogEntry {
            term: self.current_term,
            index: last_idx + 1,
            command: cmd,
            is_config: false, // 普通命令
        };

        // 追加日志
        let _ = self.storage.append(&[new_entry.clone()]);
        self.callbacks.save_log_entries(vec![new_entry]).await;

        // 记录客户端请求与日志索引的映射
        self.client_requests.insert(request_id, last_idx + 1);

        // 立即同步日志
        self.broadcast_append_entries().await;
    }

    async fn apply_committed_logs(&mut self) {
        // 应用已提交但未应用的日志
        if self.last_applied >= self.commit_index {
            return;
        }

        let start = self.last_applied + 1;
        let end = self.commit_index;
        let entries = match self.storage.entries(start, end + 1) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!("读取日志失败: {}", e.0);
                return;
            }
        };

        // 逐个应用日志
        for entry in entries {
            self.callbacks.apply_command(entry.index, entry.term, entry.command).await;
            self.last_applied = entry.index;

            // 如果是客户端请求，返回响应
            self.check_client_response(entry.index).await;
        }

        // 继续定时应用
        self.callbacks.set_apply_timer(self.apply_interval).await;
    }

    async fn check_client_response(&mut self, log_index: u64) {
        // 查找该日志索引对应的客户端请求并响应
        let mut completed = vec![];
        for (req_id, idx) in &self.client_requests {
            if *idx == log_index {
                self.callbacks.client_response(*req_id, Ok(log_index)).await;
                completed.push(*req_id);
            }
        }
        // 移除已响应的请求
        for req_id in completed {
            self.client_requests.remove(&req_id);
        }
    }

    // === 集群配置变更 ===
    async fn process_config_entries(&mut self, entries: &[LogEntry]) {
        for entry in entries {
            if entry.is_config {
                // 解析配置变更命令（实际实现需序列化/反序列化）
                if let Ok(new_config) = bincode::deserialize(&entry.command) {
                    self.config = new_config;
                    self.callbacks.save_cluster_config(self.config.clone()).await;
                    
                    // 如果是联合配置且已提交，尝试退出联合状态
                    if self.config.joint.is_some() && entry.index <= self.commit_index {
                        self.try_leave_joint().await;
                    }
                }
            }
        }
    }

    async fn try_leave_joint(&mut self) {
        // 检查是否所有节点都已复制联合配置日志
        let all_replicated = self.peers.iter()
            .all(|peer| self.match_index.get(peer).copied().unwrap_or(0) >= self.commit_index);

        if all_replicated {
            self.config.leave_joint();
            self.callbacks.save_cluster_config(self.config.clone()).await;
        }
    }

    // === 辅助方法 ===
    fn get_effective_voters(&self) -> HashSet<NodeId> {
        match &self.config.joint {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.config.voters.clone(),
        }
    }

    fn get_effective_peers(&self) -> Vec<NodeId> {
        self.get_effective_voters().into_iter().filter(|id| *id != self.id).collect()
    }

    fn get_last_log_index(&self) -> u64 {
        self.storage.last_index().unwrap_or(0).max(self.last_snapshot_index)
    }

    fn get_last_log_term(&self) -> u64 {
        let last_log_idx = self.storage.last_index().unwrap_or(0);
        if last_log_idx == 0 {
            self.last_snapshot_term
        } else {
            self.storage.term(last_log_idx).unwrap_or(self.last_snapshot_term)
        }
    }

    fn update_commit_index(&mut self) {
        // 仅Leader更新commit_index：寻找大多数节点已复制的日志
        let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
        match_indices.push(self.get_last_log_index()); // 包含自身
        match_indices.sort_unstable_by(|a, b| b.cmp(a)); // 降序排列

        let quorum = self.config.quorum();
        if match_indices.len() >= quorum {
            let candidate = match_indices[quorum - 1];
            // 确保候选索引的任期与当前任期相同（Raft约束）
            if candidate > self.commit_index && self.storage.term(candidate).unwrap_or(0) == self.current_term {
                self.commit_index = candidate;
            }
        }
    }
}

// === 外部驱动层 ===
pub struct RaftDriver {
    state: RaftState,
}

impl RaftDriver {
    pub fn new(state: RaftState) -> Self {
        Self { state }
    }

    pub async fn run(&mut self) {
        loop {
            // 实际实现应从事件源（网络/定时器/客户端）接收事件
            let event = self.receive_event().await;
            self.state.handle_event(event).await;
        }
    }

    async fn receive_event(&self) -> Event {
        // 示例：等待定时器事件（实际应实现真实事件接收）
        tokio::time::sleep(Duration::from_secs(1)).await;
        Event::ElectionTimeout
    }
}

// === 回调接口的默认实现（示例）===
#[derive(Clone)]
pub struct DefaultCallbacks {
    network: Arc<dyn Network + Send + Sync>,
    storage: Arc<dyn Storage>,
}

impl DefaultCallbacks {
    pub fn new(network: Arc<dyn Network + Send + Sync>, storage: Arc<dyn Storage>) -> Self {
        Self { network, storage }
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
            let _ = network.send_request_vote(target, args).await;
        })
    }

    fn send_request_vote_response(
        &self,
        target: NodeId,
        _args: RequestVoteResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
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

    fn send_append_entries_response(
        &self,
        target: NodeId,
        _args: AppendEntriesResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
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

    fn send_install_snapshot_reply(
        &self,
        target: NodeId,
        _args: InstallSnapshotReply,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
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

    fn save_snapshot(&self, snap: Snapshot) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let storage = self.storage.clone();
        Box::pin(async move {
            let _ = storage.save_snapshot(&snap);
        })
    }

    fn save_cluster_config(
        &self,
        conf: ClusterConfig,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let storage = self.storage.clone();
        Box::pin(async move {
            let _ = storage.save_cluster_config(&conf);
        })
    }

    fn set_election_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_heartbeat_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_apply_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn client_response(
        &self,
        _request_id: RequestId,
        _result: Result<u64, Error>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn state_changed(&self, _role: Role) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn apply_command(&self, _index: u64, _term: u64, _cmd: Command) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }
}
