use crate::cluster_config::ClusterConfig;
use crate::cluster_config::JointConfig;
use crate::cluster_config::QuorumRequirement;
use crate::error::*;
use crate::traits::*;
use anyhow::Result;
use async_trait::async_trait;

use crate::message::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

pub mod cluster_config;
pub mod error;
pub mod message;
pub mod mutl_raft_driver;
pub mod network;
pub mod pipeline;
pub mod tests;
pub mod traits;
pub mod wal;
// 类型定义

pub type GroupId = String;
pub type Command = Vec<u8>;
pub type TimerId = u64; // 定时器ID类型
pub type NodeId = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RaftId {
    pub group: GroupId,
    pub node: NodeId,
}

impl Display for RaftId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.group, self.node)
    }
}

impl RaftId {
    pub fn new(group: GroupId, node: NodeId) -> Self {
        Self { group, node }
    }
}

// 请求ID类型（用于过滤超时响应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new() -> Self {
        Self(rand::random::<u64>())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<u64> for RequestId {
    fn into(self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

// === 事件定义（输入）===
#[derive(Debug, Clone)]
pub enum Event {
    // 定时器事件
    ElectionTimeout,     // 选举超时（Follower/Candidate 触发）
    HeartbeatTimeout,    // 心跳超时（Leader 触发日志同步）
    ApplyLogTimeout,     // 定期将已提交日志应用到状态机
    ConfigChangeTimeout, // 配置变更超时

    // RPC 请求事件（来自其他节点）
    RequestVoteRequest(RaftId, RequestVoteRequest),
    AppendEntriesRequest(RaftId, AppendEntriesRequest),
    InstallSnapshotRequest(RaftId, InstallSnapshotRequest),

    // RPC 响应事件（其他节点对本节点请求的回复）
    RequestVoteResponse(RaftId, RequestVoteResponse),
    AppendEntriesResponse(RaftId, AppendEntriesResponse),
    InstallSnapshotResponse(RaftId, InstallSnapshotResponse),

    // 领导人转移相关事件
    LeaderTransfer {
        target: RaftId,
        request_id: RequestId,
    },
    LeaderTransferTimeout,

    // 客户端事件
    ClientPropose {
        cmd: Command,
        request_id: RequestId, // 客户端请求ID，用于关联响应
    },
    // 配置变更事件
    ChangeConfig {
        new_voters: HashSet<RaftId>,
        request_id: RequestId,
    },

    // Learner 管理事件
    AddLearner {
        learner: RaftId,
        request_id: RequestId,
    },
    RemoveLearner {
        learner: RaftId,
        request_id: RequestId,
    },

    // 快照生成
    CreateSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Follower => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader => write!(f, "Leader"),
        }
    }
}

// === 状态机（可变状态，无 Clone）===
pub struct RaftState {
    // 节点标识与配置
    id: RaftId,
    leader_id: Option<RaftId>,
    config: ClusterConfig,

    // 核心状态
    role: Role,
    current_term: u64,
    voted_for: Option<RaftId>,

    // 日志与提交状态
    commit_index: u64,
    last_applied: u64,
    last_snapshot_index: u64, // 最后一个快照的索引
    last_snapshot_term: u64,  // 最后一个快照的任期
    last_log_index: u64,      // 最后一个日志条目的索引
    last_log_term: u64,       // 最后一个日志条目的任期

    // Leader 专用状态
    next_index: HashMap<RaftId, u64>,
    match_index: HashMap<RaftId, u64>,
    client_requests: HashMap<RequestId, u64>, // 客户端请求ID -> 日志索引
    client_requests_revert: HashMap<u64, RequestId>, // 日志索引 -> 客户端请求ID

    // 配置变更相关状态
    config_change_in_progress: bool,
    config_change_start_time: Option<Instant>,
    config_change_timeout: Duration,
    joint_config_log_index: u64, // 联合配置的日志索引

    // 定时器配置
    election_timeout: Duration,
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: Duration,
    heartbeat_interval_timer_id: Option<TimerId>,
    apply_interval: Duration, // 日志应用到状态机的间隔
    apply_interval_timer: Option<TimerId>,
    config_change_timer: Option<TimerId>,

    last_heartbeat: Instant,

    // 外部依赖
    callbacks: Arc<dyn RaftCallbacks>,

    // 统一错误处理器
    error_handler: CallbackErrorHandler,

    // 选举跟踪（仅 Candidate 状态有效）
    election_votes: HashMap<RaftId, bool>,
    election_max_term: u64,
    current_election_id: Option<RequestId>,

    // 快照请求跟踪（仅 Follower 有效）
    current_snapshot_request_id: Option<RequestId>,

    // 快照相关状态（Leader 用）
    follower_snapshot_states: HashMap<RaftId, InstallSnapshotState>,
    follower_last_snapshot_index: HashMap<RaftId, u64>,
    snapshot_probe_schedules: Vec<SnapshotProbeSchedule>,
    schedule_snapshot_probe_interval: Duration,
    schedule_snapshot_probe_retries: u32,

    // 领导人转移相关状态
    leader_transfer_target: Option<RaftId>,
    leader_transfer_request_id: Option<RequestId>,
    leader_transfer_timeout: Duration,
    leader_transfer_start_time: Option<Instant>,

    election_timer: Option<TimerId>,        // 选举定时器ID
    leader_transfer_timer: Option<TimerId>, // 领导人转移定时器ID

    // Pipeline 状态管理（反馈控制和超时管理）
    pipeline: pipeline::PipelineState,

    // 其他状态（可扩展）
    options: RaftStateOptions,
}

#[derive(Debug, Clone)]
pub struct RaftStateOptions {
    pub id: RaftId,
    pub peers: Vec<RaftId>,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64,
    pub apply_interval: u64,
    pub config_change_timeout: Duration,
    pub leader_transfer_timeout: Duration,
    pub apply_batch_size: u64, // 每次应用到状态机的日志条数
    pub schedule_snapshot_probe_interval: Duration,
    pub schedule_snapshot_probe_retries: u32,
    // 反馈控制相关配置
    pub max_inflight_requests: u64,  // 最大InFlight请求数
    pub initial_batch_size: u64,     // 初始批次大小
    pub max_batch_size: u64,         // 最大批次大小
    pub min_batch_size: u64,         // 最小批次大小
    pub feedback_window_size: usize, // 反馈窗口大小（用于计算平均值）

    // 智能超时配置
    pub base_request_timeout: Duration, // 基础请求超时时间 (默认3秒)
    pub max_request_timeout: Duration,  // 最大请求超时时间 (默认30秒)
    pub min_request_timeout: Duration,  // 最小请求超时时间 (默认1秒)
    pub timeout_response_factor: f64,   // 响应时间权重因子 (默认2.0，表示超时=2倍平均响应时间)
}

impl Default for RaftStateOptions {
    fn default() -> Self {
        Self {
            id: RaftId::new("".to_string(), "".to_string()),
            peers: vec![],
            election_timeout_min: 500,
            election_timeout_max: 1000,
            heartbeat_interval: 50,
            apply_interval: 100,
            apply_batch_size: 64,
            config_change_timeout: Duration::from_secs(10),
            leader_transfer_timeout: Duration::from_secs(10),
            schedule_snapshot_probe_interval: Duration::from_secs(5),
            schedule_snapshot_probe_retries: 24 * 60 * 60 / 5, // 默认尝试24小时
            // 反馈控制默认配置
            max_inflight_requests: 64, // 最大InFlight请求数
            initial_batch_size: 10,    // 初始批次大小，保守起步
            max_batch_size: 100,       // 最大批次大小
            min_batch_size: 1,         // 最小批次大小
            feedback_window_size: 10,  // 反馈窗口大小
            // 智能超时默认配置
            base_request_timeout: Duration::from_secs(3),
            max_request_timeout: Duration::from_secs(30),
            min_request_timeout: Duration::from_secs(1),
            timeout_response_factor: 2.0,
        }
    }
}

impl RaftState {
    /// 初始化状态
    pub async fn new(options: RaftStateOptions, callbacks: Arc<dyn RaftCallbacks>) -> Result<Self> {
        // 从回调加载持久化状态
        let (current_term, voted_for) = match callbacks.load_hard_state(&options.id).await {
            Ok(Some(hard_state)) => (hard_state.term, hard_state.voted_for),
            Ok(None) => (0, None), // 如果没有持久化状态，则初始化为0
            Err(err) => {
                // 加载失败时也初始化为0
                error!("Failed to load hard state: {}", err);
                return Err(RaftError::Storage(err).into());
            }
        };

        let loaded_config = match callbacks.load_cluster_config(&options.id).await {
            Ok(conf) => conf,
            Err(err) => {
                error!("Failed to load cluster config: {}", err);
                // 如果加载失败，使用默认空配置
                return Err(RaftError::Storage(err).into());
            }
        };

        let snap = match callbacks.load_snapshot(&options.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                info!("Node {} No snapshot found", options.id);
                // 如果没有快照，初始化为索引0，任期0的空快照
                Snapshot {
                    index: 0,
                    term: 0,
                    data: vec![],
                    config: ClusterConfig::empty(),
                }
            }
            Err(err) => {
                error!("Failed to load snapshot: {}", err);
                // 如果加载失败，使用默认快照
                return Err(RaftError::Storage(err).into());
            }
        };
        let timeout = options.election_timeout_min
            + rand::random::<u64>()
                % (options.election_timeout_max - options.election_timeout_min + 1);

        let (last_log_index, last_log_term) = match callbacks.get_last_log_index(&options.id).await
        {
            Ok((index, term)) => (index, term),
            Err(err) => {
                error!("Failed to get last log index: {}", err);
                return Err(RaftError::Storage(err).into());
            }
        };

        Ok(RaftState {
            schedule_snapshot_probe_interval: options.schedule_snapshot_probe_interval,
            schedule_snapshot_probe_retries: options.schedule_snapshot_probe_retries,
            error_handler: CallbackErrorHandler::new(options.id.clone()),
            leader_id: None,
            leader_transfer_request_id: None,
            leader_transfer_start_time: None,
            leader_transfer_target: None,
            leader_transfer_timeout: options.leader_transfer_timeout,
            id: options.id.clone(),
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            commit_index: snap.index, // 提交索引从快照开始
            last_applied: snap.index,
            last_log_index,
            last_log_term,
            last_snapshot_index: snap.index,
            last_snapshot_term: snap.term,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            client_requests: HashMap::new(),
            client_requests_revert: HashMap::new(),
            config_change_in_progress: false,
            config_change_start_time: None,
            heartbeat_interval_timer_id: None,
            config_change_timeout: options.config_change_timeout,
            joint_config_log_index: 0, // 联合配置的日志索引初始化为0
            election_timeout: Duration::from_millis(timeout),
            election_timeout_min: options.election_timeout_min,
            election_timeout_max: options.election_timeout_max,
            heartbeat_interval: Duration::from_millis(options.heartbeat_interval),
            apply_interval: Duration::from_millis(options.apply_interval),
            apply_interval_timer: None,
            config_change_timer: None,
            last_heartbeat: Instant::now(),
            callbacks: callbacks,
            election_votes: HashMap::new(),
            election_max_term: current_term,
            current_election_id: None,
            current_snapshot_request_id: None,
            follower_snapshot_states: HashMap::new(),
            follower_last_snapshot_index: HashMap::new(),
            snapshot_probe_schedules: Vec::new(),
            election_timer: None,
            leader_transfer_timer: None,
            // 初始化 Pipeline 状态管理
            pipeline: pipeline::PipelineState::new(options.clone()),
            options,
        })
    }

    /// 处理事件（主入口）
    pub async fn handle_event(&mut self, event: Event) {
        match event {
            Event::ElectionTimeout => self.handle_election_timeout().await,
            Event::RequestVoteRequest(sender, request) => {
                self.handle_request_vote(sender, request).await
            }
            Event::AppendEntriesRequest(sender, request) => {
                self.handle_append_entries_request(sender, request).await
            }
            Event::RequestVoteResponse(sender, response) => {
                self.handle_request_vote_response(sender, response).await
            }
            Event::AppendEntriesResponse(sender, response) => {
                self.handle_append_entries_response(sender, response).await
            }
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().await,
            Event::ClientPropose { cmd, request_id } => {
                self.handle_client_propose(cmd, request_id).await
            }
            Event::InstallSnapshotRequest(sender, request) => {
                self.handle_install_snapshot(sender, request).await
            }
            Event::InstallSnapshotResponse(sender, response) => {
                self.handle_install_snapshot_response(sender, response)
                    .await
            }
            Event::ApplyLogTimeout => self.apply_committed_logs().await,
            Event::ConfigChangeTimeout => self.handle_config_change_timeout().await,
            Event::ChangeConfig {
                new_voters,
                request_id,
            } => self.handle_change_config(new_voters, request_id).await,
            Event::AddLearner {
                learner,
                request_id,
            } => {
                self.handle_add_learner(learner, request_id).await;
            }
            Event::RemoveLearner {
                learner,
                request_id,
            } => {
                self.handle_remove_learner(learner, request_id).await;
            }
            Event::LeaderTransfer { target, request_id } => {
                self.handle_leader_transfer(target, request_id).await;
            }
            Event::LeaderTransferTimeout => {
                self.handle_leader_transfer_timeout().await;
            }
            Event::CreateSnapshot => self.create_snapshot().await,
        }
    }
    async fn handle_election_timeout(&mut self) {
        if self.role == Role::Leader {
            info!(target: "raft", "Node {} is the leader and will not start a new election", self.id);
            return; // Leader 不处理选举超时
        }

        // Learner 不参与选举，只接收日志复制
        if !self.config.voters_contains(&self.id) {
            warn!("Node {} is a Learner and cannot start an election", self.id);
            return; // Learner 不处理选举超时
        }

        info!(
            "Node {} starting election for term {}",
            self.id,
            self.current_term + 1
        );

        // 切换为 Candidate 并递增任期
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());

        // 持久化状态变更
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .save_hard_state(
                        &self.id,
                        HardState {
                            raft_id: self.id.clone(),
                            term: self.current_term,
                            voted_for: self.voted_for.clone(),
                        },
                    )
                    .await,
                "save_hard_state",
                None,
            )
            .await;

        // 如果持久化失败是致命的，error_handler 会处理（如进入只读模式）
        // 如果不是致命的，我们继续，但最好记录下来

        // 重置选举定时器
        self.reset_election().await;

        // 生成新选举ID并初始化跟踪状态
        let election_id = RequestId::new();
        self.current_election_id = Some(election_id);
        self.election_votes.clear();
        self.election_votes.insert(self.id.clone(), true);
        self.election_max_term = self.current_term;

        // 获取日志信息用于选举
        // 注意：如果 get_xxx 失败，会使用默认值 0，这可能影响选举结果
        // error_handler 会记录相关错误
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();

        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: election_id,
        };

        // 发送投票请求
        // 使用 peers 而不是 voters，因为可能需要通知非 voter 的节点（如果有的话）
        // 但根据 get_effective_voters 的实现，这里应该是指 voters
        for peer in self.config.get_effective_voters() {
            if *peer != self.id {
                let target = peer;
                let args = req.clone();

                // handle 会根据错误 severity 记录日志，我们不需要在这里额外处理 None
                let result = self
                    .error_handler // <--- 添加这行来处理发送结果（虽然通常忽略）
                    .handle(
                        self.callbacks
                            .send_request_vote_request(&self.id, &target, args)
                            .await,
                        "send_request_vote_request",
                        Some(&target),
                    )
                    .await;
                // 可选：添加调试日志来显示发送结果
                if result.is_none() {
                    warn!(
                        "Failed to send RequestVote to {}, will retry or ignore based on error severity",
                        target
                    );
                }
            }
        }

        // 通知上层应用状态变更
        // 确保在关键状态（任期、投票）更新和持久化之后调用
        let _state_change_result = self
            .error_handler
            .handle_void(
                self.callbacks
                    .state_changed(&self.id, Role::Candidate)
                    .await,
                "state_changed",
                None,
            )
            .await;
        // handle_void 内部已处理错误，这里通常不需要额外处理
    }

    async fn handle_request_vote(&mut self, sender: RaftId, request: RequestVoteRequest) {
        if sender != request.candidate_id {
            warn!(
                "Node {} received vote request from {}, but candidate is {}",
                self.id, sender, request.candidate_id
            );
            return;
        }

        // --- 1. 处理更高任期 ---
        // 如果收到的任期大于当前任期，更新任期并转换为 Follower
        if request.term > self.current_term {
            info!(
                "Node {} stepping down to Follower, updating term from {} to {}",
                self.id, self.current_term, request.term
            );
            self.current_term = request.term;
            self.role = Role::Follower;
            self.voted_for = None; // 重置投票状态
            self.leader_id = None; // 清除领导者ID，因为在新任期中领导者未知
            // 持久化新的任期和投票状态
            let _ = self
                .error_handler // 使用统一错误处理
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
            // 通知状态变更
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks.state_changed(&self.id, Role::Follower).await,
                    "state_changed",
                    None,
                )
                .await;
            // 注意：这里没有 return，因为即使任期更新了，我们仍然需要决定是否给这个候选人投票
            // 这与 AppendEntries 的处理不同，那里通常会直接拒绝旧任期的请求。
        }

        // --- 2. 决定是否投票 ---
        // 初始化投票结果为 false
        let mut vote_granted = false;

        // 在决定投票时，必须使用处理完任期后的最终状态 (self.current_term)
        // 投票条件：
        // a. 候选人的任期 >= 当前节点的任期 (通常是等于，因为如果大于已经在上面处理了)
        // b. 当前节点没有投过票，或者已经投给了这个候选人
        // c. 候选人的日志至少和当前节点的日志一样新
        if request.term >= self.current_term // 使用 >= 确保即使任期相等也进行检查
        && (self.voted_for.is_none() || self.voted_for == Some(request.candidate_id.clone()))
        {
            // --- 3. 日志最新性检查 ---
            let log_ok = self
                .is_log_up_to_date(request.last_log_index, request.last_log_term)
                .await;

            if log_ok {
                // 满足所有投票条件，授予投票
                self.voted_for = Some(request.candidate_id.clone());
                vote_granted = true;
                info!(
                    "Node {} granting vote to {} for term {}",
                    self.id,
                    request.candidate_id,
                    self.current_term // 使用更新后的任期
                );
                // 持久化新的投票状态
                let _ = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .save_hard_state(
                                &self.id,
                                HardState {
                                    raft_id: self.id.clone(),
                                    term: self.current_term,
                                    voted_for: self.voted_for.clone(),
                                },
                            )
                            .await,
                        "save_hard_state",
                        None,
                    )
                    .await;
                // 注意：这里不需要再次调用 state_changed，因为角色已经在任期更新时确定为 Follower
            } else {
                info!(
                    "Node {} rejecting vote for {}, logs not up-to-date",
                    self.id, request.candidate_id
                );
            }
        } else {
            // 不满足投票条件 (任期不够新，或已投票给其他人)
            info!(
                "Node {} rejecting vote for {} in term {}, already voted for {:?} or term mismatch (args.term: {}, self.current_term: {})",
                self.id,
                request.candidate_id,
                self.current_term,
                self.voted_for,
                request.term,
                self.current_term
            );
        }

        // --- 4. 发送响应 ---
        let resp = RequestVoteResponse {
            term: self.current_term, // 使用处理完后的最终任期
            vote_granted,
            request_id: request.request_id,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_request_vote_response(&self.id, &request.candidate_id, resp)
                    .await,
                "send_request_vote_response",
                Some(&request.candidate_id), // 添加目标节点信息到错误日志
            )
            .await;
    }
    // 优化的日志最新性检查
    async fn is_log_up_to_date(
        &mut self,
        candidate_last_index: u64,
        candidate_last_term: u64,
    ) -> bool {
        let self_last_term = self.get_last_log_term();
        let self_last_index = self.get_last_log_index();

        candidate_last_term > self_last_term
            || (candidate_last_term == self_last_term && candidate_last_index >= self_last_index)
    }

    async fn handle_request_vote_response(&mut self, peer: RaftId, response: RequestVoteResponse) {
        // 过滤非候选人状态或过期请求
        info!("node {}: received vote response: {:?}", self.id, response);
        if self.role != Role::Candidate || self.current_election_id != Some(response.request_id) {
            return;
        }

        // 过滤无效投票者
        if !self.config.voters_contains(&peer) {
            warn!(
                "node {}: received vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // 处理更高任期
        if response.term > self.current_term {
            info!(
                "Stepping down from candidate due to higher term {} from peer {} (current term {})",
                response.term, peer, self.current_term
            );
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.election_votes.clear();
            self.reset_election().await;

            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;

            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks.state_changed(&self.id, Role::Follower).await,
                    "state_changed",
                    None,
                )
                .await;
            return;
        }

        // 记录投票结果
        if response.term == self.current_term {
            self.election_votes.insert(peer, response.vote_granted);
        }

        // 检查是否赢得选举
        self.check_election_result().await;
    }

    async fn check_election_result(&mut self) {
        info!(
            "Node {}: check_election_result, votes: {:?}, current_term: {}, role: {:?}, election_id: {:?}",
            self.id, self.election_votes, self.current_term, self.role, self.current_election_id
        );

        let granted_votes: HashSet<RaftId> = self
            .election_votes
            .iter()
            .filter_map(|(id, &granted)| if granted { Some(id.clone()) } else { None })
            .collect();

        let win = self.config.majority(&granted_votes);
        if win {
            info!(
                "Node {} becomes leader for term {}",
                self.id, self.current_term
            );
            self.become_leader().await;
        }
    }

    // 启动心跳和日志应用定时器
    async fn reset_heartbeat_timer(&mut self) {
        if let Some(timer_id) = self.heartbeat_interval_timer_id {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        self.heartbeat_interval_timer_id = Some(
            self.callbacks
                .set_heartbeat_timer(&self.id, self.heartbeat_interval),
        );
    }

    async fn become_leader(&mut self) {
        warn!(
            "Node {} becoming leader for term {} (previous role: {:?})",
            self.id, self.current_term, self.role
        );

        self.role = Role::Leader;
        self.current_election_id = None;
        self.leader_id = None; // Leader 不需要跟踪其他 Leader

        // 初始化复制状态
        let last_log_index = self.get_last_log_index();
        self.next_index.clear();
        self.match_index.clear();
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        // 收集所有需要管理的节点（voters + learners）
        let all_peers: Vec<RaftId> = self
            .config
            .get_all_nodes()
            .into_iter()
            .filter(|peer| *peer != self.id)
            .collect();

        for peer in &all_peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }

        self.reset_heartbeat_timer().await;

        if let Err(err) = self.callbacks.state_changed(&self.id, Role::Leader).await {
            log::error!(
                "Failed to change state to Leader for node {}: {}",
                self.id,
                err
            );
        }

        // 立即发送心跳
        self.broadcast_append_entries().await;

        // 启动日志应用定时器
        self.adjust_apply_interval().await;
    }

    async fn reset_election(&mut self) {
        self.current_election_id = None;
        let new_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(new_timeout);

        if let Some(timer_id) = self.election_timer {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        self.election_timer = Some(
            self.callbacks
                .set_election_timer(&self.id, self.election_timeout),
        );
    }

    // === 日志同步相关逻辑 ===
    async fn handle_heartbeat_timeout(&mut self) {
        // 移除冗余的心跳日志，只在必要时记录
        if self.role != Role::Leader {
            return;
        }

        // 执行高效的超时检查
        self.pipeline.periodic_timeout_check();

        self.broadcast_append_entries().await;

        // 定期检查联合配置状态
        if self.config.is_joint() {
            self.check_joint_exit_condition().await;
        }
    }

    async fn broadcast_append_entries(&mut self) {
        let current_term = self.current_term;
        let leader_id = self.id.clone();
        let leader_commit = self.commit_index;
        let last_log_index = self.get_last_log_index();
        let now = Instant::now();

        self.reset_heartbeat_timer().await;
        // 检查并执行到期的快照探测计划
        self.process_pending_probes(now).await;

        // 向所有节点发送日志或探测消息
        let peers = self.get_effective_peers();
        for peer in peers {
            if peer == self.id {
                continue;
            }

            // 检查Follower是否正在安装快照
            if let Some(state) = self.follower_snapshot_states.get(&peer) {
                match state {
                    InstallSnapshotState::Installing => {
                        // 探测计划已处理，此处不需要额外操作
                        continue;
                    }
                    InstallSnapshotState::Failed(_) => {
                        // 失败状态，尝试重新发送快照
                        self.send_snapshot_to(peer.clone()).await;
                        continue;
                    }
                    _ => {}
                }
            }

            // 检查InFlight请求限制
            if !self.pipeline.can_send_to_peer(&peer) {
                debug!("Peer {} has too many inflight requests, skipping", peer);
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
                match self
                    .error_handler
                    .handle(
                        self.callbacks.get_log_term(&self.id, prev_log_index).await,
                        "get_log_term",
                        Some(&peer),
                    )
                    .await
                {
                    Some(term) => term,
                    None => continue,
                }
            };

            // 使用反馈控制的批次大小
            let batch_size = self.pipeline.get_adaptive_batch_size(&peer);
            let high = std::cmp::min(next_idx + batch_size, last_log_index + 1);
            let entries = match self
                .error_handler
                .handle(
                    self.callbacks
                        .get_log_entries(&self.id, next_idx, high)
                        .await,
                    "get_log_entries",
                    Some(&peer),
                )
                .await
            {
                Some(entries) => entries,
                None => {
                    continue;
                }
            };

            let entries_len = entries.len();
            let request_id = RequestId::new();
            let req = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                request_id,
            };

            // 只在发送日志条目时记录，心跳不记录
            if entries_len > 0 {
                info!(
                    "Leader {} sending {} entries to {} (batch_size={}): prev_log_index={}, next_index={} -> {}",
                    self.id,
                    entries_len,
                    peer,
                    batch_size,
                    prev_log_index,
                    next_idx,
                    next_idx + entries_len as u64
                );
            }

            // 记录InFlight请求
            self.pipeline.track_inflight_request(
                &peer,
                request_id,
                next_idx + entries_len as u64,
                now,
            );

            if let Err(err) = self
                .callbacks
                .send_append_entries_request(&self.id, &peer, req)
                .await
            {
                warn!(
                    "node {}: send append entries request failed: {}",
                    self.id, err
                );
                // 移除失败的InFlight请求
                self.pipeline.remove_inflight_request(&peer, request_id);
            } else {
                // 乐观更新next_index，避免重复发送相同的日志条目
                // 如果AppendEntries失败，会在响应处理中回退next_index
                let new_next_idx = next_idx + entries_len as u64;
                self.next_index.insert(peer.clone(), new_next_idx);
                debug!(
                    "Leader {} optimistically updated next_index for {} to {}",
                    self.id, peer, new_next_idx
                );
            }
        }
    }

    async fn handle_append_entries_request(
        &mut self,
        sender: RaftId,
        request: AppendEntriesRequest,
    ) {
        if sender != request.leader_id {
            warn!(
                "Node {} received AppendEntries from {}, but leader is {}",
                self.id, sender, request.leader_id
            );
            return;
        }

        // 只在接收到日志条目时记录，心跳不记录
        if !request.entries.is_empty() {
            info!(
                "Node {} received {} entries from {} (term {}, prev_log_index {})",
                self.id,
                request.entries.len(),
                request.leader_id,
                request.term,
                request.prev_log_index
            );
        }

        let mut success;
        let mut conflict_index = None;
        let mut conflict_term = None;
        let mut matched_index = self.get_last_log_index(); // 初始值：当前的最后日志索引

        // --- 1. 处理更低任期的请求 ---
        if request.term < self.current_term {
            warn!(
                "Node {} rejecting AppendEntries from {} (term {}) - local term is {}",
                self.id, request.leader_id, request.term, self.current_term
            );
            // conflict_term 计算，使用 error_handler
            conflict_term = if request.prev_log_index <= self.last_snapshot_index {
                Some(self.last_snapshot_term)
            } else {
                self.error_handler
                    .handle(
                        self.callbacks
                            .get_log_term(&self.id, request.prev_log_index)
                            .await,
                        "get_log_term",
                        Some(&request.leader_id), // 添加来源信息到错误日志
                    )
                    .await
                // .ok() // 如果 handle 返回 None，conflict_term 就是 None
            };
            // conflict_index 通常设为 follower 的 last_log_index + 1，表示 follower 期望接收的下一个日志索引
            conflict_index = Some(self.get_last_log_index() + 1);

            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index,
                conflict_term,
                request_id: request.request_id,
                matched_index,
            };

            info!(
                "Node {} sending rejection response to {} with higher term {} (request term was {})",
                self.id, request.leader_id, self.current_term, request.term
            );

            let _ = self
                .error_handler // 使用 error_handler 处理发送响应的错误
                .handle(
                    self.callbacks
                        .send_append_entries_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_append_entries_response",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // --- 2. 切换为Follower并重置状态 ---
        if self.role != Role::Follower || self.leader_id.as_ref() != Some(&request.leader_id) {
            info!(
                "Node {} recognizing {} as leader for term {}",
                self.id, request.leader_id, request.term
            );
        }

        let was_candidate = self.role == Role::Candidate;
        self.role = Role::Follower;
        self.leader_id = Some(request.leader_id.clone());

        // 如果收到的任期更高，或者任期相等但需要更新状态
        if request.term > self.current_term {
            info!(
                "Node {} updating term from {} to {}",
                self.id, self.current_term, request.term
            );
            self.current_term = request.term;
            self.voted_for = None; // 任期更新时重置投票
            // 持久化任期和投票状态
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
        } else if was_candidate && request.term == self.current_term {
            // 如果是Candidate在同一任期收到AppendEntries，说明有其他节点赢得了选举
            // 重置投票状态
            self.voted_for = None;
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
        }
        self.last_heartbeat = Instant::now();
        // 重置超时选举定时器
        self.reset_election().await;

        // --- 3. 日志连续性检查 ---
        let prev_log_ok = if request.prev_log_index == 0 {
            true // 从0开始的日志无需检查
        } else if request.prev_log_index <= self.last_snapshot_index {
            // 快照覆盖的日志，检查任期是否匹配快照
            request.prev_log_term == self.last_snapshot_term
        } else {
            // 检查日志任期是否匹配
            // 使用 error_handler 处理 get_log_term 错误
            let local_prev_log_term_result = self
                .error_handler
                .handle(
                    self.callbacks
                        .get_log_term(&self.id, request.prev_log_index)
                        .await,
                    "get_log_term",
                    Some(&request.leader_id),
                )
                .await;

            match local_prev_log_term_result {
                Some(local_term) => local_term == request.prev_log_term,
                None => {
                    // 获取本地日志任期失败，为安全起见，拒绝请求
                    warn!(
                        "Node {} failed to get local log term for index {} during consistency check, rejecting AppendEntries",
                        self.id, request.prev_log_index
                    );
                    false
                }
            }
        };

        if !prev_log_ok {
            warn!(
                "Node {} rejecting AppendEntries from {} - log inconsistency at index {} (leader: {}, local: {:?})",
                self.id,
                request.leader_id,
                request.prev_log_index,
                request.prev_log_term,
                if request.prev_log_index <= self.last_snapshot_index {
                    Some(self.last_snapshot_term)
                } else {
                    self.callbacks
                        .get_log_term(&self.id, request.prev_log_index)
                        .await
                        .ok()
                }
            );
            // 计算冲突索引和任期
            let local_last_log_index = self.get_last_log_index();
            conflict_index = if request.prev_log_index > local_last_log_index {
                // Leader 想要追加的日志索引超出了 Follower 的日志范围
                Some(local_last_log_index + 1)
            } else {
                // Leader 想要追加的日志索引在 Follower 日志范围内，但任期不匹配
                Some(request.prev_log_index)
            };

            conflict_term = if request.prev_log_index <= self.last_snapshot_index {
                Some(self.last_snapshot_term)
            } else {
                // 使用 error_handler 处理 get_log_term 错误
                self.error_handler
                    .handle(
                        self.callbacks
                            .get_log_term(&self.id, request.prev_log_index)
                            .await,
                        "get_log_term",
                        Some(&request.leader_id),
                    )
                    .await
                // .ok() // 如果 handle 返回 None，conflict_term 就是 None
            };

            let resp = AppendEntriesResponse {
                success: false,
                conflict_index,
                conflict_term,
                matched_index,
                term: self.current_term,
                request_id: request.request_id,
            };
            let _ = self
                .error_handler
                .handle(
                    self.callbacks
                        .send_append_entries_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_append_entries_response",
                    Some(&request.leader_id),
                )
                .await;
            return; // 直接返回，不执行后续步骤
        }

        success = true; // 假设后续步骤会成功
        // --- 4. 截断冲突日志并追加新日志 ---
        // 在截断之前，检查是否试图截断已提交的日志
        if request.prev_log_index < self.get_last_log_index() {
            // 计算要截断的日志起始索引
            let truncate_from_index = request.prev_log_index + 1;

            // *** 关键检查：不能截断已提交的日志 ***
            if truncate_from_index <= self.commit_index {
                warn!(
                    "Node {} rejecting AppendEntries from {} - attempt to truncate committed logs (truncate_from: {}, commit_index: {})",
                    self.id, request.leader_id, truncate_from_index, self.commit_index
                );
                // 因为日志连续性检查已经通过，但这里又发现要截断已提交日志，
                // 这表明请求本身有问题或状态异常。最安全的做法是拒绝请求。
                // 我们可以通过返回一个特定的 conflict_index 来指示 Leader 回退，
                // 或者简单地返回失败。这里选择返回失败。

                // conflict_index 和 conflict_term 的设置可以更精确，
                // 但返回失败是明确的。
                let resp = AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    conflict_index: Some(self.commit_index + 1), // 告诉 Leader 从 commit_index + 1 开始发送
                    conflict_term: None, // 或者可以尝试获取 commit_index 的任期
                    request_id: request.request_id,
                    matched_index: self.get_last_log_index(), // 如果有这个字段
                };
                let _ = self
                    .error_handler
                    .handle(
                        self.callbacks
                            .send_append_entries_response(&self.id, &request.leader_id, resp)
                            .await,
                        "send_append_entries_response",
                        Some(&request.leader_id),
                    )
                    .await;
                return; // 直接返回，不执行后续步骤
            }

            // 如果检查通过，则执行截断
            info!(
                "Node {} truncating log suffix from index {} (request_id: {:?}, leader: {}, prev_log_index: {}, entries_count: {})",
                self.id,
                truncate_from_index,
                request.request_id,
                request.leader_id,
                request.prev_log_index,
                request.entries.len()
            );
            let truncate_result = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .truncate_log_suffix(&self.id, truncate_from_index)
                        .await,
                    "truncate_log_suffix",
                    None,
                )
                .await;
            if !truncate_result {
                error!("Node {} failed to truncate log suffix", self.id);
                success = false;
                // 对于截断失败，通常没有特定的 conflict_index/term，Leader 会根据 next_index 回退
                // 这里可以不设置 conflict_index/term，或者设置为指示存储错误的值（如果协议支持）
            }
        }
        // --- 5. 验证并追加新日志 ---
        if success && !request.entries.is_empty() {
            // 验证日志条目连续性 (索引递增)
            if let Some(first_entry) = request.entries.first() {
                if first_entry.index != request.prev_log_index + 1 {
                    error!(
                        "Node {} rejecting AppendEntries - log entry index discontinuity: expected {}, got {}",
                        self.id,
                        request.prev_log_index + 1,
                        first_entry.index
                    );
                    success = false;
                    conflict_index = Some(request.prev_log_index + 1);
                    // conflict_term 对于索引不连续的情况通常不设置或设为 None
                    conflict_term = None;
                } else {
                    // 检查是否有试图覆盖已提交日志的客户端请求 (安全防护)
                    for entry in &request.entries {
                        if entry.client_request_id.is_some() && entry.index <= self.commit_index {
                            error!(
                                "Node {} rejecting AppendEntries - attempt to overwrite committed log entry {} with client request",
                                self.id, entry.index
                            );
                            success = false;
                            conflict_index = Some(entry.index);
                            conflict_term = Some(entry.term); // 提供冲突条目的任期
                            break; // 一旦发现冲突，立即停止检查并返回
                        }
                    }
                }
            }

            if success {
                info!(
                    "Node {} appending {} log entries starting from index {}",
                    self.id,
                    request.entries.len(),
                    request.prev_log_index + 1
                );
                let append_result = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .append_log_entries(&self.id, &request.entries)
                            .await,
                        "append_log_entries",
                        None,
                    )
                    .await;
                if !append_result {
                    error!("Node {} failed to append log entries", self.id);
                    success = false;
                    // 对于 append 失败，通常没有特定的 conflict_index/term，Leader 会根据 next_index 回退
                    // 这里可以不设置 conflict_index/term，或者设置为指示存储错误的值（如果协议支持）
                } else {
                    // 成功追加日志后，更新matched_index为最后一个追加的日志条目的索引
                    if let Some(last_entry) = request.entries.last() {
                        matched_index = last_entry.index;
                        // 更新 Follower 的 last_log_index 和 last_log_term
                        self.last_log_index = last_entry.index;
                        self.last_log_term = last_entry.term;
                        info!(
                            "Node {} successfully appended log entries, updated matched_index to {}, last_log_index to {}",
                            self.id, matched_index, self.last_log_index
                        );
                        // DEBUG: Check last log index after append
                        let after_append_last_index = self.get_last_log_index();
                        debug!(
                            "Node {} last_log_index after append: {}",
                            self.id, after_append_last_index
                        );
                    }
                }
            }
        }

        // --- 6. 更新提交索引 ---
        if success && request.leader_commit > self.commit_index {
            let new_commit_index = std::cmp::min(request.leader_commit, self.get_last_log_index());
            if new_commit_index > self.commit_index {
                info!(
                    "Node {} updating commit index from {} to {}",
                    self.id, self.commit_index, new_commit_index
                );
                self.commit_index = new_commit_index;
            }
        }

        // --- 7. 发送响应 ---
        // 注意：当前 AppendEntriesResponse 定义中没有 matched_index 字段
        let resp = AppendEntriesResponse {
            term: self.current_term,
            success,
            request_id: request.request_id,
            conflict_index: if success { None } else { conflict_index }, // 成功时通常不设置 conflict_index
            conflict_term: if success { None } else { conflict_term }, // 成功时通常不设置 conflict_term
            matched_index,                                             // 使用更新后的matched_index
        };
        let _send_result = self
            .error_handler
            .handle(
                self.callbacks
                    .send_append_entries_response(&self.id, &request.leader_id, resp)
                    .await,
                "send_append_entries_response",
                Some(&request.leader_id),
            )
            .await;
        // 可选：检查 send_result 并在失败时记录

        // --- 8. 应用已提交的日志 ---
        if success && self.commit_index > self.last_applied {
            debug!(
                "Node {} applying committed logs up to index {}",
                self.id, self.commit_index
            );
            self.apply_committed_logs().await;
        }
    }
    async fn handle_append_entries_response(
        &mut self,
        peer: RaftId,
        response: AppendEntriesResponse,
    ) {
        self.pipeline.record_append_entries_response_feedback(
            &peer,
            response.request_id,
            response.success,
        );

        // 非Leader角色不处理AppendEntries响应
        if self.role != Role::Leader {
            return;
        }

        debug!(
            "Leader {} received AppendEntries response from {}: term={}, success={}, current_term={}",
            self.id, peer, response.term, response.success, self.current_term
        );

        // 处理更高任期：当前Leader发现更高任期，降级为Follower
        if response.term > self.current_term {
            warn!(
                "Node {} stepping down to Follower, found higher term {} from {} (current term {})",
                self.id, response.term, peer, self.current_term
            );
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;

            // 持久化状态变更
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;

            // 通知状态变更
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks.state_changed(&self.id, Role::Follower).await,
                    "state_changed",
                    None,
                )
                .await;

            return;
        }

        // 仅处理当前任期的响应，忽略过期响应
        if response.term == self.current_term {
            debug!(
                "Leader {} received AppendEntries response from {}: req_id={}, success={}, matched_index={}",
                self.id, peer, response.request_id, response.success, response.matched_index
            );

            // 处理成功响应
            if response.success {
                let match_index = response.matched_index;

                // 更新match_index
                // 只有当新的 matched_index 更大时才更新
                let current_match = self.match_index.get(&peer).copied().unwrap_or(0);
                if match_index > current_match {
                    // matched_index 有真正的进展，更新它
                    self.match_index.insert(peer.clone(), match_index);

                    // next_index 应该至少是 matched_index + 1
                    // 但如果当前 next_index 更大（由于乐观更新），保持较大值
                    let current_next = self.next_index.get(&peer).copied().unwrap_or(1);
                    let min_next = match_index + 1;
                    if current_next < min_next {
                        // 当前 next_index 太小，需要更新
                        self.next_index.insert(peer.clone(), min_next);
                        info!(
                            "Node {} updated replication state for {}: next_index={} (corrected), match_index={}",
                            self.id, peer, min_next, match_index
                        );
                    } else {
                        // 保持当前的 next_index（可能是乐观更新的结果）
                        // info!(
                        //     "Node {} updated match_index for {}: match_index={} (next_index={} kept)",
                        //     self.id, peer, match_index, current_next
                        // );
                    }
                } else {
                    // 这是过期的响应，忽略所有更新
                    // info!(
                    //     "Node {} Ignoring stale response from {}: matched_index={} <= current={}",
                    //     self.id, peer, match_index, current_match
                    // );
                }

                // 尝试更新commit_index
                self.update_commit_index().await;
            } else {
                // 处理日志冲突
                warn!(
                    "Node {} received log conflict from {}: index={:?}, term={:?}",
                    self.id, peer, response.conflict_index, response.conflict_term
                );

                // 更新match_index（如果提供）
                if response.matched_index > 0 {
                    self.match_index
                        .insert(peer.clone(), response.matched_index);
                }

                // 计算新的next_index - 使用冲突优化策略
                let new_next = self.resolve_log_conflict(&peer, &response).await;
                self.next_index.insert(peer.clone(), new_next);

                info!(
                    "Node {} updated next_index for {} to {} (conflict resolution)",
                    self.id, peer, new_next
                );
            }

            // 检查领导权转移状态
            if let Some(transfer_target) = &self.leader_transfer_target {
                if &peer == transfer_target {
                    // 克隆response用于异步处理，避免借用冲突
                    self.process_leader_transfer_target_response(peer.clone(), response.clone())
                        .await;
                }
            }

            // 若当前处于联合配置，更新确认状态
            if response.success && self.config.is_joint() {
                debug!(
                    "Checking joint exit condition after successful replication to {}",
                    peer
                );
                self.check_joint_exit_condition().await;
            }
        }
    }
    /// 解析日志冲突并计算新的next_index
    /// 基于Raft优化策略：利用冲突信息减少日志同步的重试次数
    async fn resolve_log_conflict(&self, peer: &RaftId, response: &AppendEntriesResponse) -> u64 {
        if response.matched_index > 0 {
            // 有明确的匹配索引，直接使用它作为基准
            info!(
                "Using matched_index {} for peer {} conflict resolution",
                response.matched_index, peer
            );
            response.matched_index + 1
        } else if let Some(conflict_index) = response.conflict_index {
            // 尝试使用冲突任期优化
            if let Some(conflict_term) = response.conflict_term {
                info!(
                    "Optimizing with conflict_term: {} at index: {} for peer {}",
                    conflict_term, conflict_index, peer
                );

                // 从冲突索引开始向前查找，优先找到冲突任期的最后一个条目
                // 这是Raft协议推荐的优化策略，减少日志同步的重试次数
                let storage = &*self.callbacks;
                let start_index = conflict_index.min(self.last_log_index);
                let mut last_conflict_term_index = None;

                // 第一遍：查找冲突任期的最后一个索引
                for i in (1..=start_index).rev() {
                    match storage.get_log_entries_term(&self.id, i, i + 1).await {
                        Ok(entries) if !entries.is_empty() => {
                            info!(
                                "Checking index {} with term {} for peer {}",
                                i, entries[0].1, peer
                            );

                            if entries[0].1 == conflict_term {
                                // 记录冲突任期的最后一个索引
                                last_conflict_term_index = Some(i);
                                break; // 反向遍历，找到第一个即最后一个
                            } else if entries[0].1 < conflict_term {
                                // 任期小于冲突任期，无需继续查找
                                break;
                            }
                            // 任期大于冲突任期，继续向前查找是否有冲突任期的条目
                        }
                        Ok(_entries) => {
                            info!("Index {} returned empty entries for peer {}", i, peer);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to get log entry at index {} for peer {}: {:?}",
                                i, peer, e
                            );
                            // 日志查询失败，使用保守策略
                            return conflict_index.max(1);
                        }
                    }
                }

                // 如果找到冲突任期的条目，使用其索引+1
                if let Some(index) = last_conflict_term_index {
                    info!(
                        "Found matching term {} at index {} for peer {}",
                        conflict_term, index, peer
                    );
                    return index + 1;
                }

                // 第二遍：查找第一个任期大于冲突任期的索引
                for i in (1..=start_index).rev() {
                    match storage.get_log_entries(&self.id, i, i + 1).await {
                        Ok(entries) if !entries.is_empty() => {
                            if entries[0].term > conflict_term {
                                info!(
                                    "Found optimized index {} for peer {} conflict resolution",
                                    i, peer
                                );
                                return i;
                            }
                        }
                        Ok(_entries) => continue,
                        Err(e) => {
                            error!(
                                "Failed to get log entry at index {} for peer {}: {:?}",
                                i, peer, e
                            );
                            return conflict_index.max(1);
                        }
                    }
                }

                // 如果遍历完所有日志都没有找到更大的任期，使用冲突索引
                conflict_index.max(1)
            } else {
                info!(
                    "Using conflict_index {} without term for peer {}",
                    conflict_index, peer
                );
                conflict_index.max(1)
            }
        } else {
            // 最后的保守回退：当前next_index - 1
            info!(
                "Using conservative fallback for peer {} conflict resolution",
                peer
            );
            self.next_index
                .get(peer)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1)
                .max(1)
        }
    }

    /// 处理目标节点的日志响应
    async fn process_leader_transfer_target_response(
        &mut self,
        peer: RaftId,
        reply: AppendEntriesResponse,
    ) {
        if reply.success {
            // 检查目标节点日志是否最新
            let target_match_index = self.match_index.get(&peer).copied().unwrap_or(0);
            let last_log_index = self.get_last_log_index();

            if target_match_index >= last_log_index {
                // 目标节点日志最新，可以转移领导权
                self.transfer_leadership_to(peer).await;
                return;
            }
        }

        // 目标节点日志不够新，需要发送更多日志
        self.send_heartbeat_to(peer).await;
    }

    /// 转移领导权给目标节点
    async fn transfer_leadership_to(&mut self, target: RaftId) {
        // 发送超时投票请求，让目标节点立即开始选举
        let req = RequestVoteRequest {
            term: self.current_term + 1, // 使用更高任期触发选举
            candidate_id: target.clone(),
            last_log_index: self.get_last_log_index(),
            last_log_term: self.get_last_log_term(),
            request_id: RequestId::new(),
        };

        if let Err(err) = self
            .callbacks
            .send_request_vote_request(&self.id, &target, req)
            .await
        {
            log::error!(
                "Failed to send request vote request to {} during leader transfer: {}",
                target,
                err
            );
        }

        // 完成转移，响应客户端
        if let Some(request_id) = self.leader_transfer_request_id.take() {
            if let Err(err) = self
                .callbacks
                .client_response(
                    &self.id,
                    request_id,
                    Ok(0), // 成功代码
                )
                .await
            {
                log::error!(
                    "Failed to send client response for leader transfer completion: {}",
                    err
                );
            }
        }

        // 清理转移状态
        self.leader_transfer_target = None;
        self.leader_transfer_start_time = None;
    }

    // === 快照相关逻辑 ===
    async fn send_snapshot_to(&mut self, target: RaftId) {
        let snap = match self.callbacks.load_snapshot(&self.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("没有可用的快照，无法发送");
                return;
            }
            Err(e) => {
                error!("加载快照失败: {}", e);
                return;
            }
        };

        // 验证快照与当前日志的一致性
        if !self.verify_snapshot_consistency(&snap).await {
            error!("快照与当前日志不一致，无法发送");
            return;
        }

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: snap.index,
            last_included_term: snap.term,
            data: snap.data.clone(),
            config: snap.config.clone(), // 包含快照中的配置信息
            request_id: RequestId::new(),
            is_probe: false,
        };

        // 记录发送的快照信息
        self.follower_last_snapshot_index
            .insert(target.clone(), snap.index);
        self.follower_snapshot_states
            .insert(target.clone(), InstallSnapshotState::Installing);

        // 为这个Follower创建探测计划
        self.schedule_snapshot_probe(
            target.clone(),
            self.schedule_snapshot_probe_interval,
            self.schedule_snapshot_probe_retries,
        );

        if let Err(err) = self
            .callbacks
            .send_install_snapshot_request(&self.id, &target, req)
            .await
        {
            log::error!(
                "node {}: failed to send InstallSnapshotRequest: {}",
                self.id,
                err
            );
        }
    }

    // 验证快照与当前日志的一致性
    async fn verify_snapshot_consistency(&self, snap: &Snapshot) -> bool {
        // 检查快照的最后一条日志是否与当前日志匹配
        if snap.index == 0 {
            return true; // 空快照总是有效的
        }

        // 检查快照索引是否小于等于最后一条日志索引
        let last_log_index = self.get_last_log_index();
        if snap.index > last_log_index {
            return false;
        }

        // 检查快照的任期是否与对应日志的任期匹配
        let log_term = if snap.index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            match self.callbacks.get_log_term(&self.id, snap.index).await {
                Ok(term) => term,
                Err(_) => return false,
            }
        };

        snap.term == log_term
    }

    // 发送探测消息检查快照安装状态（Leader端）
    async fn probe_snapshot_status(&mut self, target: &RaftId) {
        info!(
            "Probing snapshot status for follower {} at term {}, last_snapshot_index {}",
            target,
            self.current_term,
            self.follower_last_snapshot_index.get(target).unwrap_or(&0)
        );

        let last_snap_index = self
            .follower_last_snapshot_index
            .get(target)
            .copied()
            .unwrap_or(0);

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: last_snap_index,
            last_included_term: 0,       // 探测消息不需要实际任期
            data: vec![],                // 空数据
            config: self.config.clone(), // 探测请求使用当前配置
            request_id: RequestId::new(),
            is_probe: true,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_install_snapshot_request(&self.id, target, req)
                    .await,
                "send_install_snapshot_request",
                Some(target),
            )
            .await;
    }

    async fn handle_install_snapshot(&mut self, sender: RaftId, request: InstallSnapshotRequest) {
        if sender != request.leader_id {
            warn!(
                "Node {} received InstallSnapshot from {}, but leader is {}",
                self.id, sender, request.leader_id
            );
            return;
        }
        // 处理更低任期的请求
        if request.term < self.current_term {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed("Term too low".into()),
                error_message: "Term too low".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        if self.leader_id.is_some() && self.leader_id.clone().unwrap() != request.leader_id {
            warn!(
                "Node {} received InstallSnapshot, old leader is {} , new leader is {}",
                self.id,
                self.leader_id.clone().unwrap(),
                request.leader_id,
            );
        }

        // 切换为Follower并更新状态
        self.leader_id = Some(request.leader_id.clone());
        self.role = Role::Follower;
        self.current_term = request.term;
        self.last_heartbeat = Instant::now();

        // 重置选举超时
        self.reset_election().await;

        // 处理空探测消息
        if request.is_probe {
            // 返回当前快照安装状态
            let current_state = if let Some(req_id) = &self.current_snapshot_request_id {
                // 如果是正在处理的那个快照请求
                if *req_id == request.request_id {
                    InstallSnapshotState::Installing
                } else {
                    InstallSnapshotState::Failed("No such snapshot in progress".into())
                }
            } else {
                // 没有正在处理的快照
                InstallSnapshotState::Success
            };

            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: current_state,
                error_message: "".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 仅处理比当前快照更新的快照
        if request.last_included_index <= self.last_snapshot_index {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Success,
                error_message: "".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 验证快照配置与当前配置的兼容性
        if !self.verify_snapshot_config_compatibility(&request).await {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed(
                    "Snapshot config incompatible with current config".into(),
                ),
                error_message: "Snapshot config incompatible with current config".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 记录当前正在处理的快照请求
        self.current_snapshot_request_id = Some(request.request_id);

        // 立即返回正在安装状态，不等待实际处理完成
        let resp = InstallSnapshotResponse {
            term: self.current_term,
            request_id: request.request_id,
            state: InstallSnapshotState::Installing,
            error_message: "".into(),
        };
        self.error_handler
            .handle_void(
                self.callbacks
                    .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                    .await,
                "send_install_snapshot_reply",
                Some(&request.leader_id),
            )
            .await;

        // 将快照数据交给业务层处理（异步）
        // 注意：这里不阻塞Raft状态机，实际处理由业务层完成
        self.error_handler
            .handle_void(
                self.callbacks
                    .process_snapshot(
                        &self.id,
                        request.last_included_index,
                        request.last_included_term,
                        request.data,
                        request.config.clone(), // 传递配置信息
                        request.request_id,
                    )
                    .await,
                "process_snapshot",
                None,
            )
            .await;
    }

    async fn verify_snapshot_config_compatibility(&self, _req: &InstallSnapshotRequest) -> bool {
        // Todo: 验证快照配置与当前配置的兼容性
        // 检查快照配置是否与当前配置兼容
        // 简单检查：快照中的配置应该是当前配置的祖先或相同
        // 实际实现可能需要更复杂的检查逻辑
        return true;
    }

    // 业务层完成快照处理后调用此方法更新状态（Follower端）
    pub async fn complete_snapshot_installation(
        &mut self,
        request_id: RequestId,
        success: bool,
        _reason: Option<String>,
        index: u64,
        term: u64,
        config: Option<ClusterConfig>,
    ) {
        // 检查是否是当前正在处理的快照
        if self.current_snapshot_request_id != Some(request_id) {
            return;
        }

        // 更新快照状态
        if success {
            // 对于快照安装，我们信任传入的index和term参数
            // 快照安装意味着用快照替换所有小于等于index的日志条目
            self.last_snapshot_index = index;
            self.last_snapshot_term = term;
            self.commit_index = self.commit_index.max(index);
            self.last_applied = self.last_applied.max(index);

            // 应用快照中的配置信息到当前RaftState
            if let Some(snapshot_config) = config {
                info!(
                    "Node {} applying snapshot config: old_config={:?}, new_config={:?}",
                    self.id, self.config, snapshot_config
                );
                self.config = snapshot_config;

                // 如果节点在新配置中不是voter，且当前是leader或candidate，需要转为follower
                if !self.config.voters_contains(&self.id)
                    && (self.role == Role::Leader || self.role == Role::Candidate)
                {
                    warn!(
                        "Node {} is no longer a voter in snapshot config, stepping down to follower",
                        self.id
                    );
                    self.role = Role::Follower;
                    self.voted_for = None;
                    self.leader_id = None;

                    // 持久化状态变更
                    let _ = self
                        .error_handler
                        .handle_void(
                            self.callbacks
                                .save_hard_state(
                                    &self.id,
                                    HardState {
                                        raft_id: self.id.clone(),
                                        term: self.current_term,
                                        voted_for: self.voted_for.clone(),
                                    },
                                )
                                .await,
                            "save_hard_state",
                            None,
                        )
                        .await;

                    // 通知状态变更
                    let _ = self
                        .error_handler
                        .handle_void(
                            self.callbacks.state_changed(&self.id, self.role).await,
                            "state_changed",
                            None,
                        )
                        .await;
                }

                // 持久化新的配置
                let _ = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .save_cluster_config(&self.id, self.config.clone())
                            .await,
                        "save_cluster_config",
                        None,
                    )
                    .await;
            }
        }

        // 清除当前处理标记
        self.current_snapshot_request_id = None;
    }

    async fn handle_install_snapshot_response(
        &mut self,
        peer: RaftId,
        response: InstallSnapshotResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }

        // 处理更高任期
        if response.term > self.current_term {
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
            self.error_handler
                .handle_void(
                    self.callbacks.state_changed(&self.id, Role::Follower).await,
                    "state_changed",
                    None,
                )
                .await;
            // 清除该节点的探测计划
            self.remove_snapshot_probe(&peer);
            return;
        }

        // 更新Follower的快照状态
        self.follower_snapshot_states
            .insert(peer.clone(), response.state.clone());

        match response.state {
            InstallSnapshotState::Success => {
                // 快照安装成功，更新复制状态
                let snap_index = self
                    .follower_last_snapshot_index
                    .get(&peer)
                    .copied()
                    .unwrap_or(0);
                self.next_index.insert(peer.clone(), snap_index + 1);
                self.match_index.insert(peer.clone(), snap_index);
                info!("Follower {} completed snapshot installation", peer);
                // 清除探测计划
                self.remove_snapshot_probe(&peer);
            }
            InstallSnapshotState::Installing => {
                // 仍在安装中，更新探测计划（延长尝试次数）
                info!("Follower {} is still installing snapshot", peer);
                self.extend_snapshot_probe(&peer);
            }
            InstallSnapshotState::Failed(reason) => {
                warn!("Follower {} snapshot install failed: {}", peer, reason);
                // 清除探测计划
                self.remove_snapshot_probe(&peer);
                // 可以安排重试
                self.schedule_snapshot_retry(peer).await;
            }
        }
    }

    // 安排快照状态探测（无内部线程）
    fn schedule_snapshot_probe(&mut self, peer: RaftId, interval: Duration, max_attempts: u32) {
        // 先移除可能存在的旧计划
        self.remove_snapshot_probe(&peer);

        // 添加新的探测计划
        self.snapshot_probe_schedules.push(SnapshotProbeSchedule {
            peer: peer.clone(),
            next_probe_time: Instant::now() + interval,
            interval,
            max_attempts,
            attempts: 0,
        });
    }

    // 延长快照探测计划
    fn extend_snapshot_probe(&mut self, peer: &RaftId) {
        if let Some(schedule) = self
            .snapshot_probe_schedules
            .iter_mut()
            .find(|s| &s.peer == peer)
        {
            schedule.attempts += 1;

            if schedule.attempts >= schedule.max_attempts {
                // 达到最大尝试次数，标记为失败
                self.follower_snapshot_states.insert(
                    peer.clone(),
                    InstallSnapshotState::Failed("Max probe attempts reached".into()),
                );
                self.remove_snapshot_probe(peer);
            } else {
                // 还没达到最大尝试次数，计划下次探测
                schedule.next_probe_time = Instant::now() + schedule.interval;
            }
        }
    }

    // 移除快照探测计划
    fn remove_snapshot_probe(&mut self, peer: &RaftId) {
        self.snapshot_probe_schedules.retain(|s| &s.peer != peer);
    }

    // 处理到期的探测计划
    async fn process_pending_probes(&mut self, now: Instant) {
        // 收集需要执行的探测
        let pending_peers: Vec<RaftId> = self
            .snapshot_probe_schedules
            .iter()
            .filter(|s| s.next_probe_time <= now)
            .map(|s| s.peer.clone())
            .collect();

        // 执行每个到期的探测
        for peer in pending_peers {
            self.probe_snapshot_status(&peer).await;
            // 更新探测计划状态
            self.extend_snapshot_probe(&peer);
        }
    }

    // 安排快照重发（无内部线程）
    async fn schedule_snapshot_retry(&mut self, peer: RaftId) {
        // 直接在当前事件循环中延迟发送，而非启动新线程
        // 实际实现中可根据需要调整重试延迟
        self.send_snapshot_to(peer).await;
    }

    // === 客户端请求与日志应用 ===
    async fn handle_client_propose(&mut self, cmd: Command, request_id: RequestId) {
        info!(
            "Node {} handling ClientPropose with request_id={:?}, role={:?}",
            self.id, request_id, self.role
        );

        if self.role != Role::Leader {
            warn!(
                "Node {} rejecting ClientPropose (not leader, current role: {:?}, leader: {:?})",
                self.id, self.role, self.leader_id
            );
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 如果已经提交，直接返回结果
        if let Some(&index) = self.client_requests.get(&request_id) {
            if index <= self.commit_index {
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(&self.id, request_id, Ok(index))
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                return;
            }
            // 否则等待已存在的日志提交
            return;
        }

        // 生成日志条目
        let index = self.get_last_log_index() + 1;
        let new_entry = LogEntry {
            term: self.current_term,
            index: index,
            command: cmd,
            is_config: false,                    // 普通命令
            client_request_id: Some(request_id), // 关联客户端请求ID
        };

        // 追加日志
        info!(
            "Node {} (Leader) appending log entry at index {} for request_id={:?}",
            self.id, index, request_id
        );
        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, &[new_entry.clone()])
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            error!(
                "Node {} failed to append log entry for request_id={:?}",
                self.id, request_id
            );
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        info!(
            "Node {} successfully appended log entry at index {}, broadcasting to followers",
            self.id, index
        );
        self.last_log_index = index;
        self.last_log_term = self.current_term;

        // 记录客户端请求与日志索引的映射
        self.client_requests.insert(request_id, index);
        self.client_requests_revert.insert(index, request_id);

        // 立即同步日志
        info!(
            "Node {} broadcasting append entries for log index {}",
            self.id, index
        );
        self.broadcast_append_entries().await;
    }

    async fn apply_committed_logs(&mut self) {
        // 应用已提交但未应用的日志
        if self.last_applied >= self.commit_index {
            debug!(
                "Node {} apply_committed_logs: nothing to apply (last_applied={}, commit_index={})",
                self.id, self.last_applied, self.commit_index
            );
            return;
        }

        let start = self.last_applied + 1;
        let end = std::cmp::min(
            self.commit_index,
            self.last_applied + self.options.apply_batch_size,
        );

        // 只在有大量日志要应用时记录
        if end - start + 1 > 5 {
            info!(
                "Node {} applying {} logs from {} to {}",
                self.id,
                end - start + 1,
                start,
                end
            );
        }

        let entries = match self
            .callbacks
            .get_log_entries(&self.id, start, end + 1)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                error!("读取日志失败: {}", e);
                return;
            }
        };

        // 逐个应用日志
        let mut i = 0;
        for entry in entries {
            let expected_index = start + i as u64;

            if entry.index != expected_index {
                error!(
                    "Log index discontinuous: expected {}, got {}",
                    expected_index, entry.index
                );
                break; // 中断应用，避免后续日志顺序错误
            }
            i += 1;
            // 验证日志尚未被应用且已提交
            if entry.index <= self.last_applied || entry.index > self.commit_index {
                continue;
            }
            if entry.is_config {
                if let Ok(new_config) = serde_json::from_slice::<ClusterConfig>(&entry.command) {
                    // 使用增强的配置变更安全验证
                    if let Err(validation_error) =
                        self.validate_config_change_safety(&new_config, entry.index)
                    {
                        error!(
                            "Configuration change validation failed for entry {}: {}. Config: {:?}",
                            entry.index, validation_error, new_config
                        );
                        // 验证失败时，仍需更新last_applied以保持日志应用连续性
                        self.last_applied = entry.index;
                        continue;
                    }

                    // 防止Leader配置回滚：检查配置版本单调性
                    if self.config.log_index() > new_config.log_index() {
                        warn!(
                            "Node {} received outdated cluster config (current: {}, received: {}), ignoring to prevent rollback: {:?}",
                            self.id,
                            self.config.log_index(),
                            new_config.log_index(),
                            new_config
                        );
                        // 即使忽略配置，也需要更新last_applied以保持日志应用的连续性
                        self.last_applied = entry.index;

                        // 对于Leader，记录潜在的配置冲突警告
                        if self.role == Role::Leader {
                            error!(
                                "Leader {} detected configuration rollback attempt - this may indicate a serious issue with log replication or configuration management",
                                self.id
                            );
                        }
                        continue;
                    } else {
                        info!(
                            "Node {} applying new cluster config: {:?}",
                            self.id, new_config
                        );

                        // 在应用新配置前，检查是否有节点被移除
                        let old_config = &self.config;

                        // 检查 learner 删除
                        let old_learners = old_config.learners().cloned().unwrap_or_default();
                        let new_learners = new_config.learners().cloned().unwrap_or_default();
                        let removed_learners: Vec<RaftId> =
                            old_learners.difference(&new_learners).cloned().collect();

                        // 检查 voter 删除
                        let old_voters = old_config
                            .get_effective_voters()
                            .iter()
                            .cloned()
                            .collect::<HashSet<_>>();
                        let new_voters = new_config
                            .get_effective_voters()
                            .iter()
                            .cloned()
                            .collect::<HashSet<_>>();
                        let removed_voters: Vec<RaftId> =
                            old_voters.difference(&new_voters).cloned().collect();

                        // 检查当前节点是否被删除（voter 或 learner）
                        let current_node_removed = removed_voters.contains(&self.id)
                            || removed_learners.contains(&self.id);

                        // 应用新配置
                        self.config = new_config;

                        // 清理被移除的 learner 的复制状态（仅 Leader）
                        if self.role == Role::Leader {
                            for removed_learner in &removed_learners {
                                self.cleanup_learner_replication_state(removed_learner);
                                info!(
                                    "Cleaned up replication state for removed learner: {}",
                                    removed_learner
                                );
                            }

                            // 清理被移除的 voter 的复制状态
                            for removed_voter in &removed_voters {
                                self.next_index.remove(removed_voter);
                                self.match_index.remove(removed_voter);
                                info!(
                                    "Cleaned up replication state for removed voter: {}",
                                    removed_voter
                                );
                            }
                        }

                        // 如果当前节点被删除，准备优雅退出
                        if current_node_removed {
                            warn!(
                                "Current node {} has been removed from cluster, initiating graceful shutdown",
                                self.id
                            );

                            // 清理定时器
                            if let Some(timer_id) = self.election_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.heartbeat_interval_timer_id.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.apply_interval_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.config_change_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.leader_transfer_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }

                            // 通知业务层节点已被删除
                            let _ = self
                                .error_handler
                                .handle_void(
                                    self.callbacks.node_removed(&self.id).await,
                                    "node_removed",
                                    None,
                                )
                                .await;

                            // 注意：这里不直接停止Raft状态机，由业务层决定如何处理
                            // 业务层可能需要完成一些清理工作后再停止节点
                        }
                    }

                    let success = self
                        .error_handler
                        .handle_void(
                            self.callbacks
                                .save_cluster_config(&self.id, self.config.clone())
                                .await,
                            "save_cluster_config",
                            None,
                        )
                        .await;
                    if success {
                        info!(
                            "Node {} updated cluster config to: {:?}",
                            self.id, self.config
                        );
                    } else {
                        error!("Failed to save new cluster config");
                        return;
                    }

                    // 检查是否可以退出联合配置
                    if self.config.is_joint() {
                        self.check_joint_exit_condition().await;
                    }

                    // 重要：更新 last_applied 索引以避免日志索引不连续
                    self.last_applied = entry.index;
                    info!(
                        "Node {} updated last_applied to {} after config change",
                        self.id, self.last_applied
                    );
                }
            } else {
                let index = entry.index;
                debug!(
                    "Node {} applying command to state machine: index={}, term={}",
                    self.id, entry.index, entry.term
                );
                let _ = self
                    .callbacks
                    .apply_command(&self.id, entry.index, entry.term, entry.command)
                    .await;
                self.last_applied = index;

                // 如果是客户端请求，返回响应
                self.check_client_response(index).await;
            }
        }

        // 继续定时应用
        self.adjust_apply_interval().await;
    }

    async fn adjust_apply_interval(&mut self) {
        // 根据负载动态调整应用间隔
        let current_load = self.commit_index - self.last_applied;

        self.apply_interval = if current_load > 1000 {
            Duration::from_micros(100) // 高负载时更频繁
        } else {
            Duration::from_millis(10) // 低负载时减少频率
        };

        if let Some(timer) = self.apply_interval_timer {
            self.callbacks.del_timer(&self.id, timer);
        }

        self.apply_interval_timer = Some(
            self.callbacks
                .set_apply_timer(&self.id, self.apply_interval),
        );
    }

    async fn check_client_response(&mut self, log_index: u64) {
        // 查找该日志索引对应的客户端请求并响应
        let mut completed = vec![];
        for (req_id, idx) in &self.client_requests {
            if *idx == log_index {
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(&self.id, *req_id, Ok(log_index))
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                completed.push(*req_id);
            }
        }
        // 移除已响应的请求
        for req_id in completed {
            self.client_requests.remove(&req_id);
        }
    }

    // === 集群配置变更 ===
    async fn handle_change_config(&mut self, new_voters: HashSet<RaftId>, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 检查是否已有配置变更在进行中
        if self.config_change_in_progress {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Conflict(anyhow::anyhow!(
                                "config change in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;
        // 验证新配置的合法性
        let new_config = ClusterConfig::simple(new_voters.clone(), index);
        if !new_config.is_valid() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "invalid cluster config"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 创建联合配置
        let old_voters = self.config.get_effective_voters().clone();
        let mut joint_config = self.config.clone();
        if let Err(e) = joint_config.enter_joint(old_voters, new_voters, None, None, index) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "Failed to enter joint config: {}",
                                e
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 生成配置变更日志
        let config_data = match serde_json::to_vec(&joint_config) {
            Ok(data) => data,
            Err(e) => {
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(
                                &self.id,
                                request_id,
                                Err(ClientError::Internal(anyhow::anyhow!(
                                    "failed to serialize config: {}",
                                    e
                                ))),
                            )
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                return;
            }
        };

        let new_entry = LogEntry {
            term: self.current_term,
            index: index,
            command: config_data,
            is_config: true,
            client_request_id: Some(request_id),
        };

        // 追加配置变更日志
        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, &[new_entry.clone()])
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append config log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录配置变更状态
        self.config = joint_config;
        self.joint_config_log_index = index;
        self.config_change_in_progress = true;
        self.config_change_start_time = Some(Instant::now());
        self.client_requests.insert(request_id, last_idx + 1);

        // 更新 Leader 的本地日志状态
        self.last_log_index = index;
        self.last_log_term = self.current_term;

        assert!(
            self.is_leader_joint_config(),
            "Leader should be in joint config after initiating config change"
        );

        // 设置配置变更超时定时器
        self.config_change_timer = Some(
            self.callbacks
                .set_config_change_timer(&self.id, self.config_change_timeout),
        );

        // 立即同步日志
        self.broadcast_append_entries().await;
    }

    async fn handle_config_change_timeout(&mut self) {
        // 前置条件校验：仅Leader在联合配置变更过程中执行
        if !self.is_leader_joint_config() {
            warn!("Not in joint config or not leader, cannot handle config change timeout");
            return;
        }

        // 检查是否超时
        let start_time = match self.config_change_start_time {
            Some(t) => t,
            None => {
                self.config_change_in_progress = false;
                return;
            }
        };

        if start_time.elapsed() < self.config_change_timeout {
            // 未超时，重新设置定时器
            self.config_change_timer = Some(
                self.callbacks
                    .set_config_change_timer(&self.id, self.config_change_timeout),
            );

            return;
        }

        self.check_joint_exit_condition().await;
    }

    // 检查联合配置退出条件
    async fn check_joint_exit_condition(&mut self) {
        // 前置条件校验：仅Leader在联合配置变更过程中执行
        if !self.is_leader_joint_config() {
            return;
        }

        // 如果配置变更日志已经被提交，直接退出联合配置
        if self.joint_config_log_index <= self.commit_index {
            info!(
                "exiting joint config as it is committed config log index {} committed index {}",
                self.joint_config_log_index, self.commit_index
            );
            self.exit_joint_config(false).await;
            return;
        }

        // 配置变更日志还没有被提交，检查是否超时
        let enter_time = match self.config_change_start_time {
            Some(t) => t,
            None => {
                warn!("No config change start time recorded, clearing config change state");
                self.config_change_in_progress = false;
                return;
            }
        };

        if enter_time.elapsed() >= self.config_change_timeout {
            // 超时了，进行超时处理（可能回滚）
            self.check_joint_timeout().await;
        } else {
            // 没有超时，继续等待日志复制，但可以主动推进
            self.try_advance_joint_config().await;
        }
    }

    async fn try_advance_joint_config(&mut self) {
        // 尝试推进联合配置的日志复制
        debug!(
            "Trying to advance joint config: joint_index={}, commit_index={}",
            self.joint_config_log_index, self.commit_index
        );

        // 主动发送心跳来推进日志复制
        self.broadcast_append_entries().await;
    }

    fn is_leader_joint_config(&self) -> bool {
        self.role == Role::Leader
            && self.config.is_joint()
            && self.config_change_in_progress
            && self.config_change_start_time.is_some()
            && self.config_change_timeout > Duration::ZERO
    }

    async fn check_joint_timeout(&mut self) {
        // 前置条件校验：仅Leader在联合配置变更过程中且记录了开始时间时执行
        if !self.is_leader_joint_config() {
            return;
        }

        // 这个函数只在确实超时时被调用，所以直接进行超时处理
        warn!(
            "Joint config timeout reached (elapsed: {:?}, timeout: {:?}), checking for rollback",
            self.config_change_start_time.unwrap().elapsed(),
            self.config_change_timeout
        );

        // 安全获取联合配置（避免unwrap恐慌）
        let joint = match self.config.joint() {
            Some(j) => j,
            None => {
                error!("Joint config missing during timeout check");
                return;
            }
        };

        // 获取新旧配置的多数派阈值
        let (old_quorum, new_quorum) = match self.config.quorum() {
            QuorumRequirement::Simple(_) => {
                error!("Unexpected simple quorum in joint config");
                return; // 替换panic为安全退出
            }
            QuorumRequirement::Joint { old, new } => (old, new),
        };

        // 计算新旧配置的多数派日志索引
        let new_majority_index = match self
            .find_majority_index(&joint.new_voters, new_quorum)
            .await
        {
            Some(idx) => idx,
            None => {
                warn!("Failed to find majority index for new config");
                0
            }
        };
        let old_majority_index = match self
            .find_majority_index(&joint.old_voters, old_quorum)
            .await
        {
            Some(idx) => idx,
            None => {
                warn!("Failed to find majority index for old config");
                0
            }
        };

        // 验证联合配置日志是否被多数派支持
        let joint_config_index = self.joint_config_log_index;
        if old_majority_index < joint_config_index && new_majority_index < joint_config_index {
            warn!(
                "Joint config index {} not supported by majority: old config got {}, new config got {}",
                joint_config_index, old_majority_index, new_majority_index
            );
            return; // 无法形成多数派，保持当前状态
        }

        // 回滚决策：新配置未达多数则回滚至旧配置
        let rollback = new_majority_index < joint_config_index;
        if rollback {
            // 回滚前二次验证旧配置有效性
            if old_majority_index >= joint_config_index {
                warn!(
                    "New config lacks majority (got index {}, needed >= {}), rolling back to old config",
                    new_majority_index, joint_config_index
                );
            } else {
                warn!("Both configs lack majority, forced rollback to old config as fallback");
            }
        } else {
            info!(
                "New config has majority (index {} >= {}), exiting to new config",
                new_majority_index, joint_config_index
            );
        }

        // 执行退出联合配置操作
        self.exit_joint_config(rollback).await;
    }

    async fn exit_joint_config(&mut self, rollback: bool) {
        // 前置条件校验：仅Leader在联合配置变更过程中执行
        if !self.is_leader_joint_config() {
            warn!("Not in joint config or not leader, cannot exit joint config");
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;

        // 生成目标配置（回滚至旧配置或正常切换至新配置）
        let new_config = if rollback {
            // 回滚到旧配置
            if let Some(joint) = self.config.joint() {
                ClusterConfig::simple(joint.old_voters.clone(), index)
            } else {
                warn!("Not in joint config, cannot rollback");
                return; // 不在联合配置中，无需操作
            }
        } else {
            // 正常退出到新配置
            match self.config.leave_joint(index) {
                Ok(config) => config,
                Err(e) => {
                    error!("Failed to leave joint config: {}", e);
                    return; // 退出失败，保持当前状态
                }
            }
        };

        // 验证新配置有效性（确保多数派可达）
        if !new_config.is_valid() {
            error!("Generated new config is invalid, aborting exit joint");
            return;
        }

        // 序列化新配置
        let config_data = match serde_json::to_vec(&new_config) {
            Ok(data) => data,
            Err(error) => {
                error!("Failed to serialize new config: {}", error);
                return;
            }
        };

        // 创建退出联合配置的日志条目
        let exit_entry = LogEntry {
            term: self.current_term,
            index: index,
            command: config_data,
            is_config: true,
            client_request_id: None,
        };

        // 追加日志并同步（检查追加结果，失败则终止）
        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, &[exit_entry])
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            error!("Failed to append exit joint log entry, aborting");
            return;
        }

        // 广播日志条目至集群
        self.broadcast_append_entries().await;

        // 更新 Leader 的本地日志状态
        self.last_log_index = index;
        self.last_log_term = self.current_term;

        // 更新本地配置（无论回滚还是正常退出，均与日志保持一致）
        // 注：严格遵循论文需等待日志提交后更新，此处为基础修复，完整实现需监听提交事件
        self.config = new_config;
        self.joint_config_log_index = 0; // 清除联合配置日志索引

        // 清理配置变更状态并持久化配置
        self.config_change_in_progress = false;
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .save_cluster_config(&self.id, self.config.clone())
                    .await,
                "save_cluster_config",
                None,
            )
            .await;

        info!(
            "Exited joint config (rollback: {}) to new config: {:?}",
            rollback, self.config
        );
    }

    // === Learner 管理方法 ===
    async fn handle_add_learner(&mut self, learner: RaftId, request_id: RequestId) {
        self.handle_learner_operation(learner, request_id, true)
            .await;
    }

    async fn handle_remove_learner(&mut self, learner: RaftId, request_id: RequestId) {
        self.handle_learner_operation(learner, request_id, false)
            .await;
    }

    async fn handle_learner_operation(
        &mut self,
        learner: RaftId,
        request_id: RequestId,
        is_add: bool,
    ) {
        // 1. 检查是否为 Leader
        if !self.validate_leader_for_config_change(request_id).await {
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;

        // 2. 验证 learner 操作的合法性（不修改配置）
        let mut temp_config = self.config.clone();
        let operation_result = if is_add {
            temp_config.add_learner(learner.clone(), index)
        } else {
            temp_config.remove_learner(&learner, index)
        };

        if let Err(e) = operation_result {
            self.send_client_error(
                request_id,
                ClientError::BadRequest(anyhow::anyhow!(
                    "{} learner failed: {}",
                    if is_add { "Add" } else { "Remove" },
                    e
                )),
            )
            .await;
            return;
        }

        // 3. 创建并追加配置变更日志（使用验证过的配置）
        if !self
            .create_and_append_config_log(&temp_config, index, request_id)
            .await
        {
            return;
        }

        // 4. 更新日志状态和客户端请求映射（但不更新配置）
        self.last_log_index = index;
        self.last_log_term = self.current_term;
        self.client_requests.insert(request_id, index);
        self.client_requests_revert.insert(index, request_id);

        // 5. 对于添加 learner，可以立即初始化复制状态开始同步
        // 对于移除 learner，必须等待日志提交后才能停止同步
        if is_add {
            self.initialize_learner_replication_state(&learner, index);
        }
        // 注意：移除 learner 的清理工作在 apply_committed_logs 中进行

        // 6. 广播日志
        self.broadcast_append_entries().await;

        info!(
            "Initiated {} learner {} (will take effect after commit)",
            if is_add { "add" } else { "remove" },
            learner
        );
    }

    // === 辅助方法 ===
    async fn validate_leader_for_config_change(&mut self, request_id: RequestId) -> bool {
        if self.role != Role::Leader {
            self.send_client_error(request_id, ClientError::NotLeader(self.leader_id.clone()))
                .await;
            return false;
        }

        if self.config_change_in_progress {
            self.send_client_error(
                request_id,
                ClientError::Conflict(anyhow::anyhow!("Configuration change in progress")),
            )
            .await;
            return false;
        }

        true
    }

    async fn send_client_error(&mut self, request_id: RequestId, error: ClientError) {
        self.error_handler
            .handle_void(
                self.callbacks
                    .client_response(&self.id, request_id, Err(error))
                    .await,
                "client_response",
                None,
            )
            .await;
    }

    async fn create_and_append_config_log(
        &mut self,
        config: &ClusterConfig,
        index: u64,
        request_id: RequestId,
    ) -> bool {
        // 序列化配置
        let config_data = match serde_json::to_vec(config) {
            Ok(data) => data,
            Err(e) => {
                self.send_client_error(
                    request_id,
                    ClientError::Internal(anyhow::anyhow!("Failed to serialize config: {}", e)),
                )
                .await;
                return false;
            }
        };

        // 创建日志条目
        let entry = LogEntry {
            term: self.current_term,
            index,
            command: config_data,
            is_config: true,
            client_request_id: Some(request_id),
        };

        // 追加日志
        if !self
            .error_handler
            .handle_void(
                self.callbacks.append_log_entries(&self.id, &[entry]).await,
                "append_log_entries",
                None,
            )
            .await
        {
            self.send_client_error(
                request_id,
                ClientError::Internal(anyhow::anyhow!("Failed to append log")),
            )
            .await;
            return false;
        }

        true
    }

    fn initialize_learner_replication_state(&mut self, learner: &RaftId, index: u64) {
        self.next_index.insert(learner.clone(), index + 1);
        self.match_index.insert(learner.clone(), 0);
    }

    fn cleanup_learner_replication_state(&mut self, learner: &RaftId) {
        self.next_index.remove(learner);
        self.match_index.remove(learner);
        self.follower_snapshot_states.remove(learner);
        self.follower_last_snapshot_index.remove(learner);
    }

    /// 验证配置变更的安全性，防止配置回滚
    fn validate_config_change_safety(
        &self,
        new_config: &ClusterConfig,
        entry_index: u64,
    ) -> Result<(), String> {
        // 1. 基本有效性检查
        if !new_config.is_valid() {
            return Err("Invalid cluster configuration".to_string());
        }

        // 2. 配置版本单调性检查：防止回滚到更旧的配置
        if new_config.log_index() < self.config.log_index() {
            return Err(format!(
                "Configuration rollback detected: current log_index={}, new log_index={}",
                self.config.log_index(),
                new_config.log_index()
            ));
        }

        // 3. 日志索引一致性检查：配置的log_index应该与日志条目索引匹配
        if new_config.log_index() != entry_index {
            return Err(format!(
                "Configuration log_index mismatch: config.log_index={}, entry.index={}",
                new_config.log_index(),
                entry_index
            ));
        }

        // 4. Leader额外检查：确保配置变更的合理性
        if self.role == Role::Leader {
            // Leader不应该收到来自未来的配置（这可能表明日志复制有问题）
            if new_config.log_index() > self.get_last_log_index() {
                return Err(format!(
                    "Leader received configuration from future: config.log_index={}, last_log_index={}",
                    new_config.log_index(),
                    self.get_last_log_index()
                ));
            }

            // 检查配置变更是否过于激进（一次删除太多节点可能危险）
            if let Some(removed_voters_count) = self.calculate_voter_removal_count(new_config) {
                let current_voter_count = self.config.get_effective_voters().len();
                if removed_voters_count > current_voter_count / 2 {
                    return Err(format!(
                        "Dangerous configuration change: removing {} voters from {} total voters",
                        removed_voters_count, current_voter_count
                    ));
                }
            }
        }

        Ok(())
    }

    /// 计算配置变更中被删除的voter数量
    fn calculate_voter_removal_count(&self, new_config: &ClusterConfig) -> Option<usize> {
        let current_voters: HashSet<_> =
            self.config.get_effective_voters().iter().cloned().collect();
        let new_voters: HashSet<_> = new_config.get_effective_voters().iter().cloned().collect();

        let removed_count = current_voters.difference(&new_voters).count();
        if removed_count > 0 {
            Some(removed_count)
        } else {
            None
        }
    }

    // === 辅助方法 ===
    fn get_effective_peers(&self) -> Vec<RaftId> {
        // 包含所有 voters 和 learners (排除自己)
        self.config
            .get_all_nodes()
            .into_iter()
            .filter(|id| *id != self.id)
            .collect()
    }

    fn get_last_log_index(&self) -> u64 {
        // The effective last log index is the maximum of the actual log index and the snapshot index
        std::cmp::max(self.last_log_index, self.last_snapshot_index)
    }

    fn get_last_log_term(&self) -> u64 {
        self.last_log_term
    }

    pub fn get_role(&self) -> Role {
        self.role
    }

    /// 获取当前InFlight请求总数
    pub fn get_inflight_request_count(&self) -> usize {
        self.pipeline.get_inflight_request_count()
    }

    /// 处理领导权转移请求
    pub async fn handle_leader_transfer(&mut self, target: RaftId, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if self.leader_transfer_target.is_some() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer already in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if !self.config.voters_contains(&target) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "target node not in cluster"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if target == self.id {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "cannot transfer to self"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录转移状态
        self.leader_transfer_target = Some(target.clone());
        self.leader_transfer_request_id = Some(request_id);
        self.leader_transfer_start_time = Some(Instant::now());

        // 立即发送心跳重置目标节点选举计时器
        self.send_heartbeat_to(target).await;

        // 设置超时定时器

        self.leader_transfer_timer = Some(
            self.callbacks
                .set_leader_transfer_timer(&self.id, self.leader_transfer_timeout),
        );
    }

    /// 发送心跳到特定节点
    async fn send_heartbeat_to(&mut self, target: RaftId) {
        let prev_log_index = self.next_index[&target] - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else if prev_log_index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            self.callbacks
                .get_log_term(&self.id, prev_log_index)
                .await
                .unwrap_or(0)
        };

        let req = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries: vec![], // 空日志表示心跳
            leader_commit: self.commit_index,
            request_id: RequestId::new(),
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_append_entries_request(&self.id, &target, req)
                    .await,
                "send_append_entries_request",
                None,
            )
            .await;
    }

    /// 生成快照并持久化
    async fn create_snapshot(&mut self) {
        let begin = Instant::now();

        // 1. 检查是否有需要快照的日志（避免重复快照）
        if self.commit_index <= self.last_snapshot_index {
            info!(
                "No new committed logs to snapshot (commit_index: {}, last_snapshot_index: {})",
                self.commit_index, self.last_snapshot_index
            );
            return;
        }

        // 2. 使用commit_index作为快照索引，确保只快照已提交的数据
        let snapshot_index = self.commit_index;
        let snapshot_term = if snapshot_index == 0 {
            0
        } else if snapshot_index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            match self.callbacks.get_log_term(&self.id, snapshot_index).await {
                Ok(term) => term,
                Err(e) => {
                    error!(
                        "Failed to get log term for snapshot at index {}: {}",
                        snapshot_index, e
                    );
                    return;
                }
            }
        };

        // 3. 获取当前集群配置（用于验证快照配置的兼容性）
        let _config = self.config.clone();

        // 4. 让业务层生成快照数据
        let (snap_index, snap_term) = match self
            .error_handler
            .handle(
                self.callbacks.create_snapshot(&self.id).await,
                "create_snapshot",
                None,
            )
            .await
        {
            Some((idx, term)) => (idx, term),
            None => {
                error!("Failed to create snapshot via callback");
                return;
            }
        };

        // 5. 验证返回的快照元数据
        if snap_index != snapshot_index {
            warn!(
                "Snapshot index mismatch: expected {}, got {}, using expected value",
                snapshot_index, snap_index
            );
        }
        if snap_term != snapshot_term {
            warn!(
                "Snapshot term mismatch: expected {}, got {}, using expected value",
                snapshot_term, snap_term
            );
        }

        // 6. 读取快照数据（业务层应已持久化）
        let snap = match self.callbacks.load_snapshot(&self.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("No snapshot found after creation");
                return;
            }
            Err(e) => {
                error!("Failed to load snapshot after creation: {}", e);
                return;
            }
        };

        // 7. 校验快照元数据（使用实际期望值而非回调返回值）
        if snap.index < snapshot_index {
            error!(
                "Snapshot index too old: expected >= {}, got {}",
                snapshot_index, snap.index
            );
            return;
        }

        // 允许快照包含比当前配置更新的信息，这在某些场景下是正常的
        info!(
            "Snapshot validation passed: index={}, term={}, config_valid={}",
            snap.index,
            snap.term,
            snap.config.is_valid()
        );

        // 8. 截断日志前缀（基于实际快照的索引）
        if snap.index > 0 {
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .truncate_log_prefix(&self.id, snap.index + 1)
                        .await,
                    "truncate_log_prefix",
                    None,
                )
                .await;
        }

        // 9. 更新本地状态（使用实际快照的元数据）
        self.last_snapshot_index = snap.index;
        self.last_snapshot_term = snap.term;
        self.last_applied = self.last_applied.max(snap.index);
        self.commit_index = self.commit_index.max(snap.index);

        let elapsed = begin.elapsed();
        info!(
            "Snapshot created at index {}, term {} (elapsed: {:?})",
            snap.index, snap.term, elapsed
        );
    }

    /// 处理领导权转移超时
    async fn handle_leader_transfer_timeout(&mut self) {
        if let (Some(_target), Some(request_id)) = (
            self.leader_transfer_target.take(),
            self.leader_transfer_request_id.take(),
        ) {
            self.leader_transfer_start_time = None;

            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer timeout"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
        }
    }

    async fn update_commit_index(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        // 获取各节点的match index（包含自身）

        // 计算候选提交索引
        let candidate_index = match self.config.quorum() {
            QuorumRequirement::Joint { old, new } => {
                // 联合配置需要同时满足两个quorum
                let old_majority = self
                    .find_majority_index(&self.config.joint().unwrap().old_voters, old)
                    .await;
                let new_majority = self
                    .find_majority_index(&self.config.joint().unwrap().new_voters, new)
                    .await;

                // 取两个配置的交集最小值
                match (old_majority, new_majority) {
                    (Some(old_idx), Some(new_idx)) => std::cmp::min(old_idx, new_idx),
                    _ => return, // 未达到双重要求
                }
            }
            QuorumRequirement::Simple(quorum) => {
                let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
                match_indices.push(self.get_last_log_index());
                match_indices.sort_unstable_by(|a, b| b.cmp(a));

                if match_indices.len() >= quorum {
                    match_indices[quorum - 1]
                } else {
                    return;
                }
            }
        };

        // 记录原来的commit_index
        let old_commit_index = self.commit_index;

        // 任期检查
        if candidate_index > self.commit_index {
            let candidate_term = match self
                .error_handler
                .handle(
                    self.callbacks.get_log_term(&self.id, candidate_index).await,
                    "get_log_term",
                    None,
                )
                .await
            {
                Some(term) => term,
                None => return,
            };

            if candidate_term == self.current_term {
                self.commit_index = candidate_index;
            } else {
                // 检查是否有当前任期的日志被提交
                let mut has_current_term = false;
                for i in (self.commit_index + 1)..=candidate_index {
                    if self.callbacks.get_log_term(&self.id, i).await.unwrap_or(0)
                        == self.current_term
                    {
                        has_current_term = true;
                        break;
                    }
                }

                if has_current_term {
                    self.commit_index = candidate_index;
                }
            }
        }

        // 如果commit_index有更新，立即应用已提交的日志
        if self.commit_index > old_commit_index {
            info!(
                "Node {} commit_index updated from {} to {}, applying committed logs",
                self.id, old_commit_index, self.commit_index
            );
            self.apply_committed_logs().await;
        }
    }

    // 辅助方法：在指定配置中找到多数派索引
    async fn find_majority_index(&self, voters: &HashSet<RaftId>, quorum: usize) -> Option<u64> {
        // 检查配置和多数派要求
        if voters.is_empty() || quorum == 0 {
            warn!("Invalid voters or quorum for majority calculation");
            return None;
        }

        let mut voter_indices: Vec<u64> = Vec::new();
        // 收集该配置中所有节点的match index
        for voter in voters {
            if voter == &self.id {
                voter_indices.push(self.get_last_log_index()); // Leader自身
            } else {
                voter_indices.push(self.match_index.get(voter).copied().unwrap_or(0));
            }
        }

        voter_indices.sort_unstable_by(|a, b| b.cmp(a));

        info!(
            "Node {} found voter indices: {:?} for quorum {}",
            self.id, voter_indices, quorum
        );

        if voter_indices.len() >= quorum {
            Some(voter_indices[quorum - 1])
        } else {
            None
        }
    }
}
