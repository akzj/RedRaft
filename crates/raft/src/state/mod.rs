//! Raft State Machine Module
//!
//! This module contains the core `RaftState` struct and its implementation,
//! split across multiple files for maintainability:
//!
//! - `mod.rs` - State struct definition and options
//! - `election.rs` - Election handling
//! - `replication.rs` - Log replication
//! - `snapshot.rs` - Snapshot management
//! - `client.rs` - Client request handling
//! - `config.rs` - Configuration changes
//! - `leader_transfer.rs` - Leadership transfer

mod client;
mod config;
mod election;
mod leader_transfer;
mod read_index;
mod replication;
mod snapshot;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tracing::{error, info};

use crate::cluster_config::ClusterConfig;
use crate::error::{CallbackErrorHandler, RaftError};
use crate::event::Role;
use crate::message::{InstallSnapshotState, Snapshot, SnapshotProbeSchedule};
use crate::pipeline;
use crate::traits::RaftCallbacks;
use crate::types::{RaftId, RequestId, TimerId};

/// ReadIndex 请求状态
#[derive(Debug, Clone)]
pub struct ReadIndexState {
    /// 读取索引
    pub read_index: u64,
    /// 已确认的节点
    pub acks: HashSet<RaftId>,
    /// 请求时间（用于超时）
    pub request_time: Instant,
}

/// Raft 状态机配置选项
#[derive(Debug, Clone)]
pub struct RaftStateOptions {
    pub id: RaftId,
    pub peers: Vec<RaftId>,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub apply_interval: Duration,
    pub config_change_timeout: Duration,
    pub leader_transfer_timeout: Duration,
    /// 每次应用到状态机的日志条数
    pub apply_batch_size: u64,
    pub schedule_snapshot_probe_interval: Duration,
    pub schedule_snapshot_probe_retries: u32,

    /// 是否启用 Pre-Vote（防止网络分区节点干扰集群）
    pub pre_vote_enabled: bool,

    /// 是否启用 LeaderLease（0 RTT 读优化）
    /// 注意：LeaderLease 依赖时钟同步，在时钟偏移较大的环境下可能有风险
    pub leader_lease_enabled: bool,

    // 反馈控制相关配置
    /// 最大InFlight请求数
    pub max_inflight_requests: u64,
    /// 初始批次大小
    pub initial_batch_size: u64,
    /// 最大批次大小
    pub max_batch_size: u64,
    /// 最小批次大小
    pub min_batch_size: u64,
    /// 反馈窗口大小（用于计算平均值）
    pub feedback_window_size: usize,

    // 智能超时配置
    /// 基础请求超时时间 (默认3秒)
    pub base_request_timeout: Duration,
    /// 最大请求超时时间 (默认30秒)
    pub max_request_timeout: Duration,
    /// 最小请求超时时间 (默认1秒)
    pub min_request_timeout: Duration,
    /// 响应时间权重因子 (默认2.0，表示超时=2倍平均响应时间)
    pub timeout_response_factor: f64,
    /// 批次调整的目标响应时间 (默认100ms)
    pub target_response_time: Duration,
}

impl Default for RaftStateOptions {
    fn default() -> Self {
        Self {
            id: RaftId::new("".to_string(), "".to_string()),
            peers: vec![],
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            apply_interval: Duration::from_millis(1),
            apply_batch_size: 54,
            config_change_timeout: Duration::from_secs(10),
            leader_transfer_timeout: Duration::from_secs(10),
            schedule_snapshot_probe_interval: Duration::from_secs(5),
            schedule_snapshot_probe_retries: 24 * 60 * 60 / 5, // 默认尝试24小时
            pre_vote_enabled: true,                            // 默认启用 Pre-Vote
            leader_lease_enabled: false, // 默认禁用 LeaderLease（需要时钟同步）
            // 反馈控制默认配置
            max_inflight_requests: 64,
            initial_batch_size: 10,
            max_batch_size: 100,
            min_batch_size: 1,
            feedback_window_size: 10,
            // 智能超时默认配置
            base_request_timeout: Duration::from_secs(3),
            max_request_timeout: Duration::from_secs(30),
            min_request_timeout: Duration::from_secs(1),
            timeout_response_factor: 2.0,
            target_response_time: Duration::from_millis(100),
        }
    }
}

/// Raft 状态机（可变状态，无 Clone）
pub struct RaftState {
    // 节点标识与配置
    pub id: RaftId,
    pub leader_id: Option<RaftId>,
    pub config: ClusterConfig,

    // 核心状态
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<RaftId>,

    // 日志与提交状态
    pub commit_index: u64,
    pub last_applied: u64,
    /// 最后一个快照的索引
    pub last_snapshot_index: u64,
    /// 最后一个快照的任期
    pub last_snapshot_term: u64,
    /// 最后一个日志条目的索引
    pub last_log_index: u64,
    /// 最后一个日志条目的任期
    pub last_log_term: u64,

    // Leader 专用状态
    pub next_index: HashMap<RaftId, u64>,
    pub match_index: HashMap<RaftId, u64>,
    /// 客户端请求ID -> 日志索引
    pub client_requests: HashMap<RequestId, u64>,
    /// 日志索引 -> 客户端请求ID
    pub client_requests_revert: HashMap<u64, RequestId>,
    /// 客户端请求ID -> 创建时间（用于过期清理）
    pub client_request_timestamps: HashMap<RequestId, Instant>,

    // 配置变更相关状态
    pub config_change_in_progress: bool,
    pub config_change_start_time: Option<Instant>,
    pub config_change_timeout: Duration,
    /// 联合配置的日志索引
    pub joint_config_log_index: u64,

    // 定时器配置
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_interval_timer_id: Option<TimerId>,
    /// 日志应用到状态机的间隔
    pub apply_interval: Duration,
    pub apply_interval_timer: Option<TimerId>,
    pub config_change_timer: Option<TimerId>,

    pub last_heartbeat: Instant,

    // 外部依赖
    pub callbacks: Arc<dyn RaftCallbacks>,

    // 统一错误处理器
    pub error_handler: CallbackErrorHandler,

    // 选举跟踪（仅 Candidate 状态有效）
    pub election_votes: HashMap<RaftId, bool>,
    pub election_max_term: u64,
    pub current_election_id: Option<RequestId>,

    // Pre-Vote 跟踪
    pub pre_vote_votes: HashMap<RaftId, bool>,
    pub current_pre_vote_id: Option<RequestId>,

    // ReadIndex 跟踪（用于线性一致性读）
    /// 待确认的 ReadIndex 请求: request_id -> ReadIndexState
    pub pending_read_indices: HashMap<RequestId, ReadIndexState>,
    /// 等待应用完成的读请求: request_id -> read_index
    pub pending_reads: HashMap<RequestId, u64>,

    // LeaderLease 跟踪（0 RTT 读优化）
    /// Leader 租约结束时间（None 表示无有效租约）
    pub lease_end: Option<Instant>,

    // 快照请求跟踪（仅 Follower 有效）
    pub current_snapshot_request_id: Option<RequestId>,
    /// 安装快照成功标志
    pub install_snapshot_success: Option<(bool, RequestId, Option<crate::error::SnapshotError>)>,

    // 快照相关状态（Leader 用）
    pub follower_snapshot_states: HashMap<RaftId, InstallSnapshotState>,
    pub follower_last_snapshot_index: HashMap<RaftId, u64>,
    pub snapshot_probe_schedules: Vec<SnapshotProbeSchedule>,
    pub schedule_snapshot_probe_interval: Duration,
    pub schedule_snapshot_probe_retries: u32,

    // 领导人转移相关状态
    pub(crate) leader_transfer_target: Option<RaftId>,
    pub leader_transfer_request_id: Option<RequestId>,
    pub(crate) leader_transfer_timeout: Duration,
    pub leader_transfer_start_time: Option<Instant>,

    /// 选举定时器ID
    pub election_timer: Option<TimerId>,
    /// 领导人转移定时器ID
    pub leader_transfer_timer: Option<TimerId>,

    // Pipeline 状态管理（反馈控制和超时管理）
    pub pipeline: pipeline::PipelineState,

    // 其他状态（可扩展）
    pub options: RaftStateOptions,
}

impl RaftState {
    /// 初始化状态
    pub async fn new(options: RaftStateOptions, callbacks: Arc<dyn RaftCallbacks>) -> Result<Self> {
        // 从回调加载持久化状态
        let (current_term, voted_for) = match callbacks.load_hard_state(&options.id).await {
            Ok(Some(hard_state)) => (hard_state.term, hard_state.voted_for),
            Ok(None) => (0, None),
            Err(err) => {
                error!("Failed to load hard state: {}", err);
                return Err(RaftError::Storage(err).into());
            }
        };

        let loaded_config = match callbacks.load_cluster_config(&options.id).await {
            Ok(conf) => conf,
            Err(err) => {
                error!("Failed to load cluster config: {}", err);
                return Err(RaftError::Storage(err).into());
            }
        };

        let snap = match callbacks.load_snapshot(&options.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                info!("Node {} No snapshot found", options.id);
                Snapshot {
                    index: 0,
                    term: 0,
                    data: vec![],
                    config: ClusterConfig::empty(),
                }
            }
            Err(err) => {
                error!("Failed to load snapshot: {}", err);
                return Err(RaftError::Storage(err).into());
            }
        };

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
            commit_index: snap.index,
            last_applied: snap.index,
            last_log_index,
            last_log_term,
            last_snapshot_index: snap.index,
            last_snapshot_term: snap.term,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            client_requests: HashMap::new(),
            client_requests_revert: HashMap::new(),
            client_request_timestamps: HashMap::new(),
            config_change_in_progress: false,
            config_change_start_time: None,
            heartbeat_interval_timer_id: None,
            config_change_timeout: options.config_change_timeout,
            joint_config_log_index: 0,
            election_timeout_min: options.election_timeout_min,
            election_timeout_max: options.election_timeout_max,
            heartbeat_interval: options.heartbeat_interval,
            apply_interval: options.apply_interval,
            apply_interval_timer: None,
            config_change_timer: None,
            last_heartbeat: Instant::now(),
            callbacks,
            election_votes: HashMap::new(),
            election_max_term: current_term,
            current_election_id: None,
            pre_vote_votes: HashMap::new(),
            current_pre_vote_id: None,
            pending_read_indices: HashMap::new(),
            pending_reads: HashMap::new(),
            lease_end: None,
            install_snapshot_success: None,
            current_snapshot_request_id: None,
            follower_snapshot_states: HashMap::new(),
            follower_last_snapshot_index: HashMap::new(),
            snapshot_probe_schedules: Vec::new(),
            election_timer: None,
            leader_transfer_timer: None,
            pipeline: pipeline::PipelineState::new(options.clone()),
            options,
        })
    }

    /// 获取有效的对等节点列表
    pub fn get_effective_peers(&self) -> Vec<RaftId> {
        self.config
            .get_all_nodes()
            .into_iter()
            .filter(|id| *id != self.id)
            .collect()
    }

    /// 获取最后一个日志条目的索引
    pub fn get_last_log_index(&self) -> u64 {
        std::cmp::max(self.last_log_index, self.last_snapshot_index)
    }

    /// 获取最后一个日志条目的任期
    pub fn get_last_log_term(&self) -> u64 {
        self.last_log_term
    }

    /// 获取当前角色
    pub fn get_role(&self) -> Role {
        self.role
    }

    /// 获取当前 term
    pub fn get_current_term(&self) -> u64 {
        self.current_term
    }

    /// 获取当前 commit_index
    pub fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    /// 获取当前 last_applied
    pub fn get_last_applied(&self) -> u64 {
        self.last_applied
    }

    /// 获取当前InFlight请求总数
    pub fn get_inflight_request_count(&self) -> usize {
        self.pipeline.get_inflight_request_count()
    }

    /// 统一保存 HardState（集中管理持久化）
    pub async fn persist_hard_state(&mut self) {
        use crate::message::HardState;
        let hard_state = HardState {
            raft_id: self.id.clone(),
            term: self.current_term,
            voted_for: self.voted_for.clone(),
        };
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks.save_hard_state(&self.id, hard_state).await,
                "save_hard_state",
                None,
            )
            .await;
    }

    /// 清理 Leader 专用状态（角色切换时调用）
    pub(crate) fn clear_leader_state(&mut self) {
        // 清理复制状态
        self.next_index.clear();
        self.match_index.clear();

        // 清理客户端请求跟踪
        self.client_requests.clear();
        self.client_requests_revert.clear();
        self.client_request_timestamps.clear();

        // 清理 ReadIndex 状态
        self.clear_read_index_state();

        // 清理 LeaderLease 状态
        self.lease_end = None;

        // 清理快照状态
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        // 清理配置变更状态
        self.config_change_in_progress = false;
        self.config_change_start_time = None;
        self.joint_config_log_index = 0;

        // 清理领导权转移状态
        self.leader_transfer_target = None;
        self.leader_transfer_request_id = None;
        self.leader_transfer_start_time = None;

        // 清理 Pipeline 状态
        self.pipeline.clear_all();

        // 清理心跳定时器
        if let Some(timer_id) = self.heartbeat_interval_timer_id.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        // 清理配置变更定时器
        if let Some(timer_id) = self.config_change_timer.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        // 清理领导权转移定时器
        if let Some(timer_id) = self.leader_transfer_timer.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }
    }

    /// 从 Leader 降级为 Follower
    pub(crate) async fn step_down_to_follower(&mut self, new_term: Option<u64>) {
        if let Some(term) = new_term.filter(|&t| t > self.current_term) {
            self.current_term = term;
        }

        let was_leader = self.role == Role::Leader;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;

        // 清理 Leader 状态
        if was_leader {
            self.clear_leader_state();
        }

        // 持久化状态变更
        self.persist_hard_state().await;

        // 通知上层
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .on_state_changed(&self.id, Role::Follower)
                    .await,
                "state_changed",
                None,
            )
            .await;
    }

    /// 处理事件（主入口）
    pub async fn handle_event(&mut self, event: crate::Event) {
        use crate::Event;

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
            Event::ReadIndex { request_id } => self.handle_read_index(request_id).await,
            Event::InstallSnapshotRequest(sender, request) => {
                self.handle_install_snapshot(sender, request).await
            }
            Event::InstallSnapshotResponse(sender, response) => {
                self.handle_install_snapshot_response(sender, response)
                    .await
            }
            Event::PreVoteRequest(sender, request) => {
                self.handle_pre_vote_request(sender, request).await
            }
            Event::PreVoteResponse(sender, response) => {
                self.handle_pre_vote_response(sender, response).await
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
            Event::CompleteSnapshotInstallation(complete_snapshot_installation) => {
                self.handle_complete_snapshot_installation(complete_snapshot_installation)
                    .await;
            }
        }
    }
}
