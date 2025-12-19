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

/// ReadIndex request state
#[derive(Debug, Clone)]
pub struct ReadIndexState {
    /// Read index
    pub read_index: u64,
    /// Confirmed nodes
    pub acks: HashSet<RaftId>,
    /// Request time (for timeout)
    pub request_time: Instant,
}

/// Raft state machine configuration options
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
    /// Number of logs applied to state machine per batch
    pub apply_batch_size: u64,
    pub schedule_snapshot_probe_interval: Duration,
    pub schedule_snapshot_probe_retries: u32,

    /// Whether to enable Pre-Vote (prevents network partition nodes from interfering with the cluster)
    pub pre_vote_enabled: bool,

    /// Whether to enable LeaderLease (0 RTT read optimization)
    /// Note: LeaderLease depends on clock synchronization and may be risky in environments with large clock skew
    pub leader_lease_enabled: bool,

    // Feedback control related configuration
    /// Maximum number of in-flight requests
    pub max_inflight_requests: u64,
    /// Initial batch size
    pub initial_batch_size: u64,
    /// Maximum batch size
    pub max_batch_size: u64,
    /// Minimum batch size
    pub min_batch_size: u64,
    /// Feedback window size (for calculating averages)
    pub feedback_window_size: usize,

    // Smart timeout configuration
    /// Base request timeout (default 3 seconds)
    pub base_request_timeout: Duration,
    /// Maximum request timeout (default 30 seconds)
    pub max_request_timeout: Duration,
    /// Minimum request timeout (default 1 second)
    pub min_request_timeout: Duration,
    /// Response time weight factor (default 2.0, meaning timeout = 2x average response time)
    pub timeout_response_factor: f64,
    /// Target response time for batch adjustment (default 100ms)
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
            schedule_snapshot_probe_retries: 24 * 60 * 60 / 5, // Default attempts for 24 hours
            pre_vote_enabled: true,                            // Pre-Vote enabled by default
            leader_lease_enabled: false, // LeaderLease disabled by default (requires clock synchronization)
            // Feedback control default configuration
            max_inflight_requests: 64,
            initial_batch_size: 10,
            max_batch_size: 100,
            min_batch_size: 1,
            feedback_window_size: 10,
            // Smart timeout default configuration
            base_request_timeout: Duration::from_secs(3),
            max_request_timeout: Duration::from_secs(30),
            min_request_timeout: Duration::from_secs(1),
            timeout_response_factor: 2.0,
            target_response_time: Duration::from_millis(100),
        }
    }
}

/// Raft state machine (mutable state, no Clone)
pub struct RaftState {
    // Node identification and configuration
    pub id: RaftId,
    pub leader_id: Option<RaftId>,
    pub config: ClusterConfig,

    // Core state
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<RaftId>,

    // Log and commit state
    pub commit_index: u64,
    pub last_applied: u64,
    /// Last snapshot index
    pub last_snapshot_index: u64,
    /// Last snapshot term
    pub last_snapshot_term: u64,
    /// Last log entry index
    pub last_log_index: u64,
    /// Last log entry term
    pub last_log_term: u64,

    // Leader-specific state
    pub next_index: HashMap<RaftId, u64>,
    pub match_index: HashMap<RaftId, u64>,
    /// Client request ID -> log index
    pub client_requests: HashMap<RequestId, u64>,
    /// Log index -> client request ID
    pub client_requests_revert: HashMap<u64, RequestId>,
    /// Client request ID -> creation time (for expiration cleanup)
    pub client_request_timestamps: HashMap<RequestId, Instant>,

    // Configuration change related state
    pub config_change_in_progress: bool,
    pub config_change_start_time: Option<Instant>,
    pub config_change_timeout: Duration,
    /// Log index of joint configuration
    pub joint_config_log_index: u64,

    // Timer configuration
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_interval_timer_id: Option<TimerId>,
    /// Interval for applying logs to state machine
    pub apply_interval: Duration,
    pub apply_interval_timer: Option<TimerId>,
    pub config_change_timer: Option<TimerId>,

    pub last_heartbeat: Instant,

    // External dependencies
    pub callbacks: Arc<dyn RaftCallbacks>,

    // Unified error handler
    pub error_handler: CallbackErrorHandler,

    // Election tracking (only valid for Candidate state)
    pub election_votes: HashMap<RaftId, bool>,
    pub election_max_term: u64,
    pub current_election_id: Option<RequestId>,

    // Pre-Vote tracking
    pub pre_vote_votes: HashMap<RaftId, bool>,
    pub current_pre_vote_id: Option<RequestId>,

    // ReadIndex tracking (for linearizable reads)
    /// Pending ReadIndex requests: request_id -> ReadIndexState
    pub pending_read_indices: HashMap<RequestId, ReadIndexState>,
    /// Pending read requests waiting for application: request_id -> read_index
    pub pending_reads: HashMap<RequestId, u64>,

    // LeaderLease tracking (0 RTT read optimization)
    /// Leader lease expiration time (None means no valid lease)
    pub lease_end: Option<Instant>,

    // Snapshot request tracking (only valid for Follower)
    pub current_snapshot_request_id: Option<RequestId>,
    /// Snapshot installation success flag
    pub install_snapshot_success: Option<(bool, RequestId, Option<crate::error::SnapshotError>)>,

    /// Snapshot creation in progress flag (prevents concurrent snapshot creation)
    pub snapshot_in_progress: bool,

    // Snapshot-related state (for Leader)
    pub follower_snapshot_states: HashMap<RaftId, InstallSnapshotState>,
    pub follower_last_snapshot_index: HashMap<RaftId, u64>,
    pub snapshot_probe_schedules: Vec<SnapshotProbeSchedule>,
    pub schedule_snapshot_probe_interval: Duration,
    pub schedule_snapshot_probe_retries: u32,

    // Leadership transfer related state
    pub(crate) leader_transfer_target: Option<RaftId>,
    pub leader_transfer_request_id: Option<RequestId>,
    pub(crate) leader_transfer_timeout: Duration,
    pub leader_transfer_start_time: Option<Instant>,

    /// Election timer ID
    pub election_timer: Option<TimerId>,
    /// Leader transfer timer ID
    pub leader_transfer_timer: Option<TimerId>,

    // Pipeline state management (feedback control and timeout management)
    pub pipeline: pipeline::PipelineState,

    // Other state (extensible)
    pub options: RaftStateOptions,
}

impl RaftState {
    /// Initialize state
    pub async fn new(options: RaftStateOptions, callbacks: Arc<dyn RaftCallbacks>) -> Result<Self> {
        // Load persistent state from callbacks
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
            snapshot_in_progress: false,
            follower_snapshot_states: HashMap::new(),
            follower_last_snapshot_index: HashMap::new(),
            snapshot_probe_schedules: Vec::new(),
            election_timer: None,
            leader_transfer_timer: None,
            pipeline: pipeline::PipelineState::new(options.clone()),
            options,
        })
    }

    /// Get list of valid peer nodes
    pub fn get_effective_peers(&self) -> Vec<RaftId> {
        self.config
            .get_all_nodes()
            .into_iter()
            .filter(|id| *id != self.id)
            .collect()
    }

    /// Get last log entry index
    pub fn get_last_log_index(&self) -> u64 {
        std::cmp::max(self.last_log_index, self.last_snapshot_index)
    }

    /// Get last log entry term
    pub fn get_last_log_term(&self) -> u64 {
        self.last_log_term
    }

    /// Get current role
    pub fn get_role(&self) -> Role {
        self.role
    }

    /// Get current term
    pub fn get_current_term(&self) -> u64 {
        self.current_term
    }

    /// Get current commit_index
    pub fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Get current last_applied
    pub fn get_last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Get current total number of in-flight requests
    pub fn get_inflight_request_count(&self) -> usize {
        self.pipeline.get_inflight_request_count()
    }

    /// Unified saving of HardState (centralized persistence management)
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

    /// Clean up Leader-specific state (called during role switch)
    pub(crate) fn clear_leader_state(&mut self) {
        // Clear replication state
        self.next_index.clear();
        self.match_index.clear();

        // Clear client request tracking
        self.client_requests.clear();
        self.client_requests_revert.clear();
        self.client_request_timestamps.clear();

        // Clear ReadIndex state
        self.clear_read_index_state();

        // Clear LeaderLease state
        self.lease_end = None;

        // Clear snapshot state
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        // Clear configuration change state
        self.config_change_in_progress = false;
        self.config_change_start_time = None;
        self.joint_config_log_index = 0;

        // Clear leadership transfer state
        self.leader_transfer_target = None;
        self.leader_transfer_request_id = None;
        self.leader_transfer_start_time = None;

        // Clear Pipeline state
        self.pipeline.clear_all();

        // Clear heartbeat timer
        if let Some(timer_id) = self.heartbeat_interval_timer_id.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        // Clear configuration change timer
        if let Some(timer_id) = self.config_change_timer.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        // Clear leadership transfer timer
        if let Some(timer_id) = self.leader_transfer_timer.take() {
            self.callbacks.del_timer(&self.id, timer_id);
        }
    }

    /// Step down from Leader to Follower
    pub(crate) async fn step_down_to_follower(&mut self, new_term: Option<u64>) {
        if let Some(term) = new_term.filter(|&t| t > self.current_term) {
            self.current_term = term;
        }

        let was_leader = self.role == Role::Leader;
        self.role = Role::Follower;
        self.voted_for = None;
        self.leader_id = None;

        // Clear Leader state
        if was_leader {
            self.clear_leader_state();
        }

        // Persist state changes
        self.persist_hard_state().await;

        // Notify upper layer
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

    /// Handle events (main entry)
    pub async fn handle_event(&mut self, event: crate::Event) {
        use crate::Event;

        match event {
            // ========== Timer Events ==========
            Event::ElectionTimeout => self.handle_election_timeout().await,
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().await,
            Event::ApplyLogTimeout => self.apply_committed_logs().await,
            Event::ConfigChangeTimeout => self.handle_config_change_timeout().await,
            Event::LeaderTransferTimeout => self.handle_leader_transfer_timeout().await,

            // ========== Election & Pre-Vote ==========
            Event::RequestVoteRequest(sender, request) => {
                self.handle_request_vote(sender, request).await
            }
            Event::RequestVoteResponse(sender, response) => {
                self.handle_request_vote_response(sender, response).await
            }
            Event::PreVoteRequest(sender, request) => {
                self.handle_pre_vote_request(sender, request).await
            }
            Event::PreVoteResponse(sender, response) => {
                self.handle_pre_vote_response(sender, response).await
            }

            // ========== Log Replication ==========
            Event::AppendEntriesRequest(sender, request) => {
                self.handle_append_entries_request(sender, request).await
            }
            Event::AppendEntriesResponse(sender, response) => {
                self.handle_append_entries_response(sender, response).await
            }

            // ========== Client Requests ==========
            Event::ClientPropose { cmd, request_id } => {
                self.handle_client_propose(cmd, request_id).await
            }
            Event::ReadIndex { request_id } => {
                self.handle_read_index(request_id).await
            }

            // ========== Snapshot ==========
            Event::CreateSnapshot(request) => self.trigger_snapshot_creation(Some(request)).await,
            Event::SnapshotCreated(result) => self.handle_snapshot_created(result).await,
            Event::InstallSnapshotRequest(sender, request) => {
                self.handle_install_snapshot(sender, request).await
            }
            Event::InstallSnapshotResponse(sender, response) => {
                self.handle_install_snapshot_response(sender, response).await
            }
            Event::CompleteSnapshotInstallation(event) => {
                self.handle_complete_snapshot_installation(event).await
            }

            // ========== Config Change ==========
            Event::ChangeConfig { new_voters, request_id } => {
                self.handle_change_config(new_voters, request_id).await
            }
            Event::AddLearner { learner, request_id } => {
                self.handle_add_learner(learner, request_id).await
            }
            Event::RemoveLearner { learner, request_id } => {
                self.handle_remove_learner(learner, request_id).await
            }

            // ========== Leader Transfer ==========
            Event::LeaderTransfer { target, request_id } => {
                self.handle_leader_transfer(target, request_id).await
            }
        }
    }
}
