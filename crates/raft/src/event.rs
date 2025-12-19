use std::collections::HashSet;
use std::fmt::{self, Display};

use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, CompleteSnapshotInstallation, CreateSnapshot,
    InstallSnapshotRequest, InstallSnapshotResponse, PreVoteRequest, PreVoteResponse,
    RequestVoteRequest, RequestVoteResponse, SnapshotCreated,
};
use crate::types::{Command, RaftId, RequestId};

/// Raft event definitions (input)
#[derive(Debug, Clone)]
pub enum Event {
    // ========== Timer Events ==========
    /// Election timeout (triggered by Follower/Candidate)
    ElectionTimeout,
    /// Heartbeat timeout (Leader triggers log synchronization)
    HeartbeatTimeout,
    /// Periodically apply committed logs to state machine
    ApplyLogTimeout,
    /// Config change timeout
    ConfigChangeTimeout,
    /// Leader transfer timeout
    LeaderTransferTimeout,

    // ========== Election & Pre-Vote ==========
    /// Request vote from other nodes
    RequestVoteRequest(RaftId, RequestVoteRequest),
    /// Response to vote request
    RequestVoteResponse(RaftId, RequestVoteResponse),
    /// Pre-vote request (prevents network partitioned nodes from disrupting cluster)
    PreVoteRequest(RaftId, PreVoteRequest),
    /// Pre-vote response
    PreVoteResponse(RaftId, PreVoteResponse),

    // ========== Log Replication ==========
    /// Append entries request from Leader
    AppendEntriesRequest(RaftId, AppendEntriesRequest),
    /// Response to append entries
    AppendEntriesResponse(RaftId, AppendEntriesResponse),

    // ========== Client Requests ==========
    /// Client proposes a command
    ClientPropose { cmd: Command, request_id: RequestId },
    /// ReadIndex request (for linearizable reads)
    ReadIndex { request_id: RequestId },

    // ========== Snapshot ==========
    /// Trigger async snapshot creation
    CreateSnapshot(CreateSnapshot),
    /// Snapshot creation completed (async notification)
    SnapshotCreated(SnapshotCreated),
    /// Install snapshot request from Leader
    InstallSnapshotRequest(RaftId, InstallSnapshotRequest),
    /// Response to install snapshot
    InstallSnapshotResponse(RaftId, InstallSnapshotResponse),
    /// Snapshot installation completed
    CompleteSnapshotInstallation(CompleteSnapshotInstallation),

    // ========== Config Change ==========
    /// Change cluster configuration (voters)
    ChangeConfig {
        new_voters: HashSet<RaftId>,
        request_id: RequestId,
    },
    /// Add a learner node
    AddLearner {
        learner: RaftId,
        request_id: RequestId,
    },
    /// Remove a learner node
    RemoveLearner {
        learner: RaftId,
        request_id: RequestId,
    },

    // ========== Leader Transfer ==========
    /// Transfer leadership to target node
    LeaderTransfer {
        target: RaftId,
        request_id: RequestId,
    },
}

/// Raft node role
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// Learner role (non-voting member)
    Learner,
}

impl Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Follower => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader => write!(f, "Leader"),
            Role::Learner => write!(f, "Learner"),
        }
    }
}
