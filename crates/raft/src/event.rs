use std::collections::HashSet;
use std::fmt::{self, Display};

use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, CompleteSnapshotInstallation,
    InstallSnapshotRequest, InstallSnapshotResponse, PreVoteRequest, PreVoteResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use crate::types::{Command, RaftId, RequestId};

/// Raft event definitions (input)
#[derive(Debug, Clone)]
pub enum Event {
    // Timer events
    /// Election timeout (triggered by Follower/Candidate)
    ElectionTimeout,
    /// Heartbeat timeout (Leader triggers log synchronization)
    HeartbeatTimeout,
    /// Periodically apply committed logs to state machine
    ApplyLogTimeout,
    /// Config change timeout
    ConfigChangeTimeout,

    // RPC request events (from other nodes)
    RequestVoteRequest(RaftId, RequestVoteRequest),
    AppendEntriesRequest(RaftId, AppendEntriesRequest),
    InstallSnapshotRequest(RaftId, InstallSnapshotRequest),

    // RPC response events (other nodes' replies to this node's requests)
    RequestVoteResponse(RaftId, RequestVoteResponse),
    AppendEntriesResponse(RaftId, AppendEntriesResponse),
    InstallSnapshotResponse(RaftId, InstallSnapshotResponse),

    // Pre-Vote events (prevent network partitioned nodes from disrupting the cluster)
    PreVoteRequest(RaftId, PreVoteRequest),
    PreVoteResponse(RaftId, PreVoteResponse),

    // Leader transfer related events
    LeaderTransfer {
        target: RaftId,
        request_id: RequestId,
    },
    LeaderTransferTimeout,

    // Client events
    ClientPropose {
        cmd: Command,
        request_id: RequestId,
    },

    /// ReadIndex request (for linearizable reads)
    ReadIndex {
        request_id: RequestId,
    },

    // Config change events
    ChangeConfig {
        new_voters: HashSet<RaftId>,
        request_id: RequestId,
    },

    // Learner management events
    AddLearner {
        learner: RaftId,
        request_id: RequestId,
    },
    RemoveLearner {
        learner: RaftId,
        request_id: RequestId,
    },

    // Snapshot generation
    CreateSnapshot,

    // Snapshot installation result
    CompleteSnapshotInstallation(CompleteSnapshotInstallation),
}

/// Raft node role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

