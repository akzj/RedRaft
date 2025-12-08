use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Ok, Result};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{Command, RaftId, RequestId, cluster_config::ClusterConfig};

// === Network Interfaces ===
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: RaftId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,          // Cluster config information included in snapshot
    pub snapshot_request_id: RequestId, // Snapshot request ID
    pub request_id: RequestId,          // Request ID
    // Empty message flag - used to probe installation status
    pub is_probe: bool,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteSnapshotInstallation {
    pub index: u64,
    pub term: u64,
    pub success: bool,
    pub request_id: RequestId,
    pub reason: Option<String>,
    pub config: Option<ClusterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstallSnapshotState {
    Failed(String), // Failed, with reason
    Installing,     // Currently installing
    Success,        // Successfully completed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub request_id: RequestId,
    pub state: InstallSnapshotState,
    pub error_message: String, // Error message, if any
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Decode, Encode)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}

// Snapshot probe schedule structure
#[derive(Debug, Clone)]
pub struct SnapshotProbeSchedule {
    pub snapshot_request_id: RequestId,
    pub peer: RaftId,
    pub next_probe_time: Instant,
    pub interval: Duration, // Probe interval
    pub max_attempts: u32,  // Maximum number of attempts
    pub attempts: u32,      // Current attempt count
}

// === Core State and Logic ===

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub is_config: bool,                      // Flag indicating if this is a config change log
    pub client_request_id: Option<RequestId>, // Associated client request ID for deduplication
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: RaftId,
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

/// Pre-Vote request (doesn't increment term, used to prevent network partitioned nodes from disrupting the cluster)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    /// The term the candidate expects (current_term + 1), but doesn't actually increment
    pub term: u64,
    pub candidate_id: RaftId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub request_id: RequestId,
}

/// Pre-Vote response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub request_id: RequestId,
}

/// ReadIndex request (for linearizable reads)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadIndexRequest {
    pub request_id: RequestId,
}

/// ReadIndex response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadIndexResponse {
    pub request_id: RequestId,
    pub read_index: u64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: RaftId,
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
    pub conflict_term: Option<u64>, // For more efficient log conflict handling
    pub request_id: RequestId,
    pub matched_index: u64, // For fast synchronization
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Default)]
pub struct HardStateMap(HashMap<RaftId, HardState>);

impl HardStateMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, key: RaftId, value: HardState) {
        self.0.insert(key, value);
    }

    pub fn get(&self, key: &RaftId) -> Option<&HardState> {
        self.0.get(key)
    }

    pub fn remove(&mut self, key: &RaftId) -> Option<HardState> {
        self.0.remove(key)
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RaftId, &HardState)> {
        self.0.iter()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct HardState {
    pub raft_id: RaftId,
    pub term: u64,
    pub voted_for: Option<RaftId>,
}

impl PartialEq for HardState {
    fn eq(&self, other: &Self) -> bool {
        self.raft_id == other.raft_id
            && self.term == other.term
            && self.voted_for == other.voted_for
    }
}

impl LogEntry {
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        let config = bincode::config::standard();
        Ok(bincode::decode_from_slice(data, config).map_err(|e| {
            warn!("Failed to deserialize log entry: {}", e);
            e
        })?)
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let config = bincode::config::standard();
        Ok(bincode::encode_to_vec(self, config)?)
    }
}
