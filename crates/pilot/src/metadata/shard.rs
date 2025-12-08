//! Shard information definitions

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::NodeId;

/// Shard ID
/// 
/// Business layer concept: A shard can split into multiple shards.
/// At the Raft layer, each shard corresponds to one Raft Group (GroupId),
/// but ShardId and GroupId are concepts at different layers.
pub type ShardId = String;

/// Total number of hash slots (similar to Redis Cluster's 16384)
pub const TOTAL_SLOTS: u32 = 16384;

/// Key range [start, end)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    /// Start slot (inclusive)
    pub start: u32,
    /// End slot (exclusive)
    pub end: u32,
}

impl KeyRange {
    pub fn new(start: u32, end: u32) -> Self {
        assert!(start < end, "start must be less than end");
        assert!(end <= TOTAL_SLOTS, "end must not exceed TOTAL_SLOTS");
        Self { start, end }
    }

    /// Create empty range (for representing shards with no assigned slots)
    pub fn empty() -> Self {
        Self { start: 0, end: 0 }
    }

    /// Check if range is empty (no assigned slots)
    pub fn is_empty(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Number of slots
    pub fn slot_count(&self) -> u32 {
        self.end - self.start
    }

    /// Check if slot is within range
    pub fn contains(&self, slot: u32) -> bool {
        if self.is_empty() {
            false
        } else {
            slot >= self.start && slot < self.end
        }
    }
}

impl std::fmt::Display for KeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {})", self.start, self.end)
    }
}

/// Shard status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    /// Normal service
    Normal,
    /// Creating
    Creating,
    /// Migrating
    Migrating,
    /// Splitting
    Splitting,
    /// Deleting
    Deleting,
    /// Error status
    Error,
}

impl Default for ShardStatus {
    fn default() -> Self {
        Self::Creating
    }
}

impl std::fmt::Display for ShardStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardStatus::Normal => write!(f, "normal"),
            ShardStatus::Creating => write!(f, "creating"),
            ShardStatus::Migrating => write!(f, "migrating"),
            ShardStatus::Splitting => write!(f, "splitting"),
            ShardStatus::Deleting => write!(f, "deleting"),
            ShardStatus::Error => write!(f, "error"),
        }
    }
}

// ==================== Split-related definitions ====================

/// Split task status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitStatus {
    /// Preparation phase - create target shard
    Preparing,
    /// Snapshot transfer phase
    SnapshotTransfer,
    /// Incremental catch-up phase
    CatchingUp,
    /// Buffer requests phase
    Buffering,
    /// Route switching phase
    Switching,
    /// Cleanup phase
    Cleanup,
    /// Completed
    Completed,
    /// Failed
    Failed(String),
    /// Cancelled
    Cancelled,
}

impl std::fmt::Display for SplitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitStatus::Preparing => write!(f, "preparing"),
            SplitStatus::SnapshotTransfer => write!(f, "snapshot_transfer"),
            SplitStatus::CatchingUp => write!(f, "catching_up"),
            SplitStatus::Buffering => write!(f, "buffering"),
            SplitStatus::Switching => write!(f, "switching"),
            SplitStatus::Cleanup => write!(f, "cleanup"),
            SplitStatus::Completed => write!(f, "completed"),
            SplitStatus::Failed(reason) => write!(f, "failed: {}", reason),
            SplitStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Split task progress
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SplitProgress {
    /// Whether snapshot transfer is complete
    pub snapshot_done: bool,
    /// Log index at snapshot cutoff
    pub snapshot_index: u64,
    /// Source shard's latest log index
    pub source_last_index: u64,
    /// Target shard's applied log index
    pub target_applied_index: u64,
    /// Current number of buffered requests
    pub buffered_requests: usize,
}

impl SplitProgress {
    /// Calculate delay (number of log entries)
    pub fn delay(&self) -> u64 {
        self.source_last_index.saturating_sub(self.target_applied_index)
    }
}

/// Split task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitTask {
    /// Task ID
    pub id: String,
    /// Source shard ID
    pub source_shard: ShardId,
    /// Target shard ID
    pub target_shard: ShardId,
    /// Split point slot (target shard responsible for [split_slot, source.end))
    pub split_slot: u32,
    /// Task status
    pub status: SplitStatus,
    /// Progress information
    pub progress: SplitProgress,
    /// Creation time
    pub created_at: DateTime<Utc>,
    /// Update time
    pub updated_at: DateTime<Utc>,
}

impl SplitTask {
    /// Create new split task
    pub fn new(
        source_shard: ShardId,
        target_shard: ShardId,
        split_slot: u32,
    ) -> Self {
        let now = Utc::now();
        let id = format!(
            "split_{}_{}_{}", 
            source_shard, 
            split_slot,
            now.timestamp_millis()
        );
        
        Self {
            id,
            source_shard,
            target_shard,
            split_slot,
            status: SplitStatus::Preparing,
            progress: SplitProgress::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Update status
    pub fn set_status(&mut self, status: SplitStatus) {
        self.status = status;
        self.updated_at = Utc::now();
    }

    /// Update progress
    pub fn update_progress(&mut self, progress: SplitProgress) {
        self.progress = progress;
        self.updated_at = Utc::now();
    }

    /// Whether completed (success or failure)
    pub fn is_finished(&self) -> bool {
        matches!(
            self.status,
            SplitStatus::Completed | SplitStatus::Failed(_) | SplitStatus::Cancelled
        )
    }

    /// Whether successfully completed
    pub fn is_successful(&self) -> bool {
        matches!(self.status, SplitStatus::Completed)
    }
}

/// Raft group status reported by nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftGroupStatus {
    /// Raft group created successfully and ready
    Ready,
    /// Raft group creation failed
    Failed,
    /// Raft group removed (e.g., during shard deletion or migration)
    Removed,
}

impl std::fmt::Display for RaftGroupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftGroupStatus::Ready => write!(f, "ready"),
            RaftGroupStatus::Failed => write!(f, "failed"),
            RaftGroupStatus::Removed => write!(f, "removed"),
        }
    }
}

/// Shard's role in split
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitRole {
    /// Source shard (shard being split)
    Source,
    /// Target shard (newly created shard)
    Target,
}

/// Shard's split state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSplitState {
    /// Associated split task ID
    pub split_task_id: String,
    /// Split point slot
    pub split_slot: u32,
    /// Role in split
    pub role: SplitRole,
}

/// Shard information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Shard ID
    pub id: ShardId,
    /// Responsible key range
    pub key_range: KeyRange,
    /// Replica node list (first is preferred leader)
    pub replicas: Vec<NodeId>,
    /// Current Raft leader
    pub leader: Option<NodeId>,
    /// Shard status
    pub status: ShardStatus,
    /// Replica factor
    pub replica_factor: u32,
    /// Split state (if splitting)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_state: Option<ShardSplitState>,
}

impl ShardInfo {
    /// Create new shard
    pub fn new(id: ShardId, key_range: KeyRange, replica_factor: u32) -> Self {
        Self {
            id,
            key_range,
            replicas: Vec::new(),
            leader: None,
            status: ShardStatus::Creating,
            replica_factor,
            split_state: None,
        }
    }

    /// Check if splitting
    pub fn is_splitting(&self) -> bool {
        self.split_state.is_some()
    }

    /// Set split state
    pub fn set_split_state(&mut self, state: ShardSplitState) {
        self.split_state = Some(state);
        self.status = ShardStatus::Splitting;
    }

    /// Clear split state
    pub fn clear_split_state(&mut self) {
        self.split_state = None;
        self.status = ShardStatus::Normal;
    }

    /// Add replica node
    pub fn add_replica(&mut self, node_id: NodeId) {
        if !self.replicas.contains(&node_id) {
            self.replicas.push(node_id);
        }
    }

    /// Remove replica node
    pub fn remove_replica(&mut self, node_id: &NodeId) {
        self.replicas.retain(|n| n != node_id);
        if self.leader.as_ref() == Some(node_id) {
            self.leader = None;
        }
    }

    /// Set leader
    pub fn set_leader(&mut self, node_id: NodeId) {
        if self.replicas.contains(&node_id) {
            self.leader = Some(node_id);
        }
    }

    /// Check if replica count satisfies requirement
    pub fn is_replica_satisfied(&self) -> bool {
        self.replicas.len() >= self.replica_factor as usize
    }

    /// Check if healthy (has leader and replica count satisfied)
    pub fn is_healthy(&self) -> bool {
        self.status == ShardStatus::Normal 
            && self.leader.is_some() 
            && self.is_replica_satisfied()
    }
}
