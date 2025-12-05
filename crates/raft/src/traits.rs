use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::error::StateChangeError;
use crate::message::{PreVoteRequest, PreVoteResponse};
use crate::{cluster_config::ClusterConfig, *};

// 结果类型别名
pub type RaftResult<T> = Result<T, RaftError>;
pub type RpcResult<T> = Result<T, RpcError>;
pub type StorageResult<T> = Result<T, StorageError>;
pub type TimerResult<T> = Result<T, TimerError>;
pub type ClientResult<T> = Result<T, ClientError>;
pub type ApplyResult<T> = Result<T, ApplyError>;
pub type SnapshotResult<T> = Result<T, SnapshotError>;

#[async_trait::async_trait]
pub trait Network: Send + Sync {
    // 发送 RPC 回调
    async fn send_request_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()>;

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()>;

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()>;

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()>;

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()>;

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()>;

    // Pre-Vote RPC（防止网络分区节点干扰集群）
    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> RpcResult<()>;

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> RpcResult<()>;
}

/// The `Storage` trait defines the interface for persistent storage required by a Raft node.
/// Implementors of this trait are responsible for storing and retrieving Raft state, log entries,
/// snapshots, and cluster configuration in a thread-safe and asynchronous manner.
///
/// All methods are asynchronous and must be safe to call from multiple threads concurrently.
///
/// # Methods
///
/// - `save_hard_state`: Persist the current term and the candidate voted for by this node.
/// - `load_hard_state`: Load the persisted term and voted-for candidate for this node.
/// - `append_log_entries`: Append a slice of log entries to the log.
/// - `get_log_entries`: Retrieve a range of log entries by index (inclusive of `low`, exclusive of `high`).
/// - `get_log_entries_term`: Retrieve a range of log entry indices and their terms.
/// - `truncate_log_suffix`: Remove all log entries after the given index (inclusive).
/// - `truncate_log_prefix`: Remove all log entries before the given index (exclusive).
/// - `get_last_log_index`: Get the last log index and its term.
/// - `get_log_term`: Get the term for a specific log index.
/// - `save_snapshot`: Persist a snapshot of the state machine.
/// - `load_snapshot`: Load the latest snapshot, if any.
/// - `create_snapshot`: Create a new snapshot and return its index and term.
/// - `save_cluster_config`: Persist the current cluster configuration.
/// - `load_cluster_config`: Load the persisted cluster configuration.
///
/// # Parameters
///
/// - `from`: The Raft node identifier for which the operation is performed.
/// - `term`: The current term to persist.
/// - `voted_for`: The candidate voted for in the current term.
/// - `entries`: The log entries to append.
/// - `low`, `high`: The range of log indices to retrieve.
/// - `idx`: The log index for truncation or term retrieval.
/// - `snap`: The snapshot to persist.
/// - `conf`: The cluster configuration to persist.
///
/// # Returns
///
/// All methods return a `StorageResult` indicating success or failure, with the appropriate result type.

#[async_trait]
pub trait HardStateStorage: Send + Sync {
    async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()>;
    async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>>;
}

#[async_trait]
pub trait LogEntryStorage: Send + Sync {
    async fn append_log_entries(&self, from: &RaftId, entries: &[LogEntry]) -> StorageResult<()>;
    async fn get_log_entries(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<LogEntry>>;

    async fn get_log_entries_term(
        &self,
        from: &RaftId,
        low: u64,
        high: u64,
    ) -> StorageResult<Vec<(u64, u64)>>;

    async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()>;

    async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()>;

    async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)>;

    async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64>;
}

#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    async fn save_snapshot(&self, from: &RaftId, snap: Snapshot) -> StorageResult<()>;
    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<Snapshot>>;
}

#[async_trait]
pub trait ClusterConfigStorage: Send + Sync {
    async fn save_cluster_config(&self, from: &RaftId, conf: ClusterConfig) -> StorageResult<()>;
    async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig>;
}

#[async_trait]
pub trait Storage:
    ClusterConfigStorage + HardStateStorage + SnapshotStorage + LogEntryStorage + Send + Sync
{
}

pub trait TimerService: Send + Sync {
    fn del_timer(&self, from: &RaftId, timer_id: TimerId) -> ();
    fn set_leader_transfer_timer(&self, from: &RaftId, dur: Duration) -> TimerId;
    fn set_election_timer(&self, from: &RaftId, dur: Duration) -> TimerId;
    fn set_heartbeat_timer(&self, from: &RaftId, dur: Duration) -> TimerId;
    fn set_apply_timer(&self, from: &RaftId, dur: Duration) -> TimerId;
    fn set_config_change_timer(&self, from: &RaftId, dur: Duration) -> TimerId;
}

#[async_trait]
pub trait EventSender: Send + Sync {
    async fn send(&self, target: RaftId, event: Event) -> anyhow::Result<()>;
}

#[async_trait]
pub trait StateMachine: Send + Sync {
    // 日志应用到状态机的回调
    async fn apply_command(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        cmd: Command,
    ) -> ApplyResult<()>;

    // 处理快照数据
    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,
        config: ClusterConfig, // 添加配置信息参数
        request_id: RequestId,
        oneshot: oneshot::Sender<SnapshotResult<()>>,
    );

    // create snapshot and save to
    async fn create_snapshot(
        &self,
        from: &RaftId,
        cluster_config: ClusterConfig,
        saver: Arc<dyn SnapshotStorage>,
    ) -> StorageResult<(u64, u64)>;

    // 客户端响应回调
    async fn client_response(
        &self,
        from: &RaftId,
        request_id: RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()>;

    /// ReadIndex 响应回调（用于线性一致性读）
    /// result: Ok(read_index) 表示成功，返回可安全读取的索引
    /// result: Err(e) 表示失败（如 NotLeader, Timeout）
    async fn read_index_response(
        &self,
        from: &RaftId,
        request_id: RequestId,
        result: ClientResult<u64>,
    ) -> ClientResult<()>;
}

#[async_trait]
pub trait EventNotify {
    // 状态变更通知回调
    async fn on_state_changed(&self, from: &RaftId, role: Role) -> Result<(), StateChangeError>;

    // 节点被从集群中删除的回调，用于优雅退出
    async fn on_node_removed(&self, node_id: &RaftId) -> Result<(), StateChangeError>;
}

#[async_trait]
pub trait RaftCallbacks:
    StateMachine + Network + Storage + TimerService + EventSender + EventNotify
{
}
