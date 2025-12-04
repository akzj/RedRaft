use std::collections::HashSet;
use std::fmt::{self, Display};

use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, CompleteSnapshotInstallation,
    InstallSnapshotRequest, InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::types::{Command, RaftId, RequestId};

/// Raft 事件定义（输入）
#[derive(Debug, Clone)]
pub enum Event {
    // 定时器事件
    /// 选举超时（Follower/Candidate 触发）
    ElectionTimeout,
    /// 心跳超时（Leader 触发日志同步）
    HeartbeatTimeout,
    /// 定期将已提交日志应用到状态机
    ApplyLogTimeout,
    /// 配置变更超时
    ConfigChangeTimeout,

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
        request_id: RequestId,
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

    // 快照安装结果
    CompleteSnapshotInstallation(CompleteSnapshotInstallation),
}

/// Raft 节点角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// 学习者角色（非投票成员）
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

