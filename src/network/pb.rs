use anyhow::Result;

use crate::network::{OutgoingMessage, pb};

tonic::include_proto!("pb");

impl From<crate::RaftId> for pb::RaftId {
    fn from(value: crate::RaftId) -> Self {
        pb::RaftId {
            group: value.group,
            node: value.node,
        }
    }
}

impl From<crate::RequestVoteRequest> for pb::RequestVoteRequest {
    fn from(req: crate::RequestVoteRequest) -> Self {
        pb::RequestVoteRequest {
            term: req.term,
            candidate_id: Some(req.candidate_id.into()),
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
            request_id: req.request_id.into(),
        }
    }
}

impl From<crate::RequestVoteResponse> for pb::RequestVoteResponse {
    fn from(resp: crate::RequestVoteResponse) -> Self {
        pb::RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
            request_id: resp.request_id.into(),
        }
    }
}

impl From<crate::LogEntry> for pb::LogEntry {
    fn from(entry: crate::LogEntry) -> Self {
        pb::LogEntry {
            term: entry.term,
            index: entry.index,
            is_config: entry.is_config,
            command: entry.command.into(),
            client_request_id: entry.client_request_id.map(|id| id.into()),
        }
    }
}

impl From<crate::AppendEntriesRequest> for pb::AppendEntriesRequest {
    fn from(req: crate::AppendEntriesRequest) -> Self {
        pb::AppendEntriesRequest {
            term: req.term,
            leader_id: Some(req.leader_id.into()),
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.into_iter().map(|e| e.into()).collect(),
            leader_commit: req.leader_commit,
            request_id: req.request_id.into(),
        }
    }
}

impl From<crate::AppendEntriesResponse> for pb::AppendEntriesResponse {
    fn from(resp: crate::AppendEntriesResponse) -> Self {
        pb::AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            request_id: resp.request_id.into(),
            conflict_index: resp.conflict_index,
            conflict_term: resp.conflict_term,
            matched_index: resp.matched_index,
        }
    }
}

impl From<crate::JointConfig> for pb::JointConfig {
    fn from(value: crate::JointConfig) -> Self {
        pb::JointConfig {
            log_index: value.log_index,
            old_voters: value.old_voters.into_iter().map(|v| v.into()).collect(),
            new_voters: value.new_voters.into_iter().map(|v| v.into()).collect(),
            old_learners: value
                .old_learners
                .into_iter()
                .flat_map(|set| set.into_iter().map(Into::into))
                .collect(),
            new_learners: value
                .new_learners
                .into_iter()
                .flat_map(|set| set.into_iter().map(Into::into))
                .collect(),
        }
    }
}

impl From<crate::ClusterConfig> for pb::ClusterConfig {
    fn from(config: crate::ClusterConfig) -> Self {
        pb::ClusterConfig {
            epoch: config.epoch,
            log_index: config.log_index,
            voters: config.voters.into_iter().map(|v| v.into()).collect(),
            learners: config
                .learners
                .into_iter()
                .flat_map(|set| set.into_iter().map(Into::into))
                .collect(),
            joint: config.joint.map(|j| j.into()),
        }
    }
}

impl From<pb::JointConfig> for crate::JointConfig {
    fn from(value: pb::JointConfig) -> Self {
        crate::JointConfig {
            log_index: value.log_index,
            old_voters: value.old_voters.into_iter().map(|v| v.into()).collect(),
            new_voters: value.new_voters.into_iter().map(|v| v.into()).collect(),
            old_learners: value
                .old_learners
                .into_iter()
                .map(|id| Some(id.into()))
                .collect(),
            new_learners: value
                .new_learners
                .into_iter()
                .map(|id| Some(id.into()))
                .collect(),
        }
    }
}

impl From<pb::ClusterConfig> for crate::ClusterConfig {
    fn from(config: pb::ClusterConfig) -> Self {
        crate::ClusterConfig {
            epoch: config.epoch,
            log_index: config.log_index,
            voters: config.voters.into_iter().map(|v| v.into()).collect(),
            learners: config
                .learners
                .into_iter()
                .map(|id| Some(id.into()))
                .collect(),
            joint: config.joint.map(|j| j.into()),
        }
    }
}

impl From<crate::InstallSnapshotRequest> for pb::InstallSnapshotRequest {
    fn from(req: crate::InstallSnapshotRequest) -> Self {
        pb::InstallSnapshotRequest {
            term: req.term,
            data: req.data,
            is_probe: req.is_probe,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            leader_id: Some(req.leader_id.into()),
            request_id: req.request_id.into(),
            config: Some(req.config.into()),
        }
    }
}

impl From<crate::InstallSnapshotResponse> for pb::InstallSnapshotResponse {
    fn from(resp: crate::InstallSnapshotResponse) -> Self {
        pb::InstallSnapshotResponse {
            term: resp.term,
            request_id: resp.request_id.into(),
            state: resp.state.into(),
            error_message: resp.error_message,
        }
    }
}

impl From<crate::InstallSnapshotState> for i32 {
    fn from(value: crate::InstallSnapshotState) -> Self {
        match value {
            crate::InstallSnapshotState::Installing => 2,
            crate::InstallSnapshotState::Failed(msg) => 1,
            crate::InstallSnapshotState::Success => 3,
        }
    }
}

impl From<OutgoingMessage> for pb::RpcMessage {
    fn from(msg: OutgoingMessage) -> Self {
        match msg {
            OutgoingMessage::RequestVote { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::RequestVote(args.into())),
            },
            OutgoingMessage::RequestVoteResponse { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::RequestVoteResponse(args.into())),
            },
            OutgoingMessage::AppendEntries { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::AppendEntries(args.into())),
            },
            OutgoingMessage::AppendEntriesResponse { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::AppendEntriesResponse(args.into())),
            },
            OutgoingMessage::InstallSnapshot { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::InstallSnapshot(args.into())),
            },
            OutgoingMessage::InstallSnapshotResponse { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::InstallSnapshotResponse(
                    args.into(),
                )),
            },
        }
    }
}
// 反向转换实现
impl From<pb::RaftId> for crate::RaftId {
    fn from(value: pb::RaftId) -> Self {
        crate::RaftId {
            group: value.group,
            node: value.node,
        }
    }
}

impl From<pb::RequestVoteRequest> for Result<crate::RequestVoteRequest> {
    fn from(req: pb::RequestVoteRequest) -> Self {
        Ok(crate::RequestVoteRequest {
            term: req.term,
            candidate_id: match req.candidate_id.map(crate::RaftId::from) {
                Some(candidate_id) => candidate_id,
                None => {
                    return Err(anyhow::anyhow!("Missing candidate_id"));
                }
            },
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
            request_id: req.request_id.into(),
        })
    }
}

impl From<pb::RequestVoteResponse> for crate::RequestVoteResponse {
    fn from(resp: pb::RequestVoteResponse) -> Self {
        crate::RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
            request_id: resp.request_id.into(),
        }
    }
}

impl From<pb::LogEntry> for crate::LogEntry {
    fn from(entry: pb::LogEntry) -> Self {
        crate::LogEntry {
            term: entry.term,
            index: entry.index,
            is_config: entry.is_config,
            command: entry.command.into(),
            client_request_id: entry.client_request_id.map(|id| id.into()),
        }
    }
}

impl From<pb::AppendEntriesRequest> for Result<crate::AppendEntriesRequest> {
    fn from(req: pb::AppendEntriesRequest) -> Self {
        Ok(crate::AppendEntriesRequest {
            term: req.term,
            leader_id: match req.leader_id.map(crate::RaftId::from) {
                Some(leader_id) => leader_id,
                None => {
                    return Err(anyhow::anyhow!("Missing leader_id"));
                }
            },
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.into_iter().map(|e| e.into()).collect(),
            leader_commit: req.leader_commit,
            request_id: req.request_id.into(),
        })
    }
}

impl From<pb::AppendEntriesResponse> for crate::AppendEntriesResponse {
    fn from(resp: pb::AppendEntriesResponse) -> Self {
        crate::AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            request_id: resp.request_id.into(),
            conflict_index: resp.conflict_index,
            conflict_term: resp.conflict_term,
            matched_index: resp.matched_index,
        }
    }
}

impl From<pb::InstallSnapshotRequest> for Result<crate::InstallSnapshotRequest> {
    fn from(req: pb::InstallSnapshotRequest) -> Self {
        Ok(crate::InstallSnapshotRequest {
            term: req.term,
            data: req.data,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            request_id: req.request_id.into(),
            is_probe: req.is_probe,
            config: match req.config.map(crate::ClusterConfig::from) {
                Some(config) => config,
                None => {
                    return Err(anyhow::anyhow!("Missing config"));
                }
            },
            leader_id: match req.leader_id.map(crate::RaftId::from) {
                Some(leader_id) => leader_id,
                None => {
                    return Err(anyhow::anyhow!("Missing leader_id"));
                }
            },
        })
    }
}

impl From<pb::InstallSnapshotResponse> for crate::InstallSnapshotResponse {
    fn from(resp: pb::InstallSnapshotResponse) -> Self {
        crate::InstallSnapshotResponse {
            term: resp.term,
            request_id: resp.request_id.into(),
            state: resp.state.into(),
            error_message: resp.error_message,
        }
    }
}

impl From<i32> for crate::InstallSnapshotState {
    fn from(value: i32) -> Self {
        match value {
            2 => crate::InstallSnapshotState::Installing,
            1 => crate::InstallSnapshotState::Failed(String::new()), // 反序列化时无法恢复msg
            3 => crate::InstallSnapshotState::Success,
            _ => crate::InstallSnapshotState::Failed(String::from("Unknown state")),
        }
    }
}

impl From<pb::RpcMessage> for Result<OutgoingMessage> {
    fn from(msg: pb::RpcMessage) -> Self {
        match msg.message {
            Some(inner_msg) => match inner_msg {
                rpc_message::Message::RequestVote(request_vote_request) => {
                    Ok(OutgoingMessage::RequestVote {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: match request_vote_request.into() {
                            Ok(args) => args,
                            Err(e) => return Err(e),
                        },
                    })
                }
                rpc_message::Message::RequestVoteResponse(request_vote_response) => {
                    Ok(OutgoingMessage::RequestVoteResponse {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: request_vote_response.into(),
                    })
                }
                rpc_message::Message::AppendEntries(append_entries_request) => {
                    Ok(OutgoingMessage::AppendEntries {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: match append_entries_request.into() {
                            Ok(args) => args,
                            Err(e) => return Err(e),
                        },
                    })
                }
                rpc_message::Message::AppendEntriesResponse(append_entries_response) => {
                    Ok(OutgoingMessage::AppendEntriesResponse {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: append_entries_response.into(),
                    })
                }
                rpc_message::Message::InstallSnapshot(install_snapshot_request) => {
                    Ok(OutgoingMessage::InstallSnapshot {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: match install_snapshot_request.into() {
                            Ok(args) => args,
                            Err(e) => return Err(e),
                        },
                    })
                }
                rpc_message::Message::InstallSnapshotResponse(install_snapshot_response) => {
                    Ok(OutgoingMessage::InstallSnapshotResponse {
                        from: match msg.from {
                            Some(from) => from.into(),
                            None => Err(anyhow::anyhow!("Missing from field"))?,
                        },
                        target: match msg.target {
                            Some(target) => target.into(),
                            None => Err(anyhow::anyhow!("Missing target field"))?,
                        },
                        args: install_snapshot_response.into(),
                    })
                }
            },
            None => {
                return Err(anyhow::anyhow!("Missing message"));
            }
        }
    }
}
