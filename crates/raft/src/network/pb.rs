use anyhow::{anyhow, Result};

use crate::network::{pb, OutgoingMessage};

tonic::include_proto!("pb");

/// Helper function to extract RaftId from Option, returning an error if None
fn extract_raft_id(opt: Option<pb::RaftId>, field_name: &str) -> Result<crate::RaftId> {
    opt.map(crate::RaftId::from)
        .ok_or_else(|| anyhow!("Missing {} field", field_name))
}

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
            command: entry.command,
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
            snapshot_request_id: req.snapshot_request_id.into(),
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
            crate::InstallSnapshotState::Failed(_msg) => 1,
            crate::InstallSnapshotState::Success => 3,
        }
    }
}

// Pre-Vote conversion
impl From<crate::message::PreVoteRequest> for pb::PreVoteRequest {
    fn from(req: crate::message::PreVoteRequest) -> Self {
        pb::PreVoteRequest {
            term: req.term,
            candidate_id: Some(req.candidate_id.into()),
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
            request_id: req.request_id.into(),
        }
    }
}

impl From<crate::message::PreVoteResponse> for pb::PreVoteResponse {
    fn from(resp: crate::message::PreVoteResponse) -> Self {
        pb::PreVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
            request_id: resp.request_id.into(),
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
            OutgoingMessage::PreVote { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::PreVote(args.into())),
            },
            OutgoingMessage::PreVoteResponse { from, target, args } => pb::RpcMessage {
                from: Some(from.into()),
                target: Some(target.into()),
                message: Some(pb::rpc_message::Message::PreVoteResponse(args.into())),
            },
        }
    }
}
// Reverse conversion implementation
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
            command: entry.command,
            client_request_id: entry.client_request_id.map(|id| id.into()),
        }
    }
}

impl From<pb::AppendEntriesRequest> for Result<crate::AppendEntriesRequest> {
    fn from(req: pb::AppendEntriesRequest) -> Self {
        Ok(crate::AppendEntriesRequest {
            term: req.term,
            leader_id: extract_raft_id(req.leader_id, "leader_id")?,
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
            snapshot_request_id: req.snapshot_request_id.into(),
            is_probe: req.is_probe,
            config: req
                .config
                .map(crate::ClusterConfig::from)
                .ok_or_else(|| anyhow!("Missing config"))?,
            leader_id: extract_raft_id(req.leader_id, "leader_id")?,
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
            1 => crate::InstallSnapshotState::Failed(
                "Snapshot installation failed (error message not available in protobuf)"
                    .to_string(),
            ),
            3 => crate::InstallSnapshotState::Success,
            _ => crate::InstallSnapshotState::Failed(format!("Unknown snapshot state: {}", value)),
        }
    }
}

// Pre-Vote reverse conversion
impl From<pb::PreVoteRequest> for Result<crate::message::PreVoteRequest> {
    fn from(req: pb::PreVoteRequest) -> Self {
        Ok(crate::message::PreVoteRequest {
            term: req.term,
            candidate_id: extract_raft_id(req.candidate_id, "candidate_id")?,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
            request_id: req.request_id.into(),
        })
    }
}

impl From<pb::PreVoteResponse> for crate::message::PreVoteResponse {
    fn from(resp: pb::PreVoteResponse) -> Self {
        crate::message::PreVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
            request_id: resp.request_id.into(),
        }
    }
}

impl From<pb::RpcMessage> for Result<OutgoingMessage> {
    fn from(msg: pb::RpcMessage) -> Self {
        let from = extract_raft_id(msg.from, "from")?;
        let target = extract_raft_id(msg.target, "target")?;

        match msg.message {
            Some(inner_msg) => match inner_msg {
                rpc_message::Message::RequestVote(request_vote_request) => {
                    Ok(OutgoingMessage::RequestVote {
                        from,
                        target,
                        args: <pb::RequestVoteRequest as Into<
                            Result<crate::RequestVoteRequest>,
                        >>::into(request_vote_request)?,
                    })
                }
                rpc_message::Message::RequestVoteResponse(request_vote_response) => {
                    Ok(OutgoingMessage::RequestVoteResponse {
                        from,
                        target,
                        args: request_vote_response.into(),
                    })
                }
                rpc_message::Message::AppendEntries(append_entries_request) => {
                    Ok(OutgoingMessage::AppendEntries {
                        from,
                        target,
                        args: <pb::AppendEntriesRequest as Into<
                            Result<crate::AppendEntriesRequest>,
                        >>::into(append_entries_request)?,
                    })
                }
                rpc_message::Message::AppendEntriesResponse(append_entries_response) => {
                    Ok(OutgoingMessage::AppendEntriesResponse {
                        from,
                        target,
                        args: append_entries_response.into(),
                    })
                }
                rpc_message::Message::InstallSnapshot(install_snapshot_request) => {
                    Ok(OutgoingMessage::InstallSnapshot {
                        from,
                        target,
                        args: <pb::InstallSnapshotRequest as Into<
                            Result<crate::InstallSnapshotRequest>,
                        >>::into(install_snapshot_request)?,
                    })
                }
                rpc_message::Message::InstallSnapshotResponse(install_snapshot_response) => {
                    Ok(OutgoingMessage::InstallSnapshotResponse {
                        from,
                        target,
                        args: install_snapshot_response.into(),
                    })
                }
                rpc_message::Message::PreVote(pre_vote_request) => Ok(OutgoingMessage::PreVote {
                    from,
                    target,
                    args:
                        <pb::PreVoteRequest as Into<Result<crate::message::PreVoteRequest>>>::into(
                            pre_vote_request,
                        )?,
                }),
                rpc_message::Message::PreVoteResponse(pre_vote_response) => {
                    Ok(OutgoingMessage::PreVoteResponse {
                        from,
                        target,
                        args: pre_vote_response.into(),
                    })
                }
            },
            None => Err(anyhow!("Missing message")),
        }
    }
}
