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

impl From<crate::InstallSnapshotRequest> for pb::InstallSnapshotRequest {
    fn from(req: crate::InstallSnapshotRequest) -> Self {
        pb::InstallSnapshotRequest {
            term: req.term,
            leader_id: Some(req.leader_id.into()),
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data,
            request_id: req.request_id.into(),
            is_probe: req.is_probe,
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
