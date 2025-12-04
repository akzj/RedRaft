//! Leader transfer handling for Raft state machine

use std::time::Instant;

use tracing::error;

use super::RaftState;
use crate::error::ClientError;
use crate::event::Role;
use crate::message::{AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest};
use crate::types::{RaftId, RequestId};

impl RaftState {
    /// 处理领导权转移请求
    pub async fn handle_leader_transfer(&mut self, target: RaftId, request_id: RequestId) {
        if self.role != Role::Leader {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::NotLeader(self.leader_id.clone())),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if self.leader_transfer_target.is_some() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer already in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if !self.config.voters_contains(&target) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "target node not in cluster"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        if target == self.id {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "cannot transfer to self"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        // 记录转移状态
        self.leader_transfer_target = Some(target.clone());
        self.leader_transfer_request_id = Some(request_id);
        self.leader_transfer_start_time = Some(Instant::now());

        // 立即发送心跳重置目标节点选举计时器
        self.send_heartbeat_to(target).await;

        // 设置超时定时器
        self.leader_transfer_timer = Some(
            self.callbacks
                .set_leader_transfer_timer(&self.id, self.leader_transfer_timeout),
        );
    }

    /// 发送心跳到特定节点
    pub(crate) async fn send_heartbeat_to(&mut self, target: RaftId) {
        let prev_log_index = self.next_index[&target] - 1;
        let prev_log_term = if prev_log_index == 0 {
            0
        } else if prev_log_index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            self.callbacks
                .get_log_term(&self.id, prev_log_index)
                .await
                .unwrap_or(0)
        };

        let req = AppendEntriesRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries: vec![],
            leader_commit: self.commit_index,
            request_id: RequestId::new(),
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_append_entries_request(&self.id, &target, req)
                    .await,
                "send_append_entries_request",
                None,
            )
            .await;
    }

    /// 处理目标节点的日志响应
    pub(crate) async fn process_leader_transfer_target_response(
        &mut self,
        peer: RaftId,
        reply: AppendEntriesResponse,
    ) {
        if reply.success {
            let target_match_index = self.match_index.get(&peer).copied().unwrap_or(0);
            let last_log_index = self.get_last_log_index();

            if target_match_index >= last_log_index {
                self.transfer_leadership_to(peer).await;
                return;
            }
        }

        self.send_heartbeat_to(peer).await;
    }

    /// 转移领导权给目标节点
    pub(crate) async fn transfer_leadership_to(&mut self, target: RaftId) {
        let req = RequestVoteRequest {
            term: self.current_term + 1,
            candidate_id: target.clone(),
            last_log_index: self.get_last_log_index(),
            last_log_term: self.get_last_log_term(),
            request_id: RequestId::new(),
        };

        if let Err(err) = self
            .callbacks
            .send_request_vote_request(&self.id, &target, req)
            .await
        {
            error!(
                "Failed to send request vote request to {} during leader transfer: {}",
                target,
                err
            );
        }

        if let Some(request_id) = self.leader_transfer_request_id.take() {
            let _ = self
                .callbacks
                .client_response(&self.id, request_id, Ok(0))
                .await
                .inspect_err(|err| {
                    error!(
                        "Failed to send client response for leader transfer completion: {}",
                        err
                    );
                });
        }

        self.leader_transfer_target = None;
        self.leader_transfer_start_time = None;
    }

    /// 处理领导权转移超时
    pub(crate) async fn handle_leader_transfer_timeout(&mut self) {
        if let (Some(_target), Some(request_id)) = (
            self.leader_transfer_target.take(),
            self.leader_transfer_request_id.take(),
        ) {
            self.leader_transfer_start_time = None;

            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "leader transfer timeout"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
        }
    }
}

