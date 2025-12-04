//! Election handling for Raft state machine

use std::collections::HashSet;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, info, warn};

use super::RaftState;
use crate::event::Role;
use crate::message::{RequestVoteRequest, RequestVoteResponse};
use crate::types::RequestId;

impl RaftState {
    /// 处理选举超时
    pub(crate) async fn handle_election_timeout(&mut self) {
        if self.role == Role::Leader {
            info!(target: "raft", "Node {} is the leader and will not start a new election", self.id);
            return;
        }

        // Learner 不参与选举
        if !self.config.voters_contains(&self.id) {
            warn!("Node {} is a Learner and cannot start an election", self.id);
            return;
        }

        info!(
            "Node {} starting election for term {}",
            self.id,
            self.current_term + 1
        );

        // 切换为 Candidate 并递增任期
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());

        // 持久化状态变更
        self.persist_hard_state().await;

        // 重置选举定时器
        self.reset_election().await;

        // 生成新选举ID并初始化跟踪状态
        let election_id = RequestId::new();
        self.current_election_id = Some(election_id);
        self.election_votes.clear();
        self.election_votes.insert(self.id.clone(), true);
        self.election_max_term = self.current_term;

        // 获取日志信息用于选举
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();

        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: election_id,
        };

        // 发送投票请求
        for peer in self.config.get_effective_voters() {
            if *peer != self.id {
                let target = peer;
                let args = req.clone();

                let result = self
                    .error_handler
                    .handle(
                        self.callbacks
                            .send_request_vote_request(&self.id, &target, args)
                            .await,
                        "send_request_vote_request",
                        Some(&target),
                    )
                    .await;

                if result.is_none() {
                    warn!(
                        "Failed to send RequestVote to {}, will retry or ignore based on error severity",
                        target
                    );
                }
            }
        }

        // 通知上层应用状态变更
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .on_state_changed(&self.id, Role::Candidate)
                    .await,
                "state_changed",
                None,
            )
            .await;
    }

    /// 处理投票请求
    pub(crate) async fn handle_request_vote(
        &mut self,
        sender: crate::types::RaftId,
        request: RequestVoteRequest,
    ) {
        if sender != request.candidate_id {
            warn!(
                "Node {} received vote request from {}, but candidate is {}",
                self.id, sender, request.candidate_id
            );
            return;
        }

        // 处理更高任期
        if request.term > self.current_term {
            info!(
                "Node {} stepping down to Follower, updating term from {} to {}",
                self.id, self.current_term, request.term
            );
            self.step_down_to_follower(Some(request.term)).await;
        }

        // 决定是否投票
        let mut vote_granted = false;

        if request.term >= self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(request.candidate_id.clone()))
        {
            let log_ok = self
                .is_log_up_to_date(request.last_log_index, request.last_log_term)
                .await;

            if log_ok {
                self.voted_for = Some(request.candidate_id.clone());
                vote_granted = true;
                self.reset_election().await;
                info!(
                    "Node {} granting vote to {} for term {}",
                    self.id, request.candidate_id, self.current_term
                );

                self.persist_hard_state().await;
            } else {
                info!(
                    "Node {} rejecting vote for {}, logs not up-to-date",
                    self.id, request.candidate_id
                );
            }
        } else {
            info!(
                "Node {} rejecting vote for {} in term {}, already voted for {:?} or term mismatch (args.term: {}, self.current_term: {})",
                self.id,
                request.candidate_id,
                self.current_term,
                self.voted_for,
                request.term,
                self.current_term
            );
        }

        // 发送响应
        let resp = RequestVoteResponse {
            term: self.current_term,
            vote_granted,
            request_id: request.request_id,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_request_vote_response(&self.id, &request.candidate_id, resp)
                    .await,
                "send_request_vote_response",
                Some(&request.candidate_id),
            )
            .await;
    }

    /// 检查日志是否足够新
    pub(crate) async fn is_log_up_to_date(
        &mut self,
        candidate_last_index: u64,
        candidate_last_term: u64,
    ) -> bool {
        let self_last_term = self.get_last_log_term();
        let self_last_index = self.get_last_log_index();

        candidate_last_term > self_last_term
            || (candidate_last_term == self_last_term && candidate_last_index >= self_last_index)
    }

    /// 处理投票响应
    pub(crate) async fn handle_request_vote_response(
        &mut self,
        peer: crate::types::RaftId,
        response: RequestVoteResponse,
    ) {
        info!("node {}: received vote response: {:?}", self.id, response);
        if self.role != Role::Candidate || self.current_election_id != Some(response.request_id) {
            return;
        }

        // 过滤无效投票者
        if !self.config.voters_contains(&peer) {
            warn!(
                "node {}: received vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // 处理更高任期
        if response.term > self.current_term {
            info!(
                "Stepping down from candidate due to higher term {} from peer {} (current term {})",
                response.term, peer, self.current_term
            );
            self.step_down_to_follower(Some(response.term)).await;
            self.election_votes.clear();
            self.reset_election().await;
            return;
        }

        // 记录投票结果
        if response.term == self.current_term {
            self.election_votes.insert(peer, response.vote_granted);
        }

        // 检查是否赢得选举
        self.check_election_result().await;
    }

    /// 检查选举结果
    pub(crate) async fn check_election_result(&mut self) {
        info!(
            "Node {}: check_election_result, votes: {:?}, current_term: {}, role: {:?}, election_id: {:?}",
            self.id, self.election_votes, self.current_term, self.role, self.current_election_id
        );

        let granted_votes: HashSet<_> = self
            .election_votes
            .iter()
            .filter_map(|(id, &granted)| if granted { Some(id.clone()) } else { None })
            .collect();

        let win = self.config.majority(&granted_votes);
        if win {
            info!(
                "Node {} becomes leader for term {}",
                self.id, self.current_term
            );
            self.become_leader().await;
        }
    }

    /// 重置心跳定时器
    pub(crate) async fn reset_heartbeat_timer(&mut self) {
        if let Some(timer_id) = self.heartbeat_interval_timer_id {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        self.heartbeat_interval_timer_id = Some(
            self.callbacks
                .set_heartbeat_timer(&self.id, self.heartbeat_interval),
        );
    }

    /// 成为 Leader
    pub(crate) async fn become_leader(&mut self) {
        warn!(
            "Node {} becoming leader for term {} (previous role: {:?})",
            self.id, self.current_term, self.role
        );

        self.role = Role::Leader;
        self.current_election_id = None;
        self.leader_id = None;

        // 初始化复制状态
        let last_log_index = self.get_last_log_index();
        self.next_index.clear();
        self.match_index.clear();
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        // 收集所有需要管理的节点
        let all_peers: Vec<_> = self
            .config
            .get_all_nodes()
            .into_iter()
            .filter(|peer| *peer != self.id)
            .collect();

        for peer in &all_peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }

        self.reset_heartbeat_timer().await;

        if let Err(err) = self
            .callbacks
            .on_state_changed(&self.id, Role::Leader)
            .await
        {
            log::error!(
                "Failed to change state to Leader for node {}: {}",
                self.id,
                err
            );
        }

        // 立即发送心跳
        self.broadcast_append_entries().await;

        // 启动日志应用定时器
        self.adjust_apply_interval().await;
    }

    /// 重置选举定时器
    pub(crate) async fn reset_election(&mut self) {
        self.current_election_id = None;

        let min_ms = self.election_timeout_min.as_millis() as u64;
        let max_ms = self.election_timeout_max.as_millis() as u64;

        let (actual_min, actual_max) = if min_ms < max_ms {
            (min_ms, max_ms)
        } else {
            warn!(
                "Node {} has invalid election timeout range (min: {:?}, max: {:?}), using default range",
                self.id, self.election_timeout_min, self.election_timeout_max
            );
            (150, 300)
        };

        let range = actual_max - actual_min + 1;
        let mut rng = rand::rng();
        let random_offset = rng.random_range(0..range);

        let election_timeout = Duration::from_millis(actual_min + random_offset);

        debug!(
            "Node {} reset election timer to {:?} (range: {:?}-{:?})",
            self.id,
            election_timeout,
            Duration::from_millis(actual_min),
            Duration::from_millis(actual_max)
        );

        if let Some(timer_id) = self.election_timer.take() {
            self.callbacks.del_timer(&self.id, timer_id)
        }

        self.election_timer = Some(
            self.callbacks
                .set_election_timer(&self.id, election_timeout),
        );
    }
}

