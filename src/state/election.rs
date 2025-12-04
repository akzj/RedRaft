//! Election handling for Raft state machine

use std::collections::HashSet;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, error, info, warn};

use super::RaftState;
use crate::event::Role;
use crate::message::{PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse};
use crate::types::{RaftId, RequestId};

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

        // 检查是否启用 Pre-Vote
        if self.options.pre_vote_enabled {
            self.start_pre_vote().await;
        } else {
            self.start_real_election().await;
        }
    }

    /// 开始 Pre-Vote 阶段（不增加 term）
    pub(crate) async fn start_pre_vote(&mut self) {
        info!(
            "Node {} starting pre-vote for prospective term {}",
            self.id,
            self.current_term + 1
        );

        // 生成 Pre-Vote ID 并初始化跟踪状态
        let pre_vote_id = RequestId::new();
        self.current_pre_vote_id = Some(pre_vote_id);
        self.pre_vote_votes.clear();
        self.pre_vote_votes.insert(self.id.clone(), true); // 自己投自己

        // 重置选举定时器
        self.reset_election().await;

        // 获取日志信息
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();

        let req = PreVoteRequest {
            term: self.current_term + 1, // 使用 prospective term，但不实际递增
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: pre_vote_id,
        };

        // 发送 Pre-Vote 请求
        for peer in self.config.get_effective_voters() {
            if *peer != self.id {
                let target = peer;
                let args = req.clone();

                let result = self
                    .error_handler
                    .handle(
                        self.callbacks
                            .send_pre_vote_request(&self.id, target, args)
                            .await,
                        "send_pre_vote_request",
                        Some(target),
                    )
                    .await;

                if result.is_none() {
                    warn!(
                        "Failed to send PreVote to {}, will retry or ignore based on error severity",
                        target
                    );
                }
            }
        }

        // 单节点集群：立即检查结果
        self.check_pre_vote_result().await;
    }

    /// 开始真实选举（递增 term）
    pub(crate) async fn start_real_election(&mut self) {
        info!(
            "Node {} starting real election for term {}",
            self.id,
            self.current_term + 1
        );

        // 清理 Pre-Vote 状态
        self.current_pre_vote_id = None;
        self.pre_vote_votes.clear();

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
                            .send_request_vote_request(&self.id, target, args)
                            .await,
                        "send_request_vote_request",
                        Some(target),
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

        // 单节点集群：立即检查结果
        self.check_election_result().await;
    }

    /// 处理 Pre-Vote 请求
    pub(crate) async fn handle_pre_vote_request(&mut self, sender: RaftId, request: PreVoteRequest) {
        if sender != request.candidate_id {
            warn!(
                "Node {} received pre-vote request from {}, but candidate is {}",
                self.id, sender, request.candidate_id
            );
            return;
        }

        let mut vote_granted = false;

        // Pre-Vote 条件检查（注意：不修改自身状态！）
        // 1. prospective term >= 当前 term
        // 2. 日志必须足够新
        // 3. 最近没有收到 Leader 心跳（防止干扰正常运行的集群）
        let leader_active = self.last_heartbeat.elapsed() < self.election_timeout_min;

        if request.term >= self.current_term {
            if leader_active {
                info!(
                    "Node {} rejecting pre-vote for {} because leader is still active (last heartbeat: {:?} ago)",
                    self.id, request.candidate_id, self.last_heartbeat.elapsed()
                );
            } else {
                let log_ok = self
                    .is_log_up_to_date(request.last_log_index, request.last_log_term)
                    .await;

                if log_ok {
                    vote_granted = true;
                    info!(
                        "Node {} granting pre-vote to {} for prospective term {}",
                        self.id, request.candidate_id, request.term
                    );
                } else {
                    info!(
                        "Node {} rejecting pre-vote for {}, logs not up-to-date",
                        self.id, request.candidate_id
                    );
                }
            }
        } else {
            info!(
                "Node {} rejecting pre-vote for {} in term {} (prospective term {} < current term {})",
                self.id, request.candidate_id, request.term, request.term, self.current_term
            );
        }

        // 发送响应（使用自己的 current_term）
        let resp = PreVoteResponse {
            term: self.current_term,
            vote_granted,
            request_id: request.request_id,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_pre_vote_response(&self.id, &request.candidate_id, resp)
                    .await,
                "send_pre_vote_response",
                Some(&request.candidate_id),
            )
            .await;
    }

    /// 处理 Pre-Vote 响应
    pub(crate) async fn handle_pre_vote_response(&mut self, peer: RaftId, response: PreVoteResponse) {
        // 检查是否是当前 Pre-Vote 轮次
        if self.current_pre_vote_id != Some(response.request_id) {
            debug!(
                "Node {} ignoring stale pre-vote response from {} (expected {:?}, got {:?})",
                self.id, peer, self.current_pre_vote_id, response.request_id
            );
            return;
        }

        // 过滤无效投票者
        if !self.config.voters_contains(&peer) {
            warn!(
                "Node {}: received pre-vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // 如果发现更高的 term，不需要降级（因为 Pre-Vote 不增加 term）
        // 但需要更新对集群状态的认知
        if response.term > self.current_term {
            info!(
                "Node {} discovered higher term {} from {} during pre-vote (current term {})",
                self.id, response.term, peer, self.current_term
            );
            // 放弃当前 Pre-Vote
            self.current_pre_vote_id = None;
            self.pre_vote_votes.clear();
            return;
        }

        // 记录 Pre-Vote 结果
        self.pre_vote_votes.insert(peer.clone(), response.vote_granted);
        info!(
            "Node {} received pre-vote response from {}: granted={}",
            self.id, peer, response.vote_granted
        );

        // 检查是否获得多数
        self.check_pre_vote_result().await;
    }

    /// 检查 Pre-Vote 结果
    pub(crate) async fn check_pre_vote_result(&mut self) {
        if self.current_pre_vote_id.is_none() {
            return;
        }

        let granted_votes: HashSet<_> = self
            .pre_vote_votes
            .iter()
            .filter_map(|(id, &granted)| if granted { Some(id.clone()) } else { None })
            .collect();

        let win = self.config.majority(&granted_votes);
        if win {
            info!(
                "Node {} won pre-vote with {} votes, starting real election",
                self.id,
                granted_votes.len()
            );
            // Pre-Vote 成功，开始真实选举
            self.start_real_election().await;
        }
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
            error!(
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

