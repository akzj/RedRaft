//! Log replication handling for Raft state machine

use std::time::Instant;

use tracing::{debug, error, info, warn};

use super::RaftState;
use crate::event::Role;
use crate::message::{AppendEntriesRequest, AppendEntriesResponse};
use crate::types::{RaftId, RequestId};

impl RaftState {
    /// 处理心跳超时
    pub(crate) async fn handle_heartbeat_timeout(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        // 执行高效的超时检查
        self.pipeline.periodic_timeout_check();

        self.broadcast_append_entries().await;

        // 定期检查联合配置状态
        if self.config.is_joint() {
            self.check_joint_exit_condition().await;
        }
    }

    /// 广播 AppendEntries 请求
    pub(crate) async fn broadcast_append_entries(&mut self) {
        let current_term = self.current_term;
        let leader_id = self.id.clone();
        let leader_commit = self.commit_index;
        let last_log_index = self.get_last_log_index();
        let now = Instant::now();

        self.reset_heartbeat_timer().await;
        // 检查并执行到期的快照探测计划
        self.process_pending_probes(now).await;

        // 向所有节点发送日志或探测消息
        let peers = self.get_effective_peers();
        for peer in peers {
            if peer == self.id {
                continue;
            }

            // 检查Follower是否正在安装快照
            if let Some(state) = self.follower_snapshot_states.get(&peer) {
                match state {
                    crate::message::InstallSnapshotState::Installing => {
                        continue;
                    }
                    crate::message::InstallSnapshotState::Failed(_) => {
                        self.send_snapshot_to(peer.clone()).await;
                        continue;
                    }
                    _ => {}
                }
            }

            // 检查InFlight请求限制
            if !self.pipeline.can_send_to_peer(&peer) {
                debug!("Peer {} has too many inflight requests, skipping", peer);
                continue;
            }

            // 检查是否需要发送快照
            let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
            if next_idx <= self.last_snapshot_index {
                self.send_snapshot_to(peer.clone()).await;
                continue;
            }

            // 构造AppendEntries请求
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index == 0 {
                0
            } else if prev_log_index <= self.last_snapshot_index {
                self.last_snapshot_term
            } else {
                match self
                    .error_handler
                    .handle(
                        self.callbacks.get_log_term(&self.id, prev_log_index).await,
                        "get_log_term",
                        Some(&peer),
                    )
                    .await
                {
                    Some(term) => term,
                    None => continue,
                }
            };

            // 使用反馈控制的批次大小
            let batch_size = self.pipeline.get_adaptive_batch_size(&peer);
            let high = std::cmp::min(next_idx + batch_size, last_log_index + 1);
            let entries = match self
                .error_handler
                .handle(
                    self.callbacks
                        .get_log_entries(&self.id, next_idx, high)
                        .await,
                    "get_log_entries",
                    Some(&peer),
                )
                .await
            {
                Some(entries) => entries,
                None => continue,
            };

            let entries_len = entries.len();
            let request_id = RequestId::new();
            let req = AppendEntriesRequest {
                term: current_term,
                leader_id: leader_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                request_id,
            };

            if entries_len > 0 {
                info!(
                    "Leader {} sending {} entries to {} (batch_size={}): prev_log_index={}, next_index={} -> {}",
                    self.id,
                    entries_len,
                    peer,
                    batch_size,
                    prev_log_index,
                    next_idx,
                    next_idx + entries_len as u64
                );

                self.pipeline.track_inflight_request(
                    &peer,
                    request_id,
                    next_idx + entries_len as u64,
                    now,
                );
            }

            if let Err(err) = self
                .callbacks
                .send_append_entries_request(&self.id, &peer, req)
                .await
            {
                warn!(
                    "node {}: send append entries request failed: {}",
                    self.id, err
                );
                self.pipeline.remove_inflight_request(&peer, request_id);
            } else {
                let new_next_idx = next_idx + entries_len as u64;
                self.next_index.insert(peer.clone(), new_next_idx);
                debug!(
                    "Leader {} optimistically updated next_index for {} to {}",
                    self.id, peer, new_next_idx
                );
            }
        }
    }

    /// 处理 AppendEntries 请求
    pub(crate) async fn handle_append_entries_request(
        &mut self,
        sender: RaftId,
        request: AppendEntriesRequest,
    ) {
        if sender != request.leader_id {
            warn!(
                "Node {} received AppendEntries from {}, but leader is {}",
                self.id, sender, request.leader_id
            );
            return;
        }

        if !request.entries.is_empty() {
            info!(
                "Node {} received {} entries from {} (term {}, prev_log_index {})",
                self.id,
                request.entries.len(),
                request.leader_id,
                request.term,
                request.prev_log_index
            );
        }

        let mut success;
        let mut conflict_index = None;
        let mut conflict_term = None;
        let mut matched_index = self.get_last_log_index();

        // 处理更低任期的请求
        if request.term < self.current_term {
            warn!(
                "Node {} rejecting AppendEntries from {} (term {}) - local term is {}",
                self.id, request.leader_id, request.term, self.current_term
            );

            conflict_term = if request.prev_log_index <= self.last_snapshot_index {
                Some(self.last_snapshot_term)
            } else {
                self.error_handler
                    .handle(
                        self.callbacks
                            .get_log_term(&self.id, request.prev_log_index)
                            .await,
                        "get_log_term",
                        Some(&request.leader_id),
                    )
                    .await
            };
            conflict_index = Some(self.get_last_log_index() + 1);

            let resp = AppendEntriesResponse {
                term: self.current_term,
                success: false,
                conflict_index,
                conflict_term,
                request_id: request.request_id,
                matched_index,
            };

            info!(
                "Node {} sending rejection response to {} with higher term {} (request term was {})",
                self.id, request.leader_id, self.current_term, request.term
            );

            let _ = self
                .error_handler
                .handle(
                    self.callbacks
                        .send_append_entries_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_append_entries_response",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 切换为Follower并重置状态
        if self.role != Role::Follower || self.leader_id.as_ref() != Some(&request.leader_id) {
            info!(
                "Node {} recognizing {} as leader for term {}",
                self.id, request.leader_id, request.term
            );
        }

        let was_candidate = self.role == Role::Candidate;
        self.role = Role::Follower;
        self.leader_id = Some(request.leader_id.clone());

        if request.term > self.current_term {
            info!(
                "Node {} updating term from {} to {}",
                self.id, self.current_term, request.term
            );
            self.current_term = request.term;
            self.voted_for = None;
            self.persist_hard_state().await;
        } else if was_candidate && request.term == self.current_term {
            self.voted_for = None;
            self.persist_hard_state().await;
        }
        self.last_heartbeat = Instant::now();
        self.reset_election().await;

        // 日志连续性检查 - 缓存本地日志 term 避免重复查询
        let local_prev_log_term: Option<u64> = if request.prev_log_index == 0 {
            Some(0) // index 0 的 term 是 0
        } else if request.prev_log_index <= self.last_snapshot_index {
            Some(self.last_snapshot_term)
        } else {
            self.error_handler
                .handle(
                    self.callbacks
                        .get_log_term(&self.id, request.prev_log_index)
                        .await,
                    "get_log_term",
                    Some(&request.leader_id),
                )
                .await
        };

        let prev_log_ok = match local_prev_log_term {
            // 对于 index 0，local_prev_log_term 是 Some(0)，
            // 需要验证 request.prev_log_term 也是 0（Raft 协议要求）
            Some(local_term) => local_term == request.prev_log_term,
            None => {
                warn!(
                    "Node {} failed to get local log term for index {} during consistency check, rejecting AppendEntries",
                    self.id, request.prev_log_index
                );
                false
            }
        };

        if !prev_log_ok {
            warn!(
                "Node {} rejecting AppendEntries from {} - log inconsistency at index {} (leader: {}, local: {:?})",
                self.id,
                request.leader_id,
                request.prev_log_index,
                request.prev_log_term,
                local_prev_log_term
            );

            let local_last_log_index = self.get_last_log_index();
            conflict_index = if request.prev_log_index > local_last_log_index {
                Some(local_last_log_index + 1)
            } else {
                Some(request.prev_log_index)
            };

            // 复用已查询的 local_prev_log_term
            conflict_term = local_prev_log_term;

            let resp = AppendEntriesResponse {
                success: false,
                conflict_index,
                conflict_term,
                matched_index,
                term: self.current_term,
                request_id: request.request_id,
            };
            let _ = self
                .error_handler
                .handle(
                    self.callbacks
                        .send_append_entries_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_append_entries_response",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        success = true;

        // 截断冲突日志并追加新日志
        if request.prev_log_index < self.get_last_log_index() {
            let truncate_from_index = request.prev_log_index + 1;

            if truncate_from_index <= self.commit_index {
                warn!(
                    "Node {} rejecting AppendEntries from {} - attempt to truncate committed logs (truncate_from: {}, commit_index: {})",
                    self.id, request.leader_id, truncate_from_index, self.commit_index
                );

                let resp = AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    conflict_index: Some(self.commit_index + 1),
                    conflict_term: None,
                    request_id: request.request_id,
                    matched_index: self.get_last_log_index(),
                };
                let _ = self
                    .error_handler
                    .handle(
                        self.callbacks
                            .send_append_entries_response(&self.id, &request.leader_id, resp)
                            .await,
                        "send_append_entries_response",
                        Some(&request.leader_id),
                    )
                    .await;
                return;
            }

            info!(
                "Node {} truncating log suffix from index {} (request_id: {:?}, leader: {}, prev_log_index: {}, entries_count: {})",
                self.id,
                truncate_from_index,
                request.request_id,
                request.leader_id,
                request.prev_log_index,
                request.entries.len()
            );
            let truncate_result = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .truncate_log_suffix(&self.id, truncate_from_index)
                        .await,
                    "truncate_log_suffix",
                    None,
                )
                .await;
            if !truncate_result {
                error!("Node {} failed to truncate log suffix", self.id);
                success = false;
            }
        }

        // 验证并追加新日志
        if success && !request.entries.is_empty() {
            if let Some(first_entry) = request.entries.first() {
                if first_entry.index != request.prev_log_index + 1 {
                    error!(
                        "Node {} rejecting AppendEntries - log entry index discontinuity: expected {}, got {}",
                        self.id,
                        request.prev_log_index + 1,
                        first_entry.index
                    );
                    success = false;
                    conflict_index = Some(request.prev_log_index + 1);
                    conflict_term = None;
                } else {
                    for entry in &request.entries {
                        if entry.client_request_id.is_some() && entry.index <= self.commit_index {
                            error!(
                                "Node {} rejecting AppendEntries - attempt to overwrite committed log entry {} with client request",
                                self.id, entry.index
                            );
                            success = false;
                            conflict_index = Some(entry.index);
                            conflict_term = Some(entry.term);
                            break;
                        }
                    }
                }
            }

            if success {
                info!(
                    "Node {} appending {} log entries starting from index {}",
                    self.id,
                    request.entries.len(),
                    request.prev_log_index + 1
                );
                let append_result = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .append_log_entries(&self.id, &request.entries)
                            .await,
                        "append_log_entries",
                        None,
                    )
                    .await;
                if !append_result {
                    error!("Node {} failed to append log entries", self.id);
                    success = false;
                } else {
                    if let Some(last_entry) = request.entries.last() {
                        matched_index = last_entry.index;
                        self.last_log_index = last_entry.index;
                        self.last_log_term = last_entry.term;
                        info!(
                            "Node {} successfully appended log entries, updated matched_index to {}, last_log_index to {}",
                            self.id, matched_index, self.last_log_index
                        );
                        let after_append_last_index = self.get_last_log_index();
                        debug!(
                            "Node {} last_log_index after append: {}",
                            self.id, after_append_last_index
                        );
                    }
                }
            }
        }

        // 更新提交索引
        if success && request.leader_commit > self.commit_index {
            let new_commit_index = std::cmp::min(request.leader_commit, self.get_last_log_index());
            if new_commit_index > self.commit_index {
                info!(
                    "Node {} updating commit index from {} to {}",
                    self.id, self.commit_index, new_commit_index
                );
                self.commit_index = new_commit_index;
            }
        }

        // 发送响应
        let resp = AppendEntriesResponse {
            term: self.current_term,
            success,
            request_id: request.request_id,
            conflict_index: if success { None } else { conflict_index },
            conflict_term: if success { None } else { conflict_term },
            matched_index,
        };
        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_append_entries_response(&self.id, &request.leader_id, resp)
                    .await,
                "send_append_entries_response",
                Some(&request.leader_id),
            )
            .await;

        // 应用已提交的日志
        if success && self.commit_index > self.last_applied {
            debug!(
                "Node {} applying committed logs up to index {}",
                self.id, self.commit_index
            );
            self.apply_committed_logs().await;
        }
    }

    /// 处理 AppendEntries 响应
    pub(crate) async fn handle_append_entries_response(
        &mut self,
        peer: RaftId,
        response: AppendEntriesResponse,
    ) {
        self.pipeline.record_append_entries_response_feedback(
            &peer,
            response.request_id,
            response.success,
        );

        if self.role != Role::Leader {
            return;
        }

        debug!(
            "Leader {} received AppendEntries response from {}: term={}, success={}, current_term={}",
            self.id, peer, response.term, response.success, self.current_term
        );

        // 处理更高任期
        if response.term > self.current_term {
            warn!(
                "Node {} stepping down to Follower, found higher term {} from {} (current term {})",
                self.id, response.term, peer, self.current_term
            );
            self.step_down_to_follower(Some(response.term)).await;
            return;
        }

        if response.term == self.current_term {
            debug!(
                "Leader {} received AppendEntries response from {}: req_id={}, success={}, matched_index={}",
                self.id, peer, response.request_id, response.success, response.matched_index
            );

            if response.success {
                let match_index = response.matched_index;
                let current_match = self.match_index.get(&peer).copied().unwrap_or(0);
                if match_index > current_match {
                    self.match_index.insert(peer.clone(), match_index);
                    let current_next = self.next_index.get(&peer).copied().unwrap_or(1);
                    let min_next = match_index + 1;
                    if current_next < min_next {
                        self.next_index.insert(peer.clone(), min_next);
                        info!(
                            "Node {} updated replication state for {}: next_index={} (corrected), match_index={}",
                            self.id, peer, min_next, match_index
                        );
                    } else {
                        debug!(
                            "Node {} updated match_index for {}: match_index={} (next_index={} kept)",
                            self.id, peer, match_index, current_next
                        );
                    }
                } else {
                    debug!(
                        "Node {} Ignoring stale response from {}: matched_index={} <= current={}",
                        self.id, peer, match_index, current_match
                    );
                }

                self.update_commit_index().await;
            } else {
                warn!(
                    "Node {} received log conflict from {}: index={:?}, term={:?}",
                    self.id, peer, response.conflict_index, response.conflict_term
                );

                if response.matched_index > 0 {
                    self.match_index
                        .insert(peer.clone(), response.matched_index);
                }

                let new_next = self.resolve_log_conflict(&peer, &response).await;
                self.next_index.insert(peer.clone(), new_next);

                info!(
                    "Node {} updated next_index for {} to {} (conflict resolution)",
                    self.id, peer, new_next
                );
            }

            // 检查领导权转移状态
            if let Some(transfer_target) = &self.leader_transfer_target {
                if &peer == transfer_target {
                    self.process_leader_transfer_target_response(peer.clone(), response.clone())
                        .await;
                }
            }

            // 若当前处于联合配置，更新确认状态
            if response.success && self.config.is_joint() {
                debug!(
                    "Checking joint exit condition after successful replication to {}",
                    peer
                );
                self.check_joint_exit_condition().await;
            }
        }
    }

    /// 解析日志冲突并计算新的next_index
    pub(crate) async fn resolve_log_conflict(
        &self,
        peer: &RaftId,
        response: &AppendEntriesResponse,
    ) -> u64 {
        if response.matched_index > 0 {
            info!(
                "Using matched_index {} for peer {} conflict resolution",
                response.matched_index, peer
            );
            response.matched_index + 1
        } else if let Some(conflict_index) = response.conflict_index {
            if let Some(conflict_term) = response.conflict_term {
                info!(
                    "Optimizing with conflict_term: {} at index: {} for peer {}",
                    conflict_term, conflict_index, peer
                );

                let storage = &*self.callbacks;
                let start_index = conflict_index.min(self.last_log_index);
                let mut last_conflict_term_index = None;

                for i in (1..=start_index).rev() {
                    match storage.get_log_entries_term(&self.id, i, i + 1).await {
                        Ok(entries) if !entries.is_empty() => {
                            info!(
                                "Checking index {} with term {} for peer {}",
                                i, entries[0].1, peer
                            );

                            if entries[0].1 == conflict_term {
                                last_conflict_term_index = Some(i);
                                break;
                            } else if entries[0].1 < conflict_term {
                                break;
                            }
                        }
                        Ok(_entries) => {
                            info!("Index {} returned empty entries for peer {}", i, peer);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to get log entry at index {} for peer {}: {:?}",
                                i, peer, e
                            );
                            return conflict_index.max(1);
                        }
                    }
                }

                if let Some(index) = last_conflict_term_index {
                    info!(
                        "Found matching term {} at index {} for peer {}",
                        conflict_term, index, peer
                    );
                    return index + 1;
                }

                for i in (1..=start_index).rev() {
                    match storage.get_log_entries(&self.id, i, i + 1).await {
                        Ok(entries) if !entries.is_empty() => {
                            if entries[0].term > conflict_term {
                                info!(
                                    "Found optimized index {} for peer {} conflict resolution",
                                    i, peer
                                );
                                return i;
                            }
                        }
                        Ok(_entries) => continue,
                        Err(e) => {
                            error!(
                                "Failed to get log entry at index {} for peer {}: {:?}",
                                i, peer, e
                            );
                            return conflict_index.max(1);
                        }
                    }
                }

                conflict_index.max(1)
            } else {
                info!(
                    "Using conflict_index {} without term for peer {}",
                    conflict_index, peer
                );
                conflict_index.max(1)
            }
        } else {
            info!(
                "Using conservative fallback for peer {} conflict resolution",
                peer
            );
            self.next_index
                .get(peer)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1)
                .max(1)
        }
    }

    /// 更新提交索引
    pub(crate) async fn update_commit_index(&mut self) {
        use crate::cluster_config::QuorumRequirement;

        if self.role != Role::Leader {
            return;
        }

        let candidate_index = match self.config.quorum() {
            QuorumRequirement::Joint { old, new } => {
                let old_majority = self
                    .find_majority_index(&self.config.joint().unwrap().old_voters, old)
                    .await;
                let new_majority = self
                    .find_majority_index(&self.config.joint().unwrap().new_voters, new)
                    .await;

                match (old_majority, new_majority) {
                    (Some(old_idx), Some(new_idx)) => std::cmp::min(old_idx, new_idx),
                    _ => return,
                }
            }
            QuorumRequirement::Simple(quorum) => {
                let mut match_indices: Vec<u64> = self.match_index.values().cloned().collect();
                match_indices.push(self.get_last_log_index());
                match_indices.sort_unstable_by(|a, b| b.cmp(a));

                if match_indices.len() >= quorum {
                    match_indices[quorum - 1]
                } else {
                    return;
                }
            }
        };

        let old_commit_index = self.commit_index;

        if candidate_index > self.commit_index {
            let candidate_term = match self
                .error_handler
                .handle(
                    self.callbacks.get_log_term(&self.id, candidate_index).await,
                    "get_log_term",
                    None,
                )
                .await
            {
                Some(term) => term,
                None => return,
            };

            if candidate_term == self.current_term {
                self.commit_index = candidate_index;
            } else {
                let mut has_current_term = false;
                for i in (self.commit_index + 1)..=candidate_index {
                    if self.callbacks.get_log_term(&self.id, i).await.unwrap_or(0)
                        == self.current_term
                    {
                        has_current_term = true;
                        break;
                    }
                }

                if has_current_term {
                    self.commit_index = candidate_index;
                }
            }
        }

        if self.commit_index > old_commit_index {
            info!(
                "Node {} commit_index updated from {} to {}, applying committed logs",
                self.id, old_commit_index, self.commit_index
            );
            self.apply_committed_logs().await;
        }
    }

    /// 在指定配置中找到多数派索引
    pub(crate) async fn find_majority_index(
        &self,
        voters: &std::collections::HashSet<RaftId>,
        quorum: usize,
    ) -> Option<u64> {
        use tracing::warn;

        if voters.is_empty() || quorum == 0 {
            warn!("Invalid voters or quorum for majority calculation");
            return None;
        }

        let mut voter_indices: Vec<u64> = Vec::new();
        for voter in voters {
            if voter == &self.id {
                voter_indices.push(self.get_last_log_index());
            } else {
                voter_indices.push(self.match_index.get(voter).copied().unwrap_or(0));
            }
        }

        voter_indices.sort_unstable_by(|a, b| b.cmp(a));

        info!(
            "Node {} found voter indices: {:?} for quorum {}",
            self.id, voter_indices, quorum
        );

        if voter_indices.len() >= quorum {
            Some(voter_indices[quorum - 1])
        } else {
            None
        }
    }
}

