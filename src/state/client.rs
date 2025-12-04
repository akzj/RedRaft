//! Client request handling for Raft state machine

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use super::RaftState;
use crate::cluster_config::ClusterConfig;
use crate::error::ClientError;
use crate::event::Role;
use crate::message::LogEntry;
use crate::types::{Command, RaftId, RequestId};

/// 客户端请求的默认超时时间（30秒）
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl RaftState {
    /// 处理客户端提议
    pub(crate) async fn handle_client_propose(&mut self, cmd: Command, request_id: RequestId) {
        info!(
            "Node {} handling ClientPropose with request_id={:?}, role={:?}",
            self.id, request_id, self.role
        );

        if self.role != Role::Leader {
            warn!(
                "Node {} rejecting ClientPropose (not leader, current role: {:?}, leader: {:?})",
                self.id, self.role, self.leader_id
            );
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

        // 如果请求已存在，检查状态
        if let Some(&index) = self.client_requests.get(&request_id) {
            if index <= self.commit_index {
                // 已提交，直接返回成功
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(&self.id, request_id, Ok(index))
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                return;
            }
            // 未提交但正在处理中，返回处理中状态
            debug!(
                "Node {} request {} already in progress at index {}",
                self.id, request_id, index
            );
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Conflict(anyhow::anyhow!(
                                "Request already in progress at index {}", index
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }
        
        // 清理过期的客户端请求
        self.cleanup_expired_client_requests().await;

        // 生成日志条目
        let index = self.get_last_log_index() + 1;
        let new_entry = LogEntry {
            term: self.current_term,
            index,
            command: cmd,
            is_config: false,
            client_request_id: Some(request_id),
        };

        // 追加日志
        info!(
            "Node {} (Leader) appending log entry at index {} for request_id={:?}",
            self.id, index, request_id
        );
        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, std::slice::from_ref(&new_entry))
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            error!(
                "Node {} failed to append log entry for request_id={:?}",
                self.id, request_id
            );
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        info!(
            "Node {} successfully appended log entry at index {}, broadcasting to followers",
            self.id, index
        );
        self.last_log_index = index;
        self.last_log_term = self.current_term;

        // 记录客户端请求与日志索引的映射
        self.client_requests.insert(request_id, index);
        self.client_requests_revert.insert(index, request_id);
        self.client_request_timestamps.insert(request_id, Instant::now());

        // 立即同步日志
        info!(
            "Node {} broadcasting append entries for log index {}",
            self.id, index
        );
        self.broadcast_append_entries().await;
    }

    /// 应用已提交的日志
    pub(crate) async fn apply_committed_logs(&mut self) {
        if self.last_applied >= self.commit_index {
            debug!(
                "Node {} apply_committed_logs: nothing to apply (last_applied={}, commit_index={})",
                self.id, self.last_applied, self.commit_index
            );
            return;
        }

        let start = self.last_applied + 1;
        let end = std::cmp::min(
            self.commit_index,
            self.last_applied + self.options.apply_batch_size,
        );

        if end - start + 1 > 5 {
            info!(
                "Node {} applying {} logs from {} to {}",
                self.id,
                end - start + 1,
                start,
                end
            );
        }

        let entries = match self
            .callbacks
            .get_log_entries(&self.id, start, end + 1)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                error!("读取日志失败: {}", e);
                return;
            }
        };

        for (i, entry) in entries.into_iter().enumerate() {
            let expected_index = start + i as u64;

            if entry.index != expected_index {
                error!(
                    "Log index discontinuous: expected {}, got {}",
                    expected_index, entry.index
                );
                break;
            }

            if entry.index <= self.last_applied || entry.index > self.commit_index {
                continue;
            }

            if entry.is_config {
                if let Ok(new_config) = serde_json::from_slice::<ClusterConfig>(&entry.command) {
                    if let Err(validation_error) =
                        self.validate_config_change_safety(&new_config, entry.index)
                    {
                        error!(
                            "Configuration change validation failed for entry {}: {}. Config: {:?}",
                            entry.index, validation_error, new_config
                        );
                        self.last_applied = entry.index;
                        continue;
                    }

                    if self.config.log_index() > new_config.log_index() {
                        warn!(
                            "Node {} received outdated cluster config (current: {}, received: {}), ignoring to prevent rollback: {:?}",
                            self.id,
                            self.config.log_index(),
                            new_config.log_index(),
                            new_config
                        );
                        self.last_applied = entry.index;

                        if self.role == Role::Leader {
                            error!(
                                "Leader {} detected configuration rollback attempt - this may indicate a serious issue with log replication or configuration management",
                                self.id
                            );
                        }
                        continue;
                    } else {
                        info!(
                            "Node {} applying new cluster config: {:?}",
                            self.id, new_config
                        );

                        let old_config = &self.config;

                        let old_learners = old_config.learners().cloned().unwrap_or_default();
                        let new_learners = new_config.learners().cloned().unwrap_or_default();
                        let removed_learners: Vec<RaftId> =
                            old_learners.difference(&new_learners).cloned().collect();

                        let old_voters = old_config
                            .get_effective_voters()
                            .iter()
                            .cloned()
                            .collect::<HashSet<_>>();
                        let new_voters = new_config
                            .get_effective_voters()
                            .iter()
                            .cloned()
                            .collect::<HashSet<_>>();
                        let removed_voters: Vec<RaftId> =
                            old_voters.difference(&new_voters).cloned().collect();

                        let current_node_removed = removed_voters.contains(&self.id)
                            || removed_learners.contains(&self.id);

                        self.config = new_config;

                        if self.role == Role::Leader {
                            for removed_learner in &removed_learners {
                                self.cleanup_learner_replication_state(removed_learner);
                                info!(
                                    "Cleaned up replication state for removed learner: {}",
                                    removed_learner
                                );
                            }

                            for removed_voter in &removed_voters {
                                self.next_index.remove(removed_voter);
                                self.match_index.remove(removed_voter);
                                info!(
                                    "Cleaned up replication state for removed voter: {}",
                                    removed_voter
                                );
                            }
                        }

                        if current_node_removed {
                            warn!(
                                "Current node {} has been removed from cluster, initiating graceful shutdown",
                                self.id
                            );

                            if let Some(timer_id) = self.election_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.heartbeat_interval_timer_id.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.apply_interval_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.config_change_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }
                            if let Some(timer_id) = self.leader_transfer_timer.take() {
                                self.callbacks.del_timer(&self.id, timer_id);
                            }

                            let _ = self
                                .error_handler
                                .handle_void(
                                    self.callbacks.on_node_removed(&self.id).await,
                                    "node_removed",
                                    None,
                                )
                                .await;
                        }
                    }

                    let success = self
                        .error_handler
                        .handle_void(
                            self.callbacks
                                .save_cluster_config(&self.id, self.config.clone())
                                .await,
                            "save_cluster_config",
                            None,
                        )
                        .await;
                    if success {
                        info!(
                            "Node {} updated cluster config to: {:?}",
                            self.id, self.config
                        );
                    } else {
                        error!("Failed to save new cluster config");
                        return;
                    }

                    if self.config.is_joint() {
                        self.check_joint_exit_condition().await;
                    }

                    self.last_applied = entry.index;
                    info!(
                        "Node {} updated last_applied to {} after config change",
                        self.id, self.last_applied
                    );
                }
            } else {
                let index = entry.index;
                debug!(
                    "Node {} applying command to state machine: index={}, term={}",
                    self.id, entry.index, entry.term
                );
                let _ = self
                    .callbacks
                    .apply_command(&self.id, entry.index, entry.term, entry.command)
                    .await;
                self.last_applied = index;

                self.check_client_response(index).await;
            }
        }

        // 检查 ReadIndex 待处理的读请求
        self.check_pending_reads().await;

        self.adjust_apply_interval().await;
    }

    /// 调整应用间隔
    pub(crate) async fn adjust_apply_interval(&mut self) {
        let current_load = self.commit_index - self.last_applied;

        self.apply_interval = if current_load > 1000 {
            Duration::from_micros(100)
        } else {
            Duration::from_millis(10)
        };

        if let Some(timer) = self.apply_interval_timer {
            self.callbacks.del_timer(&self.id, timer);
        }

        self.apply_interval_timer = Some(
            self.callbacks
                .set_apply_timer(&self.id, self.apply_interval),
        );
    }

    /// 检查客户端响应（使用 O(1) 查找）
    pub(crate) async fn check_client_response(&mut self, log_index: u64) {
        // 使用 client_requests_revert 进行 O(1) 查找
        if let Some(&req_id) = self.client_requests_revert.get(&log_index) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(&self.id, req_id, Ok(log_index))
                        .await,
                    "client_response",
                    None,
                )
                .await;
            
            // 清理所有相关映射
            self.client_requests.remove(&req_id);
            self.client_requests_revert.remove(&log_index);
            self.client_request_timestamps.remove(&req_id);
        }
    }

    /// 清理过期的客户端请求
    pub(crate) async fn cleanup_expired_client_requests(&mut self) {
        let now = Instant::now();
        let mut expired_requests = Vec::new();

        // 找出过期的请求
        for (req_id, timestamp) in &self.client_request_timestamps {
            if now.duration_since(*timestamp) > CLIENT_REQUEST_TIMEOUT {
                expired_requests.push(*req_id);
            }
        }

        // 清理过期请求并发送超时错误
        for req_id in expired_requests {
            if let Some(index) = self.client_requests.remove(&req_id) {
                self.client_requests_revert.remove(&index);
                self.client_request_timestamps.remove(&req_id);
                
                warn!(
                    "Node {} cleaning up expired client request {} at index {}",
                    self.id, req_id, index
                );
                
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(
                                &self.id,
                                req_id,
                                Err(ClientError::Timeout),
                            )
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
            }
        }
    }
}

