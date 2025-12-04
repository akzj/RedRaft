//! Configuration change handling for Raft state machine

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};

use super::RaftState;
use crate::cluster_config::{ClusterConfig, QuorumRequirement};
use crate::error::ClientError;
use crate::event::Role;
use crate::message::LogEntry;
use crate::types::{RaftId, RequestId};

impl RaftState {
    /// 处理配置变更请求
    pub(crate) async fn handle_change_config(
        &mut self,
        new_voters: HashSet<RaftId>,
        request_id: RequestId,
    ) {
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

        if self.config_change_in_progress {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Conflict(anyhow::anyhow!(
                                "config change in progress"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;

        let new_config = ClusterConfig::simple(new_voters.clone(), index);
        if !new_config.is_valid() {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "invalid cluster config"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        let old_voters = self.config.get_effective_voters().clone();
        let mut joint_config = self.config.clone();
        if let Err(e) = joint_config.enter_joint(old_voters, new_voters, None, None, index) {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "Failed to enter joint config: {}",
                                e
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        let config_data = match serde_json::to_vec(&joint_config) {
            Ok(data) => data,
            Err(e) => {
                self.error_handler
                    .handle_void(
                        self.callbacks
                            .client_response(
                                &self.id,
                                request_id,
                                Err(ClientError::Internal(anyhow::anyhow!(
                                    "failed to serialize config: {}",
                                    e
                                ))),
                            )
                            .await,
                        "client_response",
                        None,
                    )
                    .await;
                return;
            }
        };

        let new_entry = LogEntry {
            term: self.current_term,
            index,
            command: config_data,
            is_config: true,
            client_request_id: Some(request_id),
        };

        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, &[new_entry.clone()])
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            self.error_handler
                .handle_void(
                    self.callbacks
                        .client_response(
                            &self.id,
                            request_id,
                            Err(ClientError::Internal(anyhow::anyhow!(
                                "failed to append config log"
                            ))),
                        )
                        .await,
                    "client_response",
                    None,
                )
                .await;
            return;
        }

        self.config = joint_config;
        self.joint_config_log_index = index;
        self.config_change_in_progress = true;
        self.config_change_start_time = Some(Instant::now());
        self.client_requests.insert(request_id, last_idx + 1);

        self.last_log_index = index;
        self.last_log_term = self.current_term;

        assert!(
            self.is_leader_joint_config(),
            "Leader should be in joint config after initiating config change"
        );

        self.config_change_timer = Some(
            self.callbacks
                .set_config_change_timer(&self.id, self.config_change_timeout),
        );

        self.broadcast_append_entries().await;
    }

    /// 处理配置变更超时
    pub(crate) async fn handle_config_change_timeout(&mut self) {
        if !self.is_leader_joint_config() {
            warn!("Not in joint config or not leader, cannot handle config change timeout");
            return;
        }

        let start_time = match self.config_change_start_time {
            Some(t) => t,
            None => {
                self.config_change_in_progress = false;
                return;
            }
        };

        if start_time.elapsed() < self.config_change_timeout {
            self.config_change_timer = Some(
                self.callbacks
                    .set_config_change_timer(&self.id, self.config_change_timeout),
            );
            return;
        }

        self.check_joint_exit_condition().await;
    }

    /// 检查联合配置退出条件
    pub(crate) async fn check_joint_exit_condition(&mut self) {
        if !self.is_leader_joint_config() {
            return;
        }

        if self.joint_config_log_index <= self.commit_index {
            info!(
                "exiting joint config as it is committed config log index {} committed index {}",
                self.joint_config_log_index, self.commit_index
            );
            self.exit_joint_config(false).await;
            return;
        }

        let enter_time = match self.config_change_start_time {
            Some(t) => t,
            None => {
                warn!("No config change start time recorded, clearing config change state");
                self.config_change_in_progress = false;
                return;
            }
        };

        if enter_time.elapsed() >= self.config_change_timeout {
            self.check_joint_timeout().await;
        } else {
            self.try_advance_joint_config().await;
        }
    }

    /// 尝试推进联合配置
    pub(crate) async fn try_advance_joint_config(&mut self) {
        debug!(
            "Trying to advance joint config: joint_index={}, commit_index={}",
            self.joint_config_log_index, self.commit_index
        );
        self.broadcast_append_entries().await;
    }

    /// 检查是否是 Leader 且处于联合配置
    pub(crate) fn is_leader_joint_config(&self) -> bool {
        self.role == Role::Leader
            && self.config.is_joint()
            && self.config_change_in_progress
            && self.config_change_start_time.is_some()
            && self.config_change_timeout > Duration::ZERO
    }

    /// 检查联合配置超时
    pub(crate) async fn check_joint_timeout(&mut self) {
        if !self.is_leader_joint_config() {
            return;
        }

        // 防御性检查：虽然 is_leader_joint_config() 已检查，但仍需安全处理
        let elapsed = match self.config_change_start_time {
            Some(start_time) => start_time.elapsed(),
            None => {
                error!("config_change_start_time is None after is_leader_joint_config check");
                return;
            }
        };

        warn!(
            "Joint config timeout reached (elapsed: {:?}, timeout: {:?}), checking for rollback",
            elapsed,
            self.config_change_timeout
        );

        let joint = match self.config.joint() {
            Some(j) => j,
            None => {
                error!("Joint config missing during timeout check");
                return;
            }
        };

        let (old_quorum, new_quorum) = match self.config.quorum() {
            QuorumRequirement::Simple(_) => {
                error!("Unexpected simple quorum in joint config");
                return;
            }
            QuorumRequirement::Joint { old, new } => (old, new),
        };

        let new_majority_index = match self
            .find_majority_index(&joint.new_voters, new_quorum)
            .await
        {
            Some(idx) => idx,
            None => {
                warn!("Failed to find majority index for new config");
                0
            }
        };
        let old_majority_index = match self
            .find_majority_index(&joint.old_voters, old_quorum)
            .await
        {
            Some(idx) => idx,
            None => {
                warn!("Failed to find majority index for old config");
                0
            }
        };

        let joint_config_index = self.joint_config_log_index;
        if old_majority_index < joint_config_index && new_majority_index < joint_config_index {
            warn!(
                "Joint config index {} not supported by majority: old config got {}, new config got {}",
                joint_config_index, old_majority_index, new_majority_index
            );
            return;
        }

        let rollback = new_majority_index < joint_config_index;
        if rollback {
            if old_majority_index >= joint_config_index {
                warn!(
                    "New config lacks majority (got index {}, needed >= {}), rolling back to old config",
                    new_majority_index, joint_config_index
                );
            } else {
                warn!("Both configs lack majority, forced rollback to old config as fallback");
            }
        } else {
            info!(
                "New config has majority (index {} >= {}), exiting to new config",
                new_majority_index, joint_config_index
            );
        }

        self.exit_joint_config(rollback).await;
    }

    /// 退出联合配置
    pub(crate) async fn exit_joint_config(&mut self, rollback: bool) {
        if !self.is_leader_joint_config() {
            warn!("Not in joint config or not leader, cannot exit joint config");
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;

        let new_config = if rollback {
            if let Some(joint) = self.config.joint() {
                ClusterConfig::simple(joint.old_voters.clone(), index)
            } else {
                warn!("Not in joint config, cannot rollback");
                return;
            }
        } else {
            match self.config.leave_joint(index) {
                Ok(config) => config,
                Err(e) => {
                    error!("Failed to leave joint config: {}", e);
                    return;
                }
            }
        };

        if !new_config.is_valid() {
            error!("Generated new config is invalid, aborting exit joint");
            return;
        }

        let config_data = match serde_json::to_vec(&new_config) {
            Ok(data) => data,
            Err(error) => {
                error!("Failed to serialize new config: {}", error);
                return;
            }
        };

        let exit_entry = LogEntry {
            term: self.current_term,
            index,
            command: config_data,
            is_config: true,
            client_request_id: None,
        };

        let append_success = self
            .error_handler
            .handle_void(
                self.callbacks
                    .append_log_entries(&self.id, &[exit_entry])
                    .await,
                "append_log_entries",
                None,
            )
            .await;
        if !append_success {
            error!("Failed to append exit joint log entry, aborting");
            return;
        }

        self.broadcast_append_entries().await;

        self.last_log_index = index;
        self.last_log_term = self.current_term;

        self.config = new_config;
        self.joint_config_log_index = 0;

        self.config_change_in_progress = false;
        let _ = self
            .error_handler
            .handle_void(
                self.callbacks
                    .save_cluster_config(&self.id, self.config.clone())
                    .await,
                "save_cluster_config",
                None,
            )
            .await;

        info!(
            "Exited joint config (rollback: {}) to new config: {:?}",
            rollback, self.config
        );
    }

    /// 处理添加 Learner
    pub(crate) async fn handle_add_learner(&mut self, learner: RaftId, request_id: RequestId) {
        self.handle_learner_operation(learner, request_id, true)
            .await;
    }

    /// 处理移除 Learner
    pub(crate) async fn handle_remove_learner(&mut self, learner: RaftId, request_id: RequestId) {
        self.handle_learner_operation(learner, request_id, false)
            .await;
    }

    /// 处理 Learner 操作
    async fn handle_learner_operation(
        &mut self,
        learner: RaftId,
        request_id: RequestId,
        is_add: bool,
    ) {
        if !self.validate_leader_for_config_change(request_id).await {
            return;
        }

        let last_idx = self.get_last_log_index();
        let index = last_idx + 1;

        let mut temp_config = self.config.clone();
        let operation_result = if is_add {
            temp_config.add_learner(learner.clone(), index)
        } else {
            temp_config.remove_learner(&learner, index)
        };

        if let Err(e) = operation_result {
            self.send_client_error(
                request_id,
                ClientError::BadRequest(anyhow::anyhow!(
                    "{} learner failed: {}",
                    if is_add { "Add" } else { "Remove" },
                    e
                )),
            )
            .await;
            return;
        }

        if !self
            .create_and_append_config_log(&temp_config, index, request_id)
            .await
        {
            return;
        }

        self.last_log_index = index;
        self.last_log_term = self.current_term;
        self.client_requests.insert(request_id, index);
        self.client_requests_revert.insert(index, request_id);

        if is_add {
            self.initialize_learner_replication_state(&learner, index);
        }

        self.broadcast_append_entries().await;

        info!(
            "Initiated {} learner {} (will take effect after commit)",
            if is_add { "add" } else { "remove" },
            learner
        );
    }

    /// 验证 Leader 配置变更
    pub(crate) async fn validate_leader_for_config_change(&mut self, request_id: RequestId) -> bool {
        if self.role != Role::Leader {
            self.send_client_error(request_id, ClientError::NotLeader(self.leader_id.clone()))
                .await;
            return false;
        }

        if self.config_change_in_progress {
            self.send_client_error(
                request_id,
                ClientError::Conflict(anyhow::anyhow!("Configuration change in progress")),
            )
            .await;
            return false;
        }

        true
    }

    /// 发送客户端错误
    pub(crate) async fn send_client_error(&mut self, request_id: RequestId, error: ClientError) {
        self.error_handler
            .handle_void(
                self.callbacks
                    .client_response(&self.id, request_id, Err(error))
                    .await,
                "client_response",
                None,
            )
            .await;
    }

    /// 创建并追加配置日志
    pub(crate) async fn create_and_append_config_log(
        &mut self,
        config: &ClusterConfig,
        index: u64,
        request_id: RequestId,
    ) -> bool {
        let config_data = match serde_json::to_vec(config) {
            Ok(data) => data,
            Err(e) => {
                self.send_client_error(
                    request_id,
                    ClientError::Internal(anyhow::anyhow!("Failed to serialize config: {}", e)),
                )
                .await;
                return false;
            }
        };

        let entry = LogEntry {
            term: self.current_term,
            index,
            command: config_data,
            is_config: true,
            client_request_id: Some(request_id),
        };

        if !self
            .error_handler
            .handle_void(
                self.callbacks.append_log_entries(&self.id, &[entry]).await,
                "append_log_entries",
                None,
            )
            .await
        {
            self.send_client_error(
                request_id,
                ClientError::Internal(anyhow::anyhow!("Failed to append log")),
            )
            .await;
            return false;
        }

        true
    }

    /// 初始化 Learner 复制状态
    pub(crate) fn initialize_learner_replication_state(&mut self, learner: &RaftId, index: u64) {
        self.next_index.insert(learner.clone(), index + 1);
        self.match_index.insert(learner.clone(), 0);
    }

    /// 清理 Learner 复制状态
    pub(crate) fn cleanup_learner_replication_state(&mut self, learner: &RaftId) {
        self.next_index.remove(learner);
        self.match_index.remove(learner);
        self.follower_snapshot_states.remove(learner);
        self.follower_last_snapshot_index.remove(learner);
    }

    /// 验证配置变更的安全性
    pub(crate) fn validate_config_change_safety(
        &self,
        new_config: &ClusterConfig,
        entry_index: u64,
    ) -> Result<(), String> {
        if !new_config.is_valid() {
            return Err("Invalid cluster configuration".to_string());
        }

        if new_config.log_index() < self.config.log_index() {
            return Err(format!(
                "Configuration rollback detected: current log_index={}, new log_index={}",
                self.config.log_index(),
                new_config.log_index()
            ));
        }

        if new_config.log_index() != entry_index {
            return Err(format!(
                "Configuration log_index mismatch: config.log_index={}, entry.index={}",
                new_config.log_index(),
                entry_index
            ));
        }

        if self.role == Role::Leader {
            if new_config.log_index() > self.get_last_log_index() {
                return Err(format!(
                    "Leader received configuration from future: config.log_index={}, last_log_index={}",
                    new_config.log_index(),
                    self.get_last_log_index()
                ));
            }

            if let Some(removed_voters_count) = self.calculate_voter_removal_count(new_config) {
                let current_voter_count = self.config.get_effective_voters().len();
                if removed_voters_count > current_voter_count / 2 {
                    return Err(format!(
                        "Dangerous configuration change: removing {} voters from {} total voters",
                        removed_voters_count, current_voter_count
                    ));
                }
            }
        }

        Ok(())
    }

    /// 计算配置变更中被删除的 voter 数量
    pub(crate) fn calculate_voter_removal_count(&self, new_config: &ClusterConfig) -> Option<usize> {
        let current_voters: HashSet<_> =
            self.config.get_effective_voters().iter().cloned().collect();
        let new_voters: HashSet<_> = new_config.get_effective_voters().iter().cloned().collect();

        let removed_count = current_voters.difference(&new_voters).count();
        if removed_count > 0 {
            Some(removed_count)
        } else {
            None
        }
    }
}

