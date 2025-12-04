//! Snapshot handling for Raft state machine

use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{error, info, warn};

use super::RaftState;
use crate::event::Role;
use crate::error::SnapshotError;
use crate::message::{
    CompleteSnapshotInstallation, HardState, InstallSnapshotRequest, InstallSnapshotResponse,
    InstallSnapshotState, Snapshot, SnapshotProbeSchedule,
};
use crate::types::{RaftId, RequestId};
use crate::Event;

impl RaftState {
    /// 发送快照到目标节点
    pub(crate) async fn send_snapshot_to(&mut self, target: RaftId) {
        let snap = match self.callbacks.load_snapshot(&self.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("没有可用的快照，无法发送");
                return;
            }
            Err(e) => {
                error!("加载快照失败: {}", e);
                return;
            }
        };

        if !self.verify_snapshot_consistency(&snap).await {
            error!("快照与当前日志不一致，无法发送");
            return;
        }

        let snapshot_request_id = RequestId::new();

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: snap.index,
            last_included_term: snap.term,
            data: snap.data.clone(),
            config: snap.config.clone(),
            request_id: snapshot_request_id,
            snapshot_request_id,
            is_probe: false,
        };

        self.follower_last_snapshot_index
            .insert(target.clone(), snap.index);
        self.follower_snapshot_states
            .insert(target.clone(), InstallSnapshotState::Installing);

        self.schedule_snapshot_probe(
            target.clone(),
            snapshot_request_id,
            self.schedule_snapshot_probe_interval,
            self.schedule_snapshot_probe_retries,
        );

        if let Err(err) = self
            .callbacks
            .send_install_snapshot_request(&self.id, &target, req)
            .await
        {
            log::error!(
                "node {}: failed to send InstallSnapshotRequest: {}",
                self.id,
                err
            );
        }
    }

    /// 验证快照与当前日志的一致性
    pub(crate) async fn verify_snapshot_consistency(&self, snap: &Snapshot) -> bool {
        if snap.index == 0 {
            return true;
        }

        let last_log_index = self.get_last_log_index();
        if snap.index > last_log_index {
            return false;
        }

        let log_term = if snap.index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            match self.callbacks.get_log_term(&self.id, snap.index).await {
                Ok(term) => term,
                Err(_) => return false,
            }
        };

        snap.term == log_term
    }

    /// 发送探测消息检查快照安装状态
    pub(crate) async fn probe_snapshot_status(
        &mut self,
        target: &RaftId,
        snapshot_request_id: RequestId,
    ) {
        info!(
            "Probing snapshot status for follower {} at term {}, last_snapshot_index {}",
            target,
            self.current_term,
            self.follower_last_snapshot_index.get(target).unwrap_or(&0)
        );

        let last_snap_index = self
            .follower_last_snapshot_index
            .get(target)
            .copied()
            .unwrap_or(0);

        let req = InstallSnapshotRequest {
            term: self.current_term,
            leader_id: self.id.clone(),
            last_included_index: last_snap_index,
            last_included_term: 0,
            data: vec![],
            config: self.config.clone(),
            snapshot_request_id,
            request_id: RequestId::new(),
            is_probe: true,
        };

        let _ = self
            .error_handler
            .handle(
                self.callbacks
                    .send_install_snapshot_request(&self.id, target, req)
                    .await,
                "send_install_snapshot_request",
                Some(target),
            )
            .await;
    }

    /// 处理安装快照请求
    pub(crate) async fn handle_install_snapshot(
        &mut self,
        sender: RaftId,
        request: InstallSnapshotRequest,
    ) {
        if sender != request.leader_id {
            warn!(
                "Node {} received InstallSnapshot from {}, but leader is {}",
                self.id, sender, request.leader_id
            );
            return;
        }

        if request.term < self.current_term {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed("Term too low".into()),
                error_message: "Term too low".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        if self.leader_id.is_some() && self.leader_id.clone().unwrap() != request.leader_id {
            warn!(
                "Node {} received InstallSnapshot, old leader is {} , new leader is {}",
                self.id,
                self.leader_id.clone().unwrap(),
                request.leader_id,
            );
        }

        self.leader_id = Some(request.leader_id.clone());
        self.role = Role::Follower;
        self.current_term = request.term;
        self.last_heartbeat = Instant::now();
        self.reset_election().await;

        // 处理空探测消息
        if request.is_probe {
            let current_state = if let Some(current_snapshot_request_id) =
                &self.current_snapshot_request_id
            {
                if *current_snapshot_request_id == request.snapshot_request_id {
                    match self.install_snapshot_success.clone() {
                        Some((success, request_id, error)) => {
                            assert!(request_id == request.snapshot_request_id);
                            if success {
                                InstallSnapshotState::Success
                            } else {
                                InstallSnapshotState::Failed(format!(
                                    "Snapshot installation failed: {}",
                                    error.unwrap()
                                ))
                            }
                        }
                        None => InstallSnapshotState::Installing,
                    }
                } else {
                    warn!(
                        "Node {} received InstallSnapshot {} probe, but no snapshot request_id not match {} ",
                        self.id, request.snapshot_request_id, current_snapshot_request_id
                    );
                    InstallSnapshotState::Failed("No such snapshot in progress".into())
                }
            } else {
                warn!(
                    "Node {} received InstallSnapshot probe, but no snapshot is in progress",
                    self.id
                );
                InstallSnapshotState::Success
            };

            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: current_state,
                error_message: "".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        // 仅处理比当前快照更新的快照
        if request.last_included_index <= self.last_snapshot_index {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Success,
                error_message: "".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        if !self.verify_snapshot_config_compatibility(&request).await {
            let resp = InstallSnapshotResponse {
                term: self.current_term,
                request_id: request.request_id,
                state: InstallSnapshotState::Failed(
                    "Snapshot config incompatible with current config".into(),
                ),
                error_message: "Snapshot config incompatible with current config".into(),
            };
            self.error_handler
                .handle_void(
                    self.callbacks
                        .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                        .await,
                    "send_install_snapshot_reply",
                    Some(&request.leader_id),
                )
                .await;
            return;
        }

        self.current_snapshot_request_id = Some(request.request_id);

        let resp = InstallSnapshotResponse {
            term: self.current_term,
            request_id: request.request_id,
            state: InstallSnapshotState::Installing,
            error_message: "".into(),
        };
        self.error_handler
            .handle_void(
                self.callbacks
                    .send_install_snapshot_response(&self.id, &request.leader_id, resp)
                    .await,
                "send_install_snapshot_reply",
                Some(&request.leader_id),
            )
            .await;

        let (oneshot_tx, oneshot_rx) = oneshot::channel();

        self.callbacks.process_snapshot(
            &self.id,
            request.last_included_index,
            request.last_included_term,
            request.data,
            request.config.clone(),
            request.request_id,
            oneshot_tx,
        );

        let callbacks = self.callbacks.clone();
        let self_id = self.id.clone();

        tokio::task::spawn(async move {
            let result = oneshot_rx.await;
            let result = match result {
                Ok(result) => {
                    match &result {
                        Ok(_) => {
                            info!("Snapshot processing succeeded");
                        }
                        Err(error) => {
                            warn!("Snapshot processing failed: {}", error);
                        }
                    };
                    result
                }
                Err(error) => {
                    error!("Snapshot processing failed: {}", error);
                    Err(SnapshotError::Unknown)
                }
            };

            match callbacks
                .send(
                    self_id,
                    Event::CompleteSnapshotInstallation(CompleteSnapshotInstallation {
                        index: request.last_included_index,
                        term: request.last_included_term,
                        success: result.is_ok(),
                        request_id: request.request_id,
                        reason: result.err().map(|e| e.to_string()),
                        config: Some(request.config.clone()),
                    }),
                )
                .await
            {
                Ok(()) => {
                    info!("Snapshot installation result sent successfully");
                }
                Err(error) => {
                    error!("Snapshot installation result send failed: {}", error);
                }
            }
        });
    }

    /// 验证快照配置兼容性
    pub(crate) async fn verify_snapshot_config_compatibility(
        &self,
        _req: &InstallSnapshotRequest,
    ) -> bool {
        true
    }

    /// 处理快照安装完成
    pub async fn handle_complete_snapshot_installation(
        &mut self,
        result: CompleteSnapshotInstallation,
    ) {
        if self.current_snapshot_request_id != Some(result.request_id) {
            warn!(
                "Node {} received completion for unknown snapshot request_id: {:?}, current: {:?}",
                self.id, result.request_id, self.current_snapshot_request_id
            );
            return;
        }

        if result.success {
            self.last_snapshot_index = result.index;
            self.last_snapshot_term = result.term;
            self.commit_index = self.commit_index.max(result.index);
            self.last_applied = self.last_applied.max(result.index);

            if let Some(snapshot_config) = result.config {
                info!(
                    "Node {} applying snapshot config: old_config={:?}, new_config={:?}",
                    self.id, self.config, snapshot_config
                );

                let old_config = self.config.clone();
                self.config = snapshot_config;
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

                match (
                    self.config.voters_contains(&self.id),
                    self.config.learners_contains(&self.id),
                ) {
                    (false, true) => {
                        warn!(
                            "Node {} is no longer a voter in snapshot config, stepping down to learner",
                            self.id
                        );
                        self.role = Role::Learner;
                        let _ = self
                            .error_handler
                            .handle_void(
                                self.callbacks.on_state_changed(&self.id, self.role).await,
                                "state_changed",
                                None,
                            )
                            .await;
                    }
                    (false, false) => {
                        if old_config.voters_contains(&self.id)
                            || old_config.learners_contains(&self.id)
                        {
                            warn!(
                                "Node {} is no longer a member in snapshot config, remove self",
                                self.id
                            );
                            self.callbacks
                                .on_node_removed(&self.id)
                                .await
                                .unwrap_or_else(|e| {
                                    error!("Failed to remove node {}: {}", self.id, e);
                                });
                        } else {
                            info!(
                                "Node {} snapshot config not contain self, continue to sync log from leader",
                                self.id
                            );
                        }
                    }
                    (true, false) => {
                        self.role = Role::Follower;
                        let _ = self
                            .error_handler
                            .handle_void(
                                self.callbacks.on_state_changed(&self.id, self.role).await,
                                "state_changed",
                                None,
                            )
                            .await;
                    }
                    (true, true) => {
                        error!(
                            "Node {} cannot be both voter and learner in snapshot config",
                            self.id
                        );
                    }
                }
            }
        } else {
            warn!(
                "Node {} snapshot installation failed: {}",
                self.id,
                result.reason.clone().unwrap_or("Unknown reason".into())
            );
        }

        self.current_snapshot_request_id = None;
    }

    /// 处理安装快照响应
    pub(crate) async fn handle_install_snapshot_response(
        &mut self,
        peer: RaftId,
        response: InstallSnapshotResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }

        if response.term > self.current_term {
            self.current_term = response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.error_handler
                .handle_void(
                    self.callbacks
                        .save_hard_state(
                            &self.id,
                            HardState {
                                raft_id: self.id.clone(),
                                term: self.current_term,
                                voted_for: self.voted_for.clone(),
                            },
                        )
                        .await,
                    "save_hard_state",
                    None,
                )
                .await;
            self.error_handler
                .handle_void(
                    self.callbacks
                        .on_state_changed(&self.id, Role::Follower)
                        .await,
                    "state_changed",
                    None,
                )
                .await;
            self.remove_snapshot_probe(&peer);
            return;
        }

        self.follower_snapshot_states
            .insert(peer.clone(), response.state.clone());

        match response.state {
            InstallSnapshotState::Success => {
                let snap_index = self
                    .follower_last_snapshot_index
                    .get(&peer)
                    .copied()
                    .unwrap_or(0);
                self.next_index.insert(peer.clone(), snap_index + 1);
                self.match_index.insert(peer.clone(), snap_index);
                info!("Follower {} completed snapshot installation", peer);
                self.remove_snapshot_probe(&peer);
            }
            InstallSnapshotState::Installing => {
                info!("Follower {} is still installing snapshot", peer);
                self.extend_snapshot_probe(&peer);
            }
            InstallSnapshotState::Failed(reason) => {
                warn!("Follower {} snapshot install failed: {}", peer, reason);
                self.remove_snapshot_probe(&peer);
                self.schedule_snapshot_retry(peer).await;
            }
        }
    }

    /// 安排快照状态探测
    pub(crate) fn schedule_snapshot_probe(
        &mut self,
        peer: RaftId,
        snapshot_request_id: RequestId,
        interval: Duration,
        max_attempts: u32,
    ) {
        self.remove_snapshot_probe(&peer);

        self.snapshot_probe_schedules.push(SnapshotProbeSchedule {
            peer: peer.clone(),
            next_probe_time: Instant::now() + interval,
            interval,
            max_attempts,
            attempts: 0,
            snapshot_request_id,
        });
    }

    /// 延长快照探测计划
    pub(crate) fn extend_snapshot_probe(&mut self, peer: &RaftId) {
        if let Some(schedule) = self
            .snapshot_probe_schedules
            .iter_mut()
            .find(|s| &s.peer == peer)
        {
            schedule.attempts += 1;

            if schedule.attempts >= schedule.max_attempts {
                self.follower_snapshot_states.insert(
                    peer.clone(),
                    InstallSnapshotState::Failed("Max probe attempts reached".into()),
                );
                self.remove_snapshot_probe(peer);
            } else {
                schedule.next_probe_time = Instant::now() + schedule.interval;
            }
        }
    }

    /// 移除快照探测计划
    pub(crate) fn remove_snapshot_probe(&mut self, peer: &RaftId) {
        self.snapshot_probe_schedules.retain(|s| &s.peer != peer);
    }

    /// 处理到期的探测计划
    pub(crate) async fn process_pending_probes(&mut self, now: Instant) {
        let pending_peers: Vec<(RaftId, RequestId)> = self
            .snapshot_probe_schedules
            .iter()
            .filter(|s| s.next_probe_time <= now)
            .map(|s| (s.peer.clone(), s.snapshot_request_id))
            .collect();

        for (peer, snapshot_request_id) in pending_peers {
            self.probe_snapshot_status(&peer, snapshot_request_id).await;
            self.extend_snapshot_probe(&peer);
        }
    }

    /// 安排快照重发
    pub(crate) async fn schedule_snapshot_retry(&mut self, peer: RaftId) {
        self.send_snapshot_to(peer).await;
    }

    /// 生成快照并持久化
    pub(crate) async fn create_snapshot(&mut self) {
        let begin = Instant::now();

        if self.commit_index <= self.last_snapshot_index {
            info!(
                "No new committed logs to snapshot (commit_index: {}, last_snapshot_index: {})",
                self.commit_index, self.last_snapshot_index
            );
            return;
        }

        let snapshot_index = self.commit_index;
        let snapshot_term = if snapshot_index == 0 {
            0
        } else if snapshot_index <= self.last_snapshot_index {
            self.last_snapshot_term
        } else {
            match self.callbacks.get_log_term(&self.id, snapshot_index).await {
                Ok(term) => term,
                Err(e) => {
                    error!(
                        "Failed to get log term for snapshot at index {}: {}",
                        snapshot_index, e
                    );
                    return;
                }
            }
        };

        let config = self.config.clone();

        let (snap_index, snap_term) = match self
            .error_handler
            .handle(
                self.callbacks
                    .create_snapshot(&self.id, config, self.callbacks.clone())
                    .await,
                "create_snapshot",
                None,
            )
            .await
        {
            Some((idx, term)) => (idx, term),
            None => {
                error!("Failed to create snapshot via callback");
                return;
            }
        };

        if snap_index != snapshot_index {
            warn!(
                "Snapshot index mismatch: expected {}, got {}, using expected value",
                snapshot_index, snap_index
            );
        }
        if snap_term != snapshot_term {
            warn!(
                "Snapshot term mismatch: expected {}, got {}, using expected value",
                snapshot_term, snap_term
            );
        }

        let snap = match self.callbacks.load_snapshot(&self.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("No snapshot found after creation");
                return;
            }
            Err(e) => {
                error!("Failed to load snapshot after creation: {}", e);
                return;
            }
        };

        if snap.index < snapshot_index {
            error!(
                "Snapshot index too old: expected >= {}, got {}",
                snapshot_index, snap.index
            );
            return;
        }

        info!(
            "Snapshot validation passed: index={}, term={}, config_valid={}",
            snap.index,
            snap.term,
            snap.config.is_valid()
        );

        if snap.index > 0 {
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks
                        .truncate_log_prefix(&self.id, snap.index + 1)
                        .await,
                    "truncate_log_prefix",
                    None,
                )
                .await;
        }

        self.last_snapshot_index = snap.index;
        self.last_snapshot_term = snap.term;
        self.last_applied = self.last_applied.max(snap.index);
        self.commit_index = self.commit_index.max(snap.index);

        let elapsed = begin.elapsed();
        info!(
            "Snapshot created at index {}, term {} (elapsed: {:?})",
            snap.index, snap.term, elapsed
        );
    }
}

