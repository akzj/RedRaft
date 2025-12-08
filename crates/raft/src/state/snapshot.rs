//! Snapshot handling for Raft state machine
//! 
//! # Snapshot Module Overview
//! 
//! This module is responsible for Raft snapshot creation, sending, and installation. Snapshots are used for:
//! - Compressing logs to prevent infinite growth
//! - Allowing lagging Followers to quickly catch up with the Leader
//! - Fast recovery after node restart
//! 
//! # Snapshot Installation Process
//!
//! ## Flowchart
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                              Follower Node Internal                          │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │  ┌─────────────┐    ①InstallSnapshotRequest     ┌────────────────────────┐ │
//! │  │   Leader    │ ──────────────────────────────▶│  handle_install_       │ │
//! │  │   (Remote)   │                                │  snapshot()            │ │
//! │  └─────────────┘                                └───────────┬────────────┘ │
//! │        ▲                                                    │              │
//! │        │                                                    │②             │
//! │        │ ⑥InstallSnapshotResponse                          ▼              │
//! │        │   (Success/Failed)                    ┌────────────────────────┐  │
//! │        │                                       │ callbacks.process_     │  │
//! │        │                                       │ snapshot(..., tx)      │  │
//! │        │                                       │ (business layer/StateMachine)  │  │
//! │        │                                       └───────────┬────────────┘  │
//! │        │                                                   │               │
//! │        │                                                   │③ oneshot::tx  │
//! │        │                                                   ▼               │
//! │        │                                       ┌────────────────────────┐  │
//! │        │                                       │ tokio::spawn async task  │  │
//! │        │                                       │ wait for oneshot::rx       │  │
//! │        │                                       └───────────┬────────────┘  │
//! │        │                                                   │               │
//! │        │                                                   │④ Event::      │
//! │        │                                                   │ CompleteSnapshot│
//! │        │                                                   ▼               │
//! │        │                                       ┌────────────────────────┐  │
//! │        │                                       │ RaftState::tick()      │  │
//! │        └───────────────────────────────────────│ handle_complete_       │  │
//! │                                                │ snapshot_installation()│  │
//! │                                                └────────────────────────┘  │
//! │                                                         ⑤                  │
//! │                                                  - Log truncation                │
//! │                                                  - Persist HardState        │
//! │                                                  - Clean expired requests            │
//! │                                                  - Apply configuration                │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Detailed Steps
//!
//! 1. **Leader Sends Snapshot Request**
//!    - Leader detects Follower is too far behind (logs have been truncated)
//!    - Calls [`RaftState::send_snapshot_to`] to send [`InstallSnapshotRequest`]
//!
//! 2. **Follower Processes Snapshot Request**
//!    - [`RaftState::handle_install_snapshot`] receives the request
//!    - First responds with `InstallSnapshotState::Installing` to inform Leader it's processing
//!    - Calls `callbacks.process_snapshot()` to let business layer process snapshot data
//!    - Starts `tokio::spawn` async task to wait for processing result
//!
//! 3. **Business Layer Processes Snapshot**
//!    - [`StateMachine::process_snapshot`](crate::traits::StateMachine::process_snapshot) is called
//!    - Deserializes and restores state machine state
//!    - Notifies result via `oneshot::Sender` when done
//!
//! 4. **Self-Notification of Completion Event**
//!    - Async task receives processing result
//!    - Sends [`Event::CompleteSnapshotInstallation`] to itself
//!    - This is an **Actor pattern self-message** to prevent snapshot processing (which may be time-consuming) from blocking the Raft main loop
//!
//! 5. **Complete Snapshot Installation**
//!    - [`RaftState::handle_complete_snapshot_installation`] handles the completion event
//!    - Performs the following operations:
//!    - Updates `last_snapshot_index`, `last_snapshot_term`
//!    - Updates `commit_index`, `last_applied`
//!      - **Truncate logs** - Delete log entries before snapshot
//!      - **Persist HardState** - Save term and voted_for
//!      - **Clean expired client requests** - Remove requests covered by snapshot
//!      - **Apply cluster configuration** - If snapshot contains configuration changes
//!
//! 6. **Respond to Leader**
//!    - Leader queries installation status via probe messages (`is_probe=true`)
//!    - Follower returns final `Success` or `Failed` state
//!
//! ## Key Data Structures
//!
//! - [`CompleteSnapshotInstallation`] - Event data for snapshot installation completion
//! - [`InstallSnapshotRequest`] - Snapshot installation request sent by Leader
//! - [`InstallSnapshotResponse`] - Follower's response
//! - [`InstallSnapshotState`] - Installation state enum (Installing/Success/Failed)
//!
//! ## Design Points
//!
//! | Item | Description |
//! |------|------|
//! | **Sender** | Follower itself (via `tokio::spawn` async task) |
//! | **Receiver** | Follower itself (`RaftState::tick`) |
//! | **Purpose** | Asynchronous decoupling to prevent snapshot processing (which may be time-consuming) from blocking the Raft main loop |
//! | **Trigger Timing** | After business layer completes `process_snapshot`, notified via oneshot channel |
//!
//! [`InstallSnapshotRequest`]: crate::message::InstallSnapshotRequest
//! [`InstallSnapshotResponse`]: crate::message::InstallSnapshotResponse
//! [`InstallSnapshotState`]: crate::message::InstallSnapshotState
//! [`CompleteSnapshotInstallation`]: crate::message::CompleteSnapshotInstallation
//! [`Event::CompleteSnapshotInstallation`]: crate::Event::CompleteSnapshotInstallation

use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

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
    /// Send snapshot to target node
    pub(crate) async fn send_snapshot_to(&mut self, target: RaftId) {
        let snap = match self.callbacks.load_snapshot(&self.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                error!("No snapshot available, cannot send");
                return;
            }
            Err(e) => {
                error!("Failed to load snapshot: {}", e);
                return;
            }
        };

        if !self.verify_snapshot_consistency(&snap).await {
            error!("Snapshot inconsistent with current logs, cannot send");
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
            error!(
                "node {}: failed to send InstallSnapshotRequest: {}",
                self.id,
                err
            );
        }
    }

    /// Verify snapshot consistency with current logs
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

    /// Send probe message to check snapshot installation status
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

    /// Handle install snapshot request
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

        if self
            .leader_id
            .as_ref()
            .is_some_and(|current| current != &request.leader_id)
        {
            warn!(
                "Node {} received InstallSnapshot, old leader is {:?} , new leader is {}",
                self.id, self.leader_id, request.leader_id,
            );
        }

        self.leader_id = Some(request.leader_id.clone());
        self.role = Role::Follower;
        self.current_term = request.term;
        self.last_heartbeat = Instant::now();
        self.reset_election().await;

        // Handle empty probe message
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
                                    error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
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

        // Only process snapshots newer than current snapshot
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

    /// Verify snapshot config compatibility
    pub(crate) async fn verify_snapshot_config_compatibility(
        &self,
        _req: &InstallSnapshotRequest,
    ) -> bool {
        true
    }

    /// Handle snapshot installation completion
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
            info!(
                "Node {} snapshot installation succeeded at index {}, term {}",
                self.id, result.index, result.term
            );

            self.last_snapshot_index = result.index;
            self.last_snapshot_term = result.term;
            self.commit_index = self.commit_index.max(result.index);
            self.last_applied = self.last_applied.max(result.index);

            // 1. Truncate log entries before snapshot
            if result.index > 0 {
                let _ = self
                    .error_handler
                    .handle_void(
                        self.callbacks
                            .truncate_log_prefix(&self.id, result.index + 1)
                            .await,
                        "truncate_log_prefix",
                        None,
                    )
                    .await;
                info!(
                    "Node {} truncated log prefix up to index {}",
                    self.id, result.index
                );
            }

            // 2. Persist hard state
            let hard_state = HardState {
                raft_id: self.id.clone(),
                term: self.current_term,
                voted_for: self.voted_for.clone(),
            };
            let _ = self
                .error_handler
                .handle_void(
                    self.callbacks.save_hard_state(&self.id, hard_state).await,
                    "save_hard_state",
                    None,
                )
                .await;

            // 3. Clean up expired client requests (requests with index <= snapshot_index are invalid)
            let snapshot_index = result.index;
            let expired_requests: Vec<_> = self
                .client_requests
                .iter()
                .filter(|(_, &idx)| idx <= snapshot_index)
                .map(|(req_id, _)| *req_id)
                .collect();

            for req_id in expired_requests {
                if let Some(idx) = self.client_requests.remove(&req_id) {
                    self.client_requests_revert.remove(&idx);
                    self.client_request_timestamps.remove(&req_id);
                    debug!(
                        "Node {} cleaned up expired client request {:?} at index {}",
                        self.id, req_id, idx
                    );
                }
            }

            // 4. Apply snapshot config
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

            info!(
                "Node {} completed snapshot installation: last_applied={}, commit_index={}",
                self.id, self.last_applied, self.commit_index
            );
        } else {
            warn!(
                "Node {} snapshot installation failed: {}",
                self.id,
                result.reason.clone().unwrap_or("Unknown reason".into())
            );
        }

        self.current_snapshot_request_id = None;
    }

    /// Handle install snapshot response
    pub(crate) async fn handle_install_snapshot_response(
        &mut self,
        peer: RaftId,
        response: InstallSnapshotResponse,
    ) {
        if self.role != Role::Leader {
            return;
        }

        if response.term > self.current_term {
            warn!(
                "Node {} stepping down to Follower, found higher term {} from {} (current term {})",
                self.id, response.term, peer, self.current_term
            );
            // step_down_to_follower will clear all snapshot probes via clear_leader_state
            self.step_down_to_follower(Some(response.term)).await;
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

    /// Schedule snapshot status probe
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

    /// Extend snapshot probe schedule
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

    /// Remove snapshot probe schedule
    pub(crate) fn remove_snapshot_probe(&mut self, peer: &RaftId) {
        self.snapshot_probe_schedules.retain(|s| &s.peer != peer);
    }

    /// Process expired probe schedules
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

    /// Schedule snapshot retry
    pub(crate) async fn schedule_snapshot_retry(&mut self, peer: RaftId) {
        self.send_snapshot_to(peer).await;
    }

    /// Generate and persist snapshot
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

