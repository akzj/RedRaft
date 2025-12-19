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
    /// Handle election timeout
    pub(crate) async fn handle_election_timeout(&mut self) {
        if self.role == Role::Leader {
            info!(target: "raft", "Node {} is the leader and will not start a new election", self.id);
            return;
        }

        // Learner does not participate in elections
        if !self.config.voters_contains(&self.id) {
            warn!("Node {} is a Learner and cannot start an election", self.id);
            return;
        }

        // Check if Pre-Vote is enabled
        if self.options.pre_vote_enabled {
            self.start_pre_vote().await;
        } else {
            self.start_real_election().await;
        }
    }

    /// Start Pre-Vote phase (does not increment term)
    pub(crate) async fn start_pre_vote(&mut self) {
        info!(
            "Node {} starting pre-vote for prospective term {}",
            self.id,
            self.current_term + 1
        );

        // Generate Pre-Vote ID and initialize tracking state
        let pre_vote_id = RequestId::new();
        self.current_pre_vote_id = Some(pre_vote_id);
        self.pre_vote_votes.clear();
        self.pre_vote_votes.insert(self.id.clone(), true); // Vote for self

        // Reset election timer
        self.reset_election().await;

        // Get log information
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();

        let req = PreVoteRequest {
            term: self.current_term + 1, // Use prospective term, but don't actually increment
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: pre_vote_id,
        };

        // Send Pre-Vote requests
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

        // Single-node cluster: check result immediately
        self.check_pre_vote_result().await;
    }

    /// Start real election (increment term)
    pub(crate) async fn start_real_election(&mut self) {
        info!(
            "Node {} starting real election for term {}",
            self.id,
            self.current_term + 1
        );

        // Clean up Pre-Vote state
        self.current_pre_vote_id = None;
        self.pre_vote_votes.clear();

        // Switch to Candidate and increment term
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());

        // Persist state changes
        self.persist_hard_state().await;

        // Reset election timer
        self.reset_election().await;

        // Generate new election ID and initialize tracking state
        let election_id = RequestId::new();
        self.current_election_id = Some(election_id);
        self.election_votes.clear();
        self.election_votes.insert(self.id.clone(), true);
        self.election_max_term = self.current_term;

        // Get log information for election
        let last_log_index = self.get_last_log_index();
        let last_log_term = self.get_last_log_term();

        let req = RequestVoteRequest {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index,
            last_log_term,
            request_id: election_id,
        };

        // Send vote requests
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

        // Notify upper layer of state change
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

        // Single-node cluster: check result immediately
        self.check_election_result().await;
    }

    /// Handle Pre-Vote request
    pub(crate) async fn handle_pre_vote_request(
        &mut self,
        sender: RaftId,
        request: PreVoteRequest,
    ) {
        if sender != request.candidate_id {
            warn!(
                "Node {} received pre-vote request from {}, but candidate is {}",
                self.id, sender, request.candidate_id
            );
            return;
        }

        let mut vote_granted = false;

        // Pre-Vote condition check (Note: Does not modify own state!)
        // 1. prospective term >= current term
        // 2. logs must be up-to-date
        // 3. no recent Leader heartbeat (to avoid disturbing a running cluster)
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

        // Send response (using own current_term)
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

    /// Handle Pre-Vote response
    pub(crate) async fn handle_pre_vote_response(
        &mut self,
        peer: RaftId,
        response: PreVoteResponse,
    ) {
        // Check if it's the current Pre-Vote round
        if self.current_pre_vote_id != Some(response.request_id) {
            debug!(
                "Node {} ignoring stale pre-vote response from {} (expected {:?}, got {:?})",
                self.id, peer, self.current_pre_vote_id, response.request_id
            );
            return;
        }

        // Filter invalid voters
        if !self.config.voters_contains(&peer) {
            warn!(
                "Node {}: received pre-vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // If a higher term is discovered, no need to step down (since Pre-Vote doesn't increment term)
        // But need to update cluster state awareness
        if response.term > self.current_term {
            info!(
                "Node {} discovered higher term {} from {} during pre-vote (current term {})",
                self.id, response.term, peer, self.current_term
            );
            // Abandon current Pre-Vote
            self.current_pre_vote_id = None;
            self.pre_vote_votes.clear();
            return;
        }

        // Record Pre-Vote result
        self.pre_vote_votes
            .insert(peer.clone(), response.vote_granted);
        info!(
            "Node {} received pre-vote response from {}: granted={}",
            self.id, peer, response.vote_granted
        );

        // Check if majority is achieved
        self.check_pre_vote_result().await;
    }

    /// Check Pre-Vote result
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
            // Pre-Vote succeeded, starting real election
            self.start_real_election().await;
        }
    }

    /// Handle vote request
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

        // Handle higher term
        if request.term > self.current_term {
            info!(
                "Node {} stepping down to Follower, updating term from {} to {}",
                self.id, self.current_term, request.term
            );
            self.step_down_to_follower(Some(request.term)).await;
        }

        // Decide whether to vote
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

        // Send response
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

    /// Check if log is up to date
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

    /// Handle vote response
    pub(crate) async fn handle_request_vote_response(
        &mut self,
        peer: crate::types::RaftId,
        response: RequestVoteResponse,
    ) {
        info!("node {}: received vote response: {:?}", self.id, response);
        if self.role != Role::Candidate || self.current_election_id != Some(response.request_id) {
            return;
        }

        // Filter out invalid voters
        if !self.config.voters_contains(&peer) {
            warn!(
                "node {}: received vote response from unknown peer {}",
                self.id, peer
            );
            return;
        }

        // Handle higher term
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

        // Record vote result
        if response.term == self.current_term {
            self.election_votes.insert(peer, response.vote_granted);
        }

        // Check if election is won
        self.check_election_result().await;
    }

    /// Check election result
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

    /// Reset heartbeat timer
    pub(crate) async fn reset_heartbeat_timer(&mut self) {
        if let Some(timer_id) = self.heartbeat_interval_timer_id {
            self.callbacks.del_timer(&self.id, timer_id);
        }

        self.heartbeat_interval_timer_id = Some(
            self.callbacks
                .set_heartbeat_timer(&self.id, self.heartbeat_interval),
        );
    }

    /// Become Leader
    pub(crate) async fn become_leader(&mut self) {
        warn!(
            "Node {} becoming leader for term {} (previous role: {:?})",
            self.id, self.current_term, self.role
        );

        self.role = Role::Leader;
        self.current_election_id = None;
        self.leader_id = None;

        // Initialize replication state
        let last_log_index = self.get_last_log_index();
        self.next_index.clear();
        self.match_index.clear();
        self.follower_snapshot_states.clear();
        self.follower_last_snapshot_index.clear();
        self.snapshot_probe_schedules.clear();

        // Collect all nodes that need to be managed
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
                self.id, err
            );
        }

        // Send heartbeat immediately
        self.broadcast_append_entries().await;

        // Start log apply timer
        self.adjust_apply_interval().await;
    }

    /// Reset election timer
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
