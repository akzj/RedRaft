use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use tracing::{debug, trace, warn};

use crate::{RaftId, RaftStateOptions, RequestId};

/// Timeout queue cleanup threshold: triggers batch cleanup when expired entries exceed this count
const TIMEOUT_QUEUE_CLEANUP_THRESHOLD: usize = 100;

/// Pipeline state manager, responsible for handling feedback control and timeout management for InFlight requests
pub struct PipelineState {
    // Feedback control state
    /// peer -> (request_id -> (next_ack, send_time))
    inflight_requests: HashMap<RaftId, HashMap<RequestId, (u64, Instant)>>,
    /// Current batch size for each peer
    current_batch_size: HashMap<RaftId, u64>,
    /// Response time history window
    response_times: HashMap<RaftId, VecDeque<Duration>>,
    /// Success rate history window
    success_rates: HashMap<RaftId, VecDeque<bool>>,

    // Timeout management - ordered by send time
    /// peer -> time-ordered request queue (for efficient timeout checking)
    inflight_timeout_queue: HashMap<RaftId, VecDeque<(RequestId, Instant)>>,
    /// Accumulated stale entry count (used to trigger batch cleanup)
    stale_entry_count: usize,

    // Configuration options
    options: RaftStateOptions,
}

impl PipelineState {
    /// Create a new Pipeline state manager
    pub fn new(options: RaftStateOptions) -> Self {
        Self {
            inflight_requests: HashMap::new(),
            current_batch_size: HashMap::new(),
            response_times: HashMap::new(),
            success_rates: HashMap::new(),
            inflight_timeout_queue: HashMap::new(),
            stale_entry_count: 0,
            options,
        }
    }

    /// Check if requests can be sent to peer (InFlight limit)
    pub fn can_send_to_peer(&self, peer: &RaftId) -> bool {
        let inflight_count = self
            .inflight_requests
            .get(peer)
            .map(|requests| requests.len() as u64)
            .unwrap_or(0);

        inflight_count < self.options.max_inflight_requests
    }

    /// Get feedback-controlled batch size
    pub fn get_adaptive_batch_size(&mut self, peer: &RaftId) -> u64 {
        let current_batch = self
            .current_batch_size
            .get(peer)
            .copied()
            .unwrap_or(self.options.initial_batch_size);

        let avg_rtt = self.calculate_average_response_time(peer);
        let success_rate = self.calculate_success_rate(peer);

        let adjusted = self.adjust_batch_size_based_on_feedback(current_batch, avg_rtt, success_rate);
        self.current_batch_size.insert(peer.clone(), adjusted);

        trace!(
            "Batch size for {}: {} (rtt: {:?}, success: {:.0}%)",
            peer, adjusted, avg_rtt, success_rate * 100.0
        );

        adjusted
    }

    /// Track InFlight requests
    pub fn track_inflight_request(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack: u64,
        _send_time: Instant,
    ) {
        self.track_inflight_request_with_timeout(peer, request_id, next_ack);
    }

    /// Remove InFlight request (called on failed response)
    pub fn remove_inflight_request(&mut self, peer: &RaftId, request_id: RequestId) {
        if self.remove_inflight_request_with_request_id(peer, request_id) {
            // Mark that there are stale entries to cleanup (this entry still exists in timeout_queue)
            self.stale_entry_count += 1;
            self.maybe_cleanup_stale_entries();
        }
    }

    /// Record feedback for successful responses
    pub fn record_success_response(&mut self, peer: &RaftId, request_id: RequestId) {
        let removed_request = self
            .inflight_requests
            .get_mut(peer)
            .and_then(|peer_requests| peer_requests.remove(&request_id));

        if let Some((_, send_time)) = removed_request {
            let response_time = send_time.elapsed();

            trace!(
                "Response from {} (request: {}, rtt: {:?})",
                peer, request_id, response_time
            );

            self.record_response_feedback(peer, response_time, true);
            
            // Mark that there are stale entries to cleanup
            self.stale_entry_count += 1;
            self.maybe_cleanup_stale_entries();
        }
    }

    /// Record AppendEntries response feedback
    pub fn record_append_entries_response_feedback(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        success: bool,
    ) {
        if success {
            self.record_success_response(peer, request_id);
        } else {
            self.remove_inflight_request(peer, request_id);
        }
    }

    /// Optimized periodic timeout check - called during heartbeats
    pub fn periodic_timeout_check(&mut self) {
        self.check_and_cleanup_expired_requests();
    }

    /// Record response feedback data
    fn record_response_feedback(&mut self, peer: &RaftId, response_time: Duration, success: bool) {
        // Record response time
        let response_times = self.response_times.entry(peer.clone()).or_default();
        response_times.push_back(response_time);
        if response_times.len() > self.options.feedback_window_size {
            response_times.pop_front();
        }

        // Record success rate
        let success_rates = self.success_rates.entry(peer.clone()).or_default();
        success_rates.push_back(success);
        if success_rates.len() > self.options.feedback_window_size {
            success_rates.pop_front();
        }
    }

    /// Calculate average response time
    fn calculate_average_response_time(&self, peer: &RaftId) -> Option<Duration> {
        self.response_times.get(peer).and_then(|times| {
            if times.is_empty() {
                None
            } else {
                let total = times.iter().sum::<Duration>();
                Some(total / times.len() as u32)
            }
        })
    }

    /// Calculate success rate
    fn calculate_success_rate(&self, peer: &RaftId) -> f64 {
        self.success_rates
            .get(peer)
            .map(|rates| {
                if rates.is_empty() {
                    1.0 // Default success rate
                } else {
                    let success_count = rates.iter().filter(|&&success| success).count();
                    success_count as f64 / rates.len() as f64
                }
            })
            .unwrap_or(1.0)
    }

    /// Adjust batch size based on feedback
    fn adjust_batch_size_based_on_feedback(
        &self,
        current_batch: u64,
        avg_response_time: Option<Duration>,
        success_rate: f64,
    ) -> u64 {
        let mut new_batch = current_batch;

        // Adjust based on success rate
        if success_rate < 0.8 {
            // Low success rate, decrease batch size
            new_batch = (new_batch * 3 / 4).max(self.options.min_batch_size);
        } else if success_rate > 0.95 {
            // High success rate, can try to increase batch size
            new_batch = (new_batch * 5 / 4).min(self.options.max_batch_size);
        }

        // Adjust based on response time
        if let Some(response_time) = avg_response_time {
            let target = self.options.target_response_time;

            if response_time > target * 2 {
                // Response time too long, decrease batch size
                new_batch = (new_batch * 2 / 3).max(self.options.min_batch_size);
            } else if response_time < target / 2 {
                // Response time very short, can increase batch size
                new_batch = (new_batch * 6 / 5).min(self.options.max_batch_size);
            }
        }

        // Ensure within legal range
        new_batch.clamp(self.options.min_batch_size, self.options.max_batch_size)
    }

    /// Batch cleanup timeout queue when stale entries accumulate to threshold
    fn maybe_cleanup_stale_entries(&mut self) {
        if self.stale_entry_count < TIMEOUT_QUEUE_CLEANUP_THRESHOLD {
            return;
        }

        let mut cleaned = 0;
        for (peer, queue) in self.inflight_timeout_queue.iter_mut() {
            let inflight = self.inflight_requests.get(peer);
            let before_len = queue.len();
            
            // Keep entries that are still in inflight_requests
            queue.retain(|(req_id, _)| {
                inflight.is_some_and(|reqs| reqs.contains_key(req_id))
            });
            
            cleaned += before_len - queue.len();
        }

        if cleaned > 0 {
            debug!(
                "Cleaned {} stale entries from timeout queues (threshold: {})",
                cleaned, TIMEOUT_QUEUE_CLEANUP_THRESHOLD
            );
        }

        self.stale_entry_count = 0;
    }

    /// Simple and efficient timeout check - leverages time-ordered nature with smart timeout calculation
    fn check_and_cleanup_expired_requests(&mut self) {
        let now = Instant::now();
        let mut expired_items = Vec::new();
        let mut peer_timeouts = Vec::new();

        // Pre-calculate timeout durations for all peers to avoid borrowing conflicts
        for peer in self.inflight_timeout_queue.keys() {
            let timeout_duration = self.calculate_smart_timeout(peer);
            peer_timeouts.push((peer.clone(), timeout_duration));
        }

        // Iterate through each peer's timeout queue
        for (peer, timeout_duration) in peer_timeouts {
            if let Some(timeout_queue) = self.inflight_timeout_queue.get_mut(&peer) {
                // Check from the front of the queue, stop when encountering unexpired items
                while let Some((_request_id, send_time)) = timeout_queue.front() {
                    if now.duration_since(*send_time) > timeout_duration {
                        let (expired_request_id, expired_send_time) =
                            timeout_queue.pop_front().unwrap();
                        expired_items.push((
                            peer.clone(),
                            expired_request_id,
                            expired_send_time,
                            timeout_duration,
                        ));
                    } else {
                        // Encountered an unexpired request, subsequent ones won't be expired, stop immediately
                        break;
                    }
                }
            }
        }

        // Batch process expired requests
        for (peer, expired_request_id, expired_send_time, timeout_used) in expired_items {
            // Remove from inflight_requests
            if let Some(peer_requests) = self.inflight_requests.get_mut(&peer) {
                if peer_requests.remove(&expired_request_id).is_some() {
                    let elapsed = expired_send_time.elapsed();
                    warn!(
                        "Cleaned up expired inflight request {} to peer {} (elapsed: {:?}, timeout: {:?})",
                        expired_request_id, peer, elapsed, timeout_used
                    );

                    // Record as failed feedback
                    self.record_response_feedback(&peer, elapsed, false);
                }
            }
        }
    }

    /// Calculate smart timeout based on average response time
    fn calculate_smart_timeout(&self, peer: &RaftId) -> Duration {
        let avg_rtt = self.calculate_average_response_time(peer);

        if let Some(rtt) = avg_rtt {
            // Timeout = avg_rtt * factor, limited to reasonable range
            rtt.mul_f64(self.options.timeout_response_factor)
                .clamp(self.options.min_request_timeout, self.options.max_request_timeout)
        } else {
            // Use base timeout when there's no historical data
            self.options.base_request_timeout
        }
    }

    /// Add InFlight request to timeout queue
    fn track_inflight_request_with_timeout(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
        next_ack_index: u64,
    ) {
        let now = Instant::now();

        // Add to inflight_requests
        self.inflight_requests
            .entry(peer.clone())
            .or_default()
            .insert(request_id, (next_ack_index, now));

        // Add to the end of timeout queue (maintain time order)
        self.inflight_timeout_queue
            .entry(peer.clone())
            .or_default()
            .push_back((request_id, now));

        trace!(
            "Tracked inflight {} to {} (next_ack: {}, queue_size: {})",
            request_id,
            peer,
            next_ack_index,
            self.inflight_timeout_queue.get(peer).map(|q| q.len()).unwrap_or(0)
        );
    }

    /// Remove InFlight request (O(1) operation)
    /// 
    /// Note: Do not remove from timeout_queue because removal from VecDeque requires O(n).
    /// Timeout check will automatically skip entries not in inflight_requests.
    fn remove_inflight_request_with_request_id(
        &mut self,
        peer: &RaftId,
        request_id: RequestId,
    ) -> bool {
        self.inflight_requests
            .get_mut(peer)
            .and_then(|peer_requests| peer_requests.remove(&request_id))
            .is_some()
    }

    /// Get current total InFlight requests
    pub fn get_inflight_request_count(&self) -> usize {
        self.inflight_requests
            .values()
            .map(|peer_requests| peer_requests.len())
            .sum()
    }

    /// Get InFlight request count for specific peer
    pub fn get_peer_inflight_count(&self, peer: &RaftId) -> usize {
        self.inflight_requests
            .get(peer)
            .map(|reqs| reqs.len())
            .unwrap_or(0)
    }

    /// Get Pipeline statistics
    pub fn get_stats(&self) -> PipelineStats {
        let total_inflight = self.get_inflight_request_count();
        let total_timeout_queue: usize = self
            .inflight_timeout_queue
            .values()
            .map(|q| q.len())
            .sum();

        let peer_stats: HashMap<RaftId, PeerPipelineStats> = self
            .inflight_requests
            .keys()
            .map(|peer| {
                let inflight = self.get_peer_inflight_count(peer);
                let avg_rtt = self.calculate_average_response_time(peer);
                let success_rate = self.calculate_success_rate(peer);
                let batch_size = self.current_batch_size.get(peer).copied().unwrap_or(0);

                (
                    peer.clone(),
                    PeerPipelineStats {
                        inflight_count: inflight,
                        avg_response_time: avg_rtt,
                        success_rate,
                        current_batch_size: batch_size,
                    },
                )
            })
            .collect();

        PipelineStats {
            total_inflight,
            total_timeout_queue_size: total_timeout_queue,
            stale_entry_count: self.stale_entry_count,
            peer_stats,
        }
    }

    /// Clear all state (called on role change)
    pub fn clear_all(&mut self) {
        self.inflight_requests.clear();
        self.current_batch_size.clear();
        self.response_times.clear();
        self.success_rates.clear();
        self.inflight_timeout_queue.clear();
        self.stale_entry_count = 0;
    }
}

/// Pipeline overall statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    /// Total InFlight requests
    pub total_inflight: usize,
    /// Total timeout queue size (may contain processed stale entries)
    pub total_timeout_queue_size: usize,
    /// Stale entries pending cleanup
    pub stale_entry_count: usize,
    /// Per-peer statistics
    pub peer_stats: HashMap<RaftId, PeerPipelineStats>,
}

/// Single peer Pipeline statistics
#[derive(Debug, Clone)]
pub struct PeerPipelineStats {
    /// InFlight request count
    pub inflight_count: usize,
    /// Average response time
    pub avg_response_time: Option<Duration>,
    /// Success rate
    pub success_rate: f64,
    /// Current batch size
    pub current_batch_size: u64,
}
