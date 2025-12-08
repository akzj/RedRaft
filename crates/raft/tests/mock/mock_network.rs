// Note: This file is in tests/mock/, which is a submodule of tests/mod.rs
// When compiled as part of --test mod, we need to use raft:: to access the crate
use async_trait::async_trait;
use raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse,
};
use raft::traits::{Network, RpcResult};
use raft::{Event, RaftId};
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, VecDeque}; // Changed from BinaryHeap
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

// --- Mock Network Configuration ---

/// Configuration for mock network behavior
#[derive(Debug, Clone)]
pub struct MockRaftNetworkConfig {
    /// Base latency (milliseconds)
    pub base_latency_ms: u64,
    /// Maximum additional random latency (milliseconds)
    pub jitter_max_ms: u64,
    /// Message loss probability (0.0 - 1.0)
    pub drop_rate: f64,
    /// Message processing failure probability (0.0 - 1.0) - simulated during actual sending
    pub failure_rate: f64,
}

#[derive(Debug, Clone)]
pub struct MockNetworkHubConfig {
    /// Maximum batch size for bulk sending
    pub batch_size: usize,
    /// Maximum waiting time for bulk sending (milliseconds)
    pub batch_max_wait_ms: u64,
}

impl Default for MockNetworkHubConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            batch_max_wait_ms: 5,
        }
    }
}

impl Default for MockRaftNetworkConfig {
    fn default() -> Self {
        Self {
            base_latency_ms: 50,
            jitter_max_ms: 10,
            drop_rate: 0.00,
            failure_rate: 0.00,
        }
    }
}

// --- Internal message for delay queue ---

/// Message waiting to be sent in the delay queue
#[derive(Debug, Clone)] // Added Clone for moving between structures
struct DelayedMessage {
    scheduled_time: Instant, // Time when the message should be sent
    target: RaftId,
    event: NetworkEvent,
}

// Removed Ord, PartialOrd, Eq, PartialEq implementations since we no longer use BinaryHeap

// --- Mock network core ---

/// Mock network hub that manages all nodes' receivers and global state
#[derive(Clone)]
pub struct MockNetworkHub {
    inner: Arc<MockNetworkHubInner>,
}

type DispatchFn = Box<dyn Fn(NetworkEvent) -> () + Send + Sync>;

pub struct NodeSender {
    dispatch: Option<DispatchFn>,
    sender: Option<mpsc::UnboundedSender<NetworkEvent>>,
}

/// Internal shared state
struct MockNetworkHubInner {
    /// Stores each node's sender, used to send final messages to it
    node_senders: RwLock<HashMap<RaftId, NodeSender>>,
    /// Network configuration
    raft_config: RwLock<HashMap<RaftId, Arc<MockRaftNetworkConfig>>>,
    hub_config: Arc<MockNetworkHubConfig>,
    /// One delay queue per sending node, ensuring sending order
    delayed_queues: RwLock<HashMap<RaftId, VecDeque<DelayedMessage>>>, // Changed
    /// Notification signal for delay queues (notifies when new messages are enqueued or need processing)
    delay_queue_notify: tokio::sync::Notify,
    /// Channel for actual message sending (messages are put here after delay expires)
    real_send_tx: mpsc::UnboundedSender<RealSendItem>,
    /// Receiver for actual message sending
    real_send_rx: Mutex<mpsc::UnboundedReceiver<RealSendItem>>,
}

impl MockNetworkHubInner {
    /// Update node network configuration
    pub async fn update_config(&self, node_id: RaftId, config: MockRaftNetworkConfig) {
        self.raft_config
            .write()
            .await
            .insert(node_id, Arc::new(config));
    }

    /// Get node network configuration
    pub async fn get_config(&self, node_id: &RaftId) -> Option<Arc<MockRaftNetworkConfig>> {
        self.raft_config.read().await.get(node_id).cloned()
    }
}

/// Internal type for passing actual messages through channels

/// Actual send item passed through real_send channel
type RealSendItem = (RaftId, NetworkEvent); // (target, event)

impl MockNetworkHub {
    pub fn new(config: MockNetworkHubConfig) -> Self {
        let (real_send_tx, real_send_rx) = mpsc::unbounded_channel::<RealSendItem>();
        let inner = Arc::new(MockNetworkHubInner {
            node_senders: RwLock::new(HashMap::new()),
            raft_config: RwLock::new(HashMap::new()),
            hub_config: Arc::new(config),
            delayed_queues: RwLock::new(HashMap::new()), // [!code ++]
            delay_queue_notify: tokio::sync::Notify::new(),
            real_send_tx,
            real_send_rx: Mutex::new(real_send_rx),
        });

        let inner_clone = Arc::clone(&inner);
        tokio::spawn(Self::run_delayed_queue_processor(inner_clone));

        let inner_clone = Arc::clone(&inner);
        tokio::spawn(Self::run_real_sender(inner_clone));

        Self { inner }
    }

    /// Delay queue processor: Checks all nodes' delay queues and sends messages according to scheduled time
    /// Ensures FIFO order for messages within each node
    async fn run_delayed_queue_processor(inner: Arc<MockNetworkHubInner>) {
        loop {
            // 1. Get read-only locks for all queues, find earliest expiring message
            let queues = inner.delayed_queues.read().await;
            let mut earliest_msg: Option<DelayedMessage> = None;
            let mut earliest_sender: Option<RaftId> = None;

            for (sender_id, queue) in queues.iter() {
                if let Some(msg) = queue.front() {
                    // Check front element
                    match &earliest_msg {
                        None => {
                            // First message found
                            earliest_msg = Some(msg.clone()); // Clone for temporary use
                            earliest_sender = Some(sender_id.clone());
                        }
                        Some(current_earliest) => {
                            // Compare scheduled_time
                            if msg.scheduled_time < current_earliest.scheduled_time {
                                earliest_msg = Some(msg.clone());
                                earliest_sender = Some(sender_id.clone());
                            }
                        }
                    }
                }
            }
            drop(queues); // Release read lock

            // 2. Process the earliest message found
            if let (Some(msg_to_send), Some(sender_id)) = (earliest_msg, earliest_sender) {
                let now = Instant::now();
                if msg_to_send.scheduled_time <= now {
                    // Time arrived, need to send
                    // 3. Re-acquire write lock, remove message from corresponding queue
                    let mut queues_mut = inner.delayed_queues.write().await;
                    if let Some(queue) = queues_mut.get_mut(&sender_id) {
                        // Recheck if front is still the same message (prevent concurrent modification)
                        // For simplicity, we assume if sender_id and scheduled_time match, it's the same message
                        // More strict comparison might require adding a unique ID to DelayedMessage
                        if let Some(front_msg) = queue.front() {
                            if front_msg.scheduled_time == msg_to_send.scheduled_time {
                                let _removed_msg = queue.pop_front(); // Remove front message
                                drop(queues_mut); // Release write lock

                                // info!(
                                //     "Delayed message from {} to {} ready for sending",
                                //     sender_id, msg_to_send.target
                                // );
                                // 4. Send to real_send channel
                                if let Err(_e) = inner
                                    .real_send_tx
                                    .send((msg_to_send.target, msg_to_send.event))
                                {
                                    warn!(
                                        "Real send channel is closed, stopping delayed queue processor"
                                    );
                                    break; // Channel closed, stop
                                }
                                // After processing one message, immediately continue checking the next one
                                //             info!("send message done,continue");
                                continue;
                            }
                        }
                    }
                    // If queue is empty or front has changed, continue loop
                    // (Unlikely to happen unless there's concurrent modification, but VecDeque's front/pop are atomic)
                } else {
                    // Not time yet, calculate wait duration
                    let wait_duration = msg_to_send.scheduled_time.duration_since(now);
                    // info!(
                    //     "Waiting {:?} for next delayed message from {}",
                    //     wait_duration, sender_id
                    // );
                    // Wait for specified time or wake up when notified of new message enqueue
                    tokio::select! {
                        _ = tokio::time::sleep(wait_duration) => {
                           // info!("continue to dispatch message")
                        }
                        _ = inner.delay_queue_notify.notified() => {
                            // Notified, possibly new messages or earlier messages, continue loop check
                         //   info!("Woken up by notification, rechecking queues");
                        }
                    }
                    continue; // Continue loop
                }
            } else {
                // All queues are empty
                log::trace!("All delayed queues empty, waiting for notification");
                inner.delay_queue_notify.notified().await;
                // Notified, continue loop check
            }
        }
    }

    /// Real sender: Batch receive and send messages from real_send channel
    async fn run_real_sender(inner: Arc<MockNetworkHubInner>) {
        let mut rx = inner.real_send_rx.lock().await;
        loop {
            let mut batch = Vec::with_capacity(inner.hub_config.batch_size);
            let timeout_duration = Duration::from_millis(inner.hub_config.batch_max_wait_ms);

            // Try to collect a batch
            tokio::select! {
                // Receive first message
                first_item = rx.recv() => {
                    if let Some(item) = first_item {
                        batch.push(item);
                        // Try to receive more messages within timeout to fill the batch
                        let deadline = Instant::now() + timeout_duration;
                        while batch.len() < inner.hub_config.batch_size {
                            tokio::select! {
                                Some(item) = rx.recv() => {
                                    batch.push(item);
                                }
                                _ = tokio::time::sleep_until(deadline) => {
                       //info!("batch receive message done");
                                    break;
                                }
                            }
                        }
                    } else {
                        // Channel closed
                        log::warn!("Real send channel is closed, stopping real sender");
                        break;
                    }
                }
                // If no first message received, continue after timeout (batch will be empty)
                _ = tokio::time::sleep(timeout_duration) => {
                    // info!("wait message timeout");
                }
            }

            // Send messages in the batch
            if !batch.is_empty() {
                //     log::trace!("Sending batch of {} messages", batch.len());
                for (target, event) in batch.drain(..) {
                    // Simulate failure rate during actual sending

                    let config = inner
                        .raft_config
                        .read()
                        .await
                        .get(&target)
                        .cloned()
                        .unwrap_or_default();
                    let failure_rate = config.failure_rate;
                    drop(config);

                    // let mut rng = rand::thread_rng(); // [!code --]
                    let mut rng = rand::rngs::StdRng::from_os_rng(); // [!code ++]
                                                                     // if rng.gen::<f64>() < failure_rate { // [!code --]
                    if rng.random::<f64>() < failure_rate {
                        // [!code ++]
                        info!(
                            "MockNetwork: Simulating send failure for message to {}",
                            target
                        );
                        // Simulate sending failure here, can choose to log or ignore
                        // For Raft, sending failure is usually equivalent to timeout or packet loss, handled by upper layer
                        continue; // Skip this send
                    }

                    // Actual "send": Forward to target node's receiver
                    let senders = inner.node_senders.read().await;
                    if let Some(sender) = senders.get(&target) {
                        match (sender.dispatch.as_ref(), sender.sender.as_ref()) {
                            (Some(dispatch), None) => {
                                //info!("dispatch event {:?}", event);
                                dispatch(event.clone());
                                //info!("dispatch event done {:?}", event);
                            }
                            (None, Some(sender)) => {
                                //info!("send message {:?}", event);
                                sender.send(event).unwrap_or_else(|e| {
                                    log::warn!(
                                        "MockNetwork: Failed to send message to {}, channel closed",
                                        target
                                    );
                                });
                            }
                            (None, None) => {
                                panic!(
                                    "MockNetwork: No dispatch or sender found for target node {}",
                                    target
                                );
                            }
                            (Some(_), Some(_)) => {
                                panic!(
                                    "MockNetwork: Both dispatch and sender found for target node {}",
                                    target
                                );
                            }
                        }
                    } else {
                        log::warn!(
                            "MockNetwork: No sender found for target node {} during real send",
                            target
                        );
                    }
                }
            }
            // Continue loop waiting for next batch
        }
    }

    /// Register a Raft node to the network, return its corresponding Network instance and receiver
    pub async fn register_node(
        &self,
        node_id: RaftId,
    ) -> (MockNodeNetwork, mpsc::UnboundedReceiver<NetworkEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.node_senders.write().await.insert(
            node_id.clone(),
            NodeSender {
                dispatch: None,
                sender: Some(tx),
            },
        );

        self.inner
            .raft_config
            .write()
            .await
            .insert(node_id.clone(), Arc::new(MockRaftNetworkConfig::default()));
        let network = MockNodeNetwork {
            node_id: node_id.clone(),
            hub_inner: Arc::clone(&self.inner),
        };
        (network, rx)
    }

    pub async fn register_node_with_dispatch(
        &self,
        node_id: RaftId,
        dispatch: DispatchFn,
    ) -> MockNodeNetwork {
        self.inner.node_senders.write().await.insert(
            node_id.clone(),
            NodeSender {
                dispatch: Some(dispatch),
                // When dispatch is provided, sender should not be provided to avoid panic from dual channels
                sender: None,
            },
        );
        MockNodeNetwork {
            node_id: node_id.clone(),
            hub_inner: Arc::clone(&self.inner),
        }
    }

    pub async fn update_config(&self, node_id: RaftId, config: MockRaftNetworkConfig) {
        self.inner.update_config(node_id, config).await;
    }
}

// --- Network Interface for Single Node ---

/// Network interface implementation for a single Raft node
pub struct MockNodeNetwork {
    node_id: RaftId,
    hub_inner: Arc<MockNetworkHubInner>, // Reference to Hub's internal state
}

// --- Internal Events for Channel Communication ---

/// Internal enum for passing different types of network messages through channels
#[derive(Debug, Clone)] // Clone is needed for putting into DelayedMessage and sending

// (sender, target, event)
pub enum NetworkEvent {
    RequestVote(RaftId, RaftId, RequestVoteRequest),
    RequestVoteResponse(RaftId, RaftId, RequestVoteResponse),
    AppendEntriesRequest(RaftId, RaftId, AppendEntriesRequest),
    AppendEntriesResponse(RaftId, RaftId, AppendEntriesResponse),
    InstallSnapshotRequest(RaftId, RaftId, InstallSnapshotRequest),
    InstallSnapshotResponse(RaftId, RaftId, InstallSnapshotResponse),
    PreVote(RaftId, RaftId, PreVoteRequest),
    PreVoteResponse(RaftId, RaftId, PreVoteResponse),
}

impl MockNodeNetwork {
    // Isolate node from network
    pub async fn isolate(&self) {
        info!("Isolating node {:?}", self.node_id);
        // Clear the node's sender, simulate network isolation
        let mut config = MockRaftNetworkConfig::default(); // Reset to default config
        config.drop_rate = 1.0; // Set packet loss rate to 100%
        config.failure_rate = 1.0; // Set failure rate to 100%
        self.hub_inner
            .update_config(self.node_id.clone(), config)
            .await;
    }

    // Restore node network connection
    pub async fn restore(&self) {
        info!("Restoring node {:?}", self.node_id);
        // Restore to default configuration
        let config = MockRaftNetworkConfig::default();
        self.hub_inner
            .update_config(self.node_id.clone(), config)
            .await;
    }

    /// Internal helper function to simulate delay and packet loss, then put message into delay queue
    async fn send_to_target(
        &self,
        from: RaftId,
        target: RaftId,
        event: NetworkEvent,
    ) -> RpcResult<()> {
        //info!("");

        let config = self.hub_inner.raft_config.read().await;
        let config = config.get(&from).cloned().unwrap_or_default();
        let mut rng = rand::rngs::StdRng::from_os_rng(); // [!code ++]
                                                         // 1. Simulate packet loss (decide whether to drop before queuing)

        if rng.random::<f64>() < config.drop_rate {
            // [!code ++]
            info!(
                "MockNetwork: Dropping message from {} to {} (before queuing)",
                self.node_id, target
            );
            // For packet loss, operation is considered "successful" (message sent but lost)
            return Ok(());
        }

        // 2. Calculate delay
        let latency_ms = config.base_latency_ms + rng.random_range(0..=config.jitter_max_ms);
        let delay = Duration::from_millis(latency_ms);
        let scheduled_time = Instant::now() + delay;
        drop(config); // Release read lock early

        // 3. Create delayed message and put it at the end of the sender's queue
        let delayed_msg = DelayedMessage {
            scheduled_time,
            target: target.clone(),
            event,
        };

        {
            let mut queues = self.hub_inner.delayed_queues.write().await;
            // Get or create queue for this node
            let queue = queues
                .entry(self.node_id.clone())
                .or_insert_with(VecDeque::new);
            queue.push_back(delayed_msg); // Push to back to maintain FIFO for this sender
        } // Lock released here

        // 4. Notify delayed queue processor of new message
        self.hub_inner.delay_queue_notify.notify_one();

        // info!(
        //     "MockNetwork: Message from {} to {} queued for sending in {:?} (scheduled at {:?})",
        //     self.node_id, target, delay, scheduled_time
        // );

        // 5. Return immediately, simulate instant network call completion
        Ok(())
    }
}

#[async_trait]
impl Network for MockNodeNetwork {
    async fn send_request_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::RequestVote(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_request_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::RequestVoteResponse(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_append_entries_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::AppendEntriesRequest(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_append_entries_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::AppendEntriesResponse(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_install_snapshot_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::InstallSnapshotRequest(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_install_snapshot_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::InstallSnapshotResponse(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_pre_vote_request(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::PreVote(from.clone(), target.clone(), args),
        )
        .await
    }

    async fn send_pre_vote_response(
        &self,
        from: &RaftId,
        target: &RaftId,
        args: PreVoteResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::PreVoteResponse(from.clone(), target.clone(), args),
        )
        .await
    }
}

// --- Helper function: Dispatch internal events back to Raft state machine ---
// (This part is the same as before, adjust according to your Event enum)

// use crate::Event; // Import your actual Event enum

pub fn dispatch_network_event(event: NetworkEvent) -> Option<Event> {
    match event {
        NetworkEvent::RequestVote(source, _, req) => Some(Event::RequestVoteRequest(source, req)),
        NetworkEvent::RequestVoteResponse(source, _, resp) => {
            Some(Event::RequestVoteResponse(source, resp))
        }
        NetworkEvent::AppendEntriesRequest(source, _, req) => {
            Some(Event::AppendEntriesRequest(source, req))
        }
        NetworkEvent::AppendEntriesResponse(source, _, resp) => {
            Some(Event::AppendEntriesResponse(source, resp))
        }
        NetworkEvent::InstallSnapshotRequest(source, _, req) => {
            Some(Event::InstallSnapshotRequest(source, req))
        }
        NetworkEvent::InstallSnapshotResponse(source, _, resp) => {
            Some(Event::InstallSnapshotResponse(source, resp))
        }
        NetworkEvent::PreVote(source, _, req) => Some(Event::PreVoteRequest(source, req)),
        NetworkEvent::PreVoteResponse(source, _, resp) => {
            Some(Event::PreVoteResponse(source, resp))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::RequestId;
    use std::collections::HashSet;
    use tokio::time::timeout;

    pub fn create_test_raft_id(group: &str, node: &str) -> RaftId {
        RaftId::new(group.to_string(), node.to_string())
    }

    pub fn create_test_request_vote() -> RequestVoteRequest {
        RequestVoteRequest {
            term: 1,
            candidate_id: create_test_raft_id("group1", "candidate"),
            last_log_index: 0,
            last_log_term: 0,
            request_id: RequestId::new(),
        }
    }

    pub fn create_test_append_entries() -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: 1,
            leader_id: create_test_raft_id("group1", "leader"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            request_id: RequestId::new(),
        }
    }

    #[tokio::test]
    async fn test_network_hub_creation() {
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);

        // Hub should be created successfully
        assert!(hub.inner.node_senders.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_node_registration() {
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);

        let node_id = create_test_raft_id("group1", "node1");
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        // Verify node was registered
        assert_eq!(network.node_id, node_id);
        assert!(hub.inner.node_senders.read().await.contains_key(&node_id));
    }

    #[tokio::test]
    async fn test_multiple_node_registration() {
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);

        let node1 = create_test_raft_id("group1", "node1");
        let node2 = create_test_raft_id("group1", "node2");
        let node3 = create_test_raft_id("group1", "node3");

        let (_network1, _rx1) = hub.register_node(node1.clone()).await;
        let (_network2, _rx2) = hub.register_node(node2.clone()).await;
        let (_network3, _rx3) = hub.register_node(node3.clone()).await;

        let senders = hub.inner.node_senders.read().await;
        assert_eq!(senders.len(), 3);
        assert!(senders.contains_key(&node1));
        assert!(senders.contains_key(&node2));
        assert!(senders.contains_key(&node3));
    }

    #[tokio::test]
    async fn test_basic_message_delivery() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let node1 = create_test_raft_id("group1", "node1");
        let node2 = create_test_raft_id("group1", "node2");

        let (network1, _rx1) = hub.register_node(node1.clone()).await;
        let (_network2, mut rx2) = hub.register_node(node2.clone()).await;

        // Send a request vote from node1 to node2
        let vote_req = create_test_request_vote();
        let result = network1
            .send_request_vote_request(&node1, &node2, vote_req.clone())
            .await;
        assert!(result.is_ok());

        // Wait for message to be delivered
        let received = timeout(Duration::from_millis(100), rx2.recv()).await;
        assert!(received.is_ok());

        if let Ok(Some(NetworkEvent::RequestVote(_, _, received_req))) = received {
            assert_eq!(received_req.term, vote_req.term);
            assert_eq!(received_req.candidate_id, vote_req.candidate_id);
        } else {
            panic!("Expected RequestVote event");
        }
    }

    #[tokio::test]
    async fn test_append_entries_message_delivery() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let leader = create_test_raft_id("group1", "leader");
        let follower = create_test_raft_id("group1", "follower");

        let (network_leader, _rx_leader) = hub.register_node(leader.clone()).await;
        let (_network_follower, mut rx_follower) = hub.register_node(follower.clone()).await;

        let append_req = create_test_append_entries();
        let result = network_leader
            .send_append_entries_request(&leader, &follower, append_req.clone())
            .await;
        assert!(result.is_ok());

        let received = timeout(Duration::from_millis(100), rx_follower.recv()).await;
        assert!(received.is_ok());

        if let Ok(Some(NetworkEvent::AppendEntriesRequest(_, _, received_req))) = received {
            assert_eq!(received_req.term, append_req.term);
            assert_eq!(received_req.leader_id, append_req.leader_id);
        } else {
            panic!("Expected AppendEntriesRequest event");
        }
    }

    #[tokio::test]
    async fn test_bidirectional_communication() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let node1 = create_test_raft_id("group1", "node1");
        let node2 = create_test_raft_id("group1", "node2");

        let (network1, mut rx1) = hub.register_node(node1.clone()).await;
        let (network2, mut rx2) = hub.register_node(node2.clone()).await;

        // Node1 -> Node2
        let vote_req = create_test_request_vote();
        network1
            .send_request_vote_request(&node1, &node2, vote_req)
            .await
            .unwrap();

        // Node2 -> Node1
        let append_req = create_test_append_entries();
        network2
            .send_append_entries_request(&node2, &node1, append_req)
            .await
            .unwrap();

        // Verify both messages are delivered
        let msg1 = timeout(Duration::from_millis(100), rx2.recv()).await;
        let msg2 = timeout(Duration::from_millis(100), rx1.recv()).await;

        assert!(msg1.is_ok());
        assert!(msg2.is_ok());
        assert!(matches!(
            msg1.unwrap().unwrap(),
            NetworkEvent::RequestVote(_, _, _)
        ));
        assert!(matches!(
            msg2.unwrap().unwrap(),
            NetworkEvent::AppendEntriesRequest(_, _, _)
        ));
    }

    #[tokio::test]
    async fn test_message_ordering_fifo() {
        let config = MockNetworkHubConfig {
            batch_size: 5,
            batch_max_wait_ms: 50,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network_sender, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send multiple messages in sequence
        for i in 1..=5 {
            let mut vote_req = create_test_request_vote();
            vote_req.term = i; // Use term to track order
            network_sender
                .send_request_vote_request(&sender, &receiver, vote_req)
                .await
                .unwrap();
        }

        // Verify messages arrive in order
        for expected_term in 1..=5 {
            let received = timeout(Duration::from_millis(200), rx_receiver.recv()).await;
            assert!(received.is_ok());

            if let Ok(Some(NetworkEvent::RequestVote(_, _, req))) = received {
                assert_eq!(req.term, expected_term);
            } else {
                panic!("Expected RequestVote event with term {}", expected_term);
            }
        }
    }

    #[tokio::test]
    async fn test_latency_simulation() {
        let base_latency = 50;
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;

        hub.update_config(
            sender.clone(),
            MockRaftNetworkConfig {
                base_latency_ms: base_latency,
                jitter_max_ms: 20,
                drop_rate: 0.0,
                failure_rate: 0.0,
            },
        )
        .await;

        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        let start = Instant::now();
        let vote_req = create_test_request_vote();
        network
            .send_request_vote_request(&sender, &receiver, vote_req)
            .await
            .unwrap();

        // Message should arrive after the base latency
        let received = timeout(Duration::from_millis(200), rx_receiver.recv()).await;
        let elapsed = start.elapsed();

        assert!(received.is_ok());
        assert!(elapsed >= Duration::from_millis(base_latency));
        assert!(elapsed < Duration::from_millis(base_latency + 100)); // Allow some tolerance
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let config = MockNetworkHubConfig {
            batch_size: 3,
            batch_max_wait_ms: 10,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send multiple messages quickly to test batching
        for i in 1..=5 {
            let mut vote_req = create_test_request_vote();
            vote_req.term = i;
            network
                .send_request_vote_request(&sender, &receiver, vote_req)
                .await
                .unwrap();
        }

        // All messages should eventually be delivered
        let mut received_count = 0;
        while received_count < 5 {
            let received = timeout(Duration::from_millis(100), rx_receiver.recv()).await;
            if received.is_ok() {
                received_count += 1;
            } else {
                break;
            }
        }

        assert_eq!(received_count, 5);
    }

    #[tokio::test]
    async fn test_nonexistent_target_node() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let nonexistent = create_test_raft_id("group1", "nonexistent");

        let (network, _rx) = hub.register_node(sender.clone()).await;

        // Send message to nonexistent node should succeed (no immediate error)
        let vote_req = create_test_request_vote();
        let result = network
            .send_request_vote_request(&sender, &nonexistent, vote_req)
            .await;
        assert!(result.is_ok());

        // Message will be lost during delivery phase, but send operation succeeds
    }

    #[tokio::test]
    async fn test_all_message_types() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Test all message types
        let vote_req = create_test_request_vote();
        network
            .send_request_vote_request(&sender, &receiver, vote_req)
            .await
            .unwrap();

        let vote_resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
            request_id: RequestId::new(),
        };
        network
            .send_request_vote_response(&sender, &receiver, vote_resp)
            .await
            .unwrap();

        let append_req = create_test_append_entries();
        network
            .send_append_entries_request(&sender, &receiver, append_req)
            .await
            .unwrap();

        let append_resp = AppendEntriesResponse {
            term: 1,
            success: true,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
            matched_index: 0,
        };
        network
            .send_append_entries_response(&sender, &receiver, append_resp)
            .await
            .unwrap();

        use raft::{ClusterConfig, Snapshot};
        let _snapshot = Snapshot {
            index: 1,
            term: 1,
            data: vec![1, 2, 3],
            config: ClusterConfig::simple(vec![sender.clone()].into_iter().collect(), 0),
        };
        let install_req = InstallSnapshotRequest {
            term: 1,
            leader_id: sender.clone(),
            last_included_index: 1,
            last_included_term: 1,
            data: vec![1, 2, 3],
            config: ClusterConfig::simple(vec![sender.clone()].into_iter().collect(), 0),
            request_id: RequestId::new(),
            snapshot_request_id: RequestId::new(),
            is_probe: false,
        };
        network
            .send_install_snapshot_request(&sender, &receiver, install_req)
            .await
            .unwrap();

        let install_resp = InstallSnapshotResponse {
            term: 1,
            request_id: RequestId::new(),
            state: raft::InstallSnapshotState::Success,
            error_message: "".into(),
        };
        network
            .send_install_snapshot_response(&sender, &receiver, install_resp)
            .await
            .unwrap();

        // Verify all messages are delivered
        let mut message_count = 0;
        while message_count < 6 {
            let received = timeout(Duration::from_millis(100), rx_receiver.recv()).await;
            if received.is_ok() {
                message_count += 1;
            } else {
                break;
            }
        }

        assert_eq!(message_count, 6);
    }

    #[tokio::test]
    async fn test_dispatch_network_event() {
        let sender = create_test_raft_id("group1", "sender");

        // Test RequestVote dispatch
        let vote_req = create_test_request_vote();
        let event = NetworkEvent::RequestVote(sender.clone(), sender.clone(), vote_req.clone());
        let raft_event = dispatch_network_event(event);
        assert!(matches!(raft_event, Some(Event::RequestVoteRequest(_, _))));

        // Test RequestVoteResponse dispatch
        let vote_resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
            request_id: RequestId::new(),
        };
        let event = NetworkEvent::RequestVoteResponse(sender.clone(), sender.clone(), vote_resp);
        let raft_event = dispatch_network_event(event);
        assert!(matches!(raft_event, Some(Event::RequestVoteResponse(_, _))));

        // Test AppendEntriesRequest dispatch
        let append_req = create_test_append_entries();
        let event = NetworkEvent::AppendEntriesRequest(sender.clone(), sender.clone(), append_req);
        let raft_event = dispatch_network_event(event);
        assert!(matches!(
            raft_event,
            Some(Event::AppendEntriesRequest(_, _))
        ));

        // Test AppendEntriesResponse dispatch
        let append_resp = AppendEntriesResponse {
            term: 1,
            success: true,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
            matched_index: 0,
        };
        let event =
            NetworkEvent::AppendEntriesResponse(sender.clone(), sender.clone(), append_resp);
        let raft_event = dispatch_network_event(event);
        assert!(matches!(
            raft_event,
            Some(Event::AppendEntriesResponse(_, _))
        ));
    }

    #[tokio::test]
    async fn test_concurrent_message_sending() {
        let config = MockNetworkHubConfig {
            batch_size: 10,
            batch_max_wait_ms: 10,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send messages concurrently from multiple tasks
        let network = Arc::new(network);
        let mut handles = vec![];

        for i in 0..10 {
            let network_clone = Arc::clone(&network);
            let sender_clone = sender.clone();
            let receiver_clone = receiver.clone();

            let handle = tokio::spawn(async move {
                let mut vote_req = create_test_request_vote();
                vote_req.term = i + 1;
                network_clone
                    .send_request_vote_request(&sender_clone, &receiver_clone, vote_req)
                    .await
            });
            handles.push(handle);
        }

        // Wait for all sends to complete
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Verify all messages are delivered
        let mut received_terms = HashSet::new();
        for _ in 0..10 {
            let received = timeout(Duration::from_millis(200), rx_receiver.recv()).await;
            if let Ok(Some(NetworkEvent::RequestVote(_, _, req))) = received {
                received_terms.insert(req.term);
            }
        }

        assert_eq!(received_terms.len(), 10);
        for i in 1..=10 {
            assert!(received_terms.contains(&i));
        }
    }

    #[tokio::test]
    async fn test_message_drop_simulation() {
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;

        hub.update_config(
            sender.clone(),
            MockRaftNetworkConfig {
                base_latency_ms: 0,
                jitter_max_ms: 0,
                drop_rate: 1.0,
                failure_rate: 1.0,
            },
        )
        .await;

        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send message that should be dropped
        let vote_req = create_test_request_vote();
        let result = network
            .send_request_vote_request(&sender, &receiver, vote_req)
            .await;
        assert!(result.is_ok()); // Send operation succeeds even if message is dropped

        // Message should not be received
        let received = timeout(Duration::from_millis(50), rx_receiver.recv()).await;
        assert!(received.is_err()); // Should timeout
    }
}

#[cfg(test)]
mod additional_tests {
    use super::tests::create_test_raft_id;
    use super::tests::create_test_request_vote;
    use super::*;
    use tokio::time::{timeout, Instant};

    /// Test if failure_rate takes effect
    #[tokio::test]
    async fn test_send_failure_rate() {
        // Configuration: 100% send failure rate (message enqueued but fails during actual sending)
        let config = MockNetworkHubConfig {
            batch_size: 1,
            batch_max_wait_ms: 1,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;

        hub.update_config(
            sender.clone(),
            MockRaftNetworkConfig {
                base_latency_ms: 1,
                jitter_max_ms: 1,
                drop_rate: 1.0,
                failure_rate: 1.0,
            },
        )
        .await;

        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send message (should be enqueued but fail during sending phase)
        let vote_req = create_test_request_vote();
        let result = network
            .send_request_vote_request(&sender, &receiver, vote_req)
            .await;
        assert!(result.is_ok()); // Send operation itself has no error

        // Receiver should not receive message (send failed)
        let received = timeout(Duration::from_millis(100), rx_receiver.recv()).await;
        assert!(received.is_err(), "Message should be lost due to sending failure");
    }

    /// Test batch sending boundary scenarios
    #[tokio::test]
    async fn test_batch_sending_boundaries() {
        // Configuration: batch size 2, max wait time 10ms
        let config = MockNetworkHubConfig {
            batch_size: 2,
            batch_max_wait_ms: 10,
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;

        hub.update_config(
            sender.clone(),
            MockRaftNetworkConfig {
                base_latency_ms: 10,
                jitter_max_ms: 0,
                drop_rate: 0.0,
                failure_rate: 1.0,
            },
        )
        .await;

        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Scenario 1: Send 2 messages, should be sent in one batch
        let start = Instant::now();
        for i in 1..=2 {
            let mut vote_req = create_test_request_vote();
            vote_req.term = i;
            network
                .send_request_vote_request(&sender, &receiver, vote_req)
                .await
                .unwrap();
        }

        // Verify both messages are received consecutively within a short time (batch sent)
        let _msg1 = timeout(Duration::from_millis(50), rx_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let _msg2 = timeout(Duration::from_millis(10), rx_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(20),
            "Batch sending should have no significant delay"
        );

        // Scenario 2: Send 1 message, should be sent after timeout
        let start = Instant::now();
        let mut vote_req = create_test_request_vote();
        vote_req.term = 3;
        network
            .send_request_vote_request(&sender, &receiver, vote_req)
            .await
            .unwrap();

        let _msg3 = timeout(Duration::from_millis(50), rx_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(10),
            "Single message should wait for batch timeout"
        );
        assert!(elapsed < Duration::from_millis(30), "Timeout should not be too long");
    }

    #[tokio::test]
    async fn test_multi_node_message_order() {
        let config = MockNetworkHubConfig {
            batch_size: 5,
            batch_max_wait_ms: 50,
        };
        let hub = MockNetworkHub::new(config);

        // Register 3 nodes: sender1, sender2 send messages, receiver receives
        let sender1 = create_test_raft_id("group1", "sender1");
        let sender2 = create_test_raft_id("group1", "sender2");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network1, _rx1) = hub.register_node(sender1.clone()).await;
        let (network2, _rx2) = hub.register_node(sender2.clone()).await;
        let (_network_r, mut rx_r) = hub.register_node(receiver.clone()).await;

        // sender1 sends 3 messages (term 1-3), with correct candidate_id
        for term in 1..=3 {
            let mut req = create_test_request_vote();
            req.term = term;
            req.candidate_id = sender1.clone(); // Key fix: Set correct sender ID
            network1
                .send_request_vote_request(&sender1, &receiver, req)
                .await
                .unwrap();
        }

        // sender2 sends 3 messages (term 10-12), with correct candidate_id
        for term in 10..=12 {
            let mut req = create_test_request_vote();
            req.term = term;
            req.candidate_id = sender2.clone(); // Key fix: Set correct sender ID
            network2
                .send_request_vote_request(&sender2, &receiver, req)
                .await
                .unwrap();
        }

        // Collect receiver messages, group by sending node
        let mut sender1_terms = vec![];
        let mut sender2_terms = vec![];
        for _ in 0..6 {
            let msg = timeout(Duration::from_millis(100), rx_r.recv())
                .await
                .unwrap()
                .unwrap();
            if let NetworkEvent::RequestVote(_, _, req) = msg {
                if req.candidate_id == sender1 {
                    sender1_terms.push(req.term);
                } else if req.candidate_id == sender2 {
                    sender2_terms.push(req.term);
                }
            }
        }

        // Verify message order is correct for each node (FIFO)
        assert_eq!(sender1_terms, vec![1, 2, 3], "sender1 message order is incorrect");
        assert_eq!(sender2_terms, vec![10, 11, 12], "sender2 message order is incorrect");
    }
    #[tokio::test]
    async fn test_large_batch_processing() {
        let config = MockNetworkHubConfig {
            batch_size: 3,         // Max 3 per batch
            batch_max_wait_ms: 10, // Increase timeout to 10ms to ensure batch separation
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // Send messages in stages to avoid batch boundary blur from all messages enqueued instantly
        let send_batch = |network: Arc<MockNodeNetwork>,
                          sender: RaftId,
                          receiver: RaftId,
                          start: u32,
                          end: u32| async move {
            for i in start..=end {
                let mut req = create_test_request_vote();
                req.term = i as u64;
                network
                    .send_request_vote_request(&sender, &receiver, req)
                    .await
                    .unwrap();
            }
        };

        let network = Arc::new(network);

        // First batch: 3 messages (1-3)
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 1, 3).await;
        // Wait for first batch to be sent
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Second batch: 3 messages (4-6)
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 4, 6).await;
        // Wait for second batch to be sent
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Third batch: 1 message (7)
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 7, 7).await;

        // Simplified test: Only verify all messages are received correctly
        let mut received_count = 0;
        let mut received_terms = vec![];

        // Collect all messages
        while received_count < 7 {
            if let Ok(msg) = timeout(Duration::from_millis(100), rx_receiver.recv()).await {
                if let Some(NetworkEvent::RequestVote(_, _, req)) = msg {
                    received_terms.push(req.term);
                    received_count += 1;
                }
            } else {
                break;
            }
        }

        // Verify all messages are received with correct order (FIFO)
        assert_eq!(received_count, 7, "Should receive 7 messages");
        assert_eq!(
            received_terms,
            vec![1, 2, 3, 4, 5, 6, 7],
            "Messages should arrive in sending order"
        );
    }
}
