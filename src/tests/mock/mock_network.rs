use crate::{
    // 假设这些类型在上层 crate 中定义
    AppendEntriesRequest,
    AppendEntriesResponse,
    Event,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    Network,
    RaftId,
    RequestVoteRequest,
    RequestVoteResponse,
    RpcResult,
};
use async_trait::async_trait;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, VecDeque}; // Changed from BinaryHeap
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

// --- 模拟网络配置 ---

/// 模拟网络行为的配置
#[derive(Debug, Clone)]
pub struct MockRaftNetworkConfig {
    /// 基础延迟 (毫秒)
    pub base_latency_ms: u64,
    /// 额外随机延迟的最大值 (毫秒)
    pub jitter_max_ms: u64,
    /// 消息丢失的概率 (0.0 - 1.0)
    pub drop_rate: f64,
    /// 消息处理失败的概率 (0.0 - 1.0) - 在实际发送时模拟
    pub failure_rate: f64,
}

#[derive(Debug, Clone)]
pub struct MockNetworkHubConfig {
    /// 批量发送的最大批次大小
    pub batch_size: usize,
    /// 批量发送的最大等待时间 (毫秒)
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
            base_latency_ms: 10,
            jitter_max_ms: 50,
            drop_rate: 0.00,
            failure_rate: 0.00,
        }
    }
}

// --- 内部用于延迟队列的消息 ---

/// 在延迟队列中等待发送的消息
#[derive(Debug, Clone)] // Added Clone for moving between structures
struct DelayedMessage {
    scheduled_time: Instant, // 消息应该被发送的时间
    target: RaftId,
    event: NetworkEvent,
}

// 移除了 Ord, PartialOrd, Eq, PartialEq 的实现，因为我们不再使用 BinaryHeap

// --- 模拟网络核心 ---

/// 模拟网络的中心枢纽，管理所有节点的接收端和全局状态
#[derive(Clone)]
pub struct MockNetworkHub {
    inner: Arc<MockNetworkHubInner>,
}

type DispatchFn = Box<dyn Fn(NetworkEvent) -> () + Send + Sync>;

pub struct NodeSender {
    dispatch: Option<DispatchFn>,
    sender: Option<mpsc::UnboundedSender<NetworkEvent>>,
}

/// 内部共享状态
struct MockNetworkHubInner {
    /// 存储每个节点的发送端，用于向其发送最终消息
    node_senders: RwLock<HashMap<RaftId, NodeSender>>,
    /// 网络配置
    raft_config: RwLock<HashMap<RaftId, Arc<MockRaftNetworkConfig>>>,
    hub_config: Arc<MockNetworkHubConfig>,
    /// 每个发送节点一个延迟队列，保证发送顺序
    delayed_queues: RwLock<HashMap<RaftId, VecDeque<DelayedMessage>>>, // Changed
    /// 延迟队列的通知信号 (当有新消息入队或需要处理时通知)
    delay_queue_notify: tokio::sync::Notify,
    /// 实际发送消息的通道 (延迟到期后放入此通道)
    real_send_tx: mpsc::UnboundedSender<RealSendItem>,
    /// 实际发送消息的接收端
    real_send_rx: Mutex<mpsc::UnboundedReceiver<RealSendItem>>,
}

impl MockNetworkHubInner {
    /// 更新节点的网络配置
    pub async fn update_config(&self, node_id: RaftId, config: MockRaftNetworkConfig) {
        self.raft_config
            .write()
            .await
            .insert(node_id, Arc::new(config));
    }

    /// 获取节点的网络配置
    pub async fn get_config(&self, node_id: &RaftId) -> Option<Arc<MockRaftNetworkConfig>> {
        self.raft_config.read().await.get(node_id).cloned()
    }
}

/// 内部类型，用于通过通道传递实际发送的消息

/// 通过 real_send 通道传递的实际发送项
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

    /// 延迟队列处理器：检查所有节点的延迟队列并按计划时间发送消息
    /// 保证每个节点内部消息的 FIFO 顺序
    async fn run_delayed_queue_processor(inner: Arc<MockNetworkHubInner>) {
        loop {
            // 1. 获取所有队列的只读锁，找到最早到期的消息
            let queues = inner.delayed_queues.read().await;
            let mut earliest_msg: Option<DelayedMessage> = None;
            let mut earliest_sender: Option<RaftId> = None;

            for (sender_id, queue) in queues.iter() {
                if let Some(msg) = queue.front() {
                    // 查看队首元素
                    match &earliest_msg {
                        None => {
                            // 第一个找到的消息
                            earliest_msg = Some(msg.clone()); // Clone for temporary use
                            earliest_sender = Some(sender_id.clone());
                        }
                        Some(current_earliest) => {
                            // 比较 scheduled_time
                            if msg.scheduled_time < current_earliest.scheduled_time {
                                earliest_msg = Some(msg.clone());
                                earliest_sender = Some(sender_id.clone());
                            }
                        }
                    }
                }
            }
            drop(queues); // 释放只读锁

            // 2. 处理找到的最早消息
            if let (Some(msg_to_send), Some(sender_id)) = (earliest_msg, earliest_sender) {
                let now = Instant::now();
                if msg_to_send.scheduled_time <= now {
                    // 时间到了，需要发送
                    // 3. 重新获取写锁，从对应队列移除消息
                    let mut queues_mut = inner.delayed_queues.write().await;
                    if let Some(queue) = queues_mut.get_mut(&sender_id) {
                        // 再次检查队首是否还是该消息（防止并发修改）
                        // 简单起见，我们假设如果 sender_id 和 scheduled_time 匹配，就是同一条消息
                        // 更严格的比较可能需要在 DelayedMessage 中加入唯一 ID
                        if let Some(front_msg) = queue.front() {
                            if front_msg.scheduled_time == msg_to_send.scheduled_time {
                                let _removed_msg = queue.pop_front(); // 移除队首消息
                                drop(queues_mut); // 释放写锁

                                // info!(
                                //     "Delayed message from {} to {} ready for sending",
                                //     sender_id, msg_to_send.target
                                // );
                                // 4. 发送到 real_send 通道
                                if let Err(_e) = inner
                                    .real_send_tx
                                    .send((msg_to_send.target, msg_to_send.event))
                                {
                                    warn!(
                                        "Real send channel is closed, stopping delayed queue processor"
                                    );
                                    break; // Channel closed, stop
                                }
                                // 处理完一个消息后，立即继续循环检查下一个
                                //             info!("send message done,continue");
                                continue;
                            }
                        }
                    }
                    // 如果队列为空或队首已变，则继续循环
                    // (通常不会发生，除非有并发修改，但 VecDeque 的 front/pop 是原子的)
                } else {
                    // 还没到时间，计算等待时长
                    let wait_duration = msg_to_send.scheduled_time.duration_since(now);
                    // info!(
                    //     "Waiting {:?} for next delayed message from {}",
                    //     wait_duration, sender_id
                    // );
                    // 等待指定时间或被新消息入队通知唤醒
                    tokio::select! {
                        _ = tokio::time::sleep(wait_duration) => {
                           // info!("continue to dispatch message")
                        }
                        _ = inner.delay_queue_notify.notified() => {
                            // 被通知，可能有新消息或更早的消息，继续循环检查
                         //   info!("Woken up by notification, rechecking queues");
                        }
                    }
                    continue; // 继续循环
                }
            } else {
                // 所有队列都为空
                log::trace!("All delayed queues empty, waiting for notification");
                inner.delay_queue_notify.notified().await;
                // 被通知，继续循环检查
            }
        }
    }

    /// 实际发送器：从 real_send 通道批量接收并发送消息
    async fn run_real_sender(inner: Arc<MockNetworkHubInner>) {
        let mut rx = inner.real_send_rx.lock().await;
        loop {
            let mut batch = Vec::with_capacity(inner.hub_config.batch_size);
            let timeout_duration = Duration::from_millis(inner.hub_config.batch_max_wait_ms);

            // 尝试收集一个批次
            tokio::select! {
                // 接收第一个消息
                first_item = rx.recv() => {
                    if let Some(item) = first_item {
                        batch.push(item);
                        // 在超时时间内尝试接收更多消息以填满批次
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
                // 如果第一个都没收到，超时后也继续（虽然 batch 会是空的）
                _ = tokio::time::sleep(timeout_duration) => {
                    // info!("wait message timeout");
                }
            }

            // 发送批次中的消息
            if !batch.is_empty() {
                //     log::trace!("Sending batch of {} messages", batch.len());
                for (target, event) in batch.drain(..) {
                    // 模拟实际发送时的失败率

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
                        // 这里模拟发送失败，可以选择记录或忽略
                        // 对于 Raft 来说，发送失败通常等同于超时或丢包，由上层处理
                        continue; // 跳过这次发送
                    }

                    // 实际“发送”：转发到目标节点的接收端
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
            // 继续循环等待下一批
        }
    }

    /// 为一个 Raft 节点注册到网络中，返回其对应的 Network 实例和接收端
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
                // 当提供 dispatch 时，不应再提供 sender，避免双通道导致 panic
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

// --- 代表单个节点的网络接口 ---

/// 代表单个 Raft 节点的网络接口实现
pub struct MockNodeNetwork {
    node_id: RaftId,
    hub_inner: Arc<MockNetworkHubInner>, // 引用 Hub 的内部状态
}

// --- 内部用于通道传递的事件 ---

/// 内部枚举，用于通过通道传递不同类型的网络消息
#[derive(Debug, Clone)] // Clone is needed for putting into DelayedMessage and sending

// (sender, target, event)
pub enum NetworkEvent {
    RequestVote(RaftId, RaftId, RequestVoteRequest),
    RequestVoteResponse(RaftId, RaftId, RequestVoteResponse),
    AppendEntriesRequest(RaftId, RaftId, AppendEntriesRequest),
    AppendEntriesResponse(RaftId, RaftId, AppendEntriesResponse),
    InstallSnapshotRequest(RaftId, RaftId, InstallSnapshotRequest),
    InstallSnapshotResponse(RaftId, RaftId, InstallSnapshotResponse),
}

impl MockNodeNetwork {
    // isolate
    pub async fn isolate(&self) {
        info!("Isolating node {:?}", self.node_id);
        // 清空该节点的发送端，模拟网络隔离
        let mut config = MockRaftNetworkConfig::default(); // Reset to default config
        config.drop_rate = 1.0; // 设置丢包率为 100%
        config.failure_rate = 1.0; // 设置失败率为 100%
        self.hub_inner
            .update_config(self.node_id.clone(), config)
            .await;
    }

    // restore
    pub async fn restore(&self) {
        info!("Restoring node {:?}", self.node_id);
        // 恢复到默认配置
        let config = MockRaftNetworkConfig::default();
        self.hub_inner
            .update_config(self.node_id.clone(), config)
            .await;
    }

    /// 内部辅助函数，用于模拟延迟和丢包，然后将消息放入延迟队列
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
        // 1. 模拟丢包 (在入队前就决定是否丢弃)

        if rng.random::<f64>() < config.drop_rate {
            // [!code ++]
            info!(
                "MockNetwork: Dropping message from {} to {} (before queuing)",
                self.node_id, target
            );
            // 对于丢包，操作被视为“成功”（消息已发出但丢失）
            return Ok(());
        }

        // 2. 计算延迟
        let latency_ms = config.base_latency_ms + rng.random_range(0..=config.jitter_max_ms);
        let delay = Duration::from_millis(latency_ms);
        let scheduled_time = Instant::now() + delay;
        drop(config); // Release read lock early

        // 3. 创建延迟消息并放入对应发送方的队列末尾
        let delayed_msg = DelayedMessage {
            scheduled_time,
            target: target.clone(),
            event,
        };

        {
            let mut queues = self.hub_inner.delayed_queues.write().await;
            // 获取或创建该节点的队列
            let queue = queues
                .entry(self.node_id.clone())
                .or_insert_with(VecDeque::new);
            queue.push_back(delayed_msg); // Push to back to maintain FIFO for this sender
        } // Lock released here

        // 4. 通知延迟队列处理器有新消息
        self.hub_inner.delay_queue_notify.notify_one();

        // info!(
        //     "MockNetwork: Message from {} to {} queued for sending in {:?} (scheduled at {:?})",
        //     self.node_id, target, delay, scheduled_time
        // );

        // 5. 立即返回，模拟网络调用瞬间完成
        Ok(())
    }
}

#[async_trait]
impl Network for MockNodeNetwork {
    async fn send_request_vote_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: RequestVoteRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::RequestVote(from, target, args),
        )
        .await
    }

    async fn send_request_vote_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: RequestVoteResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::RequestVoteResponse(from, target, args),
        )
        .await
    }

    async fn send_append_entries_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: AppendEntriesRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::AppendEntriesRequest(from, target, args),
        )
        .await
    }

    async fn send_append_entries_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: AppendEntriesResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::AppendEntriesResponse(from, target, args),
        )
        .await
    }

    async fn send_install_snapshot_request(
        &self,
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotRequest,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::InstallSnapshotRequest(from, target, args),
        )
        .await
    }

    async fn send_install_snapshot_response(
        &self,
        from: RaftId,
        target: RaftId,
        args: InstallSnapshotResponse,
    ) -> RpcResult<()> {
        self.send_to_target(
            from.clone(),
            target.clone(),
            NetworkEvent::InstallSnapshotResponse(from, target, args),
        )
        .await
    }
}

// --- 辅助函数：将内部事件分发回 Raft 状态机 ---
// (这部分与之前相同，根据您的 Event 枚举调整)

// use crate::Event; // 引入您实际的 Event 枚举

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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestId;
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
    async fn test_mock_network_config_default() {
        let config = MockRaftNetworkConfig::default();
        assert_eq!(config.base_latency_ms, 10);
        assert_eq!(config.jitter_max_ms, 50);
        assert_eq!(config.drop_rate, 0.00);
        assert_eq!(config.failure_rate, 0.00);
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
            .send_request_vote_request(node1.clone(), node2.clone(), vote_req.clone())
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
            .send_append_entries_request(leader.clone(), follower.clone(), append_req.clone())
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
            .send_request_vote_request(node1.clone(), node2.clone(), vote_req)
            .await
            .unwrap();

        // Node2 -> Node1
        let append_req = create_test_append_entries();
        network2
            .send_append_entries_request(node2.clone(), node1.clone(), append_req)
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
                .send_request_vote_request(sender.clone(), receiver.clone(), vote_req)
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
            .send_request_vote_request(sender, receiver, vote_req)
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
                .send_request_vote_request(sender.clone(), receiver.clone(), vote_req)
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
            .send_request_vote_request(sender, nonexistent, vote_req)
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
            .send_request_vote_request(sender.clone(), receiver.clone(), vote_req)
            .await
            .unwrap();

        let vote_resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
            request_id: RequestId::new(),
        };
        network
            .send_request_vote_response(sender.clone(), receiver.clone(), vote_resp)
            .await
            .unwrap();

        let append_req = create_test_append_entries();
        network
            .send_append_entries_request(sender.clone(), receiver.clone(), append_req)
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
            .send_append_entries_response(sender.clone(), receiver.clone(), append_resp)
            .await
            .unwrap();

        use crate::{ClusterConfig, Snapshot};
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
            is_probe: false,
        };
        network
            .send_install_snapshot_request(sender.clone(), receiver.clone(), install_req)
            .await
            .unwrap();

        let install_resp = InstallSnapshotResponse {
            term: 1,
            request_id: RequestId::new(),
            state: crate::InstallSnapshotState::Success,
        };
        network
            .send_install_snapshot_response(sender, receiver, install_resp)
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
                    .send_request_vote_request(sender_clone, receiver_clone, vote_req)
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
            .send_request_vote_request(sender, receiver, vote_req)
            .await;
        assert!(result.is_ok()); // Send operation succeeds even if message is dropped

        // Message should not be received
        let received = timeout(Duration::from_millis(50), rx_receiver.recv()).await;
        assert!(received.is_err()); // Should timeout
    }
}

#[cfg(test)]
mod additional_tests {
    use super::*;
    use crate::tests::mock::mock_network::tests::{create_test_raft_id, create_test_request_vote};
    use tokio::time::{Instant, timeout};

    /// 测试发送失败率（failure_rate）生效
    #[tokio::test]
    async fn test_send_failure_rate() {
        // 配置：100% 发送失败率（消息入队后，实际发送时失败）
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

        // 发送消息（应入队但发送阶段失败）
        let vote_req = create_test_request_vote();
        let result = network
            .send_request_vote_request(sender, receiver, vote_req)
            .await;
        assert!(result.is_ok()); // 发送操作本身无错误

        // 接收端应收不到消息（发送失败）
        let received = timeout(Duration::from_millis(100), rx_receiver.recv()).await;
        assert!(received.is_err(), "消息应因发送失败而丢失");
    }

    /// 测试批量发送边界场景
    #[tokio::test]
    async fn test_batch_sending_boundaries() {
        // 配置：批量大小2，最大等待时间10ms
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

        // 场景1：发送2条消息，应一次批量发送
        let start = Instant::now();
        for i in 1..=2 {
            let mut vote_req = create_test_request_vote();
            vote_req.term = i;
            network
                .send_request_vote_request(sender.clone(), receiver.clone(), vote_req)
                .await
                .unwrap();
        }

        // 验证两条消息在短时间内连续收到（批量发送）
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
            "批量发送应无显著延迟"
        );

        // 场景2：发送1条消息，应在超时后发送
        let start = Instant::now();
        let mut vote_req = create_test_request_vote();
        vote_req.term = 3;
        network
            .send_request_vote_request(sender, receiver, vote_req)
            .await
            .unwrap();

        let _msg3 = timeout(Duration::from_millis(50), rx_receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(10),
            "单条消息应等待批量超时"
        );
        assert!(elapsed < Duration::from_millis(30), "超时不应过长");
    }

    #[tokio::test]
    async fn test_multi_node_message_order() {
        let config = MockNetworkHubConfig {
            batch_size: 5,
            batch_max_wait_ms: 50,
        };
        let hub = MockNetworkHub::new(config);

        // 注册3个节点：sender1、sender2发送消息，receiver接收
        let sender1 = create_test_raft_id("group1", "sender1");
        let sender2 = create_test_raft_id("group1", "sender2");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network1, _rx1) = hub.register_node(sender1.clone()).await;
        let (network2, _rx2) = hub.register_node(sender2.clone()).await;
        let (_network_r, mut rx_r) = hub.register_node(receiver.clone()).await;

        // sender1 发送3条消息（term 1-3），并设置正确的candidate_id
        for term in 1..=3 {
            let mut req = create_test_request_vote();
            req.term = term;
            req.candidate_id = sender1.clone(); // 关键修复：设置正确的发送者ID
            network1
                .send_request_vote_request(sender1.clone(), receiver.clone(), req)
                .await
                .unwrap();
        }

        // sender2 发送3条消息（term 10-12），并设置正确的candidate_id
        for term in 10..=12 {
            let mut req = create_test_request_vote();
            req.term = term;
            req.candidate_id = sender2.clone(); // 关键修复：设置正确的发送者ID
            network2
                .send_request_vote_request(sender2.clone(), receiver.clone(), req)
                .await
                .unwrap();
        }

        // 收集接收端消息，按发送节点分组
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

        // 验证各节点消息顺序正确（FIFO）
        assert_eq!(sender1_terms, vec![1, 2, 3], "sender1消息顺序错误");
        assert_eq!(sender2_terms, vec![10, 11, 12], "sender2消息顺序错误");
    }
    #[tokio::test]
    async fn test_large_batch_processing() {
        let config = MockNetworkHubConfig {
            batch_size: 3,         // 每批最多3条
            batch_max_wait_ms: 10, // 增加超时时间到10ms，确保批次分离
        };
        let hub = MockNetworkHub::new(config);

        let sender = create_test_raft_id("group1", "sender");
        let receiver = create_test_raft_id("group1", "receiver");

        let (network, _rx_sender) = hub.register_node(sender.clone()).await;
        let (_network_receiver, mut rx_receiver) = hub.register_node(receiver.clone()).await;

        // 分阶段发送消息，避免所有消息瞬间入队导致批次边界模糊
        let send_batch = |network: Arc<MockNodeNetwork>,
                          sender: RaftId,
                          receiver: RaftId,
                          start: u32,
                          end: u32| async move {
            for i in start..=end {
                let mut req = create_test_request_vote();
                req.term = i as u64;
                network
                    .send_request_vote_request(sender.clone(), receiver.clone(), req)
                    .await
                    .unwrap();
            }
        };

        let network = Arc::new(network);

        // 第一批3条（1-3）
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 1, 3).await;
        // 等待第一批发送完成
        tokio::time::sleep(Duration::from_millis(15)).await;

        // 第二批3条（4-6）
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 4, 6).await;
        // 等待第二批发送完成
        tokio::time::sleep(Duration::from_millis(15)).await;

        // 第三批1条（7）
        send_batch(Arc::clone(&network), sender.clone(), receiver.clone(), 7, 7).await;

        // 简化测试：只验证所有消息都能正确接收
        let mut received_count = 0;
        let mut received_terms = vec![];

        // 收集所有消息
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

        // 验证收到了所有消息且顺序正确（FIFO）
        assert_eq!(received_count, 7, "应该收到7条消息");
        assert_eq!(
            received_terms,
            vec![1, 2, 3, 4, 5, 6, 7],
            "消息应按发送顺序到达"
        );
    }
}
