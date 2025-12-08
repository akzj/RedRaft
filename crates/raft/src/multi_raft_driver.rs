//! Multi-Raft Driver
//! 
//! Responsible for managing event scheduling and timer services for multiple Raft groups.
//! Uses lock-free design and efficient state machine transitions to handle concurrent events.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, info, trace, warn};

use crate::{Event, RaftId, TimerId};

/// Event channel capacity (provides backpressure protection)
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Raft node status enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftNodeStatus {
    /// Idle state (no pending messages, not being processed by Worker)
    Idle = 0,
    /// Pending state (has pending messages, added to active_map)
    Pending = 1,
    /// Active state (being processed by Worker)
    Active = 2,
    /// Stop state
    Stop = 3,
}

impl RaftNodeStatus {
    /// Convert from u8, return None if value is invalid
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(RaftNodeStatus::Idle),
            1 => Some(RaftNodeStatus::Pending),
            2 => Some(RaftNodeStatus::Active),
            3 => Some(RaftNodeStatus::Stop),
            _ => None,
        }
    }
}

use std::sync::atomic::AtomicU8;

/// Atomic RaftNodeStatus wrapper
pub struct AtomicRaftNodeStatus(AtomicU8);

impl AtomicRaftNodeStatus {
    pub fn new(status: RaftNodeStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn load(&self, order: Ordering) -> RaftNodeStatus {
        RaftNodeStatus::from_u8(self.0.load(order)).unwrap_or(RaftNodeStatus::Stop)
    }

    pub fn store(&self, status: RaftNodeStatus, order: Ordering) {
        self.0.store(status as u8, order);
    }

    pub fn compare_exchange(
        &self,
        current: RaftNodeStatus,
        new: RaftNodeStatus,
        success: Ordering,
        failure: Ordering,
    ) -> Result<RaftNodeStatus, RaftNodeStatus> {
        match self
            .0
            .compare_exchange(current as u8, new as u8, success, failure)
        {
            Ok(val) => Ok(RaftNodeStatus::from_u8(val).unwrap_or(RaftNodeStatus::Stop)),
            Err(val) => Err(RaftNodeStatus::from_u8(val).unwrap_or(RaftNodeStatus::Stop)),
        }
    }
}

/// Timer event
#[derive(Debug)]
struct TimerEvent {
    timer_id: TimerId,
    node_id: RaftId,
    event: Event,
    trigger_time: Instant,
    delay: Duration,
}

impl PartialEq for TimerEvent {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_time.eq(&other.trigger_time)
    }
}

impl Eq for TimerEvent {}

impl PartialOrd for TimerEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order to implement min-heap
        other.trigger_time.cmp(&self.trigger_time)
    }
}

/// Event handling trait
#[async_trait::async_trait]
pub trait HandleEventTrait: Send + Sync {
    async fn handle_event(&self, event: Event);
}

/// Raft group core data
struct RaftGroupCore {
    status: AtomicRaftNodeStatus,
    sender: mpsc::Sender<Event>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Event>>,
    handle_event: Box<dyn HandleEventTrait>,
}

/// Timer inner state
pub struct TimerInner {
    timer_id_counter: AtomicU64,
    timer_heap: Mutex<BinaryHeap<TimerEvent>>,
    /// Cancelled timer IDs (lazy deletion)
    cancelled_timers: Mutex<HashSet<TimerId>>,
}

/// Timer service
#[derive(Clone)]
pub struct Timers {
    inner: Arc<TimerInner>,
    notify: Arc<Notify>,
}

impl Timers {
    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            inner: Arc::new(TimerInner {
                timer_id_counter: AtomicU64::new(0),
                timer_heap: Mutex::new(BinaryHeap::new()),
                cancelled_timers: Mutex::new(HashSet::new()),
            }),
        }
    }

    /// Add timer, return timer ID
    pub fn add_timer(&self, node_id: &RaftId, event: Event, delay: Duration) -> TimerId {
        let timer_id = self.inner.timer_id_counter.fetch_add(1, Ordering::Relaxed);
        let trigger_time = Instant::now() + delay;
        let timer_event = TimerEvent {
            timer_id,
            event,
            delay,
            trigger_time,
            node_id: node_id.clone(),
        };

        self.inner.timer_heap.lock().push(timer_event);
        self.notify.notify_one();

        trace!(
            "Added timer {} for node {} with delay {:?}",
            timer_id, node_id, delay
        );
        timer_id
    }

    /// Delete timer (O(1) lazy deletion)
    pub fn del_timer(&self, timer_id: TimerId) {
        self.inner.cancelled_timers.lock().insert(timer_id);
        trace!("Cancelled timer {}", timer_id);
    }

    /// Delete all timers for a node
    pub fn del_all_timers_for_node(&self, node_id: &RaftId) {
        let mut timer_heap = self.inner.timer_heap.lock();
        let before_len = timer_heap.len();

        // Rebuild heap, excluding timers for this node
        let remaining: Vec<_> = timer_heap
            .drain()
            .filter(|t| &t.node_id != node_id)
            .collect();

        for timer in remaining {
            timer_heap.push(timer);
        }

        let removed = before_len - timer_heap.len();
        if removed > 0 {
            debug!("Removed {} timers for node {}", removed, node_id);
        }
    }

    /// Process expired timers, return triggered events and wait time for next timer
    fn process_expired_timers(&self) -> (Vec<TimerEvent>, Option<Duration>) {
        let now = Instant::now();
        let mut events = Vec::new();
        let mut timer_heap = self.inner.timer_heap.lock();
        let cancelled = self.inner.cancelled_timers.lock();

        while let Some(timer_event) = timer_heap.peek() {
            // Skip cancelled timers
            if cancelled.contains(&timer_event.timer_id) {
                timer_heap.pop();
                continue;
            }

            if timer_event.trigger_time <= now {
                let event = timer_heap.pop().unwrap();
                events.push(event);
            } else {
                return (events, Some(timer_event.trigger_time - now));
            }
        }

        (events, None)
    }

    /// Clean up cancelled timers set (called periodically to free memory)
    pub fn cleanup_cancelled(&self) {
        let mut cancelled = self.inner.cancelled_timers.lock();
        if cancelled.len() > 1000 {
            // Only clean up when accumulation is large
            let timer_heap = self.inner.timer_heap.lock();
            let active_ids: HashSet<_> = timer_heap.iter().map(|t| t.timer_id).collect();
            cancelled.retain(|id| active_ids.contains(id));
        }
    }
}

impl Deref for Timers {
    type Target = TimerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Send event result
#[derive(Debug)]
pub enum SendEventResult {
    Success,
    NotFound,
    SendFailed,
    ChannelFull,
}

/// Multi-Raft Driver inner state
pub struct MultiRaftDriverInner {
    timer_service: Timers,
    groups: Mutex<HashMap<RaftId, Arc<RaftGroupCore>>>,
    active_map: Mutex<HashSet<RaftId>>,
    notify: Arc<Notify>,
    stop: AtomicBool,
}

/// Multi-Raft Driver
#[derive(Clone)]
pub struct MultiRaftDriver {
    inner: Arc<MultiRaftDriverInner>,
}

impl Deref for MultiRaftDriver {
    type Target = MultiRaftDriverInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl MultiRaftDriver {
    /// Create a new MultiRaftDriver
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        Self {
            inner: Arc::new(MultiRaftDriverInner {
                timer_service: Timers::new(notify.clone()),
                groups: Mutex::new(HashMap::new()),
                active_map: Mutex::new(HashSet::new()),
                notify,
                stop: AtomicBool::new(false),
            }),
        }
    }

    /// Get timer service
    pub fn get_timer_service(&self) -> Timers {
        self.inner.timer_service.clone()
    }

    /// Add a new Raft group
    pub fn add_raft_group(&self, group_id: RaftId, handle_event: Box<dyn HandleEventTrait>) {
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let core = RaftGroupCore {
            status: AtomicRaftNodeStatus::new(RaftNodeStatus::Idle),
            sender: tx,
            receiver: tokio::sync::Mutex::new(rx),
            handle_event,
        };
        self.groups.lock().insert(group_id.clone(), Arc::new(core));
        info!("Added Raft group: {}", group_id);
    }

    /// Delete a Raft group
    pub fn del_raft_group(&self, group_id: &RaftId) {
        if self.groups.lock().remove(group_id).is_some() {
            info!("Removed Raft group: {}", group_id);
            self.timer_service.del_all_timers_for_node(group_id);
        }
    }

    /// Stop the driver
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.notify.notify_waiters();
        info!("MultiRaftDriver stop signal sent");
    }

    /// Process expired timers
    async fn process_expired_timers(&self) -> Option<Duration> {
        let (events, duration) = self.timer_service.process_expired_timers();

        for event in events {
            let result = self.dispatch_event(event.node_id.clone(), event.event.clone());
            match result {
                SendEventResult::Success => {}
                SendEventResult::NotFound => {
                    debug!(
                        "Timer event for removed node ignored: node_id: {}",
                        event.node_id
                    );
                }
                SendEventResult::SendFailed | SendEventResult::ChannelFull => {
                    warn!(
                        "Failed to send timer event: node_id: {}, delay: {:?}",
                        event.node_id, event.delay
                    );
                }
            }
        }

        // Periodically clean up cancelled timers
        self.timer_service.cleanup_cancelled();

        duration
    }

    /// Send event to specified Raft group
    pub fn dispatch_event(&self, target: RaftId, event: Event) -> SendEventResult {
        let core = {
            let groups = self.groups.lock();
            match groups.get(&target) {
                Some(core) => core.clone(),
                None => return SendEventResult::NotFound,
            }
        };

        // 1. Send event to channel
        match core.sender.try_send(event) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("Event channel full for node {}", target);
                return SendEventResult::ChannelFull;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return SendEventResult::SendFailed;
            }
        }

        // 2. Determine if group needs activation based on current state
        let current_status = core.status.load(Ordering::Acquire);
        match current_status {
            RaftNodeStatus::Idle => {
                // From Idle -> Pending, add to active_map and wake up Worker
                if core
                    .status
                    .compare_exchange(
                        RaftNodeStatus::Idle,
                        RaftNodeStatus::Pending,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.active_map.lock().insert(target);
                    self.notify.notify_one();
                }
            }
            RaftNodeStatus::Pending | RaftNodeStatus::Active => {
                // Already being processed
            }
            RaftNodeStatus::Stop => {
                warn!("Target node {} is in Stop status", target);
                return SendEventResult::NotFound;
            }
        }

        SendEventResult::Success
    }

    /// Main loop
    pub async fn main_loop(&self) {
        info!("Starting main loop for MultiRaftDriver");

        loop {
            // Process expired timers
            let wait_duration = self.process_expired_timers().await;

            // Wait: if there are timers, wait until expiration, otherwise wait for wakeup
            if let Some(duration) = wait_duration {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {
                        continue;
                    }
                    _ = self.notify.notified() => {}
                }
            } else {
                self.notify.notified().await;
            }

            // Check stop flag
            if self.stop.load(Ordering::Acquire) {
                info!("Stop signal received, exiting main loop");
                break;
            }

            // Process active nodes
            self.process_active_nodes().await;
        }
    }

    /// Process all pending groups in active_map
    async fn process_active_nodes(&self) {
        let pendings: HashSet<RaftId> = {
            let mut active_map = self.active_map.lock();
            std::mem::take(&mut *active_map)
        };

        for node_id in pendings {
            let driver = self.clone();
            tokio::spawn(async move {
                driver.process_single_group(node_id).await;
            });
        }
    }

    /// Process all events for a single Raft group
    async fn process_single_group(&self, node_id: RaftId) {
        let core = {
            let groups = self.groups.lock();
            match groups.get(&node_id) {
                Some(core) => core.clone(),
                None => {
                    debug!("Group {} not found, skipping", node_id);
                    return;
                }
            }
        };

        // 1. Attempt to acquire processing rights (Pending -> Active)
        if core
            .status
            .compare_exchange(
                RaftNodeStatus::Pending,
                RaftNodeStatus::Active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            trace!(
                "Group {} not pending, current: {:?}",
                node_id,
                core.status.load(Ordering::Acquire)
            );
            return;
        }

        // 2. Process event loop
        let mut rx = core.receiver.lock().await;

        loop {
            match rx.try_recv() {
                Ok(event) => {
                    // Check if already stopped
                    if core.status.load(Ordering::Acquire) == RaftNodeStatus::Stop {
                        warn!("Node {} stopped, ignoring event", node_id);
                        return;
                    }
                    core.handle_event.handle_event(event).await;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    // Attempt to transition to Idle
                    if core
                        .status
                        .compare_exchange(
                            RaftNodeStatus::Active,
                            RaftNodeStatus::Idle,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        // After successful transition, check for new messages (without removing)
                        // Use is_empty() instead of try_recv() to ensure messages stay in queue
                        if !rx.is_empty() {
                            // New messages available, attempt to return to Active state
                            if core
                                .status
                                .compare_exchange(
                                    RaftNodeStatus::Idle,
                                    RaftNodeStatus::Active,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                // Successfully returned to Active, continue processing
                                continue;
                            } else {
                                // CAS failed, indicating dispatch_event has changed status to Pending
                                // and added to active_map, new worker will take over
                                // Messages remain in queue, no loss
                                trace!(
                                    "Node {} yielding to new worker (status changed during idle check)",
                                    node_id
                                );
                                break;
                            }
                        } else {
                            // Queue is actually empty, exit
                            break;
                        }
                    } else {
                        // State change failed, exit
                        break;
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    core.status.store(RaftNodeStatus::Idle, Ordering::Release);
                    debug!("Channel disconnected for node {}", node_id);
                    return;
                }
            }
        }
    }
}

impl Default for MultiRaftDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::time::sleep;

    #[derive(Clone)]
    struct MockHandleEvent {
        pub events_received: Arc<std::sync::Mutex<Vec<Event>>>,
    }

    impl MockHandleEvent {
        fn new() -> Self {
            Self {
                events_received: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        pub fn get_events(&self) -> Vec<Event> {
            self.events_received.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HandleEventTrait for MockHandleEvent {
        async fn handle_event(&self, event: Event) {
            self.events_received.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn test_add_raft_group() {
        let manager = MultiRaftDriver::new();
        let clone = manager.clone();

        tokio::spawn(async move {
            clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout),
            SendEventResult::Success
        ));

        sleep(Duration::from_millis(30)).await;

        assert_eq!(mock_handler.get_events().len(), 1);
    }

    #[tokio::test]
    async fn test_send_event_success() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let result = manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout);
        assert!(matches!(result, SendEventResult::Success));

        sleep(Duration::from_millis(50)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 1);
    }

    #[tokio::test]
    async fn test_send_multiple_events() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::HeartbeatTimeout),
            SendEventResult::Success
        ));
        assert!(matches!(
            manager.dispatch_event(group_id.clone(), Event::ElectionTimeout),
            SendEventResult::Success
        ));

        sleep(Duration::from_millis(50)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 2);
    }

    #[tokio::test]
    async fn test_timer_service() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let timers = manager.get_timer_service();
        timers.add_timer(
            &group_id,
            Event::HeartbeatTimeout,
            Duration::from_millis(100),
        );

        sleep(Duration::from_millis(150)).await;

        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 1);
    }

    #[tokio::test]
    async fn test_timer_cancellation() {
        let manager = MultiRaftDriver::new();
        let driver_clone = manager.clone();

        tokio::spawn(async move {
            driver_clone.main_loop().await;
        });

        let group_id = RaftId::new("test_group".to_string(), "node1".to_string());
        let mock_handler = MockHandleEvent::new();

        manager.add_raft_group(group_id.clone(), Box::new(mock_handler.clone()));

        let timers = manager.get_timer_service();
        let timer_id = timers.add_timer(
            &group_id,
            Event::HeartbeatTimeout,
            Duration::from_millis(100),
        );

        // Cancel immediately
        timers.del_timer(timer_id);

        sleep(Duration::from_millis(150)).await;

        // Timer has been canceled, should not receive events
        let received_events = mock_handler.get_events();
        assert_eq!(received_events.len(), 0);
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_group() {
        let manager = MultiRaftDriver::new();
        let group_id = RaftId::new("nonexistent".to_string(), "node1".to_string());

        let result = manager.dispatch_event(group_id, Event::HeartbeatTimeout);
        assert!(matches!(result, SendEventResult::NotFound));
    }
}
