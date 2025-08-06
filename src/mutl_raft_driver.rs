use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

// Raft组状态枚举（原子操作安全）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftNodeStatus {
    Idle,    // 空闲状态（无未处理消息，未被Worker处理）
    Pending, // 待处理状态（有未处理消息，已加入active_map）
    Active,  // 活跃状态（正在被Worker处理）
}

// Raft事件类型（示例）
use std::sync::atomic::AtomicU8;

use crate::{
    Event, RaftId, RaftState,
};

// 原子RaftGroupStatus包装器
pub struct AtomicRaftNodeStatus(AtomicU8);

impl AtomicRaftNodeStatus {
    pub fn new(status: RaftNodeStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn load(&self, order: Ordering) -> RaftNodeStatus {
        match self.0.load(order) {
            0 => RaftNodeStatus::Idle,
            1 => RaftNodeStatus::Pending,
            2 => RaftNodeStatus::Active,
            _ => panic!("Invalid RaftGroupStatus value"),
        }
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
            Ok(val) => Ok(match val {
                0 => RaftNodeStatus::Idle,
                1 => RaftNodeStatus::Pending,
                2 => RaftNodeStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
            Err(val) => Err(match val {
                0 => RaftNodeStatus::Idle,
                1 => RaftNodeStatus::Pending,
                2 => RaftNodeStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
        }
    }
}

#[derive(Debug)]
struct TimerEvent {
    node_id: RaftId,
    event: Event,
    trigger_time: Instant,
}

// Implement ordering for TimerEvent so it can be used in BinaryHeap (min-heap by trigger_time)
impl PartialEq for TimerEvent {
    fn eq(&self, other: &Self) -> bool {
        self.trigger_time.eq(&other.trigger_time)
    }
}

impl Eq for TimerEvent {}

impl PartialOrd for TimerEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse order for min-heap
        other.trigger_time.partial_cmp(&self.trigger_time)
    }
}

impl Ord for TimerEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
        other.trigger_time.cmp(&self.trigger_time)
    }
}

// Raft组核心数据（状态+通道+业务状态）
struct RaftGroupCore {
    status: AtomicRaftNodeStatus,     // 原子状态管理
    tx: mpsc::Sender<Event>,          // 事件发送端
    rx: Mutex<mpsc::Receiver<Event>>, // 事件接收端
    state: Mutex<RaftState>,          // Raft业务状态（如日志、任期等）
}

// Multi-Raft驱动核心
pub struct MultiRaftDriver {
    groups: Arc<Mutex<HashMap<RaftId, Arc<RaftGroupCore>>>>, // 所有Raft组
    active_map: Arc<Mutex<HashSet<RaftId>>>,                 // 待处理组集合
    notify: Arc<Notify>,                                     // Worker唤醒信号
    stop: AtomicBool,                                        // 停止标志
    network_tx: mpsc::Sender<(RaftId, Vec<u8>)>,             // 网络发送通道
    // 定时器堆（全局管理所有组的定时器）
    timer_heap: Arc<Mutex<BinaryHeap<TimerEvent>>>,
}

impl MultiRaftDriver {
    // 创建新的MultiRaftDriver
    pub fn new(network_tx: mpsc::Sender<(RaftId, Vec<u8>)>) -> Self {
        Self {
            network_tx,
            timer_heap: Arc::new(Mutex::new(BinaryHeap::new())),
            groups: Arc::new(Mutex::new(HashMap::new())),
            active_map: Arc::new(Mutex::new(HashSet::new())),
            notify: Arc::new(Notify::new()),
            stop: AtomicBool::new(false),
        }
    }

    // 添加新的Raft组
    pub fn add_raft_group(&self, group_id: RaftId, state: RaftState) {
        let (tx, rx) = mpsc::channel(100); // 事件通道（缓冲区大小100）
        let core = RaftGroupCore {
            status: AtomicRaftNodeStatus::new(RaftNodeStatus::Idle),
            tx,
            rx: Mutex::new(rx), // 使用Mutex包装接收端
            state: Mutex::new(state),
        };
        self.groups.lock().unwrap().insert(group_id, Arc::new(core));
    }

    fn process_expired_timers(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut timer_heap = self.timer_heap.lock().unwrap();

        while let Some(timer_event) = timer_heap.peek() {
            if timer_event.trigger_time <= now {
                let timer_event = timer_heap.pop().unwrap();
                let node_id = timer_event.node_id;

                // 发送定时器事件到组通道
                let groups = self.groups.lock().unwrap();
                if let Some(group) = groups.get(&node_id) {
                    let _ = group.tx.try_send(timer_event.event);

                    // 激活组并唤醒 Driver
                    let mut active_groups = self.active_map.lock().unwrap();
                    active_groups.insert(node_id);
                    let _ = self.notify.notify_one();
                }
            } else {
                // 未来时间 - 当前时间 = 正的剩余时间
                return Some(timer_event.trigger_time - now);
            }
        }
        None
    }

    // 向指定Raft组发送事件
    pub async fn send_event(&self, group_id: RaftId, event: Event) -> bool {
        let groups = self.groups.lock().unwrap();
        let Some(core) = groups.get(&group_id) else {
            return false; // 组不存在
        };

        // 1. 先发送事件到通道（确保消息不丢失）
        if core.tx.send(event).await.is_err() {
            return false; // 通道已关闭
        }

        // 2. 根据当前状态决定是否需要激活组
        let current_status = core.status.load(Ordering::Acquire);
        match current_status {
            RaftNodeStatus::Idle => {
                // 从Idle -> Pending，加入active_map并唤醒Worker
                let swapped = core
                    .status
                    .compare_exchange(
                        RaftNodeStatus::Idle,
                        RaftNodeStatus::Pending,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();

                if swapped {
                    let mut active_map = self.active_map.lock().unwrap();
                    active_map.insert(group_id);
                    let _ = self.notify.notify_one(); // 唤醒Worker
                }
            }
            RaftNodeStatus::Pending | RaftNodeStatus::Active => {
                // 已在处理流程中，无需额外操作
            }
        }
        true
    }

    // 启动Worker（可启动多个）
    pub async fn main_loop(&self) {
        loop {
            // 处理过期定时器，获取下一个定时器的剩余时间
            let next_timer_duration = self.process_expired_timers();

            // 等待逻辑：有定时器则等待到其到期，否则无限等待唤醒
            if let Some(duration) = next_timer_duration {
                tokio::select! {
                    _ = tokio::time::sleep(duration) => {
                        continue; // 定时器到期，继续处理
                    },
                    _ = self.notify.notified() => {},
                }
            } else {
                // 无定时器，等待唤醒（新事件或停止信号）
                self.notify.notified().await;
            }

            // 检查停止标志
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            // 处理活跃节点
            self.process_active_nodes().await;
        }
    }
    // 处理active_map中的所有待处理组
    async fn process_active_nodes(&self) {
        // 一次性取出所有待处理组（减少锁竞争）
        let pendings = {
            let mut active_map = self.active_map.lock().unwrap();
            std::mem::take(&mut *active_map)
        };

        // 逐个处理组
        for node_id in pendings {
            self.process_single_group(node_id).await;
        }
    }

    // 处理单个Raft组的所有事件
    async fn process_single_group(&self, node_id: RaftId) {
        // 获取组核心数据
        let core = {
            let groups = self.groups.lock().unwrap(); // 非mut锁
            groups.get(&node_id).cloned() // 假设RaftGroupCore实现了Clone，或仅获取引用
        };
        let Some(core) = core else {
            return;
        }; // 组已删除
        // 1. 尝试获取处理权（Pending -> Active）
        let swapped = core
            .status
            .compare_exchange(
                RaftNodeStatus::Pending,
                RaftNodeStatus::Active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();

        if !swapped {
            return; // 已被其他Worker处理
        }

        // 2. 克隆接收端用于处理（避免持有groups锁）

        let state = &core.state;

        // 3. 循环处理所有事件
        loop {
            match core.rx.lock().unwrap().try_recv() {
                Ok(event) => {
                    // 处理事件并获取响应
                    let mut raft_state = state.lock().unwrap();
                    raft_state.handle_event(event.into()).await;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // 暂时无消息，尝试转为Idle并检查是否有新消息
                    let swapped = core
                        .status
                        .compare_exchange(
                            RaftNodeStatus::Active,
                            RaftNodeStatus::Idle,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok();

                    if swapped {
                        // 转为Idle后检查是否有新消息
                        let has_remaining = !core.rx.lock().unwrap().is_empty();
                        if has_remaining {
                            // 尝试抢回处理权
                            let swapped = core
                                .status
                                .compare_exchange(
                                    RaftNodeStatus::Idle,
                                    RaftNodeStatus::Active,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok();

                            if swapped {
                                continue; // 抢回成功，继续处理
                            } else {
                                // 抢回失败，由其他Worker处理
                                break;
                            }
                        } else {
                            // 无新消息，保持Idle
                            break;
                        }
                    } else {
                        // 状态已被修改（如其他事件触发了Pending）
                        break;
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    // 通道关闭，清理状态
                    core.status.store(RaftNodeStatus::Idle, Ordering::Release);
                    return;
                }
            }
        }
    }
}
