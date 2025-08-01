use std::collections::{HashMap, HashSet};
use std::sync::atomic::{self, AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, mpsc};

// 定义Raft组ID类型
type RaftGroupId = u64;
// 定义节点ID类型
type NodeId = u64;

// Raft组状态枚举（原子操作安全）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftGroupStatus {
    Idle,    // 空闲状态（无未处理消息，未被Worker处理）
    Pending, // 待处理状态（有未处理消息，已加入active_map）
    Active,  // 活跃状态（正在被Worker处理）
}

// Raft事件类型（示例）
#[derive(Debug)]
pub enum RaftEvent {
    NetworkMessage { from: NodeId, data: Vec<u8> },
    TimerExpired { timer_id: u64 },
    LocalCommand { command: Vec<u8> },
}

impl Into<Event> for RaftEvent {
    fn into(self) -> Event {
        todo!("Convert RaftEvent to Event")
    }
}

use std::sync::atomic::AtomicU8;

use crate::{Event, RaftState};

// 原子RaftGroupStatus包装器
pub struct AtomicRaftGroupStatus(AtomicU8);

impl AtomicRaftGroupStatus {
    pub fn new(status: RaftGroupStatus) -> Self {
        Self(AtomicU8::new(status as u8))
    }

    pub fn load(&self, order: Ordering) -> RaftGroupStatus {
        match self.0.load(order) {
            0 => RaftGroupStatus::Idle,
            1 => RaftGroupStatus::Pending,
            2 => RaftGroupStatus::Active,
            _ => panic!("Invalid RaftGroupStatus value"),
        }
    }

    pub fn store(&self, status: RaftGroupStatus, order: Ordering) {
        self.0.store(status as u8, order);
    }

    pub fn compare_exchange(
        &self,
        current: RaftGroupStatus,
        new: RaftGroupStatus,
        success: Ordering,
        failure: Ordering,
    ) -> Result<RaftGroupStatus, RaftGroupStatus> {
        match self
            .0
            .compare_exchange(current as u8, new as u8, success, failure)
        {
            Ok(val) => Ok(match val {
                0 => RaftGroupStatus::Idle,
                1 => RaftGroupStatus::Pending,
                2 => RaftGroupStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
            Err(val) => Err(match val {
                0 => RaftGroupStatus::Idle,
                1 => RaftGroupStatus::Pending,
                2 => RaftGroupStatus::Active,
                _ => panic!("Invalid RaftGroupStatus value"),
            }),
        }
    }
}

// Raft组核心数据（状态+通道+业务状态）
struct RaftGroupCore {
    status: AtomicRaftGroupStatus, // 原子状态管理
    tx: mpsc::Sender<RaftEvent>,   // 事件发送端
    rx: mpsc::Receiver<RaftEvent>, // 事件接收端
    state: Mutex<RaftState>,       // Raft业务状态（如日志、任期等）
}

// Multi-Raft驱动核心
pub struct MultiRaftDriver {
    groups: Arc<Mutex<HashMap<RaftGroupId, RaftGroupCore>>>, // 所有Raft组
    active_map: Arc<Mutex<HashSet<RaftGroupId>>>,            // 待处理组集合
    notify: Arc<Notify>,                                     // Worker唤醒信号
    stop: AtomicBool,                                        // 停止标志
    network_tx: mpsc::Sender<(NodeId, Vec<u8>)>,             // 网络发送通道
}

impl MultiRaftDriver {
    // 创建新的MultiRaftDriver
    pub fn new(network_tx: mpsc::Sender<(NodeId, Vec<u8>)>) -> Self {
        Self {
            network_tx,
            groups: Arc::new(Mutex::new(HashMap::new())),
            active_map: Arc::new(Mutex::new(HashSet::new())),
            notify: Arc::new(Notify::new()),
            stop: AtomicBool::new(false),
        }
    }

    // 添加新的Raft组
    pub fn add_raft_group(&self, group_id: RaftGroupId, state: RaftState) {
        let (tx, rx) = mpsc::channel(100); // 事件通道（缓冲区大小100）
        let core = RaftGroupCore {
            status: AtomicRaftGroupStatus::new(RaftGroupStatus::Idle),
            tx,
            rx,
            state: Mutex::new(state),
        };
        self.groups.lock().unwrap().insert(group_id, core);
    }

    // 向指定Raft组发送事件
    pub async fn send_event(&self, group_id: RaftGroupId, event: RaftEvent) -> bool {
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
            RaftGroupStatus::Idle => {
                // 从Idle -> Pending，加入active_map并唤醒Worker
                let swapped = core
                    .status
                    .compare_exchange(
                        RaftGroupStatus::Idle,
                        RaftGroupStatus::Pending,
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
            RaftGroupStatus::Pending | RaftGroupStatus::Active => {
                // 已在处理流程中，无需额外操作
            }
        }
        true
    }

    // 启动Worker（可启动多个）
    pub async fn start_worker(self: Arc<Self>) {
        let notify = self.notify.clone();

        loop {
            // 等待唤醒信号（有新任务或定时器事件）
            let _ = notify.notified().await;

            // 检查停止标志
            if self.stop.load(Ordering::Relaxed) {
                break; // 停止Worker
            }
            // 处理所有待处理组
            self.process_active_groups().await;
        }
    }

    // 处理active_map中的所有待处理组
    async fn process_active_groups(&self) {
        // 一次性取出所有待处理组（减少锁竞争）
        let pending_groups = {
            let mut active_map = self.active_map.lock().unwrap();
            std::mem::take(&mut *active_map)
        };

        // 逐个处理组
        for group_id in pending_groups {
            self.process_single_group(group_id).await;
        }
    }

    // 处理单个Raft组的所有事件
    async fn process_single_group(&self, group_id: RaftGroupId) {
        // 获取组核心数据
        let mut groups = self.groups.lock().unwrap();
        let Some(core) = groups.get_mut(&group_id) else {
            return; // 组已被删除
        };

        // 1. 尝试获取处理权（Pending -> Active）
        let swapped = core
            .status
            .compare_exchange(
                RaftGroupStatus::Pending,
                RaftGroupStatus::Active,
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
            match core.rx.try_recv() {
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
                            RaftGroupStatus::Active,
                            RaftGroupStatus::Idle,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok();

                    if swapped {
                        // 转为Idle后检查是否有新消息
                        let has_remaining = !core.rx.is_empty();
                        if has_remaining {
                            // 尝试抢回处理权
                            let swapped = core
                                .status
                                .compare_exchange(
                                    RaftGroupStatus::Idle,
                                    RaftGroupStatus::Active,
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
                    core.status.store(RaftGroupStatus::Idle, Ordering::Release);
                    return;
                }
            }
        }
    }
}
