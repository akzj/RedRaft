# 网络架构与快照传输分析

## 1. 网络架构概览

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    MultiRaftNetwork                              │
│  - gRPC Server: 接收来自其他节点的消息                           │
│  - gRPC Client: 发送消息到其他节点                              │
│  - 批量消息处理: 提高网络效率                                    │
│  - 消息路由: 根据 RaftId (group + node) 路由消息                │
└─────────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
        ▼                                       ▼
┌───────────────┐                      ┌───────────────┐
│  gRPC Server │                      │  gRPC Client  │
│  (接收消息)   │                      │  (发送消息)    │
└───────────────┘                      └───────────────┘
        │                                       │
        │ RaftService::send_batch               │ BatchRequest
        │                                       │
        ▼                                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MessageDispatcher                            │
│  - 分发消息到对应的 Raft 组                                      │
│  - 处理 InstallSnapshot, AppendEntries, RequestVote 等          │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 gRPC 服务定义

**服务接口**:
```protobuf
service RaftService {
    // 发送批量 RPC 消息（支持所有类型的请求/响应）
    rpc SendBatch(BatchRequest) returns (BatchResponse);
}
```

**消息类型**:
- `RequestVoteRequest/Response` - 选举投票
- `AppendEntriesRequest/Response` - 日志复制
- `InstallSnapshotRequest/Response` - 快照安装
- `PreVoteRequest/Response` - 预投票

**批量消息容器**:
```protobuf
message BatchRequest {
    string node_id = 1;          // 发送方节点 ID
    repeated RpcMessage messages = 2; // 批量消息列表
}
```

### 1.3 消息发送机制

**批量发送**:
- 每个目标节点有一个独立的发送任务（`run_message_sender`）
- 消息通过 `mpsc::channel` 发送到发送任务
- 发送任务批量收集消息（`batch_size`），然后一次性发送
- 提高网络效率，减少 RPC 调用次数

**消息路由**:
```rust
// 根据 target RaftId 路由消息
outgoing_tx[target.node].send(message).await
```

## 2. 快照传输机制

### 2.1 快照发送流程

#### 2.1.1 触发条件

快照发送在以下情况触发：
1. **Follower 落后太多**: `next_index <= last_snapshot_index`
2. **日志已被截断**: Follower 需要的日志已经被快照覆盖

**触发位置**: `crates/raft/src/state/replication.rs`
```rust
// Check if snapshot needs to be sent
let next_idx = *self.next_index.get(&peer).unwrap_or(&1);
if next_idx <= self.last_snapshot_index {
    self.send_snapshot_to(peer.clone()).await;
    continue;
}
```

#### 2.1.2 发送流程

```
Leader RaftState
    │
    │ 1. 检测到 Follower 需要快照
    │
    ▼
send_snapshot_to(target)
    │
    │ 2. 加载最新快照
    │    callbacks.load_snapshot()
    │
    ▼
验证快照一致性
    │
    │ 3. 构造 InstallSnapshotRequest
    │    - term, leader_id
    │    - last_included_index, last_included_term
    │    - data (完整快照数据)
    │    - config
    │    - snapshot_request_id
    │
    ▼
通过 Network 发送
    │
    │ 4. callbacks.send_install_snapshot_request()
    │
    ▼
MultiRaftNetwork
    │
    │ 5. 转换为 OutgoingMessage::InstallSnapshot
    │
    ▼
批量发送任务
    │
    │ 6. 添加到 BatchRequest
    │
    ▼
gRPC Client
    │
    │ 7. 调用 RaftService::SendBatch
    │
    ▼
目标节点 gRPC Server
```

#### 2.1.3 关键代码

**发送快照** (`crates/raft/src/state/snapshot.rs`):
```rust
pub(crate) async fn send_snapshot_to(&mut self, target: RaftId) {
    // 1. 加载快照
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

    // 2. 验证快照一致性
    if !self.verify_snapshot_consistency(&snap).await {
        error!("Snapshot inconsistent with current logs, cannot send");
        return;
    }

    // 3. 构造请求
    let snapshot_request_id = RequestId::new();
    let req = InstallSnapshotRequest {
        term: self.current_term,
        leader_id: self.id.clone(),
        last_included_index: snap.index,
        last_included_term: snap.term,
        data: snap.data.clone(),  // 完整快照数据（可能很大）
        config: snap.config.clone(),
        request_id: snapshot_request_id,
        snapshot_request_id,
        is_probe: false,
    };

    // 4. 记录状态
    self.follower_last_snapshot_index.insert(target.clone(), snap.index);
    self.follower_snapshot_states.insert(target.clone(), InstallSnapshotState::Installing);

    // 5. 调度探测任务（定期检查安装状态）
    self.schedule_snapshot_probe(
        target.clone(),
        snapshot_request_id,
        self.schedule_snapshot_probe_interval,
        self.schedule_snapshot_probe_retries,
    );

    // 6. 发送请求
    if let Err(err) = self
        .callbacks
        .send_install_snapshot_request(&self.id, &target, req)
        .await
    {
        error!("Failed to send InstallSnapshotRequest: {}", err);
    }
}
```

**网络层发送** (`crates/raft/src/network/mod.rs`):
```rust
async fn send_install_snapshot_request(
    &self,
    from: &RaftId,
    target: &RaftId,
    args: InstallSnapshotRequest,
) -> RpcResult<()> {
    self.send_message(target, OutgoingMessage::InstallSnapshot {
        from: from.clone(),
        target: target.clone(),
        args,
    })
    .await
}

// send_message 将消息放入对应节点的发送通道
async fn send_message(&self, target: &RaftId, msg: OutgoingMessage) -> RpcResult<()> {
    let tx = self.outgoing_tx
        .read()
        .await
        .get(&target.node)
        .ok_or_else(|| RpcError::NodeNotFound(target.node.clone()))?;
    
    tx.send(msg).await
        .map_err(|_| RpcError::ChannelClosed)?;
    
    Ok(())
}
```

### 2.2 快照接收流程

#### 2.2.1 接收入口

**gRPC Server 接收**:
```
目标节点 gRPC Server
    │
    │ 1. 接收 BatchRequest
    │
    ▼
RaftService::send_batch()
    │
    │ 2. 解析 RpcMessage
    │
    ▼
MessageDispatcher::dispatch()
    │
    │ 3. 分发到对应的 Raft 组
    │
    ▼
RaftState::handle_install_snapshot()
```

#### 2.2.2 处理流程

```
Follower RaftState
    │
    │ 1. 接收 InstallSnapshotRequest
    │
    ▼
handle_install_snapshot()
    │
    │ 2. 验证 term 和 leader_id
    │
    ▼
处理探测请求 (is_probe=true)
    │    - 返回当前安装状态
    │
    ▼
处理实际快照 (is_probe=false)
    │
    │ 3. 立即响应 Installing 状态
    │    send_install_snapshot_response(Installing)
    │
    ▼
异步处理快照
    │
    │ 4. 调用 callbacks.process_snapshot()
    │    - 业务层处理快照数据
    │    - 通过 oneshot channel 返回结果
    │
    ▼
tokio::spawn 异步任务
    │
    │ 5. 等待处理结果
    │    oneshot::rx.await
    │
    ▼
发送完成事件
    │
    │ 6. 发送 Event::CompleteSnapshotInstallation
    │    - 自通知机制（Actor 模式）
    │
    ▼
RaftState::tick() 处理事件
    │
    │ 7. handle_complete_snapshot_installation()
    │    - 更新 last_snapshot_index, last_snapshot_term
    │    - 更新 commit_index, last_applied
    │    - 截断日志（删除快照之前的日志）
    │    - 持久化 HardState
    │    - 清理过期的客户端请求
    │    - 应用集群配置
    │
    ▼
更新安装状态
    │
    │ 8. install_snapshot_success = Some((success, request_id, error))
    │
    ▼
Leader 探测时返回最终状态
```

#### 2.2.3 关键代码

**接收快照** (`crates/raft/src/state/snapshot.rs`):
```rust
pub(crate) async fn handle_install_snapshot(
    &mut self,
    sender: RaftId,
    request: InstallSnapshotRequest,
) {
    // 1. 验证 sender 和 term
    if sender != request.leader_id {
        warn!("Node {} received InstallSnapshot from {}, but leader is {}", ...);
        return;
    }

    if request.term < self.current_term {
        // 返回失败响应
        return;
    }

    // 2. 更新 leader 信息
    self.leader_id = Some(request.leader_id.clone());
    self.role = Role::Follower;
    self.current_term = request.term;

    // 3. 处理探测请求
    if request.is_probe {
        // 返回当前安装状态
        return;
    }

    // 4. 立即响应 Installing 状态
    let resp = InstallSnapshotResponse {
        term: self.current_term,
        request_id: request.request_id,
        state: InstallSnapshotState::Installing,
        error_message: "".into(),
    };
    self.callbacks.send_install_snapshot_response(&self.id, &request.leader_id, resp).await;

    // 5. 记录当前快照请求 ID
    self.current_snapshot_request_id = Some(request.snapshot_request_id);

    // 6. 异步处理快照
    let (tx, rx) = oneshot::channel();
    let snapshot_data = request.data.clone();
    let snapshot_index = request.last_included_index;
    let snapshot_term = request.last_included_term;
    let snapshot_config = request.config.clone();
    let snapshot_request_id = request.snapshot_request_id;
    let raft_id = self.id.clone();
    let leader_id = request.leader_id.clone();

    // 调用业务层处理快照
    self.callbacks.process_snapshot(
        &raft_id,
        snapshot_index,
        snapshot_term,
        snapshot_config,
        snapshot_request_id,
        snapshot_data,
        tx,
    ).await;

    // 7. 等待处理结果并发送完成事件
    tokio::spawn(async move {
        match rx.await {
            Ok(Ok(())) => {
                // 发送完成事件
                let event = Event::CompleteSnapshotInstallation(CompleteSnapshotInstallation {
                    index: snapshot_index,
                    term: snapshot_term,
                    success: true,
                    request_id: snapshot_request_id,
                    reason: None,
                    config: Some(snapshot_config),
                });
                // 通过 EventSender 发送事件
            }
            Ok(Err(e)) => {
                // 发送失败事件
            }
            Err(_) => {
                // Channel 关闭
            }
        }
    });
}
```

**完成快照安装** (`crates/raft/src/state/snapshot.rs`):
```rust
pub(crate) async fn handle_complete_snapshot_installation(
    &mut self,
    event: CompleteSnapshotInstallation,
) {
    // 1. 更新快照信息
    self.last_snapshot_index = event.index;
    self.last_snapshot_term = event.term;

    // 2. 更新提交和应用索引
    self.commit_index = event.index;
    self.last_applied = event.index;

    // 3. 截断日志（删除快照之前的日志）
    if event.index > 0 {
        self.callbacks.truncate_log_prefix(&self.id, event.index + 1).await;
    }

    // 4. 持久化 HardState
    let hard_state = HardState {
        term: self.current_term,
        voted_for: self.voted_for.clone(),
    };
    self.callbacks.save_hard_state(&self.id, hard_state).await;

    // 5. 清理过期的客户端请求
    // ...

    // 6. 应用集群配置
    if let Some(config) = event.config {
        self.apply_cluster_config(config).await;
    }

    // 7. 记录安装结果
    self.install_snapshot_success = Some((
        event.success,
        event.request_id,
        event.reason,
    ));
}
```

### 2.3 快照传输特点

#### 2.3.1 当前实现的特点

1. **一次性传输**: 快照数据在单个 `InstallSnapshotRequest` 中传输
   - `data: Vec<u8>` 包含完整快照数据
   - 对于大快照（如 10GB），可能导致：
     - 内存占用大
     - 网络传输时间长
     - gRPC 消息大小限制问题

2. **批量消息机制**: 快照请求通过批量消息发送
   - 与其他消息（AppendEntries 等）一起批量发送
   - 提高网络效率，但快照数据可能很大

3. **异步处理**: 快照处理是异步的
   - Follower 立即响应 `Installing` 状态
   - 在后台异步处理快照数据
   - 通过 `oneshot` channel 通知处理结果
   - 通过 `Event::CompleteSnapshotInstallation` 自通知完成

4. **探测机制**: Leader 定期探测快照安装状态
   - 使用 `is_probe=true` 的探测请求
   - 不包含快照数据，只检查状态
   - 支持超时和重试

#### 2.3.2 当前实现的限制

1. **不支持分块传输**: 
   - 快照数据必须在单个请求中传输
   - 对于大快照，可能超过 gRPC 消息大小限制（默认 4MB）

2. **不支持断点续传**:
   - 如果传输失败，必须重新传输整个快照
   - 没有进度追踪机制

3. **内存占用大**:
   - Leader 需要将整个快照数据加载到内存
   - Follower 需要接收整个快照数据到内存

## 3. 分裂快照传输的设计考虑

### 3.1 复用现有机制 vs 独立协议

**方案 A: 复用 Raft InstallSnapshot**

优点:
- 复用现有网络层和消息处理逻辑
- 自动处理 Leader → Follower 的复制
- 代码改动小

缺点:
- 快照数据在单个请求中，不支持分块
- 需要修改以支持"外部快照"（分裂快照）
- 可能不符合 Raft 标准语义

**方案 B: 独立的分裂快照传输协议**

优点:
- 语义清晰，专门用于分裂场景
- 支持分块传输、断点续传
- 不影响 Raft 标准流程

缺点:
- 需要实现新的传输机制
- 需要手动触发其他节点复制

**推荐方案 B**: 使用独立的 gRPC 服务

### 3.2 分裂快照传输设计

#### 3.2.1 新的 gRPC 服务

```protobuf
service SplitSnapshotService {
    // 传输分裂快照（分块）
    rpc TransferSplitSnapshot(stream TransferSplitSnapshotRequest) 
        returns (TransferSplitSnapshotResponse);
    
    // 查询传输进度
    rpc GetTransferProgress(GetTransferProgressRequest) 
        returns (GetTransferProgressResponse);
}

message TransferSplitSnapshotRequest {
    string source_shard_id = 1;
    string target_shard_id = 2;
    uint32 split_slot = 3;
    uint64 snapshot_index = 4;
    uint64 snapshot_term = 5;
    bytes snapshot_data = 6;  // 分块数据
    bool is_last_chunk = 7;
    uint32 chunk_index = 8;
    uint32 total_chunks = 9;
    bytes checksum = 10;      // 块校验和
}
```

#### 3.2.2 分块传输流程

```
源分片 Leader
    │
    │ 1. 获取 Raft 快照
    │ 2. 筛选槽位范围
    │ 3. 分块（1MB/chunk）
    │
    ▼
for each chunk:
    │
    │ 4. 构造 TransferSplitSnapshotRequest
    │ 5. 通过 gRPC stream 发送
    │
    ▼
目标分片 Leader
    │
    │ 6. 接收并验证 chunk
    │ 7. 存储到临时位置
    │ 8. 返回确认
    │
    ▼
所有 chunks 接收完成
    │
    │ 9. 组装完整快照
    │ 10. 验证完整性
    │ 11. 调用 merge_from_snapshot
    │ 12. 通过 Raft InstallSnapshot 复制到 Follower
```

#### 3.2.3 与现有机制的区别

| 特性 | Raft InstallSnapshot | 分裂快照传输 |
|------|---------------------|-------------|
| 用途 | Leader → Follower 同步 | 源分片 → 目标分片分裂 |
| 数据范围 | 整个分片数据 | 部分槽位范围数据 |
| 传输方式 | 单次请求 | 分块流式传输 |
| 数据来源 | Raft 快照 | 筛选后的分裂快照 |
| 目标节点 | 同一 Raft 组的 Follower | 不同分片的 Leader |
| 复制机制 | Raft 自动复制 | 手动触发 Raft InstallSnapshot |

## 4. 网络架构总结

### 4.1 当前架构

- **协议**: gRPC (基于 HTTP/2)
- **服务**: `RaftService::SendBatch` (批量消息)
- **消息类型**: RequestVote, AppendEntries, InstallSnapshot, PreVote
- **传输方式**: 批量发送，提高效率
- **快照传输**: 单次请求，完整数据

### 4.2 分裂快照传输需求

- **独立服务**: `SplitSnapshotService`
- **分块传输**: 支持大快照（1MB/chunk）
- **断点续传**: 支持传输失败后恢复
- **进度追踪**: 支持查询传输进度
- **校验机制**: 每个块包含校验和

### 4.3 实现建议

1. **新增 gRPC 服务**: 在 Node 端添加 `SplitSnapshotService`
2. **复用网络层**: 使用现有的 gRPC client/server 基础设施
3. **分块传输**: 使用 gRPC streaming 支持分块传输
4. **状态管理**: 在 Node 端管理分裂快照接收状态
5. **复制机制**: 目标 Leader 接收完成后，通过 Raft InstallSnapshot 复制到 Follower
