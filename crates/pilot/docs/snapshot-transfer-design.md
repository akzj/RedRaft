# 快照传输详细设计

## 概述

快照传输是 Shard Split 的核心环节，需要解决以下关键问题：

1. **复用 Raft 快照** - 保证与后续增量日志的连贯性
2. **筛选槽位范围** - 只传输目标分片负责的数据
3. **防止快照被删除** - 传输期间保护快照和日志不被清理
4. **发送目标** - 发送给 leader，其他节点通过 Raft 复制
5. **加载机制** - 目标分片如何加载外部快照

## 核心设计原则

### 1. 复用 Raft 快照

**为什么必须复用 Raft 快照？**

- Raft 快照包含 `index` 和 `term`，与日志条目有明确的对应关系
- 快照的 `index` 是后续增量日志的起始点
- 如果使用独立快照，无法保证与 Raft 日志的连贯性

**设计**：
```rust
// 使用 Raft 层已有的快照
let raft_snapshot = raft_state.get_latest_snapshot().await?;
// raft_snapshot.index 是后续日志的起始点
// raft_snapshot.term 是快照时的 term
```

### 2. 筛选槽位范围

**问题**：Raft 快照包含整个分片的数据，但分裂只需要部分槽位的数据。

**解决方案**：解析快照数据，筛选出目标槽位范围的数据，重新生成快照。

## 详细设计

### 阶段 1: 获取并保护 Raft 快照

#### 1.1 获取最新快照

```rust
// 在源分片的 Leader 节点执行
async fn get_split_snapshot(
    raft_id: &RaftId,
    split_slot: u32,
) -> Result<SplitSnapshot, String> {
    // 1. 获取 Raft 层最新快照
    let raft_snapshot = raft_state
        .get_latest_snapshot()
        .await
        .ok_or("No snapshot available")?;
    
    // 2. 记录快照信息
    let snapshot_index = raft_snapshot.index;
    let snapshot_term = raft_snapshot.term;
    
    // 3. 标记快照为"分裂使用中"，防止被删除
    raft_state.mark_snapshot_in_use(snapshot_index).await?;
    
    // 4. 防止日志被截断（保护快照 index 之前的日志）
    raft_state.protect_log_range(0, snapshot_index).await?;
    
    Ok(SplitSnapshot {
        snapshot_index,
        snapshot_term,
        raw_data: raft_snapshot.data,
        config: raft_snapshot.config,
    })
}
```

#### 1.2 保护机制

**防止快照被删除**：
```rust
// 在 RaftState 中添加保护机制
struct SnapshotProtection {
    in_use_snapshots: HashSet<u64>,  // 正在使用的快照 index
    protected_log_ranges: Vec<(u64, u64)>,  // 受保护的日志范围
}

impl RaftState {
    /// 标记快照为使用中，防止被删除
    async fn mark_snapshot_in_use(&mut self, index: u64) {
        self.snapshot_protection.in_use_snapshots.insert(index);
    }
    
    /// 释放快照保护
    async fn release_snapshot(&mut self, index: u64) {
        self.snapshot_protection.in_use_snapshots.remove(&index);
    }
    
    /// 检查快照是否可以被删除
    fn can_delete_snapshot(&self, index: u64) -> bool {
        !self.snapshot_protection.in_use_snapshots.contains(&index)
    }
}
```

**防止日志被截断**：
```rust
// 在日志截断时检查保护范围
async fn truncate_log_suffix(&self, from_index: u64) -> Result<()> {
    // 检查是否在保护范围内
    for (start, end) in &self.snapshot_protection.protected_log_ranges {
        if from_index < *end {
            return Err(format!(
                "Cannot truncate log at {}: protected range [{}, {})",
                from_index, start, end
            ));
        }
    }
    
    // 执行截断...
}
```

### 阶段 2: 解析并筛选快照数据

#### 2.1 快照数据格式

假设快照数据是序列化的键值对列表：

```rust
// 快照数据格式（示例）
struct SnapshotData {
    entries: Vec<SnapshotEntry>,
}

struct SnapshotEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    expire_at: Option<u64>,
    // ... 其他元数据
}
```

#### 2.2 筛选目标槽位范围

```rust
/// 从完整快照中筛选出目标槽位范围的数据
fn filter_snapshot_by_slot_range(
    snapshot_data: &[u8],
    slot_start: u32,
    slot_end: u32,
    total_slots: u32,
) -> Result<Vec<u8>, String> {
    // 1. 反序列化快照数据
    let full_snapshot: SnapshotData = bincode::deserialize(snapshot_data)
        .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;
    
    // 2. 筛选目标槽位范围的数据
    let filtered_entries: Vec<SnapshotEntry> = full_snapshot
        .entries
        .into_iter()
        .filter(|entry| {
            let slot = calculate_slot(&entry.key, total_slots);
            slot >= slot_start && slot < slot_end
        })
        .collect();
    
    // 3. 重新序列化
    let filtered_snapshot = SnapshotData {
        entries: filtered_entries,
    };
    
    bincode::serialize(&filtered_snapshot)
        .map_err(|e| format!("Failed to serialize filtered snapshot: {}", e))
}

/// 计算 key 的槽位
fn calculate_slot(key: &[u8], total_slots: u32) -> u32 {
    crc16(key) as u32 % total_slots
}
```

#### 2.3 在 StateMachine 层实现

```rust
// redisstore/src/traits.rs
impl RedisStore for MemoryStore {
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        // 1. 获取完整快照数据
        let full_snapshot = self.create_snapshot()?;
        
        // 2. 筛选目标槽位范围
        filter_snapshot_by_slot_range(&full_snapshot, slot_start, slot_end, total_slots)
    }
}
```

**更好的方案**：直接在创建快照时筛选，避免序列化/反序列化开销：

```rust
impl RedisStore for MemoryStore {
    fn create_split_snapshot(
        &self,
        slot_start: u32,
        slot_end: u32,
        total_slots: u32,
    ) -> Result<Vec<u8>, String> {
        // 直接遍历数据，只序列化目标槽位范围的数据
        let mut entries = Vec::new();
        
        for (key, entry) in &self.data {
            let slot = calculate_slot(key, total_slots);
            if slot >= slot_start && slot < slot_end {
                entries.push(SnapshotEntry {
                    key: key.clone(),
                    value: entry.value.clone(),
                    expire_at: entry.expire_at,
                });
            }
        }
        
        let snapshot = SnapshotData { entries };
        bincode::serialize(&snapshot)
            .map_err(|e| format!("Failed to serialize: {}", e))
    }
}
```

### 阶段 3: 传输快照到目标分片

#### 3.1 发送给谁？

**设计决策：发送给目标分片的 Leader**

- **原因 1**: Leader 是唯一可以写入的节点
- **原因 2**: 其他节点通过 Raft 的 `InstallSnapshot` 机制自动复制
- **原因 3**: 简化实现，避免多节点协调

**流程**：
```
源分片 Leader
    │
    │ 1. 获取 Raft 快照
    │ 2. 筛选槽位范围
    │ 3. 发送到目标分片 Leader
    │
    ▼
目标分片 Leader
    │
    │ 1. 接收快照
    │ 2. 通过 Raft InstallSnapshot 安装
    │ 3. 其他节点自动复制
    │
    ▼
目标分片所有节点
```

#### 3.2 传输协议

**方案 A: 复用 Raft InstallSnapshot 协议**

优点：
- 复用现有机制
- 自动处理分片传输
- 自动复制到其他节点

缺点：
- 需要修改 InstallSnapshot 支持"外部快照"
- 可能不符合 Raft 标准语义

**方案 B: 独立的分裂快照传输协议**

优点：
- 语义清晰
- 不影响 Raft 标准流程

缺点：
- 需要实现新的传输机制
- 需要手动触发其他节点复制

**推荐方案 B**：使用独立的 gRPC 接口

```rust
// 定义分裂快照传输服务
service SplitSnapshotService {
    // 传输分裂快照
    rpc TransferSplitSnapshot(TransferSplitSnapshotRequest) 
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
    bytes snapshot_data = 6;  // 分块传输，这里是一块
    bool is_last_chunk = 7;
    uint32 chunk_index = 8;
}

message TransferSplitSnapshotResponse {
    bool success = 1;
    string error_message = 2;
    uint64 received_bytes = 3;
}
```

#### 3.3 分块传输

```rust
const SNAPSHOT_CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1MB

async fn transfer_split_snapshot(
    target_leader_addr: &str,
    snapshot_data: Vec<u8>,
    metadata: SplitSnapshotMetadata,
) -> Result<(), String> {
    let chunks: Vec<Vec<u8>> = snapshot_data
        .chunks(SNAPSHOT_CHUNK_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect();
    
    for (index, chunk) in chunks.iter().enumerate() {
        let is_last = index == chunks.len() - 1;
        
        let request = TransferSplitSnapshotRequest {
            source_shard_id: metadata.source_shard_id.clone(),
            target_shard_id: metadata.target_shard_id.clone(),
            split_slot: metadata.split_slot,
            snapshot_index: metadata.snapshot_index,
            snapshot_term: metadata.snapshot_term,
            snapshot_data: chunk.clone(),
            is_last_chunk: is_last,
            chunk_index: index as u32,
        };
        
        // 发送到目标 Leader
        send_chunk_to_target_leader(target_leader_addr, request).await?;
    }
    
    Ok(())
}
```

### 阶段 4: 目标分片加载快照

#### 4.1 接收并组装快照

```rust
// 在目标分片 Leader 节点
struct SplitSnapshotReceiver {
    chunks: HashMap<u32, Vec<u8>>,
    metadata: Option<SplitSnapshotMetadata>,
    total_chunks: Option<u32>,
}

impl SplitSnapshotReceiver {
    async fn receive_chunk(
        &mut self,
        request: TransferSplitSnapshotRequest,
    ) -> Result<(), String> {
        // 1. 保存第一个 chunk 的元数据
        if request.chunk_index == 0 {
            self.metadata = Some(SplitSnapshotMetadata {
                source_shard_id: request.source_shard_id,
                target_shard_id: request.target_shard_id,
                split_slot: request.split_slot,
                snapshot_index: request.snapshot_index,
                snapshot_term: request.snapshot_term,
            });
        }
        
        // 2. 保存 chunk
        self.chunks.insert(request.chunk_index, request.snapshot_data);
        
        // 3. 如果是最后一个 chunk，组装完整快照
        if request.is_last_chunk {
            self.total_chunks = Some(request.chunk_index + 1);
            self.assemble_and_install().await?;
        }
        
        Ok(())
    }
    
    async fn assemble_and_install(&mut self) -> Result<(), String> {
        // 1. 组装完整快照数据
        let mut snapshot_data = Vec::new();
        for i in 0..self.total_chunks.unwrap() {
            let chunk = self.chunks.remove(&i)
                .ok_or_else(|| format!("Missing chunk {}", i))?;
            snapshot_data.extend_from_slice(&chunk);
        }
        
        // 2. 获取元数据
        let metadata = self.metadata.take()
            .ok_or("Missing metadata")?;
        
        // 3. 通过 Raft InstallSnapshot 安装
        self.install_via_raft(snapshot_data, metadata).await?;
        
        Ok(())
    }
}
```

#### 4.2 通过 Raft 安装快照

**关键问题**：如何让 Raft 接受"外部快照"？

**方案 A: 直接调用 StateMachine 的 merge_from_snapshot**

```rust
async fn install_via_raft(
    &self,
    snapshot_data: Vec<u8>,
    metadata: SplitSnapshotMetadata,
) -> Result<(), String> {
    // 1. 直接调用 StateMachine 的 merge_from_snapshot
    //    不通过 Raft InstallSnapshot 流程
    let state_machine = get_state_machine(&metadata.target_shard_id).await?;
    state_machine.store().merge_from_snapshot(&snapshot_data)?;
    
    // 2. 手动设置 applied_index = snapshot_index
    state_machine.set_applied_index(metadata.snapshot_index).await?;
    
    // 3. 通过 Raft 发送 InstallSnapshot 给其他节点
    //    让其他节点也安装这个快照
    self.replicate_to_followers(metadata, snapshot_data).await?;
    
    Ok(())
}
```

**方案 B: 构造 Raft InstallSnapshot 请求**

```rust
async fn install_via_raft(
    &self,
    snapshot_data: Vec<u8>,
    metadata: SplitSnapshotMetadata,
) -> Result<(), String> {
    // 1. 构造 Raft Snapshot 对象
    let snapshot = raft::Snapshot {
        index: metadata.snapshot_index,
        term: metadata.snapshot_term,
        data: snapshot_data,
        config: ClusterConfig::default(), // 使用默认配置
    };
    
    // 2. 调用 Raft 的 InstallSnapshot 流程
    //    这会自动复制到其他节点
    let raft_id = get_raft_id(&metadata.target_shard_id).await?;
    raft_state.install_external_snapshot(raft_id, snapshot).await?;
    
    Ok(())
}
```

**推荐方案 A**：因为分裂快照是"合并"操作，不是"替换"操作。

#### 4.3 复制到其他节点

```rust
async fn replicate_to_followers(
    &self,
    metadata: SplitSnapshotMetadata,
    snapshot_data: Vec<u8>,
) -> Result<(), String> {
    // 1. 获取目标分片的所有节点
    let target_nodes = get_shard_nodes(&metadata.target_shard_id).await?;
    
    // 2. 对每个 Follower 节点发送 InstallSnapshot
    for node_id in target_nodes.iter().skip(1) { // 跳过 Leader
        let node_addr = get_node_addr(node_id).await?;
        
        // 构造 InstallSnapshotRequest
        let request = InstallSnapshotRequest {
            term: current_term,
            leader_id: self.raft_id.clone(),
            last_included_index: metadata.snapshot_index,
            last_included_term: metadata.snapshot_term,
            data: snapshot_data.clone(), // 分块传输
            config: ClusterConfig::default(),
            snapshot_request_id: RequestId::new(),
            request_id: RequestId::new(),
            is_probe: false,
        };
        
        // 发送到 Follower
        send_install_snapshot(node_addr, request).await?;
    }
    
    Ok(())
}
```

### 阶段 5: 释放保护

```rust
// 在快照传输完成后
async fn release_snapshot_protection(
    source_shard_id: &str,
    snapshot_index: u64,
) -> Result<(), String> {
    // 1. 释放快照保护
    raft_state.release_snapshot(snapshot_index).await?;
    
    // 2. 释放日志保护
    raft_state.release_log_protection(0, snapshot_index).await?;
    
    // 3. 允许 Raft 正常清理日志和快照
    Ok(())
}
```

## 完整流程

```
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 1: 获取并保护快照                                            │
├─────────────────────────────────────────────────────────────────┤
│ 源分片 Leader:                                                   │
│   1. 获取 Raft 最新快照 (index=N, term=T)                       │
│   2. mark_snapshot_in_use(N)  // 防止被删除                      │
│   3. protect_log_range(0, N)  // 防止日志被截断                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 2: 筛选槽位范围                                              │
├─────────────────────────────────────────────────────────────────┤
│ 源分片 Leader:                                                   │
│   1. 解析快照数据 (反序列化)                                      │
│   2. 筛选 slot ∈ [split_slot, end) 的数据                        │
│   3. 重新序列化为分裂快照                                         │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 3: 传输到目标分片                                            │
├─────────────────────────────────────────────────────────────────┤
│ 源分片 Leader ──分块传输──▶ 目标分片 Leader                      │
│   - 分块大小: 1MB                                                │
│   - 包含元数据: snapshot_index, snapshot_term, split_slot       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 4: 加载快照                                                  │
├─────────────────────────────────────────────────────────────────┤
│ 目标分片 Leader:                                                 │
│   1. 接收并组装所有 chunks                                       │
│   2. 调用 StateMachine::merge_from_snapshot()                   │
│   3. 设置 applied_index = snapshot_index                         │
│   4. 通过 Raft InstallSnapshot 复制到其他节点                    │
│                                                                  │
│ 目标分片 Follower:                                               │
│   1. 接收 Leader 的 InstallSnapshot 请求                        │
│   2. 调用 StateMachine::merge_from_snapshot()                    │
│   3. 设置 applied_index = snapshot_index                         │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 5: 释放保护                                                  │
├─────────────────────────────────────────────────────────────────┤
│ 源分片 Leader:                                                   │
│   1. release_snapshot(snapshot_index)                            │
│   2. release_log_protection(0, snapshot_index)                   │
│   3. 允许 Raft 正常清理                                           │
└─────────────────────────────────────────────────────────────────┘
```

## 关键数据结构

```rust
/// 分裂快照元数据
struct SplitSnapshotMetadata {
    source_shard_id: ShardId,
    target_shard_id: ShardId,
    split_slot: u32,
    snapshot_index: u64,  // Raft 快照的 index
    snapshot_term: u64,   // Raft 快照的 term
}

/// 快照保护机制
struct SnapshotProtection {
    /// 正在使用的快照 index（防止被删除）
    in_use_snapshots: HashSet<u64>,
    
    /// 受保护的日志范围（防止被截断）
    protected_log_ranges: Vec<(u64, u64)>,
}
```

## 实现要点

### 1. 快照保护机制

- 在 RaftState 中添加 `SnapshotProtection` 结构
- 在快照清理前检查 `in_use_snapshots`
- 在日志截断前检查 `protected_log_ranges`

### 2. 筛选实现

- 在 `RedisStore::create_split_snapshot` 中实现
- 直接遍历数据，只序列化目标槽位范围
- 避免全量序列化/反序列化开销

### 3. 传输协议

- 使用独立的 gRPC 服务 `SplitSnapshotService`
- 支持分块传输（1MB/chunk）
- 包含完整的元数据信息

### 4. 加载机制

- Leader 直接调用 `merge_from_snapshot`
- 手动设置 `applied_index = snapshot_index`
- 通过 Raft InstallSnapshot 复制到 Follower

### 5. 错误处理

- 传输失败：重试或回滚
- 加载失败：清理目标分片，标记分裂失败
- 保护释放：确保在传输完成后释放

## 待实现功能清单

- [ ] 在 RaftState 中添加快照保护机制
- [ ] 实现 `create_split_snapshot` 槽位范围筛选
- [ ] 实现分裂快照传输 gRPC 服务
- [ ] 实现目标分片接收和组装逻辑
- [ ] 实现通过 Raft InstallSnapshot 复制到 Follower
- [ ] 实现保护释放机制
- [ ] 添加错误处理和重试逻辑
