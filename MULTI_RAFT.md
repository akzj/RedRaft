# Multi-Raft 架构详细设计

## 核心概念

### Raft 组（Shard）作为"幽灵实例"

在 RaftKV 中，每个 Raft 组（Shard）是一个可以在集群节点间"漂移"的独立共识单元：

```
┌─────────────────────────────────────────────────────────┐
│  Raft Group "shard_0" - 可以在节点间漂移的"幽灵"         │
│                                                          │
│  时刻 T1: 在 node1, node2, node3                       │
│  时刻 T2: 迁移到 node2, node3, node4  ← 漂移           │
│  时刻 T3: 合并到 shard_1                                │
│  时刻 T4: 从 shard_1 分裂出来                          │
└─────────────────────────────────────────────────────────┘
```

## 架构设计

### 1. 键值路由

#### 路由算法

```rust
pub struct ShardRouter {
    shard_count: usize,
    active_shards: HashSet<String>,
}

impl ShardRouter {
    /// 根据键计算应该路由到哪个 Raft 组
    pub fn route(&self, key: &[u8]) -> String {
        let hash = self.hash_key(key);
        let shard_id = hash % self.shard_count;
        format!("shard_{}", shard_id)
    }
    
    fn hash_key(&self, key: &[u8]) -> usize {
        // 使用一致性哈希或简单哈希
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}
```

#### 路由表更新

当 Raft 组创建/删除/合并/分裂时，需要更新路由表：

```rust
// 创建新组：shard_count 增加
router.shard_count += 1;
router.active_shards.insert("shard_10".to_string());

// 删除组：从活跃列表移除
router.active_shards.remove("shard_5");

// 合并组：多个 shard_id → 一个 shard_id
router.merge_routes(vec!["shard_1", "shard_2"], "shard_1");

// 分裂组：一个 shard_id → 多个 shard_id
router.split_route("shard_0", vec!["shard_0", "shard_10"]);
```

### 2. Raft 组创建

#### 创建流程

```rust
async fn create_shard(
    shard_id: String,
    initial_nodes: Vec<String>,
) -> Result<()> {
    // 1. 创建 RaftId（group=shard_id, node=当前节点）
    let raft_id = RaftId::new(shard_id.clone(), node_id.clone());
    
    // 2. 创建初始集群配置
    let mut voters = HashSet::new();
    for node in initial_nodes {
        voters.insert(RaftId::new(shard_id.clone(), node));
    }
    let config = ClusterConfig::simple(voters, 0);
    
    // 3. 初始化 RaftState
    let options = RaftStateOptions {
        id: raft_id.clone(),
        peers: initial_nodes.iter().map(|n| 
            RaftId::new(shard_id.clone(), n.clone())
        ).collect(),
        // ... 其他配置
    };
    let raft_state = RaftState::new(options, callbacks).await?;
    
    // 4. 注册到 MultiRaftDriver
    driver.register_raft_group(raft_id.clone(), raft_state).await?;
    
    // 5. 更新路由表
    router.add_shard(shard_id.clone());
    
    Ok(())
}
```

### 3. Raft 组迁移 ⭐⭐⭐

#### 迁移场景

1. **负载均衡**：从高负载节点迁移到低负载节点
2. **节点下线**：节点下线前迁移所有 Raft 组
3. **手动迁移**：管理员手动触发迁移

#### 迁移流程

```
阶段 1: 准备
  ├─ 在目标节点创建新的 Raft 组（Learner 模式）
  └─ 开始数据同步

阶段 2: 数据迁移
  ├─ 从源节点读取所有键值对
  ├─ 传输到目标节点
  └─ 在目标节点应用（通过 Raft 日志）

阶段 3: 配置变更
  ├─ Joint Consensus: 添加目标节点为 Voter
  ├─ 等待新配置提交
  └─ 移除源节点

阶段 4: 切换
  ├─ 更新路由表（所有节点）
  ├─ 客户端重定向到新节点
  └─ 停止源节点的 Raft 组

阶段 5: 清理
  └─ 删除源节点数据
```

#### 实现细节

```rust
async fn migrate_shard(
    shard_id: String,
    from_node: String,
    to_node: String,
) -> Result<()> {
    // 1. 在目标节点创建 Learner
    let target_raft_id = RaftId::new(shard_id.clone(), to_node.clone());
    create_shard_as_learner(target_raft_id.clone()).await?;
    
    // 2. 数据迁移
    let source_state_machine = get_state_machine(&shard_id, &from_node).await?;
    let all_kvs = source_state_machine.get_all_key_values().await?;
    
    for (key, value) in all_kvs {
        // 通过 Raft 日志写入目标节点（保证一致性）
        let op = KVOperation::Put { key, value };
        propose_to_raft_group(target_raft_id.clone(), op).await?;
    }
    
    // 3. 配置变更：添加目标节点
    let config_change = ConfigChange {
        add_voters: vec![target_raft_id.clone()],
        remove_voters: vec![],
    };
    change_config(&shard_id, config_change).await?;
    
    // 4. 配置变更：移除源节点
    let source_raft_id = RaftId::new(shard_id.clone(), from_node.clone());
    let config_change = ConfigChange {
        add_voters: vec![],
        remove_voters: vec![source_raft_id],
    };
    change_config(&shard_id, config_change).await?;
    
    // 5. 更新路由表（所有节点）
    update_route_table_all_nodes(&shard_id, &to_node).await?;
    
    // 6. 清理源节点
    remove_shard_from_node(&shard_id, &from_node).await?;
    
    Ok(())
}
```

### 4. Raft 组合并

#### 合并流程

```
阶段 1: 准备
  ├─ 选择目标组（通常是第一个组）
  └─ 锁定所有源组（停止接受新请求）

阶段 2: 数据合并
  ├─ 读取所有源组的数据
  ├─ 处理键冲突（保留最新版本）
  └─ 写入目标组（通过 Raft 日志）

阶段 3: 配置变更
  ├─ Joint Consensus: 合并所有组的配置
  └─ 移除源组

阶段 4: 路由更新
  └─ 更新路由表（多个 shard_id → 一个 shard_id）

阶段 5: 清理
  └─ 删除源组数据
```

#### 实现细节

```rust
async fn merge_shards(
    source_shards: Vec<String>,
    target_shard: String,
) -> Result<()> {
    // 1. 锁定源组（停止接受新请求）
    for shard_id in &source_shards {
        lock_shard(shard_id).await?;
    }
    
    // 2. 合并数据
    for shard_id in &source_shards {
        let state_machine = get_state_machine(shard_id).await?;
        let all_kvs = state_machine.get_all_key_values().await?;
        
        for (key, value) in all_kvs {
            // 检查目标组是否已有该键
            if let Some(existing) = get_from_target(&target_shard, &key).await? {
                // 冲突处理：保留版本号更大的
                if value.version > existing.version {
                    let op = KVOperation::Put { key, value };
                    propose_to_raft_group(&target_shard, op).await?;
                }
            } else {
                // 无冲突，直接写入
                let op = KVOperation::Put { key, value };
                propose_to_raft_group(&target_shard, op).await?;
            }
        }
    }
    
    // 3. 配置变更：合并配置
    let target_config = get_config(&target_shard).await?;
    let mut merged_voters = target_config.voters.clone();
    
    for shard_id in &source_shards {
        let config = get_config(shard_id).await?;
        merged_voters.extend(config.voters);
    }
    
    let new_config = ClusterConfig::simple(merged_voters, target_config.log_index);
    change_config(&target_shard, new_config).await?;
    
    // 4. 更新路由表
    router.merge_routes(source_shards.clone(), target_shard.clone());
    
    // 5. 删除源组
    for shard_id in source_shards {
        delete_shard(&shard_id).await?;
    }
    
    Ok(())
}
```

### 5. Raft 组分裂

#### 分裂流程

```
阶段 1: 准备
  ├─ 确定分裂策略（按键范围或哈希）
  └─ 创建新的 Raft 组

阶段 2: 数据分裂
  ├─ 遍历源组所有键值对
  ├─ 根据分裂策略分配到新组
  └─ 写入新组（通过 Raft 日志）

阶段 3: 配置变更
  ├─ 创建新组的配置
  └─ 移除源组（如果完全分裂）

阶段 4: 路由更新
  └─ 更新路由表（一个 shard_id → 多个 shard_id）

阶段 5: 清理
  └─ 删除源组数据（如果完全分裂）
```

#### 实现细节

```rust
async fn split_shard(
    source_shard: String,
    new_shards: Vec<String>,
    split_strategy: SplitStrategy,
) -> Result<()> {
    // 1. 创建新 Raft 组
    for shard_id in &new_shards {
        create_shard(shard_id.clone(), get_initial_nodes()).await?;
    }
    
    // 2. 分裂数据
    let source_state_machine = get_state_machine(&source_shard).await?;
    let all_kvs = source_state_machine.get_all_key_values().await?;
    
    for (key, value) in all_kvs {
        // 根据分裂策略决定分配到哪个新组
        let target_shard = match split_strategy {
            SplitStrategy::Hash => {
                let hash = hash_key(&key);
                let idx = hash % new_shards.len();
                new_shards[idx].clone()
            }
            SplitStrategy::Range { ranges } => {
                // 根据键的范围分配
                find_range(&key, &ranges)
            }
        };
        
        let op = KVOperation::Put { key, value };
        propose_to_raft_group(&target_shard, op).await?;
    }
    
    // 3. 更新路由表
    router.split_route(source_shard.clone(), new_shards.clone());
    
    // 4. 删除源组（如果完全分裂）
    if should_delete_source(&split_strategy) {
        delete_shard(&source_shard).await?;
    }
    
    Ok(())
}
```

## 数据一致性保证

### 组内一致性

- 每个 Raft 组内部保证线性一致性
- 使用 ReadIndex 和 LeaderLease 保证读一致性

### 跨组一致性

- 不同 Raft 组之间**没有全局顺序保证**（这是分片的特性）
- 如果需要跨组事务，需要额外的协调机制（不在本设计范围内）

### 迁移/合并/分裂一致性

- **迁移**：使用 Raft 配置变更保证迁移过程的一致性
- **合并**：合并过程中锁定源组，确保数据不丢失
- **分裂**：分裂过程中锁定源组，确保数据完整分配

## 性能优势

### 并发能力

**单 Raft 组**：
- 所有请求串行处理
- 吞吐量受限于单组性能

**Multi-Raft（N 组）**：
- N 个组并发处理
- 吞吐量 ≈ N × 单组吞吐量

### 扩展性

- **水平扩展**：通过增加 Raft 组数量提高吞吐量
- **负载均衡**：通过迁移实现负载均衡
- **动态调整**：根据负载动态创建/合并/分裂组

## 实现挑战

### 1. 路由表一致性

- **问题**：所有节点的路由表必须一致
- **解决**：通过管理 Raft 组（meta group）维护路由表

### 2. 迁移原子性

- **问题**：迁移过程中不能丢失数据
- **解决**：使用 Raft 配置变更 + 数据同步

### 3. 跨组操作（SCAN）

- **问题**：SCAN 需要查询多个组
- **解决**：并发查询所有相关组，合并结果

### 4. 负载均衡

- **问题**：如何决定何时迁移/合并/分裂
- **解决**：监控各组负载，自动触发操作

## 元数据管理集群

### 设计目标

元数据管理集群是一个独立的 Raft 集群，用于管理整个 RaftKV 集群的元数据信息。它解决了以下问题：

1. **配置一致性**：确保所有节点看到的 Shard 配置、路由表等信息一致
2. **动态管理**：支持 Shard 的创建、迁移、合并、分裂等操作的协调
3. **高可用性**：元数据集群本身使用 Raft 保证高可用
4. **实时同步**：通过 Watch 机制实时通知数据节点配置变更

### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│           元数据管理集群 (Metadata Cluster)                  │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │Meta Node1│  │Meta Node2│  │Meta Node3│  │Meta Node4│   │
│  │          │  │          │  │          │  │          │   │
│  │ Raft     │  │ Raft     │  │ Raft     │  │ Raft     │   │
│  │ Group    │  │ Group    │  │ Group    │  │ Group    │   │
│  │"meta"    │  │"meta"    │  │"meta"    │  │"meta"    │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
│       │              │              │              │         │
│       └──────────────┴──────────────┴──────────────┘         │
│                    Raft Consensus                             │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ Metadata API (gRPC/HTTP)
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──────┐  ┌─────▼──────┐  ┌────▼──────┐
│ Data Node 1  │  │ Data Node 2│  │ Data Node N│
│              │  │            │  │            │
│ ┌──────────┐ │  │ ┌────────┐│  │ ┌────────┐│
│ │Shard     │ │  │ │ Shard  ││  │ │ Shard  ││
│ │Manager   │ │  │ │Manager ││  │ │Manager ││
│ │          │ │  │ │        ││  │ │        ││
│ │ Watch    │ │  │ │ Watch  ││  │ │ Watch  ││
│ │ Metadata │ │  │ │Metadata││  │ │Metadata││
│ └──────────┘ │  │ └────────┘│  │ └────────┘│
└──────────────┘  └───────────┘  └────────────┘
```

### 元数据状态机

元数据管理集群使用一个专门的 Raft 组（group_id="meta"）来存储元数据。状态机存储以下信息：

```rust
// 元数据状态机
pub struct MetadataStateMachine {
    // Shard 配置表：shard_id -> ShardConfig
    shards: HashMap<String, ShardConfig>,
    
    // 节点信息表：node_id -> NodeInfo
    nodes: HashMap<String, NodeInfo>,
    
    // 路由表配置
    routing_table: RoutingTable,
    
    // 集群状态
    cluster_state: ClusterState,
}

// Shard 配置
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardConfig {
    pub shard_id: String,
    pub nodes: Vec<String>,              // 节点列表
    pub state: ShardState,                // Active, Migrating, Merging, Splitting
    pub version: u64,                     // 配置版本
    pub created_at: u64,
    pub updated_at: u64,
}

// 节点信息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub address: String,                  // gRPC 地址
    pub state: NodeState,                 // Online, Offline, Unhealthy
    pub shards: Vec<String>,              // 参与的 Shard 列表
    pub load: NodeLoad,                   // 负载信息
    pub last_heartbeat: u64,
}

// 路由表
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingTable {
    pub version: u64,
    pub shard_count: usize,
    pub active_shards: Vec<String>,
    pub hash_function: String,            // "hash" or "consistent_hash"
    pub updated_at: u64,
}
```

### 元数据操作流程

#### 1. Shard 创建流程

```rust
// 客户端请求创建 Shard
async fn create_shard(
    metadata_cluster: &MetadataCluster,
    shard_id: String,
    initial_nodes: Vec<String>,
) -> Result<()> {
    // 1. 向元数据集群提交创建请求
    let config = ShardConfig {
        shard_id: shard_id.clone(),
        nodes: initial_nodes.clone(),
        state: ShardState::Creating,
        version: 1,
        created_at: now(),
        updated_at: now(),
    };
    
    // 2. 元数据集群通过 Raft 共识确认
    metadata_cluster.apply_command(
        MetadataCommand::CreateShard(config)
    ).await?;
    
    // 3. 元数据集群通知相关数据节点
    for node_id in initial_nodes {
        notify_node(node_id, MetadataEvent::ShardCreated(shard_id.clone())).await?;
    }
    
    // 4. 数据节点收到通知后，创建本地 Shard 实例
    // 5. 数据节点确认创建完成后，更新元数据集群
    metadata_cluster.apply_command(
        MetadataCommand::UpdateShardState(shard_id, ShardState::Active)
    ).await?;
    
    Ok(())
}
```

#### 2. Shard 迁移流程

```rust
async fn migrate_shard(
    metadata_cluster: &MetadataCluster,
    shard_id: String,
    from_node: String,
    to_node: String,
) -> Result<()> {
    // 1. 更新元数据：标记 Shard 为迁移中
    metadata_cluster.apply_command(
        MetadataCommand::UpdateShardState(shard_id.clone(), ShardState::Migrating)
    ).await?;
    
    // 2. 通知源节点和目标节点
    notify_node(from_node.clone(), 
        MetadataEvent::ShardMigrationStart(shard_id.clone(), to_node.clone())
    ).await?;
    notify_node(to_node.clone(),
        MetadataEvent::ShardMigrationStart(shard_id.clone(), from_node.clone())
    ).await?;
    
    // 3. 执行数据迁移（由 Shard Manager 协调）
    // 4. 迁移完成后，更新元数据：更新节点列表
    let mut config = metadata_cluster.get_shard_config(&shard_id).await?;
    config.nodes.retain(|n| n != &from_node);
    config.nodes.push(to_node);
    config.state = ShardState::Active;
    config.version += 1;
    
    metadata_cluster.apply_command(
        MetadataCommand::UpdateShardConfig(config)
    ).await?;
    
    // 5. 通知所有相关节点更新配置
    notify_all_nodes(MetadataEvent::ShardConfigUpdated(shard_id)).await?;
    
    Ok(())
}
```

#### 3. 路由表更新流程

```rust
async fn update_routing_table(
    metadata_cluster: &MetadataCluster,
    new_table: RoutingTable,
) -> Result<()> {
    // 1. 更新元数据集群中的路由表
    metadata_cluster.apply_command(
        MetadataCommand::UpdateRoutingTable(new_table.clone())
    ).await?;
    
    // 2. 通过 Watch 机制通知所有数据节点
    // 数据节点会收到路由表变更事件，更新本地路由表
    broadcast_event(MetadataEvent::RoutingTableUpdated(new_table)).await?;
    
    Ok(())
}
```

### Watch 机制

数据节点通过 Watch 机制监听元数据变更：

```rust
pub struct MetadataWatcher {
    metadata_cluster: Arc<MetadataCluster>,
    watch_stream: mpsc::Receiver<MetadataEvent>,
}

impl MetadataWatcher {
    async fn watch(&mut self) -> Result<()> {
        // 1. 建立与元数据集群的连接
        let mut stream = self.metadata_cluster.watch_metadata().await?;
        
        // 2. 接收元数据变更事件
        while let Some(event) = stream.recv().await {
            match event {
                MetadataEvent::ShardCreated(shard_id) => {
                    self.handle_shard_created(shard_id).await?;
                }
                MetadataEvent::ShardConfigUpdated(shard_id) => {
                    self.handle_shard_config_updated(shard_id).await?;
                }
                MetadataEvent::RoutingTableUpdated(table) => {
                    self.handle_routing_table_updated(table).await?;
                }
                // ... 其他事件
            }
        }
        
        Ok(())
    }
    
    async fn handle_shard_created(&self, shard_id: String) -> Result<()> {
        // 获取 Shard 配置
        let config = self.metadata_cluster.get_shard_config(&shard_id).await?;
        
        // 如果当前节点在节点列表中，创建本地 Shard 实例
        if config.nodes.contains(&self.node_id) {
            self.shard_manager.create_local_shard(&config).await?;
        }
        
        Ok(())
    }
}
```

### 元数据集群的部署

1. **独立部署**（推荐）
   - 元数据集群与数据集群物理分离
   - 元数据集群节点数量较少（3-5 个节点）
   - 元数据集群的负载相对较低

2. **混合部署**
   - 元数据集群节点可以是数据节点的一部分
   - 需要确保元数据集群的高可用性

3. **高可用性**
   - 元数据集群使用 Raft 共识算法
   - 支持节点故障自动恢复
   - 元数据变更需要多数派确认

## 使用示例

### 创建 Raft 组

```rust
// 创建 shard_0，初始节点为 node1, node2, node3
create_shard("shard_0", vec!["node1", "node2", "node3"]).await?;
```

### 迁移 Raft 组

```rust
// 将 shard_0 从 node1 迁移到 node4
migrate_shard("shard_0", "node1", "node4").await?;
```

### 合并 Raft 组

```rust
// 将 shard_1 和 shard_2 合并到 shard_1
merge_shards(vec!["shard_1", "shard_2"], "shard_1").await?;
```

### 分裂 Raft 组

```rust
// 将 shard_0 分裂成 shard_0 和 shard_10
split_shard("shard_0", vec!["shard_0", "shard_10"], SplitStrategy::Hash).await?;
```

