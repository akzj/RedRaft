# RaftKV - 分布式键值存储系统设计文档

## 项目概述

**RaftKV** 是基于 `raft-lite` 实现的分布式键值存储系统，用于验证和测试 Raft 共识算法的正确性、可用性和性能。

### 核心特性：Multi-Raft 架构

**RaftKV 的核心创新是 Multi-Raft 架构**，解决了单一 Raft 节点串行提交、并发能力低的缺点：

- ✅ **多 Raft 实例并发**：单个节点支持多个 Raft 组（shard），不同组可以并发提交
- ✅ **数据分片**：通过键值路由到不同的 Raft 组，实现数据分片和负载均衡
- ✅ **动态管理**：支持 Raft 实例的实时创建、迁移、合并
- ✅ **实例漂移**：Raft 实例可以在集群节点间"漂移"，实现动态负载均衡和故障恢复

### 项目目标

1. **功能验证**：验证 Raft 算法的正确性（选举、日志复制、配置变更等）
2. **可用性测试**：测试系统在故障场景下的行为（节点宕机、网络分区等）
3. **性能分析**：评估系统在不同负载下的性能表现
4. **压测优化**：通过压测发现性能瓶颈并进行优化
5. **Multi-Raft 验证**：验证多 Raft 实例的并发能力和动态管理能力

## 架构设计

### Multi-Raft 系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RaftKV Client                                   │
│              (PUT/GET/DELETE/SCAN operations)                            │
└──────────────────────┬──────────────────────────────────────────────────┘
                       │ gRPC/HTTP
                       │ Key Routing (hash(key) % shard_count)
                       │
    ┌──────────────────┼──────────────────┬──────────────────┬──────────┐
    │                  │                  │                  │          │
┌───▼───┐    ┌─────▼─────┐   ┌───▼───┐   ┌───▼───┐   ┌───▼───┐  ...  ┌───▼───┐
│Node 1 │    │  Node 2   │   │Node 3 │   │Node 4 │   │Node 5 │       │Node N │
│       │    │           │   │       │   │       │   │       │       │(1000+)│
│┌─────┐│    │┌─────────┐│   │┌─────┐│   │┌─────┐│   │┌─────┐│       │┌─────┐│
││Multi││    ││  Multi  ││   ││Multi││   ││Multi││   ││Multi││  ...  ││Multi││
││Raft ││    ││  Raft   ││   ││Raft ││   ││Raft ││   ││Raft ││       ││Raft ││
││Driver│    ││  Driver ││   ││Driver││   ││Driver││   ││Driver││       ││Driver││
│└─────┘│    │└─────────┘│   │└─────┘│   │└─────┘│   │└─────┘│       │└─────┘│
│       │    │           │   │       │   │       │   │       │       │       │
│┌─────┐│    │┌─────────┐│   │┌─────┐│   │       │   │       │       │       │
││Shard││    ││  Shard  ││   ││Shard││   │       │   │       │  ...  │       │
││  0  ││    ││    0    ││   ││  0  ││   │       │   │       │       │       │
│└─────┘│    │└─────────┘│   │└─────┘│   │       │   │       │       │       │
│       │    │           │   │       │   │       │   │       │       │       │
│       │    │┌─────────┐│   │┌─────┐│   │┌─────┐│   │       │       │       │
│       │    ││  Shard  ││   ││Shard││   ││Shard││   │       │  ...  │       │
│       │    ││    1    ││   ││  1  ││   ││  1  ││   │       │       │       │
│       │    │└─────────┘│   │└─────┘│   │└─────┘│   │       │       │       │
│       │    │           │   │       │   │       │   │       │       │       │
│       │    │           │   │┌─────┐│   │       │   │┌─────┐│       │       │
│       │    │           │   ││Shard││   │       │   ││Shard││  ...  │       │
│       │    │           │   ││  2  ││   │       │   ││  2  ││       │       │
│       │    │           │   │└─────┘│   │       │   │└─────┘│       │       │
│  ...  │    │   ...     │   │  ...  │   │  ...  │   │  ...  │  ...  │  ...  │
│       │    │           │   │       │   │       │   │       │       │       │
│┌─────┐│    │┌─────────┐│   │┌─────┐│   │┌─────┐│   │┌─────┐│       │┌─────┐│
││File ││    ││  File   ││   ││File ││   ││File ││   ││File ││  ...  ││File ││
││Store││    ││  Store  ││   ││Store││   ││Store││   ││Store││       ││Store││
│└─────┘│    │└─────────┘│   │└─────┘│   │└─────┘│   │└─────┘│       │└─────┘│
└───────┘    └───────────┘   └───────┘   └───────┘   └───────┘       └───────┘
    │              │              │              │              │              │
    └──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
                    Raft Network (P2P communication)

说明：
- 节点数量：可以扩展到 1000+ 个物理节点
- Shard 数量：通常只有 3-5 个逻辑分片（独立配置）
- 每个 Shard 在多个节点上复制（例如：Shard 0 在 Node 1,2,3；Shard 1 在 Node 2,3,4；Shard 2 在 Node 3,5）
- 节点可以参与多个 Shard 的复制（负载均衡）
```

### 元数据管理集群架构

元数据管理集群是一个独立的 Raft 集群，用于管理整个 RaftKV 集群的元数据信息。它确保元数据的一致性和高可用性。

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    元数据管理集群 (Metadata Cluster)                       │
│                                                                           │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐          │
│  │Meta Node │    │Meta Node │    │Meta Node │    │Meta Node │  ...     │
│  │    1     │    │    2     │    │    3     │    │    4     │          │
│  │          │    │          │    │          │    │          │          │
│  │┌────────┐│    │┌────────┐│    │┌────────┐│    │┌────────┐│          │
│  ││  Raft  ││    ││  Raft  ││    ││  Raft  ││    ││  Raft  ││          │
│  ││ Group  ││    ││ Group  ││    ││ Group  ││    ││ Group  ││          │
│  ││"meta"  ││    ││"meta"  ││    ││"meta"  ││    ││"meta"  ││          │
│  │└────────┘│    │└────────┘│    │└────────┘│    │└────────┘│          │
│  │          │    │          │    │          │    │          │          │
│  │┌────────┐│    │┌────────┐│    │┌────────┐│    │┌────────┐│          │
│  ││Metadata││    ││Metadata││    ││Metadata││    ││Metadata││          │
│  ││ State  ││    ││ State  ││    ││ State  ││    ││ State  ││          │
│  ││Machine││    ││Machine ││    ││Machine ││    ││Machine ││          │
│  │└────────┘│    │└────────┘│    │└────────┘│    │└────────┘│          │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘          │
│       │                │                │                │              │
│       └────────────────┴────────────────┴────────────────┘              │
│                      Raft Consensus (3-5 nodes)                          │
└─────────────────────────────────────────────────────────────────────────┘
                              │
                              │ gRPC/HTTP (Metadata API)
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼──────┐    ┌─────────▼─────────┐  ┌───────▼──────┐
│  Data Node 1 │    │   Data Node 2     │  │  Data Node N │
│              │    │                   │  │              │
│ ┌──────────┐ │    │  ┌──────────────┐ │  │ ┌──────────┐ │
│ │Shard     │ │    │  │ Shard        │ │  │ │ Shard     │ │
│ │Manager   │ │    │  │ Manager      │ │  │ │ Manager   │ │
│ │          │ │    │  │              │ │  │ │           │ │
│ │  ┌─────┐ │ │    │  │  ┌─────────┐ │ │  │ │  ┌─────┐  │ │
│ │  │Watch│ │ │    │  │  │ Watch   │ │ │  │ │  │Watch│  │ │
│ │  │Meta│ │ │    │  │  │ Meta     │ │ │  │ │  │Meta │  │ │
│ │  └─────┘ │ │    │  │  └─────────┘ │ │  │ │  └─────┘  │ │
│ └──────────┘ │    │  └──────────────┘ │  │ └──────────┘ │
└──────────────┘    └───────────────────┘  └──────────────┘
```

#### 元数据管理集群的核心职责

1. **Shard 配置管理**
   - 存储每个 Shard 的配置信息（Shard ID、节点列表、状态等）
   - 管理 Shard 的创建、删除、迁移、合并、分裂操作
   - 维护 Shard 的版本号和变更历史

2. **节点信息管理**
   - 存储集群中所有节点的信息（节点 ID、地址、状态、负载等）
   - 管理节点的加入、离开、故障恢复
   - 维护节点与 Shard 的映射关系

3. **路由表管理**
   - 存储键值到 Shard 的映射规则
   - 管理路由表的版本和变更
   - 支持路由表的动态更新和通知

4. **集群状态管理**
   - 存储集群的整体状态（Shard 数量、节点数量、负载分布等）
   - 提供集群健康检查和监控信息
   - 支持集群配置的全局变更

#### 元数据存储结构

```rust
// 元数据状态机存储的数据结构

// Shard 配置
pub struct ShardConfig {
    pub shard_id: String,
    pub nodes: Vec<NodeId>,           // 该 Shard 所在的节点列表
    pub state: ShardState,             // 状态：Active, Migrating, Merging, Splitting
    pub version: u64,                  // 配置版本号
    pub created_at: u64,               // 创建时间
    pub updated_at: u64,                // 更新时间
}

// 节点信息
pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: String,               // gRPC 地址
    pub state: NodeState,              // 状态：Online, Offline, Unhealthy
    pub shards: Vec<String>,           // 该节点参与的 Shard 列表
    pub load: NodeLoad,                // 负载信息
    pub last_heartbeat: u64,           // 最后心跳时间
}

// 路由表配置
pub struct RoutingTable {
    pub version: u64,                   // 路由表版本
    pub shard_count: usize,              // Shard 总数
    pub active_shards: Vec<String>,     // 活跃的 Shard 列表
    pub hash_function: String,          // 哈希函数类型
    pub updated_at: u64,                // 更新时间
}

// 集群状态
pub struct ClusterState {
    pub total_nodes: usize,
    pub total_shards: usize,
    pub active_shards: usize,
    pub cluster_version: u64,
    pub last_updated: u64,
}
```

#### 元数据管理集群的特点

1. **独立部署**
   - 元数据集群与数据集群物理分离（可选）
   - 元数据集群节点数量较少（通常 3-5 个节点）
   - 元数据集群的负载相对较低

2. **高可用性**
   - 使用 Raft 共识算法保证一致性
   - 支持节点故障自动恢复
   - 元数据变更需要多数派确认

3. **实时同步**
   - 数据节点通过 Watch 机制监听元数据变更
   - 元数据变更后自动通知所有相关数据节点
   - 支持增量更新和全量同步

4. **版本控制**
   - 所有元数据变更都有版本号
   - 支持元数据的回滚和恢复
   - 提供元数据变更历史查询

#### 元数据 API

```rust
// 元数据管理 API

// Shard 管理
async fn create_shard(shard_id: String, nodes: Vec<NodeId>) -> Result<()>;
async fn delete_shard(shard_id: String) -> Result<()>;
async fn migrate_shard(shard_id: String, from: NodeId, to: NodeId) -> Result<()>;
async fn merge_shards(shard_ids: Vec<String>, target_shard: String) -> Result<()>;
async fn split_shard(shard_id: String, new_shards: Vec<String>) -> Result<()>;
async fn get_shard_config(shard_id: String) -> Result<ShardConfig>;
async fn list_shards() -> Result<Vec<ShardConfig>>;

// 节点管理
async fn register_node(node_id: NodeId, address: String) -> Result<()>;
async fn unregister_node(node_id: NodeId) -> Result<()>;
async fn update_node_state(node_id: NodeId, state: NodeState) -> Result<()>;
async fn get_node_info(node_id: NodeId) -> Result<NodeInfo>;
async fn list_nodes() -> Result<Vec<NodeInfo>>;

// 路由表管理
async fn update_routing_table(table: RoutingTable) -> Result<()>;
async fn get_routing_table() -> Result<RoutingTable>;
async fn watch_routing_table() -> Result<Stream<RoutingTable>>;

// 集群状态
async fn get_cluster_state() -> Result<ClusterState>;
async fn watch_cluster_state() -> Result<Stream<ClusterState>>;
```

#### 数据节点与元数据集群的交互

1. **启动时**
   - 数据节点启动时从元数据集群获取当前配置
   - 注册节点信息到元数据集群
   - 订阅元数据变更通知

2. **运行时**
   - 定期向元数据集群发送心跳
   - 监听元数据变更（Shard 配置、路由表等）
   - 根据元数据变更动态调整本地 Shard 配置

3. **变更时**
   - Shard 创建/删除/迁移操作通过元数据集群协调
   - 元数据集群确认变更后，通知相关数据节点
   - 数据节点执行变更并确认完成

### Multi-Raft 核心概念

#### 1. Raft 组（Shard）作为"幽灵实例"

每个 Raft 组是一个独立的共识单元，可以在集群节点间"漂移"：

- **独立性**：每个 Raft 组有独立的 term、日志、状态机
- **并发性**：不同 Raft 组可以并发处理请求，互不阻塞
- **可迁移性**：Raft 组可以在节点间迁移，实现动态负载均衡
- **可合并性**：多个小 Raft 组可以合并成一个大组
- **可分裂性**：一个大 Raft 组可以分裂成多个小组

#### 2. 键值路由

```
Client Request (key="user:123")
    ↓
Hash(key) → shard_id = hash("user:123") % shard_count
    ↓
Route to Raft Group "shard_{shard_id}"
    ↓
Concurrent processing (other shards not blocked)
```

#### 3. 动态管理

```
┌─────────────────────────────────────────┐
│      Raft Group Management API          │
├─────────────────────────────────────────┤
│  create_shard(shard_id, nodes)          │  ← 创建新 Raft 组
│  migrate_shard(shard_id, from, to)      │  ← 迁移 Raft 组
│  merge_shards(shard1, shard2)           │  ← 合并 Raft 组
│  split_shard(shard_id, new_shards)       │  ← 分裂 Raft 组
│  list_shards()                           │  ← 列出所有 Raft 组
└─────────────────────────────────────────┘
```

### 组件说明

1. **RaftKV Server**
   - 每个节点运行一个 RaftKV 服务器
   - 处理客户端请求（PUT/GET/DELETE/SCAN）
   - **键值路由**：根据键的哈希值路由到对应的 Raft 组
   - 提供 gRPC 和 HTTP 接口

2. **MultiRaftDriver**
   - 使用 `raft-lite` 的 `MultiRaftDriver`
   - **管理多个 Raft 组**：每个节点可以运行多个 Raft 实例
   - **并发处理**：不同 Raft 组的事件可以并发处理
   - **事件调度**：高效的事件分发和定时器管理

3. **Raft Group (Shard)**
   - 每个 Raft 组是一个独立的共识单元
   - 使用 `raft-lite` 的 `RaftState`
   - 处理选举、日志复制、配置变更
   - 维护该组的数据一致性

4. **FileStorage**
   - 使用 `raft-lite` 的 `FileStorage`
   - **多组共享**：所有 Raft 组共享同一个存储实例
   - **数据隔离**：通过 `RaftId` (group_id, node_id) 区分不同组
   - 持久化日志、硬状态、集群配置、快照
   - 支持数据恢复

5. **KV State Machine**
   - 每个 Raft 组有独立的状态机实例
   - 将 Raft 日志应用到 KV 存储
   - 维护该组的键值对（内存 HashMap）
   - 支持快照和恢复

6. **Shard Manager**
   - **Raft 组生命周期管理**：创建、删除、迁移、合并、分裂
   - **路由表管理**：维护键值到 Raft 组的映射
   - **负载均衡**：监控各组的负载，动态调整
   - **元数据同步**：与元数据管理集群交互，获取和更新配置

7. **元数据管理集群 (Metadata Cluster)**
   - **独立的 Raft 集群**：使用 Raft 共识算法保证元数据一致性
   - **Shard 配置管理**：存储和管理所有 Shard 的配置信息
   - **节点信息管理**：存储和管理集群中所有节点的信息
   - **路由表管理**：存储和管理键值到 Shard 的映射规则
   - **变更通知**：通过 Watch 机制向数据节点推送元数据变更
   - **高可用性**：通常部署 3-5 个节点，支持节点故障自动恢复

## 数据模型

### 键值对存储

```rust
pub struct KeyValue {
    pub key: Vec<u8>,      // 键（任意字节）
    pub value: Vec<u8>,    // 值（任意字节）
    pub version: u64,      // 版本号（单调递增）
    pub timestamp: u64,    // 时间戳
}
```

### Raft 组标识

```rust
// RaftId 用于标识不同的 Raft 组
pub struct RaftId {
    pub group: String,    // Raft 组 ID (如 "shard_0", "shard_1")
    pub node: String,     // 节点 ID (如 "node1", "node2")
}

// 示例：
// - shard_0 在 node1: RaftId { group: "shard_0", node: "node1" }
// - shard_0 在 node2: RaftId { group: "shard_0", node: "node2" }
// - shard_1 在 node1: RaftId { group: "shard_1", node: "node1" }
```

### 操作类型

```rust
pub enum KVOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    // 内部操作，不对外暴露
    NoOp,  // 用于测试
}
```

### 日志条目

- 每个 KV 操作作为一个 Raft 日志条目
- 日志条目包含：操作类型、键、值、客户端请求 ID
- **每个 Raft 组有独立的日志序列**

### 路由表

```rust
pub struct ShardRouter {
    // 键值到 Raft 组的映射
    // 通过 hash(key) % shard_count 计算
    shard_count: usize,
    
    // 活跃的 Raft 组列表
    active_shards: HashSet<String>,  // {"shard_0", "shard_1", ...}
    
    // Raft 组到节点的映射（用于迁移）
    shard_locations: HashMap<String, Vec<String>>,  // shard_id -> [node1, node2, ...]
}
```

## API 设计

### gRPC API

```protobuf
service RaftKVService {
    // ========== KV 操作 ==========
    // 写入键值对（自动路由到对应的 Raft 组）
    rpc Put(PutRequest) returns (PutResponse);
    
    // 读取键值对（自动路由到对应的 Raft 组）
    rpc Get(GetRequest) returns (GetResponse);
    
    // 删除键值对（自动路由到对应的 Raft 组）
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    
    // 范围扫描（可能跨多个 Raft 组）
    rpc Scan(ScanRequest) returns (ScanResponse);
    
    // ========== 集群管理 ==========
    // 获取集群状态
    rpc GetClusterStatus(Empty) returns (ClusterStatus);
    
    // 获取节点信息
    rpc GetNodeInfo(Empty) returns (NodeInfo);
    
    // ========== Multi-Raft 管理 ==========
    // 创建新的 Raft 组（Shard）
    rpc CreateShard(CreateShardRequest) returns (CreateShardResponse);
    
    // 删除 Raft 组
    rpc DeleteShard(DeleteShardRequest) returns (DeleteShardResponse);
    
    // 迁移 Raft 组（从一个节点迁移到另一个节点）
    rpc MigrateShard(MigrateShardRequest) returns (MigrateShardResponse);
    
    // 合并多个 Raft 组
    rpc MergeShards(MergeShardsRequest) returns (MergeShardsResponse);
    
    // 分裂 Raft 组（一个大组分裂成多个小组）
    rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);
    
    // 列出所有 Raft 组
    rpc ListShards(Empty) returns (ListShardsResponse);
    
    // 获取 Raft 组状态
    rpc GetShardStatus(GetShardStatusRequest) returns (GetShardStatusResponse);
}
```

### HTTP REST API

```
# ========== KV 操作 ==========
PUT    /kv/{key}           - 写入键值对（自动路由）
GET    /kv/{key}           - 读取键值对（自动路由）
DELETE /kv/{key}           - 删除键值对（自动路由）
GET    /kv?prefix={prefix} - 范围扫描（跨组）

# ========== 集群管理 ==========
GET    /status             - 集群状态
GET    /metrics            - 性能指标

# ========== Multi-Raft 管理 ==========
POST   /admin/shard        - 创建 Raft 组
DELETE /admin/shard/{id}   - 删除 Raft 组
POST   /admin/shard/{id}/migrate - 迁移 Raft 组
POST   /admin/shard/merge  - 合并 Raft 组
POST   /admin/shard/{id}/split - 分裂 Raft 组
GET    /admin/shard        - 列出所有 Raft 组
GET    /admin/shard/{id}   - 获取 Raft 组状态
```

## 一致性保证

### 线性一致性（Linearizability）

- **每个 Raft 组内部**：所有读写操作满足线性一致性
- **跨 Raft 组**：不同组之间没有全局顺序保证（这是分片的特性）
- 使用 ReadIndex 机制确保读操作的线性一致性
- LeaderLease 优化（可选，需要时钟同步）

### 写操作流程（Multi-Raft）

```
Client Request (key="user:123")
    ↓
Hash(key) → shard_id = hash("user:123") % shard_count
    ↓
Route to Raft Group "shard_{shard_id}"
    ↓
Leader (of shard_{shard_id}) → Raft Log → Commit → Apply → State Machine → Response
    ↓
(其他 Raft 组并发处理，不阻塞)
```

### 读操作流程（Multi-Raft）

```
Client Request (key="user:123")
    ↓
Hash(key) → shard_id
    ↓
Route to Raft Group "shard_{shard_id}"
    ↓
Leader → ReadIndex → Wait for Apply → Read State Machine → Response
```

### 跨组操作（SCAN）

```
Client Request (prefix="user:")
    ↓
Hash all keys with prefix → multiple shards
    ↓
Concurrent queries to all relevant shards
    ↓
Merge results → Response
```

## 性能目标

### Multi-Raft 并发优势

**单 Raft 组**：
- 写吞吐：~10,000 ops/s（串行提交）
- 读吞吐：~50,000 ops/s

**Multi-Raft（10 个组）**：
- 写吞吐：~100,000 ops/s（10 组并发）
- 读吞吐：~500,000 ops/s（10 组并发）

### 延迟目标

- **写操作（P99）**: < 10ms（本地网络，单组）
- **读操作（P99）**: < 5ms（使用 LeaderLease，单组）
- **读操作（P99）**: < 10ms（使用 ReadIndex，单组）
- **跨组操作（SCAN）**: 取决于涉及的组数量

### 吞吐量目标

- **单组写吞吐**: > 10,000 ops/s
- **单组读吞吐**: > 50,000 ops/s
- **Multi-Raft 写吞吐**: 线性扩展（N 组 = N × 10,000 ops/s）
- **Multi-Raft 读吞吐**: 线性扩展（N 组 = N × 50,000 ops/s）
- **集群吞吐**: 3 节点 × N 组 = 3N × 10,000 ops/s（写）

## 功能要点

### 核心功能

1. **基本操作**
   - ✅ PUT：写入键值对（自动路由到对应 Raft 组）
   - ✅ GET：读取键值对（自动路由到对应 Raft 组）
   - ✅ DELETE：删除键值对（自动路由到对应 Raft 组）
   - ✅ SCAN：范围扫描（跨多个 Raft 组，并发查询）

2. **Multi-Raft 核心功能** ⭐
   - ✅ **键值路由**：根据键的哈希值自动路由到对应的 Raft 组
   - ✅ **并发处理**：多个 Raft 组并发处理请求，互不阻塞
   - ✅ **动态创建**：运行时创建新的 Raft 组
   - ✅ **实例迁移**：Raft 组可以在节点间迁移（数据迁移 + 配置变更）
   - ✅ **实例合并**：多个小 Raft 组合并成一个大组
   - ✅ **实例分裂**：一个大 Raft 组分裂成多个小组
   - ✅ **路由表管理**：动态更新键值到 Raft 组的映射

3. **集群管理**
   - ✅ 节点启动和加入集群
   - ✅ 节点退出和移除
   - ✅ 集群配置变更（动态添加/删除节点）
   - ✅ 集群状态查询
   - ✅ **Raft 组状态查询**：查询每个 Raft 组的状态

4. **故障处理**
   - ✅ Leader 故障转移（每个 Raft 组独立）
   - ✅ 节点重启数据恢复（恢复所有 Raft 组）
   - ✅ 网络分区处理（每个 Raft 组独立处理）
   - ✅ 数据一致性保证（每个 Raft 组内部保证）

5. **持久化**
   - ✅ 所有操作持久化到磁盘（所有 Raft 组共享 FileStorage）
   - ✅ 快照支持（每个 Raft 组独立快照）
   - ✅ 数据恢复（重启后自动恢复所有 Raft 组）

### 测试功能

5. **功能测试**
   - ✅ 基本操作正确性测试
   - ✅ 并发操作测试
   - ✅ 故障场景测试
   - ✅ 数据一致性验证

6. **性能测试**
   - ✅ 延迟测试（P50/P95/P99）
   - ✅ 吞吐量测试
   - ✅ 压力测试工具
   - ✅ 性能基准测试

7. **监控和指标**
   - ✅ 操作延迟统计
   - ✅ 吞吐量统计
   - ✅ Raft 指标（term、commit index、apply index）
   - ✅ 存储指标（磁盘使用、段数量）
   - ✅ Prometheus metrics 导出

### 优化功能

8. **性能优化**
   - ✅ 批量写入优化
   - ✅ 读操作缓存（可选）
   - ✅ 连接池管理
   - ✅ 异步操作优化

9. **可观测性**
   - ✅ 结构化日志（tracing）
   - ✅ 性能指标导出
   - ✅ 健康检查端点
   - ✅ 调试接口

## 实现计划

### Phase 1: 基础实现
- [ ] 项目结构和依赖配置
- [ ] 基本 KV 状态机实现
- [ ] gRPC/HTTP 服务器
- [ ] 与 MultiRaftDriver 集成
- [ ] 基本 PUT/GET/DELETE 操作
- [ ] **键值路由实现**（哈希路由到 Raft 组）

### Phase 2: Multi-Raft 核心功能 ⭐
- [ ] **Shard Router**：键值到 Raft 组的路由表
- [ ] **动态创建 Raft 组**：运行时创建新的 shard
- [ ] **Raft 组迁移**：
  - [ ] 数据迁移（从源节点复制到目标节点）
  - [ ] 配置变更（从旧配置迁移到新配置）
  - [ ] 路由表更新
- [ ] **Raft 组合并**：
  - [ ] 合并多个组的数据
  - [ ] 配置变更（Joint Consensus）
  - [ ] 路由表更新
- [ ] **Raft 组分裂**：
  - [ ] 分裂数据到多个新组
  - [ ] 创建新的 Raft 组
  - [ ] 路由表更新

### Phase 3: 集群功能
- [ ] 多节点集群支持
- [ ] 配置变更功能（每个 Raft 组独立）
- [ ] 节点加入/退出
- [ ] 集群状态查询
- [ ] **Raft 组状态查询**

### Phase 4: 测试工具
- [ ] 功能测试套件
- [ ] **Multi-Raft 并发测试**
- [ ] **迁移/合并/分裂测试**
- [ ] 性能测试工具
- [ ] 压测工具
- [ ] 一致性验证工具

### Phase 5: 监控和优化
- [ ] Metrics 导出（每个 Raft 组独立指标）
- [ ] 性能分析工具
- [ ] **负载均衡**：监控各组负载，自动迁移
- [ ] 性能优化
- [ ] 文档完善

## 技术栈

- **Rust**: 主要开发语言
- **raft-lite**: Raft 共识算法实现
- **tokio**: 异步运行时
- **tonic**: gRPC 框架
- **axum/warp**: HTTP 框架（可选）
- **tracing**: 日志和追踪
- **prometheus**: 指标导出（可选）

## 目录结构

```
examples/raft-kv/
├── Cargo.toml              # 项目配置
├── DESIGN.md               # 设计文档（本文件）
├── README.md               # 使用说明
├── src/
│   ├── main.rs            # 服务器入口
│   ├── server.rs          # 服务器实现
│   ├── state_machine.rs   # KV 状态机
│   ├── client.rs          # 客户端实现
│   ├── api/
│   │   ├── grpc.rs        # gRPC 服务
│   │   └── http.rs        # HTTP 服务
│   └── metrics.rs         # 指标收集
├── benches/
│   ├── throughput.rs      # 吞吐量基准测试
│   └── latency.rs         # 延迟基准测试
└── tests/
    ├── functional.rs      # 功能测试
    ├── consistency.rs     # 一致性测试
    └── performance.rs     # 性能测试
```

## 使用场景

1. **功能验证**
   ```bash
   cargo test --example raft-kv
   ```

2. **性能测试**
   ```bash
   cargo bench --example raft-kv
   ```

3. **压测**
   ```bash
   cargo run --example raft-kv --release -- --benchmark
   ```

4. **集群演示**
   ```bash
   # 启动 3 节点集群
   cargo run --example raft-kv -- --node-id node1 --port 5001
   cargo run --example raft-kv -- --node-id node2 --port 5002
   cargo run --example raft-kv -- --node-id node3 --port 5003
   ```

## 后续扩展

- [ ] 事务支持
- [ ] 二级索引
- [ ] 数据分片
- [ ] 多数据中心支持
- [ ] 备份和恢复工具

