# RaftKV 项目结构

## 目录结构

```
raft-kv/
├── Cargo.toml              # Workspace 配置
├── DESIGN.md               # 架构设计文档
├── FEATURES.md             # 功能要点清单
├── README.md               # 使用说明
├── STRUCTURE.md            # 项目结构说明（本文件）
├── MULTI_RAFT.md          # Multi-Raft 详细设计
│
├── crates/
│   └── raft/              # Raft 共识算法库（独立 crate）
│       ├── Cargo.toml
│       ├── build.rs       # Protobuf 构建脚本
│       ├── src/
│       │   ├── lib.rs     # 库入口
│       │   ├── types.rs   # 基础类型定义
│       │   ├── event.rs   # 事件和角色定义
│       │   ├── message.rs # Raft 消息类型
│       │   ├── traits.rs  # Trait 定义
│       │   ├── error.rs   # 错误类型
│       │   ├── state/     # Raft 状态机实现
│       │   ├── network/   # 网络通信
│       │   ├── storage/   # 存储实现
│       │   ├── pipeline.rs # 管道状态管理
│       │   ├── cluster_config.rs # 集群配置
│       │   └── multi_raft_driver.rs # Multi-Raft 驱动
│       ├── proto/         # gRPC 协议定义
│       │   └── pb.proto
│       └── tests/          # Raft 库测试
│
└── raft-kv/               # 主项目（Redis 兼容的 KV 存储）
    ├── Cargo.toml
    └── src/
        ├── main.rs        # 服务器入口点
        ├── server.rs      # 服务器主逻辑
        ├── state_machine.rs # KV 状态机实现
        ├── client.rs      # 客户端实现
        ├── router.rs      # 键值路由（Multi-Raft 核心）
        ├── shard_manager.rs # Raft 组管理
        ├── metadata/      # 元数据管理
        │   ├── mod.rs
        │   ├── cluster.rs # 元数据集群
        │   └── watcher.rs # 元数据监听
        ├── api/           # API 实现
        │   ├── mod.rs
        │   ├── redis.rs   # Redis 协议实现
        │   ├── grpc.rs    # gRPC 服务实现
        │   └── http.rs    # HTTP REST API 实现
        ├── metrics.rs     # 指标收集和导出
        └── config.rs      # 配置管理
```

## 模块说明

### Raft 库 (`crates/raft/`)

独立的 Raft 共识算法实现，可以被其他项目复用。

#### 核心模块

- **`types.rs`**: 基础类型定义（RaftId, RequestId, TimerId 等）
- **`event.rs`**: 事件和角色枚举
- **`message.rs`**: Raft 消息类型（AppendEntries, RequestVote, InstallSnapshot 等）
- **`traits.rs`**: Trait 定义（Callbacks, Storage 等）
- **`error.rs`**: 错误类型定义

#### 状态机模块 (`state/`)

- **`mod.rs`**: Raft 状态机主模块
- **`election.rs`**: 选举逻辑（包括 Pre-Vote）
- **`replication.rs`**: 日志复制逻辑
- **`snapshot.rs`**: 快照处理
- **`client.rs`**: 客户端请求处理
- **`read_index.rs`**: ReadIndex 和 LeaderLease 实现
- **`leader_transfer.rs`**: Leader 转移

#### 网络模块 (`network/`)

- **`mod.rs`**: 网络通信主模块
- **`pb.rs`**: Protobuf 消息定义和转换

#### 存储模块 (`storage/`)

- **`mod.rs`**: 存储接口定义
- **`log/`**: 日志存储实现
- **`snapshot/`**: 快照存储实现
- **`file_storage.rs`**: 统一文件存储实现

#### 其他模块

- **`pipeline.rs`**: 管道状态管理，用于反馈控制
- **`cluster_config.rs`**: 集群配置管理（Joint Consensus）
- **`multi_raft_driver.rs`**: Multi-Raft 驱动，管理多个 Raft 实例

### RaftKV 主项目 (`raft-kv/`)

Redis 兼容的分布式键值存储系统实现。

#### 核心模块

##### `main.rs`
- 程序入口点
- 命令行参数解析
- 服务器启动和关闭

##### `server.rs`
- RaftKV 服务器主逻辑
- 集成 MultiRaftDriver 和 FileStorage
- **键值路由**：根据键的哈希值路由到对应的 Raft 组
- 请求路由和处理

##### `state_machine.rs`
- KV 状态机实现（每个 Raft 组独立实例）
- 将 Raft 日志应用到内存中的键值对
- 快照创建和恢复
- Redis 命令处理（GET, SET, DEL, SCAN 等）

##### `client.rs`
- 客户端实现
- 自动 Leader 发现和重定向
- 错误处理和重试

### Multi-Raft 模块

##### `router.rs`
- 键值路由实现
- 哈希算法（一致性哈希或简单取模）
- 路由表管理（创建/删除/合并/分裂时更新）
- 与元数据集群交互获取路由表

##### `shard_manager.rs`
- Raft 组生命周期管理
- 创建新 Raft 组
- 迁移 Raft 组（数据迁移 + 配置变更）
- 合并多个 Raft 组
- 分裂 Raft 组
- 负载均衡（监控负载，自动迁移）

### 元数据管理模块 (`metadata/`)

##### `cluster.rs`
- 元数据集群客户端
- 与元数据集群交互
- Shard 配置管理
- 节点信息管理
- 路由表管理

##### `watcher.rs`
- 元数据变更监听
- Watch 机制实现
- 自动同步配置变更

### API 模块 (`api/`)

##### `redis.rs`
- Redis 协议实现
- RESP (Redis Serialization Protocol) 解析
- 支持 Redis 命令（GET, SET, DEL, SCAN, MSET, MGET 等）
- 兼容 Redis 客户端

##### `grpc.rs`
- gRPC 服务定义和实现
- 使用 tonic 框架
- 处理 PUT/GET/DELETE/SCAN 请求

##### `http.rs`
- HTTP REST API 实现
- 使用 axum 框架
- 提供 RESTful 接口
- 管理 API（Shard 创建、迁移、合并、分裂）

### 工具模块

##### `metrics.rs`
- 性能指标收集（每个 Raft 组独立指标）
- Prometheus 格式导出
- 指标统计和聚合

##### `config.rs`
- 配置管理
- 命令行参数解析
- 配置文件支持

## 测试模块

### Raft 库测试 (`crates/raft/tests/`)

- **`common/`**: 测试工具和辅助函数
- **`cluster_test.rs`**: 集群测试
- **`prevote_readindex_test.rs`**: Pre-Vote 和 ReadIndex 测试
- **`storage_persistence_test.rs`**: 存储持久化测试
- **`snapshot_test.rs`**: 快照测试

### RaftKV 测试 (`raft-kv/tests/`)

- **`functional.rs`**: 基本操作测试
- **`consistency.rs`**: 线性一致性验证
- **`performance.rs`**: 性能测试
- **`redis_compatibility.rs`**: Redis 兼容性测试

## 基准测试

### `raft-kv/benches/`

- **`throughput.rs`**: 吞吐量基准测试
- **`latency.rs`**: 延迟基准测试
- **`redis_compatibility.rs`**: Redis 兼容性基准测试

## 数据目录

运行时创建的数据目录结构：

```
data/
├── node1/
│   ├── logs/              # 日志段文件
│   └── snapshots/         # 快照文件
├── node2/
│   ├── logs/
│   └── snapshots/
└── node3/
    ├── logs/
    └── snapshots/
```

## 依赖关系

```
raft-kv (workspace)
├── crates/raft/           # Raft 共识算法库
│   ├── tokio
│   ├── async-trait
│   ├── tonic (gRPC)
│   └── ...
└── raft-kv/               # 主项目
    ├── raft (path dependency)
    ├── redis (Redis 协议)
    ├── resp (RESP 协议)
    ├── axum (HTTP)
    ├── tracing (日志)
    └── prometheus (指标)
```

## 构建和运行

### 开发模式

```bash
# 构建整个 workspace
cargo build

# 只构建 RaftKV
cargo build -p raft-kv

# 只构建 Raft 库
cargo build -p raft
```

### 发布模式

```bash
cargo build --release
cargo run --release -p raft-kv
```

### 测试

```bash
# 运行所有测试
cargo test

# 运行 Raft 库测试
cargo test -p raft

# 运行 RaftKV 测试
cargo test -p raft-kv
```

### 基准测试

```bash
cargo bench
```
