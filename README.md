# raft-lite

一个轻量级的 Raft 共识算法实现，用 Rust 编写。

## 特性

- 完整实现 Raft 共识算法的核心功能
- 领导人选举（支持 Pre-Vote 防止网络分区干扰）
- 日志复制（支持多段日志管理和自动轮转）
- 线性一致性读（ReadIndex 和 LeaderLease）
- 安全性保证
- 集群配置变更（Joint Consensus 两阶段）
- 快照处理（文件存储，支持校验和）
- 持久化存储（统一的 FileStorage 实现）
- 异步接口设计

## 架构

该实现分为以下几个主要组件：

1. **核心状态管理**：`RaftState` 结构体负责维护 Raft 节点的核心状态和逻辑。
2. **存储接口**：`Storage` trait 定义了持久化存储的接口，`FileStorage` 提供了完整的文件存储实现。
3. **网络接口**：`Network` trait 定义了节点间通信的接口，支持 gRPC 批量消息传输。
4. **回调接口**：`RaftCallbacks` trait 定义了各种事件处理的回调方法。
5. **驱动层**：`MultiRaftDriver` 负责多 Raft 组的事件循环和调度。

## 使用示例

### 使用 FileStorage 进行持久化存储

```rust
use raft_lite::storage::{FileStorage, FileStorageOptions};
use raft_lite::{RaftId, HardState, ClusterConfig};
use std::collections::HashSet;
use std::sync::Arc;

// 创建文件存储
let options = FileStorageOptions::with_base_dir("./raft_data");
let (storage, rx) = FileStorage::new(options)?;

// 启动后台处理器（必须调用）
storage.start(rx);

// 使用存储
let raft_id = RaftId::new("group1".to_string(), "node1".to_string());

// 保存硬状态
let hard_state = HardState {
    raft_id: raft_id.clone(),
    term: 1,
    voted_for: None,
};
storage.save_hard_state(&raft_id, hard_state).await?;

// 保存集群配置
let mut voters = HashSet::new();
voters.insert(raft_id.clone());
let config = ClusterConfig::simple(voters, 1);
storage.save_cluster_config(&raft_id, config).await?;

// 追加日志条目
let entries = vec![/* ... */];
storage.append_log_entries(&raft_id, &entries).await?;

// 加载数据
let loaded_state = storage.load_hard_state(&raft_id).await?;
let loaded_config = storage.load_cluster_config(&raft_id).await?;
let loaded_entries = storage.get_log_entries(&raft_id, 1, 10).await?;
```

### 存储目录结构

```
{base_dir}/
├── logs/              # 日志段文件
│   └── segment_*.log  # 包含日志条目、硬状态、集群配置
└── snapshots/         # 快照文件
    └── {group}_{node}/
        ├── meta.json
        └── data.bin
```

### 存储特性

- **持久化**：所有数据写入磁盘，重启后自动恢复
- **多段日志**：自动轮转，支持跨段读取
- **数据完整性**：快照使用 SHA256 校验和
- **多 Raft 组**：同一存储实例支持多个 Raft 组
- **异步操作**：所有存储操作都是异步的

## 运行示例

运行 FileStorage 使用示例：

```bash
cargo run --example file_storage_example
```

该示例演示了：
- 创建和配置 FileStorage
- 保存和加载硬状态、集群配置、日志条目、快照
- 存储统计信息
- 数据持久化验证（重启后数据恢复）

## 测试

运行所有测试：

```bash
cargo test
```

运行持久化测试（验证数据恢复）：

```bash
cargo test storage_persistence
```

运行存储相关测试：

```bash
cargo test storage::
```

## 存储配置

`FileStorageOptions` 提供以下配置选项：

- `base_dir`: 存储根目录（默认：`./data`）
- `max_segment_size`: 日志段最大大小（默认：64MB）
- `max_io_threads`: I/O 线程数（默认：4）
- `sync_on_write`: 每次写入后同步到磁盘（默认：true）
- `batch_size`: 批处理大小（默认：100）
- `cache_entries_size`: 缓存条目数（默认：1000）
- `min_free_disk_space`: 最小剩余磁盘空间（默认：100MB）
- `verify_snapshot_checksum`: 验证快照校验和（默认：true）

## 文档

```bash
cargo doc --open
```

## 贡献

欢迎提交 issue 和 pull request。

## 许可证

本项目采用 MIT 许可证，详情请见 LICENSE 文件。