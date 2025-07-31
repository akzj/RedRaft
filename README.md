# raft-lite

一个轻量级的 Raft 共识算法实现，用 Rust 编写。

## 特性

- 完整实现 Raft 共识算法的核心功能
- 领导人选举
- 日志复制
- 安全性保证
- 集群配置变更
- 快照处理
- 异步接口设计

## 架构

该实现分为以下几个主要组件：

1. **核心状态管理**：`RaftState` 结构体负责维护 Raft 节点的核心状态和逻辑。
2. **存储接口**：`Storage` trait 定义了持久化存储的接口，`MemoryStorage` 提供了一个基于内存的实现。
3. **网络接口**：`Network` trait 定义了节点间通信的接口，`MockNetwork` 提供了一个用于测试的模拟实现。
4. **回调接口**：`RaftCallbacks` trait 定义了各种事件处理的回调方法。
5. **驱动层**：`RaftDriver` 负责事件循环和调度。

## 使用示例

```rust
// 示例代码位于 examples/simple.rs
use raft_lite::{memory_storage::MemoryStorage, mock_network::MockNetwork, RaftState, RaftCallbacks};
use std::sync::Arc;

// 创建网络和存储
let network = Arc::new(MockNetwork::new());
let storage = Arc::new(MemoryStorage::new());

// 创建节点 ID 和回调
let node_id = "node1".to_string();
let callbacks = Arc::new(YourCallbacks::new(network.clone(), storage.clone()));

// 创建 Raft 状态
let mut raft_state = RaftState::new(
    node_id.clone(),
    vec!["node2".to_string(), "node3".to_string()],
    storage.clone(),
    network.clone(),
    150,    // 选举超时最小值
    300,    // 选举超时最大值
    50,     // 心跳间隔
    10,     // 应用间隔
    callbacks.clone(),
);

// 处理事件
// ...
```

## 运行示例

```bash
cargo run --example simple
```

## 测试

```bash
cargo test
```

## 文档

```bash
cargo doc --open
```

## 贡献

欢迎提交 issue 和 pull request。

## 许可证

本项目采用 MIT 许可证，详情请见 LICENSE 文件。