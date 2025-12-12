# gRPC Server 共享示例

## 概述

业务层可以创建 gRPC Server，然后添加 Raft 服务和业务服务，共享同一个端口。这样既复用了基础设施，又保持了架构清晰。

## 使用方法

### 1. 在业务层创建 Server

```rust
use tonic::transport::Server;
use raft::network::MultiRaftNetwork;

// 创建网络层
let network_options = MultiRaftNetworkOptions::default()
    .with_node_id("node1".to_string())
    .with_grpc_addr("127.0.0.1:50051".to_string());
let mut network = Arc::new(MultiRaftNetwork::new(network_options));

// 设置 dispatcher（处理 Raft 消息）
network.set_dispatcher(dispatch);

// 获取 Raft 服务
let raft_service = network.get_raft_service();

// 创建业务服务（示例）
// let business_service = BusinessServiceServer::new(business_impl);

// 创建并启动 gRPC Server
let server_addr = "127.0.0.1:50051".parse()?;
tokio::spawn(async move {
    Server::builder()
        .add_service(raft_service)           // Raft 服务
        // .add_service(business_service)     // 业务服务（如快照传输）
        .serve(server_addr)
        .await
        .expect("gRPC server failed");
});
```

### 2. 优势

- **共享端口**：Raft 和业务服务使用同一个端口，简化部署
- **不修改 Raft 代码**：Raft Network 提供 `get_raft_service()` 方法，业务层自己管理 Server
- **向后兼容**：原有的 `start_grpc_server()` 方法仍然可用
- **灵活扩展**：业务层可以添加任意数量的服务

### 3. 完整示例

```rust
use std::sync::Arc;
use tonic::transport::Server;
use raft::network::{MultiRaftNetwork, MultiRaftNetworkOptions, MessageDispatcher};
use anyhow::Result;

// 实现 MessageDispatcher
struct MyDispatcher;
#[async_trait::async_trait]
impl MessageDispatcher for MyDispatcher {
    async fn dispatch(&self, msg: OutgoingMessage) -> Result<()> {
        // 处理 Raft 消息
        Ok(())
    }
}

async fn setup_grpc_server() -> Result<()> {
    // 创建网络层
    let network_options = MultiRaftNetworkOptions::default()
        .with_node_id("node1".to_string())
        .with_grpc_addr("127.0.0.1:50051".to_string());
    let mut network = Arc::new(MultiRaftNetwork::new(network_options));
    
    // 设置 dispatcher
    let dispatch = Arc::new(MyDispatcher);
    network.set_dispatcher(dispatch);
    
    // 获取 Raft 服务
    let raft_service = network.get_raft_service();
    
    // 创建业务服务（假设已定义）
    // let business_service = BusinessServiceServer::new(business_impl);
    
    // 启动 Server
    let addr = "127.0.0.1:50051".parse()?;
    Server::builder()
        .add_service(raft_service)
        // .add_service(business_service)  // 添加业务服务
        .serve(addr)
        .await?;
    
    Ok(())
}
```

## 注意事项

1. **Dispatcher 必须设置**：在调用 `get_raft_service()` 之前，必须先调用 `set_dispatcher()`
2. **Server 启动**：业务层负责启动 Server，可以添加 shutdown 逻辑
3. **向后兼容**：如果不需要添加业务服务，仍然可以使用 `start_grpc_server()` 方法

## 与独立 Channel 结合

如果需要为业务操作（如快照传输）创建独立的 gRPC Channel（避免影响 Raft 消息），可以：

1. **共享 Server**：使用上述方法共享 Server
2. **独立 Channel**：为业务操作创建独立的 gRPC Client Channel

```rust
// 为业务操作创建独立的 Channel
let business_endpoint = Endpoint::from_shared(target_addr)?
    .timeout(Duration::from_secs(300))  // 长超时
    .connect_timeout(Duration::from_secs(10));
let business_channel = business_endpoint.connect().await?;
let business_client = BusinessServiceClient::new(business_channel);

// 使用独立的 Channel 进行快照传输，不影响 Raft Channel
```

这样既共享了 Server 基础设施，又通过独立 Channel 实现了带宽隔离。

