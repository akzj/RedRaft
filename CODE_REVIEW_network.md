# Code Review: `crates/raft/src/network/mod.rs`

## 🔴 严重问题 (Critical Issues)

### 1. **消息发送循环提前退出** (Line 268)
**问题**: 成功发送一批消息后直接 `break`，导致只发送一批就退出循环
```rust
// 当前代码 (错误)
match client.send_batch(batch_requests).await {
    Ok(response) => {
        if response.get_ref().success {
            info!("Batch {} sent successfully", msg_len);
            break;  // ❌ 错误：只发送一批就退出
        }
    }
}
```
**影响**: 后续消息无法发送，导致消息丢失
**修复**: 移除 `break`，继续循环处理更多消息

### 2. **发送失败后消息丢失** (Line 264-277)
**问题**: 发送失败后没有重试机制，消息直接丢失
```rust
Err(err) => {
    error!("Failed to send batch: {}", err);
    // ❌ 消息丢失，没有重试
}
```
**影响**: 网络临时故障时消息永久丢失
**修复**: 实现重试机制或错误队列

### 3. **使用 panic/unwrap 在生产代码中** (Lines 346, 355, 358, 376)
**问题**: 
- Line 346: `assert!` 在 release 模式下可能被优化掉
- Line 355: `unwrap()` 可能 panic
- Line 358: `panic!` 会导致整个进程崩溃
- Line 376: `unwrap()` 可能 panic（dispatch 可能为 None）

**修复**: 使用 `Result` 返回错误而不是 panic

### 4. **批处理容量设置不合理** (Line 212)
**问题**: `Vec::with_capacity(batch_size / 4)` 但后面尝试填充到 `batch_size`
```rust
let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(batch_size / 4);
// 但后面尝试填充到 batch_size
while batch.len() < batch_size {
    // ...
}
```
**影响**: 可能导致多次内存重新分配，性能下降
**修复**: 使用 `batch_size` 作为初始容量

## 🟡 逻辑问题 (Logic Issues)

### 5. **批处理逻辑混乱** (Lines 214-245)
**问题**: `recv_many` 已经接收了最多 `batch_size` 条消息，但后面又尝试 `try_recv` 填充
```rust
size = rx.recv_many(&mut batch, batch_size) => {
    // 已经接收了最多 batch_size 条消息
}
// 但后面又尝试填充
while batch.len() < batch_size {
    match rx.try_recv() {
        // 这个逻辑可能永远不会执行
    }
}
```
**修复**: 简化逻辑，`recv_many` 已经处理了批处理

### 6. **节点清理逻辑不完整** (Lines 300-306)
**问题**: 清理已删除节点时，`notifies` 中的节点没有被移除
```rust
for (node_id, notify) in notifies.iter() {
    if node_map.contains_key(node_id) {
        continue;
    }
    notify.notify_one();
    // ❌ notifies 中的节点没有被移除，可能导致内存泄漏
}
```
**修复**: 移除 `notifies` 中已删除的节点

### 7. **异步块使用不当** (Lines 214-245)
**问题**: 使用 `async { ... }.await` 包装同步逻辑，增加了不必要的复杂度
**修复**: 直接使用 `tokio::select!` 和后续逻辑

## 🟢 代码质量问题 (Code Quality)

### 8. **错误处理不一致**
- 有些地方返回 `Result`，有些地方使用 `panic`
- 错误信息不够详细

### 9. **缺少文档注释**
- 关键方法缺少文档说明
- 批处理逻辑缺少注释

### 10. **资源清理**
- `start_sender` 中创建的 sender 任务没有等待机制
- 关闭时可能没有完全清理资源

## 📋 建议修复优先级

1. **P0 (立即修复)**:
   - 修复消息发送循环提前退出 (Issue #1)
   - 修复 panic/unwrap 问题 (Issue #3)
   - 修复发送失败后消息丢失 (Issue #2)

2. **P1 (高优先级)**:
   - 修复批处理逻辑 (Issue #5)
   - 修复节点清理逻辑 (Issue #6)
   - 修复批处理容量设置 (Issue #4)

3. **P2 (中优先级)**:
   - 改进错误处理
   - 添加文档注释
   - 优化资源清理

## 🔧 修复建议

### 修复 Issue #1: 消息发送循环
```rust
match client.send_batch(batch_requests).await {
    Ok(response) => {
        if response.get_ref().success {
            info!("Batch {} sent successfully", msg_len);
            // ✅ 继续循环，不要 break
        } else {
            error!("Failed to send batch: {:?}", response.get_ref().error);
            // 考虑重试或错误处理
        }
    }
    Err(err) => {
        error!("Failed to send batch: {}", err);
        // 考虑重试或错误处理
    }
}
```

### 修复 Issue #3: 移除 panic
```rust
// Line 346
pub async fn start_grpc_server(&mut self, dispatch: Arc<dyn MessageDispatcher>) -> Result<()> {
    if self.dispatch.is_some() {
        return Err(anyhow::anyhow!("gRPC server already running"));
    }
    // ...
}

// Line 355
let addr = server_addr.parse()
    .map_err(|e| anyhow::anyhow!("Invalid server address {}: {}", server_addr, e))?;

// Line 358
if let Err(e) = Server::builder()... {
    error!("Failed to start gRPC server: {}", e);
    // 可以考虑返回错误或使用错误通道通知
}

// Line 376
let dispatch = self.dispatch.as_ref()
    .ok_or_else(|| tonic::Status::internal("Dispatcher not initialized"))?
    .clone();
```

### 修复 Issue #4: 批处理容量
```rust
let mut batch: Vec<OutgoingMessage> = Vec::with_capacity(batch_size);
```

