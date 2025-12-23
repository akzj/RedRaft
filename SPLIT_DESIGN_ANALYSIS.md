# Split 设计问题分析

## 当前设计流程

```
1. Leader 收到 SplitSlotsRange 请求
2. Leader 立即在内存中阻塞目标 slot range 的写入（本地操作）
3. Leader 提交 SplitSlotsRange 命令到 Raft
4. 等待 SplitSlotsRange 命令 apply
5. Apply 完成后，说明之前的所有日志都已 apply
6. 此时新的 region B 可以安全读写了
7. 解除阻塞，将请求路由到新的 region B 的 state_machine
```

## 🔴 严重问题

### 1. Leader 失败后的状态恢复问题 ✅ **已解决**

**解决方案**：如果 leader 失败，整个 PrepareSplit 操作失败，Pilot 重试
- Leader 在内存中阻塞写入（本地操作）
- Leader 提交 noop 命令并等待 apply
- 如果 leader 失败，操作失败，Pilot 重试
- 新 Leader 重新执行 PrepareSplit，重新阻塞写入

**优势**：
- 不需要 BlockWrites 命令，更简单
- Leader 失败时，操作失败，Pilot 重试，保证一致性
- 利用 Pilot 的重试机制处理失败场景

### 2. Follower 的写入问题

**问题场景**：
- 虽然通常客户端连接到 Leader，但如果 Follower 收到写入请求（比如读请求转发、错误路由等）
- Follower 不知道需要阻塞，可能继续处理写入

**影响**：
- 通常不是问题，因为 Follower 不应该处理写入
- 但如果实现有 bug，可能导致问题

**解决方案**：
- 确保 Follower 拒绝写入请求（返回 redirect 或错误）
- 在路由层检查，确保只有 Leader 处理写入

## 🟡 重要问题

### 3. 阻塞状态的持久化

**问题**：
- 内存阻塞状态没有持久化
- 如果节点重启，阻塞状态丢失
- Split 操作可能处于中间状态

**影响**：
- 如果 split 操作很快完成（秒级），重启概率不大
- 但如果 split 操作很慢，重启后状态丢失

**解决方案**：
- Split 状态应该存储在 Raft 日志中（通过命令）
- 节点重启后，从日志中恢复 split 状态

### 4. 路由表更新的时机

**问题**：
- SplitSlotsRange apply 后，所有节点都会更新路由表
- 但在 apply 之前，路由表可能不一致
- 客户端可能路由到错误的节点

**影响**：
- 在 split 过程中，可能有短暂的 routing 不一致
- 客户端可能收到 MOVED 重定向

**解决方案**：
- 这是可以接受的，客户端应该处理 MOVED 重定向
- 或者使用两阶段：先更新路由表，再启用 target Raft group

### 5. 阻塞请求的处理

**问题**：
- 如果 Leader 失败，所有被阻塞的请求都会失败
- 客户端需要重试，但可能不知道应该重试到哪个节点

**影响**：
- 用户体验：请求失败，需要重试
- 但这是可以接受的，因为 split 操作应该很快

**解决方案**：
- 返回明确的错误信息（如 "LEADER_LOST"）
- 客户端可以重试，新的 Leader 会处理

## 🟢 可以接受的问题

### 6. 性能影响

**问题**：
- 阻塞写入会影响性能
- 但 split 操作应该很快完成（秒级）

**影响**：
- 可以接受，因为 split 操作不频繁

### 7. 并发 Split

**问题**：
- 如果同时有多个 split 操作，可能冲突
- 需要确保不会同时 split 同一个 slot range

**影响**：
- Pilot 应该协调，避免冲突
- 可以在 Raft 命令中检查冲突

## 推荐的改进方案

### 方案：在 SplitSlotsRange 命令中包含阻塞信息

**改进后的流程**：
```
1. Leader 收到 SplitSlotsRange 请求
2. Leader 立即提交 SplitSlotsRange 命令到 Raft（命令中包含阻塞信息）
3. Leader 在内存中阻塞写入（本地操作，快速响应）
4. 等待 SplitSlotsRange 命令 apply
5. 所有节点 apply 时，都会阻塞写入（通过命令中的信息）
6. Apply 完成后，说明之前的所有日志都已 apply
7. 此时新的 region B 可以安全读写了
8. 解除阻塞，将请求路由到新的 region B 的 state_machine
```

**关键改进**：
- SplitSlotsRange 命令中包含阻塞信息（slot_start, slot_end）
- 所有节点在 apply 时都会阻塞写入
- 即使 Leader 失败，新 Leader 也会从日志中恢复阻塞状态

**优势**：
- 保持简单性（仍然是一个命令）
- 通过 Raft 保证所有节点都知道需要阻塞
- Leader 失败后，新 Leader 可以从日志恢复状态

## 总结

### 必须解决的问题
1. ✅ **Leader 失败后的状态恢复**：在命令中包含阻塞信息，apply 时阻塞

### 需要注意的问题
2. ✅ **Follower 的写入**：确保 Follower 拒绝写入
3. ✅ **阻塞状态的持久化**：通过 Raft 命令持久化

### 可以接受的问题
4. ✅ **路由表更新的时机**：客户端处理 MOVED 重定向
5. ✅ **阻塞请求的处理**：返回明确错误，客户端重试

