# 分片分裂快照传输细节设计

## 1. 快照复用与连贯性保证

### 1.1 复用Raft快照机制
- **设计原则**：严格复用Raft协议生成的标准快照，确保分裂过程中快照与后续日志的连贯性
- **实现机制**：
  ```rust
  // 扩展Raft状态机接口，支持生成带slot范围的快照
  trait StateMachine {
      // Raft标准快照生成方法
      fn create_snapshot(&self) -> Result<Snapshot, Error>;
      
      // 分裂专用：从标准快照中提取指定slot范围
      fn create_split_snapshot(
          &self, 
          base_snapshot: &Snapshot,  // 复用的Raft标准快照
          slot_range: (u32, u32)     // 目标slot范围
      ) -> Result<SplitSnapshot, Error>;
  }
  ```

### 1.2 快照元数据设计
- **设计**：在快照中添加明确的索引和slot信息，确保连贯性
- **实现**：
  ```rust
  // 分裂专用快照结构
  struct SplitSnapshot {
      // 基础快照元数据（确保与Raft日志连贯性）
      raft_index: u64,       // 对应Raft日志索引
      raft_term: u64,        // 对应Raft任期
      
      // 分裂相关元数据
      source_shard_id: ShardId,  // 源分片ID
      target_shard_id: ShardId,  // 目标分片ID
      slot_range: (u32, u32),    // 包含的slot范围
      
      // 数据部分
      data: Vec<ShardEntry>,     // 仅包含目标slot范围的数据
      
      // 校验信息
      checksum: u32,             // CRC32校验和
      timestamp: u64,            // 生成时间戳
  }
  ```

## 2. Slot范围提取与快照生成

### 2.1 快照解析与重生成流程
- **设计**：从完整快照中解析数据，按slot范围过滤后重新生成分裂专用快照
- **实现流程**：
  ```
  ┌─────────────────────────────────────────────────────────┐
  │  1. 获取最新Raft快照（base_snapshot）                   │
  └─────────────┬───────────────────────────────────────────┘
                │
  ┌─────────────▼───────────────────────────────────────────┐
  │  2. 解析base_snapshot，提取所有键值对                   │
  └─────────────┬───────────────────────────────────────────┘
                │
  ┌─────────────▼───────────────────────────────────────────┐
  │  3. 根据split_key计算目标slot范围：[split_key, 8192)   │
  └─────────────┬───────────────────────────────────────────┘
                │
  ┌─────────────▼───────────────────────────────────────────┐
  │  4. 过滤键值对：仅保留slot在目标范围内的数据           │
  └─────────────┬───────────────────────────────────────────┘
                │
  ┌─────────────▼───────────────────────────────────────────┐
  │  5. 重新生成SplitSnapshot，包含过滤后的数据和元数据     │
  └─────────────┬───────────────────────────────────────────┘
                │
  ┌─────────────▼───────────────────────────────────────────┐
  │  6. 对SplitSnapshot进行压缩和校验                       │
  └─────────────────────────────────────────────────────────┘
  ```

### 2.2 高效的slot过滤算法
- **设计**：利用键到slot的映射关系，高效过滤数据
- **实现**：
  ```rust
  // 键到slot的映射函数
  fn key_to_slot(key: &[u8]) -> u32 {
      // 使用Raft标准的CRC32算法
      let crc = crc32::checksum_ieee(key);
      crc % 8192
  }
  
  // 快照数据过滤函数
  fn filter_snapshot_by_slot(
      base_snapshot: &Snapshot, 
      slot_range: (u32, u32)
  ) -> Vec<ShardEntry> {
      let mut filtered = Vec::new();
      
      // 遍历快照中的所有键值对
      for entry in &base_snapshot.data {
          let slot = key_to_slot(&entry.key);
          // 检查slot是否在目标范围内
          if slot >= slot_range.0 && slot < slot_range.1 {
              filtered.push(entry.clone());
          }
      }
      
      filtered
  }
  ```

## 3. 快照保护机制

### 3.1 防止快照被意外删除
- **设计**：为分裂过程中的快照添加保护标记，避免被Raft的快照清理机制删除
- **实现**：
  ```rust
  // 快照管理结构
  struct SnapshotManager {
      snapshots: Vec<Snapshot>,           // 所有快照
      protected_snapshots: HashSet<u64>,  // 受保护的快照索引集合
  }
  
  impl SnapshotManager {
      // 保护指定索引的快照
      fn protect_snapshot(&mut self, index: u64) {
          self.protected_snapshots.insert(index);
      }
      
      // 取消保护
      fn unprotect_snapshot(&mut self, index: u64) {
          self.protected_snapshots.remove(&index);
      }
      
      // 清理快照时跳过受保护的快照
      fn cleanup_snapshots(&mut self, keep_min_index: u64) {
          self.snapshots.retain(|s| {
              s.index >= keep_min_index || self.protected_snapshots.contains(&s.index)
          });
      }
  }
  ```

### 3.2 分裂快照的生命周期管理
- **设计**：为每个分裂任务创建快照保护上下文，任务完成后自动解除保护
- **实现**：
  ```rust
  // 分裂任务上下文
  struct SplitContext {
      task_id: String,
      protected_snapshot_index: u64,  // 受保护的快照索引
      // 其他上下文信息...
  }
  
  impl Drop for SplitContext {
      // 任务完成或取消时自动解除保护
      fn drop(&mut self) {
          snapshot_manager.unprotect_snapshot(self.protected_snapshot_index);
      }
  }
  ```

## 4. 快照传输目标节点选择

### 4.1 目标节点选择策略
- **设计原则**：快照始终先发送给目标Shard的Raft Leader，利用Raft协议自身的日志复制机制同步到其他节点
- **实现机制**：
  ```rust
  // 分裂协调器中的目标Leader选择
  fn select_target_leader(
      &self, 
      target_shard_id: &ShardId
  ) -> Result<NodeId, Error> {
      // 获取目标Shard的当前Leader
      let target_shard = self.shard_manager.get_shard(target_shard_id)?;
      target_shard.get_leader()
  }
  ```

### 4.2 快照同步到所有目标节点
- **设计**：利用目标Shard自身的Raft协议，将收到的分裂快照同步到所有节点
- **实现流程**：
  ```
  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
  │  源Shard Leader │────▶│ 目标Shard Leader│────▶│ 目标Shard Follower1 │
  └─────────────────┘     └─────────────────┘     └─────────────────┘
                              │
                              ▼
                        ┌─────────────────┐
                        │ 目标Shard Follower2 │
                        └─────────────────┘
  ```

## 5. 外部快照加载机制

### 5.1 目标Shard接收快照流程
- **设计**：扩展Raft协议，支持接收和处理外部分裂快照
- **实现**：
  ```rust
  // 扩展Raft节点接口
  trait RaftNode {
      // 标准Raft接口
      fn append_entries(&self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse, Error>;
      fn request_vote(&self, req: RequestVoteRequest) -> Result<RequestVoteResponse, Error>;
      
      // 分裂专用：接收外部分裂快照
      fn install_split_snapshot(
          &self, 
          req: InstallSplitSnapshotRequest
      ) -> Result<InstallSplitSnapshotResponse, Error>;
  }
  ```

### 5.2 分裂快照请求结构
- **设计**：定义专门的分裂快照安装请求，包含必要的元数据和数据
- **实现**：
  ```rust
  // 分裂快照安装请求
  struct InstallSplitSnapshotRequest {
      // Raft基础字段，确保与后续日志连贯性
      term: u64,              // 源Shard的当前任期
      leader_id: NodeId,      // 源Shard Leader ID
      
      // 快照元数据
      snapshot_index: u64,    // 快照对应的Raft索引
      snapshot_term: u64,     // 快照对应的Raft任期
      
      // 分裂专用字段
      source_shard_id: ShardId,  // 源分片ID
      target_shard_id: ShardId,  // 目标分片ID
      slot_range: (u32, u32),    // 快照包含的slot范围
      
      // 数据部分（分块传输）
      chunk_id: u64,          // 当前块ID
      total_chunks: u64,      // 总块数
      data: Vec<u8>,          // 压缩后的块数据
      checksum: u32,          // 块校验和
      done: bool,             // 是否为最后一块
  }
  ```

### 5.3 外部快照加载流程
- **设计**：目标Shard Leader接收分裂快照后，按Raft协议流程处理并同步到所有节点
- **实现流程**：
  
  #### 阶段1：目标Leader接收快照
  1. **接收验证**：验证快照元数据、校验和和完整性
  2. **临时存储**：将接收到的快照块存储到临时目录
  3. **完整性检查**：所有块接收完成后，验证完整快照的完整性
  
  #### 阶段2：目标Leader应用快照
  4. **状态检查**：确保当前Raft日志索引小于快照索引（避免覆盖新数据）
  5. **应用快照**：调用状态机的特殊接口加载分裂快照
  6. **更新硬状态**：更新Raft硬状态（term, vote, commit_index）
  7. **清理旧日志**：删除快照索引之前的所有日志
  
  #### 阶段3：同步到目标Follower节点
  8. **发起InstallSnapshot**：目标Leader向所有Follower发送标准InstallSnapshot RPC
  9. **Follower应用**：Follower接收并应用快照，更新自身状态
  10. **确认完成**：所有Follower确认快照安装完成

### 5.4 状态机外部快照加载接口
- **设计**：扩展状态机接口，支持加载外部分裂快照
- **实现**：
  ```rust
  // 状态机外部快照加载接口
  trait StateMachine {
      // 加载分裂快照的特殊方法
      fn apply_split_snapshot(
          &mut self, 
          snapshot: &SplitSnapshot
      ) -> Result<(), Error>;
      
      // 验证快照兼容性
      fn verify_split_snapshot(
          &self, 
          snapshot: &SplitSnapshot
      ) -> Result<(), Error>;
  }
  
  // 具体实现示例
  impl StateMachine for MyStateMachine {
      fn apply_split_snapshot(
          &mut self, 
          snapshot: &SplitSnapshot
      ) -> Result<(), Error> {
          // 1. 验证快照与当前状态的兼容性
          self.verify_split_snapshot(snapshot)?;
          
          // 2. 清空目标slot范围的现有数据
          self.clear_slot_range(snapshot.slot_range)?;
          
          // 3. 加载快照中的数据
          for entry in &snapshot.data {
              self.apply_entry(entry)?;
          }
          
          // 4. 更新状态机元数据
          self.last_applied_split_snapshot = Some(snapshot.raft_index);
          
          Ok(())
      }
  }
  ```

## 6. 快照传输可靠性保证

### 6.1 分块传输与重试机制
- **设计**：将大快照分块传输，支持断点续传和重试
- **实现**：
  ```rust
  // 快照传输管理器
  struct SnapshotTransferManager {
      task_id: String,
      source_node: NodeId,
      target_node: NodeId,
      snapshot: SplitSnapshot,
      chunks: Vec<SnapshotChunk>,
      sent_chunks: HashSet<u64>,      // 已发送成功的块ID
      retry_count: HashMap<u64, u32>, // 块重试计数
  }
  
  impl SnapshotTransferManager {
      // 发送快照块
      async fn send_chunk(&mut self, chunk_id: u64) -> Result<(), Error> {
          let chunk = &self.chunks[chunk_id as usize];
          let req = InstallSplitSnapshotRequest {
              // ... 填充请求字段 ...
              chunk_id,
              data: chunk.data.clone(),
              // ...
          };
          
          // 发送请求并处理响应
          let resp = self.target_node.send(req).await?;
          if resp.success {
              self.sent_chunks.insert(chunk_id);
              self.retry_count.remove(&chunk_id);
          } else {
              // 处理失败，增加重试计数
              let count = self.retry_count.entry(chunk_id).or_insert(0);
              *count += 1;
              if *count > MAX_RETRIES {
                  return Err(Error::MaxRetriesExceeded);
              }
          }
          
          Ok(())
      }
      
      // 断点续传逻辑
      async fn resume_transfer(&mut self) -> Result<(), Error> {
          // 发送所有未成功的块
          for (i, _) in self.chunks.iter().enumerate() {
              let chunk_id = i as u64;
              if !self.sent_chunks.contains(&chunk_id) {
                  self.send_chunk(chunk_id).await?;
              }
          }
          Ok(())
      }
  }
  ```

### 6.2 传输进度跟踪
- **设计**：实时跟踪快照传输进度，支持暂停和恢复
- **实现**：
  ```rust
  // 快照传输进度
  struct SnapshotTransferProgress {
      total_bytes: u64,       // 快照总字节数
      sent_bytes: u64,        // 已发送字节数
      total_chunks: u64,      // 总分块数
      sent_chunks: u64,       // 已发送分块数
      status: TransferStatus, // 当前状态
      error: Option<Error>,   // 错误信息（如果有）
  }
  ```

## 7. 异常处理与恢复

### 7.1 快照传输失败处理
- **设计**：处理快照传输过程中的各种异常情况
- **实现策略**：
  ```rust
  // 快照传输异常处理
  async fn handle_transfer_error(
      manager: &mut SnapshotTransferManager,
      error: Error
  ) -> Result<(), Error> {
      match error {
          // 网络错误：重试
          Error::Network(_) => {
              // 等待一段时间后重试
              tokio::time::sleep(Duration::from_secs(1)).await;
              manager.resume_transfer().await
          },
          
          // 目标节点错误：切换到其他节点
          Error::TargetNodeUnavailable => {
              // 选择新的目标Leader
              let new_target = select_new_target_leader(manager.target_shard_id).await?;
              manager.target_node = new_target;
              // 重置状态并重试
              manager.reset_transfer_state();
              manager.start_transfer().await
          },
          
          // 其他致命错误：返回失败
          _ => Err(error),
      }
  }
  ```

### 7.2 快照应用失败处理
- **设计**：处理目标Shard应用快照失败的情况
- **实现策略**：
  1. **验证失败**：检查快照格式、版本、slot范围等，返回具体错误
  2. **兼容性失败**：当前状态与快照不兼容时，清理临时数据并回滚
  3. **部分失败**：单个Follower应用失败时，重试或替换该节点

## 8. 完整快照传输流程示例

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  源Shard Leader │     │ 源Shard State   │     │ 目标Shard Leader│
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │ 1. 获取最新Raft快照   │                       │
         ├──────────────────────▶│                       │
         │                       │                       │
         │ 2. 提取目标slot范围   │                       │
         ├──────────────────────▶│                       │
         │                       │                       │
         │ 3. 生成分裂快照       │                       │
         │◀──────────────────────┘                       │
         │                                               │
         │ 4. 保护快照不被删除    │                       │
         ├───────────────────────────────────────────────▶
         │                                               │
         │ 5. 分块发送快照        │                       │
         ├───────────────────────────────────────────────▶
         │                                               │
         │ 6. 接收并验证快照块    │                       │
         │◀───────────────────────────────────────────────┤
         │                                               │
         │ 7. 快照完整，开始应用  │                       │
         │◀───────────────────────────────────────────────┤
         │                                               │
         │ 8. 同步到所有Follower │                       │
         │◀───────────────────────────────────────────────┤
         │                                               │
         │ 9. 所有节点应用完成    │                       │
         │◀───────────────────────────────────────────────┤
         │                                               │
         │ 10. 解除快照保护       │                       │
         └───────────────────────────────────────────────▶
```

## 总结

本设计方案详细解决了分片分裂过程中的快照传输问题，主要特点包括：

1. **严格复用Raft快照**：确保快照与后续日志的连贯性，避免数据不一致
2. **高效的slot范围提取**：从完整快照中过滤出目标slot范围的数据，重新生成分裂专用快照
3. **可靠的快照保护机制**：通过保护标记防止分裂快照被意外删除，同时支持自动清理
4. **合理的目标节点选择**：快照始终发送给目标Shard的Leader，利用Raft协议同步到所有节点
5. **完整的外部快照加载流程**：支持分块传输、断点续传、完整性验证和可靠应用
6. **全面的异常处理**：覆盖传输失败、应用失败等各种异常情况，确保分裂过程的可靠性

该设计确保了分片分裂过程中快照传输的可靠性、连贯性和效率，为后续的增量日志追赶和路由切换奠定了坚实基础。