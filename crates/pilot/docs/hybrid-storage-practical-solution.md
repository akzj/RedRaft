# 混合存储架构的快照现实解决方案

## 核心挑战总结

### 1. 跨引擎一致性地狱
```rust
// 用户视角的简单事务
MULTI
SET user:1:name "Alice"          // RocksDB引擎  
LPUSH user:1:posts "hello"       // 内存List引擎
ZADD active_users 100 user:1     // 内存ZSet引擎  
EXEC

// 快照时的噩梦场景：
// T1: String引擎快照完成 ✅
// T2: List引擎快照中... ⏳  
// T3: ZSet引擎快照失败 ❌
// 结果：数据不一致！Alice有名字但没帖子
```

### 2. 分片分裂的多维度灾难
```rust
// slot 1000-2000分裂时需要：
// 1. RocksDB扫描所有key，筛选slot范围
// 2. 内存引擎遍历所有list，检查slot
// 3. ZSet序列化跳跃表，同时筛选slot
// 4. Stream处理日志分段，保持消息ID连续
// 5. 所有操作必须在同一时间点完成！
```

## 现实可行的解决方案

### 方案1：分层一致性模型（推荐）

```rust
/// 三层次一致性模型 - 根据业务需求选择
pub enum ConsistencyLevel {
    Strict,      // 严格一致：所有引擎同一时间点（性能差，用于关键业务）
    Bounded,     // 有界一致：允许毫秒级差异（推荐，平衡性能）  
    Eventual,    // 最终一致：通过后台修复（高性能，用于非关键数据）
}

impl HybridSnapshotCoordinator {
    async fn create_snapshot(&self, level: ConsistencyLevel) -> Result<HybridSnapshot, Error> {
        match level {
            ConsistencyLevel::Strict => self.create_strict_snapshot().await,
            ConsistencyLevel::Bounded => self.create_bounded_snapshot(Duration::from_millis(10)).await,
            ConsistencyLevel::Eventual => self.create_eventual_snapshot().await,
        }
    }
    
    /// 有界一致性 - 现实可行
    async fn create_bounded_snapshot(&self, max_diff: Duration) -> Result<HybridSnapshot, Error> {
        let start_time = Instant::now();
        
        // 1. 并行创建各引擎快照（不阻塞写操作）
        let mut futures = Vec::new();
        for (engine_type, engine) in &self.engines {
            let future = self.create_engine_snapshot_async(engine.clone());
            futures.push((engine_type, future));
        }
        
        // 2. 收集结果，检查时间差异
        let mut engine_snapshots = HashMap::new();
        for (engine_type, future) in futures {
            let (snapshot, timestamp) = future.await?;
            engine_snapshots.insert(engine_type, (snapshot, timestamp));
        }
        
        // 3. 验证时间差异是否在允许范围内
        let timestamps: Vec<_> = engine_snapshots.values()
            .map(|(_, ts)| *ts)
            .collect();
        let max_timestamp = timestamps.iter().max().unwrap();
        let min_timestamp = timestamps.iter().min().unwrap();
        
        if max_timestamp - min_timestamp > max_diff {
            return Err(SnapshotError::InconsistentTiming {
                max_diff: max_timestamp - min_timestamp,
                allowed_diff: max_diff,
            });
        }
        
        Ok(HybridSnapshot {
            engine_snapshots: engine_snapshots.into_iter()
                .map(|(k, (v, _))| (k, v))
                .collect(),
            consistency_level: ConsistencyLevel::Bounded,
            max_time_diff: max_timestamp - min_timestamp,
        })
    }
}
```

### 方案2：渐进式混合存储迁移

避免一次性全量迁移，采用渐进式策略：

```rust
/// 渐进式迁移策略
pub struct ProgressiveHybridMigration {
    phase: MigrationPhase,
    
    // 阶段1：按命令类型路由（保守）
    command_router: HashMap<CommandType, EngineType>,
    
    // 阶段2：按访问模式路由（优化）
    access_tracker: AccessPatternTracker,
    
    // 阶段3：按数据特性路由（智能）
    data_classifier: DataFeatureClassifier,
}

impl ProgressiveHybridMigration {
    /// 阶段1：保守的类型路由
    fn route_by_command_type(&self, cmd: &Command) -> EngineType {
        match cmd {
            // 字符串操作 -> RocksDB（安全的选择）
            Command::Set(_, _) | Command::Get(_) | Command::Mset(_) => {
                EngineType::StringRocksDB
            }
            
            // List操作 -> 内存（保持现状）
            Command::Lpush(_, _) | Command::Lpop(_) | Command::Lrange(_, _, _) => {
                EngineType::ListMemory
            }
            
            // ZSet操作 -> 内存（保持现状）
            Command::Zadd(_, _) | Command::Zrange(_, _, _) | Command::Zrem(_, _) => {
                EngineType::ZSetMemory
            }
            
            // Hash操作 -> RocksDB（安全的选择）
            Command::Hset(_, _, _) | Command::Hget(_, _) | Command::Hgetall(_) => {
                EngineType::HashRocksDB
            }
            
            // Stream操作 -> 专用磁盘存储
            Command::Xadd(_, _) | Command::Xread(_, _) | Command::Xgroup(_, _, _) => {
                EngineType::StreamDisk
            }
            
            _ => EngineType::StringRocksDB, // 默认保守选择
        }
    }
    
    /// 阶段2：基于访问模式的智能路由
    fn route_by_access_pattern(&self, key: &[u8], cmd: &Command) -> EngineType {
        let access_info = self.access_tracker.get_pattern(key);
        
        match access_info {
            // 高频写 + 小数据 -> 内存
            Pattern { frequency: High, avg_size: Small, write_ratio: >0.8 } => {
                EngineType::ListMemory
            }
            
            // 低频读 + 大数据 -> 磁盘
            Pattern { frequency: Low, avg_size: Large, write_ratio: <0.2 } => {
                EngineType::StringRocksDB
            }
            
            // 中频读写 + 中等数据 -> 根据类型决定
            Pattern { frequency: Medium, avg_size: Medium, .. } => {
                self.route_by_command_type(cmd)
            }
            
            _ => self.route_by_command_type(cmd), // 回退到保守策略
        }
    }
}
```

### 方案3：引擎特化的增量快照

```rust
/// 各引擎独立的增量策略trait
pub trait EngineIncrementalStrategy {
    /// 获取自base以来的增量数据
    fn get_incremental_delta(&self, base: &SnapshotMetadata) -> Result<EngineDelta, Error>;
    
    /// 验证增量数据的完整性
    fn validate_delta(&self, delta: &EngineDelta) -> Result<bool, Error>;
}

/// RocksDB增量：基于WAL sequence
impl EngineIncrementalStrategy for StringRocksDBEngine {
    fn get_incremental_delta(&self, base: &SnapshotMetadata) -> Result<EngineDelta, Error> {
        let current_sequence = self.db.get_latest_sequence_number()?;
        let base_sequence = base.wal_sequence.context("No WAL sequence")?;
        
        // 只获取WAL变更，O(Δ)复杂度
        let wal_changes = self.db.get_wal_changes(base_sequence, current_sequence)?;
        
        Ok(EngineDelta::RocksDBWAL { 
            start_sequence: base_sequence,
            end_sequence: current_sequence,
            changes: wal_changes,
            estimated_size: wal_changes.iter().map(|c| c.size()).sum(),
        })
    }
}

/// 内存增量：基于操作日志
impl EngineIncrementalStrategy for ListMemoryEngine {
    fn get_incremental_delta(&self, base: &SnapshotMetadata) -> Result<EngineDelta, Error> {
        let current_position = self.op_log.current_position()?;
        let base_position = base.op_log_position.context("No op log position")?;
        
        // 只获取操作日志，O(Δ)复杂度
        let operations = self.op_log.get_operations_since(base_position)?;
        
        Ok(EngineDelta::MemoryOpLog {
            start_position: base_position,
            end_position: current_position,
            operations,
            affected_keys: operations.iter().map(|op| op.key.clone()).collect(),
        })
    }
}

/// ZSet增量：基于跳跃表版本
impl EngineIncrementalStrategy for ZSetMemoryEngine {
    fn get_incremental_delta(&self, base: &SnapshotMetadata) -> Result<EngineDelta, Error> {
        // ZSet的特殊性：需要同时考虑字典和跳跃表
        let dict_delta = self.dict.get_delta_since(base.dict_version)?;
        let zsl_delta = self.zsl.get_delta_since(base.zsl_version)?;
        
        Ok(EngineDelta::ZSetIncremental {
            dict_changes: dict_delta,
            zsl_changes: zsl_delta,
            affected_keys: self.get_affected_keys(&dict_delta, &zsl_delta)?,
        })
    }
}
```

### 方案4：统一快照恢复协议

```rust
/// 混合快照恢复协调器
pub struct HybridSnapshotRestoreCoordinator {
    engines: HashMap<EngineType, Arc<dyn Engine>>,
    restore_order: Vec<EngineType>, // 恢复顺序（解决依赖关系）
}

impl HybridSnapshotRestoreCoordinator {
    /// 统一的快照恢复
    async fn restore_hybrid_snapshot(&self, snapshot: HybridSnapshot) -> Result<(), RestoreError> {
        info!("Starting hybrid snapshot restore: {:?}", snapshot.summary());
        
        // 1. 验证快照完整性
        self.verify_snapshot_integrity(&snapshot).await?;
        
        // 2. 按依赖顺序恢复各引擎
        for engine_type in &self.restore_order {
            if let Some(engine_snapshot) = snapshot.engine_snapshots.get(engine_type) {
                info!("Restoring engine: {:?}", engine_type);
                
                let engine = self.engines.get(engine_type)
                    .context("Engine not found")?;
                
                // 3. 引擎特化的恢复策略
                match engine_type {
                    EngineType::StringRocksDB => {
                        self.restore_rocksdb_engine(engine, engine_snapshot).await?;
                    }
                    EngineType::ListMemory => {
                        self.restore_list_engine(engine, engine_snapshot).await?;
                    }
                    EngineType::ZSetMemory => {
                        self.restore_zset_engine(engine, engine_snapshot).await?;
                    }
                    EngineType::StreamDisk => {
                        self.restore_stream_engine(engine, engine_snapshot).await?;
                    }
                }
                
                info!("Engine {:?} restored successfully", engine_type);
            }
        }
        
        // 4. 恢复元数据
        self.restore_metadata(&snapshot.metadata_snapshot).await?;
        
        // 5. 最终一致性验证
        self.verify_final_consistency(&snapshot).await?;
        
        info!("Hybrid snapshot restore completed successfully");
        Ok(())
    }
    
    /// RocksDB引擎恢复：批量写入优化
    async fn restore_rocksdb_engine(&self, engine: &Arc<dyn Engine>, snapshot: &EngineSnapshot) -> Result<(), RestoreError> {
        let string_snapshot = snapshot.as_string_snapshot()?;
        let batch_size = 1000; // 批量写入大小
        
        let mut batch = WriteBatch::new();
        for (i, (key, value)) in string_snapshot.entries.iter().enumerate() {
            batch.put(key, value);
            
            if i % batch_size == 0 {
                self.db.write(batch)?;
                batch = WriteBatch::new();
            }
        }
        
        if batch.len() > 0 {
            self.db.write(batch)?;
        }
        
        Ok(())
    }
}
```

## 性能优化建议

### 1. 并行化策略
```rust
/// 并行快照创建
async fn create_parallel_snapshot(&self) -> Result<HybridSnapshot, Error> {
    let start = Instant::now();
    
    // 并行创建各引擎快照
    let futures: Vec<_> = self.engines.iter().map(|(engine_type, engine)| {
        tokio::spawn(async move {
            let snapshot = engine.create_snapshot().await?;
            Ok((*engine_type, snapshot))
        })
    }).collect();
    
    let results = futures::future::join_all(futures).await;
    
    // 收集结果
    let mut engine_snapshots = HashMap::new();
    for result in results {
        let (engine_type, snapshot) = result??;
        engine_snapshots.insert(engine_type, snapshot);
    }
    
    info!("Parallel snapshot created in {:?}", start.elapsed());
    Ok(HybridSnapshot { engine_snapshots })
}
```

### 2. 增量压缩
```rust
/// 增量数据压缩
fn compress_incremental_delta(delta: &EngineDelta) -> Result<CompressedDelta, Error> {
    match delta {
        EngineDelta::RocksDBWAL { changes, .. } => {
            // RocksDB WAL变更通常有重复模式，适合字典压缩
            let compressed = zstd::encode_all(changes.as_slice(), 3)?;
            Ok(CompressedDelta::Zstd(compressed))
        }
        
        EngineDelta::MemoryOpLog { operations, .. } => {
            // 操作日志通常有相似结构，适合Snappy压缩
            let compressed = snap::raw::Encoder::new().compress_vec(operations.as_slice())?;
            Ok(CompressedDelta::Snappy(compressed))
        }
        
        EngineDelta::ZSetIncremental { dict_changes, zsl_changes, .. } => {
            // ZSet增量包含复杂结构，适合LZ4快速压缩
            let combined = combine_zset_delta(dict_changes, zsl_changes);
            let compressed = lz4::block::compress(&combined, None, true)?;
            Ok(CompressedDelta::Lz4(compressed))
        }
    }
}
```

### 3. 流式传输
```rust
/// 流式快照传输，避免内存峰值
async fn stream_hybrid_snapshot(&self, snapshot: HybridSnapshot, target: NodeId) -> Result<(), TransferError> {
    let chunk_size = 4 * 1024 * 1024; // 4MB chunks
    
    // 1. 发送快照元数据
    self.send_snapshot_metadata(&snapshot.metadata, target).await?;
    
    // 2. 流式发送各引擎快照
    for (engine_type, engine_snapshot) in &snapshot.engine_snapshots {
        info!("Streaming engine {:?} snapshot to {}", engine_type, target);
        
        let mut offset = 0;
        while offset < engine_snapshot.size() {
            let chunk = engine_snapshot.get_chunk(offset, chunk_size)?;
            self.send_snapshot_chunk(*engine_type, chunk, target).await?;
            offset += chunk_size;
        }
    }
    
    // 3. 发送完成信号
    self.send_snapshot_complete(target).await?;
    
    Ok(())
}
```

## 结论与建议

### 现实可行的实施路径：

1. **第一阶段**：实现保守的类型路由 + 有界一致性快照
2. **第二阶段**：添加访问模式分析 + 引擎特化增量
3. **第三阶段**：优化压缩算法 + 流式传输
4. **第四阶段**：完善一致性验证 + 自动修复

### 关键成功因素：
- **渐进式迁移**：避免一次性全量切换
- **分层一致性**：根据业务需求选择一致性级别  
- **引擎特化**：针对不同存储引擎设计专门的快照策略
- **并行优化**：充分利用多核CPU和网络带宽

这种方案虽然复杂，但是现实可行的，能够在性能、一致性和复杂度之间找到平衡点。