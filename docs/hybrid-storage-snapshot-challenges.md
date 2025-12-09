# 混合存储架构的快照复杂性分析

## 1. 核心复杂性挑战

### 1.1 数据类型-存储引擎映射复杂性

**当前统一存储架构**：
```rust
// 当前架构：所有数据类型统一存储在内存HashMap中
pub struct MemoryStore {
    data: Arc<RwLock<HashMap<Vec<u8>, Entry>>>, // 一锅端
}

// Entry包含所有数据类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisValue {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}
```

**混合架构设想**：
```rust
pub struct HybridStore {
    string_engine: Arc<dyn StringStore>,      // RocksDB - KV持久化
    list_engine: Arc<dyn ListStore>,          // 内存 - 快速访问  
    zset_engine: Arc<dyn ZSetStore>,          // 内存 - 复杂数据结构
    stream_engine: Arc<dyn StreamStore>,      // 磁盘 - 大容量日志
    hash_engine: Arc<dyn HashStore>,           // 可选：独立Hash存储
    set_engine: Arc<dyn SetStore>,             // 可选：独立Set存储
    
    // 元数据管理
    metadata_engine: Arc<dyn MetadataStore>,   // 存储key->engine映射
}
```

### 1.2 跨引擎一致性地狱

**问题场景**：
```rust
// 事务操作涉及多个数据类型
MULTI
SET user:1:name "Alice"      // -> RocksDB String引擎
LPUSH user:1:posts "post1"   // -> 内存 List引擎  
ZADD user_scores 100 user:1  // -> 内存 ZSet引擎
EXEC
```

**快照时的噩梦**：
- 每个引擎的快照**时间点不一致**
- 某个引擎快照失败怎么办？
- 如何确保跨引擎的**原子性**？
- 回滚时的复杂性（部分引擎成功，部分失败）

### 1.3 分片分裂的复杂性爆炸

```rust
// 分片分裂时需要：
fn split_shard(hybrid_store: &HybridStore, slot_range: (u32, u32)) -> SplitResult {
    // 1. 从RocksDB筛选String类型key-value
    let string_data = hybrid_store.string_engine.filter_by_slot(slot_range)?;
    
    // 2. 从内存引擎筛选List数据  
    let list_data = hybrid_store.list_engine.filter_by_slot(slot_range)?;
    
    // 3. 从内存引擎筛选ZSet数据
    let zset_data = hybrid_store.zset_engine.filter_by_slot(slot_range)?;
    
    // 4. 从磁盘引擎筛选Stream数据
    let stream_data = hybrid_store.stream_engine.filter_by_slot(slot_range)?;
    
    // 5. 确保所有引擎的slot范围一致
    // 6. 处理跨类型的关联数据
    // ...
}
```

## 2. 多引擎数据分布策略

### 2.1 基于数据特性的引擎选择

| 数据类型 | 推荐引擎 | 理由 | 挑战 |
|---------|---------|------|------|
| **String** | RocksDB | 简单KV，持久化需求 | 范围扫描性能 |
| **List** | 内存 + WAL | 频繁操作，快速访问 | 内存限制，故障恢复 |
| **ZSet** | 内存 + 磁盘索引 | 复杂操作，排序需求 | 跳跃表序列化 |
| **Hash** | RocksDB | 中等复杂度，持久化 | 字段级操作 |
| **Set** | 内存/磁盘可选 | 取决于大小 | 交并集操作 |
| **Stream** | 专用磁盘存储 | 大容量日志特性 | 消费组管理 |

### 2.2 统一元数据管理

```rust
/// Key元数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    pub key: Vec<u8>,
    pub engine_type: EngineType,        // 存储引擎类型
    pub slot: u32,                       // 所属slot
    pub created_at: u64,                 // 创建时间
    pub last_modified: u64,              // 最后修改时间
    pub data_size: usize,                // 数据大小预估
    pub ttl: Option<u64>,                // 过期时间
}

/// 引擎类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineType {
    StringRocksDB,
    ListMemory,
    ZSetMemory,
    HashRocksDB,
    StreamDisk,
}

/// 混合存储元数据管理器
pub struct HybridMetadataManager {
    // key -> metadata 映射（可存储在专用RocksDB）
    key_metadata: Arc<RwLock<HashMap<Vec<u8>, KeyMetadata>>>,
    
    // slot -> keys 反向索引（加速分片操作）
    slot_index: Arc<RwLock<HashMap<u32, HashSet<Vec<u8>>>>>,
    
    // 引擎统计信息
    engine_stats: Arc<RwLock<HashMap<EngineType, EngineStats>>>,
}

impl HybridMetadataManager {
    /// 根据key选择存储引擎
    fn select_engine(&self, key: &[u8], cmd: &Command) -> EngineType {
        match cmd {
            Command::Set(_, _) => EngineType::StringRocksDB,
            Command::Lpush(_, _) | Command::Rpush(_, _) => EngineType::ListMemory,
            Command::Zadd(_, _) => EngineType::ZSetMemory,
            Command::Hset(_, _, _) => EngineType::HashRocksDB,
            Command::Xadd(_, _) => EngineType::StreamDisk,
            // ... 其他命令
            _ => self.get_key_engine(key).unwrap_or(EngineType::StringRocksDB),
        }
    }
    
    /// 获取指定slot范围的所有key
    fn get_keys_in_slot_range(&self, slot_range: (u32, u32)) -> Vec<KeyMetadata> {
        let slot_index = self.slot_index.read();
        let mut result = Vec::new();
        
        for slot in slot_range.0..slot_range.1 {
            if let Some(keys) = slot_index.get(&slot) {
                for key in keys {
                    if let Some(metadata) = self.get_key_metadata(key) {
                        result.push(metadata);
                    }
                }
            }
        }
        
        result
    }
}
```

### 2.3 跨引擎事务管理

```rust
/// 跨引擎事务协调器
pub struct CrossEngineTransaction {
    tx_id: Uuid,
    operations: Vec<EngineOperation>,
    participants: HashSet<EngineType>,
    state: TransactionState,
}

#[derive(Debug, Clone)]
pub struct EngineOperation {
    pub engine: EngineType,
    pub operation: Command,
    pub key: Vec<u8>,
}

/// 两阶段提交协议
impl CrossEngineTransaction {
    /// 阶段1：预提交，锁定资源
    async fn prepare(&mut self) -> Result<(), TransactionError> {
        for engine in &self.participants {
            match engine.prepare_operation(&self.operations).await {
                Ok(_) => {
                    info!("Engine {:?} prepared successfully", engine);
                }
                Err(e) => {
                    // 任一引擎失败，回滚所有已准备的引擎
                    self.rollback().await?;
                    return Err(e);
                }
            }
        }
        Ok(())
    }
    
    /// 阶段2：提交或回滚
    async fn commit(&mut self) -> Result<(), TransactionError> {
        for engine in &self.participants {
            engine.commit_operations(&self.operations).await?;
        }
        Ok(())
    }
    
    async fn rollback(&mut self) -> Result<(), TransactionError> {
        for engine in &self.participants {
            engine.rollback_operations(&self.operations).await?;
        }
        Ok(())
    }
}
```

## 3. 不同数据类型的快照需求分析

### 3.1 String类型（RocksDB）

**快照需求**：
- 支持前缀扫描，按slot筛选
- 增量快照支持（基于WAL）
- 压缩传输
- 断点续传

**实现策略**：
```rust
impl StringRocksDBEngine {
    fn create_split_snapshot(&self, slot_range: (u32, u32)) -> Result<StringSnapshot, String> {
        let snapshot = self.db.snapshot();
        let mut entries = Vec::new();
        
        // 使用前缀迭代器避免全表扫描
        for slot in slot_range.0..slot_range.1 {
            let prefix = format!("slot:{:04x}:", slot);
            let iter = snapshot.prefix_iterator(prefix.as_bytes());
            
            for item in iter {
                let (key, value) = item?;
                entries.push((key.to_vec(), value.to_vec()));
            }
        }
        
        Ok(StringSnapshot { entries })
    }
}
```

### 3.2 List类型（内存+WAL）

**快照需求**：
- 内存数据结构序列化
- WAL日志备份
- 快速恢复
- 支持增量（基于操作日志）

**实现策略**：
```rust
impl ListMemoryEngine {
    fn create_snapshot(&self) -> Result<ListSnapshot, String> {
        let data = self.data.read();
        let mut entries = HashMap::new();
        
        for (key, list) in data.iter() {
            entries.insert(key.clone(), list.iter().cloned().collect::<Vec<_>>());
        }
        
        // 同时记录WAL位置
        let wal_position = self.wal.current_position();
        
        Ok(ListSnapshot {
            entries,
            wal_position,
        })
    }
}
```

### 3.3 ZSet类型（内存+磁盘索引）

**快照需求**：
- 跳跃表数据结构序列化
- 排序信息保持
- 分数索引备份
- 大容量优化

**实现策略**：
```rust
impl ZSetMemoryEngine {
    fn create_snapshot(&self) -> Result<ZSetSnapshot, String> {
        let data = self.data.read();
        let mut entries = HashMap::new();
        
        for (key, zset) in data.iter() {
            // 序列化跳跃表和哈希表
            let snapshot = ZSetDataSnapshot {
                dict: zset.dict.clone(),           // member -> score映射
                zsl: zset.zsl.serialize(),         // 跳跃表序列化
            };
            entries.insert(key.clone(), snapshot);
        }
        
        Ok(ZSetSnapshot { entries })
    }
}
```

### 3.4 Stream类型（专用磁盘存储）

**快照需求**：
- 大容量日志分段
- 消费组状态备份
- 消息ID连续性
- 磁盘空间管理

**实现策略**：
```rust
impl StreamDiskEngine {
    fn create_snapshot(&self) -> Result<StreamSnapshot, String> {
        // 1. 获取当前日志分段信息
        let segments = self.get_active_segments()?;
        
        // 2. 消费组状态
        let consumer_groups = self.get_consumer_groups()?;
        
        // 3. 最后确认的消息ID
        let last_acknowledged = self.get_last_acknowledged()?;
        
        Ok(StreamSnapshot {
            segments: segments.into_iter().map(|s| s.metadata()).collect(),
            consumer_groups,
            last_acknowledged,
            created_at: SystemTime::now(),
        })
    }
}
```

## 4. 统一的多引擎快照方案

### 4.1 协调式快照架构

```rust
/// 混合存储快照协调器
pub struct HybridSnapshotCoordinator {
    engines: HashMap<EngineType, Arc<dyn EngineSnapshot>>,
    metadata_manager: Arc<HybridMetadataManager>,
}

impl HybridSnapshotCoordinator {
    /// 创建统一的跨引擎快照
    async fn create_coordinated_snapshot(
        &self,
        slot_range: Option<(u32, u32)>,
    ) -> Result<HybridSnapshot, SnapshotError> {
        
        // 1. 获取快照起始时间（一致性点）
        let snapshot_timestamp = SystemTime::now();
        
        // 2. 暂停所有引擎的写操作（短时间锁定）
        let write_locks = self.acquire_write_locks().await?;
        
        // 3. 并行创建各引擎快照
        let mut engine_snapshots = HashMap::new();
        
        for (engine_type, engine) in &self.engines {
            let engine_snapshot = match slot_range {
                Some(range) => engine.create_split_snapshot(range).await?,
                None => engine.create_full_snapshot().await?,
            };
            engine_snapshots.insert(*engine_type, engine_snapshot);
        }
        
        // 4. 释放写锁
        drop(write_locks);
        
        // 5. 创建统一的元数据快照
        let metadata_snapshot = self.metadata_manager.create_snapshot(slot_range).await?;
        
        Ok(HybridSnapshot {
            timestamp: snapshot_timestamp,
            engine_snapshots,
            metadata_snapshot,
            slot_range,
        })
    }
}
```

### 4.2 混合快照数据结构

```rust
/// 统一的混合存储快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSnapshot {
    /// 快照创建时间（一致性点）
    pub timestamp: SystemTime,
    
    /// 各引擎独立快照
    pub engine_snapshots: HashMap<EngineType, EngineSnapshotData>,
    
    /// 元数据快照
    pub metadata_snapshot: MetadataSnapshot,
    
    /// 分片范围（None表示全量快照）
    pub slot_range: Option<(u32, u32)>,
    
    /// 快照版本信息
    pub version: SnapshotVersion,
}

/// 引擎快照数据枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineSnapshotData {
    StringRocksDB(StringSnapshot),
    ListMemory(ListSnapshot),
    ZSetMemory(ZSetSnapshot),
    HashRocksDB(HashSnapshot),
    StreamDisk(StreamSnapshot),
}

/// 快照版本控制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotVersion {
    pub format_version: u32,
    pub engine_versions: HashMap<EngineType, u32>,
    pub metadata_version: u32,
}
```

### 4.3 增量快照支持

```rust
/// 混合存储增量快照
pub struct HybridIncrementalSnapshot {
    /// 基础快照信息
    pub base_snapshot: HybridSnapshot,
    
    /// 各引擎的增量变更
    pub engine_deltas: HashMap<EngineType, EngineDelta>,
    
    /// 变更的元数据
    pub metadata_delta: MetadataDelta,
    
    /// 增量统计信息
    pub delta_stats: IncrementalStats,
}

impl HybridSnapshotCoordinator {
    /// 创建增量快照
    async fn create_incremental_snapshot(
        &self,
        base_snapshot: &HybridSnapshot,
        slot_range: (u32, u32),
    ) -> Result<HybridIncrementalSnapshot, SnapshotError> {
        
        // 1. 获取自base_snapshot以来的变更
        let mut engine_deltas = HashMap::new();
        
        for (engine_type, engine) in &self.engines {
            if let Some(base_engine_data) = base_snapshot.engine_snapshots.get(engine_type) {
                let delta = engine.get_delta_since(base_engine_data, slot_range).await?;
                engine_deltas.insert(*engine_type, delta);
            }
        }
        
        // 2. 获取元数据变更
        let metadata_delta = self.metadata_manager
            .get_delta_since(&base_snapshot.metadata_snapshot, slot_range)
            .await?;
        
        Ok(HybridIncrementalSnapshot {
            base_snapshot: base_snapshot.clone(),
            engine_deltas,
            metadata_delta,
            delta_stats: self.calculate_delta_stats(&engine_deltas, &metadata_delta),
        })
    }
}
```

## 5. 跨引擎一致性解决方案

### 5.1 分布式一致性协议

```rust
/// 使用Raft协议保证跨引擎一致性
pub struct ConsistentHybridStore {
    /// 统一的Raft状态机管理所有引擎
    raft_state_machine: Arc<HybridStateMachine>,
    
    /// 各引擎实例
    engines: HashMap<EngineType, Arc<dyn Engine>>,
    
    /// 一致性协调器
    consistency_coordinator: Arc<ConsistencyCoordinator>,
}

impl ConsistentHybridStore {
    /// 应用带一致性的命令
    async fn apply_consistent_command(&self, cmd: Command) -> Result<ApplyResult, StoreError> {
        // 1. 确定目标引擎
        let target_engine = self.select_engine(&cmd)?;
        
        // 2. 通过Raft确保一致性
        let raft_result = self.raft_state_machine.propose_command(cmd).await?;
        
        // 3. 应用到具体引擎
        target_engine.apply(raft_result).await
    }
}
```

### 5.2 快照一致性保证

```rust
/// 一致性快照管理器
pub struct ConsistentSnapshotManager {
    coordinator: Arc<HybridSnapshotCoordinator>,
    
    /// 快照一致性检查点
    consistency_checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
}

impl ConsistentSnapshotManager {
    /// 创建一致性快照
    async fn create_consistent_snapshot(&self) -> Result<ConsistentSnapshot, SnapshotError> {
        // 1. 创建全局一致性点
        let checkpoint = self.create_global_checkpoint().await?;
        
        // 2. 等待所有引擎同步到检查点
        self.wait_for_engines_sync(&checkpoint).await?;
        
        // 3. 创建协调快照
        let hybrid_snapshot = self.coordinator.create_coordinated_snapshot(None).await?;
        
        // 4. 验证一致性
        self.verify_consistency(&hybrid_snapshot, &checkpoint).await?;
        
        Ok(ConsistentSnapshot {
            hybrid_snapshot,
            checkpoint,
            consistency_proof: self.generate_consistency_proof(),
        })
    }
}
```

## 结论

混合存储架构的快照问题确实极其复杂，主要体现在：

1. **多引擎协调复杂性**：不同存储引擎的特性差异巨大
2. **跨引擎一致性挑战**：事务和快照的原子性保证
3. **分片分裂的维度爆炸**：每个引擎都需要独立的slot筛选逻辑
4. **增量快照的交叉依赖**：各引擎的增量数据需要协调

**解决方案方向**：
- 统一元数据管理，建立key->engine映射
- 采用协调式快照，确保时间点一致性
- 实现分布式事务，保证跨引擎原子性
- 设计引擎特化的增量机制，支持混合增量快照

这种架构虽然复杂，但通过合理的设计可以实现高性能和强一致性的平衡。