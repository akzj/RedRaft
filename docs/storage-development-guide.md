# Redis 兼容存储系统开发指导文档

## 1. 存储架构概览

### 1.1 混合存储架构

本系统采用**混合存储架构**，根据 Redis 数据结构的特性选择最适合的存储后端：

```
┌─────────────────────────────────────────────────────────────┐
│                    混合存储协调器                              │
│              (MultiStorageCoordinator)                       │
└─────────────────────────────────────────────────────────────┘
         │              │              │              │
         ▼              ▼              ▼              ▼
    ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
    │ RocksDB │   │ Memory  │   │ Stream  │   │  TTL    │
    │ Engine  │   │ Engine  │   │ Engine  │   │ Manager │
    └─────────┘   └─────────┘   └─────────┘   └─────────┘
```

### 1.2 存储后端类型

| 存储后端 | 用途 | 特性 |
|---------|------|------|
| **RocksDB** | String、Hash | 持久化、高性能、支持范围查询 |
| **Memory** | List、Set、ZSet | 低延迟、复杂数据结构、易失性 |
| **Stream** | Stream | 大容量日志、消息队列、持久化 |
| **TTL Manager** | 过期管理 | 独立TTL存储、定时清理 |

## 2. 数据结构存储方案

### 2.1 String（字符串）

**存储方案**: `RocksDB`

**理由**:
- ✅ 简单 KV 结构，适合 RocksDB
- ✅ 持久化需求高
- ✅ 支持大 value（RocksDB 支持大对象）
- ✅ 范围扫描性能好

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| GET | ✅ 完全支持 | 优秀 | O(1) 查询 |
| SET | ✅ 完全支持 | 优秀 | O(1) 写入 |
| SETNX | ✅ 完全支持 | 优秀 | 原子操作 |
| SETEX | ✅ 完全支持 | 优秀 | 带 TTL |
| MSET | ✅ 完全支持 | 良好 | 批量写入，使用 WriteBatch |
| MGET | ✅ 完全支持 | 良好 | 批量读取 |
| INCR | ✅ 完全支持 | 优秀 | 原子递增 |
| INCRBY | ✅ 完全支持 | 优秀 | 原子递增 |
| DECR | ✅ 完全支持 | 优秀 | 原子递减 |
| DECRBY | ✅ 完全支持 | 优秀 | 原子递减 |
| APPEND | ✅ 完全支持 | 良好 | 需要读-修改-写 |
| STRLEN | ✅ 完全支持 | 优秀 | O(1) 获取长度 |
| GETSET | ✅ 完全支持 | 良好 | 读-修改-写 |
| GETRANGE | ✅ 完全支持 | 良好 | 子串提取 |
| SETRANGE | ✅ 完全支持 | 良好 | 部分更新 |
| BITCOUNT | ⚠️ 支持但性能差 | 较差 | 需要全量扫描 value |
| BITOP | ⚠️ 支持但性能差 | 较差 | 多 key 操作，需要合并 |
| BITFIELD | ⚠️ 支持但性能差 | 较差 | 位操作复杂 |

**性能特征**:
- **单 key 操作**: O(1)，性能优秀
- **批量操作**: 使用 WriteBatch，性能良好
- **位操作**: 需要全量读取 value，大数据时性能较差
- **范围操作**: RocksDB 原生支持，性能良好

**开发注意事项**:
```rust
// ✅ 推荐：使用 WriteBatch 批量写入
let mut batch = WriteBatch::default();
for (key, value) in kvs {
    batch.put(key, value);
}
db.write(batch)?;

// ⚠️ 注意：BITOP 需要读取多个 key 到内存
// 大数据量时可能内存占用高
```

---

### 2.2 Hash（哈希表）

**存储方案**: `RocksDB`

**理由**:
- ✅ 中等复杂度，适合 RocksDB
- ✅ 持久化需求
- ✅ 字段级操作，RocksDB 支持良好
- ✅ 支持大 Hash（通过 key 前缀组织）

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| HSET | ✅ 完全支持 | 优秀 | O(1) 字段写入 |
| HGET | ✅ 完全支持 | 优秀 | O(1) 字段读取 |
| HDEL | ✅ 完全支持 | 优秀 | O(1) 字段删除 |
| HGETALL | ✅ 完全支持 | 良好 | 需要范围扫描，O(N) |
| HMSET | ✅ 完全支持 | 良好 | 批量写入，使用 WriteBatch |
| HMGET | ✅ 完全支持 | 良好 | 批量读取 |
| HKEYS | ✅ 完全支持 | 良好 | 范围扫描，O(N) |
| HVALS | ✅ 完全支持 | 良好 | 范围扫描，O(N) |
| HLEN | ✅ 完全支持 | 良好 | 需要计数或缓存 |
| HEXISTS | ✅ 完全支持 | 优秀 | O(1) 存在性检查 |
| HINCRBY | ✅ 完全支持 | 良好 | 读-修改-写 |
| HINCRBYFLOAT | ✅ 完全支持 | 良好 | 浮点数操作 |
| HSETNX | ✅ 完全支持 | 优秀 | 原子操作 |
| HSTRLEN | ✅ 完全支持 | 优秀 | O(1) 字段长度 |
| HSCAN | ⚠️ 支持但性能差 | 较差 | 大数据量时扫描慢 |
| HGETALL | ⚠️ 大数据量性能差 | 较差 | 需要全量扫描 |

**性能特征**:
- **单字段操作**: O(1)，性能优秀
- **全量操作** (HGETALL/HKEYS/HVALS): O(N)，大数据量时性能较差
- **批量操作**: 使用 WriteBatch，性能良好
- **计数操作** (HLEN): 需要维护计数器或全量扫描

**开发注意事项**:
```rust
// ✅ 推荐：使用 key 前缀组织 Hash 字段
// key = "hash:{hash_key}:{field}"
let hash_key = format!("hash:{}:{}", hash_name, field);
db.put(hash_key, value)?;

// ⚠️ 注意：HGETALL 需要范围扫描
// 大数据量 Hash 时性能较差，建议使用 HSCAN
```

---

### 2.3 List（列表）

**存储方案**: `Memory` (VecDeque)

**理由**:
- ✅ 频繁的头部/尾部操作，内存性能最优
- ✅ 需要保持顺序，内存结构简单
- ✅ 快速访问，O(1) 头尾操作
- ⚠️ 易失性：需要 WAL 保证持久化

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| LPUSH | ✅ 完全支持 | 优秀 | O(1) 头部插入 |
| RPUSH | ✅ 完全支持 | 优秀 | O(1) 尾部插入 |
| LPOP | ✅ 完全支持 | 优秀 | O(1) 头部弹出 |
| RPOP | ✅ 完全支持 | 优秀 | O(1) 尾部弹出 |
| LLEN | ✅ 完全支持 | 优秀 | O(1) 长度获取 |
| LINDEX | ✅ 完全支持 | 良好 | O(N) 索引访问 |
| LRANGE | ✅ 完全支持 | 良好 | O(N) 范围访问 |
| LSET | ✅ 完全支持 | 良好 | O(N) 索引设置 |
| LTRIM | ✅ 完全支持 | 良好 | O(N) 裁剪操作 |
| LINSERT | ✅ 完全支持 | 较差 | O(N) 插入操作 |
| LREM | ✅ 完全支持 | 较差 | O(N) 删除操作 |
| RPOPLPUSH | ✅ 完全支持 | 良好 | O(1) 原子操作 |
| BRPOP | ⚠️ 支持但性能差 | 较差 | 阻塞操作，需要轮询 |
| BLPOP | ⚠️ 支持但性能差 | 较差 | 阻塞操作，需要轮询 |
| BRPOPLPUSH | ⚠️ 支持但性能差 | 较差 | 阻塞操作，需要轮询 |

**性能特征**:
- **头尾操作** (LPUSH/RPUSH/LPOP/RPOP): O(1)，性能优秀
- **索引操作** (LINDEX/LSET): O(N)，大数据量时性能较差
- **范围操作** (LRANGE): O(N)，性能取决于范围大小
- **阻塞操作** (BLPOP/BRPOP): 需要轮询机制，性能较差

**开发注意事项**:
```rust
// ✅ 推荐：使用 VecDeque 实现 List
let mut list = VecDeque::new();
list.push_front(value);  // LPUSH
list.push_back(value);   // RPUSH

// ⚠️ 注意：需要 WAL 保证持久化
// 每次写操作需要记录到 WAL
wal.append(LogEntry::LPush { key, values })?;

// ⚠️ 注意：LINDEX/LSET 在大数据量时性能差
// 建议限制 List 大小或使用其他数据结构
```

---

### 2.4 Set（集合）

**存储方案**: `Memory` (HashSet)

**理由**:
- ✅ 需要快速成员检查，HashSet O(1)
- ✅ 交并集操作，内存性能最优
- ✅ 简单数据结构，内存实现简单
- ⚠️ 易失性：需要 WAL 保证持久化

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| SADD | ✅ 完全支持 | 优秀 | O(1) 添加成员 |
| SREM | ✅ 完全支持 | 优秀 | O(1) 删除成员 |
| SISMEMBER | ✅ 完全支持 | 优秀 | O(1) 成员检查 |
| SMEMBERS | ✅ 完全支持 | 良好 | O(N) 全量获取 |
| SCARD | ✅ 完全支持 | 优秀 | O(1) 计数 |
| SPOP | ✅ 完全支持 | 良好 | O(1) 随机弹出 |
| SRANDMEMBER | ✅ 完全支持 | 良好 | O(1) 随机成员 |
| SINTER | ⚠️ 支持但性能差 | 较差 | O(N*M) 交集，大数据量慢 |
| SUNION | ⚠️ 支持但性能差 | 较差 | O(N+M) 并集，大数据量慢 |
| SDIFF | ⚠️ 支持但性能差 | 较差 | O(N*M) 差集，大数据量慢 |
| SINTERSTORE | ⚠️ 支持但性能差 | 较差 | 交集并存储 |
| SUNIONSTORE | ⚠️ 支持但性能差 | 较差 | 并集并存储 |
| SDIFFSTORE | ⚠️ 支持但性能差 | 较差 | 差集并存储 |
| SMOVE | ✅ 完全支持 | 良好 | O(1) 移动成员 |
| SSCAN | ⚠️ 支持但性能差 | 较差 | 大数据量时扫描慢 |

**性能特征**:
- **单成员操作** (SADD/SREM/SISMEMBER): O(1)，性能优秀
- **集合运算** (SINTER/SUNION/SDIFF): O(N*M)，大数据量时性能较差
- **全量操作** (SMEMBERS): O(N)，大数据量时性能较差

**开发注意事项**:
```rust
// ✅ 推荐：使用 HashSet 实现 Set
let mut set = HashSet::new();
set.insert(member);

// ⚠️ 注意：集合运算在大数据量时性能差
// 建议限制 Set 大小或使用分批处理
fn sinter(sets: &[&HashSet<Vec<u8>>]) -> HashSet<Vec<u8>> {
    // 选择最小的 Set 作为基准
    let smallest = sets.iter().min_by_key(|s| s.len()).unwrap();
    smallest.iter()
        .filter(|member| sets.iter().all(|s| s.contains(*member)))
        .cloned()
        .collect()
}
```

---

### 2.5 Sorted Set（有序集合）

**存储方案**: `Memory` (跳跃表 + 哈希表)

**理由**:
- ✅ 需要排序和范围查询，跳跃表性能最优
- ✅ 复杂操作（ZRANGE/ZREVRANGE），内存实现简单
- ✅ 分数索引，需要快速查找
- ⚠️ 易失性：需要 WAL 保证持久化
- ⚠️ 序列化复杂：跳跃表结构复杂

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| ZADD | ✅ 完全支持 | 优秀 | O(log N) 添加成员 |
| ZREM | ✅ 完全支持 | 优秀 | O(log N) 删除成员 |
| ZSCORE | ✅ 完全支持 | 优秀 | O(1) 获取分数 |
| ZCARD | ✅ 完全支持 | 优秀 | O(1) 计数 |
| ZRANK | ✅ 完全支持 | 良好 | O(log N) 获取排名 |
| ZREVRANK | ✅ 完全支持 | 良好 | O(log N) 获取反向排名 |
| ZRANGE | ✅ 完全支持 | 良好 | O(log N + M) 范围查询 |
| ZREVRANGE | ✅ 完全支持 | 良好 | O(log N + M) 反向范围查询 |
| ZRANGEBYSCORE | ✅ 完全支持 | 良好 | O(log N + M) 分数范围查询 |
| ZREVRANGEBYSCORE | ✅ 完全支持 | 良好 | O(log N + M) 反向分数范围 |
| ZINCRBY | ✅ 完全支持 | 优秀 | O(log N) 分数递增 |
| ZCOUNT | ✅ 完全支持 | 良好 | O(log N + M) 计数 |
| ZLEXCOUNT | ⚠️ 支持但性能差 | 较差 | 字典序计数，需要遍历 |
| ZRANGEBYLEX | ⚠️ 支持但性能差 | 较差 | 字典序范围，需要遍历 |
| ZREMRANGEBYRANK | ✅ 完全支持 | 良好 | O(log N + M) 按排名删除 |
| ZREMRANGEBYSCORE | ✅ 完全支持 | 良好 | O(log N + M) 按分数删除 |
| ZREMRANGEBYLEX | ⚠️ 支持但性能差 | 较差 | 字典序删除，需要遍历 |
| ZINTERSTORE | ⚠️ 支持但性能差 | 较差 | 交集并存储，O(N*K*log M) |
| ZUNIONSTORE | ⚠️ 支持但性能差 | 较差 | 并集并存储，O(N*K + N*log N) |
| ZSCAN | ⚠️ 支持但性能差 | 较差 | 大数据量时扫描慢 |
| ZPOPMAX | ✅ 完全支持 | 优秀 | O(log N) 弹出最大 |
| ZPOPMIN | ✅ 完全支持 | 优秀 | O(log N) 弹出最小 |
| BZPOPMAX | ⚠️ 支持但性能差 | 较差 | 阻塞操作，需要轮询 |
| BZPOPMIN | ⚠️ 支持但性能差 | 较差 | 阻塞操作，需要轮询 |

**性能特征**:
- **单成员操作** (ZADD/ZREM/ZSCORE): O(log N) 或 O(1)，性能优秀
- **范围查询** (ZRANGE/ZRANGEBYSCORE): O(log N + M)，性能良好
- **集合运算** (ZINTERSTORE/ZUNIONSTORE): O(N*K*log M)，大数据量时性能较差
- **字典序操作** (ZRANGEBYLEX): 需要遍历，性能较差

**开发注意事项**:
```rust
// ✅ 推荐：使用跳跃表 + 哈希表实现 ZSet
struct ZSet {
    dict: HashMap<Vec<u8>, f64>,  // member -> score
    zsl: SkipList,                 // score -> member (排序)
}

// ⚠️ 注意：跳跃表序列化复杂
// 快照时需要特殊处理
fn serialize_zset(zset: &ZSet) -> Vec<u8> {
    // 序列化 dict 和 zsl
}

// ⚠️ 注意：集合运算在大数据量时性能差
// 建议限制 ZSet 大小或使用分批处理
```

---

### 2.6 Stream（流）

**存储方案**: `Stream Engine` (专用磁盘存储，基于 RocksDB)

**理由**:
- ✅ 大容量日志特性，需要持久化
- ✅ 消息 ID 连续性要求
- ✅ 消费组管理复杂
- ✅ 支持分段存储，适合磁盘

**支持的指令**:

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| XADD | ✅ 完全支持 | 优秀 | O(1) 添加消息 |
| XREAD | ✅ 完全支持 | 良好 | O(N) 读取消息 |
| XRANGE | ✅ 完全支持 | 良好 | O(log N + M) 范围查询 |
| XREVRANGE | ✅ 完全支持 | 良好 | O(log N + M) 反向范围 |
| XDEL | ✅ 完全支持 | 良好 | O(log N) 删除消息 |
| XLEN | ✅ 完全支持 | 优秀 | O(1) 获取长度 |
| XTRIM | ✅ 完全支持 | 良好 | O(N) 裁剪流 |
| XGROUP CREATE | ✅ 完全支持 | 优秀 | 创建消费组 |
| XGROUP DESTROY | ✅ 完全支持 | 优秀 | 删除消费组 |
| XREADGROUP | ✅ 完全支持 | 良好 | 消费组读取 |
| XACK | ✅ 完全支持 | 优秀 | O(1) 确认消息 |
| XPENDING | ✅ 完全支持 | 良好 | 查询待处理消息 |
| XCLAIM | ✅ 完全支持 | 良好 | 认领消息 |
| XINFO | ✅ 完全支持 | 优秀 | 查询流信息 |
| XINFO GROUPS | ✅ 完全支持 | 优秀 | 查询消费组信息 |
| XINFO CONSUMERS | ✅ 完全支持 | 优秀 | 查询消费者信息 |

**性能特征**:
- **消息操作** (XADD/XDEL): O(log N)，性能优秀
- **范围查询** (XRANGE/XREVRANGE): O(log N + M)，性能良好
- **消费组操作**: 需要维护状态，性能良好
- **大流处理**: 支持分段存储，性能良好

**开发注意事项**:
```rust
// ✅ 推荐：使用 RocksDB 存储 Stream 数据
// key = "stream:{stream_key}:{message_id}"
let stream_key = format!("stream:{}:{}", stream_name, message_id);
db.put(stream_key, message_data)?;

// ✅ 推荐：使用独立列族存储 Stream 元数据
let cf_metadata = db.cf_handle("stream_metadata")?;
cf_metadata.put(stream_name, metadata)?;

// ⚠️ 注意：需要维护消息 ID 连续性
// 使用时间戳 + 序列号生成消息 ID
```

---

### 2.7 其他数据结构

#### 2.7.1 Bitmap

**存储方案**: `RocksDB` (作为 String 存储)

**支持状态**: ⚠️ 部分支持，性能较差

**理由**:
- Bitmap 在 Redis 中实际是 String 的位操作
- 大数据量时位操作性能较差
- 需要全量读取 value 到内存

**支持的指令**:
- SETBIT/GETBIT: ⚠️ 支持但性能差
- BITCOUNT: ⚠️ 支持但性能差
- BITOP: ⚠️ 支持但性能差
- BITFIELD: ⚠️ 支持但性能差

#### 2.7.2 HyperLogLog

**存储方案**: `RocksDB` (作为 String 存储)

**支持状态**: ❌ 不支持

**理由**:
- 需要特殊的概率算法实现
- 使用场景较少
- 优先级较低

#### 2.7.3 Geospatial

**存储方案**: `Memory` (使用 ZSet 实现)

**支持状态**: ⚠️ 部分支持

**理由**:
- 使用 ZSet 存储地理位置
- 需要 GeoHash 编码
- 范围查询性能一般

**支持的指令**:
- GEOADD: ⚠️ 支持（基于 ZADD）
- GEODIST: ⚠️ 支持但性能差
- GEOHASH: ⚠️ 支持
- GEOPOS: ⚠️ 支持
- GEORADIUS: ⚠️ 支持但性能差
- GEORADIUSBYMEMBER: ⚠️ 支持但性能差

---

## 3. 通用指令支持

### 3.1 Key 管理指令

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| DEL | ✅ 完全支持 | 良好 | 跨引擎删除 |
| EXISTS | ✅ 完全支持 | 优秀 | O(1) 检查 |
| EXPIRE | ✅ 完全支持 | 优秀 | TTL 管理 |
| EXPIREAT | ✅ 完全支持 | 优秀 | TTL 管理 |
| PEXPIRE | ✅ 完全支持 | 优秀 | TTL 管理 |
| PEXPIREAT | ✅ 完全支持 | 优秀 | TTL 管理 |
| TTL | ✅ 完全支持 | 优秀 | O(1) 查询 |
| PTTL | ✅ 完全支持 | 优秀 | O(1) 查询 |
| PERSIST | ✅ 完全支持 | 优秀 | 移除 TTL |
| TYPE | ✅ 完全支持 | 优秀 | O(1) 查询 |
| RENAME | ✅ 完全支持 | 良好 | 跨引擎重命名 |
| RENAMENX | ✅ 完全支持 | 良好 | 跨引擎重命名 |
| KEYS | ⚠️ 支持但性能差 | 较差 | 全量扫描，生产环境不推荐 |
| SCAN | ⚠️ 支持但性能差 | 较差 | 迭代扫描，大数据量慢 |
| RANDOMKEY | ⚠️ 支持但性能差 | 较差 | 需要随机访问 |
| DUMP | ⚠️ 支持但性能差 | 较差 | 序列化操作 |
| RESTORE | ⚠️ 支持但性能差 | 较差 | 反序列化操作 |
| MIGRATE | ❌ 不支持 | - | 需要跨节点迁移 |
| MOVE | ❌ 不支持 | - | 单机多 DB 功能 |

### 3.2 事务指令

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| MULTI | ✅ 完全支持 | 优秀 | 事务开始 |
| EXEC | ✅ 完全支持 | 良好 | 跨引擎事务，需要协调 |
| DISCARD | ✅ 完全支持 | 优秀 | 取消事务 |
| WATCH | ⚠️ 支持但性能差 | 较差 | 需要乐观锁机制 |
| UNWATCH | ✅ 完全支持 | 优秀 | 取消监视 |

**开发注意事项**:
```rust
// ⚠️ 注意：跨引擎事务需要两阶段提交
// 性能开销较大，建议避免跨引擎事务
```

### 3.3 发布订阅指令

**存储方案**: `Memory` (内存实现，不持久化)

**理由**:
- ✅ 实时消息传递，不需要持久化
- ✅ 内存实现性能优秀，低延迟
- ⚠️ 消息不持久化，服务重启或崩溃会丢失
- ⚠️ 单机实现，不支持跨节点消息传递

| 指令 | 支持状态 | 性能 | 备注 |
|------|---------|------|------|
| PUBLISH | ✅ 完全支持 | 优秀 | O(N) N=订阅者数量，内存分发 |
| SUBSCRIBE | ✅ 完全支持 | 优秀 | O(1) 添加订阅者 |
| PSUBSCRIBE | ✅ 完全支持 | 良好 | O(1) 模式订阅，需要模式匹配 |
| UNSUBSCRIBE | ✅ 完全支持 | 优秀 | O(1) 移除订阅者 |
| PUNSUBSCRIBE | ✅ 完全支持 | 优秀 | O(1) 移除模式订阅 |
| PUBSUB CHANNELS | ✅ 完全支持 | 良好 | O(N) 列出所有频道 |
| PUBSUB NUMSUB | ✅ 完全支持 | 优秀 | O(1) 查询频道订阅数 |
| PUBSUB NUMPAT | ✅ 完全支持 | 优秀 | O(1) 查询模式订阅数 |

**性能特征**:
- **发布消息** (PUBLISH): O(N)，N 为订阅者数量，内存分发性能优秀
- **订阅/取消订阅**: O(1)，性能优秀
- **模式订阅**: 需要模式匹配，性能良好
- **查询操作**: O(1) 或 O(N)，性能良好

**限制说明**:
- ⚠️ **不持久化**: 消息不持久化，服务重启或崩溃会丢失所有订阅和消息
- ⚠️ **单机实现**: 仅在单节点内有效，不支持跨节点消息传递
- ⚠️ **内存限制**: 大量订阅者时内存占用较高
- ⚠️ **消息丢失**: 订阅者离线时消息会丢失，无重试机制

**开发注意事项**:
```rust
// ✅ 推荐：使用 HashMap + Vec 实现 Pub/Sub
struct PubSubManager {
    // channel -> subscribers
    channels: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
    // pattern -> subscribers  
    patterns: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
}

// ✅ 推荐：使用 channel 实现消息分发
impl PubSubManager {
    fn publish(&self, channel: &str, message: Vec<u8>) -> usize {
        let channels = self.channels.read();
        let subscribers = channels.get(channel).cloned().unwrap_or_default();
        
        // 分发消息到所有订阅者
        let mut delivered = 0;
        for subscriber in subscribers {
            if subscriber.send(message.clone()).is_ok() {
                delivered += 1;
            }
        }
        
        // 模式匹配订阅者
        let patterns = self.patterns.read();
        for (pattern, pattern_subscribers) in patterns.iter() {
            if self.match_pattern(pattern, channel) {
                for subscriber in pattern_subscribers {
                    if subscriber.send(message.clone()).is_ok() {
                        delivered += 1;
                    }
                }
            }
        }
        
        delivered
    }
}

// ⚠️ 注意：订阅者离线时需要清理
// 使用心跳机制检测订阅者是否在线
```

---

## 4. 性能优化建议

### 4.1 存储选择原则

1. **简单 KV → RocksDB**: String、Hash
2. **复杂结构 → Memory**: List、Set、ZSet
3. **大容量日志 → Stream Engine**: Stream
4. **频繁操作 → Memory**: 需要低延迟的操作（包括 Pub/Sub）
5. **持久化需求 → RocksDB/Stream**: 需要持久化的数据
6. **实时消息 → Memory**: Pub/Sub（不持久化，可接受消息丢失）

### 4.2 性能瓶颈识别

| 操作类型 | 性能瓶颈 | 优化建议 |
|---------|---------|---------|
| 全量扫描 | O(N) 遍历 | 使用 SCAN 替代 KEYS |
| 集合运算 | O(N*M) 复杂度 | 限制集合大小，分批处理 |
| 位操作 | 全量读取 value | 限制 value 大小 |
| 阻塞操作 | 轮询机制 | 使用异步处理 |
| 跨引擎事务 | 两阶段提交 | 避免跨引擎事务 |

### 4.3 开发最佳实践

```rust
// ✅ 推荐：使用 WriteBatch 批量写入
let mut batch = WriteBatch::default();
for (key, value) in entries {
    batch.put(key, value);
}
db.write(batch)?;

// ✅ 推荐：使用列族隔离不同类型数据
let string_cf = db.cf_handle("strings")?;
let hash_cf = db.cf_handle("hashes")?;

// ⚠️ 避免：全量扫描操作
// 不要在生产环境使用 KEYS 指令

// ⚠️ 避免：大数据量的集合运算
// 限制 Set/ZSet 大小，或使用分批处理

// ⚠️ 避免：跨引擎事务
// 尽量在同一引擎内完成事务
```

---

## 5. 开发路线图

### 5.1 第一阶段：核心数据结构（已完成）

- [x] String (RocksDB)
- [x] Hash (RocksDB)
- [x] List (Memory)
- [x] Set (Memory)

### 5.2 第二阶段：高级数据结构（进行中）

- [ ] ZSet (Memory) - 跳跃表实现
- [ ] Stream (Stream Engine) - 专用存储

### 5.3 第三阶段：优化和扩展

- [ ] 位操作优化 (Bitmap)
- [ ] 地理位置支持 (Geospatial)
- [ ] 集合运算优化
- [ ] 阻塞操作优化
- [ ] 发布订阅 (Pub/Sub) - Memory 实现，不持久化

### 5.4 第四阶段：高级特性

- [ ] HyperLogLog
- [ ] Lua 脚本支持
- [ ] 模块系统
- [ ] Pub/Sub 跨节点支持（可选，需要消息中间件）

---

## 6. 总结

### 6.1 存储方案总结

| 数据结构 | 存储方案 | 支持度 | 性能评级 |
|---------|---------|--------|---------|
| String | RocksDB | ✅ 完全支持 | ⭐⭐⭐⭐⭐ |
| Hash | RocksDB | ✅ 完全支持 | ⭐⭐⭐⭐⭐ |
| List | Memory | ✅ 完全支持 | ⭐⭐⭐⭐ |
| Set | Memory | ✅ 完全支持 | ⭐⭐⭐⭐ |
| ZSet | Memory | ✅ 完全支持 | ⭐⭐⭐⭐ |
| Stream | Stream Engine | ✅ 完全支持 | ⭐⭐⭐⭐ |
| Pub/Sub | Memory | ✅ 完全支持 | ⭐⭐⭐⭐ |
| Bitmap | RocksDB | ⚠️ 部分支持 | ⭐⭐ |
| Geospatial | Memory (ZSet) | ⚠️ 部分支持 | ⭐⭐⭐ |
| HyperLogLog | - | ❌ 不支持 | - |

### 6.2 指令支持统计

- **完全支持**: ~85% 的常用指令（包括 Pub/Sub）
- **部分支持**: ~12% 的指令（性能较差或功能受限）
- **不支持**: ~3% 的指令（特殊功能或低优先级）

**注意**: Pub/Sub 在 Memory 上实现，不支持持久化，消息可能丢失。

### 6.3 关键设计决策

1. **混合存储架构**: 根据数据结构特性选择最适合的存储后端
2. **性能优先**: 常用操作（GET/SET/LPUSH/ZADD）性能优秀
3. **渐进式实现**: 先实现核心功能，再优化高级特性
4. **可扩展性**: 支持未来添加新的存储后端和数据结构

---

## 附录：指令快速参考

### A.1 String 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| GET/SET | RocksDB | ✅ | ⭐⭐⭐⭐⭐ |
| MSET/MGET | RocksDB | ✅ | ⭐⭐⭐⭐ |
| INCR/DECR | RocksDB | ✅ | ⭐⭐⭐⭐⭐ |
| APPEND | RocksDB | ✅ | ⭐⭐⭐ |
| BITOP | RocksDB | ⚠️ | ⭐⭐ |

### A.2 Hash 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| HSET/HGET | RocksDB | ✅ | ⭐⭐⭐⭐⭐ |
| HMSET/HMGET | RocksDB | ✅ | ⭐⭐⭐⭐ |
| HGETALL | RocksDB | ✅ | ⭐⭐⭐ |
| HSCAN | RocksDB | ⚠️ | ⭐⭐ |

### A.3 List 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| LPUSH/RPUSH | Memory | ✅ | ⭐⭐⭐⭐⭐ |
| LPOP/RPOP | Memory | ✅ | ⭐⭐⭐⭐⭐ |
| LRANGE | Memory | ✅ | ⭐⭐⭐ |
| BLPOP/BRPOP | Memory | ⚠️ | ⭐⭐ |

### A.4 Set 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| SADD/SREM | Memory | ✅ | ⭐⭐⭐⭐⭐ |
| SMEMBERS | Memory | ✅ | ⭐⭐⭐ |
| SINTER/SUNION | Memory | ⚠️ | ⭐⭐ |

### A.5 ZSet 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| ZADD/ZREM | Memory | ✅ | ⭐⭐⭐⭐⭐ |
| ZRANGE | Memory | ✅ | ⭐⭐⭐⭐ |
| ZINTERSTORE | Memory | ⚠️ | ⭐⭐ |

### A.6 Stream 指令

| 指令 | 存储 | 支持 | 性能 |
|------|------|------|------|
| XADD | Stream | ✅ | ⭐⭐⭐⭐⭐ |
| XREAD | Stream | ✅ | ⭐⭐⭐⭐ |
| XGROUP | Stream | ✅ | ⭐⭐⭐⭐ |

### A.7 Pub/Sub 指令

| 指令 | 存储 | 支持 | 性能 | 备注 |
|------|------|------|------|------|
| PUBLISH | Memory | ✅ | ⭐⭐⭐⭐ | 不持久化 |
| SUBSCRIBE | Memory | ✅ | ⭐⭐⭐⭐⭐ | 不持久化 |
| PSUBSCRIBE | Memory | ✅ | ⭐⭐⭐⭐ | 不持久化 |
| PUBSUB | Memory | ✅ | ⭐⭐⭐⭐ | 不持久化 |

---

**文档版本**: v1.0  
**最后更新**: 2025-12-9  
**维护者**: Storage Team

