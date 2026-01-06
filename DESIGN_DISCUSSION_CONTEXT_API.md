# 设计讨论：如何优雅地传递 log_seq 和其他元数据

## 问题分析

当前需要在所有 trait 方法中添加 `log_seq` 参数，但这样会导致：
1. **API 不稳定**：每次添加新字段都需要修改所有方法签名
2. **维护困难**：有几十个 trait 方法需要修改
3. **扩展性差**：未来可能需要 `apply_index`、`read_index`、`slot`、`term` 等字段

## 解决方案对比

### 方案 1：Context 结构体（推荐）⭐

**设计思路**：
- 创建一个 `ApplyContext` 结构体，包含所有元数据
- 为需要上下文的方法提供 `*_with_context` 版本
- 保持原有方法不变（向后兼容）

**优点**：
- ✅ **向后兼容**：原有方法签名不变
- ✅ **易于扩展**：添加新字段只需修改 `ApplyContext`
- ✅ **类型安全**：编译时检查
- ✅ **清晰的 API**：明确哪些操作需要上下文
- ✅ **可选使用**：不需要上下文时使用原方法

**缺点**：
- ⚠️ 需要为每个需要上下文的方法添加 `*_with_context` 版本
- ⚠️ 实现者需要决定是否使用上下文

**示例代码**：
```rust
#[derive(Debug, Clone, Default)]
pub struct ApplyContext {
    pub log_seq: Option<u64>,
    pub apply_index: Option<u64>,
    pub read_index: Option<u64>,
    pub slot: Option<u32>,
    pub term: Option<u64>,
    // 未来可以轻松添加新字段
}

pub trait StringStore: Send + Sync {
    // 原有方法保持不变
    fn set(&self, key: &[u8], value: Bytes) -> StoreResult<()>;
    
    // 新增带上下文的方法（默认实现调用原方法）
    fn set_with_context(&self, key: &[u8], value: Bytes, ctx: &ApplyContext) -> StoreResult<()> {
        self.set(key, value)  // 默认忽略上下文
    }
}

// 在 apply 中使用
fn apply(&self, ctx: &ApplyContext, cmd: &Command) -> ApplyResult {
    match cmd {
        Command::Set { key, value, .. } => {
            self.set_with_context(key, value.clone(), ctx)
        }
        // ...
    }
}
```

---

### 方案 2：只在顶层传递，底层通过 trait 方法获取

**设计思路**：
- 只在 `apply` 方法传递上下文
- Store 实现一个 `get_context()` 方法
- 底层方法通过 `self.get_context()` 获取

**优点**：
- ✅ 不需要修改所有方法签名
- ✅ 实现简单

**缺点**：
- ❌ **线程安全问题**：多线程环境下 context 可能混乱
- ❌ **不够灵活**：所有操作共享同一个 context
- ❌ **难以测试**：需要 mock context
- ❌ **生命周期问题**：context 的生命周期管理复杂

**示例代码**：
```rust
pub trait RedisStore {
    fn set_context(&mut self, ctx: ApplyContext);
    fn get_context(&self) -> Option<&ApplyContext>;
    
    fn set(&self, key: &[u8], value: Bytes) -> StoreResult<()> {
        if let Some(ctx) = self.get_context() {
            // 使用 ctx.log_seq 等
        }
        // ...
    }
}
```

---

### 方案 3：使用关联类型（Generic Context）

**设计思路**：
- 在 trait 中定义关联类型 `Context`
- 不同实现可以使用不同的上下文类型

**优点**：
- ✅ 类型安全
- ✅ 灵活，不同实现可以使用不同上下文

**缺点**：
- ❌ **过度设计**：对于当前需求来说太复杂
- ❌ **使用复杂**：调用者需要知道具体的 Context 类型
- ❌ **难以统一**：不同实现的 Context 类型不同

**示例代码**：
```rust
pub trait RedisStore {
    type Context: Default + Clone;
    
    fn apply(&self, ctx: Self::Context, cmd: &Command) -> ApplyResult;
}
```

---

### 方案 4：Thread-local 存储（不推荐）❌

**设计思路**：
- 将 context 存储在 thread-local 变量中
- 方法通过 thread-local 获取 context

**优点**：
- ✅ 不需要修改方法签名

**缺点**：
- ❌ **不够灵活**：难以在不同线程间传递
- ❌ **难以测试**：需要设置 thread-local
- ❌ **性能问题**：thread-local 访问有开销
- ❌ **生命周期问题**：context 的生命周期难以管理

---

## 推荐方案：Context 结构体 + 可选方法

### 实现策略

1. **创建 `ApplyContext` 结构体**
   ```rust
   #[derive(Debug, Clone, Default)]
   pub struct ApplyContext {
       pub log_seq: Option<u64>,
       pub apply_index: Option<u64>,
       pub read_index: Option<u64>,
       pub slot: Option<u32>,
       pub term: Option<u64>,
   }
   ```

2. **为需要上下文的方法添加 `*_with_context` 版本**
   - 只在写操作（需要 WAL）的方法中添加
   - 读操作通常不需要上下文
   - 默认实现调用原方法（向后兼容）

3. **在 `apply` 方法中使用上下文**
   ```rust
   fn apply(&self, read_index: u64, apply_index: u64, log_seq: u64, cmd: &Command) -> ApplyResult {
       let ctx = ApplyContext {
           read_index: Some(read_index),
           apply_index: Some(apply_index),
           log_seq: Some(log_seq),
           ..Default::default()
       };
       self.apply_with_context(&ctx, cmd)
   }
   ```

4. **实现者可以选择性覆盖**
   - 如果不需要上下文，使用默认实现
   - 如果需要上下文（如 HybridStore），覆盖 `*_with_context` 方法

### 迁移策略

1. **阶段 1**：添加 `ApplyContext` 和 `*_with_context` 方法（默认实现调用原方法）
2. **阶段 2**：在 `HybridStore` 中实现需要上下文的方法
3. **阶段 3**：逐步迁移调用方使用 `apply_with_context`
4. **阶段 4**（可选）：未来可以废弃原方法，统一使用 `*_with_context`

### 优势总结

- ✅ **向后兼容**：原有代码不需要修改
- ✅ **渐进式迁移**：可以逐步迁移
- ✅ **易于扩展**：添加新字段只需修改 `ApplyContext`
- ✅ **类型安全**：编译时检查
- ✅ **清晰的意图**：明确哪些操作需要上下文

## 其他考虑

### 性能影响

- `ApplyContext` 很小（几个 `Option<u64>`），复制成本低
- 使用 `&ApplyContext` 传递引用，避免复制
- 对于不需要上下文的操作，默认实现直接调用原方法，无额外开销

### 使用示例

```rust
// 创建上下文
let ctx = ApplyContext::with_apply(apply_index, log_seq);

// 使用上下文执行命令
match cmd {
    Command::LPush { key, values } => {
        store.lpush_with_context(key, values, &ctx)?;
    }
    Command::SAdd { key, members } => {
        store.sadd_with_context(key, members, &ctx)?;
    }
    // 读操作不需要上下文
    Command::Get { key } => {
        store.get(key)?;  // 使用原方法
    }
}
```

## 结论

**推荐使用方案 1（Context 结构体）**，因为：
1. 向后兼容，不需要大规模重构
2. 易于扩展，未来添加字段只需修改 `ApplyContext`
3. 类型安全，编译时检查
4. 清晰的 API，明确哪些操作需要上下文
5. 性能影响小，使用引用传递

