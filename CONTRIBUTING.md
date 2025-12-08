# Contributing Guidelines

## Code Style

### Language Requirements

**All code comments, documentation, and git commit messages MUST be in English.**

- ✅ Use English for all code comments
- ✅ Use English for all documentation
- ✅ Use English for all git commit messages
- ❌ Do NOT use Chinese or other non-English languages in code

### Examples

**Good:**
```rust
/// Fetch routing table from Pilot
/// 
/// # Arguments
/// - `current_version`: Current routing table version number
pub async fn fetch_routing_table(&self, current_version: Option<u64>) -> Result<RoutingTable, PilotError> {
    // If version provided, add to query parameters
    if let Some(version) = current_version {
        url = format!("{}?version={}", url, version);
    }
    // ...
}
```

**Bad:**
```rust
/// 获取路由表
/// 
/// # 参数
/// - `current_version`: 当前路由表版本号
pub async fn fetch_routing_table(&self, current_version: Option<u64>) -> Result<RoutingTable, PilotError> {
    // 如果提供了版本号，添加到查询参数
    if let Some(version) = current_version {
        url = format!("{}?version={}", url, version);
    }
    // ...
}
```

### Git Commit Messages

**Good:**
```
feat: add routing table watch mechanism

- Implement watch manager for routing table updates
- Support version-based watch registration
- Notify all waiting clients when routing table updates
```

**Bad:**
```
feat: 添加路由表 watch 机制

- 实现路由表更新的 watch 管理器
- 支持基于版本的 watch 注册
- 路由表更新时通知所有等待的客户端
```

## Enforcement

This rule is enforced through:
1. Code review - reviewers should reject PRs with non-English comments
2. Pre-commit hooks (optional) - can be added to check for non-ASCII characters in comments
3. CI checks (optional) - can be added to validate commit messages

## Rationale

- English is the standard language for open-source projects
- Improves code readability for international contributors
- Makes the codebase more accessible to global developers
- Aligns with industry best practices
