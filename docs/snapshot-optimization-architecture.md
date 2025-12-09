# 快照优化架构设计：业务层实现方案

## 1. 架构决策

### 1.1 问题分析

**当前实现的问题**：
- Raft 框架层通过 `InstallSnapshotRequest` 传输完整快照数据（`data: Vec<u8>`）
- 对于大快照（10GB+），存在以下问题：
  - 内存占用大（Leader 和 Follower 都需要完整快照在内存中）
  - 网络传输时间长，可能超过 gRPC 消息大小限制
  - 不支持断点续传，失败后需要重新传输
  - 不支持压缩，网络带宽浪费

**优化需求**：
- 分块传输
- 断点续传
- 压缩
- 进度追踪
- 灵活的传输协议（HTTP、gRPC streaming 等）

### 1.2 架构选择

**核心原则：Raft 框架保持通用，不关心快照传输细节**

**方案：完全在业务层实现**
- **Raft 框架层**：
  - 完全不变，`InstallSnapshotRequest.data` 可以是任何内容（业务层决定）
  - 不关心 `data` 是完整快照还是元数据
  - 快照存储管理也是业务层的事情

- **业务层**：
  - `load_snapshot` 返回的 `Snapshot.data` 可以是元数据的序列化
  - `process_snapshot` 接收 `data`，业务层自己判断是元数据还是完整数据
  - 实现快照下载逻辑（HTTP、gRPC 等）
  - 实现快照提供者服务

**优势**：
- Raft 框架完全解耦，保持通用性
- 业务层可以灵活选择传输策略
- 支持多种传输方式和优化

## 2. 设计方案

### 2.1 核心思想

**Raft 框架层**：
- 完全不变，`InstallSnapshotRequest.data` 字段可以是任何内容
- 不关心 `data` 的具体含义（完整快照 or 元数据）
- 快照存储管理由业务层实现

**业务层**：
- `load_snapshot` 可以返回元数据的序列化（而不是完整快照数据）
- `process_snapshot` 接收 `data`，业务层自己解析：
  - 如果是元数据，则通过 HTTP/gRPC 下载快照
  - 如果是完整数据，则直接使用
- 实现快照下载和提供者服务

### 2.2 元数据结构设计（业务层）

```rust
// crates/node/src/snapshot/metadata.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Snapshot index
    pub index: u64,
    /// Snapshot term
    pub term: u64,
    /// Snapshot size in bytes
    pub size: u64,
    /// Snapshot checksum (SHA256)
    pub checksum: Vec<u8>,
    /// Download URL (HTTP)
    pub download_url: Option<String>,
    /// Download endpoint (gRPC)
    pub download_endpoint: Option<SnapshotDownloadEndpoint>,
    /// Compression algorithm (if any)
    pub compression: Option<CompressionType>,
    /// Chunk size for download
    pub chunk_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotDownloadEndpoint {
    /// Node ID that provides the snapshot
    pub node_id: String,
    /// Raft group ID
    pub group_id: String,
    /// Snapshot request ID (for tracking)
    pub snapshot_request_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

impl SnapshotMetadata {
    /// Serialize metadata to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::encode_to_vec(self, bincode::config::standard())
    }
    
    /// Deserialize metadata from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::decode_from_slice(data, bincode::config::standard())
            .map(|(metadata, _)| metadata)
    }
    
    /// Check if data is metadata (magic number)
    pub fn is_metadata(data: &[u8]) -> bool {
        // Use magic number to identify metadata
        data.len() >= 4 && &data[0..4] == b"SMET"  // Snapshot METadata
    }
}
```

### 2.3 业务层实现

#### 2.3.1 快照下载服务

在 Node 端实现快照下载服务：

```rust
// crates/node/src/snapshot/downloader.rs

pub struct SnapshotDownloader {
    http_client: reqwest::Client,
    storage: Arc<dyn SnapshotStorage>,
}

impl SnapshotDownloader {
    /// Download snapshot from metadata
    pub async fn download_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        target_path: &Path,
    ) -> Result<(), SnapshotDownloadError> {
        match &metadata.download_url {
            Some(url) => {
                // HTTP 下载
                self.download_via_http(url, metadata, target_path).await
            }
            None => {
                // gRPC 下载
                match &metadata.download_endpoint {
                    Some(endpoint) => {
                        self.download_via_grpc(endpoint, metadata, target_path).await
                    }
                    None => {
                        Err(SnapshotDownloadError::NoDownloadMethod)
                    }
                }
            }
        }
    }
    
    /// HTTP 下载（支持断点续传）
    async fn download_via_http(
        &self,
        url: &str,
        metadata: &SnapshotMetadata,
        target_path: &Path,
    ) -> Result<(), SnapshotDownloadError> {
        // 检查已下载的部分
        let mut downloaded_size = if target_path.exists() {
            std::fs::metadata(target_path)?.len()
        } else {
            0
        };
        
        // 如果已下载完整，验证 checksum
        if downloaded_size == metadata.size {
            if self.verify_checksum(target_path, &metadata.checksum).await? {
                return Ok(());
            }
            // Checksum 不匹配，重新下载
            downloaded_size = 0;
            std::fs::remove_file(target_path)?;
        }
        
        // 断点续传：从已下载位置继续
        let mut request = self.http_client
            .get(url)
            .header("Range", format!("bytes={}-", downloaded_size));
        
        // 如果支持压缩，添加 Accept-Encoding
        if let Some(compression) = &metadata.compression {
            match compression {
                CompressionType::Gzip => {
                    request = request.header("Accept-Encoding", "gzip");
                }
                CompressionType::Zstd => {
                    request = request.header("Accept-Encoding", "zstd");
                }
                _ => {}
            }
        }
        
        let mut response = request.send().await?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(target_path)?;
        
        // 分块下载
        let mut buffer = vec![0u8; metadata.chunk_size as usize];
        let mut total_downloaded = downloaded_size;
        
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk)?;
            total_downloaded += chunk.len() as u64;
            
            // 进度回调（可选）
            if total_downloaded % (10 * 1024 * 1024) == 0 {
                info!("Downloaded {}/{} bytes", total_downloaded, metadata.size);
            }
        }
        
        // 验证 checksum
        self.verify_checksum(target_path, &metadata.checksum).await?;
        
        Ok(())
    }
    
    /// gRPC streaming 下载
    async fn download_via_grpc(
        &self,
        endpoint: &SnapshotDownloadEndpoint,
        metadata: &SnapshotMetadata,
        target_path: &Path,
    ) -> Result<(), SnapshotDownloadError> {
        // 实现 gRPC streaming 下载
        // ...
    }
    
    /// 验证快照 checksum
    async fn verify_checksum(
        &self,
        path: &Path,
        expected_checksum: &[u8],
    ) -> Result<bool, SnapshotDownloadError> {
        use sha2::{Sha256, Digest};
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut hasher)?;
        let computed = hasher.finalize();
        Ok(computed.as_slice() == expected_checksum)
    }
}
```

#### 2.3.2 扩展 load_snapshot（返回元数据）

```rust
// crates/node/src/node.rs

#[async_trait]
impl SnapshotStorage for NodeCallbacks {
    async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<raft::Snapshot>> {
        // 检查是否启用元数据模式
        if self.use_snapshot_metadata {
            // 返回元数据的序列化
            let metadata = self.create_snapshot_metadata(from).await?;
            let metadata_bytes = metadata.serialize()?;
            
            Ok(Some(raft::Snapshot {
                index: metadata.index,
                term: metadata.term,
                data: metadata_bytes,  // 元数据的序列化，而不是完整快照
                config: ClusterConfig::default(),  // 可以从 metadata 中获取
            }))
        } else {
            // 旧方式：返回完整快照数据
            self.storage.load_snapshot(from).await
        }
    }
    
    /// 创建快照元数据
    async fn create_snapshot_metadata(
        &self,
        from: &RaftId,
    ) -> Result<SnapshotMetadata, StorageError> {
        // 1. 加载完整快照（用于计算 checksum 和 size）
        let full_snapshot = self.storage.load_snapshot(from).await?;
        let snapshot = full_snapshot.ok_or_else(|| {
            StorageError::SnapshotNotFound
        })?;
        
        // 2. 计算 checksum
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&snapshot.data);
        let checksum = hasher.finalize().to_vec();
        
        // 3. 构造下载 URL
        let download_url = format!(
            "http://{}:{}/snapshots/{}/{}",
            self.node_address,
            self.http_port,
            from.group,
            snapshot.index
        );
        
        // 4. 创建元数据
        Ok(SnapshotMetadata {
            index: snapshot.index,
            term: snapshot.term,
            size: snapshot.data.len() as u64,
            checksum,
            download_url: Some(download_url),
            download_endpoint: None,
            compression: Some(CompressionType::Gzip),
            chunk_size: 1024 * 1024,  // 1MB chunks
        })
    }
}
```

#### 2.3.3 修改 process_snapshot 实现

```rust
// crates/node/src/state_machine.rs

impl StateMachine for KVStateMachine {
    fn process_snapshot(
        &self,
        from: &RaftId,
        index: u64,
        term: u64,
        data: Vec<u8>,  // 可能是完整快照数据，也可能是元数据的序列化
        config: ClusterConfig,
        request_id: RequestId,
        oneshot: tokio::sync::oneshot::Sender<raft::SnapshotResult<()>>,
    ) {
        let store = self.store.clone();
        let version = self.version.clone();
        let apply_results = self.apply_results.clone();
        let from = from.clone();
        let downloader = self.snapshot_downloader.clone();
        
        tokio::task::spawn(async move {
            // 判断 data 是元数据还是完整快照
            let snapshot_data = if SnapshotMetadata::is_metadata(&data) {
                // 是元数据，需要下载
                info!("Detected snapshot metadata, downloading snapshot...");
                
                // 解析元数据
                let metadata = match SnapshotMetadata::deserialize(&data) {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                            anyhow::anyhow!("Failed to deserialize metadata: {}", e)
                        ))));
                        return;
                    }
                };
                
                // 下载快照
                let temp_path = format!("/tmp/snapshot_{}_{}.tmp", from.group, index);
                match downloader.download_snapshot(&metadata, &temp_path).await {
                    Ok(()) => {
                        // 读取下载的快照
                        match std::fs::read(&temp_path) {
                            Ok(snapshot_data) => {
                                // 清理临时文件
                                let _ = std::fs::remove_file(&temp_path);
                                snapshot_data
                            }
                            Err(e) => {
                                let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                                    anyhow::anyhow!("Failed to read downloaded snapshot: {}", e)
                                ))));
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = oneshot.send(Err(raft::SnapshotError::DownloadFailed(Arc::new(
                            anyhow::anyhow!("Failed to download snapshot: {}", e)
                        ))));
                        return;
                    }
                }
            } else {
                // 是完整快照数据，直接使用
                data
            };
            
            // 恢复状态机
            match store.restore_from_snapshot(&snapshot_data) {
                Ok(()) => {
                    version.store(index, std::sync::atomic::Ordering::SeqCst);
                    apply_results.lock().clear();
                    
                    info!(
                        "Snapshot installed successfully for {} at index {}, {} keys restored",
                        from, index, store.dbsize()
                    );
                    
                    let _ = oneshot.send(Ok(()));
                }
                Err(e) => {
                    warn!(
                        "Failed to install snapshot for {} at index {}: {}",
                        from, index, e
                    );
                    let _ = oneshot.send(Err(raft::SnapshotError::DataCorrupted(Arc::new(
                        anyhow::anyhow!(e)
                    ))));
                }
            }
        });
    }
}
```

### 2.4 Raft 框架层

**完全不需要修改！**

- `InstallSnapshotRequest.data` 字段可以是任何内容（业务层决定）
- Raft 框架不关心 `data` 是完整快照还是元数据
- 快照存储管理完全由业务层实现

### 2.5 快照提供者服务

在 Leader 节点提供快照下载服务：

```rust
// crates/node/src/snapshot/provider.rs

use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
    Router,
};

pub struct SnapshotProvider {
    storage: Arc<dyn SnapshotStorage>,
}

impl SnapshotProvider {
    pub fn router() -> Router {
        Router::new()
            .route("/snapshots/:group_id/:index", get(serve_snapshot))
            .route("/snapshots/:group_id/:index/metadata", get(serve_snapshot_metadata))
    }
    
    /// 提供快照下载（支持 Range 请求，断点续传）
    async fn serve_snapshot(
        Path((group_id, index)): Path<(String, u64)>,
        headers: HeaderMap,
    ) -> Result<Response, StatusCode> {
        // 1. 加载快照
        let raft_id = RaftId { group: group_id, node: "".to_string() };
        let snapshot = storage.load_snapshot(&raft_id).await
            .map_err(|_| StatusCode::NOT_FOUND)?;
        
        if snapshot.is_none() || snapshot.as_ref().unwrap().index != index {
            return Err(StatusCode::NOT_FOUND);
        }
        
        let snapshot = snapshot.unwrap();
        
        // 2. 处理 Range 请求（断点续传）
        let range = headers.get("range");
        let (start, end) = if let Some(range) = range {
            parse_range(range.to_str().unwrap(), snapshot.data.len())
        } else {
            (0, snapshot.data.len())
        };
        
        // 3. 检查压缩
        let accept_encoding = headers.get("accept-encoding");
        let (data, content_encoding) = if accept_encoding.map(|h| h.to_str().unwrap().contains("gzip")).unwrap_or(false) {
            // 压缩
            let compressed = compress_gzip(&snapshot.data[start..end])?;
            (compressed, "gzip")
        } else {
            (snapshot.data[start..end].to_vec(), "identity")
        };
        
        // 4. 构造响应
        let mut response = Response::builder()
            .status(if range.is_some() { 206 } else { 200 })
            .header("content-type", "application/octet-stream")
            .header("content-length", data.len())
            .header("content-encoding", content_encoding)
            .header("accept-ranges", "bytes");
        
        if range.is_some() {
            response = response.header(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, snapshot.data.len())
            );
        }
        
        Ok(response.body(data.into()).unwrap())
    }
}
```

## 3. 实现步骤

### 3.1 Phase 1: 业务层元数据定义

1. **定义 SnapshotMetadata 结构**
   - 在 `crates/node/src/snapshot/metadata.rs` 中定义
   - 包含下载 URL、checksum、压缩信息等
   - 实现序列化/反序列化方法
   - 实现 `is_metadata` 方法（通过 magic number 识别）

### 3.2 Phase 2: 扩展 load_snapshot

1. **修改 NodeCallbacks::load_snapshot**
   - 添加配置选项：是否使用元数据模式
   - 如果启用元数据模式，返回元数据的序列化
   - 否则返回完整快照数据（向后兼容）

2. **实现 create_snapshot_metadata**
   - 加载完整快照（用于计算 checksum 和 size）
   - 构造下载 URL
   - 创建元数据对象

### 3.3 Phase 3: 实现快照下载

1. **实现 SnapshotDownloader**
   - HTTP 下载（支持断点续传、Range 请求）
   - gRPC streaming 下载（可选）
   - 压缩/解压
   - Checksum 验证

2. **实现 SnapshotProvider**
   - HTTP 服务提供快照下载
   - 支持 Range 请求（断点续传）
   - 支持压缩（gzip/zstd）

### 3.4 Phase 4: 修改 process_snapshot

1. **判断 data 类型**
   - 使用 `SnapshotMetadata::is_metadata` 判断
   - 如果是元数据，解析并下载
   - 如果是完整数据，直接使用

2. **错误处理**
   - 下载失败处理
   - Checksum 验证失败处理
   - 临时文件清理

### 3.5 Phase 5: 优化和测试

1. **性能优化**
   - 并发下载多个 chunks
   - 压缩算法选择（gzip/zstd）
   - CDN 集成（可选）

2. **测试**
   - 单元测试（元数据序列化/反序列化）
   - 集成测试（大快照传输）
   - 断点续传测试
   - 压缩测试
   - 向后兼容测试（完整数据模式）

## 4. 优势总结

### 4.1 架构优势

1. **完全解耦**
   - Raft 框架完全不变，保持通用性
   - 业务层完全控制快照传输策略
   - 快照存储管理也是业务层的事情

2. **灵活性**
   - 支持多种传输协议（HTTP、gRPC、对象存储）
   - 可以针对场景优化（CDN、加密等）
   - 可以动态切换传输方式（元数据 or 完整数据）

3. **可扩展性**
   - 易于添加新的传输方式
   - 易于添加新的优化（压缩、加密等）
   - 不影响 Raft 框架

### 4.2 性能优势

1. **内存效率**
   - Leader 不需要将完整快照加载到内存（只序列化元数据）
   - Follower 可以流式下载和处理

2. **网络效率**
   - 支持压缩，减少带宽
   - 支持断点续传，避免重复传输
   - 支持并发下载

3. **可靠性**
   - 断点续传提高成功率
   - Checksum 验证保证完整性

## 5. 注意事项

### 5.1 向后兼容

- **完全向后兼容**：如果 `load_snapshot` 返回完整快照数据，`process_snapshot` 直接使用
- 通过配置选项控制是否启用元数据模式
- 可以动态切换（元数据模式 or 完整数据模式）

### 5.2 元数据识别

- 使用 magic number (`b"SMET"`) 识别元数据
- 如果识别为元数据，解析并下载
- 否则当作完整快照数据使用

### 5.3 错误处理

- 下载失败时，返回明确的错误信息
- 支持重试机制
- 记录下载进度，支持断点续传
- 临时文件清理

### 5.4 安全性

- 下载 URL 应该包含认证信息（可选）
- 支持 TLS/HTTPS
- 验证快照来源（checksum）

### 5.5 监控和日志

- 记录下载进度
- 记录下载时间
- 记录失败原因
- 监控下载速度

## 6. 关键设计点

### 6.1 Raft 框架层

**完全不需要修改！**

- `InstallSnapshotRequest.data` 可以是任何内容
- Raft 框架不关心 `data` 的具体含义
- 快照存储管理由业务层实现

### 6.2 业务层

**完全控制快照传输策略**

- `load_snapshot` 可以返回元数据的序列化
- `process_snapshot` 自己判断是元数据还是完整数据
- 实现快照下载和提供者服务

### 6.3 元数据格式

- 使用 magic number 识别元数据
- 序列化格式：bincode（或其他）
- 包含下载 URL、checksum、压缩信息等
