//! RedRaft - Redis-compatible distributed key-value store
//!
//! Built on Raft consensus algorithm for reliability and consistency.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use raft::storage::FileStorage;
use raft::network::MultiRaftNetwork;

use redraft::node::RedRaftNode;
use redraft::server::RedisServer;

/// RedRaft 节点配置
#[derive(Parser, Debug)]
#[command(name = "redraft")]
#[command(about = "RedRaft - Redis-compatible distributed key-value store")]
struct Args {
    /// 节点 ID
    #[arg(short, long, default_value = "node1")]
    node_id: String,

    /// Redis 服务器监听地址
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    redis_addr: String,

    /// 数据存储目录
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Shard 数量
    #[arg(short, long, default_value = "3")]
    shard_count: usize,

    /// 日志级别
    #[arg(long, default_value = "info")]
    log_level: String,

    /// 其他节点地址（用于集群）
    #[arg(long)]
    peers: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 初始化日志
    let level = match args.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting RedRaft node: {}", args.node_id);
    info!("Redis server will listen on: {}", args.redis_addr);
    info!("Data directory: {:?}", args.data_dir);
    info!("Shard count: {}", args.shard_count);

    // 创建数据目录
    std::fs::create_dir_all(&args.data_dir)?;

    // 创建存储后端
    let options = raft::storage::FileStorageOptions::with_base_dir(args.data_dir.clone());
    let (file_storage, _log_receiver) = FileStorage::new(options)?;
    let storage = Arc::new(file_storage);

    // 创建网络层
    let network_options = raft::network::MultiRaftNetworkOptions::default()
        .with_node_id(args.node_id.clone());
    let network = Arc::new(MultiRaftNetwork::new(network_options));

    // 创建 RedRaft 节点
    let node = Arc::new(RedRaftNode::new(
        args.node_id.clone(),
        storage,
        network,
        args.shard_count,
    ));

    // 启动节点
    node.start().await?;

    // 创建并启动 Redis 服务器
    let addr: SocketAddr = args.redis_addr.parse()?;
    let server = RedisServer::new(node, addr);

    info!("RedRaft node is ready!");
    info!("Connect with: redis-cli -h {} -p {}", addr.ip(), addr.port());

    // 启动服务器（阻塞）
    server.start().await?;

    Ok(())
}
