//! RedRaft - Redis-compatible distributed key-value store
//!
//! Built on Raft consensus algorithm for reliability and consistency.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use raft::storage::FileStorage;
use raft::network::MultiRaftNetwork;

use redraft::node::RedRaftNode;
use redraft::pilot_client::{PilotClient, PilotClientConfig};
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

    /// gRPC 服务地址（用于 Raft 通信）
    #[arg(short, long, default_value = "127.0.0.1:50051")]
    grpc_addr: String,

    /// 数据存储目录
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Shard 数量（仅在无 pilot 时使用）
    #[arg(short, long, default_value = "3")]
    shard_count: usize,

    /// 日志级别
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Pilot 控制面地址（可选，如果不指定则独立运行）
    #[arg(long)]
    pilot_addr: Option<String>,

    /// 心跳间隔（秒）
    #[arg(long, default_value = "10")]
    heartbeat_interval: u64,

    /// 其他节点地址（用于集群，无 pilot 时使用）
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
    info!("Redis server: {}", args.redis_addr);
    info!("gRPC server: {}", args.grpc_addr);
    info!("Data directory: {:?}", args.data_dir);

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

    // 如果指定了 Pilot 地址，则连接 Pilot
    let _pilot_client = if let Some(pilot_addr) = args.pilot_addr {
        info!("Connecting to pilot at {}", pilot_addr);

        let config = PilotClientConfig {
            pilot_addr,
            heartbeat_interval_secs: args.heartbeat_interval,
            ..Default::default()
        };

        let client = Arc::new(PilotClient::new(
            config,
            args.node_id.clone(),
            args.grpc_addr.clone(),
            args.redis_addr.clone(),
        ));

        // 连接并初始化
        match client.connect().await {
            Ok(()) => {
                info!("Connected to pilot successfully");
                
                // 更新节点路由表
                {
                    let routing = client.routing_table();
                    let table = routing.read();
                    node.router().update_from_pilot(&table);
                }
                
                // 启动后台任务
                let _handles = client.clone().start_background_tasks();
                
                // 启动路由表同步任务
                let router = node.router();
                let routing_table = client.routing_table();
                tokio::spawn(async move {
                    use tokio::time::{interval, Duration};
                    let mut interval = interval(Duration::from_secs(5));
                    loop {
                        interval.tick().await;
                        let table = routing_table.read();
                        router.update_from_pilot(&table);
                    }
                });
                
                Some(client)
            }
            Err(e) => {
                warn!("Failed to connect to pilot: {}, running in standalone mode", e);
                None
            }
        }
    } else {
        info!("No pilot address specified, running in standalone mode");
        None
    };

    // 打印路由信息
    {
        let router = node.router();
        if router.is_pilot_routing() {
            info!(
                "Using pilot routing: version {}, {} shards",
                router.routing_version(),
                router.shard_count()
            );
        } else {
            info!(
                "Using local routing: {} shards",
                router.shard_count()
            );
        }
    }

    // 创建并启动 Redis 服务器
    let addr: SocketAddr = args.redis_addr.parse()?;
    let server = RedisServer::new(node, addr);

    info!("RedRaft node is ready!");
    info!("Connect with: redis-cli -h {} -p {}", addr.ip(), addr.port());

    // 启动服务器（阻塞）
    server.start().await?;

    Ok(())
}
