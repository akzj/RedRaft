//! Pilot 控制面服务入口

use std::sync::Arc;

use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use pilot::{Pilot, PilotConfig, api::HttpApi};

/// Pilot - 分布式控制面
#[derive(Parser, Debug)]
#[command(name = "pilot")]
#[command(about = "Distributed control plane for raft-lite cluster")]
struct Args {
    /// 集群名称
    #[arg(short, long, default_value = "default")]
    cluster: String,

    /// 数据目录
    #[arg(short, long, default_value = "./pilot_data")]
    data_dir: String,

    /// HTTP API 监听地址
    #[arg(long, default_value = "0.0.0.0:8080")]
    http_addr: String,

    /// 心跳超时时间（秒）
    #[arg(long, default_value = "30")]
    heartbeat_timeout: i64,

    /// 日志级别
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 初始化日志
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // 创建配置
    let config = PilotConfig {
        cluster_name: args.cluster.clone(),
        data_dir: args.data_dir.clone(),
        http_addr: args.http_addr.clone(),
        node_manager: pilot::node_manager::NodeManagerConfig {
            heartbeat_timeout_secs: args.heartbeat_timeout,
            ..Default::default()
        },
    };

    info!("Starting Pilot control plane...");
    info!("  Cluster: {}", config.cluster_name);
    info!("  Data dir: {}", config.data_dir);
    info!("  HTTP API: {}", config.http_addr);

    // 创建 Pilot
    let pilot = Arc::new(Pilot::new(config.clone()).await?);

    // 启动后台任务
    let _heartbeat_handle = pilot.start_heartbeat_checker();
    let _save_handle = pilot.clone().start_periodic_save(60); // 每分钟保存

    // 执行初始调度（为预创建的分片分配节点）
    let assignments = pilot.scheduler().schedule_shard_placement().await;
    if !assignments.is_empty() {
        info!("Initial scheduling: {} shard assignments", assignments.len());
        pilot.save().await?;
    }

    // 打印集群状态
    let metadata = pilot.metadata().await;
    info!(
        "Cluster ready: {} shards, {} slots assigned",
        metadata.shards.len(),
        metadata.routing_table.slots.iter().filter(|s| s.is_some()).count()
    );

    // 启动 HTTP API
    let http_api = HttpApi::new(pilot.clone());
    let app = http_api.router();

    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    info!("HTTP API listening on {}", config.http_addr);

    axum::serve(listener, app).await?;

    Ok(())
}
