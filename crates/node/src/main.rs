//! RedRaft - Redis-compatible distributed key-value store
//!
//! Built on Raft consensus algorithm for reliability and consistency.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use storage::store::HybridStore;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use raft::network::MultiRaftNetwork;
use raft::storage::FileStorage;

use redraft::config::Config;
use redraft::node::RedRaftNode;
use redraft::pilot_client::{PilotClient, PilotClientConfig};
use redraft::server::RedisServer;

/// RedRaft node configuration
#[derive(Parser, Debug)]
#[command(name = "redraft")]
#[command(about = "RedRaft - Redis-compatible distributed key-value store")]
struct Args {
    /// Node ID
    #[arg(short, long, default_value = "node1")]
    node_id: String,

    /// Redis server listen address
    #[arg(short, long, default_value = "127.0.0.1:6379")]
    redis_addr: String,

    /// gRPC service address (for Raft communication)
    #[arg(short, long, default_value = "127.0.0.1:50051")]
    grpc_addr: String,

    /// Data storage directory
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Shard count (only used when no pilot)
    #[arg(short, long, default_value = "3")]
    shard_count: usize,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Pilot control plane address (optional, runs standalone if not specified)
    #[arg(long)]
    pilot_addr: Option<String>,

    /// Heartbeat interval (seconds)
    #[arg(long, default_value = "10")]
    heartbeat_interval: u64,

    /// Other node addresses (for cluster, used when no pilot)
    #[arg(long)]
    peers: Vec<String>,

    /// Configuration file path (YAML format)
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load configuration from file if specified, otherwise use defaults
    let mut config = if let Some(config_path) = &args.config {
        info!("Loading configuration from: {:?}", config_path);
        Config::from_file(config_path)?
    } else {
        Config::default()
    };

    // Override config with command line arguments
    if !args.node_id.is_empty() {
        config.node.node_id = args.node_id.clone();
    }
    if !args.redis_addr.is_empty() {
        config.network.redis_addr = args.redis_addr.clone();
    }
    if !args.grpc_addr.is_empty() {
        config.network.grpc_addr = args.grpc_addr.clone();
    }
    if !args.data_dir.as_os_str().is_empty() {
        config.storage.data_dir = args.data_dir.clone();
    }
    if args.shard_count > 0 {
        config.node.shard_count = args.shard_count;
    }
    if !args.log_level.is_empty() {
        config.log.level = args.log_level.clone();
    }
    if let Some(pilot_addr) = &args.pilot_addr {
        config.pilot = Some(redraft::config::PilotConfig {
            pilot_addr: pilot_addr.clone(),
            heartbeat_interval_secs: args.heartbeat_interval,
            ..Default::default()
        });
    }

    // Initialize logging
    let level = match config.log.level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting RedRaft node: {}", config.node.node_id);
    info!("Redis server: {}", config.network.redis_addr);
    info!("gRPC server: {}", config.network.grpc_addr);
    info!("Data directory: {:?}", config.storage.data_dir);

    // Create data directory
    std::fs::create_dir_all(&config.storage.data_dir)?;

    // Create storage backend
    let options = raft::storage::FileStorageOptions::with_base_dir(config.storage.data_dir.clone());
    let (file_storage, _log_receiver) = FileStorage::new(options)?;
    let storage = Arc::new(file_storage);

    // Create network layer
    let network_options =
        raft::network::MultiRaftNetworkOptions::default().with_node_id(config.node.node_id.clone());
    let network = Arc::new(MultiRaftNetwork::new(network_options));

    // Create Redis store
    let redis_store = Arc::new(HybridStore::new(
        storage::snapshot::SnapshotConfig::default(),
        config.storage.data_dir.clone(),
    )?);

    // Create RedRaft node (pass config for internal use)
    let node = Arc::new(RedRaftNode::new(
        config.node.node_id.clone(),
        storage,
        network,
        redis_store,
        config.node.shard_count,
        config.clone(),
    ));

    // Start node
    node.start().await?;

    // If Pilot address specified, connect to Pilot
    let _pilot_client = if let Some(pilot_config) = &config.pilot {
        info!("Connecting to pilot at {}", pilot_config.pilot_addr);

        let pilot_client_config = PilotClientConfig {
            pilot_addr: pilot_config.pilot_addr.clone(),
            heartbeat_interval_secs: pilot_config.heartbeat_interval_secs,
            routing_refresh_interval_secs: pilot_config.routing_refresh_interval_secs,
            request_timeout_secs: pilot_config.request_timeout_secs,
        };

        let client = Arc::new(PilotClient::new(
            pilot_client_config,
            config.node.node_id.clone(),
            config.network.grpc_addr.clone(),
            config.network.redis_addr.clone(),
        ));

        // Connect and initialize
        match client.connect().await {
            Ok(()) => {
                info!("Connected to pilot successfully");

                // Update node routing table and sync Raft groups
                {
                    let routing = client.routing_table();
                    let table = routing.read();
                    node.router().update_from_pilot(&table);
                    // Create Raft groups this node is responsible for based on routing table
                    let created = node.sync_raft_groups_from_routing(&table).await;
                    if created > 0 {
                        info!("Initial sync: created {} Raft groups", created);
                    }
                }

                // Set Pilot client for status reporting
                node.set_pilot_client(client.clone());

                // Start background tasks
                let _handles = client.clone().start_background_tasks();

                // Start routing table sync task
                let node_clone = node.clone();
                let routing_table = client.routing_table();
                let sync_interval = config.server.routing_sync_interval();
                tokio::spawn(async move {
                    use tokio::time::interval;
                    let mut interval = interval(sync_interval);
                    loop {
                        interval.tick().await;
                        let table = routing_table.read().clone();
                        node_clone.router().update_from_pilot(&table);
                        // Sync Raft groups
                        node_clone.sync_raft_groups_from_routing(&table).await;
                    }
                });

                Some(client)
            }
            Err(e) => {
                warn!(
                    "Failed to connect to pilot: {}, running in standalone mode",
                    e
                );
                None
            }
        }
    } else {
        info!("No pilot address specified, running in standalone mode");
        None
    };

    // Print routing information
    {
        let router = node.router();
        if router.is_pilot_routing() {
            info!(
                "Using pilot routing: version {}, {} shards",
                router.routing_version(),
                router.shard_count()
            );
        } else {
            info!("Using local routing: {} shards", router.shard_count());
        }
    }

    // Create and start Redis server
    let addr: SocketAddr = config.network.redis_addr.parse()?;
    let server = RedisServer::new(node, addr);

    info!("RedRaft node is ready!");
    info!(
        "Connect with: redis-cli -h {} -p {}",
        addr.ip(),
        addr.port()
    );

    // Start server (blocking)
    server.start().await?;

    Ok(())
}
