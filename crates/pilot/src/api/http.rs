//! HTTP management API
//!
//! Provides RESTful API for cluster management

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::metadata::{NodeInfo, ShardInfo, SplitTask};
use crate::node_manager::RegisterResult;
use crate::Pilot;

/// HTTP API service
pub struct HttpApi {
    pilot: Arc<Pilot>,
}

impl HttpApi {
    /// Create HTTP API
    pub fn new(pilot: Arc<Pilot>) -> Self {
        Self { pilot }
    }

    /// Create router
    pub fn router(self) -> Router {
        let pilot = self.pilot;

        Router::new()
            // Cluster
            .route("/api/v1/cluster", get(get_cluster))
            .route("/api/v1/cluster/stats", get(get_cluster_stats))
            // Nodes
            .route("/api/v1/nodes", get(list_nodes))
            .route("/api/v1/nodes", post(register_node))
            .route("/api/v1/nodes/:node_id", get(get_node))
            .route("/api/v1/nodes/:node_id/heartbeat", post(node_heartbeat))
            .route("/api/v1/nodes/:node_id/drain", post(drain_node))
            .route("/api/v1/nodes/:node_id", axum::routing::delete(remove_node))
            // Shards
            .route("/api/v1/shards", get(list_shards))
            .route("/api/v1/shards", post(create_shard))
            .route("/api/v1/shards/:shard_id", get(get_shard))
            .route("/api/v1/shards/:shard_id/migrate", post(migrate_shard))
            // Routing table
            .route("/api/v1/routing", get(get_routing_table))
            // Migration tasks
            .route("/api/v1/migrations", get(list_migrations))
            .route("/api/v1/migrations/:task_id", get(get_migration))
            // Split tasks
            .route("/api/v1/shards/:shard_id/split", post(split_shard))
            .route("/api/v1/splits", get(list_splits))
            .route("/api/v1/splits/:task_id", get(get_split))
            .route(
                "/api/v1/splits/:task_id",
                axum::routing::delete(cancel_split),
            )
            // Scheduling
            .route("/api/v1/schedule", post(trigger_schedule))
            .route("/api/v1/rebalance", post(trigger_rebalance))
            .with_state(pilot)
    }
}

// ==================== Response Types ====================

#[derive(Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    fn ok(data: T) -> Json<Self> {
        Json(Self {
            success: true,
            data: Some(data),
            error: None,
        })
    }

    fn err(msg: impl Into<String>) -> Json<Self> {
        Json(Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        })
    }
}

// ==================== Request Types ====================

#[derive(Deserialize)]
struct RegisterNodeRequest {
    node_id: String,
    grpc_addr: String,
    redis_addr: String,
}

#[derive(Deserialize)]
struct MigrateShardRequest {
    from_node: String,
    to_node: String,
}

#[derive(Deserialize)]
struct CreateShardRequest {
    /// Shard ID (optional, auto-generated if not provided)
    shard_id: Option<String>,
    /// Replica node list
    replica_nodes: Vec<String>,
}

#[derive(Serialize)]
struct RegisterResponse {
    is_new: bool,
}

#[derive(Deserialize)]
struct SplitShardRequest {
    /// Split point slot (target shard will be responsible for [split_slot, source.end))
    split_slot: u32,
    /// Target shard ID (must be provided, and target shard must exist and be healthy)
    target_shard_id: String,
}

// ==================== Handler Functions ====================

// Cluster
async fn get_cluster(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let metadata = pilot.metadata().await;
    ApiResponse::ok(ClusterOverview {
        name: metadata.name.clone(),
        created_at: metadata.created_at.to_rfc3339(),
        routing_version: metadata.routing_table.version,
        node_count: metadata.nodes.len(),
        shard_count: metadata.shards.len(),
    })
}

#[derive(Serialize)]
struct ClusterOverview {
    name: String,
    created_at: String,
    routing_version: u64,
    node_count: usize,
    shard_count: usize,
}

async fn get_cluster_stats(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let metadata = pilot.metadata().await;
    ApiResponse::ok(metadata.stats())
}

// Nodes
async fn list_nodes(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let nodes = pilot.node_manager().list_nodes().await;
    ApiResponse::ok(nodes)
}

async fn get_node(
    State(pilot): State<Arc<Pilot>>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match pilot.node_manager().get_node(&node_id).await {
        Some(node) => ApiResponse::ok(node),
        None => ApiResponse::<NodeInfo>::err(format!("Node {} not found", node_id)),
    }
}

async fn register_node(
    State(pilot): State<Arc<Pilot>>,
    Json(req): Json<RegisterNodeRequest>,
) -> impl IntoResponse {
    let node = NodeInfo::new(req.node_id.clone(), req.grpc_addr, req.redis_addr);
    let result = pilot.node_manager().register(node).await;

    // Trigger scheduling
    let _ = pilot.scheduler().schedule_shard_placement().await;

    // Save metadata
    if let Err(e) = pilot.save().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResponse::<RegisterResponse>::err(e.to_string()),
        );
    }

    info!("Node {} registered via HTTP API", req.node_id);
    let is_new = result == RegisterResult::NewNode;
    (
        StatusCode::CREATED,
        ApiResponse::ok(RegisterResponse { is_new }),
    )
}

async fn node_heartbeat(
    State(pilot): State<Arc<Pilot>>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    if pilot.node_manager().heartbeat(&node_id).await {
        ApiResponse::ok(())
    } else {
        ApiResponse::<()>::err(format!("Node {} not found", node_id))
    }
}

async fn drain_node(
    State(pilot): State<Arc<Pilot>>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    if pilot.node_manager().drain_node(&node_id).await {
        let _ = pilot.save().await;
        ApiResponse::ok(())
    } else {
        ApiResponse::<()>::err(format!("Node {} not found", node_id))
    }
}

async fn remove_node(
    State(pilot): State<Arc<Pilot>>,
    Path(node_id): Path<String>,
) -> impl IntoResponse {
    match pilot.node_manager().remove_node(&node_id).await {
        Some(_) => {
            let _ = pilot.save().await;
            ApiResponse::ok(())
        }
        None => ApiResponse::<()>::err(format!("Node {} not found", node_id)),
    }
}

// Shards
async fn list_shards(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let metadata = pilot.metadata().await;
    let shards: Vec<_> = metadata.shards.values().cloned().collect();
    ApiResponse::ok(shards)
}

async fn create_shard(
    State(pilot): State<Arc<Pilot>>,
    Json(req): Json<CreateShardRequest>,
) -> impl IntoResponse {
    info!(
        "Creating shard: id={:?}, replicas={:?} (empty shard, slots will be assigned via migration)",
        req.shard_id, req.replica_nodes
    );

    let mut metadata = pilot.metadata_mut().await;
    let old_version = metadata.routing_table.version;
    match metadata.create_shard(req.shard_id, req.replica_nodes) {
        Ok(shard) => {
            let new_version = metadata.routing_table.version;
            drop(metadata);
            
            // If routing table version updated, notify watches
            if new_version > old_version {
                pilot.notify_routing_watchers().await;
            }
            
            let _ = pilot.save().await;
            (StatusCode::CREATED, ApiResponse::ok(shard))
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<ShardInfo>::err(e)),
    }
}

async fn get_shard(
    State(pilot): State<Arc<Pilot>>,
    Path(shard_id): Path<String>,
) -> impl IntoResponse {
    let metadata = pilot.metadata().await;
    match metadata.shards.get(&shard_id) {
        Some(shard) => ApiResponse::ok(shard.clone()),
        None => ApiResponse::<ShardInfo>::err(format!("Shard {} not found", shard_id)),
    }
}

async fn migrate_shard(
    State(pilot): State<Arc<Pilot>>,
    Path(shard_id): Path<String>,
    Json(req): Json<MigrateShardRequest>,
) -> impl IntoResponse {
    match pilot
        .scheduler()
        .migrate_shard(&shard_id, &req.from_node, &req.to_node)
        .await
    {
        Ok(task) => {
            let _ = pilot.save().await;
            (StatusCode::CREATED, ApiResponse::ok(task))
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::err(e)),
    }
}

// Routing table
#[derive(Deserialize)]
struct RoutingTableQuery {
    /// Client's current version number (optional)
    /// If provided and matches server version, register watch and wait for updates
    version: Option<u64>,
}

async fn get_routing_table(
    State(pilot): State<Arc<Pilot>>,
    Query(query): Query<RoutingTableQuery>,
) -> impl IntoResponse {
    let metadata = pilot.metadata().await;
    let current_version = metadata.routing_table.version;
    
    // If client provided version number
    if let Some(client_version) = query.version {
        // Versions match, register watch and wait for updates
        if client_version == current_version {
            use tokio::time::{timeout, Duration};
            
            let watcher = pilot.routing_watcher();
            let notify = watcher.watch(client_version).await;
            
            // Wait up to 30 seconds
            match timeout(Duration::from_secs(30), notify.notified()).await {
                Ok(_) => {
                    // Received notification, return latest routing table
                    let metadata = pilot.metadata().await;
                    ApiResponse::ok(metadata.routing_table.clone())
                }
                Err(_) => {
                    // Timeout, return current routing table (even if version is same)
                    ApiResponse::ok(metadata.routing_table.clone())
                }
            }
        } else {
            // Versions don't match, return latest routing table directly
            ApiResponse::ok(metadata.routing_table.clone())
        }
    } else {
        // No version provided, return latest routing table directly
        ApiResponse::ok(metadata.routing_table.clone())
    }
}

// Migration tasks
async fn list_migrations(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let tasks = pilot.scheduler().migration_manager().all_tasks();
    ApiResponse::ok(tasks)
}

async fn get_migration(
    State(pilot): State<Arc<Pilot>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match pilot.scheduler().migration_manager().get_task(&task_id) {
        Some(task) => ApiResponse::ok(task),
        None => ApiResponse::err(format!("Migration task {} not found", task_id)),
    }
}

// Scheduling
async fn trigger_schedule(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let assignments = pilot.scheduler().schedule_shard_placement().await;
    let _ = pilot.save().await;
    ApiResponse::ok(ScheduleResult {
        assignments: assignments.len(),
    })
}

#[derive(Serialize)]
struct ScheduleResult {
    assignments: usize,
}

async fn trigger_rebalance(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let tasks = pilot.scheduler().rebalance().await;
    let _ = pilot.save().await;
    ApiResponse::ok(RebalanceResult {
        migrations: tasks.len(),
    })
}

#[derive(Serialize)]
struct RebalanceResult {
    migrations: usize,
}

// Split tasks
async fn split_shard(
    State(pilot): State<Arc<Pilot>>,
    Path(shard_id): Path<String>,
    Json(req): Json<SplitShardRequest>,
) -> impl IntoResponse {
    info!(
        "Splitting shard {}: split_slot={}, target={:?}",
        shard_id, req.split_slot, req.target_shard_id
    );

    match pilot
        .scheduler()
        .split_shard(&shard_id, req.split_slot, req.target_shard_id)
        .await
    {
        Ok(task) => {
            let _ = pilot.save().await;
            (StatusCode::CREATED, ApiResponse::ok(task))
        }
        Err(e) => (StatusCode::BAD_REQUEST, ApiResponse::<SplitTask>::err(e)),
    }
}

async fn list_splits(State(pilot): State<Arc<Pilot>>) -> impl IntoResponse {
    let tasks = pilot.scheduler().split_manager().all_tasks();
    ApiResponse::ok(tasks)
}

async fn get_split(
    State(pilot): State<Arc<Pilot>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match pilot.scheduler().split_manager().get_task(&task_id) {
        Some(task) => ApiResponse::ok(task),
        None => ApiResponse::<SplitTask>::err(format!("Split task {} not found", task_id)),
    }
}

async fn cancel_split(
    State(pilot): State<Arc<Pilot>>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    match pilot
        .scheduler()
        .split_manager()
        .cancel_split(&task_id)
        .await
    {
        Ok(()) => {
            let _ = pilot.save().await;
            ApiResponse::ok(CancelSplitResult { cancelled: true })
        }
        Err(e) => ApiResponse::<CancelSplitResult>::err(e),
    }
}

#[derive(Serialize)]
struct CancelSplitResult {
    cancelled: bool,
}
