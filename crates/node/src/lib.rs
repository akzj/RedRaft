//! RedRaft - Redis-compatible distributed key-value store
//!
//! Built on Raft consensus algorithm for reliability and consistency.

pub mod config;
pub mod node;
pub mod pilot_client;
pub mod router;
pub mod server;
pub mod snapshot_transfer;
pub mod snapshot_service;
pub mod snapshot_restore;
pub mod state_machine;

pub use config::{Config, ConfigError};
pub use node::RedRaftNode;
pub use pilot_client::{PilotClient, PilotClientConfig, RoutingTable};
// Re-export RaftGroupStatus from pilot crate
pub use pilot::RaftGroupStatus;
pub use server::RedisServer;
pub use state_machine::KVStateMachine;
