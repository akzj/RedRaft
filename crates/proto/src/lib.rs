//! Shared gRPC protocol definitions for node-to-node communication
//!
//! This crate provides protocol definitions for communication between nodes,
//! such as snapshot transfer, data migration, and health checks.
//! These protocols are separate from Raft consensus protocols (defined in `crates/raft/proto`).

// Include generated protobuf code
// The include_proto! macro generates a module matching the proto package name
pub mod node {
    tonic::include_proto!("node");
}

// Re-export commonly used types for convenience
pub use node::*;

