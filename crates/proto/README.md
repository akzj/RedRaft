# Proto Crate

Shared gRPC protocol definitions for node-to-node communication.

## Overview

This crate provides protocol definitions for communication between nodes, such as:
- **Split Snapshot Transfer**: Transferring split snapshots during shard splitting
- **Data Migration**: Migrating data between nodes during rebalancing
- **Health Checks**: Health check and status queries between nodes

These protocols are separate from Raft consensus protocols (which are defined in `crates/raft/proto`).

## Services

### SplitSnapshotService

Used for transferring split snapshots during shard splitting operations.

- `TransferSplitSnapshot`: Stream-based, chunked transfer of snapshot data
- `GetTransferProgress`: Query the progress of an ongoing transfer

### MigrationService

Used for migrating data between nodes during rebalancing or node removal.

- `StartMigration`: Start a data migration task
- `TransferMigrationData`: Stream-based transfer of migration data
- `CompleteMigration`: Complete a migration task
- `CancelMigration`: Cancel an ongoing migration

### HealthService

Used for health checks and status queries between nodes.

- `HealthCheck`: Check the health of a node
- `GetNodeStatus`: Get detailed status information about a node

## Usage

### Adding to your Cargo.toml

```toml
[dependencies]
proto = { path = "../proto" }
```

### Using in your code

```rust
use proto::node::{
    split_snapshot_service_server::{SplitSnapshotService, SplitSnapshotServiceServer},
    split_snapshot_service_client::SplitSnapshotServiceClient,
    TransferSplitSnapshotRequest,
    TransferSplitSnapshotResponse,
    // ... other types
};
```

### Example: Adding to gRPC Server

```rust
use tonic::transport::Server;
use proto::node::split_snapshot_service_server::SplitSnapshotServiceServer;

// Create your service implementation
let split_snapshot_service = SplitSnapshotServiceServer::new(my_impl);

// Add to server (along with Raft service)
Server::builder()
    .add_service(raft_service)              // From raft::network
    .add_service(split_snapshot_service)    // From proto crate
    .serve(addr)
    .await?;
```

## Protocol Design Principles

1. **Separation of Concerns**: Business protocols are separate from Raft consensus protocols
2. **Streaming Support**: Large data transfers use streaming to support chunked transfers
3. **Progress Tracking**: Long-running operations support progress queries
4. **Error Handling**: All operations include error messages for debugging
5. **Extensibility**: Protocols are designed to be extended with new services as needed

## Building

The proto files are automatically compiled during `cargo build` using the `build.rs` script.

To regenerate the Rust code from proto files:

```bash
cargo build --package proto
```

## Adding New Services

1. Add your service definition to `proto/node.proto`
2. Run `cargo build --package proto` to regenerate Rust code
3. Use the generated types in your implementation

