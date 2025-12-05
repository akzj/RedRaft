//! Snapshot storage module for Raft consensus.
//!
//! This module provides persistent snapshot storage with support for multi-raft scenarios.
//! Each Raft group's snapshots are stored in a separate directory with metadata and data files.
//!
//! # Directory Structure
//!
//! ```text
//! {base_dir}/
//! └── {group_id}_{node_id}/
//!     └── snapshot/
//!         ├── meta.json       # Snapshot metadata (index, term, config)
//!         ├── data.bin        # Snapshot data
//!         └── checksum.sha256 # Data checksum for integrity verification
//! ```
//!
//! # Features
//!
//! - Atomic snapshot writes using temporary files and rename
//! - Checksum verification for data integrity
//! - Support for multiple Raft groups
//! - Async I/O operations

mod store;

#[cfg(test)]
mod tests;

pub use store::{FileSnapshotStorage, SnapshotStorageOptions};

