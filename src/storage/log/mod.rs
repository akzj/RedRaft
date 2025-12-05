//! Log storage module for Raft consensus.
//! 
//! This module provides append-only log storage with support for multi-raft scenarios.
//! All operations (append, truncate) are written to log segments without actual deletion,
//! and an index is maintained to track valid entries.
//!
//! # Module Structure
//! 
//! - `entry`: Entry metadata and serialization structures
//! - `segment`: Log segment file operations  
//! - `store`: High-level log store with caching and async operations
//! - `manager`: Multi-segment management with rotation and cleanup

mod entry;
mod manager;
mod segment;
mod store;

#[cfg(test)]
mod tests;

// Re-export public types
pub use entry::{
    EntryHeader, EntryMeta, EntryType, HardStateRecord, Index, LogEntryRecord, 
    RaftEntryIndex, TruncateRecord, ENTRY_HEADER_SIZE, ENTRY_MAGIC_NUM,
};

pub use manager::{
    DiskStats, SegmentManager, SegmentManagerOptions, SegmentMeta,
    DEFAULT_MAX_SEGMENT_SIZE, MIN_SEGMENT_SIZE,
};

pub use segment::{
    LogSegment, LogSegmentTail, LogStoreSnapshot, 
    SegmentTruncateLog, SnapshotStore,
};

pub use store::{
    LogEntryOp, LogEntryOpRequest, LogEntryStore, 
    LogEntryStoreInner, LogEntryStoreOptions,
    ManagedLogEntryStore, ManagedLogEntryStoreOptions,
};
